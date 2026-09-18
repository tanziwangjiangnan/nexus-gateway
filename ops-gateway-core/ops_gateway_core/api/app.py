"""HTTP API 层 — FastAPI 应用构建器。

职责边界（见 docs/模块约定.md）：本文件只做「装配 + 转发」——
鉴权、请求解析、调用 provider_router 的决策函数、构造响应；
额度 / 降级 / 选路 / 评分等业务判断都在 provider_router 对应模块里。

一次 POST /v1/chat/completions 的处理顺序：
  1. 鉴权（gateway_key）+ 请求解析（model / messages / stream / 用户自带 key）
  2. 判定是否「显式指定模型」（auto、*、gateway-auto、default-auto 视为未指定 → 维护通道）
  3. 前置检查层（仅未显式指定时）：复杂度规则 + token 置信度（1s，失败忽略）
     → select_path() 给出路由规则（routing_rules）
  4. 定位资源池，优先级：模型名精确匹配 > 路由规则 > 关键词路由 > 默认 pool_a
  5. 故障转移链：池内按「额度 → 能力标签 → 权重」选 provider → 调用；
     失败按 pool.fallback 链降级（pool_a → pool_b → pool_c），决策在 provider_router/fallback.py

build_app(cfg, deps) 返回 FastAPI 实例；共享状态与外部能力全部经 deps 注入，
既避免与 gateway.py 循环依赖，也让测试可以替换 DB / 选路实现。

v3.2: 从 gateway.py 拆分。
v3.12: 降级决策抽到 provider_router/fallback.py。
v3.13: 额度包装下沉到 provider_router/quota.py（本文件只留连接与日志）。
"""

# 保持与原 gateway.py 相同的模块级导入，确保依赖可用
import datetime
import hashlib
import asyncio
import json
import os
import random
import re
import subprocess
import shlex
import sys
import time
import threading
import yaml
import httpx

from provider_router import Router
from provider_router import select_provider_auto
from provider_router.fallback import plan_pool_fallback
from ..cfg import get_db, db_conn as _default_db_conn
from ..fiber import FiberRuntime

# ── 额度感知与任务保护（计划书 v1）──
# 纯逻辑在 provider_router.quota；此处只做别名，保持 app.py 内命名简短
from provider_router import quota as _quota_mod
from provider_router import select_runner_up
from ..constants import APPROVAL_TTL as _APPROVAL_TTL  # 审批缓存 TTL（与 routes_admin 共用）
from provider_router.scoring import (
    read_force_on as _read_force_on,
    should_score as _should_score,
    supervisor_cfg as _supervisor_cfg,
)
_quota_classify = _quota_mod.classify_error
_quota_budget_level = _quota_mod.budget_level
_quota_get_statuses = _quota_mod.get_statuses
_quota_filter = _quota_mod.filter_candidates
_quota_record_error = _quota_mod.record_error
_quota_recover_due = _quota_mod.recover_due

# prometheus 客户端（延迟导入，兼容无 prometheus 环境）
try:
    from prometheus_client import Counter, Gauge, Histogram, generate_latest, REGISTRY
    _HAS_PROMETHEUS = True
except ImportError:
    _HAS_PROMETHEUS = False
    def generate_latest(*a, **k):
        return b""
    Counter = Gauge = Histogram = None
    REGISTRY = None


from ..scoring_worker import (
    get_provider_from_last_usage as _get_provider_from_last_usage,
    score_by_runner_up as _score_by_runner_up,
)


# ── 监督者评分：触发判定在 provider_router/scoring.py（纯逻辑），
#    后台打分任务在 ops_gateway_core/scoring_worker.py（IO）。
#    设计文档：.openhands/memory/designs/supervisor-scoring.md


def build_app(cfg, deps):
    """构建 FastAPI 应用实例。

    deps 注入的共享状态：
    - disabled_providers / router_state / fiber_runtime
    - dynamic_weights / approval_cache / pending_approvals / lock
    - serial_locks / throttle_windows / prometheus_registered / monitor
    - 函数委托：execute_plugin / format_string / global_call_* / undo_* / fiber_* / select_*
    """
    # ── 解构依赖（保持原函数体内的名字不变） ──
    _disabled_providers = deps["disabled_providers"]
    _router_state = deps["router_state"]
    _fiber_runtime = deps["fiber_runtime"]
    _dynamic_weights = deps["dynamic_weights"]
    _approval_cache = deps["approval_cache"]
    _pending_approvals = deps["pending_approvals"]
    _lock = deps["lock"]
    _serial_locks = deps["serial_locks"]
    _throttle_windows = deps["throttle_windows"]
    get_db = deps["get_db"]
    # db_conn 同样走注入：生产用模块级全局路径，测试可指向临时库。
    # 否则 app 内 with db_conn() 会写生产库，与注入的 get_db 分叉。
    db_conn = deps.get("db_conn") or _default_db_conn
    _execute_plugin = deps["execute_plugin"]
    _format_string = deps["format_string"]
    _global_call_lookup = deps["global_call_lookup"]
    _global_call_add = deps["global_call_add"]
    undo_register = deps["undo_register"]
    undo_pop = deps["undo_pop"]
    fiber_create = deps["fiber_create"]
    fiber_register = deps["fiber_register"]
    fiber_fail = deps["fiber_fail"]
    fiber_commit = deps["fiber_commit"]
    find_model_config = deps["find_model_config"]
    select_pool_by_keywords = deps["select_pool_by_keywords"]
    select_provider_by_weight = deps["select_provider_by_weight"]
    select_provider_with_runner_up = deps["select_provider_with_runner_up"]
    select_provider_by_strategy = deps.get("select_provider_by_strategy")
    check_rate_limit = deps["check_rate_limit"]
    _quality_factors = deps["quality_factors"]
    _benchmark_loaded = deps.get("benchmark_loaded", False)
    # ── 第二名检查者（Runner-up Scoring）的运行时状态 ──
    # 自适应采样：冷启动每次都审 → 稳态概率衰减 → 大变量强制复审
    # [2026-09-18] 原本在 build_app 末尾（插件端点附近）偶然定义，chat 主链靠闭包引用；
    # 抽成 router 后必须在此显式定义，否则主链与插件会各自持有一份状态。
    _scoring_state = {}  # {provider: {count, last, last_runners, recent_scores, variance_boost_remaining}}
    _user_factors = deps["user_factors"]
    _log_matches = deps["log_matches"]
    _parse_log_line = deps["parse_log_line"]

    from fastapi import FastAPI, Request, HTTPException
    from fastapi.responses import JSONResponse, StreamingResponse, Response

    app = FastAPI(title="模型池网关 v2", version="0.2.0")

    # ── 额度过滤辅助（计划书 v1）：普通请求 / 角色路径 / 聚合共用 ──
    def _apply_quota_filter(providers, budget):
        """额度过滤（含冷却复活）—— 逻辑在 provider_router/quota.py。

        [2026-09-18] 模块约定第 1 步拆分：DB 与过滤逻辑下沉到模块，
        本函数只做「开连接 → 调用 → 打日志」；异常时不阻断路由（原样返回候选）。
        """
        try:
            with db_conn() as conn:
                allowed, blocked = _quota_mod.prepare_candidates(conn, providers, budget, cfg)
            if blocked:
                print("🚫 额度过滤: " + ", ".join(f"{n}({r})" for n, r in blocked))
            return allowed
        except Exception as e:
            print(f"⚠️  额度过滤异常（忽略）: {e}")
            return providers

    def _quota_write_error(provider, http_status, body):
        """额度耗尽时落库并返回 True —— 分类与写入都在 provider_router/quota.py。

        [2026-09-18] 同上：只保留连接与日志；任何异常按「非额度」处理，避免误伤路由。
        """
        try:
            with db_conn() as conn:
                exhausted, detail = _quota_mod.record_error_if_exhausted(
                    conn, provider, http_status, body, cfg)
            if exhausted:
                print(f"💰 额度耗尽: {provider} ({detail})")
            return exhausted
        except Exception:
            return False

    # ── 鉴权中间件 ──
    @app.middleware("http")
    async def auth_check(request: Request, call_next):
        if request.url.path in ("/health", "/metrics", "/chat"):
            return await call_next(request)
        if request.url.path.startswith("/v1/plugins/"):
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        expected = f"Bearer {cfg['gateway_key']}"
        if auth != expected:
            return JSONResponse(status_code=401, content={"error": "unauthorized"})
        return await call_next(request)

    # ── 健康检查 ──
    @app.get("/health")
    async def health():
        # 更新池健康指标
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            enabled = sum(1 for pv in pool_cfg.get("providers", [])
                          if pv["name"] not in _disabled_providers)
            app.state.pool_health.labels(pool=pool_name).set(1 if enabled > 0 else 0)
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            for pv in pool_cfg.get("providers", []):
                app.state.provider_up.labels(provider=pv["name"]).set(
                    0 if pv["name"] in _disabled_providers else 1)
        return {"status": "ok", "version": "0.2.0", "time": datetime.datetime.now().isoformat()}

    # ── 聊天页面（免鉴权） ──
    from .routes_meta import build_meta_router
    app.include_router(build_meta_router(
        Response=Response,
        _disabled_providers=_disabled_providers,
        cfg=cfg,
        db_conn=db_conn,
    ))
    from .routes_chat import build_chat_router
    app.include_router(build_chat_router(
        HTTPException=HTTPException,
        Request=Request,
        Response=Response,
        Router=Router,
        StreamingResponse=StreamingResponse,
        _apply_quota_filter=_apply_quota_filter,
        _benchmark_loaded=_benchmark_loaded,
        _disabled_providers=_disabled_providers,
        _quality_factors=_quality_factors,
        _quota_budget_level=_quota_budget_level,
        _quota_write_error=_quota_write_error,
        _router_state=_router_state,
        _score_by_runner_up=_score_by_runner_up,
        _scoring_state=_scoring_state,
        _should_score=_should_score,
        app=app,
        asyncio=asyncio,
        cfg=cfg,
        check_rate_limit=check_rate_limit,
        db_conn=db_conn,
        fiber_commit=fiber_commit,
        fiber_create=fiber_create,
        fiber_fail=fiber_fail,
        find_model_config=find_model_config,
        httpx=httpx,
        json=json,
        plan_pool_fallback=plan_pool_fallback,
        select_pool_by_keywords=select_pool_by_keywords,
        select_provider_auto=select_provider_auto,
        select_provider_by_strategy=select_provider_by_strategy,
        select_provider_with_runner_up=select_provider_with_runner_up,
        select_runner_up=select_runner_up,
        time=time,
    ))
    # ── Prometheus 指标（模块级，避免重复注册） ──
    _prometheus_registered = False
    def _ensure_prometheus():
        nonlocal _prometheus_registered
        if _prometheus_registered:
            return
        from prometheus_client import Counter, Gauge, Histogram
        app.state.req_counter = Counter("gateway_requests_total", "Total requests", ["pool", "provider", "status"])
        app.state.pool_health = Gauge("gateway_pool_healthy", "Pool health 1/0", ["pool"])
        app.state.provider_up = Gauge("gateway_provider_up", "Provider up 1/0", ["provider"])
        app.state.req_duration = Histogram("gateway_request_duration_seconds", "Request latency",
                                           ["provider"], buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0])
        _prometheus_registered = True

    _ensure_prometheus()

    @app.get("/metrics")
    async def metrics():
        from prometheus_client import generate_latest, REGISTRY
        return Response(content=generate_latest(REGISTRY).decode(), media_type="text/plain")

    # ── Admin 端点：已抽到 api/routes_admin.py（2026-09-18，见 docs/模块约定.md 第 3 步）──
    from .routes_admin import build_admin_router
    app.include_router(build_admin_router(
        cfg=cfg,
        db_conn=db_conn,
        get_db=get_db,
        undo_register=undo_register,
        undo_pop=undo_pop,
        fiber_create=fiber_create,
        fiber_fail=fiber_fail,
        fiber_commit=fiber_commit,
        fiber_register=fiber_register,
        _approval_cache=_approval_cache,
        _disabled_providers=_disabled_providers,
        _dynamic_weights=_dynamic_weights,
        _fiber_runtime=_fiber_runtime,
        _lock=_lock,
        _log_matches=_log_matches,
        _parse_log_line=_parse_log_line,
        _user_factors=_user_factors,
        _get_provider_from_last_usage=_get_provider_from_last_usage,
    ))

    from .routes_plugins import build_plugins_router
    app.include_router(build_plugins_router(
        HTTPException=HTTPException,
        Request=Request,
        _APPROVAL_TTL=_APPROVAL_TTL,
        _approval_cache=_approval_cache,
        _execute_plugin=_execute_plugin,
        _fiber_runtime=_fiber_runtime,
        _format_string=_format_string,
        _global_call_add=_global_call_add,
        _global_call_lookup=_global_call_lookup,
        _serial_locks=_serial_locks,
        _throttle_windows=_throttle_windows,
        asyncio=asyncio,
        cfg=cfg,
        fiber_commit=fiber_commit,
        fiber_create=fiber_create,
        fiber_fail=fiber_fail,
        fiber_register=fiber_register,
        hashlib=hashlib,
        httpx=httpx,
        json=json,
        subprocess=subprocess,
        time=time,
    ))
    return app

