#!/usr/bin/env python3
"""模型池统一网关 v2 — 三资源池版
独立基础设施，所有智能体平等消费。
OpenAI 兼容接口，按 model 名路由到三池，池内权重轮询，跨池故障转移。

用法:
  python3 gateway.py                    # 启动网关 :8646
  python3 gateway.py probe              # 一次性健康探测所有模型
  python3 gateway.py probe --watch      # 持续探测，异常→QQ告警
  python3 gateway.py models             # 列出模型目录
  python3 gateway.py usage              # 查看用量
  python3 gateway.py git-log            # 配置逆栈：commit 历史
  python3 gateway.py git-diff           # 配置逆栈：未提交变更
  python3 gateway.py sync-runtime       # 运行时同步（发 SIGHUP 重载）
  python3 gateway.py undo               # 运行时逆栈：撤销最后一条操作
  python3 gateway.py undo-list          # 运行时逆栈：查看所有操作
  python3 gateway.py fiber              # 查看 fiber 树（通过 Admin API）
  python3 gateway.py scan-agents         # 自动发现并接入智能体（含插件声明）
  python3 gateway.py scan-agents --dir /opt/agents  # 指定扫描目录
  python3 gateway.py check-deps           # 反向依赖扫描：检查 Key 被哪些组件引用
  python3 gateway.py check-deps --auto-sync  # 扫描 + 自动同步（本地 sed + 远程 SSH）
  python3 gateway.py check-deps "sk-xxx"  # 只检查特定 Key
  python3 gateway.py quality             # 查看 Provider 质量排名（检查者评分）
  python3 gateway.py feedback-stats      # 查看 Provider 用户反馈统计

API 端点:
  GET  /chat                    → 聊天页面（免鉴权，自备 Key）
  GET  /v1/models                → 模型目录（含池/provider/健康/用量）
  POST /v1/chat/completions      → OpenAI 兼容，三池路由 + 自定义 Key
  POST /v1/plugins/{id}/call     → 统一插件调用：capabilities 校验 + 重复调用拦截 + Fiber 逆操作 + 动态校验
  GET  /health                   → 网关自身健康检查
  GET  /metrics                  → Prometheus 指标
  GET  /admin/pools              → 查看各池/provider 状态
  POST /admin/pools/{pool}/providers/{provider}/toggle  → 启停 provider
  GET  /admin/undo               → 运行时逆栈：撤销最后一条操作
  GET  /admin/undo-list          → 运行时逆栈：查看所有操作
  POST /admin/mcp/toggle         → MCP 工具：Agent 调 toggle，走审批缓存 + fiber
  GET  /admin/mcp/approvals      → 查看审批缓存
  GET  /admin/mcp/status         → MCP 总览：熔断+权重+审批（含错误率）
  POST /admin/fiber/create       → 创建 Fiber 任务树节点
  POST /admin/fiber/{id}/fail    → 失败 fiber：级联回滚子孙
  POST /admin/fiber/{id}/commit  → 提交 fiber：合并 undo 到父节点
  GET  /admin/fiber/tree         → 查看 fiber 森林
  GET  /admin/agents/declaration → 返回智能体声明配置（agents 段）
  GET  /admin/agents/status      → 返回所有声明 Agent 的存活状态
  POST /admin/fiber/check        → 创建检查任务 fiber（执行者-检查者模式）
  GET  /admin/logs               → 聚合所有声明 Agent 的日志，按时间合并，支持 ?level=&agent=&lines= 过滤
"""
import datetime
import hashlib
import asyncio
import json
import os
import random
import re
import signal
import subprocess
import shlex
import sys
import time
import threading
import yaml
import httpx

# ── 组件包导入 ──
from provider_router import Router, RouterState, CircuitBreakerMonitor
from fiber_tree import FiberTree, MemoryStorage
from ops_gateway_core import ConfigLoader, get_db, init_registry
from ops_gateway_core.fiber import FiberRuntime
from ops_gateway_core.ops.check_deps import check_deps_on_diff

# ── 路径 ──
BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE, "gateway.yaml")
DB = os.path.join(BASE, "gateway.db")

# ── 全局运行时状态 ──
_config = {}                    # 当前配置
_disabled_providers = set()     # 被自动或手动禁用的 provider
_rate_limit_buckets = {}        # {provider_name: [t1, t2, ...]}
_dynamic_weights = {}           # {provider_name: effective_weight} 由熔断线程更新
_quality_factors = {}           # {provider_name: float} 质量信誉因子（检查者评分驱动）
_user_factors = {}              # {provider_name: float} 用户信誉因子（用户反馈驱动）
_approval_cache = {}            # {(agent_id, action_hash): expiry_timestamp}
_pending_approvals = {}         # {action_id: {action, params, agent_id}}
_lock = threading.Lock()
_fiber_runtime = FiberRuntime(lock=_lock)  # 运行时 Fiber + undo + 全局去重

# ── 组件实例 ──
_router_state = RouterState(
    disabled_providers=_disabled_providers,
    dynamic_weights=_dynamic_weights,
    quality_factors=_quality_factors,
    user_factors=_user_factors,
    rate_limit_buckets=_rate_limit_buckets,
    lock=_lock,
)
_fiber_tree = FiberTree()

# ── 插件排队机制（v2.8） ──
_serial_locks = {}              # {resource_lock_key: asyncio.Lock} — 串行锁池
_throttle_windows = {}          # {plugin_id: [t1, t2, ...]} — 速率限制窗口
# ── 配置加载（委托给 hermes_cfg） ──
_config_loader = ConfigLoader(path=CONFIG_PATH)

def load_config(path=None):
    if path:
        # 指定路径时直接加载（不更新全局）
        from provider_router.config import load_config as pr_load
        return pr_load(path)
    return _config_loader.load()

def reload_config():
    """重载 YAML 配置，同步运行时状态，清空运行时逆栈。
    
    幂等补偿说明：
    1. YAML 文件 → 原地替换 _config（闭包引用同步）
    2. _disabled_providers.clear() — 运行时禁用状态重置
       （防止 git revert 后 YAML 变回但某 provider 仍被禁用）
    3. _undo_stack.clear() — 运行时逆栈清空，新配置是新起点
    4. _rate_limit_buckets 不清（滑动窗口，自动过期）
    """
    global _config, _disabled_providers
    new_cfg = _config_loader.reload()
    if _config:
        check_deps_on_diff(_config, new_cfg, BASE, label="配置热加载")
        _config.clear()
        _config.update(new_cfg)
    else:
        _config = new_cfg
    _disabled_providers.clear()
    undo_clear("配置重载")
    return _config
# ── 数据库（委托给 hermes_cfg） ──
# get_db() 和 init_registry() 已从 hermes_cfg 导入
# ── 自动熔断 + 动态权重（后台线程，每 30s） ──
_circuit_breaker = None  # 在 main() 中初始化

def _circuit_breaker_loop(cfg):
    """后台线程 wrapper — 委托给 CircuitBreakerMonitor。"""
    global _circuit_breaker
    _circuit_breaker = CircuitBreakerMonitor(
        get_db=get_db,
        cfg_getter=lambda: _config,
        disabled_providers=_disabled_providers,
        dynamic_weights=_dynamic_weights,
        quality_factors=_quality_factors,
        user_factors=_user_factors,
        undo_register=undo_register,
        interval=30.0,
        lock=_lock,
    )
    _circuit_breaker._loop()

# ── 运行时逆栈（任务级幂等补偿，委托给 hermes_fiber） ──
def undo_register(description, revert_callable):
    """注册运行时操作的撤销回调。每个原子操作应有对应的逆操作。"""
    _fiber_runtime.undo_register(description, revert_callable)

def undo_pop():
    """弹出并执行最后一条撤销回调。"""
    return _fiber_runtime.undo_pop()

def undo_clear(reason=""):
    """清空全局逆栈（配置热加载时调用，因为新配置是新起点）。"""
    _fiber_runtime.undo_clear(reason)

# ── Fiber 树形上下文（Agent 任务级可逆，委托给 hermes_fiber） ──
def fiber_create(agent_id, description, parent_id=None, capabilities=None):
    return _fiber_runtime.fiber_create(agent_id, description, parent_id, capabilities)

def fiber_register(fiber_id, description, revert_callable):
    return _fiber_runtime.fiber_register(fiber_id, description, revert_callable)

def fiber_fail(fiber_id, cascade_parent=True):
    return _fiber_runtime.fiber_fail(fiber_id, cascade_parent)

def fiber_commit(fiber_id):
    return _fiber_runtime.fiber_commit(fiber_id)

# ── Root 级全局去重表（v2.8，委托给 hermes_fiber） ──
def _global_call_lookup(plugin_id, params_hash):
    return _fiber_runtime.global_call_lookup(plugin_id, params_hash)

def _global_call_add(plugin_id, params_hash, fiber_id, result_preview=""):
    _fiber_runtime.global_call_add(plugin_id, params_hash, fiber_id, result_preview)

def _global_call_remove(plugin_id, params_hash):
    _fiber_runtime.global_call_remove(plugin_id, params_hash)

def _cleanup_global_history_for_fiber(fiber_id):
    _fiber_runtime._cleanup_global_history_for_fiber(fiber_id)

def _cleanup_global_history_periodic():
    _fiber_runtime.cleanup_global_history_periodic()

# ── 插件执行器（v2.8 排队用） ──
async def _execute_plugin(plugin: dict, plugin_id: str, params: dict, timeout: int):
    """执行插件（http/cli），返回 (result, error)。独立于排队逻辑。"""
    global _disabled_providers
    exec_mode = plugin.get("execution", "http")
    result = {}
    error = None

    try:
        if exec_mode == "http":
            endpoint = plugin.get("endpoint", "")
            if not endpoint:
                error = f"plugin '{plugin_id}' has no endpoint"
                return result, error
            call_url = _format_string(endpoint, params)
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(call_url, json=params)
                if resp.status_code >= 400:
                    error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                else:
                    result = resp.json()

        elif exec_mode == "cli":
            command = plugin.get("command", "")
            if not command:
                error = f"plugin '{plugin_id}' has no command"
                return result, error
            cmd = _format_string(command, params)
            try:
                ret = subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, timeout=timeout
                )
                if ret.returncode != 0:
                    error = f"CLI exit {ret.returncode}: {ret.stderr[:200] or ret.stdout[:200]}"
                else:
                    try:
                        result = json.loads(ret.stdout)
                    except json.JSONDecodeError:
                        result = {"stdout": ret.stdout.strip(), "stderr": ret.stderr.strip()}
            except subprocess.TimeoutExpired:
                error = f"CLI timeout after {timeout}s"
        else:
            error = f"unsupported execution mode: {exec_mode}"

    except httpx.TimeoutException:
        error = f"HTTP timeout after {timeout}s"
    except httpx.ConnectError:
        provider_name = plugin.get("provider", "")
        if provider_name and provider_name not in _disabled_providers:
            _disabled_providers.add(provider_name)
        error = f"provider {provider_name} unreachable"
    except Exception as e:
        error = str(e)

    return result, error

# ── 日志解析辅助 ──
_LOG_PATTERN = re.compile(
    r'(?P<ts>\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)'
    r'\s*'
    r'(?:\[?\s*(?P<level>DEBUG|INFO|WARN(?:ING)?|ERROR|CRITICAL|FATAL|TRACE)\s*\]?)?',
    re.IGNORECASE,
)

def _parse_log_line(line: str, agent_id: str, source: str) -> dict | None:
    """解析单行日志，返回结构化字典或 None。"""
    line = line.rstrip("\n\r")
    if not line:
        return None
    m = _LOG_PATTERN.search(line)
    ts = m.group("ts") if m else ""
    level = m.group("level").upper() if m and m.group("level") else "INFO"
    # 规范化级别名
    level = level.replace("WARNING", "WARN").replace("CRITICAL", "FATAL")
    return {
        "timestamp": ts,
        "level": level,
        "agent_id": agent_id,
        "source": source,
        "message": line,
    }

def _log_matches(entry: dict, filter_levels: list[str], since_str: str) -> bool:
    """检查日志条目是否匹配过滤条件。"""
    if filter_levels and entry.get("level", "") not in filter_levels:
        return False
    if since_str and entry.get("timestamp", "") < since_str:
        return False
    return True

def _format_string(template: str, params: dict) -> str:
    """替换模板字符串中的 {key} 占位符为 params 中的值。"""
    import re
    def _replacer(m):
        key = m.group(1)
        val = params.get(key, m.group(0))
        return str(val) if val is not None else m.group(0)
    return re.sub(r'\{(\w+)\}', _replacer, template)

# ── 路由引擎 ──

def find_model_config(cfg, model):
    """遍历所有池查找 model 所属的 (pool_name, provider_config, canonical_model_name)；大小写不敏感"""
    return Router.find_model(cfg, model)

def select_pool_by_keywords(cfg, messages_text):
    """关键词匹配 → 返回 pool_name 或 None"""
    return Router.select_pool_by_keywords(cfg, messages_text)

def select_provider_by_weight(providers, model=None):
    """按权重随机选一个 provider，跳过禁用的；若指定 model 则只选有该模型的（大小写不敏感）"""
    return Router.select_provider(providers, _router_state, model=model)

def select_provider_with_runner_up(providers, model=None):
    """按权重选 provider，同时返回第二名（检查者）。返回 (selected, runner_up, weights)"""
    return Router.select_provider_with_runner_up(providers, _router_state, model=model)

def check_rate_limit(provider_name, max_rps):
    """滑动窗口限流，返回 True=通过 False=限流"""
    return Router.check_rate_limit(provider_name, max_rps, _router_state)
# ── FastAPI 应用 ──
def create_app(cfg):
    """构建 FastAPI 应用实例（薄封装，委托给 hermes_api.build_app）。"""
    from ops_gateway_core import build_app
    deps = {
        "disabled_providers": _disabled_providers,
        "router_state": _router_state,
        "fiber_runtime": _fiber_runtime,
        "dynamic_weights": _dynamic_weights,
        "approval_cache": _approval_cache,
        "pending_approvals": _pending_approvals,
        "lock": _lock,
        "serial_locks": _serial_locks,
        "throttle_windows": _throttle_windows,
        "get_db": get_db,
        "execute_plugin": _execute_plugin,
        "format_string": _format_string,
        "global_call_lookup": _global_call_lookup,
        "global_call_add": _global_call_add,
        "undo_register": undo_register,
        "undo_pop": undo_pop,
        "fiber_create": fiber_create,
        "fiber_register": fiber_register,
        "fiber_fail": fiber_fail,
        "fiber_commit": fiber_commit,
        "find_model_config": find_model_config,
        "select_pool_by_keywords": select_pool_by_keywords,
        "select_provider_by_weight": select_provider_by_weight,
        "select_provider_with_runner_up": select_provider_with_runner_up,
        "check_rate_limit": check_rate_limit,
        "quality_factors": _quality_factors,
        "user_factors": _user_factors,
        "log_matches": _log_matches,
        "parse_log_line": _parse_log_line,
    }
    return build_app(cfg, deps)


# ── 主入口（薄分发器，委托给 hermes_ops） ──
def main():
    global _config
    _config = reload_config()
    os.makedirs(BASE, exist_ok=True)
    init_registry(_config)

    from ops_gateway_core import (
        probe_all, cmd_models, cmd_usage,
        cmd_quality, cmd_feedback_stats,
        cmd_git_log, cmd_git_diff,
        cmd_sync_runtime,
        cmd_undo_remote, cmd_undo_list_remote,
        cmd_fiber_view,
        cmd_check_deps, cmd_scan_agents,
    )

    args = sys.argv[1:]

    if not args:
        import uvicorn
        app = create_app(_config)
        with open(os.path.join(BASE, "gateway.pid"), "w") as f:
            f.write(str(os.getpid()))
        total_models = sum(len(pv.get("models", []))
                           for pc in _config.get("pools", {}).values()
                           for pv in pc.get("providers", []))
        total_providers = len(_config.get("providers", {}))
        print(f"🔌 模型池网关 v2 启动: http://{_config.get("host","127.0.0.1")}:{_config.get("port",8646)}")
        print(f"   {total_models} 个模型, {total_providers} 个 provider, {len(_config.get("pools",{}))} 个资源池")
        print(f"   SIGHUP 热加载已启用 (kill -HUP {os.getpid()} 重载配置)")
        t = threading.Thread(target=_circuit_breaker_loop, args=(_config,), daemon=True)
        t.start()
        print(f"   🔄 自动熔断已启用 (30s 滑动窗口, 错误率 >20% 自动禁用)")
        t2 = threading.Thread(target=_cleanup_global_history_periodic, daemon=True)
        t2.start()
        print(f"   📋 全局去重已启用 (24h TTL, 1h 定时清理)")
        def _hup(signum, frame):
            try:
                subprocess.run(
                    ["git", "add", "gateway.yaml", "gateway.py"],
                    cwd=BASE, capture_output=True, timeout=10)
                subprocess.run(
                    ["git", "commit", "--allow-empty", "-m",
                     f"auto-snap before SIGHUP {datetime.datetime.now().strftime('%H:%M:%S')}"],
                    cwd=BASE, capture_output=True, timeout=10)
                reload_config()
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 配置已热加载 (git snap)")
            except Exception as e:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 配置重载失败: {e}")
        signal.signal(signal.SIGHUP, _hup)
        try:
            uvicorn.run(app, host=_config.get("host", "127.0.0.1"), port=_config.get("port", 8646), log_level="info")
        finally:
            pid_file = os.path.join(BASE, "gateway.pid")
            if os.path.exists(pid_file):
                os.remove(pid_file)

    elif args[0] == "probe":
        watch = "--watch" in args
        if watch:
            print("🔄 持续探测模型池（每 60s）...")
            while True:
                print(f"\n--- {datetime.datetime.now().strftime('%H:%M:%S')} ---")
                probe_all(_config)
                time.sleep(60)
        else:
            print("🔍 一次性探测模型池...")
            probe_all(_config)

    elif args[0] == "models":
        cmd_models(_config)
    elif args[0] == "usage":
        cmd_usage(_config)
    elif args[0] == "quality":
        cmd_quality(_config)
    elif args[0] == "feedback-stats":
        cmd_feedback_stats(_config)
    elif args[0] == "git-log":
        cmd_git_log(_config, BASE)
    elif args[0] == "git-diff":
        cmd_git_diff(_config, BASE)
    elif args[0] == "sync-runtime":
        cmd_sync_runtime(_config, BASE)
    elif args[0] == "undo":
        cmd_undo_remote(_config)
    elif args[0] == "undo-list":
        cmd_undo_list_remote(_config)
    elif args[0] == "fiber":
        cmd_fiber_view(_config)
    elif args[0] == "check-deps":
        target_key = None
        auto_sync = False
        for a in args[1:]:
            if a == "--auto-sync":
                auto_sync = True
            elif not a.startswith("--"):
                target_key = a
        cmd_check_deps(_config, BASE, target_key=target_key, auto_sync=auto_sync)
    elif args[0] == "scan-agents":
        scan_dir = "~/agents"
        if "--dir" in args:
            idx = args.index("--dir")
            if idx + 1 < len(args):
                scan_dir = args[idx + 1]
        cmd_scan_agents(_config, BASE, CONFIG_PATH, scan_dir=scan_dir)
    else:
        print(f"未知命令: {args[0]}")

if __name__ == "__main__":
    main()
