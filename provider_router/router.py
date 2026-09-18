"""Provider Router — 核心路由逻辑

所有可变状态通过 RouterState 注入，由调用方维护。
纯 Python 标准库，不依赖 FastAPI。
"""
import dataclasses
import json
import os
import random
import threading
import time
from typing import Any, Callable, Optional


def _model_ok(provider_entry, model_lower, state):
    """该 provider 中是否有匹配 model_lower 且未被模型级熔断的模型。"""
    name = provider_entry.get("name")
    for m in provider_entry.get("models", []):
        if m.lower() == model_lower and ("%s::%s" % (name, m)) not in state.disabled_providers:
            return True
    return False


def _has_any_model(provider_entry, state):
    """该 provider 是否至少还有一个未被模型级熔断的模型（无 models 字段则视为可用）。"""
    name = provider_entry.get("name")
    models = provider_entry.get("models", [])
    if not models:
        return True
    return any(("%s::%s" % (name, m)) not in state.disabled_providers for m in models)


@dataclasses.dataclass
class RouterState:
    """路由引擎的可变状态容器，由调用方创建并维护生命周期。"""
    disabled_providers: set = dataclasses.field(default_factory=set)
    dynamic_weights: dict = dataclasses.field(default_factory=dict)
    quality_factors: dict = dataclasses.field(default_factory=dict)
    user_factors: dict = dataclasses.field(default_factory=dict)
    rate_limit_buckets: dict = dataclasses.field(default_factory=dict)
    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)


class Router:
    """三池路由引擎 — 纯函数式路由逻辑。"""

    @staticmethod
    def find_model(cfg: dict, model: str):
        """遍历所有池查找 model 所属的 (pool_name, pool_config, provider_config, canonical_model_name)；
        大小写不敏感。
        """
        model_lower = model.lower()
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            for pv in pool_cfg.get("providers", []):
                for m in pv.get("models", []):
                    if model_lower == m.lower():
                        return pool_name, pool_cfg, pv, m
        return None, None, None, None

    @staticmethod
    def select_pool_by_keywords(cfg: dict, messages_text: str):
        """关键词匹配 → 返回 pool_name 或 None"""
        for rule in cfg.get("routing", {}).get("rules", []):
            for kw in rule.get("keywords", []):
                if kw in messages_text:
                    return rule["pool"]
        return None

    @staticmethod
    def select_provider(providers: list, state: RouterState, model: str = None,
                        weight_fn: Callable = None, query_caps: dict = None,
                        capability_threshold: float = None):
        """按权重随机选一个 provider，跳过禁用的；
        若指定 model 则只选有该模型的（大小写不敏感）。
        weight_fn 可选，签名 (provider_cfg, state) → float，用于自定义权重计算。
        默认权重 = 动态权重 × 质量因子 × 用户因子，保底 0.1。
        query_caps — 能力需求向量，不为 None 时启用能力标签过滤+匹配因子。
        capability_threshold — 能力过滤阈值，默认 0.3。
        返回 provider 配置 dict 或 None。
        """
        picked, _ = Router._select_weighted(providers, state, model, weight_fn,
                                            query_caps, capability_threshold)
        return picked

    @staticmethod
    def select_provider_with_runner_up(providers: list, state: RouterState,
                                        model: str = None, weight_fn: Callable = None,
                                        query_caps: dict = None,
                                        capability_threshold: float = None):
        """按权重选一个 provider，同时返回第二名（候选池中权重次高者）。

        第二名作为"检查者"：主请求完成后，第二名会收到主请求的问题+回答，
        并打一个质量分（0-100）。返回 (selected, runner_up, all_weights)。

        若候选不足 2 个，runner_up 为 None。
        query_caps — 能力需求向量，不为 None 时启用能力标签过滤+匹配因子。
        """
        candidates = [p for p in providers
                      if p["name"] not in state.disabled_providers and _has_any_model(p, state)]
        if model:
            model_lower = model.lower()
            candidates = [p for p in candidates if _model_ok(p, model_lower, state)]
        # 能力标签过滤：query_caps 不为 None 时执行
        if query_caps is not None:
            from provider_router.assessor import filter_providers_by_capability
            if capability_threshold is None:
                capability_threshold = 0.3
            filtered = filter_providers_by_capability(candidates, query_caps, capability_threshold)
            cap_scores = {p["name"]: s for p, s in filtered}
            candidates = [p for p, _ in filtered]
        else:
            cap_scores = {}
        if not candidates:
            return None, None, {}
        # 计算所有候选的权重
        weights = []
        for p in candidates:
            if weight_fn:
                w = weight_fn(p, state)
            else:
                w = state.dynamic_weights.get(p["name"]) or p.get("weight", 1)
                qf = state.quality_factors.get(p["name"], 1.0)
                uf = state.user_factors.get(p["name"], 1.0)
                cf = cap_scores.get(p["name"], 1.0)  # 能力匹配因子
                w = w * qf * uf * cf
                w = max(w, 0.1)
            weights.append(w)
        # 轮盘赌选第一名
        total = sum(weights)
        r = random.uniform(0, total)
        upto = 0
        picked_idx = 0
        for i, p in enumerate(candidates):
            upto += weights[i]
            if r <= upto:
                picked_idx = i
                break
        picked = candidates[picked_idx]
        # 从剩余中再按权重选第二名
        remaining = [candidates[i] for i in range(len(candidates)) if i != picked_idx]
        remaining_w = [weights[i] for i in range(len(weights)) if i != picked_idx]
        runner_up = None
        if remaining:
            r2 = random.uniform(0, sum(remaining_w))
            upto2 = 0
            for i, p in enumerate(remaining):
                upto2 += remaining_w[i]
                if r2 <= upto2:
                    runner_up = p
                    break
        all_weights = {p["name"]: w for p, w in zip(candidates, weights)}
        return picked, runner_up, all_weights

    @staticmethod
    def _select_weighted(providers, state, model=None, weight_fn=None,
                         query_caps=None, capability_threshold=None):
        """内部：轮盘赌选一个，返回 (provider, weights_list)。

        新增 query_caps / capability_threshold — 能力标签过滤，参见 select_provider_with_runner_up。
        """
        candidates = [p for p in providers
                      if p["name"] not in state.disabled_providers and _has_any_model(p, state)]
        if model:
            model_lower = model.lower()
            candidates = [p for p in candidates if _model_ok(p, model_lower, state)]
        # 能力标签过滤
        if query_caps is not None:
            from provider_router.assessor import filter_providers_by_capability
            if capability_threshold is None:
                capability_threshold = 0.3
            filtered = filter_providers_by_capability(candidates, query_caps, capability_threshold)
            cap_scores = {p["name"]: s for p, s in filtered}
            candidates = [p for p, _ in filtered]
        else:
            cap_scores = {}
        if not candidates:
            return None, []
        weights = []
        for p in candidates:
            if weight_fn:
                w = weight_fn(p, state)
            else:
                w = state.dynamic_weights.get(p["name"]) or p.get("weight", 1)
                qf = state.quality_factors.get(p["name"], 1.0)
                uf = state.user_factors.get(p["name"], 1.0)
                cf = cap_scores.get(p["name"], 1.0)
                w = w * qf * uf * cf
                w = max(w, 0.1)
            weights.append(w)
        total = sum(weights)
        r = random.uniform(0, total)
        upto = 0
        for i, p in enumerate(candidates):
            upto += weights[i]
            if r <= upto:
                return p, weights
        return candidates[-1], weights

    @staticmethod
    def check_rate_limit(provider_name: str, max_rps: int, state: RouterState):
        """滑动窗口限流，返回 True=通过 False=限流"""
        if not max_rps or max_rps <= 0:
            return True
        now = time.time()
        with state.lock:
            bucket = state.rate_limit_buckets.setdefault(provider_name, [])
            cutoff = now - 1.0
            bucket[:] = [t for t in bucket if t > cutoff]
            if len(bucket) >= max_rps:
                return False
            bucket.append(now)
        return True

    @staticmethod
    def resolve_env_key(api_key: str) -> str:
        """解析 ${VAR} 环境变量引用，返回实际值。"""
        if isinstance(api_key, str) and api_key.startswith("${") and api_key.endswith("}"):
            return os.environ.get(api_key[2:-1], "")
        return api_key

    @staticmethod
    def get_provider_from_last_usage(get_db: Callable):
        """返回最近一次调用使用的 provider 名（用于检查者评分关联）。"""
        conn = get_db()
        row = conn.execute("SELECT provider FROM usage ORDER BY id DESC LIMIT 1").fetchone()
        conn.close()
        return row["provider"] if row else None


def call_model_router(query: str, candidates: list, config: dict,
                      session_id: str = None) -> Optional[str]:
    """同步调用外部路由模型服务，返回选中的 provider 名称。

    参数:
        query: 用户发来的原始请求内容
        candidates: 候选 provider 名称列表
        config: model_router 配置段（endpoint/timeout_ms/fallback 等）
        session_id: 可选，会话级上下文

    返回:
        provider 名称；若服务不可用、超时或返回无效结果，返回 None 表示需降级。
    """
    from .cache import get_cache

    endpoint = (config or {}).get("endpoint") or ""
    timeout_ms = int((config or {}).get("timeout_ms", 500))
    cache_enabled = bool((config or {}).get("cache_enabled", True))
    ttl_seconds = int((config or {}).get("cache_ttl_seconds", 300))

    if not endpoint:
        return None

    # 缓存命中：相同 query 在 TTL 内复用路由结果
    if cache_enabled:
        cached = get_cache().get(query)
        if cached is not None:
            return cached

    payload = {
        "query": query,
        "candidates": candidates,
        "context": {"session_id": session_id or "unknown", "task_type": "general"},
    }

    try:
        # 延迟导入 urllib，避免引入 httpx 依赖（provider_router 保持零外部依赖）
        import urllib.request
        import urllib.error

        req = urllib.request.Request(
            endpoint,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        resp = urllib.request.urlopen(req, timeout=timeout_ms / 1000.0)
        result = json.loads(resp.read().decode())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None
    except Exception:
        return None

    selected = result.get("selected") if isinstance(result, dict) else None
    if selected in candidates:
        if cache_enabled:
            get_cache().set(query, selected)
        return selected
    return None


def select_provider_auto(providers: list, state: RouterState, cfg: dict):
    """自动优先级路由（routing_strategy.mode=auto）。

    按 routing_strategy.auto.priority_models 从高到低找第一个「有可用 provider」的档位；
    同档多个 provider 按权重并排选择。不调用任何模型，纯本地规则。

    priority_models 项为模型名（大小写不敏感，跨池匹配），或 "*" 表示任意可用。

    返回 (provider, runner_up, all_weights)，全空时 (None, None, {})。
    """
    auto_cfg = (cfg.get("routing_strategy", {}) or {}).get("auto", {}) or {}
    tiers = auto_cfg.get("priority_models") or []

    # 可用性初筛：排除已禁用（熔断）
    available = [p for p in providers
                 if p["name"] not in state.disabled_providers and _has_any_model(p, state)]
    if not available:
        return None, None, {}

    for tier in tiers:
        if tier == "*":
            # 只剩至少一个未被模型级熔断的模型，才算候选
            candidates = [p for p in available if _has_any_model(p, state)]
        else:
            t_lower = str(tier).lower()
            candidates = [p for p in available if _model_ok(p, t_lower, state)]
        if candidates:
            picked, runner_up, weights = Router.select_provider_with_runner_up(candidates, state)
            if picked:
                return picked, runner_up, weights

    # priority_models 为空时，退化为在全部可用里按权重选
    if not tiers:
        return Router.select_provider_with_runner_up(available, state)
    return None, None, {}


def select_provider_by_strategy(providers: list, state: RouterState, cfg: dict,
                                model: str = None, query: str = None,
                                session_id: str = None,
                                query_caps: dict = None,
                                capability_threshold: float = None) -> Optional[dict]:
    """按路由策略选择 provider（v2.8 模型路由）。

    模式:
        formula — 现有确定性权重逻辑（静态权重 × 动态因子）
        model   — 调用外部路由模型服务；失败时按 fallback 配置降级
        hybrid  — 优先模型路由，失败时回退 formula

    返回:
        provider 配置 dict；model 模式下且 fallback=error 时返回 None。
    query_caps — 能力需求向量，不为 None 时 formula/hybrid 降级启用能力过滤。
    """
    routing_cfg = cfg.get("routing_strategy", {}) or {}
    mode = routing_cfg.get("mode", "formula")
    model_router_cfg = routing_cfg.get("model_router", {}) or {}

    # 模型路由模式：model / hybrid 且提供了 query
    if mode in ("model", "hybrid") and query:
        candidates = [p["name"] for p in providers
                      if p["name"] not in state.disabled_providers]
        if model:
            model_lower = model.lower()
            candidates = [p["name"] for p in providers
                          if p["name"] not in state.disabled_providers
                          and model_lower in [m.lower() for m in p.get("models", [])]]
        if not candidates:
            return None
        selected = call_model_router(query, candidates, model_router_cfg, session_id)
        if selected:
            for p in providers:
                if p["name"] == selected and p["name"] not in state.disabled_providers:
                    return p
            return None

        if mode == "model":
            if model_router_cfg.get("fallback", "formula") == "error":
                return None
            # fallback == "formula" → 落到下方公式逻辑

    # formula 模式或 hybrid 降级：确定性权重
    return Router.select_provider(providers, state, model=model,
                                  query_caps=query_caps,
                                  capability_threshold=capability_threshold)


