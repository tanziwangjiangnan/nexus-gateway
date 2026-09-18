"""降级链决策（纯逻辑，零 IO）

职责：给定配置与当前请求状态，决定「失败之后去哪个池、用哪个模型」。
被谁调用：ops_gateway_core/api/app.py 的故障转移链（_advance_failure 只负责应用计划）。

关键不变量：
  - 纯函数：不读环境变量、不连数据库、不发网络请求 —— 只看传入的 cfg 与状态，因此可单测。
  - 只沿 pool.fallback 往下走：不回退、不循环（已试过的池返回 None）。
  - 目标池里没有同名模型时，返回该池第一个可用 provider 的首个模型。

版本：v3.12（2026-09-18）从 app.py 抽出 —— 起因是「pool_a 掉线后降级被模型过滤掐断」，
修的时候发现这类决策逻辑不该埋在 2200 行的请求处理函数里（见 docs/模块约定.md）。
"""
from typing import Iterable, NamedTuple, Optional


class FallbackPlan(NamedTuple):
    """降级目标：去哪个池、用哪个模型。"""

    pool: str
    model: str


def pool_model(cfg: dict, pool_name: str, want_model: str,
               disabled: Iterable[str] = ()) -> Optional[str]:
    """给目标池挑一个模型名：优先同名，否则该池第一个可用 provider 的首个模型。

    Args:
        cfg: 网关配置（只读，取 cfg["pools"][pool_name]["providers"]）。
        pool_name: 目标池名，如 "pool_b"。
        want_model: 客户端请求 / 上游传来的模型名。
        disabled: 需要跳过的 provider 名集合（熔断或人工禁用）。

    Returns:
        可用模型名；该池没有任何可用 provider 时返回 None。

    为什么需要它：请求的 model 若不在目标池，路由侧的模型过滤会把整池排除，
    降级看着接了、实际永远走不到（实测：pool_a 掉线时 model=DeepSeek-V4-Flash
    一路走到 pool_c 仍 503）。
    """
    disabled = set(disabled or ())
    pools = (cfg or {}).get("pools", {}) or {}
    pool = pools.get(pool_name) or {}
    providers = pool.get("providers", []) or []

    for pv in providers:
        if pv.get("name") in disabled:
            continue
        for m in (pv.get("models") or []):
            if str(m).lower() == str(want_model).lower():
                return m

    for pv in providers:
        if pv.get("name") in disabled:
            continue
        if pv.get("models"):
            return pv["models"][0]
    return None


def plan_pool_fallback(cfg: dict, current_pool: str, current_model: str,
                       tried_pools: Iterable[str] = (),
                       disabled: Iterable[str] = ()) -> Optional[FallbackPlan]:
    """按 pool.fallback 链给出下一步；链尾 / 目标池已试过 / 目标池不存在时返回 None。

    注意：目标池即使没有可用 provider 也会返回计划（模型保持原样），
    让调用方进入下一轮、再由下一轮继续沿链往下 —— 与改造前的行为一致。
    """
    pools = (cfg or {}).get("pools", {}) or {}
    cur = pools.get(current_pool) or {}
    target = cur.get("fallback")
    if not target:
        return None
    tried = set(tried_pools or ())
    if target in tried or target not in pools:
        return None
    model = pool_model(cfg, target, current_model, disabled) or current_model
    return FallbackPlan(pool=target, model=model)
