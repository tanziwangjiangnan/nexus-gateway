"""自动优先级路由（routing_strategy.mode=auto）— 单元测试"""
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router import RouterState, select_provider_auto


def _state(disabled=()):
    return RouterState(
        disabled_providers=set(disabled), dynamic_weights={}, quality_factors={},
        user_factors={}, rate_limit_buckets={}, lock=threading.Lock())


def _cfg(tiers):
    return {"routing_strategy": {"mode": "auto", "auto": {"priority_models": tiers}}}


PV_FLASH_A = {"name": "flash-a", "weight": 1, "models": ["deepseek-v4-flash"]}
PV_FLASH_B = {"name": "flash-b", "weight": 1, "models": ["DeepSeek-V4-Flash"]}
PV_PRO = {"name": "pro", "weight": 1, "models": ["deepseek-v4-pro"]}
PV_OTHER = {"name": "other", "weight": 1, "models": ["some-model"]}
ALL = [PV_FLASH_A, PV_FLASH_B, PV_PRO, PV_OTHER]


def test_auto_picks_priority_tier_first():
    """优先档位有可用 → 只在主档里选（不会用到 pro/other）。"""
    picked, _, _ = select_provider_auto(ALL, _state(), _cfg(["deepseek-v4-flash", "deepseek-v4-pro"]))
    assert picked["name"] in ("flash-a", "flash-b")

def test_auto_matches_model_case_insensitive():
    """档位名大小写不敏感，能命中 DeepSeek-V4-Flash。"""
    picked, _, _ = select_provider_auto([PV_FLASH_B], _state(), _cfg(["deepseek-v4-flash"]))
    assert picked["name"] == "flash-b"

def test_auto_falls_to_next_tier_when_primary_unavailable():
    """主档 provider 全禁用 → 降到次档。"""
    picked, _, _ = select_provider_auto(
        ALL, _state(disabled=("flash-a", "flash-b")),
        _cfg(["deepseek-v4-flash", "deepseek-v4-pro"]))
    assert picked["name"] == "pro"

def test_auto_wildcard_fallback():
    """有 '*' 兜底 → 主/次档都没有时用任意可用。"""
    # 档位里没有 flash/pro 的模型名时，落到 *
    picked, _, _ = select_provider_auto(
        [PV_OTHER], _state(), _cfg(["deepseek-v4-flash", "*"]))
    assert picked["name"] == "other"

def test_auto_none_when_all_tiers_empty_and_no_wildcard():
    """档位都匹配不到且无 '*' → None（调用方走原故障转移兜底）。"""
    picked, _, _ = select_provider_auto(
        [PV_OTHER], _state(), _cfg(["deepseek-v4-flash", "deepseek-v4-pro"]))
    assert picked is None

def test_auto_empty_tiers_degrades_to_weight():
    """priority_models 为空 → 退化为全部可用里按权重选（不崩）。"""
    picked, _, _ = select_provider_auto(ALL, _state(), _cfg([]))
    assert picked is not None
    assert picked["name"] in [p["name"] for p in ALL]

def test_auto_all_disabled_returns_none():
    picked, _, _ = select_provider_auto(
        ALL, _state(disabled=tuple(p["name"] for p in ALL)), _cfg(["*"]))
    assert picked is None

def test_auto_same_tier_is_load_balanced():
    """同档两个 provider（并排）→ 多次选择两者都会出现。"""
    picked_names = set()
    for _ in range(60):
        p, _, _ = select_provider_auto(ALL, _state(), _cfg(["deepseek-v4-flash"]))
        if p:
            picked_names.add(p["name"])
    assert picked_names == {"flash-a", "flash-b"}

def test_auto_returns_runner_up():
    """同档有 2 个 → 返回 runner_up（供检查者用）。"""
    _, runner_up, weights = select_provider_auto(ALL, _state(), _cfg(["deepseek-v4-flash"]))
    assert runner_up is not None
    assert runner_up["name"] in ("flash-a", "flash-b")
    assert len(weights) == 2
