"""降级链纯逻辑（provider_router/fallback.py）— 单元测试"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router.fallback import plan_pool_fallback, pool_model

CFG = {
    "pools": {
        "pool_a": {"fallback": "pool_b",
                   "providers": [{"name": "p1", "models": ["m-a"]}]},
        "pool_b": {"fallback": "pool_c",
                   "providers": [{"name": "p1", "models": ["m-b1", "m-b2"]},
                                 {"name": "p2", "models": ["m-b3"]}]},
        "pool_c": {"providers": [{"name": "p3", "models": ["m-c"]}]},
        "pool_x": {"providers": []},
    }
}


def test_pool_model_prefers_same_name():
    assert pool_model(CFG, "pool_b", "m-b2") == "m-b2"


def test_pool_model_falls_back_to_first_available():
    # 请求的 model 不在该池 -> 用池内第一个可用模型（这正是修复点）
    assert pool_model(CFG, "pool_b", "m-a") == "m-b1"


def test_pool_model_skips_disabled_provider():
    assert pool_model(CFG, "pool_b", "m-a", disabled=("p1",)) == "m-b3"


def test_pool_model_none_when_no_usable_provider():
    assert pool_model(CFG, "pool_b", "m-a", disabled=("p1", "p2")) is None


def test_pool_model_missing_pool():
    assert pool_model(CFG, "not-exist", "m") is None


def test_plan_follows_chain():
    plan = plan_pool_fallback(CFG, "pool_a", "m-a", set())
    assert plan is not None
    assert plan.pool == "pool_b"
    assert plan.model == "m-b1"


def test_plan_stops_at_chain_end():
    assert plan_pool_fallback(CFG, "pool_c", "m-c", set()) is None


def test_plan_stops_when_target_already_tried():
    assert plan_pool_fallback(CFG, "pool_a", "m-a", {"pool_b"}) is None


def test_plan_keeps_model_when_target_has_no_provider():
    # pool_b 没有可用 provider 时：仍给计划（模型不变），由下一轮继续沿链走
    plan = plan_pool_fallback(CFG, "pool_b", "m-b1", set(), disabled=("p1", "p2"))
    assert plan is not None
    assert plan.pool == "pool_c"


def test_plan_no_fallback_key():
    assert plan_pool_fallback(CFG, "pool_x", "m", set()) is None
