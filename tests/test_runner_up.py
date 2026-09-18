"""检查者（runner_up）挑选（provider_router/router.select_runner_up）— 单元测试"""
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router import RouterState
from provider_router.router import select_runner_up

PROVS = [
    {"name": "deepseek-direct", "weight": 3, "models": ["deepseek-v4-pro"]},
    {"name": "qfg-new", "weight": 2, "models": ["grok-4.3"]},
    {"name": "xiaomi", "weight": 1, "models": ["mimo-v2.5"]},
]


def _state(disabled=()):
    return RouterState(disabled_providers=set(disabled), dynamic_weights={}, quality_factors={},
                       user_factors={}, rate_limit_buckets={}, lock=threading.Lock())


def test_runner_up_allows_different_model():
    """主 provider 是 deepseek-v4-pro，检查者可以是另一个模型的 provider。"""
    ru = select_runner_up(PROVS, _state(), "deepseek-direct")
    assert ru is not None
    assert ru["name"] != "deepseek-direct"
    assert ru["models"][0] != "deepseek-v4-pro"


def test_runner_up_none_when_only_one_provider():
    ru = select_runner_up(PROVS[:1], _state(), "deepseek-direct")
    assert ru is None


def test_runner_up_skips_disabled():
    ru = select_runner_up(PROVS, _state(["qfg-new"]), "deepseek-direct")
    assert ru["name"] == "xiaomi"


def test_runner_up_handles_empty_input():
    assert select_runner_up([], _state(), "x") is None
