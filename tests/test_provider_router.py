"""测试 provider_router 包：Router + RouterState + CircuitBreakerMonitor"""
import pytest, time, threading
from provider_router import Router, RouterState, CircuitBreakerMonitor

SAMPLE_CFG = {
    "pools": {
        "pool_a": {
            "description": "主池",
            "fallback": "pool_b",
            "providers": [
                {"name": "scnet-tp", "weight": 5, "models": ["DeepSeek-V4-Flash", "GLM-5.2"]},
                {"name": "xiaomi", "weight": 2, "models": ["mimo-v2.5-pro"]},
            ],
        },
        "pool_b": {
            "description": "备用池",
            "providers": [
                {"name": "qfg-codex", "weight": 3, "models": ["codex-v1"]},
            ],
        },
    },
    "providers": {
        "scnet-tp": {"api": "https://scnet.test", "api_key": "sk-test", "max_rps": 100},
        "xiaomi": {"api": "https://xiaomi.test", "api_key": "sk-test", "max_rps": 100},
        "qfg-codex": {"api": "https://qfg.test", "api_key": "sk-test", "max_rps": 100},
    },
}


class TestRouterFindModel:
    def test_find_exact(self):
        pool, pcfg, pv, canonical = Router.find_model(SAMPLE_CFG, "DeepSeek-V4-Flash")
        assert pool == "pool_a"
        assert pv["name"] == "scnet-tp"
        assert canonical == "DeepSeek-V4-Flash"

    def test_find_case_insensitive(self):
        pool, pcfg, pv, canonical = Router.find_model(SAMPLE_CFG, "deepseek-v4-flash")
        assert pool == "pool_a"
        assert canonical == "DeepSeek-V4-Flash"

    def test_find_not_found(self):
        result = Router.find_model(SAMPLE_CFG, "non-existent-model")
        assert result == (None, None, None, None)

    def test_find_from_pool_b(self):
        pool, pcfg, pv, canonical = Router.find_model(SAMPLE_CFG, "codex-v1")
        assert pool == "pool_b"
        assert pv["name"] == "qfg-codex"


class TestRouterSelectPoolByKeywords:
    def test_no_keywords_no_match(self):
        assert Router.select_pool_by_keywords(SAMPLE_CFG, "你好") is None

    def test_pool_with_keywords(self):
        cfg = dict(SAMPLE_CFG)
        cfg["routing"] = {"rules": [{"keywords": ["hello"], "pool": "pool_a"}]}
        assert Router.select_pool_by_keywords(cfg, "hello world") == "pool_a"

    def test_keyword_field_not_present(self):
        assert Router.select_pool_by_keywords(SAMPLE_CFG, "anything") is None


class TestRouterSelectProvider:
    def test_select_by_weight(self):
        providers = SAMPLE_CFG["pools"]["pool_a"]["providers"]
        state = RouterState()
        selected = []
        for _ in range(50):
            pv = Router.select_provider(providers, state)
            assert pv is not None
            selected.append(pv["name"])
        assert len(set(selected)) >= 1

    def test_select_with_model_filter(self):
        providers = SAMPLE_CFG["pools"]["pool_a"]["providers"]
        state = RouterState()
        pv = Router.select_provider(providers, state, model="DeepSeek-V4-Flash")
        assert pv is not None
        assert pv["name"] == "scnet-tp"

    def test_select_with_model_no_match(self):
        providers = SAMPLE_CFG["pools"]["pool_a"]["providers"]
        state = RouterState()
        pv = Router.select_provider(providers, state, model="non-existent")
        assert pv is None

    def test_select_disabled_provider_skipped(self):
        providers = SAMPLE_CFG["pools"]["pool_a"]["providers"]
        state = RouterState()
        state.disabled_providers.add("scnet-tp")
        pv = Router.select_provider(providers, state)
        assert pv is not None
        assert pv["name"] != "scnet-tp"


class TestRouterCheckRateLimit:
    def test_rate_limit_pass(self):
        state = RouterState()
        assert Router.check_rate_limit("test-provider", 1000, state)

    def test_rate_limit_blocked(self):
        state = RouterState()
        now = time.time()
        state.rate_limit_buckets["test-provider"] = [(now - 0.01)] * 1000
        assert not Router.check_rate_limit("test-provider", 10, state)

    def test_rate_limit_after_expiry(self):
        state = RouterState()
        now = time.time()
        state.rate_limit_buckets["test-provider"] = [(now - 2)] * 100
        assert Router.check_rate_limit("test-provider", 10, state)


class TestRouterResolveEnvKey:
    def test_resolve_plain(self):
        assert Router.resolve_env_key("sk-static") == "sk-static"

    def test_resolve_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_KEY", "sk-from-env")
        assert Router.resolve_env_key("${MY_KEY}") == "sk-from-env"

    def test_resolve_env_empty(self, monkeypatch):
        monkeypatch.delenv("NO_SUCH_KEY", raising=False)
        assert Router.resolve_env_key("${NO_SUCH_KEY}") == ""


class TestCircuitBreakerMonitor:
    def test_create(self):
        """CircuitBreakerMonitor 需要 6 个位置参数"""
        monitor = CircuitBreakerMonitor(
            get_db=lambda: None,
            cfg_getter=lambda: {},
            disabled_providers=set(),
            dynamic_weights={},
            quality_factors={},
            user_factors={},
        )
        assert monitor is not None
        assert monitor.interval == 30.0

    def test_default_quality_factor(self):
        assert CircuitBreakerMonitor._default_quality_factor([50]) == 0.75
        assert CircuitBreakerMonitor._default_quality_factor([]) == 1.0
        assert CircuitBreakerMonitor._default_quality_factor([100]) == 1.0

    def test_default_user_factor(self):
        uf = CircuitBreakerMonitor._default_user_factor([1, 1])
        assert 1.0 < uf < 1.5
        assert CircuitBreakerMonitor._default_user_factor([]) == 1.0