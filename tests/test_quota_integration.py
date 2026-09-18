"""额度感知 — 端到端集成测试（走真实路由链 + sqlite + 假上游）"""
import os
import sys
import threading

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ops_gateway_core import build_app
from provider_router import RouterState, quota
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def reset_prometheus():
    for c in list(REGISTRY._collector_to_names.keys()):
        REGISTRY.unregister(c)


def make_cfg(db_path):
    return {
        "gateway_key": "gw-test",
        "port": 8646,
        "capability_dimensions": ["code", "summary", "analysis", "creative", "reasoning", "general"],
        "capability_threshold": 0.3,
        "quota_guard": {"enabled": True, "cooldown_seconds": 600,
                        "budget_thresholds": {"small": 512, "large": 4096}},
        "providers": {
            "pv-bad": {"api": "https://bad.test/v1", "api_key": "k-bad"},
            "pv-good": {"api": "https://good.test/v1", "api_key": "k-good"},
        },
        "pools": {
            "pool_x": {
                "description": "test",
                "fallback": "pool_y",
                "providers": [
                    {"name": "pv-bad", "weight": 100, "models": ["m1"]},
                ],
            },
            "pool_y": {
                "description": "backup",
                "providers": [
                    {"name": "pv-good", "weight": 1, "models": ["m1"]},
                ],
            },
        },
        "routing_rules": {"trivial": {"action": "direct_return", "message": "hi"}},
        "routing_strategy": {"mode": "formula"},
    }


def make_deps(db_path):
    import sqlite3

    def _get_db():
        from ops_gateway_core.cfg.db import get_db
        return get_db(db_path)

    return {
        "disabled_providers": set(),
        "router_state": RouterState(
            disabled_providers=set(), dynamic_weights={}, quality_factors={},
            user_factors={}, rate_limit_buckets={}, lock=threading.Lock()),
        "fiber_runtime": None,
        "dynamic_weights": {},
        "approval_cache": {},
        "pending_approvals": {},
        "lock": threading.Lock(),
        "serial_locks": {},
        "throttle_windows": {},
        "get_db": _get_db,
        "execute_plugin": lambda *a, **k: (False, "no plugin"),
        "format_string": lambda t, p: t,
        "global_call_lookup": lambda p, h: None,
        "global_call_add": lambda *a, **k: None,
        "undo_register": lambda d, f: None,
        "undo_pop": lambda: (False, "empty"),
        "fiber_create": lambda *a, **k: 1,
        "fiber_register": lambda *a, **k: True,
        "fiber_fail": lambda f: (True, []),
        "fiber_commit": lambda f: True,
        "find_model_config": lambda cfg, m: ("pool_x", cfg["pools"]["pool_x"], cfg["pools"]["pool_x"]["providers"][0], m),
        "select_pool_by_keywords": lambda cfg, t: None,
        "select_provider_by_weight": lambda p, m=None: None,
        "select_provider_with_runner_up": lambda p, m=None, **kw: (p[0] if p else None, None, {}),
        "select_provider_by_strategy": None,
        "check_rate_limit": lambda p, r: True,
        "quality_factors": {},
        "user_factors": {},
        "benchmark_loaded": True,
        "log_matches": lambda e, l, s: True,
        "parse_log_line": lambda l, a, s: {},
    }


class FakeTransport(httpx.AsyncBaseTransport):
    """pv-bad → 402 insufficient balance；pv-good → 200 正常返回。"""

    def __init__(self, calls):
        self.calls = calls

    async def handle_async_request(self, request):
        url = str(request.url)
        self.calls.append(url)
        if "bad.test" in url:
            return httpx.Response(402, json={"error": {"message": "insufficient balance"}})
        return httpx.Response(200, json={
            "choices": [{"message": {"role": "assistant", "content": "from good"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        })


def test_quota_exhausted_triggers_fallback_and_writes_state(tmp_path, monkeypatch):
    db_path = str(tmp_path / "gw.db")
    cfg = make_cfg(db_path)
    deps = make_deps(db_path)
    calls = []

    # 拦截 httpx.AsyncClient 使用假 transport
    real_client = httpx.AsyncClient

    class PatchedClient(real_client):
        def __init__(self, *a, **k):
            k["transport"] = FakeTransport(calls)
            super().__init__(*a, **k)

    monkeypatch.setattr(httpx, "AsyncClient", PatchedClient)

    app = build_app(cfg, deps)
    from fastapi.testclient import TestClient
    client = TestClient(app)
    resp = client.post("/v1/chat/completions",
                       headers={"Authorization": "Bearer gw-test"},
                       json={"model": "m1", "messages": [{"role": "user", "content": "写一段代码"}]})

    assert resp.status_code == 200, resp.text
    # 两个 provider 都被尝试：bad 失败 → fallback 到 good
    assert any("bad.test" in u for u in calls), calls
    assert any("good.test" in u for u in calls), calls

    # 状态表：pv-bad 已标记 exhausted
    from ops_gateway_core.cfg.db import get_db
    conn = get_db(db_path)
    st = quota.get_status(conn, "pv-bad")
    assert st["status"] == "exhausted"
    # 事件留痕
    ev = conn.execute("SELECT COUNT(*) c FROM provider_events WHERE provider='pv-bad'").fetchone()["c"]
    assert ev >= 1
    conn.close()


def test_exhausted_provider_is_skipped_next_request(tmp_path, monkeypatch):
    """前置标记 exhausted 后，后续请求不再选它。"""
    db_path = str(tmp_path / "gw.db")
    cfg = make_cfg(db_path)
    deps = make_deps(db_path)

    # 预置 pv-bad 为 exhausted（冷却未到）
    from ops_gateway_core.cfg.db import get_db
    conn = get_db(db_path)
    quota.set_status(conn, "pv-bad", quota.EXHAUSTED, "pre-set", "manual", cooldown_seconds=3600)
    conn.close()

    calls = []
    real_client = httpx.AsyncClient

    class PatchedClient(real_client):
        def __init__(self, *a, **k):
            k["transport"] = FakeTransport(calls)
            super().__init__(*a, **k)

    monkeypatch.setattr(httpx, "AsyncClient", PatchedClient)

    app = build_app(cfg, deps)
    from fastapi.testclient import TestClient
    client = TestClient(app)
    resp = client.post("/v1/chat/completions",
                       headers={"Authorization": "Bearer gw-test"},
                       json={"model": "m1", "messages": [{"role": "user", "content": "写一段代码"}]})

    assert resp.status_code == 200, resp.text
    # 只应调用 good（bad 被额度过滤剔除）
    assert not any("bad.test" in u for u in calls), calls
    assert any("good.test" in u for u in calls), calls


def test_admin_quota_endpoints(tmp_path):
    db_path = str(tmp_path / "gw.db")
    cfg = make_cfg(db_path)
    deps = make_deps(db_path)
    app = build_app(cfg, deps)
    from fastapi.testclient import TestClient
    client = TestClient(app)

    # 初始总览
    r = client.get("/admin/quota", headers={"Authorization": "Bearer gw-test"})
    assert r.status_code == 200
    assert r.json()["summary"]["enabled"] is True

    # 人工标记
    r = client.post("/admin/quota/pv-bad/set",
                    headers={"Authorization": "Bearer gw-test"},
                    json={"status": "exhausted", "reason": "manual test"})
    assert r.status_code == 200
    assert r.json()["status"] == "exhausted"

    r = client.get("/admin/quota", headers={"Authorization": "Bearer gw-test"})
    names = {p["provider"]: p["status"] for p in r.json()["summary"]["providers"]}
    assert names.get("pv-bad") == "exhausted"

    # 非法状态
    r = client.post("/admin/quota/pv-bad/set",
                    headers={"Authorization": "Bearer gw-test"},
                    json={"status": "bogus"})
    assert r.status_code == 400

    # 不存在的 provider
    r = client.post("/admin/quota/nope/set",
                    headers={"Authorization": "Bearer gw-test"},
                    json={"status": "exhausted"})
    assert r.status_code == 404
