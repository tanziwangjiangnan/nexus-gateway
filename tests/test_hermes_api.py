"""测试 hermes_api 包：build_app"""
import pytest, threading
from prometheus_client import REGISTRY
from hermes_api import build_app


# 每个测试前重置 prometheus 注册表，避免重复注册错误
@pytest.fixture(autouse=True)
def reset_prometheus():
    collectors = list(REGISTRY._collector_to_names.keys())
    for c in collectors:
        REGISTRY.unregister(c)


# 构建最小依赖注入字典
def make_deps():
    return {
        "disabled_providers": set(),
        "router_state": None,
        "fiber_runtime": None,
        "dynamic_weights": {},
        "approval_cache": {},
        "pending_approvals": {},
        "lock": threading.Lock(),
        "serial_locks": {},
        "throttle_windows": {},
        "get_db": lambda: None,
        "execute_plugin": lambda *a, **k: (False, "no plugin"),
        "format_string": lambda template, params: template,
        "global_call_lookup": lambda p, h: None,
        "global_call_add": lambda *a, **k: None,
        "undo_register": lambda d, f: None,
        "undo_pop": lambda: (False, "empty"),
        "fiber_create": lambda *a, **k: 1,
        "fiber_register": lambda *a, **k: True,
        "fiber_fail": lambda f: (True, []),
        "fiber_commit": lambda f: True,
        "find_model_config": lambda cfg, m: (None, None, None, None),
        "select_pool_by_keywords": lambda cfg, t: None,
        "select_provider_by_weight": lambda p, m=None: None,
        "check_rate_limit": lambda p, r: True,
        "log_matches": lambda e, l, s: True,
        "parse_log_line": lambda l, a, s: {},
    }


class TestBuildApp:
    def test_build_app_returns_app(self):
        cfg = {"gateway_key": "test", "port": 8646}
        app = build_app(cfg, make_deps())
        assert app is not None

    def test_health_route(self):
        cfg = {"gateway_key": "test", "port": 8646}
        app = build_app(cfg, make_deps())
        paths = [r.path for r in app.routes if hasattr(r, "path")]
        assert "/health" in paths

    def test_admin_routes(self):
        cfg = {"gateway_key": "test", "port": 8646}
        app = build_app(cfg, make_deps())
        paths = [r.path for r in app.routes if hasattr(r, "path")]
        assert "/admin/fiber/tree" in paths
        assert "/admin/undo-list" in paths
        assert "/v1/models" in paths

    def test_minimal_deps(self):
        """build_app 需要全部 deps 键，缺少任一键都报 KeyError"""
        cfg = {"gateway_key": "test"}
        with pytest.raises(KeyError):
            build_app(cfg, {})

    def test_route_count_stable(self):
        cfg = {"gateway_key": "test", "port": 8646}
        app = build_app(cfg, make_deps())
        paths = [r.path for r in app.routes if hasattr(r, "path")]
        assert len(paths) >= 20