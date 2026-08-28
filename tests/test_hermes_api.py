"""测试 hermes_api 包：build_app"""
import pytest, threading
from prometheus_client import REGISTRY
from hermes_api import build_app, _should_score, _score_by_runner_up


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
        "select_provider_with_runner_up": lambda p, m=None: (None, None, {}),
        "check_rate_limit": lambda p, r: True,
        "quality_factors": {},
        "user_factors": {},
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

class TestShouldScore:
    """测试监督者（Supervisor）评分机制的三层触发决策。

    覆盖：冷启动、稳态采样、新鲜度窗口、超时、裁判变化、方差突变、方差爆发期、配置关闭、向后兼容。
    """

    def _cfg(self, **kw):
        qf = {"runner_up_scoring": True, "scoring_warmup": 10, "scoring_max_interval": 3600}
        qf.update(kw)
        return {"quality_feedback": qf}

    def _super_cfg(self, **kw):
        base = {"enabled": True, "cold_start_count": 10, "min_sample_rate": 0.05}
        base.update(kw)
        return {"supervisor": base}

    def _state(self, count=0, last=0, last_runners=None, recent_scores=None, variance_boost=0):
        return {"count": count, "last": last, "last_runners": last_runners or [],
                "recent_scores": recent_scores or [], "variance_boost_remaining": variance_boost}

    # ── 基础守卫 ──
    def test_no_runner_up(self):
        decision, reason = _should_score(self._cfg(), "A", None, {})
        assert decision is False, reason
        assert reason == "no_runner_up"

    def test_disabled_by_config(self):
        cfg = {"quality_feedback": {"runner_up_scoring": False}}
        decision, reason = _should_score(cfg, "A", {"name": "B"}, {})
        assert decision is False, reason
        assert reason == "disabled_by_config"

    def test_disabled_by_supervisor(self):
        cfg = {"supervisor": {"enabled": False}}
        decision, reason = _should_score(cfg, "A", {"name": "B"}, {})
        assert decision is False, reason
        assert reason == "disabled_by_config"

    # ── 冷启动 ──
    def test_cold_start_always_scores(self):
        decision, reason = _should_score(self._cfg(), "A", {"name": "B"}, {})
        assert decision is True, reason
        assert reason == "cold_start"

    def test_warmup_below_threshold(self):
        st = self._state(count=5, last=1000, last_runners=["B"])
        decision, reason = _should_score(self._cfg(), "A", {"name": "B"}, {"A": st}, now=2000)
        assert decision is True, reason
        assert reason == "warmup"

    # ── 新鲜度窗口 ──
    def test_freshness_skips_when_recent(self):
        st = self._state(count=50, last=1000, last_runners=["B"])
        decision, reason = _should_score(self._cfg(), "A", {"name": "B"}, {"A": st}, now=1100)
        assert decision is False, reason
        assert "freshness" in reason

    # ── 超时强制复审 ──
    def test_stale_triggers_review(self):
        st = self._state(count=50, last=1000, last_runners=["B"])
        decision, reason = _should_score(self._cfg(), "A", {"name": "B"}, {"A": st}, now=2000 + 4000)
        assert decision is True, reason
        assert reason == "stale"

    # ── 裁判变化 ──
    def test_new_judge_triggers_review(self):
        st = self._state(count=100, last=1000, last_runners=["B"])
        decision, reason = _should_score(self._cfg(), "A", {"name": "C"}, {"A": st}, now=2000)
        assert decision is True, reason
        assert reason == "new_judge"

    def test_known_judge_does_not_trigger(self):
        st = self._state(count=100, last=1000, last_runners=["B", "C"])
        decision, reason = _should_score(self._cfg(), "A", {"name": "C"}, {"A": st}, now=2000)
        # 新鲜度窗口 + 裁判已知 → 跳过
        assert "freshness" in reason or "skip" in reason

    # ── 方差突变 ──
    def test_variance_spike_triggers(self):
        st = self._state(count=100, last=2000, last_runners=["B"],
                         recent_scores=[90, 50, 85])  # 标准差 ≈ 21.8 > 15
        decision, reason = _should_score(self._cfg(), "A", {"name": "B"}, {"A": st}, now=3000)
        assert decision is True, reason
        assert reason == "variance_spike"

    def test_variance_boost_high_rate(self):
        st = self._state(count=100, last=2000, last_runners=["B"],
                         recent_scores=[90, 50, 85], variance_boost=3)
        # 在爆发期，即便裁判没变、超时未到，仍有 50% 概率触发
        results = [
            _should_score(self._cfg(), "A", {"name": "B"}, {"A": st}, now=3000)
            for _ in range(200)
        ]
        triggered = [r for r in results if r[0] is True and r[1] == "variance_boost"]
        assert len(triggered) > 20  # 50% 概率，200 次至少 20 次

    # ── 稳态概率 ──
    def test_steady_state_probabilistic(self):
        st = self._state(count=100, last=2000, last_runners=["B"])
        results = [
            _should_score(self._cfg(), "A", {"name": "B"}, {"A": st}, now=3000)
            for _ in range(200)
        ]
        assert any(r[0] for r in results), "should trigger sometimes"
        assert not all(r[0] for r in results), "should skip sometimes"

    def test_steady_state_high_count_still_scores_min_rate(self):
        st = self._state(count=100000, last=2000, last_runners=["B"])
        results = [
            _should_score(self._cfg(), "A", {"name": "B"}, {"A": st}, now=3000)
            for _ in range(1000)
        ]
        assert any(r[0] for r in results), "min rate should never be zero"

    # ── 向后兼容：supervisor 段优先 ──
    def test_supervisor_section_priority(self):
        cfg = {
            "quality_feedback": {"runner_up_scoring": False, "scoring_warmup": 5},
            "supervisor": {"enabled": True, "cold_start_count": 3},
        }
        st = self._state(count=2, last=1000, last_runners=["B"])
        decision, reason = _should_score(cfg, "A", {"name": "B"}, {"A": st}, now=2000)
        assert decision is True, reason
        # cold_start_count=3 > count=2 → warmup
        assert reason == "warmup"

    def test_supervisor_disables_quality_feedback(self):
        cfg = {
            "quality_feedback": {"runner_up_scoring": True},
            "supervisor": {"enabled": False},
        }
        decision, reason = _should_score(cfg, "A", {"name": "B"}, {})
        assert decision is False, reason
        assert reason == "disabled_by_config"


def test_should_score_importable():
    assert callable(_should_score)
    assert callable(_score_by_runner_up)
