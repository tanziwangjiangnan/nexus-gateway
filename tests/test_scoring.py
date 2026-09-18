"""监督者评分触发判定（provider_router/scoring.py）— 单元测试"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router import scoring


def _cfg(**sup):
    return {"supervisor": sup}


def test_no_runner_up_never_scores():
    assert scoring.should_score({}, "A", None, {}) == (False, "no_runner_up")


def test_cold_start_scores():
    d, r = scoring.should_score({}, "A", {"name": "B"}, {})
    assert d is True and r == "cold_start"


def test_disabled_by_config():
    d, r = scoring.should_score(_cfg(enabled=False), "A", {"name": "B"}, {})
    assert d is False and r == "disabled_by_config"


def test_supervisor_cfg_reads_new_section_first():
    cfg = {"supervisor": {"cold_start_count": 3}, "quality_feedback": {"scoring_warmup": 99}}
    assert scoring.supervisor_cfg(cfg, "cold_start_count", 10) == 3


def test_supervisor_cfg_legacy_fallback():
    cfg = {"quality_feedback": {"runner_up_scoring": False}}
    assert scoring.supervisor_cfg(cfg, "enabled", True) is False


def test_read_force_on_parses_list_and_default():
    cfg = {"supervisor": {"force_on": [{"timeout": 1}, "new_judge"]}}
    assert scoring.read_force_on(cfg) == {"timeout", "new_judge"}
    assert scoring.read_force_on({}) == {"timeout", "runner_changed"}


def test_stale_forces_rescore(monkeypatch):
    st = {"A": {"count": 50, "last": 1000}}
    d, r = scoring.should_score(_cfg(**{"force_on.timeout": 10}), "A", {"name": "B"}, st, now=5000)
    assert d is True and r == "stale"


def test_freshness_window_skips():
    st = {"A": {"count": 50, "last": 1000, "last_runners": ["B"]}}
    d, r = scoring.should_score(_cfg(), "A", {"name": "B"}, st, now=1100)
    assert d is False and r == "freshness_skip"


def test_steady_state_sampling_respects_min_rate(monkeypatch):
    st = {"A": {"count": 10000, "last": 0, "last_runners": []}}
    monkeypatch.setattr(scoring.random, "random", lambda: 0.999)
    d, r = scoring.should_score(_cfg(min_sample_rate=0.05), "A", {"name": "B"}, st, now=10 ** 9)
    assert d is False and r.startswith("skip_p=")
    monkeypatch.setattr(scoring.random, "random", lambda: 0.0001)
    d2, r2 = scoring.should_score(_cfg(min_sample_rate=0.05), "A", {"name": "B"}, st, now=10 ** 9)
    assert d2 is True and r2.startswith("sample_p=")


def test_variance_spike_forces_rescore():
    st = {"A": {"count": 100, "last": 0, "recent_scores": [10, 90, 20]}}
    d, r = scoring.should_score(_cfg(), "A", {"name": "B"}, st, now=10 ** 9)
    assert d is True and r == "variance_spike"
