"""额度感知与任务保护 — 单元测试（计划书-模型额度感知与任务保护-v1）"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router import quota
from ops_gateway_core.cfg.db import get_db


# ── 分类：额度耗尽 vs 普通限流 vs 服务故障 ──

def test_classify_402_is_exhausted():
    c = quota.classify_error(402, '{"error": "Payment Required"}')
    assert c["kind"] == "exhausted"

def test_classify_429_with_quota_keyword_is_exhausted():
    c = quota.classify_error(429, '{"error": {"message": "You exceeded your current quota"}}')
    assert c["kind"] == "exhausted"

def test_classify_insufficient_balance_keyword():
    c = quota.classify_error(400, 'insufficient balance, please recharge')
    assert c["kind"] == "exhausted"

def test_classify_chinese_keyword():
    c = quota.classify_error(400, '账户余额不足')
    assert c["kind"] == "exhausted"

def test_classify_429_rate_limit_is_not_exhausted():
    """普通 429 限流与额度耗尽要分开。"""
    c = quota.classify_error(429, "rate limit exceeded, too many requests")
    assert c["kind"] == "rate_limit"

def test_classify_500_is_unavailable():
    c = quota.classify_error(500, "internal server error")
    assert c["kind"] == "unavailable"

def test_classify_401_is_unavailable():
    c = quota.classify_error(401, "unauthorized")
    assert c["kind"] == "unavailable"

def test_classify_unknown_400_is_other():
    c = quota.classify_error(400, "bad request")
    assert c["kind"] == "other"

def test_classify_custom_keywords_from_cfg():
    cfg = {"quota_guard": {"exhausted_keywords": ["无额度了"]}}
    c = quota.classify_error(400, "抱歉，无额度了", cfg)
    assert c["kind"] == "exhausted"

def test_classify_402_wins_even_without_keyword():
    c = quota.classify_error(402, "")
    assert c["kind"] == "exhausted"


# ── 预算等级 ──

def test_budget_small():
    assert quota.budget_level(100) == "small"

def test_budget_normal():
    assert quota.budget_level(2000) == "normal"

def test_budget_large():
    assert quota.budget_level(8000) == "large"

def test_budget_none_is_normal():
    assert quota.budget_level(None) == "normal"

def test_budget_custom_thresholds():
    cfg = {"quota_guard": {"budget_thresholds": {"small": 10, "large": 20}}}
    assert quota.budget_level(5, cfg) == "small"
    assert quota.budget_level(15, cfg) == "normal"
    assert quota.budget_level(50, cfg) == "large"


# ── 状态读写 ──

def _fresh_db(tmp_path):
    return get_db(str(tmp_path / "q.db"))

def test_get_status_unknown_when_missing(tmp_path):
    conn = _fresh_db(tmp_path)
    s = quota.get_status(conn, "pv-a")
    assert s["status"] == "unknown"
    conn.close()

def test_set_and_get_status(tmp_path):
    conn = _fresh_db(tmp_path)
    quota.set_status(conn, "pv-a", quota.EXHAUSTED, "402", "auto", cooldown_seconds=600)
    s = quota.get_status(conn, "pv-a")
    assert s["status"] == "exhausted"
    assert s["reason"] == "402"
    assert s["cooldown_until"] is not None
    conn.close()

def test_record_error_writes_event(tmp_path):
    conn = _fresh_db(tmp_path)
    cls = quota.classify_error(402, "payment required")
    st = quota.record_error(conn, "pv-a", cls, {"quota_guard": {"cooldown_seconds": 60}})
    assert st == "exhausted"
    ev = conn.execute("SELECT event_type, status FROM provider_events ORDER BY id DESC LIMIT 1").fetchone()
    assert ev["event_type"].startswith("status:")
    assert ev["status"] == "exhausted"
    conn.close()

def test_record_rate_limit_does_not_change_quota(tmp_path):
    """普通 429 只记事件，不动额度状态。"""
    conn = _fresh_db(tmp_path)
    cls = quota.classify_error(429, "rate limit exceeded")
    quota.record_error(conn, "pv-a", cls, {})
    s = quota.get_status(conn, "pv-a")
    assert s["status"] == "unknown"
    cnt = conn.execute("SELECT COUNT(*) c FROM provider_events WHERE provider='pv-a'").fetchone()["c"]
    assert cnt == 1
    conn.close()


# ── 冷却恢复 ──

def test_recover_due_revives_expired(tmp_path):
    conn = _fresh_db(tmp_path)
    # 冷却已过期：直接构造 cooldown_until 在过去
    conn.execute(
        "INSERT INTO provider_quota (provider, status, reason, source, cooldown_until) "
        "VALUES ('pv-a','exhausted','402','auto', datetime('now','-10 seconds'))")
    conn.commit()
    revived = quota.recover_due(conn)
    assert revived == ["pv-a"]
    assert quota.get_status(conn, "pv-a")["status"] == "unknown"
    conn.close()

def test_recover_due_skips_active_cooldown(tmp_path):
    conn = _fresh_db(tmp_path)
    quota.set_status(conn, "pv-a", quota.EXHAUSTED, "402", "auto", cooldown_seconds=3600)
    assert quota.recover_due(conn) == []
    assert quota.get_status(conn, "pv-a")["status"] == "exhausted"
    conn.close()


# ── 候选过滤 ──

def _p(name):
    return {"name": name}

def test_filter_excludes_exhausted():
    providers = [_p("a"), _p("b")]
    statuses = {"a": {"status": "exhausted"}, "b": {"status": "available"}}
    allowed, blocked = quota.filter_candidates(providers, statuses, "normal")
    assert [p["name"] for p in allowed] == ["b"]
    assert blocked == [("a", "exhausted")]

def test_filter_low_blocks_large_only():
    providers = [_p("a")]
    statuses = {"a": {"status": "low"}}
    allowed, blocked = quota.filter_candidates(providers, statuses, "large")
    assert allowed == []
    assert blocked[0][1].startswith("low")
    # normal 预算放行
    allowed2, _ = quota.filter_candidates(providers, statuses, "normal")
    assert [p["name"] for p in allowed2] == ["a"]
    # small 预算放行
    allowed3, _ = quota.filter_candidates(providers, statuses, "small")
    assert [p["name"] for p in allowed3] == ["a"]

def test_filter_unknown_and_available_pass():
    providers = [_p("a"), _p("b")]
    statuses = {"a": {"status": "unknown"}, "b": {"status": "available"}}
    allowed, blocked = quota.filter_candidates(providers, statuses, "large")
    assert [p["name"] for p in allowed] == ["a", "b"]
    assert blocked == []

def test_filter_disabled_guard_passes_all():
    providers = [_p("a")]
    statuses = {"a": {"status": "exhausted"}}
    allowed, blocked = quota.filter_candidates(providers, statuses, "normal",
                                               cfg={"quota_guard": {"enabled": False}})
    assert [p["name"] for p in allowed] == ["a"]
    assert blocked == []


# ── 管理总览 ──

def test_provider_status_summary(tmp_path):
    conn = _fresh_db(tmp_path)
    quota.set_status(conn, "pv-a", quota.LOW, "近预警线", "auto")
    summ = quota.provider_status_summary(conn, {"quota_guard": {"enabled": True}})
    assert summ["enabled"] is True
    assert summ["count"] == 1
    assert summ["providers"][0]["provider"] == "pv-a"
    conn.close()
