"""额度组合操作（provider_router/quota.py 下沉的两个函数）— 单元测试

覆盖：冷却复活、额度过滤、限额摘除、非额度失败不改状态。
用内存库自建表，不碰生产 gateway.db。
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router import quota as q

SCHEMA = '''
CREATE TABLE provider_quota(
  provider TEXT PRIMARY KEY, status TEXT, reason TEXT, source TEXT,
  last_checked_at TEXT, cooldown_until TEXT, updated_at TEXT);
CREATE TABLE provider_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT, event_type TEXT,
  status TEXT, http_status INTEGER, detail TEXT, created_at TEXT);
'''


def _db():
    """新建一个只有额度表的内存库。"""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA)
    return c


def _providers(*names):
    return [{"name": n, "weight": 1, "models": ["m-" + n]} for n in names]


def _set(c, provider, status, cooldown):
    c.execute(
        "INSERT INTO provider_quota(provider,status,reason,source,cooldown_until,updated_at) "
        "VALUES(?,?,?,?,?,datetime('now'))",
        (provider, status, "test", "auto", cooldown))
    c.commit()


def test_prepare_candidates_recovers_expired_cooldown():
    """冷却到期的 exhausted 应被复活并放行。"""
    c = _db()
    _set(c, "kouri", q.EXHAUSTED, "2000-01-01 00:00:00")
    allowed, blocked = q.prepare_candidates(c, _providers("kouri"), q.BUDGET_NORMAL, {})
    assert [p["name"] for p in allowed] == ["kouri"]
    assert blocked == []
    assert q.get_status(c, "kouri")["status"] == q.UNKNOWN


def test_prepare_candidates_blocks_exhausted_in_cooldown():
    """冷却期内的 exhausted 应被剔除。"""
    c = _db()
    _set(c, "kouri", q.EXHAUSTED, "2999-01-01 00:00:00")
    allowed, blocked = q.prepare_candidates(c, _providers("kouri", "ds"), q.BUDGET_NORMAL, {})
    assert [p["name"] for p in allowed] == ["ds"]
    assert [n for n, _ in blocked] == ["kouri"]


def test_prepare_candidates_low_blocks_large_budget_only():
    """low 状态：大预算剔除，小预算放行。"""
    c = _db()
    _set(c, "ds", q.LOW, None)
    allowed_large, blocked_large = q.prepare_candidates(c, _providers("ds"), q.BUDGET_LARGE, {})
    assert allowed_large == [] and len(blocked_large) == 1
    allowed_small, _ = q.prepare_candidates(c, _providers("ds"), q.BUDGET_SMALL, {})
    assert [p["name"] for p in allowed_small] == ["ds"]


def test_record_error_marks_exhausted_on_quota_message():
    """403 + Insufficient quota（空格版）应被认定为额度耗尽。"""
    c = _db()
    body = '{"error":{"message":"Insufficient quota. Please top up your account."}}'
    exhausted, detail = q.record_error_if_exhausted(c, "kouri", 403, body, {})
    assert exhausted is True
    assert "insufficient quota" in detail.lower()
    assert q.get_status(c, "kouri")["status"] == q.EXHAUSTED


def test_record_error_ignores_rate_limit():
    """普通 429 限流不应改额度状态，只记事件。"""
    c = _db()
    exhausted, detail = q.record_error_if_exhausted(c, "ds", 429, '{"error":{"message":"rate limit"}}', {})
    assert exhausted is False
    assert "rate" in detail.lower()
    assert q.get_status(c, "ds")["status"] == q.UNKNOWN
    # 429 限流不写额度事件（交熔断路径处理），额度状态保持 unknown
    n = c.execute("SELECT COUNT(*) FROM provider_events").fetchone()[0]
    assert n == 0


def test_record_error_ignores_auth_401():
    """401 鉴权失败属熔断范畴，不当额度耗尽。"""
    c = _db()
    exhausted, _ = q.record_error_if_exhausted(c, "ds", 401, '{"error":"invalid token"}', {})
    assert exhausted is False
    assert q.get_status(c, "ds")["status"] == q.UNKNOWN
