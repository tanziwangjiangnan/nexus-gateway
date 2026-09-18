"""数据库连接管理 — SQLite（WAL 模式）。

负责建表（registry/usage/health_log）与列迁移。
get_db() 每次调用返回新的连接，调用方负责 close()。
"""
import os
import sqlite3
from contextlib import contextmanager

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# [2026-09-18] 支持 GATEWAY_DB_PATH 覆盖：此前路径写死，导致「用 GATEWAY_DB_PATH 另起
# 一个隔离实例做实验」时，registry/usage/provider_quota 仍然写进生产库
# （2026-09-18 隔离实例把 kouri 误标 exhausted 到生产库，实测复现）。
DB_PATH = os.environ.get("GATEWAY_DB_PATH") or os.path.join(_PROJECT_ROOT, "gateway.db")

_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS registry (
        model TEXT PRIMARY KEY, pool TEXT, provider TEXT NOT NULL,
        tier TEXT DEFAULT 'B', status TEXT DEFAULT 'unknown',
        notes TEXT DEFAULT '', created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT, model TEXT NOT NULL,
        pool TEXT, provider TEXT, prompt_tokens INTEGER DEFAULT 0,
        completion_tokens INTEGER DEFAULT 0, ok INTEGER DEFAULT 1,
        checker_score REAL DEFAULT NULL,
        user_feedback INTEGER DEFAULT 0,
        path_type TEXT DEFAULT 'normal',
        role TEXT DEFAULT NULL,
        agent_id TEXT DEFAULT NULL,
        called_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS health_log (
        model TEXT NOT NULL, pool TEXT, provider TEXT,
        ok INTEGER NOT NULL, latency_ms INTEGER DEFAULT 0,
        error TEXT DEFAULT '', checked_at TEXT DEFAULT (datetime('now'))
    )""",
    # 额度状态：与普通故障状态分离（见 计划书-模型额度感知与任务保护-v1）
    """CREATE TABLE IF NOT EXISTS provider_quota (
        provider TEXT PRIMARY KEY,
        status TEXT DEFAULT 'unknown',
        reason TEXT DEFAULT '',
        source TEXT DEFAULT '',
        last_checked_at TEXT,
        cooldown_until TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    # 额度事件日志：分类结果留痕（402 / 429 / 关键词 / 手动）
    """CREATE TABLE IF NOT EXISTS provider_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider TEXT NOT NULL,
        event_type TEXT NOT NULL,
        status TEXT,
        http_status INTEGER,
        detail TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now'))
    )""",
]

# v2.7 迁移：安全追加列（若缺失）
_USAGE_MIGRATIONS = [
    ("checker_score", "REAL DEFAULT NULL"),
    ("user_feedback", "INTEGER DEFAULT 0"),
    ("path_type", "TEXT DEFAULT 'normal'"),
    ("role", "TEXT DEFAULT NULL"),
    ("agent_id", "TEXT DEFAULT NULL"),
]


def get_db(db_path: str = None) -> sqlite3.Connection:
    """获取 SQLite 连接（WAL 模式），确保表结构存在。"""
    # [2026-09-13 修复] isolation_level=None = 自动提交。
    # 未提交的写事务会一直持有 SQLite 写锁；服务里存在
    # 「get_db() → INSERT →（异常路径跳过 commit/close）」的写法，
    # 一旦泄漏，之后所有写者都报 database is locked。
    # 日志/注册表这类写入用自动提交即可，从根上消除长事务。
    conn = sqlite3.connect(db_path or DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # [2026-09-13 修复] WAL 下读不阻塞、写会立即 SQLITE_BUSY；
    # 且 PRAGMA journal_mode 本身需要短暂排他锁、不受 busy_timeout 保护，
    # 每次连接都执行会让 CLI 与服务并发时报 "database is locked"。
    # 改为：先给 busy_timeout，再读当前模式，只有非 WAL 才切换，且容忍失败。
    conn.execute("PRAGMA busy_timeout=8000")
    try:
        _mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if str(_mode).lower() != "wal":
            conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        pass  # 并发下切模式失败无妨，读连接照常可用
    for stmt in _SCHEMA:
        conn.execute(stmt)
    for col, col_type in _USAGE_MIGRATIONS:
        try:
            conn.execute(f"ALTER TABLE usage ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass  # 列已存在
    # [2026-09-13] registry 增加 providers 列：同一模型可能来自多个 provider，
    # 原来只有单值 provider 字段，多来源会被覆盖看不见。
    try:
        conn.execute("ALTER TABLE registry ADD COLUMN providers TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    return conn

@contextmanager
def db_conn(db_path: str = None):
    """连接上下文管理器：无论正常退出还是抛异常，都保证提交 + 关闭。

    [2026-09-13 新增] 替代裸的 `conn = get_db() ... conn.close()` 写法，
    避免异常路径跳过 close 造成连接/事务泄漏（曾导致 database is locked）。
    """
    conn = get_db(db_path)
    try:
        yield conn
    finally:
        try:
            conn.commit()
        except sqlite3.Error:
            pass
        finally:
            conn.close()
