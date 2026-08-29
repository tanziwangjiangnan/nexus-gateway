"""数据库连接管理 — SQLite（WAL 模式）。

负责建表（registry/usage/health_log）与列迁移。
get_db() 每次调用返回新的连接，调用方负责 close()。
"""
import os
import sqlite3

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_PROJECT_ROOT, "gateway.db")

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
        called_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS health_log (
        model TEXT NOT NULL, pool TEXT, provider TEXT,
        ok INTEGER NOT NULL, latency_ms INTEGER DEFAULT 0,
        error TEXT DEFAULT '', checked_at TEXT DEFAULT (datetime('now'))
    )""",
]

# v2.7 迁移：安全追加列（若缺失）
_USAGE_MIGRATIONS = [
    ("checker_score", "REAL DEFAULT NULL"),
    ("user_feedback", "INTEGER DEFAULT 0"),
]


def get_db(db_path: str = None) -> sqlite3.Connection:
    """获取 SQLite 连接（WAL 模式），确保表结构存在。"""
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    for stmt in _SCHEMA:
        conn.execute(stmt)
    for col, col_type in _USAGE_MIGRATIONS:
        try:
            conn.execute(f"ALTER TABLE usage ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass  # 列已存在
    conn.commit()
    return conn