"""存储层抽象 — 支持 SQLite 和纯内存两种实现。

Fiber 数据、undo_log、call_history 通过此接口持久化或暂存。
"""
import abc
import json
import sqlite3
import threading
import time
from typing import Any


class Storage(abc.ABC):
    """Fiber 存储抽象接口"""

    @abc.abstractmethod
    def create_fiber(self, fiber_id: int, parent_id: int | None,
                     agent_id: str, description: str,
                     capabilities: list, created_at: float) -> dict:
        ...

    @abc.abstractmethod
    def get_fiber(self, fiber_id: int) -> dict | None:
        ...

    @abc.abstractmethod
    def update_fiber(self, fiber_id: int, **kwargs) -> bool:
        ...

    @abc.abstractmethod
    def add_child(self, fiber_id: int, child_id: int) -> bool:
        ...

    @abc.abstractmethod
    def get_children(self, fiber_id: int) -> list[int]:
        ...

    @abc.abstractmethod
    def add_undo_log(self, fiber_id: int, description: str, revert_callable: Any) -> bool:
        ...

    @abc.abstractmethod
    def pop_undo_log(self, fiber_id: int) -> tuple | None:
        ...

    @abc.abstractmethod
    def get_undo_log(self, fiber_id: int) -> list:
        ...

    @abc.abstractmethod
    def add_call_history(self, fiber_id: int, plugin_id: str,
                         params_hash: str, timestamp: float) -> bool:
        ...

    @abc.abstractmethod
    def get_call_history(self, fiber_id: int) -> list:
        ...

    @abc.abstractmethod
    def get_all_fibers(self) -> dict[int, dict]:
        ...

    @abc.abstractmethod
    def get_all_call_history(self) -> dict:
        ...

    @abc.abstractmethod
    def remove_call_history(self, plugin_id: str, params_hash: str) -> bool:
        ...

    @abc.abstractmethod
    def add_global_call_history(self, key: str, fiber_id: int,
                                plugin_id: str, params_hash: str,
                                timestamp: float, result_preview: str) -> bool:
        ...

    @abc.abstractmethod
    def get_global_call_history(self, key: str) -> dict | None:
        ...

    @abc.abstractmethod
    def has_undo_stack(self) -> bool:
        ...

    @abc.abstractmethod
    def push_undo_stack(self, description: str, revert_callable: Any):
        ...

    @abc.abstractmethod
    def pop_undo_stack(self) -> tuple | None:
        ...

    @abc.abstractmethod
    def clear_undo_stack(self):
        ...

    @abc.abstractmethod
    def list_undo_stack(self) -> list:
        ...


class MemoryStorage(Storage):
    """纯内存存储 — 用于测试或轻量场景。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._fibers: dict[int, dict] = {}
        self._undo_logs: dict[int, list] = {}
        self._call_history: dict[int, list] = {}
        self._global_call_history: dict = {}
        self._undo_stack: list = []

    def create_fiber(self, fiber_id, parent_id, agent_id, description,
                     capabilities, created_at):
        f = {
            "id": fiber_id,
            "parent_id": parent_id,
            "agent_id": agent_id,
            "description": description,
            "status": "active",
            "capabilities": list(capabilities) if capabilities else [],
            "children": [],
            "created_at": created_at,
        }
        with self._lock:
            self._fibers[fiber_id] = f
            self._undo_logs[fiber_id] = []
            self._call_history[fiber_id] = []
            if parent_id is not None and parent_id in self._fibers:
                self._fibers[parent_id]["children"].append(fiber_id)
        return f

    def get_fiber(self, fiber_id):
        return self._fibers.get(fiber_id)

    def update_fiber(self, fiber_id, **kwargs):
        with self._lock:
            f = self._fibers.get(fiber_id)
            if not f:
                return False
            f.update(kwargs)
            return True

    def add_child(self, fiber_id, child_id):
        with self._lock:
            f = self._fibers.get(fiber_id)
            if not f:
                return False
            if child_id not in f["children"]:
                f["children"].append(child_id)
            return True

    def get_children(self, fiber_id):
        f = self._fibers.get(fiber_id)
        return list(f["children"]) if f else []

    def add_undo_log(self, fiber_id, description, revert_callable):
        with self._lock:
            logs = self._undo_logs.get(fiber_id)
            if logs is None:
                return False
            logs.append((description, revert_callable))
            return True

    def pop_undo_log(self, fiber_id):
        with self._lock:
            logs = self._undo_logs.get(fiber_id)
            if not logs:
                return None
            return logs.pop()

    def get_undo_log(self, fiber_id):
        return list(self._undo_logs.get(fiber_id, []))

    def add_call_history(self, fiber_id, plugin_id, params_hash, timestamp):
        with self._lock:
            ch = self._call_history.get(fiber_id)
            if ch is None:
                return False
            ch.append({"plugin_id": plugin_id, "params_hash": params_hash, "time": timestamp})
            return True

    def get_call_history(self, fiber_id):
        return list(self._call_history.get(fiber_id, []))

    def get_all_fibers(self):
        return dict(self._fibers)

    def get_all_call_history(self):
        return dict(self._global_call_history)

    def remove_call_history(self, plugin_id, params_hash):
        key = f"{plugin_id}:{params_hash}"
        with self._lock:
            return self._global_call_history.pop(key, None) is not None

    def add_global_call_history(self, key, fiber_id, plugin_id, params_hash, timestamp, result_preview):
        with self._lock:
            self._global_call_history[key] = {
                "fiber_id": fiber_id,
                "timestamp": timestamp,
                "result_preview": result_preview[:200],
            }
        return True

    def get_global_call_history(self, key):
        return self._global_call_history.get(key)

    def has_undo_stack(self):
        return len(self._undo_stack) > 0

    def push_undo_stack(self, description, revert_callable):
        self._undo_stack.append((description, revert_callable))

    def pop_undo_stack(self):
        if not self._undo_stack:
            return None
        return self._undo_stack.pop()

    def clear_undo_stack(self):
        self._undo_stack.clear()

    def list_undo_stack(self):
        return [desc for desc, _ in self._undo_stack]


class SQLiteStorage(Storage):
    """SQLite 持久化存储。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        conn = self._get_conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS fibers (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            agent_id TEXT NOT NULL,
            description TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            capabilities TEXT DEFAULT '[]',
            created_at REAL NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS fiber_children (
            fiber_id INTEGER NOT NULL,
            child_id INTEGER NOT NULL,
            PRIMARY KEY (fiber_id, child_id)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS fiber_undo_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiber_id INTEGER NOT NULL,
            description TEXT NOT NULL,
            idx INTEGER NOT NULL DEFAULT 0
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS fiber_call_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fiber_id INTEGER NOT NULL,
            plugin_id TEXT NOT NULL,
            params_hash TEXT NOT NULL,
            timestamp REAL NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS global_call_history (
            key TEXT PRIMARY KEY,
            fiber_id INTEGER NOT NULL,
            plugin_id TEXT NOT NULL,
            params_hash TEXT NOT NULL,
            timestamp REAL NOT NULL,
            result_preview TEXT DEFAULT ''
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS undo_stack (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL,
            idx INTEGER NOT NULL DEFAULT 0
        )""")
        conn.commit()
        conn.close()

    def create_fiber(self, fiber_id, parent_id, agent_id, description,
                     capabilities, created_at):
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO fibers (id, parent_id, agent_id, description, status, capabilities, created_at) VALUES (?,?,?,?,?,?,?)",
                (fiber_id, parent_id, agent_id, description, "active",
                 json.dumps(capabilities or []), created_at))
            if parent_id is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO fiber_children (fiber_id, child_id) VALUES (?,?)",
                    (parent_id, fiber_id))
            conn.commit()
            conn.close()
        return self.get_fiber(fiber_id)

    def get_fiber(self, fiber_id):
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM fibers WHERE id=?", (fiber_id,)).fetchone()
        conn.close()
        if not row:
            return None
        f = dict(row)
        f["capabilities"] = json.loads(f.get("capabilities", "[]"))
        # 加载 children
        conn2 = self._get_conn()
        children = [r["child_id"] for r in conn2.execute(
            "SELECT child_id FROM fiber_children WHERE fiber_id=?", (fiber_id,)).fetchall()]
        conn2.close()
        f["children"] = children
        return f

    def update_fiber(self, fiber_id, **kwargs):
        with self._lock:
            conn = self._get_conn()
            sets = ", ".join(f"{k}=?" for k in kwargs)
            vals = list(kwargs.values()) + [fiber_id]
            conn.execute(f"UPDATE fibers SET {sets} WHERE id=?", vals)
            conn.commit()
            conn.close()
        return True

    def add_child(self, fiber_id, child_id):
        with self._lock:
            conn = self._get_conn()
            conn.execute("INSERT OR IGNORE INTO fiber_children (fiber_id, child_id) VALUES (?,?)",
                         (fiber_id, child_id))
            conn.commit()
            conn.close()
        return True

    def get_children(self, fiber_id):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT child_id FROM fiber_children WHERE fiber_id=?", (fiber_id,)).fetchall()
        conn.close()
        return [r["child_id"] for r in rows]

    def add_undo_log(self, fiber_id, description, revert_callable):
        with self._lock:
            conn = self._get_conn()
            # 获取下一个 idx
            row = conn.execute("SELECT COALESCE(MAX(idx), -1) + 1 as n FROM fiber_undo_log WHERE fiber_id=?",
                               (fiber_id,)).fetchone()
            idx = row["n"] if row else 0
            conn.execute(
                "INSERT INTO fiber_undo_log (fiber_id, description, idx) VALUES (?,?,?)",
                (fiber_id, description, idx))
            conn.commit()
            conn.close()
        return True

    def pop_undo_log(self, fiber_id):
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT id, description, idx FROM fiber_undo_log WHERE fiber_id=? ORDER BY idx DESC LIMIT 1",
                (fiber_id,)).fetchone()
            if not row:
                conn.close()
                return None
            conn.execute("DELETE FROM fiber_undo_log WHERE id=?", (row["id"],))
            conn.commit()
            conn.close()
            return (row["description"], None)

    def get_undo_log(self, fiber_id):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT description FROM fiber_undo_log WHERE fiber_id=? ORDER BY idx",
            (fiber_id,)).fetchall()
        conn.close()
        return [(r["description"], None) for r in rows]

    def add_call_history(self, fiber_id, plugin_id, params_hash, timestamp):
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO fiber_call_history (fiber_id, plugin_id, params_hash, timestamp) VALUES (?,?,?,?)",
                (fiber_id, plugin_id, params_hash, timestamp))
            conn.commit()
            conn.close()
        return True

    def get_call_history(self, fiber_id):
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT plugin_id, params_hash, timestamp FROM fiber_call_history WHERE fiber_id=? ORDER BY id",
            (fiber_id,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_all_fibers(self):
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM fibers").fetchall()
        conn.close()
        result = {}
        for r in rows:
            f = dict(r)
            f["capabilities"] = json.loads(f.get("capabilities", "[]"))
            f["children"] = self.get_children(f["id"])
            result[f["id"]] = f
        return result

    def get_all_call_history(self):
        conn = self._get_conn()
        rows = conn.execute("SELECT * FROM global_call_history").fetchall()
        conn.close()
        return {r["key"]: {
            "fiber_id": r["fiber_id"],
            "timestamp": r["timestamp"],
            "result_preview": r["result_preview"],
        } for r in rows}

    def remove_call_history(self, plugin_id, params_hash):
        key = f"{plugin_id}:{params_hash}"
        with self._lock:
            conn = self._get_conn()
            conn.execute("DELETE FROM global_call_history WHERE key=?", (key,))
            conn.commit()
            conn.close()
        return True

    def add_global_call_history(self, key, fiber_id, plugin_id, params_hash, timestamp, result_preview):
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT OR REPLACE INTO global_call_history (key, fiber_id, plugin_id, params_hash, timestamp, result_preview) VALUES (?,?,?,?,?,?)",
                (key, fiber_id, plugin_id, params_hash, timestamp, result_preview[:200]))
            conn.commit()
            conn.close()
        return True

    def get_global_call_history(self, key):
        conn = self._get_conn()
        row = conn.execute("SELECT * FROM global_call_history WHERE key=?", (key,)).fetchone()
        conn.close()
        if not row:
            return None
        return {
            "fiber_id": row["fiber_id"],
            "timestamp": row["timestamp"],
            "result_preview": row["result_preview"],
        }

    def has_undo_stack(self):
        conn = self._get_conn()
        row = conn.execute("SELECT COUNT(*) as n FROM undo_stack").fetchone()
        conn.close()
        return row["n"] > 0

    def push_undo_stack(self, description, revert_callable):
        with self._lock:
            conn = self._get_conn()
            row = conn.execute("SELECT COALESCE(MAX(idx), -1) + 1 as n FROM undo_stack").fetchone()
            idx = row["n"] if row else 0
            conn.execute("INSERT INTO undo_stack (description, idx) VALUES (?,?)",
                         (description, idx))
            conn.commit()
            conn.close()

    def pop_undo_stack(self):
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT id, description FROM undo_stack ORDER BY idx DESC LIMIT 1").fetchone()
            if not row:
                conn.close()
                return None
            conn.execute("DELETE FROM undo_stack WHERE id=?", (row["id"],))
            conn.commit()
            conn.close()
            return (row["description"], None)

    def clear_undo_stack(self):
        with self._lock:
            conn = self._get_conn()
            conn.execute("DELETE FROM undo_stack")
            conn.commit()
            conn.close()

    def list_undo_stack(self):
        conn = self._get_conn()
        rows = conn.execute("SELECT description FROM undo_stack ORDER BY idx").fetchall()
        conn.close()
        return [r["description"] for r in rows]


