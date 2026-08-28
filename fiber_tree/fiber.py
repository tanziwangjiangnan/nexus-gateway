"""Fiber Tree — 任务树/逆栈引擎核心逻辑

Fiber（任务光纤）构成树形上下文，每棵 fiber 树对应一个统筹 Agent 的任务。
- 子 fiber 失败时级联回滚祖先
- undo_log 是 fiber 本地逆栈，提交时合并到父 fiber 或全局栈
- capabilities 声明此 fiber 的权限
- call_history 记录已调用的 (plugin_id, params_hash)，用于重复调用拦截
"""
import threading
import time
from typing import Any

from .storage import Storage, MemoryStorage


_GLOBAL_HISTORY_TTL = 86400  # 24 小时


class FiberTree:
    """Fiber 任务树管理器。

    用法:
        tree = FiberTree()
        fid = tree.create("agent-1", "执行任务X")
        tree.register(fid, "删除文件", lambda: os.remove("/tmp/x"))
        tree.commit(fid)

        # 或失败回滚:
        tree.fail(fid, cascade=True)
    """

    def __init__(self, storage: Storage = None):
        self.storage = storage or MemoryStorage()
        self._lock = threading.Lock()
        self._next_fiber_id = 0

    # ── Fiber 生命周期 ──

    def create(self, agent_id: str, description: str,
               parent_id: int = None, capabilities: list = None) -> int:
        """创建新 fiber。返回 fiber_id。"""
        with self._lock:
            self._next_fiber_id += 1
            fid = self._next_fiber_id
            self.storage.create_fiber(
                fiber_id=fid,
                parent_id=parent_id,
                agent_id=agent_id,
                description=description,
                capabilities=capabilities or [],
                created_at=time.time(),
            )
            return fid

    def register(self, fiber_id: int, description: str,
                 revert_callable: Any) -> bool:
        """向 fiber 注册撤销操作。若 fiber 已终止则拒绝。"""
        f = self.storage.get_fiber(fiber_id)
        if not f or f["status"] != "active":
            return False
        return self.storage.add_undo_log(fiber_id, description, revert_callable)

    def fail(self, fiber_id: int, cascade: bool = True):
        """失败 fiber：LIFO 回滚自己的 undo_log，然后递归失败所有子 fiber。
        若 cascade=True 且此 fiber 有父节点，级联失败父 fiber。
        返回 (ok, 操作列表)。
        """
        f = self.storage.get_fiber(fiber_id)
        if not f or f["status"] != "active":
            return False, []

        # 先递归失败子 fiber
        for child_id in list(f.get("children", [])):
            self.fail(child_id, cascade=False)

        # LIFO 回滚自己的 undo_log
        ops = []
        while True:
            item = self.storage.pop_undo_log(fiber_id)
            if item is None:
                break
            desc, fn = item
            try:
                if fn is not None:
                    fn()
                ops.append(f"回滚: {desc}")
            except Exception as e:
                ops.append(f"回滚失败 ({desc}): {e}")

        self.storage.update_fiber(fiber_id, status="failed")

        # 级联失败父 fiber
        if cascade and f.get("parent_id") is not None:
            parent = self.storage.get_fiber(f["parent_id"])
            if parent and parent["status"] == "active":
                _, parent_ops = self.fail(f["parent_id"], cascade=False)
                ops.extend(parent_ops)

        return True, ops

    def commit(self, fiber_id: int) -> bool:
        """提交 fiber：合并 undo_log 到父 fiber（或全局栈），标记 committed。"""
        f = self.storage.get_fiber(fiber_id)
        if not f or f["status"] != "active":
            return False

        # 所有子 fiber 必须已终止
        for child_id in f.get("children", []):
            child = self.storage.get_fiber(child_id)
            if child and child["status"] == "active":
                return False
            if child and child["status"] == "failed":
                return False

        # 合并 undo_log 到父 fiber 或全局栈
        undo_log = self.storage.get_undo_log(fiber_id)
        if f.get("parent_id") is not None:
            parent = self.storage.get_fiber(f["parent_id"])
            if parent and parent["status"] == "active":
                for desc, fn in undo_log:
                    self.storage.add_undo_log(f["parent_id"], desc, fn)
        else:
            for desc, fn in undo_log:
                self.storage.push_undo_stack(desc, fn)

        # 清理 fiber 下的 undo_log
        for _ in undo_log:
            self.storage.pop_undo_log(fiber_id)

        self.storage.update_fiber(fiber_id, status="committed")
        self._cleanup_history_for_fiber(fiber_id)
        return True

    # ── 全局去重表 ──

    def global_call_lookup(self, plugin_id: str, params_hash: str) -> dict | None:
        """惰性清理：查找全局去重表，命中但超时则删除并返回 None。"""
        history = self.storage.get_all_call_history()
        key = f"{plugin_id}:{params_hash}"
        entry = history.get(key)
        if entry is None:
            return None
        now = time.time()
        if now - entry["timestamp"] > _GLOBAL_HISTORY_TTL:
            self.storage.remove_call_history(plugin_id, params_hash)
            return None
        return entry

    def global_call_add(self, plugin_id: str, params_hash: str,
                        fiber_id: int, result_preview: str = ""):
        """写入全局去重表。"""
        key = f"{plugin_id}:{params_hash}"
        # 直接通过 storage 的全局表存储
        # 使用 MemoryStorage 的 _global_call_history 或 SQLite 的 global_call_history
        # 简化处理：通过 storage 的 add_call_history 和独立存储
        # 这里用 storage 的通用方法
        fiber = self.storage.get_fiber(fiber_id)
        if fiber:
            self.storage.add_call_history(fiber_id, plugin_id, params_hash, time.time())
        # 也存到全局表
        # 对于 MemoryStorage，存到 _global_call_history
        # 对于 SQLiteStorage，存到 global_call_history 表
        # 通过 storage 特定方法处理
        self._add_global_call(key, fiber_id, plugin_id, params_hash, result_preview)

    def _add_global_call(self, key: str, fiber_id: int,
                         plugin_id: str, params_hash: str, result_preview: str):
        """内部方法：向全局去重表写入一条记录。"""
        self.storage.add_global_call_history(
            key, fiber_id, plugin_id, params_hash,
            time.time(), result_preview[:200])

    def global_call_remove(self, plugin_id: str, params_hash: str):
        """从全局去重表删除单条记录。"""
        self.storage.remove_call_history(plugin_id, params_hash)

    def _cleanup_history_for_fiber(self, fiber_id: int):
        """主动清理：遍历 fiber 及其子树，删除所有 call_history 对应的全局表条目。"""
        f = self.storage.get_fiber(fiber_id)
        if not f:
            return
        for entry in self.storage.get_call_history(fiber_id):
            self.storage.remove_call_history(entry["plugin_id"], entry["params_hash"])
        for child_id in f.get("children", []):
            self._cleanup_history_for_fiber(child_id)

    # ── 全局 undo 栈 ──

    def undo_pop(self):
        """弹出并执行最后一条撤销回调。"""
        item = self.storage.pop_undo_stack()
        if item is None:
            return False, "undo_stack 为空"
        desc, fn = item
        try:
            if fn is not None:
                fn()
            return True, f"已撤销: {desc}"
        except Exception as e:
            self.storage.push_undo_stack(desc, fn)
            return False, f"撤销失败 ({desc}): {e}"

    def undo_clear(self, reason=""):
        """清空全局逆栈。"""
        self.storage.clear_undo_stack()

    def undo_list(self) -> list:
        """查看全局逆栈。"""
        return self.storage.list_undo_stack()

    # ── 查询 ──

    def get_fiber(self, fiber_id: int) -> dict | None:
        return self.storage.get_fiber(fiber_id)

    def get_all_fibers(self) -> dict:
        return self.storage.get_all_fibers()

    # ── 定时清理 ──

    def _cleanup_global_history_periodic(self):
        """定时清理：后台线程每 1 小时扫描，删除超时条目。"""
        while True:
            time.sleep(3600)
            now = time.time()
            history = self.storage.get_all_call_history()
            expired = [k for k, v in history.items()
                       if now - v["timestamp"] > _GLOBAL_HISTORY_TTL]
            for k in expired:
                # 从 key 解析 plugin_id 和 params_hash
                if ":" in k:
                    pid, ph = k.split(":", 1)
                    self.storage.remove_call_history(pid, ph)
            if expired:
                print(f"[全局去重] 定时清理 {len(expired)} 条过期记录")

    def start_cleanup_thread(self):
        """启动定时清理后台线程（daemon）。"""
        t = threading.Thread(target=self._cleanup_global_history_periodic, daemon=True)
        t.start()
        return t