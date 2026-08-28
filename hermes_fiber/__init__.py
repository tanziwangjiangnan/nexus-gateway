"""Hermes Fiber Runtime — 运行时 Fiber 管理 + 全局去重 + undo 栈

与 `fiber_tree/` 的区别：
- `fiber_tree/` = 持久化存储抽象（数据库层），独立于 FastAPI
- `hermes_fiber/` = 运行时内存状态（fiber 生命周期、undo 栈、全局去重表）

依赖：`hermes_cfg/`（可选，仅用于日志回调）
"""
from __future__ import annotations

import dataclasses
import threading
import time
from typing import Callable


@dataclasses.dataclass
class Fiber:
    """任务光纤。每棵 fiber 树对应一个统筹 Agent 的任务。
    - 子 fiber 失败时级联回滚祖先
    - undo_log 是 fiber 本地逆栈，提交时合并到父 fiber 或全局栈
    - capabilities 声明此 fiber 的权限
    - call_history 记录本 fiber 及其父 fiber 已调用的 (plugin_id, params_hash)
    """
    id: int
    parent_id: int | None
    agent_id: str
    description: str
    status: str = "active"       # active | committed | failed
    undo_log: list = dataclasses.field(default_factory=list)
    children: list = dataclasses.field(default_factory=list)
    capabilities: list = dataclasses.field(default_factory=list)
    call_history: list = dataclasses.field(default_factory=list)
    created_at: float = dataclasses.field(default_factory=time.time)


_GLOBAL_HISTORY_TTL = 86400  # 24 小时


class FiberRuntime:
    """运行时 Fiber 管理器。持有所有状态，实例化后供 gateway.py 使用。"""

    def __init__(self, lock: threading.Lock = None):
        self._lock = lock or threading.Lock()
        self._fibers: dict[int, Fiber] = {}
        self._undo_stack: list[tuple[str, Callable]] = []
        self._global_call_history: dict[str, dict] = {}
        self._next_fiber_id = 0

    # ── undo 栈 ──

    def undo_register(self, description: str, revert_callable: Callable):
        """注册运行时操作的撤销回调。"""
        self._undo_stack.append((description, revert_callable))

    def undo_pop(self) -> tuple[bool, str]:
        """弹出并执行最后一条撤销回调。"""
        if not self._undo_stack:
            return False, "undo_stack 为空"
        desc, fn = self._undo_stack.pop()
        try:
            fn()
            return True, f"已撤销: {desc}"
        except Exception as e:
            self._undo_stack.append((desc, fn))
            return False, f"撤销失败 ({desc}): {e}"

    def undo_clear(self, reason: str = ""):
        """清空全局逆栈。"""
        self._undo_stack.clear()

    def undo_list(self) -> list[str]:
        return [desc for desc, _ in self._undo_stack]

    # ── Fiber 生命周期 ──

    def fiber_create(self, agent_id: str, description: str,
                     parent_id: int = None, capabilities: list = None) -> int:
        """创建新 fiber。返回 fiber_id。"""
        with self._lock:
            self._next_fiber_id += 1
            fid = self._next_fiber_id
            f = Fiber(id=fid, parent_id=parent_id, agent_id=agent_id, description=description)
            if capabilities:
                f.capabilities = capabilities
            self._fibers[fid] = f
            if parent_id is not None and parent_id in self._fibers:
                self._fibers[parent_id].children.append(fid)
            return fid

    def fiber_register(self, fiber_id: int, description: str,
                       revert_callable: Callable) -> bool:
        """向 fiber 注册撤销操作。若 fiber 已终止则拒绝。"""
        f = self._fibers.get(fiber_id)
        if not f or f.status != "active":
            return False
        f.undo_log.append((description, revert_callable))
        return True

    def fiber_fail(self, fiber_id: int, cascade_parent: bool = True) -> tuple[bool, list]:
        """失败 fiber：LIFO 回滚自己的 undo_log，然后递归失败所有子 fiber。"""
        f = self._fibers.get(fiber_id)
        if not f or f.status != "active":
            return False, []
        # 先递归失败子 fiber
        for child_id in list(f.children):
            self.fiber_fail(child_id, cascade_parent=False)
        # LIFO 回滚
        ops = []
        while f.undo_log:
            desc, fn = f.undo_log.pop()
            try:
                fn()
                ops.append(f"回滚: {desc}")
            except Exception as e:
                ops.append(f"回滚失败 ({desc}): {e}")
        f.status = "failed"
        # 级联失败父 fiber
        if cascade_parent and f.parent_id is not None and f.parent_id in self._fibers:
            parent = self._fibers[f.parent_id]
            if parent.status == "active":
                _, parent_ops = self.fiber_fail(f.parent_id, cascade_parent=False)
                ops.extend(parent_ops)
        return True, ops

    def fiber_commit(self, fiber_id: int) -> bool:
        """提交 fiber：合并 undo_log 到父 fiber（或全局栈），标记 committed。"""
        f = self._fibers.get(fiber_id)
        if not f or f.status != "active":
            return False
        for child_id in f.children:
            child = self._fibers.get(child_id)
            if child and child.status == "active":
                return False
            if child and child.status == "failed":
                return False
        if f.parent_id is not None and f.parent_id in self._fibers:
            parent = self._fibers[f.parent_id]
            if parent.status == "active":
                parent.undo_log.extend(f.undo_log)
        else:
            self._undo_stack.extend(f.undo_log)
        f.undo_log.clear()
        f.status = "committed"
        self._cleanup_global_history_for_fiber(fiber_id)
        return True

    def fiber_get(self, fiber_id: int) -> Fiber | None:
        return self._fibers.get(fiber_id)

    def fiber_all(self) -> dict[int, Fiber]:
        return dict(self._fibers)

    # ── 全局去重表 ──

    def _global_call_key(self, plugin_id: str, params_hash: str) -> str:
        return f"{plugin_id}:{params_hash}"

    def global_call_lookup(self, plugin_id: str, params_hash: str) -> dict | None:
        key = self._global_call_key(plugin_id, params_hash)
        entry = self._global_call_history.get(key)
        if entry is None:
            return None
        now = time.time()
        if now - entry["timestamp"] > _GLOBAL_HISTORY_TTL:
            del self._global_call_history[key]
            return None
        return entry

    def global_call_add(self, plugin_id: str, params_hash: str,
                        fiber_id: int, result_preview: str = ""):
        key = self._global_call_key(plugin_id, params_hash)
        self._global_call_history[key] = {
            "fiber_id": fiber_id,
            "timestamp": time.time(),
            "result_preview": result_preview[:200],
        }

    def global_call_remove(self, plugin_id: str, params_hash: str):
        key = self._global_call_key(plugin_id, params_hash)
        self._global_call_history.pop(key, None)

    def _cleanup_global_history_for_fiber(self, fiber_id: int):
        f = self._fibers.get(fiber_id)
        if not f:
            return
        for entry in f.call_history:
            self.global_call_remove(entry["plugin_id"], entry["params_hash"])
        for child_id in list(f.children):
            self._cleanup_global_history_for_fiber(child_id)

    def cleanup_global_history_periodic(self):
        """后台定时清理线程。"""
        while True:
            time.sleep(3600)
            now = time.time()
            expired = [k for k, v in self._global_call_history.items()
                       if now - v["timestamp"] > _GLOBAL_HISTORY_TTL]
            for k in expired:
                self._global_call_history.pop(k, None)
            if expired:
                print(f"[全局去重] 定时清理 {len(expired)} 条过期记录")