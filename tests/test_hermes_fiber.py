"""测试 hermes_fiber 包：FiberRuntime"""
import pytest, threading
from ops_gateway_core.fiber import FiberRuntime, Fiber


@pytest.fixture
def runtime():
    return FiberRuntime()


class TestFiberRuntime:
    def test_fiber_create(self, runtime):
        fid = runtime.fiber_create("agent-1", "测试任务")
        assert fid == 1
        f = runtime.fiber_get(fid)
        assert f is not None
        assert f.agent_id == "agent-1"
        assert f.description == "测试任务"
        assert f.status == "active"

    def test_fiber_create_with_parent(self, runtime):
        p = runtime.fiber_create("agent-1", "父任务")
        c = runtime.fiber_create("agent-2", "子任务", parent_id=p)
        assert c == 2
        f = runtime.fiber_get(c)
        assert f.parent_id == p

    def test_fiber_register(self, runtime):
        fid = runtime.fiber_create("agent-1", "任务")
        called = False

        def revert():
            nonlocal called
            called = True

        runtime.fiber_register(fid, "回滚操作", revert)
        ok, ops = runtime.fiber_fail(fid)
        assert ok
        assert called

    def test_fiber_commit(self, runtime):
        fid = runtime.fiber_create("agent-1", "任务")
        assert runtime.fiber_commit(fid)
        assert runtime.fiber_get(fid).status == "committed"

    def test_fiber_commit_twice(self, runtime):
        fid = runtime.fiber_create("agent-1", "任务")
        assert runtime.fiber_commit(fid)
        assert not runtime.fiber_commit(fid)

    def test_fiber_fail(self, runtime):
        fid = runtime.fiber_create("agent-1", "任务")
        ok, ops = runtime.fiber_fail(fid)
        assert ok
        assert runtime.fiber_get(fid).status == "failed"

    def test_fiber_fail_not_found(self, runtime):
        ok, ops = runtime.fiber_fail(999)
        assert not ok

    def test_fiber_get_nonexistent(self, runtime):
        assert runtime.fiber_get(999) is None

    def test_fiber_all(self, runtime):
        runtime.fiber_create("a1", "t1")
        runtime.fiber_create("a2", "t2")
        all_f = runtime.fiber_all()
        assert len(all_f) == 2

    def test_undo_register(self, runtime):
        called = []

        def revert():
            called.append(1)

        runtime.undo_register("全局操作", revert)
        ok, msg = runtime.undo_pop()
        assert ok
        assert len(called) == 1

    def test_undo_clear(self, runtime):
        runtime.undo_register("op1", lambda: None)
        runtime.undo_register("op2", lambda: None)
        runtime.undo_clear("清理测试")
        ok, msg = runtime.undo_pop()
        assert not ok
        assert "空" in msg

    def test_undo_list(self, runtime):
        runtime.undo_register("op1", lambda: None)
        runtime.undo_register("op2", lambda: None)
        lst = runtime.undo_list()
        assert len(lst) == 2

    def test_global_call_lookup_miss(self, runtime):
        assert runtime.global_call_lookup("plugin1", "hash1") is None

    def test_global_call_add_and_lookup(self, runtime):
        fid = runtime.fiber_create("a1", "t")
        runtime.global_call_add("plugin1", "hash1", fid, "ok")
        hit = runtime.global_call_lookup("plugin1", "hash1")
        assert hit is not None
        assert hit["fiber_id"] == fid

    def test_global_call_remove(self, runtime):
        fid = runtime.fiber_create("a1", "t")
        runtime.global_call_add("plugin1", "hash1", fid, "ok")
        runtime.global_call_remove("plugin1", "hash1")
        assert runtime.global_call_lookup("plugin1", "hash1") is None

    def test_cleanup_global_history_for_fiber(self, runtime):
        fid = runtime.fiber_create("a1", "t")
        runtime.global_call_add("p1", "h1", fid, "ok")
        runtime.global_call_add("p2", "h2", fid, "ok")
        # 手动向 fiber 的 call_history 添加记录，cleanup 才能找到
        f = runtime.fiber_get(fid)
        f.call_history.append({"plugin_id": "p1", "params_hash": "h1"})
        f.call_history.append({"plugin_id": "p2", "params_hash": "h2"})
        runtime._cleanup_global_history_for_fiber(fid)
        assert runtime.global_call_lookup("p1", "h1") is None
        assert runtime.global_call_lookup("p2", "h2") is None


class TestFiberClass:
    def test_fiber_attributes(self):
        f = Fiber(id=1, agent_id="a1", description="d", parent_id=None, status="active")
        assert f.id == 1
        assert f.agent_id == "a1"
        assert f.status == "active"

    def test_fiber_defaults(self):
        f = Fiber(id=1, agent_id="a1", description="d", parent_id=None)
        assert f.status == "active"
        assert f.undo_log == []
        assert f.children == []