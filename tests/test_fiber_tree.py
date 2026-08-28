"""测试 fiber_tree 包：FiberTree + MemoryStorage + SQLiteStorage"""
import pytest, os, tempfile
from fiber_tree import FiberTree, Storage, MemoryStorage, SQLiteStorage

# ── 夹具 ──

@pytest.fixture
def mem_store():
    return MemoryStorage()

@pytest.fixture
def sqlite_store():
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        path = f.name
    store = SQLiteStorage(path)
    yield store
    os.unlink(path)

@pytest.fixture
def tree_mem():
    return FiberTree(storage=MemoryStorage())

@pytest.fixture
def tree_sqlite():
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        path = f.name
    store = SQLiteStorage(path)
    t = FiberTree(storage=store)
    yield t
    os.unlink(path)

# ── MemoryStorage 基本 CRUD ──

class TestMemoryStorage:
    def test_create_and_get_fiber(self, mem_store):
        mem_store.create_fiber(1, None, 'a1', 'desc', 'active', 'cap')
        f = mem_store.get_fiber(1)
        assert f is not None
        assert f['agent_id'] == 'a1'
        assert f['description'] == 'desc'
        assert f['status'] == 'active'

    def test_get_fiber_missing(self, mem_store):
        assert mem_store.get_fiber(999) is None

    def test_update_fiber(self, mem_store):
        mem_store.create_fiber(1, None, 'a1', 'desc', 'active', 'cap')
        mem_store.update_fiber(1, status='failed')
        f = mem_store.get_fiber(1)
        assert f['status'] == 'failed'

    def test_child_relationship(self, mem_store):
        mem_store.create_fiber(1, None, 'a1', 'p', 'active', '')
        mem_store.create_fiber(2, 1, 'a2', 'c', 'active', '')
        mem_store.add_child(1, 2)
        assert mem_store.get_children(1) == [2]

    def test_undo_log(self, mem_store):
        mem_store.push_undo_stack('op1', lambda: None)
        assert mem_store.has_undo_stack()
        desc, fn = mem_store.pop_undo_stack()
        assert desc == 'op1'
        assert callable(fn)
        assert not mem_store.has_undo_stack()

    def test_undo_clear(self, mem_store):
        mem_store.push_undo_stack('op1', lambda: None)
        mem_store.push_undo_stack('op2', lambda: None)
        mem_store.clear_undo_stack()
        assert not mem_store.has_undo_stack()

    def test_global_call_history(self, mem_store):
        mem_store.add_global_call_history('p1:h1', 1, 'p1', 'h1', 'ts', 'ok')
        assert mem_store.get_global_call_history('p1:h1') is not None
        mem_store.remove_call_history('p1', 'h1')
        assert mem_store.get_global_call_history('p1:h1') is None

    def test_get_all_fibers(self, mem_store):
        mem_store.create_fiber(1, None, 'a1', 'd1', 'active', '')
        mem_store.create_fiber(2, 1, 'a2', 'd2', 'active', '')
        all_f = mem_store.get_all_fibers()
        assert len(all_f) == 2

# ── SQLiteStorage ──

class TestSQLiteStorage:
    def test_create_and_get_fiber(self, sqlite_store):
        sqlite_store.create_fiber(1, None, 'a1', 'desc', 'active', '')
        f = sqlite_store.get_fiber(1)
        assert f is not None
        assert f['agent_id'] == 'a1'

    def test_get_fiber_missing(self, sqlite_store):
        assert sqlite_store.get_fiber(999) is None

    def test_update_fiber(self, sqlite_store):
        sqlite_store.create_fiber(1, None, 'a1', 'd', 'active', '')
        sqlite_store.update_fiber(1, status='committed')
        assert sqlite_store.get_fiber(1)['status'] == 'committed'

    def test_child_relationship(self, sqlite_store):
        sqlite_store.create_fiber(1, None, 'a1', 'p', 'active', '')
        sqlite_store.create_fiber(2, 1, 'a2', 'c', 'active', '')
        sqlite_store.add_child(1, 2)
        assert sqlite_store.get_children(1) == [2]

    def test_undo_log_fiber(self, sqlite_store):
        sqlite_store.create_fiber(1, None, 'a1', 'd', 'active', '')
        sqlite_store.add_undo_log(1, 'op1', 'lambda')
        logs = sqlite_store.get_undo_log(1)
        assert len(logs) == 1
        popped = sqlite_store.pop_undo_log(1)
        assert popped[0] == 'op1'

    def test_get_all_fibers(self, sqlite_store):
        sqlite_store.create_fiber(1, None, 'a1', 'd1', 'active', '')
        sqlite_store.create_fiber(2, 1, 'a2', 'd2', 'active', '')
        assert len(sqlite_store.get_all_fibers()) == 2

# ── FiberTree 集成 ──

class TestFiberTreeIntegration:
    def test_fiber_lifecycle_mem(self, tree_mem):
        fid = tree_mem.create('a1', 'test')
        assert fid == 1
        f = tree_mem.get_fiber(fid)
        assert f['status'] == 'active'
        assert tree_mem.commit(fid)
        assert tree_mem.get_fiber(fid)['status'] == 'committed'

    def test_fiber_create_with_parent(self, tree_mem):
        p = tree_mem.create('a1', 'parent')
        c = tree_mem.create('a2', 'child', parent_id=p)
        assert c == 2
        assert tree_mem.get_fiber(c)['parent_id'] == p

    def test_fiber_fail_no_cascade(self, tree_mem):
        p = tree_mem.create('a1', 'p')
        c = tree_mem.create('a2', 'c', parent_id=p)
        tree_mem.fail(c, cascade=False)
        assert tree_mem.get_fiber(c)['status'] == 'failed'
        assert tree_mem.get_fiber(p)['status'] == 'active'

    def test_fiber_fail_cascade(self, tree_mem):
        p = tree_mem.create('a1', 'p')
        c = tree_mem.create('a2', 'c', parent_id=p)
        tree_mem.fail(c, cascade=True)
        assert tree_mem.get_fiber(c)['status'] == 'failed'
        assert tree_mem.get_fiber(p)['status'] == 'failed'

    def test_fiber_commit_fails_children_active(self, tree_mem):
        p = tree_mem.create('a1', 'p')
        tree_mem.create('a2', 'c', parent_id=p)
        assert not tree_mem.commit(p)

    def test_commit_after_children_done(self, tree_mem):
        p = tree_mem.create('a1', 'p')
        c = tree_mem.create('a2', 'c', parent_id=p)
        # 子节点 failed 也不能 commit（FiberTree 要求所有子节点 committed）
        tree_mem.fail(c, cascade=False)
        assert not tree_mem.commit(p)

    def test_commit_after_all_children_committed(self, tree_mem):
        p = tree_mem.create('a1', 'p')
        c = tree_mem.create('a2', 'c', parent_id=p)
        tree_mem.commit(c)
        assert tree_mem.commit(p)

    def test_global_call_dedup(self, tree_mem):
        tree_mem.create('a1', 'root')
        tree_mem.global_call_add('p1', 'h1', 1, 'ok')
        hit = tree_mem.global_call_lookup('p1', 'h1')
        assert hit is not None
        assert hit['fiber_id'] == 1
        miss = tree_mem.global_call_lookup('p1', 'h2')
        assert miss is None

    def test_undo_stack(self, tree_mem):
        tree_mem.storage.push_undo_stack('op1', lambda: print('undo'))
        tree_mem.storage.push_undo_stack('op2', lambda: print('undo'))
        assert len(tree_mem.undo_list()) == 2
        ok, msg = tree_mem.undo_pop()
        assert ok
        assert 'op2' in msg
        assert len(tree_mem.undo_list()) == 1
        tree_mem.undo_clear()
        assert len(tree_mem.undo_list()) == 0

    def test_sqlite_equivalent(self, tree_sqlite):
        """SQLite 后端与 Memory 行为一致"""
        fid = tree_sqlite.create('a1', 'test')
        tree_sqlite.register(fid, 'step1', lambda: None)
        assert tree_sqlite.commit(fid)
        assert tree_sqlite.get_fiber(fid)['status'] == 'committed'