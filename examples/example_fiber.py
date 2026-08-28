#!/usr/bin/env python3
"""FiberTree 使用示例 — 独立于网关，展示任务树/逆栈引擎核心功能。"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fiber_tree import FiberTree, MemoryStorage, SQLiteStorage


def main():
    # ── 示例 1: 内存存储（默认） ──
    print("=" * 60)
    print("示例 1: 内存存储 FiberTree")
    print("=" * 60)

    tree = FiberTree()  # 默认使用 MemoryStorage

    # 创建根 fiber
    root_id = tree.create("coordinator", "统筹任务: 发送邮件报告")
    print(f"✅ 创建根 fiber: id={root_id}, agent=coordinator")

    # 创建子 fiber（执行者）
    exec_id = tree.create("executor-1", "生成邮件内容", parent_id=root_id)
    print(f"✅ 创建执行者 fiber: id={exec_id}, parent={root_id}")

    # 注册撤销操作
    tree.register(exec_id, "删除临时文件", lambda: print("  ↪ 撤销: 删除临时文件"))
    tree.register(exec_id, "恢复数据库状态", lambda: print("  ↪ 撤销: 恢复数据库状态"))
    print(f"📝 注册了 2 个撤销操作")

    # 创建子 fiber（检查者）
    check_id = tree.create("checker-1", "检查邮件内容", parent_id=exec_id,
                           capabilities=["read", "validate"])
    print(f"✅ 创建检查者 fiber: id={check_id}, capabilities={['read', 'validate']}")

    # 提交检查者
    tree.commit(check_id)
    print(f"✅ 提交检查者 fiber: id={check_id}")

    # 提交执行者
    tree.commit(exec_id)
    print(f"✅ 提交执行者 fiber: id={exec_id}")

    # 查看 fiber 树
    fibers = tree.get_all_fibers()
    print(f"📊 Fiber 树: {len(fibers)} 个节点")
    for fid, f in fibers.items():
        print(f"   id={fid}, status={f['status']}, parent={f['parent_id']}, desc={f['description']}")

    # ── 示例 2: 级联回滚 ──
    print("\n" + "=" * 60)
    print("示例 2: 级联回滚 (fail)")
    print("=" * 60)

    tree2 = FiberTree()

    root = tree2.create("coordinator", "发送报告")
    exec1 = tree2.create("executor", "生成报告", parent_id=root)
    check1 = tree2.create("checker", "检查报告", parent_id=exec1, capabilities=["read", "validate"])

    tree2.register(exec1, "写入文件", lambda: print("  ↪ 撤销: 写入文件"))
    tree2.register(exec1, "发送通知", lambda: print("  ↪ 撤销: 发送通知"))

    # 检查者不通过 → 触发执行者级联回滚
    print("🔴 检查者不通过，触发级联回滚:")
    ok, ops = tree2.fail(check1, cascade=True)
    print(f"   回滚结果: ok={ok}, 操作数={len(ops)}")
    for op in ops:
        print(f"   {op}")

    # 验证状态
    for fid in [root, exec1, check1]:
        f = tree2.get_fiber(fid)
        print(f"   fiber {fid} ({f['description']}): status={f['status']}")

    # ── 示例 3: 全局去重 ──
    print("\n" + "=" * 60)
    print("示例 3: 全局去重")
    print("=" * 60)

    tree3 = FiberTree()
    fid = tree3.create("agent", "测试去重")

    # 第一次调用
    tree3.global_call_add("plugin-send-email", "hash123", fid, "邮件已发送")
    print(f"📝 注册全局调用: plugin-send-email, hash123")

    # 查找 - 应该命中
    entry = tree3.global_call_lookup("plugin-send-email", "hash123")
    print(f"🔍 查找 (hash123): {'✅ 命中' if entry else '❌ 未命中'}")

    # 查找不同的 hash - 应该未命中
    entry = tree3.global_call_lookup("plugin-send-email", "hash456")
    print(f"🔍 查找 (hash456): {'✅ 命中' if entry else '❌ 未命中'}")

    # ── 示例 4: 全局 undo 栈 ──
    print("\n" + "=" * 60)
    print("示例 4: 全局 undo 栈")
    print("=" * 60)

    tree4 = FiberTree()
    tree4.storage.push_undo_stack("创建文件", lambda: print("  ↪ 撤销: 创建文件"))
    tree4.storage.push_undo_stack("修改配置", lambda: print("  ↪ 撤销: 修改配置"))

    print(f"📋 undo 栈: {tree4.undo_list()}")

    ok, msg = tree4.undo_pop()
    print(f"↩️  撤销: ok={ok}, msg={msg}")

    print(f"📋 undo 栈剩余: {tree4.undo_list()}")

    # ── 示例 5: SQLite 存储 ──
    print("\n" + "=" * 60)
    print("示例 5: SQLite 持久化存储")
    print("=" * 60)

    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        storage = SQLiteStorage(db_path)
        tree5 = FiberTree(storage=storage)

        fid = tree5.create("agent-sqlite", "持久化测试")
        print(f"✅ 创建 fiber (SQLite): id={fid}")

        tree5.commit(fid)
        print(f"✅ 提交 fiber (SQLite): id={fid}")

        # 重新读取
        f = tree5.get_fiber(fid)
        print(f"🔍 读取 fiber: id={f['id']}, status={f['status']}")
    finally:
        os.unlink(db_path)

    print("\n✅ FiberTree 示例完成")


if __name__ == "__main__":
    main()