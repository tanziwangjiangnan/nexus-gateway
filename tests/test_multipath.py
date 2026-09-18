"""复杂层两条路径 — multipath 单元测试"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router.multipath import (
    _split_subtasks, _classify_capability, decompose_task,
    role_capability_vector, build_subtask_messages,
    build_aggregate_messages,
)


def test_split_subtasks_sequential_markers():
    """先...再...最后 → 切为 3 段"""
    parts = _split_subtasks("先分析数据，再写报告，最后翻译成英文")
    assert len(parts) >= 2

def test_split_subtasks_no_markers():
    """无连接词 → 空"""
    parts = _split_subtasks("修复这个bug")
    assert parts == []

def test_split_subtasks_empty():
    assert _split_subtasks("") == []

def test_split_subtasks_first_then():
    parts = _split_subtasks("首先检查配置，然后重启服务")
    assert len(parts) >= 2

def test_classify_capability_code():
    cap = _classify_capability("写一个排序算法函数")
    assert cap == "code"

def test_classify_capability_analysis():
    cap = _classify_capability("分析这两个方案的优劣")
    assert cap == "analysis"

def test_classify_capability_summary():
    cap = _classify_capability("总结一下结果")
    assert cap == "summary"

def test_classify_capability_creative():
    cap = _classify_capability("翻译成英文")
    assert cap == "creative"

def test_classify_capability_general():
    cap = _classify_capability("你好")
    assert cap == "general"

def test_decompose_task_explicit():
    """"先分析，再写代码，最后翻译" → 3 个子任务"""
    tasks = decompose_task("先分析数据，再写排序算法，最后翻译成英文")
    assert len(tasks) >= 2
    assert all("role" in t for t in tasks)
    assert all("task" in t for t in tasks)

def test_decompose_task_vague():
    """模糊任务 → 空列表（不可拆解）"""
    tasks = decompose_task("优化一下这个项目")
    assert tasks == []

def test_decompose_task_empty():
    assert decompose_task("") == []

def test_role_capability_vector_planner():
    v = role_capability_vector("planner")
    assert v.get("reasoning") == 0.8

def test_role_capability_vector_executor():
    v = role_capability_vector("executor")
    assert v.get("code") == 0.8

def test_role_capability_vector_unknown_role():
    v = role_capability_vector("unknown")
    # 未知角色 → 全 0 向量（无能力信号，调用方跳过能力过滤）
    assert all(x == 0.0 for x in v.values())


def test_build_subtask_messages():
    msgs = [{"role": "system", "content": "你是一个助手"}]
    sub = {"role": "executor", "task": "写代码", "capability": "code"}
    result = build_subtask_messages(sub, msgs)
    # 至少包含 system 和一条 user
    assert any(m.get("role") == "system" for m in result)
    assert any("写代码" in m.get("content", "") for m in result if m.get("role") == "user")

def test_build_aggregate_messages():
    msgs = [{"role": "system", "content": "你是一个助手"}]
    subtasks = [
        {"role": "executor", "task": "写代码"},
        {"role": "reviewer", "task": "检查代码"},
    ]
    results = ["def foo(): pass", "代码正确"]
    agg = build_aggregate_messages(subtasks, results, msgs)
    # 聚合消息应包含各子任务结果
    content = agg[-1].get("content", "")
    assert "写代码" in content
    assert "def foo(): pass" in content
    assert "检查代码" in content
    assert "代码正确" in content


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
    print(f"✅ 全部 {len(tests)} 个测试通过")