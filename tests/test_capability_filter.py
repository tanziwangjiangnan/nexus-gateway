"""能力标签匹配 + 复杂层两条路径 单元测试"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router.assessor import (
    capability_dimensions, extract_query_capabilities,
    cosine_similarity, filter_providers_by_capability,
    load_provider_capabilities, can_decompose, select_path,
)

dims = ["code", "summary", "analysis", "creative", "reasoning", "general"]

def test_capability_dimensions_default():
    d = capability_dimensions()
    assert d == dims

def test_capability_dimensions_from_cfg():
    d = capability_dimensions({"capability_dimensions": ["code", "creative"]})
    assert d == ["code", "creative"]

def test_code_query():
    caps = extract_query_capabilities("帮我写一个排序算法")
    assert caps["code"] == 0.8
    assert caps["summary"] == 0.0

def test_summary_query():
    caps = extract_query_capabilities("总结一下这篇文章")
    assert caps["summary"] == 0.8

def test_analysis_query():
    caps = extract_query_capabilities("分析这两个方案的优劣")
    assert caps["analysis"] == 0.8

def test_greeting_query():
    caps = extract_query_capabilities("你好，请问今天天气怎么样？")
    assert caps["general"] == 0.8
    assert caps["code"] == 0.0

def test_empty_query():
    caps = extract_query_capabilities("")
    assert all(v == 0.0 for v in caps.values())

def test_cosine_identical():
    a = {"code": 0.9, "summary": 0.3}
    b = {"code": 0.9, "summary": 0.3}
    assert abs(cosine_similarity(a, b) - 1.0) < 0.001

def test_cosine_orthogonal():
    a = {"code": 0.9, "summary": 0.0}
    b = {"code": 0.0, "summary": 0.9}
    assert abs(cosine_similarity(a, b) - 0.0) < 0.001

def test_cosine_zero():
    assert cosine_similarity({"code": 0.0}, {"code": 0.9}) == 0.0
    assert cosine_similarity({}, {}) == 0.0

_PROVIDERS = [
    {"name": "scnet-tp", "capability_profile": {"code": 0.9, "summary": 0.3, "analysis": 0.7, "creative": 0.2, "reasoning": 0.8, "general": 0.6}},
    {"name": "xiaomi", "capability_profile": {"code": 0.4, "summary": 0.8, "analysis": 0.3, "creative": 0.7, "reasoning": 0.4, "general": 0.7}},
    {"name": "qfg-new", "capability_profile": {"code": 0.9, "summary": 0.6, "analysis": 0.8, "creative": 0.8, "reasoning": 0.9, "general": 0.8}},
    {"name": "deepseek-direct", "capability_profile": {"code": 0.7, "summary": 0.5, "analysis": 0.6, "creative": 0.3, "reasoning": 0.7, "general": 0.8}},
]

def test_filter_creative():
    """创意任务：scnet-tp (creative=0.2) 被过滤"""
    caps = {d: 0.0 for d in dims}; caps["creative"] = 0.8
    q = filter_providers_by_capability(_PROVIDERS, caps, 0.3)
    names = [p["name"] for p, _ in q]
    assert "scnet-tp" not in names
    assert "xiaomi" in names

def test_filter_code_scores():
    """代码任务：xiaomi 被过滤（code=0.4 低于阈值 0.3）"""
    caps = {d: 0.0 for d in dims}; caps["code"] = 0.8
    q = filter_providers_by_capability(_PROVIDERS, caps, 0.3)
    names = [p["name"] for p, _ in q]
    assert "xiaomi" not in names, f"xiaomi should be filtered for code task: {names}"
    assert "scnet-tp" in names
    assert "qfg-new" in names

def test_filter_without_profile():
    q = filter_providers_by_capability([{"name": "x"}], {"code": 0.8}, 0.3)
    assert len(q) == 1

def test_filter_threshold_zero():
    q = filter_providers_by_capability(_PROVIDERS, {d: 0.0 for d in dims}, 0.0)
    assert len(q) == 4

def test_load_provider_capabilities_defaults():
    loaded = load_provider_capabilities({})
    assert all(v == 0.5 for v in loaded.values())


# ── can_decompose：复杂层两条路径判断 ──

def test_can_decompose_explicit_subgoals():
    """有明确子目标 + 多能力需求 → 可拆解"""
    r = can_decompose("先分析数据，再写报告，最后翻译成英文")
    assert r["can"] is True
    assert r["details"]["has_explicit_subgoals"] is True
    assert r["details"]["has_explicit_capability_needs"] is True

def test_can_decompose_vague_task_not_decomposable():
    """模糊探索性任务 → 不可拆解（走智能体路径）"""
    r = can_decompose("帮我修复这个项目的bug")
    assert r["can"] is False

def test_can_decompose_empty():
    r = can_decompose("")
    assert r["can"] is False
    assert r["reason"] == "empty_text"

def test_can_decompose_single_verb_not_enough():
    """单个动词，无子目标标记 → 不可拆解"""
    r = can_decompose("分析一下这个方案")
    assert r["can"] is False

def test_can_decompose_reason_populated():
    r = can_decompose("先分析，再总结")
    assert r["can"] is True
    assert "明确子目标" in r["reason"]


# ── select_path：very_complex 分流到两条路径 ──

def test_select_path_very_complex_role_based():
    rules = {"very_complex": {"pool": "pool_c"}}
    rule = select_path({"complexity": {"level": "very_complex"}}, rules,
                       text="先分析数据，再写报告，最后翻译")
    assert rule["path"] == "role_based"
    assert rule["pool"] == "fiber_split"

def test_select_path_very_complex_agent_based():
    rules = {"very_complex": {"pool": "pool_c"}}
    rule = select_path({"complexity": {"level": "very_complex"}}, rules,
                       text="帮我修复这个项目的bug")
    assert rule["path"] == "agent_based"
    assert rule["pool"] == "fiber_split"

def test_select_path_simple_unchanged():
    """非 very_complex 不受新逻辑影响（向后兼容）"""
    rules = {"simple": {"pool": "pool_a"}}
    rule = select_path({"complexity": {"level": "simple"}}, rules, text="先分析再总结")
    assert rule["pool"] == "pool_a"
    assert "path" not in rule

def test_select_path_no_text_backward_compatible():
    """不传 text 时保持旧行为"""
    rules = {"very_complex": {"pool": "pool_c"}}
    rule = select_path({"complexity": {"level": "very_complex"}}, rules)
    assert rule["pool"] == "pool_c"

def test_select_path_fallback_rule():
    rule = select_path({"complexity": {"level": "medium"}}, {})
    assert rule["pool"] == "pool_b"

if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
    print(f"✅ 全部 {len(tests)} 个测试通过")