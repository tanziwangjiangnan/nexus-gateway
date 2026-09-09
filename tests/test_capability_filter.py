"""能力标签匹配单元测试"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from provider_router.assessor import (
    capability_dimensions, extract_query_capabilities,
    cosine_similarity, filter_providers_by_capability,
    load_provider_capabilities,
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

if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for t in tests:
        t()
    print(f"✅ 全部 {len(tests)} 个测试通过")