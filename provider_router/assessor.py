"""前置检查层 — 复杂度感知路由

并行双分支：
  分支1: Token 概率检测（轻量模型调用, max_tokens=1, 看 logprobs 确定度）
  分支2: 复杂度评估（纯规则, 0 模型调用, <5ms）

合并输出导航信息供元决策层参考，不诱导后续模型。
"""

import re
import math
import json
from typing import Optional

import httpx


# ── 复杂度等级 ──
LEVELS = ["trivial", "simple", "medium", "complex", "very_complex"]


def complexity_assess(text: str) -> dict:
    """纯规则复杂度评估，0 模型调用，<5ms。

    评估维度：
    - 文本长度 → 等级
    - 结构索引（段落/列表/表格/代码块）
    - 任务类型关键词匹配
    """
    length = len(text)

    # 1. 长度分级
    if length < 5:
        level = "trivial"
    elif length < 200:
        level = "simple"
    elif length < 2000:
        level = "medium"
    elif length < 5000:
        level = "complex"
    else:
        level = "very_complex"

    # 2. 结构检测
    paragraphs = [p.strip() for p in text.split('\n') if p.strip()]
    para_count = len(paragraphs)
    has_lists = bool(re.search(r'^[-\*\d+\.]\s', text, re.MULTILINE))
    has_tables = bool(re.search(r'\|.+\|\n\|[-| :]+\|', text))
    has_code_blocks = bool(re.search(r'```', text))

    # 3. 任务类型关键词匹配
    task_type = _detect_task_type(text)

    # 4. 段落结构索引
    sections = _build_section_index(text, paragraphs)

    return {
        "level": level,
        "length": length,
        "paragraph_count": para_count,
        "has_lists": has_lists,
        "has_tables": has_tables,
        "has_code_blocks": has_code_blocks,
        "task_type": task_type,
        "sections": sections,
    }


async def token_confidence(text: str, api: str, api_key: str, model: str,
                           timeout_ms: int = 3000) -> dict:
    """Token 概率检测 — 调用轻量模型，max_tokens=1，解析 logprobs 确定度。

    参数:
        text: 用户消息文本（前 200 字符足够）
        api: provider API 基础 URL
        api_key: provider API key
        model: 模型名
        timeout_ms: 探测超时毫秒数（短超时避免拖慢请求）

    返回:
        { "confidence": "high"|"low"|"unknown",
          "top_prob": float,   # top token 概率
          "gap": float,         # top 与 second 概率差
          "detail": str }       # 说明
    """
    if not text or not api or not api_key:
        return {"confidence": "unknown", "detail": "missing_config"}

    prompt = text[:200]  # 截短，够用

    try:
        async with httpx.AsyncClient(timeout=timeout_ms / 1000.0) as client:
            resp = await client.post(
                f"{api.rstrip('/')}/chat/completions",
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 1,
                    "temperature": 0,
                    "logprobs": True,
                    "top_logprobs": 3,
                },
                headers={"Authorization": f"Bearer {api_key}"},
            )
            data = resp.json()
            choice = data.get("choices", [{}])[0]
            logprobs_data = choice.get("logprobs")

            if logprobs_data and "content" in logprobs_data and logprobs_data["content"]:
                top_tokens = logprobs_data["content"][0]
                top_logprob = top_tokens.get("logprob", -1)

                if top_logprob > -20:  # 合理范围
                    top_prob = math.exp(top_logprob)
                else:
                    top_prob = 0.0

                candidates = top_tokens.get("top_logprobs", [])
                if len(candidates) >= 2:
                    second_prob = math.exp(candidates[1].get("logprob", -99))
                    gap = top_prob - second_prob
                else:
                    second_prob = 0.0
                    gap = top_prob

                confidence = "high" if top_prob > 0.7 else "low"
                return {
                    "confidence": confidence,
                    "top_prob": round(top_prob, 4),
                    "gap": round(gap, 4),
                    "detail": f"top_prob={top_prob:.3f}, gap={gap:.3f}",
                }
            else:
                return {"confidence": "unknown", "detail": "no_logprobs"}
    except httpx.TimeoutException:
        return {"confidence": "unknown", "detail": "timeout"}
    except Exception as e:
        return {"confidence": "unknown", "detail": str(e)[:60]}


# ── 路径选择 ──

def select_path(pre_check: dict, routing_rules: dict) -> dict:
    """根据前置检查结果 + 规则表 选择执行路径。

    routing_rules 示例:
        trivial:    { action: "direct_return", message: "..." }
        simple_high_confidence: { pool: "pool_a" }
        simple_low_confidence:  { pool: "pool_b", validation: "light" }
        medium:     { pool: "pool_b", supervisor: true }
        complex:    { pool: "pool_c", supervisor: true }
        very_complex: { pool: "fiber_split", segments: "auto" }
    """
    level = pre_check.get("complexity", {}).get("level", "medium")
    confidence = pre_check.get("confidence", {}).get("confidence", "unknown")
    rules = routing_rules or {}

    # 规则匹配优先级：精确匹配（simple_high_confidence）→ 降级匹配（simple/medium/...）
    key = f"{level}_{confidence}_confidence" if confidence in ("high", "low") else level
    rule = rules.get(key) or rules.get(level)

    if not rule:
        # 兜底：中等复杂度走池B
        rule = {"pool": "pool_b", "supervisor": True}

    return rule


# ── 内部辅助 ──

def _detect_task_type(text: str) -> str:
    """关键词匹配任务类型"""
    text_lower = text.lower()
    patterns = {
        "doc_generation": ["生成", "写", "创作", "编写", "撰写", "文档"],
        "code": ["代码", "函数", "实现", "class ", "def ", "import "],
        "analysis": ["分析", "总结", "归纳", "提炼", "对比", "比较"],
        "translation": ["翻译", "译成", "translate"],
        "qa": ["什么", "为什么", "如何", "怎么", "是否", "多少"],
        "creative": ["故事", "诗歌", "小说", "脚本", "剧本", "创意"],
    }
    scores = {}
    for ttype, keywords in patterns.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > 0:
            scores[ttype] = score
    if not scores:
        return "general"
    return max(scores, key=scores.get)


def _build_section_index(text: str, paragraphs: list) -> list:
    """构建段落结构索引 — 检测标题行"""
    sections = []
    pos = 0
    for para in paragraphs:
        start = text.find(para, pos)
        if start >= 0:
            is_heading = (para.startswith('#') or para.startswith('##')
                          or bool(re.match(r'^[第—二三四五六七八九十\d]+[、\.\s]', para)))
            if is_heading:
                sections.append({
                    "title": para[:60],
                    "start": start,
                    "end": start + len(para),
                    "type": "heading",
                })
            pos = start + len(para)
    return sections


# ── 能力标签匹配（前置过滤） ──
# 核心原则：标签匹配做过滤（能不能做），权重体系做排序（谁做得更好）。
# 查询能力向量 → 与 provider capability_profile 余弦相似度 ≥ 阈值 才进入候选池。

# 查询侧关键词 → 能力维度（复用 _detect_task_type 的部分模式，独立定义以便扩展）
_QUERY_CAP_KEYWORDS = {
    "code": ["写", "实现", "代码", "function", "class", "def ", "import", "编程", "脚本", "算法", "bug", "调试"],
    "summary": ["总结", "概述", "提炼", "概括", "归纳", "摘要", "一句话"],
    "analysis": ["分析", "评估", "判断", "推理", "比较", "对比", "评判", "诊断"],
    "creative": ["创意", "故事", "诗", "诗歌", "构思", "设计", "风格", "文案", "剧本"],
    "reasoning": ["推理", "推导", "证明", "解释", "为什么", "如何", "逻辑", "所以"],
    "general": ["你好", "hi", "hello", "随便", "聊天"],
}


def capability_dimensions(cfg: dict = None) -> list:
    """能力维度定义：优先取 YAML 的 capability_dimensions，否则用内置默认。"""
    dims = []
    if cfg:
        dims = (cfg.get("capability_dimensions") or [])
    return [d for d in (dims or ["code", "summary", "analysis", "creative", "reasoning", "general"]) if d]


def extract_query_capabilities(query_text: str, cfg: dict = None) -> dict:
    """从查询文本提取能力需求向量（维度为 capability_dimensions，未命中维度取 0）。

    命中关键词的维度置 0.8；general 仅在命中问候词时升 0.8，否则为 0。
    全 0 向量 = 无明确能力需求 → 调用方应跳过能力过滤（纯权重调度）。
    """
    dims = capability_dimensions(cfg)
    caps = {d: 0.0 for d in dims}
    q = (query_text or "").lower()
    for dim, keywords in _QUERY_CAP_KEYWORDS.items():
        if dim in caps and any(k in q for k in keywords):
            caps[dim] = 0.8
    return caps


def load_provider_capabilities(provider_cfg: dict, cfg: dict = None) -> dict:
    """从 provider 配置加载 capability_profile，缺失维度补默认值 0.5。

    与计划书一致：未配置的维度按 0.5 温和对待，避免全 0 向量被全部过滤。
    """
    dims = capability_dimensions(cfg)
    profile = dict(provider_cfg.get("capability_profile") or {})
    return {d: profile.get(d, 0.5) for d in dims}


def cosine_similarity(vec_a: dict, vec_b: dict) -> float:
    """两个能力向量的余弦相似度（0~1）。向量维度以 vec_a 为准；缺失维度按 0 计。"""
    if not vec_a or not vec_b:
        return 0.0
    dot = sum(vec_a.get(k, 0.0) * vec_b.get(k, 0.0) for k in vec_a)
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(vec_b.get(k, 0.0) ** 2 for k in vec_a))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def filter_providers_by_capability(providers: list, query_caps: dict,
                                   threshold: float = 0.3) -> list:
    """按能力阈值过滤 Provider，返回 [(provider, score), ...]（保持候选顺序）。

    未配置 capability_profile 的 provider 视为"通用型"，余弦=0 时仍给予 0.3 门槛豁免
    （即与通用 query 等价对待——不因缺配置而全灭）。threshold<=0 时不做过滤。
    """
    if threshold is None:
        threshold = 0.3
    if threshold <= 0:
        return [(p, 0.5) for p in providers]
    qualified = []
    for provider in providers:
        profile = provider.get("capability_profile") or {}
        if not profile:
            # 缺配置：不假设能力，但也不因缺配置全灭 → 给固定 0.3 分数再比阈值
            score = 0.3
        else:
            score = cosine_similarity(query_caps, profile)
        if score >= threshold:
            qualified.append((provider, score))
    return qualified