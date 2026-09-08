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