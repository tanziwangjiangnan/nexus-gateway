"""复杂层两条路径 — 任务拆解与角色分配（纯规则，0 模型调用）

核心规则：路由层能提前拆解任务 → 角色路径；不能提前拆解 → 智能体路径。

本模块只做纯逻辑（拆解/角色分配/能力向量），不发网络请求；
实际模型调用由调用方（api/app.py）负责，便于单测。
"""
import re

# 顺序连接词 — 用于把任务切分为有前后依赖的子任务
_SEQUENTIAL_MARKERS = [
    "首先", "其次", "然后", "接着", "之后", "最后", "下一步",
    "先", "再", "第一步", "第二步", "第三步", "第四步",
]

# 角色 → 能力维度（与 assessor.capability_dimensions 对齐）
ROLE_CAPABILITY = {
    "planner": "reasoning",     # 规划者：拆解/推理
    "executor": "code",         # 执行者：实现/操作
    "reviewer": "analysis",     # 审查者：验证/评估
    "summarizer": "summary",    # 汇总者：整合结论
    "translator": "creative",   # 译者/文案：语言转换
    "checker": "analysis",      # 检查者：排查诊断
    "asker": "general",         # 追问者：对话澄清
}

# 动词/关键词 → 能力维度（用于给子任务定角色）
_CAPABILITY_KEYWORDS = {
    "code": ["实现", "代码", "编写", "写", "编程", "脚本", "修复", "重构", "函数", "接口"],
    "analysis": ["分析", "评估", "审查", "检查", "排查", "诊断", "验证", "对比", "比较"],
    "summary": ["总结", "汇总", "归纳", "提炼", "概括", "整合", "报告"],
    "creative": ["翻译", "创作", "文案", "故事", "设计", "润色", "改写"],
    "reasoning": ["规划", "拆解", "推导", "推理", "论证", "设计"],
    "general": ["追问", "询问", "确认", "澄清"],
}

# 能力维度 → 角色（_CAPABILITY_KEYWORDS 的逆映射，取首选角色）
_CAPABILITY_ROLE = {
    "code": "executor",
    "analysis": "reviewer",
    "summary": "summarizer",
    "creative": "translator",
    "reasoning": "planner",
    "general": "asker",
}


def _split_subtasks(text: str) -> list:
    """按顺序连接词切分文本为子任务段落。

    返回去除空白的段落列表；若切不出 ≥2 段则返回空列表（视为不可提前拆解）。
    """
    if not text:
        return []
    pattern = "|".join(re.escape(m) for m in _SEQUENTIAL_MARKERS)
    # 在连接词前插入分隔符，保留连接词本身
    marked = re.sub(f"({pattern})", r"\n\1", text)
    parts = [p.strip() for p in marked.split("\n") if p.strip()]
    # 过滤掉纯连接词段落（如只有"然后"）
    pure_markers = set(_SEQUENTIAL_MARKERS)
    parts = [p for p in parts if p not in pure_markers]
    return parts if len(parts) >= 2 else []


def _classify_capability(segment: str) -> str:
    """判断子任务需要的能力维度（命中最多者胜，无命中返回 general）。"""
    scores = {}
    for dim, kws in _CAPABILITY_KEYWORDS.items():
        score = sum(1 for kw in kws if kw in segment)
        if score > 0:
            scores[dim] = score
    if not scores:
        return "general"
    return max(scores, key=scores.get)


def decompose_task(text: str) -> list:
    """把任务拆解为 [{id, role, task, capability}, ...]（纯规则）。

    切不出多个子任务时返回空列表 → 调用方应走智能体路径。
    """
    segments = _split_subtasks(text)
    if not segments:
        return []
    subtasks = []
    for i, seg in enumerate(segments, start=1):
        cap = _classify_capability(seg)
        subtasks.append({
            "id": i,
            "role": _CAPABILITY_ROLE.get(cap, "executor"),
            "task": seg,
            "capability": cap,
        })
    return subtasks


def role_capability_vector(role: str, cfg: dict = None) -> dict:
    """角色 → 能力需求向量（供能力标签匹配过滤 provider）。"""
    from provider_router.assessor import capability_dimensions
    dims = capability_dimensions(cfg)
    vec = {d: 0.0 for d in dims}
    dim = ROLE_CAPABILITY.get(role)
    if dim and dim in vec:
        vec[dim] = 0.8
    return vec


def build_subtask_messages(subtask: dict, original_messages: list) -> list:
    """为子任务构造独立的 messages（保留原始上下文，附加角色指令）。

    首条 system 保留原请求背景；子任务追加为新的 user 消息。
    """
    msgs = []
    for m in original_messages:
        if isinstance(m, dict) and m.get("role") == "system":
            msgs.append(m)
    role_hint = {
        "planner": "你是规划者，负责拆解与推理。",
        "executor": "你是执行者，负责具体实现。",
        "reviewer": "你是审查者，负责验证与评估。",
        "summarizer": "你是汇总者，负责整合结论。",
        "translator": "你是译者，负责语言转换。",
        "checker": "你是检查者，负责排查与诊断。",
        "asker": "你是追问者，负责澄清缺失信息。",
    }.get(subtask.get("role"), "")
    msgs.append({
        "role": "user",
        "content": f"[角色: {subtask.get('role')}] {role_hint}\n\n子任务：{subtask.get('task')}",
    })
    return msgs


def build_aggregate_messages(subtasks: list, results: list, original_messages: list) -> list:
    """构造聚合请求：把各子任务结果交给汇总者整合。"""
    msgs = []
    for m in original_messages:
        if isinstance(m, dict) and m.get("role") == "system":
            msgs.append(m)
    blocks = []
    for st, res in zip(subtasks, results):
        blocks.append(f"【{st.get('role')}】{st.get('task')}\n结果：{res}")
    msgs.append({
        "role": "user",
        "content": "以下是各子任务的执行结果，请整合成最终答复：\n\n" + "\n\n".join(blocks),
    })
    return msgs
