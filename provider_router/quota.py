"""模型额度感知与任务保护 — 额度状态与候选过滤（纯逻辑，0 模型调用）

见 计划书-模型额度感知与任务保护-v1。核心：区分「服务不可达」与「额度耗尽」，
在请求开始前及多步任务每一步前剔除已耗尽的 provider。

本模块只做纯逻辑（错误分类 / 预算等级 / 候选过滤），不发网络请求；
状态读写通过注入的 sqlite 连接完成。
"""
import re

# ── 额度状态枚举 ──
UNKNOWN = "unknown"          # 尚未取得余额数据 → 可调用
AVAILABLE = "available"      # 明确可用 → 正常参与
LOW = "low"                  # 余额低于预警线 → 仅接轻量请求
EXHAUSTED = "exhausted"      # 明确余额不足 → 不选择
UNAVAILABLE = "unavailable"  # 连通性/鉴权/服务异常 → 交给熔断

# 「不选择」与「仅接轻量」两组
_BLOCKING = (EXHAUSTED,)
_LIGHT_ONLY = (LOW,)

# ── 预算等级 ──
BUDGET_SMALL = "small"
BUDGET_NORMAL = "normal"
BUDGET_LARGE = "large"

_DEFAULT_BUDGET_THRESHOLDS = {"small": 512, "large": 4096}

# ── 额度耗尽的关键词（小写匹配）──
DEFAULT_EXHAUSTED_KEYWORDS = [
    "insufficient balance",
    "insufficient_balance",
    "insufficient quota",
    "insufficient_quota",
    "exceeded your current quota",
    "quota exceeded",
    "balance not enough",
    "余额不足",
    "额度不足",
    "额度已用尽",
    "账户欠费",
]

# 「429 但非额度」——限流，交给熔断/限流，不算额度耗尽
_RATE_LIMIT_KEYWORDS = [
    "rate limit",
    "rate_limit",
    "too many requests",
    "requests per",
    "请求过于频繁",
]


def classify_error(http_status: int, body: str, cfg: dict = None) -> dict:
    """把一次失败响应归类为额度事件或普通故障。

    返回 {kind, status, detail}：
    - kind="exhausted"  → 明确额度不足
    - kind="rate_limit" → 普通 429 限流（非额度）
    - kind="unavailable"→ 连通性/鉴权/服务异常
    - kind="other"      → 其他
    """
    text = (body or "")
    text_lower = text.lower()
    keywords = _exhausted_keywords(cfg)

    # 1. 已配置关键词优先
    for kw in keywords:
        if kw.lower() in text_lower:
            return {"kind": "exhausted", "status": http_status,
                    "detail": f"keyword:{kw}"}

    # 2. 402 Payment Required → 额度
    if http_status == 402:
        return {"kind": "exhausted", "status": http_status, "detail": "http 402"}

    # 3. 429：区分限流与额度
    if http_status == 429:
        for kw in _RATE_LIMIT_KEYWORDS:
            if kw in text_lower:
                return {"kind": "rate_limit", "status": http_status,
                        "detail": f"keyword:{kw}"}
        return {"kind": "exhausted", "status": http_status, "detail": "http 429"}

    # 4. 鉴权/服务异常
    if http_status in (401, 403):
        return {"kind": "unavailable", "status": http_status, "detail": "auth"}
    if http_status >= 500:
        return {"kind": "unavailable", "status": http_status, "detail": "upstream"}

    return {"kind": "other", "status": http_status, "detail": ""}


def _exhausted_keywords(cfg: dict) -> list:
    """配置关键词 **合并** 内置默认词（配置不再整体覆盖默认）。

    [2026-09-18] 起因：gateway.yaml 里只写了 "insufficient_quota"（下划线），
    而 kouri 实际返回 "Insufficient quota."（空格）-> 未被识别为额度耗尽，
    于是既没写 exhausted 状态、又按 403「鉴权错误」透传客户端。
    """
    raw = ((cfg or {}).get("quota_guard", {}) or {}).get("exhausted_keywords")
    out = [str(k) for k in DEFAULT_EXHAUSTED_KEYWORDS]
    lower = [x.lower() for x in out]
    if isinstance(raw, list):
        for k in raw:
            s = str(k)
            if s and s.lower() not in lower:
                out.append(s)
                lower.append(s.lower())
    return out


def budget_level(max_tokens, cfg: dict = None) -> str:
    """按 max_tokens 计算预算等级 small/normal/large。"""
    raw = ((cfg or {}).get("quota_guard", {}) or {}).get("budget_thresholds") or {}
    small = raw.get("small", _DEFAULT_BUDGET_THRESHOLDS["small"])
    large = raw.get("large", _DEFAULT_BUDGET_THRESHOLDS["large"])
    if max_tokens is None:
        return BUDGET_NORMAL
    try:
        n = int(max_tokens)
    except (TypeError, ValueError):
        return BUDGET_NORMAL
    if n <= small:
        return BUDGET_SMALL
    if n >= large:
        return BUDGET_LARGE
    return BUDGET_NORMAL


# ── 状态读写（注入 sqlite 连接）──

def get_status(conn, provider: str) -> dict:
    """读取单个 provider 的额度状态；无记录返回 unknown。"""
    row = conn.execute(
        "SELECT provider, status, reason, source, last_checked_at, cooldown_until "
        "FROM provider_quota WHERE provider=?", (provider,)).fetchone()
    if not row:
        return {"provider": provider, "status": UNKNOWN, "reason": "",
                "source": "", "last_checked_at": None, "cooldown_until": None}
    return dict(row)


def get_statuses(conn, providers=None) -> dict:
    """批量读取额度状态 {provider: status_dict}。"""
    if providers:
        out = {}
        for p in providers:
            out[p] = get_status(conn, p)
        return out
    rows = conn.execute(
        "SELECT provider, status, reason, source, last_checked_at, cooldown_until "
        "FROM provider_quota").fetchall()
    return {r["provider"]: dict(r) for r in rows}


def set_status(conn, provider: str, status: str, reason: str = "",
               source: str = "auto", cooldown_seconds: int = None) -> None:
    """写入额度状态 + 事件留痕。

    cooldown_seconds 仅对 exhausted 有意义：冷却期内不重新探测，到期后自动恢复。
    """
    cooldown_expr = "NULL"
    params_cooldown = None
    if cooldown_seconds and status == EXHAUSTED:
        cooldown_expr = "datetime('now', ?)"
        params_cooldown = f"+{int(cooldown_seconds)} seconds"

    conn.execute(
        f"""INSERT INTO provider_quota
            (provider, status, reason, source, last_checked_at, cooldown_until, updated_at)
            VALUES (?,?,?,?,datetime('now'),{cooldown_expr},datetime('now'))
            ON CONFLICT(provider) DO UPDATE SET
                status=excluded.status, reason=excluded.reason, source=excluded.source,
                last_checked_at=excluded.last_checked_at,
                cooldown_until=excluded.cooldown_until,
                updated_at=datetime('now')""",
        (provider, status, reason, source) + ((params_cooldown,) if params_cooldown else ()))
    conn.execute(
        "INSERT INTO provider_events (provider, event_type, status, http_status, detail) "
        "VALUES (?,?,?,?,?)",
        (provider, f"status:{status}", status, None, reason))
    conn.commit()


def record_error(conn, provider: str, cls: dict, cfg: dict = None) -> str:
    """把分类结果落到状态表，返回施加的状态。

    - exhausted  → set exhausted + 冷却
    - rate_limit → 只记事件，不动额度状态（交给现有熔断/限流）
    - unavailable→ 交给现有熔断，不动额度状态
    """
    kind = cls.get("kind")
    cooldown = int((((cfg or {}).get("quota_guard", {}) or {})
                    .get("cooldown_seconds", 600)))
    if kind == "exhausted":
        set_status(conn, provider, EXHAUSTED, cls.get("detail", ""), "auto", cooldown)
        return EXHAUSTED
    # 其他只留事件，不改额度状态
    conn.execute(
        "INSERT INTO provider_events (provider, event_type, status, http_status, detail) "
        "VALUES (?,?,?,?,?)",
        (provider, f"error:{kind}", None, cls.get("status"),
         cls.get("detail", "")))
    conn.commit()
    return UNKNOWN


def recover_due(conn) -> list:
    """冷却到期的 exhausted → 复活为 unknown（待重新探测）。返回复活的 provider 列表。"""
    rows = conn.execute(
        "SELECT provider FROM provider_quota "
        "WHERE status=? AND cooldown_until IS NOT NULL "
        "AND cooldown_until <= datetime('now')", (EXHAUSTED,)).fetchall()
    names = [r["provider"] for r in rows]
    for n in names:
        conn.execute(
            "UPDATE provider_quota SET status=?, reason='cooldown expired', "
            "cooldown_until=NULL, source='auto', updated_at=datetime('now') "
            "WHERE provider=?", (UNKNOWN, n))
        conn.execute(
            "INSERT INTO provider_events (provider, event_type, status, detail) "
            "VALUES (?,?,?,?)", (n, "recover", UNKNOWN, "cooldown expired"))
    if names:
        conn.commit()
    return names


def filter_candidates(providers: list, statuses: dict, budget: str,
                      cfg: dict = None) -> tuple:
    """按额度状态过滤候选 provider。

    返回 (allowed, blocked)：
    - blocked = [(provider_name, reason)]  被剔除
    - allowed = 通过过滤的 provider 列表（保持原顺序）
    - exhausted → 剔除
    - low + large 预算 → 剔除
    - unknown/available → 放行
    """
    enable = ((cfg or {}).get("quota_guard", {}) or {}).get("enabled", True)
    allowed, blocked = [], []
    for p in providers:
        name = p.get("name")
        st = (statuses.get(name) or {}).get("status", UNKNOWN)
        if not enable or st in (UNKNOWN, AVAILABLE):
            allowed.append(p)
            continue
        if st in _BLOCKING:
            blocked.append((name, EXHAUSTED))
            continue
        if st in _LIGHT_ONLY and budget == BUDGET_LARGE:
            blocked.append((name, f"{LOW}:{budget}"))
            continue
        allowed.append(p)
    return allowed, blocked


def provider_status_summary(conn, cfg: dict = None) -> dict:
    """管理接口用：全 provider 额度状态总览。"""
    enabled = ((cfg or {}).get("quota_guard", {}) or {}).get("enabled", True)
    rows = conn.execute(
        "SELECT provider, status, reason, source, last_checked_at, cooldown_until, updated_at "
        "FROM provider_quota ORDER BY provider").fetchall()
    return {
        "enabled": enabled,
        "count": len(rows),
        "providers": [dict(r) for r in rows],
    }


# ── 组合操作（给 HTTP 层用的薄接口，2026-09-18 从 app.py 下沉）──

def prepare_candidates(conn, providers: list, budget: str, cfg: dict = None) -> tuple:
    """冷却复活 + 候选过滤，一次连接内完成。

    返回 (allowed, blocked)；blocked 为 [(provider, reason)] 便于调用方打日志。
    副作用：会就地复活冷却到期的 exhausted 记录（recover_due）。
    为什么放这里：这是「额度」这个关注点的完整语义，不该散在 HTTP 层（见 docs/模块约定.md）。
    """
    recover_due(conn)
    statuses = get_statuses(conn, [p["name"] for p in providers])
    return filter_candidates(providers, statuses, budget, cfg)


def record_error_if_exhausted(conn, provider: str, http_status: int, body: str,
                              cfg: dict = None) -> tuple:
    """分类一次失败响应；只在「额度耗尽」时落库。

    返回 (是否额度耗尽, 说明)；说明用于日志（如 keyword:insufficient quota）。
    非额度类失败（限流/鉴权/5xx）只记 provider_events，不改额度状态 —— 交给熔断处理。
    """
    cls = classify_error(http_status, body, cfg)
    if cls.get("kind") == EXHAUSTED:
        record_error(conn, provider, cls, cfg)
        return True, cls.get("detail", "")
    return False, cls.get("detail", "")
