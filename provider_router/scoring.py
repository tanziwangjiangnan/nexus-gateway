"""监督者（Supervisor）评分——触发判定（纯逻辑，零 IO）

职责：决定「这次请求要不要启动第二名检查者打分」。
被谁调用：apps 侧的请求处理（api/app.py）在成功返回后调用；后台打分任务 `_score_by_runner_up` 仍在 app.py（它要发 HTTP + 写库）。

关键不变量：
  - 纯函数：不读环境变量、不连 DB、不发网络；只看传入的 cfg / scoring_state。
  - 随机采样只在稳态期发生；冷启动、超时、裁判变化、方差突变都会强制复审。

设计文档：.openhands/memory/designs/supervisor-scoring.md
v3.14（2026-09-18）从 api/app.py 抽出（见 docs/模块约定.md 第 4 步）。
"""
import random
import time

def supervisor_cfg(cfg, key, default=None):
    """从 supervisor 或 quality_feedback 段读取配置，新段优先。"""
    v = cfg.get("supervisor", {}).get(key)
    if v is not None:
        return v
    # 兼容旧配置路径
    legacy_map = {
        "enabled": ("quality_feedback", "runner_up_scoring"),
        "cold_start_count": ("quality_feedback", "scoring_warmup"),
        "force_on.timeout": ("quality_feedback", "scoring_max_interval"),
    }
    if key in legacy_map:
        sec, old_key = legacy_map[key]
        v = cfg.get(sec, {}).get(old_key)
        if v is not None:
            return v
    return default


def read_force_on(cfg):
    """读取 force_on 列表，返回 set 或默认值。"""
    raw = cfg.get("supervisor", {}).get("force_on")
    if isinstance(raw, list):
        items = set()
        for item in raw:
            if isinstance(item, dict):
                items.update(item.keys())
            elif isinstance(item, str):
                items.add(item)
        return items
    return {"timeout", "runner_changed"}


def should_score(cfg, provider, runner_up, scoring_state, now=None):
    """判断是否应该对这次请求启动评分。

    三层触发：
    1. 冷启动期 count < cold_start_count → 100%
    2. 稳态期 p = max(min_sample_rate, 1/sqrt(count))
    3. 强制复审（跳过概率）：
       - 超时：now - last > force_on.timeout
       - 裁判变化：runner_up 不在 last_runners 中
       - 方差突变：连续 3 次评分标准差 > 15
       - 新鲜度窗口：5 分钟内已评且无变化，跳过强制复审
    """
    if not runner_up:
        return False, "no_runner_up"
    # 显式关闭则不触发（默认开启）
    if not supervisor_cfg(cfg, "enabled", True):
        return False, "disabled_by_config"
    now = now or time.time()
    st = scoring_state.get(provider, {})
    count = st.get("count", 0)
    last = st.get("last", 0)

    # ── 大变量检查：从未评分 ──
    if count == 0:
        return True, "cold_start"

    # 读取配置
    force_on = read_force_on(cfg)
    max_interval = supervisor_cfg(cfg, "force_on.timeout", 3600)
    cold_start = supervisor_cfg(cfg, "cold_start_count", 10)
    min_rate = supervisor_cfg(cfg, "min_sample_rate", 0.05)

    # ── 新鲜度窗口：5 分钟内已评且无变量变化，跳过强制复审 ──
    freshness_window = supervisor_cfg(cfg, "freshness_window", 300)
    if last and (now - last) < freshness_window:
        # 裁判没变 → 跳过
        if "runner_changed" in force_on and runner_up["name"] in st.get("last_runners", []):
            return False, "freshness_skip"
        return False, "freshness_skip"

    # ── 强制复审 1：超时 ──
    if "timeout" in force_on and last and (now - last) > max_interval:
        return True, "stale"

    # ── 强制复审 2：裁判变化 ──
    if "runner_changed" in force_on:
        last_runners = st.get("last_runners", [])
        if last_runners and runner_up["name"] not in last_runners:
            return True, "new_judge"

    # ── 强制复审 3：方差突变 ──
    recent_scores = st.get("recent_scores", [])
    variance_boost = st.get("variance_boost_remaining", 0)
    if variance_boost > 0:
        # 方差爆发期：采样率提升到 50%
        if random.random() < 0.5:
            return True, "variance_boost"
    elif len(recent_scores) >= 3:
        # 算标准差
        mean = sum(recent_scores) / len(recent_scores)
        variance = sum((s - mean) ** 2 for s in recent_scores) / len(recent_scores)
        stddev = variance ** 0.5
        if stddev > 15:
            return True, "variance_spike"

    # ── 冷启动期 ──
    if count < cold_start:
        return True, "warmup"

    # ── 稳态：概率采样 ──
    p = max(min_rate, 1.0 / (count ** 0.5))
    if random.random() < p:
        return True, f"sample_p={p:.2f}"
    return False, f"skip_p={p:.2f}"
