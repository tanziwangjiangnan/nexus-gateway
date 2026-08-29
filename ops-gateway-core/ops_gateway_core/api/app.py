"""HTTP API 层 — FastAPI 应用构建器。
v3.2: 从 gateway.py 拆分。build_app(cfg, deps) 返回 FastAPI 实例，
所有共享状态通过 deps 注入，避免循环依赖。
"""

# 保持与原 gateway.py 相同的模块级导入，确保依赖可用
import datetime
import hashlib
import asyncio
import json
import os
import random
import re
import subprocess
import shlex
import sys
import time
import threading
import yaml
import httpx

from provider_router import Router
from ..cfg import get_db
from ..fiber import FiberRuntime

# prometheus 客户端（延迟导入，兼容无 prometheus 环境）
try:
    from prometheus_client import Counter, Gauge, Histogram, generate_latest, REGISTRY
    _HAS_PROMETHEUS = True
except ImportError:
    _HAS_PROMETHEUS = False
    def generate_latest(*a, **k):
        return b""
    Counter = Gauge = Histogram = None
    REGISTRY = None


def _get_provider_from_last_usage():
    """返回最近一次调用使用的 provider 名（用于检查者评分关联）。"""
    conn = get_db()
    row = conn.execute("SELECT provider FROM usage ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return row["provider"] if row else None


# ── 监督者（Supervisor）评分机制 ──
# 默认开启但不每次都审：冷启动 100% → 稳态概率衰减 → 大变量强制复审
# 完整设计文档见 .openhands/memory/designs/supervisor-scoring.md

# 配置段读取（向后兼容 quality_feedback.* → supervisor.*）
def _supervisor_cfg(cfg, key, default=None):
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


def _read_force_on(cfg):
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


def _should_score(cfg, provider, runner_up, scoring_state, now=None):
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
    if not _supervisor_cfg(cfg, "enabled", True):
        return False, "disabled_by_config"
    now = now or time.time()
    st = scoring_state.get(provider, {})
    count = st.get("count", 0)
    last = st.get("last", 0)

    # ── 大变量检查：从未评分 ──
    if count == 0:
        return True, "cold_start"

    # 读取配置
    force_on = _read_force_on(cfg)
    max_interval = _supervisor_cfg(cfg, "force_on.timeout", 3600)
    cold_start = _supervisor_cfg(cfg, "cold_start_count", 10)
    min_rate = _supervisor_cfg(cfg, "min_sample_rate", 0.05)

    # ── 新鲜度窗口：5 分钟内已评且无变量变化，跳过强制复审 ──
    freshness_window = _supervisor_cfg(cfg, "freshness_window", 300)
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


async def _score_by_runner_up(cfg, provider, runner_up,
                               provider_model, messages, resp_body,
                               quality_factors, scoring_state):
    """后台任务：调第二名给第一名的回答打分。"""
    try:
        # 构造评分 prompt
        data = json.loads(resp_body)
        assistant_reply = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        if not assistant_reply:
            return

        scoring_cfg = cfg.get("supervisor", {}).get("scoring", {})
        scoring_prompt = scoring_cfg.get("prompt") or (
            "你是一个质量评分员。请根据用户的提问和 AI 的回答，"
            "给回答的质量评分（0-100，整数）。"
            "考虑：准确性、完整性、逻辑性、语言质量。"
            "只返回数字，不要其他文字。"
        )
        scoring_messages = [
            {"role": "system", "content": scoring_prompt},
            {
                "role": "user",
                "content": f"问题：{messages[-1]['content'] if messages else ''}\n\n回答：{assistant_reply[:2000]}"
            },
        ]

        # 确定评分模型
        scoring_model = scoring_cfg.get("model")
        # 找 runner-up 的 API 配置
        rp_cfg = cfg.get("providers", {}).get(runner_up["name"])
        if not rp_cfg:
            return
        rp_key = Router.resolve_env_key(rp_cfg.get("api_key", ""))
        if not rp_key:
            return
        rp_api = rp_cfg["api"].rstrip("/")
        # 如果配置了评分模型，用评分模型；否则用 runner_up 的第一个模型
        if scoring_model:
            scoring_model_used = scoring_model
        else:
            scoring_model_used = runner_up.get("models", [provider_model])[0]

        # 发起评分请求
        async with httpx.AsyncClient(timeout=30) as client:
            score_resp = await client.post(
                f"{rp_api}/chat/completions",
                json={
                    "model": scoring_model_used,
                    "messages": scoring_messages,
                    "max_tokens": 50,
                    "temperature": 0,
                },
                headers={"Authorization": f"Bearer {rp_key}"},
            )
            if score_resp.status_code != 200:
                return
            score_data = score_resp.json()
            score_text = (score_data.get("choices", [{}])[0]
                          .get("message", {}).get("content", ""))
            # 解析数字
            score = None
            for token in score_text.strip().split():
                try:
                    s = int(''.join(c for c in token if c.isdigit() or c == '-'))
                    if 0 <= s <= 100:
                        score = s
                        break
                except ValueError:
                    continue
            if score is None:
                return
            # 写入 DB
            conn = get_db()
            conn.execute(
                "UPDATE usage SET checker_score = ? WHERE id = (SELECT id FROM usage WHERE provider = ? ORDER BY id DESC LIMIT 1)",
                (score, provider))
            conn.commit()
            conn.close()
            # 更新运行时质量因子
            quality_window = cfg.get("quality_feedback", {}).get("quality_window", 20)
            conn2 = get_db()
            rows = conn2.execute(
                "SELECT checker_score FROM usage WHERE provider = ? AND checker_score IS NOT NULL ORDER BY id DESC LIMIT ?",
                (provider, quality_window)
            ).fetchall()
            conn2.close()
            if rows:
                avg = sum(r[0] for r in rows) / len(rows)
                quality_factors[provider] = max(0.5, min(1.0, avg / 100.0))
            # 更新评分状态
            if provider not in scoring_state:
                scoring_state[provider] = {}
            st = scoring_state[provider]
            now = time.time()
            st["last"] = now
            st["count"] = st.get("count", 0) + 1
            st["last_score_value"] = score
            # 更新 last_runners（最多保留 3 个）
            last_runners = st.get("last_runners", [])
            if runner_up["name"] not in last_runners:
                last_runners.append(runner_up["name"])
                if len(last_runners) > 3:
                    last_runners.pop(0)
            st["last_runners"] = last_runners
            # 更新 recent_scores（最多 5 个，用于方差检测）
            recent = st.get("recent_scores", [])
            recent.append(score)
            if len(recent) > 5:
                recent.pop(0)
            st["recent_scores"] = recent
            # 方差爆发期递减
            if st.get("variance_boost_remaining", 0) > 0:
                st["variance_boost_remaining"] -= 1
            # 如果这次评分确实触发了方差突变，设置爆发期
            stddev = 0.0
            if len(recent) >= 3:
                mean = sum(recent) / len(recent)
                variance = sum((s - mean) ** 2 for s in recent) / len(recent)
                stddev = variance ** 0.5
                if stddev > 15:
                    st["variance_boost_remaining"] = 5
            print(f"📋 监督者评分 [{runner_up['name']}]→{provider}: {score}分 "
                  f"(第{st['count']}次, 标准差={stddev:.1f})")
    except Exception:
        import traceback
        traceback.print_exc()


def build_app(cfg, deps):
    """构建 FastAPI 应用实例。

    deps 注入的共享状态：
    - disabled_providers / router_state / fiber_runtime
    - dynamic_weights / approval_cache / pending_approvals / lock
    - serial_locks / throttle_windows / prometheus_registered / monitor
    - 函数委托：execute_plugin / format_string / global_call_* / undo_* / fiber_* / select_*
    """
    # ── 解构依赖（保持原函数体内的名字不变） ──
    _disabled_providers = deps["disabled_providers"]
    _router_state = deps["router_state"]
    _fiber_runtime = deps["fiber_runtime"]
    _dynamic_weights = deps["dynamic_weights"]
    _approval_cache = deps["approval_cache"]
    _pending_approvals = deps["pending_approvals"]
    _lock = deps["lock"]
    _serial_locks = deps["serial_locks"]
    _throttle_windows = deps["throttle_windows"]
    get_db = deps["get_db"]
    _execute_plugin = deps["execute_plugin"]
    _format_string = deps["format_string"]
    _global_call_lookup = deps["global_call_lookup"]
    _global_call_add = deps["global_call_add"]
    undo_register = deps["undo_register"]
    undo_pop = deps["undo_pop"]
    fiber_create = deps["fiber_create"]
    fiber_register = deps["fiber_register"]
    fiber_fail = deps["fiber_fail"]
    fiber_commit = deps["fiber_commit"]
    find_model_config = deps["find_model_config"]
    select_pool_by_keywords = deps["select_pool_by_keywords"]
    select_provider_by_weight = deps["select_provider_by_weight"]
    select_provider_with_runner_up = deps["select_provider_with_runner_up"]
    check_rate_limit = deps["check_rate_limit"]
    _quality_factors = deps["quality_factors"]
    _user_factors = deps["user_factors"]
    _log_matches = deps["log_matches"]
    _parse_log_line = deps["parse_log_line"]

    from fastapi import FastAPI, Request, HTTPException
    from fastapi.responses import JSONResponse, StreamingResponse, Response

    app = FastAPI(title="模型池网关 v2", version="0.2.0")

    # ── 鉴权中间件 ──
    @app.middleware("http")
    async def auth_check(request: Request, call_next):
        if request.url.path in ("/health", "/metrics", "/chat"):
            return await call_next(request)
        if request.url.path.startswith("/v1/plugins/"):
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        expected = f"Bearer {cfg['gateway_key']}"
        if auth != expected:
            return JSONResponse(status_code=401, content={"error": "unauthorized"})
        return await call_next(request)

    # ── 健康检查 ──
    @app.get("/health")
    async def health():
        # 更新池健康指标
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            enabled = sum(1 for pv in pool_cfg.get("providers", [])
                          if pv["name"] not in _disabled_providers)
            app.state.pool_health.labels(pool=pool_name).set(1 if enabled > 0 else 0)
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            for pv in pool_cfg.get("providers", []):
                app.state.provider_up.labels(provider=pv["name"]).set(
                    0 if pv["name"] in _disabled_providers else 1)
        return {"status": "ok", "version": "0.2.0", "time": datetime.datetime.now().isoformat()}

    # ── 聊天页面（免鉴权） ──
    @app.get("/chat")
    async def chat_page():
        """简洁的聊天入口，用户带自己的 key 走三池路由"""
        gw_key = cfg.get("gateway_key", "")
        # 收集可用模型
        all_models = []
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            for pv in pool_cfg.get("providers", []):
                for m in pv.get("models", []):
                    if m not in all_models:
                        all_models.append(m)
        model_options = "\n".join(f'<option value="{m}">{m}</option>' for m in all_models)

        html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>模型池网关 · 聊天</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, "Segoe UI", sans-serif; background: #f5f5f5; height: 100vh; display: flex; flex-direction: column; }}
  .header {{ background: #1a1a2e; color: #eee; padding: 14px 24px; display: flex; align-items: center; gap: 16px; }}
  .header h1 {{ font-size: 18px; font-weight: 600; }}
  .header span {{ font-size: 12px; color: #888; }}
  .toolbar {{ display: flex; gap: 12px; padding: 12px 24px; background: #fff; border-bottom: 1px solid #e0e0e0; align-items: center; flex-wrap: wrap; }}
  .toolbar label {{ font-size: 13px; color: #555; }}
  .toolbar select, .toolbar input {{ padding: 6px 10px; border: 1px solid #ccc; border-radius: 6px; font-size: 13px; }}
  .toolbar input[type="text"] {{ flex: 1; min-width: 160px; }}
  .toolbar .status {{ font-size: 12px; color: #888; margin-left: auto; }}
  #messages {{ flex: 1; overflow-y: auto; padding: 24px; display: flex; flex-direction: column; gap: 16px; }}
  .msg {{ max-width: 80%; padding: 12px 16px; border-radius: 12px; line-height: 1.5; font-size: 14px; white-space: pre-wrap; }}
  .msg.user {{ align-self: flex-end; background: #1a73e8; color: #fff; border-bottom-right-radius: 4px; }}
  .msg.assistant {{ align-self: flex-start; background: #fff; color: #222; border: 1px solid #e0e0e0; border-bottom-left-radius: 4px; }}
  .msg.system {{ align-self: center; background: #fff3cd; color: #856404; font-size: 12px; border-radius: 6px; }}
  .msg .meta {{ font-size: 11px; color: #999; margin-top: 6px; }}
  .input-area {{ display: flex; gap: 8px; padding: 16px 24px; background: #fff; border-top: 1px solid #e0e0e0; }}
  .input-area textarea {{ flex: 1; padding: 10px; border: 1px solid #ccc; border-radius: 8px; resize: none; font-size: 14px; min-height: 44px; max-height: 120px; }}
  .input-area button {{ padding: 10px 24px; background: #1a73e8; color: #fff; border: none; border-radius: 8px; font-size: 14px; cursor: pointer; }}
  .input-area button:disabled {{ opacity: 0.5; cursor: not-allowed; }}
  .loading {{ display: inline-block; width: 16px; height: 16px; border: 2px solid #ccc; border-top-color: #1a73e8; border-radius: 50%; animation: spin 0.8s linear infinite; }}
  @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
</style>
</head>
<body>
<div class="header">
  <h1>🗣 模型池</h1>
  <span>三池路由 · 自备 Key</span>
</div>
<div class="toolbar">
  <label>模型</label>
  <select id="model">{model_options}</select>
  <label>Key</label>
  <input type="text" id="api_key" placeholder="sk-..." value="">
  <span class="status" id="status">就绪</span>
</div>
<div id="messages"></div>
<div class="input-area">
  <textarea id="input" placeholder="输入消息..." rows="1"></textarea>
  <button id="send">发送</button>
</div>
<script>
  const el = id => document.getElementById(id);
  const msgBox = el('messages');
  const input = el('input');
  const sendBtn = el('send');
  const status = el('status');
  let loading = false;

  function addMsg(role, content, meta) {{
    const div = document.createElement('div');
    div.className = 'msg ' + role;
    div.textContent = content;
    if (meta) {{
      const m = document.createElement('div');
      m.className = 'meta';
      m.textContent = meta;
      div.appendChild(m);
    }}
    msgBox.appendChild(div);
    msgBox.scrollTop = msgBox.scrollHeight;
  }}

  input.addEventListener('input', () => {{
    input.style.height = 'auto';
    input.style.height = Math.min(input.scrollHeight, 120) + 'px';
  }});
  input.addEventListener('keydown', e => {{
    if (e.key === 'Enter' && !e.shiftKey) {{ e.preventDefault(); send(); }}
  }});

  async def send() {{
    const model = el('model').value;
    const key = el('api_key').value.trim();
    const text = input.value.trim();
    if (!text || loading) return;
    if (!key) {{ addMsg('system', '请在上方输入你的 API Key'); return; }}
    addMsg('user', text);
    input.value = '';
    input.style.height = 'auto';
    loading = true;
    sendBtn.disabled = true;
    status.textContent = '请求中...';
    try {{
      const resp = await fetch('/v1/chat/completions', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json', 'Authorization': 'Bearer {gw_key}' }},
        body: JSON.stringify({{ model, messages: [{{ role: 'user', content: text }}], api_key: key }})
      }});
      if (!resp.ok) {{
        const err = await resp.json().catch(() => ({{}}));
        addMsg('system', `请求失败: ${{err.error || resp.statusText}} (HTTP ${{resp.status}})`);
        return;
      }}
      const data = await resp.json();
      const reply = data.choices?.[0]?.message?.content || '(空响应)';
      const usage = data.usage ? `⬆${{data.usage.prompt_tokens||0}} ⬇${{data.usage.completion_tokens||0}}` : '';
      addMsg('assistant', reply, usage);
    }} catch(e) {{
      addMsg('system', '网络错误: ' + e.message);
    }} finally {{
      loading = false;
      sendBtn.disabled = false;
      status.textContent = '就绪';
    }}
  }}
</script>
</body>
</html>"""
        return Response(html, media_type="text/html")

    # ── 模型列表 ──
    @app.get("/v1/models")
    async def list_models():
        conn = get_db()
        rows = conn.execute("""SELECT r.model, r.pool, r.provider, r.tier, r.status,
                                      COALESCE(SUM(u.prompt_tokens+u.completion_tokens), 0) as tokens
                               FROM registry r LEFT JOIN usage u ON u.model=r.model
                               GROUP BY r.model ORDER BY r.pool, r.model""").fetchall()
        conn.close()

        # 计算每个 provider 最近 5 分钟的错误率
        conn2 = get_db()
        err_rows = conn2.execute("""
            SELECT provider,
                   COUNT(*) as total,
                   SUM(ok) as success
            FROM usage
            WHERE called_at > datetime('now', '-5 minutes')
            GROUP BY provider
        """).fetchall()
        conn2.close()
        provider_errors = {}
        for r in err_rows:
            total = r["total"]
            if total >= 5:
                success = r["success"] or 0
                provider_errors[r["provider"]] = 1.0 - (success / total)

        # 构建 provider → capabilities 映射
        provider_caps = {}
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            for pv in pool_cfg.get("providers", []):
                caps = pv.get("capabilities", [])
                if caps:
                    provider_caps[pv["name"]] = caps

        data = []
        seen = set()
        for r in rows:
            model_id = r["model"]
            if model_id in seen:
                continue
            seen.add(model_id)
            provider_name = r["provider"]
            err_rate = provider_errors.get(provider_name, 0.0)

            # 实时状态：熔断覆盖 DB 状态
            if provider_name in _disabled_providers:
                model_status = "disabled"
            elif err_rate > 0.20:
                model_status = "throttled"
            else:
                model_status = "active"

            entry = {
                "id": model_id,
                "object": "model",
                "pool": r["pool"],
                "provider": provider_name,
                "tier": r["tier"],
                "status": model_status,
                "error_rate": round(err_rate, 3) if err_rate > 0 else None,
                "today_tokens": r["tokens"],
            }
            caps = provider_caps.get(provider_name, [])
            if caps:
                entry["capabilities"] = caps
            data.append(entry)

        return {"object": "list", "data": data}

    # ── 聊天补全（三池路由核心） ──
    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        t0 = time.time()
        body = await request.json()
        model = body.get("model", "DeepSeek-V4-Flash")
        messages = body.get("messages", [])
        stream = body.get("stream", False)
        # 用户自定义 key（从聊天页面带入），覆盖 provider 配置的 key
        user_key = body.pop("api_key", None)
        kwargs = {k: v for k, v in body.items() if k not in ("model", "messages", "stream")}

        # 1. 模型名精确匹配（大小写不敏感，优先级最高）
        model_pool, pool_cfg, _, canonical_model = find_model_config(cfg, model)
        if model_pool:
            pool_name = model_pool
            model = canonical_model
            if not pool_cfg:
                pool_cfg = cfg.get("pools", {}).get(pool_name)
        else:
            pool_name = None
            pool_cfg = None

        # 2. 关键词路由（仅当模型路由未命中时使用）
        if not pool_name:
            messages_text = json.dumps(messages, ensure_ascii=False)
            kw_pool = select_pool_by_keywords(cfg, messages_text)
            if kw_pool:
                pool_name = kw_pool

        # 3. 兜底默认池
        if not pool_name:
            pool_name = cfg.get("routing", {}).get("default_pool", "pool_a")

        # 3. 走故障转移链
        tried_pools = set()
        current_pool = pool_name
        last_error = "no available provider"
        used_provider = ""

        while current_pool and current_pool not in tried_pools:
            tried_pools.add(current_pool)
            pool_cfg = cfg.get("pools", {}).get(current_pool)
            if not pool_cfg:
                break

            # 4. 按权重选 provider（同时选出第二名作为潜在检查者）
            # 用户自定义 key 时不限制模型名（用户用自己的 key 调任何 provider），
            # 否则只选有该模型的 provider
            model_filter = None if user_key else model
            pv, runner_up, _ = select_provider_with_runner_up(
                pool_cfg.get("providers", []), model=model_filter)
            if not pv:
                last_error = f"pool '{current_pool}' all providers disabled"
                current_pool = pool_cfg.get("fallback")
                continue

            provider_cfg = cfg.get("providers", {}).get(pv["name"])
            # 有效 key：用户自定义 key 优先，否则用 provider 配置的 key（支持 ${ENV} 引用）
            configured_key = Router.resolve_env_key(provider_cfg.get("api_key", "")) if provider_cfg else ""
            effective_key = user_key or configured_key
            if not provider_cfg or not effective_key:
                last_error = f"provider '{pv['name']}' key not resolved"
                current_pool = pool_cfg.get("fallback")
                continue

            # 5. 限流检查
            if not check_rate_limit(pv["name"], provider_cfg.get("max_rps")):
                last_error = f"provider '{pv['name']}' rate limited"
                # 限流不触发 fallback，只是拒绝这次请求
                app.state.req_counter.labels(pool=current_pool, provider=pv["name"], status="429").inc()
                app.state.req_duration.labels(provider=pv["name"]).observe(time.time() - t0)
                raise HTTPException(status_code=429, detail=last_error)

            # 6. 发起调用
            used_provider = pv["name"]
            try:
                async with httpx.AsyncClient(timeout=120) as client:
                    api = provider_cfg["api"].rstrip("/")
                    # 用当前 provider 自己的模型名（大小写按 YAML 配置来）
                    provider_model = model
                    for m in pv.get("models", []):
                        if m.lower() == model.lower():
                            provider_model = m
                            break
                    req_body = {"model": provider_model, "messages": messages, "stream": stream, **kwargs}
                    resp = await client.post(
                        f"{api}/chat/completions",
                        json=req_body,
                        headers={"Authorization": f"Bearer {effective_key}"},
                    )
                    status_code = resp.status_code
                    resp_body = resp.text

                    if stream:
                        return StreamingResponse(resp.aiter_bytes(), media_type="text/event-stream", status_code=status_code)

                    # 记录用量
                    try:
                        data = resp.json()
                        conn = get_db()
                        conn.execute("INSERT INTO usage (model, pool, provider, prompt_tokens, completion_tokens, ok) VALUES (?,?,?,?,?,?)",
                                     (model, current_pool, pv["name"],
                                      data.get("usage", {}).get("prompt_tokens", 0),
                                      data.get("usage", {}).get("completion_tokens", 0),
                                      1 if status_code == 200 else 0))
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass

                    # 失败但不 fallback 的情况（HTTP 4xx 是客户端问题）
                    if status_code in (400, 401, 403, 404, 422):
                        app.state.req_counter.labels(pool=current_pool, provider=pv["name"], status=str(status_code)).inc()
                        app.state.req_duration.labels(provider=pv["name"]).observe(time.time() - t0)
                        return Response(content=resp_body, status_code=status_code, media_type="application/json")

                    # 5xx → fallback 到下一个池
                    if status_code >= 500:
                        last_error = f"HTTP {status_code}"
                        print(f"🔴 上游 {status_code}: {pv['name']} model={model}")
                        current_pool = pool_cfg.get("fallback")
                        continue

                    app.state.req_counter.labels(pool=current_pool, provider=pv["name"], status="200").inc()
                    app.state.req_duration.labels(provider=pv["name"]).observe(time.time() - t0)
                    # 第二名检查者：自适应采样频率
                    # 默认开启，冷启动每次都审 → 稳态概率衰减 → 大变量强制复审
                    if runner_up and not user_key:
                        decision, reason = _should_score(cfg, pv["name"], runner_up, _scoring_state)
                        if decision:
                            asyncio.get_event_loop().create_task(
                                _score_by_runner_up(
                                    cfg=cfg, provider=pv["name"], runner_up=runner_up,
                                    provider_model=provider_model,
                                    messages=messages, resp_body=resp_body,
                                    quality_factors=_quality_factors,
                                    scoring_state=_scoring_state,
                                )
                            )
                    return Response(content=resp_body, status_code=status_code, media_type="application/json")

            except httpx.TimeoutException:
                last_error = "timeout"
                print(f"⏱️ 超时: {pv['name']} model={model} timeout=120s")
                current_pool = pool_cfg.get("fallback")
                continue
            except httpx.ConnectError:
                last_error = "unreachable"
                print(f"🔌 不可达: {pv['name']} api={provider_cfg.get('api','?')}")
                current_pool = pool_cfg.get("fallback")
                continue
            except Exception as e:
                last_error = str(e)
                print(f"⚠️ 请求异常: {pv['name']} model={model} error={e}")
                current_pool = pool_cfg.get("fallback")
                continue

        app.state.req_counter.labels(pool=pool_name, provider=used_provider or "none", status="503").inc()
        app.state.req_duration.labels(provider=used_provider or "none").observe(time.time() - t0)
        raise HTTPException(status_code=503, detail=f"all pools exhausted: {last_error}")

    # ── 直连端点：不走池路由、关键词匹配、故障转移 ──
    @app.post("/v1/direct/chat/completions")
    async def direct_chat_completions(request: Request):
        """直接调用指定 provider，不走任何路由逻辑。

        请求体与 /v1/chat/completions 相同，额外支持:
        - `_provider`: 指定 provider 名称（如 scnet-tp、deepseek-direct），可选
        - 若不指定，自动从 model 名查找所属 provider
        """
        t0 = time.time()
        body = await request.json()
        model = body.get("model", "DeepSeek-V4-Flash")
        messages = body.get("messages", [])
        stream = body.get("stream", False)
        kwargs = {k: v for k, v in body.items() if k not in ("model", "messages", "stream", "_provider")}

        direct_provider_name = body.get("_provider")
        if not direct_provider_name:
            # 自动从 model 名查找第一个匹配的 provider
            _, _, pv, m = find_model_config(cfg, model)
            if pv:
                direct_provider_name = pv["name"]
                model = m
            if not direct_provider_name:
                raise HTTPException(status_code=400, detail=f"model '{model}' not found in any provider")

        provider_cfg = cfg.get("providers", {}).get(direct_provider_name)
        direct_key = Router.resolve_env_key(provider_cfg.get("api_key", "")) if provider_cfg else ""
        if not provider_cfg or not direct_key:
            raise HTTPException(status_code=400, detail=f"provider '{direct_provider_name}' not configured or key not resolved")
        api = provider_cfg["api"].rstrip("/")

        async with httpx.AsyncClient(timeout=120) as client:
            req_body = {"model": model, "messages": messages, "stream": stream, **kwargs}
            resp = await client.post(
                f"{api}/chat/completions",
                json=req_body,
                headers={"Authorization": f"Bearer {direct_key}"},
            )

        if stream:
            return StreamingResponse(resp.aiter_bytes(), media_type="text/event-stream", status_code=resp.status_code)

        app.state.req_counter.labels(pool="direct", provider=direct_provider_name, status=str(resp.status_code)).inc()
        app.state.req_duration.labels(provider=direct_provider_name).observe(time.time() - t0)
        return Response(content=resp.text, status_code=resp.status_code, media_type="application/json")

    # ── Prometheus 指标（模块级，避免重复注册） ──
    _prometheus_registered = False
    def _ensure_prometheus():
        nonlocal _prometheus_registered
        if _prometheus_registered:
            return
        from prometheus_client import Counter, Gauge, Histogram
        app.state.req_counter = Counter("gateway_requests_total", "Total requests", ["pool", "provider", "status"])
        app.state.pool_health = Gauge("gateway_pool_healthy", "Pool health 1/0", ["pool"])
        app.state.provider_up = Gauge("gateway_provider_up", "Provider up 1/0", ["provider"])
        app.state.req_duration = Histogram("gateway_request_duration_seconds", "Request latency",
                                           ["provider"], buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0])
        _prometheus_registered = True

    _ensure_prometheus()

    @app.get("/metrics")
    async def metrics():
        from prometheus_client import generate_latest, REGISTRY
        return Response(content=generate_latest(REGISTRY).decode(), media_type="text/plain")

    # ── Admin API ──
    @app.get("/admin/pools")
    async def admin_pools():
        result = {}
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            providers = []
            for pv in pool_cfg.get("providers", []):
                pc = cfg.get("providers", {}).get(pv["name"], {})
                providers.append({
                    "name": pv["name"],
                    "weight": pv.get("weight", 1),
                    "models": pv.get("models", []),
                    "disabled": pv["name"] in _disabled_providers,
                    "max_rps": pc.get("max_rps", 0),
                    "api": pc.get("api", ""),
                })
            result[pool_name] = {
                "description": pool_cfg.get("description", ""),
                "fallback": pool_cfg.get("fallback"),
                "providers": providers,
            }
        return result

    @app.post("/admin/pools/{pool_name}/providers/{provider_name}/toggle")
    async def admin_toggle_provider(pool_name: str, provider_name: str):
        # 验证 provider 存在
        found = False
        for pn, pc in cfg.get("pools", {}).items():
            for pv in pc.get("providers", []):
                if pv["name"] == provider_name:
                    found = True
                    break
        if not found:
            raise HTTPException(status_code=404, detail=f"provider '{provider_name}' not found")
        with _lock:
            if provider_name in _disabled_providers:
                _disabled_providers.discard(provider_name)
                undo_register(f"启用 {provider_name}",
                              lambda n=provider_name: _disabled_providers.add(n))
                return {"provider": provider_name, "status": "enabled"}
            else:
                _disabled_providers.add(provider_name)
                undo_register(f"禁用 {provider_name}",
                              lambda n=provider_name: _disabled_providers.discard(n))
                return {"provider": provider_name, "status": "disabled"}

    # ── 运行时逆栈 Admin API ──
    @app.get("/admin/undo")
    async def admin_undo():
        ok, msg = undo_pop()
        return {"ok": ok, "message": msg}

    @app.get("/admin/undo-list")
    async def admin_undo_list():
        return {"stack": _fiber_runtime.undo_list()}

    # ── MCP 审批回调 ──
    # 让统筹 Agent 通过 HTTP 调用 toggle，走审批缓存 + fiber 树形上下文
    _APPROVAL_TTL = 300  # 5 分钟

    @app.get("/admin/mcp/approvals")
    async def admin_mcp_approvals():
        """查看审批缓存状态"""
        now = time.time()
        active = {k: v for k, v in _approval_cache.items() if v > now}
        return {
            "active_approvals": len(active),
            "approvals": [{"hash": k, "expires_at": datetime.datetime.fromtimestamp(v).isoformat()}
                          for k, v in sorted(active.items())],
        }

    @app.get("/admin/mcp/status")
    async def admin_mcp_status():
        """MCP 状态总览：熔断 + 权重 + 审批"""
        # 5 分钟滑动窗口错误率
        conn = get_db()
        rows = conn.execute("""
            SELECT provider, COUNT(*) as total, SUM(ok) as success
            FROM usage WHERE called_at > datetime('now', '-5 minutes')
            GROUP BY provider
        """).fetchall()
        conn.close()
        providers_status = []
        for r in rows:
            total = r["total"]
            success = r["success"] or 0
            err_rate = 1.0 - (success / total) if total > 0 else 0
            providers_status.append({
                "name": r["provider"],
                "total_requests": total,
                "error_rate": round(err_rate, 3),
                "disabled": r["provider"] in _disabled_providers,
                "dynamic_weight": round(_dynamic_weights.get(r["provider"], 1.0), 2),
                "static_weight": next(
                    (pv.get("weight", 1.0) for pc in cfg.get("pools", {}).values()
                     for pv in pc.get("providers", []) if pv["name"] == r["provider"]),
                    1.0),
            })
        return {
            "pools": {pn: {
                "providers": [{
                    "name": pv["name"],
                    "disabled": pv["name"] in _disabled_providers,
                    "dynamic_weight": round(_dynamic_weights.get(pv["name"], pv.get("weight", 1.0)), 2),
                } for pv in pc.get("providers", [])]
            } for pn, pc in cfg.get("pools", {}).items()},
            "providers": providers_status,
            "approvals_active": len([k for k, v in _approval_cache.items() if v > time.time()]),
        }

    # ── Fiber 树形上下文 API（Agent 任务级可逆） ──
    @app.post("/admin/feedback")
    async def admin_feedback(request: Request):
        """用户反馈端点。
        请求体: {"fiber_id": 1, "feedback": 1, "modified_text": "..."}
        feedback: 1=点赞/采纳，-1=点踩/修改建议
        """
        body = await request.json()
        feedback = body.get("feedback")
        if feedback not in (1, -1):
            raise HTTPException(status_code=400, detail="feedback must be 1 or -1")
        # 找到与 fiber 关联的 provider 最近一条 usage
        provider = _get_provider_from_last_usage()
        if not provider:
            raise HTTPException(status_code=404, detail="no usage record found")
        conn = get_db()
        conn.execute(
            "UPDATE usage SET user_feedback = ? WHERE id = (SELECT id FROM usage WHERE provider = ? ORDER BY called_at DESC LIMIT 1)",
            (feedback, provider))
        conn.commit()
        conn.close()
        # 实时更新用户因子（不等30秒循环）
        qf_cfg = cfg.get("quality_feedback", {}).get("user_window", 20)
        conn2 = get_db()
        urows = conn2.execute(
            "SELECT user_feedback FROM usage WHERE provider = ? AND user_feedback != 0 ORDER BY called_at DESC LIMIT ?",
            (provider, qf_cfg)
        ).fetchall()
        conn2.close()
        total = sum(r[0] for r in urows)
        _user_factors[provider] = max(0.5, min(1.5, 1.0 + total * 0.1))
        return {"status": "ok", "provider": provider, "user_factor": _user_factors[provider]}

    @app.post("/admin/fiber/create")
    async def admin_fiber_create(request: Request):
        body = await request.json()
        # 校验 capabilities（检查者只能有只读权限）
        capabilities = body.get("capabilities", [])
        valid_read = {"read", "validate", "inspect"}
        valid_write = {"write", "execute"}
        agent_id = body.get("agent_id", "unknown")
        for cap in capabilities:
            if cap not in valid_read | valid_write:
                raise HTTPException(status_code=422, detail=f"无效能力: {cap}")

        # 根据 agent 声明校验权限
        agents_cfg = cfg.get("agents", [])
        agent_decl = next((a for a in agents_cfg if a.get("id") == agent_id), None)
        if agent_decl:
            declared_caps = set(agent_decl.get("capabilities", []))
            requested_caps = set(capabilities)
            if requested_caps - declared_caps:
                raise HTTPException(
                    status_code=403,
                    detail=f"Agent {agent_id} 声明的能力 ({declared_caps}) 不包含请求的 ({requested_caps})",
                )
            # 检查者不能有写权限
            if declared_caps <= valid_read and requested_caps & valid_write:
                raise HTTPException(
                    status_code=403,
                    detail=f"检查者 Agent {agent_id} 只允许 {valid_read} 能力，不能请求 {requested_caps & valid_write}",
                )

        fid = fiber_create(
            agent_id=agent_id,
            description=body.get("description", ""),
            parent_id=body.get("parent_id"),
            capabilities=capabilities,
        )
        f = _fiber_runtime.fiber_get(fid)
        return {"fiber_id": fid, "parent_id": f.parent_id, "status": f.status, "description": f.description, "capabilities": capabilities}

    @app.post("/admin/fiber/{fiber_id}/fail")
    async def admin_fiber_fail(fiber_id: int, request: Request):
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        ok, ops = fiber_fail(fiber_id)
        if not ok:
            raise HTTPException(status_code=404, detail=f"fiber {fiber_id} not found or not active")
        result = {"fiber_id": fiber_id, "status": "failed", "rollback_ops": ops}
        # 检查者证据：检查者可以通过 evidence 字段附上日志片段
        evidence = body.get("evidence", "")
        if evidence:
            result["evidence"] = evidence
        # 自动收集检查者日志（如果此 fiber 是检查者节点）
        f = _fiber_runtime.fiber_get(fiber_id)
        if f and f.agent_id and "checker" in f.agent_id.lower():
            try:
                evidence_logs = []
                agents_cfg = cfg.get("agents", [])
                checker_cfg = next((a for a in agents_cfg if a.get("id") == f.agent_id), None)
                if checker_cfg:
                    # 读检查者自身日志的最后 20 行
                    pid_file = checker_cfg.get("pid_file", "")
                    if pid_file and os.path.exists(pid_file):
                        with open(pid_file) as pf:
                            pid = pf.read().strip()
                        # 尝试读 journalctl 或日志文件
                        log_dir = os.path.join(os.path.dirname(pid_file), "logs")
                        if os.path.isdir(log_dir):
                            for lf in sorted(os.listdir(log_dir))[-3:]:
                                lfp = os.path.join(log_dir, lf)
                                try:
                                    with open(lfp, errors="replace") as lf_obj:
                                        log_lines = lf_obj.readlines()[-20:]
                                    evidence_logs.extend(log_lines)
                                except Exception:
                                    pass
                if evidence_logs:
                    result["checker_logs"] = evidence_logs
            except Exception:
                pass
        return result

    @app.post("/admin/fiber/{fiber_id}/commit")
    async def admin_fiber_commit(fiber_id: int, request: Request):
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        ok = fiber_commit(fiber_id)
        if not ok:
            raise HTTPException(status_code=409, detail=f"fiber {fiber_id} cannot commit: not active or children incomplete")
        # v2.7：检查者提交评分——若检查者 fiber 提交时携带 score，写入最近一条 usage 记录
        score = body.get("score")
        if score is not None:
            f = _fiber_runtime.fiber_get(fiber_id)
            if f and f.parent_id is not None:
                provider = _get_provider_from_last_usage()
                if provider:
                    conn = get_db()
                    conn.execute(
                        "UPDATE usage SET checker_score = ? WHERE id = (SELECT id FROM usage WHERE provider = ? ORDER BY called_at DESC LIMIT 1)",
                        (score, provider))
                    conn.commit()
                    conn.close()
        return {"fiber_id": fiber_id, "status": "committed"}

    @app.get("/admin/fiber/tree")
    async def admin_fiber_tree():
        """返回 fiber 森林（含状态、undo_log 摘要、子节点）。"""
        def _serialize(f):
            return {
                "id": f.id,
                "parent_id": f.parent_id,
                "agent_id": f.agent_id,
                "description": f.description,
                "status": f.status,
                "undo_count": len(f.undo_log),
                "children": sorted(f.children),
                "capabilities": f.capabilities,
                "call_history": f.call_history,
                "created_at": datetime.datetime.fromtimestamp(f.created_at).isoformat(),
            }
        return {"fibers": {fid: _serialize(f) for fid, f in sorted(_fiber_runtime.fiber_all().items())}}

    # ── 智能体声明式接入 ──
    @app.get("/admin/agents/declaration")
    async def admin_agents_declaration(agent_id: str = None):
        """返回 gateway.yaml 中 agents 段的完整声明。
        智能体启动时调用此端点，根据自身 id 找到对应配置块，自动接入。
        支持 ?agent_id=xxx 参数，返回该 Agent 可用的插件列表（按 capabilities 过滤）。
        """
        agents = cfg.get("agents", [])
        if not agents:
            return {"agents": [], "plugins": []}

        result = {"agents": agents}

        # 如果指定了 agent_id，返回该 Agent 可调用的插件列表
        if agent_id:
            agent_cfg = next((a for a in agents if a.get("id") == agent_id), None)
            agent_caps = set(agent_cfg.get("capabilities", [])) if agent_cfg else set()
            all_plugins = cfg.get("plugins", [])
            if agent_caps:
                available = []
                for p in all_plugins:
                    required = set(p.get("capabilities", []))
                    if required - agent_caps:
                        continue  # 缺少能力，跳过
                    available.append(p)
                result["plugins"] = available
            else:
                # 未知 Agent 或没有 capabilities → 只返回 read 插件
                result["plugins"] = [p for p in all_plugins
                                     if not (set(p.get("capabilities", [])) - {"read"})]
        else:
            result["plugins"] = cfg.get("plugins", [])

        return result

    @app.get("/admin/agents/status")
    async def admin_agents_status():
        """探测所有声明 Agent 的存活状态。
        根据 type 使用不同探测方式：
        - openhands: 检查 workspace 下是否有锁文件或 PID
        - astrbot: GET base_url/health，超时 2s
        - generic: 检查 pid_file 是否存在且进程存活
        - docker 容器（container_name / container_id / compose_project）:
          先 resolve 出 base_url，再 GET /health
        """
        agents = cfg.get("agents", [])
        results = []
        for agent in agents:
            aid = agent.get("id", "unknown")
            atype = agent.get("type", "generic")
            status = "unknown"
            detail = ""

            try:
                if agent.get("container_name") or agent.get("container_id") or agent.get("compose_project"):
                    # 容器化智能体（最高优先级）— 通过 Docker 解析 base_url 后探测
                    try:
                        from ops_gateway_core.ops.agent_discovery import resolve_agent_target
                        url, method = resolve_agent_target(agent)
                        if url:
                            try:
                                async with httpx.AsyncClient(timeout=3) as client:
                                    resp = await client.get(f"{url}/health")
                                    status = "online" if resp.status_code < 500 else "degraded"
                                    detail = f"{method}:{url} http_{resp.status_code}"
                            except (httpx.TimeoutException, httpx.ConnectError) as e:
                                status = "offline"
                                detail = f"{method}:{url} {str(e)[:50]}"
                        else:
                            status = "offline"
                            detail = f"resolve_failed:{method}"
                    except Exception as e:
                        status = "error"
                        detail = str(e)[:50]

                elif atype == "openhands":
                    ws = agent.get("workspace", "")
                    # 检查锁文件或 PID 文件
                    lock_file = os.path.join(ws, ".openhands.lock") if ws else ""
                    if lock_file and os.path.exists(lock_file):
                        with open(lock_file) as f:
                            pid = f.read().strip()
                        status = "online" if pid and os.path.exists(f"/proc/{pid}") else "offline"
                        detail = f"lock_pid={pid}" if status == "online" else "lock_stale"
                    else:
                        status = "offline"
                        detail = "no_lock_file"

                elif atype == "astrbot":
                    base_url = agent.get("base_url", "")
                    if base_url:
                        try:
                            async with httpx.AsyncClient(timeout=2) as client:
                                resp = await client.get(f"{base_url}/health")
                                status = "online" if resp.status_code < 500 else "degraded"
                                detail = f"http_{resp.status_code}"
                        except (httpx.TimeoutException, httpx.ConnectError) as e:
                            status = "offline"
                            detail = str(e)[:50]
                    else:
                        status = "offline"
                        detail = "no_base_url"

                elif atype == "generic":
                    pid_file = agent.get("pid_file", "")
                    if pid_file and os.path.exists(pid_file):
                        with open(pid_file) as f:
                            pid = f.read().strip()
                        if pid and pid.isdigit():
                            status = "online" if os.path.exists(f"/proc/{pid}") else "offline"
                            detail = f"pid={pid}" if status == "online" else "pid_stale"
                        else:
                            status = "offline"
                            detail = "invalid_pid_file"
                    else:
                        status = "offline"
                        detail = "no_pid_file"

                else:
                    status = "unknown"
                    detail = f"unsupported_type:{atype}"

            except Exception as e:
                status = "error"
                detail = str(e)[:50]

            results.append({
                "id": aid,
                "type": atype,
                "status": status,
                "detail": detail,
                "capabilities": agent.get("capabilities", []),
            })

        return {"agents": results}

    # ── 日志聚合 /admin/logs ──
    @app.get("/admin/logs")
    async def admin_logs(request: Request):
        """聚合所有声明 Agent 的日志，按时间戳合并排序。
        查询参数:
        - agent: 按 agent id 过滤（逗号分隔）
        - level: 按日志级别过滤（DEBUG, INFO, WARN, ERROR，逗号分隔）
        - lines: 每个 Agent 最多读取行数（默认 100）
        - since: 只返回此时间戳之后的日志（ISO 格式）
        """
        params = dict(request.query_params)
        filter_agents = params.get("agent", "").split(",") if params.get("agent") else []
        filter_levels = params.get("level", "").upper().split(",") if params.get("level") else []
        max_lines = int(params.get("lines", 100))
        since_str = params.get("since", "")

        agents = cfg.get("agents", [])
        if filter_agents:
            agents = [a for a in agents if a.get("id") in filter_agents]
        if not agents:
            return {"logs": [], "total": 0, "agents_checked": 0}

        all_entries = []
        errors = []

        for agent in agents:
            aid = agent.get("id", "unknown")
            atype = agent.get("type", "generic")
            try:
                if agent.get("container_name") or agent.get("container_id") or agent.get("compose_project"):
                    # 容器化智能体（最高优先级）— 通过 docker logs 获取日志
                    container = agent.get("container_name") or agent.get("container_id")
                    if not container and agent.get("compose_project"):
                        try:
                            from ops_gateway_core.ops.agent_discovery import _docker
                            ok, out = _docker("compose", "-p", agent["compose_project"], "ps", "-q",
                                              agent.get("compose_service", ""))
                            container = out.splitlines()[0] if ok and out.strip() else ""
                        except Exception:
                            container = ""
                    if container:
                        try:
                            r = subprocess.run(
                                ["docker", "logs", "--tail", str(max_lines), "-t", container],
                                capture_output=True, text=True, timeout=10,
                            )
                            raw = r.stdout + r.stderr
                            for line in raw.split("\n")[-max_lines:]:
                                if not line.strip():
                                    continue
                                parsed = _parse_log_line(line, aid, "docker")
                                if parsed and _log_matches(parsed, filter_levels, since_str):
                                    all_entries.append(parsed)
                        except Exception as e:
                            errors.append({"agent_id": aid, "error": f"docker logs: {str(e)[:80]}"})
                    else:
                        errors.append({"agent_id": aid, "error": "docker logs: container 解析失败"})

                elif atype == "openhands":
                    ws = agent.get("workspace", "")
                    log_dirs = [
                        os.path.join(ws, "logs"),
                        os.path.join(ws, "log"),
                        ws,
                    ]
                    seen = set()
                    for ld in log_dirs:
                        if not os.path.isdir(ld):
                            continue
                        for fname in sorted(os.listdir(ld)):
                            if not fname.endswith((".log", ".txt", ".out", ".err")):
                                continue
                            fpath = os.path.join(ld, fname)
                            if fpath in seen:
                                continue
                            seen.add(fpath)
                            try:
                                with open(fpath, errors="replace") as f:
                                    lines = f.readlines()
                            except Exception:
                                continue
                            for line in lines[-max_lines:]:
                                parsed = _parse_log_line(line, aid, fname)
                                if parsed and _log_matches(parsed, filter_levels, since_str):
                                    all_entries.append(parsed)

                elif atype == "astrbot":
                    base_url = agent.get("base_url", "")
                    if base_url:
                        try:
                            async with httpx.AsyncClient(timeout=5) as client:
                                resp = await client.get(f"{base_url}/logs")
                                if resp.status_code == 200:
                                    raw = resp.text
                                    for line in raw.split("\n")[-max_lines:]:
                                        parsed = _parse_log_line(line, aid, "remote")
                                        if parsed and _log_matches(parsed, filter_levels, since_str):
                                            all_entries.append(parsed)
                        except Exception as e:
                            errors.append({"agent_id": aid, "error": f"remote fetch: {str(e)[:80]}"})

                elif atype == "generic":
                    # 尝试读日志目录（约定 workspace/logs/ 或 command 所在目录的 logs/）
                    ws = agent.get("workspace", "")
                    candidate_dirs = []
                    if ws:
                        candidate_dirs = [
                            os.path.join(ws, "logs"),
                            os.path.join(ws, "log"),
                        ]
                    pid_file = agent.get("pid_file", "")
                    if pid_file:
                        pid_dir = os.path.dirname(pid_file)
                        candidate_dirs.append(os.path.join(pid_dir, "logs"))
                    seen = set()
                    for ld in candidate_dirs:
                        if not os.path.isdir(ld):
                            continue
                        for fname in sorted(os.listdir(ld)):
                            if not fname.endswith((".log", ".txt", ".out", ".err")):
                                continue
                            fpath = os.path.join(ld, fname)
                            if fpath in seen:
                                continue
                            seen.add(fpath)
                            try:
                                with open(fpath, errors="replace") as f:
                                    lines = f.readlines()
                            except Exception:
                                continue
                            for line in lines[-max_lines:]:
                                parsed = _parse_log_line(line, aid, fname)
                                if parsed and _log_matches(parsed, filter_levels, since_str):
                                    all_entries.append(parsed)

            except Exception as e:
                errors.append({"agent_id": aid, "error": str(e)[:80]})

        # 按时间戳合并排序
        all_entries.sort(key=lambda x: x.get("timestamp", ""))

        # 截断总行数
        total = len(all_entries)
        if total > 5000:
            all_entries = all_entries[:5000]

        return {
            "logs": all_entries,
            "total": total,
            "returned": len(all_entries),
            "agents_checked": len(agents),
            "errors": errors if errors else None,
        }

    # ── 执行者-检查者模式：创建检查任务 fiber ──
    @app.post("/admin/fiber/check")
    async def admin_fiber_check(request: Request):
        """创建检查任务 fiber。
        执行者完成任务后，L3 大脑调用此端点创建检查任务。
        检查者通过只读工具验收结果：
        - 通过 → commit 检查任务，undo_log 合并到执行者 fiber
        - 不通过 → fail 检查任务，触发执行者 fiber 级联回滚

        请求体:
        {
            "executor_fiber_id": 1,       # 执行者的 fiber ID
            "checker_agent_id": "hermes-checker",
            "description": "检查邮件发送结果",
            "validation_mode": "adaptive",  # off | conservative | adaptive
            "confidence": 0.6               # 仅 adaptive 使用
        }
        """
        body = await request.json()
        executor_fiber_id = body.get("executor_fiber_id")
        checker_agent_id = body.get("checker_agent_id", "checker")
        description = body.get("description", "检查任务")
        validation_mode = body.get("validation_mode", "adaptive")
        confidence = body.get("confidence", 1.0)

        # 校验执行者 fiber 存在
        if executor_fiber_id is not None and executor_fiber_id not in _fiber_runtime.fiber_all():
            raise HTTPException(status_code=404, detail=f"executor fiber {executor_fiber_id} not found")

        # 根据 validation mode 判断是否需要检查
        need_check = True
        if validation_mode == "off":
            need_check = False
        elif validation_mode == "adaptive":
            threshold = cfg.get("validation", {}).get("confidence_threshold", 0.7)
            need_check = confidence < threshold

        if not need_check:
            return {
                "check_fiber_id": None,
                "skipped": True,
                "reason": f"validation_mode={validation_mode}, confidence={confidence}",
            }

        # 创建检查任务 fiber，挂在执行者 fiber 下
        check_fid = fiber_create(
            agent_id=checker_agent_id,
            description=f"[检查] {description}",
            parent_id=executor_fiber_id,
        )

        return {
            "check_fiber_id": check_fid,
            "skipped": False,
            "executor_fiber_id": executor_fiber_id,
            "description": f"[检查] {description}",
            "status": "active",
        }

    # MCP toggle 的 fiber 感知版本
    @app.post("/admin/mcp/toggle")
    async def admin_mcp_toggle(request: Request):
        """MCP 工具：切换 provider 启用/禁用状态，走审批缓存。
        支持 fiber_id 参数，操作注册到 fiber 而非全局 undo_stack。
        """
        body = await request.json()
        agent_id = body.get("agent_id", "unknown")
        pool_name = body.get("pool", "")
        provider_name = body.get("provider", "")
        reason = body.get("reason", "")
        fiber_id = body.get("fiber_id")  # optional

        if not pool_name or not provider_name:
            raise HTTPException(status_code=422, detail="pool and provider required")

        # 检查 provider 是否存在
        found = False
        for pn, pc in cfg.get("pools", {}).items():
            for pv in pc.get("providers", []):
                if pv["name"] == provider_name and pn == pool_name:
                    found = True
                    break
        if not found:
            raise HTTPException(status_code=404, detail=f"provider '{provider_name}' not found in pool '{pool_name}'")

        # 计算操作 hash（同一 agent 同一 provider 同一操作免审）
        action_hash = hashlib.md5(f"{agent_id}:{provider_name}:toggle".encode()).hexdigest()
        now = time.time()
        cached = action_hash in _approval_cache and _approval_cache[action_hash] > now

        if not cached:
            _approval_cache[action_hash] = now + _APPROVAL_TTL

        # 执行 toggle
        with _lock:
            if provider_name in _disabled_providers:
                _disabled_providers.discard(provider_name)
                revert = lambda n=provider_name: _disabled_providers.add(n)
                status = "enabled"
            else:
                _disabled_providers.add(provider_name)
                revert = lambda n=provider_name: _disabled_providers.discard(n)
                status = "disabled"

        # 注册撤销：优先 fiber，无则全局栈
        desc = f"MCP {'启用' if status == 'enabled' else '禁用'} {provider_name} (by {agent_id})"
        if fiber_id is not None:
            ok = fiber_register(fiber_id, desc, revert)
            if not ok:
                # fiber 不存在或已终止，回退到全局栈
                undo_register(desc, revert)
        else:
            undo_register(desc, revert)

        return {
            "approved": True,
            "action": "toggle",
            "provider": provider_name,
            "pool": pool_name,
            "status": status,
            "reason": reason,
            "cached": cached,
            "agent_id": agent_id,
            "fiber_id": fiber_id,
        }

    # ── 统一插件调用 /v1/plugins/{id}/call ──
    @app.post("/v1/plugins/{plugin_id}/call")
    async def v1_plugins_call(plugin_id: str, request: Request):
        """统一插件调用入口。所有智能体通过此端点调用插件。
        网关根据 execution 模式适配（http/cli），结果通过 Fiber 树可逆。
        """
        body = await request.json()
        agent_id = body.get("agent_id", "unknown")
        params = body.get("params", {})
        fiber_id = body.get("fiber_id")  # optional: 挂到现有 fiber 下
        reason = body.get("reason", "")

        # 1. 查找插件
        plugins = cfg.get("plugins", [])
        plugin = next((p for p in plugins if p["id"] == plugin_id), None)
        if not plugin:
            raise HTTPException(status_code=404, detail=f"plugin '{plugin_id}' not found")

        # 2. 校验 capabilities（调用者必须有插件所需的能力）
        plugin_caps = set(plugin.get("capabilities", []))
        agent_caps = set()
        if plugin_caps:
            # 查找调用者声明的 capabilities
            if "caller" in body:
                agent_caps = set(body["caller"].get("capabilities", []))
            else:
                # 从 agents 声明中查找
                agents_cfg = cfg.get("agents", [])
                caller_cfg = next((a for a in agents_cfg if a.get("id") == agent_id), None)
                if caller_cfg:
                    agent_caps = set(caller_cfg.get("capabilities", []))
            if not agent_caps:
                # 未知调用者，只允许 read 插件
                if plugin_caps - {"read"}:
                    raise HTTPException(status_code=403,
                        detail=f"unknown agent '{agent_id}' cannot call plugin with capabilities: {plugin_caps}")
            else:
                missing = plugin_caps - agent_caps
                if missing:
                    raise HTTPException(status_code=403,
                        detail=f"agent '{agent_id}' missing capabilities: {missing}")

        # 3. 审批缓存 + 全局去重 key
        action_body = {}
        for k, v in params.items():
            action_body[k] = v
        params_json = json.dumps(action_body, sort_keys=True)
        action_hash = hashlib.md5(f"{agent_id}:{plugin_id}:{params_json}".encode()).hexdigest()
        now = time.time()
        cached = action_hash in _approval_cache and _approval_cache[action_hash] > now
        if not cached:
            _approval_cache[action_hash] = now + _APPROVAL_TTL

        # 4. 全局去重检测（跨分支，Root 级）
        params_hash = hashlib.md5(f"{plugin_id}:{params_json}".encode()).hexdigest()
        global_entry = _global_call_lookup(plugin_id, params_hash)
        if global_entry:
            # 重复调用，不执行
            return {
                "plugin_id": plugin_id,
                "status": "duplicate",
                "error": "duplicate_call_detected",
                "params_hash": params_hash,
                "first_executed_at": global_entry.get("timestamp"),
                "first_fiber_id": global_entry.get("fiber_id"),
            }

        # 5. 创建 fiber 子任务
        timeout = plugin.get("timeout", 30)
        concurrent = plugin.get("concurrent", False)
        fid = fiber_create(
            agent_id=agent_id,
            description=f"[插件] {plugin.get('display_name', plugin_id)}",
            parent_id=fiber_id,
            capabilities=list(agent_caps) if agent_caps else None,
        )

        # 记录本次调用到 fiber 的 call_history
        call_entry = {"plugin_id": plugin_id, "params_hash": params_hash, "time": time.time()}
        if fid in _fiber_runtime.fiber_all():
            _fiber_runtime.fiber_get(fid).call_history.append(call_entry)
        if fiber_id is not None and fiber_id in _fiber_runtime.fiber_all():
            _fiber_runtime.fiber_get(fiber_id).call_history.append(call_entry)

        # 6. 排队调度 + 执行插件
        concurrency = plugin.get("concurrency", "parallel")
        resource_key = plugin.get("resource_lock_key", plugin_id)
        throttle_limit = plugin.get("throttle_limit", 0)
        result = {}
        error = None

        if concurrency == "serial" and resource_key:
            # 串行：按 resource_lock_key 分组加锁
            if resource_key not in _serial_locks:
                _serial_locks[resource_key] = asyncio.Lock()
            lock = _serial_locks[resource_key]
            async with lock:
                result, error = await _execute_plugin(plugin, plugin_id, params, timeout)

        elif concurrency == "throttle" and throttle_limit > 0:
            # 限流：滑动窗口检查
            now = time.time()
            window = _throttle_windows.setdefault(plugin_id, [])
            _throttle_windows[plugin_id] = [t for t in window if now - t < 1.0]
            if len(_throttle_windows[plugin_id]) >= throttle_limit:
                error = f"rate limit exceeded: {throttle_limit}/s"
            else:
                _throttle_windows[plugin_id].append(now)
                result, error = await _execute_plugin(plugin, plugin_id, params, timeout)

        else:
            # parallel：直接执行
            result, error = await _execute_plugin(plugin, plugin_id, params, timeout)

        # 6. 注册逆操作（如果插件失败，不注册逆操作，Fiber fail 只需回滚自己）
        if error:
            # 失败：fail 此 fiber
            fiber_fail(fid, cascade_parent=False)
            return {
                "plugin_id": plugin_id,
                "status": "error",
                "error": error,
                "fiber_id": fid,
                "cached": cached,
            }

        # ── 任务3: 工具调用级动态校验 ──
        validation_fid = fiber_create(
            agent_id=plugin.get("provider", "gateway"),
            description=f"[校验] {plugin.get('display_name', plugin_id)} 结果",
            parent_id=fid,
            capabilities=["validate"],
        )

        validation_errors = []
        # 3a. Schema 校验（匹配 output_schema）
        output_schema = plugin.get("output_schema", {})
        if output_schema:
            for field, expected_type in output_schema.items():
                actual = result.get(field)
                if actual is None:
                    validation_errors.append(f"缺少字段 {field}")
                    continue
                if expected_type in ("string", "integer", "array", "object", "boolean", "number"):
                    type_map = {
                        "string": str, "integer": int, "number": (int, float),
                        "array": list, "object": dict, "boolean": bool,
                    }
                    expected_py = type_map.get(expected_type, str)
                    if not isinstance(actual, expected_py):
                        validation_errors.append(f"字段 {field} 期望 {expected_type}，实际 {type(actual).__name__}")

        if validation_errors:
            fiber_fail(validation_fid, cascade_parent=True)  # 级联回滚 tool_exec
            return {
                "plugin_id": plugin_id,
                "status": "validation_failed",
                "result": result,
                "validation_errors": validation_errors,
                "fiber_id": fid,
                "validation_fiber_id": validation_fid,
                "cached": cached,
            }

        # 3b. 检查者语义验证（若配置了 checker_agent，通过 HTTP 调用验证）
        # 当前仅做 schema 校验；检查者 HTTP 端点待后续接入
        checker_agent = cfg.get("validation", {}).get("checker_agent", "")
        if checker_agent:
            # 预留：检查者 Agent 的 HTTP 验证接口 — 当前仅做 schema 校验
            pass

        # 提交校验节点
        fiber_commit(validation_fid)

        # 写入全局去重表（全树可见）
        result_preview = json.dumps(result, ensure_ascii=False)[:200]
        _global_call_add(plugin_id, params_hash, fid, result_preview)

        # 成功：注册逆操作
        inverse_id = plugin.get("inverse")
        if inverse_id:
            # 注册撤销：调用逆插件
            def _make_inverse(pid, p, aid):
                def _inverse():
                    # 查找逆插件
                    inv_plugin = next((pl for pl in cfg.get("plugins", []) if pl["id"] == pid), None)
                    if not inv_plugin:
                        return
                    inv_exec = inv_plugin.get("execution", "http")
                    inv_params = {"_original_result": result, **p}
                    if inv_exec == "http":
                        inv_url = _format_string(inv_plugin.get("endpoint", ""), inv_params)
                        try:
                            asyncio.run(httpx.AsyncClient(timeout=10).post(inv_url, json=inv_params))
                        except Exception:
                            pass
                    elif inv_exec == "cli":
                        inv_cmd = _format_string(inv_plugin.get("command", ""), inv_params)
                        try:
                            subprocess.run(inv_cmd, shell=True, capture_output=True, timeout=10)
                        except Exception:
                            pass
                return _inverse
            fiber_register(fid, f"逆操作: {plugin.get('display_name', plugin_id)} 调用 {inverse_id}",
                           _make_inverse(inverse_id, params, agent_id))

        # 提交 fiber（合并到父或全局栈）
        fiber_commit(fid)

        return {
            "plugin_id": plugin_id,
            "status": "ok",
            "result": result,
            "fiber_id": fid,
            "validation_fiber_id": validation_fid,
            "cached": cached,
        }

    # ── 第二名检查者（Runner-up Scoring）──
    # 自适应采样频率：冷启动每次都审 → 稳态概率衰减 → 大变量强制复审
    # 默认开启，无需配置

    _scoring_state = {}  # {provider: {"count", "last", "last_runners", "recent_scores", "variance_boost_remaining"}}

    return app

