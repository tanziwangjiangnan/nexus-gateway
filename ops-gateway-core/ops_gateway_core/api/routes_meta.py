"""对外元信息端点：/chat 页 + /v1/models 模型目录。

参数由 app.py 注入（见 docs/模块约定.md）。
"""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse


def build_meta_router(*,
    Response,
    _disabled_providers,
    cfg,
    db_conn,
):
    """参数由 app.py 注入（见 docs/模块约定.md）；路由内容与抽出前逐字一致。"""
    router = APIRouter()
    @router.get("/chat")
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
    @router.get("/v1/models")
    async def list_models():
        with db_conn() as conn:
            rows = conn.execute("""SELECT r.model, r.pool, r.provider, r.tier, r.status,
                                          COALESCE(SUM(u.prompt_tokens+u.completion_tokens), 0) as tokens
                                   FROM registry r LEFT JOIN usage u ON u.model=r.model
                                   GROUP BY r.model ORDER BY r.pool, r.model""").fetchall()

        # 计算每个 provider 最近 5 分钟的错误率
        with db_conn() as conn2:
            err_rows = conn2.execute("""
                SELECT provider,
                       COUNT(*) as total,
                       SUM(ok) as success
                FROM usage
                WHERE called_at > datetime('now', '-5 minutes')
                GROUP BY provider
            """).fetchall()
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

        # [2026-09-14] 补一个合成的 auto 条目（维护通道）：
        # 客户端若用 /v1/models 校验模型清单，没有它就会拒绝 model=auto。
        data.insert(0, {
            "id": "auto",
            "object": "model",
            "pool": "auto",
            "provider": "auto",
            "tier": "-",
            "status": "active",
            "error_rate": None,
            "today_tokens": 0,
            "description": "维护通道：按稳定优先序自动选择模型（不指定模型即走这里）",
        })
        return {"object": "list", "data": data}

    # ── 聊天补全（三池路由核心） ──

    return router
