#!/usr/bin/env python3
"""模型池统一网关 v2 — 三资源池版
独立基础设施，所有智能体平等消费。
OpenAI 兼容接口，按 model 名路由到三池，池内权重轮询，跨池故障转移。

用法:
  python3 gateway.py                    # 启动网关 :8646
  python3 gateway.py probe              # 一次性健康探测所有模型
  python3 gateway.py probe --watch      # 持续探测，异常→QQ告警
  python3 gateway.py models             # 列出模型目录
  python3 gateway.py usage              # 查看用量
  python3 gateway.py git-log            # 配置逆栈：commit 历史
  python3 gateway.py git-diff           # 配置逆栈：未提交变更
  python3 gateway.py sync-runtime       # 运行时同步（发 SIGHUP 重载）
  python3 gateway.py undo               # 运行时逆栈：撤销最后一条操作
  python3 gateway.py undo-list          # 运行时逆栈：查看所有操作

API 端点:
  GET  /v1/models                → 模型目录（含池/provider/健康/用量）
  POST /v1/chat/completions      → OpenAI 兼容，三池路由 + 故障转移
  GET  /health                   → 网关自身健康检查
  GET  /metrics                  → Prometheus 指标
  GET  /admin/pools              → 查看各池/provider 状态
  POST /admin/pools/{pool}/providers/{provider}/toggle  → 启停 provider
  GET  /admin/undo               → 运行时逆栈：撤销最后一条操作
  GET  /admin/undo-list          → 运行时逆栈：查看所有操作
  POST /admin/mcp/toggle         → MCP 工具：Agent 调 toggle，走审批缓存
  GET  /admin/mcp/approvals      → 查看审批缓存
  GET  /admin/mcp/status         → MCP 总览：熔断+权重+审批（含错误率）
"""
import datetime
import json
import os
import random
import signal
import sqlite3
import subprocess
import sys
import time
import threading
import yaml

# ── 路径 ──
BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE, "gateway.yaml")
DB = os.path.join(BASE, "gateway.db")
QQ_PUSH = "/root/experiments/qq-push.sh"
QQ_TARGET = "1310893084"

# ── 全局运行时状态 ──
_config = {}                    # 当前配置
_disabled_providers = set()     # 被自动或手动禁用的 provider
_rate_limit_buckets = {}        # {provider_name: [t1, t2, ...]}
_undo_stack = []                # 运行时逆栈：[(description, callable), ...]
_dynamic_weights = {}           # {provider_name: effective_weight} 由熔断线程更新
_approval_cache = {}            # {(agent_id, action_hash): expiry_timestamp}
_pending_approvals = {}         # {action_id: {action, params, agent_id}}
_lock = threading.Lock()

# ── 配置加载 ──
def load_config(path=None):
    path = path or CONFIG_PATH
    with open(path) as f:
        cfg = yaml.safe_load(f)
    # 解析 ${VAR} 环境变量引用
    for pname, pcfg in cfg.get("providers", {}).items():
        key = pcfg.get("api_key", "")
        if isinstance(key, str) and key.startswith("${") and key.endswith("}"):
            env_name = key[2:-1]
            val = os.environ.get(env_name, "")
            if val:
                pcfg["api_key"] = val
    return cfg

def reload_config():
    """重载 YAML 配置，同步运行时状态，清空运行时逆栈。
    
    幂等补偿说明：
    1. YAML 文件 → 原地替换 _config（闭包引用同步）
    2. _disabled_providers.clear() — 运行时禁用状态重置
       （防止 git revert 后 YAML 变回但某 provider 仍被禁用）
    3. _undo_stack.clear() — 运行时逆栈清空，新配置是新起点
    4. _rate_limit_buckets 不清（滑动窗口，自动过期）
    """
    global _config, _disabled_providers
    new_cfg = load_config()
    if _config:
        _config.clear()
        _config.update(new_cfg)
    else:
        _config = new_cfg
    _disabled_providers.clear()
    undo_clear("配置重载")
    return _config

# ── 数据库 ──
def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS registry (
        model TEXT PRIMARY KEY, pool TEXT, provider TEXT NOT NULL,
        tier TEXT DEFAULT 'B', status TEXT DEFAULT 'unknown',
        notes TEXT DEFAULT '', created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT, model TEXT NOT NULL,
        pool TEXT, provider TEXT, prompt_tokens INTEGER DEFAULT 0,
        completion_tokens INTEGER DEFAULT 0, ok INTEGER DEFAULT 1,
        called_at TEXT DEFAULT (datetime('now'))
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS health_log (
        model TEXT NOT NULL, pool TEXT, provider TEXT,
        ok INTEGER NOT NULL, latency_ms INTEGER DEFAULT 0,
        error TEXT DEFAULT '', checked_at TEXT DEFAULT (datetime('now'))
    )""")
    conn.commit()
    return conn

def init_registry(cfg):
    conn = get_db()
    for pool_name, pool_cfg in cfg.get("pools", {}).items():
        for pv in pool_cfg.get("providers", []):
            provider_name = pv["name"]
            for model in pv.get("models", []):
                conn.execute("""INSERT OR IGNORE INTO registry
                    (model, pool, provider, tier, status, notes)
                    VALUES (?, ?, ?, ?, 'unknown', ?)""",
                    (model, pool_name, provider_name,
                     "A" if pool_name == "pool_a" else "B" if pool_name == "pool_b" else "C",
                     pool_cfg.get("description", "")))
    conn.commit()
    conn.close()

# ── 自动熔断 + 动态权重（后台线程，每 30s） ──
def _circuit_breaker_loop(cfg):
    """后台线程：滑动窗口错误率 >20% → 自动 disable，<10% → 恢复。
    同时根据成功率更新动态权重。
    """
    while True:
        time.sleep(30)
        try:
            conn = get_db()
            rows = conn.execute("""
                SELECT provider,
                       COUNT(*) as total,
                       SUM(ok) as success
                FROM usage
                WHERE called_at > datetime('now', '-5 minutes')
                GROUP BY provider
            """).fetchall()
            conn.close()
            now = time.time()
            for r in rows:
                name = r["provider"]
                total = r["total"]
                if total < 5:  # 样本不足，不动作
                    continue
                success = r["success"] or 0
                err_rate = 1.0 - (success / total)
                is_disabled = name in _disabled_providers

                # 熔断：错误率 >20% → 自动禁用
                if err_rate > 0.20 and not is_disabled:
                    with _lock:
                        _disabled_providers.add(name)
                    undo_register(f"自动熔断禁用 {name} (err={err_rate:.0%})",
                                  lambda n=name: _disabled_providers.discard(n))
                    print(f"🔌 熔断: {name} 错误率 {err_rate:.0%} → 已禁用")

                # 恢复：错误率 <10% 且是被熔断禁用的 → 自动恢复
                elif err_rate < 0.10 and is_disabled:
                    with _lock:
                        _disabled_providers.discard(name)
                    print(f"🔌 恢复: {name} 错误率 {err_rate:.0%} → 已启用")

                # 动态权重：base_weight × (1 - err_rate)，保底 0.1
                base = 1.0
                for pc in cfg.get("pools", {}).values():
                    for pv in pc.get("providers", []):
                        if pv["name"] == name:
                            base = pv.get("weight", 1.0)
                            break
                _dynamic_weights[name] = max(base * (1.0 - err_rate), 0.1)

        except Exception as e:
            print(f"⚠️ 熔断循环异常: {e}")

# ── 运行时逆栈（任务级幂等补偿） ──
def undo_register(description, revert_callable):
    """注册运行时操作的撤销回调。每个原子操作应有对应的逆操作。"""
    _undo_stack.append((description, revert_callable))

def undo_pop():
    """弹出并执行最后一条撤销回调。"""
    if not _undo_stack:
        return False, "undo_stack 为空"
    desc, fn = _undo_stack.pop()
    try:
        fn()
        return True, f"已撤销: {desc}"
    except Exception as e:
        _undo_stack.append((desc, fn))  # 失败放回，留给重试
        return False, f"撤销失败 ({desc}): {e}"

def undo_clear(reason=""):
    """清空逆栈（配置热加载时调用，因为新配置是新起点）。"""
    _undo_stack.clear()

# ── 路由引擎 ──

def find_model_config(cfg, model):
    """遍历所有池查找 model 所属的 (pool_name, provider_config)"""
    for pool_name, pool_cfg in cfg.get("pools", {}).items():
        for pv in pool_cfg.get("providers", []):
            if model in pv.get("models", []):
                return pool_name, pool_cfg, pv
    return None, None, None

def select_pool_by_keywords(cfg, messages_text):
    """关键词匹配 → 返回 pool_name 或 None"""
    for rule in cfg.get("routing", {}).get("rules", []):
        for kw in rule.get("keywords", []):
            if kw in messages_text:
                return rule["pool"]
    return None

def select_provider_by_weight(providers, model=None):
    """按权重随机选一个 provider，跳过禁用的；若指定 model 则只选有该模型的"""
    candidates = [p for p in providers if p["name"] not in _disabled_providers]
    if model:
        candidates = [p for p in candidates if model in p.get("models", [])]
    if not candidates:
        return None
    total = 0
    weights = []
    for p in candidates:
        # 动态权重优先，无则用 YAML 静态权重
        w = _dynamic_weights.get(p["name"]) or p.get("weight", 1)
        w = max(w, 0.1)  # 保底
        weights.append(w)
        total += w
    r = random.uniform(0, total)
    upto = 0
    for i, p in enumerate(candidates):
        upto += weights[i]
        if r <= upto:
            return p
    return candidates[-1]

def check_rate_limit(provider_name, max_rps):
    """滑动窗口限流，返回 True=通过 False=限流"""
    if not max_rps or max_rps <= 0:
        return True
    now = time.time()
    with _lock:
        bucket = _rate_limit_buckets.setdefault(provider_name, [])
        # 清理窗口外的记录
        cutoff = now - 1.0
        bucket[:] = [t for t in bucket if t > cutoff]
        if len(bucket) >= max_rps:
            return False
        bucket.append(now)
    return True

def call_provider_http(provider_cfg, model, messages, stream=False, **kwargs):
    """调用后端 provider，返回 (status_code, body_bytes_or_str, latency_ms, error)"""
    import urllib.request
    import urllib.error
    api = provider_cfg["api"].rstrip("/")
    key = provider_cfg["api_key"]
    body = {"model": model, "messages": messages, "stream": stream, **kwargs}
    req = urllib.request.Request(
        f"{api}/chat/completions",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
        method="POST",
    )
    try:
        t0 = time.time()
        resp = urllib.request.urlopen(req, timeout=120)
        latency = int((time.time() - t0) * 1000)
        data = resp.read()
        return resp.status, data, latency, None
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else str(e)
        return e.code, err_body.encode(), 0, str(e)
    except Exception as e:
        return 0, str(e).encode(), 0, str(e)

# ── 健康探测 ──
def probe_model(cfg, model, pool_name, provider_cfg, pv):
    if not provider_cfg or not provider_cfg.get("api_key"):
        return {"model": model, "ok": False, "error": "no key"}
    status, body, latency, err = call_provider_http(
        provider_cfg, model, [{"role": "user", "content": "ping"}], max_tokens=5)
    ok = status == 200
    error = err or ("" if ok else f"HTTP {status}")
    conn = get_db()
    conn.execute("INSERT INTO health_log (model, pool, provider, ok, latency_ms, error) VALUES (?,?,?,?,?,?)",
                 (model, pool_name, pv["name"], 1 if ok else 0, latency, error))
    conn.execute("UPDATE registry SET status=?, updated_at=datetime('now') WHERE model=?",
                 ("healthy" if ok else "down", model))
    conn.commit()
    conn.close()
    return {"model": model, "pool": pool_name, "ok": ok, "latency_ms": latency, "error": error}

def probe_all(cfg, watch=False):
    results = []
    for pool_name, pool_cfg in cfg.get("pools", {}).items():
        for pv in pool_cfg.get("providers", []):
            provider_cfg = cfg.get("providers", {}).get(pv["name"])
            for model in pv.get("models", []):
                r = probe_model(cfg, model, pool_name, provider_cfg, pv)
                results.append(r)
                print(f"  {r['model']:30s} {'✅' if r['ok'] else '❌'} {r.get('latency_ms',0):>5}ms"
                      + (f" {r['error']}" if not r['ok'] else ""))
    failed = [r for r in results if not r["ok"]]
    if failed and os.path.exists(QQ_PUSH):
        msg = "🚨 模型池探测异常:\n" + "\n".join(f"  ❌ {r['model']}: {r['error']}" for r in failed)
        try:
            subprocess.run(["bash", QQ_PUSH, QQ_TARGET, msg], capture_output=True, timeout=15)
        except Exception:
            pass
    return results

# ── CLI 命令 ──
def cmd_models(cfg):
    conn = get_db()
    rows = conn.execute("""SELECT r.model, r.pool, r.provider, r.tier, r.status,
                                  COALESCE(SUM(u.prompt_tokens+u.completion_tokens), 0) as tokens
                           FROM registry r LEFT JOIN usage u ON u.model=r.model
                           GROUP BY r.model ORDER BY r.pool, r.model""").fetchall()
    conn.close()
    print(f"{'模型名':<28s} {'池':<8s} {'Provider':<14s} {'档位':<4s} {'状态':<10s} {'今日用量'}")
    print("─" * 85)
    for r in rows:
        print(f"{r['model']:<28s} {r['pool']:<8s} {r['provider']:<14s} {r['tier']:<4s} {r['status']:<10s} {r['tokens']:>8d} tokens")
    print(f"\n共 {len(rows)} 个模型")

def cmd_usage(cfg):
    conn = get_db()
    today = datetime.date.today().isoformat()
    rows = conn.execute("""SELECT model, pool, provider, SUM(prompt_tokens) as p, SUM(completion_tokens) as c,
                                  COUNT(*) as n, SUM(ok) as ok, SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) as fail
                           FROM usage WHERE date(called_at)=?
                           GROUP BY model ORDER BY (p+c) DESC""", (today,)).fetchall()
    total = conn.execute("""SELECT SUM(prompt_tokens) as p, SUM(completion_tokens) as c
                            FROM usage WHERE date(called_at)=?""", (today,)).fetchone()
    conn.close()
    if not rows:
        print("今日无用量")
        return
    print(f"📊 今日用量 ({today})")
    print(f"{'模型名':<28s} {'池':<8s} {'Provider':<14s} {'Prompt':>8s} {'Completion':>10s} {'调用':>5s} {'成功':>5s}")
    print("─" * 85)
    for r in rows:
        print(f"{r['model']:<28s} {r['pool']:<8s} {r['provider']:<14s} {r['p']:>8d} {r['c']:>10d} {r['n']:>5d} {r['ok']:>5d}")
    if total:
        print(f"\n总计: {total['p']} prompt + {total['c']} completion = {total['p']+total['c']} tokens")

# ── FastAPI 应用 ──
def create_app(cfg):
    from fastapi import FastAPI, Request, HTTPException
    from fastapi.responses import JSONResponse, StreamingResponse, Response
    import httpx

    app = FastAPI(title="模型池网关 v2", version="0.2.0")

    # ── 鉴权中间件 ──
    @app.middleware("http")
    async def auth_check(request: Request, call_next):
        if request.url.path in ("/health", "/metrics"):
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

    # ── 模型列表 ──
    @app.get("/v1/models")
    async def list_models():
        conn = get_db()
        rows = conn.execute("""SELECT r.model, r.pool, r.provider, r.tier, r.status,
                                      COALESCE(SUM(u.prompt_tokens+u.completion_tokens), 0) as tokens
                               FROM registry r LEFT JOIN usage u ON u.model=r.model
                               GROUP BY r.model ORDER BY r.pool, r.model""").fetchall()
        conn.close()
        data = [{"id": r["model"], "object": "model", "pool": r["pool"],
                 "provider": r["provider"], "tier": r["tier"],
                 "status": r["status"], "today_tokens": r["tokens"]} for r in rows]
        return {"object": "list", "data": data}

    # ── 聊天补全（三池路由核心） ──
    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        t0 = time.time()
        body = await request.json()
        model = body.get("model", "DeepSeek-V4-Flash")
        messages = body.get("messages", [])
        stream = body.get("stream", False)
        kwargs = {k: v for k, v in body.items() if k not in ("model", "messages", "stream")}

        # 1. 关键词路由（最高优先级）
        messages_text = json.dumps(messages, ensure_ascii=False)
        kw_pool = select_pool_by_keywords(cfg, messages_text)
        pool_name = kw_pool
        pool_cfg = None

        # 2. 模型名精确匹配
        if not pool_name:
            pool_name, pool_cfg, _ = find_model_config(cfg, model)
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

            # 4. 按权重选 provider（只选有该模型的）
            pv = select_provider_by_weight(pool_cfg.get("providers", []), model=model)
            if not pv:
                last_error = f"pool '{current_pool}' all providers disabled"
                current_pool = pool_cfg.get("fallback")
                continue

            provider_cfg = cfg.get("providers", {}).get(pv["name"])
            if not provider_cfg or not provider_cfg.get("api_key") or provider_cfg["api_key"].startswith("${"):
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
                    req_body = {"model": model, "messages": messages, "stream": stream, **kwargs}
                    resp = await client.post(
                        f"{api}/chat/completions",
                        json=req_body,
                        headers={"Authorization": f"Bearer {provider_cfg['api_key']}"},
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
                        current_pool = pool_cfg.get("fallback")
                        continue

                    app.state.req_counter.labels(pool=current_pool, provider=pv["name"], status="200").inc()
                    app.state.req_duration.labels(provider=pv["name"]).observe(time.time() - t0)
                    return Response(content=resp_body, status_code=status_code, media_type="application/json")

            except httpx.TimeoutException:
                last_error = "timeout"
                current_pool = pool_cfg.get("fallback")
                continue
            except httpx.ConnectError:
                last_error = "unreachable"
                current_pool = pool_cfg.get("fallback")
                continue
            except Exception as e:
                last_error = str(e)
                current_pool = pool_cfg.get("fallback")
                continue

        app.state.req_counter.labels(pool=pool_name, provider=used_provider or "none", status="503").inc()
        app.state.req_duration.labels(provider=used_provider or "none").observe(time.time() - t0)
        raise HTTPException(status_code=503, detail=f"all pools exhausted: {last_error}")

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
        return {"stack": [desc for desc, _ in _undo_stack]}

    # ── MCP 审批回调 ──
    # 让统筹 Agent 通过 HTTP 调用 toggle，走审批缓存（同一 agent 同一操作 5 分钟内免审）
    _APPROVAL_TTL = 300  # 5 分钟

    @app.post("/admin/mcp/toggle")
    async def admin_mcp_toggle(request: Request):
        """MCP 工具：切换 provider 启用/禁用状态，走审批缓存。
        
        请求体: {"agent_id": "hermes", "pool": "pool_a", "provider": "xiaomi", "reason": "rate limit"}
        响应: {"approved": true, "action": "toggle", "provider": "xiaomi", "status": "disabled", "cached": false}
        """
        body = await request.json()
        agent_id = body.get("agent_id", "unknown")
        pool_name = body.get("pool", "")
        provider_name = body.get("provider", "")
        reason = body.get("reason", "")

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
        import hashlib
        action_hash = hashlib.md5(f"{agent_id}:{provider_name}:toggle".encode()).hexdigest()
        now = time.time()
        cached = action_hash in _approval_cache and _approval_cache[action_hash] > now

        if not cached:
            # 新操作 → 需要审批（模拟审批：记录到 pending，但这里直接通过+缓存）
            # ponytail: 真实场景可对接外部审批系统，这里直接缓存授权
            _approval_cache[action_hash] = now + _APPROVAL_TTL

        # 执行 toggle
        with _lock:
            if provider_name in _disabled_providers:
                _disabled_providers.discard(provider_name)
                undo_register(f"MCP 启用 {provider_name} (by {agent_id})",
                              lambda n=provider_name: _disabled_providers.add(n))
                status = "enabled"
            else:
                _disabled_providers.add(provider_name)
                undo_register(f"MCP 禁用 {provider_name} (by {agent_id})",
                              lambda n=provider_name: _disabled_providers.discard(n))
                status = "disabled"

        return {
            "approved": True,
            "action": "toggle",
            "provider": provider_name,
            "pool": pool_name,
            "status": status,
            "reason": reason,
            "cached": cached,
            "agent_id": agent_id,
        }

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

    return app

# ── 主入口 ──
def main():
    global _config
    _config = reload_config()
    os.makedirs(BASE, exist_ok=True)
    init_registry(_config)

    args = sys.argv[1:]

    if not args:
        import uvicorn
        app = create_app(_config)
        # 写 PID 文件
        with open(os.path.join(BASE, "gateway.pid"), "w") as f:
            f.write(str(os.getpid()))
        total_models = sum(len(pv.get("models", []))
                           for pc in _config.get("pools", {}).values()
                           for pv in pc.get("providers", []))
        total_providers = len(_config.get("providers", {}))
        print(f"🔌 模型池网关 v2 启动: http://{_config.get('host','127.0.0.1')}:{_config.get('port',8646)}")
        print(f"   {total_models} 个模型, {total_providers} 个 provider, {len(_config.get('pools',{}))} 个资源池")
        print(f"   SIGHUP 热加载已启用 (kill -HUP {os.getpid()} 重载配置)")
        # 启动后台熔断线程
        t = threading.Thread(target=_circuit_breaker_loop, args=(_config,), daemon=True)
        t.start()
        print(f"   🔄 自动熔断已启用 (30s 滑动窗口, 错误率 >20% 自动禁用)")

        # SIGHUP 热加载 — 自动 git commit 作为逆栈快照
        def _hup(signum, frame):
            try:
                # 先 commit 当前配置作为回滚点
                subprocess.run(
                    ["git", "add", "gateway.yaml", "gateway.py"],
                    cwd=BASE, capture_output=True, timeout=10)
                subprocess.run(
                    ["git", "commit", "--allow-empty", "-m",
                     f"auto-snap before SIGHUP {datetime.datetime.now().strftime('%H:%M:%S')}"],
                    cwd=BASE, capture_output=True, timeout=10)
                reload_config()
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 配置已热加载 (git snap)")
            except Exception as e:
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] 配置重载失败: {e}")
        signal.signal(signal.SIGHUP, _hup)
        try:
            uvicorn.run(app, host=_config.get("host", "127.0.0.1"), port=_config.get("port", 8646), log_level="info")
        finally:
            pid_file = os.path.join(BASE, "gateway.pid")
            if os.path.exists(pid_file):
                os.remove(pid_file)

    elif args[0] == "probe":
        watch = "--watch" in args
        if watch:
            print("🔄 持续探测模型池（每 60s）...")
            while True:
                print(f"\n--- {datetime.datetime.now().strftime('%H:%M:%S')} ---")
                probe_all(_config)
                time.sleep(60)
        else:
            print("🔍 一次性探测模型池...")
            probe_all(_config)

    elif args[0] == "models":
        cmd_models(_config)

    elif args[0] == "usage":
        cmd_usage(_config)

    elif args[0] == "git-log":
        subprocess.run(["git", "log", "--oneline", "-20"], cwd=BASE)

    elif args[0] == "git-diff":
        subprocess.run(["git", "diff", "HEAD", "--", "gateway.yaml"], cwd=BASE)

    elif args[0] == "sync-runtime":
        """向运行中进程发 SIGHUP → 触发 reload_config()
        
        幂等补偿行为：
        1. 重读 YAML 并原地替换 _config
        2. 清空 _disabled_providers（恢复所有 provider 为启用状态）
        3. 清空 _undo_stack（运行时逆栈，新配置是新起点）
        """
        pid_file = os.path.join(BASE, "gateway.pid")
        if os.path.exists(pid_file):
            with open(pid_file) as f:
                pid = int(f.read().strip())
            os.kill(pid, signal.SIGHUP)
            print(f"✅ 已向 PID {pid} 发送 SIGHUP，运行时同步中")
        else:
            print(f"⚠️  未找到 pid 文件，尝试 systemctl reload gateway")
            subprocess.run(["systemctl", "reload", "gateway"])

    elif args[0] == "undo":
        """撤销运行时逆栈的最后一条操作（通过 Admin API）"""
        import urllib.request
        gw_key = _config.get("gateway_key", "")
        req = urllib.request.Request(f"http://127.0.0.1:8646/admin/undo",
                                     headers={"Authorization": f"Bearer {gw_key}"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                icon = "✅" if data.get("ok") else "❌"
                print(f"{icon} {data.get('message', '')}")
        except Exception as e:
            print(f"❌ 调用失败: {e}")

    elif args[0] == "undo-list":
        """查看运行时逆栈（通过 Admin API）"""
        import urllib.request
        gw_key = _config.get("gateway_key", "")
        req = urllib.request.Request(f"http://127.0.0.1:8646/admin/undo-list",
                                     headers={"Authorization": f"Bearer {gw_key}"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                stack = data.get("stack", [])
                if not stack:
                    print("运行时逆栈为空")
                else:
                    for i, desc in enumerate(stack, 1):
                        print(f"  {i}. {desc}")
        except Exception as e:
            print(f"❌ 调用失败: {e}")

    else:
        print(f"未知命令: {args[0]}")

if __name__ == "__main__":
    main()