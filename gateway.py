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

API 端点:
  GET  /v1/models                → 模型目录（含池/provider/健康/用量）
  POST /v1/chat/completions      → OpenAI 兼容，三池路由 + 故障转移
  GET  /health                   → 网关自身健康检查
  GET  /metrics                  → Prometheus 指标
  GET  /admin/pools              → 查看各池/provider 状态
  POST /admin/pools/{pool}/providers/{provider}/toggle  → 启停 provider
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
_disabled_providers = set()     # 被 admin 手动禁用的 provider
_rate_limit_buckets = {}        # {provider_name: [t1, t2, ...]}
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
    global _config, _disabled_providers
    new_cfg = load_config()
    _config = new_cfg
    _disabled_providers.clear()  # 同步：YAML 回滚后运行时状态一并重置
    return new_cfg

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
    total = sum(p.get("weight", 1) for p in candidates)
    r = random.uniform(0, total)
    upto = 0
    for p in candidates:
        upto += p.get("weight", 1)
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
                raise HTTPException(status_code=429, detail=last_error)

            # 6. 发起调用
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
                        return Response(content=resp_body, status_code=status_code, media_type="application/json")

                    # 5xx → fallback 到下一个池
                    if status_code >= 500:
                        last_error = f"HTTP {status_code}"
                        current_pool = pool_cfg.get("fallback")
                        continue

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

        raise HTTPException(status_code=503, detail=f"all pools exhausted: {last_error}")

    # ── Prometheus 指标 ──
    @app.get("/metrics")
    async def metrics():
        from prometheus_client import generate_latest, REGISTRY, Counter, Gauge, Histogram
        # 注册指标（幂等）
        if "gateway_requests_total" not in [m.name for m in REGISTRY.collect()]:
            Counter("gateway_requests_total", "Total requests", ["pool", "provider", "status"])
            Gauge("gateway_pool_healthy", "Pool health 1/0", ["pool"])
            Gauge("gateway_provider_up", "Provider up 1/0", ["provider"])
            Histogram("gateway_request_duration_seconds", "Request latency", ["provider"],
                      buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0])
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
                return {"provider": provider_name, "status": "enabled"}
            else:
                _disabled_providers.add(provider_name)
                return {"provider": provider_name, "status": "disabled"}

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
        # 向运行中的网关进程发 SIGHUP，触发 reload_config()
        pid_file = os.path.join(BASE, "gateway.pid")
        if os.path.exists(pid_file):
            with open(pid_file) as f:
                pid = int(f.read().strip())
            os.kill(pid, signal.SIGHUP)
            print(f"✅ 已向 PID {pid} 发送 SIGHUP，运行时同步中")
        else:
            print(f"⚠️  未找到 pid 文件，尝试 systemctl reload gateway")
            subprocess.run(["systemctl", "reload", "gateway"])

    else:
        print(f"未知命令: {args[0]}")

if __name__ == "__main__":
    main()