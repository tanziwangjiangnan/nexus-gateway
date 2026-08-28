"""健康探测 — provider/model 可用性探测。

v3.3: 从 gateway.py 拆分。依赖通过参数/模块级函数注入。
"""

import json
import os
import subprocess
import time

from hermes_cfg import get_db

# 模块级可覆盖的路径（由 gateway 初始化）
QQ_PUSH = "/root/experiments/qq-push.sh"
QQ_TARGET = "1310893084"


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
                      + (f" {r['error']}" if not r["ok"] else ""))
    failed = [r for r in results if not r["ok"]]
    if failed and os.path.exists(QQ_PUSH):
        msg = "🚨 模型池探测异常:\n" + "\n".join(f"  ❌ {r['model']}: {r['error']}" for r in failed)
        try:
            subprocess.run(["bash", QQ_PUSH, QQ_TARGET, msg], capture_output=True, timeout=15)
        except Exception:
            pass
    return results