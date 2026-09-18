"""健康探测 — provider/model 可用性探测。

v3.3: 从 gateway.py 拆分。依赖通过参数/模块级函数注入。
"""

import json
import os
import re
import subprocess
import time

from ..cfg import get_db

# 模块级可覆盖的路径（由 gateway 初始化）
QQ_PUSH = os.environ.get("HERMES_QQ_PUSH_SCRIPT", "")
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


_IMAGE_MODEL_RE = re.compile(r"(image|imagine)", re.I)


def call_provider_images(provider_cfg, model, timeout=60):
    """图像生成模型专用探测：POST /images/generations。

    [2026-09-13 新增] 图像/视频类模型用 /chat/completions 探测必然失败，
    会被误判为 down（实测 grok-imagine-image 等其实可用）。
    """
    import json as _json
    import urllib.request
    import urllib.error
    api = provider_cfg["api"].rstrip("/")
    key = provider_cfg["api_key"]
    body = {"model": model, "prompt": "a red apple", "n": 1}
    req = urllib.request.Request(
        f"{api}/images/generations",
        data=_json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
        method="POST",
    )
    t0 = time.time()
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        data = resp.read()
        return resp.status, data, int((time.time() - t0) * 1000), None
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else str(e)
        return e.code, err_body.encode(), 0, str(e)
    except Exception as e:
        return 0, str(e).encode(), 0, str(e)

def probe_model(cfg, model, pool_name, provider_cfg, pv):
    if not provider_cfg or not provider_cfg.get("api_key"):
        return {"model": model, "ok": False, "error": "no key"}
    if _IMAGE_MODEL_RE.search(model or ""):
        status, body, latency, err = call_provider_images(provider_cfg, model)
    else:
        status, body, latency, err = call_provider_http(
            provider_cfg, model, [{"role": "user", "content": "ping"}], max_tokens=5)
    ok = status == 200
    error = err or ("" if ok else f"HTTP {status}")
    conn = get_db()
    conn.execute("INSERT INTO health_log (model, pool, provider, ok, latency_ms, error) VALUES (?,?,?,?,?,?)",
                 (model, pool_name, pv["name"], 1 if ok else 0, latency, error))
    conn.execute("UPDATE registry SET status=?, updated_at=datetime('now') WHERE model=?",
                 ("healthy" if ok else "down", model))
    # 额度感知（计划书 v1）：探测失败时分类，区分「额度耗尽」与「服务故障」
    if not ok:
        try:
            from provider_router.quota import classify_error, record_error
            body_text = body.decode() if isinstance(body, (bytes, bytearray)) else str(body)
            cls = classify_error(status, body_text, cfg)
            record_error(conn, pv["name"], cls, cfg)
        except Exception:
            pass
    conn.commit()
    conn.close()
    return {"model": model, "pool": pool_name, "ok": ok, "latency_ms": latency, "error": error}


def probe_all(cfg, watch=False):
    results = []
    for pool_name, pool_cfg in cfg.get("pools", {}).items():
        # [2026-09-14 修复] 跳过 auto_routable=false 的池（pool_test）：
        # 熔断是按 provider 计失败率的，测试池里的模型本来就不可用，
        # 探测它们会把共享同一 provider 的生产模型一起熔断（已发生过一次全站 503）。
        if pool_cfg.get("auto_routable", True) is False:
            continue
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