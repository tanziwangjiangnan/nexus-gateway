"""极简路由模型服务 — 用 scnet-tp 做路由决策（每次 ~10 tokens）。

启动: python3 test_router_server.py
"""
import json
import os
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler

ROUTE_PROMPT = "From the candidates below, pick the best provider for this query. Reply with ONLY the provider name, nothing else.\n\nCandidates: {candidates}\n\nQuery: {query}\n\nSelected provider:"

# 路由决策用 scnet-tp（轻量非推理，~10 tokens/次）
ROUTE_API = os.environ.get("ROUTE_API", "https://api.scnet.cn/api/llm/v1")
ROUTE_MODEL = os.environ.get("ROUTE_MODEL", "scnet-tp")
ROUTE_API_KEY = os.environ.get("SCNET_TP_KEY", "")


class RouteHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        if self.path != "/v1/route":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        query = body.get("query", "")
        candidates = body.get("candidates", [])
        if not query or not candidates:
            self._respond(400, {"error": "query and candidates required"})
            return

        if not ROUTE_API_KEY:
            self._respond(503, {"error": "routing API key not set"})
            return

        prompt = ROUTE_PROMPT.format(candidates=", ".join(candidates), query=query[:500])
        payload = json.dumps({
            "model": ROUTE_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 20,
            "temperature": 0.1,
        }).encode()

        req = urllib.request.Request(
            f"{ROUTE_API.rstrip('/')}/chat/completions",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {ROUTE_API_KEY}",
            },
        )
        try:
            resp = urllib.request.urlopen(req, timeout=15)
            result = json.loads(resp.read().decode())
            selected = result["choices"][0]["message"]["content"].strip().strip('"').strip("'").strip(".")
            # 模糊匹配：取第一个命中的候选
            for c in candidates:
                if c in selected or selected in c:
                    selected = c
                    break
            if selected in candidates:
                self._respond(200, {
                    "selected": selected,
                    "confidence": 0.7,
                    "reason": "路由模型决策",
                    "alternatives": [{"provider": c, "score": 0.3} for c in candidates if c != selected],
                })
            else:
                self._respond(200, {
                    "selected": candidates[0],
                    "confidence": 0.5,
                    "reason": f"模型返回'{selected}'无效，兜底第一个候选",
                    "alternatives": [],
                })
        except Exception as e:
            self._respond(503, {"error": str(e)})

    def _respond(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, fmt, *args):
        print(f"[路由模型] {args[0]}" if args else "")


if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", 9090), RouteHandler)
    print("🚀 路由模型服务启动: http://127.0.0.1:9090/v1/route")
    print(f"   API={ROUTE_API}  MODEL={ROUTE_MODEL}")
    print(f"   KEY={'已设置' if ROUTE_API_KEY else '未设置'}")
    server.serve_forever()
