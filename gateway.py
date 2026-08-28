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
  python3 gateway.py fiber              # 查看 fiber 树（通过 Admin API）
  python3 gateway.py scan-agents         # 自动发现并接入智能体（含插件声明）
  python3 gateway.py scan-agents --dir /opt/agents  # 指定扫描目录
  python3 gateway.py check-deps           # 反向依赖扫描：检查 Key 被哪些组件引用
  python3 gateway.py check-deps --auto-sync  # 扫描 + 自动同步（本地 sed + 远程 SSH）
  python3 gateway.py check-deps "sk-xxx"  # 只检查特定 Key
  python3 gateway.py quality             # 查看 Provider 质量排名（检查者评分）
  python3 gateway.py feedback-stats      # 查看 Provider 用户反馈统计

API 端点:
  GET  /chat                    → 聊天页面（免鉴权，自备 Key）
  GET  /v1/models                → 模型目录（含池/provider/健康/用量）
  POST /v1/chat/completions      → OpenAI 兼容，三池路由 + 自定义 Key
  POST /v1/plugins/{id}/call     → 统一插件调用：capabilities 校验 + 重复调用拦截 + Fiber 逆操作 + 动态校验
  GET  /health                   → 网关自身健康检查
  GET  /metrics                  → Prometheus 指标
  GET  /admin/pools              → 查看各池/provider 状态
  POST /admin/pools/{pool}/providers/{provider}/toggle  → 启停 provider
  GET  /admin/undo               → 运行时逆栈：撤销最后一条操作
  GET  /admin/undo-list          → 运行时逆栈：查看所有操作
  POST /admin/mcp/toggle         → MCP 工具：Agent 调 toggle，走审批缓存 + fiber
  GET  /admin/mcp/approvals      → 查看审批缓存
  GET  /admin/mcp/status         → MCP 总览：熔断+权重+审批（含错误率）
  POST /admin/fiber/create       → 创建 Fiber 任务树节点
  POST /admin/fiber/{id}/fail    → 失败 fiber：级联回滚子孙
  POST /admin/fiber/{id}/commit  → 提交 fiber：合并 undo 到父节点
  GET  /admin/fiber/tree         → 查看 fiber 森林
  GET  /admin/agents/declaration → 返回智能体声明配置（agents 段）
  GET  /admin/agents/status      → 返回所有声明 Agent 的存活状态
  POST /admin/fiber/check        → 创建检查任务 fiber（执行者-检查者模式）
  GET  /admin/logs               → 聚合所有声明 Agent 的日志，按时间合并，支持 ?level=&agent=&lines= 过滤
"""
import datetime
import hashlib
import asyncio
import json
import os
import random
import re
import signal
import subprocess
import shlex
import sys
import time
import threading
import yaml
import httpx

# ── 组件包导入 ──
from provider_router import Router, RouterState, CircuitBreakerMonitor
from fiber_tree import FiberTree, MemoryStorage
from hermes_cfg import ConfigLoader, get_db, init_registry
from hermes_fiber import FiberRuntime

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
_dynamic_weights = {}           # {provider_name: effective_weight} 由熔断线程更新
_quality_factors = {}           # {provider_name: float} 质量信誉因子（检查者评分驱动）
_user_factors = {}              # {provider_name: float} 用户信誉因子（用户反馈驱动）
_approval_cache = {}            # {(agent_id, action_hash): expiry_timestamp}
_pending_approvals = {}         # {action_id: {action, params, agent_id}}
_lock = threading.Lock()
_fiber_runtime = FiberRuntime(lock=_lock)  # 运行时 Fiber + undo + 全局去重

# ── 组件实例 ──
_router_state = RouterState(
    disabled_providers=_disabled_providers,
    dynamic_weights=_dynamic_weights,
    quality_factors=_quality_factors,
    user_factors=_user_factors,
    rate_limit_buckets=_rate_limit_buckets,
    lock=_lock,
)
_fiber_tree = FiberTree()

# ── 插件排队机制（v2.8） ──
_serial_locks = {}              # {resource_lock_key: asyncio.Lock} — 串行锁池
_throttle_windows = {}          # {plugin_id: [t1, t2, ...]} — 速率限制窗口

# ── 配置加载（委托给 hermes_cfg） ──
_config_loader = ConfigLoader(path=CONFIG_PATH)

def load_config(path=None):
    if path:
        # 指定路径时直接加载（不更新全局）
        from provider_router.config import load_config as pr_load
        return pr_load(path)
    return _config_loader.load()

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
    new_cfg = _config_loader.reload()
    if _config:
        _config.clear()
        _config.update(new_cfg)
    else:
        _config = new_cfg
    _disabled_providers.clear()
    undo_clear("配置重载")
    return _config

# ── 数据库（委托给 hermes_cfg） ──
# get_db() 和 init_registry() 已从 hermes_cfg 导入

# ── 自动熔断 + 动态权重（后台线程，每 30s） ──
_circuit_breaker = None  # 在 main() 中初始化

def _circuit_breaker_loop(cfg):
    """后台线程 wrapper — 委托给 CircuitBreakerMonitor。"""
    global _circuit_breaker
    _circuit_breaker = CircuitBreakerMonitor(
        get_db=get_db,
        cfg_getter=lambda: _config,
        disabled_providers=_disabled_providers,
        dynamic_weights=_dynamic_weights,
        quality_factors=_quality_factors,
        user_factors=_user_factors,
        undo_register=undo_register,
        interval=30.0,
        lock=_lock,
    )
    _circuit_breaker._loop()

# ── 运行时逆栈（任务级幂等补偿，委托给 hermes_fiber） ──
def undo_register(description, revert_callable):
    """注册运行时操作的撤销回调。每个原子操作应有对应的逆操作。"""
    _fiber_runtime.undo_register(description, revert_callable)

def undo_pop():
    """弹出并执行最后一条撤销回调。"""
    return _fiber_runtime.undo_pop()

def undo_clear(reason=""):
    """清空全局逆栈（配置热加载时调用，因为新配置是新起点）。"""
    _fiber_runtime.undo_clear(reason)

# ── Fiber 树形上下文（Agent 任务级可逆，委托给 hermes_fiber） ──
def fiber_create(agent_id, description, parent_id=None, capabilities=None):
    return _fiber_runtime.fiber_create(agent_id, description, parent_id, capabilities)

def fiber_register(fiber_id, description, revert_callable):
    return _fiber_runtime.fiber_register(fiber_id, description, revert_callable)

def fiber_fail(fiber_id, cascade_parent=True):
    return _fiber_runtime.fiber_fail(fiber_id, cascade_parent)

def fiber_commit(fiber_id):
    return _fiber_runtime.fiber_commit(fiber_id)

# ── Root 级全局去重表（v2.8，委托给 hermes_fiber） ──
def _global_call_lookup(plugin_id, params_hash):
    return _fiber_runtime.global_call_lookup(plugin_id, params_hash)

def _global_call_add(plugin_id, params_hash, fiber_id, result_preview=""):
    _fiber_runtime.global_call_add(plugin_id, params_hash, fiber_id, result_preview)

def _global_call_remove(plugin_id, params_hash):
    _fiber_runtime.global_call_remove(plugin_id, params_hash)

def _cleanup_global_history_for_fiber(fiber_id):
    _fiber_runtime._cleanup_global_history_for_fiber(fiber_id)

def _cleanup_global_history_periodic():
    _fiber_runtime.cleanup_global_history_periodic()


# ── 插件执行器（v2.8 排队用） ──
async def _execute_plugin(plugin: dict, plugin_id: str, params: dict, timeout: int):
    """执行插件（http/cli），返回 (result, error)。独立于排队逻辑。"""
    global _disabled_providers
    exec_mode = plugin.get("execution", "http")
    result = {}
    error = None

    try:
        if exec_mode == "http":
            endpoint = plugin.get("endpoint", "")
            if not endpoint:
                error = f"plugin '{plugin_id}' has no endpoint"
                return result, error
            call_url = _format_string(endpoint, params)
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(call_url, json=params)
                if resp.status_code >= 400:
                    error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                else:
                    result = resp.json()

        elif exec_mode == "cli":
            command = plugin.get("command", "")
            if not command:
                error = f"plugin '{plugin_id}' has no command"
                return result, error
            cmd = _format_string(command, params)
            try:
                ret = subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, timeout=timeout
                )
                if ret.returncode != 0:
                    error = f"CLI exit {ret.returncode}: {ret.stderr[:200] or ret.stdout[:200]}"
                else:
                    try:
                        result = json.loads(ret.stdout)
                    except json.JSONDecodeError:
                        result = {"stdout": ret.stdout.strip(), "stderr": ret.stderr.strip()}
            except subprocess.TimeoutExpired:
                error = f"CLI timeout after {timeout}s"
        else:
            error = f"unsupported execution mode: {exec_mode}"

    except httpx.TimeoutException:
        error = f"HTTP timeout after {timeout}s"
    except httpx.ConnectError:
        provider_name = plugin.get("provider", "")
        if provider_name and provider_name not in _disabled_providers:
            _disabled_providers.add(provider_name)
        error = f"provider {provider_name} unreachable"
    except Exception as e:
        error = str(e)

    return result, error

# ── 日志解析辅助 ──
_LOG_PATTERN = re.compile(
    r'(?P<ts>\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)'
    r'\s*'
    r'(?:\[?\s*(?P<level>DEBUG|INFO|WARN(?:ING)?|ERROR|CRITICAL|FATAL|TRACE)\s*\]?)?',
    re.IGNORECASE,
)

def _parse_log_line(line: str, agent_id: str, source: str) -> dict | None:
    """解析单行日志，返回结构化字典或 None。"""
    line = line.rstrip("\n\r")
    if not line:
        return None
    m = _LOG_PATTERN.search(line)
    ts = m.group("ts") if m else ""
    level = m.group("level").upper() if m and m.group("level") else "INFO"
    # 规范化级别名
    level = level.replace("WARNING", "WARN").replace("CRITICAL", "FATAL")
    return {
        "timestamp": ts,
        "level": level,
        "agent_id": agent_id,
        "source": source,
        "message": line,
    }

def _log_matches(entry: dict, filter_levels: list[str], since_str: str) -> bool:
    """检查日志条目是否匹配过滤条件。"""
    if filter_levels and entry.get("level", "") not in filter_levels:
        return False
    if since_str and entry.get("timestamp", "") < since_str:
        return False
    return True

def _format_string(template: str, params: dict) -> str:
    """替换模板字符串中的 {key} 占位符为 params 中的值。"""
    import re
    def _replacer(m):
        key = m.group(1)
        val = params.get(key, m.group(0))
        return str(val) if val is not None else m.group(0)
    return re.sub(r'\{(\w+)\}', _replacer, template)

# ── 路由引擎 ──

def find_model_config(cfg, model):
    """遍历所有池查找 model 所属的 (pool_name, provider_config, canonical_model_name)；大小写不敏感"""
    return Router.find_model(cfg, model)

def select_pool_by_keywords(cfg, messages_text):
    """关键词匹配 → 返回 pool_name 或 None"""
    return Router.select_pool_by_keywords(cfg, messages_text)

def select_provider_by_weight(providers, model=None):
    """按权重随机选一个 provider，跳过禁用的；若指定 model 则只选有该模型的（大小写不敏感）"""
    return Router.select_provider(providers, _router_state, model=model)

def check_rate_limit(provider_name, max_rps):
    """滑动窗口限流，返回 True=通过 False=限流"""
    return Router.check_rate_limit(provider_name, max_rps, _router_state)

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
    """构建 FastAPI 应用实例（薄封装，委托给 hermes_api.build_app）。"""
    from hermes_api import build_app
    deps = {
        "disabled_providers": _disabled_providers,
        "router_state": _router_state,
        "fiber_runtime": _fiber_runtime,
        "dynamic_weights": _dynamic_weights,
        "approval_cache": _approval_cache,
        "pending_approvals": _pending_approvals,
        "lock": _lock,
        "serial_locks": _serial_locks,
        "throttle_windows": _throttle_windows,
        "get_db": get_db,
        "execute_plugin": _execute_plugin,
        "format_string": _format_string,
        "global_call_lookup": _global_call_lookup,
        "global_call_add": _global_call_add,
        "undo_register": undo_register,
        "undo_pop": undo_pop,
        "fiber_create": fiber_create,
        "fiber_register": fiber_register,
        "fiber_fail": fiber_fail,
        "fiber_commit": fiber_commit,
        "find_model_config": find_model_config,
        "select_pool_by_keywords": select_pool_by_keywords,
        "select_provider_by_weight": select_provider_by_weight,
        "check_rate_limit": check_rate_limit,
        "log_matches": _log_matches,
        "parse_log_line": _parse_log_line,
    }
    return build_app(cfg, deps)



# ── 反向依赖检查 ──

def _collect_all_keys(config):
    """从 config 中提取所有 Key（gateway_key + 各 provider 的 api_key + 外部 URL）。"""
    keys = {}
    gw_key = config.get("gateway_key", "")
    if gw_key:
        keys["gateway_key"] = gw_key
        # 外部组件可能去掉前缀（如 `gw-`），添加变体
        for prefix in ("gw-", "gw_", "hermes-", "hermes_"):
            if gw_key.startswith(prefix):
                variant = gw_key[len(prefix):]
                keys[f"gateway_key_variant.{prefix}"] = variant
                break
    for pname, pcfg in config.get("providers", {}).items():
        ak = pcfg.get("api_key", "")
        if ak and not ak.startswith("${"):
            keys[f"providers.{pname}.api_key"] = ak
        # 也记录 provider 的 api_base URL，外部组件可能引用它
        api = pcfg.get("api", "")
        if api:
            keys[f"providers.{pname}.api"] = api
    # 添加本机地址/端口，外部组件可能引用
    host = config.get("host", "127.0.0.1")
    port = config.get("port", 8646)
    keys["_self_url"] = f"http://{host}:{port}"
    # 也添加可能的公网地址（nginx 暴露的端口和域名）
    keys["_self_url_https"] = "https://117.72.220.114:8643"
    keys["_self_url_domain"] = "https://hermes.jiangnande.cloud:8643"
    return keys


def _scan_local(index, config):
    """扫描本地依赖：.env, 已知 Agent 配置文件, 环境变量。"""
    # 1. 检查 .env 文件中的环境变量引用
    env_path = os.path.join(BASE, ".env")
    if os.path.isfile(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                for key_name, key_val in config.items():
                    if key_val in v:
                        index.append({
                            "component": "本机 .env",
                            "file": env_path,
                            "key_name": key_name,
                            "current_value": key_val,
                            "found_at": f"{k} = {v[:60]}",
                            "fixable": True,
                            "fix_type": "sed",
                        })

    # 2. OpenHands 配置（~/.config/oh/config.toml 或 ~/.openhands/config.toml）
    oh_paths = [
        os.path.expanduser("~/.openhands/config.toml"),
        os.path.expanduser("~/.config/oh/config.toml"),
    ]
    for oh_path in oh_paths:
        if os.path.isfile(oh_path):
            with open(oh_path) as f:
                content = f.read()
                for key_name, key_val in config.items():
                    if key_val in content:
                        index.append({
                            "component": "OpenHands",
                            "file": oh_path,
                            "key_name": key_name,
                            "current_value": key_val,
                            "found_at": f"配置文件中引用",
                            "fixable": True,
                            "fix_type": "sed",
                        })

    # 3. 环境变量（检查进程环境）
    for key_name, key_val in config.items():
        for env_name, env_val in sorted(os.environ.items()):
            if key_val == env_val or (key_val and key_val in env_val):
                if env_name in ("XIAOMI_API_KEY", "DEEPSEEK_API_KEY", "OPENHANDS_API_KEY", "GATEWAY_KEY"):
                    index.append({
                        "component": f"环境变量 {env_name}",
                        "file": "进程环境变量",
                        "key_name": key_name,
                        "current_value": key_val,
                        "found_at": f"{env_name} = {env_val[:60]}",
                        "fixable": False,
                        "fix_type": "env",
                    })


def _scan_remote(host, port, cmd, config, label):
    """通过 SSH 在远程主机上扫描 Key 引用。"""
    try:
        full_cmd = f'ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 -p {port} root@{host} {shlex.quote(cmd)}'
        result = subprocess.run(full_cmd, shell=True, capture_output=True, timeout=15, text=True)
        if result.returncode != 0:
            return []
        content = result.stdout
        hits = []
        for key_name, key_val in config.items():
            if key_val in content:
                # 尝试定位行号
                for i, line in enumerate(content.split("\n"), 1):
                    if key_val in line:
                        hits.append({
                            "component": label,
                            "file": f"{host}:{port} — 远程配置",
                            "key_name": key_name,
                            "current_value": key_val,
                            "found_at": f"第 {i} 行: {line.strip()[:80]}",
                            "fixable": True,
                            "fix_type": "ssh_sed",
                            "_remote": {
                                "host": host,
                                "port": port,
                                "file": cmd.split("cat ")[-1] if "cat " in cmd else "",
                            },
                        })
                        break
        return hits
    except Exception as e:
        return [{"component": label, "file": f"{host}:{port}", "key_name": "—",
                 "current_value": "—", "found_at": f"SSH 连接失败: {e}",
                 "fixable": False, "fix_type": "unreachable"}]


def cmd_check_deps(config, target_key=None, auto_sync=False):
    """check-deps 命令：反向依赖扫描 + 可选自动同步。"""
    all_keys = _collect_all_keys(config)
    if target_key:
        # 过滤：只保留匹配目标值的 key
        filtered = {}
        for kn, kv in all_keys.items():
            if target_key in kv or target_key in kn:
                filtered[kn] = kv
        if not filtered:
            print(f"🔍 未找到匹配 '{target_key}' 的 Key")
            return
        all_keys = filtered

    print(f"\n🔍 反向依赖扫描 — 共 {len(all_keys)} 个配置项\n")
    for kn, kv in all_keys.items():
        if kn.startswith("_"):
            continue  # 内部标记不展示
        print(f"  📌 {kn}: {kv[:60]}...")
    print()

    # 收集所有依赖
    index = []
    _scan_local(index, all_keys)
    # 远程扫描
    index += _scan_remote("106.14.40.189", "2222",
                          "cat /opt/qq-bot/bot/astrbot/data/cmd_config.json",
                          all_keys, "AstrBot（老机）")
    index += _scan_remote("106.14.20.149", "2222",
                          "cat /opt/kb-agent/config.json 2>/dev/null || cat /app/config.json 2>/dev/null || echo 'NO_CONFIG'",
                          all_keys, "kb_agent（新机）")

    if not index:
        print("✅ 未发现任何外部依赖，配置变更安全。")
        return

    # 分组展示
    fixable_deps = [d for d in index if d.get("fixable")]
    unfixable_deps = [d for d in index if not d.get("fixable")]

    if unfixable_deps:
        print("⚠️  以下依赖无法自动修复：\n")
        for d in unfixable_deps:
            print(f"   ❌ {d['component']}")
            print(f"      {d['found_at']}")
        print()

    if fixable_deps:
        print(f"🔧 以下 {len(fixable_deps)} 个依赖可自动同步：\n")
        for d in fixable_deps:
            print(f"   📎 {d['component']} ({d['file']})")
            print(f"      {d['found_at']}")
        print()

    # auto-sync 逻辑
    if auto_sync and fixable_deps:
        print("=" * 50)
        print("🔄 自动同步模式启用\n")
        failed = False
        for d in fixable_deps:
            if d["fix_type"] == "sed" and os.path.isfile(d["file"]):
                # 本地文件：备份 + sed 替换
                old_val = d["current_value"]
                new_val = all_keys.get(d["key_name"], "")
                if not new_val:
                    continue
                bak = d["file"] + ".bak"
                try:
                    subprocess.run(f"cp {d['file']} {bak}", shell=True, capture_output=True, timeout=5)
                    # 用 sed 替换
                    esc_old = old_val.replace("/", "\\/").replace("'", "'\\''")
                    esc_new = new_val.replace("/", "\\/").replace("'", "'\\''")
                    r = subprocess.run(
                        f"sed -i 's/{esc_old}/{esc_new}/g' {d['file']}",
                        shell=True, capture_output=True, timeout=10, text=True)
                    if r.returncode == 0:
                        print(f"   ✅ {d['component']} — 已同步")
                    else:
                        print(f"   ❌ {d['component']} — sed 失败: {r.stderr[:80]}")
                        failed = True
                except Exception as e:
                    print(f"   ❌ {d['component']} — 异常: {e}")
                    failed = True

            elif d["fix_type"] == "ssh_sed" and d.get("_remote"):
                # 远程文件：SSH + sed 替换
                rhost = d["_remote"]["host"]
                rport = d["_remote"]["port"]
                rfile = d["_remote"]["file"]
                old_val = d["current_value"]
                new_val = all_keys.get(d["key_name"], "")
                if not new_val or not rfile:
                    continue
                esc_old = old_val.replace("/", "\\/").replace("'", "'\\''")
                esc_new = new_val.replace("/", "\\/").replace("'", "'\\''")
                try:
                    # 先备份
                    bak_cmd = f"ssh -o StrictHostKeyChecking=no -p {rport} root@{rhost} 'cp {rfile} {rfile}.depsync.bak' 2>/dev/null"
                    subprocess.run(bak_cmd, shell=True, capture_output=True, timeout=10)
                    # 再替换
                    sed_cmd = shlex.quote(f"sed -i 's/{esc_old}/{esc_new}/g' {rfile}")
                    full = f"ssh -o StrictHostKeyChecking=no -p {rport} root@{rhost} {sed_cmd}"
                    r = subprocess.run(full, shell=True, capture_output=True, timeout=15, text=True)
                    if r.returncode == 0:
                        print(f"   ✅ {d['component']} ({rhost}) — 已同步")
                    else:
                        print(f"   ❌ {d['component']} ({rhost}) — 同步失败: {r.stderr[:80]}")
                        failed = True
                except Exception as e:
                    print(f"   ❌ {d['component']} ({rhost}) — 异常: {e}")
                    failed = True

        if failed:
            print("\n   ❌ 部分依赖同步失败，请检查日志。")
        else:
            print("\n   ✅ 所有依赖已更新，变更安全。")

    elif auto_sync and not fixable_deps:
        print("🔄 没有可自动修复的依赖。")

    print()


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
        # 启动全局去重定时清理线程
        t2 = threading.Thread(target=_cleanup_global_history_periodic, daemon=True)
        t2.start()
        print(f"   📋 全局去重已启用 (24h TTL, 1h 定时清理)")

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

    elif args[0] == "quality":
        """查看每个 Provider 的质量排名（基于检查者评分）。"""
        conn = get_db()
        rows = conn.execute("""
            SELECT provider, COUNT(*) as n, ROUND(AVG(checker_score), 1) as avg_score
            FROM usage WHERE checker_score IS NOT NULL
            GROUP BY provider ORDER BY avg_score DESC
        """).fetchall()
        conn.close()
        if not rows:
            print("暂无检查者评分数据")
        else:
            print(f"\n📊 质量排名（检查者评分）")
            print(f"  {'Provider':<20s} {'样本数':>6s} {'平均分':>6s}")
            print(f"  {'─'*35}")
            for r in rows:
                print(f"  {r['provider']:<20s} {r['n']:>6d} {r['avg_score']:>6.1f}")
            print()

    elif args[0] == "feedback-stats":
        """查看每个 Provider 的用户反馈统计。"""
        conn = get_db()
        rows = conn.execute("""
            SELECT provider,
                   COUNT(*) as n,
                   SUM(CASE WHEN user_feedback=1 THEN 1 ELSE 0 END) as likes,
                   SUM(CASE WHEN user_feedback=-1 THEN 1 ELSE 0 END) as dislikes
            FROM usage WHERE user_feedback != 0
            GROUP BY provider ORDER BY (likes - dislikes) DESC
        """).fetchall()
        conn.close()
        if not rows:
            print("暂无用户反馈数据")
        else:
            print(f"\n👍 用户反馈统计")
            print(f"  {'Provider':<20s} {'样本':>4s} {'点赞':>4s} {'点踩':>4s} {'净分':>5s}")
            print(f"  {'─'*42}")
            for r in rows:
                net = r["likes"] - r["dislikes"]
                print(f"  {r['provider']:<20s} {r['n']:>4d} {r['likes']:>4d} {r['dislikes']:>4d} {net:>+4d}")
            print()

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

    elif args[0] == "fiber":
        """查看 fiber 树（通过 Admin API）"""
        import urllib.request
        gw_key = _config.get("gateway_key", "")
        req = urllib.request.Request(f"http://127.0.0.1:8646/admin/fiber/tree",
                                     headers={"Authorization": f"Bearer {gw_key}"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                fibers = data.get("fibers", {})
                if not fibers:
                    print("fiber 森林为空")
                else:
                    # 按 id 排序，树形缩进打印
                    def _print_tree(fid, indent=0):
                        f = fibers.get(str(fid))
                        if not f:
                            return
                        prefix = "  " * indent + ("└─ " if indent > 0 else "")
                        icon = {"active": "🟢", "committed": "✅", "failed": "❌"}.get(f["status"], "⚪")
                        print(f"{prefix}{icon} #{f['id']} {f['description']} [{f['status']}] agent={f['agent_id']} undo={f['undo_count']}")
                        for child_id in sorted(f["children"]):
                            _print_tree(child_id, indent + 1)
                    # 打印根节点（parent_id=None 的）
                    for fid, f in sorted(fibers.items()):
                        if f["parent_id"] is None:
                            _print_tree(int(fid))
        except Exception as e:
            print(f"❌ 调用失败: {e}")

    elif args[0] == "check-deps":
        """检查 Key 的反向依赖：扫描所有已知组件，找出哪些引用了指定 Key（或全部 Key）。"""
        target_key = None
        auto_sync = False
        for a in args[1:]:
            if a == "--auto-sync":
                auto_sync = True
            elif not a.startswith("--"):
                target_key = a
        cmd_check_deps(_config, target_key=target_key, auto_sync=auto_sync)

    elif args[0] == "scan-agents":
        """自动发现并接入智能体。
        扫描指定目录（默认 ~/agents/），寻找 Agent 特征文件，
        交互式确认后自动写入 gateway.yaml 并热加载。
        """
        scan_dir = "~/agents"
        if "--dir" in args:
            idx = args.index("--dir")
            if idx + 1 < len(args):
                scan_dir = args[idx + 1]
        scan_dir = os.path.expanduser(scan_dir)

        if not os.path.isdir(scan_dir):
            print(f"⚠️  目录不存在: {scan_dir}")
            print(f"   创建后重试，或指定: python3 gateway.py scan-agents --dir /path/to/agents")
            return

        found = []
        print(f"🔍 扫描 {scan_dir} ...")
        for root, dirs, files in os.walk(scan_dir):
            rel = os.path.relpath(root, scan_dir)
            if rel.startswith(".") or rel.startswith("_"):
                continue
            # 跳过无关目录
            basename = os.path.basename(root)
            if basename in ("node_modules", "__pycache__", ".git", ".venv", "venv", "env", ".tox"):
                dirs[:] = []  # 不深入
                continue

            # 特征: OpenHands — config.toml 含 [core] 或 .lock 文件
            if "config.toml" in files:
                try:
                    content = open(os.path.join(root, "config.toml")).read()
                    if "[core]" in content:
                        found.append({
                            "id": f"openhands-{len(found)}",
                            "display_name": f"OpenHands ({rel})",
                            "type": "openhands",
                            "workspace": root,
                            "capabilities": ["read", "write", "execute"],
                            "confidence": "high",
                            "evidence": f"config.toml → [core]",
                        })
                        continue
                except:
                    pass
            lock_files = [f for f in files if f.endswith(".lock")]
            if lock_files:
                found.append({
                    "id": f"openhands-{len(found)}",
                    "display_name": f"OpenHands ({rel})",
                    "type": "openhands",
                    "workspace": root,
                    "capabilities": ["read", "write", "execute"],
                    "confidence": "medium",
                    "evidence": f"lock 文件: {', '.join(lock_files[:3])}",
                })
                continue

            # 特征: AstrBot — main.py 含 AstrBot 或 config.yaml 含 adapters
            if "main.py" in files:
                try:
                    content = open(os.path.join(root, "main.py")).read()
                    if "AstrBot" in content or "astrbot" in content.lower():
                        found.append({
                            "id": f"astrbot-{len(found)}",
                            "display_name": f"AstrBot ({rel})",
                            "type": "astrbot",
                            "base_url": "http://127.0.0.1:12345",
                            "capabilities": ["read"],
                            "confidence": "high",
                            "evidence": "main.py → AstrBot",
                        })
                        continue
                except:
                    pass
            if "config.yaml" in files:
                try:
                    content = open(os.path.join(root, "config.yaml")).read()
                    if "adapters" in content:
                        found.append({
                            "id": f"astrbot-{len(found)}",
                            "display_name": f"AstrBot ({rel})",
                            "type": "astrbot",
                            "base_url": "http://127.0.0.1:12345",
                            "capabilities": ["read"],
                            "confidence": "medium",
                            "evidence": "config.yaml → adapters",
                        })
                        continue
                except:
                    pass

            # 特征: 通用脚本 — 有 .pid 文件或 main.py 含 daemon
            pid_files = [f for f in files if f.endswith(".pid")]
            if pid_files:
                found.append({
                    "id": f"agent-{len(found)}",
                    "display_name": f"Agent ({rel})",
                    "type": "generic",
                    "command": f"python3 {os.path.join(root, 'main.py')}" if "main.py" in files else "",
                    "pid_file": os.path.join(root, pid_files[0]),
                    "capabilities": ["read"],
                    "confidence": "medium",
                    "evidence": f"pid 文件: {pid_files[0]}",
                })
                continue

        if not found:
            print(f"  未发现已知的智能体。")
            print(f"  提示: 将智能体放在 {scan_dir} 下的子目录中，或手动编辑 gateway.yaml 的 agents 段。")
            return

        # 扫描插件清单：每个 Agent 目录下的 plugins.yaml
        discovered_plugins = []
        for agent in found:
            agent_dir = agent.get("workspace") or os.path.dirname(agent.get("pid_file", ""))
            if not agent_dir:
                continue
            plugin_file = os.path.join(agent_dir, "plugins.yaml")
            if os.path.isfile(plugin_file):
                try:
                    with open(plugin_file) as f:
                        raw = f.read()
                    plugin_list = yaml.safe_load(raw) or []
                    if isinstance(plugin_list, list):
                        for p in plugin_list:
                            p["provider"] = agent["id"]
                            if "id" not in p:
                                p["id"] = f"{agent['id']}-{p.get('display_name', 'plugin')}"
                            discovered_plugins.append(p)
                except Exception as e:
                    print(f"  ⚠️  解析 {agent['id']} 的 plugins.yaml 失败: {e}")

        print(f"\n📋 发现 {len(found)} 个智能体候选:\n")
        for i, agent in enumerate(found, 1):
            icon = {"openhands": "🤖", "astrbot": "💬", "generic": "⚙️"}.get(agent["type"], "❓")
            print(f"  {i}. {icon} {agent['display_name']}")
            print(f"     类型: {agent['type']} | 置信度: {agent['confidence']}")
            print(f"     证据: {agent['evidence']}")
            if agent.get("workspace"):
                print(f"     路径: {agent['workspace']}")
            if agent.get("pid_file"):
                print(f"     PID: {agent['pid_file']}")
            # 显示该 Agent 提供的插件
            agent_plugins = [p for p in discovered_plugins if p.get("provider") == agent["id"]]
            if agent_plugins:
                for p in agent_plugins:
                    print(f"     📦 插件: {p.get('display_name', p['id'])} ({p.get('execution', '?')})")
            print()

        # 交互式确认
        print(f"是否接入以上 {len(found)} 个智能体到 gateway.yaml？")
        # 读取已存在的 agents 列表，避免重复
        existing = _config.get("agents", [])
        existing_ids = {a["id"] for a in existing}

        to_add = []
        for agent in found:
            if agent["id"] in existing_ids:
                print(f"  ⏭️  {agent['display_name']} 已存在，跳过")
                continue
            # 构造 YAML 配置块
            entry = {
                "id": agent["id"],
                "display_name": agent["display_name"],
                "type": agent["type"],
                "capabilities": agent["capabilities"],
            }
            if agent.get("workspace"):
                entry["workspace"] = agent["workspace"]
            if agent.get("base_url"):
                entry["base_url"] = agent["base_url"]
            if agent.get("command"):
                entry["command"] = agent["command"]
            if agent.get("pid_file"):
                entry["pid_file"] = agent["pid_file"]
            to_add.append(entry)

        if not to_add:
            print("  没有新增的智能体需要接入。")
            return

        print(f"  将接入 {len(to_add)} 个新智能体。确认？[Y/n] ", end="")
        try:
            resp = input().strip().lower()
        except:
            resp = "y"
        if resp not in ("", "y", "yes"):
            print("  已取消")
            return

        # 写入 YAML
        yaml_path = CONFIG_PATH
        with open(yaml_path) as f:
            raw = f.read()

        # 找到 agents 段或 validation 段前插入
        agents_yaml = yaml.dump(to_add, default_flow_style=False, allow_unicode=True, sort_keys=False)
        # 缩进处理
        agents_yaml = "\n".join("  " + line if line.strip() else "" for line in agents_yaml.strip().split("\n"))

        if "# ── 智能体声明" in raw:
            # 追加到现有 agents 段
            insert_pos = raw.rfind("\n  - id:")
            raw = raw[:insert_pos] + "\n" + agents_yaml + raw[insert_pos:]
        else:
            # 在 validation 前插入新 agents 段
            marker = "# ── 验证模式"
            agents_block = f"\n# ── 智能体声明 ──\n# 自动发现: scan-agents 命令生成\n# 不迁移、不复制任何用户文件，只需声明路径/地址。\nagents:\n{agents_yaml}\n\n"
            if marker in raw:
                raw = raw.replace(marker, agents_block + marker)
            else:
                raw += "\n" + agents_block

        # 写入发现的插件清单
        if discovered_plugins:
            # 去重：已有 plugins 段则合并，避免重复
            existing_plugins = {p["id"] for p in _config.get("plugins", [])}
            new_plugins = [p for p in discovered_plugins if p["id"] not in existing_plugins]
            if new_plugins:
                plugins_yaml = yaml.dump(new_plugins, default_flow_style=False, allow_unicode=True, sort_keys=False)
                plugins_yaml = "\n".join("  " + line if line.strip() else "" for line in plugins_yaml.strip().split("\n"))

                if "# ── 插件声明" in raw:
                    # 追加到现有 plugins 段
                    insert_pos = raw.rfind("\n  - id:")
                    # 找到 plugins 段的最后一个 id，而不是 agents 段的
                    # 更可靠：在 ## 插件声明 之后找最后一个 - id:
                    plugins_section = raw[raw.find("# ── 插件声明"):]
                    last_id_in_plugins = plugins_section.rfind("\n  - id:")
                    if last_id_in_plugins >= 0:
                        abs_pos = raw.find("# ── 插件声明") + last_id_in_plugins
                        raw = raw[:abs_pos] + "\n" + plugins_yaml + raw[abs_pos:]
                    else:
                        raw = raw.replace("# ── 插件声明", f"# ── 插件声明 ──\nplugins:\n{plugins_yaml}\n")
                else:
                    agents_block = f"\n# ── 插件声明 ──\n# 自动发现: scan-agents 命令生成\nplugins:\n{plugins_yaml}\n\n"
                    # 在 validation 前插入
                    marker = "# ── 验证模式"
                    if marker in raw:
                        raw = raw.replace(marker, agents_block + marker)
                    else:
                        raw += "\n" + agents_block

        with open(yaml_path, "w") as f:
            f.write(raw)

        # 热加载
        print(f"  ✅ 已写入 {yaml_path}")
        try:
            subprocess.run(["systemctl", "reload", "gateway"], capture_output=True, timeout=10)
            # 也直接 SIGHUP 以防 systemctl 没生效
            pid_file = os.path.join(BASE, "gateway.pid")
            if os.path.exists(pid_file):
                with open(pid_file) as f:
                    pid = int(f.read().strip())
                os.kill(pid, signal.SIGHUP)
            print(f"  🔄 gateway 已热加载")
        except Exception as e:
            print(f"  ⚠️  热加载失败: {e}，手动执行: systemctl reload gateway")
        print(f"\n✨ 已完成。新增 {len(to_add)} 个智能体:")
        for a in to_add:
            print(f"   • {a['display_name']} ({a['type']})")

    else:
        print(f"未知命令: {args[0]}")

if __name__ == "__main__":
    main()