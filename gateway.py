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
import sqlite3
import subprocess
import sys
import time
import threading
import yaml
import httpx

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
_undo_stack = []                # 全局运行时逆栈：人类操作
_dynamic_weights = {}           # {provider_name: effective_weight} 由熔断线程更新
_approval_cache = {}            # {(agent_id, action_hash): expiry_timestamp}
_pending_approvals = {}         # {action_id: {action, params, agent_id}}
_fibers = {}                    # {fiber_id: Fiber} — Agent 任务树
_next_fiber_id = 0
_lock = threading.Lock()
_global_call_history = {}       # {f"{plugin_id}:{params_hash}": {"fiber_id": int, "timestamp": float, "result_preview": str}}
                                # Root 级全局去重表，24h TTL，三层清理

# ── 插件排队机制（v2.8） ──
_serial_locks = {}              # {resource_lock_key: asyncio.Lock} — 串行锁池
_throttle_windows = {}          # {plugin_id: [t1, t2, ...]} — 速率限制窗口

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
    """清空全局逆栈（配置热加载时调用，因为新配置是新起点）。"""
    _undo_stack.clear()

# ── Fiber 树形上下文（Agent 任务级可逆） ──
import dataclasses

@dataclasses.dataclass
class Fiber:
    """任务光纤。每棵 fiber 树对应一个统筹 Agent 的任务。
    - 子 fiber 失败时级联回滚祖先
    - undo_log 是 fiber 本地逆栈，提交时合并到父 fiber 或全局栈
    - capabilities 声明此 fiber 的权限（执行者: write/execute, 检查者: read/validate/inspect）
    - call_history 记录本 fiber 及其父 fiber 已调用的 (plugin_id, params_hash)，用于重复调用拦截
    """
    id: int
    parent_id: int | None
    agent_id: str
    description: str
    status: str = "active"       # active | committed | failed
    undo_log: list = dataclasses.field(default_factory=list)  # [(desc, callable), ...]
    children: list = dataclasses.field(default_factory=list)  # [fiber_id, ...]
    capabilities: list = dataclasses.field(default_factory=list)  # ["read", "write", "validate", "inspect", "execute"]
    call_history: list = dataclasses.field(default_factory=list)  # [{"plugin_id": str, "params_hash": str, "time": float}, ...]
    created_at: float = dataclasses.field(default_factory=time.time)

def fiber_create(agent_id, description, parent_id=None, capabilities=None):
    """创建新 fiber。返回 fiber_id。
    capabilities 可选，用于执行者-检查者权限校验：
    - 检查者只能有 read/validate/inspect
    - 执行者可以有 write/execute
    """
    global _next_fiber_id
    with _lock:
        _next_fiber_id += 1
        fid = _next_fiber_id
        f = Fiber(id=fid, parent_id=parent_id, agent_id=agent_id, description=description)
        if capabilities:
            f.capabilities = capabilities
        _fibers[fid] = f
        if parent_id is not None and parent_id in _fibers:
            _fibers[parent_id].children.append(fid)
        return fid

def fiber_register(fiber_id, description, revert_callable):
    """向 fiber 注册撤销操作。若 fiber 已终止则拒绝。"""
    f = _fibers.get(fiber_id)
    if not f:
        return False
    if f.status != "active":
        return False
    f.undo_log.append((description, revert_callable))
    return True

def fiber_fail(fiber_id, cascade_parent=True):
    """失败 fiber：LIFO 回滚自己的 undo_log，然后递归失败所有子 fiber。
    若 cascade_parent=True 且此 fiber 有父节点，级联失败父 fiber（执行者-检查者模式：
    检查者不通过 → 自动回滚执行者所有操作）。
    返回 (ok, 操作列表)。
    """
    f = _fibers.get(fiber_id)
    if not f or f.status != "active":
        return False, []
    # 先递归失败子 fiber
    for child_id in list(f.children):
        fiber_fail(child_id, cascade_parent=False)
    # LIFO 回滚自己的 undo_log
    ops = []
    while f.undo_log:
        desc, fn = f.undo_log.pop()
        try:
            fn()
            ops.append(f"回滚: {desc}")
        except Exception as e:
            ops.append(f"回滚失败 ({desc}): {e}")
    f.status = "failed"
    # 级联失败父 fiber（检查者不通过 → 执行者回滚）
    if cascade_parent and f.parent_id is not None and f.parent_id in _fibers:
        parent = _fibers[f.parent_id]
        if parent.status == "active":
            _, parent_ops = fiber_fail(f.parent_id, cascade_parent=False)
            ops.extend(parent_ops)
    return True, ops

def fiber_commit(fiber_id):
    """提交 fiber：合并 undo_log 到父 fiber（或全局栈），标记 committed。"""
    f = _fibers.get(fiber_id)
    if not f or f.status != "active":
        return False
    # 所有子 fiber 必须已终止
    for child_id in f.children:
        child = _fibers.get(child_id)
        if child and child.status == "active":
            return False  # 有未完成的子 fiber
        if child and child.status == "failed":
            return False  # 子 fiber 已失败，父不能提交
    # 合并到父 fiber 或全局栈
    if f.parent_id is not None and f.parent_id in _fibers:
        parent = _fibers[f.parent_id]
        if parent.status == "active":
            parent.undo_log.extend(f.undo_log)
    else:
        _undo_stack.extend(f.undo_log)
    f.undo_log.clear()
    f.status = "committed"
    # 主动清理：该 fiber 下所有 call_history 条目从全局表删除
    _cleanup_global_history_for_fiber(fiber_id)
    return True


# ── Root 级全局去重表（v2.8） ──
_GLOBAL_HISTORY_TTL = 86400  # 24 小时

def _global_call_key(plugin_id: str, params_hash: str) -> str:
    return f"{plugin_id}:{params_hash}"

def _global_call_lookup(plugin_id: str, params_hash: str) -> dict | None:
    """惰性清理：查找全局去重表，命中但超时则删除并返回 None。"""
    key = _global_call_key(plugin_id, params_hash)
    entry = _global_call_history.get(key)
    if entry is None:
        return None
    now = time.time()
    if now - entry["timestamp"] > _GLOBAL_HISTORY_TTL:
        del _global_call_history[key]
        return None
    return entry

def _global_call_add(plugin_id: str, params_hash: str, fiber_id: int, result_preview: str = ""):
    """写入全局去重表。"""
    key = _global_call_key(plugin_id, params_hash)
    _global_call_history[key] = {
        "fiber_id": fiber_id,
        "timestamp": time.time(),
        "result_preview": result_preview[:200],
    }

def _global_call_remove(plugin_id: str, params_hash: str):
    """从全局去重表删除单条记录。"""
    key = _global_call_key(plugin_id, params_hash)
    _global_call_history.pop(key, None)

def _cleanup_global_history_for_fiber(fiber_id: int):
    """主动清理：遍历 fiber 及其子树，删除所有 call_history 对应的全局表条目。"""
    f = _fibers.get(fiber_id)
    if not f:
        return
    # 清理本 fiber 的 call_history
    for entry in f.call_history:
        _global_call_remove(entry["plugin_id"], entry["params_hash"])
    # 递归清理子 fiber
    for child_id in list(f.children):
        _cleanup_global_history_for_fiber(child_id)

def _cleanup_global_history_periodic():
    """定时清理：后台线程每 1 小时扫描，删除超时条目。"""
    while True:
        time.sleep(3600)
        now = time.time()
        expired = [k for k, v in _global_call_history.items()
                   if now - v["timestamp"] > _GLOBAL_HISTORY_TTL]
        for k in expired:
            _global_call_history.pop(k, None)
        if expired:
            print(f"[全局去重] 定时清理 {len(expired)} 条过期记录")


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
        # 用户自定义 key（从聊天页面带入），覆盖 provider 配置的 key
        user_key = body.pop("api_key", None)
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

            # 4. 按权重选 provider
            # 用户自定义 key 时不限制模型名（用户用自己的 key 调任何 provider），
            # 否则只选有该模型的 provider
            model_filter = None if user_key else model
            pv = select_provider_by_weight(pool_cfg.get("providers", []), model=model_filter)
            if not pv:
                last_error = f"pool '{current_pool}' all providers disabled"
                current_pool = pool_cfg.get("fallback")
                continue

            provider_cfg = cfg.get("providers", {}).get(pv["name"])
            # 有效 key：用户自定义 key 优先，否则用 provider 配置的 key
            effective_key = user_key or provider_cfg.get("api_key", "") if provider_cfg else user_key
            if not provider_cfg or not effective_key or (not user_key and provider_cfg.get("api_key", "").startswith("${")):
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
        f = _fibers[fid]
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
        f = _fibers.get(fiber_id)
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
    async def admin_fiber_commit(fiber_id: int):
        ok = fiber_commit(fiber_id)
        if not ok:
            raise HTTPException(status_code=409, detail=f"fiber {fiber_id} cannot commit: not active or children incomplete")
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
        return {"fibers": {fid: _serialize(f) for fid, f in sorted(_fibers.items())}}

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
        """
        agents = cfg.get("agents", [])
        results = []
        for agent in agents:
            aid = agent.get("id", "unknown")
            atype = agent.get("type", "generic")
            status = "unknown"
            detail = ""

            try:
                if atype == "openhands":
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
                if atype == "openhands":
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
        if executor_fiber_id is not None and executor_fiber_id not in _fibers:
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
        if fid in _fibers:
            _fibers[fid].call_history.append(call_entry)
        if fiber_id is not None and fiber_id in _fibers:
            _fibers[fiber_id].call_history.append(call_entry)

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