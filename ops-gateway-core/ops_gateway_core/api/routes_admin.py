"""Admin 端点（从 api/app.py 抽出，2026-09-18）

职责：运维 / 排障用的 /admin/* 接口（池与 provider 状态、额度标记与 undo、
fiber 任务树、MCP 审批缓存、智能体声明与状态、日志聚合）。

设计：工厂函数 `build_admin_router(...)` 接收所需的共享状态与能力（都由 app.py 从 deps 拆出后注入），
返回一个 `APIRouter`；路由内容与抽出前逐字一致（仅 `@app.` 改为 `@router.`）。
鉴权由 app.py 的中间件统一负责（/admin/* 需 gateway_key），本模块不重复判。
"""
import datetime
import hashlib
import os
import subprocess
import time

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from provider_router import quota as _quota_mod


def build_admin_router(*,
    cfg,
    db_conn,
    get_db,
    undo_register,
    undo_pop,
    fiber_create,
    fiber_fail,
    fiber_commit,
    fiber_register,
    _approval_cache,
    _disabled_providers,
    _dynamic_weights,
    _fiber_runtime,
    _lock,
    _log_matches,
    _parse_log_line,
    _user_factors,
    _get_provider_from_last_usage,
):
    """构建 /admin/* 路由。参数均由 app.py 注入（见上方说明）。"""
    router = APIRouter()

    # ── Admin API ──
    # ══════════════════════════════
    # Admin 端点（排障 / 运维用，全部要求 gateway_key）
    #   /admin/pools   池与 provider 状态、启停
    #   /admin/quota   额度状态与人工标记（可 undo）
    #   /admin/fiber*  任务树（创建 / 失败级联 / 提交 / 查看）
    #   /admin/undo*   运行时逆栈
    #   /admin/mcp*    工具审批缓存
    #   /admin/logs    聚合各智能体日志
    # ══════════════════════════════
    @router.get("/admin/pools")
    async def admin_pools():
        result = {}
        for pool_name, pool_cfg in cfg.get("pools", {}).items():
            providers = []
            for pv in pool_cfg.get("providers", []):
                pc = cfg.get("providers", {}).get(pv["name"], {})
                # 从 registry 表读取真实健康状态（由熔断器主动探测更新）
                try:
                    with db_conn() as conn:
                        row = conn.execute(
                            "SELECT status FROM registry WHERE provider = ? ORDER BY updated_at DESC LIMIT 1",
                            (pv["name"],)
                        ).fetchone()
                        reg_status = row[0] if row else "unknown"
                except Exception:
                    reg_status = "unknown"
                providers.append({
                    "name": pv["name"],
                    "weight": pv.get("weight", 1),
                    "models": pv.get("models", []),
                    "disabled": pv["name"] in _disabled_providers,
                    "status": reg_status,
                    "max_rps": pc.get("max_rps", 0),
                    "api": pc.get("api", ""),
                })
            result[pool_name] = {
                "description": pool_cfg.get("description", ""),
                "fallback": pool_cfg.get("fallback"),
                "providers": providers,
            }
        return result

    @router.post("/admin/pools/{pool_name}/providers/{provider_name}/toggle")
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
    @router.get("/admin/undo")
    async def admin_undo():
        ok, msg = undo_pop()
        return {"ok": ok, "message": msg}

    @router.get("/admin/undo-list")
    async def admin_undo_list():
        return {"stack": _fiber_runtime.undo_list()}

    # ── 额度状态 Admin API（计划书 v1）──
    @router.get("/admin/quota")
    async def admin_quota():
        """查看所有 provider 的额度状态。"""
        conn = get_db()
        try:
            summary = _quota_mod.provider_status_summary(conn, cfg)
            events = [dict(r) for r in conn.execute(
                "SELECT id, provider, event_type, status, http_status, detail, created_at "
                "FROM provider_events ORDER BY id DESC LIMIT 50").fetchall()]
        finally:
            conn.close()
        return {"summary": summary, "recent_events": events}

    @router.post("/admin/quota/{provider_name}/set")
    async def admin_quota_set(provider_name: str, request: Request):
        """人工标记额度状态：{"status": "available|low|exhausted|unknown", "reason": "..."}。

        注册逆操作，可通过 /admin/undo 撤销（恢复原状态）。
        """
        body = await request.json()
        new_status = body.get("status")
        valid = {"unknown", "available", "low", "exhausted", "unavailable"}
        if new_status not in valid:
            raise HTTPException(status_code=400, detail=f"invalid status, one of {sorted(valid)}")
        # 验证 provider 存在
        found = any(pv["name"] == provider_name
                    for pc in cfg.get("pools", {}).values()
                    for pv in pc.get("providers", []))
        if not found:
            raise HTTPException(status_code=404, detail=f"provider '{provider_name}' not found")

        conn = get_db()
        try:
            prev = _quota_mod.get_status(conn, provider_name)
        finally:
            conn.close()

        def _revert(name=provider_name, prev=prev):
            c = get_db()
            try:
                _quota_mod.set_status(c, name, prev.get("status", "unknown"),
                                      f"undo: {prev.get('reason','')}", "manual")
            finally:
                c.close()

        conn = get_db()
        try:
            _quota_mod.set_status(conn, provider_name, new_status,
                                  body.get("reason", "manual"), "manual")
        finally:
            conn.close()
        undo_register(f"额度状态 {provider_name} → {prev.get('status')}",
                      _revert)
        return {"provider": provider_name, "status": new_status,
                "previous": prev.get("status")}

    # ── MCP 审批回调 ──
    # 让统筹 Agent 通过 HTTP 调用 toggle，走审批缓存 + fiber 树形上下文
    from ..constants import APPROVAL_TTL as _APPROVAL_TTL  # noqa: F401  (共享常量，见 constants.py)

    @router.get("/admin/mcp/approvals")
    async def admin_mcp_approvals():
        """查看审批缓存状态"""
        now = time.time()
        active = {k: v for k, v in _approval_cache.items() if v > now}
        return {
            "active_approvals": len(active),
            "approvals": [{"hash": k, "expires_at": datetime.datetime.fromtimestamp(v).isoformat()}
                          for k, v in sorted(active.items())],
        }

    @router.get("/admin/mcp/status")
    async def admin_mcp_status():
        """MCP 状态总览：熔断 + 权重 + 审批"""
        # 5 分钟滑动窗口错误率
        with db_conn() as conn:
            rows = conn.execute("""
                SELECT provider, COUNT(*) as total, SUM(ok) as success
                FROM usage WHERE called_at > datetime('now', '-5 minutes')
                GROUP BY provider
            """).fetchall()
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
    @router.post("/admin/feedback")
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
        with db_conn() as conn:
            provider = _get_provider_from_last_usage(conn)
            if not provider:
                raise HTTPException(status_code=404, detail="no usage record found")
            conn.execute(
                "UPDATE usage SET user_feedback = ? WHERE id = (SELECT id FROM usage WHERE provider = ? ORDER BY called_at DESC LIMIT 1)",
                (feedback, provider))
            conn.commit()
        # 实时更新用户因子（不等30秒循环）
        qf_cfg = cfg.get("quality_feedback", {}).get("user_window", 20)
        with db_conn() as conn2:
            urows = conn2.execute(
                "SELECT user_feedback FROM usage WHERE provider = ? AND user_feedback != 0 ORDER BY called_at DESC LIMIT ?",
                (provider, qf_cfg)
            ).fetchall()
        total = sum(r[0] for r in urows)
        _user_factors[provider] = max(0.5, min(1.5, 1.0 + total * 0.1))
        return {"status": "ok", "provider": provider, "user_factor": _user_factors[provider]}

    @router.post("/admin/fiber/create")
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

    @router.post("/admin/fiber/{fiber_id}/fail")
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

    @router.post("/admin/fiber/{fiber_id}/commit")
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
                with db_conn() as conn:
                    provider = _get_provider_from_last_usage(conn)
                    if provider:
                        conn.execute(
                            "UPDATE usage SET checker_score = ? WHERE id = (SELECT id FROM usage WHERE provider = ? ORDER BY called_at DESC LIMIT 1)",
                            (score, provider))
                        conn.commit()
        return {"fiber_id": fiber_id, "status": "committed"}

    @router.get("/admin/fiber/tree")
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
    @router.get("/admin/agents/declaration")
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

    @router.get("/admin/agents/status")
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
    @router.get("/admin/logs")
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
    @router.post("/admin/fiber/check")
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
    @router.post("/admin/mcp/toggle")
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
    return router
