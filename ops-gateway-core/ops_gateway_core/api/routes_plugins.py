"""统一插件调用端点：能力校验 → 重复调用拦截 → 成功即登记逆操作。

参数由 app.py 注入（见 docs/模块约定.md）。
"""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse


def build_plugins_router(*,
    HTTPException,
    Request,
    _APPROVAL_TTL,
    _approval_cache,
    _execute_plugin,
    _fiber_runtime,
    _format_string,
    _global_call_add,
    _global_call_lookup,
    _serial_locks,
    _throttle_windows,
    asyncio,
    cfg,
    fiber_commit,
    fiber_create,
    fiber_fail,
    fiber_register,
    hashlib,
    httpx,
    json,
    subprocess,
    time,
):
    """参数由 app.py 注入（见 docs/模块约定.md）；路由内容与抽出前逐字一致。"""
    router = APIRouter()
    # ── 插件调用端点：能力校验 → 重复调用拦截 → 成功即登记逆操作 ──
    @router.post("/v1/plugins/{plugin_id}/call")
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



    return router
