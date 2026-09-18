"""chat 主链：/v1/chat/completions（三池路由 + 降级）与 /v1/direct/chat/completions。

参数由 app.py 注入（见 docs/模块约定.md）。
"""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse


def build_chat_router(*,
    HTTPException,
    Request,
    Response,
    Router,
    StreamingResponse,
    _apply_quota_filter,
    _benchmark_loaded,
    _disabled_providers,
    _quality_factors,
    _quota_budget_level,
    _quota_write_error,
    _router_state,
    _score_by_runner_up,
    _scoring_state,
    _should_score,
    app,
    asyncio,
    cfg,
    check_rate_limit,
    db_conn,
    fiber_commit,
    fiber_create,
    fiber_fail,
    find_model_config,
    httpx,
    json,
    plan_pool_fallback,
    select_pool_by_keywords,
    select_provider_auto,
    select_provider_by_strategy,
    select_provider_with_runner_up,
    select_runner_up,
    time,
):
    """参数由 app.py 注入（见 docs/模块约定.md）；路由内容与抽出前逐字一致。"""
    router = APIRouter()
    @router.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        t0 = time.time()
        body = await request.json()
        # [2026-09-14] 维护通道哨兵：model ∈ {auto, *, gateway-auto, default-auto}
        # 视为「未显式指定模型」→ 走 auto（维护通道）。
        # 原因：Codex / DSH / Claude Code 等 OpenAI 兼容客户端**必须**带 model 字段，
        # 否则无法请求维护通道；而「完全不传 model」只有手写 curl 才做得到。
        _AUTO_SENTINELS = ("auto", "*", "gateway-auto", "default-auto")
        _model_raw = body.get("model", "")
        _is_auto_sentinel = str(_model_raw or "").strip().lower() in _AUTO_SENTINELS
        model = "DeepSeek-V4-Flash" if (not _model_raw or _is_auto_sentinel) else _model_raw
        messages = body.get("messages", [])
        stream = body.get("stream", False)
        # 用户自定义 key（从聊天页面带入），覆盖 provider 配置的 key
        user_key = body.pop("api_key", None)
        explicit_model = ("model" in body) and not _is_auto_sentinel
        kwargs = {k: v for k, v in body.items() if k not in ("model", "messages", "stream")}

        # 1. 模型名精确匹配（大小写不敏感，优先级最高）
        model_pool, pool_cfg, _, canonical_model = find_model_config(cfg, model)
        if model_pool:
            pool_name = model_pool
            model = canonical_model
            if not pool_cfg:
                pool_cfg = cfg.get("pools", {}).get(pool_name)
        else:
            pool_name = None
            pool_cfg = None

        # ── 提取消息文本用于模型路由 / 关键词路由 ──
        messages_text = json.dumps(messages, ensure_ascii=False)

        # ── 前置检查层：复杂度评估 + Token 概率检测 ──
        # 显式指定模型时跳过（用户知道自己要什么），减少白打网络请求
        _pre_check = {}

        # 取最后一条用户消息作分析文本（能力标签匹配与前置检查共享）
        probe_text = ""
        for m in reversed(messages):
            if isinstance(m, dict) and m.get("role") == "user" and m.get("content"):
                probe_text = m["content"]
                break

        if not explicit_model:
            try:
                from provider_router.assessor import complexity_assess, token_confidence, select_path
                if probe_text:
                    # 分支1: 复杂度评估（纯规则，<5ms）— 永不抛异常
                    _pre_check["complexity"] = complexity_assess(probe_text)
                    # 分支2: Token 概率检测（网络调用，需独立保护）
                    # 1s 硬超时 + httpx 超时，确保前置检查不拖慢主流程
                    _pool_a_cfg = cfg.get("pools", {}).get("pool_a", {})
                    _first_pv = (_pool_a_cfg.get("providers") or [None])[0]
                    if _first_pv:
                        try:
                            _pc = cfg.get("providers", {}).get(_first_pv["name"], {})
                            _api = _pc.get("api", "")
                            _key = (user_key or Router.resolve_env_key(_pc.get("api_key", "")))
                            if _first_pv.get("models"):
                                _pre_check["confidence"] = await asyncio.wait_for(
                                    token_confidence(probe_text, _api, _key, _first_pv["models"][0], timeout_ms=1000),
                                    timeout=1.2)
                        except Exception:
                            pass  # Token 概率检测失败不影响主流程和路径选择
                    # 路径选择（纯本地规则，<5ms）— 新增 probe_text 传参支持 complex 层两条路径
                    _routing_rules = cfg.get("routing_rules", {})
                    _rule = select_path(_pre_check, _routing_rules, text=probe_text)
                    _pre_check["rule"] = _rule
                    print(f"🔍 前置检查: level={_pre_check.get('complexity',{}).get('level')}, "
                          f"confidence={_pre_check.get('confidence',{}).get('confidence')}, "
                          f"rule_pool={_rule.get('pool','-')}")
            except Exception as _pe:
                # 前置检查不应影响主流程，出错静默降级
                pass

        # 2. 关键词路由（仅当模型路由未命中时使用）
        if not pool_name:
            kw_pool = select_pool_by_keywords(cfg, messages_text)
            if kw_pool:
                pool_name = kw_pool

        # 3. 兜底默认池
        if not pool_name:
            pool_name = cfg.get("routing", {}).get("default_pool", "pool_a")

        # 3b. 前置检查路径控制（仅当用户未显式指定模型时生效）
        #     规则表决定路径，不由模型决定；trivial 直接返回不调模型
        _path_rule = _pre_check.get("rule") if _pre_check else None
        if _path_rule and pool_name and not explicit_model and not user_key:
            _rule_pool = _path_rule.get("pool")
            # [2026-09-14] 维护通道（model=auto）不做 trivial 短路：
            # agent 排障时发来的短请求也应真正调用模型，而不是收到固定话术。
            if _path_rule.get("action") == "direct_return" and not _is_auto_sentinel:
                _msg = _path_rule.get("message") or "您好，请问有什么可以帮您？"
                app.state.req_counter.labels(pool="direct", provider="none", status="200").inc()
                return Response(
                    content=json.dumps({
                        "id": f"chatcmpl-precheck-{int(t0*1000)}",
                        "object": "chat.completion",
                        "created": int(t0),
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "message": {"role": "assistant", "content": _msg},
                            "finish_reason": "stop",
                        }],
                        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                    }),
                    status_code=200, media_type="application/json")
            if _rule_pool and _rule_pool != "fiber_split":
                # 规则指定的池在配置中存在且与关键词路由冲突时，规则优先
                _rule_pool_cfg = cfg.get("pools", {}).get(_rule_pool)
                if _rule_pool_cfg and any(p.get("name") not in _disabled_providers
                                          for p in _rule_pool_cfg.get("providers", [])):
                    pool_name = _rule_pool
                    # model 需属于该池，否则模型过滤会排除所有 provider
                    # → 规则选池时自动指定该池第一个可用 provider 的模型
                    if not any(model.lower() in [m.lower() for m in pv.get("models", [])]
                               for pv in _rule_pool_cfg.get("providers", [])):
                        for _pv in _rule_pool_cfg.get("providers", []):
                            if _pv.get("name") not in _disabled_providers and _pv.get("models"):
                                model = _pv["models"][0]
                                break

        # 3c. 复杂层两条路径：fiber_split（角色路径 / 智能体路径）
        #     very_complex 且 can_decompose → role_based；否则 → agent_based
        #     路径标记先初始化，供下方故障转移链写 usage 使用
        _used_path = "agent_based" if (_pre_check.get("rule", {}).get("path") == "agent_based") else "normal"
        _agent_id = None
        if (_path_rule and _path_rule.get("pool") == "fiber_split"
                and not explicit_model and not user_key):
            _sub_path = _path_rule.get("path", "agent_based")
            if _sub_path == "role_based":
                # ── 角色路径：拆解 → 分配角色 → 每角色独立选模型 → 执行 → 聚合 ──
                try:
                    from provider_router.multipath import (
                        decompose_task, build_subtask_messages, build_aggregate_messages,
                        role_capability_vector)
                    _subtasks = decompose_task(probe_text)
                    if _subtasks:
                        print(f"🔀 角色路径: 拆解为 {len(_subtasks)} 个子任务")
                        _parent_fid = fiber_create("complex_role", probe_text[:80])

                        # 汇总全部 provider（跨池）供角色选模型
                        _all_providers = []
                        for _pn, _pc in cfg.get("pools", {}).items():
                            # [2026-09-13] 跳过标记为 auto_routable=false 的池（如 pool_test），
                            # 使其只可按模型名显式调用，不进入自动路由候选
                            if _pc.get("auto_routable", True) is False:
                                continue
                            _all_providers.extend(_pc.get("providers", []))

                        _results = []
                        _agg_model_used = model
                        for _st in _subtasks:
                            _st_fid = fiber_create(f"role_{_st['role']}", _st['task'][:60], parent_id=_parent_fid)
                            _sub_msgs = build_subtask_messages(_st, messages)
                            # 角色 → 能力向量 → 能力标签匹配选模型
                            _cap_vec = role_capability_vector(_st.get("role"), cfg)
                            # model=None：跨池按能力选，不受用户 model 限制
                            # 额度过滤：角色子任务按 normal 预算（计划书 v1：每步前选可用模型）
                            _pv, _, _ = select_provider_with_runner_up(
                                _apply_quota_filter(_all_providers, _quota_budget_level(kwargs.get("max_tokens"), cfg)),
                                model=None,
                                query_caps=_cap_vec,
                                capability_threshold=0.2,
                            )
                            if not _pv:
                                _results.append(f"[{_st['role']}] 无可用 provider")
                                fiber_fail(_st_fid)
                                continue
                            _st_pcfg = cfg.get("providers", {}).get(_pv["name"], {})
                            _st_key = Router.resolve_env_key(_st_pcfg.get("api_key", ""))
                            if not _st_key:
                                _results.append(f"[{_st['role']}] key 未配置")
                                fiber_fail(_st_fid)
                                continue
                            _st_api = _st_pcfg.get("api", "").rstrip("/")
                            _st_model = _pv.get("models", [model])[0]
                            try:
                                async with httpx.AsyncClient(timeout=120) as _st_client:
                                    _st_resp = await _st_client.post(
                                        f"{_st_api}/chat/completions",
                                        json={"model": _st_model, "messages": _sub_msgs, "max_tokens": 2000},
                                        headers={"Authorization": f"Bearer {_st_key}"},
                                    )
                                    _st_data = _st_resp.json()
                                    _st_content = ""
                                    for _ch in _st_data.get("choices", []):
                                        _m = _ch.get("message", {})
                                        if isinstance(_m, dict):
                                            _st_content += _m.get("content", "") or ""
                                    _results.append(_st_content)
                                    # 额度感知（计划书 v1）：子任务失败时也做分类落库
                                    if _st_resp.status_code != 200:
                                        _quota_write_error(_pv["name"], _st_resp.status_code, _st_resp.text)
                                    try:
                                        with db_conn() as _conn:
                                            _conn.execute(
                                                "INSERT INTO usage (model, pool, provider, path_type, role, ok) "
                                                "VALUES (?,?,?,?,?,?)",
                                                (_st_model, "fiber_split", _pv["name"], "role_based", _st["role"],
                                                 1 if _st_resp.status_code == 200 else 0))
                                            _conn.commit()
                                    except Exception:
                                        pass
                                    fiber_commit(_st_fid)
                            except Exception as _st_e:
                                _results.append(f"[{_st['role']}] 执行失败: {str(_st_e)[:80]}")
                                fiber_fail(_st_fid)

                        # 聚合：调一次汇总模型整合各子任务结果
                        _agg_content = None
                        _agg_pv, _, _ = select_provider_with_runner_up(
                            _apply_quota_filter(
                                _all_providers,
                                _quota_budget_level(kwargs.get("max_tokens"), cfg)))
                        if _agg_pv:
                            _agg_pcfg = cfg.get("providers", {}).get(_agg_pv["name"], {})
                            _agg_key = Router.resolve_env_key(_agg_pcfg.get("api_key", ""))
                            _agg_api = _agg_pcfg.get("api", "").rstrip("/")
                            _agg_model = _agg_pv.get("models", [model])[0]
                            _agg_msgs = build_aggregate_messages(_subtasks, _results, messages)
                            try:
                                async with httpx.AsyncClient(timeout=120) as _agg_client:
                                    _agg_resp = await _agg_client.post(
                                        f"{_agg_api}/chat/completions",
                                        json={"model": _agg_model, "messages": _agg_msgs, "max_tokens": 2000},
                                        headers={"Authorization": f"Bearer {_agg_key}"},
                                    )
                                    _agg_data = _agg_resp.json()
                                    _agg_content = ""
                                    for _ch in _agg_data.get("choices", []):
                                        _m = _ch.get("message", {})
                                        if isinstance(_m, dict):
                                            _agg_content += _m.get("content", "") or ""
                                    if _agg_content:
                                        _agg_model_used = _agg_model
                            except Exception:
                                _agg_content = None

                        fiber_commit(_parent_fid)

                        if _agg_content:
                            return Response(
                                content=json.dumps({
                                    "id": f"chatcmpl-rolepath-{int(t0*1000)}",
                                    "object": "chat.completion",
                                    "created": int(t0),
                                    "model": _agg_model_used,
                                    "choices": [{
                                        "index": 0,
                                        "message": {"role": "assistant", "content": _agg_content},
                                        "finish_reason": "stop",
                                    }],
                                    "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                                }),
                                status_code=200, media_type="application/json")

                        # 聚合失败兜底：拼接各子任务结果
                        _fallback_content = "\n\n".join(
                            f"【{st.get('role')}】{st.get('task')}\n{r}"
                            for st, r in zip(_subtasks, _results))
                        return Response(
                            content=json.dumps({
                                "id": f"chatcmpl-rolepath-{int(t0*1000)}",
                                "object": "chat.completion",
                                "created": int(t0),
                                "model": model,
                                "choices": [{
                                    "index": 0,
                                    "message": {"role": "assistant", "content": _fallback_content},
                                    "finish_reason": "stop",
                                }],
                                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                            }),
                            status_code=200, media_type="application/json")
                except Exception as _role_e:
                    print(f"⚠️ 角色路径异常，降级智能体路径: {_role_e}")
                    # 降级：继续走智能体路径

            # ── 智能体路径：选一个智能体（= 选一个 provider），自行处理 ──
            # 取消 fiber_split 的 pool 控制，让下方故障转移链正常选 provider
            print(f"🔀 智能体路径: 交给单一智能体处理")
            _path_rule = None  # 释放路径控制，走常规路由
            _agent_id = "agent_single"
            # 由故障转移链记录 usage（path_type=agent_based 在写库处附加）

        # 4. 走故障转移链
        tried_pools = set()
        current_pool = pool_name
        last_error = "no available provider"
        used_provider = ""

        # [2026-09-18] auto 维护通道：某档的 provider 失败后，**排除它再按优先序选下一档**。
        # 背景：原来失败只会看 `pool_cfg.fallback`，而 pool_fallback 没有 fallback，
        # 于是一个额度耗尽/鉴权失败的兜底档会挡住后面的 '*' 档（实测 auto 直接 403/503）。
        _auto_failed = set()

        def _auto_advance():
            """auto 模式专用换档。成功则更新 current_pool/model/model_filter 并返回 True。"""
            nonlocal current_pool, model, model_filter, pool_cfg
            if not (_is_auto_sentinel and not user_key and strategy_mode == "auto"):
                return False
            if not used_provider or used_provider in _auto_failed:
                return False
            _auto_failed.add(used_provider)
            _list = []
            for _pc in cfg.get("pools", {}).values():
                if _pc.get("auto_routable", True) is False:
                    continue
                _list.extend(_pc.get("providers", []))
            _list = [p for p in _apply_quota_filter(_list, _quota_budget_level(kwargs.get("max_tokens"), cfg))
                     if p["name"] not in _auto_failed]
            _pv2, _, _ = select_provider_auto(_list, _router_state, cfg)
            if not _pv2:
                return False
            _pool2 = _m2 = None
            for _pn, _pcfg in cfg.get("pools", {}).items():
                for _x in _pcfg.get("providers", []):
                    if _x["name"] != _pv2["name"]:
                        continue
                    for _mm in (_x.get("models") or []):
                        if ("%s::%s" % (_pv2["name"], _mm)) not in _disabled_providers:
                            _pool2, _m2 = _pn, _mm
                            break
                if _pool2:
                    break
            if not _pool2:
                return False
            print(f"↩️ auto 换档 → {_pv2['name']} @ {_pool2} (model={_m2})", flush=True)
            current_pool, model, model_filter = _pool2, _m2, _m2
            pool_cfg = cfg["pools"][_pool2]
            tried_pools.discard(_pool2)
            return True

        def _advance_failure():
            """失败/无候选后找下一个候选，成功返回 True。

            ① auto 维护通道：排除失败 provider 后按优先序换档；
            ② 其它情况：沿 pool.fallback 链降级（pool_a 到 pool_b 到 pool_c），
               目标池没有该 model 时自动换成池内可用模型。

            [2026-09-18] 决策逻辑抽到 `provider_router/fallback.py`（纯函数 + 单测），
            这里只把决策应用到 current_pool / model / model_filter / pool_cfg。
            """
            nonlocal current_pool, model, model_filter, pool_cfg
            if _auto_advance():
                return True
            plan = plan_pool_fallback(cfg, current_pool, model, tried_pools, _disabled_providers)
            if not plan:
                return False
            if str(plan.model).lower() != str(model).lower():
                print(f"↩️ 降级换模型: {model} → {plan.model} @ {plan.pool}", flush=True)
                model, model_filter = plan.model, plan.model
            current_pool = plan.pool
            pool_cfg = cfg.get("pools", {}).get(plan.pool) or pool_cfg
            return True

        # 能力标签匹配：提取一次，复用多池迭代
        _query_caps = None
        _threshold = None
        if not user_key and not explicit_model and probe_text:
            from provider_router.assessor import extract_query_capabilities
            _query_caps = extract_query_capabilities(probe_text, cfg)
            # 全 0 向量 = 无明确能力需求 → 用 None 跳过能力过滤
            if not any(v > 0 for v in _query_caps.values()):
                _query_caps = None
            else:
                _threshold = cfg.get("capability_threshold", 0.3)

        while current_pool and current_pool not in tried_pools:
            tried_pools.add(current_pool)
            pool_cfg = cfg.get("pools", {}).get(current_pool)
            if not pool_cfg:
                break

            # 4. 按路由策略选 provider（v2.8 模型路由）
            #    formula: 现有权重轮询（同时选出第二名作为潜在检查者）
            #    model/hybrid: 先调用外部路由模型服务，失败时按配置降级
            #    auto: 按优先级档位跨池自动选（不手动指定模型）
            # 用户自定义 key 时不限制模型名（用户用自己的 key 调任何 provider），
            # 否则只选有该模型的 provider
            model_filter = None if user_key else model
            strategy_mode = (cfg.get("routing_strategy") or {}).get("mode", "formula")

            # ── auto 模式：跨池按优先级档位自动选（仅当用户未显式指定模型）──
            if (strategy_mode == "auto" and select_provider_auto
                    and not explicit_model and not user_key):
                _all_pv = []
                for _pc in cfg.get("pools", {}).values():
                    # [2026-09-13] 同上：跳过 auto_routable=false 的池
                    if _pc.get("auto_routable", True) is False:
                        continue
                    _all_pv.extend(_pc.get("providers", []))
                _budget_a = _quota_budget_level(kwargs.get("max_tokens"), cfg)
                _auto_pool_list = _apply_quota_filter(_all_pv, _budget_a)
                _auto_pv, _auto_ru, _ = select_provider_auto(_auto_pool_list, _router_state, cfg)
                if _auto_pv:
                    # [2026-09-14 修复] 反查池必须**同时匹配 provider 名字和这次要用的模型**。
                    # 同一 provider 名会出现在多个池里（deepseek-direct 在 pool_a/b/c 都有，
                    # 但各池挂的模型不同）。只按名字找第一个池，会把
                    # pool_c/deepseek-direct(deepseek-chat) 错认成 pool_a/deepseek-direct(deepseek-flash)，
                    # 随后用 model=deepseek-chat 在该池过滤 -> 无候选 -> 误报 all pools exhausted。
                    _ms = _auto_pv.get("models") or [model]
                    _ok_ms = [m for m in _ms
                              if ("%s::%s" % (_auto_pv["name"], m)) not in _disabled_providers]
                    model = (_ok_ms or _ms)[0]
                    _auto_pool = None
                    for _pn, _pcfg in cfg.get("pools", {}).items():
                        if any(x["name"] == _auto_pv["name"] and model in x.get("models", [])
                               for x in _pcfg.get("providers", [])):
                            _auto_pool = _pn
                            break
                    if _auto_pool:
                        print(f"🤖 auto 路由 → {_auto_pv['name']} @ {_auto_pool} (model={model})", flush=True)
                        current_pool = _auto_pool
                        tried_pools.add(_auto_pool)
                        pool_cfg = cfg["pools"][_auto_pool]
                        model_filter = model
                else:
                    print(f"🤖 auto 无可用档位（候选={[p['name'] for p in _auto_pool_list]}）", flush=True)

            strategy_pv = None
            if select_provider_by_strategy and strategy_mode in ("model", "hybrid") and messages_text:
                strategy_pv = select_provider_by_strategy(
                    pool_cfg.get("providers", []), cfg, model=model_filter,
                    query=messages_text, session_id=body.get("session_id"),
                    query_caps=_query_caps, capability_threshold=_threshold)
                # model 模式 + fallback=error：模型路由失败直接 503，不走降级
                if strategy_pv is None and strategy_mode == "model":
                    fallback = (cfg.get("routing_strategy", {}).get("model_router", {}) or {}).get("fallback", "formula")
                    if fallback == "error":
                        last_error = "model router failed (fallback=error)"
                        current_pool = None
                        break
            if strategy_pv:
                pv, runner_up = strategy_pv, None
            else:
                # 额度过滤（计划书 v1）：剔除 exhausted / low+large 预算
                _budget = _quota_budget_level(kwargs.get("max_tokens"), cfg)
                _cands = _apply_quota_filter(pool_cfg.get("providers", []), _budget)
                pv, runner_up, _ = select_provider_with_runner_up(
                    _cands, model=model_filter,
                    query_caps=_query_caps, capability_threshold=_threshold)
                if pv and not runner_up:
                    # [2026-09-18] 池内各 provider 模型名互不相同 -> 带 model 过滤后只剩 1 个候选，
                    # runner_up 会是 None，导致在线监督者评分永不触发。这里放宽为「同池其它可用 provider」。
                    runner_up = select_runner_up(_cands, _router_state, pv["name"],
                                                query_caps=_query_caps, capability_threshold=_threshold)
            if not pv:
                last_error = f"pool '{current_pool}' all providers disabled"
                if _advance_failure():
                    continue
                current_pool = pool_cfg.get("fallback")
                continue

            provider_cfg = cfg.get("providers", {}).get(pv["name"])
            # 有效 key：用户自定义 key 优先，否则用 provider 配置的 key（支持 ${ENV} 引用）
            configured_key = Router.resolve_env_key(provider_cfg.get("api_key", "")) if provider_cfg else ""
            effective_key = user_key or configured_key
            if not provider_cfg or not effective_key:
                last_error = f"provider '{pv['name']}' key not resolved"
                if _advance_failure():
                    continue
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
                    # 用当前 provider 自己的模型名（大小写按 YAML 配置来）
                    provider_model = model
                    for m in pv.get("models", []):
                        if m.lower() == model.lower():
                            provider_model = m
                            break
                    req_body = {"model": provider_model, "messages": messages, "stream": stream, **kwargs}
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
                        with db_conn() as conn:
                            conn.execute(
                                "INSERT INTO usage (model, pool, provider, prompt_tokens, completion_tokens, ok, path_type, agent_id) "
                                "VALUES (?,?,?,?,?,?,?,?)",
                                (model, current_pool, pv["name"],
                                 data.get("usage", {}).get("prompt_tokens", 0),
                                 data.get("usage", {}).get("completion_tokens", 0),
                                 1 if status_code == 200 else 0,
                                 _used_path, _agent_id))
                            conn.commit()
                    except Exception:
                        pass

                    # 额度分类（计划书 v1）：402 / 429 / 关键词 → 写状态并当场尝试下一个候选
                    if _quota_write_error(pv["name"], status_code, resp_body):
                        last_error = f"quota exhausted: {pv['name']}"
                        app.state.req_counter.labels(pool=current_pool, provider=pv["name"], status="quota").inc()
                        if _advance_failure():
                            continue
                        current_pool = pool_cfg.get("fallback")
                        continue

                    # [2026-09-18] 401/403 属 provider 侧问题（key 失效 / 额度 / 权限），
                    # 不是客户端错误：auto 维护通道继续换档；显式指定模型时仍原样透传（便于诊断）。
                    if status_code in (401, 403):
                        app.state.req_counter.labels(pool=current_pool, provider=pv["name"], status=str(status_code)).inc()
                        _b = (resp_body or "")[:160].replace("\n", " ")
                        print(f"🔑 上游 {status_code}: {pv['name']} model={model} → {_b}", flush=True)
                        last_error = f"upstream {status_code} from {pv['name']}"
                        if _advance_failure():
                            continue
                        return Response(content=resp_body, status_code=status_code, media_type="application/json")

                    # 失败但不 fallback 的情况（HTTP 4xx 是客户端问题）
                    if status_code in (400, 404, 422):
                        app.state.req_counter.labels(pool=current_pool, provider=pv["name"], status=str(status_code)).inc()
                        app.state.req_duration.labels(provider=pv["name"]).observe(time.time() - t0)
                        return Response(content=resp_body, status_code=status_code, media_type="application/json")

                    # 5xx → fallback 到下一个池
                    if status_code >= 500:
                        last_error = f"HTTP {status_code}"
                        print(f"🔴 上游 {status_code}: {pv['name']} model={model}")
                        if _advance_failure():
                            continue
                        current_pool = pool_cfg.get("fallback")
                        continue

                    app.state.req_counter.labels(pool=current_pool, provider=pv["name"], status="200").inc()
                    app.state.req_duration.labels(provider=pv["name"]).observe(time.time() - t0)
                    # 第二名检查者：自适应采样频率
                    # 默认开启，冷启动每次都审 → 稳态概率衰减 → 大变量强制复审
                    # 若已加载基准评分（benchmark_loaded），跳过在线评分，零 Token 开销
                    if runner_up and not user_key and not _benchmark_loaded:
                        decision, reason = _should_score(cfg, pv["name"], runner_up, _scoring_state)
                        if decision:
                            asyncio.get_event_loop().create_task(
                                _score_by_runner_up(
                                    cfg=cfg, provider=pv["name"], runner_up=runner_up,
                                    provider_model=provider_model,
                                    messages=messages, resp_body=resp_body,
                                    quality_factors=_quality_factors,
                                    scoring_state=_scoring_state,
                                )
                            )
                    return Response(content=resp_body, status_code=status_code, media_type="application/json")

            except httpx.TimeoutException:
                last_error = "timeout"
                print(f"⏱️ 超时: {pv['name']} model={model} timeout=120s")
                if _advance_failure():
                    continue
                current_pool = pool_cfg.get("fallback")
                continue
            except httpx.ConnectError:
                last_error = "unreachable"
                print(f"🔌 不可达: {pv['name']} api={provider_cfg.get('api','?')}")
                if _advance_failure():
                    continue
                current_pool = pool_cfg.get("fallback")
                continue
            except Exception as e:
                last_error = str(e)
                print(f"⚠️ 请求异常: {pv['name']} model={model} error={e}")
                if _advance_failure():
                    continue
                current_pool = pool_cfg.get("fallback")
                continue

        app.state.req_counter.labels(pool=pool_name, provider=used_provider or "none", status="503").inc()
        app.state.req_duration.labels(provider=used_provider or "none").observe(time.time() - t0)
        raise HTTPException(status_code=503, detail=f"all pools exhausted: {last_error}")

    # ── 直连端点：不走池路由、关键词匹配、故障转移 ──
    @router.post("/v1/direct/chat/completions")
    async def direct_chat_completions(request: Request):
        """直接调用指定 provider，不走任何路由逻辑。

        请求体与 /v1/chat/completions 相同，额外支持:
        - `_provider`: 指定 provider 名称（如 scnet-tp、deepseek-direct），可选
        - 若不指定，自动从 model 名查找所属 provider
        """
        t0 = time.time()
        body = await request.json()
        model = body.get("model", "DeepSeek-V4-Flash")
        messages = body.get("messages", [])
        stream = body.get("stream", False)
        kwargs = {k: v for k, v in body.items() if k not in ("model", "messages", "stream", "_provider")}

        direct_provider_name = body.get("_provider")
        if not direct_provider_name:
            # 自动从 model 名查找第一个匹配的 provider
            _, _, pv, m = find_model_config(cfg, model)
            if pv:
                direct_provider_name = pv["name"]
                model = m
            if not direct_provider_name:
                raise HTTPException(status_code=400, detail=f"model '{model}' not found in any provider")

        provider_cfg = cfg.get("providers", {}).get(direct_provider_name)
        direct_key = Router.resolve_env_key(provider_cfg.get("api_key", "")) if provider_cfg else ""
        if not provider_cfg or not direct_key:
            raise HTTPException(status_code=400, detail=f"provider '{direct_provider_name}' not configured or key not resolved")
        api = provider_cfg["api"].rstrip("/")

        async with httpx.AsyncClient(timeout=120) as client:
            req_body = {"model": model, "messages": messages, "stream": stream, **kwargs}
            resp = await client.post(
                f"{api}/chat/completions",
                json=req_body,
                headers={"Authorization": f"Bearer {direct_key}"},
            )

        if stream:
            return StreamingResponse(resp.aiter_bytes(), media_type="text/event-stream", status_code=resp.status_code)

        app.state.req_counter.labels(pool="direct", provider=direct_provider_name, status=str(resp.status_code)).inc()
        app.state.req_duration.labels(provider=direct_provider_name).observe(time.time() - t0)
        return Response(content=resp.text, status_code=resp.status_code, media_type="application/json")

    return router
