"""ops-gateway-core — Hermes 网关核心（合并包）。

v3.9: 将 hermes_cfg / hermes_fiber / hermes_api / hermes_ops 四个内部包
合并为单一 ops-gateway-core 包，对外暴露统一 API。

对外依赖：
- ops-provider-router：三池路由引擎
- ops-fiber-tree：Fiber 任务树存储
"""

from .cfg import ConfigLoader, get_db, init_registry
from .fiber import FiberRuntime, Fiber
from .api import build_app, _should_score, _score_by_runner_up
from .ops import (
    probe_model,
    probe_all,
    call_provider_http,
    cmd_models,
    cmd_usage,
    cmd_quality,
    cmd_feedback_stats,
    cmd_git_log,
    cmd_git_diff,
    cmd_sync_runtime,
    cmd_undo_remote,
    cmd_undo_list_remote,
    cmd_fiber_view,
    cmd_check_deps,
    cmd_scan_agents,
    resolve_agent_target,
    resolve_by_docker,
    resolve_by_compose,
    cmd_benchmark,
    load_quality_benchmark,
    BENCHMARK_QUESTIONS,
    BENCHMARK_FILENAME,
)

__all__ = [
    "ConfigLoader", "get_db", "init_registry",
    "FiberRuntime", "Fiber",
    "build_app", "_should_score", "_score_by_runner_up",
    "probe_model", "probe_all", "call_provider_http",
    "cmd_models", "cmd_usage", "cmd_quality", "cmd_feedback_stats",
    "cmd_git_log", "cmd_git_diff", "cmd_sync_runtime",
    "cmd_undo_remote", "cmd_undo_list_remote", "cmd_fiber_view",
    "cmd_check_deps", "cmd_scan_agents",
    "resolve_agent_target", "resolve_by_docker", "resolve_by_compose",
    "cmd_benchmark", "load_quality_benchmark", "BENCHMARK_QUESTIONS", "BENCHMARK_FILENAME",
]
