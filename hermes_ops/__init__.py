"""hermes_ops — 操作层（Shim）

从 ops-gateway-core 重新导出，保持向后兼容。

已废弃的兼容壳：仅为旧代码的 `import hermes_*` 保留；仓库内已无引用，新代码一律用 `ops_gateway_core.*`。确认外部也不用后可整体删除本目录。
"""

from ops_gateway_core.ops import (
    probe_model, probe_all, call_provider_http,
    cmd_models, cmd_usage, cmd_quality, cmd_feedback_stats,
    cmd_git_log, cmd_git_diff, cmd_sync_runtime,
    cmd_undo_remote, cmd_undo_list_remote, cmd_fiber_view,
    cmd_check_deps, cmd_scan_agents,
    resolve_agent_target, resolve_by_docker, resolve_by_compose,
)  # noqa: F401

__all__ = [
    "probe_model", "probe_all", "call_provider_http",
    "cmd_models", "cmd_usage", "cmd_quality", "cmd_feedback_stats",
    "cmd_git_log", "cmd_git_diff", "cmd_sync_runtime",
    "cmd_undo_remote", "cmd_undo_list_remote", "cmd_fiber_view",
    "cmd_check_deps", "cmd_scan_agents",
    "resolve_agent_target", "resolve_by_docker", "resolve_by_compose",
]