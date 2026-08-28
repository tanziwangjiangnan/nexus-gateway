"""hermes_ops — 操作层（CLI 命令 + 健康探测 + 反向依赖检查 + 智能体发现）。

v3.3: 从 gateway.py 拆分。
"""

from .probe import probe_model, probe_all, call_provider_http
from .commands import (
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
)
from .check_deps import cmd_check_deps
from .scan_agents import cmd_scan_agents

__all__ = [
    "probe_model", "probe_all", "call_provider_http",
    "cmd_models", "cmd_usage", "cmd_quality", "cmd_feedback_stats",
    "cmd_git_log", "cmd_git_diff", "cmd_sync_runtime",
    "cmd_undo_remote", "cmd_undo_list_remote", "cmd_fiber_view",
    "cmd_check_deps",
    "cmd_scan_agents",
]