"""hermes_api — HTTP API 层（Shim）

从 ops-gateway-core 重新导出，保持向后兼容。
"""

from ops_gateway_core.api import build_app, _should_score, _score_by_runner_up  # noqa: F401

__all__ = ["build_app", "_should_score", "_score_by_runner_up"]