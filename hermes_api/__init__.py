"""hermes_api — HTTP API 层（Shim）

从 ops-gateway-core 重新导出，保持向后兼容。

已废弃的兼容壳：仅为旧代码的 `import hermes_*` 保留；仓库内已无引用，新代码一律用 `ops_gateway_core.*`。确认外部也不用后可整体删除本目录。
"""

from ops_gateway_core.api import build_app, _should_score, _score_by_runner_up  # noqa: F401

__all__ = ["build_app", "_should_score", "_score_by_runner_up"]