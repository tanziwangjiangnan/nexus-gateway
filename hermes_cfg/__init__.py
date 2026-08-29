"""hermes_cfg — 配置管理（Shim）

从 ops-gateway-core 重新导出，保持向后兼容。
"""

from ops_gateway_core.cfg import ConfigLoader, get_db, init_registry  # noqa: F401

__all__ = ["ConfigLoader", "get_db", "init_registry"]