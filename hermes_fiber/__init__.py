"""hermes_fiber — Fiber 运行时（Shim）

从 ops-gateway-core 重新导出，保持向后兼容。
"""

from ops_gateway_core.fiber import FiberRuntime, Fiber  # noqa: F401

__all__ = ["FiberRuntime", "Fiber"]