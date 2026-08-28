"""hermes_api — HTTP API 层构建器。

v3.2: 从 gateway.py 拆分。build_app(cfg, deps) 返回 FastAPI 实例，
所有共享状态通过 deps 注入，避免循环依赖。
"""

from .app import build_app

__all__ = ["build_app"]