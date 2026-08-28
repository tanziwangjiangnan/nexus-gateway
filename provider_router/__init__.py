"""Provider Router — 三池路由引擎

独立于 FastAPI 的纯路由逻辑，零 Web 依赖。
所有可变状态通过 RouterState 注入，由调用方维护。
"""

from .router import Router, RouterState
from .monitor import CircuitBreakerMonitor
from .config import load_config

__all__ = ["Router", "RouterState", "CircuitBreakerMonitor", "load_config"]