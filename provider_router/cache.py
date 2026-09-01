"""路由缓存 — 相同查询在 TTL 内复用路由结果，避免高频重复调用模型路由服务。"""
import time
import threading
from typing import Optional


class RouteCache:
    """线程安全的 TTL 路由缓存。

    缓存键为 query 的哈希，存储选中的 provider 名称。
    """

    def __init__(self, ttl_seconds: int = 300):
        self._ttl = ttl_seconds
        self._data: dict[int, tuple[str, float]] = {}  # hash → (provider, expiry)
        self._lock = threading.Lock()

    def get(self, query: str) -> Optional[str]:
        """返回缓存的 provider 名，若不存在或已过期返回 None。"""
        h = hash(query)
        with self._lock:
            entry = self._data.get(h)
            if entry is None:
                return None
            provider, expiry = entry
            if time.time() > expiry:
                del self._data[h]
                return None
            return provider

    def set(self, query: str, provider: str):
        """缓存查询结果。"""
        h = hash(query)
        with self._lock:
            self._data[h] = (provider, time.time() + self._ttl)

    def clear(self):
        """清空全部缓存。"""
        with self._lock:
            self._data.clear()

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._data)


# 全局单例，默认 300s TTL
_global_cache = RouteCache(ttl_seconds=300)


def get_cache() -> RouteCache:
    return _global_cache


def set_cache_ttl(ttl_seconds: int):
    """热更新 TTL（不影响已有缓存项）。"""
    _global_cache._ttl = ttl_seconds
