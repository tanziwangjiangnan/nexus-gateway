#!/usr/bin/env python3
"""Rewrite monitor.py cleanly."""
content = r'''"""Circuit Breaker Monitor — 后台熔断/健康监控线程

自动熔断：滑动窗口错误率超过阈值 → 自动禁用，低于恢复阈值 → 自动恢复。
动态权重：base_weight × (1 - err_rate)，保底 0.1。
质量/用户因子公式可通过回调注入，默认使用线性映射。
"""
import threading
import time
from typing import Callable


class CircuitBreakerMonitor:
    """后台熔断监控线程，每 interval 秒扫描一次。

    参数:
        get_db: 返回数据库连接的可调用对象
        cfg_getter: 返回配置字典的可调用对象
        disabled_providers: 禁用 provider 集合（调用方维护）
        dynamic_weights: 动态权重字典（调用方维护）
        quality_factors: 质量信誉因子字典（调用方维护）
        user_factors: 用户信誉因子字典（调用方维护）
        undo_register: 可选，注册逆操作的回调
        interval: 扫描间隔秒数
        error_threshold: 错误率高于此值触发熔断，默认 0.20
        recover_threshold: 错误率低于此值自动恢复，默认 0.10
        quality_factor_fn: 可选，签名 (scores: list[float]) -> float，默认 0.5 + (avg/100)*0.5
        user_factor_fn: 可选，签名 (feedbacks: list[int]) -> float，默认 max(0.5, min(1.5, 1.0+total*0.1))
    """

    def __init__(self, get_db: Callable, cfg_getter: Callable,
                 disabled_providers: set, dynamic_weights: dict,
                 quality_factors: dict, user_factors: dict,
                 undo_register: Callable = None,
                 interval: float = 30.0,
                 lock: threading.Lock = None,
                 error_threshold: float = 0.20,
                 recover_threshold: float = 0.10,
                 quality_factor_fn: Callable = None,
                 user_factor_fn: Callable = None):
        self.get_db = get_db
        self.cfg_getter = cfg_getter
        self.disabled_providers = disabled_providers
        self.dynamic_weights = dynamic_weights
        self.quality_factors = quality_factors
        self.user_factors = user_factors
        self.undo_register = undo_register
        self.interval = interval
        self.lock = lock or threading.Lock()
        self.error_threshold = error_threshold
        self.recover_threshold = recover_threshold
        self.quality_factor_fn = quality_factor_fn or self._default_quality_factor
        self.user_factor_fn = user_factor_fn or self._default_user_factor
        self._thread = None

    @staticmethod
    def _default_quality_factor(scores: list) -> float:
        """默认质量因子：0->0.5, 50->0.75, 100->1.0"""
        if not scores:
            return 1.0
        avg = sum(scores) / len(scores)
        return 0.5 + (avg / 100.0) * 0.5

    @staticmethod
    def _default_user_factor(feedbacks: list) -> float:
        """默认用户因子：1 + total*0.1, 范围 0.5~1.5"""
        total = sum(feedbacks)
        return max(0.5, min(1.5, 1.0 + total * 0.1))

    def start(self):
        """启动后台线程（daemon）。"""
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        while True:
            time.sleep(self.interval)
            try:
                self._scan()
            except Exception as e:
                print(f"熔断循环异常: {e}")

    def _scan(self):
        cfg = self.cfg_getter()
        conn = self.get_db()
        rows = conn.execute("""
            SELECT provider,
                   COUNT(*) as total,
                   SUM(ok) as success
            FROM usage
            WHERE called_at > datetime('now', '-5 minutes')
            GROUP BY provider
        """).fetchall()

        # 质量信誉因子（检查者评分驱动）
        quality_cfg = cfg.get("quality_feedback", {})
        quality_window = quality_cfg.get("quality_window", 20)
        quality_min_samples = quality_cfg.get("quality_min_samples", 5)
        qf_enabled = quality_cfg.get("enabled", True)

        if qf_enabled:
            all_providers = set()
            for pc in cfg.get("pools", {}).values():
                for pv in pc.get("providers", []):
                    all_providers.add(pv["name"])
            for provider in all_providers:
                qrows = conn.execute(
                    "SELECT checker_score FROM usage WHERE provider = ? AND checker_score IS NOT NULL ORDER BY called_at DESC LIMIT ?",
                    (provider, quality_window)
                ).fetchall()
                scores = [r[0] for r in qrows if r[0] is not None]
                if len(scores) >= quality_min_samples:
                    self.quality_factors[provider] = self.quality_factor_fn(scores)
                else:
                    self.quality_factors[provider] = 1.0

            # 用户信誉因子（用户反馈驱动）
            user_window = quality_cfg.get("user_window", 20)
            for provider in all_providers:
                urows = conn.execute(
                    "SELECT user_feedback FROM usage WHERE provider = ? AND user_feedback != 0 ORDER BY called_at DESC LIMIT ?",
                    (provider, user_window)
                ).fetchall()
                feedbacks = [r[0] for r in urows]
                self.user_factors[provider] = self.user_factor_fn(feedbacks)

        conn.close()
        for r in rows:
            name = r["provider"]
            total = r["total"]
            if total < 5:
                continue
            success = r["success"] or 0
            err_rate = 1.0 - (success / total)
            is_disabled = name in self.disabled_providers

            # 熔断：错误率超过阈值 -> 自动禁用
            if err_rate > self.error_threshold and not is_disabled:
                with self.lock:
                    self.disabled_providers.add(name)
                if self.undo_register:
                    self.undo_register(f"自动熔断禁用 {name} (err={err_rate:.0%})",
                                       lambda n=name: self.disabled_providers.discard(n))
                print(f"熔断: {name} 错误率 {err_rate:.0%} -> 已禁用")

            # 恢复：错误率低于恢复阈值且是被熔断禁用的 -> 自动恢复
            elif err_rate < self.recover_threshold and is_disabled:
                with self.lock:
                    self.disabled_providers.discard(name)
                print(f"恢复: {name} 错误率 {err_rate:.0%} -> 已启用")

            # 动态权重：base_weight * (1 - err_rate)，保底 0.1
            base = 1.0
            for pc in cfg.get("pools", {}).values():
                for pv in pc.get("providers", []):
                    if pv["name"] == name:
                        base = pv.get("weight", 1.0)
                        break
            self.dynamic_weights[name] = max(base * (1.0 - err_rate), 0.1)
'''

import os
path = os.path.join(os.path.dirname(__file__), 'provider_router', 'monitor.py')
with open(path, 'w', encoding='utf-8') as f:
    f.write(content)
print(f'OK: {path}')