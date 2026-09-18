"""Circuit Breaker Monitor — 后台熔断/健康监控线程

自动熔断：滑动窗口错误率超过阈值 → 自动禁用，低于恢复阈值 → 自动恢复。
动态权重：base_weight × (1 - err_rate)，保底 0.1。
质量/用户因子公式可通过回调注入，默认使用线性映射。
"""
import json
import threading
import time
import urllib.error
import urllib.request
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
        quality_factor_fn: 可选，签名 (scores: list[float]) -> float
        user_factor_fn: 可选，签名 (feedbacks: list[int]) -> float
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
        # [2026-09-14 修复] 排除测试池（auto_routable=false）里的模型：
        # 它们本来就不可用，其调用失败不该计入共享 provider 的错误率，
        # 否则一个注定失败的模型就能把整个 provider 熔断（已造成过 8.5 小时故障）。
        test_models = []
        for _pc in cfg.get("pools", {}).values():
            if _pc.get("auto_routable", True) is False:
                for _pv in _pc.get("providers", []):
                    test_models.extend(_pv.get("models", []))
        if test_models:
            _qmarks = ",".join("?" * len(test_models))
            rows = conn.execute(
                f"""SELECT provider, model, COUNT(*) as total, SUM(ok) as success
                FROM usage
                WHERE called_at > datetime('now', '-5 minutes')
                  AND model NOT IN ({_qmarks})
                GROUP BY provider, model""",
                tuple(test_models)).fetchall()
        else:
            rows = conn.execute("""
                SELECT provider, model,
                       COUNT(*) as total,
                       SUM(ok) as success
                FROM usage
                WHERE called_at > datetime('now', '-5 minutes')
                GROUP BY provider, model""").fetchall()

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

        # ── 熔断/恢复/动态权重：基于 5 分钟滑动窗口错误率 ──
        # [2026-09-14] 粒度改为**模型级**：熔断键 "provider::model"。
        # 原来按 provider 统计，单个模型的故障会把整个 provider（及其它健康模型）一起禁用。
        # 现在只禁故障模型本身；provider 级禁用仅由「主动探测」路径负责（端点不可达才是 provider 的问题）。
        prov_agg = {}
        for r in rows:
            name = r["provider"]
            model = r["model"]
            total = r["total"]
            a = prov_agg.setdefault(name, [0, 0])
            a[0] += total
            a[1] += (r["success"] or 0)
            if total < 5:
                continue
            success = r["success"] or 0
            err_rate = 1.0 - (success / total)
            key = "%s::%s" % (name, model)
            is_disabled = key in self.disabled_providers

            # 熔断：该模型错误率超阈值 -> 只禁这个模型
            if err_rate > self.error_threshold and not is_disabled:
                with self.lock:
                    self.disabled_providers.add(key)
                if self.undo_register:
                    self.undo_register(f"模型熔断 {key} (err={err_rate:.0%})",
                                       lambda k=key: self.disabled_providers.discard(k))
                print(f"熔断(模型级): {key} 错误率 {err_rate:.0%} -> 已禁用该模型")

            # 恢复：该模型错误率回到阈值以下 -> 解除
            elif err_rate < self.recover_threshold and is_disabled:
                with self.lock:
                    self.disabled_providers.discard(key)
                print(f"恢复(模型级): {key} 错误率 {err_rate:.0%} -> 已启用")

        # 动态权重仍按 provider 聚合（影响 provider 之间的权重分配）
        for name, (total, success) in prov_agg.items():
            err_rate = 1.0 - (success / total) if total else 0.0
            base = 1.0
            for pc in cfg.get("pools", {}).values():
                for pv in pc.get("providers", []):
                    if pv["name"] == name:
                        base = pv.get("weight", 1.0)
                        break
            self.dynamic_weights[name] = max(base * (1.0 - err_rate), 0.1)

        # ── 主动探测：usage 无流量的 provider 做一次 HTTP 连通性探测 ──
        # 打通 probe/registry/熔断三孤岛：探测失败 -> 熔断禁用 + registry=down
        active = {r["provider"] for r in rows}
        handled_providers = set()   # [2026-09-14] 每个 provider 每轮只探一次
        for pc in cfg.get("pools", {}).values():
            # [2026-09-14 修复] 跳过测试池（auto_routable=false）：
            # 主动探测取的是 provider 在该池里的 models[0]，而测试池里放的都是
            # 注定不可用的模型（图像模型/已废弃模型名），用它们探测会把
            # 共享同一 provider 的生产模型一起熔断 —— 这是 09-13 夜 8.5 小时故障的直接原因。
            if pc.get("auto_routable", True) is False:
                continue
            for pv in pc.get("providers", []):
                name = pv["name"]
                if name in active or name in handled_providers:
                    # [2026-09-14] 同一 provider 出现在多个池时只探第一次（最早=主池里的模型），
                    # 否则 pool_b 里 qfg-new 的 models[0]=gpt-5.4（间歇失败）会把整个 provider
                    # 判死，而下一个池又探成功再启用，写成 enable/disable 来回翻转。
                    continue
                handled_providers.add(name)
                pc_raw = cfg.get("providers", {}).get(name, {})
                api = pc_raw.get("api", "")
                key = pc_raw.get("api_key", "")
                if not api or not key:
                    continue
                is_disabled = name in self.disabled_providers
                try:
                    models = pv.get("models", [])
                    probe_model = models[0] if models else "probe"
                    body = json.dumps({"model": probe_model,
                                       "messages": [{"role": "user", "content": "ping"}],
                                       "max_tokens": 1}).encode()
                    t0 = time.time()
                    req = urllib.request.Request(
                        f"{api.rstrip('/')}/chat/completions",
                        data=body,
                        headers={"Content-Type": "application/json",
                                 "Authorization": f"Bearer {key}"},
                        method="POST",
                    )
                    resp = urllib.request.urlopen(req, timeout=30)
                    latency = int((time.time() - t0) * 1000)
                    ok = resp.status == 200
                    if ok and is_disabled:
                        with self.lock:
                            self.disabled_providers.discard(name)
                        print(f"探测恢复: {name} 可达 -> 已启用")
                    conn.execute(
                        "INSERT INTO health_log (model, pool, provider, ok, latency_ms, error) VALUES (?,?,?,?,?,?)",
                        (probe_model, "unknown", name, 1 if ok else 0, latency, ""))
                    conn.execute(
                        "UPDATE registry SET status=?, updated_at=datetime('now') WHERE provider=? AND status != ?",
                        ("healthy" if ok else "down", name, "healthy" if ok else "down"))
                except urllib.error.HTTPError as e:
                    err_str = str(e)
                    # 401/403 = 鉴权或配置错误，不熔断（上游可达，只是 key 有问题）
                    if e.code in (401, 403):
                        if not is_disabled:
                            print(f"探测告警: {name} 鉴权错误 ({e.code}) -> registry=unhealthy, 但不熔断")
                        conn.execute(
                            "INSERT INTO health_log (model, pool, provider, ok, latency_ms, error) VALUES (?,?,?,?,?,?)",
                            ("probe", "unknown", name, 0, 0, err_str[:200]))
                        conn.execute(
                            "UPDATE registry SET status='unhealthy', updated_at=datetime('now') WHERE provider=? AND status != 'healthy'",
                            (name,))
                    else:
                        # 其他 HTTP 错误（5xx 等）或连接失败 → 熔断
                        if not is_disabled:
                            with self.lock:
                                self.disabled_providers.add(name)
                            if self.undo_register:
                                self.undo_register(f"主动探测熔断 {name} ({err_str[:50]})",
                                                   lambda n=name: self.disabled_providers.discard(n))
                            print(f"探测熔断: {name} 不可达 (HTTP {e.code}) -> 已禁用 ({err_str[:60]})")
                        conn.execute(
                            "INSERT INTO health_log (model, pool, provider, ok, latency_ms, error) VALUES (?,?,?,?,?,?)",
                            ("probe", "unknown", name, 0, 0, err_str[:200]))
                        conn.execute(
                            "UPDATE registry SET status='down', updated_at=datetime('now') WHERE provider=? AND status != 'down'",
                            (name,))
                except Exception as e:
                    err_str = str(e)
                    # 连接失败/超时/其他异常 → 熔断
                    if not is_disabled:
                        with self.lock:
                            self.disabled_providers.add(name)
                        if self.undo_register:
                            self.undo_register(f"主动探测熔断 {name} ({err_str[:50]})",
                                               lambda n=name: self.disabled_providers.discard(n))
                        print(f"探测熔断: {name} 不可达 -> 已禁用 ({err_str[:60]})")
                    conn.execute(
                        "INSERT INTO health_log (model, pool, provider, ok, latency_ms, error) VALUES (?,?,?,?,?,?)",
                        ("probe", "unknown", name, 0, 0, err_str[:200]))
                    conn.execute(
                        "UPDATE registry SET status='down', updated_at=datetime('now') WHERE provider=? AND status != 'down'",
                        (name,))
        conn.commit()
        conn.close()
