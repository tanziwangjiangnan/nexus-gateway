"""注册表初始化 — 将 YAML 配置中的模型/池/Provider 写入 registry 表。"""
from .db import get_db


def init_registry(cfg: dict, db_path: str = None):
    """将配置中的模型同步到 registry 表。

    注册表以 model 为主键，而同一模型可能来自多个 provider（例如 deepseek-v4-pro
    同时挂在 deepseek-direct 与 qfg-new 下）。因此：pool 取配置中首个出现的池，
    provider 取首个来源，providers 记录**全部来源**（逗号分隔），
    供 CLI 与排障判断「这个模型到底是谁提供的」。
    """
    conn = get_db(db_path)
    agg = {}
    for pool_name, pool_cfg in cfg.get("pools", {}).items():
        tier = "A" if pool_name == "pool_a" else "B" if pool_name == "pool_b" else "C"
        for pv in pool_cfg.get("providers", []):
            provider_name = pv["name"]
            for model in pv.get("models", []):
                e = agg.setdefault(model, {"pool": pool_name, "tier": tier,
                                           "notes": pool_cfg.get("description", ""),
                                           "providers": []})
                if provider_name not in e["providers"]:
                    e["providers"].append(provider_name)
    for model, e in agg.items():
        provs = sorted(e["providers"])
        conn.execute("""INSERT INTO registry
            (model, pool, provider, providers, tier, status, notes)
            VALUES (?, ?, ?, ?, ?, 'unknown', ?)
            ON CONFLICT(model) DO UPDATE SET
                pool=excluded.pool,
                provider=excluded.provider,
                providers=excluded.providers,
                tier=excluded.tier,
                notes=excluded.notes""",
            (model, e["pool"], provs[0], ",".join(provs), e["tier"], e["notes"]))
    if agg:
        q = "DELETE FROM registry WHERE model NOT IN (%s)" % ",".join("?" * len(agg))
        conn.execute(q, tuple(sorted(agg)))
    conn.commit()
    conn.close()
