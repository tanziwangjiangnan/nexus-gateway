"""注册表初始化 — 将 YAML 配置中的模型/池/Provider 写入 registry 表。"""
from .db import get_db


def init_registry(cfg: dict, db_path: str = None):
    """将配置中的模型注册到 registry 表。"""
    conn = get_db(db_path)
    for pool_name, pool_cfg in cfg.get("pools", {}).items():
        for pv in pool_cfg.get("providers", []):
            provider_name = pv["name"]
            for model in pv.get("models", []):
                conn.execute("""INSERT OR IGNORE INTO registry
                    (model, pool, provider, tier, status, notes)
                    VALUES (?, ?, ?, ?, 'unknown', ?)""",
                    (model, pool_name, provider_name,
                     "A" if pool_name == "pool_a" else "B" if pool_name == "pool_b" else "C",
                     pool_cfg.get("description", "")))
    conn.commit()
    conn.close()