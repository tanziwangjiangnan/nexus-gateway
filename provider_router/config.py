"""配置加载 — 加载 gateway.yaml 并解析 ${VAR} 环境变量引用。"""
import os
import yaml


def load_config(path: str, providers_override: dict = None) -> dict:
    """加载 YAML 配置，解析 ${VAR} 环境变量引用。

    Args:
        path: YAML 文件路径
        providers_override: 可选的 provider 配置覆盖（用于测试或运行时注入）

    Returns:
        解析后的配置字典
    """
    with open(path) as f:
        cfg = yaml.safe_load(f)
    # 解析 ${VAR} 环境变量引用
    for pname, pcfg in cfg.get("providers", {}).items():
        key = pcfg.get("api_key", "")
        if isinstance(key, str) and key.startswith("${") and key.endswith("}"):
            env_name = key[2:-1]
            val = os.environ.get(env_name, "")
            if val:
                pcfg["api_key"] = val
    # 如果有覆盖，合并到 providers 段
    if providers_override:
        cfg.setdefault("providers", {}).update(providers_override)
    return cfg