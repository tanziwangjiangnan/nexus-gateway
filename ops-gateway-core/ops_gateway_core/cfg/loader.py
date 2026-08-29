"""ConfigLoader — YAML 配置加载与热加载。

封装 load_config + reload_config 的逻辑：
- 加载 gateway.yaml（含 ${VAR} 环境变量解析）
- reload() 原地替换目标字典，保持闭包引用同步
- 通过 on_reload 回调通知运行时状态同步（清空禁用集合、撤销栈等）
"""
from __future__ import annotations

import os
from typing import Callable

from provider_router.config import load_config as pr_load_config

# 默认配置路径：项目根目录下的 gateway.yaml
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "gateway.yaml")


class ConfigLoader:
    def __init__(self, path: str = None, on_reload: Callable[[], None] = None):
        self.path = path or DEFAULT_CONFIG_PATH
        self._config = {}
        self._on_reload = on_reload

    def load(self) -> dict:
        """首次加载配置。"""
        self._config = pr_load_config(self.path)
        return self._config

    def reload(self) -> dict:
        """热加载：原地替换目标字典，保持引用同步，触发回调。"""
        new_cfg = pr_load_config(self.path)
        if self._config:
            self._config.clear()
            self._config.update(new_cfg)
        else:
            self._config = new_cfg
        if self._on_reload:
            self._on_reload()
        return self._config

    @property
    def config(self) -> dict:
        return self._config