# AGENTS.md — 在本仓库工作的智能体请先读这段

1. **遵守 [`docs/模块约定.md`](docs/模块约定.md)**：落位规则、依赖方向、改动流程。
2. 新逻辑先落到模块 + 测试，**再**改 `api/app.py` 接线；不要往 `app.py` 里加新的业务判断。
3. 改完必须依次做到：`python3 -m pytest tests/ -q` 全绿 → 导入自检 → `systemctl restart gateway.service`
   → 实测一个正常路径 + 一个失败路径 → 在《操作手册》留痕（改了什么 / 为什么 / 备份 / 回滚）。
4. 禁忌：模块里读 `os.environ` / 直连 DB / 直发网络；提交 `*.bak.*`；把一次性脚本留在仓库。
5. `gateway.yaml`（含 key 引用与网关 key）**不入库**；改配置要备份 + SIGHUP 或重启 + 留痕。
6. 改代码 **不能** 用 SIGHUP 热加载生效 —— SIGHUP 只重载 YAML。
