# Changelog

## v3.9 (2026-08-29)

### 重构
- 新增 `ops-gateway-core` 合并包：将 `hermes_cfg/fiber/api/ops` 四个子包合并为单一 `ops_gateway_core` 包（4 个子模块：`cfg/fiber/api/ops`）
- 新增 `nexus-gateway` 聚合包：依赖 `ops-gateway-core` + `ops-provider-router` + `ops-fiber-tree`，一键安装全部组件
- 原 4 包保留为 shim（从 `ops-gateway-core` 重新导出），旧导入路径 `from hermes_cfg import ...` 仍可用

### 新增
- `deploy/` 目录：Docker Compose 一键部署 + K8s/ACK 全套编排（namespace/configmap/deployment/service/ingress/kustomization）
- `deploy/.env.example` 环境变量模板，`deploy/config/gateway.yaml.example` 配置模板（无硬编码密钥）

### 安全
- 所有 `api_key` 统一使用 `${VAR}` 环境变量引用，密钥通过 Secret（K8s）或 .env（Compose）注入
- 无硬编码密钥/路径，git 历史无敏感信息

### 部署
- 三种部署方式共存：Docker Compose / K8s(ACK) / 裸机进程（原方式不变）
- Dockerfile 更新（tag v3.9，安装新包）
- 零影响现有组件部署架构

## v0.3.1 (2026-08-29)

### 修复
- 中转链路：gateway.yaml 改用 ${VAR} 环境变量引用，scnet 路径修正为 /api/llm/v1，app.py 补上 resolve_env_key()
- 实验网关 gateway_key 改为生产 key（与 hermes.jiangnande.cloud 一致）

### 新增
- 配置变更下游感知：check_deps_on_diff() 注入 SIGHUP 热加载，变更前自动扫描下游引用
- 扫描范围扩展：~/.hermes/config.yaml 和 ~/.hermes/.env


## v0.3.0 (2026-08-13)

### 重构
- 从单体 `gateway.py`（2633 行）拆分为 6 个独立包
- 每个包有独立 `pyproject.toml`、版本号、PyPI 包名（`ops-` 前缀）

### 新增
- `provider_router/` (v2.9.0) — 三池路由引擎，支持关键词/模型名/默认池三种路由策略，权重轮询 + 动态缩放
- `fiber_tree/` (v2.9.0) — 持久化存储层，MemoryStorage / SQLiteStorage 双实现
- `hermes_fiber/` (v3.1.0) — Fiber 运行时管理：任务生命周期、undo 撤销栈、全局去重表
- `hermes_cfg/` (v3.0.0) — 配置管理：YAML 加载、热加载、环境变量注入、SQLite registry
- `hermes_api/` (v3.2.0) — HTTP API 层：FastAPI 构建，依赖注入架构，27+ 路由
- `hermes_ops/` (v3.3.0) — CLI 操作层：模型查询、用量统计、健康探测、反向依赖检查、智能体发现
- `tests/` — 84 个单元测试，覆盖全部 6 个包

### 安全
- 清除 `gateway.yaml` 和 `gateway.db` 的 git 历史（filter-repo）
- 移除所有硬编码本地路径（`/root/experiments/`、`/home/user/`）
- `QQ_PUSH` 改为环境变量 `HERMES_QQ_PUSH_SCRIPT`
- 添加 `gateway.yaml.example` 配置模板（占位符格式）
- 添加 `.gitignore`（Python + venv + IDE + 本地配置 + OpenHands agent memory）

### 文档
- 新增 README.md 架构说明 + 包依赖表 + 快速开始
- 新增 LICENSE（MIT）、CONTRIBUTING.md、SECURITY.md、CHANGELOG.md