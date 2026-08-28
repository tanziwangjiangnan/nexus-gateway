# 网关项目记忆

## 架构
- 三资源池路由（pool_a/b/c），池内权重轮询，池间故障转移
- 三层逆栈：Git 配置层 → 运行时 undo_stack → Fiber 任务树
- 代码 ~870 行（v2.4），~1200 行（v2.9），内存 ~48MB
- `/v1/models` 实时状态：`status` 来自 `_disabled_providers` + 错误率，`capabilities` 来自 YAML provider 声明

## 用户关键决策
- 方案 C（自己写网关），所有智能体平等消费独立模型池网关
- 不强制 Docker 或 SaaS，源码包 + gateway.yaml 声明式交付
- 不迁移用户文件，通过声明发现并适配现有工作区

## 版本演进
- v2.0: 基础三池路由 + 熔断/动态权重
- v2.4: Fiber 树形上下文（子任务级联回滚，undo 合并）
- v2.5: 聊天页面 /chat + 智能体声明式接入 + scan-agents 自动发现 + 日志聚合 /admin/logs
- v2.6: 执行者-检查者模式（/admin/fiber/check，级联回滚，capabilities 权限校验，检查者证据）
- v2.7: 重复调用拦截（call_history 自底向上遍历）+ 动态校验（output_schema Schema 校验 + 熔断联动）
- v2.8: 跨分支全局去重（`_global_call_history` + 三层清理）+ 插件排队（serial/throttle 并发控制）
- v2.9: 组件打包——`provider_router/` 和 `fiber_tree/` 独立包，`gateway.py` 导入适配，示例编写并验证
- v2.10: 接口设计缺陷修复——FiberTree 无状态重构（移除 `_fibers` 类变量和单例）；Storage 抽象完善（`add_global_call_history` / `get_global_call_history` 方法）；Provider Router 可扩展钩子（`select_provider(weight_fn=...)`、`CircuitBreakerMonitor(quality_factor_fn=..., user_factor_fn=..., error_threshold=..., recover_threshold=...)`）
- v2.11: 反向依赖检查 `check-deps` 命令（Key/URL 被哪些组件引用 + `--auto-sync` 远程同步）
- v3.0: 拆分 `hermes_cfg/` 配置管理包（原 config_manager 因 PyPI 包名冲突更名）；ConfigLoader 类 + get_db + init_registry
- v3.1: 拆分 `hermes_fiber/` 运行时包（FiberRuntime 类：fiber 生命周期 + undo 栈 + 全局去重表）
- v3.2: 拆分 `hermes_api/` HTTP API 层（build_app 函数 + 依赖注入，gateway.py 仅剩 1180 行薄封装）

## 组件导出接口

### `hermes_cfg/`（v3.0，原 config_manager 更名）
- `ConfigLoader`（load/reload + on_reload 回调）
- `get_db()`（SQLite WAL，建表 + 列迁移）
- `init_registry()`
- **命名规则**: 所有 hermes 组件统一用 `hermes_*` 前缀，避免与 PyPI 包名冲突

### `provider_router/`
- `Router`（静态方法：`find_model`, `select_pool_by_keywords`, `select_provider(weight_fn=...)`, `check_rate_limit`）
- `RouterState` dataclass
- `CircuitBreakerMonitor`（熔断阈值参数化，质量/用户因子公式可注入）
- `load_config()`（依赖 `pyyaml`）

### `fiber_tree/`
- `FiberTree`（实例变量，无单例，无类共享状态）
- `Storage`（抽象基类：`create_fiber`, `get_fiber`, `update_fiber`, `delete_fiber`, `add_global_call_history`, `get_global_call_history` 等）

### `hermes_fiber/`（v3.1）
- `FiberRuntime` 类：fiber 生命周期（create/register/fail/commit），undo 栈（register/pop/clear），全局去重表（lookup/add/remove）
- `Fiber` dataclass（id, parent_id, agent_id, description, status, undo_log, children, capabilities, call_history, created_at）
- 与 `fiber_tree/` 的区别：`fiber_tree/` = 持久化存储抽象（数据库层），`hermes_fiber/` = 运行时内存状态
- `MemoryStorage`, `SQLiteStorage`

## 关键命令
- `python3 gateway.py scan-agents` — 自动发现并接入智能体
- `python3 gateway.py sync-runtime` — 热加载配置（SIGHUP）
- `python3 gateway.py fiber` — 查看 Agent 任务树
- `python3 gateway.py quality` — 查看 Provider 质量排名（v2.10）
- `python3 gateway.py feedback-stats` — 查看用户反馈统计（v2.10）
- `python3 gateway.py check-deps` — 反向依赖扫描：检查 Key/URL 被哪些组件引用（v2.11）
- `python3 gateway.py check-deps --auto-sync` — 扫描 + 自动同步到远程（v2.11）
- `systemctl reload gateway` — 配置热加载
- `journalctl -u gateway --no-pager -n 50` — 查看网关日志

## 触发词
- **"三层架构" / "three-layer" / "走三层" / "三层干活"** → 激活三层架构操作协议（配置层 Git commit → 运行时层 undo_register → 任务层 fiber 树），技能文件在 `~/.openhands/skills/three-layer-ops/SKILL.md`

## 外部域名
- `hermes.jiangnande.cloud:8648` → 网关（三池路由），阿里云安全组已开
- API key: `gw-hermes-5612a2cbbf5bc057af2d4268`
## 插件系统（v2.5+）

- **端点**: `POST /v1/plugins/{id}/call` — 公共 API，免鉴权
- **审批缓存键**: `(agent_id, plugin_id, sorted_params)`，TTL 5 分钟
- **执行模式**: `http`（转发到 endpoint）或 `cli`（subprocess，超时 30s）
- **Capabilities 校验**: 调用者必须拥有插件声明的所有能力，否则 403
- **Fiber 集成**: 成功调用注册逆操作到 Fiber 树，`fiber_fail` 自动回滚
- **跨分支全局去重（v2.8）**: `_global_call_history` 全局 dict，key=`{plugin_id}:{params_hash}`，24h TTL，三层清理（主动/fiber commit/fail → 定时/1h → 惰性/查询时）
- **插件排队（v2.8）**: 字段 `concurrency`（parallel/serial/throttle）+ `resource_lock_key`（串行锁分组）+ `throttle_limit`（每秒上限）
- **动态校验（v2.7）**: 执行后创建 `[校验]` 子 fiber，`output_schema` 字段名/类型校验，失败级联回滚父 fiber
- **熔断联动（v2.7）**: HTTP ConnectError 自动标记 provider 不可达
- **无 fallback**: 插件不可达时直接返回错误
- **占位符**: `_format_string()` 支持 `{key}` 替换，用于 CLI 命令模板

## scan-agents 插件自动声明
- Agent 目录下放 `plugins.yaml`，`scan-agents` 自动发现
- 自动填充 `provider` 为 Agent 的 `id`
- 去重写入 `gateway.yaml` 的 `plugins` 段 + 热加载
## 模型别名
- 2026-08-26: DeepSeek 上游废弃 `deepseek-chat-direct`，只接受 `deepseek-v4-pro`、`deepseek-v4-flash`、`deepseek-v4-flash-vision-exp`
- 网关已添加别名映射：`deepseek-chat-direct` → `deepseek-v4-flash`（在 deepseek-direct provider 的 alias 字段声明）
- 别名在 `/v1/models` 列表里新旧两个名字都可见

## 直连端点
- `/v1/direct/chat/completions` — 不走池路由/关键词/故障转移，直接透传到指定 provider
- 通过 `_provider` 字段指定 provider，或自动从 model 名查找
- nginx 8649 端口对外暴露，重写 `/v1/chat/completions` → `/v1/direct/chat/completions`

## 2026-08-26 修复记录
- **路由优先级修复**: 模型路由(`find_model_config`)优先于关键词路由(`select_pool_by_keywords`)。当用户指定 model 名且匹配到池时，直接走模型路由，不再被关键词劫持
- **Provider 模型名大小写透传**: 每个 provider 用自己的模型名发请求（`scnet-tp` 发 `DeepSeek-V4-Flash`，`deepseek-direct` 发 `deepseek-v4-flash`），避免 fallback 时大小写不匹配
- **直连端点 8649**: 新端口绕过所有池路由/关键词/故障转移，nginx 重写 `/v1/chat/completions` → `/v1/direct/chat/completions`，同时 `/models` → `/v1/models` 补齐

## QQ 机器人架构
- **三台服务器**：
  - 老机 `106.14.40.189`（阿里云 2C2G）：NapCat + AstrBot + kb_knowledge
  - 新机 `106.14.20.149`（阿里云 2C2G）：kb_agent（Open WebUI 已退出）
  - 当前机 `117.72.220.114`（京东云 4C8G）：Hermes 网关 + OpenHands
- **链路**：`QQ → NapCat → AstrBot → Hermes 网关(:8643) → LLM`
- **AstrBot 配置路径**：`/opt/qq-bot/bot/astrbot/data/cmd_config.json`（老机）
- **AstrBot 默认 provider**：`hermes/DeepSeek-V4-Flash`，通过 `hermes.jiangnande.cloud:8643/v1` 访问
- **SSL 注意事项**：AstrBot 容器内连 Hermes 必须用域名 `hermes.jiangnande.cloud`，不能用 IP，否则 SSL 证书验证失败（证书绑定域名）
- **SSH**：`ssh -p 2222 root@106.14.40.189`（老机），`ssh -p 2222 root@106.14.20.149`（新机）
