# 网关项目记忆

## 架构
- 三资源池路由（pool_a/b/c），池内权重轮询，池间故障转移
- 三层逆栈：Git 配置层 → 运行时 undo_stack → Fiber 任务树
- 代码 ~870 行（v2.4），~1100 行（v2.6），内存 ~48MB

## 用户关键决策
- 方案 C（自己写网关），所有智能体平等消费独立模型池网关
- 不强制 Docker 或 SaaS，源码包 + gateway.yaml 声明式交付
- 不迁移用户文件，通过声明发现并适配现有工作区

## 版本演进
- v2.0: 基础三池路由 + 熔断/动态权重
- v2.4: Fiber 树形上下文（子任务级联回滚，undo 合并）
- v2.5: 聊天页面 /chat + 智能体声明式接入 + scan-agents 自动发现 + 日志聚合 /admin/logs
- v2.6: 执行者-检查者模式（/admin/fiber/check，级联回滚，capabilities 权限校验，检查者证据）

## 关键命令
- `python3 gateway.py scan-agents` — 自动发现并接入智能体
- `python3 gateway.py sync-runtime` — 热加载配置（SIGHUP）
- `python3 gateway.py fiber` — 查看 Agent 任务树
- `systemctl reload gateway` — 配置热加载

## 外部域名
- `hermes.jiangnande.cloud:8648` → 网关（三池路由），阿里云安全组已开
- API key: `gw-hermes-5612a2cbbf5bc057af2d4268`
## 插件系统（v2.5+）

- **端点**: `POST /v1/plugins/{id}/call` — 公共 API，免鉴权
- **审批缓存键**: `(agent_id, plugin_id, sorted_params)`，TTL 5 分钟
- **执行模式**: `http`（转发到 endpoint）或 `cli`（subprocess，超时 30s）
- **Capabilities 校验**: 调用者必须拥有插件声明的所有能力，否则 403
- **Fiber 集成**: 成功调用注册逆操作到 Fiber 树，`fiber_fail` 自动回滚
- **无 fallback**: 插件不可达时直接返回错误
- **占位符**: `_format_string()` 支持 `{key}` 替换，用于 CLI 命令模板

## scan-agents 插件自动声明
- Agent 目录下放 `plugins.yaml`，`scan-agents` 自动发现
- 自动填充 `provider` 为 Agent 的 `id`
- 去重写入 `gateway.yaml` 的 `plugins` 段 + 热加载
