# 模型池统一网关 v2

三资源池路由引擎，OpenAI 兼容接口，独立基础设施，所有智能体平等消费。

## 快速开始

```bash
# 依赖
pip install fastapi uvicorn httpx prometheus-client pyyaml

# 启动
python3 gateway.py

# 确认
curl http://127.0.0.1:8646/health
# → {"status":"ok","version":"0.2.0"}
```

## 三行架构

```
请求 → ①关键词匹配 → ②模型名精确匹配 → ③默认池
  → 选中池: pool_a/pool_b/pool_c
  → 池内权重轮询: scnet-tp(5) / xiaomi(2) / deepseek-direct(1)
  → 故障转移: 5xx/超时/连接失败 → 下一个池 (pool_a→pool_b→pool_c)
  → 全部失败 → 返回 503
  → 4xx/422 客户端错误 → 直接返回，不触发故障转移
  → 429 限流 → 直接返回，不触发故障转移
```

- **池内**：权重轮询（支持动态权重，按错误率自动缩放）
- **池间**：pool_a(日常) → pool_b(增强) → pool_c(兜底)
- **熔断**：后台线程每 30s 扫描 5 分钟窗口，错误率 >20% 自动禁用，<10% 恢复

## 启动选项

```bash
python3 gateway.py                    # 启动 HTTP 服务
python3 gateway.py probe              # 一次性健康探测
python3 gateway.py probe --watch      # 持续探测（每 60s）
python3 gateway.py models             # 列出模型目录
python3 gateway.py usage              # 查看用量统计
python3 gateway.py sync-runtime       # 热加载配置（SIGHUP）
python3 gateway.py undo               # 撤销上一条运行时操作
python3 gateway.py undo-list          # 查看运行时逆栈
python3 gateway.py fiber              # 查看 Agent 任务树
python3 gateway.py scan-agents        # 自动发现并接入智能体
python3 gateway.py scan-agents --dir /opt/agents  # 指定扫描目录
python3 gateway.py git-log            # 配置变更历史
python3 gateway.py git-diff           # 未提交的配置变更
```

## 聊天页面（免鉴权）

打开浏览器访问 `http://<网关地址>:8646/chat`

- 选择模型
- 输入你自己的 API Key
- 发送消息

网关用你的 Key 调用上游 provider，走三池路由 + 故障转移。Key 不在网关落地，仅透传。

## 关键端点

| 端点 | 说明 |
|------|------|
| `GET /health` | 健康检查（免鉴权） |
| `GET /chat` | 聊天页面（免鉴权） |
| `POST /v1/chat/completions` | OpenAI 兼容推理（支持 `api_key` 字段透传自定义 Key） |
| `POST /v1/plugins/{id}/call` | 统一插件调用：capabilities 校验 + 审批缓存 + Fiber 逆操作 |
| `GET /v1/models` | 模型目录 |
| `GET /metrics` | Prometheus 指标 |
| `GET /admin/pools` | 池/provider 状态 |
| `POST /admin/pools/{pool}/providers/{provider}/toggle` | 手动启停 provider |
| `POST /admin/mcp/toggle` | Agent 调用 toggle（走审批缓存 + fiber） |
| `GET /admin/mcp/approvals` | 查看审批缓存 |
| `GET /admin/mcp/status` | 熔断 + 权重 + 错误率总览 |
| `POST /admin/fiber/create` | 创建 Agent 任务树节点 |
| `POST /admin/fiber/{id}/fail` | 失败/级联回滚 |
| `POST /admin/fiber/{id}/commit` | 提交/合并 undo |
| `GET /admin/fiber/tree` | 查看 fiber 森林 |
| `POST /admin/fiber/check` | 创建检查任务 fiber（执行者-检查者模式） |
| `GET /admin/undo` | 撤销上一条运行时操作 |
| `GET /admin/undo-list` | 查看运行时逆栈 |
| `GET /admin/agents/declaration` | 返回智能体声明配置 + 可用插件列表（支持 `?agent_id=xxx` 过滤） |
| `GET /admin/agents/status` | 返回所有声明 Agent 的存活状态 |
| `GET /admin/logs` | 聚合所有 Agent 日志，按时间合并，支持 level/agent 过滤 |

## 配置

`gateway.yaml` — 三层结构：池 → provider → 模型。

```yaml
pools:
  pool_a:
    providers:
      - name: scnet-tp
        weight: 5
        models: [gpt-4o, claude-3.5-sonnet]
        api: https://api.scnet.cn/v1
        api_key: sk-xxx
```

## 智能体声明式接入

在 `gateway.yaml` 中声明已有智能体的位置，网关自动发现并接入。**不迁移、不复制任何用户文件。**

```yaml
agents:
  - id: openhands
    display_name: "OpenHands"
    type: openhands
    workspace: /home/user/openhands/workspace
    capabilities:
      - read
      - write
      - execute

  - id: hermes-checker
    display_name: "Hermes 检查者"
    type: generic
    command: python3 /opt/hermes/checker.py --daemon
    pid_file: /tmp/hermes-checker.pid
    capabilities:
      - read
      - validate
      - inspect
```

`GET /admin/agents/declaration` 返回完整声明，智能体启动时自动调用此端点完成接入。

支持 `?agent_id=xxx` 参数，返回该 Agent **可调用的插件列表**（按 capabilities 过滤）：

```bash
# 获取 openhands 可用的插件（只返回 read 插件，因为 send_email 需要 destructive）
curl -H "Authorization: Bearer $GW_KEY" 'http://127.0.0.1:8646/admin/agents/declaration?agent_id=openhands'

# 获取所有插件
curl -H "Authorization: Bearer $GW_KEY" http://127.0.0.1:8646/admin/agents/declaration
```

智能体启动时只需调用 `GET /admin/agents/declaration?agent_id=<自身id>`，即可同时获得配置和可用插件，无需额外发现步骤。

`GET /admin/agents/status` 根据 type 探测存活状态：
- `openhands`: 检查 workspace 下锁文件/PID
- `astrbot`: GET base_url/health，超时 2s
- `generic`: 检查 pid_file 是否存在且进程存活

## 执行者-检查者模式

执行者负责干活，检查者负责验收。两者角色分离：

| 角色 | 职责 | 权限 |
|------|------|------|
| 执行者（Executor） | 根据用户需求执行任务（写代码、发邮件等） | read + write + execute |
| 检查者（Checker） | 验证执行者的输出是否满足用户需求 | read + validate + inspect |

### 工作流程

1. 执行者创建 fiber 开始任务
2. 执行者完成任务后，L3 大脑调用 `POST /admin/fiber/check` 创建检查任务
3. 检查者通过只读工具验收结果
4. **通过** → commit 检查任务，undo_log 合并到执行者 fiber
5. **不通过** → fail 检查任务，触发执行者 fiber 级联回滚（自动撤销所有操作）

### 三层验证模式

```yaml
validation:
  mode: adaptive              # off | conservative | adaptive
  confidence_threshold: 0.7   # 仅 adaptive 模式
  checker_agent: hermes-checker
  max_retries: 2
```

| 模式 | 行为 |
|------|------|
| `off` | 不检查任何任务（最快，调试用） |
| `conservative` | 检查所有 write/destructive 操作（最安全） |
| `adaptive` | 只检查置信度低于阈值的任务（默认） |

### 逆栈覆盖

检查不通过时，自动触发执行者的逆栈回滚。例如检查者发现"执行者发了邮件但收件人错了"，只需 fail 检查任务，触发执行者的逆栈，自动撤回邮件。检查者本身不负责修复，只负责验收。

## 自动发现智能体

无需手动编辑 YAML，只需把智能体放在约定目录，跑一次命令即可接入：

```bash
# 默认扫描 ~/agents/
python3 gateway.py scan-agents

# 指定目录
python3 gateway.py scan-agents --dir /opt/agents
```

网关自动识别：
- **OpenHands** — 识别 `config.toml` 含 `[core]` 或 `.lock` 文件
- **AstrBot** — 识别 `main.py` 含 `AstrBot` 或 `config.yaml` 含 `adapters`
- **通用脚本** — 识别 `.pid` 文件

### 插件自动声明

每个 Agent 目录下放一个 `plugins.yaml`，声明该 Agent 提供的插件：

```yaml
# /opt/agents/hermes-smtp/plugins.yaml
- id: send_email
  display_name: "发送邮件"
  description: "通过 SMTP 发送邮件"
  execution: cli
  command: python3 /opt/hermes/smtp.py --to "{to}" --subject "{subject}" --body "{body}"
  input_schema: { to: string, subject: string, body: string }
  output_schema: { msg_id: string, status: string }
  inverse: recall_email
  capabilities: [write, destructive]
  timeout: 30
  concurrent: false
```

`scan-agents` 扫描时自动发现 `plugins.yaml`，自动填充 `provider` 字段为 Agent 的 `id`，去重写入 `gateway.yaml` 的 `plugins` 段并热加载。**无需手动编辑任何文件。**

## 日志聚合

`GET /admin/logs` 统一读取所有声明 Agent 的日志，按时间戳合并排序，无需切换终端翻日志。

```bash
# 查看所有 Agent 日志
curl http://127.0.0.1:8646/admin/logs

# 只看 ERROR
curl 'http://127.0.0.1:8646/admin/logs?level=ERROR'

# 只看特定 Agent
curl 'http://127.0.0.1:8646/admin/logs?agent=openhands'

# 只看某个时间点之后的日志
curl 'http://127.0.0.1:8646/admin/logs?since=2026-08-25T12:00:00&level=ERROR,WARN'

# 每个 Agent 只看最后 50 行
curl 'http://127.0.0.1:8646/admin/logs?lines=50'
```

### 读取方式

| Agent 类型 | 读取方式 |
|-----------|---------|
| `openhands` | 扫描 workspace/logs/ 下所有 `.log`/`.txt`/`.out`/`.err` 文件 |
| `astrbot` | `GET base_url/logs` 远程拉取（需 Agent 暴露端点） |
| `generic` | 扫描 workspace/logs/ 或 pid_file 所在目录的 logs/ |

### 与检查者协同

检查者验收失败时，可以通过 `POST /admin/fiber/{id}/fail` 的 `evidence` 字段附上日志片段：

```json
{
  "evidence": "检查者日志：\n12:00:10 [ERROR] KeyError: model \"gpt-5\" not found\n12:00:11 [INFO] 回滚操作已执行"
}
```

网关自动收集检查者自身的日志并附在失败报告中，让问题定位一目了然。

## 统一插件调用

所有智能体通过 `POST /v1/plugins/{id}/call` 统一调用插件，网关根据 `execution` 模式适配。

### 插件声明

在 `gateway.yaml` 的 `plugins` 段声明：

```yaml
plugins:
  - id: web_search
    display_name: "联网搜索"
    provider: tavily-connector
    execution: http                              # http | cli
    endpoint: https://api.tavily.com/search
    input_schema: { query: string, max_results: integer }
    capabilities: [read]                         # 调用者需有此能力
    timeout: 30
    concurrent: true

  - id: send_email
    display_name: "发送邮件"
    provider: hermes-smtp
    execution: cli
    command: python3 /opt/hermes/smtp.py --to "{to}" --subject "{subject}" --body "{body}"
    input_schema: { to: string, subject: string, body: string }
    inverse: recall_email                        # 逆操作（Fiber 回滚时自动调用）
    capabilities: [write, destructive]
    timeout: 30
    concurrent: false
```

### 调用方式

```bash
curl -X POST http://127.0.0.1:8646/v1/plugins/web_search/call \
  -H "Content-Type: application/json" \
  -d '{"agent_id":"openhands","params":{"query":"today news","max_results":5}}'
```

### 安全保障

| 机制 | 说明 |
|------|------|
| Capabilities 校验 | 调用者必须拥有插件所需的能力（如 write 插件拒绝 read-only agent） |
| 审批缓存 | 键 `(agent_id, plugin_id, params)`，5 分钟内重复调用免审 |
| Fiber 逆操作 | 成功调用注册逆操作到 Fiber 树，`POST /admin/fiber/{id}/fail` 自动回滚 |
| 无 fallback | Provider 不可达时直接返回错误，不触发故障转移 |

## 部署

```bash
# systemd 服务（已配置）
systemctl restart gateway
systemctl reload gateway   # 热加载配置
```

SIGHUP 热加载：自动 git commit 快照 + 清空运行时状态 + 重启生效。

## 可逆性

三层逆栈，从配置到运行时全覆盖：

| 层 | 机制 | 命令 |
|----|------|------|
| 配置层 | git commit 快照 | `git revert HEAD; python3 gateway.py sync-runtime` |
| 运行时层 | `_undo_stack`（人类操作） | `python3 gateway.py undo` |
| 任务层 | `Fiber` 树（Agent 操作） | `POST /admin/fiber/{id}/fail` 级联回滚 |

## 设计文档

`/opt/workspace/ops/轻量化三资源池管理智能体-设计方案.md` — 含架构决策、替代方案评估、演进历史。

## 版本记录

| 版本 | 功能 | 日期 |
|------|------|------|
| v2.0 | 三资源池路由，自动熔断+动态权重 | 2026-08 |
| v2.1 | MCP 审批回调，AI 可调 toggle | 2026-08 |
| v2.2 | 运行时逆栈 undo_stack，支持人类撤销 | 2026-08 |
| v2.3 | 自治三件套：自动熔断+动态权重+MCP 审批 | 2026-08 |
| v2.4 | Fiber 树形上下文：子任务级联回滚，undo 合并 | 2026-08 |
| v2.5 | 聊天页面 `/chat`，自定义 Key 透传 | 2026-08 |
| v2.5 | 智能体声明式接入，`/admin/agents/declaration` + `/admin/agents/status` | 2026-08-25 |
| v2.5 | `scan-agents` 自动发现：扫描目录→交互确认→写入 YAML→热加载 | 2026-08-25 |
| v2.5 | 日志聚合 `/admin/logs`：统一读取所有 Agent 日志，按时间合并，level/agent 过滤 | 2026-08-25 |
| v2.5 | 统一插件调用 `POST /v1/plugins/{id}/call`：capabilities 校验 + 审批缓存 + Fiber 逆操作 | 2026-08-25 |
| v2.6 | 执行者-检查者模式：`/admin/fiber/check`，级联回滚，capabilities 权限校验，检查者证据 | 2026-08-25 |