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
请求(model=代码生成) → 关键词匹配 → pool_a
  → pool_a: scnet-tp(权重5) / xiaomi(权重2) / deepseek-direct(权重1)
  → 故障转移: 5xx/超时/连接失败 → pool_b → pool_c
  → 全部失败 → 返回 503
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
| `GET /v1/models` | 模型目录 |
| `GET /metrics` | Prometheus 指标 |
| `GET /admin/pools` | 池/provider 状态 |
| `POST /admin/pools/{pool}/providers/{provider}/toggle` | 手动启停 provider |
| `POST /admin/mcp/toggle` | Agent 调用 toggle（走审批缓存 + fiber） |
| `GET /admin/mcp/status` | 熔断 + 权重 + 错误率总览 |
| `POST /admin/fiber/create` | 创建 Agent 任务树节点 |
| `POST /admin/fiber/{id}/fail` | 失败/级联回滚 |
| `POST /admin/fiber/{id}/commit` | 提交/合并 undo |
| `GET /admin/undo` | 撤销上一条运行时操作 |
| `GET /admin/undo-list` | 查看运行时逆栈 |
| `GET /admin/agents/declaration` | 返回智能体声明配置（agents 段） |
| `GET /admin/agents/status` | 返回所有声明 Agent 的存活状态 |
| `POST /admin/fiber/check` | 创建检查任务 fiber（执行者-检查者模式） |

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