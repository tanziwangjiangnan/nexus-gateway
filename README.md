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

## 关键端点

| 端点 | 说明 |
|------|------|
| `GET /health` | 健康检查（免鉴权） |
| `POST /v1/chat/completions` | OpenAI 兼容推理 |
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