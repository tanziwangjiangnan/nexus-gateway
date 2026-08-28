# 服务器操作手册

> 面向运维人员，涵盖日常启停、监控、故障排查、备份恢复。

---

## 目录

1. [部署概览](#1-部署概览)
2. [日常启停](#2-日常启停)
3. [日志查看](#3-日志查看)
4. [监控与告警](#4-监控与告警)
5. [配置管理](#5-配置管理)
6. [故障排查](#6-故障排查)
7. [备份与恢复](#7-备份与恢复)
8. [升级步骤](#8-升级步骤)

---

## 1. 部署概览

| 项目 | 说明 |
|------|------|
| 安装路径 | `/root/experiments/gateway/` |
| 主程序 | `gateway.py` |
| 配置文件 | `gateway.yaml` |
| 监听端口 | `8646`（HTTP） |
| 外部域名 | `hermes.jiangnande.cloud:8648`（nginx 反向代理转发） |
| 启动方式 | systemd 系统服务 `gateway.service` |
| Python 解释器 | `/root/.cache/uv/archive-v0/Pg3LQmYVDNb9R4EO/bin/python3` |
| 环境变量文件 | `/root/.hermes/.env` |
| 依赖管理 | `uv`（项目级依赖，非系统 pip） |
| API Key | `gw-hermes-5612a2cbbf5bc057af2d4268` |

### 目录结构

```
/root/experiments/gateway/
├── gateway.py              # 主程序
├── gateway.yaml            # 配置（池、provider、模型、插件、智能体声明）
├── provider_router/        # 路由组件（独立包）
│   ├── __init__.py
│   ├── router.py           # Router 类（find_model, select_provider, check_rate_limit）
│   ├── monitor.py          # CircuitBreakerMonitor（熔断/动态权重/质量因子）
│   └── config.py           # load_config()
├── fiber_tree/             # 任务树组件（独立包）
│   ├── __init__.py
│   ├── fiber.py            # FiberTree（无状态，实例变量）
│   └── storage.py          # Storage 抽象 + MemoryStorage + SQLiteStorage
├── examples/               # 示例
│   ├── example_router.py
│   └── example_fiber.py
├── .openhands/memory/      # 项目记忆（AI 辅助维护）
│   ├── MEMORY.md
│   └── YYYY-MM-DD.md      # 工作日志
└── OPS.md                  # 本文件
```

### 依赖

```bash
# 运行时依赖（已安装）
uv pip install fastapi uvicorn httpx prometheus-client pyyaml
```

---

## 2. 日常启停

### 启动

```bash
systemctl start gateway
```

### 停止

```bash
systemctl stop gateway
```

### 重启

```bash
systemctl restart gateway
```

### 查看状态

```bash
systemctl status gateway
```

### 开机自启

```bash
systemctl enable gateway
```

### 手动启动（调试用，前台运行）

```bash
cd /root/experiments/gateway
python3 gateway.py
```

---

## 3. 日志查看

### 服务日志

```bash
# 最近 50 行
journalctl -u gateway --no-pager -n 50

# 实时跟踪
journalctl -u gateway -f

# 查看某时间之后
journalctl -u gateway --since "2026-08-13 10:00:00"

# 仅查看 ERROR
journalctl -u gateway --no-pager -n 200 | grep -i error
```

### 应用日志

网关启动时日志输出到 stdout/stderr，由 systemd 收集到 journald。

### 各 Agent 日志聚合

```bash
# 通过 HTTP API 统一查看所有 Agent 日志
curl http://127.0.0.1:8646/admin/logs

# 只看 ERROR
curl 'http://127.0.0.1:8646/admin/logs?level=ERROR'

# 只看特定 Agent
curl 'http://127.0.0.1:8646/admin/logs?agent=openhands'
```

---

## 4. 监控与告警

### 健康检查

```bash
curl http://127.0.0.1:8646/health
# → {"status":"ok","version":"0.2.0"}
```

### 熔断与权重状态

```bash
# 池/provider 状态
curl http://127.0.0.1:8646/admin/pools

# 熔断 + 权重 + 错误率总览
curl http://127.0.0.1:8646/admin/mcp/status

# 模型目录（含实时状态、错误率）
curl http://127.0.0.1:8646/v1/models
```

### 质量排名

```bash
python3 gateway.py quality           # 查看 Provider 质量排名
python3 gateway.py feedback-stats    # 查看用户反馈统计
```

### Prometheus 指标

```bash
curl http://127.0.0.1:8646/metrics
```

可在 nginx 端配置反向代理暴露 `/metrics` 端点，接入 Prometheus + Grafana。

### 用量统计

```bash
python3 gateway.py usage
```

### 熔断阈值

- **熔断触发**：最近 5 分钟内错误率 > 20%（默认）
- **自动恢复**：错误率 < 10%（默认）
- **扫描间隔**：每 30 秒
- **最小样本**：至少 5 次请求才评估

可通过 `CircuitBreakerMonitor` 构造函数参数 `error_threshold`/`recover_threshold` 自定义（需改代码后重启）。

---

## 5. 配置管理

### 配置文件

`gateway.yaml` 是唯一配置文件，包含：

- `port` / `host` — 监听地址
- `gateway_key` — API 鉴权密钥
- `providers` — 上游 provider 配置（API 地址、Key、模型列表）
- `pools` — 资源池定义（池内 provider 权重）
- `plugins` — 插件声明（capabilities、执行模式、逆操作）
- `agents` — 智能体声明（类型、工作区、能力）
- `quality_feedback` — 质量反馈闭环配置
- `models` — 模型别名

### 热加载

修改配置后无需重启服务：

```bash
systemctl reload gateway
# 或
python3 gateway.py sync-runtime
```

热加载流程：
1. 自动 git commit 当前配置快照
2. 清空运行时状态（熔断、动态权重、限流计数器）
3. 重新加载配置生效

### 配置变更历史

```bash
python3 gateway.py git-log      # 查看配置变更历史
python3 gateway.py git-diff     # 查看未提交的配置变更
```

### 撤销配置变更

```bash
# 回滚到上一个 git commit
git revert HEAD
# 重新加载
python3 gateway.py sync-runtime
```

---

## 6. 故障排查

### 网关无法启动

```bash
# 查看错误详情
journalctl -u gateway --no-pager -n 100

# 常见原因：
# 1. 端口被占用 → lsof -i :8646
# 2. 配置文件语法错误 → python3 -c "import yaml; yaml.safe_load(open('gateway.yaml'))"
# 3. 依赖缺失 → uv pip install fastapi uvicorn httpx prometheus-client pyyaml
# 4. 环境变量未加载 → 检查 /root/.hermes/.env 是否存在
```

### 上游 Provider 不可达

```bash
# 查看熔断状态
curl http://127.0.0.1:8646/admin/pools

# 手动启用被熔断的 provider
curl -X POST http://127.0.0.1:8646/admin/pools/{pool}/providers/{provider}/toggle
```

### 路由不生效

```bash
# 排查步骤：
# 1. 确认模型名在配置中存在
# 2. 确认 provider 未被熔断禁用
# 3. 查看路由日志
journalctl -u gateway --no-pager -n 200 | grep -E "路由|熔断|provider"
```

### 端口被占用

```bash
lsof -i :8646
kill <PID>
```

### 手动探测

```bash
# 一次性健康探测
python3 gateway.py probe

# 持续探测（每 60s）
python3 gateway.py probe --watch
```

### 查看运行时逆栈

```bash
# 查看可撤销的操作列表
python3 gateway.py undo-list

# 撤销上一条操作
python3 gateway.py undo
```

### 查看 Fiber 任务树

```bash
python3 gateway.py fiber
```

---

## 7. 备份与恢复

### 需要备份的文件

| 文件 | 说明 | 备份频率 |
|------|------|---------|
| `gateway.yaml` | 配置文件 | 每次修改后 |
| `.openhands/memory/` | 项目记忆（AI 辅助，非关键） | 可选 |
| SQLite 数据库 | 任务树持久化存储（`fiber_tree/` 下） | 可选 |

### 备份命令

```bash
# 完整备份
tar -czf /tmp/gateway-backup-$(date +%Y%m%d).tar.gz \
  /root/experiments/gateway/gateway.yaml \
  /root/experiments/gateway/gateway.py \
  /root/experiments/gateway/provider_router/ \
  /root/experiments/gateway/fiber_tree/

# 配置热加载时自动 git commit，git log 即备份历史
cd /root/experiments/gateway
git log --oneline gateway.yaml
```

### 恢复步骤

```bash
# 从备份恢复
systemctl stop gateway
tar -xzf /tmp/gateway-backup-20260813.tar.gz -C /
systemctl start gateway
```

---

## 8. 升级步骤

```bash
# 1. 备份当前配置
cp gateway.yaml gateway.yaml.bak

# 2. 拉取新代码或修改代码

# 3. 语法检查
python3 -c "import py_compile; py_compile.compile('gateway.py', doraise=True)"
python3 -c "import py_compile; py_compile.compile('provider_router/router.py', doraise=True)"
python3 -c "import py_compile; py_compile.compile('provider_router/monitor.py', doraise=True)"
python3 -c "import py_compile; py_compile.compile('fiber_tree/fiber.py', doraise=True)"
python3 -c "import py_compile; py_compile.compile('fiber_tree/storage.py', doraise=True)"

# 4. 运行示例验证
python3 examples/example_router.py
python3 examples/example_fiber.py

# 5. 重启服务
systemctl restart gateway

# 6. 确认健康
curl http://127.0.0.1:8646/health
```

---

## 附录：集群架构

```
                          ┌──────────────┐
                          │   nginx 8648  │  ← hermes.jiangnande.cloud:8648
                          │  (反向代理)    │
                          └──────┬───────┘
                                 │
                          ┌──────▼───────┐
                          │  gateway 8646 │  ← 三池路由引擎
                          │  (本机)       │
                          └──────┬───────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
   ┌──────────┐           ┌──────────┐           ┌──────────┐
   │ pool_a   │           │ pool_b   │           │ pool_c   │
   │ scnet-tp │           │ xiaomi   │           │ qfg-codex│
   │ deepseek │           │          │           │          │
   └──────────┘           └──────────┘           └──────────┘
```

---

*最后更新: 2026-08-13*