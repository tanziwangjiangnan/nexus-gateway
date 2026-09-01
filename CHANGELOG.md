# Changelog
## v3.11 (2026-09-01)

### 新增：可插拔模型路由策略
- 新增 provider_router/router.py 中 call_model_router() 和 select_provider_by_strategy() 两个函数
- 新增 provider_router/cache.py — RouteCache 类，线程安全 TTL 缓存，默认 300s
- 新增 provider_router/config.py 中 routing_strategy 配置段解析（含默认值填充）
- 网关集成：gateway.py wrapper + app.py chat_completions 三模式入口
  - formula — 原有确定性权重轮询（默认，零行为变化）
  - model — 调用外部路由模型服务做决策，支持 fallback=error|formula
  - hybrid — 优先模型路由，失败时降级 formula
- 路由缓存：相同 query 在 TTL 内复用决策结果，零额外 Token 开销

### 兼容
- 默认 mode: formula，未配置 routing_strategy 段时行为完全不变
- test_router_server.py 提供参考实现：用 scnet-tp 做路由决策模型
- 路由决策模型选型说明：deepseek-v4-flash 是推理模型，输出全耗在 reasoning_content，不适合做路由决策；改用 scnet-tp（轻量非推理，约10 tokens/次）

### 灵感来源
- RL 强化学习路由策略学习笔记（4.1.2~4.3.3）：从固定权重轮询到模型自主决策的演进路径
- 核心思路：路由本身是一个轻量决策问题，不需要高成本推理模型，一个非推理模型足以从候选列表中选择最优 provider
- 缓存设计（TTL 路由缓存）来自"路由开销不应超过请求本身"的工程原则

### 切换方式
在 gateway.yaml 中设置：
routing_strategy:
  mode: hybrid  # formula | model | hybrid
  model_router:
    endpoint: http://127.0.0.1:9090/v1/route
    timeout_ms: 500
    fallback: formula
    cache_enabled: true
    cache_ttl_seconds: 300
- mode: formula → 默认，纯权重轮询，无需任何额外配置
- mode: model → 纯模型路由，路由服务不可用时返回 503（fallback=error）或降级 formula
- mode: hybrid → 优先模型路由，失败自动降级 formula
- 切换后 SIGHUP 热加载生效：kill -HUP $(cat gateway.pid)
## v3.10 (2026-08-29)

### 新增：离线基准评分（benchmark）
- 新增 `benchmark` CLI 命令：`python3 gateway.py benchmark --all` 在本地跑固定测试题集，生成 `quality_benchmark.yaml`
- 固定测试题集 10 题，覆盖 5 个维度（code / math / translation / general_chat / creative_writing）
- 评分流程：每个 provider 用其模型回答 10 题 → 用评分模型逐一打分（0-100）→ 按 provider 聚合为 `quality_factors` 写入 YAML
- 生产环境启动时自动加载 `quality_benchmark.yaml`，填充 `quality_factors` 后**跳过在线监督者评分，零 Token 开销**
- 用户可随时重新评分：`python3 gateway.py benchmark --all --model=<评分模型>` 或 `--provider=<某provider>`
- 演进路径：阶段 1 离线基准打包 → 阶段 2 用户手动更新 → 阶段 3 未来可恢复在线模式
- 新增 `ops_gateway_core/ops/benchmark.py`（cmd_benchmark + load_quality_benchmark）

### 兼容
- 未生成 `quality_benchmark.yaml` 时行为完全不变（在线监督者照常工作）
- 已加载基准评分时自动禁用在线评分（`_benchmark_loaded` 标志）
- 路由读取 `quality_factors` 的方式不变，零影响现有组件部署

