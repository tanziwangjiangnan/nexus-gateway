# Changelog

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

