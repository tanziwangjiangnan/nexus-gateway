# 贡献指南

感谢你愿意为 nexus-gateway 贡献代码！请遵循以下流程。

## 开发环境

```bash
# 安装所有包（editable 模式）
pip install -e ./provider_router -e ./fiber_tree -e ./hermes_fiber -e ./hermes_cfg -e ./hermes_ops -e ./hermes_api

# 运行测试
python3 -m pytest tests/ -v
```

## 提交 PR 流程

1. **Fork** 本仓库并创建你的特性分支
   ```bash
   git checkout -b feature/your-feature
   ```
2. **编写代码**，遵循项目风格（见下方「代码风格」）
3. **添加/更新测试**：新功能必须有对应测试（`tests/` 目录，按包分文件）
4. **运行全部测试**，确保 84+ 全部通过
5. **提交** 并推送
6. 在 GitHub 上创建 **Pull Request**，描述：
   - 解决了什么问题
   - 改动思路
   - 测试情况

## 代码风格

- Python 3.12+
- 遵循 [PEP 8](https://peps.python.org/pep-0008/)
- 模块 docstring 说明「解决什么问题」+ 版本号（如 `v3.3:`）
- 包边界清晰：改 `provider_router` 不要破坏 `hermes_api` 的依赖注入接口
- 不要硬编码密钥/本地绝对路径；配置走环境变量或 `gateway.yaml`

## 提交规范

- 提交信息用 `vX.Y: 一句话描述` 格式（与历史一致）
- 每个逻辑改动一个提交，保持历史清晰
- 重大重构需在 PR 描述中附「留痕」说明（影响面、依赖顺序）

## 测试要求

| 变更类型 | 要求 |
|---------|------|
| Bug 修复 | 添加回归测试 |
| 新功能 | 添加功能测试 |
| 重构 | 现有测试全通过即可 |
| 纯文档 | 无需测试 |

## 问题报告

- 使用 GitHub Issues，标题含「包名 + 简述」
- 描述：复现步骤 / 期望行为 / 实际行为 / 日志片段
- 涉及密钥信息请打码，或通过 SECURITY.md 流程私密报告
