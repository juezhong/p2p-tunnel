# p2p-tunnel 状态交接

> 2026-10-09 初始建档。以最新源代码、PR、CI 为准，而不是只凭本文。

## 当前状态

- 项目阶段：**M0 规划**，只有 LICENSE 与本次项目/Agent 指南，尚无业务源码。
- 语言：Rust；界面最终要求：CLI / TUI / GUI；共享业务 core。
- SDK：`juezhong/p2p-sdk`，网络穿透、端到端认证、数据通道、诊断全部依赖它。
- 业务目标：本地监听 TCP，经经身份认证的 SDK QUIC Stream 直连到远端授权 TCP 服务；SSH 用户和密钥验证仍由 OpenSSH 负责。
- 设计尚待细化：见 `docs/ARCHITECTURE.md`，不要将建议性内容当作已实施代码。
- 无 CI 测试可报告。后续代码 PR 应更新此处，附带实测证据。

## 下一任务

1. 核查 SDK 当前可调用的 Rust API 和 M1 连通性进展。
2. 先讨论并确定本应用最小业务 API、权限、平台差异与测试矩阵。
3. 新建 `feat/` 分支提交应用 core/测试骨架（尚无 SDK 真实网络能力时可用 mock），PR 中明确实际功能进度。
