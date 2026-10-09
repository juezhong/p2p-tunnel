# p2p-tunnel

安全的 P2P TCP 端口转发（SSH/Web）

> **状态：规划阶段。** 当前尚无可用的 Rust 实现，也无已完成的 CLI/TUI/GUI。请从 [AGENTS.md](AGENTS.md) → [docs/WORKFLOW.md](docs/WORKFLOW.md) → [docs/STATUS.md](docs/STATUS.md) 开始。

## 产品目标

本地监听 TCP，经经身份认证的 SDK QUIC Stream 直连到远端授权 TCP 服务；SSH 用户和密钥验证仍由 OpenSSH 负责。

使用 Rust + Tokio，共享 [p2p-sdk](https://github.com/juezhong/p2p-sdk) 的 ICE、NAT/IPv4/IPv6、身份认证、Quinn/QUIC、诊断能力。所有业务数据直连、端到端加密，不经信令服务器中继。发行版必须提供 **CLI、TUI、GUI**，三种界面共享同一业务 core。

完整开发说明见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。
