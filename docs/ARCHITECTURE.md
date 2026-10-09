# p2p-tunnel 设计基线（待细化）

## 需求

- 用户在本机 `127.0.0.1:2222` 连接 SSH 客户端，P2P Tunnel 将 TCP 字节流通过 SDK QUIC Stream 送到远端授权的 `127.0.0.1:22`；Web/TCP 服务同理。
- **SSH 身份认证、Host Key 校验由原生 OpenSSH 完成**。P2P 对端身份及可转发的远端地址/端口授权仍必须单独由 SDK 与 Tunnel 检查。
- TCP 半关闭、取消、背压、超时、并发 Stream 资源限制是硬性需求；默认本地绑定 loopback，禁止任意目标内网扫描/未授权端口转发。
- 初版以本地端口转发为核心。反向端口转发、SOCKS5 动态代理均属未来可评审选项，不默认实施。
- 共享 `tunnel-core`；必须独立交付 CLI、TUI、GUI，三者功能一致。
- SDK 负责 ICE/QUIC 与网络诊断，Tunnel 只负责 TCP 代理语义和访问授权。

## 最小里程碑

- M0：Rust Workspace、目标权限与 TCP 半关闭模型、单元测试。
- M1：本机 TCP echo + mock Stream，验证背压和双向关闭。
- M2：接入已验收 SDK Session，远程 SSH 端口转发。
- M3：Web/TCP 并发、失败注入、平台测试、诊断。
- M4：CLI/TUI/GUI 完整交付与正式发布。

## 关键手测

原生 OpenSSH 密钥认证/主机指纹不被绕过；本地 loopback 监听；多个 SSH/Web 连接；断开/取消后端口资源回收；未授权目标拒绝；IPv4/IPv6 NAT 无中继。
