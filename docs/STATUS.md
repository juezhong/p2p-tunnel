# p2p-tunnel 状态交接

> 2026-10-09 初始建档；2026-11-24 v0.1 CLI 完成；2026-11-25 真机跨平台联调通过并发布 v0.1.0。以最新源码、分支、CI 与手工记录为准，不要只凭本文。

## 2026-11-25：v0.1.0 发布与真机联调

### 真机联调（用户手工，跨设备）

| 链路 | 结果 | 证据要点 |
| --- | --- | --- |
| macOS (Apple Silicon) ↔ Windows x86_64 | **配对与规则同步打通** | 两端真实 SDK 配对，比较码一致（`372470`），`链路: 已连接（managed link）`；`bootstrap_outbound` 一侧 false、一侧 true；双方 `status` 都能看到对端规则（如 `对端:2233 -> 本机:8022`）。 |
| Termux (Android aarch64) ↔ Linux x86_64 | **真实隧道转发打通** | Termux 侧 `add 2233 8022`、Linux 侧 `add 2233 22`，双向规则同步；目标端审计 `target_port=8022 peer_session=... result=ok`；`status` 活动连接从 1 条随客户端关闭回到 0。 |
| Windows ↔ Termux | **配对与链路已通** | 2026-11-25：两端比较码一致，`配对完成，managed 数据链路已连接`，`bootstrap_outbound` 分别为 false/true，`status` 均显示已连接。缺少真实 `ssh -p` 双机、端口冲突、RST、后台限制/防火墙记录。 |

结论：v0.1 的数据面在 Android/bionic、macOS、Windows、Linux 四类真实环境上都能建立会话并转发 TCP；ticket 08 的“双机验收清单”仍不完整，不能宣称完成。

### 真机联调暴露的问题（已记录，下一版处理）

1. **对端退出后仍重连**：一端 `quit`，另一端进入 `重连中` 并等满 120 秒才报会话丢失。两端都主动退出时不需要重连，需要会话级告别（见 `.scratch` issue 11）。
2. **提示符与日志交错**：空回车会重复打印 `> `；`INFO` 审计行落在提示符之后，界面显乱（见 issue 12）。
3. **没有 `exit` 别名**：用户输入 `exit` 得到未知命令（issue 12）。
4. 活动连接只在拥有规则的一侧显示；对端侧只看到审计日志，容易被误读为“没有连接”。

### 2026-11-25 的 CLI/构建改动

- 增加 `> ` 提示符；`status` 行改为 `本机:<p> -> 对端:<q>`，不再出现 `B`。
- 逐条连接建立/关闭不再打印，连接数只在 `status` 的活动连接里体现。
- quinn 的 `UDP demultiplexer shut down` 属配对后关闭 bootstrap 的预期噪声，默认过滤；tracing 改走 stderr。
- 新增 Android aarch64 产物（NDK r28、bionic、PIE；二进制品无 TLS 段，因此不受 64 字节对齐限制）。Termux 不能用 linux-musl（非 PIE）或 linux-gnu（glibc TLS 对齐 8 字节）。

## 2026-11-24：v0.1 CLI 完成（ticket 10）

- CLI 已完整接线：`add` / `close` / `status` / `help` / `quit`；`1`/`create`、`2`/`join` 是菜单命令别名，`3` 是 `quit` 的菜单别名（SPEC §8.1 菜单需要）。`exit`、`?` 不支持。
- 配对：创建方/加入方流程、INVITE/REPLY 粘贴、比较码自动确认（v0.1 不阻塞）。`Ready` 每次配对只发一次；重连只发 `LinkState(Connected)`。
- `status` 四段按 SPEC §8.3 输出：链路 / 本机规则 / 对端规则 / 活动连接。
- `close` 只允许关自己的规则；成功时对本机该规则的活动 TCP 发 RST，对端打印 `[对端] 远程主动断开了 <port>`。
- `Command::Stop` 已实现（停止监听、RST 活动连接、关闭数据链路，保留 core/进程）；v0.1 CLI 不使用。
- 退出：`quit`/Ctrl-C/stdin EOF 发送 `Shutdown` 并排空事件，5 秒上限；退出码 `0` 正常、`1` 运行错误、`2` 参数错误。
- stdin 行通道有界容量 64；`crates/tunnel-cli/tests/cli.rs` 用 3000 行输入覆盖背压。
- 有意保留的 API 扩展：`Event::PeerRulesUnknown`、`Event::PeerRulesStale`、`TunnelError::SessionLost`，用于如实表示规则同步/链路状态。

## 测试与验证证据

### 已验证（本地 Linux x86_64，2026-11-25）

| 项目 | 证据 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 通过 |
| `cargo test --workspace` | **111 passed**（tunnel-core 单元 71 + `listener_echo` 14 + `rule_sync` 8 + tunnel-cli 单元 4 + 黑盒 14） |
| 真实 SDK 双进程环回 | 2026-11-24 ticket 06/10 手工记录（配对、双向 echo、close/RemoteClosed、审计 `result=ok`）；记录在 `.tmp/`，不提交 |
| 真机跨平台 | 见上表：macOS ↔ Windows、Termux ↔ Linux |

### CI 与发布产物（2026-11-25）

| 项目 | 证据 |
| --- | --- |
| GitHub Actions | run [45526832293](https://github.com/juezhong/p2p-tunnel/actions/runs/45526832293)（head `8350741`）全绿：2 个 `test` job + 7 个 `build binaries` job。 |
| 测试分流 | Linux job 跑 fmt + clippy + test + UI 边界守卫；Windows job 只跑 test。 |
| 交叉构建 | Linux aarch64-gnu / x86_64-musl / aarch64-musl 用 `houseabsolute/actions-rust-cross`；aarch64-musl 冒烟经 `docker/setup-qemu-action` 真实执行二进制。 |
| Android | NDK r28 构建 `aarch64-linux-android`；冒烟校验 PIE 成功，二进制无 TLS 段（`tls=none`），不受 arm64 TLS 对齐限制。 |
| macOS | `macos-15` 原生构建 + ad-hoc `codesign`；Gatekeeper quarantine 需用户手动去掉。 |
| 产物 | 7 个 artifact：linux-x86_64-gnu / linux-aarch64-gnu / linux-x86_64-musl / linux-aarch64-musl / windows-x86_64 / macos-aarch64 / android-aarch64，均含二进制与 `SHA256SUMS`。 |
| 发布 | tag `v0.1.0`（commit `a4ee6b6`）已发布 [Release](https://github.com/juezhong/p2p-tunnel/releases/tag/v0.1.0)：7 个带平台名的二进制 + `SHA256SUMS`；run [45559849832](https://github.com/juezhong/p2p-tunnel/actions/runs/45559849832) 全绿。 |

### 未验证（不要引用为已完成）

| 项目 | 状态 |
| --- | --- |
| **ticket 08：Windows ↔ Termux LAN 双机验收** | **部分完成**：配对与链路已通（比较码一致）；真实 `ssh -p` 双机、端口冲突、RST、防火墙/后台限制记录仍缺。 |
| **ticket 09：真实 NAT/CGNAT** | **未完成、未验证**。没有跨网配对、没有 STUN/ICE 诊断字段、没有无中继证据。 |
| 真实 `ssh -p` 会话 | 真机记录里是端口转发与短连接，OpenSSH 长会话未留存记录。 |
| 会话丢失 120s → 退出码 1 | 只有代码路径与 core 测试覆盖，没有端到端手工记录。 |
| 对端主动退出 → 会话结束 | 未实现；当前会走重连（issue 11）。 |
| 绑定失败文案 | 由 core 单元/集成测试覆盖，未在真实端口冲突场景手工复核。 |

## 现在能做什么

- 从 Releases 下载对应平台二进制，两端配对后 `add <本机端口> <对端端口>` 即可转发；Android/Termux 用 `android-aarch64` 包。
- 同机双进程或跨设备（Windows/macOS/Linux/Termux）都能建立会话；`status` 看规则与活动连接。
- 所有 UI 文案与退出码有黑盒测试兜底。

## 下一任务

1. issue 11：会话级告别（一端 `quit` → 另一端立即结束，不重连 120 秒）。
2. issue 12：CLI 交互与日志显示打磨（空回车提示符、`exit` 别名、日志分流）。
3. ticket 08：Windows ↔ Termux 双机验收全清单（真实 `ssh -p`、RST、端口冲突、后台限制）。
4. ticket 09：真实路由器/CGNAT 记录、无中继证据。
5. TUI（v0.2）/GUI（v0.3）不阻塞 v0.1。
