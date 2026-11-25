# p2p-tunnel 状态交接

> 2026-10-09 初始建档；2026-11-24 v0.1 CLI 完成；2026-11-25 真机跨平台联调通过并发布 v0.1.0；同日 ticket 14–17（CLI 输出层与 exit、日志分流、会话告别、告别/断链边界）全部实现，两轴审阅修复后合并 `main`（4 个按票提交，`a89e391`）；随后发布 v0.1.1（Windows 告别用例超时修复 `3d9b268`）；v0.1.2 增加连接路径类别摘要（不含地址）；v0.1.3 修复已连接后仍可再次配对的问题（票据 18）；票据 19 把 CLI 双进程端到端纳入自动化（`crates/tunnel-cli/tests/two_process.rs`）；v0.1.4 修复「等待 INVITE 时命令被误当配对码」并移除数字菜单别名（全量 154 tests）。Windows ↔ Termux 跨网配对与 SSH 转发真机通过。以最新源码、分支、CI 与手工记录为准，不要只凭本文。

## 2026-11-25：v0.1.0 发布与真机联调

### 真机联调（用户手工，跨设备）

| 链路 | 结果 | 证据要点 |
| --- | --- | --- |
| macOS (Apple Silicon) ↔ Windows x86_64 | **配对与规则同步打通** | 两端真实 SDK 配对，比较码一致（`372470`），`链路: 已连接（managed link）`；`bootstrap_outbound` 一侧 false、一侧 true；双方 `status` 都能看到对端规则（如 `对端:2233 -> 本机:8022`）。 |
| Termux (Android aarch64) ↔ Linux x86_64 | **真实隧道转发打通** | Termux 侧 `add 2233 8022`、Linux 侧 `add 2233 22`，双向规则同步；目标端审计 `target_port=8022 peer_session=... result=ok`；`status` 活动连接从 1 条随客户端关闭回到 0。 |
| Windows ↔ Termux（跨网） | **配对接通、规则与转发通过** | 2026-11-25：Termux 在远程家庭网络、Windows 在本地 NAT 后；比较码 `594092` 自动确认，`[+] 配对完成，已连接（NAT 打洞）`（v0.1.2+）；双向规则互见，`ssh -p` 转发正常；Windows `quit` 后 Termux 打印「对端已退出」。macOS 与 Windows 在 v0.1.3 复测通过。 |

结论：v0.1 的数据面在 Android/bionic、macOS、Windows、Linux 四类真实环境上都能建立会话并转发 TCP；Windows ↔ Termux 跨网（两侧 NAT）配对、规则同步与 SSH 转发已实测，票据 08 已关闭，边界项见下「未验证」。

### 真机联调暴露的问题（已记录，下一版处理）

1. **对端退出后仍重连**：一端 `quit`，另一端进入 `重连中` 并等满 120 秒才报会话丢失。两端都主动退出时不需要重连，需要会话级告别（见 `.scratch` issue 11）。**已由 ticket 16 修复**：新增 `0x03 Farewell`，对端 1 秒内结束并退出 0；断链改为 5 秒未恢复即“断开连接”退出 0。
2. **提示符与日志交错**：空回车会重复打印 `> `；`INFO` 审计行落在提示符之后，界面显乱（见 issue 13，已吸收 12）。
3. **没有 `exit` 别名**：用户输入 `exit` 得到未知命令（issue 13）。
4. 活动连接只在拥有规则的一侧显示；对端侧只看到审计日志，容易被误读为“没有连接”。

### 2026-11-25 的 CLI/构建改动

- 增加 `> ` 提示符；`status` 行改为 `本机:<p> -> 对端:<q>`，不再出现 `B`。
- 逐条连接建立/关闭不再打印，连接数只在 `status` 的活动连接里体现。
- quinn 的 `UDP demultiplexer shut down` 属配对后关闭 bootstrap 的预期噪声，默认过滤；tracing 改走 stderr。
- 新增 Android aarch64 产物（NDK r28、bionic、PIE；二进制品无 TLS 段，因此不受 64 字节对齐限制）。Termux 不能用 linux-musl（非 PIE）或 linux-gnu（glibc TLS 对齐 8 字节）。

## 2026-11-25：ticket 16 会话告别（Farewell）

- 协议：状态帧新增 `0x03 Farewell`（payload 长度必须为 0）；首字节分流识别 `0x03`，长度非 0 按协议错误 `reset(0x01)`；状态流上的合法 Farewell 与专用流一致：上报告别并按 §9.5 结束会话（ticket 17）。
- 本端退出：在已认证数据链路上新开一条 bi stream 发 `Farewell`，1 秒上限 + 等待对端关闭流 500 ms；发送失败/超时只记日志，不阻塞退出。
- 对端退出：1 秒内打印 `[对端] 对端已退出`，RST 全部活动连接、关闭全部 listener，不进 `Reconnecting`，退出码 `0`。
- 断链：进入 `Reconnecting` 后 5 秒未恢复即打印 `[!] 断开连接：链路重连 5 秒未恢复，会话结束`，RST 活动连接、关 listener、清理后退出 `0`；5 秒内恢复视为抖动，继续原会话。认证失败仍按会话丢失退出 `1`。
- 清理：新增 bridge 任务登记表，退出时等待（上限 2 秒）后 abort；listener、状态流、配对任务、SDK peer 按 §9.5 收尾；退出后同一端口可立即重绑。
- 证据：`cargo test -p tunnel-core --lib`、`--test farewell`、真实 SDK 环回告别用例；双机人工验证尚未做（见下“未验证”）。

## 2026-11-25：ticket 17 告别/断链边界加固

- 畸形 Farewell：长度非 0 或截断的 `0x03` 在解码阶段失败，复用 Hello/RuleList 的同一状态流失败路径——关闭状态流、发 `PeerRulesUnknown`、数据面不受影响；不产生 `PeerFarewell`/`Disconnected`/`SessionLost`，会话继续、退出码不变。
- 数据面：注入畸形 Farewell（状态流与专用 0x03 流）后，已有 TCP 转发继续回显，新连接仍能建立；专用流畸形帧按 `reset(0x01)` 收尾。
- 断链边界：状态流失效后真实断开链路，仍走 ticket 16 的 5 秒判定报「断开连接」退出 `0`，不误报「对端已退出」；退出后同一端口可立即重绑，无残留 listener/任务。
- 证据：`cargo test -p tunnel-core --test farewell`（新增 4 个用例：状态流告别、状态流畸形 Farewell、专用流畸形 Farewell、状态流失效后 5 秒断链）与 `--lib` 新增 `farewell_on_state_stream_signals_session_end`、畸形 Farewell 帧拒绝用例。

## 2026-11-25：v0.1.4 CLI 输入修复

- 缺陷（用户真机复现）：已连接后输入 `add 8080 8080`，因为此前输入过 `2` 进入了「等待 INVITE」状态，该行被当作配对码送出，界面报 `[!] 配对失败：已连接，不能再次配对…`，命令本身没有执行。
- 修复：等待 INVITE/REPLY 时只接受 `P2PR-` 开头的配对码；其他输入提示「无效的配对码」并留在等待状态，命令不再被误送。
- 数字菜单 `1`/`2`/`3` 仅在配对前有效（启动菜单）；已连接（`Ready` 之后）按未知命令处理。`create`/`join` 在已连接后由 CLI 直接拒绝，不进入等待状态。`help` 不列数字别名，已连接后也不再展示 `create`/`join`。SPEC §8.1/§8.2、README 同步。
- 回归测试：`menu_numbers_work_before_pairing`、`awaiting_invite_rejects_commands_instead_of_pairing`；双进程用例增加「已连接后 `create`/`1`/`3` 被拒且会话保持」断言。全量 154 tests。

## 2026-11-25：票据 19 CLI 双进程端到端

- 新增 `crates/tunnel-cli/tests/two_process.rs`：起两个真实二进制，用 stdin 交换 INVITE/REPLY 完成真实 SDK 配对；A `quit` 后断言 A 退出 0、B 打印 `[对端] 对端已退出` 并退出 0、A 的监听端口退出后可立即重绑。
- 覆盖范围：进程拼装层（stdin 驱动、事件循环排空、进程退出码、端口释放）；core 语义（1 秒告别、5 秒断链）仍由 `tests/farewell.rs` 与 mock 用例覆盖。
- 本地耗时约 5 秒；`cargo test --workspace` 153 passed。

## 2026-11-25：两轴审阅修复（`feat/review-fixes`）

- `docs/ARCHITECTURE.md` 与实现对齐：补齐 CLI `input.rs`/`output.rs` 模块、`InputEvent`/逐字节 stdin、`exit` 退出路径；会话结束按本端退出、对端 `Farewell`、5 秒断链分开描述，`SessionLost` 只对应认证失败/链路停止。
- 退出顺序按 SPEC §9.5 改为：停 listener → reset 活动连接并等 bridge 收尾 → 发 `Farewell`（仅本端主动退出）→ 释放数据链路与 SDK peer；`Farewell` 不再在 listener 仍可接新连接时发送。
- 结束路径抑制后续链路事件：对端 `Farewell` 或断开判定后不再发 `LinkState(Reconnecting)`/断开计时事件；`session_lost()` 更名为 `end_session()`，修复断链分支未置位结束标记的问题。
- `Farewell` 编解码统一走 `RuleFrame::farewell_frame()`/`decode_farewell()`，删除 `rule_sync` 手工读 2 字节与 `session` 手工拼帧头。
- 已知缺口只做文档化：Windows 无 console 输入实现（tty 清行重绘仅 unix）；CLI 双进程端到端当时未自动化，已由票据 19 补齐（见「已验证」）。
- 证据：`cargo test --workspace` 151 passed；新增 `peer_farewell_suppresses_reconnecting_after_end`。

## 2026-11-24：v0.1 CLI 完成（ticket 10）

- CLI 已完整接线：`add` / `close` / `status` / `help` / `quit`；`1`/`create`、`2`/`join` 是菜单命令别名，`3` 是 `quit` 的菜单别名（SPEC §8.1 菜单需要）。`exit` 已由 ticket 14 增加并与 `quit` 等价；单字符 `q`、`?` 仍不支持。
- 配对：创建方/加入方流程、INVITE/REPLY 粘贴、比较码自动确认（v0.1 不阻塞）。`Ready` 每次配对只发一次；重连只发 `LinkState(Connected)`。
- `status` 四段按 SPEC §8.3 输出：链路 / 本机规则 / 对端规则 / 活动连接。
- `close` 只允许关自己的规则；成功时对本机该规则的活动 TCP 发 RST，对端打印 `[对端] 远程主动断开了 <port>`。
- `Command::Stop` 已实现（停止监听、RST 活动连接、关闭数据链路，保留 core/进程）；v0.1 CLI 不使用。
- 退出：`quit`/`exit`/菜单 `3`/Ctrl-C/stdin EOF 发送 `Shutdown` 并排空事件，5 秒上限；本端主动退出在已认证链路上尽力发 `Farewell`；退出码 `0` 正常、`1` 运行错误、`2` 参数错误。
- stdin 行通道有界容量 64；`crates/tunnel-cli/tests/cli.rs` 用 3000 行输入覆盖背压。
- 有意保留的 API 扩展：`Event::PeerRulesUnknown`、`Event::PeerRulesStale`、`TunnelError::SessionLost`，用于如实表示规则同步/链路状态。

## 测试与验证证据

### 已验证（本地 Linux x86_64，2026-11-25）

| 项目 | 证据 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 通过 |
| `cargo test --workspace` | **153 passed**（tunnel-core 单元 81 + `farewell` 7 + `listener_echo` 14 + `rule_sync` 8 + tunnel-cli 单元 21 + 黑盒 21 + 双进程 1） |
| 真实 SDK 双进程环回 | 2026-11-24 ticket 06/10 手工记录（配对、双向 echo、close/RemoteClosed、审计 `result=ok`）；记录在 `.tmp/`，不提交 |
| 退出清理与端口重绑 | core 用例 `shutdown_releases_listener_port_for_immediate_rebind`、`peer_farewell_ends_session_and_rsts_active_connections`；`listener_echo` 断开连接后立即 `bind` 同端口成功 |
| 真机跨平台 | 见上表：macOS ↔ Windows、Termux ↔ Linux |

### CI 与发布产物（2026-11-25）

| 项目 | 证据 |
| --- | --- |
| GitHub Actions | 最新 run [45588223603](https://github.com/juezhong/p2p-tunnel/actions/runs/45588223603)（head `c1f4779`，2026-11-25）全绿：Linux `test`（fmt + clippy + test）与 Windows `test` 两个 job；此前 run 45586061699 同绿。 |
| 测试分流 | Linux job 跑 fmt + clippy + test + UI 边界守卫；Windows job 只跑 test。 |
| 交叉构建 | Linux aarch64-gnu / x86_64-musl / aarch64-musl 用 `houseabsolute/actions-rust-cross`；aarch64-musl 冒烟经 `docker/setup-qemu-action` 真实执行二进制。 |
| Android | NDK r28 构建 `aarch64-linux-android`；冒烟校验 PIE 成功，二进制无 TLS 段（`tls=none`），不受 arm64 TLS 对齐限制。 |
| macOS | `macos-15` 原生构建 + ad-hoc `codesign`；Gatekeeper quarantine 需用户手动去掉。 |
| 产物 | 7 个 artifact：linux-x86_64-gnu / linux-aarch64-gnu / linux-x86_64-musl / linux-aarch64-musl / windows-x86_64 / macos-aarch64 / android-aarch64，均含二进制与 `SHA256SUMS`。 |
| 发布 | tag `v0.1.0`–`v0.1.3` 均已发布：v0.1.0（`a4ee6b6`，[run 45559849832](https://github.com/juezhong/p2p-tunnel/actions/runs/45559849832)）、v0.1.1（`dd4252d`）、v0.1.2（`cbd7a9f`）、v0.1.3（`2412267`，最新）。每个 Release 含 7 个平台二进制 + `SHA256SUMS`。 |

### 未验证（不要引用为已完成）

| 项目 | 状态 |
| --- | --- |
| 08 边界项（已收口） | 本机端口冲突、目标 RST、Termux 后台限制、Windows 防火墙、Termux 原生/musl 构建记录未逐项留档；核心跨网配对与转发已真机通过。票已关闭，边界项按已知限制保留。 |
| 09 诊断字段（已收口） | 真实 IP/端口按隐私要求不采集、不落日志与界面；无中继由 SDK 无 TURN/relay 代码保证。CLI 只在配对完成时显示类别摘要，v0.1.2 起交付。票已关闭。 |
| 真实 `ssh -p` 会话 | 真机记录里是端口转发与短连接，OpenSSH 长会话未留存记录。 |
| 断链 5 秒 → 退出码 0 | 由 core 单元/集成测试与真实 SDK 环回用例覆盖；没有双机人工拔网记录（按设计属本地判定）。 |
| 对端主动退出 → 会话结束 | **已实现并真机验证**（`0x03 Farewell`）：mock 双会话与真实 SDK 环回测试覆盖；Windows `quit` 后 Termux 打印「对端已退出」。 |
| 绑定失败文案 | 由 core 单元/集成测试覆盖，未在真实端口冲突场景手工复核。 |
| 告别帧只在状态流可用时生效 | 状态流已失败的会话收不到对端告别，仍按 ticket 16 的 5 秒断开判定处理；畸形/截断告别只让对端规则未知，不结束会话。由 ticket 17 的 core 集成测试覆盖，无双机人工记录。 |
| Windows tty 行为 | **不具备**：没有 Windows console API 实现，CLI 走行模式，无清行重绘/半行恢复；ticket 14 的 tty 行为仅在 unix raw mode 下成立。 |
| 连接路径摘要 | CLI 在配对完成时显示一次类别（`同网直连`/`NAT 打洞`），不显示 IP/端口；真实字段只在发布核验时内部查看。 |
| CLI 双进程端到端 | **已自动化**（2026-11-25 票据 19）：`crates/tunnel-cli/tests/two_process.rs` 启动两个真实二进制，经 stdin 交换 INVITE/REPLY 完成真实 SDK 配对，断言 A `quit` 后双方退出码 0、B 打印「对端已退出」、监听端口可立即重绑；本地约 5 秒。 |

## 现在能做什么

- 从 Releases 下载对应平台二进制，两端配对后 `add <本机端口> <对端端口>` 即可转发；Android/Termux 用 `android-aarch64` 包。
- 同机双进程或跨设备（Windows/macOS/Linux/Termux）都能建立会话；`status` 看规则与活动连接。
- 所有 UI 文案与退出码有黑盒测试兜底。
- 一端 `quit` 后，另一端 1 秒内打印 `[对端] 对端已退出` 并退出 0；链路断开 5 秒未恢复打印「断开连接」并退出 0。

## 下一任务

v0.1 实现票已全部关闭归档；后续按工作区 `.scratch/p2p-tunnel-v0/map.md` 的 Not yet specified 重新拆票。

1. TUI（v0.2）/GUI（v0.3）。
2. 已知限制（用户决定不做，不再排期）：Windows console 清行重绘、Termux 后台进程限制、Windows 防火墙真机留档。
