# p2p-tunnel 架构

> 实现规格见 [`SPEC.md`](SPEC.md)；本文描述**当前代码的实际结构**与数据流。
> 验证状态（哪些跑过、哪些没有）见 [`STATUS.md`](STATUS.md)。

## 需求边界

- 本机 `127.0.0.1:<本机端口>` 监听，字节流经 SDK 认证加密的 QUIC Stream 送到对端，再由对端连 `127.0.0.1:<对端端口>`。
- 目标固定为对端 loopback；每条规则由「本机端口 + 对端端口」组成，可由任意一端动态 `add`/`close`。
- **配对即信任**：配对后对端可访问本机任意 loopback 端口，不逐端口审批。SSH 的身份认证与 Host Key 校验仍由 OpenSSH 完成。
- 业务数据不中继、不回退 TURN/HTTP 代理；没有直连路径就报失败。
- STUN/ICE/NAT 穿透、身份加密、Quinn/QUIC、managed link 重连全部由 `juezhong/p2p-sdk`（pin `d4cdd2e`）提供，本仓库不复制。
- CLI、TUI、GUI 共享 `tunnel-core`；UI 不得出现 `quinn` / `p2p_sdk` 类型（CI 有 grep guard）。

## 实际模块布局

```text
crates/
├── tunnel-core/src/
│   ├── api.rs          Command / Event / Forward / LinkState / TunnelError（UI 唯一接口）
│   ├── session.rs      Session::spawn、命令-事件主循环、规则表、listener、链路状态、退出清理
│   ├── pairing.rs      手动配对状态机；包装 SDK begin_creator/begin_joiner、比较码自动确认
│   ├── transport/
│   │   ├── mod.rs      DataLink / TunnelStream / TunnelRead / TunnelWrite seam；open_bi 5s 超时
│   │   ├── sdk.rs      SDK ManagedAuthenticatedLink → DataLink 适配（跨 generation 重连）
│   │   ├── quinn_link.rs 测试/诊断用单条 quinn::Connection → DataLink 适配（生产不走这条）
│   │   └── mock.rs     内存双端链路，供测试
│   ├── protocol.rs     6 字节数据流头、Hello/RuleList 状态帧、错误码
│   ├── rule_sync.rs    规则同步流：首字节分流、全量快照重发、Unknown/Stale 降级
│   ├── bridge.rs       单条 TCP ↔ bi stream 手动双泵（16 KiB 有界缓冲）、FIN/RST/STOP、目标拨号与审计日志
│   └── limits.rs       每规则 64 连接、每侧 96 数据流的 RAII 名额
├── tunnel-cli/src/main.rs   v0.1 唯一可用 UI
├── tunnel-tui/src/lib.rs    占位（v0.2）
└── tunnel-gui/src/lib.rs    占位（v0.3）
```

编译产物：`tunnel-cli` 的二进制名是 `p2p-tunnel`。

## 运行时结构

```text
stdin 线程 ──行──▶ CLI select! ──Command──▶ Session worker
                      ▲                        │
                      └──────── Event ◀────────┘
                                               │
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                        pairing 任务      rule_sync 任务     listener/accept
                        (SDK ICE/QUIC)    (状态流)          (每规则 1 个)
                                               │
                                               ▼
                                        bridge（每条 TCP 一条）
```

- 一个 `Session` 独占一个 tokio task；所有状态修改在 worker 内串行发生，UI 只消费事件。
- 配对成功后才建数据链路与 listener；`Ready` 前 `AddForward` 直接返回 `Error`。
- 客户端 `bootstrap` 只作会话锚点，应用数据走 `manage_link()` 返回的附加 QUIC。
- 链路阶段变化（`Connecting/Connected/Reconnecting/AuthenticationFailed/Stopped`）由 SDK 订阅转成 `LinkState` 事件；重连期间先发 `PeerRulesStale`，恢复后重发规则快照。`Ready` 每次配对只发一次，重连只发 `LinkState(Connected)`。
- 会话不可恢复时依次发 `Error(SessionLost)` 与 `LinkState(Stopped)`；CLI 以退出码 `1` 结束。

## CLI 行为要点

- 命令按空白切分；`add`/`close` 先做本地校验（端口、参数个数、是否本机规则），再交给 core。
- 等待粘贴 INVITE/REPLY 时仍接受 `help`/`status`/`quit`，避免交互死锁；其余整行按配对码发送。
- stdin 读取线程通过**有界**通道（容量 64）把行交给主循环；通道满时读线程阻塞，主循环卡住时不会把输入堆进内存。
- `status` 按 SPEC §8.3 输出四段：链路（连接时含 `managed link`）、本机规则（`监听中` + 连接数）、对端规则（`B:<port> -> 本机:<port>`）、逐条活动连接（`本机:<port> 的连接 -> 对端:<port>`）。
- 状态全部来自 core 事件：未配对显示「未连接」，重连显示「链路: 重连中」并标注对端规则可能过期，规则同步流不可用时标注「规则未知」。
- `close` 成功的 `[-] <port> 已关闭（活动连接已重置）` 在 core 的 `RulesChanged` 到达后才打印；失败会回滚并显示 `[!]` 错误。
- 退出路径（`quit`/Ctrl-C/EOF）统一发送 `Shutdown`，先排空已产生事件再等 core 清理，总上限 5 秒。

## 测试布局

- `crates/tunnel-core/src/**`：单元测试（编解码、bridge、limits、session 状态机等）。
- `crates/tunnel-core/tests/listener_echo.rs`：真实 TCP/listener 行为。
- `crates/tunnel-core/tests/rule_sync.rs`：规则同步与 JSON 快照语义。
- `crates/tunnel-cli/tests/cli.rs`：启动真实二进制、管道驱动 stdin，断言文案与退出码；不需要网络。

## 与规格的关系

`docs/SPEC.md` 是实现规格（协议、状态机、CLI 文案、验收 gate），改动实现前先读对应章节。
ticket 10 任务描述里写的 “SPEC §7.7” 在 `SPEC.md` 中不存在；CLI 文案的实际来源是 §8。

### 有意的 API 扩展

以下接口在 SPEC §7.1/§7.5 的类型表里没有逐项列出，但为如实表达 SPEC §5.4（规则同步降级）与 §9.1/§9.2（链路重连、会话丢失）而保留：

- `Event::PeerRulesUnknown`：状态流不可用、对端规则未知。
- `Event::PeerRulesStale`：链路重连中，最后一次快照可能过期。
- `TunnelError::SessionLost`：会话不可恢复，CLI 据此清空对端规则并以退出码 1 结束。
- CLI 文案「未配对时链路显示未连接」「对端规则未知」「链路重连中…」。

`Command::Stop` 已按 §7.3/§7.5 实现（停 listener、RST 活动连接、关数据链路、保留 core），v0.1 CLI 不使用，保留给 TUI/GUI。
