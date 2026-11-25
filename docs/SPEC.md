# p2p-tunnel v0.1 开发规格

> 状态：**draft for review**（决策已收敛；实现票已拆分，待开工）
> 日期：2026-11-24
> 上游决策：工作区 `.scratch/p2p-tunnel-v0/map.md`（wayfinder，已关闭）
> 关键决策票：001–014；实现研究：工作区 `.scratch/p2p-tunnel-v0/research/009-sdk-baseline.md`、`.scratch/p2p-tunnel-v0/research/013-quinn-tcp-bridge.md`
> SDK pin：`d4cdd2ed47285248da31e54abbdc6e27f99361d2`

本文把 wayfinder 的决策收敛成可开工的规格。实现票从本文拆分；本文不替代 `docs/ARCHITECTURE.md` 的产品背景，也不表示代码已完成。

---

## 1. 目标与成功标准

### 1.1 目标

v0.1 交付一个 Rust + Tokio 的 CLI：两个已配对的设备 A/B，通过 `p2p-sdk` 建立**直连、无中继**的认证 QUIC 会话；每端可以动态添加/关闭任意多条 TCP 端口转发：

```text
本机 127.0.0.1:<listen_port>  ──隧道──>  对端 127.0.0.1:<peer_port>
```

典型用法：A 上 `ssh -p 2211 user@127.0.0.1`，流量经隧道到达 B 的 `127.0.0.1:22`。SSH 的身份认证和 Host Key 校验仍由 OpenSSH 完成。

### 1.2 成功标准

- A/B 配对成功，数据链路建立；两端都能添加多条规则并同时工作。
- `ssh -p <listen_port>`、HTTP/TCP echo 等普通 TCP 流量可双向传输。
- TCP 半关闭（FIN）、RST、STOP_SENDING、背压行为正确；错误不会被伪装成正常 EOF。
- 数据链路断开后自动重拨/重新认证；listener 保持，新 TCP 连接恢复；活动 TCP 连接断开（已接受）。
- 每端只能关闭自己的规则；对端只读，并显示“远程主动断开了 `<port>`”。
- 不出现 `0.0.0.0` 监听、任意 IP 目标、UDP、SOCKS5、TURN/中继。
- Windows x86_64、Linux x86_64、Android Termux aarch64 的可测试路径明确；CI 覆盖 Linux/Windows。

### 1.3 非目标

- `0.0.0.0` / 同网段共享。
- 任意 IP 目标、内网扫描、动态 SOCKS5。
- UDP 转发、TUN/L3 虚拟网络（属于 `p2p-net`）。
- TURN/中继/HTTP 代理；没有直连路径就明确失败。
- 应用层额外加密、端口 allowlist、逐连接审批（v0.1 配对即信任）。
- 持久配置、设备持久身份、bootstrap 全量会话恢复。
- TUI/GUI 的完整功能（v0.2/v0.3，v0.1 只留 crate 占位）。

---

## 2. 术语

| 术语 | 含义 |
| --- | --- |
| 会话（session） | A/B 一次配对；由 SDK 的 bootstrap QUIC 作锚点。 |
| 数据链路（data link） | SDK `manage_link()` 管理的一条附加 QUIC；承载 TCP 数据和规则同步流。 |
| 规则（rule） | `listen_port -> peer_port`；由**监听端**拥有。 |
| 本机规则 | 本端拥有的规则；本端监听 `127.0.0.1:listen_port`，目标是对端 `127.0.0.1:peer_port`。 |
| 对端规则 | 对端拥有的规则；本端只读显示。 |
| 监听端 | 在本机 `127.0.0.1` 上 accept TCP 的一侧。 |
| 目标端 | 收到数据流后拨本机 `127.0.0.1:port` 的一侧。 |
| 数据流 | 每条 TCP 连接对应的一条 QUIC bi stream。 |
| 规则同步流 | 一条 QUIC bi stream，只交换 `Hello` / `RuleList` 元数据；不做授权、限速、流量控制。 |
| bootstrap | SDK `ConnectedTransportPeer.bootstrap`，会话锚点；不承载 TCP 数据。 |
| generation | managed link 每次重新认证通过后递增的代数；用于识别新链路。 |

---

## 3. 产品模型

### 3.1 规则

- 每端可以**动态**添加/关闭任意多条本机规则；规则只在内存，退出或重新配对即清空。
- 监听地址固定 `127.0.0.1`；目标固定为对端 `127.0.0.1:<peer_port>`。
- 端口范围 `1..=65535`；`0` 无效。
- 本机监听端口重复 → 报错，不启动新 listener。
- 多个本机端口可以指向同一个对端端口（例如 2211、2222 都到 B:22）。
- 两端可以同时配置；状态同时列出本机规则和对端规则。
- 监听端可以在任一侧，因此 SSH `-L` 与 `-R` 是同一个原语。

### 3.2 信任与授权

- **配对即信任**：INVITE/REPLY + SDK 身份认证是唯一的信任边界。
- 配对后不再逐端口/逐连接确认；没有端口 allowlist，没有“允许/拒绝”弹窗。
- 目标端收到数据流头后，直接拨 `127.0.0.1:<port>`；接受任意 loopback 端口。
- 安全边界必须写在 UI/文档里：**只和可信设备配对；配对后对端可访问本机任意 loopback 端口。**
- 目标端记录“哪个会话访问了哪个端口”的审计日志（见 §10）。

### 3.3 关闭语义

- 只能 `close` 自己的规则；对端规则只读，不能远程关闭。
- 本地 `close <listen_port>`：
  1. 停止该 listener；
  2. reset 该规则的所有活动数据流/TCP 连接；
  3. 从本机规则列表移除；
  4. 重发 `RuleList`。
- 对端收到新的 `RuleList`，发现该 `listen_port` 消失：
  - 显示 `[对端] 远程主动断开了 <listen_port>`；
  - 该规则从“对端规则”列表移除；
  - 对应的目标侧 TCP 连接由对端 reset 数据流后自然关闭；本端不额外发“远程关闭请求”。
- 没有 `close peer`，没有 `CloseRequest` 协议消息。
- `close` 不存在的本机端口 → 报错，不产生协议消息。

### 3.4 明确不做

- `0.0.0.0` / LAN 共享。
- 任意 IP / 端口扫描。
- 端口 allowlist / 每次连接审批。
- 规则持久化/配置文件。
- 旧 Go `p2p-friend` 线协议兼容。

---

## 4. 架构与 workspace

### 4.1 Crate 布局

```text
p2p-tunnel/
├── Cargo.toml                 # workspace
├── Cargo.lock                 # 必须提交
├── crates/
│   ├── tunnel-core/           # 会话、配对、协议、bridge、规则
│   ├── tunnel-cli/            # v0.1 唯一交付的 UI
│   ├── tunnel-tui/            # v0.2 占位，不阻塞 v0.1
│   └── tunnel-gui/            # v0.3 占位，不阻塞 v0.1
├── docs/
│   ├── ARCHITECTURE.md
│   ├── SPEC.md                # 本文
│   └── STATUS.md
└── .github/workflows/ci.yml
```

### 4.2 边界

- `tunnel-core` 拥有：配对状态机、`Session`、规则表、listener、data link、规则同步流、TCP↔QUIC bridge。
- `tunnel-core` 通过 `mpsc` 的 `Command` / `Event` 与 UI 通信。
- UI **不接触** `quinn::Connection`、`ConnectedTransportPeer`、`ManagedAuthenticatedLink`。
- `tunnel-core` 内部用一个 SDK 适配层封装 `begin_creator` / `begin_joiner` / `connect_transport` / `manage_link`；`quinn::Connection` 只出现在 bridge/transport 模块。
- 不复制 SDK 的 ICE/STUN/QUIC/证书/HMAC/诊断代码。

### 4.3 数据面拓扑

```text
A: CLI ──Command/Event── A: tunnel-core
                              │
                              ├── 127.0.0.1:2211 listener (本机规则)
                              │
                              ├── bootstrap QUIC (会话锚点，不承载 TCP)
                              │
                              └── managed data link (一条附加 QUIC)
                                      │  bi stream / TCP
                                      ▼
B: tunnel-core ── accept_bi ── 拨 127.0.0.1:22
      │
      └── 127.0.0.1:8081 listener (B 自己的规则，可选)

规则同步流：data link 上的一条 bi stream，双向交换 Hello / RuleList。
```

- bootstrap 只负责配对/认证锚点；TCP 数据走 managed data link。
- 两端各调用一次 `manage_link()`；`bootstrap_outbound` 侧 `open_link`，另一侧 `accept_link`，形成**一条**逻辑数据链路。
- 数据链路断开后 SDK 自动重拨/重新认证；listener 不关闭。
- 规则同步流由 `bootstrap_outbound` 侧打开；两端都可在这条 bi stream 的发送方向写自己的 `RuleList`。
- 应用通过 `peer.diagnostic().bootstrap_outbound` 判断哪一侧负责打开状态流（该字段由 SDK 公开在 `TransportDiagnostic` 中）。

---

## 5. 线协议

### 5.1 数据流头

每条 TCP 连接一条 bi stream；发起方在 `open_bi()` 后**立刻**写 6 字节头。Quinn 要求 open 方先写数据，对端 `accept_bi()` 才会返回。

| offset | size | field | 值 |
| --- | --- | --- | --- |
| 0 | 1 | magic | `0x50` (`P`) |
| 1 | 1 | magic | `0x32` (`2`) |
| 2 | 1 | version | `0x01` |
| 3 | 1 | flags | `0x00`（v0.1 保留；非 0 视为协议错误） |
| 4 | 2 | target port | 大端 `u16`，`1..=65535` |

校验规则：

- magic 不是 `P2` → `reset(0x01)`。
- version 不是 `0x01` → `reset(0x01)`。
- flags 非 `0x00` → `reset(0x01)`。
- port 为 `0` → `reset(0x01)`。
- 6 字节头必须在 5 秒内读满；超时/短读/EOF → `reset(0x01)`。

### 5.2 数据流建立

监听端：

1. `TcpListener::accept()` 得到 TCP。
2. 如果本侧活动 data stream 已达 96 → 关闭 TCP，日志“连接数已满”。
3. 如果数据链路不在 `Connected`，最多等 5 秒等 `current()`；等不到 → 关闭 TCP，日志“链路重连中”。
4. `timeout(5s, connection.open_bi())`；失败/超时 → 关闭 TCP。
5. 立刻写 6 字节头（目标端口来自本机规则）。
6. 进入双泵。

目标端：

1. 在 data link 上常驻 `accept_bi()` 循环。
2. 读第一字节：
   - `0x50` (`P`) → 数据流；继续读 5 字节头。
   - `0x01` / `0x02` → 规则同步流帧；交给状态流处理。
   - 其他 → `reset(0x01)`，继续 accept。
3. 数据流：校验头；校验失败 → `reset(0x01)`。
4. 并发检查：该目标端口活动连接数 ≥64，或本侧总数 ≥96 → `reset(0x04)`。
5. `timeout(10s, TcpStream::connect(("127.0.0.1", port)))`；失败 → `reset(0x02)`。
6. 进入双泵；记录审计日志。

> 第一字节分流是必要的：规则同步流也是 bi stream，和数据流共用 `accept_bi()`。数据流首字节固定 `0x50`，状态帧首字节固定 `0x01/0x02`，不会冲突。

### 5.3 半关闭、RST、STOP

手动双泵，**禁止** `tokio::io::copy_bidirectional`（它保留 FIN，但会把 RST 伪装成正常 EOF）。

| TCP 事件 | QUIC 动作 | 对端表现 |
| --- | --- | --- |
| TCP 读 EOF | `SendStream::finish()` | 对端读到 `Ok(None)` |
| TCP 读错误（RST 等） | `SendStream::reset(0x10)` | 对端 `ReadError::Reset(0x10)` |
| TCP 写错误/取消 | `SendStream::reset(0x10)` | 对端 `ReadError::Reset(0x10)` |
| QUIC 读 `Ok(None)` | TCP `shutdown(Write)` | 对端 TCP 读到 EOF |
| QUIC 读 `ReadError::Reset(_)` | 本端 TCP RST：`set_zero_linger()` + drop | 对端 TCP `ECONNRESET` |
| QUIC 写 `WriteError::Stopped(_)` | 停止读 TCP，关闭该连接 | 不再发送 |
| 会话/数据链路断开 | 所有活动连接 TCP RST + stream reset/stop | 对端看到 RST |

实现约束：

- 每方向固定 16 KiB 缓冲；写操作 `await`，不建无界队列。
- 两个方向各自独立；正常 EOF 不打断另一方向；只有错误/取消才互相中止。
- 两个方向都结束后 drop stream；**不要**用 `Connection::close()` 关闭单条 TCP（会杀掉所有桥接）。
- 错误路径必须显式 `reset`/`stop`；不能依赖 `SendStream::drop` 的隐式 `finish`。
- QUIC 侧读错误映射为 TCP RST 时，使用 `TcpStream::set_zero_linger()`，不要先 `shutdown(Write)`。

### 5.4 规则同步流

一条 bi stream，由 `bootstrap_outbound` 侧在数据链路 `Connected` 后打开；两端都在这条流上写自己的帧、读对端的帧。

帧格式（v0.1）：

```text
frame  := type:u8 | len:u16 BE | payload[len]
type 0x01 Hello    payload := version:u8 (0x01)
type 0x02 RuleList payload := count:u16 BE | count * (listen_port:u16 BE, peer_port:u16 BE)
```

- `len` 是 payload 长度；Hello 的 `len == 1`；RuleList 的 `len == 2 + 4 * count`。
- `RuleList` 是**全量快照**，不是增量。
- `listen_port` 在同一个 `RuleList` 内必须唯一；重复 → 状态流协议错误。
- 收到未知 type、长度不匹配、version 不匹配 → 关闭状态流，标记“对端规则未知”；**不影响数据流**。
- 任一侧 `add` / `close` 后重发完整 `RuleList`。
- 状态流失败（数据链路仍存活）：数据继续；`status` 显示“对端规则未知”；由 `bootstrap_outbound` 侧重开状态流并重发。
- 数据链路断开：没有状态流；保留最后已知对端规则并标记“可能过期”；重连后以新 `RuleList` 为准，缺失项触发 `RemoteClosed`。
- 链路重连后，`bootstrap_outbound` 侧在新 data link 上重新打开状态流；两端重新 `Hello` + `RuleList`。
- 状态流是长生命周期流：发送 `Hello` / `RuleList` 后**不要** `finish()`；链路断开/退出时才关闭。
- 状态流只做规则生命周期/状态；不做授权、限速、流量控制。

### 5.5 错误码

| code | 含义 | 使用场景 |
| --- | --- | --- |
| `0x01` | 协议错误 | magic/version/flags/port 无效；头超时；状态帧格式错误（状态流用关闭代替 reset） |
| `0x02` | 目标拨号失败 | `127.0.0.1:port` 连接失败/超时 |
| `0x03` | 目标端口被拒绝 | 保留给未来 allowlist；v0.1 不使用 |
| `0x04` | 超过并发上限 | 每 forward 64 / 每侧 96 |
| `0x05` | 取消 | 本地主动放弃 |
| `0x10` | TCP RST / 读写错误 | 将 TCP 错误传给对端 |

### 5.6 加密与版本

- 所有隧道字节都在 SDK 的 QUIC（TLS 1.3 + mTLS + 会话 HMAC）内；tunnel **不另加应用层加密**。
- 版本 `0x01`；未知版本 → `reset(0x01)`。
- 未来若加应用层加密，放在 6 字节头之后、业务字节之前，并升 version；v0.1 不做。

---

## 6. 并发、超时、背压

### 6.1 限制

| 项 | 值 | 说明 |
| --- | --- | --- |
| 每 forward 活动连接 | ≤64 | 监听端按本机规则计数；目标端按目标端口计数 |
| 同一侧活动 data stream 合计 | ≤96 | 给 SDK 默认 100 条 bidi/方向留余量（96 条数据 + 1 条状态流） |
| 规则同步流 | 1 条 | 占用 1 条 bidi |
| `open_bi` 超时 | 5 s | 额度耗尽时 Quinn 会挂起，必须外层超时 |
| 数据流头读取 | 5 s | 目标端 |
| 目标拨号 | 10 s | `127.0.0.1:port` |
| 重连期间新 TCP 等待 | 5 s | 等不到 `current()` 就关闭 |
| 会话丢失判定 | 120 s | 连续 Reconnecting 且无 Connected；或 AuthenticationFailed |
| QUIC keepalive / idle | 10 s / 120 s | SDK 固定，应用不可调 |
| 应用层 TCP idle timeout | 无 | 避免杀掉空闲 SSH；链路死亡由 QUIC 检测 |

### 6.2 背压

- 依赖 Quinn 的流控（默认 stream receive window 1.25 MB）。
- 应用层每方向只持有一个 16 KiB 缓冲；读写都 `await`，不 `read_to_end`，不建无界 channel。
- TCP 内核缓冲可能自动调优到 MB 级；v0.1 不额外限制 `SO_SNDBUF`/`SO_RCVBUF`。
- 超限时：
  - 监听端：关闭新接受的 TCP，日志“该规则连接数已满”。
  - 目标端：`reset(0x04)`。

### 6.3 运行时调节

- v0.1 不调用 `Connection::set_max_concurrent_bi_streams`；保持 SDK 默认 100。
- 如未来需要 >96，先评审 SDK 固定参数/多 data link 方案，不在此 spec 内。

---

## 7. tunnel-core API

### 7.1 类型

```rust
pub struct Forward {
    pub listen_port: u16,
    pub peer_port: u16,
}

pub enum LinkState {
    Connecting,
    Connected,
    Reconnecting,
    AuthenticationFailed,
    Stopped,
}

pub enum Command {
    CreateInvite,
    JoinInvite(String),
    ProvideReply(String),
    ConfirmComparison,
    AddForward(Forward),
    CloseForward { listen_port: u16 },
    Stop,       // v0.1 CLI 不使用；保留给 TUI/GUI
    Shutdown,
}

pub enum Event {
    InviteCode(String),
    ReplyCode(String),
    ComparisonCode(String),
    Ready,
    LinkState(LinkState),
    RulesChanged,
    PeerRules(Vec<Forward>),
    RemoteClosed { listen_port: u16 },
    ListenerStarted(u16),
    ConnectionOpened { listen_port: u16 },
    ConnectionEstablished { listen_port: u16 },
    ConnectionClosed { listen_port: u16 },
    Error(TunnelError),
}
```

字段约定：

- `PeerRules` 中的 `Forward` 是**对端视角**：`listen_port` 是对端监听端口，`peer_port` 是本机目标端口。
- `RemoteClosed { listen_port }` 只在对端 `RuleList` 中该 `listen_port` 消失时发出。
- `ConnectionOpened/Established/Closed` 只针对**本机规则**（本端 listener 接受的 TCP），用于 CLI 状态展示；目标侧连接只记日志/内部计数。
- `ConnectionEstablished` 在本端表示“data stream 已打开且 6 字节头已写出”，**不是**端到端确认；v0.1 没有目标拨号成功的 ack。
- `LinkState` 映射 SDK `ManagedLinkPhase`：
  - `Connecting` ← `Connecting`
  - `Connected` ← `Connected`
  - `Reconnecting` ← `Reconnecting`
  - `AuthenticationFailed` ← `AuthenticationFailed`
  - `Stopped` ← `Stopped`

### 7.2 配对序列

创建方（creator）：

1. `Command::CreateInvite` → `Event::InviteCode(invite)`。
2. 通过可信渠道把 INVITE 给对方；收到 REPLY 后 `Command::ProvideReply(reply)`。
3. core 计算比较码 → `Event::ComparisonCode(code)`。
4. CLI 打印比较码，**不强制人工核对**，立即发送 `Command::ConfirmComparison`。
5. core 调 `confirmation().confirm(&code)`，然后 `connect_transport()` → `Event::Ready`。

加入方（joiner）：

1. `Command::JoinInvite(invite)` → `Event::ReplyCode(reply)`。
2. 把 REPLY 给创建方；core 计算比较码 → `Event::ComparisonCode(code)`。
3. 同创建方：CLI 自动 `ConfirmComparison` → `connect_transport()` → `Ready`。

约定：

- INVITE/REPLY 必须通过可信私密渠道交换；比较码只用于可选人工核对，v0.1 不阻塞。
- 配对超时/失败 → `Event::Error(TunnelError::Pairing)`；不自动重试配对。
- 配对成功后，core 立即 `manage_link()`，等数据链路 `Connected`，再允许 `AddForward` 启动 listener。

### 7.3 会话生命周期

```text
Idle
  ├─ CreateInvite → InviteCreated ──ProvideReply──┐
  └─ JoinInvite  → ReplyCreated  ────────────────┤
                                                  ▼
                                             Confirming
                                                  │ ConfirmComparison
                                                  ▼
                                             Connecting
                                                  │ data link Connected
                                                  ▼
                                               Ready
                                                  │ data link lost
                                                  ▼
                                            Reconnecting
                                              │        │
                                     Connected│        │120s / AuthFailed
                                              ▼        ▼
                                            Ready    Stopped
```

- `Ready` 之前不接受 `AddForward`（返回 `Error`），避免 listener 在无链路时启动。
- `Ready` 后 listener 常驻；即使本端没有规则，也运行 `accept_bi()` 循环，以便对端访问本端任意 loopback 端口。
- `Stop`：停止 listener、reset 活动连接、关闭 data link，保留进程/core；v0.1 CLI 不使用。
- `Shutdown`：上述清理后结束 core task；CLI 退出。

### 7.4 规则生命周期

`AddForward(Forward)`：

1. 校验端口 `1..=65535`；本机 `listen_port` 不重复。
2. `TcpListener::bind(("127.0.0.1", listen_port))`；失败 → `Error(TunnelError::Bind)`，规则不加入。
3. 加入本机规则表；启动 accept loop；`ListenerStarted(listen_port)`；`RulesChanged`。
4. 重发 `RuleList`（若状态流可用）。

`CloseForward { listen_port }`：

1. 必须是本机规则；否则 `Error(TunnelError::RuleNotFound)`。
2. 停止 listener（取消 accept loop）。
3. reset 该规则所有活动数据流和 TCP 连接。
4. 从规则表移除；`RulesChanged`。
5. 重发 `RuleList`。
6. 对端收到后发 `RemoteClosed`。

对端 `RuleList` 更新：

- 新 `listen_port` 出现 → 加入对端规则表；`PeerRules`。
- 旧 `listen_port` 消失 → 移除；`RemoteClosed { listen_port }`；`PeerRules`。
- 同一 `listen_port` 的 `peer_port` 变化 → 更新显示；不触发 `RemoteClosed`。
- 链路断开时保留最后已知对端规则，标记“可能过期”；重连后以新 `RuleList` 为准。

### 7.5 错误类型

```rust
pub enum TunnelError {
    Pairing(String),
    Network(String),
    Bind { port: u16, message: String },
    TargetDial { port: u16, message: String },
    Protocol(String),
    Timeout { operation: &'static str },
    Limit { port: u16 },
    RuleNotFound { listen_port: u16 },
    Cancelled,
    Internal(String),
}
```

- 实现 `Display` + `std::error::Error`；`Event::Error` 携带可展示消息。
- CLI 打印错误后继续命令循环；致命错误（会话丢失/AuthenticationFailed）由 CLI 决定退出或回菜单。

### 7.6 Transport seam（可测试性）

`tunnel-core` 的 bridge 不直接依赖 `quinn::Connection`，而是通过一个最小 trait：

```rust
#[async_trait::async_trait]
pub trait DataLink: Send + Sync {
    async fn open_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError>;
    async fn accept_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError>;
}

pub trait TunnelStream: Send + Unpin {
    fn split(self: Box<Self>) -> (Box<dyn TunnelRead>, Box<dyn TunnelWrite>);
}

pub trait TunnelRead: AsyncRead + Send + Unpin {
    fn stop(&mut self, code: u32) -> Result<(), TunnelError>;
}

pub trait TunnelWrite: AsyncWrite + Send + Unpin {
    fn finish(&mut self) -> Result<(), TunnelError>;
    fn reset(&mut self, code: u32) -> Result<(), TunnelError>;
}
```

- 生产实现包装 `quinn::Connection` / `SendStream` + `RecvStream`；`split` 后读写各自持有半条流，避免同一 stream 的 `&mut` 借用冲突。
- `TunnelWrite::finish` = QUIC FIN；`reset` = QUIC RST；`TunnelRead::stop` = STOP_SENDING。
- mock 实现用 `tokio::io::duplex` 或自定义内存流；M1 的 TCP echo、FIN/STOP/RST、背压、超时都用 mock。
- 签名可在实现票中微调，但 **seam 必须存在**；UI 永远看不到 quinn 类型。

---

## 8. CLI（v0.1）

### 8.1 启动

```text
$ p2p-tunnel
1) 创建连接   2) 加入连接   3) 退出
```

- 创建：打印 INVITE，等待用户粘贴 REPLY。
- 加入：提示粘贴 INVITE，打印 REPLY。
- 配对后打印比较码（信息性），不要求输入确认；自动进入命令模式。
- 不提供 `--listen`/`--target` 启动参数；规则用命令动态加。
- 支持 `--help` / `--version`；不支持配置文件。

### 8.2 命令

| 命令 | 作用 |
| --- | --- |
| `add <本机端口> <对端端口>` | 新增一条本机规则并启动 listener |
| `close <本机端口>` | 关闭自己的规则；活动连接 reset；对端提示“远程主动断开了” |
| `status` | 链路 + 本机规则 + 对端规则 + 活动连接 |
| `help` | 帮助 |
| `quit` | 优雅退出（等同 `Shutdown`） |

### 8.3 示例

```text
> add 2211 22
[+] 127.0.0.1:2211 -> 对端:22 监听中
> add 8081 80
[+] 127.0.0.1:8081 -> 对端:80 监听中
> status
链路: 已连接（managed link）
本机规则:
  本机:2211 -> 对端:22   监听中   1 个连接
  本机:8081 -> 对端:80   监听中   0 个连接
对端规则:
  对端:2211 -> 本机:22   监听中
活动连接:
  本机:2211 的连接 -> 对端:22
> close 2211
[-] 2211 已关闭（活动连接已重置）
[对端] 远程主动断开了 2211
> quit
```

### 8.4 状态与错误展示

- 链路状态变化：`连接中` / `已连接` / `重连中` / `认证失败` / `已停止`。
- 重连中：`链路重连中；活动连接会断开，新连接将在恢复后建立`。
- 对端规则过期：`对端规则：链路重连中，可能已过期`。
- 绑定失败：`[!] 无法监听 127.0.0.1:<port>：<原因>`。
- 目标拨号失败：只影响该 TCP 连接；日志/状态不报“会话失败”。
- 对端 close：`[对端] 远程主动断开了 <port>`。
- 本端 close 不存在：`[!] 没有本机规则监听 <port>`。
- 端口非法：`[!] 端口必须是 1–65535`。
- 退出码：`0` 正常；`1` 运行错误；`2` 参数错误。

### 8.5 交互约束

- 命令解析按空白切分；多余参数报用法错误，不 panic。
- `Ctrl-C` 等同 `quit`；最多等 5 秒清理，然后退出。
- 所有用户可见文案中文；日志可英文。

---

## 9. 失败与重连

### 9.1 数据链路断开

触发：`ManagedLinkPhase::Reconnecting`（QUIC closed / 拨号失败）。`AuthenticationFailed` 见 §9.2。

处理：

1. 关闭该 generation 上所有 data stream；所有本机规则的活动 TCP 连接 RST（`set_zero_linger`）。
2. **所有 listener 保持**；本机规则保留。
3. 新 TCP 连接：最多等 5 秒等 `ManagedLinkStatus.connection`；等不到 → 关闭 TCP，日志“链路重连中”。
4. CLI 显示“重连中”；对端规则标记可能过期。
5. SDK 自动重拨/重新认证（500 ms → 5 s backoff）；每次重连都重新走 mTLS PIN + 会话 HMAC。
6. 新 `Connected`：启动新的 accept loop；`bootstrap_outbound` 侧重开状态流；重发 `Hello` + `RuleList`；`LinkState(Connected)`。

### 9.2 会话丢失

- 连续 `Reconnecting` 超过 **120 秒**，或 `ManagedLinkPhase::AuthenticationFailed`：
  - 关闭所有 listener；
  - reset 所有活动连接；
  - `LinkState(AuthenticationFailed/Stopped)` + `Error`；
  - CLI 显示“会话丢失，请重新配对”。
- 不伪造无缝恢复；不自动重新配对。

### 9.3 bootstrap 断开

- bootstrap 是会话锚点；SDK 没有公开的 bootstrap 重连 API。
- 数据链路能否在 bootstrap 断开后继续/重拨，SDK 文档标注“未验证”；v0.1 必须用 SDK 环回测试验证（见 §11）。
- v0.1 **不承诺** bootstrap 断开后的全量会话恢复；如果 data link 也失效 → 按会话丢失处理。
- 若验证发现 data link 依赖 bootstrap：记录为已知限制；不在 tunnel 里复制 SDK 的 NAT/认证逻辑；必要时开 SDK ticket。

### 9.4 局部失败

- 目标拨号失败/超时（10 s）：只 reset 该 data stream（`0x02`），不影响链路和其他连接。
- 单条 data stream 协议错误：只 reset 该 stream（`0x01`）。
- 规则同步流失败：只影响“对端规则”显示；数据继续。
- 本机 listener bind 失败：只影响该规则；其他规则继续。
- `open_bi` 超时：关闭该 TCP；不触发链路重连。

### 9.5 关闭路径

- 正常 TCP 半关闭：按 §5.3；两个方向都结束后 drop stream。
- 本地 `close`：停 listener → reset 该规则连接 → 重发 `RuleList`。
- 退出：停所有 listener → reset 所有连接 → 关闭状态流 → `ManagedAuthenticatedLink::shutdown()` → 释放 SDK peer。
- SDK `ConnectedTransportPeer::shutdown(self)` 消费 `self`；core 必须先 drop 所有 `Arc` clone，再 `Arc::try_unwrap`。若 `try_unwrap` 失败，至少 `bootstrap.close()` 后退出，并记录 SDK 缺口（G4）。
- 任一时刻都不**直接**调用 `Connection::close()` 来关闭单条 TCP；会话退出由 `ManagedAuthenticatedLink::shutdown()` 负责关闭 data link。

---

## 10. 日志与审计

- `tunnel-core` 使用 `tracing` 宏；`tunnel-cli` 初始化 subscriber：日志写 stderr，stdout 只承载交互界面。
- 默认（未设置 `RUST_LOG`）只输出 `warn` 及以上；`quinn=off`、`quinn_proto=off` 保持静音。每条 TCP 的打开/关闭与审计行都是 `info` 级，默认不上屏。
- 设置 `RUST_LOG` 时完全以它为准：`RUST_LOG=info` 可临时看到每条 TCP 打开/关闭与审计行，去掉即恢复默认。
- 不提供 `--log-file`（按未知参数处理，打印用法并退出码 2），也不引入任何日志文件路径或需要清理的日志缓存。
- 连接建立成功只在会话建立时提示一次（`[+] 配对完成，已连接`）；链路已连接状态下不重复打印。
- 日志字段：`listen_port`、`peer_port`、`target_port`、`generation`、`link_phase`、`error`。
- 目标端审计日志（`info` 级，默认不上屏，`RUST_LOG=info` 可见）：
  ```text
  target_port=22 peer_session=<hash> result=ok
  target_port=3306 peer_session=<hash> result=dial_failed
  ```
  - `peer_session` 用比较码的哈希或会话内稳定标识；v0.1 没有持久设备身份，不声称能区分“同一设备的不同会话”。
- 不记录 INVITE/REPLY、会话密钥、完整证书。
- 诊断：发布前用 `TransportDiagnostic` 的 `actual_local_udp` / `actual_remote_udp` / `remote_candidate_kind` / `stun_consistency` / `gateway_mapping` / `authenticated_peer_reflexive` 作为无中继证据；截图/日志脱敏保存。
- 诊断是快照，不是事件流；不把 localhost 当 NAT 证据。

---

## 11. 测试矩阵

| 层级 | 测试 | 里程碑 | 执行 |
| --- | --- | --- | --- |
| 单元 | 6 字节头编解码；magic/version/flags/port 非法；错误码映射 | M0 | CI Linux/Windows |
| 单元 | 规则同步帧编解码；长度/count/重复端口校验 | M0 | CI |
| 单元 | 半关闭状态机；并发计数（64/96）；超时常量 | M0/M1 | CI |
| Mock 集成 | `tokio::io::duplex` 上的 TCP echo；FIN→EOF | M1 | CI |
| Mock 集成 | TCP RST→QUIC reset→对端 TCP RST；STOP→停止读 | M1 | CI |
| Mock 集成 | 慢读背压；16 KiB 缓冲有界；不无界增长 | M1 | CI |
| Mock 集成 | `open_bi` 挂起→5 s 超时；64 并发上限；96 总上限 | M1 | CI |
| Mock 集成 | 动态 add/close 多规则；close 一条不影响另一条 | M1 | CI |
| Mock 集成 | 对端 RuleList 删除→`RemoteClosed`；状态流失败→数据继续 | M1 | CI |
| 真实 SDK 环回 | 配对→bootstrap→manage_link→状态流→两条 data stream | M2 | 手工/CI |
| 真实 SDK 环回 | 双向同时；关闭一条规则不影响另一条 | M2 | 手工/CI |
| 真实 SDK 环回 | 数据链路断开→listener 保持→新连接恢复；活动连接 RST | M2/M3 | 手工/CI |
| 真实 SDK 环回 | bootstrap 关闭后 data link 是否继续/重拨（风险验证） | M2 | 手工 |
| 双机 LAN | Windows ↔ Termux；`ssh -p`；双向；端口冲突；目标 RST | M2/M3 | 手工 |
| CI | Linux x86_64 + Windows x86_64：`fmt` / `clippy -D warnings` / `test` | M0 起 | GitHub Actions |
| 交叉构建 | `aarch64-linux-android`（Termux）；`aarch64-unknown-linux-musl` 保留给静态 Linux | M0 起 | CI/手工 |
| 真实 NAT | 家庭路由器/CGNAT 一次；记录 STUN/ICE 诊断；无直连则明确失败 | M3/发布前 | 手工 |

补充：

- localhost/mock 不能替代 LAN/NAT 证据。
- Windows 重点：`127.0.0.1` 绑定、端口占用、TCP 半关闭、防火墙提示、RST。
- Termux 重点：Android/bionic、musl 静态构建或 Termux 原生 cargo、后台限制、UDP/STUN、端口占用。
- macOS/ARM64 后补，v0.1 不作为 gate。

---

## 12. 里程碑与验收

### M0：workspace + 协议骨架

- workspace 4 crate 可构建；`Cargo.lock` 提交；SDK pin `d4cdd2e`；`quinn = "0.11"`。
- `tunnel-core` 类型、6 字节头编解码、状态帧编解码、错误类型、transport trait + mock。
- 单元测试通过；GitHub Actions Linux/Windows 跑 `fmt`/`clippy`/`test`。
- `aarch64-linux-android` 交叉构建通过（PIE、TLS 段 64 字节对齐），或在 Termux 内原生构建并记录。
  Android 的 bionic 不接受 glibc 产物（TLS 对齐）也不接受非 PIE 的 musl 产物。
- **Gate**：CI 全绿；`cargo build --release` 通过；无 SDK 类型泄漏到 CLI。

### M1：mock TCP echo + 规则生命周期

- mock transport 上本机 TCP echo；FIN/STOP/RST、背压、`open_bi` 超时、64/96 限制有测试。
- 动态 `add`/`close` 多条规则；close 一条不影响另一条；对端 `RemoteClosed` 提示。
- 状态流 `Hello`/`RuleList` 快照/重发/失败降级。
- **Gate**：M1 测试全绿；Linux/Windows CI 全绿。

### M2：真实 SDK + Windows ↔ Termux（LAN）

- 真实配对、`manage_link`、状态流、双向数据流；`ssh -p <listen_port>` 到对端 `22`。
- Windows x86_64 ↔ Android Termux aarch64（LAN）跑通。
- 活动连接断开后 listener 保持、新连接恢复；活动 TCP 重置符合预期。
- **Gate**：手工记录（命令、版本、截图/日志脱敏）；无中继证据；bootstrap 关闭行为有结论。

### M3：双向/并发/失败注入/真实 NAT

- 双向同时、多规则并发、失败注入（kill 链路、目标端口拒绝、目标进程崩溃）。
- Windows ↔ Termux 双机；GitHub Actions Linux/Windows 全绿。
- 发布前完成一次真实 NAT/CGNAT 记录；记录 STUN/ICE 诊断；无直连则明确失败。
- **Gate**：上述记录齐全；无中继证据；已知限制写入 `docs/STATUS.md`。

### M4：CLI 完整

- `add`/`close`/`status`/`help`/`quit` 与 spec 一致；错误提示、重连状态、对端关闭提示完整。
- TUI/GUI 按 v0.2/v0.3，不阻塞 v0.1。
- **Gate**：CLI 手工走查通过；文档更新。

### v0.1 发布 Gate

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo test`
- [ ] `cargo build --release`
- [ ] GitHub Actions Linux x86_64 + Windows x86_64 全绿
- [ ] Windows ↔ Termux LAN 双机记录
- [ ] 真实 NAT/CGNAT 记录（发布前）
- [ ] 无中继证据（诊断 + SDK 无 relay/TURN）
- [ ] 已知限制写入 `docs/STATUS.md`

---

## 13. 实现票（已落盘）

> 已落盘：工作区 `.scratch/p2p-tunnel-v0/issues/`（01、02、03a/03b、04–10，共 11 张，带 blocking edges）。

按依赖顺序，每张票应自包含、可独立验证：

1. **01 M0 workspace 与 CI**：4 crate、SDK pin、`Cargo.lock`、fmt/clippy/test、Linux/Windows workflow、musl 构建。
2. **02 M0 协议编解码 + transport seam + mock**：6 字节头、状态帧、错误码、`DataLink`/`TunnelStream`、quinn 适配、duplex mock、超时/取消、单元测试。
3. **03a M1 listener + 单规则 TCP echo**：`AddForward`/`CloseForward`、listener、mock 目标 echo、`status`、端口校验、事件。
4. **03b M1 FIN 半关闭与有序关闭**：双向 FIN、尾部数据、`ConnectionClosed`、关闭活动规则。
5. **04 M1 关闭语义、背压与上限**：RST/STOP、16 KiB 有界缓冲、每 forward 64/每侧 96、`open_bi` 5 s 超时、错误码映射。
6. **05 M1 规则同步与动态多规则**：`Hello`/`RuleList` 快照、`RemoteClosed`、状态流降级。
7. **06 M2 SDK 配对与真实转发**：INVITE/REPLY、比较码、managed link、目标拨号、真实 SDK 环回测试、审计日志。
8. **07 M2 重连与失败注入**：`ManagedLinkStatus`、generation 切换、listener 保持、活动连接 reset、120 s 会话丢失、bootstrap 关闭验证。
9. **08 M2 平台 LAN 验收**：Windows/Termux 双机、端口冲突、RST、后台/防火墙。
10. **09 M3 真实 NAT 验收**：跨 NAT/CGNAT 一次、STUN/ICE 诊断、无中继证据。
11. **10 M4 CLI 完整与文档**：命令、状态、错误文案、退出码、`STATUS.md`/`README` 更新。

依赖关系：

```text
01 → 02 → 03a → 03b ─┬→ 04 ─┐
                      └→ 05 ─┴→ 06 → 07 ─┬→ 08 → 09
                                        └→ 10
```

---

## 14. 风险、未验证、开放问题

### 14.1 风险

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| SDK data link 在 bootstrap 关闭后能否继续/重拨未验证 | 重连承诺可能不成立 | M2 用 SDK 环回测试验证；失败则记录限制/开 SDK ticket，不复制 SDK 逻辑 |
| `open_bi()` 额度耗尽会挂起 | 新 TCP 无响应 | 96 上限 + 5 s 超时；超限关闭 TCP |
| SDK 固定 100 bidi/方向、1.25 MB 窗口、120 s idle | 高并发/大流量受限 | 64/96 限制；v0.1 不调；需要时评审多 data link |
| 没有持久身份/全量会话恢复 | bootstrap 断开后需重新配对 | v0.1 明确不承诺；文档写清 |
| Termux 不是标准 glibc Linux | bionic/PIE 限制 | `aarch64-linux-android`（NDK）或 Termux 原生构建；真机测试 |
| Windows 防火墙/loopback 差异 | 绑定/RST 行为不同 | Windows CI + 真机；M0 起覆盖 |
| 真实 NAT/CGNAT 未验证 | 发布阻断 | M3 发布前必须跑一次；无直连明确失败 |
| 审计日志没有持久设备身份 | “谁访问”只能到会话级 | 记录会话标识；文档说明限制 |
| 状态流首字节分流/重开 | 实现复杂度 | M1 用 mock 覆盖；协议固定 `0x50` vs `0x01/0x02` |
| SDK API 漂移 | 编译/行为变化 | pin `d4cdd2e` + 提交 `Cargo.lock`；升级单独评审 |

### 14.2 未验证

- SDK `manage_link` 在 bootstrap 关闭后的行为（SDK 文档明确“未验证”）。
- 真实 NAT/CGNAT、IPv6、长时稳定性。
- Termux 后台限制、musl 静态构建的实际行为。
- Windows `set_zero_linger` 的 RST 传播（需真机）。
- 真实 SSH/HTTP 大流量下的背压与性能。

### 14.3 开放问题（不阻塞 v0.1）

- TUI/GUI 布局与交互（v0.2/v0.3）。
- 应用层额外加密/端口策略（未来评审）。
- 持久设备身份/SDK 会话恢复 API（需要 SDK ticket）。
- 规则数量上限/状态帧分片（v0.1 规则数远小于 u16 帧上限）。

---

## 15. 目录、依赖、交付约定

### 15.1 依赖

```toml
[workspace.dependencies]
p2p-sdk = { git = "https://github.com/juezhong/p2p-sdk", rev = "d4cdd2ed47285248da31e54abbdc6e27f99361d2" }
quinn = "0.11"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "io-util", "sync", "time", "signal"] }
tokio-util = { version = "0.7", features = ["rt"] }  # CancellationToken
async-trait = "0.1"       # dyn DataLink
thiserror = "2"
tracing = "0.1"
tracing-subscriber = "0.3"
```

- **必须提交 `Cargo.lock`**：SDK 的 `quinn = "0.11"` 是 caret，lock 决定实际补丁版本。
- SDK 不 re-export quinn；tunnel 必须自己依赖 `quinn = "0.11"`，版本不能是 0.12。
- 本地联调可用不提交的 `.cargo/config.toml` `[patch]` 指向 `../p2p-sdk`；提交的 manifest 保持 git rev。

### 15.2 CI

- `.github/workflows/ci.yml`：Linux x86_64 + Windows x86_64 matrix。
- 步骤：`cargo fmt --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test`、`cargo build --release`。
- Termux 构建：`cargo build --target aarch64-linux-android`（NDK 工具链），或 Termux 内 `cargo build` 手工记录。
- macOS 后补。

### 15.3 分支/PR

- 在 `feat/<name>` 分支开发；不直接推 main。
- PR 中文描述；明确区分“文档规划 / 有源码 / CI 通过 / 真机验证”。
- 每个实现票结束更新 `docs/STATUS.md`；PR 附测试证据。

---

## 16. 明确不做

- `0.0.0.0` / 同网段共享。
- 任意 IP 目标、内网扫描、动态 SOCKS5。
- UDP 转发、TUN/L3。
- TURN/中继/HTTP 代理。
- 应用层额外加密、端口 allowlist、逐连接审批。
- 持久配置/设备身份/全量会话恢复。
- `close peer` / 远程关闭请求。
- 旧 Go `p2p-friend` 线协议兼容。
