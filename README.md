# p2p-tunnel

安全的 P2P TCP 端口转发（SSH / Web）：在本机 `127.0.0.1` 监听，经 SDK 配对与端到端加密的 QUIC 直连，转发到对端 `127.0.0.1` 的任意端口。

> **状态：v0.1.6 已发布**。真机验证覆盖 macOS ↔ Windows、Termux ↔ Linux，以及 Windows ↔ Termux 跨网（两侧 NAT，显示「NAT 打洞」）。TUI/GUI 仍是占位 crate，v0.2/v0.3 交付。
> 哪些已验证、哪些没有，以 [docs/STATUS.md](docs/STATUS.md) 为准。

## 下载

从 [Releases](https://github.com/juezhong/p2p-tunnel/releases/latest) 下载对应产物，解压后直接运行：

| 平台 | 发布资产 |
| --- | --- |
| Windows x86_64 | `p2p-tunnel-v0.1.6-windows-x86_64.exe` |
| macOS Apple Silicon | `p2p-tunnel-v0.1.6-macos-aarch64` |
| Linux x86_64（glibc） | `p2p-tunnel-v0.1.6-linux-x86_64-gnu` |
| Linux aarch64（glibc） | `p2p-tunnel-v0.1.6-linux-aarch64-gnu` |
| Linux x86_64（静态） | `p2p-tunnel-v0.1.6-linux-x86_64-musl` |
| Linux aarch64（静态） | `p2p-tunnel-v0.1.6-linux-aarch64-musl` |
| Android / Termux aarch64 | `p2p-tunnel-v0.1.6-android-aarch64` |

```bash
# Termux：Android 只接受 PIE + bionic，必须用 android 资产
chmod +x p2p-tunnel-v0.1.6-android-aarch64
./p2p-tunnel-v0.1.6-android-aarch64 --version

# macOS：ad-hoc 签名没有公证，首次运行前去掉 quarantine
xattr -d com.apple.quarantine ./p2p-tunnel-v0.1.6-macos-aarch64
```

产物命名规则、校验和与 CI 细节见 [docs/ARTIFACTS.md](docs/ARTIFACTS.md)。

## 从源码构建

```bash
cargo build --release -p tunnel-cli
# 二进制：target/release/p2p-tunnel
```

两端各启动一个进程：

```text
# A（创建方）
$ target/release/p2p-tunnel
1) 创建连接   2) 加入连接   3) 退出
> 1
INVITE（通过可信渠道发给对方）：
P2PR-INV2-...
粘贴对方的 REPLY：

# B（加入方）
$ target/release/p2p-tunnel
> 2
粘贴对方的 INVITE：
P2PR-INV2-...        # 粘贴 A 的 INVITE，回车
REPLY（通过可信渠道发给对方）：
P2PR-REP2-...        # 通过可信渠道发回给 A

# A 粘贴 REPLY；两端显示比较码并自动确认（v0.1 不阻塞）
比较码：862737（已自动确认；v0.1 不阻塞配对）
[+] 配对完成，已连接（NAT 打洞）
```

括号里的连接方式只在配对完成时打印一次：`同网直连` / `NAT 打洞` / `端口映射直连` / `直连`。真实 IP 与端口不会出现在界面或日志里。

配对后加规则并验证：

```text
> add 2211 22        # 本机 127.0.0.1:2211 -> 对端 127.0.0.1:22
[+] 127.0.0.1:2211 -> 对端:22 监听中

# 另一个终端
ssh -p 2211 user@127.0.0.1

> status             # 链路 / 本机规则 / 对端规则 / 活动连接
> close 2211         # 只关自己的规则；对端会看到“远程主动断开了 2211”
> quit               # 或 exit / Ctrl-C；最多等 5 秒清理
```

一次配对只对应一个对端：已连接后再输入 `create`/`join` 会被拒绝，需要退出后重新启动。

## 命令

| 命令 | 作用 |
| --- | --- |
| `add <本机端口> <对端端口>` | 新增一条本机规则并启动 `127.0.0.1` 监听 |
| `close <本机端口>` | 关闭自己的规则；该规则的活动连接被重置 |
| `status` | 显示链路、本机规则、对端规则、活动连接 |
| `help` | 帮助 |
| `quit` / `exit` | 优雅退出（等同 `Shutdown`；也可 Ctrl-C） |
| `1` / `2` / `3` | 配对前菜单：创建 / 加入 / 退出 |

配对阶段（未连接）只接受 `1`/`2`/`3`；`add`/`close`/`status`/`help`/`create`/`join` 在连接建立前不接受。连接建立后 `1`/`2`/`3` 按未知命令处理，`create`/`join` 被拒绝。

提示符反映当前状态：`> `（菜单）、`INVITE> `（等 INVITE）、`REPLY> `（等 REPLY）、`[NAT 打洞]> ` 或 `[同网直连]> `（已连接）。

命令行参数只有 `--help` / `--version`；其余参数按用法错误处理，退出码 `2`。退出码约定：`0` 正常（含对端主动退出、链路断开 5 秒未恢复）、`1` 运行错误（含认证失败导致的会话丢失）、`2` 参数错误。

## 限制（v0.1）

- 只支持 `127.0.0.1` 监听、目标为对端 `127.0.0.1`；不支持任意 IP、SOCKS5、UDP、`0.0.0.0` 共享。
- **不做任何中继**：没有直连路径就失败，不自动退回 TURN/HTTP 代理。
- 只交付 CLI。TUI、GUI 是占位 crate，尚未实现。
- Windows 没有 console 输入实现：清行重绘与半行恢复只在 unix raw mode 下成立，Windows 走普通行模式。
- 数据链路断开时 SDK 自动重连；5 秒内恢复继续原会话，5 秒未恢复打印「断开连接」并退出 `0`。重连期间对端规则可能过期。
- 已知边界未逐项留档：本机端口冲突、目标拒绝/RST、Termux 后台限制、Windows 防火墙行为（见 [docs/STATUS.md](docs/STATUS.md)）。
- 配对即信任：对端可访问本机任意 loopback 端口，只与可信设备配对；INVITE/REPLY 必须走私密渠道。
- v0.1 不提供持久设备身份，也不承诺 bootstrap 中断后的全量会话恢复。

## Windows 端口转发助手（port_forwarding.bat）

`port_forwarding.bat` 是 `netsh interface portproxy` 的菜单式包装：配置「本机监听端口 → 目标 IP:端口」的转发规则，不用手敲 netsh 参数。双击运行，或在 `cmd` 窗口里执行：

| 命令 | 作用 | 需要管理员 |
| --- | --- | --- |
| `port_forwarding.bat` | 打开交互菜单 | 是（自动弹 UAC） |
| `port_forwarding.bat -l` / `--list` | 列出已有转发规则 | 否 |
| `port_forwarding.bat -p` / `--ports` | 列出本机 TCP 监听端口 | 否 |
| `port_forwarding.bat -h` / `--help` | 显示帮助 | 否 |

菜单：`1` 添加、`2` 删除（列出序号后选）、`3` 只看规则、`4` 查看监听端口（分页，`n`/`p` 或 `j`/`k` 翻页、`q` 返回）、`q` 退出。

添加分四步，任意一步输 `q` 回主菜单：

1. 监听 IP：本机用哪个地址接连接，直接回车 = `0.0.0.0`（所有网卡）
2. 监听端口：本机对外开放的端口
3. 目标 IP：本机收到的连接转发到哪台机器
4. 目标端口：那台机器上服务的端口

方向是单向的：外部连到本机 `监听IP:监听端口`，本机主动连 `目标IP:目标端口`。目标机器不用装东西，但本机要能连通它。规则写进注册表，重启仍生效，删的时候用菜单 `2`。

注意：

- 添加/删除要管理员权限；`-l`、`-p`、`-h` 不提权，方便快速查看。
- 监听端口已被别的程序占用会被拦下，先用 `-p` 看谁占着。
- 文件是 GBK + CRLF；要改成 UTF-8 必须是「无 BOM + CRLF + `@echo off` 后加 `chcp 65001 >nul`」。
- 脚本不依赖 `p2p-tunnel`，只是把 Windows 转发规则配好，用来验证两台机器之间的连通性。

## 文档

- [docs/SPEC.md](docs/SPEC.md)：协议、状态机、CLI 文案与验收 gate（实现规格）。
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)：实际模块布局与数据流。
- [docs/STATUS.md](docs/STATUS.md)：已验证 / 未验证证据表与下一步。
- [docs/RELEASES.md](docs/RELEASES.md)：版本记录与各版变更。
- [docs/ARTIFACTS.md](docs/ARTIFACTS.md)：产物命名、平台矩阵与构建/冒烟规则。
- [AGENTS.md](AGENTS.md)、[docs/WORKFLOW.md](docs/WORKFLOW.md)：开发与提交约定。
