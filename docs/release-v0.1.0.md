# v0.1.0 — 首个可用的 CLI 版本

p2p-tunnel 用 SDK 配对建立端到端加密的 QUIC 直连，把本机 `127.0.0.1` 端口转发到对端 `127.0.0.1` 端口。v0.1 只交付 CLI，不做中继。

## 能力

- 交互式配对：一方创建 INVITE，另一方回 REPLY，双方比较码一致后自动进入命令模式。
- 动态多规则：`add <本机端口> <对端端口>`，双方规则实时同步（Hello + RuleList 全量快照）。
- `status` 显示链路、本机规则、对端规则、活动连接；`close` 只关自己的规则。
- 双向 TCP 转发，保留 FIN 半关闭与 RST：目标侧重置会传到客户端，客户端断开也会反映到目标侧。
- 并发上限 64 连接/规则、96 数据流/侧；16 KiB 有界缓冲背压；5 秒建流、10 秒拨号超时。
- 数据链路断开自动重连；重连 120 秒仍失败或认证失败则结束会话并提示重新配对。
- 一端 `quit`/Ctrl-C 干净退出，最多等 5 秒清理。

## 平台与产物

| 产物 | 平台 |
| --- | --- |
| `p2p-tunnel-windows-x86_64` | Windows 10/11 x86_64 |
| `p2p-tunnel-macos-aarch64` | macOS Apple Silicon |
| `p2p-tunnel-linux-x86_64-gnu` | Linux x86_64（glibc） |
| `p2p-tunnel-linux-aarch64-gnu` | Linux aarch64（glibc） |
| `p2p-tunnel-linux-x86_64-musl` | Linux x86_64（静态） |
| `p2p-tunnel-linux-aarch64-musl` | Linux aarch64（静态） |
| `p2p-tunnel-android-aarch64` | Android / Termux aarch64（NDK r28、bionic、PIE） |

发布资产按 `p2p-tunnel-v0.1.0-<平台>` 命名：六个 Unix 可执行文件、一个 `.exe`，外加汇总的 `SHA256SUMS` 附件。

## 安装

```bash
# macOS：没有公证，首次运行前去掉 quarantine
xattr -d com.apple.quarantine ./p2p-tunnel-v0.1.0-macos-aarch64

# Termux
chmod +x p2p-tunnel-v0.1.0-android-aarch64

# 校验（资产与校验和放在同一目录）
sha256sum -c SHA256SUMS
```

## 快速使用

```text
A（创建方）                     B（加入方）
> 1                            > 2
把 INVITE 发给 B  →            粘贴 INVITE，得到 REPLY
粘贴 REPLY        ←            把 REPLY 发回 A
配对完成，两端显示比较码

> add 2211 22                  # 本机 127.0.0.1:2211 → 对端 127.0.0.1:22
[+] 127.0.0.1:2211 -> 对端:22 监听中
ssh -p 2211 user@127.0.0.1     # 在 A 上连
```

## 已验证

- 本地：`cargo fmt`、`clippy -D warnings`、`cargo test --workspace` 111 项通过。
- CI：Linux/Windows 测试 + 7 个平台产物构建全绿，aarch64-musl 经 qemu 真实执行。
- 真机：macOS ↔ Windows 配对与规则同步；Termux ↔ Linux 真实转发。

## 未验证 / 已知限制

- Windows ↔ Termux 双机验收清单（真实 `ssh -p`、端口冲突、RST、后台限制、防火墙）未完成。
- 真实 NAT/CGNAT 跨网、无中继证据未完成。
- 一端退出后，另一端会进入重连并等待会话超时；会话级告别将在下一个版本实现。
- 只支持 `127.0.0.1` 监听与对端 loopback 目标；不支持 SOCKS5、UDP、`0.0.0.0`。
- 配对即信任：对端可访问本机任意 loopback 端口；只与可信设备配对，INVITE/REPLY 必须私密交换。

## 反馈

问题与复现步骤请开 [issue](https://github.com/juezhong/p2p-tunnel/issues)。详细设计见 [docs/SPEC.md](docs/SPEC.md)，验证状态见 [docs/STATUS.md](docs/STATUS.md)。
