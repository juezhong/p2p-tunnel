# 版本记录

发布产物、校验和与平台矩阵见 [ARTIFACTS.md](ARTIFACTS.md)；验证状态见 [STATUS.md](STATUS.md)。

| 版本 | 要点 |
| --- | --- |
| v0.1.6 | 配对阶段只认 `1`/`2`/`3`（`create`/`join` 在配对前报未知；连接后仍拒绝再次配对）；提示符按状态切换 `> ` / `INVITE> ` / `REPLY> `；连接后提示符显示连接类别 `[NAT 打洞]> `、`[同网直连]> `。 |
| v0.1.5 | 配对阶段模态化：未连接时只接受 `1`/`2`/`3`（及 `create`/`join`），其他命令提示「尚未连接」；`help` 只在已连接后可用，且不列数字别名与配对命令。 |
| v0.1.4 | 等待 INVITE/REPLY 时只接受 `P2PR-` 配对码，命令不再被误当配对码；数字菜单 `1`/`2`/`3` 仅在配对前有效，已连接后 `create`/`join` 与数字命令在 CLI 层被拒绝；`help` 随状态变化且不列数字。 |
| v0.1.3 | 已连接后禁止再次 `create`/`join`（单对端），配对提示显示连接路径类别。 |
| v0.1.2 | 配对完成提示增加路径类别摘要（`同网直连`/`NAT 打洞`/`端口映射直连`），不含地址。 |
| v0.1.1 | Windows 真实 SDK 告别用例放宽 CI 调度余量。 |
| v0.1.0 | 首个可用 CLI：配对、动态多规则、双向 TCP 转发、FIN/RST、5 秒断开退出。 |

## v0.1.0

- 交互式配对：一方创建 INVITE，另一方回 REPLY，比较码自动确认（不阻塞配对）。
- 动态多规则：`add <本机端口> <对端端口>`，双方规则实时同步（Hello + RuleList 全量快照）。
- `status` 显示链路、本机规则、对端规则、活动连接；`close` 只关自己的规则。
- 双向 TCP 转发，保留 FIN 半关闭与 RST；限 64 连接/规则、96 数据流/侧。
- 会话级告别：本端退出发 `0x03 Farewell`，对端 1 秒内结束；断链 5 秒未恢复打印「断开连接」并退出 0。

## 平台与产物

七个产物（Linux × 4、Android/Termux、Windows、macOS），命名 `p2p-tunnel-<tag>-<平台>`，另附 `SHA256SUMS`；由 `.github/workflows/build-binaries.yml` 构建，`release.yml` 在 `v*` tag 上发布。

```bash
# macOS：没有公证，首次运行前去掉 quarantine
xattr -d com.apple.quarantine ./p2p-tunnel-v0.1.3-macos-aarch64

# Termux
chmod +x p2p-tunnel-v0.1.3-android-aarch64

# 校验（资产与校验和放在同一目录）
sha256sum -c SHA256SUMS
```

## 已知限制

见 [STATUS.md](STATUS.md) 的「未验证 / 已知限制」与 README 的「限制（v0.1）」。
