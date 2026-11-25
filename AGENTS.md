# p2p-tunnel：新会话 Agent 开发入口

**必读顺序：** `AGENTS.md` → `docs/WORKFLOW.md` → `docs/STATUS.md` → `docs/ARCHITECTURE.md` → `docs/SPEC.md`，随后查看 main、分支、未合并 PR 和 CI。

## 当前状态（2026-11-25，v0.1.3）

- CLI 已发布 v0.1.0–v0.1.3；配对、动态多规则、双向 TCP 转发、Farewell 告别、5 秒断开退出、配对路径摘要、单对端限制均已实现并有测试。
- 真机验证：macOS ↔ Windows、Termux ↔ Linux、Windows ↔ Termux 跨网（两侧 NAT，显示「NAT 打洞」）。
- 实现票 `01`–`19`（含伞票 `11`/`13`）全部关闭，归档在工作区 `.scratch/p2p-tunnel-v0/archive/issues/`。
- TUI/GUI 仍是占位 crate；下一步从 `.scratch/p2p-tunnel-v0/map.md` 的 Not yet specified 拆票。

## 工作区环境

源码在 `/home/jrlc/workspace/github_projects/P2P-Proj/p2p-tunnel`；工作区根有 `env.sh`，**先 `. ./env.sh`** 才能用 cargo（工具链与缓存都在工作区 `tools/`、`.uv/`，不写系统目录）。

```bash
. ./env.sh
cd p2p-tunnel
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

本地 issue tracker 是 Markdown（无 GitHub Issues）：`.scratch/p2p-tunnel-v0/` 的 `map.md`、`issues/`、`archive/`。先读 `map.md` 找任务；关闭票据要写 `Status: done`、`Closed`、`## Resolution`，再移入 `archive/issues/`。

## 不可违反

- 所有新业务代码使用 **Rust**，Tokio；应用层 CLI/TUI/GUI **三种都必须交付**，业务逻辑共享一个 core。
- STUN/ICE/IPv6/IPv4/NAT/端口映射/Quinn/身份加密/诊断由 `juezhong/p2p-sdk` 统一实现，本仓库不得自行复制。
- **禁止**业务数据中继；不允许自动退回 TURN、HTTP 代理或服务器转发；连接无直连可用路径时明确报告失败。
- 手动配对和可选只交换元数据的信令服务器由 SDK 提供。
- 在独立 `feat/` 分支开发、通过测试后提交中文 PR；状态写入 `docs/STATUS.md`。
- 明确区分“文档规划”“有源码”“经过 CI/手工真实双机验证”，不虚报已完成功能。
- 不修改旧 Go `p2p-friend` 仓库；Go v0.16.4 仅为 Transfer 的只读参考。
- 跨仓库修改必须检查 SDK 当前真实代码、主分支与尚未合并的功能 PR。
- **行为规格先改 `docs/SPEC.md`，再改代码**；交付前同步 `docs/STATUS.md` 与 `README.md` 的口径。
- 隐私：真实 IP/端口不落日志、不进 `status`、不出现在界面；只显示路径类别摘要。

## Git 提交约定（多 agent 必须一致）

- 作者和提交者统一为 `DeepSeek Agent <agent@dsh.local>`；**不得**使用用户姓名、邮箱或用户的 GPG key。
- **禁止 GPG 签名**：仓库本地 `commit.gpgsign=false`；不要用 `-S` 或 `--gpg-sign`。
- 多 agent 用 commit body 的 `Agent: <role>` trailer 区分（如 `Agent: lead`、`Agent: implementer-01`）；author/committer 保持统一，不改身份。
- 提交信息用中文，遵循 Conventional Commits（`feat:` / `fix:` / `docs:` / `test:` / `chore:`）；一个提交只做一件事。
- 不提交 `target/`、密钥、token、本机绝对路径。
- 如果环境强制签名、身份不符或无法确认规则，**停止提交并报告**，不要改用用户身份绕过。

提交前自检：

```bash
git config --local --get user.name   # DeepSeek Agent
git config --local --get user.email  # agent@dsh.local
git config --local --get commit.gpgsign  # false
git log -1 --format='%an <%ae> | %G?'
```

## 发布

1. 合并到 `main`，确认 CI 全绿。
2. 升 `Cargo.toml`（workspace）版本，提交 `chore: 发布 vX.Y.Z`。
3. 打 tag `vX.Y.Z` 推送到 `origin`（`juezhong/p2p-tunnel`）；`release.yml` 自动构建 7 个产物并发布。
4. 版本要点补进 `docs/RELEASES.md`，验证状态补进 `docs/STATUS.md`。

## 新会话用法

用户只需说：“读取 `p2p-tunnel` 仓库状态并继续开发”，Agent 就应当按上面的顺序自主寻找任务、核实 CI、按现有里程碑推进，而不是等待用户重复整个历史。
