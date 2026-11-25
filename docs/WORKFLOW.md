# p2p-tunnel 开发工作流

1. 读 `AGENTS.md`、`docs/STATUS.md`、`docs/ARCHITECTURE.md`、`docs/SPEC.md`。
2. 检查 main HEAD、分支、未合并 PR 与 GitHub Actions；对未合并代码明确分支，不把 PR 里的实现误认为 main 已具备。
3. 任务来源：工作区 `.scratch/p2p-tunnel-v0/map.md`（本地 wayfinder tracker），按 `issues/` 的 frontier 取票；认领后把票移出 blocked 状态并写进度。
4. 行为改动先改 `docs/SPEC.md` 对应章节，再改实现；在 `feat/` 分支增量开发，跑：
   `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`。
5. 构建产物命名遵循 `docs/ARTIFACTS.md`（target triple，不用 runner 镜像名）。
6. 按真实网络与系统平台验证，不把 localhost 当成 NAT 成功；记录脱敏日志，真实 IP/端口不落盘。
7. 中文 PR 写出变更范围、测试、阻塞、下一项任务，同步 `docs/STATUS.md` 与 `README.md` 的口径。
8. 提交前按 `AGENTS.md` 的「Git 提交约定」自检：作者/提交者 `DeepSeek Agent <agent@dsh.local>`，禁 GPG，多 agent 在 commit body 写 `Agent: <role>`。

## 复制到新会话的简短指令

> 读取 `p2p-tunnel` 的 AGENTS.md / docs/WORKFLOW.md / docs/STATUS.md，先 `. ./env.sh`，检查 main、未合并 PR 和 CI，再到 `.scratch/p2p-tunnel-v0/map.md` 找当前 frontier 票。继续第一个未完成项，用独立分支提交中文 PR 并维护状态文件。不要复制 SDK 穿透实现，也不要把未经测试的功能标记为完成。
