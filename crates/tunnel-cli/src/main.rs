//! p2p-tunnel CLI（v0.1 唯一交付的 UI）。
//!
//! 只依赖 `tunnel-core` 的公开类型：本文件不得出现 `quinn` 或 `p2p_sdk`
//! 的类型，所有网络细节留在 core 内。
//!
//! 交互与文案规格：`docs/SPEC.md` §8。

mod input;
mod output;

use input::{EditAction, InputEvent, LineEditor};
use output::Output;
use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;
use std::time::Duration;

use tokio::sync::mpsc;
use tunnel_core::{Command, Event, Forward, LinkState, Session, TunnelError};

/// 等待核心清理的上限，见 `docs/SPEC.md` §8.5。
const SHUTDOWN_WAIT: Duration = Duration::from_secs(5);

/// stdin 行通道容量：有界，读线程在通道满时阻塞（背压），不会无界增长。
const STDIN_CHANNEL: usize = 64;

const MENU: &str = "1) 创建连接   2) 加入连接   3) 退出";

const HELP: &str = "\
p2p-tunnel — 安全的 P2P TCP 端口转发

用法:
  p2p-tunnel            启动交互式 CLI
  p2p-tunnel --help     显示本帮助
  p2p-tunnel --version  显示版本

命令:
  add <本机端口> <对端端口>   新增一条本机规则并开始监听 127.0.0.1
  close <本机端口>           关闭自己的规则（活动连接会被重置）
  status                     显示链路、本机规则、对端规则、活动连接
  help                       显示命令帮助
  quit / exit                优雅退出（也可用 Ctrl-C）

说明:
  - 监听地址固定 127.0.0.1，目标固定为对端 127.0.0.1:<端口>。
  - 配对即信任：只和可信设备配对；配对后对端可访问本机任意 loopback 端口。
  - INVITE/REPLY 必须通过可信私密渠道交换。

配对阶段（未连接）只接受菜单 1/2/3；连接建立后才能使用上面的命令。
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => {}
        [arg] if arg == "-h" || arg == "--help" => {
            let mut out = Output::new(io::stdout(), false);
            out.block(HELP.trim_end());
            return ExitCode::SUCCESS;
        }
        [arg] if arg == "-V" || arg == "--version" => {
            let mut out = Output::new(io::stdout(), false);
            out.line(&format!("p2p-tunnel {}", env!("CARGO_PKG_VERSION")));
            return ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("[!] 未知参数：{}", args.join(" "));
            eprintln!("用法: p2p-tunnel [--help|--version]");
            return ExitCode::from(2);
        }
    }

    init_tracing();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("[!] 无法启动运行时：{error}");
            return ExitCode::from(1);
        }
    };

    runtime.block_on(run())
}

fn init_tracing() {
    // 日志写 stderr，stdout 只留交互界面。默认只让 warn 及以上上屏；
    // 设置 `RUST_LOG` 时完全以它为准（如 `RUST_LOG=info` 临时排查）。
    let rust_log = std::env::var("RUST_LOG").ok();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(log_filter(rust_log.as_deref()))
        .with_target(false)
        .with_writer(io::stderr)
        .try_init();
}

/// 未设置 `RUST_LOG` 时默认 `warn`；quinn 的 bootstrap 收尾噪声默认静音。
/// 解析失败时退回默认，避免一条坏环境变量让程序无法启动。
fn log_filter(rust_log: Option<&str>) -> tracing_subscriber::EnvFilter {
    // bootstrap 连接按设计在配对后关闭，quinn 会打一条 ERROR 级
    // “UDP demultiplexer shut down”，对用户是噪声；默认关掉，调试时用
    // `RUST_LOG` 打开。
    let default = || tracing_subscriber::EnvFilter::new("warn,quinn=off,quinn_proto=off");
    match rust_log {
        Some(directives) => {
            tracing_subscriber::EnvFilter::try_new(directives).unwrap_or_else(|_| default())
        }
        None => default(),
    }
}

async fn run() -> ExitCode {
    // 终端里接管 stdin：逐字符读入才能保存“敲了一半的命令”。
    // 非 tty（脚本/管道）继续走行读取，输出层也不发控制序列。
    let raw_mode = if io::stdin().is_terminal() && io::stdout().is_terminal() {
        input::RawMode::enable()
    } else {
        None
    };
    let mut out = Output::new(io::stdout(), raw_mode.is_some());

    out.line(&format!(
        "p2p-tunnel v{} — 安全的 P2P TCP 端口转发",
        env!("CARGO_PKG_VERSION")
    ));
    out.line(MENU);
    out.prompt();

    let mut session = Session::spawn();
    let commands = session.sender();

    // stdin 读取放到独立线程，主循环用 select! 同时处理事件与输入。
    // 通道有界：核心处理不过来时读线程阻塞，输入不会在内存里堆积。
    let (input_tx, mut input_rx) = mpsc::channel::<InputEvent>(STDIN_CHANNEL);
    if raw_mode.is_some() {
        input::spawn_tty_reader(input_tx);
    } else {
        std::thread::spawn(move || {
            input::forward_lines(io::stdin().lock(), &input_tx);
        });
    }
    let mut editor = LineEditor::new();

    let mut ui = UiState::default();
    loop {
        tokio::select! {
            incoming = input_rx.recv() => {
                match incoming {
                    Some(InputEvent::Line(line)) => {
                        if !handle_input(line.trim(), &commands, &mut ui, &mut out).await {
                            break;
                        }
                        out.prompt();
                    }
                    Some(InputEvent::Byte(byte)) => match editor.feed(byte) {
                        EditAction::None => {}
                        EditAction::Redraw => out.set_input(editor.text()),
                        EditAction::Submit => {
                            out.submit_line();
                            let line = editor.take_line();
                            if !handle_input(line.trim(), &commands, &mut ui, &mut out).await {
                                break;
                            }
                            out.prompt();
                        }
                        EditAction::Interrupt => {
                            out.end_prompt();
                            if !out.is_tty() {
                                out.line("");
                            }
                            out.line("收到 Ctrl-C，正在退出…");
                            break;
                        }
                        EditAction::Eof => break,
                    },
                    // stdin EOF（管道结束或 Ctrl-D）按 quit 处理。
                    Some(InputEvent::Eof) | None => break,
                }
            }
            event = session.recv() => {
                match event {
                    Some(event) => {
                        if let Some(command) = handle_event(event, &mut ui, &mut out) {
                            send(&commands, command, &mut out).await;
                        }
                        if ui.session_lost || ui.session_ended {
                            break;
                        }
                        out.prompt();
                    }
                    None => {
                        // core 未收到 Shutdown 就结束：属于运行错误。
                        if !ui.session_lost {
                            out.line("[!] core 已退出");
                            ui.session_lost = true;
                        }
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                out.end_prompt();
                if !out.is_tty() {
                    out.line("");
                }
                out.line("收到 Ctrl-C，正在退出…");
                break;
            }
        }
    }

    // 退出前把 core 已经发出但还没渲染的事件排空（例如刚提交 add 就 quit 的
    // 错误回包），避免用户看不到错误。总预算仍是 SHUTDOWN_WAIT。
    out.end_prompt();
    ui.exiting = true;
    let _ = commands.send(Command::Shutdown).await;
    let _ = tokio::time::timeout(SHUTDOWN_WAIT, async {
        while let Some(event) = session.recv().await {
            let _ = handle_event(event, &mut ui, &mut out);
        }
    })
    .await;
    let _ = tokio::time::timeout(SHUTDOWN_WAIT, session.shutdown()).await;
    out.line("已退出。");

    if ui.session_lost {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

/// 处理一行输入；返回 `false` 表示退出主循环。
async fn handle_input<W: Write>(
    line: &str,
    commands: &mpsc::Sender<Command>,
    ui: &mut UiState,
    out: &mut Output<W>,
) -> bool {
    if line.is_empty() {
        if ui.awaiting == Awaiting::Invite {
            out.line("[!] INVITE 不能为空");
        }
        return true;
    }

    let mut parts = line.split_whitespace();
    let command = parts.next().unwrap_or("");

    // `exit` 与 `quit` 等价；单字符 `q`/`?` 不是别名。
    // `3` 只在配对前作为菜单退出；配对后数字一律不支持。
    if matches!(command, "quit" | "exit") || (command == "3" && !ui.ready_seen) {
        return false;
    }

    // 配对阶段（未连接）是模态的：只能选择 1/2/3，或粘贴合法配对码。
    // `add`/`close`/`status`/`help` 等命令在连接建立前不接受。
    if !ui.ready_seen {
        if ui.awaiting != Awaiting::None {
            // 只有 `P2PR-` 开头的配对码才送出；命令不会被误当配对码，
            // 避免 `add` 之类的输入触发一次失败配对（v0.1.3 真机缺陷）。
            if !line.starts_with("P2PR-") {
                out.line("[!] 无效的配对码：应以 P2PR- 开头；输入 quit 退出");
                return true;
            }
            match std::mem::replace(&mut ui.awaiting, Awaiting::None) {
                Awaiting::Reply => {
                    send(commands, Command::ProvideReply(line.to_string()), out).await
                }
                Awaiting::Invite => {
                    send(commands, Command::JoinInvite(line.to_string()), out).await
                }
                Awaiting::None => unreachable!("上面已确认存在等待状态"),
            }
            return true;
        }
        match command {
            "1" | "create" => send(commands, Command::CreateInvite, out).await,
            "2" | "join" => {
                ui.awaiting = Awaiting::Invite;
                out.line("粘贴对方的 INVITE：");
            }
            _ => {
                out.line("[!] 尚未连接：配对阶段只能输入 1/2（创建/加入）或 3（退出）");
            }
        }
        return true;
    }

    match command {
        "add" => handle_add(parts.collect(), commands, ui, out).await,
        "close" => handle_close(parts.collect(), commands, ui, out).await,
        "status" => print_status(ui, out),
        "help" => {
            out.block(HELP.trim_end());
            out.line("配对已完成；要重新配对请退出后重新启动。");
        }
        // 已连接后 `create`/`join` 没有意义（单对端会话）。
        "create" | "join" => out.line(PAIR_WHILE_CONNECTED),
        // 数字菜单只在配对前有效；这里的 1/2/3 一律未知命令。
        other => {
            out.line(&format!("[!] 未知命令：{other}（输入 help 查看帮助）"));
        }
    }
    true
}

async fn handle_add<W: Write>(
    args: Vec<&str>,
    commands: &mpsc::Sender<Command>,
    ui: &mut UiState,
    out: &mut Output<W>,
) {
    if args.len() != 2 {
        out.line("[!] 用法：add <本机端口> <对端端口>");
        return;
    }
    let (listen_port, peer_port) = match (parse_port(args[0]), parse_port(args[1])) {
        (Some(listen_port), Some(peer_port)) => (listen_port, peer_port),
        _ => {
            out.line("[!] 端口必须是 1–65535");
            return;
        }
    };
    match Forward::new(listen_port, peer_port) {
        Ok(forward) => {
            ui.pending_add = Some(forward);
            send(commands, Command::AddForward(forward), out).await;
        }
        Err(error) => out.line(&format!("[!] {error}")),
    }
}

async fn handle_close<W: Write>(
    args: Vec<&str>,
    commands: &mpsc::Sender<Command>,
    ui: &mut UiState,
    out: &mut Output<W>,
) {
    if args.len() != 1 {
        out.line("[!] 用法：close <本机端口>");
        return;
    }
    let Some(listen_port) = parse_port(args[0]) else {
        out.line("[!] 端口必须是 1–65535");
        return;
    };
    if !ui.local.iter().any(|rule| rule.listen_port == listen_port) {
        out.line(&format!("[!] 没有本机规则监听 {listen_port}"));
        return;
    }
    ui.pending_close = ui
        .local
        .iter()
        .find(|rule| rule.listen_port == listen_port)
        .copied();
    ui.local.retain(|rule| rule.listen_port != listen_port);
    ui.active.remove(&listen_port);
    send(commands, Command::CloseForward { listen_port }, out).await;
}

fn parse_port(raw: &str) -> Option<u16> {
    raw.parse::<u16>().ok().filter(|port| *port != 0)
}

/// 对端没有可展示的主机名：统一叫“对端”，本机端口显式加“本机:”前缀。
const PEER_LABEL: &str = "对端";

/// `status` 的完整布局（`docs/SPEC.md` §8.3）。
fn format_status(ui: &UiState) -> Vec<String> {
    let mut lines = Vec::new();
    // 未配对时如实显示未连接；不从“没有事件”猜成连接中。
    lines.push(match ui.link {
        Some(state) => format!("链路: {}", link_label(state)),
        None => "链路: 未连接".to_string(),
    });
    if ui.link == Some(LinkState::Reconnecting) {
        lines.push("链路重连中；活动连接会断开，新连接将在恢复后建立".to_string());
    }

    lines.push("本机规则:".to_string());
    if ui.local.is_empty() {
        lines.push("  （无）".to_string());
    } else {
        for rule in &ui.local {
            let count = ui.active.get(&rule.listen_port).copied().unwrap_or(0);
            lines.push(format!(
                "  本机:{} -> {PEER_LABEL}:{}   监听中   {} 个连接",
                rule.listen_port, rule.peer_port, count
            ));
        }
    }

    lines.push("对端规则:".to_string());
    if ui.peer.is_empty() {
        lines.push("  （无）".to_string());
    } else {
        for rule in &ui.peer {
            lines.push(format!(
                "  {PEER_LABEL}:{} -> 本机:{}   监听中",
                rule.listen_port, rule.peer_port
            ));
        }
    }
    if ui.peer_stale {
        lines.push("  对端规则：链路重连中，可能已过期".to_string());
    } else if ui.peer_unknown {
        lines.push("  对端规则：状态同步流不可用，规则未知".to_string());
    }

    lines.push("活动连接:".to_string());
    if ui.active.is_empty() {
        lines.push("  （无）".to_string());
    } else {
        let mut ports: Vec<_> = ui.active.iter().collect();
        ports.sort_by_key(|(port, _)| **port);
        for (listen_port, count) in ports {
            // 每条活动连接一行；对端端口来自本机规则镜像。
            let peer_port = ui
                .local
                .iter()
                .find(|rule| rule.listen_port == *listen_port)
                .map(|rule| rule.peer_port.to_string())
                .unwrap_or_else(|| "?".to_string());
            for _ in 0..*count {
                lines.push(format!(
                    "  本机:{listen_port} 的连接 -> {PEER_LABEL}:{peer_port}"
                ));
            }
        }
    }
    lines
}

fn print_status<W: Write>(ui: &UiState, out: &mut Output<W>) {
    for line in format_status(ui) {
        out.line(&line);
    }
}

/// 链路文案；已连接时带 SPEC §8.3 的 managed link 说明。
fn link_label(state: LinkState) -> &'static str {
    match state {
        LinkState::Connected => "已连接（managed link）",
        other => other.label(),
    }
}

fn handle_event<W: Write>(event: Event, ui: &mut UiState, out: &mut Output<W>) -> Option<Command> {
    match event {
        Event::InviteCode(code) => {
            out.line("INVITE（通过可信渠道发给对方）：");
            out.line(&code);
            ui.awaiting = Awaiting::Reply;
            out.line("粘贴对方的 REPLY：");
        }
        Event::ReplyCode(code) => {
            out.line("REPLY（通过可信渠道发给对方）：");
            out.line(&code);
        }
        Event::ComparisonCode(code) => {
            // v0.1 不阻塞人工核对：打印比较码后自动确认，配对立即继续。
            out.line(&format!("比较码：{code}（已自动确认；v0.1 不阻塞配对）"));
            return Some(Command::ConfirmComparison);
        }
        Event::Connectivity(connectivity) => {
            ui.path = Some(connectivity.label);
        }
        Event::Ready => {
            // core 只在配对完成时发一次 Ready；这里再防一层重复，避免
            // 未来事件源变化时把“配对完成”重复打印成重连提示。
            if !ui.ready_seen {
                ui.ready_seen = true;
                let suffix = ui
                    .path
                    .map(|label| format!("（{label}）"))
                    .unwrap_or_default();
                out.line(&format!("[+] 配对完成，已连接{suffix}"));
                out.line("现在可用 add <本机端口> <对端端口> 新增规则，或输入 help 查看命令。");
            }
        }
        Event::PeerFarewell => {
            // SPEC §9.5：对端主动退出，本端不再重连，清理后以退出码 0 结束。
            out.line("[对端] 对端已退出");
            ui.peer_stale = false;
            ui.session_ended = true;
        }
        Event::LinkState(state) => {
            ui.link = Some(state);
            ui.peer_stale = state == LinkState::Reconnecting;
            ui.peer_unknown = false;
            // 正常退出时 core 也会发 Stopped；这是收尾信号，不是状态变化。
            if !(ui.exiting && state == LinkState::Stopped) {
                out.line(&format!("链路: {}", link_label(state)));
            }
        }
        Event::RulesChanged => {
            // close 成功没有单独事件：RulesChanged 到达时确认关闭结果。
            if let Some(closed) = ui.pending_close.take() {
                out.line(&format!(
                    "[-] {} 已关闭（活动连接已重置）",
                    closed.listen_port
                ));
            }
        }
        Event::PeerRules(rules) => {
            ui.peer = rules;
            ui.peer_stale = false;
            ui.peer_unknown = false;
            print_status(ui, out);
        }
        Event::PeerRulesUnknown => {
            ui.peer_unknown = true;
            out.line("[!] 规则同步状态流不可用，对端规则未知（数据面不受影响）");
        }
        Event::PeerRulesStale => {
            ui.peer_stale = true;
            out.line("[!] 对端规则：链路重连中，可能已过期");
        }
        Event::Disconnected => {
            // SPEC §9.2：重连 5 秒未恢复按断开连接收尾，退出码 0。
            out.line("[!] 断开连接：链路重连 5 秒未恢复，会话结束");
            ui.peer_stale = false;
            ui.session_ended = true;
        }
        Event::RemoteClosed { listen_port } => {
            out.line(&format!("[对端] 远程主动断开了 {listen_port}"));
        }
        Event::ListenerStarted(listen_port) => {
            if let Some(pending) = ui.pending_add.take() {
                if pending.listen_port == listen_port {
                    ui.local.push(pending);
                    out.line(&format!(
                        "[+] 127.0.0.1:{} -> 对端:{} 监听中",
                        pending.listen_port, pending.peer_port
                    ));
                }
            } else {
                out.line(&format!("[+] 127.0.0.1:{listen_port} 监听中"));
            }
        }
        Event::ConnectionOpened { listen_port } => {
            *ui.active.entry(listen_port).or_insert(0) += 1;
        }
        Event::ConnectionEstablished { .. } => {
            // 不逐条打印：连接数在 `status` 的活动连接里看。
        }
        Event::ConnectionClosed { listen_port } => {
            if let Some(count) = ui.active.get_mut(&listen_port) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    ui.active.remove(&listen_port);
                }
            }
        }
        Event::Error(error) => {
            if error == TunnelError::SessionLost {
                // 会话已丢失：对端规则不再可信，禁止继续展示旧快照。
                ui.peer.clear();
                ui.peer_stale = false;
                ui.peer_unknown = false;
                ui.session_lost = true;
            }
            ui.pending_add = None;
            if let Some(closed) = ui.pending_close.take() {
                if !ui
                    .local
                    .iter()
                    .any(|rule| rule.listen_port == closed.listen_port)
                {
                    ui.local.push(closed);
                }
            }
            out.line(&format!("[!] {error}"));
        }
    }

    None
}

async fn send<W: Write>(commands: &mpsc::Sender<Command>, command: Command, out: &mut Output<W>) {
    if commands.send(command).await.is_err() {
        out.line("[!] core 已退出");
    }
}

/// 已连接时再次发起配对的统一文案（`docs/SPEC.md` §8.2）。
const PAIR_WHILE_CONNECTED: &str = "[!] 已连接，不能再次配对；请先退出后重新启动";

/// 等待用户粘贴配对码的交互状态。
#[derive(Default, PartialEq, Eq)]
enum Awaiting {
    #[default]
    None,
    Invite,
    Reply,
}

/// CLI 侧的状态镜像；真实状态由 core 的事件驱动。
#[derive(Default)]
struct UiState {
    link: Option<LinkState>,
    local: Vec<Forward>,
    peer: Vec<Forward>,
    peer_stale: bool,
    /// 规则同步状态流断开：对端规则暂时未知（`docs/SPEC.md` §5.4）。
    peer_unknown: bool,
    active: std::collections::HashMap<u16, usize>,
    pending_add: Option<Forward>,
    pending_close: Option<Forward>,
    awaiting: Awaiting,
    /// core 报告会话丢失（或 core 意外退出）：进程以退出码 1 结束。
    session_lost: bool,
    /// 会话正常结束（对端退出或断链超时）：清理后以退出码 0 结束。
    session_ended: bool,
    /// 已进入退出排空阶段：抑制 core 的收尾 Stopped 状态输出。
    exiting: bool,
    /// 已打印过“配对完成”：重连只更新链路状态，不重复打印。
    ready_seen: bool,
    /// 连接路径摘要（不含地址）：随「配对完成」打印一次。
    path: Option<&'static str>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn connected_ui() -> UiState {
        let mut ui = UiState {
            link: Some(LinkState::Connected),
            ..UiState::default()
        };
        ui.local.push(Forward {
            listen_port: 2211,
            peer_port: 22,
        });
        ui.local.push(Forward {
            listen_port: 8081,
            peer_port: 80,
        });
        ui.peer.push(Forward {
            listen_port: 2211,
            peer_port: 22,
        });
        ui.active.insert(2211, 1);
        ui
    }

    #[test]
    fn status_layout_matches_spec_section_8_3() {
        let lines = format_status(&connected_ui());
        assert_eq!(lines[0], "链路: 已连接（managed link）");
        assert!(
            lines.contains(&"  本机:2211 -> 对端:22   监听中   1 个连接".to_string()),
            "{lines:?}"
        );
        assert!(
            lines.contains(&"  本机:8081 -> 对端:80   监听中   0 个连接".to_string()),
            "{lines:?}"
        );
        assert!(
            lines.contains(&"  对端:2211 -> 本机:22   监听中".to_string()),
            "{lines:?}"
        );
        assert!(
            lines.contains(&"  本机:2211 的连接 -> 对端:22".to_string()),
            "{lines:?}"
        );
    }

    #[test]
    fn status_link_forms_are_exact() {
        let mut ui = UiState::default();
        assert_eq!(format_status(&ui)[0], "链路: 未连接");
        ui.link = Some(LinkState::Reconnecting);
        let lines = format_status(&ui);
        assert_eq!(lines[0], "链路: 重连中");
        assert!(lines.contains(&"链路重连中；活动连接会断开，新连接将在恢复后建立".to_string()));
        for (state, text) in [
            (LinkState::Connecting, "链路: 连接中"),
            (LinkState::AuthenticationFailed, "链路: 认证失败"),
            (LinkState::Stopped, "链路: 已停止"),
        ] {
            ui.link = Some(state);
            assert_eq!(format_status(&ui)[0], text);
        }
    }

    #[tokio::test]
    async fn exit_is_equivalent_to_quit() {
        let (tx, _rx) = mpsc::channel(4);
        let mut ui = UiState::default();
        let mut out = Output::new(Vec::new(), false);
        assert!(
            !handle_input("exit", &tx, &mut ui, &mut out).await,
            "exit 应与 quit 一样结束主循环"
        );
    }

    #[tokio::test]
    async fn single_char_aliases_remain_unknown_commands() {
        let (tx, mut rx) = mpsc::channel(4);

        // 配对阶段是模态的：q/? 不产生 core 命令，只提示只能输入 1/2/3。
        let mut ui = UiState::default();
        let mut out = Output::new(Vec::new(), false);
        for alias in ["q", "?"] {
            assert!(handle_input(alias, &tx, &mut ui, &mut out).await);
            assert!(rx.try_recv().is_err(), "未知命令不应产生 core 命令");
        }
        let text = String::from_utf8(out.into_writer()).expect("输出应为 UTF-8");
        assert!(
            text.contains("配对阶段只能输入 1/2（创建/加入）或 3（退出）"),
            "{text}"
        );

        // 已连接后 q/? 仍是未知命令，不是退出别名。
        let mut ui = UiState {
            ready_seen: true,
            ..UiState::default()
        };
        let mut out = Output::new(Vec::new(), false);
        for alias in ["q", "?"] {
            assert!(handle_input(alias, &tx, &mut ui, &mut out).await);
        }
        let text = String::from_utf8(out.into_writer()).expect("输出应为 UTF-8");
        assert!(
            text.contains("[!] 未知命令：q（输入 help 查看帮助）"),
            "{text}"
        );
        assert!(
            text.contains("[!] 未知命令：?（输入 help 查看帮助）"),
            "{text}"
        );
    }

    /// 收集日志输出的内存 writer，实现 `MakeWriter` 以便测试 CLI 的过滤规则。
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Capture {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("日志缓冲锁")).into_owned()
        }
    }

    impl Write for Capture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("日志缓冲锁").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Capture;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// 用 CLI 自己的过滤规则（`log_filter`）渲染一段日志，返回 stderr 文本。
    fn capture_logs(rust_log: Option<&str>, emit: impl FnOnce()) -> String {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(log_filter(rust_log))
            .with_target(false)
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, emit);
        capture.text()
    }

    // 注意：每个测试使用自己内联的 tracing 宏调用点，避免 tracing 的全局
    // callsite 兴趣缓存被另一个测试的过滤器污染（同一进程并行跑测试）。

    #[test]
    fn unset_rust_log_defaults_to_warn_and_silences_quinn() {
        let output = capture_logs(None, || {
            tracing::info!(
                target: "tunnel_core::bridge",
                target_port = 22,
                peer_session = "session-abc",
                result = "ok",
                "目标端口连接已建立"
            );
            tracing::error!(target: "quinn", "UDP demultiplexer shut down");
            tracing::error!(target: "quinn_proto", "connection closed");
            tracing::warn!(target: "tunnel_core::session", "需要用户关心的告警");
        });
        assert!(
            !output.contains("目标端口连接已建立"),
            "未设置 RUST_LOG 时 info 审计行不应上屏：\n{output}"
        );
        assert!(
            !output.contains("demultiplexer"),
            "quinn 噪声默认静音：\n{output}"
        );
        assert!(
            !output.contains("connection closed"),
            "quinn_proto 噪声默认静音：\n{output}"
        );
        assert!(
            output.contains("需要用户关心的告警"),
            "未设置 RUST_LOG 时 warn 仍应可见：\n{output}"
        );
    }

    #[test]
    fn rust_log_info_reveals_audit_line_and_fields() {
        let output = capture_logs(Some("info"), || {
            tracing::info!(
                target: "tunnel_core::bridge",
                target_port = 22,
                peer_session = "session-abc",
                result = "ok",
                "目标端口连接已建立"
            );
        });
        assert!(
            output.contains("目标端口连接已建立"),
            "RUST_LOG=info 时应显示审计行：\n{output}"
        );
        assert!(output.contains("target_port=22"), "{output}");
        assert!(output.contains("peer_session=\"session-abc\""), "{output}");
        assert!(output.contains("result=\"ok\""), "{output}");
    }
}
