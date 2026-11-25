//! p2p-tunnel CLI（v0.1 唯一交付的 UI）。
//!
//! 只依赖 `tunnel-core` 的公开类型：本文件不得出现 `quinn` 或 `p2p_sdk`
//! 的类型，所有网络细节留在 core 内。
//!
//! 交互与文案规格：`docs/SPEC.md` §8。

use std::io::{self, BufRead, Write};
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

启动后可用命令:
  add <本机端口> <对端端口>   新增一条本机规则并开始监听 127.0.0.1
  close <本机端口>           关闭自己的规则（活动连接会被重置）
  status                     显示链路、本机规则、对端规则、活动连接
  help                       显示命令帮助
  quit                       优雅退出（也可用 Ctrl-C）
  1 / create                 创建连接（生成 INVITE）
  2 / join                   加入连接（粘贴对方的 INVITE）

说明:
  - 监听地址固定 127.0.0.1，目标固定为对端 127.0.0.1:<端口>。
  - 配对即信任：只和可信设备配对；配对后对端可访问本机任意 loopback 端口。
  - INVITE/REPLY 必须通过可信私密渠道交换。
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => {}
        [arg] if arg == "-h" || arg == "--help" => {
            print!("{HELP}");
            return ExitCode::SUCCESS;
        }
        [arg] if arg == "-V" || arg == "--version" => {
            println!("p2p-tunnel {}", env!("CARGO_PKG_VERSION"));
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

/// 打印命令提示符并立刻刷新（管道/终端都要看得见）。
fn prompt() {
    print!("> ");
    let _ = io::stdout().flush();
}

/// 这个事件是否会输出行；静默事件（连接计数）不补打提示符。
fn event_prints(event: &Event) -> bool {
    matches!(
        event,
        Event::InviteCode(_)
            | Event::ReplyCode(_)
            | Event::ComparisonCode(_)
            | Event::Ready
            | Event::LinkState(_)
            | Event::RulesChanged
            | Event::PeerRules(_)
            | Event::PeerRulesUnknown
            | Event::PeerRulesStale
            | Event::RemoteClosed { .. }
            | Event::ListenerStarted(_)
            | Event::Error(_)
    )
}

fn init_tracing() {
    // bootstrap 连接按设计在配对后关闭，quinn 会打一条 ERROR 级
    // “UDP demultiplexer shut down”，对用户是噪声；默认关掉，调试时用
    // `RUST_LOG` 打开。日志写 stderr，stdout 只留交互界面。
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=off,quinn_proto=off"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(io::stderr)
        .try_init();
}

async fn run() -> ExitCode {
    println!(
        "p2p-tunnel v{} — 安全的 P2P TCP 端口转发",
        env!("CARGO_PKG_VERSION")
    );
    println!("{MENU}");

    // 没有 readline：提示符在每次命令/事件输出后补打，用户随时知道可以输入。
    prompt();

    let mut session = Session::spawn();
    let commands = session.sender();

    // stdin 读取放到独立线程，主循环用 select! 同时处理事件与输入。
    // 通道有界：核心处理不过来时读线程阻塞，输入不会在内存里堆积。
    let (line_tx, mut line_rx) = mpsc::channel::<String>(STDIN_CHANNEL);
    std::thread::spawn(move || {
        forward_lines(io::stdin().lock(), &line_tx);
    });

    let mut ui = UiState::default();
    loop {
        tokio::select! {
            incoming = line_rx.recv() => {
                match incoming {
                    Some(line) => {
                        if !handle_input(line.trim(), &commands, &mut ui).await {
                            break;
                        }
                        prompt();
                    }
                    // stdin EOF（管道结束或 Ctrl-D）按 quit 处理。
                    None => break,
                }
            }
            event = session.recv() => {
                match event {
                    Some(event) => {
                        let speaks = event_prints(&event);
                        if let Some(command) = handle_event(event, &mut ui) {
                            send(&commands, command).await;
                        }
                        if ui.session_lost {
                            break;
                        }
                        if speaks {
                            prompt();
                        }
                    }
                    None => {
                        // core 未收到 Shutdown 就结束：属于运行错误。
                        if !ui.session_lost {
                            eprintln!("[!] core 已退出");
                            ui.session_lost = true;
                        }
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n收到 Ctrl-C，正在退出…");
                break;
            }
        }
    }

    // 退出前把 core 已经发出但还没渲染的事件排空（例如刚提交 add 就 quit 的
    // 错误回包），避免用户看不到错误。总预算仍是 SHUTDOWN_WAIT。
    ui.exiting = true;
    let _ = commands.send(Command::Shutdown).await;
    let _ = tokio::time::timeout(SHUTDOWN_WAIT, async {
        while let Some(event) = session.recv().await {
            let _ = handle_event(event, &mut ui);
        }
    })
    .await;
    let _ = tokio::time::timeout(SHUTDOWN_WAIT, session.shutdown()).await;
    println!("已退出。");

    if ui.session_lost {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

/// 处理一行输入；返回 `false` 表示退出主循环。
async fn handle_input(line: &str, commands: &mpsc::Sender<Command>, ui: &mut UiState) -> bool {
    if line.is_empty() {
        if ui.awaiting == Awaiting::Invite {
            println!("[!] INVITE 不能为空");
        }
        return true;
    }

    let mut parts = line.split_whitespace();
    let command = parts.next().unwrap_or("");

    // 等待粘贴 INVITE/REPLY 时仍允许控制命令，避免用户被锁死在这一步。
    // 合法配对码以 `P2PR-` 开头，不会与这些命令冲突。help/status/quit
    // 只在这里匹配一次，下面的普通命令分支不再重复。
    if matches!(command, "3" | "quit") {
        return false;
    }
    if command == "help" {
        println!("{HELP}");
        return true;
    }
    if command == "status" {
        print_status(ui);
        return true;
    }

    if ui.awaiting != Awaiting::None {
        // 其余整行都当作配对码，成功送出后退出等待状态。
        match std::mem::replace(&mut ui.awaiting, Awaiting::None) {
            Awaiting::Reply => send(commands, Command::ProvideReply(line.to_string())).await,
            Awaiting::Invite => send(commands, Command::JoinInvite(line.to_string())).await,
            Awaiting::None => unreachable!("上面已确认存在等待状态"),
        }
        return true;
    }
    match command {
        "1" | "create" => {
            send(commands, Command::CreateInvite).await;
        }
        "2" | "join" => {
            ui.awaiting = Awaiting::Invite;
            println!("粘贴对方的 INVITE：");
        }
        "add" => handle_add(parts.collect(), commands, ui).await,
        "close" => handle_close(parts.collect(), commands, ui).await,
        other => {
            println!("[!] 未知命令：{other}（输入 help 查看帮助）");
        }
    }
    true
}

async fn handle_add(args: Vec<&str>, commands: &mpsc::Sender<Command>, ui: &mut UiState) {
    if args.len() != 2 {
        println!("[!] 用法：add <本机端口> <对端端口>");
        return;
    }
    let (listen_port, peer_port) = match (parse_port(args[0]), parse_port(args[1])) {
        (Some(listen_port), Some(peer_port)) => (listen_port, peer_port),
        _ => {
            println!("[!] 端口必须是 1–65535");
            return;
        }
    };
    match Forward::new(listen_port, peer_port) {
        Ok(forward) => {
            ui.pending_add = Some(forward);
            send(commands, Command::AddForward(forward)).await;
        }
        Err(error) => println!("[!] {error}"),
    }
}

async fn handle_close(args: Vec<&str>, commands: &mpsc::Sender<Command>, ui: &mut UiState) {
    if args.len() != 1 {
        println!("[!] 用法：close <本机端口>");
        return;
    }
    let Some(listen_port) = parse_port(args[0]) else {
        println!("[!] 端口必须是 1–65535");
        return;
    };
    if !ui.local.iter().any(|rule| rule.listen_port == listen_port) {
        println!("[!] 没有本机规则监听 {listen_port}");
        return;
    }
    ui.pending_close = ui
        .local
        .iter()
        .find(|rule| rule.listen_port == listen_port)
        .copied();
    ui.local.retain(|rule| rule.listen_port != listen_port);
    ui.active.remove(&listen_port);
    send(commands, Command::CloseForward { listen_port }).await;
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

fn print_status(ui: &UiState) {
    for line in format_status(ui) {
        println!("{line}");
    }
}

/// 链路文案；已连接时带 SPEC §8.3 的 managed link 说明。
fn link_label(state: LinkState) -> &'static str {
    match state {
        LinkState::Connected => "已连接（managed link）",
        other => other.label(),
    }
}

fn handle_event(event: Event, ui: &mut UiState) -> Option<Command> {
    match event {
        Event::InviteCode(code) => {
            println!("INVITE（通过可信渠道发给对方）：");
            println!("{code}");
            ui.awaiting = Awaiting::Reply;
            println!("粘贴对方的 REPLY：");
        }
        Event::ReplyCode(code) => {
            println!("REPLY（通过可信渠道发给对方）：");
            println!("{code}");
        }
        Event::ComparisonCode(code) => {
            // v0.1 不阻塞人工核对：打印比较码后自动确认，配对立即继续。
            println!("比较码：{code}（已自动确认；v0.1 不阻塞配对）");
            return Some(Command::ConfirmComparison);
        }
        Event::Ready => {
            // core 只在配对完成时发一次 Ready；这里再防一层重复，避免
            // 未来事件源变化时把“配对完成”重复打印成重连提示。
            if !ui.ready_seen {
                ui.ready_seen = true;
                println!("[+] 配对完成，已连接");
                println!("现在可用 add <本机端口> <对端端口> 新增规则，或输入 help 查看命令。");
            }
        }
        Event::LinkState(state) => {
            ui.link = Some(state);
            ui.peer_stale = state == LinkState::Reconnecting;
            ui.peer_unknown = false;
            // 正常退出时 core 也会发 Stopped；这是收尾信号，不是状态变化。
            if !(ui.exiting && state == LinkState::Stopped) {
                println!("链路: {}", link_label(state));
            }
        }
        Event::RulesChanged => {
            // close 成功没有单独事件：RulesChanged 到达时确认关闭结果。
            if let Some(closed) = ui.pending_close.take() {
                println!("[-] {} 已关闭（活动连接已重置）", closed.listen_port);
            }
        }
        Event::PeerRules(rules) => {
            ui.peer = rules;
            ui.peer_stale = false;
            ui.peer_unknown = false;
            print_status(ui);
        }
        Event::PeerRulesUnknown => {
            ui.peer_unknown = true;
            println!("[!] 规则同步状态流不可用，对端规则未知（数据面不受影响）");
        }
        Event::PeerRulesStale => {
            ui.peer_stale = true;
            println!("[!] 对端规则：链路重连中，可能已过期");
        }
        Event::RemoteClosed { listen_port } => {
            println!("[对端] 远程主动断开了 {listen_port}");
        }
        Event::ListenerStarted(listen_port) => {
            if let Some(pending) = ui.pending_add.take() {
                if pending.listen_port == listen_port {
                    ui.local.push(pending);
                    println!(
                        "[+] 127.0.0.1:{} -> 对端:{} 监听中",
                        pending.listen_port, pending.peer_port
                    );
                }
            } else {
                println!("[+] 127.0.0.1:{listen_port} 监听中");
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
            println!("[!] {error}");
        }
    }

    None
}

/// 把 stdin 行送入有界通道；通道满时阻塞调用线程（背压）。
fn forward_lines<R: BufRead>(reader: R, lines: &mpsc::Sender<String>) {
    for line in reader.lines() {
        match line {
            Ok(line) => {
                if lines.blocking_send(line).is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

async fn send(commands: &mpsc::Sender<Command>, command: Command) {
    if commands.send(command).await.is_err() {
        println!("[!] core 已退出");
    }
}

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
    /// 已进入退出排空阶段：抑制 core 的收尾 Stopped 状态输出。
    exiting: bool,
    /// 已打印过“配对完成”：重连只更新链路状态，不重复打印。
    ready_seen: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

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
    async fn removed_aliases_are_treated_as_unknown_commands() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut ui = UiState::default();
        // `exit`/`?` 不在 SPEC §8.2 命令表内，已删除；输入它们不再退出。
        assert!(handle_input("exit", &tx, &mut ui).await);
        assert!(handle_input("?", &tx, &mut ui).await);
        assert!(rx.try_recv().is_err(), "别名不应产生 core 命令");
    }

    #[test]
    fn stdin_forwarding_uses_bounded_backpressure() {
        let (tx, mut rx) = mpsc::channel::<String>(2);
        let input: String = (0..10_000).map(|index| format!("line-{index}\n")).collect();
        let writer = std::thread::spawn(move || forward_lines(Cursor::new(input), &tx));

        // 只有 2 个名额：生产者必须阻塞，而不是把一万行全读进内存。
        std::thread::sleep(Duration::from_millis(100));
        assert!(!writer.is_finished(), "有界通道应在满时阻塞生产者");

        let mut count = 0usize;
        while rx.blocking_recv().is_some() {
            count += 1;
        }
        writer.join().expect("写线程结束");
        assert_eq!(count, 10_000);
    }
}
