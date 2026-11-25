//! CLI 黑盒测试：直接启动 `p2p-tunnel` 二进制，用管道 stdin 驱动，
//! 断言 stdout/stderr 文案与退出码。不需要真实网络。
//!
//! 对应 `docs/SPEC.md` §8 与 ticket 10 验收项。

use std::io::{Read, Write};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_p2p-tunnel");

/// 正常路径应远快于此；超过即视为交互死锁。
const TIMEOUT: Duration = Duration::from_secs(10);

struct CliOutput {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

/// 多数用例只断言交互文案：固定 `RUST_LOG=error`，不让 core 日志混进文本。
fn run(args: &[&str], stdin: &str) -> CliOutput {
    run_with_env(args, stdin, Some("error"))
}

/// `rust_log` 为 `Some` 时设置 `RUST_LOG`；为 `None` 时移除环境变量，
/// 走「未设置 RUST_LOG」的默认路径（票据 15 的回归入口）。
fn run_with_env(args: &[&str], stdin: &str, rust_log: Option<&str>) -> CliOutput {
    run_feeding(args, rust_log, |mut pipe| {
        pipe.write_all(stdin.as_bytes()).expect("写入 stdin 失败");
        // pipe 在此 drop，stdin 关闭；即使 quit 处理有问题也会 EOF。
    })
}

/// 分步喂 stdin，给 core 的异步事件留出上屏时间。
fn run_with_steps(args: &[&str], steps: &[(&str, Duration)], rust_log: Option<&str>) -> CliOutput {
    run_feeding(args, rust_log, |mut pipe| {
        for (text, delay) in steps {
            pipe.write_all(text.as_bytes()).expect("写入 stdin 失败");
            pipe.flush().expect("刷新 stdin 失败");
            std::thread::sleep(*delay);
        }
    })
}

fn run_feeding(
    args: &[&str],
    rust_log: Option<&str>,
    feed: impl FnOnce(std::process::ChildStdin),
) -> CliOutput {
    let mut command = Command::new(BIN);
    match rust_log {
        Some(value) => {
            command.env("RUST_LOG", value);
        }
        None => {
            command.env_remove("RUST_LOG");
        }
    }
    let mut child = command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("启动 p2p-tunnel 失败");

    // stdout/stderr 必须与写入 stdin 并发读取：输入量大时子进程的
    // 输出管会填满，若父进程还在阻塞写 stdin 就会互相等待。
    let mut stdout_pipe = child.stdout.take().expect("stdout 管道");
    let stdout_reader = std::thread::spawn(move || {
        let mut stdout = String::new();
        stdout_pipe
            .read_to_string(&mut stdout)
            .expect("读取 stdout 失败");
        stdout
    });
    let mut stderr_pipe = child.stderr.take().expect("stderr 管道");
    let stderr_reader = std::thread::spawn(move || {
        let mut stderr = String::new();
        stderr_pipe
            .read_to_string(&mut stderr)
            .expect("读取 stderr 失败");
        stderr
    });

    if let Some(pipe) = child.stdin.take() {
        feed(pipe);
    }

    let status = wait_with_deadline(&mut child);
    let stdout = stdout_reader.join().expect("stdout 读取线程");
    let stderr = stderr_reader.join().expect("stderr 读取线程");
    CliOutput {
        status,
        stdout,
        stderr,
    }
}

fn wait_with_deadline(child: &mut Child) -> ExitStatus {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("try_wait 失败") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("CLI 未在 {TIMEOUT:?} 内退出（可能卡在交互等待）");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn assert_success(out: &CliOutput) {
    assert!(
        out.status.success(),
        "期望退出码 0，实际 {:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        out.stdout,
        out.stderr
    );
}

fn assert_exit(out: &CliOutput, expected: i32) {
    assert_eq!(
        out.status.code(),
        Some(expected),
        "stdout:\n{}\nstderr:\n{}",
        out.stdout,
        out.stderr
    );
}

/// 找到 `needle` 在输出中的位置，找不到就带上下文失败。
fn position(out: &CliOutput, needle: &str) -> usize {
    out.stdout
        .find(needle)
        .unwrap_or_else(|| panic!("输出缺少「{needle}」\nstdout:\n{}", out.stdout))
}

#[test]
fn help_flag_exits_zero() {
    let out = run(&["--help"], "");
    assert_success(&out);
    assert!(out.stdout.contains("用法:"), "stdout:\n{}", out.stdout);
    assert!(
        out.stdout.contains("add <本机端口> <对端端口>"),
        "stdout:\n{}",
        out.stdout
    );
}

#[test]
fn version_flag_exits_zero() {
    let out = run(&["--version"], "");
    assert_success(&out);
    assert!(
        out.stdout
            .contains(concat!("p2p-tunnel ", env!("CARGO_PKG_VERSION"))),
        "stdout:\n{}",
        out.stdout
    );
}

#[test]
fn unknown_flag_exits_two() {
    let out = run(&["--bogus"], "");
    assert_exit(&out, 2);
    assert!(
        out.stderr.contains("[!] 未知参数：--bogus"),
        "stderr:\n{}",
        out.stderr
    );
}

#[test]
fn quit_exits_zero_and_prints_goodbye() {
    let out = run(&[], "quit\n");
    assert_success(&out);
    assert!(out.stdout.contains(MENU_LINE), "stdout:\n{}", out.stdout);
    assert!(
        out.stdout.trim_end().ends_with("已退出。"),
        "stdout:\n{}",
        out.stdout
    );
    // 正常退出不乱报“链路: 已停止”（core 的收尾事件不应显示成状态变化）。
    assert!(
        !out.stdout.contains("链路: 已停止"),
        "stdout:\n{}",
        out.stdout
    );
}

const MENU_LINE: &str = "1) 创建连接   2) 加入连接   3) 退出";

#[test]
fn status_shows_required_sections_in_order() {
    let out = run(&[], "status\nquit\n");
    assert_success(&out);

    // 未配对时如实显示未连接，不猜测“连接中”。
    assert!(
        out.stdout.contains("链路: 未连接"),
        "stdout:\n{}",
        out.stdout
    );

    let link = position(&out, "链路:");
    let local = position(&out, "本机规则:");
    let peer = position(&out, "对端规则:");
    let active = position(&out, "活动连接:");
    assert!(
        link < local && local < peer && peer < active,
        "status 四段顺序错误\nstdout:\n{}",
        out.stdout
    );
    assert!(out.stdout.contains("（无）"), "stdout:\n{}", out.stdout);
}

#[test]
fn status_while_awaiting_invite_is_not_swallowed() {
    // 选择“加入连接”后输入 status：必须仍执行 status，而不是把整行当 INVITE。
    let out = run(&[], "2\nstatus\nquit\n");
    assert_success(&out);
    assert!(
        out.stdout.contains("本机规则:"),
        "等待 INVITE 时 status 被吞掉\nstdout:\n{}",
        out.stdout
    );
    assert!(
        out.stdout.trim_end().ends_with("已退出。"),
        "stdout:\n{}",
        out.stdout
    );
}

#[test]
fn thousands_of_stdin_lines_are_backpressured_not_deadlocked() {
    // 有界 stdin 通道 + 并发读取 stdout：大量输入应被背压消化，
    // 不出现死锁，也不会把整段输入堆在内存里。
    let input = "status\n".repeat(3000) + "quit\n";
    let out = run(&[], &input);
    assert_success(&out);
    assert_eq!(
        out.stdout.matches("本机规则:").count(),
        3000,
        "每条 status 都应有完整输出"
    );
    assert!(
        out.stdout.trim_end().ends_with("已退出。"),
        "stdout 结尾:\n{}",
        out.stdout
    );
}

#[test]
fn add_without_pairing_reports_not_ready() {
    // 未经配对 add：core 通过 Event::Error 回错误，CLI 不得 panic。
    let out = run(&[], "add 2211 22\nquit\n");
    assert_success(&out);
    assert!(
        out.stdout.contains("[!] 网络错误：会话尚未就绪"),
        "stdout:\n{}",
        out.stdout
    );
}

#[test]
fn add_invalid_port_uses_exact_message() {
    for input in ["add 0 22\nquit\n", "add 2211 65536\nquit\n"] {
        let out = run(&[], input);
        assert_success(&out);
        assert!(
            out.stdout.contains("[!] 端口必须是 1–65535"),
            "input={input:?}\nstdout:\n{}",
            out.stdout
        );
    }
}

#[test]
fn add_missing_args_uses_usage_text() {
    let out = run(&[], "add 2211\nquit\n");
    assert_success(&out);
    assert!(
        out.stdout.contains("[!] 用法：add <本机端口> <对端端口>"),
        "stdout:\n{}",
        out.stdout
    );
}

#[test]
fn close_missing_rule_uses_exact_message() {
    let out = run(&[], "close 2211\nquit\n");
    assert_success(&out);
    assert!(
        out.stdout.contains("[!] 没有本机规则监听 2211"),
        "stdout:\n{}",
        out.stdout
    );
}

/// 票据 15：不提供 `--log-file`，按未知参数处理（退出码 2），且不落任何日志文件。
#[test]
fn log_file_flag_is_unknown_argument_and_creates_no_file() {
    let path = std::env::temp_dir().join(format!("p2p-tunnel-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let path_arg = path.to_str().expect("临时路径应是 UTF-8");
    let pathed = format!("--log-file={path_arg}");

    for args in [
        vec!["--log-file"],
        vec!["--log-file", path_arg],
        vec![pathed.as_str()],
    ] {
        let out = run(&args, "");
        assert_exit(&out, 2);
        assert!(
            out.stderr.contains("[!] 未知参数：--log-file"),
            "args={args:?}\nstderr:\n{}",
            out.stderr
        );
        assert!(
            out.stderr.contains("用法: p2p-tunnel [--help|--version]"),
            "args={args:?}\nstderr:\n{}",
            out.stderr
        );
        assert!(!path.exists(), "不得创建日志文件 {}", path.display());
    }
}

/// 票据 15：不设置 `RUST_LOG` 时默认 `warn`——warn 仍写 stderr，且日志不上 stdout。
#[test]
fn unset_rust_log_keeps_warn_on_stderr_only() {
    // 未配对时用非法 INVITE 触发一条 warn（解析失败）；中间留出上屏时间。
    let out = run_with_steps(
        &[],
        &[
            ("2\n", Duration::from_millis(300)),
            ("bogus-invite\n", Duration::from_millis(1200)),
            ("quit\n", Duration::ZERO),
        ],
        None,
    );
    assert_success(&out);
    assert!(
        out.stderr.contains("WARN") && out.stderr.contains("配对失败"),
        "未设置 RUST_LOG 时 warn 仍应写 stderr：\nstderr:\n{}",
        out.stderr
    );
    assert!(
        !out.stderr.contains("INFO"),
        "默认级别下不应出现 info 日志：\nstderr:\n{}",
        out.stderr
    );
    assert!(
        !out.stdout.contains("WARN"),
        "日志不得写 stdout：\nstdout:\n{}",
        out.stdout
    );
}

#[test]
fn close_invalid_port_uses_exact_message() {
    let out = run(&[], "close 0\nquit\n");
    assert_success(&out);
    assert!(
        out.stdout.contains("[!] 端口必须是 1–65535"),
        "stdout:\n{}",
        out.stdout
    );
}

#[test]
fn unknown_command_prints_hint() {
    let out = run(&[], "frobnicate\nquit\n");
    assert_success(&out);
    assert!(
        out.stdout
            .contains("[!] 未知命令：frobnicate（输入 help 查看帮助）"),
        "stdout:\n{}",
        out.stdout
    );
}

#[cfg(unix)]
#[test]
fn ctrl_c_exits_zero_within_deadline() {
    let mut child = Command::new(BIN)
        .env("RUST_LOG", "error")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("启动 p2p-tunnel 失败");
    // 保持 stdin 打开：只有真正的 Ctrl-C 处理才能让进程退出。
    let _stdin = child.stdin.take();
    std::thread::sleep(Duration::from_millis(400));

    let pid = child.id().to_string();
    let sent = Command::new("kill")
        .args(["-INT", &pid])
        .status()
        .expect("执行 kill 失败");
    assert!(sent.success(), "发送 SIGINT 失败");

    let status = wait_with_deadline(&mut child);
    assert!(
        status.success(),
        "Ctrl-C 后退出码应为 0，实际 {:?}",
        status.code()
    );

    let mut stdout = String::new();
    child
        .stdout
        .take()
        .expect("stdout 管道")
        .read_to_string(&mut stdout)
        .expect("读取 stdout 失败");
    assert!(stdout.contains("已退出。"), "stdout:\n{stdout}");
}
