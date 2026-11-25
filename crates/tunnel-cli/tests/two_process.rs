//! 双进程端到端：两个真实 `p2p-tunnel` 二进制通过 SDK 配对，A `quit` 后
//! B 收到「对端已退出」，双方退出码 0，监听端口可立即重绑（票据 19）。
//!
//! 现有 `cli.rs` 只驱动单个子进程、`tunnel-core` 的告别用例在同一进程内
//! 用两个 `Session` 验证语义；本用例补的是进程拼装层：真实 stdin 驱动、
//! 真实 SDK 配对、事件循环排空与进程退出码。

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_p2p-tunnel");
/// 真实 SDK 握手（UDP + 比较码）在慢 CI 机器上也要留余量。
const PAIR_TIMEOUT: Duration = Duration::from_secs(60);
const EXIT_TIMEOUT: Duration = Duration::from_secs(20);

struct Proc {
    child: Child,
    stdin: ChildStdin,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
}

impl Proc {
    fn spawn() -> Self {
        let mut child = Command::new(BIN)
            .env("RUST_LOG", "error")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("启动 p2p-tunnel 失败");

        let stdin = child.stdin.take().expect("stdin 管道");
        let stdout = read_into_thread(child.stdout.take().expect("stdout 管道"));
        let stderr = read_into_thread(child.stderr.take().expect("stderr 管道"));
        Self {
            child,
            stdin,
            stdout,
            stderr,
        }
    }

    fn send(&mut self, line: &str) {
        writeln!(self.stdin, "{line}").expect("写入 stdin 失败");
        self.stdin.flush().expect("刷新 stdin 失败");
    }

    fn text(&self) -> String {
        self.stdout.lock().expect("stdout 锁").clone()
    }

    fn wait_for(&mut self, needle: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if self.text().contains(needle) {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("try_wait 失败") {
                panic!(
                    "进程提前退出（{status:?}），等待 {needle:?} 失败\nstdout:\n{}\nstderr:\n{}",
                    self.text(),
                    self.stderr.lock().expect("stderr 锁")
                );
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                panic!(
                    "等待 {needle:?} 超时\nstdout:\n{}\nstderr:\n{}",
                    self.text(),
                    self.stderr.lock().expect("stderr 锁")
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 等待以 `prefix` 开头的整行出现并返回该行（配对码单独占一行）。
    fn wait_line_starting_with(&mut self, prefix: &str, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(line) = self
                .text()
                .lines()
                .find(|line| line.starts_with(prefix))
                .map(str::to_string)
            {
                return line;
            }
            if self.child.try_wait().expect("try_wait 失败").is_some() {
                panic!(
                    "进程提前退出，等待 {prefix:?} 行失败\nstdout:\n{}\nstderr:\n{}",
                    self.text(),
                    self.stderr.lock().expect("stderr 锁")
                );
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                panic!(
                    "等待 {prefix:?} 行超时\nstdout:\n{}\nstderr:\n{}",
                    self.text(),
                    self.stderr.lock().expect("stderr 锁")
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_exit(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait 失败") {
                return status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!(
                    "进程未在 {timeout:?} 内退出\nstdout:\n{}\nstderr:\n{}",
                    self.text(),
                    self.stderr.lock().expect("stderr 锁")
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn read_into_thread(mut pipe: impl Read + Send + 'static) -> Arc<Mutex<String>> {
    let text = Arc::new(Mutex::new(String::new()));
    let sink = Arc::clone(&text);
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match pipe.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => sink
                    .lock()
                    .expect("输出锁")
                    .push_str(&String::from_utf8_lossy(&buf[..n])),
            }
        }
    });
    text
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("绑定临时端口")
        .local_addr()
        .expect("读取临时端口")
        .port()
}

#[test]
fn quit_ends_both_processes_with_exit_code_zero() {
    let mut a = Proc::spawn();
    let mut b = Proc::spawn();

    // A 创建邀请。
    a.send("1");
    a.wait_for("INVITE（通过可信渠道发给对方）：", PAIR_TIMEOUT);
    let invite = a.wait_line_starting_with("P2PR-INV2-", PAIR_TIMEOUT);
    a.wait_for("粘贴对方的 REPLY：", PAIR_TIMEOUT);

    // B 粘贴邀请并取出回复。
    b.send("2");
    b.wait_for("粘贴对方的 INVITE：", PAIR_TIMEOUT);
    b.send(&invite);
    let reply = b.wait_line_starting_with("P2PR-REP2-", PAIR_TIMEOUT);

    // A 粘贴回复，双端自动确认比较码并配对完成。
    a.send(&reply);
    a.wait_for("配对完成", PAIR_TIMEOUT);
    b.wait_for("配对完成", PAIR_TIMEOUT);

    // A 开一条转发，用来验证退出后监听端口真的释放。
    let listen_port = free_port();
    a.send(&format!("add {listen_port} 9"));
    a.wait_for(
        &format!("127.0.0.1:{listen_port} -> 对端:9 监听中"),
        PAIR_TIMEOUT,
    );

    // 已连接后再次发起配对应被拒绝，且不影响现有会话。
    a.send("create");
    a.wait_for("[!] 已连接，不能再次配对", PAIR_TIMEOUT);
    a.send("1");
    a.wait_for("未知命令：1", PAIR_TIMEOUT);

    a.send("3");
    a.wait_for("未知命令：3", PAIR_TIMEOUT);

    // 已连接后的帮助只列真实命令：不出现 create/join，也不出现数字别名。
    a.send("help");
    a.wait_for("配对已完成；要重新配对请退出后重新启动", PAIR_TIMEOUT);
    let text = a.text();
    let help_start = text
        .rfind("p2p-tunnel — 安全的 P2P TCP 端口转发")
        .expect("帮助已打印");
    let help_tail = &text[help_start..];
    assert!(
        help_tail.contains("quit / exit"),
        "help 应列出退出别名:\n{help_tail}"
    );
    assert!(
        help_tail.contains("add <本机端口> <对端端口>"),
        "help 应列 add 用法:\n{help_tail}"
    );
    assert!(
        !help_tail.contains("create") && !help_tail.contains("join"),
        "已连接后的帮助不应展示配对命令\n帮助:\n{help_tail}"
    );

    // 已连接后命令校验与未知命令提示仍按预期工作。
    a.send("add 0 22");
    a.wait_for("[!] 端口必须是 1–65535", PAIR_TIMEOUT);
    a.send("add 2211");
    a.wait_for("[!] 用法：add <本机端口> <对端端口>", PAIR_TIMEOUT);
    a.send("close 2211");
    a.wait_for("[!] 没有本机规则监听 2211", PAIR_TIMEOUT);
    a.send("frobnicate");
    a.wait_for("[!] 未知命令：frobnicate", PAIR_TIMEOUT);
    a.send("q");
    a.wait_for("[!] 未知命令：q", PAIR_TIMEOUT);

    // status 在已连接后可用。
    a.send("status");
    a.wait_for("本机规则:", PAIR_TIMEOUT);

    // A 退出：A 应退出 0，B 应打印「对端已退出」后退出 0。
    a.send("quit");
    let status_a = a.wait_exit(EXIT_TIMEOUT);
    assert_eq!(
        status_a.code(),
        Some(0),
        "A 退出码应为 0，实际 {status_a:?}"
    );

    b.wait_for("[对端] 对端已退出", EXIT_TIMEOUT);
    let status_b = b.wait_exit(EXIT_TIMEOUT);
    assert_eq!(
        status_b.code(),
        Some(0),
        "B 退出码应为 0，实际 {status_b:?}"
    );

    // 端口必须已释放：退出后同端口可立即重绑。
    TcpListener::bind(format!("127.0.0.1:{listen_port}")).expect("退出后监听端口应可立即重绑");
}
