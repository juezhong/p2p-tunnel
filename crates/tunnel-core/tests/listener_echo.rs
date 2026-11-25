//! Ticket 03a 集成测试：`Session` + 内存 mock 数据链路 + 真实 TCP。
//!
//! 覆盖：单规则 TCP echo 字节路径、事件顺序、`CloseForward` 生命周期、
//! 监听端口校验。目标端 echo 服务是真实 `127.0.0.1` TCP 服务，链路是
//! [`MockLink`]，因此不依赖 SDK / 外网，可在 CI 稳定运行。

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tunnel_core::transport::mock::MockLink;
use tunnel_core::transport::{DataLink, TunnelRead, TunnelStream, TunnelWrite};
use tunnel_core::{Command, Event, Forward, LinkState, Session, TunnelError, OPEN_BI_TIMEOUT};

/// 单个测试步骤的事件等待上限。
const WAIT: Duration = Duration::from_secs(5);

/// 等待下一个事件；超时或 core 退出都视为测试失败。
async fn next_event(session: &mut Session) -> Event {
    tokio::time::timeout(WAIT, session.recv())
        .await
        .expect("等待事件超时")
        .expect("core 提前退出")
}

/// 等待第一个满足条件的事件，丢弃不相关事件。
async fn wait_for(session: &mut Session, mut predicate: impl FnMut(&Event) -> bool) -> Event {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "等待目标事件超时");
        let event = tokio::time::timeout(remaining, session.recv())
            .await
            .expect("等待目标事件超时")
            .expect("core 提前退出");
        if predicate(&event) {
            return event;
        }
    }
}

/// 申请一个空闲的本地端口：绑定后立即释放。
async fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定临时端口");
    listener.local_addr().expect("读取本地地址").port()
}

/// 从临时端口范围之外取一个端口。
///
/// Windows 顺序分配临时端口：刚释放的端口很可能被并行测试的 `bind(0)`
/// 立刻拿走。这里改用 20000 起的固定段（Linux 与 Windows 的临时端口
/// 范围都在其之上），避免“端口已释放”的探针被别的测试干扰。
async fn reserved_port(offset: u16) -> u16 {
    for candidate in (20_000 + offset)..30_000 {
        if TcpListener::bind(("127.0.0.1", candidate)).await.is_ok() {
            return candidate;
        }
    }
    panic!("找不到可用的固定端口");
}

/// 启动一个真实的 TCP echo 服务，返回监听端口与后台任务。
async fn spawn_echo_server() -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定 echo 端口");
    let port = listener.local_addr().expect("读取 echo 地址").port();

    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut tcp, _peer)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buffer = vec![0u8; 16 * 1024];
                loop {
                    match tcp.read(&mut buffer).await {
                        Ok(0) => {
                            // 客户端 FIN：回 FIN 后结束。
                            let _ = tcp.shutdown().await;
                            break;
                        }
                        Ok(n) => {
                            if tcp.write_all(&buffer[..n]).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    });

    (port, handle)
}

/// 启动"读完客户端数据后回写尾部数据"的 TCP 目标服务。
///
/// 用于验证 half-close 后尾部数据不丢失：客户端 FIN → 目标 EOF →
/// 目标写 `tail` → 目标 FIN → 客户端读到 tail 后再 EOF。
async fn spawn_tail_server(tail: &'static [u8]) -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定 tail 端口");
    let port = listener.local_addr().expect("读取 tail 地址").port();

    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut tcp, _peer)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buffer = vec![0u8; 16 * 1024];
                loop {
                    match tcp.read(&mut buffer).await {
                        Ok(0) => {
                            let _ = tcp.write_all(tail).await;
                            let _ = tcp.shutdown().await;
                            break;
                        }
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            });
        }
    });

    (port, handle)
}

/// 读到 EOF 为止的全部字节。
async fn read_to_eof(read: &mut TcpStream) -> Vec<u8> {
    let mut buffer = [0u8; 64];
    let mut received = Vec::new();
    loop {
        match read.read(&mut buffer).await.expect("读取失败") {
            0 => return received,
            n => received.extend_from_slice(&buffer[..n]),
        }
    }
}

/// 启动一个已就绪的 core：mock 链路同时扮演本端与对端。
async fn ready_session(link: Arc<MockLink>) -> Session {
    let mut session = Session::spawn_with_link(link);
    match next_event(&mut session).await {
        Event::Ready => session,
        other => panic!("期望 Ready，实际 {other:?}"),
    }
}

async fn add_forward_and_wait(session: &mut Session, forward: Forward) {
    session
        .sender()
        .send(Command::AddForward(forward))
        .await
        .expect("命令通道可用");
    wait_for(
        session,
        |event| matches!(event, Event::ListenerStarted(port) if *port == forward.listen_port),
    )
    .await;
    wait_for(session, |event| matches!(event, Event::RulesChanged)).await;
}

#[tokio::test]
async fn tcp_client_round_trips_payload_through_mock_link() {
    let (echo_port, echo) = spawn_echo_server().await;
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link).await;

    let listen_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, echo_port).unwrap()).await;

    let mut client = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("listener 应接受连接");
    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionOpened { listen_port: port } if *port == listen_port)
    })
    .await;

    let payload = b"ticket-03a echo payload ".repeat(200);
    client.write_all(&payload).await.expect("客户端写入");
    let mut echoed = vec![0u8; payload.len()];
    client
        .read_exact(&mut echoed)
        .await
        .expect("客户端读回 echo");
    assert_eq!(echoed, payload, "echo 字节必须一致");

    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionEstablished { listen_port: port } if *port == listen_port)
    })
    .await;

    // 客户端 FIN：目标侧回应 FIN，两端有序收尾。
    client.shutdown().await.expect("客户端半关闭");
    let mut tail = [0u8; 1];
    assert_eq!(
        client.read(&mut tail).await.expect("客户端读到 EOF"),
        0,
        "目标 FIN 应传给客户端"
    );
    drop(client);

    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionClosed { listen_port: port } if *port == listen_port)
    })
    .await;

    session.shutdown().await;
    echo.abort();
}

#[tokio::test]
async fn close_forward_stops_listener_and_removes_rule() {
    let (echo_port, echo) = spawn_echo_server().await;
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link).await;

    let listen_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, echo_port).unwrap()).await;

    session
        .sender()
        .send(Command::CloseForward { listen_port })
        .await
        .expect("命令通道可用");
    wait_for(&mut session, |event| matches!(event, Event::RulesChanged)).await;

    // 规则已移除：再次 close 报 RuleNotFound。
    session
        .sender()
        .send(Command::CloseForward { listen_port })
        .await
        .expect("命令通道可用");
    match wait_for(&mut session, |event| matches!(event, Event::Error(_))).await {
        Event::Error(TunnelError::RuleNotFound { listen_port: port }) => {
            assert_eq!(port, listen_port);
        }
        other => panic!("期望 RuleNotFound，实际 {other:?}"),
    }

    // listener 已停止：新 TCP 连接必须失败。
    assert!(
        TcpStream::connect(("127.0.0.1", listen_port))
            .await
            .is_err(),
        "关闭后不应还能建立 TCP 连接"
    );

    session.shutdown().await;
    echo.abort();
}

#[tokio::test]
async fn add_forward_rejects_bad_and_duplicate_ports_without_listening() {
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link).await;

    // 端口 0 非法。
    session
        .sender()
        .send(Command::AddForward(Forward {
            listen_port: 0,
            peer_port: 22,
        }))
        .await
        .expect("命令通道可用");
    match next_event(&mut session).await {
        Event::Error(TunnelError::Protocol(_)) => {}
        other => panic!("期望 Protocol 错误，实际 {other:?}"),
    }

    // 重复 listen_port 被拒绝，原规则继续监听。
    let echo_port = free_port().await;
    let listen_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, echo_port).unwrap()).await;
    session
        .sender()
        .send(Command::AddForward(Forward::new(listen_port, 22).unwrap()))
        .await
        .expect("命令通道可用");
    match wait_for(&mut session, |event| matches!(event, Event::Error(_))).await {
        Event::Error(TunnelError::Protocol(_)) => {}
        other => panic!("期望重复端口 Protocol 错误，实际 {other:?}"),
    }
    TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("重复 add 失败不影响已有 listener");

    // 端口已被其他进程占用：Bind 错误，不启动 listener。
    let occupied = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("占用临时端口");
    let occupied_port = occupied.local_addr().expect("读取地址").port();
    session
        .sender()
        .send(Command::AddForward(
            Forward::new(occupied_port, 22).unwrap(),
        ))
        .await
        .expect("命令通道可用");
    match wait_for(&mut session, |event| matches!(event, Event::Error(_))).await {
        Event::Error(TunnelError::Bind { port, .. }) => assert_eq!(port, occupied_port),
        other => panic!("期望 Bind 错误，实际 {other:?}"),
    }
    drop(occupied);

    session.shutdown().await;
}

#[tokio::test]
async fn client_half_close_receives_target_tail_then_eof_once() {
    let (tail_port, tail_server) = spawn_tail_server(b"tail-after-eof").await;
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link).await;

    let listen_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, tail_port).unwrap()).await;

    let mut client = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("listener 应接受连接");
    client.write_all(b"payload").await.expect("客户端写入");
    client.shutdown().await.expect("客户端 half-close");

    // 目标在读到 EOF 后回写尾部数据；客户端必须先收全 tail 再看到 EOF。
    let received = read_to_eof(&mut client).await;
    assert_eq!(received, b"tail-after-eof", "half-close 后的尾部数据不能丢");

    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionClosed { listen_port: port } if *port == listen_port)
    })
    .await;
    // 两个方向都结束后只发一次 ConnectionClosed。
    assert!(
        tokio::time::timeout(Duration::from_millis(200), session.recv())
            .await
            .is_err(),
        "ConnectionClosed 不应重复发送"
    );

    session.shutdown().await;
    tail_server.abort();
}

#[tokio::test]
async fn close_forward_terminates_only_its_active_connection() {
    let (echo_one, echo_one_task) = spawn_echo_server().await;
    let (echo_two, echo_two_task) = spawn_echo_server().await;
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link).await;

    let closed_port = free_port().await;
    let open_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(closed_port, echo_one).unwrap()).await;
    add_forward_and_wait(&mut session, Forward::new(open_port, echo_two).unwrap()).await;

    let mut closed_client = TcpStream::connect(("127.0.0.1", closed_port))
        .await
        .expect("第一条规则应接受连接");
    closed_client.write_all(b"ping").await.expect("客户端写");
    let mut echo = [0u8; 4];
    closed_client
        .read_exact(&mut echo)
        .await
        .expect("关闭前 echo 正常");

    let mut open_client = TcpStream::connect(("127.0.0.1", open_port))
        .await
        .expect("第二条规则应接受连接");

    session
        .sender()
        .send(Command::CloseForward {
            listen_port: closed_port,
        })
        .await
        .expect("命令通道可用");
    // `RulesChanged` 与活动连接的 `ConnectionClosed` 谁先到都合法，一起等。
    let deadline = tokio::time::Instant::now() + WAIT;
    let (mut changed, mut closed) = (false, false);
    while !(changed && closed) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "等待 CloseForward 事件超时");
        let event = tokio::time::timeout(remaining, session.recv())
            .await
            .expect("等待 CloseForward 事件超时")
            .expect("core 提前退出");
        changed |= matches!(event, Event::RulesChanged);
        closed |=
            matches!(event, Event::ConnectionClosed { listen_port: port } if port == closed_port);
    }

    // SPEC §9.1/§8.2：规则关闭必须 RST 活动连接，而不是发 FIN。
    let mut closed_buffer = [0u8; 8];
    let error = tokio::time::timeout(WAIT, closed_client.read(&mut closed_buffer))
        .await
        .expect("CloseForward 应终止活动连接")
        .expect_err("规则关闭后客户端应收到 RST");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::ConnectionReset,
        "规则关闭后本地 TCP 应 RST：{error}"
    );

    // 另一条规则完全不受影响。
    open_client
        .write_all(b"still-alive")
        .await
        .expect("客户端写");
    let mut echoed = [0u8; 11];
    open_client
        .read_exact(&mut echoed)
        .await
        .expect("未被关闭的规则应继续 echo");
    assert_eq!(&echoed, b"still-alive");

    session.shutdown().await;
    echo_one_task.abort();
    echo_two_task.abort();
}

#[tokio::test]
async fn single_connection_close_does_not_close_the_link() {
    let (echo_port, echo) = spawn_echo_server().await;
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link.clone()).await;

    let listen_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, echo_port).unwrap()).await;

    // 第一条连接正常建立、收发、FIN。
    let mut first = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("第一条连接");
    first.write_all(b"first").await.expect("写第一条");
    let mut echoed = [0u8; 5];
    first.read_exact(&mut echoed).await.expect("读第一条 echo");
    assert_eq!(&echoed, b"first");
    first.shutdown().await.expect("第一条 half-close");
    assert_eq!(read_to_eof(&mut first).await, b"");
    drop(first);
    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionClosed { listen_port: port } if *port == listen_port)
    })
    .await;

    // 单条连接结束不能关闭整条数据链路。
    assert!(!link.is_closed(), "单条连接关闭不应关闭共享链路");

    // 同一规则的第二条连接仍然可用。
    let mut second = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("第二条连接");
    second.write_all(b"second").await.expect("写第二条");
    let mut echoed = [0u8; 6];
    second.read_exact(&mut echoed).await.expect("读第二条 echo");
    assert_eq!(&echoed, b"second");

    session.shutdown().await;
    echo.abort();
}

#[tokio::test]
async fn link_loss_rsts_active_connection_and_reports_reconnecting() {
    let (echo_port, _echo) = spawn_echo_server().await;
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link.clone()).await;

    let listen_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, echo_port).unwrap()).await;

    let mut client = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("listener 应接受连接");
    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionEstablished { listen_port: port } if *port == listen_port)
    })
    .await;

    // 数据链路断开：活动 TCP 必须以 RST 关闭，并进入重连状态。
    link.close();

    let mut buffer = [0u8; 8];
    let error = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer))
        .await
        .expect("链路断开后应关闭本地 TCP")
        .expect_err("链路断开应 RST 本地 TCP，而不是 EOF");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::ConnectionReset,
        "链路断开应映射为 TCP RST"
    );

    // 连接关闭事件可能先于重连状态入队，先收关闭再收状态。
    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionClosed { listen_port: port } if *port == listen_port)
    })
    .await;
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Reconnecting))
    })
    .await;
}

// ---------------------------------------------------------------------------
// Ticket 04：并发上限与 open_bi 超时
// ---------------------------------------------------------------------------

/// 只接受出站流的测试链路。
///
/// 目标侧 `accept_bi` 永不返回，因此同一会话内的 96 上限只统计 listener
/// 打开的出站流；64 上限则按监听端口统计。
struct CountingLink {
    opened: Arc<AtomicUsize>,
    /// 保留 duplex 对端，避免流因另一端被 drop 而提前 EOF。
    peers: Mutex<Vec<tokio::io::DuplexStream>>,
}

impl CountingLink {
    fn new() -> Self {
        Self {
            opened: Arc::new(AtomicUsize::new(0)),
            peers: Mutex::new(Vec::new()),
        }
    }

    fn opened(&self) -> usize {
        self.opened.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl DataLink for CountingLink {
    async fn open_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        let (ours, theirs) = tokio::io::duplex(16 * 1024);
        self.peers.lock().await.push(theirs);
        Ok(Box::new(DuplexStream(ours)))
    }

    async fn accept_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        std::future::pending().await
    }
}

struct DuplexStream(tokio::io::DuplexStream);

impl TunnelStream for DuplexStream {
    fn split(self: Box<Self>) -> (Box<dyn TunnelRead>, Box<dyn TunnelWrite>) {
        let (read, write) = tokio::io::split(self.0);
        (Box::new(DuplexRead(read)), Box::new(DuplexWrite(write)))
    }
}

struct DuplexRead(tokio::io::ReadHalf<tokio::io::DuplexStream>);

impl AsyncRead for DuplexRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl TunnelRead for DuplexRead {
    fn stop(&mut self, _code: u32) -> Result<(), TunnelError> {
        Ok(())
    }
}

struct DuplexWrite(tokio::io::WriteHalf<tokio::io::DuplexStream>);

impl AsyncWrite for DuplexWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl TunnelWrite for DuplexWrite {
    fn finish(&mut self) -> Result<(), TunnelError> {
        Ok(())
    }

    fn reset(&mut self, _code: u32) -> Result<(), TunnelError> {
        Ok(())
    }
}

async fn start_counting_session(link: Arc<CountingLink>) -> Session {
    let mut session = Session::spawn_with_link(link);
    wait_for(&mut session, |event| matches!(event, Event::Ready)).await;
    session
}

/// 开一条连接并等到 `ConnectionEstablished`。
async fn open_established(session: &mut Session, listen_port: u16) -> TcpStream {
    let client = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("连接 listener");
    wait_for(session, |event| {
        matches!(event, Event::ConnectionEstablished { listen_port: port } if *port == listen_port)
    })
    .await;
    client
}

#[tokio::test]
async fn listener_rejects_sixty_fifth_connection_with_limit_event() {
    let link = Arc::new(CountingLink::new());
    let mut session = start_counting_session(link.clone()).await;
    let listen_port = free_port().await;
    add_forward_and_wait(
        &mut session,
        Forward::new(listen_port, 9999).expect("规则有效"),
    )
    .await;

    let mut clients = Vec::new();
    for _ in 0..64 {
        clients.push(open_established(&mut session, listen_port).await);
    }
    assert_eq!(link.opened(), 64, "64 条连接应各开一条数据流");

    let mut rejected = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("连接第 65 条");
    wait_for(
        &mut session,
        |event| matches!(event, Event::Error(TunnelError::Limit { port }) if *port == listen_port),
    )
    .await;

    let mut buffer = [0u8; 1];
    let n = tokio::time::timeout(WAIT, rejected.read(&mut buffer))
        .await
        .expect("超限连接应被关闭")
        .expect("读被拒连接");
    assert_eq!(n, 0, "第 65 条应读到 EOF");
    assert_eq!(link.opened(), 64, "超限不应再打开数据流");

    drop(clients);
    session.shutdown().await;
}

#[tokio::test]
async fn listener_rejects_connection_beyond_ninety_six_side_streams() {
    let link = Arc::new(CountingLink::new());
    let mut session = start_counting_session(link.clone()).await;

    let port_a = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(port_a, 9999).expect("规则有效")).await;
    let port_b = free_port().await;
    assert_ne!(port_a, port_b, "两条规则需要不同端口");
    add_forward_and_wait(&mut session, Forward::new(port_b, 9998).expect("规则有效")).await;

    // 64 + 32 = 96，正好用满本侧名额。
    let mut clients = Vec::new();
    for _ in 0..64 {
        clients.push(open_established(&mut session, port_a).await);
    }
    for _ in 0..32 {
        clients.push(open_established(&mut session, port_b).await);
    }
    assert_eq!(link.opened(), 96);

    let mut rejected = TcpStream::connect(("127.0.0.1", port_b))
        .await
        .expect("连接第 97 条");
    wait_for(
        &mut session,
        |event| matches!(event, Event::Error(TunnelError::Limit { port }) if *port == port_b),
    )
    .await;

    let mut buffer = [0u8; 1];
    let n = tokio::time::timeout(WAIT, rejected.read(&mut buffer))
        .await
        .expect("超限连接应被关闭")
        .expect("读被拒连接");
    assert_eq!(n, 0, "第 97 条应读到 EOF");
    assert_eq!(link.opened(), 96, "超限不应再打开数据流");

    drop(clients);
    session.shutdown().await;
}

#[tokio::test]
async fn hanging_open_bi_times_out_and_closes_tcp_without_reconnecting() {
    let link = Arc::new(MockLink::hanging());
    let mut session = ready_session(link).await;
    let listen_port = free_port().await;
    add_forward_and_wait(
        &mut session,
        Forward::new(listen_port, 9999).expect("规则有效"),
    )
    .await;

    let mut client = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("连接 listener");
    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionOpened { listen_port: port } if *port == listen_port)
    })
    .await;

    let started = tokio::time::Instant::now();
    let mut buffer = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(8), client.read(&mut buffer))
        .await
        .expect("open_bi 超时后应关闭 TCP")
        .expect("读客户端");
    assert_eq!(n, 0, "超时后客户端应读到 EOF");
    assert!(
        started.elapsed() >= OPEN_BI_TIMEOUT,
        "应等满 open_bi 的 5 秒超时"
    );

    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionClosed { listen_port: port } if *port == listen_port)
    })
    .await;

    // 超时只关闭这一条 TCP，不触发数据链路重连。
    let mut reconnecting = false;
    loop {
        match tokio::time::timeout(Duration::from_millis(300), session.recv()).await {
            Err(_) => break,
            Ok(Some(event)) => {
                reconnecting |= matches!(event, Event::LinkState(LinkState::Reconnecting));
            }
            Ok(None) => break,
        }
    }
    assert!(!reconnecting, "open_bi 超时不应触发链路重连");

    session.shutdown().await;
}

// ---------------------------------------------------------------------------
// Ticket 07：数据链路重连与失败处理
// ---------------------------------------------------------------------------

/// 重连期间新 TCP 仍会被接受，但会话在 5 秒断链上限处收尾：
/// 客户端连接被关闭，随后上报 `Disconnected` 并以退出码 0 结束。
#[tokio::test]
async fn new_tcp_during_reconnect_closes_at_disconnect_deadline() {
    let (echo_port, _echo) = spawn_echo_server().await;
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link.clone()).await;

    let listen_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, echo_port).unwrap()).await;

    link.close();
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Reconnecting))
    })
    .await;

    let mut client = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("重连期间 listener 仍应接受连接");
    wait_for(&mut session, |event| {
        matches!(event, Event::ConnectionOpened { listen_port: port } if *port == listen_port)
    })
    .await;

    let mut buffer = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(8), client.read(&mut buffer))
        .await
        .expect("会话应在 5 秒断链上限处关闭该 TCP");
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "客户端连接应被关闭，实际 {read:?}"
    );

    wait_for(&mut session, |event| matches!(event, Event::Disconnected)).await;
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Stopped))
    })
    .await;
    assert!(
        session.recv().await.is_none(),
        "断开连接后 core 应退出并关闭事件通道"
    );

    let rebound = TcpListener::bind(("127.0.0.1", listen_port)).await;
    assert!(
        rebound.is_ok(),
        "断开连接后 listener 应关闭，端口 {listen_port} 可重绑：{:?}",
        rebound.err()
    );
}

/// 链路恢复后：重开状态流、重发 RuleList、回到 Ready 并恢复转发。
#[tokio::test]
async fn reconnect_resends_rules_and_resumes_forwarding() {
    let (echo_port, echo) = spawn_echo_server().await;
    let link = Arc::new(MockLink::new());
    // 自环 mock 上由本端负责打开状态流，才能观察到重连后的 RuleList 重发。
    let mut session = Session::spawn_with_link_role(link.clone(), true);
    wait_for(&mut session, |event| matches!(event, Event::Ready)).await;

    let listen_port = free_port().await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, echo_port).unwrap()).await;

    link.close();
    wait_for(&mut session, |event| matches!(event, Event::PeerRulesStale)).await;
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Reconnecting))
    })
    .await;

    link.reopen();
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Connected))
    })
    .await;
    // Ready 只在配对完成时发一次，重连不再重发（SPEC §9.1 步骤 6）。
    // 新状态流上的完整快照必须包含本地规则。
    let rules = wait_for(&mut session, |event| matches!(event, Event::PeerRules(_))).await;
    let Event::PeerRules(rules) = rules else {
        panic!("期望 PeerRules 事件");
    };
    assert!(
        rules
            .iter()
            .any(|rule| rule.listen_port == listen_port && rule.peer_port == echo_port),
        "重连后 RuleList 快照应包含 {listen_port}->{echo_port}: {rules:?}"
    );

    let mut client = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("恢复后 listener 应接受连接");
    client.write_all(b"after-reconnect").await.expect("写入");
    let mut echoed = [0u8; 15];
    tokio::time::timeout(WAIT, client.read_exact(&mut echoed))
        .await
        .expect("恢复后应正常转发")
        .expect("读回 echo");
    assert_eq!(&echoed, b"after-reconnect");

    session.shutdown().await;
    echo.abort();
}

/// AuthenticationFailed 阶段立即判定会话丢失：关闭监听器、清空规则、
/// 输出可展示的中文提示，并进入 Stopped。
#[tokio::test]
async fn authentication_failure_ends_session_with_reconnect_message() {
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link.clone()).await;

    let listen_port = reserved_port(0).await;
    add_forward_and_wait(&mut session, Forward::new(listen_port, 8080).unwrap()).await;

    link.set_phase(LinkState::AuthenticationFailed);

    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::AuthenticationFailed))
    })
    .await;

    let error = wait_for(&mut session, |event| matches!(event, Event::Error(_))).await;
    let Event::Error(error) = error else {
        panic!("期望 Error 事件");
    };
    assert_eq!(error.to_string(), "会话丢失，请重新配对");

    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Stopped))
    })
    .await;
    assert!(
        session.recv().await.is_none(),
        "会话丢失后 core 应退出并关闭事件通道"
    );

    // 监听器已关闭：同一端口可以重新绑定。
    //
    // 用 bind 而不是 connect 证明端口已释放：connect 只说明“有监听者”，
    // 无法区分是我们自己的 listener 没关，还是并行测试刚抢到了同一个临时
    // 端口；bind 成功则说明监听套接字确实已经释放。
    let rebound = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match TcpListener::bind(("127.0.0.1", listen_port)).await {
                Ok(listener) => {
                    drop(listener);
                    return;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await;
    assert!(
        rebound.is_ok(),
        "会话丢失后监听器应关闭：端口 {listen_port} 在 2 秒内仍无法重新绑定"
    );
}

/// 连续 5 秒未能恢复连接 → 按「断开连接」退出 0（不是会话丢失）。
///
/// 使用暂停时钟避免真实等待 5 秒；超时路径只依赖 `tokio::time`。
#[tokio::test(start_paused = true)]
async fn reconnecting_past_five_seconds_reports_disconnected() {
    let link = Arc::new(MockLink::new());
    let mut session = ready_session(link.clone()).await;

    link.close();
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Reconnecting))
    })
    .await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match session.recv().await {
                Some(Event::Disconnected) => return,
                Some(Event::Error(error)) => {
                    panic!("5 秒断链不应报会话丢失：{error}")
                }
                Some(_) => continue,
                None => panic!("core 提前退出"),
            }
        }
    })
    .await
    .expect("5 秒重连超时应触发断开连接");

    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Stopped))
    })
    .await;
    assert!(
        session.recv().await.is_none(),
        "断开连接后 core 应退出并关闭事件通道"
    );
}
