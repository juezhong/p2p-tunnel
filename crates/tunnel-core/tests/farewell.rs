//! 会话告别（ticket 16/17）的环回集成测试：只用 core 的公开 API 与内存链路，
//! 覆盖「对端 1 秒内结束」「5 秒内恢复继续」「退出后端口可重绑」，以及
//! ticket 17 的边界：状态流上的告别，畸形/截断 Farewell 不影响数据面、
//! 不误报退出，状态流失效后断链仍按 5 秒判定收尾。
//!
//! 规格来源：`docs/SPEC.md` §5.2、§5.4、§9.2、§9.5。

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use tunnel_core::protocol::{RuleFrame, FRAME_FAREWELL, MAX_FRAME_LEN};
use tunnel_core::transport::mock::MockLink;
use tunnel_core::transport::{DataLink, TunnelRead, TunnelWrite};
use tunnel_core::{Command, Event, Forward, LinkState, Session, TunnelError};

/// 单个事件的等待上限；真实环回不应超过这个时间。
const WAIT: Duration = Duration::from_secs(10);

async fn next_event(session: &mut Session) -> Event {
    tokio::time::timeout(WAIT, session.recv())
        .await
        .expect("等待事件超时")
        .expect("会话事件通道已关闭")
}

async fn wait_for<F>(session: &mut Session, mut predicate: F) -> Event
where
    F: FnMut(&Event) -> bool,
{
    for _ in 0..100 {
        let event = next_event(session).await;
        if predicate(&event) {
            return event;
        }
    }
    panic!("没有等到期望的事件");
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("绑定临时端口");
    listener.local_addr().expect("读取端口").port()
}

/// 回显服务：读到什么写回什么。
async fn spawn_echo() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定回显服务");
    let port = listener.local_addr().expect("读取端口").port();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buffer = [0u8; 1024];
                while let Ok(read) = socket.read(&mut buffer).await {
                    if read == 0 {
                        break;
                    }
                    if socket.write_all(&buffer[..read]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    port
}

/// 正常告别：A 退出 → B 1 秒内收到 Farewell，活动连接被关闭，B 不重连。
#[tokio::test]
async fn farewell_ends_peer_session_and_resets_active_tcp() {
    let (link_a, link_b) = MockLink::pair();
    let mut a = Session::spawn_with_link(Arc::new(link_a));
    let mut b = Session::spawn_with_link(Arc::new(link_b));
    wait_for(&mut a, |event| matches!(event, Event::Ready)).await;
    wait_for(&mut b, |event| matches!(event, Event::Ready)).await;

    let echo_port = spawn_echo().await;
    let listen_port = free_port();
    b.sender()
        .send(Command::AddForward(Forward {
            listen_port,
            peer_port: echo_port,
        }))
        .await
        .expect("channel open");
    wait_for(
        &mut b,
        |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
    )
    .await;

    let mut client = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("连接本机 listener");
    client.write_all(b"ping").await.expect("写入");
    let mut buffer = [0u8; 4];
    let read = tokio::time::timeout(WAIT, client.read(&mut buffer))
        .await
        .expect("等待回显超时")
        .expect("读取回显");
    assert_eq!(&buffer[..read], b"ping");

    a.shutdown().await;

    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for(&mut b, |event| matches!(event, Event::PeerFarewell)),
    )
    .await
    .expect("对端应在 1 秒内识别 Farewell");

    let mut tail = [0u8; 1];
    let closed = tokio::time::timeout(WAIT, client.read(&mut tail)).await;
    assert!(
        matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
        "活动连接应被关闭，实际 {closed:?}"
    );

    wait_for(&mut b, |event| {
        matches!(event, Event::LinkState(LinkState::Stopped))
    })
    .await;
    let trailing = tokio::time::timeout(Duration::from_secs(1), b.recv()).await;
    assert!(
        matches!(trailing, Ok(None)),
        "对端会话应已结束：{trailing:?}"
    );
}

/// 对端 `Farewell` 之后不得再上报 `Reconnecting`：结束路径优先于重连判定。
#[tokio::test]
async fn peer_farewell_suppresses_reconnecting_after_end() {
    let (link_a, link_b) = MockLink::pair();
    // 保留 B 端链路句柄：收到 Farewell 后模拟收尾期间的链路抖动。
    let link_b = Arc::new(link_b);
    let mut a = Session::spawn_with_link(Arc::new(link_a));
    let mut b = Session::spawn_with_link(link_b.clone());
    wait_for(&mut a, |event| matches!(event, Event::Ready)).await;
    wait_for(&mut b, |event| matches!(event, Event::Ready)).await;

    a.shutdown().await;

    let mut seen_farewell = false;
    loop {
        let event = tokio::time::timeout(WAIT, b.recv())
            .await
            .expect("等待事件超时");
        let Some(event) = event else {
            break;
        };
        match event {
            Event::PeerFarewell => {
                seen_farewell = true;
                // 收尾期间链路抖动：结束后不得再被解释为「重连中」。
                link_b.set_phase(LinkState::Reconnecting);
            }
            Event::LinkState(LinkState::Reconnecting) if seen_farewell => {
                panic!("PeerFarewell 之后不应再出现 LinkState(Reconnecting)");
            }
            Event::Disconnected if seen_farewell => {
                panic!("PeerFarewell 之后不应再出现 Disconnected");
            }
            _ => {}
        }
    }
    assert!(seen_farewell, "应收到 PeerFarewell");
}

/// 抖动恢复：重连 1 秒内恢复则继续工作，不按断开连接退出。
#[tokio::test]
async fn reconnect_within_five_seconds_keeps_session_alive() {
    let link = Arc::new(MockLink::new());
    let mut session = Session::spawn_with_link(link.clone());
    wait_for(&mut session, |event| matches!(event, Event::Ready)).await;

    link.set_phase(LinkState::Reconnecting);
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Reconnecting))
    })
    .await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    link.set_phase(LinkState::Connected);
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Connected))
    })
    .await;

    let listen_port = free_port();
    session
        .sender()
        .send(Command::AddForward(Forward {
            listen_port,
            peer_port: 22,
        }))
        .await
        .expect("恢复后会话应仍然可用");
    wait_for(
        &mut session,
        |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
    )
    .await;
    session.shutdown().await;

    // 退出清理完成后同一端口可以立即重绑。
    let rebound = TcpListener::bind(("127.0.0.1", listen_port)).await;
    assert!(
        rebound.is_ok(),
        "退出后应立即能重绑端口 {listen_port}：{:?}",
        rebound.err()
    );
}

/// 把一帧编码成独立字节串。
fn encode(frame: &RuleFrame) -> Vec<u8> {
    let mut buffer = vec![0u8; MAX_FRAME_LEN];
    let len = frame.encode(&mut buffer).expect("编码合法帧");
    buffer.truncate(len);
    buffer
}

/// 打开一条状态流并写入合法 Hello + 全量快照；读半交给调用方保持存活。
async fn inject_state_stream(
    link: &MockLink,
    rules: Vec<Forward>,
) -> (Box<dyn TunnelRead>, Box<dyn TunnelWrite>) {
    let stream = link.open_bi().await.expect("打开状态流");
    let (read, mut write) = stream.split();
    write
        .write_all(&encode(&RuleFrame::Hello))
        .await
        .expect("写 Hello");
    write
        .write_all(&encode(&RuleFrame::RuleList(rules)))
        .await
        .expect("写 RuleList");
    (read, write)
}

/// 一次 TCP 回显往返。
async fn echo_roundtrip(client: &mut TcpStream, payload: &[u8]) {
    client.write_all(payload).await.expect("写入回显请求");
    let mut echoed = vec![0u8; payload.len()];
    tokio::time::timeout(WAIT, client.read_exact(&mut echoed))
        .await
        .expect("等待回显超时")
        .expect("读取回显");
    assert_eq!(echoed, payload, "回显内容必须与请求一致");
}

/// 在窗口期内断言没有会话级退出事件（不误报「对端已退出」/「断开连接」）。
async fn assert_no_exit_events(session: &mut Session, window: Duration) {
    let deadline = tokio::time::Instant::now() + window;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        match tokio::time::timeout(remaining, session.recv()).await {
            Err(_) => return,
            Ok(None) => panic!("会话在畸形 Farewell 后意外结束"),
            Ok(Some(event)) => assert!(
                !matches!(
                    event,
                    Event::PeerFarewell
                        | Event::Disconnected
                        | Event::Error(TunnelError::SessionLost)
                ),
                "畸形 Farewell 不应触发会话级动作，实际事件：{event:?}"
            ),
        }
    }
}

/// 状态流上的合法 Farewell 与专用告别流一致：结束会话，而不是只结束状态流。
#[tokio::test]
async fn state_stream_farewell_ends_session() {
    let link = Arc::new(MockLink::new());
    let mut session = Session::spawn_with_link(link.clone());
    wait_for(&mut session, |event| matches!(event, Event::Ready)).await;

    let peer_rule = Forward {
        listen_port: 9050,
        peer_port: 22,
    };
    let (_peer_read, mut state_write) = inject_state_stream(&link, vec![peer_rule]).await;
    wait_for(
        &mut session,
        |event| matches!(event, Event::PeerRules(rules) if rules.contains(&peer_rule)),
    )
    .await;

    state_write
        .write_all(&encode(&RuleFrame::Farewell))
        .await
        .expect("写 Farewell");

    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for(&mut session, |event| matches!(event, Event::PeerFarewell)),
    )
    .await
    .expect("状态流上的 Farewell 应在 1 秒内结束会话");

    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Stopped))
    })
    .await;
    assert!(
        session.recv().await.is_none(),
        "状态流告别后 core 应退出并关闭事件通道"
    );
}

/// 状态流上的畸形 Farewell（长度非 0、截断）只让对端规则未知：
/// 已有转发继续收发，新连接仍能建立，不产生退出事件。
#[tokio::test]
async fn malformed_farewell_on_state_stream_keeps_forwarding() {
    let echo_port = spawn_echo().await;
    let link = Arc::new(MockLink::new());
    let mut session = Session::spawn_with_link(link.clone());
    wait_for(&mut session, |event| matches!(event, Event::Ready)).await;

    let listen_port = free_port();
    session
        .sender()
        .send(Command::AddForward(Forward {
            listen_port,
            peer_port: echo_port,
        }))
        .await
        .expect("发送规则");
    wait_for(
        &mut session,
        |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
    )
    .await;

    // 注入畸形帧之前，先证明该转发已经能收发。
    let mut existing = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("连接本机 listener");
    echo_roundtrip(&mut existing, b"before").await;

    // 用例一：0x03 声明长度为 1 且带 1 字节 payload。
    let (_peer_read, mut state_write) = inject_state_stream(&link, Vec::new()).await;
    wait_for(&mut session, |event| matches!(event, Event::PeerRules(_))).await;
    state_write
        .write_all(&[FRAME_FAREWELL, 0x00, 0x01, 0x00])
        .await
        .expect("写长度非 0 的 Farewell");
    wait_for(&mut session, |event| {
        matches!(event, Event::PeerRulesUnknown)
    })
    .await;

    echo_roundtrip(&mut existing, b"after-nonzero").await;
    let mut fresh = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("状态流失败后仍应接受新连接");
    echo_roundtrip(&mut fresh, b"fresh-nonzero").await;

    // 用例二：0x03 声明长度 1 但只写帧头就 FIN（截断）。
    let (_peer_read, mut state_write) = inject_state_stream(&link, Vec::new()).await;
    wait_for(&mut session, |event| matches!(event, Event::PeerRules(_))).await;
    state_write
        .write_all(&[FRAME_FAREWELL, 0x00, 0x01])
        .await
        .expect("写截断的 Farewell");
    state_write.finish().expect("结束状态流发送方向");
    wait_for(&mut session, |event| {
        matches!(event, Event::PeerRulesUnknown)
    })
    .await;

    echo_roundtrip(&mut existing, b"after-truncated").await;
    let mut fresh = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("截断 Farewell 后仍应接受新连接");
    echo_roundtrip(&mut fresh, b"fresh-truncated").await;

    assert_no_exit_events(&mut session, Duration::from_millis(500)).await;
    session.shutdown().await;
}

/// 专用 0x03 流上的畸形 Farewell 被 reset(0x01)，会话与数据面不受影响。
#[tokio::test]
async fn malformed_dedicated_farewell_is_reset_and_keeps_forwarding() {
    let echo_port = spawn_echo().await;
    let link = Arc::new(MockLink::new());
    let mut session = Session::spawn_with_link(link.clone());
    wait_for(&mut session, |event| matches!(event, Event::Ready)).await;

    let listen_port = free_port();
    session
        .sender()
        .send(Command::AddForward(Forward {
            listen_port,
            peer_port: echo_port,
        }))
        .await
        .expect("发送规则");
    wait_for(
        &mut session,
        |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
    )
    .await;

    let mut existing = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("连接本机 listener");
    echo_roundtrip(&mut existing, b"before").await;

    // 用例一：长度非 0，写方应观察到 reset(0x01)。
    let stream = link.open_bi().await.expect("打开专用告别流");
    let (mut read, mut write) = stream.split();
    write
        .write_all(&[FRAME_FAREWELL, 0x00, 0x01, 0x00])
        .await
        .expect("写长度非 0 的 Farewell");
    let mut byte = [0u8; 1];
    let reset = tokio::time::timeout(WAIT, read.read(&mut byte))
        .await
        .expect("等待 reset 超时")
        .expect_err("长度非 0 的 Farewell 必须被 reset");
    assert_eq!(reset.kind(), std::io::ErrorKind::ConnectionReset);

    // 用例二：只有类型字节就 FIN（截断）。
    let stream = link.open_bi().await.expect("打开专用告别流");
    let (mut read, mut write) = stream.split();
    write
        .write_all(&[FRAME_FAREWELL])
        .await
        .expect("写截断的 Farewell");
    write.finish().expect("结束发送方向");
    let reset = tokio::time::timeout(WAIT, read.read(&mut byte))
        .await
        .expect("等待 reset 超时")
        .expect_err("截断的 Farewell 必须被 reset");
    assert_eq!(reset.kind(), std::io::ErrorKind::ConnectionReset);

    echo_roundtrip(&mut existing, b"after").await;
    let mut fresh = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("畸形告别流后仍应接受新连接");
    echo_roundtrip(&mut fresh, b"fresh").await;

    assert_no_exit_events(&mut session, Duration::from_millis(500)).await;
    session.shutdown().await;
}

/// 状态流已失败（对端规则未知）后真实断链：仍走 5 秒判定报「断开连接」，
/// 不误报告别，退出后监听端口可立即重绑（无残留 listener/任务）。
#[tokio::test]
async fn state_stream_failure_then_disconnect_uses_five_second_path() {
    let link = Arc::new(MockLink::new());
    let mut session = Session::spawn_with_link(link.clone());
    wait_for(&mut session, |event| matches!(event, Event::Ready)).await;

    // 不需要真的转发流量，只为验证 listener 在退出时被清理。
    let listen_port = free_port();
    session
        .sender()
        .send(Command::AddForward(Forward {
            listen_port,
            peer_port: 22,
        }))
        .await
        .expect("发送规则");
    wait_for(
        &mut session,
        |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
    )
    .await;

    let (_peer_read, mut state_write) = inject_state_stream(&link, Vec::new()).await;
    wait_for(&mut session, |event| matches!(event, Event::PeerRules(_))).await;
    state_write
        .write_all(&[FRAME_FAREWELL, 0x00, 0x01, 0x00])
        .await
        .expect("写畸形 Farewell");
    wait_for(&mut session, |event| {
        matches!(event, Event::PeerRulesUnknown)
    })
    .await;

    link.close();
    wait_for(&mut session, |event| {
        matches!(event, Event::LinkState(LinkState::Reconnecting))
    })
    .await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match session.recv().await {
                Some(Event::Disconnected) => return,
                Some(Event::PeerFarewell) => {
                    panic!("状态流失败后的断链不应误报「对端已退出」")
                }
                Some(Event::Error(error)) => panic!("断链不应报会话丢失：{error}"),
                Some(_) => continue,
                None => panic!("core 提前退出"),
            }
        }
    })
    .await
    .expect("5 秒断链超时应触发断开连接");

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
        "断开连接后应立即能重绑端口 {listen_port}：{:?}",
        rebound.err()
    );
}
