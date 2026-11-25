//! 会话告别（ticket 16）的环回集成测试：只用 core 的公开 API 与内存链路，
//! 覆盖「对端 1 秒内结束」「5 秒内恢复继续」和「退出后端口可重绑」。
//!
//! 规格来源：`docs/SPEC.md` §5.2、§5.4、§9.2、§9.5。

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use tunnel_core::transport::mock::MockLink;
use tunnel_core::{Command, Event, Forward, LinkState, Session};

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
