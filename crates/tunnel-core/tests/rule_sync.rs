//! Ticket 05 集成测试：两个 `Session` 经 `MockLink::pair()` 互联，验证
//! 规则同步状态流的生命周期、全量快照语义、拒绝路径与数据面隔离。
//!
//! 覆盖 `docs/SPEC.md` §4.2、§5.4、§5.5、§7.1–§7.4。

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tunnel_core::protocol::{
    RuleFrame, DATA_VERSION, FRAME_HEADER_LEN, FRAME_HELLO, FRAME_RULE_LIST, MAX_FRAME_LEN,
};
use tunnel_core::transport::mock::MockLink;
use tunnel_core::transport::{DataLink, TunnelRead};
use tunnel_core::{Command, Event, Forward, Session, TunnelError};

/// 单个测试步骤的事件等待上限。
const WAIT: Duration = Duration::from_secs(5);

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

/// 等到 `Ready`。
async fn wait_ready(session: &mut Session) {
    wait_for(session, |event| matches!(event, Event::Ready)).await;
}

/// 等到对端规则满足条件，返回该快照。
async fn wait_peer_rules(
    session: &mut Session,
    mut predicate: impl FnMut(&[Forward]) -> bool,
) -> Vec<Forward> {
    match wait_for(session, |event| match event {
        Event::PeerRules(rules) => predicate(rules),
        _ => false,
    })
    .await
    {
        Event::PeerRules(rules) => rules,
        other => panic!("期望 PeerRules，得到 {other:?}"),
    }
}

/// 等到指定监听端口出现在对端规则里。
async fn wait_peer_rule(session: &mut Session, rule: Forward) {
    wait_peer_rules(session, |rules| rules.contains(&rule)).await;
}

/// 借一个空闲端口；不保证 bind 前不被别人抢走，测试内足够。
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("借端口");
    listener.local_addr().expect("本地地址").port()
}

fn encode(frame: &RuleFrame) -> Vec<u8> {
    let mut buffer = vec![0u8; MAX_FRAME_LEN];
    let len = frame.encode(&mut buffer).expect("编码合法帧");
    buffer.truncate(len);
    buffer
}

/// 读一帧（类型 + payload）。
async fn read_frame(read: &mut Box<dyn TunnelRead>) -> (u8, Vec<u8>) {
    let mut header = [0u8; FRAME_HEADER_LEN];
    read.read_exact(&mut header).await.expect("读帧头");
    let len = u16::from_be_bytes([header[1], header[2]]) as usize;
    let mut payload = vec![0u8; len];
    read.read_exact(&mut payload).await.expect("读 payload");
    (header[0], payload)
}

/// 读一帧全量 `RuleList`。
async fn read_rule_list(read: &mut Box<dyn TunnelRead>) -> Vec<Forward> {
    let (kind, payload) = read_frame(read).await;
    assert_eq!(kind, FRAME_RULE_LIST, "Hello 之后必须是全量 RuleList");
    let mut frame = vec![FRAME_RULE_LIST];
    frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    frame.extend_from_slice(&payload);
    match RuleFrame::decode(&frame).expect("解码 RuleList") {
        RuleFrame::RuleList(rules) => rules,
        RuleFrame::Hello => panic!("期望 RuleList"),
    }
}

/// 读状态流的 Hello 与首份全量快照。
async fn expect_hello_and_snapshot(read: &mut Box<dyn TunnelRead>) -> Vec<Forward> {
    let (kind, payload) = read_frame(read).await;
    assert_eq!(kind, FRAME_HELLO, "状态流首帧必须是 Hello");
    assert_eq!(payload, vec![DATA_VERSION]);
    read_rule_list(read).await
}

/// 在测试侧起一个单连接 echo 服务，返回端口与任务。
async fn spawn_echo_once() -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("echo 监听");
    let port = listener.local_addr().expect("echo 地址").port();
    let task = tokio::spawn(async move {
        let Ok((mut tcp, _)) = listener.accept().await else {
            return;
        };
        let mut buffer = [0u8; 1024];
        loop {
            match tcp.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tcp.write_all(&buffer[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    (port, task)
}

#[tokio::test]
async fn peer_rules_converge_on_both_ends_for_multiple_rules() {
    let (a_link, b_link) = MockLink::pair();
    let mut a = Session::spawn_with_link_role(Arc::new(a_link), true);
    let mut b = Session::spawn_with_link_role(Arc::new(b_link), false);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;

    // 两端各自加一条规则，互相收敛。
    let a_rule = Forward::new(free_port(), 22).expect("A 规则");
    let b_rule = Forward::new(free_port(), 23).expect("B 规则");
    a.sender()
        .send(Command::AddForward(a_rule))
        .await
        .expect("发送 A 规则");
    b.sender()
        .send(Command::AddForward(b_rule))
        .await
        .expect("发送 B 规则");

    assert_eq!(
        wait_peer_rules(&mut b, |rules| rules == [a_rule]).await,
        vec![a_rule]
    );
    assert_eq!(
        wait_peer_rules(&mut a, |rules| rules == [b_rule]).await,
        vec![b_rule]
    );

    // A 再加一条：B 的全量快照同时包含两条（替换语义 + 多规则）。
    let a_rule2 = Forward::new(free_port(), 24).expect("A 第二条规则");
    a.sender()
        .send(Command::AddForward(a_rule2))
        .await
        .expect("发送 A 第二条规则");
    let mut expected = vec![a_rule, a_rule2];
    expected.sort_by_key(|rule| rule.listen_port);
    let converged = wait_peer_rules(&mut b, |rules| rules.len() == 2).await;
    let mut sorted = converged.clone();
    sorted.sort_by_key(|rule| rule.listen_port);
    assert_eq!(sorted, expected, "全量快照应替换旧快照");

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn peer_rule_close_emits_remote_closed_and_resets_that_rules_streams() {
    let (echo_port, echo) = spawn_echo_once().await;
    let (a_link, b_link) = MockLink::pair();
    let mut a = Session::spawn_with_link_role(Arc::new(a_link), true);
    let mut b = Session::spawn_with_link_role(Arc::new(b_link), false);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;

    let kept = Forward::new(free_port(), echo_port).expect("保留规则");
    a.sender()
        .send(Command::AddForward(kept))
        .await
        .expect("发送保留规则");
    wait_peer_rule(&mut b, kept).await;

    // 规则上的数据流先正常工作。
    let mut client = TcpStream::connect(("127.0.0.1", kept.listen_port))
        .await
        .expect("连接本机监听端口");
    client.write_all(b"ping").await.expect("写 ping");
    let mut buffer = [0u8; 4];
    client.read_exact(&mut buffer).await.expect("读 ping 回显");
    assert_eq!(&buffer, b"ping");

    // 关闭规则：对端收到 RemoteClosed，该规则的连接被 reset。
    a.sender()
        .send(Command::CloseForward {
            listen_port: kept.listen_port,
        })
        .await
        .expect("发送关闭规则");
    wait_for(&mut b, |event| {
        matches!(event, Event::RemoteClosed { listen_port } if *listen_port == kept.listen_port)
    })
    .await;
    assert!(
        wait_peer_rules(&mut b, |rules| { rules.is_empty() })
            .await
            .is_empty(),
        "关闭后对端快照不再包含该规则"
    );

    let closed = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer)).await;
    match closed {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(n)) => panic!("规则关闭后仍读到 {n} 字节"),
        Err(_) => panic!("规则关闭后连接未断开"),
    }

    a.shutdown().await;
    b.shutdown().await;
    echo.await.expect("echo 任务");
}

#[tokio::test]
async fn state_stream_stays_open_and_is_the_only_one() {
    let (a_link, b_link) = MockLink::pair();
    let mut a = Session::spawn_with_link_role(Arc::new(a_link), true);
    wait_ready(&mut a).await;

    // 引导端只打开一条状态流；对端正常接收 Hello + 空快照。
    let state = b_link.accept_bi().await.expect("唯一一条状态流");
    let (mut read, _write) = state.split();
    assert!(expect_hello_and_snapshot(&mut read).await.is_empty());

    // 正常运行时状态流不 FIN：对端读挂起。
    let mut byte = [0u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(200), read.read(&mut byte))
            .await
            .is_err(),
        "正常运行时状态流不能被 finish"
    );
    // 不会再打开第二条状态流。
    assert!(
        tokio::time::timeout(Duration::from_millis(200), b_link.accept_bi())
            .await
            .is_err(),
        "同一时刻只能有一条状态流"
    );
    // 也没有未知/错误事件。
    assert!(
        tokio::time::timeout(Duration::from_millis(100), a.recv())
            .await
            .is_err(),
        "正常运行时不应产生额外事件"
    );

    a.shutdown().await;
}

/// 首字节分流：既不是 0x50 也不是状态帧的流被 reset(0x01)，其它流不受影响。
#[tokio::test]
async fn unknown_first_byte_is_reset_without_disturbing_other_streams() {
    let (a_link, b_link) = MockLink::pair();
    let mut a = Session::spawn_with_link_role(Arc::new(a_link), true);
    wait_ready(&mut a).await;

    // 先接住本端状态流，避免它干扰后面的 accept。
    let state = b_link.accept_bi().await.expect("状态流");
    let (mut state_read, mut state_write) = state.split();
    expect_hello_and_snapshot(&mut state_read).await;

    // 另一条流首字节非法：被 reset，错误码 0x01。
    let bogus = b_link.open_bi().await.expect("打开非法流");
    let (mut bogus_read, mut bogus_write) = bogus.split();
    bogus_write.write_all(&[0x7F]).await.expect("写非法首字节");
    let mut buffer = [0u8; 4];
    let error = bogus_read
        .read(&mut buffer)
        .await
        .expect_err("非法首字节应 reset");
    assert!(
        error.to_string().contains("0x01"),
        "错误码应为 0x01：{error}"
    );

    // 状态流仍然可用：规则同步未被这条非法流影响。
    let peer_rule = Forward {
        listen_port: 9091,
        peer_port: 22,
    };
    state_write
        .write_all(&encode(&RuleFrame::Hello))
        .await
        .expect("写 Hello");
    state_write
        .write_all(&encode(&RuleFrame::RuleList(vec![peer_rule])))
        .await
        .expect("写快照");
    wait_peer_rule(&mut a, peer_rule).await;

    a.shutdown().await;
}

/// 两条规则同时存在：关闭一条只影响那一条，另一条继续工作。
#[tokio::test]
async fn closing_one_of_two_rules_keeps_the_other() {
    let (echo_port, echo) = spawn_echo_once().await;
    let (a_link, b_link) = MockLink::pair();
    let mut a = Session::spawn_with_link_role(Arc::new(a_link), true);
    let mut b = Session::spawn_with_link_role(Arc::new(b_link), false);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;

    let doomed = Forward::new(free_port(), echo_port).expect("待关规则");
    let kept = Forward::new(free_port(), echo_port).expect("保留规则");
    for rule in [doomed, kept] {
        a.sender()
            .send(Command::AddForward(rule))
            .await
            .expect("发送规则");
    }
    let seen = wait_peer_rules(&mut b, |rules| rules.len() == 2).await;
    let mut sorted = seen.clone();
    sorted.sort_by_key(|rule| rule.listen_port);
    let mut expected = vec![doomed, kept];
    expected.sort_by_key(|rule| rule.listen_port);
    assert_eq!(sorted, expected, "B 应看到 A 的两条规则");

    // 保留规则上的连接先正常工作。
    let mut client = TcpStream::connect(("127.0.0.1", kept.listen_port))
        .await
        .expect("连接保留规则");
    client.write_all(b"ping").await.expect("写 ping");
    let mut buffer = [0u8; 4];
    client.read_exact(&mut buffer).await.expect("读回显");
    assert_eq!(&buffer, b"ping");

    a.sender()
        .send(Command::CloseForward {
            listen_port: doomed.listen_port,
        })
        .await
        .expect("关闭一条规则");
    wait_for(&mut b, |event| {
        matches!(event, Event::RemoteClosed { listen_port } if *listen_port == doomed.listen_port)
    })
    .await;
    assert_eq!(
        wait_peer_rules(&mut b, |rules| rules.len() == 1).await,
        vec![kept],
        "关闭后只剩保留规则"
    );

    // 保留下来的规则不受影响：同一条连接仍能收发。
    client.write_all(b"pong").await.expect("再次写");
    let mut buffer = [0u8; 4];
    client.read_exact(&mut buffer).await.expect("再次读回显");
    assert_eq!(&buffer, b"pong");

    a.shutdown().await;
    b.shutdown().await;
    echo.await.expect("echo 任务");
}

/// 双方都不能关闭对方持有的规则：close 只作用于本端 `local` 规则。
#[tokio::test]
async fn each_side_can_only_close_its_own_rules() {
    let (a_link, b_link) = MockLink::pair();
    let mut a = Session::spawn_with_link_role(Arc::new(a_link), true);
    let mut b = Session::spawn_with_link_role(Arc::new(b_link), false);
    wait_ready(&mut a).await;
    wait_ready(&mut b).await;

    let a_rule = Forward::new(free_port(), 22).expect("A 规则");
    let b_rule = Forward::new(free_port(), 23).expect("B 规则");
    a.sender()
        .send(Command::AddForward(a_rule))
        .await
        .expect("A 加规则");
    b.sender()
        .send(Command::AddForward(b_rule))
        .await
        .expect("B 加规则");
    let b_rule_on_a = wait_peer_rules(&mut a, |rules| rules == [b_rule]).await;
    let a_rule_on_b = wait_peer_rules(&mut b, |rules| rules == [a_rule]).await;

    // A 试图关闭 B 的规则 → RuleNotFound，B 的规则仍在。
    a.sender()
        .send(Command::CloseForward {
            listen_port: b_rule.listen_port,
        })
        .await
        .expect("A 发送关闭");
    wait_for(&mut a, |event| {
        matches!(event, Event::Error(TunnelError::RuleNotFound { listen_port }) if *listen_port == b_rule.listen_port)
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), a.recv())
            .await
            .is_err(),
        "关闭对端规则不应改变本端看到的对端规则"
    );
    assert_eq!(b_rule_on_a, vec![b_rule], "B 的规则不能被 A 关闭");

    // 反向同理。
    b.sender()
        .send(Command::CloseForward {
            listen_port: a_rule.listen_port,
        })
        .await
        .expect("B 发送关闭");
    wait_for(&mut b, |event| {
        matches!(event, Event::Error(TunnelError::RuleNotFound { listen_port }) if *listen_port == a_rule.listen_port)
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), b.recv())
            .await
            .is_err(),
        "关闭对端规则不应改变本端看到的对端规则"
    );
    assert_eq!(a_rule_on_b, vec![a_rule], "A 的规则不能被 B 关闭");

    a.shutdown().await;
    b.shutdown().await;
}

/// 全量快照语义：同一 listen_port 仅 `peer_port` 变化时只更新显示，
/// 不能误报 `RemoteClosed`。
#[tokio::test]
async fn peer_port_change_updates_display_without_remote_closed() {
    let (a_link, b_link) = MockLink::pair();
    let mut a = Session::spawn_with_link_role(Arc::new(a_link), true);
    wait_ready(&mut a).await;

    let state = b_link.accept_bi().await.expect("状态流");
    let (mut state_read, mut state_write) = state.split();
    expect_hello_and_snapshot(&mut state_read).await;

    let before = Forward::new(2211, 22).expect("旧映射");
    state_write
        .write_all(&encode(&RuleFrame::Hello))
        .await
        .expect("写 Hello");
    state_write
        .write_all(&encode(&RuleFrame::RuleList(vec![before])))
        .await
        .expect("写旧快照");
    wait_peer_rule(&mut a, before).await;

    // 同一 listen_port，目标端口变了：只更新显示。
    let after = Forward::new(2211, 23).expect("新映射");
    state_write
        .write_all(&encode(&RuleFrame::RuleList(vec![after])))
        .await
        .expect("写新快照");
    wait_for(
        &mut a,
        |event| matches!(event, Event::PeerRules(rules) if rules == &[after]),
    )
    .await;

    // 没有 RemoteClosed：端口一直存在。
    assert!(
        tokio::time::timeout(Duration::from_millis(100), a.recv())
            .await
            .is_err(),
        "仅 peer_port 变化不应触发 RemoteClosed"
    );

    a.shutdown().await;
}

/// 手写畸形帧：对端在握手后发送，状态流必须被拒绝，数据面不受影响。
async fn run_malformed_case(bad: Vec<u8>) {
    let (a_link, b_link) = MockLink::pair();
    let mut a = Session::spawn_with_link_role(Arc::new(a_link), true);
    wait_ready(&mut a).await;

    let state = b_link.accept_bi().await.expect("状态流");
    let (mut state_read, mut state_write) = state.split();
    expect_hello_and_snapshot(&mut state_read).await;
    let peer_rule = Forward {
        listen_port: 9090,
        peer_port: 22,
    };
    state_write
        .write_all(&encode(&RuleFrame::Hello))
        .await
        .expect("写 Hello");
    state_write
        .write_all(&encode(&RuleFrame::RuleList(vec![peer_rule])))
        .await
        .expect("写 RuleList");
    wait_peer_rule(&mut a, peer_rule).await;

    state_write.write_all(&bad).await.expect("写畸形帧");
    wait_for(&mut a, |event| matches!(event, Event::PeerRulesUnknown)).await;

    // 数据面仍然可用：加规则、走真实 TCP、对端 echo。
    let rule = Forward::new(free_port(), 8080).expect("数据规则");
    a.sender()
        .send(Command::AddForward(rule))
        .await
        .expect("发送数据规则");
    wait_for(
        &mut a,
        |event| matches!(event, Event::ListenerStarted(port) if *port == rule.listen_port),
    )
    .await;

    // 恢复路径：引导端重开状态流并重发本端全量快照；对端也重发规则后
    // 本端重新看到对端规则（`PeerRules` 回推）。
    //
    // 分流需要先读首字节，因此这里把首字节补回 `Hello` 帧的剩余部分。
    let (mut fresh_read, mut fresh_write) = loop {
        let stream = b_link.accept_bi().await.expect("接受重开的状态流");
        let (mut read, write) = stream.split();
        let mut first = [0u8; 1];
        read.read_exact(&mut first).await.expect("读首字节");
        if first[0] == FRAME_HELLO {
            let mut len = [0u8; 2];
            read.read_exact(&mut len).await.expect("读 Hello 长度");
            assert_eq!(u16::from_be_bytes(len), 1, "Hello 长度为 1");
            let mut version = [0u8; 1];
            read.read_exact(&mut version).await.expect("读 Hello 版本");
            assert_eq!(version, [DATA_VERSION], "Hello 版本必须为 0x01");
            break (read, write);
        }
    };
    let resent = read_rule_list(&mut fresh_read).await;
    assert_eq!(resent, vec![rule], "重开状态流应重发当前全量快照");
    assert_eq!(resent, vec![rule], "重开状态流应重发当前全量快照");
    fresh_write
        .write_all(&encode(&RuleFrame::Hello))
        .await
        .expect("写 Hello");
    fresh_write
        .write_all(&encode(&RuleFrame::RuleList(vec![peer_rule])))
        .await
        .expect("写 RuleList");
    wait_peer_rule(&mut a, peer_rule).await;

    let mut client = TcpStream::connect(("127.0.0.1", rule.listen_port))
        .await
        .expect("连接本机监听端口");

    // 接受下一条数据流；期间再次重开的状态流会被丢弃。
    let (mut data_read, mut data_write) = loop {
        let stream = b_link.accept_bi().await.expect("接受数据流");
        let (mut read, write) = stream.split();
        let mut first = [0u8; 1];
        read.read_exact(&mut first).await.expect("读首字节");
        if first[0] == b'P' {
            break (read, write);
        }
    };
    let mut rest = [0u8; 5];
    data_read.read_exact(&mut rest).await.expect("读剩余头");
    assert_eq!(u16::from_be_bytes([rest[3], rest[4]]), 8080);

    client.write_all(b"ping").await.expect("写数据");
    let mut payload = [0u8; 4];
    data_read.read_exact(&mut payload).await.expect("读数据");
    assert_eq!(&payload, b"ping");
    data_write.write_all(b"pong").await.expect("回写数据");
    let mut back = [0u8; 4];
    client.read_exact(&mut back).await.expect("读回写");
    assert_eq!(&back, b"pong");

    a.shutdown().await;
}

#[tokio::test]
async fn malformed_state_frames_are_rejected_but_data_keeps_flowing() {
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("未知帧类型", vec![0x03, 0x00, 0x00]),
        (
            "RuleList count 与 payload 不符",
            vec![
                FRAME_RULE_LIST,
                0x00,
                0x06,
                0x00,
                0x02,
                0x08,
                0xAB,
                0x00,
                0x16,
            ],
        ),
        (
            "重复 listen_port",
            vec![
                FRAME_RULE_LIST,
                0x00,
                0x0A,
                0x00,
                0x02,
                0x08,
                0xAB,
                0x00,
                0x16,
                0x08,
                0xAB,
                0x00,
                0x17,
            ],
        ),
        ("长度超过 16 KiB 缓冲", vec![FRAME_RULE_LIST, 0xFF, 0xFF]),
        ("Hello 版本错误", vec![FRAME_HELLO, 0x00, 0x01, 0x02]),
    ];
    for (name, bad) in cases {
        println!("畸形帧用例：{name}");
        run_malformed_case(bad).await;
    }
}
