//! 规则同步状态流：首字节分流、Hello/RuleList 收发与全量快照语义。
//!
//! 规格来源：`docs/SPEC.md` §4.2、§5.4、§5.5、§7.1–§7.3。
//!
//! 生命周期：
//! - `bootstrap_outbound` 为真的一端在数据链路 `Connected` 后打开**唯一**
//!   一条双向流；另一端从 `accept_bi()` 接受它。
//! - 两端都在这条流上先发 `Hello`，再发自己的全量 `RuleList` 快照；本地
//!   规则变化时重发快照。
//! - 状态流只在出错/会话结束时 `finish` + `stop`（`docs/SPEC.md` §5.5
//!   规定状态帧错误用“关闭”而不是 reset）；正常运行时绝不 `finish`。
//! - 每次接收到的 `RuleList` 都是全量替换，不是增量合并。
//!
//! 发送侧通过单槽 `watch` 通道传递快照：连续多次规则变化只会保留最新
//! 一份，天然合并；编码使用固定 16 KiB 缓冲，不会出现无界队列。

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::api::{Forward, TunnelError};
use crate::bridge;
use crate::limits::StreamBudget;
use crate::protocol::{
    RuleFrame, CODE_PROTOCOL_ERROR, DATA_MAGIC, FRAME_FAREWELL, FRAME_HEADER_LEN, FRAME_HELLO,
    FRAME_RULE_LIST,
};
use crate::transport::{open_bi_with_timeout, DataLink, TunnelRead, TunnelStream, TunnelWrite};

/// 状态流单帧最大长度，与发送侧固定缓冲一致：16 KiB。
pub(crate) const STATE_FRAME_BUFFER: usize = 16 * 1024;

/// 对端状态更新通道容量。worker 处理不过来时让状态流等待（背压）。
pub(crate) const STATE_UPDATE_CHANNEL: usize = 16;

/// 接受侧状态流分发队列容量（正常情况下最多一条）。
pub(crate) const STATE_STREAM_CHANNEL: usize = 4;

/// 状态流失败重开的最小/最大退避。
const REOPEN_BACKOFF_MIN: Duration = Duration::from_millis(100);
const REOPEN_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// 关闭状态流时等待另一方向收尾的上限。
const CLOSE_GRACE: Duration = Duration::from_millis(500);

/// 规则同步模块发给 worker 的对端规则变化。
#[derive(Debug, Clone)]
pub(crate) enum PeerUpdate {
    /// 对端最新全量快照。
    Snapshot(Vec<Forward>),
    /// 状态流失败/关闭，对端规则变为未知。
    Unknown,
}

/// 接受侧状态流的两半；由首字节分流的分类任务交给分发任务。
/// 已被分流器读掉首字节的一条状态流：`first` 必须交回帧解码器。
pub(crate) struct StateStreamParts {
    pub(crate) read: Box<dyn TunnelRead>,
    pub(crate) write: Box<dyn TunnelWrite>,
    pub(crate) first: u8,
}

/// 创建单槽快照通道：只保留最新全量快照，连续变化会被合并。
pub(crate) fn snapshot_channel() -> (watch::Sender<Vec<Forward>>, watch::Receiver<Vec<Forward>>) {
    watch::channel(Vec::new())
}

/// 在一条已打开的状态流上收发帧，直到出错、对端关闭或会话结束。
///
/// 两个方向各自持有半条流、独立收尾：任一方结束都会通知另一方关闭，
/// 最终 `finish` 写半、`stop` 读半（§5.5 的“关闭”语义）。
pub(crate) async fn run_state_stream(
    read: Box<dyn TunnelRead>,
    write: Box<dyn TunnelWrite>,
    first: Option<u8>,
    snapshots: watch::Receiver<Vec<Forward>>,
    updates: mpsc::Sender<PeerUpdate>,
) -> Result<(), TunnelError> {
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let mut writer = tokio::spawn(state_writer(write, snapshots, cancel_rx.clone()));
    let mut reader = tokio::spawn(state_reader(read, first, updates, cancel_rx));

    enum First {
        Writer(Result<Result<(), TunnelError>, tokio::task::JoinError>),
        Reader(Result<Result<(), TunnelError>, tokio::task::JoinError>),
    }

    let first = tokio::select! {
        joined = &mut writer => First::Writer(joined),
        joined = &mut reader => First::Reader(joined),
    };
    // 任一方结束：通知另一方停止，避免半条流悬挂。
    let _ = cancel_tx.send(true);

    let (result, other) = match first {
        First::Writer(joined) => (flatten(joined), &mut reader),
        First::Reader(joined) => (flatten(joined), &mut writer),
    };
    // 另一方向可能正卡在写背压上，最多等 500ms 再强制中止。
    if tokio::time::timeout(CLOSE_GRACE, &mut *other)
        .await
        .is_err()
    {
        other.abort();
        let _ = other.await;
    }
    result
}

fn flatten(
    joined: Result<Result<(), TunnelError>, tokio::task::JoinError>,
) -> Result<(), TunnelError> {
    match joined {
        Ok(result) => result,
        Err(error) => Err(TunnelError::Internal(format!("状态流任务异常：{error}"))),
    }
}

/// 状态流发送半：先 `Hello` + 全量快照，之后每次本地变化重发快照。
async fn state_writer(
    mut write: Box<dyn TunnelWrite>,
    mut snapshots: watch::Receiver<Vec<Forward>>,
    mut cancel: watch::Receiver<bool>,
) -> Result<(), TunnelError> {
    let result = state_writer_loop(&mut *write, &mut snapshots, &mut cancel).await;
    // 正常结束/出错都只关闭写方向（FIN），从不 reset（§5.5）。
    let _ = write.finish();
    result
}

async fn state_writer_loop(
    write: &mut dyn TunnelWrite,
    snapshots: &mut watch::Receiver<Vec<Forward>>,
    cancel: &mut watch::Receiver<bool>,
) -> Result<(), TunnelError> {
    let mut buffer = [0u8; STATE_FRAME_BUFFER];
    send_frame(write, &RuleFrame::Hello, &mut buffer).await?;
    let initial = snapshots.borrow_and_update().clone();
    send_frame(write, &RuleFrame::RuleList(initial), &mut buffer).await?;

    loop {
        tokio::select! {
            changed = snapshots.changed() => {
                // 发送端已释放（会话结束）：关闭写方向。
                if changed.is_err() {
                    return Ok(());
                }
                let snapshot = snapshots.borrow_and_update().clone();
                send_frame(write, &RuleFrame::RuleList(snapshot), &mut buffer).await?;
            }
            _ = cancel.changed() => return Ok(()),
        }
    }
}

/// 状态流接收半：校验帧并上报全量快照；首帧必须是 `Hello`。
async fn state_reader(
    mut read: Box<dyn TunnelRead>,
    first: Option<u8>,
    updates: mpsc::Sender<PeerUpdate>,
    mut cancel: watch::Receiver<bool>,
) -> Result<(), TunnelError> {
    let result = state_reader_loop(&mut *read, first, &updates, &mut cancel).await;
    // 不再接收：STOP_SENDING，错误码 0x01（关闭而不是 reset）。
    let _ = read.stop(CODE_PROTOCOL_ERROR);
    result
}

async fn state_reader_loop(
    read: &mut dyn TunnelRead,
    first: Option<u8>,
    updates: &mpsc::Sender<PeerUpdate>,
    cancel: &mut watch::Receiver<bool>,
) -> Result<(), TunnelError> {
    let mut buffer = [0u8; STATE_FRAME_BUFFER];
    let mut hello_seen = false;
    // 分流器可能已经读掉首字节，仅首帧需要补回。
    let mut pending_first = first;

    loop {
        let frame_first = pending_first.take();
        tokio::select! {
            frame = read_frame(read, frame_first, &mut buffer) => {
                match frame? {
                    RuleFrame::Hello => {
                        if hello_seen {
                            return Err(TunnelError::Protocol("状态流出现重复 Hello 帧".into()));
                        }
                        hello_seen = true;
                    }
                    RuleFrame::RuleList(rules) => {
                        if !hello_seen {
                            return Err(TunnelError::Protocol("状态流首帧必须是 Hello".into()));
                        }
                        updates
                            .send(PeerUpdate::Snapshot(rules))
                            .await
                            .map_err(|_| TunnelError::Internal("状态更新通道已关闭".into()))?;
                    }
                    RuleFrame::Farewell => {
                        tracing::debug!("状态流收到 Farewell，按正常结束处理");
                        return Ok(());
                    }
                }
            }
            _ = cancel.changed() => return Ok(()),
        }
    }
}

/// 编码一帧并整帧写出；状态帧很小，一次 `write_all` 足够。
async fn send_frame(
    write: &mut dyn TunnelWrite,
    frame: &RuleFrame,
    buffer: &mut [u8; STATE_FRAME_BUFFER],
) -> Result<(), TunnelError> {
    let len = frame.encode(buffer)?;
    write
        .write_all(&buffer[..len])
        .await
        .map_err(|error| TunnelError::Network(format!("写状态流失败：{error}")))
}

/// 读一整帧：先读 3 字节头，再按长度字段读 payload。
///
/// 长度字段超过 16 KiB 缓冲直接判协议错误；帧内容交给
/// [`RuleFrame::decode`] 校验（类型/版本/长度/重复端口）。
async fn read_frame(
    read: &mut dyn TunnelRead,
    first: Option<u8>,
    buffer: &mut [u8; STATE_FRAME_BUFFER],
) -> Result<RuleFrame, TunnelError> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    match first {
        Some(byte) => {
            header[0] = byte;
            read.read_exact(&mut header[1..])
                .await
                .map_err(|error| TunnelError::Network(format!("读状态流失败：{error}")))?;
        }
        None => {
            read.read_exact(&mut header)
                .await
                .map_err(|error| TunnelError::Network(format!("读状态流失败：{error}")))?;
        }
    }

    let declared = u16::from_be_bytes([header[1], header[2]]) as usize;
    if declared + FRAME_HEADER_LEN > STATE_FRAME_BUFFER {
        return Err(TunnelError::Protocol(format!(
            "状态帧长度 {declared} 超过缓冲上限 {STATE_FRAME_BUFFER}"
        )));
    }

    buffer[..FRAME_HEADER_LEN].copy_from_slice(&header);
    read.read_exact(&mut buffer[FRAME_HEADER_LEN..FRAME_HEADER_LEN + declared])
        .await
        .map_err(|error| TunnelError::Network(format!("读状态流失败：{error}")))?;

    RuleFrame::decode(&buffer[..FRAME_HEADER_LEN + declared]).map_err(TunnelError::from)
}

/// 首字节分流：`0x50` 是数据流，`0x01`/`0x02` 是状态流，`0x03` 是对端告别，其它 reset(0x01)。
///
/// 运行在每条 `accept_bi()` 流上；数据流交给 [`bridge`]，状态流交给
/// 分发任务（新状态流替换旧状态流），Farewell 通过独立的告别信号通知
/// 会话主循环（SPEC §5.2、§9.5）。
pub(crate) fn spawn_demux(
    link: Arc<dyn DataLink>,
    state: mpsc::Sender<StateStreamParts>,
    farewell: mpsc::Sender<()>,
    link_lost: watch::Receiver<u64>,
    budget: StreamBudget,
    peer_session: Arc<str>,
    lost: mpsc::Sender<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match link.accept_bi().await {
                Ok(stream) => {
                    let state = state.clone();
                    tokio::spawn(classify_stream(
                        stream,
                        state,
                        farewell.clone(),
                        link_lost.clone(),
                        budget.clone(),
                        peer_session.clone(),
                    ));
                }
                Err(error) => {
                    tracing::debug!(%error, "accept_bi 循环结束");
                    let _ = lost.send(()).await;
                    break;
                }
            }
        }
    })
}

async fn classify_stream(
    stream: Box<dyn TunnelStream>,
    state: mpsc::Sender<StateStreamParts>,
    farewell: mpsc::Sender<()>,
    link_lost: watch::Receiver<u64>,
    budget: StreamBudget,
    peer_session: Arc<str>,
) {
    let (mut read, mut write) = stream.split();
    // 首字节与剩余 5 字节共用一个截止时间（SPEC §6）。
    let started = tokio::time::Instant::now();
    let mut first = [0u8; 1];
    let read_first = tokio::time::timeout_at(
        started + bridge::HEADER_TIMEOUT,
        read.read_exact(&mut first),
    )
    .await;
    let first_byte = match read_first {
        Ok(Ok(_)) => first[0],
        _ => {
            // 首字节都读不到：协议错误，reset 数据流即可。
            let _ = write.reset(CODE_PROTOCOL_ERROR);
            return;
        }
    };

    if first_byte == DATA_MAGIC[0] {
        bridge::serve_data_stream(
            read,
            write,
            first_byte,
            started,
            link_lost,
            budget,
            peer_session,
        )
        .await;
    } else if first_byte == FRAME_HELLO || first_byte == FRAME_RULE_LIST {
        let parts = StateStreamParts {
            read,
            write,
            first: first_byte,
        };
        if state.send(parts).await.is_err() {
            tracing::debug!("状态流分发通道已关闭");
        }
    } else if first_byte == FRAME_FAREWELL {
        // Farewell 帧长度必须是 0；剩余 2 字节仍受同一个首帧截止时间约束。
        let mut rest = [0u8; 2];
        let read_rest =
            tokio::time::timeout_at(started + bridge::HEADER_TIMEOUT, read.read_exact(&mut rest))
                .await;
        if !matches!(read_rest, Ok(Ok(_))) || u16::from_be_bytes(rest) != 0 {
            let _ = write.reset(CODE_PROTOCOL_ERROR);
            return;
        }
        if farewell.send(()).await.is_err() {
            tracing::debug!("告别信号通道已关闭");
        }
        // 对端已告别：只做正常半关闭，不 reset（SPEC §5.4 关闭语义）。
        let _ = write.finish();
    } else {
        let _ = write.reset(CODE_PROTOCOL_ERROR);
    }
}

/// 接受侧一条状态流的收尾：失败时把对端规则标记为未知。
pub(crate) async fn serve_inbound_state(
    read: Box<dyn TunnelRead>,
    write: Box<dyn TunnelWrite>,
    first: u8,
    snapshots: watch::Receiver<Vec<Forward>>,
    updates: mpsc::Sender<PeerUpdate>,
) {
    match run_state_stream(read, write, Some(first), snapshots, updates.clone()).await {
        Ok(()) => {}
        Err(error) => {
            tracing::debug!(%error, "状态流结束，对端规则未知");
            let _ = updates.send(PeerUpdate::Unknown).await;
        }
    }
}

/// 打开侧监督循环：状态流出错后在链路存活期间持续重开并重发快照。
pub(crate) async fn supervise_outbound(
    link: Arc<dyn DataLink>,
    snapshots: watch::Receiver<Vec<Forward>>,
    updates: mpsc::Sender<PeerUpdate>,
) {
    let mut backoff = REOPEN_BACKOFF_MIN;
    loop {
        // 快照发送端已释放 = 会话收尾，不再重开。
        if snapshots.has_changed().is_err() {
            break;
        }

        match open_bi_with_timeout(&*link).await {
            Ok(stream) => {
                backoff = REOPEN_BACKOFF_MIN;
                let (read, write) = stream.split();
                if let Err(error) =
                    run_state_stream(read, write, None, snapshots.clone(), updates.clone()).await
                {
                    tracing::debug!(%error, "规则同步流失败，对端规则未知");
                    let _ = updates.send(PeerUpdate::Unknown).await;
                }
            }
            Err(error) => {
                tracing::debug!(%error, "打开规则同步流失败，对端规则未知");
                let _ = updates.send(PeerUpdate::Unknown).await;
            }
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(REOPEN_BACKOFF_MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{DATA_VERSION, MAX_FRAME_LEN};
    use crate::transport::mock::MockLink;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct StatePair {
        ours_read: Box<dyn TunnelRead>,
        ours_write: Box<dyn TunnelWrite>,
        peer_read: Box<dyn TunnelRead>,
        peer_write: Box<dyn TunnelWrite>,
    }

    async fn state_pair() -> StatePair {
        let (a, b) = MockLink::pair();
        let ours = a.open_bi().await.expect("本端打开状态流");
        let theirs = b.accept_bi().await.expect("对端接受状态流");
        let (ours_read, ours_write) = ours.split();
        let (peer_read, peer_write) = theirs.split();
        StatePair {
            ours_read,
            ours_write,
            peer_read,
            peer_write,
        }
    }

    fn encode(frame: &RuleFrame) -> Vec<u8> {
        let mut buffer = vec![0u8; MAX_FRAME_LEN];
        let len = frame.encode(&mut buffer).expect("编码合法帧");
        buffer.truncate(len);
        buffer
    }

    fn rule(listen_port: u16, peer_port: u16) -> Forward {
        Forward {
            listen_port,
            peer_port,
        }
    }

    async fn read_raw_frame(read: &mut Box<dyn TunnelRead>) -> (u8, Vec<u8>) {
        let mut header = [0u8; FRAME_HEADER_LEN];
        read.read_exact(&mut header).await.expect("读帧头");
        let len = u16::from_be_bytes([header[1], header[2]]) as usize;
        let mut payload = vec![0u8; len];
        read.read_exact(&mut payload).await.expect("读 payload");
        (header[0], payload)
    }

    fn decode_rule_list(payload: &[u8]) -> Vec<Forward> {
        let mut frame = vec![FRAME_RULE_LIST];
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        frame.extend_from_slice(payload);
        match RuleFrame::decode(&frame).expect("解码 RuleList") {
            RuleFrame::RuleList(rules) => rules,
            RuleFrame::Hello | RuleFrame::Farewell => panic!("期望 RuleList"),
        }
    }

    async fn expect_rule_list(read: &mut Box<dyn TunnelRead>) -> Vec<Forward> {
        let (kind, payload) = read_raw_frame(read).await;
        assert_eq!(kind, FRAME_RULE_LIST, "期望 RuleList 帧");
        decode_rule_list(&payload)
    }

    #[tokio::test]
    async fn sends_hello_then_snapshot_and_reports_peer_rules() {
        let pair = state_pair().await;
        let (snapshot_tx, snapshot_rx) = snapshot_channel();
        let (update_tx, mut update_rx) = mpsc::channel(STATE_UPDATE_CHANNEL);
        let task = tokio::spawn(run_state_stream(
            pair.ours_read,
            pair.ours_write,
            None,
            snapshot_rx,
            update_tx,
        ));
        let mut peer_read = pair.peer_read;
        let mut peer_write = pair.peer_write;

        // 本端首帧 Hello，随后是当前（空）全量快照。
        let (kind, payload) = read_raw_frame(&mut peer_read).await;
        assert_eq!(kind, FRAME_HELLO);
        assert_eq!(payload, vec![DATA_VERSION]);
        assert_eq!(expect_rule_list(&mut peer_read).await, Vec::new());

        // 对端 Hello + 一条规则 → worker 收到全量快照。
        peer_write
            .write_all(&encode(&RuleFrame::Hello))
            .await
            .expect("写 Hello");
        peer_write
            .write_all(&encode(&RuleFrame::RuleList(vec![rule(2211, 22)])))
            .await
            .expect("写 RuleList");
        match update_rx.recv().await.expect("收到对端快照") {
            PeerUpdate::Snapshot(rules) => assert_eq!(rules, vec![rule(2211, 22)]),
            PeerUpdate::Unknown => panic!("不应标记未知"),
        }

        // 本地新增规则：重发全量快照（不是增量）。
        snapshot_tx
            .send(vec![rule(2211, 22), rule(2212, 23)])
            .expect("发送快照");
        assert_eq!(
            expect_rule_list(&mut peer_read).await,
            vec![rule(2211, 22), rule(2212, 23)]
        );

        // 正常运行时状态流保持打开：对端读挂起而不是 EOF。
        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), peer_read.read(&mut byte))
                .await
                .is_err(),
            "正常运行时状态流不能被 finish"
        );

        // 会话收尾（sender 释放）后才关闭写方向。
        drop(snapshot_tx);
        task.await.expect("任务可等待").expect("正常收尾");
    }

    #[tokio::test]
    async fn rapid_changes_coalesce_to_latest_snapshot() {
        let pair = state_pair().await;
        let (snapshot_tx, snapshot_rx) = snapshot_channel();
        let (update_tx, _update_rx) = mpsc::channel(STATE_UPDATE_CHANNEL);
        let task = tokio::spawn(run_state_stream(
            pair.ours_read,
            pair.ours_write,
            None,
            snapshot_rx,
            update_tx,
        ));
        let mut peer_read = pair.peer_read;

        let (kind, _) = read_raw_frame(&mut peer_read).await;
        assert_eq!(kind, FRAME_HELLO);
        assert_eq!(expect_rule_list(&mut peer_read).await, Vec::new());

        // 两次变化紧挨着发生：允许先看到中间态，但最后一次必须是完整快照。
        snapshot_tx.send(vec![rule(2211, 22)]).expect("第一次");
        snapshot_tx
            .send(vec![rule(2211, 22), rule(2212, 23)])
            .expect("第二次");
        loop {
            let rules = expect_rule_list(&mut peer_read).await;
            if rules.len() == 2 {
                assert_eq!(rules, vec![rule(2211, 22), rule(2212, 23)]);
                break;
            }
            assert_eq!(rules, vec![rule(2211, 22)], "只允许旧的中间快照");
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_raw_frame(&mut peer_read))
                .await
                .is_err(),
            "合并后不应再发送旧快照"
        );

        drop(snapshot_tx);
        task.await.expect("任务可等待").expect("正常收尾");
    }

    /// 断言一帧畸形数据被协议拒绝，并且状态流“关闭”而不是 reset。
    async fn assert_rejected(frames: Vec<u8>, send_hello_first: bool) {
        let pair = state_pair().await;
        let (snapshot_tx, snapshot_rx) = snapshot_channel();
        let (update_tx, _update_rx) = mpsc::channel(STATE_UPDATE_CHANNEL);
        let task = tokio::spawn(run_state_stream(
            pair.ours_read,
            pair.ours_write,
            None,
            snapshot_rx,
            update_tx,
        ));
        let mut peer_read = pair.peer_read;
        let mut peer_write = pair.peer_write;

        if send_hello_first {
            peer_write
                .write_all(&encode(&RuleFrame::Hello))
                .await
                .expect("写合法 Hello");
        }
        peer_write.write_all(&frames).await.expect("写畸形帧");

        let result = task.await.expect("任务可等待");
        assert!(
            matches!(result, Err(TunnelError::Protocol(_))),
            "畸形帧必须被拒绝：{result:?}"
        );

        // 关闭语义：读半 stop(0x01)，写半 FIN（对端读到 EOF）。
        let (kind, _) = read_raw_frame(&mut peer_read).await;
        assert_eq!(kind, FRAME_HELLO);
        expect_rule_list(&mut peer_read).await;
        let mut byte = [0u8; 1];
        assert_eq!(
            peer_read.read(&mut byte).await.expect("读到 EOF"),
            0,
            "状态流应 FIN 而不是 reset"
        );
        let error = peer_write.write_all(b"x").await.expect_err("读半已 stop");
        assert!(
            error.to_string().contains("0x01"),
            "stop 错误码应为 0x01：{error}"
        );

        drop(snapshot_tx);
    }

    #[tokio::test]
    async fn malformed_frames_are_rejected_as_protocol_errors() {
        // 未知帧类型（0x03 已分配给 Farewell）。
        assert_rejected(vec![0x04, 0x00, 0x00], true).await;
        // RuleList count 与 payload 不匹配。
        assert_rejected(
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
            true,
        )
        .await;
        // 重复 listen_port。
        assert_rejected(
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
            true,
        )
        .await;
        // 长度字段超过 16 KiB 缓冲。
        assert_rejected(vec![FRAME_RULE_LIST, 0xFF, 0xFF], true).await;
        // 首帧不是 Hello。
        assert_rejected(encode(&RuleFrame::RuleList(Vec::new())), false).await;
    }

    #[tokio::test]
    async fn wrong_hello_version_is_rejected() {
        assert_rejected(vec![FRAME_HELLO, 0x00, 0x01, 0x02], true).await;
    }

    #[tokio::test]
    async fn outbound_supervisor_reopens_after_failure() {
        let (a_link, b_link) = MockLink::pair();
        let (snapshot_tx, snapshot_rx) = snapshot_channel();
        let (update_tx, mut update_rx) = mpsc::channel(STATE_UPDATE_CHANNEL);
        let task = tokio::spawn(supervise_outbound(Arc::new(a_link), snapshot_rx, update_tx));

        // 第一条状态流：完成握手后由对端关闭。
        let first = b_link.accept_bi().await.expect("第一条状态流");
        let (mut first_read, mut first_write) = first.split();
        let (kind, _) = read_raw_frame(&mut first_read).await;
        assert_eq!(kind, FRAME_HELLO);
        expect_rule_list(&mut first_read).await;
        first_write.finish().expect("对端 FIN");

        match tokio::time::timeout(Duration::from_secs(2), update_rx.recv())
            .await
            .expect("应上报未知")
            .expect("通道未关闭")
        {
            PeerUpdate::Unknown => {}
            PeerUpdate::Snapshot(rules) => panic!("不应有快照：{rules:?}"),
        }

        // 监督循环重开状态流并重发 Hello + 快照。
        let second = b_link.accept_bi().await.expect("重开的状态流");
        let (mut second_read, mut second_write) = second.split();
        let (kind, _) = read_raw_frame(&mut second_read).await;
        assert_eq!(kind, FRAME_HELLO);
        assert_eq!(expect_rule_list(&mut second_read).await, Vec::new());

        // 新流上对端的快照会正常上报。
        second_write
            .write_all(&encode(&RuleFrame::Hello))
            .await
            .expect("写 Hello");
        second_write
            .write_all(&encode(&RuleFrame::RuleList(vec![rule(3300, 3301)])))
            .await
            .expect("写 RuleList");
        match tokio::time::timeout(Duration::from_secs(2), update_rx.recv())
            .await
            .expect("应收到快照")
            .expect("通道未关闭")
        {
            PeerUpdate::Snapshot(rules) => assert_eq!(rules, vec![rule(3300, 3301)]),
            PeerUpdate::Unknown => panic!("重开后不应再未知"),
        }

        drop(snapshot_tx);
        task.abort();
        let _ = task.await;
    }

    #[tokio::test(start_paused = true)]
    async fn data_header_deadline_covers_first_byte_and_remainder() {
        let (a, b) = MockLink::pair();
        let initiator = a.open_bi().await.expect("open_bi");
        let responder = b.accept_bi().await.expect("accept_bi");
        let (mut read, mut write) = initiator.split();
        let (state_tx, _state_rx) = mpsc::channel(STATE_UPDATE_CHANNEL);
        let (farewell_tx, _farewell_rx) = mpsc::channel(1);
        let (_lost_tx, lost_rx) = watch::channel(0u64);
        let task = tokio::spawn(classify_stream(
            responder,
            state_tx,
            farewell_tx,
            lost_rx,
            StreamBudget::new(),
            Arc::from("test-session"),
        ));
        // 让 classify 在 t=0 启动并记下统一截止起点。
        tokio::task::yield_now().await;

        // 首字节 4 秒才到。
        tokio::time::advance(Duration::from_secs(4)).await;
        write.write_all(&[DATA_MAGIC[0]]).await.expect("写首字节");
        tokio::task::yield_now().await;

        // 推进到第 5 秒：统一截止时间必须在此刻触发。spinner 让运行时
        // 保持忙碌，避免暂停时钟自动跳到旧实现的第二个 5 秒预算。
        let spin = tokio::spawn(async {
            for _ in 0..200 {
                tokio::task::yield_now().await;
            }
        });
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        spin.await.expect("spinner");

        // 剩余 5 字节第 6 秒才到：不应再被接受。
        tokio::time::advance(Duration::from_secs(1)).await;
        let _ = write.write_all(&[0x32, 0x01, 0x00, 0x00, 0x16]).await;

        let mut buffer = [0u8; 8];
        let error = read
            .read(&mut buffer)
            .await
            .expect_err("6 字节头超时后应被 reset");
        assert!(
            error.to_string().contains("0x01"),
            "应 reset 0x01，实际：{error}"
        );
        task.await.expect("classify 任务结束");
    }

    /// 首字节取流：对端在专用双向流上写 `Farewell`，分流器应上报告别信号
    /// 且不 reset（SPEC §5.2、§9.5）。
    #[tokio::test]
    async fn farewell_stream_signals_session_end_without_reset() {
        let (a, b) = MockLink::pair();
        let initiator = a.open_bi().await.expect("open_bi");
        let responder = b.accept_bi().await.expect("accept_bi");
        let (mut read, mut write) = initiator.split();
        let (state_tx, _state_rx) = mpsc::channel(STATE_UPDATE_CHANNEL);
        let (farewell_tx, mut farewell_rx) = mpsc::channel(1);
        let (_lost_tx, lost_rx) = watch::channel(0u64);
        let task = tokio::spawn(classify_stream(
            responder,
            state_tx,
            farewell_tx,
            lost_rx,
            StreamBudget::new(),
            Arc::from("test-session"),
        ));

        write
            .write_all(&encode(&RuleFrame::Farewell))
            .await
            .expect("写 Farewell");
        tokio::time::timeout(Duration::from_secs(2), farewell_rx.recv())
            .await
            .expect("应上报告别")
            .expect("通道未关闭");
        task.await.expect("classify 任务结束");

        // 对端没有 reset：读半应直接读到 EOF。
        let mut buffer = [0u8; 1];
        assert_eq!(read.read(&mut buffer).await.expect("读到 EOF"), 0);
    }

    /// Farewell 的 payload 长度必须为 0；非 0 按协议错误 reset。
    #[tokio::test]
    async fn farewell_with_nonzero_length_is_reset() {
        let (a, b) = MockLink::pair();
        let initiator = a.open_bi().await.expect("open_bi");
        let responder = b.accept_bi().await.expect("accept_bi");
        let (mut read, mut write) = initiator.split();
        let (state_tx, _state_rx) = mpsc::channel(STATE_UPDATE_CHANNEL);
        let (farewell_tx, _farewell_rx) = mpsc::channel(1);
        let (_lost_tx, lost_rx) = watch::channel(0u64);
        let task = tokio::spawn(classify_stream(
            responder,
            state_tx,
            farewell_tx,
            lost_rx,
            StreamBudget::new(),
            Arc::from("test-session"),
        ));

        write
            .write_all(&[FRAME_FAREWELL, 0x00, 0x01, 0x00])
            .await
            .expect("写非法 Farewell");
        let mut buffer = [0u8; 1];
        let error = read.read(&mut buffer).await.expect_err("应 reset");
        assert!(
            error.to_string().contains("0x01"),
            "应以 0x01 reset，实际：{error}"
        );
        task.await.expect("classify 任务结束");
    }
}
