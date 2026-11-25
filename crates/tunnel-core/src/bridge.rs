//! TCP ↔ 数据流 bridge：6 字节头、目标拨号与手动双泵。
//!
//! 规格来源：`docs/SPEC.md` §5.2、§5.3、§6。本模块只负责单条连接的
//! 字节搬运；listener / 规则 / 事件由 [`crate::session`] 负责。
//!
//! 双泵不使用 `tokio::io::copy_bidirectional`：两个方向各自持有固定
//! 16 KiB 缓冲，读一次、写一次，靠 `await` 施加背压。一个方向正常 EOF
//! 只结束自己的发送方向；错误（RST/STOP）或规则关闭才中止另一方向，
//! 且必须显式 reset/stop——不能依赖流 Drop 时的隐式 finish（`docs/SPEC.md`
//! §5.3；RST/STOP 的完整矩阵归 ticket 04）。

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::api::{LinkState, TunnelError};
use crate::limits::StreamBudget;
use crate::protocol::{
    DataHeader, CODE_CANCELLED, CODE_LIMIT_EXCEEDED, CODE_PROTOCOL_ERROR, CODE_TARGET_DIAL_FAILED,
    CODE_TCP_RESET, DATA_HEADER_LEN,
};
#[cfg(test)]
use crate::transport::TunnelStream;
use crate::transport::{DataLink, TunnelRead, TunnelWrite};

/// 每个泵方向持有的固定缓冲大小（`docs/SPEC.md` §6.2）。
pub(crate) const BRIDGE_BUFFER: usize = 16 * 1024;

/// 目标端读取 6 字节数据流头的上限（`docs/SPEC.md` §6.1）。
pub(crate) const HEADER_TIMEOUT: Duration = Duration::from_secs(5);

/// 等数据链路进入 `Connected`；超时或提前释放返回 `false`。
///
/// 重连期间新接受的 TCP 最多等 5 秒（`docs/SPEC.md` §9.1），由调用方
/// 决定超时后关闭该 TCP，避免无限堆积。
pub(crate) async fn wait_for_link_connected(
    link: &mut watch::Receiver<LinkState>,
    limit: Duration,
) -> bool {
    let wait = async {
        loop {
            if *link.borrow_and_update() == LinkState::Connected {
                return true;
            }
            if link.changed().await.is_err() {
                // 发送端已释放：保持当前阶段，视为不可用。
                return false;
            }
        }
    };
    tokio::time::timeout(limit, wait).await.unwrap_or(false)
}

/// 目标端拨号 `127.0.0.1:<port>` 的上限（`docs/SPEC.md` §6.1）。
pub(crate) const TARGET_DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// 在 5 秒内读满并校验 6 字节数据流头（测试入口）。
#[cfg(test)]
pub(crate) async fn read_data_header<R>(read: &mut R) -> Result<DataHeader, TunnelError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    read_data_header_bytes(read, None, tokio::time::Instant::now()).await
}

/// 读满 6 字节头并校验；`started` 是首字节到达前的统一截止起点。
///
/// `first_byte` 为 `Some` 时表示分流器已经消费了首字节，只读剩余 5 字节。
/// 首字节与剩余字节共用同一个 `HEADER_TIMEOUT`，否则两端各等 5 秒会让
/// 6 字节头最长拖到 10 秒，违反 SPEC §6。
async fn read_data_header_bytes<R>(
    read: &mut R,
    first_byte: Option<u8>,
    started: tokio::time::Instant,
) -> Result<DataHeader, TunnelError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut bytes = [0u8; DATA_HEADER_LEN];
    let offset = match first_byte {
        Some(byte) => {
            bytes[0] = byte;
            1
        }
        None => 0,
    };
    tokio::time::timeout_at(
        started + HEADER_TIMEOUT,
        read.read_exact(&mut bytes[offset..]),
    )
    .await
    .map_err(|_| TunnelError::Timeout {
        operation: "读取数据流头",
    })?
    .map_err(|error| TunnelError::Network(format!("读取数据流头失败：{error}")))?;
    DataHeader::decode(bytes).map_err(TunnelError::from)
}

/// 头解析失败时的统一收尾：审计日志 + `reset(0x01)`。
fn reset_bad_header(write: &mut dyn TunnelWrite, error: &TunnelError) {
    tracing::debug!(%error, "数据流头无效");
    let _ = write.reset(CODE_PROTOCOL_ERROR);
}

/// 处理对端在数据链路上打开的一条流：读头、限额、拨号、双泵。
///
/// 头无效/超时 → `reset(0x01)`；目标端口或本侧数据流超限 → `reset(0x04)`；
/// 目标拨号失败/超时 → `reset(0x02)`。
#[cfg(test)]
pub(crate) async fn serve_accepted_stream(
    stream: Box<dyn TunnelStream>,
    link_lost: watch::Receiver<u64>,
    budget: StreamBudget,
) {
    serve_accepted_stream_with_dial(stream, link_lost, budget, |port| async move {
        TcpStream::connect(("127.0.0.1", port)).await
    })
    .await;
}

/// 同 `serve_accepted_stream`，但拨号动作可注入（测试超时路径）。
#[cfg(test)]
async fn serve_accepted_stream_with_dial<F, Fut>(
    stream: Box<dyn TunnelStream>,
    link_lost: watch::Receiver<u64>,
    budget: StreamBudget,
    dial: F,
) where
    F: FnOnce(u16) -> Fut,
    Fut: std::future::Future<Output = std::io::Result<TcpStream>>,
{
    let (mut read, mut write) = stream.split();

    let header = match read_data_header(&mut *read).await {
        Ok(header) => header,
        Err(error) => {
            reset_bad_header(&mut *write, &error);
            return;
        }
    };

    finish_data_stream(
        read,
        write,
        header,
        link_lost,
        budget,
        Arc::from("test-session"),
        dial,
    )
    .await;
}

/// 首字节分流后处理数据流：`first_byte` 已由分流器消费，必须是
/// `DATA_MAGIC[0]`（`0x50`），其余 5 字节仍按 6 字节头整体校验。
///
/// `started` 必须是分流器开始等待首字节的时刻：首字节 + 剩余 5 字节共用
/// 同一个 5 秒预算（SPEC §6）。
///
/// 限额、拨号与双泵与 `serve_accepted_stream` 完全一致：头无效/超时 →
/// `reset(0x01)`；目标端口或本侧数据流超限 → `reset(0x04)`；目标拨号
/// 失败/超时 → `reset(0x02)`。
pub(crate) async fn serve_data_stream(
    mut read: Box<dyn TunnelRead>,
    mut write: Box<dyn TunnelWrite>,
    first_byte: u8,
    started: tokio::time::Instant,
    link_lost: watch::Receiver<u64>,
    budget: StreamBudget,
    peer_session: Arc<str>,
) {
    let header = match read_data_header_bytes(&mut *read, Some(first_byte), started).await {
        Ok(header) => header,
        Err(error) => {
            reset_bad_header(&mut *write, &error);
            return;
        }
    };

    finish_data_stream(
        read,
        write,
        header,
        link_lost,
        budget,
        peer_session,
        |port| async move { TcpStream::connect(("127.0.0.1", port)).await },
    )
    .await;
}

/// 6 字节头校验通过后的公共数据面：限额、拨号、双泵。
async fn finish_data_stream<F, Fut>(
    read: Box<dyn TunnelRead>,
    mut write: Box<dyn TunnelWrite>,
    header: DataHeader,
    link_lost: watch::Receiver<u64>,
    budget: StreamBudget,
    peer_session: Arc<str>,
    dial: F,
) where
    F: FnOnce(u16) -> Fut,
    Fut: std::future::Future<Output = std::io::Result<TcpStream>>,
{
    // 先占名额再拨号：名额在双泵结束时随 `_slot` 归还。
    let Some(_slot) = budget.try_reserve_target(header.target_port) else {
        tracing::warn!(port = header.target_port, "连接数已满");
        let _ = write.reset(CODE_LIMIT_EXCEEDED);
        return;
    };

    // 审计日志（SPEC §10）：`RUST_LOG=info` 可读；只记录端口、会话标识与
    // 结果，不记录地址之外的连接内容。
    let target_port = header.target_port;
    let tcp = match tokio::time::timeout(TARGET_DIAL_TIMEOUT, dial(target_port)).await {
        Ok(Ok(tcp)) => {
            tracing::info!(
                target_port,
                peer_session = %peer_session,
                result = %"ok",
                "目标端口连接已建立"
            );
            tcp
        }
        Ok(Err(error)) => {
            tracing::info!(
                target_port,
                peer_session = %peer_session,
                result = %"dial_failed",
                %error,
                "目标端口拨号失败"
            );
            let _ = write.reset(CODE_TARGET_DIAL_FAILED);
            return;
        }
        Err(_) => {
            tracing::info!(
                target_port,
                peer_session = %peer_session,
                result = %"dial_failed",
                timeout_ms = TARGET_DIAL_TIMEOUT.as_millis(),
                "目标端口拨号超时"
            );
            let _ = write.reset(CODE_TARGET_DIAL_FAILED);
            return;
        }
    };

    // 目标侧没有本地规则；链路断开时同样要 RST 本地 TCP。
    pump(tcp, read, write, Aborts::target_side(link_lost)).await;
}

/// 打开一条数据流并立刻写出 6 字节头（目标为 `peer_port`）。
///
/// 规格要求开流方先写（`docs/SPEC.md` §5.1）；返回已拆分的读写两半。
pub(crate) async fn open_data_stream(
    link: &dyn DataLink,
    peer_port: u16,
) -> Result<(Box<dyn TunnelRead>, Box<dyn TunnelWrite>), TunnelError> {
    let header = DataHeader::new(peer_port)?;
    let stream = crate::transport::open_bi_with_timeout(link).await?;
    let (read, mut write) = stream.split();
    write
        .write_all(&header.encode())
        .await
        .map_err(|error| TunnelError::Network(format!("写数据流头失败：{error}")))?;
    Ok((read, write))
}

/// 一条连接的中止信号：本机规则关闭（携带错误码）或数据链路断开。
#[derive(Clone)]
pub(crate) struct Aborts {
    rule_close: watch::Receiver<u32>,
    /// 链路断开代数：每次断开 +1，连接创建时快照当前值。
    ///
    /// 用代数而不是布尔值，避免“断开→恢复”发生在两次检查之间时旧连接
    /// 漏掉 RST；恢复后新建的连接快照新代数，不会误伤（`docs/SPEC.md` §9.1）。
    link_lost: watch::Receiver<u64>,
    lost_generation: u64,
}

impl Aborts {
    pub(crate) fn new(rule_close: watch::Receiver<u32>, link_lost: watch::Receiver<u64>) -> Self {
        let lost_generation = *link_lost.borrow();
        Self {
            rule_close,
            link_lost,
            lost_generation,
        }
    }

    /// 目标侧：没有本地规则，只响应链路断开。
    pub(crate) fn target_side(link_lost: watch::Receiver<u64>) -> Self {
        let (_sender, rule_close) = watch::channel(0u32);
        let lost_generation = *link_lost.borrow();
        Self {
            rule_close,
            link_lost,
            lost_generation,
        }
    }

    /// 从不触发的信号（测试）。
    #[cfg(test)]
    pub(crate) fn never() -> Self {
        let (_sender, rule_close) = watch::channel(0u32);
        let (_sender, link_lost) = watch::channel(0u64);
        Self {
            rule_close,
            link_lost,
            lost_generation: 0,
        }
    }
}

/// 双泵结束方式：是否需要把本地 TCP 以 RST 关闭。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpEnd {
    /// 正常 EOF 或 STOP：按普通关闭处理。
    Graceful,
    /// 对端 RESET、规则关闭或数据链路断开：本地 TCP 必须以 RST 关闭
    /// （`docs/SPEC.md` §5.3、§8.2、§9.1）。
    ResetTcp,
}

/// 一个方向失败后，另一个方向需要执行的清理动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cancel {
    /// 本端 TCP 读/写失败：对向 `reset(0x10)` / `stop(0x10)`。
    TcpFailed,
    /// 对端 `RESET_STREAM`：对向收尾后本端 TCP 以 RST 关闭。
    PeerReset,
    /// 对端 `STOP_SENDING`：对向 `stop` 读半，不再读取。
    WriteStopped,
    /// 本机规则关闭：对向 `reset` / `stop` 指定错误码（CloseForward 为 0x05）。
    RuleClosed(u32),
    /// 数据链路断开：对向 `reset` / `stop` 0x05，本端 TCP 以 RST 关闭。
    LinkLost,
}

impl Cancel {
    /// 对端 RESET、链路断开或本机规则关闭时，本地 TCP 必须以 RST 关闭；
    /// 单纯 STOP 走普通关闭。
    fn needs_tcp_reset(self) -> bool {
        matches!(
            self,
            Cancel::PeerReset | Cancel::LinkLost | Cancel::RuleClosed(_)
        )
    }
}

/// 把一条已拨号的 TCP 与一条数据流双向对接。
///
/// 两个方向并发且相互独立：正常 EOF 只结束自己的发送方向，另一方向
/// 继续流动；只有错误、规则关闭或链路断开才让另一方向收尾。对端 RESET、
/// 规则关闭或链路断开时，本地 TCP 以 RST（`SO_LINGER=0`）关闭，而不是 FIN。
pub(crate) async fn pump(
    tcp: TcpStream,
    read: Box<dyn TunnelRead>,
    write: Box<dyn TunnelWrite>,
    aborts: Aborts,
) {
    // 隧道转发的是交互式流量（ssh 等小包）。关闭 Nagle，避免小包被攒批，
    // 再叠加对端 delayed ACK（Windows 默认可达 200 ms）造成明显手感延迟。
    let _ = tcp.set_nodelay(true);

    // 拆成读、写、控制三个句柄：读/写可并发，控制句柄在需要 RST 时设置
    // zero linger；三个句柄全部关闭后内核才真正关闭 socket 并发出 RST。
    let (tcp_read, tcp_write, control) = match shared_tcp(tcp) {
        Ok(parts) => parts,
        Err(error) => {
            tracing::warn!(%error, "拆分 TCP 句柄失败，关闭连接");
            return;
        }
    };

    let end = pump_halves(tcp_read, tcp_write, read, write, aborts).await;
    if end == PumpEnd::ResetTcp {
        if let Err(error) = control.set_zero_linger() {
            tracing::debug!(%error, "设置 zero linger 失败，按普通关闭处理");
        }
    }
    drop(control);
}

/// 把一条 TCP 拆成三个共享同一 socket 状态的句柄。
fn shared_tcp(tcp: TcpStream) -> std::io::Result<(TcpStream, TcpStream, TcpStream)> {
    let std_stream = tcp.into_std()?;
    let read = TcpStream::from_std(std_stream.try_clone()?)?;
    let write = TcpStream::from_std(std_stream.try_clone()?)?;
    let control = TcpStream::from_std(std_stream)?;
    Ok((read, write, control))
}

/// [`pump`] 的泛型核心；测试用假 TCP 半端注入读/写错误。
pub(crate) async fn pump_halves<R, W>(
    tcp_read: R,
    tcp_write: W,
    read: Box<dyn TunnelRead>,
    write: Box<dyn TunnelWrite>,
    aborts: Aborts,
) -> PumpEnd
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (cancel_tx, cancel_rx) = watch::channel(None::<Cancel>);
    let to_stream = pump_tcp_to_stream(
        tcp_read,
        write,
        cancel_rx.clone(),
        cancel_tx.clone(),
        aborts.clone(),
    );
    let to_tcp = pump_stream_to_tcp(read, tcp_write, cancel_rx.clone(), cancel_tx, aborts);

    // 两个方向都结束（正常 EOF、错误、规则关闭或链路断开）后泵才返回。
    let _ = tokio::join!(to_stream, to_tcp);

    let reason = *cancel_rx.borrow();
    match reason {
        Some(reason) if reason.needs_tcp_reset() => PumpEnd::ResetTcp,
        _ => PumpEnd::Graceful,
    }
}

/// 通知另一个方向按 `reason` 收尾。
fn notify(cancel: &watch::Sender<Option<Cancel>>, reason: Cancel) {
    let _ = cancel.send(Some(reason));
}

/// 等待规则关闭或链路断开；两者都不发生时永久等待，交给 select 取消。
async fn wait_abort(aborts: &mut Aborts) -> Cancel {
    loop {
        let code = *aborts.rule_close.borrow_and_update();
        if code != 0 {
            return Cancel::RuleClosed(code);
        }
        if *aborts.link_lost.borrow_and_update() != aborts.lost_generation {
            return Cancel::LinkLost;
        }
        tokio::select! {
            result = aborts.rule_close.changed() => {
                if result.is_err() {
                    return std::future::pending().await;
                }
            }
            result = aborts.link_lost.changed() => {
                if result.is_err() {
                    return std::future::pending().await;
                }
            }
        }
    }
}

/// 等待另一个方向的失败信号；sender 已释放时返回 `None`。
async fn wait_cancel(cancel: &mut watch::Receiver<Option<Cancel>>) -> Option<Cancel> {
    loop {
        if let Some(reason) = *cancel.borrow_and_update() {
            return Some(reason);
        }
        if cancel.changed().await.is_err() {
            return None;
        }
    }
}

/// 按失败原因收尾写半：规则关闭用指定码，其余错误显式 `reset(0x10)`。
fn settle_write_side(stream: &mut dyn TunnelWrite, reason: Option<Cancel>) {
    match reason {
        Some(Cancel::RuleClosed(code)) => {
            let _ = stream.reset(code);
        }
        Some(Cancel::TcpFailed | Cancel::PeerReset) => {
            let _ = stream.reset(CODE_TCP_RESET);
        }
        Some(Cancel::LinkLost) => {
            let _ = stream.reset(CODE_CANCELLED);
        }
        // 对端 STOP：本方向已无可写，保持沉默。
        Some(Cancel::WriteStopped) | None => {}
    }
}

/// 按失败原因收尾读半：规则关闭用指定码，其余错误显式 `stop`。
fn settle_read_side(stream: &mut dyn TunnelRead, reason: Option<Cancel>) {
    match reason {
        Some(Cancel::RuleClosed(code)) => {
            let _ = stream.stop(code);
        }
        Some(Cancel::TcpFailed | Cancel::PeerReset) => {
            let _ = stream.stop(CODE_TCP_RESET);
        }
        Some(Cancel::WriteStopped | Cancel::LinkLost) => {
            let _ = stream.stop(CODE_CANCELLED);
        }
        None => {}
    }
}

/// TCP 读 → 数据流写。TCP EOF 映射为 QUIC FIN。
async fn pump_tcp_to_stream<R>(
    mut tcp: R,
    mut stream: Box<dyn TunnelWrite>,
    mut cancel: watch::Receiver<Option<Cancel>>,
    cancel_tx: watch::Sender<Option<Cancel>>,
    mut aborts: Aborts,
) where
    R: AsyncRead + Unpin,
{
    let mut buffer = vec![0u8; BRIDGE_BUFFER];
    loop {
        let read = tokio::select! {
            reason = wait_abort(&mut aborts) => {
                notify(&cancel_tx, reason);
                settle_write_side(&mut *stream, Some(reason));
                return;
            }
            reason = wait_cancel(&mut cancel) => {
                settle_write_side(&mut *stream, reason);
                return;
            }
            read = tcp.read(&mut buffer) => read,
        };

        match read {
            Ok(0) => {
                // 正常半关闭：只 FIN 发送方向，不打断另一方向。
                let _ = stream.finish();
                return;
            }
            Ok(n) => {
                match write_or_cancel(&mut stream, &buffer[..n], &mut cancel).await {
                    Ok(()) => {}
                    Err(None) => {
                        if stream.write_stopped() {
                            // 对端不要再收数据：让另一方向 stop 读半，本地不 reset。
                            notify(&cancel_tx, Cancel::WriteStopped);
                            return;
                        }
                        let _ = stream.reset(CODE_TCP_RESET);
                        notify(&cancel_tx, Cancel::TcpFailed);
                        return;
                    }
                    Err(Some(reason)) => {
                        settle_write_side(&mut *stream, Some(reason));
                        return;
                    }
                }
            }
            Err(_) => {
                // TCP 读错误（RST 等）：显式 reset，绝不依赖 Drop 的隐式 FIN。
                let _ = stream.reset(CODE_TCP_RESET);
                notify(&cancel_tx, Cancel::TcpFailed);
                return;
            }
        }
    }
}

/// 写数据流或 TCP；若另一方向先失败则中止本次写，避免对向卡死在背压上。
async fn write_or_cancel<W>(
    target: &mut W,
    data: &[u8],
    cancel: &mut watch::Receiver<Option<Cancel>>,
) -> Result<(), Option<Cancel>>
where
    W: AsyncWrite + Unpin,
{
    tokio::select! {
        result = target.write_all(data) => result.map_err(|_| None),
        reason = wait_cancel(cancel) => Err(reason),
    }
}

/// 数据流读 → TCP 写。QUIC FIN 映射为 TCP `shutdown(Write)`。
async fn pump_stream_to_tcp<W>(
    mut stream: Box<dyn TunnelRead>,
    mut tcp: W,
    mut cancel: watch::Receiver<Option<Cancel>>,
    cancel_tx: watch::Sender<Option<Cancel>>,
    mut aborts: Aborts,
) where
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0u8; BRIDGE_BUFFER];
    loop {
        let read = tokio::select! {
            reason = wait_abort(&mut aborts) => {
                notify(&cancel_tx, reason);
                settle_read_side(&mut *stream, Some(reason));
                return;
            }
            reason = wait_cancel(&mut cancel) => {
                settle_read_side(&mut *stream, reason);
                return;
            }
            read = stream.read(&mut buffer) => read,
        };

        match read {
            Ok(0) => {
                // 对端 FIN：只关 TCP 写方向，继续把客户端数据读给孩子。
                let _ = tcp.shutdown().await;
                return;
            }
            Ok(n) => {
                match write_or_cancel(&mut tcp, &buffer[..n], &mut cancel).await {
                    Ok(()) => {}
                    Err(None) => {
                        // 本地 TCP 写失败：reset 对向，并停止本端读半。
                        let _ = stream.stop(CODE_TCP_RESET);
                        notify(&cancel_tx, Cancel::TcpFailed);
                        return;
                    }
                    Err(Some(reason)) => {
                        settle_read_side(&mut *stream, Some(reason));
                        return;
                    }
                }
            }
            Err(_) => {
                // 对端 RESET：不做 shutdown(Write)（那会先发 FIN）；交给
                // `pump` 用 zero linger 关闭本地 TCP，使对端收到 RST。
                notify(&cancel_tx, Cancel::PeerReset);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::{MAX_CONNECTIONS_PER_FORWARD, MAX_STREAMS_PER_SIDE};
    use crate::transport::mock::MockLink;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
    use tokio::sync::watch;

    /// 观察 initiator 读错误里的错误码（mock 把 reset code 写进消息）。
    fn assert_reset_code(error: &std::io::Error, code: u32) {
        let message = error.to_string();
        assert!(
            message.contains(&format!("{code:#04x}")),
            "期望错误码 {code:#04x}，实际：{message}"
        );
    }

    /// 建立一条真实 loopback TCP 连接，返回（客户端, 服务端）。
    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("绑定临时端口");
        let client = TcpStream::connect(listener.local_addr().expect("地址"))
            .await
            .expect("连接临时端口");
        let (server, _) = listener.accept().await.expect("接受连接");
        (client, server)
    }

    /// 从不触发的规则关闭/链路断开信号（sender 已释放）。
    fn never_aborts() -> Aborts {
        Aborts::never()
    }

    /// 从不触发的链路断开信号。
    fn never_link_lost() -> watch::Receiver<u64> {
        let (_sender, receiver) = watch::channel(0u64);
        receiver
    }

    #[tokio::test]
    async fn peer_stop_stops_tcp_read_without_reset() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, mut peer_write) = responder.split();
        let (mut client, tcp) = tcp_pair().await;

        let (read, write) = initiator.split();
        let bridge = tokio::spawn(pump(tcp, read, write, never_aborts()));

        // 对端拒收数据：STOP_SENDING 发起端的写半。
        peer_read.stop(CODE_CANCELLED).expect("stop 写半");
        // 客户端写数据触发桥接写，写失败后应停止读 TCP。
        client.write_all(b"ping").await.expect("客户端写");
        bridge.await.expect("双泵结束");

        // 对端的读半不应收到 RESET（STOP 不是错误收尾）。
        let mut buffer = [0u8; 8];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), peer_read.read(&mut buffer))
                .await
                .is_err(),
            "STOP 后不应 reset 数据流"
        );

        // 桥接的读半被 stop：对端写失败且错误码为 0x05。
        let error = peer_write
            .write_all(b"x")
            .await
            .expect_err("对端写应被 stop");
        assert_reset_code(&error, CODE_CANCELLED);

        // 客户端 TCP 被关闭。
        let n = client.read(&mut buffer).await.expect("读客户端");
        assert_eq!(n, 0, "停止读 TCP 后本地连接应关闭");
    }

    /// 第一次读取就报错的假 TCP 读半，用于验证错误映射。
    struct FailingTcpRead;

    impl AsyncRead for FailingTcpRead {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "模拟 TCP RST",
            )))
        }
    }

    /// 把客户端读到 EOF 的全部字节收集起来。
    async fn read_to_eof<R: AsyncReadExt + Unpin>(read: &mut R) -> Vec<u8> {
        let mut buffer = [0u8; 64];
        let mut received = Vec::new();
        loop {
            match read.read(&mut buffer).await.expect("读失败") {
                0 => return received,
                n => received.extend_from_slice(&buffer[..n]),
            }
        }
    }

    #[tokio::test]
    async fn tcp_read_error_resets_stream_with_tcp_reset() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, mut peer_write) = responder.split();

        let (read, write) = initiator.split();
        let bridge = tokio::spawn(pump_halves(
            FailingTcpRead,
            tokio::io::sink(),
            read,
            write,
            never_aborts(),
        ));
        bridge.await.expect("双泵结束");

        let mut buffer = [0u8; 8];
        let error = peer_read
            .read(&mut buffer)
            .await
            .expect_err("TCP 读错误应 reset 数据流");
        assert_reset_code(&error, CODE_TCP_RESET);
        let error = peer_write.write_all(b"x").await.expect_err("对向应被 stop");
        assert_reset_code(&error, CODE_TCP_RESET);
    }

    /// 第一次写入就报错的假 TCP 写半。
    struct FailingTcpWrite;

    impl AsyncWrite for FailingTcpWrite {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "模拟 TCP 写失败",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// 永不返回数据的假 TCP 读半，让另一方向保持阻塞。
    struct NeverTcpRead;

    impl AsyncRead for NeverTcpRead {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn tcp_write_error_resets_stream_with_tcp_reset() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, mut peer_write) = responder.split();

        let (read, write) = initiator.split();
        let bridge = tokio::spawn(pump_halves(
            NeverTcpRead,
            FailingTcpWrite,
            read,
            write,
            never_aborts(),
        ));

        peer_write.write_all(b"data").await.expect("对端写");
        bridge.await.expect("双泵结束");

        let mut buffer = [0u8; 8];
        let error = peer_read
            .read(&mut buffer)
            .await
            .expect_err("TCP 写失败应 reset 数据流");
        assert_reset_code(&error, CODE_TCP_RESET);
        let error = peer_write.write_all(b"x").await.expect_err("对向应被 stop");
        assert_reset_code(&error, CODE_TCP_RESET);
    }

    #[tokio::test]
    async fn peer_reset_maps_to_local_tcp_rst() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, mut peer_write) = responder.split();
        let (mut client, tcp) = tcp_pair().await;

        let (read, write) = initiator.split();
        let bridge = tokio::spawn(pump(tcp, read, write, never_aborts()));

        // 对端 RESET_STREAM：本地 TCP 必须以 RST 关闭，而不是 FIN。
        peer_write.reset(CODE_TCP_RESET).expect("对端 reset");

        let mut buffer = [0u8; 8];
        let error = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer))
            .await
            .expect("RESET 后桥接应关闭本地 TCP")
            .expect_err("本地 TCP 应收到 RST，而不是 EOF");
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "QUIC reset 应映射为 TCP RST"
        );

        // 对向写半被 stop，读半被 reset，错误码都是 0x10。
        let error = peer_write
            .write_all(b"x")
            .await
            .expect_err("对向写应被 stop");
        assert_reset_code(&error, CODE_TCP_RESET);
        let error = peer_read
            .read(&mut buffer)
            .await
            .expect_err("对向读应被 reset");
        assert_reset_code(&error, CODE_TCP_RESET);
        bridge.await.expect("双泵结束");
    }

    #[tokio::test]
    async fn client_half_close_finishes_stream_and_tail_flows_back() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, mut peer_write) = responder.split();
        let (mut client, tcp) = tcp_pair().await;

        let (read, write) = initiator.split();
        let bridge = tokio::spawn(pump(tcp, read, write, never_aborts()));

        client.write_all(b"request").await.expect("客户端写");
        client.shutdown().await.expect("客户端 half-close");

        // 目标侧读到请求数据后看到 EOF，而不是 reset。
        assert_eq!(read_to_eof(&mut peer_read).await, b"request");

        // 目标仍可回写尾部数据，客户端完整收到后再看到 EOF。
        peer_write.write_all(b"tail").await.expect("目标写尾部");
        peer_write.finish().expect("目标 FIN");
        assert_eq!(read_to_eof(&mut client).await, b"tail");
        bridge.await.expect("双泵结束");
    }

    #[tokio::test]
    async fn peer_fin_shuts_down_tcp_write_but_client_can_still_send() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, mut peer_write) = responder.split();
        let (mut client, tcp) = tcp_pair().await;

        let (read, write) = initiator.split();
        let bridge = tokio::spawn(pump(tcp, read, write, never_aborts()));

        peer_write.write_all(b"greeting").await.expect("目标写");
        peer_write.finish().expect("目标 FIN");

        // 客户端读到问候后看到 EOF；另一方向仍可继续写。
        assert_eq!(read_to_eof(&mut client).await, b"greeting");
        client.write_all(b"after").await.expect("客户端写");
        client.shutdown().await.expect("客户端 half-close");

        // 目标完整收到客户端在 FIN 之后写出的数据。
        assert_eq!(read_to_eof(&mut peer_read).await, b"after");
        bridge.await.expect("双泵结束");
    }

    #[tokio::test]
    async fn rule_close_resets_active_stream_with_cancelled() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, mut peer_write) = responder.split();
        let (client, tcp) = tcp_pair().await;

        let (read, write) = initiator.split();
        let (close_tx, close_rx) = watch::channel(0u32);
        let bridge = tokio::spawn(pump(
            tcp,
            read,
            write,
            Aborts::new(close_rx, never_link_lost()),
        ));

        close_tx.send(CODE_CANCELLED).expect("发送规则关闭信号");
        bridge.await.expect("双泵任务结束");

        let mut buffer = [0u8; 8];
        let error = peer_read
            .read(&mut buffer)
            .await
            .expect_err("规则关闭应 reset 活动流");
        assert_reset_code(&error, CODE_CANCELLED);
        let error = peer_write
            .write_all(b"x")
            .await
            .expect_err("规则关闭应 stop 读半");
        assert_reset_code(&error, CODE_CANCELLED);

        // 本机规则关闭按 SPEC §9.1/§8.2 必须 RST 活动连接：客户端读端
        // 应收到 ECONNRESET，而不是 EOF。
        let mut client = client;
        let error = client
            .read(&mut buffer)
            .await
            .expect_err("规则关闭后本地 TCP 应 RST，而不是 EOF");
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "规则关闭后本地 TCP 应为 RST：{error}"
        );
    }

    #[tokio::test]
    async fn bad_header_is_reset_with_protocol_error() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let server = tokio::spawn(serve_accepted_stream(
            responder,
            never_link_lost(),
            StreamBudget::new(),
        ));

        let (mut read, mut write) = initiator.split();
        // magic 错误：`X2`。
        write
            .write_all(&[0x58, 0x32, 0x01, 0x00, 0x00, 0x16])
            .await
            .expect("写头");

        let mut buffer = [0u8; 8];
        let error = read.read(&mut buffer).await.expect_err("应被 reset");
        assert_reset_code(&error, CODE_PROTOCOL_ERROR);
        server.await.expect("serve 任务结束");
    }

    #[tokio::test(start_paused = true)]
    async fn short_header_times_out_as_protocol_error() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let server = tokio::spawn(serve_accepted_stream(
            responder,
            never_link_lost(),
            StreamBudget::new(),
        ));

        let (mut read, mut write) = initiator.split();
        write.write_all(b"P").await.expect("写 1 字节");

        let mut buffer = [0u8; 8];
        let error = read.read(&mut buffer).await.expect_err("5 秒后应 reset");
        assert_reset_code(&error, CODE_PROTOCOL_ERROR);
        server.await.expect("serve 任务结束");
    }

    #[tokio::test]
    async fn dial_failure_is_reset_with_target_dial_failed() {
        // 先占一个端口再释放，保证该端口没有监听者。
        let probe = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("绑定临时端口");
        let target_port = probe.local_addr().expect("地址").port();
        drop(probe);

        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let server = tokio::spawn(serve_accepted_stream(
            responder,
            never_link_lost(),
            StreamBudget::new(),
        ));

        let (mut read, mut write) = initiator.split();
        let header = DataHeader::new(target_port).expect("端口有效").encode();
        write.write_all(&header).await.expect("写头");

        let mut buffer = [0u8; 8];
        let error = read.read(&mut buffer).await.expect_err("拨号失败应 reset");
        assert_reset_code(&error, CODE_TARGET_DIAL_FAILED);
        server.await.expect("serve 任务结束");
    }

    /// SPEC §10 的目标端审计日志：成功与失败都必须带 `target_port`、
    /// `peer_session`、`result`，并且 `RUST_LOG=info` 下可读。
    #[derive(Clone, Default)]
    struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Capture {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("日志锁")).into_owned()
        }
    }

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("日志锁").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Capture;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    async fn wait_for_audit_line(capture: &Capture, expected: &str) {
        for _ in 0..100 {
            if capture.contents().contains(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!(
            "审计日志缺少 {expected:?}，实际输出：\n{}",
            capture.contents()
        );
    }

    #[tokio::test]
    async fn target_audit_log_reports_ok_and_dial_failed() {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .with_writer(capture.clone())
            .finish();
        // tracing 的调用点兴趣缓存是进程级全局的：若用线程局部
        // `set_default`，并行测试线程先命中同一调用点会把该调用点缓存成
        // `never`，本测试的审计日志就会被静默丢弃（CI 上偶发失败）。
        // 安装全局订阅器可让所有线程看到同一份兴趣缓存。
        tracing::subscriber::set_global_default(subscriber)
            .expect("测试二进制内只安装一次全局订阅器");
        tracing::info!(probe = 1, "子订阅器探针");

        // 成功路径：真实 loopback listener。
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("绑定回显端口");
        let target_port = listener.local_addr().expect("地址").port();
        let accept = tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                drop(tcp);
            }
        });

        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut read, write) = responder.split();
        let (_peer_read, mut peer_write) = initiator.split();
        let header = DataHeader::new(target_port).expect("端口有效").encode();
        peer_write.write_all(&header).await.expect("写头");
        let mut first = [0u8; 1];
        read.read_exact(&mut first).await.expect("读首字节");
        assert_eq!(first[0], crate::protocol::DATA_MAGIC[0]);
        let serve = tokio::spawn(serve_data_stream(
            read,
            write,
            first[0],
            tokio::time::Instant::now(),
            never_link_lost(),
            StreamBudget::new(),
            Arc::from("session-abc"),
        ));
        wait_for_audit_line(
            &capture,
            &format!("target_port={target_port} peer_session=session-abc result=ok"),
        )
        .await;
        serve.abort();
        let _ = serve.await;
        accept.abort();
        let _ = accept.await;

        // 失败路径：端口无监听者。
        let probe = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("绑定临时端口");
        let closed_port = probe.local_addr().expect("地址").port();
        drop(probe);

        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut read, write) = responder.split();
        let (mut peer_read, mut peer_write) = initiator.split();
        let header = DataHeader::new(closed_port).expect("端口有效").encode();
        peer_write.write_all(&header).await.expect("写头");
        let mut first = [0u8; 1];
        read.read_exact(&mut first).await.expect("读首字节");
        let server = serve_data_stream(
            read,
            write,
            first[0],
            tokio::time::Instant::now(),
            never_link_lost(),
            StreamBudget::new(),
            Arc::from("session-abc"),
        );
        let writing = async {
            let mut buffer = [0u8; 8];
            let _ = peer_read.read(&mut buffer).await;
        };
        tokio::join!(server, writing);
        wait_for_audit_line(
            &capture,
            &format!("target_port={closed_port} peer_session=session-abc result=dial_failed"),
        )
        .await;
    }

    /// 目标端口满 64 条流：目标侧应 `reset(0x04)`，且不计入新名额。
    #[tokio::test]
    async fn accepted_stream_over_target_port_limit_is_reset_with_limit_code() {
        let budget = StreamBudget::new();
        let _held: Vec<_> = (0..MAX_CONNECTIONS_PER_FORWARD)
            .map(|_| budget.try_reserve_target(7100).expect("占满足额"))
            .collect();

        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let server = tokio::spawn(serve_accepted_stream(
            responder,
            never_link_lost(),
            budget.clone(),
        ));

        let (mut read, mut write) = initiator.split();
        let header = DataHeader::new(7100).expect("端口有效").encode();
        write.write_all(&header).await.expect("写头");

        let mut buffer = [0u8; 8];
        let error = tokio::time::timeout(Duration::from_secs(2), read.read(&mut buffer))
            .await
            .expect("应立即 reset")
            .expect_err("超限应 reset");
        assert_reset_code(&error, CODE_LIMIT_EXCEEDED);
        server.await.expect("serve 任务结束");
        assert_eq!(
            budget.total(),
            MAX_CONNECTIONS_PER_FORWARD,
            "拒绝不应泄漏名额"
        );
    }

    /// 本侧数据流满 96：目标侧应 `reset(0x04)`。
    #[tokio::test]
    async fn accepted_stream_over_side_limit_is_reset_with_limit_code() {
        let budget = StreamBudget::new();
        let mut held = Vec::new();
        for _ in 0..MAX_CONNECTIONS_PER_FORWARD {
            held.push(budget.try_reserve_target(7200).expect("占满足额"));
        }
        for _ in 0..(MAX_STREAMS_PER_SIDE - MAX_CONNECTIONS_PER_FORWARD) {
            held.push(budget.try_reserve_target(7201).expect("占满足额"));
        }
        assert_eq!(budget.total(), MAX_STREAMS_PER_SIDE);

        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let server = tokio::spawn(serve_accepted_stream(
            responder,
            never_link_lost(),
            budget.clone(),
        ));

        let (mut read, mut write) = initiator.split();
        let header = DataHeader::new(7202).expect("端口有效").encode();
        write.write_all(&header).await.expect("写头");

        let mut buffer = [0u8; 8];
        let error = tokio::time::timeout(Duration::from_secs(2), read.read(&mut buffer))
            .await
            .expect("应立即 reset")
            .expect_err("超限应 reset");
        assert_reset_code(&error, CODE_LIMIT_EXCEEDED);
        server.await.expect("serve 任务结束");
        assert_eq!(budget.total(), MAX_STREAMS_PER_SIDE, "拒绝不应泄漏名额");
    }

    /// 拨号挂起 10 秒：`reset(0x02)`，名额归还，不依赖真实等待。
    #[tokio::test(start_paused = true)]
    async fn slow_dial_times_out_as_target_dial_failed() {
        let budget = StreamBudget::new();
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let server = tokio::spawn(serve_accepted_stream_with_dial(
            responder,
            never_link_lost(),
            budget.clone(),
            |_port| std::future::pending::<std::io::Result<TcpStream>>(),
        ));

        let (mut read, mut write) = initiator.split();
        let header = DataHeader::new(7300).expect("端口有效").encode();
        let started = tokio::time::Instant::now();
        write.write_all(&header).await.expect("写头");

        let mut buffer = [0u8; 8];
        let error = read.read(&mut buffer).await.expect_err("拨号超时应 reset");
        assert_reset_code(&error, CODE_TARGET_DIAL_FAILED);
        assert!(
            started.elapsed() >= TARGET_DIAL_TIMEOUT,
            "应等满 10 秒拨号超时"
        );
        server.await.expect("serve 任务结束");
        assert_eq!(budget.total(), 0, "超时后名额应归还");
    }

    /// 真实 TCP 与非读对端：大数据量必须完整搬运，之后能正常收尾。
    ///
    /// 不断言客户端 `write_all` 何时停住：那是内核缓冲行为，受 OS 自动调优
    /// 影响（Windows 能把数 MiB 收进内核队列）。应用层的有界性由下面的
    /// [`tcp_to_stream_pump_stops_reading_when_peer_stalls`] 用计数器确定性地
    /// 覆盖。
    #[tokio::test]
    async fn large_tcp_payload_survives_stalled_reader() {
        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, mut peer_write) = responder.split();
        let (client, tcp) = tcp_pair().await;
        let (read, write) = initiator.split();
        let bridge = tokio::spawn(pump(tcp, read, write, never_aborts()));

        let payload = vec![0xA5u8; 4 * 1024 * 1024];
        let writer_payload = payload.clone();
        let mut client = client;
        let writer =
            tokio::spawn(async move { client.write_all(&writer_payload).await.map(|()| client) });

        let mut received = vec![0u8; payload.len()];
        tokio::time::timeout(Duration::from_secs(10), peer_read.read_exact(&mut received))
            .await
            .expect("对端开始读后应能收齐")
            .expect("读齐");
        assert!(received == payload, "数据应完整");

        let mut client = writer.await.expect("写任务结束").expect("写完成");
        // 显式 finish：mock 的 Drop 不会发 FIN，否则对向泵永远挂起。
        peer_write.finish().expect("对端 finish");
        drop(peer_write);
        drop(peer_read);
        client.shutdown().await.expect("关闭写端");
        bridge.await.expect("桥接结束");
    }

    /// TCP → 流方向的背压：对端不读时，泵最多再读一个 16 KiB 块就必须停住。
    ///
    /// 用会记录已读字节的假 TCP 读端，绕开内核缓冲差异（Windows 的收发
    /// 自动调优可以吞掉数 MiB，让真实 socket 上的“写是否阻塞”断言失去意义）。
    #[tokio::test]
    async fn tcp_to_stream_pump_stops_reading_when_peer_stalls() {
        const PAYLOAD: usize = 4 * 1024 * 1024;

        let link = MockLink::new();
        let initiator = link.open_bi().await.expect("open_bi");
        let responder = link.accept_bi().await.expect("accept_bi");
        let (mut peer_read, _peer_write) = responder.split();
        let (_read, write) = initiator.split();

        let served = Arc::new(AtomicUsize::new(0));
        let source = CountingReader {
            remaining: PAYLOAD,
            served: Arc::clone(&served),
        };
        let (cancel_tx, cancel_rx) = watch::channel(None::<Cancel>);
        let pump_task = tokio::spawn(pump_tcp_to_stream(
            source,
            write,
            cancel_rx,
            cancel_tx,
            never_aborts(),
        ));

        tokio::time::sleep(Duration::from_millis(300)).await;
        let stalled = served.load(Ordering::SeqCst);
        assert!(
            stalled <= 2 * BRIDGE_BUFFER,
            "对端不读时最多读入两个 16 KiB 块，实际 {stalled} 字节"
        );
        assert!(!pump_task.is_finished(), "对端不读时泵必须停住");

        let mut received = vec![0u8; PAYLOAD];
        tokio::time::timeout(Duration::from_secs(10), peer_read.read_exact(&mut received))
            .await
            .expect("对端开始读后应能收齐")
            .expect("读齐");
        assert!(received.iter().all(|byte| *byte == 0xA5), "数据应完整");
        pump_task.await.expect("泵任务结束");
        assert_eq!(served.load(Ordering::SeqCst), PAYLOAD, "最终应读满全部数据");
    }

    /// 会记录已读字节数的假 TCP 读端；数据全部是 0xA5。
    struct CountingReader {
        remaining: usize,
        served: Arc<AtomicUsize>,
    }

    impl AsyncRead for CountingReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.as_mut().get_mut();
            if this.remaining == 0 {
                return std::task::Poll::Ready(Ok(()));
            }
            let n = buf.remaining().min(this.remaining);
            let chunk = vec![0xA5u8; n];
            buf.put_slice(&chunk);
            this.remaining -= n;
            this.served.fetch_add(n, Ordering::SeqCst);
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// 流 → TCP 方向的背压：本地不读时，泵最多再读一个 16 KiB 块就必须停住。
    ///
    /// 用会记录已读字节的假流 + 16 KiB 有界写端，绕开内核缓冲差异（真实
    /// TCP 的发送缓冲受 OS 自动调优影响，Windows 可吞掉数 MiB）。
    #[tokio::test]
    async fn stream_to_tcp_pump_stops_writing_when_tcp_stalls() {
        const PAYLOAD: usize = 4 * 1024 * 1024;

        let served = Arc::new(AtomicUsize::new(0));
        let source = CountingStream(CountingReader {
            remaining: PAYLOAD,
            served: Arc::clone(&served),
        });
        let sink = BoundedSink::new(BRIDGE_BUFFER);
        let (cancel_tx, cancel_rx) = watch::channel(None::<Cancel>);
        let pump_task = tokio::spawn(pump_stream_to_tcp(
            Box::new(source),
            sink,
            cancel_rx,
            cancel_tx,
            never_aborts(),
        ));

        tokio::time::sleep(Duration::from_millis(300)).await;
        let stalled = served.load(Ordering::SeqCst);
        assert!(
            stalled <= 2 * BRIDGE_BUFFER,
            "本地不读时最多读入两个 16 KiB 块，实际 {stalled} 字节"
        );
        assert!(!pump_task.is_finished(), "本地不读时泵必须停住");

        pump_task.abort();
    }

    /// 把 [`CountingReader`] 包装成流读半端，供 `pump_stream_to_tcp` 使用。
    struct CountingStream(CountingReader);

    impl AsyncRead for CountingStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.as_mut().get_mut().0).poll_read(cx, buf)
        }
    }

    impl TunnelRead for CountingStream {
        fn stop(&mut self, _code: u32) -> Result<(), TunnelError> {
            Ok(())
        }
    }

    /// 最多接受 `limit` 字节，满了就返回 `Pending`（模拟对端不读的 TCP）。
    struct BoundedSink {
        limit: usize,
        written: usize,
    }

    impl BoundedSink {
        fn new(limit: usize) -> Self {
            Self { limit, written: 0 }
        }
    }

    impl AsyncWrite for BoundedSink {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.as_mut().get_mut();
            let space = this.limit - this.written;
            if space == 0 {
                return Poll::Pending;
            }
            let n = space.min(buf.len());
            this.written += n;
            Poll::Ready(Ok(n))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_link_connected_times_out_after_limit() {
        let (_tx, mut rx) = watch::channel(LinkState::Reconnecting);
        let started = tokio::time::Instant::now();
        assert!(!wait_for_link_connected(&mut rx, Duration::from_secs(5)).await);
        assert_eq!(started.elapsed(), Duration::from_secs(5));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_link_connected_returns_once_connected() {
        let (tx, mut rx) = watch::channel(LinkState::Reconnecting);
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = tx.send(LinkState::Connected);
        });
        assert!(wait_for_link_connected(&mut rx, Duration::from_secs(5)).await);
        sender.await.expect("发送任务结束");
    }
}
