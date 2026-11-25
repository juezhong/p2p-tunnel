//! Transport seam：bridge 只依赖这里的最小 trait，不直接依赖 `quinn`。
//!
//! 生产实现见 [`sdk`] 的 `SdkManagedLink`；[`mock`] 是内存实现，
//! [`quinn_link`] 是测试/诊断用的单连接适配。seam 的定义来自
//! `docs/SPEC.md` §7.6；`split` 后读写各持有半条流，避免同一 stream 的
//! `&mut` 借用冲突。
//!
//! UI（CLI/TUI/GUI）永远看不到这里的 `quinn` 类型。

use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;

use crate::api::{LinkState, TunnelError};

/// `open_bi` 的超时。Quinn 在双向流额度耗尽时会挂起，必须外层超时
/// （`docs/SPEC.md` §6.1）。
pub const OPEN_BI_TIMEOUT: Duration = Duration::from_secs(5);

/// 一条数据链路：能打开/接受双向流。
#[async_trait]
pub trait DataLink: Send + Sync {
    /// 打开一条双向流；调用方负责立刻写第一帧（Quinn 要求开流方先写）。
    async fn open_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError>;

    /// 接受对端打开的双向流。
    async fn accept_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError>;

    /// 数据链路阶段订阅（`docs/SPEC.md` §7.1）。
    ///
    /// 会话据此感知 `Reconnecting`/`AuthenticationFailed` 等阶段；默认实现
    /// 恒为 [`LinkState::Connected`]（不能重连的内存链路）。
    fn subscribe_link_state(&self) -> watch::Receiver<LinkState> {
        let (_sender, receiver) = watch::channel(LinkState::Connected);
        receiver
    }
}

/// 一条完整的双向流；bridge 在进入双泵前把它拆成读写两半。
pub trait TunnelStream: Send + Unpin {
    /// 拆成读、写两半。两半可以分别移动到不同任务。
    fn split(self: Box<Self>) -> (Box<dyn TunnelRead>, Box<dyn TunnelWrite>);
}

/// 读方向：QUIC `RecvStream` 的抽象。
pub trait TunnelRead: AsyncRead + Send + Unpin {
    /// 停止接收（QUIC `STOP_SENDING`）。
    fn stop(&mut self, code: u32) -> Result<(), TunnelError>;
}

/// 写方向：QUIC `SendStream` 的抽象。
pub trait TunnelWrite: AsyncWrite + Send + Unpin {
    /// 结束发送方向（QUIC FIN）；对端读到 EOF。
    fn finish(&mut self) -> Result<(), TunnelError>;

    /// 立即中止发送方向（QUIC RESET）；对端读到 `Reset(code)`。
    fn reset(&mut self, code: u32) -> Result<(), TunnelError>;

    /// 最近一次写失败是否来自对端 `STOP_SENDING`（quinn `WriteError::Stopped`）。
    ///
    /// 只有适配层知道这个区别；bridge 据此决定是停止读 TCP 还是再发
    /// `RESET_STREAM`。默认 `false`：按普通写错误处理。
    fn write_stopped(&self) -> bool {
        false
    }
}

/// 带 5 秒超时地打开双向流；超时映射为 [`TunnelError::Timeout`]。
///
/// 规格：`docs/SPEC.md` §5.2 步骤 4、§6.1 的 `open_bi` 超时。
pub async fn open_bi_with_timeout(
    link: &dyn DataLink,
) -> Result<Box<dyn TunnelStream>, TunnelError> {
    match tokio::time::timeout(OPEN_BI_TIMEOUT, link.open_bi()).await {
        Ok(result) => result,
        Err(_) => Err(TunnelError::Timeout {
            operation: "打开双向流",
        }),
    }
}

pub mod mock;
pub mod quinn_link;
pub(crate) mod sdk;

#[cfg(test)]
mod tests {
    use super::mock::MockLink;
    use super::*;
    use crate::protocol::{CODE_CANCELLED, CODE_TCP_RESET};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn connect(link: &MockLink) -> (Box<dyn TunnelStream>, Box<dyn TunnelStream>) {
        let initiator = link.open_bi().await.expect("open_bi 应成功");
        let responder = link.accept_bi().await.expect("accept_bi 应成功");
        (initiator, responder)
    }

    #[tokio::test]
    async fn mock_pair_moves_bytes_both_ways() {
        let link = MockLink::new();
        let (initiator, responder) = connect(&link).await;
        let (mut ir, mut iw) = initiator.split();
        let (mut rr, mut rw) = responder.split();

        iw.write_all(b"ping").await.expect("写方向可用");
        let mut buf = [0u8; 16];
        let n = rr.read(&mut buf).await.expect("读方向可用");
        assert_eq!(&buf[..n], b"ping");

        rw.write_all(b"pong").await.expect("反方向可用");
        let n = ir.read(&mut buf).await.expect("反方向可读");
        assert_eq!(&buf[..n], b"pong");
    }

    #[tokio::test]
    async fn mock_finish_is_half_close() {
        let link = MockLink::new();
        let (initiator, responder) = connect(&link).await;
        let (mut ir, mut iw) = initiator.split();
        let (mut rr, mut rw) = responder.split();

        iw.write_all(b"hello").await.expect("写方向可用");
        iw.finish().expect("FIN 应成功");

        let mut buf = [0u8; 16];
        let n = rr.read(&mut buf).await.expect("FIN 前数据仍可读");
        assert_eq!(&buf[..n], b"hello");
        assert_eq!(rr.read(&mut buf).await.expect("FIN 后是 EOF"), 0);

        // FIN 只关发送方向：对端仍可写回来。
        rw.write_all(b"after-fin").await.expect("对端仍可写");
        let n = ir.read(&mut buf).await.expect("FIN 后仍可读");
        assert_eq!(&buf[..n], b"after-fin");
    }

    #[tokio::test]
    async fn mock_reset_surfaces_as_connection_reset() {
        let link = MockLink::new();
        let (initiator, responder) = connect(&link).await;
        let (mut ir, _iw) = initiator.split();
        let (_rr, mut rw) = responder.split();

        rw.write_all(b"partial").await.expect("写方向可用");
        rw.reset(CODE_TCP_RESET).expect("RST 应成功");

        let mut buf = [0u8; 16];
        let error = ir.read(&mut buf).await.expect_err("RESET 后读必须报错");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    }

    #[tokio::test]
    async fn mock_stop_makes_peer_write_fail() {
        let link = MockLink::new();
        let (initiator, responder) = connect(&link).await;
        let (_ir, mut iw) = initiator.split();
        let (mut rr, _rw) = responder.split();

        rr.stop(CODE_CANCELLED).expect("STOP_SENDING 应成功");

        let error = iw
            .write_all(b"ignored")
            .await
            .expect_err("STOP 后写必须报错");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    }

    #[tokio::test]
    async fn mock_close_aborts_open_and_accept() {
        let link = MockLink::new();
        link.close();

        assert!(link.open_bi().await.is_err(), "关闭后 open_bi 应失败");
        assert!(link.accept_bi().await.is_err(), "关闭后 accept_bi 应失败");
    }

    #[tokio::test(start_paused = true)]
    async fn mock_write_applies_backpressure_when_buffer_is_full() {
        let link = MockLink::new();
        let (initiator, responder) = connect(&link).await;
        let (_ir, mut iw) = initiator.split();
        let (_rr, _rw) = responder.split();

        // 填满 16 KiB 有界缓冲，不再读取。
        let chunk = vec![0u8; 16 * 1024];
        iw.write_all(&chunk).await.expect("缓冲可被填满");

        let result = tokio::time::timeout(Duration::from_millis(50), iw.write_all(b"x")).await;
        assert!(result.is_err(), "缓冲满后写必须挂起（背压）");
    }

    #[tokio::test(start_paused = true)]
    async fn hanging_open_bi_times_out_after_five_seconds() {
        let link = MockLink::hanging();
        let started = tokio::time::Instant::now();

        let error = match open_bi_with_timeout(&link).await {
            Ok(_) => panic!("挂起的 open_bi 必须超时"),
            Err(error) => error,
        };
        assert!(matches!(error, TunnelError::Timeout { .. }));
        assert_eq!(started.elapsed(), OPEN_BI_TIMEOUT);
    }
}
