//! 测试/诊断用 transport 适配：包装 `quinn::Connection` / `SendStream` + `RecvStream`。
//!
//! 本模块只做适配，不含桥接逻辑。生产会话走 [`sdk`](super::sdk) 的
//! `SdkManagedLink`；这里的单连接适配只用于测试和诊断。
//! 语义依据 `docs/SPEC.md` §7.6 与 quinn 0.11：
//!
//! - `finish()` = FIN，只关发送方向；
//! - `reset(code)` = RESET_STREAM，对端读到 `ReadError::Reset(code)`；
//! - `stop(code)` = STOP_SENDING，对端写返回 `WriteError::Stopped(code)`。
//!
//! 注意：quinn 的 `SendStream` / `RecvStream` 在 drop 时会隐式 `finish` /
//! `stop(0)`，所以错误路径必须显式调用这里的 `reset` / `stop`。

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use quinn::VarInt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::api::TunnelError;
use crate::transport::{DataLink, TunnelRead, TunnelStream, TunnelWrite};

/// 把 `quinn::ConnectionError` 映射为可展示的网络错误。
fn connection_error(error: quinn::ConnectionError) -> TunnelError {
    TunnelError::Network(error.to_string())
}

/// 包装一条 `quinn::Connection` 的 [`DataLink`]。
#[derive(Debug, Clone)]
pub struct QuinnLink {
    connection: quinn::Connection,
}

/// 把一对 quinn 流包装成 [`TunnelStream`]（SDK managed link 适配层复用）。
pub(crate) fn stream(send: quinn::SendStream, recv: quinn::RecvStream) -> Box<dyn TunnelStream> {
    Box::new(QuinnStream { send, recv })
}

impl QuinnLink {
    /// 用 SDK 已建立的连接构造数据链路。
    pub fn new(connection: quinn::Connection) -> Self {
        Self { connection }
    }

    /// 底层连接（诊断/测试用）。
    pub fn connection(&self) -> &quinn::Connection {
        &self.connection
    }
}

#[async_trait]
impl DataLink for QuinnLink {
    async fn open_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let (send, recv) = self.connection.open_bi().await.map_err(connection_error)?;
        Ok(stream(send, recv))
    }

    async fn accept_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let (send, recv) = self
            .connection
            .accept_bi()
            .await
            .map_err(connection_error)?;
        Ok(stream(send, recv))
    }
}

/// 一条 quinn 双向流，进入双泵前拆成读写两半。
struct QuinnStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl TunnelStream for QuinnStream {
    fn split(self: Box<Self>) -> (Box<dyn TunnelRead>, Box<dyn TunnelWrite>) {
        let Self { send, recv } = *self;
        (
            Box::new(QuinnRead { recv }),
            Box::new(QuinnWrite {
                send,
                stopped: false,
            }),
        )
    }
}

/// 读半：包装 `quinn::RecvStream`。
struct QuinnRead {
    recv: quinn::RecvStream,
}

impl AsyncRead for QuinnRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        AsyncRead::poll_read(Pin::new(&mut self.recv), cx, buf)
    }
}

impl TunnelRead for QuinnRead {
    fn stop(&mut self, code: u32) -> Result<(), TunnelError> {
        self.recv
            .stop(VarInt::from_u32(code))
            .map_err(|error| TunnelError::Network(error.to_string()))
    }
}

/// 写半：包装 `quinn::SendStream`。
struct QuinnWrite {
    send: quinn::SendStream,
    /// 最近一次写失败是否来自 `WriteError::Stopped`（对端 STOP_SENDING）。
    stopped: bool,
}

impl AsyncWrite for QuinnWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match AsyncWrite::poll_write(Pin::new(&mut this.send), cx, buf) {
            Poll::Ready(Err(error)) => {
                this.stopped = error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<quinn::WriteError>())
                    .is_some_and(|error| matches!(error, quinn::WriteError::Stopped(_)));
                Poll::Ready(Err(error))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.send), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.send.finish().map_err(io::Error::from)?;
        Poll::Ready(Ok(()))
    }
}

impl TunnelWrite for QuinnWrite {
    fn write_stopped(&self) -> bool {
        self.stopped
    }

    fn finish(&mut self) -> Result<(), TunnelError> {
        self.send
            .finish()
            .map_err(|error| TunnelError::Network(error.to_string()))
    }

    fn reset(&mut self, code: u32) -> Result<(), TunnelError> {
        self.send
            .reset(VarInt::from_u32(code))
            .map_err(|error| TunnelError::Network(error.to_string()))
    }
}
