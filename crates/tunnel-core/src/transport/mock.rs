//! 内存 duplex mock：实现 [`DataLink`] seam，供 bridge/超时测试使用。
//!
//! 每方向一个有界缓冲（固定 16 KiB，见 `docs/SPEC.md` §5.3、§6.2），
//! 并显式建模四种 QUIC 事件：
//!
//! | mock 动作 | QUIC 语义 | 对端观察 |
//! | --- | --- | --- |
//! | [`TunnelWrite::finish`] | FIN | 读到 EOF |
//! | [`TunnelRead::stop`] | STOP_SENDING | 写返回 `ConnectionReset` |
//! | [`TunnelWrite::reset`] | RESET_STREAM | 读返回 `ConnectionReset` |
//! | [`MockLink::hanging`] | `open_bi` 挂起 | 外层 5 秒超时 |
//!
//! 不模拟丢包/乱序：那是 SDK 的 QUIC 责任，bridge 只需要可观察的
//! 半关闭与错误边界。

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{watch, Notify};

use crate::api::{LinkState, TunnelError};
use crate::transport::{DataLink, TunnelRead, TunnelStream, TunnelWrite};

/// 每个方向的内存缓冲上限。写路径在此处阻塞，不会无界增长。
const MOCK_BUFFER: usize = 16 * 1024;

struct DirState {
    /// 已写、未读的字节；长度始终 ≤ [`MOCK_BUFFER`]。
    buf: VecDeque<u8>,
    /// 写方已 `finish`：读方读完剩余数据后看到 EOF。
    fin: bool,
    /// 写方已 `reset`：读方立即看到错误，缓冲被丢弃。
    reset: Option<u32>,
    /// 读方已 `stop`：写方后续写入失败。
    stopped: Option<u32>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
}

impl DirState {
    fn new() -> Self {
        Self {
            buf: VecDeque::with_capacity(MOCK_BUFFER),
            fin: false,
            reset: None,
            stopped: None,
            read_waker: None,
            write_waker: None,
        }
    }
}

#[derive(Clone)]
struct Direction(Arc<Mutex<DirState>>);

impl Direction {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(DirState::new())))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DirState> {
        self.0.lock().expect("mock 方向状态锁未被污染")
    }
}

fn wake(slot: &mut Option<Waker>) {
    if let Some(waker) = slot.take() {
        waker.wake();
    }
}

fn reset_error(context: &str, code: u32) -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionReset,
        format!("{context}（错误码 {code:#04x}）"),
    )
}

/// 内存版数据链路。
///
/// 单端模式下（[`MockLink::new`]）同一个 `MockLink` 同时扮演连接两端，
/// `open_bi` 的对端排进自己的 `accept_bi` 队列，足够驱动 bridge 测试。
/// 双端模式下（[`MockLink::pair`]）两端各自独立，可分别交给两个
/// [`Session`](crate::session::Session)。
pub struct MockLink {
    /// 对端 `open_bi` 打开、本端 `accept_bi` 接受的流。
    incoming: Arc<Incoming>,
    /// 本端 `open_bi` 打开、对端 `accept_bi` 接受的流。
    outgoing: Arc<Incoming>,
    closed: Arc<AtomicBool>,
    /// 数据链路阶段；`pair()` 的两端共享同一个阶段。
    phase: Arc<watch::Sender<LinkState>>,
    hang_open: bool,
}

struct Incoming {
    streams: Mutex<VecDeque<MockStream>>,
    notify: Notify,
}

impl Incoming {
    fn new() -> Self {
        Self {
            streams: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
        }
    }

    fn push(&self, stream: MockStream) {
        self.streams
            .lock()
            .expect("mock 链路状态锁未被污染")
            .push_back(stream);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<MockStream> {
        self.streams
            .lock()
            .expect("mock 链路状态锁未被污染")
            .pop_front()
    }
}

impl MockLink {
    /// 正常链路：`open_bi` 立即返回。
    pub fn new() -> Self {
        let queue = Arc::new(Incoming::new());
        let (phase, _) = watch::channel(LinkState::Connected);
        Self {
            incoming: queue.clone(),
            outgoing: queue,
            closed: Arc::new(AtomicBool::new(false)),
            phase: Arc::new(phase),
            hang_open: false,
        }
    }

    /// 互相连通的两端：`a.open_bi()` 的流出现在 `b.accept_bi()`。
    pub fn pair() -> (Self, Self) {
        let a_to_b = Arc::new(Incoming::new());
        let b_to_a = Arc::new(Incoming::new());
        let closed_a = Arc::new(AtomicBool::new(false));
        let closed_b = Arc::new(AtomicBool::new(false));
        let (phase, _) = watch::channel(LinkState::Connected);
        let phase = Arc::new(phase);
        (
            Self {
                incoming: b_to_a.clone(),
                outgoing: a_to_b.clone(),
                closed: closed_a,
                phase: phase.clone(),
                hang_open: false,
            },
            Self {
                incoming: a_to_b,
                outgoing: b_to_a,
                closed: closed_b,
                phase,
                hang_open: false,
            },
        )
    }

    /// `open_bi` 永久挂起的链路，用于验证 5 秒超时路径。
    pub fn hanging() -> Self {
        Self {
            hang_open: true,
            ..Self::new()
        }
    }

    /// 模拟数据链路断开：阶段变为 `Reconnecting`，挂起的 `accept_bi` 与
    /// 后续 `open_bi` 都失败。
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.set_phase(LinkState::Reconnecting);
        // `notify_one` 会保留一个 permit，避免检查与等待之间丢唤醒。
        self.incoming.notify.notify_one();
    }

    /// 设置链路阶段；会话通过 [`DataLink::subscribe_link_state`] 感知。
    pub fn set_phase(&self, phase: LinkState) {
        // 即使当前没有订阅者也要更新，避免测试里的阶段丢失。
        self.phase.send_replace(phase);
    }

    /// 模拟 SDK 重连成功：恢复流操作并把阶段置回 `Connected`。
    pub fn reopen(&self) {
        self.closed.store(false, Ordering::SeqCst);
        self.set_phase(LinkState::Connected);
    }

    /// 链路是否已关闭。
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

impl Default for MockLink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataLink for MockLink {
    async fn open_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        if self.hang_open {
            std::future::pending::<()>().await;
        }
        if self.is_closed() {
            return Err(TunnelError::Network("mock 数据链路已关闭".into()));
        }

        let (initiator, responder) = MockStream::pair();
        self.outgoing.push(responder);
        Ok(Box::new(initiator))
    }

    fn subscribe_link_state(&self) -> watch::Receiver<LinkState> {
        self.phase.subscribe()
    }

    async fn accept_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        loop {
            if let Some(stream) = self.incoming.pop() {
                return Ok(Box::new(stream));
            }
            if self.is_closed() {
                return Err(TunnelError::Network("mock 数据链路已关闭".into()));
            }
            self.incoming.notify.notified().await;
        }
    }
}

/// 一条 mock 双向流；进入 bridge 前必须 `split`。
pub struct MockStream {
    read: MockRead,
    write: MockWrite,
}

impl MockStream {
    /// 创建互相连通的两端。
    fn pair() -> (Self, Self) {
        let ab = Direction::new();
        let ba = Direction::new();
        (
            Self {
                read: MockRead { dir: ba.clone() },
                write: MockWrite { dir: ab.clone() },
            },
            Self {
                read: MockRead { dir: ab },
                write: MockWrite { dir: ba },
            },
        )
    }
}

impl TunnelStream for MockStream {
    fn split(self: Box<Self>) -> (Box<dyn TunnelRead>, Box<dyn TunnelWrite>) {
        let Self { read, write } = *self;
        (Box::new(read), Box::new(write))
    }
}

/// 读半：对应 QUIC `RecvStream`。
pub struct MockRead {
    dir: Direction,
}

impl AsyncRead for MockRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut state = self.dir.lock();

        if let Some(code) = state.reset {
            return Poll::Ready(Err(reset_error("对端 RESET_STREAM", code)));
        }
        if !state.buf.is_empty() {
            let n = state.buf.len().min(buf.remaining());
            {
                let (front, back) = state.buf.as_slices();
                let front_len = front.len().min(n);
                buf.put_slice(&front[..front_len]);
                if front_len < n {
                    buf.put_slice(&back[..n - front_len]);
                }
            }
            state.buf.drain(..n);
            wake(&mut state.write_waker);
            return Poll::Ready(Ok(()));
        }
        if state.fin {
            // 零字节 = EOF。
            return Poll::Ready(Ok(()));
        }

        state.read_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl TunnelRead for MockRead {
    fn stop(&mut self, code: u32) -> Result<(), TunnelError> {
        let mut state = self.dir.lock();
        state.stopped = Some(code);
        state.buf.clear();
        wake(&mut state.write_waker);
        Ok(())
    }
}

/// 写半：对应 QUIC `SendStream`。
pub struct MockWrite {
    dir: Direction,
}

impl MockWrite {
    fn close_for_writing(&self) -> Result<(), TunnelError> {
        let mut state = self.dir.lock();
        if state.reset.is_some() {
            return Err(TunnelError::Network("mock 流已 RESET".into()));
        }
        state.fin = true;
        wake(&mut state.read_waker);
        Ok(())
    }
}

impl AsyncWrite for MockWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.dir.lock();

        if let Some(code) = state.stopped {
            return Poll::Ready(Err(reset_error("对端 STOP_SENDING", code)));
        }
        if let Some(code) = state.reset {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("本地 RESET_STREAM（错误码 {code:#04x}）"),
            )));
        }
        if state.fin {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "本地已 FIN，不能继续写",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let space = MOCK_BUFFER - state.buf.len();
        if space == 0 {
            state.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = space.min(buf.len());
        state.buf.extend(buf[..n].iter().copied());
        wake(&mut state.read_waker);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().close_for_writing() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(error) => Poll::Ready(Err(io::Error::other(error.to_string()))),
        }
    }
}

impl TunnelWrite for MockWrite {
    fn finish(&mut self) -> Result<(), TunnelError> {
        self.close_for_writing()
    }

    fn write_stopped(&self) -> bool {
        // `stop()` 一定先于对端写入失败发生，这里直接看方向状态。
        self.dir.lock().stopped.is_some()
    }

    fn reset(&mut self, code: u32) -> Result<(), TunnelError> {
        let mut state = self.dir.lock();
        state.reset = Some(code);
        state.buf.clear();
        wake(&mut state.read_waker);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn pair_connects_both_ends() {
        let (a, b) = MockLink::pair();
        assert!(!a.is_closed());
        assert!(!b.is_closed());

        let opened = a.open_bi().await.expect("A 打开双向流");
        let accepted = b.accept_bi().await.expect("B 接受双向流");
        let (mut a_read, mut a_write) = opened.split();
        let (mut b_read, mut b_write) = accepted.split();

        a_write.write_all(b"ping").await.expect("A 写");
        let mut buf = [0u8; 4];
        b_read.read_exact(&mut buf).await.expect("B 读");
        assert_eq!(&buf, b"ping");

        b_write.write_all(b"pong").await.expect("B 写");
        a_read.read_exact(&mut buf).await.expect("A 读");
        assert_eq!(&buf, b"pong");
    }
}
