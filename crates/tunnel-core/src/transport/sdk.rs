//! SDK managed link 适配：把跨 generation 自动重连的
//! `ManagedAuthenticatedLink` 暴露为 [`DataLink`]。
//!
//! bootstrap 连接只是配对会话的锚点；应用数据只走 `manage_link()` 返回的
//! 附加 QUIC（`docs/SPEC.md` §4.3）。本适配层不复制 SDK 的重拨逻辑，
//! 只做两件事：
//!
//! - `accept_bi`/`open_bi` 总是作用于**当前**已认证连接；
//! - 连接断开（`Reconnecting`）时等待下一个 `Connected`，不把瞬断泄漏给
//!   bridge；只有 `Stopped`/`AuthenticationFailed` 才是不可恢复错误。
//!
//! 规格来源：`docs/SPEC.md` §4.3、§9.1。

use async_trait::async_trait;
use p2p_sdk::{ManagedAuthenticatedLink, ManagedLinkPhase, ManagedLinkStatus};
use quinn::Connection;
use tokio::sync::watch;

use crate::api::{LinkState, TunnelError};
use crate::transport::quinn_link;
use crate::transport::{DataLink, TunnelStream};

/// 包装 SDK managed link 的数据链路。
pub(crate) struct SdkManagedLink {
    /// 只用于持有生命周期：drop 时向 SDK worker 发送停止信号。
    _managed: ManagedAuthenticatedLink,
    status: watch::Receiver<ManagedLinkStatus>,
    /// `ManagedLinkPhase` 映射后的公开阶段，供会话感知重连/失败。
    phase: watch::Receiver<LinkState>,
}

impl SdkManagedLink {
    pub(crate) fn new(managed: ManagedAuthenticatedLink) -> Self {
        let status = managed.subscribe();
        let (phase_tx, phase_rx) = watch::channel(link_state(status.borrow().phase));
        tokio::spawn(forward_phase(status.clone(), phase_tx));
        Self {
            _managed: managed,
            status,
            phase: phase_rx,
        }
    }
}

/// SDK `ManagedLinkPhase` → 公开 [`LinkState`]（一一对应，`docs/SPEC.md` §7.1）。
pub(crate) fn link_state(phase: ManagedLinkPhase) -> LinkState {
    match phase {
        ManagedLinkPhase::Connecting => LinkState::Connecting,
        ManagedLinkPhase::Connected => LinkState::Connected,
        ManagedLinkPhase::Reconnecting => LinkState::Reconnecting,
        ManagedLinkPhase::AuthenticationFailed => LinkState::AuthenticationFailed,
        ManagedLinkPhase::Stopped => LinkState::Stopped,
    }
}

/// 把 SDK 的阶段变化转发到单槽 `watch`；managed link 释放后自然结束。
async fn forward_phase(
    mut status: watch::Receiver<ManagedLinkStatus>,
    phase: watch::Sender<LinkState>,
) {
    loop {
        let next = link_state(status.borrow_and_update().phase);
        if *phase.borrow() != next {
            phase.send_replace(next);
        }
        if status.changed().await.is_err() {
            return;
        }
    }
}

#[async_trait]
impl DataLink for SdkManagedLink {
    fn subscribe_link_state(&self) -> watch::Receiver<LinkState> {
        self.phase.clone()
    }

    async fn open_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let mut status = self.status.clone();
        loop {
            let connection = live_connection(&mut status).await?;
            match connection.open_bi().await {
                Ok((send, recv)) => return Ok(quinn_link::stream(send, recv)),
                Err(error) => {
                    // 连接在打开瞬间关闭：等 SDK 重连后再试，而不是把瞬断
                    // 当成规则级失败（§9.1：listener 保留、规则不删）。
                    tracing::warn!(%error, "managed link 打开双向流失败，等待重连");
                    status
                        .changed()
                        .await
                        .map_err(|_| TunnelError::Network("数据链路状态通道已关闭".into()))?;
                }
            }
        }
    }

    async fn accept_bi(&self) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let mut status = self.status.clone();
        loop {
            let connection = live_connection(&mut status).await?;
            match connection.accept_bi().await {
                Ok((send, recv)) => return Ok(quinn_link::stream(send, recv)),
                Err(error) => {
                    tracing::warn!(%error, "managed link 接受双向流失败，等待重连");
                    status
                        .changed()
                        .await
                        .map_err(|_| TunnelError::Network("数据链路状态通道已关闭".into()))?;
                }
            }
        }
    }
}

/// 等待当前连接可用；不可恢复状态直接返回错误。
async fn live_connection(
    status: &mut watch::Receiver<ManagedLinkStatus>,
) -> Result<Connection, TunnelError> {
    loop {
        // 先取出拥有所有权的快照，避免把 watch 的读锁 guard 带过 await。
        let (connection, phase) = {
            let state = status.borrow_and_update();
            (state.connection.clone(), state.phase)
        };
        if let Some(connection) = connection {
            if connection.close_reason().is_none() {
                return Ok(connection);
            }
        }
        match phase {
            ManagedLinkPhase::Stopped => {
                return Err(TunnelError::Network("数据链路已停止".into()));
            }
            ManagedLinkPhase::AuthenticationFailed => {
                return Err(TunnelError::Pairing("数据链路认证失败".into()));
            }
            ManagedLinkPhase::Connecting
            | ManagedLinkPhase::Connected
            | ManagedLinkPhase::Reconnecting => {}
        }
        status
            .changed()
            .await
            .map_err(|_| TunnelError::Network("数据链路状态通道已关闭".into()))?;
    }
}
