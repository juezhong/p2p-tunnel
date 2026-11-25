//! 手动配对状态机（`docs/SPEC.md` §7.2）。
//!
//! SDK 只负责 INVITE/REPLY 编解码、ICE 打洞、认证 QUIC 与 managed link 的
//! 生命周期；本模块把 `Command` 的配对语义翻译成 SDK 调用，并在
//! `connect_transport` 成功后把 managed link 适配成 [`DataLink`] 交给会话。
//!
//! 状态流转（两端对称，拒绝乱序命令）：
//!
//! - 创建端：`CreateInvite` → `InviteCode` → `ProvideReply` → `ComparisonCode`
//!   → `ConfirmComparison` → `Ready`；
//! - 加入端：`JoinInvite` → `ReplyCode`（随后发 `ComparisonCode`）
//!   → `ConfirmComparison` → `Ready`。
//!
//! 比较码在 v0.1 只用于可选人工核对：`ConfirmComparison` 时本端自动用
//! 自己算出的比较码完成 [`p2p_sdk::peer_pin::ManualConfirmation`]，不阻塞
//! 非交互 CLI。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use p2p_sdk::peer_pin::ManualConfirmation;
use p2p_sdk::{
    begin_creator, begin_creator_auto, begin_joiner, begin_joiner_auto, ConnectedTransportPeer,
    DirectPeerError, PendingCreator, ReadyCreator, ReadyJoiner,
};
use tokio::sync::watch;

use crate::api::TunnelError;
use crate::transport::sdk::SdkManagedLink;
use crate::transport::DataLink;

/// INVITE 有效期：手动转发需要留够时间，取 1 小时（SDK 未从根导出
/// `UNLIMITED_INVITE_LIFETIME`，这里用显式值避免依赖内部模块）。
const INVITE_VALIDITY_SECS: u64 = 3600;

/// 等待 managed link 进入 `Connected` 的上限。
///
/// SDK 单次 `open_link`/`accept_link` 自带 12 秒超时，这里略宽，覆盖
/// 一次退避（500ms）加上第二次尝试。
const MANAGED_LINK_TIMEOUT: Duration = Duration::from_secs(30);

/// 配对驱动：`Session` 只依赖这个 trait，真实实现是 [`SdkPairing`]，
/// 测试可注入纯内存实现以保留 mock seam。
#[async_trait]
pub(crate) trait PairingDriver: Send {
    /// 生成 INVITE，进入等待 REPLY 的状态。
    async fn create_invite(&mut self) -> Result<String, TunnelError>;

    /// 解码对面 INVITE，返回 `(REPLY, 本端比较码)`。
    async fn join_invite(&mut self, invite: &str) -> Result<(String, String), TunnelError>;

    /// 收下对面 REPLY，返回本端比较码。
    async fn provide_reply(&mut self, reply: &str) -> Result<String, TunnelError>;

    /// 完成比较码确认、`connect_transport` 并等待 managed link 就绪。
    async fn confirm(&mut self) -> Result<ConnectedLink, TunnelError>;

    /// 释放配对期间持有的 SDK 资源（幂等）。
    async fn shutdown(&mut self);
}

/// 配对完成后会话需要的最小信息。
pub(crate) struct ConnectedLink {
    /// managed link（跨 generation 自动重连的 [`DataLink`]）。
    pub link: Arc<dyn DataLink>,
    /// 本端是否由 bootstrap 引导：决定规则同步流由谁打开。
    pub bootstrap_outbound: bool,
    /// 目标端审计日志用的稳定会话标识。
    pub peer_session: Arc<str>,
}

/// 配对阶段；命令乱序时返回 `TunnelError::Pairing` 而不是隐式重置。
enum Stage {
    Idle,
    /// 创建端：已发 INVITE，等 REPLY。
    WaitingReply(PendingCreator),
    /// 创建端：已收 REPLY，等 `ConfirmComparison`。
    Creator {
        ready: ReadyCreator,
        comparison: String,
    },
    /// 加入端：已发 REPLY，等 `ConfirmComparison`。
    Joiner {
        ready: ReadyJoiner,
        comparison: String,
    },
}

/// 本端候选地址的来源。
enum Candidates {
    /// `begin_*_auto`：枚举真实网卡 + 默认 STUN（生产路径）。
    Auto,
    /// 显式接口 + 无 STUN（确定性环回测试、CI）。
    #[cfg_attr(not(test), allow(dead_code))]
    Interfaces(Vec<SocketAddr>),
}

/// 真实 SDK 配对驱动。
pub(crate) struct SdkPairing {
    candidates: Candidates,
    stage: Stage,
    peer: Option<Arc<ConnectedTransportPeer>>,
}

impl SdkPairing {
    /// 生产默认：自动枚举网卡与默认 STUN。
    pub(crate) fn auto() -> Self {
        Self {
            candidates: Candidates::Auto,
            stage: Stage::Idle,
            peer: None,
        }
    }

    /// 只用给定本机地址、不发 STUN（环回验收用 `127.0.0.1:0`）。
    #[cfg(test)]
    pub(crate) fn with_interfaces(interfaces: Vec<SocketAddr>) -> Self {
        Self {
            candidates: Candidates::Interfaces(interfaces),
            stage: Stage::Idle,
            peer: None,
        }
    }

    fn assert_idle(&self, operation: &str) -> Result<(), TunnelError> {
        if matches!(self.stage, Stage::Idle) {
            Ok(())
        } else {
            Err(TunnelError::Pairing(format!(
                "配对已在进行中，不能再次{operation}"
            )))
        }
    }
}

#[async_trait]
impl PairingDriver for SdkPairing {
    async fn create_invite(&mut self) -> Result<String, TunnelError> {
        self.assert_idle("创建 INVITE")?;
        let (pending, invite) = match &self.candidates {
            Candidates::Auto => begin_creator_auto().await,
            Candidates::Interfaces(interfaces) => {
                begin_creator(interfaces, &[], now_secs(), INVITE_VALIDITY_SECS).await
            }
        }
        .map_err(|error| pairing_error("生成 INVITE", error))?;
        self.stage = Stage::WaitingReply(pending);
        Ok(invite)
    }

    async fn join_invite(&mut self, invite: &str) -> Result<(String, String), TunnelError> {
        self.assert_idle("加入 INVITE")?;
        let joined = match &self.candidates {
            Candidates::Auto => begin_joiner_auto(invite).await,
            Candidates::Interfaces(interfaces) => {
                begin_joiner(invite, interfaces, &[], now_secs()).await
            }
        }
        .map_err(|error| pairing_error("解析 INVITE", error))?;
        let (ready, reply) = joined;
        let comparison = ready.comparison_code();
        self.stage = Stage::Joiner {
            ready,
            comparison: comparison.clone(),
        };
        Ok((reply, comparison))
    }

    async fn provide_reply(&mut self, reply: &str) -> Result<String, TunnelError> {
        let stage = std::mem::replace(&mut self.stage, Stage::Idle);
        let pending = match stage {
            Stage::WaitingReply(pending) => pending,
            other => {
                self.stage = other;
                return Err(TunnelError::Pairing(
                    "尚未创建或已处理过 REPLY，不能再次提供 REPLY".into(),
                ));
            }
        };
        let ready = pending
            .receive_reply_now(reply)
            .map_err(|error| pairing_error("解析 REPLY", error))?;
        let comparison = ready.comparison_code();
        self.stage = Stage::Creator {
            ready,
            comparison: comparison.clone(),
        };
        Ok(comparison)
    }

    async fn confirm(&mut self) -> Result<ConnectedLink, TunnelError> {
        let stage = std::mem::replace(&mut self.stage, Stage::Idle);
        let (ready, comparison) = match stage {
            Stage::Creator { ready, comparison } => (CreatorOrJoiner::Creator(ready), comparison),
            Stage::Joiner { ready, comparison } => (CreatorOrJoiner::Joiner(ready), comparison),
            other => {
                self.stage = other;
                return Err(TunnelError::Pairing(
                    "配对尚未就绪：先创建 INVITE 或加入 INVITE".into(),
                ));
            }
        };

        // v0.1 不阻塞等人：比较码由本端自动核对（SPEC §7.2）。
        let mut confirmation =
            confirmation_for(&ready).map_err(|error| pairing_error("读取比较码", error))?;
        confirmation
            .confirm(&comparison)
            .map_err(|error| TunnelError::Pairing(format!("确认比较码失败：{error:?}")))?;

        let peer = match ready {
            CreatorOrJoiner::Creator(ready) => ready.connect_transport_now(&confirmation).await,
            CreatorOrJoiner::Joiner(ready) => ready.connect_transport_now(&confirmation).await,
        }
        .map_err(|error| pairing_error("建立认证连接", error))?;
        let peer = Arc::new(peer);
        let managed = peer.manage_link();
        wait_connected(&managed).await?;

        let bootstrap_outbound = peer.diagnostic().bootstrap_outbound;
        tracing::info!(
            bootstrap_outbound,
            peer_session = %fingerprint(&comparison),
            "配对完成，managed 数据链路已连接"
        );
        let link: Arc<dyn DataLink> = Arc::new(SdkManagedLink::new(managed));
        self.peer = Some(peer);
        Ok(ConnectedLink {
            link,
            bootstrap_outbound,
            peer_session: fingerprint(&comparison),
        })
    }

    async fn shutdown(&mut self) {
        self.stage = Stage::Idle;
        let Some(peer) = self.peer.take() else {
            return;
        };
        // manage_link 的 worker 持有一份 Arc；drop 数据链路后 worker 收到停止
        // 信号才会退出。最多等 500ms，再回退到直接关闭 bootstrap（§9.5 G4）。
        let mut peer = peer;
        for _ in 0..25 {
            match Arc::try_unwrap(peer) {
                Ok(peer) => {
                    peer.shutdown().await;
                    return;
                }
                Err(shared) => {
                    peer = shared;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        tracing::warn!("managed link 仍有其他引用，直接关闭 bootstrap（退出路径降级）");
        peer.bootstrap
            .close(quinn::VarInt::from_u32(0), b"tunnel session stopped");
    }
}

/// 两端 `Ready*` 类型不同，确认阶段需要分支。
enum CreatorOrJoiner {
    Creator(ReadyCreator),
    Joiner(ReadyJoiner),
}

fn confirmation_for(ready: &CreatorOrJoiner) -> Result<ManualConfirmation, DirectPeerError> {
    match ready {
        CreatorOrJoiner::Creator(ready) => ready.confirmation(),
        CreatorOrJoiner::Joiner(ready) => ready.confirmation(),
    }
}

/// 等待 [`ManagedAuthenticatedLink`] 进入 `Connected`（带超时）。
async fn wait_connected(managed: &p2p_sdk::ManagedAuthenticatedLink) -> Result<(), TunnelError> {
    let mut status: watch::Receiver<p2p_sdk::ManagedLinkStatus> = managed.subscribe();
    let waiting = async {
        loop {
            if status.borrow().connection.is_some() {
                return Ok(());
            }
            if status.changed().await.is_err() {
                return Err(TunnelError::Pairing("managed link 已停止".into()));
            }
        }
    };
    tokio::time::timeout(MANAGED_LINK_TIMEOUT, waiting)
        .await
        .map_err(|_| {
            TunnelError::Pairing(format!(
                "等待数据链路连接超时（{} 秒）",
                MANAGED_LINK_TIMEOUT.as_secs()
            ))
        })?
}

/// `DirectPeerError` 没有 `Display`，用 `Debug` 保留具体原因。
fn pairing_error(context: &str, error: DirectPeerError) -> TunnelError {
    TunnelError::Pairing(format!("{context}失败：{error:?}"))
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 由比较码派生稳定的 16 位十六进制会话标识（FNV-1a 64）。
///
/// 只用于日志关联，不暴露比较码本身，也不承载安全语义。
fn fingerprint(comparison: &str) -> Arc<str> {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in comparison.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    Arc::from(format!("{hash:016x}"))
}

/// Ticket 07：真实 SDK 的 managed link 故障/恢复验证。
///
/// 只在 loopback（`127.0.0.1:0`）上配对，不依赖 STUN/外网。
#[cfg(test)]
mod tests {
    use super::*;
    use p2p_sdk::{ManagedAuthenticatedLink, ManagedLinkPhase, ManagedLinkStatus};

    use crate::test_support::{real_sdk_guard, REAL_SDK_WAIT};

    type Peers = (
        Arc<ConnectedTransportPeer>,
        ManagedAuthenticatedLink,
        Arc<ConnectedTransportPeer>,
        ManagedAuthenticatedLink,
    );

    /// 完成真实 SDK 配对，但保留 managed link 句柄以便测试主动制造故障。
    async fn connect_peers() -> Peers {
        let mut creator = SdkPairing::with_interfaces(vec!["127.0.0.1:0".parse().expect("地址")]);
        let mut joiner = SdkPairing::with_interfaces(vec!["127.0.0.1:0".parse().expect("地址")]);

        let invite = creator.create_invite().await.expect("创建 INVITE");
        let (reply, _) = joiner.join_invite(&invite).await.expect("加入 INVITE");
        creator.provide_reply(&reply).await.expect("提供 REPLY");

        let (creator_side, joiner_side) = tokio::join!(
            confirm_keep_managed(&mut creator),
            confirm_keep_managed(&mut joiner),
        );
        let (creator_peer, creator_managed) = creator_side;
        let (joiner_peer, joiner_managed) = joiner_side;
        (creator_peer, creator_managed, joiner_peer, joiner_managed)
    }

    async fn confirm_keep_managed(
        pairing: &mut SdkPairing,
    ) -> (Arc<ConnectedTransportPeer>, ManagedAuthenticatedLink) {
        let stage = std::mem::replace(&mut pairing.stage, Stage::Idle);
        let (ready, comparison) = match stage {
            Stage::Creator { ready, comparison } => (CreatorOrJoiner::Creator(ready), comparison),
            Stage::Joiner { ready, comparison } => (CreatorOrJoiner::Joiner(ready), comparison),
            other => {
                pairing.stage = other;
                panic!("配对尚未就绪");
            }
        };
        let mut confirmation = confirmation_for(&ready).expect("读取比较码");
        confirmation.confirm(&comparison).expect("确认比较码");
        let peer = match ready {
            CreatorOrJoiner::Creator(ready) => ready.connect_transport_now(&confirmation).await,
            CreatorOrJoiner::Joiner(ready) => ready.connect_transport_now(&confirmation).await,
        }
        .expect("建立认证连接");
        let peer = Arc::new(peer);
        let managed = peer.manage_link();
        wait_connected(&managed)
            .await
            .expect("等待 managed link Connected");
        (peer, managed)
    }

    /// 等到 managed link 出现一次**新认证连接**，返回新 generation。
    ///
    /// 不断言瞬态 `Reconnecting` 阶段：watch 通道会合并中间值，而 `Reconnecting`
    /// 到下一次 `Connected` 的窗口可能短于调度延迟（尤其在并行负载下），
    /// 先等一端、再等另一端时，第二端可能已经回到 `Connected`，于是为一个
    /// 不会再出现的阶段白等整个预算（120 秒超时的真实根因）。
    ///
    /// `generation` 只在新的 mTLS + 会话 HMAC 认证连接就绪后递增；旧连接断开
    /// 后 SDK 必然先转入 `Reconnecting` 才可能建立新链路，因此「阶段为
    /// `Connected` 且 generation 递增」比抓瞬态阶段更严格，且不会错过。
    async fn wait_next_generation(
        status: &mut watch::Receiver<ManagedLinkStatus>,
        previous: u64,
        side: &str,
    ) -> u64 {
        let deadline = tokio::time::Instant::now() + REAL_SDK_WAIT;
        loop {
            let current = status.borrow_and_update().clone();
            if current.phase == ManagedLinkPhase::Connected && current.generation > previous {
                return current.generation;
            }
            assert!(
                !matches!(
                    current.phase,
                    ManagedLinkPhase::Stopped | ManagedLinkPhase::AuthenticationFailed
                ),
                "{side} managed link 进入不可恢复状态: {current:?}"
            );
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !left.is_zero(),
                "{side} 等待 managed link 重连超时: 当前阶段 {:?}，generation {} 未超过 {previous}",
                current.phase,
                current.generation
            );
            tokio::time::timeout(left, status.changed())
                .await
                .expect("等待 managed link 重连超时")
                .expect("managed link 状态通道关闭");
        }
    }

    /// 在 managed link 上做一次双向字节往返，证明数据平面可用。
    async fn round_trip(
        creator: &ManagedAuthenticatedLink,
        joiner: &ManagedAuthenticatedLink,
        payload: &[u8],
    ) {
        let creator_conn = creator.current().expect("创建端 managed 已连接");
        let joiner_conn = joiner.current().expect("加入端 managed 已连接");
        let (mut send, mut recv) = creator_conn.open_bi().await.expect("打开双向流");
        send.write_all(payload).await.expect("写入数据");
        let (mut jsend, mut jrecv) = joiner_conn.accept_bi().await.expect("接受双向流");
        let mut got = vec![0u8; payload.len()];
        jrecv.read_exact(&mut got).await.expect("加入端读数据");
        assert_eq!(got, payload, "managed link 字节必须一致");
        jsend.write_all(b"pong").await.expect("加入端回写");
        let mut back = [0u8; 4];
        recv.read_exact(&mut back).await.expect("创建端读回");
        assert_eq!(&back, b"pong");
    }

    async fn shutdown_peers(peers: Peers) {
        let (creator_peer, creator_managed, joiner_peer, joiner_managed) = peers;
        creator_managed.shutdown().await;
        joiner_managed.shutdown().await;
        drop(creator_peer);
        drop(joiner_peer);
    }

    /// 杀掉 managed 连接：SDK 自动重拨，两端回到 Connected 且数据恢复。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn managed_link_recovers_after_connection_loss() {
        let _serial = real_sdk_guard().await;
        let peers = connect_peers().await;
        let (_, creator_managed, _, joiner_managed) = &peers;
        let mut creator_status = creator_managed.subscribe();
        let mut joiner_status = joiner_managed.subscribe();
        let creator_generation = creator_status.borrow().generation;
        let joiner_generation = joiner_status.borrow().generation;

        creator_managed
            .current()
            .expect("初始 managed 已连接")
            .close(quinn::VarInt::from_u32(0), b"ticket-07 test: kill managed");

        // 两端同时等：任何一端先恢复都不会让另一端的「已发生」阶段被错过。
        let (creator_next, joiner_next) = tokio::join!(
            wait_next_generation(&mut creator_status, creator_generation, "创建端"),
            wait_next_generation(&mut joiner_status, joiner_generation, "加入端"),
        );
        assert!(
            creator_next > creator_generation,
            "重连后创建端 generation 必须递增: {creator_generation} -> {creator_next}"
        );
        assert!(
            joiner_next > joiner_generation,
            "重连后加入端 generation 必须递增: {joiner_generation} -> {joiner_next}"
        );
        assert_eq!(creator_status.borrow().phase, ManagedLinkPhase::Connected);
        assert_eq!(joiner_status.borrow().phase, ManagedLinkPhase::Connected);

        round_trip(creator_managed, joiner_managed, b"after-reconnect").await;
        shutdown_peers(peers).await;
    }

    /// bootstrap QUIC 关闭后 managed link 的实测行为（SPEC §9.3 要求）。
    ///
    /// loopback 实测：不能保证 managed link 在 bootstrap 关闭后存活。部分运行
    /// 中加入端会在数秒内收到 `QuicConnection` 关闭并转为 `Reconnecting`，而
    /// 创建端仍停在 `Connected`（两端状态不同步）；也有运行中两端持续存活。
    /// 因此 tunnel v0.1 不依赖 managed link 逃过 bootstrap 关闭：一旦数据链路
    /// 断开，按数据链路丢失处理，最终由 120 秒重连超时进入会话丢失；本仓库
    /// 不伪造无缝恢复，需要 SDK 提供明确的生命周期保证。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bootstrap_close_may_end_managed_link() {
        let _serial = real_sdk_guard().await;
        let peers = connect_peers().await;
        let (creator_peer, creator_managed, _, joiner_managed) = &peers;

        creator_peer.bootstrap.close(
            quinn::VarInt::from_u32(0),
            b"ticket-07 test: bootstrap closed",
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while creator_peer.bootstrap.close_reason().is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "bootstrap 关闭未生效"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !creator_peer.diagnostic().bootstrap_connected,
            "diagnostic 应报告 bootstrap 已关闭"
        );

        // 观察 8 秒；掉线与否都记录，不把 SDK 的偶发行为伪装成保证。
        let mut lost = None;
        for _ in 0..80 {
            let creator_phase = creator_managed.subscribe().borrow().phase;
            let joiner_phase = joiner_managed.subscribe().borrow().phase;
            assert_ne!(
                creator_phase,
                ManagedLinkPhase::AuthenticationFailed,
                "bootstrap 关闭不应触发 AuthenticationFailed"
            );
            assert_ne!(
                joiner_phase,
                ManagedLinkPhase::AuthenticationFailed,
                "bootstrap 关闭不应触发 AuthenticationFailed"
            );
            if creator_phase != ManagedLinkPhase::Connected
                || joiner_phase != ManagedLinkPhase::Connected
            {
                lost = Some((creator_phase, joiner_phase));
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        match lost {
            None => {
                // 本轮未掉线：等价于 bootstrap 关闭后 managed link 仍可传数据。
                round_trip(creator_managed, joiner_managed, b"after-bootstrap-close").await;
            }
            Some((creator_phase, joiner_phase)) => {
                assert!(
                    creator_phase == ManagedLinkPhase::Reconnecting
                        || joiner_phase == ManagedLinkPhase::Reconnecting,
                    "掉线时至少一端应报告 Reconnecting: creator={creator_phase:?} joiner={joiner_phase:?}"
                );
            }
        }
        shutdown_peers(peers).await;
    }
}
