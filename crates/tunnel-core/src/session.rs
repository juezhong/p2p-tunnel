//! `Session`：core 的 actor 骨架、配对状态机与 listener/bridge/规则同步接线。
//!
//! UI 通过 `Command` 通道下发操作，通过 `Event` 通道接收状态。配对由
//! [`crate::pairing`] 的状态机驱动（生产实现调用 p2p-sdk）；测试既可以
//! 注入已就绪的 [`crate::transport::mock::MockLink`]，也可以注入纯内存的
//! [`crate::pairing::PairingDriver`]，两条 seam 都不需要真实网络。
//!
//! 规格来源：`docs/SPEC.md` §4.2、§5.2、§5.4、§6、§7.1–§7.6、§9。

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::api::{Command, Event, Forward, LinkState, TunnelError};
use crate::bridge;
use crate::limits::StreamBudget;
use crate::pairing::{ConnectedLink, PairingDriver, SdkPairing};
use crate::protocol::{RuleFrame, CODE_CANCELLED};
use crate::rule_sync::{self, FarewellSignal, PeerUpdate, StateLostSignal};
use crate::transport::{open_bi_with_timeout, DataLink};

/// 命令通道容量。UI 是低频率的人机操作，32 足够且能自然施加背压。
const COMMAND_CHANNEL: usize = 32;
/// 事件通道容量。状态变化是低频事件，UI 处理不过来时应让 core 等待。
const EVENT_CHANNEL: usize = 64;
/// 优雅退出清理上限，见 `docs/SPEC.md` §8.5。
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// 配对命令通道容量：配对命令是串行的人机操作。
const PAIRING_COMMAND_CHANNEL: usize = 4;
/// 配对事件通道容量。
const PAIRING_EVENT_CHANNEL: usize = 8;
/// 重连期间新 TCP 连接等待链路恢复的上限（`docs/SPEC.md` §9.1）。
const RECONNECT_WAIT: Duration = Duration::from_secs(5);
/// 连续 `Reconnecting` 的上限；超过即按「断开连接」收尾并退出 0（`docs/SPEC.md` §9.2）。
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// 主动退出前发送 `Farewell` 的上限；失败/超时不阻塞退出（`docs/SPEC.md` §9.5）。
const FAREWELL_TIMEOUT: Duration = Duration::from_secs(1);
/// 发送 `Farewell` 后等待对端关闭流的宽限；只影响退出延迟。
const FAREWELL_GRACE: Duration = Duration::from_millis(500);
/// 退出时等待 bridge 任务收尾的上限。
const TASK_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);

/// 配对驱动向会话上报的事件。
enum PairingEvent {
    InviteCode(String),
    ReplyCode(String),
    ComparisonCode(String),
    Connected(ConnectedLink),
    Failed(TunnelError),
}

/// 会话向配对驱动下发的命令。
enum PairingCommand {
    CreateInvite,
    JoinInvite(String),
    ProvideReply(String),
    Confirm,
    Stop,
}

/// 一个已启动的 core 会话。
///
/// 由 `Session::spawn` / `Session::spawn_with_link` /
/// `Session::spawn_with_link_role` 在 Tokio runtime 内创建；`Drop` 不会
/// 隐式关闭，必须调用 `shutdown()`（CLI 的 `quit`/`Ctrl-C` 路径）。
pub struct Session {
    commands: mpsc::Sender<Command>,
    events: mpsc::Receiver<Event>,
    worker: JoinHandle<()>,
}

impl Session {
    /// 启动 core actor，并启用真实 SDK 配对（`docs/SPEC.md` §7.2）。
    ///
    /// 配对完成前没有数据链路，`AddForward` 会被拒绝；配对完成后本端
    /// 自动开始规则同步与 listener 服务。
    pub fn spawn() -> Self {
        Self::spawn_inner(None, false, Some(Box::new(SdkPairing::auto())))
    }

    /// 以一条已就绪的数据链路启动 core（被动端：不主动打开规则同步流）。
    ///
    /// 配对完成、`manage_link()` 返回 `Connected` 后进入该状态：本端
    /// 立即运行 `accept_bi()` 循环，允许 `AddForward` 启动 listener。
    /// 测试用 [`crate::transport::mock::MockLink`] 注入；生产由
    /// `crate::transport::sdk::SdkManagedLink`（SDK 管理式链路）注入。
    pub fn spawn_with_link(link: Arc<dyn DataLink>) -> Self {
        Self::spawn_inner(Some(link), false, None)
    }

    /// 以一条已就绪的数据链路启动 core，并指定是否由本端引导规则同步流。
    ///
    /// `bootstrap_outbound` 对应 SDK `diagnostic().bootstrap_outbound`：
    /// 只有该端在数据链路 `Connected` 后打开**唯一**一条规则同步双向流
    /// （`docs/SPEC.md` §5.4）；另一端从 `accept_bi()` 接受它。
    pub fn spawn_with_link_role(link: Arc<dyn DataLink>, bootstrap_outbound: bool) -> Self {
        Self::spawn_inner(Some(link), bootstrap_outbound, None)
    }

    /// 注入自定义配对驱动（测试与嵌入）。数据链路只能由驱动在
    /// `confirm()` 后提供，因此这里不接受预置链路。
    #[cfg(test)]
    pub(crate) fn spawn_with_pairing(driver: Box<dyn PairingDriver>) -> Self {
        Self::spawn_inner(None, false, Some(driver))
    }

    fn spawn_inner(
        link: Option<Arc<dyn DataLink>>,
        bootstrap_outbound: bool,
        driver: Option<Box<dyn PairingDriver>>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel::<Command>(COMMAND_CHANNEL);
        let (event_tx, event_rx) = mpsc::channel::<Event>(EVENT_CHANNEL);
        let (snapshots, _) = rule_sync::snapshot_channel();
        let (link_lost, _) = watch::channel(0u64);
        let (farewell, farewell_events) = mpsc::channel(1);
        let budget = StreamBudget::new();
        let tasks = TaskRegistry::new();

        let link_state = link.as_ref().map(|link| link.subscribe_link_state());
        let (pairing_commands, pairing_events, pairing_task) = match driver {
            Some(driver) => {
                let (command_tx, command_rx) =
                    mpsc::channel::<PairingCommand>(PAIRING_COMMAND_CHANNEL);
                let (event_tx, event_rx) = mpsc::channel::<PairingEvent>(PAIRING_EVENT_CHANNEL);
                let task = tokio::spawn(run_pairing(driver, command_rx, event_tx));
                (Some(command_tx), Some(event_rx), Some(task))
            }
            None => (None, None, None),
        };

        let worker = tokio::spawn(async move {
            Worker {
                link,
                events: event_tx,
                rules: HashMap::new(),
                peer_rules: HashMap::new(),
                peer_known: None,
                snapshots,
                updates: None,
                state_tasks: Vec::new(),
                bootstrap_outbound,
                link_lost,
                link_state: link_state.clone(),
                reconnecting_since: None,
                reconnect_deadline: None,
                budget,
                pairing_commands,
                pairing_events,
                pairing_task,
                peer_session: Arc::from("unspecified"),
                farewell,
                farewell_events: Some(farewell_events),
                tasks,
                ending: false,
            }
            .run(command_rx)
            .await;
        });

        Self {
            commands: command_tx,
            events: event_rx,
            worker,
        }
    }

    /// 取得命令发送端；可以 clone 给 UI 的其他任务。
    pub fn sender(&self) -> mpsc::Sender<Command> {
        self.commands.clone()
    }

    /// 等待下一个事件；core 结束后返回 `None`。
    pub async fn recv(&mut self) -> Option<Event> {
        self.events.recv().await
    }

    /// 发送 `Shutdown` 并等待 core 清理；超过 5 秒直接放弃等待。
    pub async fn shutdown(self) {
        let Self {
            commands,
            mut events,
            worker,
        } = self;

        let _ = commands.send(Command::Shutdown).await;
        // 先排空事件，避免 core 因事件通道满而无法退出。
        let drained = tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
            while events.recv().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            worker.abort();
            let _ = worker.await;
        } else {
            let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, worker).await;
        }
    }
}

/// 一条本机规则在运行时的资源：规则本体、停止信号、accept 任务与活动连接计数。
struct LocalRule {
    forward: Forward,
    stop: watch::Sender<bool>,
    /// 规则关闭信号（错误码），下发给该规则所有活动连接。
    close: watch::Sender<u32>,
    accept: JoinHandle<()>,
    active: Arc<AtomicUsize>,
}

/// 本会话派生的 bridge 任务登记表；退出时统一收尾，避免任务仍持有
/// TCP/QUIC 资源时进程就结束（`docs/SPEC.md` §9.5）。
#[derive(Clone, Default)]
struct TaskRegistry {
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl TaskRegistry {
    fn new() -> Self {
        Self::default()
    }

    /// 登记一个 bridge 任务；顺手清掉已经结束的句柄，避免长会话里无限累积。
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        let mut tasks = self.tasks.lock().expect("bridge 任务登记表锁");
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    /// 等待全部任务结束；超过 `limit` 仍未结束的直接 abort。
    async fn finish_all(&self, limit: Duration) {
        let handles: Vec<JoinHandle<()>> = {
            let mut tasks = self.tasks.lock().expect("bridge 任务登记表锁");
            std::mem::take(&mut *tasks)
        };
        let deadline = tokio::time::Instant::now() + limit;
        for mut handle in handles {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                handle.abort();
                continue;
            }
            if tokio::time::timeout(remaining, &mut handle).await.is_err() {
                handle.abort();
            }
        }
    }
}

/// actor 内部状态；所有可变状态只在 worker task 内访问。
struct Worker {
    link: Option<Arc<dyn DataLink>>,
    events: mpsc::Sender<Event>,
    rules: HashMap<u16, LocalRule>,
    /// 最近一次收到的对端规则（全量快照），用于回推 `RemoteClosed`。
    peer_rules: HashMap<u16, Forward>,
    /// `None` 尚未同步；`Some(true)` 已知；`Some(false)` 未知（已发事件）。
    peer_known: Option<bool>,
    /// 本端规则全量快照发送端；单槽 watch 自动合并连续变化。
    snapshots: watch::Sender<Vec<Forward>>,
    /// 规则同步模块上报的对端变化。
    updates: Option<mpsc::Receiver<PeerUpdate>>,
    /// 首字节分流、状态流分发与出站监督任务。
    state_tasks: Vec<JoinHandle<()>>,
    /// 本端是否负责打开规则同步流（SDK `bootstrap_outbound`）。
    bootstrap_outbound: bool,
    /// 链路断开代数：每次断开 +1，旧连接据此 RST 本地 TCP（§5.3、§9.1）。
    link_lost: watch::Sender<u64>,
    /// 数据链路阶段订阅；没有链路时为 `None`。
    link_state: Option<watch::Receiver<LinkState>>,
    /// 进入连续 `Reconnecting` 的时刻；恢复或会话结束时清空。
    reconnecting_since: Option<tokio::time::Instant>,
    /// 断开连接的判定死线（进入 `Reconnecting` 后 5 秒）。
    reconnect_deadline: Option<tokio::time::Instant>,
    /// 本侧数据流名额（listener 出站 + 目标侧入站共享）。
    budget: StreamBudget,
    /// 配对命令发送端；没有启用配对时为 `None`。
    pairing_commands: Option<mpsc::Sender<PairingCommand>>,
    /// 配对驱动上报的事件；由 `run` 取出轮询。
    pairing_events: Option<mpsc::Receiver<PairingEvent>>,
    /// 配对任务；退出时停止并等待它释放 SDK 资源。
    pairing_task: Option<JoinHandle<()>>,
    /// 目标端审计日志用的稳定会话标识（比较码派生，不含比较码本身）。
    peer_session: Arc<str>,
    /// 对端 `Farewell` 通知的发送端（分流器 → 主循环）；保留一份避免通道关闭。
    farewell: mpsc::Sender<FarewellSignal>,
    /// 对端 `Farewell` 通知的接收端，由 [`Worker::run`] 取出轮询。
    farewell_events: Option<mpsc::Receiver<FarewellSignal>>,
    /// 本会话派生的 bridge 任务；退出时统一收尾。
    tasks: TaskRegistry,
    /// 已进入结束路径：此后不再上报重连/断开计时相关事件（SPEC §9.5）。
    ending: bool,
}

/// 会话结束原因；决定清理时是否回发 `Farewell`、是否上报 `SessionLost`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndReason {
    /// 本端要求退出（`Shutdown`、Ctrl-C、stdin EOF）。
    LocalExit,
    /// 对端通过 `Farewell` 主动退出。
    PeerFarewell,
    /// 重连 5 秒未恢复：按「断开连接」退出。
    Disconnected,
    /// 认证失败或链路停止：会话不可恢复。
    SessionLost,
}

impl Worker {
    async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        let (lost_tx, mut lost_rx) = mpsc::channel(1);
        if self.link.is_some() {
            self.link_state = self.link.as_ref().map(|link| link.subscribe_link_state());
            self.start_state_streams(lost_tx.clone());
            let _ = self.events.send(Event::Ready).await;
        }

        let mut updates = self.updates.take();
        let mut farewells = self.farewell_events.take();
        let mut pairing = self.pairing_events.take();
        // 阶段订阅与 run 循环共享；在配对成功后重新取一次。
        let mut phases = self.link_state.clone();
        // `accept_bi` 循环退出（SDK 已停止）时的一次性兜底信号。
        let mut watching_loss = true;
        // 结束原因：除了 `SessionLost` 之外的结束都不上报「会话丢失」。
        let mut end_reason = EndReason::LocalExit;
        loop {
            tokio::select! {
                command = commands.recv() => {
                    match command {
                        Some(Command::Shutdown) | None => break,
                        Some(command) => {
                            self.handle_command(command).await;
                            // `Stop` 会释放链路并清空阶段订阅，必须让 select
                            // 看到新的订阅状态。
                            phases = self.link_state.clone();
                        }
                    }
                }
                update = recv_optional(updates.as_mut()), if updates.is_some() => {
                    match update {
                        Some(update) => self.apply_peer_update(update).await,
                        None => updates = None,
                    }
                }
                event = recv_optional(pairing.as_mut()), if pairing.is_some() => {
                    match event {
                        Some(event) => {
                            self.handle_pairing_event(event, &lost_tx).await;
                            // 配对成功后才有 update 通道与阶段订阅。
                            updates = self.updates.take();
                            phases = self.link_state.clone();
                        }
                        None => pairing = None,
                    }
                }
                phase = recv_link_phase(&mut phases) => {
                    match phase {
                        Some(phase) => {
                            if let Some(reason) = self.handle_link_state(phase, &lost_tx).await {
                                end_reason = reason;
                                break;
                            }
                            updates = self.updates.take();
                        }
                        None => phases = None,
                    }
                }
                signal = lost_rx.recv(), if watching_loss => {
                    watching_loss = false;
                    if signal.is_some() {
                        self.handle_link_lost().await;
                        updates = self.updates.take();
                    }
                }
                signal = recv_optional(farewells.as_mut()), if farewells.is_some() => {
                    if signal.is_some() {
                        self.handle_peer_farewell().await;
                        end_reason = EndReason::PeerFarewell;
                        break;
                    }
                    farewells = None;
                }
                _ = reconnect_timeout(self.reconnect_deadline), if self.reconnect_deadline.is_some() => {
                    // SPEC §9.2：重连 5 秒未恢复即按「断开连接」收尾，
                    // 退出码 0，不再等 120 秒报「会话丢失」。
                    tracing::warn!("数据链路重连 5 秒未恢复，按断开连接退出");
                    let _ = self.events.send(Event::Disconnected).await;
                    self.end_session();
                    end_reason = EndReason::Disconnected;
                    break;
                }
            }
        }

        // 退出顺序按 SPEC §9.5：先停 listener、reset 活动连接并收尾 bridge/
        // 状态流，再发 Farewell，最后释放链路与 SDK peer；先发 Farewell 时
        // listener 仍可能接新 TCP，不符合规格顺序。
        self.stop_forwarding().await;
        if end_reason == EndReason::LocalExit {
            // 只有本端主动退出才通知对端；对端 Farewell/断链/会话丢失
            // 的收尾不再发告别。
            self.send_farewell().await;
        }
        self.release_session().await;
        if end_reason == EndReason::SessionLost {
            let _ = self
                .events
                .send(Event::Error(TunnelError::SessionLost))
                .await;
        }
        let _ = self.events.send(Event::LinkState(LinkState::Stopped)).await;
    }

    /// 应用数据链路阶段变化；返回结束原因表示会话已不可恢复，循环应结束。
    ///
    /// 进入结束路径后（`ending`）直接忽略后续阶段，避免对端 `Farewell`
    /// 或断开判定之后又冒出 `Reconnecting` 与断开计时事件（SPEC §9.5）。
    async fn handle_link_state(
        &mut self,
        phase: LinkState,
        lost: &mpsc::Sender<StateLostSignal>,
    ) -> Option<EndReason> {
        if self.ending {
            return None;
        }
        match phase {
            LinkState::Connected => {
                if self.reconnecting_since.take().is_some() {
                    tracing::info!("数据链路已恢复");
                    // 5 秒内恢复（抖动）必须撤销断开倒计时（§9.1、§9.2）；
                    // 否则一次闪断会在恢复后误杀会话。
                    self.reconnect_deadline = None;
                    self.restart_state_streams(lost.clone()).await;
                    // 新链路上重发 Hello + 全量 RuleList（§5.4、§9.1）。
                    self.publish_rules();
                    let _ = self
                        .events
                        .send(Event::LinkState(LinkState::Connected))
                        .await;
                    // `Ready` 表示「配对完成」，一次配对只发一次；重连只发
                    // `LinkState(Connected)`（§9.1 步骤 6）。
                }
                None
            }
            LinkState::Reconnecting => {
                self.handle_link_lost().await;
                None
            }
            LinkState::AuthenticationFailed => {
                // SPEC §9.2：认证失败不可恢复，直接结束会话。
                tracing::warn!("数据链路认证失败，会话结束");
                let _ = self
                    .events
                    .send(Event::LinkState(LinkState::AuthenticationFailed))
                    .await;
                self.end_session();
                Some(EndReason::SessionLost)
            }
            LinkState::Stopped => {
                tracing::warn!("数据链路已停止，会话结束");
                self.end_session();
                Some(EndReason::SessionLost)
            }
            LinkState::Connecting => {
                let _ = self
                    .events
                    .send(Event::LinkState(LinkState::Connecting))
                    .await;
                None
            }
        }
    }

    /// 处理一次链路断开：RST 活动连接、保留 listener、标记对端规则过期。
    async fn handle_link_lost(&mut self) {
        if self.ending || self.reconnecting_since.is_some() {
            return;
        }
        let now = tokio::time::Instant::now();
        self.reconnecting_since = Some(now);
        self.reconnect_deadline = Some(now + DISCONNECT_TIMEOUT);
        tracing::warn!("数据链路已断开，等待 SDK 重连");
        // 代数 +1：所有旧连接在 wait_abort 中看到代数变化后 RST 本地 TCP。
        let generation = *self.link_lost.borrow() + 1;
        let _ = self.link_lost.send(generation);
        self.abort_state_streams().await;
        self.updates = None;
        let _ = self.events.send(Event::PeerRulesStale).await;
        let _ = self
            .events
            .send(Event::LinkState(LinkState::Reconnecting))
            .await;
    }

    /// 主动退出前尽力发送一次 `Farewell`；失败或超时都不阻塞退出。
    ///
    /// 仅在数据链路仍为 `Connected` 时尝试：断链/重连中本端已经没有可用
    /// 的数据面，发送只会白白等满上限（SPEC §9.5）。
    async fn send_farewell(&mut self) {
        let Some(link) = self.link.clone() else {
            return;
        };
        if !self.is_link_connected() {
            tracing::debug!("数据链路不可用，跳过发送 Farewell");
            return;
        }
        let attempt = tokio::time::timeout(FAREWELL_TIMEOUT, async {
            let stream = open_bi_with_timeout(&*link).await?;
            let (mut read, mut write) = stream.split();
            let frame = RuleFrame::farewell_frame();
            write
                .write_all(&frame)
                .await
                .map_err(|error| TunnelError::Network(format!("发送 Farewell 失败：{error}")))?;
            write.finish()?;
            // 等对端读到 Farewell 并关闭流，缩小「对端先看到断链」的竞态。
            let mut scratch = [0u8; 8];
            let _ = tokio::time::timeout(FAREWELL_GRACE, read.read(&mut scratch)).await;
            Ok::<(), TunnelError>(())
        })
        .await;
        match attempt {
            Ok(Ok(())) => tracing::info!("已发送 Farewell"),
            Ok(Err(error)) => tracing::debug!(%error, "发送 Farewell 失败，继续退出"),
            Err(_) => tracing::debug!("发送 Farewell 超时，继续退出"),
        }
    }

    /// 当前数据链路是否处于 `Connected`；没有链路订阅时视为不可用。
    fn is_link_connected(&self) -> bool {
        self.link_state
            .as_ref()
            .map(|state| *state.borrow() == LinkState::Connected)
            .unwrap_or(false)
    }

    /// 对端主动退出：不进入重连，直接结束会话（SPEC §9.5）。
    ///
    /// 先上报 [`Event::PeerFarewell`]，让 UI 尽快打印「对端已退出」，随后由
    /// `run` 的收尾流程关闭 listener、RST 活动连接并释放链路。
    async fn handle_peer_farewell(&mut self) {
        tracing::info!("收到对端 Farewell，会话结束");
        let _ = self.events.send(Event::PeerFarewell).await;
        self.end_session();
    }

    /// 终止当前链路代的规则同步任务；重连后由 [`Self::restart_state_streams`] 重开。
    async fn abort_state_streams(&mut self) {
        for task in self.state_tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }

    async fn restart_state_streams(&mut self, lost: mpsc::Sender<StateLostSignal>) {
        self.abort_state_streams().await;
        self.updates = None;
        self.start_state_streams(lost);
    }

    /// 进入会话结束路径：清空对端规则、停止重连倒计时，并抑制后续
    /// `Reconnecting`/断开计时事件；listener 由 `stop_forwarding` 关闭。
    ///
    /// 只有 `AuthenticationFailed`/`Stopped` 会再上报 `SessionLost`；
    /// 对端 `Farewell` 与 5 秒断开走正常结束（SPEC §9.2、§9.5）。
    fn end_session(&mut self) {
        self.ending = true;
        self.reconnecting_since = None;
        self.reconnect_deadline = None;
        self.peer_rules.clear();
        self.peer_known = None;
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Shutdown => {}
            // v0.1 CLI 不使用；TUI/GUI 用它停掉转发但保留进程与 core。
            Command::Stop => self.stop_session().await,
            Command::CreateInvite => self.dispatch_pairing(PairingCommand::CreateInvite).await,
            Command::JoinInvite(invite) => {
                let _ = self
                    .events
                    .send(Event::LinkState(LinkState::Connecting))
                    .await;
                self.dispatch_pairing(PairingCommand::JoinInvite(invite))
                    .await;
            }
            Command::ProvideReply(reply) => {
                self.dispatch_pairing(PairingCommand::ProvideReply(reply))
                    .await;
            }
            Command::ConfirmComparison => {
                self.dispatch_pairing(PairingCommand::Confirm).await;
            }
            Command::AddForward(forward) => self.add_forward(forward).await,
            Command::CloseForward { listen_port } => self.close_forward(listen_port).await,
        }
    }

    /// 把配对命令转交给配对驱动；未启用配对时报错而不是静默丢弃。
    async fn dispatch_pairing(&mut self, command: PairingCommand) {
        let Some(commands) = &self.pairing_commands else {
            let _ = self
                .events
                .send(Event::Error(TunnelError::Internal(
                    "本会话未启用配对：请使用 Session::spawn()".into(),
                )))
                .await;
            return;
        };
        if commands.send(command).await.is_err() {
            let _ = self
                .events
                .send(Event::Error(TunnelError::Internal(
                    "配对任务已结束，无法继续配对".into(),
                )))
                .await;
        }
    }

    /// 应用配对驱动的上报：代码事件直接转发，`Connected` 安装数据链路。
    async fn handle_pairing_event(
        &mut self,
        event: PairingEvent,
        lost: &mpsc::Sender<StateLostSignal>,
    ) {
        match event {
            PairingEvent::InviteCode(invite) => {
                let _ = self.events.send(Event::InviteCode(invite)).await;
            }
            PairingEvent::ReplyCode(reply) => {
                let _ = self.events.send(Event::ReplyCode(reply)).await;
            }
            PairingEvent::ComparisonCode(code) => {
                let _ = self.events.send(Event::ComparisonCode(code)).await;
            }
            PairingEvent::Connected(connected) => {
                self.link_state = Some(connected.link.subscribe_link_state());
                self.link = Some(connected.link);
                self.bootstrap_outbound = connected.bootstrap_outbound;
                self.peer_session = connected.peer_session;
                self.start_state_streams(lost.clone());
                let _ = self
                    .events
                    .send(Event::LinkState(LinkState::Connected))
                    .await;
                let _ = self.events.send(Event::Ready).await;
            }
            PairingEvent::Failed(error) => {
                // SPEC §7.2：配对失败只报错，不自动重试。
                tracing::warn!(%error, "配对失败");
                let _ = self.events.send(Event::Error(error)).await;
            }
        }
    }

    /// 启动首字节分流 + 状态流分发；出站端再启动打开/重开监督循环。
    fn start_state_streams(&mut self, lost: mpsc::Sender<StateLostSignal>) {
        let Some(link) = self.link.clone() else {
            return;
        };

        let (update_tx, update_rx) = mpsc::channel::<PeerUpdate>(rule_sync::STATE_UPDATE_CHANNEL);
        self.updates = Some(update_rx);

        let (state_tx, mut state_rx) =
            mpsc::channel::<rule_sync::StateStreamParts>(rule_sync::STATE_STREAM_CHANNEL);
        let inbound_updates = update_tx.clone();
        let inbound_snapshots = self.snapshots.subscribe();
        let inbound_farewell = self.farewell.clone();
        // 分发任务：新状态流替换旧状态流（重开/对端重发）。
        let inbound = tokio::spawn(async move {
            let mut current = ChildGuard::new();
            while let Some(parts) = state_rx.recv().await {
                current
                    .replace(tokio::spawn(rule_sync::serve_inbound_state(
                        parts.read,
                        parts.write,
                        parts.first,
                        inbound_snapshots.clone(),
                        inbound_updates.clone(),
                        inbound_farewell.clone(),
                    )))
                    .await;
            }
        });

        let demux = rule_sync::spawn_demux(
            link.clone(),
            state_tx,
            self.farewell.clone(),
            self.link_lost.subscribe(),
            self.budget.clone(),
            self.peer_session.clone(),
            lost,
        );
        self.state_tasks.push(demux);
        self.state_tasks.push(inbound);

        if self.bootstrap_outbound {
            let outbound = tokio::spawn(rule_sync::supervise_outbound(
                link,
                self.snapshots.subscribe(),
                update_tx,
                self.farewell.clone(),
            ));
            self.state_tasks.push(outbound);
        }
    }

    /// 把最近快照推给状态流发送半；编码与发送在状态流任务里完成。
    fn publish_rules(&self) {
        let mut rules: Vec<Forward> = self.rules.values().map(|rule| rule.forward).collect();
        rules.sort_by_key(|rule| rule.listen_port);
        let _ = self.snapshots.send(rules);
    }

    /// 应用对端规则更新：全量替换，消失的端口回推 `RemoteClosed`。
    async fn apply_peer_update(&mut self, update: PeerUpdate) {
        match update {
            PeerUpdate::Snapshot(rules) => {
                let mut next: HashMap<u16, Forward> = HashMap::new();
                for rule in &rules {
                    next.insert(rule.listen_port, *rule);
                }
                let mut closed: Vec<u16> = self
                    .peer_rules
                    .keys()
                    .copied()
                    .filter(|port| !next.contains_key(port))
                    .collect();
                closed.sort_unstable();
                for listen_port in closed {
                    tracing::info!("远程主动断开了 {listen_port}");
                    let _ = self.events.send(Event::RemoteClosed { listen_port }).await;
                }
                self.peer_rules = next;
                self.peer_known = Some(true);
                let _ = self.events.send(Event::PeerRules(rules)).await;
            }
            PeerUpdate::Unknown => {
                // 只在“已知 → 未知”时通知 UI，避免重开循环刷屏。
                if self.peer_known != Some(false) {
                    self.peer_known = Some(false);
                    let _ = self.events.send(Event::PeerRulesUnknown).await;
                }
            }
        }
    }

    async fn add_forward(&mut self, requested: Forward) {
        // 端口 0 / 重复端口必须在 bind 之前拒绝。
        let forward = match Forward::new(requested.listen_port, requested.peer_port) {
            Ok(forward) => forward,
            Err(error) => {
                let _ = self.events.send(Event::Error(error)).await;
                return;
            }
        };
        if self.link.is_none() {
            let _ = self
                .events
                .send(Event::Error(TunnelError::Network(
                    "会话尚未就绪：数据链路未连接".into(),
                )))
                .await;
            return;
        }
        if self.rules.contains_key(&forward.listen_port) {
            let _ = self
                .events
                .send(Event::Error(TunnelError::Protocol(format!(
                    "监听端口 {} 已有规则",
                    forward.listen_port
                ))))
                .await;
            return;
        }

        let listener = match TcpListener::bind(("127.0.0.1", forward.listen_port)).await {
            Ok(listener) => listener,
            Err(error) => {
                let _ = self
                    .events
                    .send(Event::Error(TunnelError::Bind {
                        port: forward.listen_port,
                        message: error.to_string(),
                    }))
                    .await;
                return;
            }
        };

        let (stop, stop_rx) = watch::channel(false);
        let (close, close_rx) = watch::channel(0u32);
        let active = Arc::new(AtomicUsize::new(0));
        let link = self.link.clone().expect("add_forward 已确认数据链路存在");
        let accept = tokio::spawn(run_listener(
            listener,
            forward,
            ListenerShared {
                link,
                events: self.events.clone(),
                close: close_rx,
                link_lost: self.link_lost.subscribe(),
                link_state: self
                    .link_state
                    .clone()
                    .unwrap_or_else(|| watch::channel(LinkState::Connected).1),
                budget: self.budget.clone(),
                tasks: self.tasks.clone(),
            },
            active.clone(),
            stop_rx,
        ));
        self.rules.insert(
            forward.listen_port,
            LocalRule {
                forward,
                stop,
                close,
                accept,
                active,
            },
        );

        // 本地规则变化：立即向对端重发全量快照（§7.4）。
        self.publish_rules();
        let _ = self
            .events
            .send(Event::ListenerStarted(forward.listen_port))
            .await;
        let _ = self.events.send(Event::RulesChanged).await;
    }

    async fn close_forward(&mut self, listen_port: u16) {
        let Some(rule) = self.rules.remove(&listen_port) else {
            let _ = self
                .events
                .send(Event::Error(TunnelError::RuleNotFound { listen_port }))
                .await;
            return;
        };

        // 先 reset 该规则的活动连接，再停 accept loop 并等它 drop listener，
        // 保证返回后新 TCP 连接失败。
        Self::close_rule(rule, "规则已关闭").await;
        // 规则关闭同样是对端可见的本地变化：重发全量快照。
        self.publish_rules();
        let _ = self.events.send(Event::RulesChanged).await;
    }

    /// `Command::Stop`：停止 listener、reset 活动连接、关闭数据链路，
    /// 但保留进程与 core，后续 `Shutdown` 仍可正常收尾（§7.3、§7.5）。
    async fn stop_session(&mut self) {
        self.stop_listeners("收到 Stop：关闭规则").await;
        self.abort_state_streams().await;
        self.updates = None;
        // 释放数据链路：drop 会让 SDK managed link 的 worker 停止。
        self.link = None;
        self.link_state = None;
        self.reconnecting_since = None;
        self.reconnect_deadline = None;
        self.peer_rules.clear();
        self.peer_known = None;
        let _ = self.events.send(Event::LinkState(LinkState::Stopped)).await;
    }

    /// 关闭全部 listener，并等待 accept loop 释放监听端口。
    async fn stop_listeners(&mut self, reason: &'static str) {
        let rules: Vec<(u16, LocalRule)> = self.rules.drain().collect();
        for (_, rule) in rules {
            Self::close_rule(rule, reason).await;
        }
    }

    /// 单条规则的收尾：reset 活动连接 → 停 accept loop → 等 listener 释放。
    async fn close_rule(rule: LocalRule, reason: &'static str) {
        let listen_port = rule.forward.listen_port;
        let _ = rule.close.send(CODE_CANCELLED);
        let _ = rule.stop.send(true);
        let _ = rule.accept.await;
        tracing::debug!(
            listen_port,
            active = rule.active.load(Ordering::SeqCst),
            reason,
            "规则已关闭"
        );
    }

    /// 退出清理第一阶段（SPEC §9.5）：停所有 listener → reset 活动连接并等
    /// bridge 收尾 → 关闭状态流。数据链路保留给随后的 Farewell 使用。
    async fn stop_forwarding(&mut self) {
        // 从这里开始不再处理重连/断开计时，避免收尾期间又上报状态。
        self.ending = true;
        self.stop_listeners("退出时关闭规则").await;
        // 等 bridge 任务把 RST 写到本地 TCP 并释放数据流名额，再拆链路。
        self.tasks.finish_all(TASK_CLEANUP_TIMEOUT).await;
        self.abort_state_streams().await;
    }

    /// 退出清理第二阶段：释放数据链路（drop 会让 SDK managed link 的
    /// worker 停止），停止配对任务并释放 SDK peer。
    async fn release_session(&mut self) {
        self.link = None;
        if let Some(commands) = self.pairing_commands.take() {
            let _ = commands.send(PairingCommand::Stop).await;
        }
        if let Some(task) = self.pairing_task.take() {
            // 配对任务退出时会调用驱动 shutdown；5 秒仍不结束就放弃等待。
            if tokio::time::timeout(SHUTDOWN_TIMEOUT, task).await.is_err() {
                tracing::warn!("配对任务未在退出上限内结束");
            }
        }
    }
}

/// 等待可选 mpsc 通道的下一条消息；通道未初始化时永不就绪。
async fn recv_optional<T>(receiver: Option<&mut mpsc::Receiver<T>>) -> Option<T> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

/// 等待数据链路的下一次阶段变化；没有链路时永不就绪。
async fn recv_link_phase(phase: &mut Option<watch::Receiver<LinkState>>) -> Option<LinkState> {
    match phase {
        Some(receiver) => match receiver.changed().await {
            Ok(()) => Some(*receiver.borrow_and_update()),
            Err(_) => None,
        },
        None => std::future::pending().await,
    }
}

/// 会话丢失倒计时；没有重连时永不就绪。
async fn reconnect_timeout(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// 配对任务：串行执行配对命令，把 SDK 结果翻译成会话事件。
///
/// 命令通道关闭或收到 `Stop` 后调用驱动 `shutdown()`，保证 SDK 的
/// managed link / bootstrap 在会话退出时释放（`docs/SPEC.md` §9.5）。
async fn run_pairing(
    mut driver: Box<dyn PairingDriver>,
    mut commands: mpsc::Receiver<PairingCommand>,
    events: mpsc::Sender<PairingEvent>,
) {
    while let Some(command) = commands.recv().await {
        // 每个命令先得到一个“要发的事件列表”，失败统一翻译成 Failed；
        // 发送路径只有一条，通道关闭即结束。
        let outcome: Result<Vec<PairingEvent>, TunnelError> = match command {
            PairingCommand::CreateInvite => driver
                .create_invite()
                .await
                .map(|invite| vec![PairingEvent::InviteCode(invite)]),
            PairingCommand::JoinInvite(invite) => {
                driver
                    .join_invite(&invite)
                    .await
                    .map(|(reply, comparison)| {
                        vec![
                            PairingEvent::ReplyCode(reply),
                            PairingEvent::ComparisonCode(comparison),
                        ]
                    })
            }
            PairingCommand::ProvideReply(reply) => driver
                .provide_reply(&reply)
                .await
                .map(|comparison| vec![PairingEvent::ComparisonCode(comparison)]),
            PairingCommand::Confirm => driver
                .confirm()
                .await
                .map(|connected| vec![PairingEvent::Connected(connected)]),
            PairingCommand::Stop => break,
        };
        let pending_events = match outcome {
            Ok(events) => events,
            Err(error) => vec![PairingEvent::Failed(error)],
        };
        let mut closed = false;
        for event in pending_events {
            if !send_pairing(&events, event).await {
                closed = true;
                break;
            }
        }
        if closed {
            break;
        }
    }
    driver.shutdown().await;
}

/// 发送一条配对事件；返回 `false` 表示接收端已关闭。
async fn send_pairing(events: &mpsc::Sender<PairingEvent>, event: PairingEvent) -> bool {
    events.send(event).await.is_ok()
}

/// 持有“当前状态流”任务的卫兵：替换或父任务被 abort 时一并取消子任务，
/// 避免重连后旧状态流任务泄漏（ticket 07 验收框）。
struct ChildGuard(Option<JoinHandle<()>>);

impl ChildGuard {
    fn new() -> Self {
        Self(None)
    }

    async fn replace(&mut self, task: JoinHandle<()>) {
        if let Some(previous) = self.0.take() {
            previous.abort();
            let _ = previous.await;
        }
        self.0 = Some(task);
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// listener accept 循环需要共享的下游资源。
struct ListenerShared {
    link: Arc<dyn DataLink>,
    events: mpsc::Sender<Event>,
    close: watch::Receiver<u32>,
    link_lost: watch::Receiver<u64>,
    link_state: watch::Receiver<LinkState>,
    budget: StreamBudget,
    tasks: TaskRegistry,
}

/// 本机 listener 的 accept 循环：每条 TCP 启动一个 bridge 任务。
async fn run_listener(
    listener: TcpListener,
    forward: Forward,
    shared: ListenerShared,
    active: Arc<AtomicUsize>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            _ = stop.changed() => break,
            result = listener.accept() => result,
        };
        match accepted {
            Ok((tcp, peer)) => {
                // 超限：关闭新 TCP 并上报本地错误（§5.2）：出站方向还没有
                // 数据流可 reset，因此不会向对端发 0x04。
                let Some(slot) = shared.budget.try_reserve_listener(forward.listen_port) else {
                    tracing::warn!(listen_port = forward.listen_port, "连接数已满");
                    let _ = shared
                        .events
                        .send(Event::Error(TunnelError::Limit {
                            port: forward.listen_port,
                        }))
                        .await;
                    tracing::info!(listen_port = forward.listen_port, "TCP 连接已关闭");
                    drop(tcp);
                    continue;
                };

                // 每条 TCP 的打开/关闭保持 info 级：默认 warn 不上屏，`RUST_LOG=info` 可排查。
                tracing::info!(listen_port = forward.listen_port, %peer, "接受 TCP 连接");
                active.fetch_add(1, Ordering::SeqCst);
                let _ = shared
                    .events
                    .send(Event::ConnectionOpened {
                        listen_port: forward.listen_port,
                    })
                    .await;

                let events = shared.events.clone();
                let active = active.clone();
                let link = shared.link.clone();
                let close = shared.close.clone();
                let link_lost = shared.link_lost.clone();
                let link_state = shared.link_state.clone();
                shared.tasks.spawn(async move {
                    bridge_local(tcp, forward, link, events, close, link_lost, link_state).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    drop(slot);
                });
            }
            Err(error) => {
                tracing::warn!(
                    listen_port = forward.listen_port,
                    %error,
                    "accept 失败，listener 停止"
                );
                break;
            }
        }
    }
}

/// 单条本机 TCP 连接：开数据流、写头、发事件、双泵。
async fn bridge_local(
    tcp: TcpStream,
    forward: Forward,
    link: Arc<dyn DataLink>,
    events: mpsc::Sender<Event>,
    close: watch::Receiver<u32>,
    link_lost: watch::Receiver<u64>,
    mut link_state: watch::Receiver<LinkState>,
) {
    let listen_port = forward.listen_port;
    // §9.1：重连期间最多等 5 秒；仍不可用就关闭这条 TCP，避免无界堆积。
    if !bridge::wait_for_link_connected(&mut link_state, RECONNECT_WAIT).await {
        tracing::warn!(listen_port, "链路重连中，关闭新 TCP 连接");
        let _ = events
            .send(Event::Error(TunnelError::Timeout {
                operation: "等待数据链路重连",
            }))
            .await;
        tracing::info!(listen_port, "TCP 连接已关闭");
        let _ = events.send(Event::ConnectionClosed { listen_port }).await;
        return;
    }
    match bridge::open_data_stream(&*link, forward.peer_port).await {
        Ok((read, write)) => {
            // `ConnectionEstablished` = 流已打开且 6 字节头已写出（§7.1）。
            let _ = events
                .send(Event::ConnectionEstablished { listen_port })
                .await;
            bridge::pump(tcp, read, write, bridge::Aborts::new(close, link_lost)).await;
        }
        Err(error) => {
            tracing::debug!(listen_port, %error, "打开数据流失败，关闭 TCP");
        }
    }
    tracing::info!(listen_port, "TCP 连接已关闭");
    let _ = events.send(Event::ConnectionClosed { listen_port }).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use async_trait::async_trait;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use crate::pairing::SdkPairing;
    use crate::test_support::{real_sdk_guard, REAL_SDK_WAIT};
    use crate::transport::mock::MockLink;

    /// 纯内存配对驱动：验证会话状态机与事件顺序，不触网。
    struct MockPairing {
        bootstrap_outbound: bool,
        fail_create: bool,
    }

    impl MockPairing {
        fn new() -> Self {
            Self {
                bootstrap_outbound: true,
                fail_create: false,
            }
        }
    }

    #[async_trait]
    impl PairingDriver for MockPairing {
        async fn create_invite(&mut self) -> Result<String, TunnelError> {
            if self.fail_create {
                return Err(TunnelError::Pairing("模拟配对失败".into()));
            }
            Ok("INVITE-mock".into())
        }

        async fn join_invite(&mut self, _invite: &str) -> Result<(String, String), TunnelError> {
            Ok(("REPLY-mock".into(), "123456".into()))
        }

        async fn provide_reply(&mut self, reply: &str) -> Result<String, TunnelError> {
            if reply.is_empty() {
                return Err(TunnelError::Pairing("空的 REPLY".into()));
            }
            Ok("123456".into())
        }

        async fn confirm(&mut self) -> Result<ConnectedLink, TunnelError> {
            Ok(ConnectedLink {
                link: Arc::new(MockLink::new()),
                bootstrap_outbound: self.bootstrap_outbound,
                peer_session: Arc::from("mock-session"),
            })
        }

        async fn shutdown(&mut self) {}
    }

    /// 等待满足条件的事件；跳过无关事件（链路状态等）。
    async fn next_matching<F>(session: &mut Session, mut predicate: F) -> Event
    where
        F: FnMut(&Event) -> bool,
    {
        for _ in 0..100 {
            let event = tokio::time::timeout(Duration::from_secs(10), session.recv())
                .await
                .expect("等待事件超时")
                .expect("会话事件通道已关闭");
            if predicate(&event) {
                return event;
            }
        }
        panic!("没有等到期望的事件");
    }

    /// 真实环回验收里不允许吞掉 `Error`：任何错误都让测试失败。
    async fn next_ok<F>(session: &mut Session, mut predicate: F) -> Event
    where
        F: FnMut(&Event) -> bool,
    {
        for _ in 0..200 {
            let event = tokio::time::timeout(Duration::from_secs(60), session.recv())
                .await
                .expect("等待事件超时")
                .expect("会话事件通道已关闭");
            if let Event::Error(error) = &event {
                panic!("真实 SDK 环回出现错误：{error}");
            }
            if predicate(&event) {
                return event;
            }
        }
        panic!("没有等到期望的事件");
    }

    fn extract_invite(event: Event) -> String {
        match event {
            Event::InviteCode(code) => code,
            other => panic!("期望 InviteCode，实际 {other:?}"),
        }
    }

    fn extract_code(event: Event, label: &str) -> String {
        match event {
            Event::InviteCode(code) | Event::ReplyCode(code) | Event::ComparisonCode(code) => code,
            other => panic!("期望 {label}，实际 {other:?}"),
        }
    }

    fn free_port() -> u16 {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("绑定临时端口");
        listener.local_addr().expect("读取端口").port()
    }

    async fn spawn_echo() -> u16 {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("绑定回显服务");
        let port = listener.local_addr().expect("读取回显端口").port();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let (mut read, mut write) = tcp.into_split();
                    let _ = tokio::io::copy(&mut read, &mut write).await;
                    let _ = write.shutdown().await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn shutdown_reports_stopped_and_terminates() {
        let session = Session::spawn();
        session.shutdown().await;
        // shutdown 消费 session；核心断言是它能在超时内返回而不是悬挂。
    }

    #[tokio::test]
    async fn create_invite_without_pairing_driver_reports_error() {
        let mut session = Session::spawn_with_link(Arc::new(MockLink::new()));
        session
            .sender()
            .send(Command::CreateInvite)
            .await
            .expect("channel open");
        let event = next_matching(&mut session, |event| {
            matches!(event, Event::Error(TunnelError::Internal(_)))
        })
        .await;
        assert!(matches!(event, Event::Error(TunnelError::Internal(_))));
        session.shutdown().await;
    }

    #[tokio::test]
    async fn add_forward_before_ready_reports_error() {
        let mut session = Session::spawn();
        session
            .sender()
            .send(Command::AddForward(Forward {
                listen_port: 2211,
                peer_port: 22,
            }))
            .await
            .expect("channel open");
        let event = session.recv().await.expect("event");
        assert!(matches!(event, Event::Error(TunnelError::Network(_))));
        session.shutdown().await;
    }

    #[tokio::test]
    async fn peer_snapshot_replaces_stale_rules_and_reports_remote_close() {
        let (_command_tx, _command_rx) = mpsc::channel::<Command>(4);
        let (event_tx, mut events) = mpsc::channel::<Event>(64);
        let (snapshots, _) = rule_sync::snapshot_channel();
        let (link_lost, _) = watch::channel(0u64);
        let mut worker = Worker {
            link: None,
            events: event_tx,
            rules: HashMap::new(),
            peer_rules: HashMap::new(),
            peer_known: None,
            snapshots,
            updates: None,
            state_tasks: Vec::new(),
            bootstrap_outbound: false,
            link_lost,
            link_state: None,
            reconnecting_since: None,
            reconnect_deadline: None,
            budget: StreamBudget::new(),
            pairing_commands: None,
            pairing_events: None,
            pairing_task: None,
            peer_session: Arc::from("test-session"),
            farewell: mpsc::channel(1).0,
            farewell_events: None,
            tasks: TaskRegistry::new(),
            ending: false,
        };

        worker
            .apply_peer_update(PeerUpdate::Snapshot(vec![
                Forward {
                    listen_port: 2211,
                    peer_port: 22,
                },
                Forward {
                    listen_port: 2212,
                    peer_port: 23,
                },
            ]))
            .await;
        assert!(
            matches!(events.recv().await, Some(Event::PeerRules(rules)) if rules.len() == 2),
            "首次快照应变已知并上报全部规则"
        );

        // 全量替换：2211 消失 → RemoteClosed；2212 只改 peer_port → 仅更新显示。
        worker
            .apply_peer_update(PeerUpdate::Snapshot(vec![Forward {
                listen_port: 2212,
                peer_port: 24,
            }]))
            .await;
        assert!(matches!(
            events.recv().await,
            Some(Event::RemoteClosed { listen_port: 2211 })
        ));
        assert!(
            matches!(events.recv().await, Some(Event::PeerRules(rules)) if rules == vec![Forward { listen_port: 2212, peer_port: 24 }]),
            "快照应替换而不是合并"
        );

        // 状态流失败：标记未知，且重复未知不刷屏。
        worker.apply_peer_update(PeerUpdate::Unknown).await;
        assert!(matches!(events.recv().await, Some(Event::PeerRulesUnknown)));
        worker.apply_peer_update(PeerUpdate::Unknown).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), events.recv())
                .await
                .is_err(),
            "重复未知不应重复上报"
        );
    }

    #[tokio::test]
    async fn create_invite_reports_invite_code() {
        let mut session = Session::spawn_with_pairing(Box::new(MockPairing::new()));
        session
            .sender()
            .send(Command::CreateInvite)
            .await
            .expect("channel open");
        let invite = extract_invite(
            next_matching(&mut session, |event| matches!(event, Event::InviteCode(_))).await,
        );
        assert_eq!(invite, "INVITE-mock");
        session.shutdown().await;
    }

    #[tokio::test]
    async fn join_invite_emits_reply_then_comparison_code() {
        let mut session = Session::spawn_with_pairing(Box::new(MockPairing::new()));
        session
            .sender()
            .send(Command::JoinInvite("INVITE-remote".into()))
            .await
            .expect("channel open");
        let reply = extract_invite_or_reply(
            next_matching(&mut session, |event| {
                matches!(event, Event::ReplyCode(_) | Event::Error(_))
            })
            .await,
        );
        assert_eq!(reply, "REPLY-mock");
        let comparison = match next_matching(&mut session, |event| {
            matches!(event, Event::ComparisonCode(_))
        })
        .await
        {
            Event::ComparisonCode(code) => code,
            other => panic!("期望 ComparisonCode，实际 {other:?}"),
        };
        assert_eq!(comparison, "123456");
        session.shutdown().await;
    }

    #[tokio::test]
    async fn confirm_installs_link_and_allows_forwarding() {
        let mut session = Session::spawn_with_pairing(Box::new(MockPairing::new()));
        session
            .sender()
            .send(Command::ConfirmComparison)
            .await
            .expect("channel open");
        next_matching(&mut session, |event| {
            matches!(event, Event::LinkState(LinkState::Connected))
        })
        .await;
        next_matching(&mut session, |event| matches!(event, Event::Ready)).await;

        let listen_port = free_port();
        session
            .sender()
            .send(Command::AddForward(Forward {
                listen_port,
                peer_port: 22,
            }))
            .await
            .expect("channel open");
        next_matching(
            &mut session,
            |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
        )
        .await;
        next_matching(&mut session, |event| matches!(event, Event::RulesChanged)).await;
        session.shutdown().await;
    }

    #[tokio::test]
    async fn pairing_failure_is_reported_as_error() {
        let mut driver = MockPairing::new();
        driver.fail_create = true;
        let mut session = Session::spawn_with_pairing(Box::new(driver));
        session
            .sender()
            .send(Command::CreateInvite)
            .await
            .expect("channel open");
        let event = next_matching(&mut session, |event| matches!(event, Event::Error(_))).await;
        assert!(matches!(event, Event::Error(TunnelError::Pairing(_))));
        session.shutdown().await;
    }

    fn extract_invite_or_reply(event: Event) -> String {
        match event {
            Event::ReplyCode(code) => code,
            Event::Error(error) => panic!("配对意外失败：{error}"),
            other => panic!("期望 ReplyCode，实际 {other:?}"),
        }
    }

    /// 真实 SDK 环回验收（ticket 06 验收框）：
    /// 两个真实 `Session` + `SdkPairing`（仅 127.0.0.1，无 STUN）走完
    /// INVITE/REPLY → 比较码 → Ready，随后规则同步、双向转发、关一条规则
    /// 另一条继续工作。
    ///
    /// 本测试不依赖公网、不需要 #[ignore]：SDK 的 ICE/打洞全部发生在
    /// 本机回环地址上（见 `pairing.rs` 与 `transport/sdk.rs`）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_sdk_loopback_pairs_and_forwards_both_directions() {
        // 先拿串行锁再开始计时：等锁不应计入本用例的等待预算。
        let _serial = real_sdk_guard().await;
        tokio::time::timeout(REAL_SDK_WAIT, real_sdk_loopback())
            .await
            .expect("真实 SDK 环回验收超时");
    }

    fn loopback_pairing() -> Box<dyn PairingDriver> {
        let address: SocketAddr = "127.0.0.1:0".parse().expect("回环地址");
        Box::new(SdkPairing::with_interfaces(vec![address]))
    }

    /// 真实 SDK 环回：走完 INVITE/REPLY → 比较码 → Ready，返回两端。
    async fn loopback_ready_sessions() -> (Session, Session) {
        let mut creator = Session::spawn_with_pairing(loopback_pairing());
        let mut joiner = Session::spawn_with_pairing(loopback_pairing());

        creator
            .sender()
            .send(Command::CreateInvite)
            .await
            .expect("channel open");
        let invite = extract_invite(
            next_ok(&mut creator, |event| matches!(event, Event::InviteCode(_))).await,
        );

        joiner
            .sender()
            .send(Command::JoinInvite(invite))
            .await
            .expect("channel open");
        let reply = extract_code(
            next_ok(&mut joiner, |event| {
                matches!(event, Event::ReplyCode(_) | Event::Error(_))
            })
            .await,
            "ReplyCode",
        );
        let joiner_code = extract_code(
            next_ok(&mut joiner, |event| {
                matches!(event, Event::ComparisonCode(_))
            })
            .await,
            "ComparisonCode",
        );

        creator
            .sender()
            .send(Command::ProvideReply(reply))
            .await
            .expect("channel open");
        let creator_code = extract_code(
            next_ok(&mut creator, |event| {
                matches!(event, Event::ComparisonCode(_))
            })
            .await,
            "ComparisonCode",
        );
        assert_eq!(creator_code, joiner_code, "两端比较码必须一致");

        // 两端必须同时 connect_transport，否则 ICE 检查无法配对。
        creator
            .sender()
            .send(Command::ConfirmComparison)
            .await
            .expect("channel open");
        joiner
            .sender()
            .send(Command::ConfirmComparison)
            .await
            .expect("channel open");
        next_ok(&mut creator, |event| matches!(event, Event::Ready)).await;
        next_ok(&mut joiner, |event| matches!(event, Event::Ready)).await;
        (creator, joiner)
    }

    /// 真实 SDK 环回：主动退出的 Farewell 必须让对端 1 秒内结束会话。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_sdk_loopback_peer_farewell_ends_session() {
        let _serial = real_sdk_guard().await;
        tokio::time::timeout(REAL_SDK_WAIT, async {
            let (creator, mut joiner) = loopback_ready_sessions().await;
            creator.shutdown().await;
            // 1 秒语义由 mock 用例精确断言（tests/farewell.rs）；真实 SDK 端到端
            // 还要算上 UDP 握手与 CI 调度，Windows 慢机曾超过 2 秒，放宽到 5 秒。
            tokio::time::timeout(Duration::from_secs(5), async {
                next_ok(&mut joiner, |event| matches!(event, Event::PeerFarewell)).await;
                next_ok(&mut joiner, |event| {
                    matches!(event, Event::LinkState(LinkState::Stopped))
                })
                .await;
            })
            .await
            .expect("对端应在 5 秒内识别 Farewell（CI 调度余量）");
            joiner.shutdown().await;
        })
        .await
        .expect("真实 SDK 告别超时");
    }

    async fn real_sdk_loopback() {
        let (mut creator, mut joiner) = loopback_ready_sessions().await;

        let echo_a = spawn_echo().await;
        let echo_b = spawn_echo().await;
        let listen_a = free_port();
        let listen_b = free_port();

        // 方向一：creator 监听，joiner 侧回显。
        creator
            .sender()
            .send(Command::AddForward(Forward {
                listen_port: listen_a,
                peer_port: echo_a,
            }))
            .await
            .expect("channel open");
        next_ok(
            &mut creator,
            |event| matches!(event, Event::ListenerStarted(port) if *port == listen_a),
        )
        .await;
        // 状态流可能在 AddForward 之前就开好并发送过空快照；必须等包含
        // listen_a 的快照，不能取第一条 PeerRules（ticket 07 定位的偶发竞态）。
        let peer_rules = match next_ok(&mut joiner, |event| {
            matches!(event, Event::PeerRules(rules) if rules.iter().any(|rule| rule.listen_port == listen_a))
        })
        .await
        {
            Event::PeerRules(rules) => rules,
            other => panic!("期望 PeerRules，实际 {other:?}"),
        };
        assert!(
            peer_rules
                .iter()
                .any(|rule| rule.listen_port == listen_a && rule.peer_port == echo_a),
            "joiner 应通过规则同步看到 creator 的规则：{peer_rules:?}"
        );

        let mut client = TcpStream::connect(("127.0.0.1", listen_a))
            .await
            .expect("连接 creator listener");
        client.write_all(b"hello-from-joiner").await.expect("写入");
        let mut buffer = [0u8; 32];
        let read = tokio::time::timeout(Duration::from_secs(10), client.read(&mut buffer))
            .await
            .expect("等待回显超时")
            .expect("读取回显");
        assert_eq!(&buffer[..read], b"hello-from-joiner");
        drop(client);

        // 方向二：joiner 监听，creator 侧回显。
        joiner
            .sender()
            .send(Command::AddForward(Forward {
                listen_port: listen_b,
                peer_port: echo_b,
            }))
            .await
            .expect("channel open");
        next_ok(
            &mut joiner,
            |event| matches!(event, Event::ListenerStarted(port) if *port == listen_b),
        )
        .await;
        next_ok(&mut creator, |event| {
            matches!(event, Event::PeerRules(rules) if rules.iter().any(|rule| rule.listen_port == listen_b))
        })
        .await;

        let mut client = TcpStream::connect(("127.0.0.1", listen_b))
            .await
            .expect("连接 joiner listener");
        client.write_all(b"hello-from-creator").await.expect("写入");
        let read = tokio::time::timeout(Duration::from_secs(10), client.read(&mut buffer))
            .await
            .expect("等待回显超时")
            .expect("读取回显");
        assert_eq!(&buffer[..read], b"hello-from-creator");
        drop(client);

        // 关闭 creator 的规则：该端口不再接受连接，joiner 的规则继续转发。
        creator
            .sender()
            .send(Command::CloseForward {
                listen_port: listen_a,
            })
            .await
            .expect("channel open");
        next_ok(&mut creator, |event| matches!(event, Event::RulesChanged)).await;
        assert!(
            TcpStream::connect(("127.0.0.1", listen_a)).await.is_err(),
            "已关闭的规则不应继续监听"
        );

        let mut client = TcpStream::connect(("127.0.0.1", listen_b))
            .await
            .expect("另一条规则仍应可用");
        client.write_all(b"still-alive").await.expect("写入");
        let read = tokio::time::timeout(Duration::from_secs(10), client.read(&mut buffer))
            .await
            .expect("等待回显超时")
            .expect("读取回显");
        assert_eq!(&buffer[..read], b"still-alive");
        drop(client);

        creator.shutdown().await;
        joiner.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_recovered_within_five_seconds_cancels_disconnect() {
        let link = Arc::new(MockLink::new());
        let mut session = Session::spawn_with_link(link.clone());
        next_matching(&mut session, |event| matches!(event, Event::Ready)).await;

        link.set_phase(LinkState::Reconnecting);
        next_matching(&mut session, |event| {
            matches!(event, Event::LinkState(LinkState::Reconnecting))
        })
        .await;

        // 4 秒内恢复：属于抖动，不能触发 5 秒断开兜底。
        tokio::time::advance(Duration::from_secs(4)).await;
        link.set_phase(LinkState::Connected);
        next_matching(&mut session, |event| {
            matches!(event, Event::LinkState(LinkState::Connected))
        })
        .await;

        // 若恢复时没有清除 5 秒倒计时，worker 会在这里判定断开并退出，
        // 下面的 AddForward 与哨兵事件都拿不到。
        tokio::time::advance(Duration::from_secs(6)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let listen_port = free_port();
        session
            .sender()
            .send(Command::AddForward(Forward {
                listen_port,
                peer_port: 22,
            }))
            .await
            .expect("恢复 5 秒内后会话应仍然存活");
        next_matching(
            &mut session,
            |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
        )
        .await;
        session.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn reconnecting_past_five_seconds_ends_session_as_disconnected() {
        let link = Arc::new(MockLink::new());
        let mut session = Session::spawn_with_link(link.clone());
        next_matching(&mut session, |event| matches!(event, Event::Ready)).await;

        link.set_phase(LinkState::Reconnecting);
        next_matching(&mut session, |event| {
            matches!(event, Event::LinkState(LinkState::Reconnecting))
        })
        .await;

        tokio::time::advance(Duration::from_secs(6)).await;

        // 断开连接不是会话丢失：先报 Disconnected，再报 Stopped，然后退出。
        next_matching(&mut session, |event| matches!(event, Event::Disconnected)).await;
        next_matching(&mut session, |event| {
            matches!(event, Event::LinkState(LinkState::Stopped))
        })
        .await;
        let trailing = tokio::time::timeout(Duration::from_secs(1), session.recv()).await;
        assert!(
            matches!(trailing, Ok(None)),
            "断开连接后 worker 应立即退出，实际 {trailing:?}"
        );
    }

    #[tokio::test]
    async fn peer_farewell_ends_session_and_rsts_active_connections() {
        let (link_a, link_b) = MockLink::pair();
        let mut a = Session::spawn_with_link(Arc::new(link_a));
        let mut b = Session::spawn_with_link(Arc::new(link_b));
        next_matching(&mut a, |event| matches!(event, Event::Ready)).await;
        next_matching(&mut b, |event| matches!(event, Event::Ready)).await;

        let echo_port = spawn_echo().await;
        let listen_port = free_port();
        b.sender()
            .send(Command::AddForward(Forward {
                listen_port,
                peer_port: echo_port,
            }))
            .await
            .expect("channel open");
        next_matching(
            &mut b,
            |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
        )
        .await;

        let mut client = TcpStream::connect(("127.0.0.1", listen_port))
            .await
            .expect("连接本机 listener");
        client.write_all(b"hello").await.expect("写入");
        let mut buffer = [0u8; 5];
        let read = tokio::time::timeout(Duration::from_secs(10), client.read(&mut buffer))
            .await
            .expect("等待回显超时")
            .expect("读取回显");
        assert_eq!(&buffer[..read], b"hello");

        // A 主动退出 → B 1 秒内报告对端已退出，且不进入重连。
        a.shutdown().await;
        tokio::time::timeout(
            Duration::from_secs(1),
            next_matching(&mut b, |event| matches!(event, Event::PeerFarewell)),
        )
        .await
        .expect("对端应在 1 秒内收到 Farewell");

        // 活动 TCP 必须被 RST（读到 EOF 或连接重置都说明已收尾）。
        let mut tail = [0u8; 1];
        let closed = tokio::time::timeout(Duration::from_secs(5), client.read(&mut tail)).await;
        assert!(
            matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
            "活动连接应被关闭，实际 {closed:?}"
        );

        next_matching(&mut b, |event| {
            matches!(event, Event::LinkState(LinkState::Stopped))
        })
        .await;
        let trailing = tokio::time::timeout(Duration::from_secs(1), b.recv()).await;
        assert!(
            matches!(trailing, Ok(None)),
            "对端告别后 worker 应退出，实际 {trailing:?}"
        );
    }

    #[tokio::test]
    async fn shutdown_releases_listener_port_for_immediate_rebind() {
        let link = Arc::new(MockLink::new());
        let mut session = Session::spawn_with_link(link);
        next_matching(&mut session, |event| matches!(event, Event::Ready)).await;

        let listen_port = free_port();
        session
            .sender()
            .send(Command::AddForward(Forward {
                listen_port,
                peer_port: 22,
            }))
            .await
            .expect("channel open");
        next_matching(
            &mut session,
            |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
        )
        .await;

        session.shutdown().await;

        // 退出清理必须已经释放 listener，同一端口可以立即重绑。
        let rebound = TcpListener::bind(("127.0.0.1", listen_port)).await;
        assert!(
            rebound.is_ok(),
            "退出后应立即能重绑端口 {listen_port}：{:?}",
            rebound.err()
        );
    }

    #[tokio::test]
    async fn reconnect_does_not_reemit_ready() {
        let link = Arc::new(MockLink::new());
        let mut session = Session::spawn_with_link(link.clone());
        next_matching(&mut session, |event| matches!(event, Event::Ready)).await;

        link.set_phase(LinkState::Reconnecting);
        next_matching(&mut session, |event| {
            matches!(event, Event::LinkState(LinkState::Reconnecting))
        })
        .await;
        link.set_phase(LinkState::Connected);
        next_matching(&mut session, |event| {
            matches!(event, Event::LinkState(LinkState::Connected))
        })
        .await;

        // 用 AddForward 的哨兵事件确认恢复后不会再收到 Ready。
        let listen_port = free_port();
        session
            .sender()
            .send(Command::AddForward(Forward {
                listen_port,
                peer_port: 22,
            }))
            .await
            .expect("channel open");
        let event = next_matching(&mut session, |event| {
            matches!(
                event,
                Event::Ready | Event::ListenerStarted(_) | Event::Error(_)
            )
        })
        .await;
        assert!(
            matches!(&event, Event::ListenerStarted(port) if *port == listen_port),
            "重连后不应重发 Ready：{event:?}"
        );
        session.shutdown().await;
    }

    #[tokio::test]
    async fn stop_closes_listeners_and_link_but_keeps_core_alive() {
        let link = Arc::new(MockLink::new());
        let mut session = Session::spawn_with_link(link.clone());
        next_matching(&mut session, |event| matches!(event, Event::Ready)).await;

        let listen_port = free_port();
        session
            .sender()
            .send(Command::AddForward(Forward {
                listen_port,
                peer_port: 22,
            }))
            .await
            .expect("channel open");
        next_matching(
            &mut session,
            |event| matches!(event, Event::ListenerStarted(port) if *port == listen_port),
        )
        .await;
        next_matching(&mut session, |event| matches!(event, Event::RulesChanged)).await;

        let mut client = TcpStream::connect(("127.0.0.1", listen_port))
            .await
            .expect("连接 listener");
        next_matching(
            &mut session,
            |event| matches!(event, Event::ConnectionOpened { listen_port: port } if *port == listen_port),
        )
        .await;

        session
            .sender()
            .send(Command::Stop)
            .await
            .expect("channel open");
        next_matching(&mut session, |event| {
            matches!(event, Event::LinkState(LinkState::Stopped))
        })
        .await;

        assert!(
            TcpStream::connect(("127.0.0.1", listen_port))
                .await
                .is_err(),
            "Stop 后 listener 应关闭"
        );

        // 活动连接必须被断开（RST 或 EOF 都可，不能继续可读）。
        let mut buffer = [0u8; 8];
        match tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => {}
            Ok(Ok(n)) => panic!("Stop 后活动连接仍可读：{n} 字节"),
        }

        // core 仍存活：没有数据链路时新规则被明确拒绝。
        let other = free_port();
        session
            .sender()
            .send(Command::AddForward(Forward {
                listen_port: other,
                peer_port: 22,
            }))
            .await
            .expect("channel open");
        let event = next_matching(&mut session, |event| matches!(event, Event::Error(_))).await;
        assert!(
            matches!(&event, Event::Error(TunnelError::Network(_))),
            "Stop 后 core 应继续处理命令：{event:?}"
        );

        // Shutdown 仍能正常收尾。
        session.shutdown().await;
    }
}
