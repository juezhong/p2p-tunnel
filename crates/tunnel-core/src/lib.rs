//! `tunnel-core`：p2p-tunnel 的业务核心。
//!
//! 拥有配对状态机、规则表、listener、数据链路、规则同步与 TCP↔QUIC
//! bridge；通过 [`Command`] / [`Event`] 与 UI 通信。UI（CLI/TUI/GUI）
//! 不接触 `quinn` 或 `p2p_sdk` 的类型。
//!
//! 可开工规格见仓库 `docs/SPEC.md`。

mod api;
mod bridge;
mod limits;
mod pairing;
pub mod protocol;
mod rule_sync;
mod session;
#[cfg(test)]
mod test_support;
pub mod transport;

pub use api::{Command, Event, Forward, LinkState, TunnelError};
pub use protocol::{DataHeader, FrameError, RuleFrame, WireError};
pub use session::Session;
pub use transport::{
    open_bi_with_timeout, DataLink, TunnelRead, TunnelStream, TunnelWrite, OPEN_BI_TIMEOUT,
};
