//! tunnel-core 的公开类型：规则、链路状态、命令、事件与错误。
//!
//! 这些类型是 UI（CLI/TUI/GUI）与 core 之间唯一的契约。UI 不得看到
//! `quinn` 或 `p2p_sdk` 的类型；协议与网络细节留在 core 内部。
//! 规格来源：`docs/SPEC.md` §7。

use thiserror::Error;

/// 一条端口转发规则：本机监听 `listen_port`，对端目标为 `127.0.0.1:peer_port`。
///
/// 规则的“所有者”是监听端。对端只读显示对端规则，不能远程关闭。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Forward {
    pub listen_port: u16,
    pub peer_port: u16,
}

impl Forward {
    /// 校验端口范围并构造规则。端口 `0` 非法。
    pub fn new(listen_port: u16, peer_port: u16) -> Result<Self, TunnelError> {
        if listen_port == 0 {
            return Err(TunnelError::Protocol("监听端口必须是 1–65535".into()));
        }
        if peer_port == 0 {
            return Err(TunnelError::Protocol("目标端口必须是 1–65535".into()));
        }
        Ok(Self {
            listen_port,
            peer_port,
        })
    }
}

impl std::fmt::Display for Forward {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "127.0.0.1:{} -> 对端:{}",
            self.listen_port, self.peer_port
        )
    }
}

/// 数据链路（SDK managed link）的粗粒度状态。
///
/// 与 SDK `ManagedLinkPhase` 一一对应，见 `docs/SPEC.md` §7.1。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    Connecting,
    Connected,
    Reconnecting,
    AuthenticationFailed,
    Stopped,
}

impl LinkState {
    /// 中文展示文案（所有 UI 共用）。
    pub fn label(self) -> &'static str {
        match self {
            Self::Connecting => "连接中",
            Self::Connected => "已连接",
            Self::Reconnecting => "重连中",
            Self::AuthenticationFailed => "认证失败",
            Self::Stopped => "已停止",
        }
    }
}

impl std::fmt::Display for LinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// UI 发给 core 的命令。
#[derive(Debug, Clone)]
pub enum Command {
    CreateInvite,
    JoinInvite(String),
    ProvideReply(String),
    ConfirmComparison,
    AddForward(Forward),
    CloseForward {
        listen_port: u16,
    },
    /// 停止转发与会话，保留进程/core；v0.1 CLI 不使用，保留给 TUI/GUI。
    Stop,
    Shutdown,
}

/// core 发给 UI 的事件。
#[derive(Debug, Clone)]
pub enum Event {
    InviteCode(String),
    ReplyCode(String),
    ComparisonCode(String),
    Ready,
    LinkState(LinkState),
    RulesChanged,
    /// 对端视角的规则：`listen_port` 是对端监听端口，`peer_port` 是本机目标端口。
    PeerRules(Vec<Forward>),
    /// 规则同步状态流不可用，对端规则暂时未知（`docs/SPEC.md` §5.4）。
    PeerRulesUnknown,
    /// 数据链路断开：保留最近一次对端规则，但可能已过期（`docs/SPEC.md` §9.1）。
    PeerRulesStale,
    /// 对端 `RuleList` 中该 `listen_port` 消失。
    RemoteClosed {
        listen_port: u16,
    },
    ListenerStarted(u16),
    ConnectionOpened {
        listen_port: u16,
    },
    ConnectionEstablished {
        listen_port: u16,
    },
    ConnectionClosed {
        listen_port: u16,
    },
    Error(TunnelError),
}

/// core 的错误类型；`Display` 输出可直接展示给用户的中文文案。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TunnelError {
    #[error("配对失败：{0}")]
    Pairing(String),
    #[error("网络错误：{0}")]
    Network(String),
    #[error("无法监听 127.0.0.1:{port}：{message}")]
    Bind { port: u16, message: String },
    #[error("无法连接目标 127.0.0.1:{port}：{message}")]
    TargetDial { port: u16, message: String },
    #[error("协议错误：{0}")]
    Protocol(String),
    #[error("操作超时：{operation}")]
    Timeout { operation: &'static str },
    #[error("连接数已满（端口 {port}）")]
    Limit { port: u16 },
    #[error("没有本机规则监听 {listen_port}")]
    RuleNotFound { listen_port: u16 },
    #[error("操作已取消")]
    Cancelled,
    /// 连续重连超时或认证失败：本会话不可恢复，必须重新配对（§9.2）。
    #[error("会话丢失，请重新配对")]
    SessionLost,
    #[error("内部错误：{0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forward_rejects_zero_ports() {
        assert!(Forward::new(0, 22).is_err());
        assert!(Forward::new(2211, 0).is_err());
        assert!(Forward::new(2211, 22).is_ok());
    }

    #[test]
    fn link_state_labels_are_chinese() {
        assert_eq!(LinkState::Connected.label(), "已连接");
        assert_eq!(LinkState::Reconnecting.label(), "重连中");
    }

    #[test]
    fn error_display_mentions_port() {
        let err = TunnelError::RuleNotFound { listen_port: 2211 };
        assert!(err.to_string().contains("2211"));
    }
}
