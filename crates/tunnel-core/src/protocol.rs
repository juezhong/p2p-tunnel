//! 线协议编解码：6 字节数据流头与规则同步帧。
//!
//! 规格来源：`docs/SPEC.md` §5.1、§5.4、§5.5。这里只做纯编解码，不碰
//! 任何 I/O；调用方在读写失败或校验失败时使用 [`WireError::code`] /
//! [`FrameError::code`] 得到要发给对端的 QUIC 错误码。
//!
//! 校验失败在本端只产生错误，不自行 reset/close：数据流与状态流的
//! 处理方式不同（reset vs 关闭状态流），由上层决定。

use std::collections::HashSet;

use thiserror::Error;

use crate::api::{Forward, TunnelError};

/// 数据流头长度（字节）。
pub const DATA_HEADER_LEN: usize = 6;
/// 数据流 magic：ASCII `P2`。
pub const DATA_MAGIC: [u8; 2] = *b"P2";
/// v0.1 协议版本。
pub const DATA_VERSION: u8 = 0x01;
/// v0.1 保留 flags，必须为 0。
pub const DATA_FLAGS: u8 = 0x00;

/// 错误码 0x01：协议错误，见 `docs/SPEC.md` §5.5。
pub const CODE_PROTOCOL_ERROR: u32 = 0x01;
/// 错误码 0x02：目标拨号失败。
pub const CODE_TARGET_DIAL_FAILED: u32 = 0x02;
/// 错误码 0x03：目标端口被拒绝（v0.1 保留未用）。
pub const CODE_TARGET_PORT_DENIED: u32 = 0x03;
/// 错误码 0x04：超过并发上限。
pub const CODE_LIMIT_EXCEEDED: u32 = 0x04;
/// 错误码 0x05：取消。
pub const CODE_CANCELLED: u32 = 0x05;
/// 错误码 0x10：TCP RST / 读写错误。
pub const CODE_TCP_RESET: u32 = 0x10;

/// 6 字节数据流头。
///
/// 发起方 `open_bi()` 后立刻写出；目标端先读首字节分流，再读剩余 5
/// 字节。`target_port` 是目标端 `127.0.0.1:<port>` 的端口。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataHeader {
    pub target_port: u16,
}

impl DataHeader {
    /// 校验端口范围并构造头。端口 `0` 非法。
    pub fn new(target_port: u16) -> Result<Self, WireError> {
        if target_port == 0 {
            return Err(WireError::ZeroPort);
        }
        Ok(Self { target_port })
    }

    /// 编码为固定 6 字节，便于一次 `write_all` 发完。
    pub const fn encode(&self) -> [u8; DATA_HEADER_LEN] {
        let port = self.target_port.to_be_bytes();
        [
            DATA_MAGIC[0],
            DATA_MAGIC[1],
            DATA_VERSION,
            DATA_FLAGS,
            port[0],
            port[1],
        ]
    }

    /// 解码并校验 magic/version/flags/port；任何失败都映射到
    /// [`CODE_PROTOCOL_ERROR`]。
    pub fn decode(bytes: [u8; DATA_HEADER_LEN]) -> Result<Self, WireError> {
        if bytes[0..2] != DATA_MAGIC {
            return Err(WireError::BadMagic);
        }
        if bytes[2] != DATA_VERSION {
            return Err(WireError::UnsupportedVersion(bytes[2]));
        }
        if bytes[3] != DATA_FLAGS {
            return Err(WireError::ReservedFlags(bytes[3]));
        }
        let target_port = u16::from_be_bytes([bytes[4], bytes[5]]);
        if target_port == 0 {
            return Err(WireError::ZeroPort);
        }
        Ok(Self { target_port })
    }
}

/// 数据流头校验错误；全部映射为 `0x01` 协议错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WireError {
    #[error("数据流头 magic 无效（期望 \"P2\"）")]
    BadMagic,
    #[error("不支持的协议版本：0x{0:02x}")]
    UnsupportedVersion(u8),
    #[error("保留 flags 必须为 0，收到 0x{0:02x}")]
    ReservedFlags(u8),
    #[error("目标端口不能为 0")]
    ZeroPort,
}

impl WireError {
    /// 要发给对端的 QUIC 错误码；数据流头失败固定为 `0x01`。
    pub const fn code(&self) -> u32 {
        CODE_PROTOCOL_ERROR
    }
}

impl From<WireError> for TunnelError {
    fn from(error: WireError) -> Self {
        TunnelError::Protocol(error.to_string())
    }
}

/// 规则同步帧类型：`Hello`。
pub const FRAME_HELLO: u8 = 0x01;
/// 规则同步帧类型：`RuleList`。
pub const FRAME_RULE_LIST: u8 = 0x02;
/// 规则同步帧类型：`Farewell`（payload 为空，表示对端主动退出）。
pub const FRAME_FAREWELL: u8 = 0x03;
/// 帧头长度：`type:u8 | len:u16 BE`。
pub const FRAME_HEADER_LEN: usize = 3;
/// `Hello` payload 长度（只有 `version:u8`）。
pub const HELLO_PAYLOAD_LEN: usize = 1;
/// `RuleList` payload 的 `count` 字段长度。
pub const RULE_LIST_COUNT_LEN: usize = 2;
/// `RuleList` 每条规则的长度（两个大端 `u16`）。
pub const RULE_ENTRY_LEN: usize = 4;
/// 单帧最大长度（长度字段是 `u16`）。
pub const MAX_FRAME_LEN: usize = FRAME_HEADER_LEN + u16::MAX as usize;
/// `RuleList` 最多可容纳的规则数。
pub const MAX_RULE_COUNT: usize = (u16::MAX as usize - RULE_LIST_COUNT_LEN) / RULE_ENTRY_LEN;

/// 规则同步流上的一帧。
///
/// [`RuleFrame::RuleList`] 是**全量快照**，不是增量：每次 `add`/`close`
/// 后重发完整列表，对端据此替换已知规则。见 `docs/SPEC.md` §5.4。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleFrame {
    /// 状态流首帧，携带规则同步协议版本。
    Hello,
    /// 全量规则快照。
    RuleList(Vec<Forward>),
    /// 主动退出通知：对端收到后结束会话，不再重连（SPEC §9.5）。
    Farewell,
}

impl RuleFrame {
    /// `Farewell` 的完整帧：3 字节帧头，payload 长度为 0（SPEC §5.4）。
    ///
    /// 发送侧直接写这份字节，避免手工拼帧头时漏掉长度字段。
    pub const fn farewell_frame() -> [u8; FRAME_HEADER_LEN] {
        let mut frame = [0u8; FRAME_HEADER_LEN];
        frame[0] = FRAME_FAREWELL;
        frame
    }

    /// 校验 `Farewell` 的 payload 长度并返回帧值。
    ///
    /// 完整帧解码（[`Self::decode`]）与首字节分流后的专用 `0x03` 流共用同一
    /// 判定，避免两处各写一遍长度校验（ticket 17）。
    pub fn decode_farewell(payload_len: usize) -> Result<Self, FrameError> {
        if payload_len == 0 {
            Ok(Self::Farewell)
        } else {
            Err(FrameError::BadFarewellLength(payload_len))
        }
    }

    /// 把整帧编码进调用方提供的缓冲，返回写入字节数。
    ///
    /// 编码不自行分配内存；缓冲不足或规则数超过帧长度上限时报错。
    pub fn encode(&self, out: &mut [u8]) -> Result<usize, FrameError> {
        match self {
            Self::Hello => {
                let needed = FRAME_HEADER_LEN + HELLO_PAYLOAD_LEN;
                if out.len() < needed {
                    return Err(FrameError::BufferTooSmall { needed });
                }
                out[0] = FRAME_HELLO;
                out[1..FRAME_HEADER_LEN].copy_from_slice(&(HELLO_PAYLOAD_LEN as u16).to_be_bytes());
                out[FRAME_HEADER_LEN] = DATA_VERSION;
                Ok(needed)
            }
            Self::RuleList(rules) => {
                if rules.len() > MAX_RULE_COUNT {
                    return Err(FrameError::TooManyRules { count: rules.len() });
                }
                let payload_len = RULE_LIST_COUNT_LEN + rules.len() * RULE_ENTRY_LEN;
                let needed = FRAME_HEADER_LEN + payload_len;
                if out.len() < needed {
                    return Err(FrameError::BufferTooSmall { needed });
                }

                // 先校验再写，避免留下半帧。集合大小有上限（规则数 ≤ 16383）。
                let mut seen = HashSet::with_capacity(rules.len());
                for rule in rules {
                    if rule.listen_port == 0 {
                        return Err(FrameError::ZeroPort {
                            field: "listen_port",
                        });
                    }
                    if rule.peer_port == 0 {
                        return Err(FrameError::ZeroPort { field: "peer_port" });
                    }
                    if !seen.insert(rule.listen_port) {
                        return Err(FrameError::DuplicateListenPort(rule.listen_port));
                    }
                }

                out[0] = FRAME_RULE_LIST;
                out[1..FRAME_HEADER_LEN].copy_from_slice(&(payload_len as u16).to_be_bytes());
                let mut offset = FRAME_HEADER_LEN;
                out[offset..offset + RULE_LIST_COUNT_LEN]
                    .copy_from_slice(&(rules.len() as u16).to_be_bytes());
                offset += RULE_LIST_COUNT_LEN;
                for rule in rules {
                    out[offset..offset + 2].copy_from_slice(&rule.listen_port.to_be_bytes());
                    out[offset + 2..offset + RULE_ENTRY_LEN]
                        .copy_from_slice(&rule.peer_port.to_be_bytes());
                    offset += RULE_ENTRY_LEN;
                }
                Ok(needed)
            }
            Self::Farewell => {
                let frame = Self::farewell_frame();
                if out.len() < frame.len() {
                    return Err(FrameError::BufferTooSmall {
                        needed: frame.len(),
                    });
                }
                out[..frame.len()].copy_from_slice(&frame);
                Ok(frame.len())
            }
        }
    }

    /// 解码一整帧（帧头 + payload，不含额外字节）。
    pub fn decode(frame: &[u8]) -> Result<Self, FrameError> {
        if frame.len() < FRAME_HEADER_LEN {
            return Err(FrameError::Truncated);
        }
        let frame_type = frame[0];
        let declared = u16::from_be_bytes([frame[1], frame[2]]) as usize;
        let actual = frame.len() - FRAME_HEADER_LEN;
        if declared != actual {
            return Err(FrameError::LengthMismatch { declared, actual });
        }
        let payload = &frame[FRAME_HEADER_LEN..];

        match frame_type {
            FRAME_HELLO => {
                if payload.len() != HELLO_PAYLOAD_LEN {
                    return Err(FrameError::BadHelloLength(payload.len()));
                }
                if payload[0] != DATA_VERSION {
                    return Err(FrameError::UnsupportedVersion(payload[0]));
                }
                Ok(Self::Hello)
            }
            FRAME_RULE_LIST => {
                if payload.len() < RULE_LIST_COUNT_LEN {
                    return Err(FrameError::BadRuleListLength(payload.len()));
                }
                let count = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                let body_len = payload.len() - RULE_LIST_COUNT_LEN;
                if body_len % RULE_ENTRY_LEN != 0 {
                    return Err(FrameError::BadRuleListLength(payload.len()));
                }
                if body_len / RULE_ENTRY_LEN != count {
                    return Err(FrameError::CountMismatch {
                        count,
                        len: payload.len(),
                    });
                }

                let mut rules = Vec::with_capacity(count);
                let mut seen = HashSet::with_capacity(count);
                let mut offset = RULE_LIST_COUNT_LEN;
                for _ in 0..count {
                    let listen_port = u16::from_be_bytes([payload[offset], payload[offset + 1]]);
                    let peer_port = u16::from_be_bytes([payload[offset + 2], payload[offset + 3]]);
                    if listen_port == 0 {
                        return Err(FrameError::ZeroPort {
                            field: "listen_port",
                        });
                    }
                    if peer_port == 0 {
                        return Err(FrameError::ZeroPort { field: "peer_port" });
                    }
                    if !seen.insert(listen_port) {
                        return Err(FrameError::DuplicateListenPort(listen_port));
                    }
                    rules.push(Forward {
                        listen_port,
                        peer_port,
                    });
                    offset += RULE_ENTRY_LEN;
                }
                Ok(Self::RuleList(rules))
            }
            FRAME_FAREWELL => Self::decode_farewell(payload.len()),
            other => Err(FrameError::UnknownType(other)),
        }
    }
}

/// 规则同步帧校验错误；全部映射为 `0x01` 协议错误。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FrameError {
    #[error("规则同步帧长度不足")]
    Truncated,
    #[error("未知的规则同步帧类型：0x{0:02x}")]
    UnknownType(u8),
    #[error("规则同步帧长度不匹配：声明 {declared}，实际 {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("Hello payload 长度必须是 1，实际 {0}")]
    BadHelloLength(usize),
    #[error("不支持的规则同步版本：0x{0:02x}")]
    UnsupportedVersion(u8),
    #[error("RuleList payload 长度不合法：{0}")]
    BadRuleListLength(usize),
    #[error("Farewell payload 长度必须是 0，实际 {0}")]
    BadFarewellLength(usize),
    #[error("RuleList count 与 payload 不匹配：count={count}，len={len}")]
    CountMismatch { count: usize, len: usize },
    #[error("RuleList 规则数超过帧长度上限：{count}")]
    TooManyRules { count: usize },
    #[error("规则端口不能为 0（{field}）")]
    ZeroPort { field: &'static str },
    #[error("RuleList 内 listen_port {0} 重复")]
    DuplicateListenPort(u16),
    #[error("输出缓冲不足：需要 {needed} 字节")]
    BufferTooSmall { needed: usize },
}

impl FrameError {
    /// 要发给对端的 QUIC 错误码；状态帧失败固定为 `0x01`。
    pub const fn code(&self) -> u32 {
        CODE_PROTOCOL_ERROR
    }
}

impl From<FrameError> for TunnelError {
    fn from(error: FrameError) -> Self {
        TunnelError::Protocol(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 错误码是线上协议的一部分，数值必须与 `docs/SPEC.md` §5.5 一致。
    #[test]
    fn error_codes_match_the_spec() {
        assert_eq!(CODE_PROTOCOL_ERROR, 0x01);
        assert_eq!(CODE_TARGET_DIAL_FAILED, 0x02);
        assert_eq!(CODE_TARGET_PORT_DENIED, 0x03);
        assert_eq!(CODE_LIMIT_EXCEEDED, 0x04);
        assert_eq!(CODE_CANCELLED, 0x05);
        assert_eq!(CODE_TCP_RESET, 0x10);
    }

    #[test]
    fn data_header_round_trips() {
        for target_port in [1, 22, 2211, 65535] {
            let header = DataHeader::new(target_port).expect("端口有效");
            let bytes = header.encode();
            assert_eq!(
                DataHeader::decode(bytes).expect("编码后可解码"),
                header,
                "端口 {target_port} 应 round-trip"
            );
        }
    }

    #[test]
    fn data_header_layout_is_spec_order() {
        let bytes = DataHeader::new(0x16_21).expect("端口有效").encode();
        assert_eq!(bytes, [0x50, 0x32, 0x01, 0x00, 0x16, 0x21]);
    }

    #[test]
    fn data_header_rejects_bad_magic_as_protocol_error() {
        let mut bytes = DataHeader::new(22).expect("端口有效").encode();
        bytes[0] = b'X';
        let error = DataHeader::decode(bytes).expect_err("magic 非法必须拒绝");
        assert_eq!(error, WireError::BadMagic);
        assert_eq!(error.code(), CODE_PROTOCOL_ERROR);
        assert!(matches!(TunnelError::from(error), TunnelError::Protocol(_)));
    }

    #[test]
    fn data_header_rejects_unknown_version_as_protocol_error() {
        let mut bytes = DataHeader::new(22).expect("端口有效").encode();
        bytes[2] = 0x02;
        let error = DataHeader::decode(bytes).expect_err("version 非法必须拒绝");
        assert_eq!(error, WireError::UnsupportedVersion(0x02));
        assert_eq!(error.code(), CODE_PROTOCOL_ERROR);
    }

    #[test]
    fn data_header_rejects_nonzero_flags_as_protocol_error() {
        let mut bytes = DataHeader::new(22).expect("端口有效").encode();
        bytes[3] = 0x80;
        let error = DataHeader::decode(bytes).expect_err("flags 非 0 必须拒绝");
        assert_eq!(error, WireError::ReservedFlags(0x80));
        assert_eq!(error.code(), CODE_PROTOCOL_ERROR);
    }

    #[test]
    fn data_header_rejects_zero_port_as_protocol_error() {
        let mut bytes = DataHeader::new(22).expect("端口有效").encode();
        bytes[4] = 0;
        bytes[5] = 0;
        let error = DataHeader::decode(bytes).expect_err("端口 0 必须拒绝");
        assert_eq!(error, WireError::ZeroPort);
        assert_eq!(error.code(), CODE_PROTOCOL_ERROR);
        assert!(DataHeader::new(0).is_err());
    }

    fn encode(frame: &RuleFrame) -> Vec<u8> {
        let mut out = vec![0u8; MAX_FRAME_LEN];
        let len = frame.encode(&mut out).expect("编码成功");
        out.truncate(len);
        out
    }

    #[test]
    fn hello_round_trips_with_exact_wire_bytes() {
        let bytes = encode(&RuleFrame::Hello);
        assert_eq!(bytes, [FRAME_HELLO, 0x00, 0x01, DATA_VERSION]);
        assert_eq!(RuleFrame::decode(&bytes).expect("可解码"), RuleFrame::Hello);
    }

    #[test]
    fn rule_list_round_trips_as_full_snapshot() {
        let empty = RuleFrame::RuleList(Vec::new());
        assert_eq!(RuleFrame::decode(&encode(&empty)).expect("可解码"), empty);

        let rules = vec![
            Forward::new(2211, 22).expect("端口有效"),
            Forward::new(8080, 80).expect("端口有效"),
            Forward::new(65535, 1).expect("端口有效"),
        ];
        let frame = RuleFrame::RuleList(rules);
        let bytes = encode(&frame);
        assert_eq!(&bytes[0..3], &[FRAME_RULE_LIST, 0x00, 14]);
        assert_eq!(RuleFrame::decode(&bytes).expect("可解码"), frame);
    }

    #[test]
    fn rule_list_round_trips_at_max_count() {
        let rules: Vec<Forward> = (1..=MAX_RULE_COUNT as u16)
            .map(|port| Forward::new(port, port).expect("端口有效"))
            .collect();
        let frame = RuleFrame::RuleList(rules);
        let bytes = encode(&frame);
        assert_eq!(
            bytes.len(),
            FRAME_HEADER_LEN + RULE_LIST_COUNT_LEN + 4 * MAX_RULE_COUNT
        );
        assert_eq!(RuleFrame::decode(&bytes).expect("可解码"), frame);
    }

    #[test]
    fn rule_frame_rejects_unknown_type() {
        let error = RuleFrame::decode(&[0x07, 0x00, 0x00]).expect_err("未知类型必须拒绝");
        assert_eq!(error, FrameError::UnknownType(0x07));
        assert_eq!(error.code(), CODE_PROTOCOL_ERROR);
        assert!(matches!(TunnelError::from(error), TunnelError::Protocol(_)));
    }

    #[test]
    fn rule_frame_rejects_truncated_and_length_mismatch() {
        assert_eq!(
            RuleFrame::decode(&[FRAME_HELLO, 0x00]).expect_err("不足 3 字节"),
            FrameError::Truncated
        );
        assert_eq!(
            RuleFrame::decode(&[FRAME_HELLO, 0x00, 0x02, 0x01]).expect_err("声明长度大于实际"),
            FrameError::LengthMismatch {
                declared: 2,
                actual: 1
            }
        );
        assert_eq!(
            RuleFrame::decode(&[FRAME_HELLO, 0x00, 0x00, 0x01]).expect_err("实际大于声明长度"),
            FrameError::LengthMismatch {
                declared: 0,
                actual: 1
            }
        );
    }

    #[test]
    fn hello_rejects_wrong_len_and_version() {
        assert_eq!(
            RuleFrame::decode(&[FRAME_HELLO, 0x00, 0x02, DATA_VERSION, 0x00])
                .expect_err("Hello len 必须为 1"),
            FrameError::BadHelloLength(2)
        );
        assert_eq!(
            RuleFrame::decode(&[FRAME_HELLO, 0x00, 0x01, 0x02]).expect_err("version 必须为 1"),
            FrameError::UnsupportedVersion(0x02)
        );
    }

    #[test]
    fn farewell_round_trips_with_empty_payload() {
        let bytes = encode(&RuleFrame::Farewell);
        assert_eq!(bytes, [FRAME_FAREWELL, 0x00, 0x00]);
        assert_eq!(RuleFrame::farewell_frame(), [FRAME_FAREWELL, 0x00, 0x00]);
        assert_eq!(
            RuleFrame::decode(&bytes).expect("可解码"),
            RuleFrame::Farewell
        );
        // 专用 0x03 流与完整帧解码共用同一长度校验。
        assert_eq!(RuleFrame::decode_farewell(0), Ok(RuleFrame::Farewell));
        assert_eq!(
            RuleFrame::decode_farewell(2),
            Err(FrameError::BadFarewellLength(2))
        );
    }

    #[test]
    fn farewell_rejects_nonempty_payload() {
        assert_eq!(
            RuleFrame::decode(&[FRAME_FAREWELL, 0x00, 0x01, 0x00]).expect_err("payload 必须为空"),
            FrameError::BadFarewellLength(1)
        );
        assert_eq!(
            RuleFrame::decode(&[FRAME_FAREWELL, 0x00, 0x01]).expect_err("声明长度与实际不符"),
            FrameError::LengthMismatch {
                declared: 1,
                actual: 0
            }
        );
    }

    #[test]
    fn rule_list_rejects_bad_payload_len_and_count_mismatch() {
        // len=1 连 count 字段都放不下。
        assert_eq!(
            RuleFrame::decode(&[FRAME_RULE_LIST, 0x00, 0x01, 0x00]).expect_err("payload 太短"),
            FrameError::BadRuleListLength(1)
        );
        // len=4 不是 2 + 4*N。
        assert_eq!(
            RuleFrame::decode(&[FRAME_RULE_LIST, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00])
                .expect_err("payload 不是合法条目长度"),
            FrameError::BadRuleListLength(4)
        );
        // len=6 (count 字段 + 1 条规则)，但 count 声称 2。
        let frame = [
            FRAME_RULE_LIST,
            0x00,
            0x06,
            0x00,
            0x02,
            0x08,
            0x9B,
            0x00,
            0x16,
        ];
        assert_eq!(
            RuleFrame::decode(&frame).expect_err("count 与 len 不匹配"),
            FrameError::CountMismatch { count: 2, len: 6 }
        );
    }

    #[test]
    fn rule_list_rejects_duplicate_listen_port() {
        let frame = [
            FRAME_RULE_LIST,
            0x00,
            0x0A,
            0x00,
            0x02,
            0x08,
            0xA3,
            0x00,
            0x16,
            0x08,
            0xA3,
            0x00,
            0x50,
        ];
        assert_eq!(
            RuleFrame::decode(&frame).expect_err("重复 listen_port 必须拒绝"),
            FrameError::DuplicateListenPort(2211)
        );
    }

    #[test]
    fn rule_list_rejects_zero_ports_on_decode() {
        let frame = [
            FRAME_RULE_LIST,
            0x00,
            0x06,
            0x00,
            0x01,
            0x00,
            0x00,
            0x00,
            0x16,
        ];
        assert_eq!(
            RuleFrame::decode(&frame).expect_err("listen_port 0 必须拒绝"),
            FrameError::ZeroPort {
                field: "listen_port"
            }
        );

        let frame = [
            FRAME_RULE_LIST,
            0x00,
            0x06,
            0x00,
            0x01,
            0x08,
            0x9B,
            0x00,
            0x00,
        ];
        assert_eq!(
            RuleFrame::decode(&frame).expect_err("peer_port 0 必须拒绝"),
            FrameError::ZeroPort { field: "peer_port" }
        );
    }

    #[test]
    fn rule_list_encode_rejects_duplicates_and_zero_ports() {
        let duplicate = RuleFrame::RuleList(vec![
            Forward::new(2211, 22).expect("端口有效"),
            Forward::new(2211, 80).expect("端口有效"),
        ]);
        assert_eq!(
            duplicate
                .encode(&mut [0u8; MAX_FRAME_LEN])
                .expect_err("重复"),
            FrameError::DuplicateListenPort(2211)
        );

        // `Forward` 字段公开，调用方可以绕过 `Forward::new` 构造非法规则。
        let zero = RuleFrame::RuleList(vec![Forward {
            listen_port: 0,
            peer_port: 22,
        }]);
        assert_eq!(
            zero.encode(&mut [0u8; MAX_FRAME_LEN]).expect_err("端口 0"),
            FrameError::ZeroPort {
                field: "listen_port"
            }
        );
    }

    #[test]
    fn rule_list_encode_rejects_too_many_rules_and_small_buffer() {
        let rules: Vec<Forward> = (1..=MAX_RULE_COUNT as u16 + 1)
            .map(|port| Forward::new(port, port).expect("端口有效"))
            .collect();
        let frame = RuleFrame::RuleList(rules);
        assert_eq!(
            frame
                .encode(&mut vec![0u8; MAX_FRAME_LEN])
                .expect_err("超上限"),
            FrameError::TooManyRules {
                count: MAX_RULE_COUNT + 1
            }
        );

        let frame = RuleFrame::RuleList(vec![
            Forward::new(2211, 22).expect("端口有效"),
            Forward::new(8080, 80).expect("端口有效"),
        ]);
        assert_eq!(
            frame.encode(&mut [0u8; 10]).expect_err("缓冲不足"),
            FrameError::BufferTooSmall { needed: 13 }
        );
    }
}
