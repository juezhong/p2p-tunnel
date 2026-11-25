//! 测试基础设施：真实 SDK 用例的串行化与统一等待预算。
//!
//! libtest 默认按 CPU 数并行执行同一 test binary 内的所有用例；真实 SDK
//! 用例会各自建立 QUIC/UDP 端点并打洞，和纯内存用例混跑时既不必要地
//! 抢占调度，也让两两之间的重拨时序更难复现。这里只做两件事：把真实
//! 网络用例串起来，以及给「链路卡死」一个唯一的保护上限。

use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

/// 真实 SDK 用例的 crate 级串行锁。
///
/// 用 `tokio::sync::Mutex` 而不是 `std::sync::Mutex`：守卫要跨 await 持有
/// 整个用例，std 守卫既会触发 `clippy::await_holding_lock`，也会在等锁时
/// 阻塞当前用例的运行时 worker。
static REAL_SDK_LOCK: Mutex<()> = Mutex::const_new(());

/// 真实 SDK 用例的等待上限（保护「链路卡死」，不是期望耗时）。
///
/// 正常重连在 1–10 秒内完成。SDK 单轮拨号超时 12 秒、退避上限 5 秒，
/// 120 秒覆盖约 7 轮重拨，并与应用层 `SESSION_LOSS_TIMEOUT` 对齐。注意
/// 这里等的是「新认证连接出现」，不能靠调大预算去等一条可能已经被
/// `Connected` 覆盖掉的瞬态 `Reconnecting` 阶段——那类等待永远不会被满足。
pub(crate) const REAL_SDK_WAIT: Duration = Duration::from_secs(120);

/// 串行化一个真实 SDK 用例；守卫 drop（含 panic 展开）时释放。
pub(crate) async fn real_sdk_guard() -> MutexGuard<'static, ()> {
    REAL_SDK_LOCK.lock().await
}
