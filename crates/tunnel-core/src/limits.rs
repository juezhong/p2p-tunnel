//! 并发上限（`docs/SPEC.md` §6.1）。
//!
//! 每个转发规则的活动连接 ≤64；本侧活动数据流总数 ≤96（入站 + 出站，
//! 不含规则同步流）。名额用 RAII 持有：流结束时自动归还，避免计数泄漏。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// 单个转发规则（或目标端口）的活动连接上限。
pub(crate) const MAX_CONNECTIONS_PER_FORWARD: usize = 64;

/// 本侧活动数据流总数上限；SDK 默认每方向 100 条双向流，留 1 条规则同步流。
pub(crate) const MAX_STREAMS_PER_SIDE: usize = 96;

/// 本侧数据流名额账本；所有 listener 与目标侧 accept 循环共享一份。
#[derive(Clone, Default)]
pub(crate) struct StreamBudget {
    inner: Arc<BudgetInner>,
}

#[derive(Default)]
struct BudgetInner {
    total: AtomicUsize,
    ports: Mutex<HashMap<PortKey, usize>>,
}

/// 端口计数分两个命名空间：本机监听规则（出站）与目标端口（入站）。
///
/// 两者都是 u16，但语义不同，混在一起会让 `listen_port == peer 目标端口`
/// 时互相误伤。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum PortKey {
    Listener(u16),
    Target(u16),
}

/// 已占用的一个数据流名额；`Drop` 时归还。
pub(crate) struct StreamSlot {
    budget: StreamBudget,
    key: PortKey,
}

impl StreamBudget {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// 当前已占用的名额总数（仅测试断言使用）。
    #[cfg(test)]
    pub(crate) fn total(&self) -> usize {
        self.inner.total.load(Ordering::SeqCst)
    }

    /// 为本机监听规则 `listen_port` 的出站流占一个名额。
    pub(crate) fn try_reserve_listener(&self, listen_port: u16) -> Option<StreamSlot> {
        self.try_reserve(PortKey::Listener(listen_port))
    }

    /// 为目标是 `target_port` 的入站流占一个名额。
    pub(crate) fn try_reserve_target(&self, target_port: u16) -> Option<StreamSlot> {
        self.try_reserve(PortKey::Target(target_port))
    }

    fn try_reserve(&self, key: PortKey) -> Option<StreamSlot> {
        let mut ports = self.inner.ports.lock().expect("名额账本锁被 poison");
        // 锁内复核总数：一次只放行一个名额，避免并发下相互越过上限。
        if self.inner.total.load(Ordering::SeqCst) >= MAX_STREAMS_PER_SIDE {
            return None;
        }
        let count = ports.get(&key).copied().unwrap_or(0);
        if count >= MAX_CONNECTIONS_PER_FORWARD {
            return None;
        }
        self.inner.total.fetch_add(1, Ordering::SeqCst);
        ports.insert(key, count + 1);
        Some(StreamSlot {
            budget: self.clone(),
            key,
        })
    }
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        let mut ports = self.budget.inner.ports.lock().expect("名额账本锁被 poison");
        if let Some(count) = ports.get_mut(&self.key) {
            if *count <= 1 {
                ports.remove(&self.key);
            } else {
                *count -= 1;
            }
        }
        self.budget.inner.total.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_up_to_the_side_limit_then_refuses() {
        let budget = StreamBudget::new();
        let mut slots = Vec::new();
        // 端口上限 64，因此 96 个名额要跨端口/命名空间凑满。
        for _ in 0..MAX_CONNECTIONS_PER_FORWARD {
            slots.push(budget.try_reserve_listener(7000).expect("未达上限应成功"));
        }
        for _ in 0..(MAX_STREAMS_PER_SIDE - MAX_CONNECTIONS_PER_FORWARD) {
            slots.push(budget.try_reserve_target(7001).expect("未达上限应成功"));
        }
        assert_eq!(budget.total(), MAX_STREAMS_PER_SIDE);
        assert!(
            budget.try_reserve_listener(7002).is_none(),
            "第 97 个名额应被拒绝"
        );

        // 归还一个后重新可用。
        slots.pop();
        assert_eq!(budget.total(), MAX_STREAMS_PER_SIDE - 1);
        assert!(budget.try_reserve_listener(7002).is_some());
    }

    #[test]
    fn limits_each_port_to_sixty_four() {
        let budget = StreamBudget::new();
        let mut slots = Vec::new();
        for _ in 0..MAX_CONNECTIONS_PER_FORWARD {
            slots.push(
                budget
                    .try_reserve_listener(7000)
                    .expect("未达端口上限应成功"),
            );
        }
        assert!(
            budget.try_reserve_listener(7000).is_none(),
            "第 65 条同端口连接应被拒绝"
        );
        // 其他端口与其他命名空间不受影响。
        assert!(budget.try_reserve_listener(7001).is_some());
        assert!(budget.try_reserve_target(7000).is_some());
    }

    #[test]
    fn dropping_every_slot_returns_the_budget_to_zero() {
        let budget = StreamBudget::new();
        let slots: Vec<_> = (0..MAX_STREAMS_PER_SIDE)
            .map(|index| {
                let port = 7000 + index as u16;
                if index % 2 == 0 {
                    budget.try_reserve_listener(port).expect("应成功")
                } else {
                    budget.try_reserve_target(port).expect("应成功")
                }
            })
            .collect();
        assert_eq!(budget.total(), MAX_STREAMS_PER_SIDE);
        drop(slots);
        assert_eq!(budget.total(), 0);
        assert!(
            budget.try_reserve_listener(7000).is_some(),
            "全部归还后应重新可用"
        );
    }
}
