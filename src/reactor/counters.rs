//! Counters for the syscalls the reactor makes, behind a feature.
//!
//! Off by default and compiled out entirely: the read path is the hottest code
//! in the crate and an unconditional atomic increment on it would be measuring
//! the measurement. On, it answers the question a latency benchmark cannot,
//! which is whether a gap is syscall count or syscall cost.

use core::sync::atomic::AtomicU64;

/// Every `recv` the reactor issued.
pub static RECV: AtomicU64 = AtomicU64::new(0);
/// Those that returned `EWOULDBLOCK`, so the task had to park.
pub static RECV_WOULD_BLOCK: AtomicU64 = AtomicU64::new(0);
/// Every `kevent`/`epoll_wait` the reactor waited in.
pub static WAIT: AtomicU64 = AtomicU64::new(0);

/// Read and clear all three.
pub fn take() -> (u64, u64, u64) {
    use core::sync::atomic::Ordering::Relaxed;
    (
        RECV.swap(0, Relaxed),
        RECV_WOULD_BLOCK.swap(0, Relaxed),
        WAIT.swap(0, Relaxed),
    )
}
