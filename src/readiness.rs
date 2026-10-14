//! Retained, coalesced wake reasons for one logical scheduling owner.

use alloc::sync::Arc;
use core::num::NonZeroU64;
use core::sync::atomic::{AtomicU64, Ordering};
use crate::sync::Notify;
use crate::{Executor, JoinHandle};

struct Signals {
    reasons: AtomicU64,
    notify: Notify,
}

/// The unique consumer of a logical owner's ready reasons.
///
/// Reasons carry no guest payload. Owned values and immutable typed Arcs
/// remain in the caller's bounded ingress/continuation records.
pub struct ReadySource {
    signals: Arc<Signals>,
}

/// A cloneable producer that can wake the consumer from any completion.
#[derive(Clone)]
pub struct ReadyHandle {
    signals: Arc<Signals>,
}

impl Default for ReadySource {
    fn default() -> Self { Self::new() }
}

impl ReadySource {
    /// Make a single-consumer source with no pending reasons.
    #[must_use]
    pub fn new() -> Self {
        Self { signals: Arc::new(Signals {
            reasons: AtomicU64::new(0), notify: Notify::new(),
        }) }
    }

    /// Give a timer, readiness callback or typed completion an owned producer.
    #[must_use]
    pub fn handle(&self) -> ReadyHandle {
        ReadyHandle { signals: Arc::clone(&self.signals) }
    }

    /// Consume retained reasons, or register and suspend until a real event.
    ///
    /// Registration precedes the atomic recheck, so a wake racing with idle
    /// suspension is retained. There is no periodic wake or account scan.
    pub async fn wait(&mut self) -> u64 {
        loop {
            let notified = self.signals.notify.notified();
            let mut notified = core::pin::pin!(notified);
            notified.as_mut().enable();
            let reasons = self.signals.reasons.swap(0, Ordering::AcqRel);
            if reasons != 0 { return reasons; }
            notified.await;
        }
    }
}

/// Rounding exceeded the monotonic clock's usable deadline range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadlineOverflow;

impl core::fmt::Display for DeadlineOverflow {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("nagoya coarse deadline overflow")
    }
}

#[cfg(feature = "std")]
impl std::error::Error for DeadlineOverflow {}

/// Round up to a configured coarse delivery quantum. Never round earlier.
/// u64::MAX is reserved by the reactor for an unarmed deadline.
pub fn coarse_deadline(deadline: u64, quantum: NonZeroU64) -> Result<u64, DeadlineOverflow> {
    let remainder = deadline % quantum.get();
    let extra = if remainder == 0 { 0 } else { quantum.get() - remainder };
    deadline.checked_add(extra).filter(|value| *value != u64::MAX)
        .ok_or(DeadlineOverflow)
}

impl ReadyHandle {
    /// Retain reasons before waking. Concurrent signals coalesce into bits.
    /// Zero has no effect. Each bit means recheck that obligation's real state.
    pub fn wake(&self, reasons: u64) {
        if reasons != 0 && self.signals.reasons.fetch_or(reasons, Ordering::AcqRel) == 0 {
            self.signals.notify.notify_one();
        }
    }

    /// Arm one retained coarse deadline on this executor and its timer host.
    ///
    /// For last-view sleep deadlines and staggered service-worker admission.
    /// The caller retains the JoinHandle and cancels it when eligibility or
    /// the due event changes. It must initialize Runtime::readiness first.
    /// This is delivery only; it does not grant CPU or service-worker admission.
    pub fn wake_at(&self, executor: &Executor, deadline: u64,
        quantum: NonZeroU64, reasons: u64) -> Result<JoinHandle<()>, DeadlineOverflow> {
        let deadline = coarse_deadline(deadline, quantum)?;
        let handle = self.clone();
        Ok(executor.spawn(async move {
            crate::sleep_until(deadline).await;
            handle.wake(reasons);
        }))
    }
}
