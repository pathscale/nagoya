//! Shared CPU credit authority with cancellation refunds and overshoot debt.

use crate::owner::{CpuBudget, CpuPermit, DispatchDenied, Rejected};
use crate::sync::{Notified, Notify};
use crate::{Executor, JoinHandle};
use alloc::sync::Arc;
use core::num::{NonZeroU64, NonZeroUsize};
use core::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

/// Compare-exchange update for `AtomicI64`; `fetch_update` is deprecated on
/// current stable and its replacement is newer than the crate MSRV.
fn update_i64(cell: &AtomicI64, mut f: impl FnMut(i64) -> Option<i64>) -> Result<i64, i64> {
    let mut current = cell.load(Ordering::Acquire);
    loop {
        let next = f(current).ok_or(current)?;
        match cell.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(previous) => return Ok(previous),
            Err(observed) => current = observed,
        }
    }
}

/// The `AtomicUsize` form of [`update_i64`].
fn update_usize(
    cell: &AtomicUsize,
    mut f: impl FnMut(usize) -> Option<usize>,
) -> Result<usize, usize> {
    let mut current = cell.load(Ordering::Acquire);
    loop {
        let next = f(current).ok_or(current)?;
        match cell.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(previous) => return Ok(previous),
            Err(observed) => current = observed,
        }
    }
}

/// One stable accounting key, shared across its agents and helper jobs.
///
/// Migration and host reentry reuse this same Arc. Replenishment is a policy
/// event, never a side effect of admission or worker selection.
pub struct CreditAccount<K> {
    key: K,
    capacity: i64,
    balance: AtomicI64,
    in_flight: AtomicUsize,
    parallelism: usize,
    changed: Notify,
}

impl<K> CreditAccount<K> {
    /// Start with capacity CPU nanoseconds and an admitted-job cap.
    /// Returns None if capacity exceeds the signed debt representation.
    #[must_use]
    pub fn new(key: K, capacity: NonZeroU64, parallelism: NonZeroUsize) -> Option<Self> {
        let capacity = i64::try_from(capacity.get()).ok()?;
        Some(Self {
            key,
            capacity,
            balance: AtomicI64::new(capacity),
            in_flight: AtomicUsize::new(0),
            parallelism: parallelism.get(),
            changed: Notify::new(),
        })
    }

    /// Immutable policy identity, such as a schemeful site and incarnation.
    #[must_use]
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Spendable CPU nanoseconds; a negative value is retained overshoot debt.
    #[must_use]
    pub fn balance(&self) -> i64 {
        self.balance.load(Ordering::Acquire)
    }

    /// Number of jobs reserved or executing across all workers.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Add policy-approved CPU nanoseconds at a retained refill/class event.
    /// Credit first pays debt and is capped at this account's capacity.
    pub fn replenish(&self, credits: u64) {
        let credits = i64::try_from(credits).unwrap_or(i64::MAX);
        self.adjust(credits);
        self.changed.notify_waiters();
    }

    /// Register before inspecting balance and in-flight state, then await a
    /// refund/refill event. Prefer wait_for after a rejected reservation:
    /// attempting reservation here can itself emit a rollback notification.
    pub fn changed(&self) -> Notified<'_> {
        self.changed.notified()
    }

    /// Suspend until this request could be admitted, without reserving or
    /// emitting notifications. The caller then retries its retained callback.
    /// A racing admission can win first, so repeat this wait after rejection.
    pub async fn wait_for(&self, credits: NonZeroU64) -> Result<(), CreditRequestTooLarge> {
        let credits = i64::try_from(credits.get()).map_err(|_| CreditRequestTooLarge)?;
        if credits > self.capacity {
            return Err(CreditRequestTooLarge);
        }
        loop {
            let notified = self.changed.notified();
            let mut notified = core::pin::pin!(notified);
            notified.as_mut().enable();
            if self.balance.load(Ordering::Acquire) >= credits
                && self.in_flight.load(Ordering::Acquire) < self.parallelism
            {
                return Ok(());
            }
            notified.await;
        }
    }

    fn adjust(&self, credits: i64) {
        let _ = update_i64(&self.balance, |balance| {
            Some(balance.saturating_add(credits).min(self.capacity))
        });
    }
}

/// A request exceeds the account's maximum spendable credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditRequestTooLarge;

impl core::fmt::Display for CreditRequestTooLarge {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("CPU credit request exceeds account capacity")
    }
}

#[cfg(feature = "std")]
impl std::error::Error for CreditRequestTooLarge {}

/// CPU admission using a host's monotonic current-worker CPU nanosecond clock.
///
/// The supplied function must never panic and must measure CPU, not wall/guest time. A turn never
/// suspends between its two reads, so per-worker clock origins may differ.
/// This hook supplies accounting, not native preemption: callers must bound
/// turns at real safepoints and react to retained debt by throttling or killing.
pub struct CpuCredits {
    clock: fn() -> u64,
}

impl CpuCredits {
    /// Bind the host's worker CPU clock without installing an OS service.
    #[must_use]
    pub const fn new(clock: fn() -> u64) -> Self {
        Self { clock }
    }

    /// Admit an attributed helper on the same executor, measuring its CPU.
    /// The rejected closure retains its typed owned/Arc input.
    pub fn dispatch<K, F, R>(
        &self,
        executor: &Executor,
        key: &Arc<CreditAccount<K>>,
        credits: u64,
        work: F,
    ) -> Result<JoinHandle<R>, Rejected<F>>
    where
        K: Send + Sync + 'static,
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let Some(mut permit) = self.reserve(key, credits) else {
            return Err(Rejected {
                reason: DispatchDenied::Budget,
                turn: work,
            });
        };
        Ok(executor.spawn(async move {
            permit.enter();
            let output = work();
            permit.finish();
            output
        }))
    }
}

/// A reservation carrying its original account across queueing and execution.
pub struct CpuReservation<K> {
    account: Arc<CreditAccount<K>>,
    reserved: i64,
    clock: fn() -> u64,
    started: Option<u64>,
    done: bool,
}

impl<K: Send + Sync + 'static> CpuBudget<CreditAccount<K>> for CpuCredits {
    type Permit = CpuReservation<K>;

    fn reserve(&self, key: &Arc<CreditAccount<K>>, credits: u64) -> Option<Self::Permit> {
        let reserved = i64::try_from(credits).ok().filter(|value| *value > 0)?;
        update_usize(&key.in_flight, |count| {
            (count < key.parallelism).then(|| count + 1)
        })
        .ok()?;
        if update_i64(&key.balance, |balance| {
            (balance >= reserved).then(|| balance - reserved)
        })
        .is_err()
        {
            key.in_flight.fetch_sub(1, Ordering::AcqRel);
            key.changed.notify_waiters();
            return None;
        }
        Some(CpuReservation {
            account: Arc::clone(key),
            reserved,
            clock: self.clock,
            started: None,
            done: false,
        })
    }
}

impl<K> CpuReservation<K> {
    fn settle(&mut self) {
        if self.done {
            return;
        }
        let actual = self.started.map_or(0, |start| {
            i64::try_from((self.clock)().saturating_sub(start)).unwrap_or(i64::MAX)
        });
        self.account.adjust(self.reserved.saturating_sub(actual));
        self.account.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.done = true;
        self.account.changed.notify_waiters();
    }
}

impl<K> CpuPermit for CpuReservation<K> {
    fn enter(&mut self) {
        assert!(
            !self.done && self.started.is_none(),
            "CPU reservation entered twice"
        );
        self.started = Some((self.clock)());
    }
    fn finish(&mut self) {
        self.settle();
    }
}

impl<K> Drop for CpuReservation<K> {
    fn drop(&mut self) {
        self.settle();
    }
}
