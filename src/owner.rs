//! Typed exclusive turns with admission before queue submission.

use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicBool, Ordering};
use crate::{Executor, JoinHandle};

/// Caller-owned CPU admission policy, shared by every worker and helper.
pub trait CpuBudget<K>: Send + Sync + 'static {
    /// A reservation refunded on cancellation and reconciled after execution.
    type Permit: CpuPermit + Send + 'static;
    /// Reserve credits for this stable key before placing work on the pool.
    fn reserve(&self, key: &Arc<K>, credits: u64) -> Option<Self::Permit>;
}

/// A CPU reservation's worker-local execution boundary.
///
/// Implementations must release/refund on Drop, including unwinding. Both
/// methods run on the same worker, with no suspension or migration between.
pub trait CpuPermit {
    /// Begin measuring this worker's CPU consumption.
    fn enter(&mut self);
    /// Reconcile actual consumption and release the in-flight reservation.
    fn finish(&mut self);
}

struct State<T> {
    admitted: AtomicBool,
    value: UnsafeCell<Option<T>>,
}

// SAFETY: a successful Acquire admission grants the only access to value.
// It is moved into Lease, then returned before Release clears admission.
// T must actually be Send; this does not assert Send for an agent's fields.
unsafe impl<T: Send> Sync for State<T> {}

struct Lease<T> {
    state: Arc<State<T>>,
    value: Option<T>,
}

impl<T> Drop for Lease<T> {
    fn drop(&mut self) {
        // SAFETY: this lease is the sole successful admission, including
        // cancellation before execution and unwinding during a turn.
        unsafe { *self.state.value.get() = self.value.take(); }
        self.state.admitted.store(false, Ordering::Release);
    }
}

/// A cloneable admission handle for one owned, movable value.
///
/// Clones share the gate, key and budget, never the mutable value. T needs
/// Send, not Sync. Turn callbacks cannot await or retain a borrow of T.
/// A native caller must end worker-bound frames/guards before returning.
pub struct Owner<T, K, B> {
    state: Arc<State<T>>,
    key: Arc<K>,
    budget: Arc<B>,
}

impl<T, K, B> Clone for Owner<T, K, B> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            key: Arc::clone(&self.key),
            budget: Arc::clone(&self.budget),
        }
    }
}

/// Why a turn was not submitted. Its callback and captured input are returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchDenied {
    /// Another queued or executing turn already owns this value.
    Busy,
    /// The stable key has insufficient credits or in-flight capacity.
    Budget,
    /// The preferred initial worker is outside this executor's pool.
    Worker,
}

/// A rejected callback, retaining all of its owned input for a reactive retry.
pub struct Rejected<F> {
    /// The reason no job was enqueued.
    pub reason: DispatchDenied,
    /// The original callback with its original captures.
    pub turn: F,
}

impl<T: Send + 'static, K: Send + Sync + 'static, B: CpuBudget<K>> Owner<T, K, B> {
    /// Move a value into a stable owner and bind its key and credit authority.
    #[must_use]
    pub fn new(value: T, key: Arc<K>, budget: Arc<B>) -> Self {
        Self {
            state: Arc::new(State {
                admitted: AtomicBool::new(false),
                value: UnsafeCell::new(Some(value)),
            }),
            key,
            budget,
        }
    }

    /// The same typed key is used by every turn, independent of worker choice.
    #[must_use]
    pub fn key(&self) -> &Arc<K> { &self.key }

    /// Dispatch one complete synchronous turn on any worker.
    pub fn dispatch<F, R>(&self, executor: &Executor, credits: u64, turn: F)
        -> Result<JoinHandle<R>, Rejected<F>>
    where F: FnOnce(&mut T) -> R + Send + 'static, R: Send + 'static {
        self.dispatch_to(executor, None, credits, turn)
    }

    /// Dispatch with an optional initial queue preference, never affinity.
    ///
    /// Work stealing may move the queued turn. The next turn can prefer a
    /// different worker only after this one has returned ownership.
    pub fn dispatch_to<F, R>(&self, executor: &Executor, worker: Option<usize>,
        credits: u64, turn: F) -> Result<JoinHandle<R>, Rejected<F>>
    where F: FnOnce(&mut T) -> R + Send + 'static, R: Send + 'static {
        if worker.is_some_and(|id| id >= executor.pool().workers()) {
            return Err(Rejected { reason: DispatchDenied::Worker, turn });
        }
        if self.state.admitted.compare_exchange(false, true,
            Ordering::Acquire, Ordering::Relaxed).is_err() {
            return Err(Rejected { reason: DispatchDenied::Busy, turn });
        }
        // SAFETY: the successful admission owns this slot until Lease drops.
        let value = unsafe { (*self.state.value.get()).take() }
            .expect("ExclusiveOwnerDuplicateAdmission");
        let mut lease = Lease { state: Arc::clone(&self.state), value: Some(value) };
        let Some(mut permit) = self.budget.reserve(&self.key, credits) else {
            drop(lease);
            return Err(Rejected { reason: DispatchDenied::Budget, turn });
        };
        let future = async move {
            permit.enter();
            let output = turn(lease.value.as_mut().expect("exclusive turn value"));
            permit.finish();
            drop(permit);
            // Publish T before the join becomes ready, so the next dispatch
            // can immediately acquire it on a different worker.
            drop(lease);
            output
        };
        Ok(JoinHandle { task: Some(crate::task::spawn_on_at(future,
            Arc::clone(executor.pool()), executor.context.clone(), worker).fallible()) })
    }
}
