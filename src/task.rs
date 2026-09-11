//! Task ownership and scoped worker routing.
//!
//! async-task owns each future, state machine and waker. A runnable becomes
//! a pool Job without another allocation. Worker wakes borrow the scope's
//! live pool; external wakes upgrade a weak reference so queued tasks do not
//! keep their containing pool alive in a cycle.

use alloc::sync::Arc;
use core::future::Future;
use core::ptr::NonNull;

use async_task::{Runnable, Task};
use st3::fanout::{Act, Job, Pool};

use crate::WorkerContext;

// The worker scope owns one pool lease. Wakes borrow it without modifying
// the pool-wide Arc counter. Queued tasks still hold only Weak<Pool>.
#[cfg(feature = "std")]
std::thread_local! {
    static CURRENT: core::cell::RefCell<Option<(Arc<Pool>, usize)>> =
        const { core::cell::RefCell::new(None) };
}

#[cfg(feature = "std")]
pub(crate) fn mark_current(pool: &Arc<Pool>, worker: usize) -> CurrentGuard {
    let previous = CURRENT.with(|current| current.replace(Some((pool.clone(), worker))));
    CurrentGuard {
        previous,
        _thread_bound: core::marker::PhantomData,
    }
}

#[cfg(feature = "std")]
pub(crate) struct CurrentGuard {
    previous: Option<(Arc<Pool>, usize)>,
    _thread_bound: core::marker::PhantomData<alloc::rc::Rc<()>>,
}

#[cfg(feature = "std")]
impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}

#[cfg(feature = "std")]
fn with_current_worker(mut visit: impl FnMut(&Pool, usize)) {
    let _ = CURRENT.try_with(|current| {
        if let Some((pool, worker)) = current.borrow().as_ref() {
            visit(pool, *worker);
        }
    });
}

#[cfg(not(feature = "std"))]
fn with_current_worker(_visit: impl FnMut(&Pool, usize)) {}

pub(crate) fn current_worker(pool: &Arc<Pool>) -> Option<usize> {
    let mut found = None;
    with_current_worker(|owner, worker| {
        if core::ptr::eq(owner, pool.as_ref()) {
            found = Some(worker);
        }
    });
    found
}

/// Put `future` on `pool` and hand back the handle to its result.
pub(crate) fn spawn<F>(
    future: F,
    pool: Arc<Pool>,
    context: Option<Arc<dyn WorkerContext>>,
) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Called for the first poll and for every wake after it. The `Runnable` is
    // a single heap allocation that already holds the future and its state, so
    // turning it into a job is `into_raw`: no box, no closure, nothing new.
    let weak_pool = Arc::downgrade(&pool);
    let schedule = move |runnable: Runnable| {
        let mut runnable = Some(runnable);
        let mut local = |owner: &Pool, worker: usize| {
            if core::ptr::eq(owner, weak_pool.as_ptr()) && worker < owner.workers() {
                if let Some(runnable) = runnable.take() {
                    owner.submit_local(worker, into_job(runnable));
                }
            }
        };
        match context.as_deref() {
            Some(context) => context.with_current_worker(&mut local),
            None => with_current_worker(&mut local),
        }
        let Some(runnable) = runnable else {
            return;
        };
        // External wakes and hosts without a scoped borrow still use a weak
        // upgrade. Pool destruction cancels an unrun task without a cycle.
        let Some(pool) = weak_pool.upgrade() else {
            drop(runnable);
            return;
        };
        let job = into_job(runnable);
        // A wake that happens *on* a worker goes straight to that worker.
        //
        // Every wake used to take the injector every worker contends on, plus a
        // wake for somebody already awake, and then came back on whichever
        // worker won the race rather than the one holding the task's cache
        // lines. `yield_now` wakes itself, so a task yielding in a retry loop
        // paid that round trip per iteration, which is the shape a contended
        // storage engine actually has.
        //
        // Off a worker thread this is the old path, which is what a wake from
        // a timer thread or an application thread wants: it has no locality to
        // preserve and the injector is where anyone can find the job.
        match worker_for(&pool, context.as_deref()) {
            Some(worker) => pool.submit_local(worker, job),
            None => pool.submit_job(job),
        }
    };

    // Keep failures attached to the join result instead of unwinding a worker.
    // This feature is enabled only by nagoya's std feature. In no_std the host
    // retains responsibility for its panic policy.
    #[cfg(feature = "std")]
    let (runnable, task) = async_task::Builder::new()
        .propagate_panic(true)
        .spawn(move |()| future, schedule);
    #[cfg(not(feature = "std"))]
    let (runnable, task) = async_task::spawn(future, schedule);
    runnable.schedule();
    task
}

fn into_job(runnable: Runnable) -> Job {
    // SAFETY: Runnable owns a Send task allocation. Job consumes it exactly
    // once, and poll_once reconstructs the same Runnable for run or drop.
    unsafe { Job::from_raw(runnable.into_raw(), poll_once) }
}

fn worker_for(pool: &Arc<Pool>, context: Option<&dyn WorkerContext>) -> Option<usize> {
    let worker = match context {
        Some(context) => context.current_worker(pool),
        None => current_worker(pool),
    };
    // A bad host hint must not index outside the pool or panic inside a waker.
    worker.filter(|id| *id < pool.workers())
}

/// Run one poll of the task this pointer owns, or release it unrun.
///
/// The `Act::Drop` arm is not a formality. A pool dropped with work still
/// queued hands every job this function with `Drop`, and taking the `Runnable`
/// back and letting it fall out of scope is what releases the future and
/// everything it captured. `async_task` treats that as cancelling the task,
/// which is the right reading: the poll it was scheduled for will never happen.
///
/// # Safety
///
/// `pointer` must have come from `Runnable::into_raw` and not yet been given to
/// `Runnable::from_raw`.
unsafe fn poll_once(pointer: NonNull<()>, act: Act) {
    // SAFETY: the caller's obligation, discharged at the one call site above.
    let runnable = unsafe { Runnable::<()>::from_raw(pointer) };
    match act {
        Act::Run => {
            runnable.run();
        }
        Act::Drop => drop(runnable),
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use st3::fanout::StdHost;

    fn pool() -> Arc<Pool> {
        Pool::new(2, 256, Arc::new(StdHost::new(2)))
    }

    #[test]
    fn routing_depends_on_identity_not_probabilistic_thread_migration() {
        let a = pool();
        let b = pool();
        assert_eq!(worker_for(&a, None), None);
        {
            let _outer = mark_current(&a, 1);
            assert_eq!(worker_for(&a, None), Some(1));
            assert_eq!(worker_for(&b, None), None);
            {
                let _inner = mark_current(&b, 0);
                assert_eq!(worker_for(&a, None), None);
                assert_eq!(worker_for(&b, None), Some(0));
            }
            assert_eq!(worker_for(&a, None), Some(1));
        }
        assert_eq!(worker_for(&a, None), None);
    }

    #[test]
    fn worker_scope_owns_a_lease_without_queued_task_ownership() {
        let owner = pool();
        let weak = Arc::downgrade(&owner);
        let guard = mark_current(&owner, 1);
        drop(owner);
        assert!(weak.upgrade().is_some());
        let mut visits = 0;
        with_current_worker(|borrowed, worker| {
            assert!(core::ptr::eq(borrowed, weak.as_ptr()));
            assert_eq!(worker, 1);
            visits += 1;
        });
        assert_eq!(visits, 1);
        drop(guard);
        assert!(weak.upgrade().is_none());
    }

    struct HostContext {
        owner: alloc::sync::Weak<Pool>,
        worker: usize,
    }

    impl WorkerContext for HostContext {
        fn current_worker(&self, pool: &Pool) -> Option<usize> {
            self.owner
                .upgrade()
                .and_then(|owner| core::ptr::eq(owner.as_ref(), pool).then_some(self.worker))
        }
    }

    #[test]
    fn host_identity_routes_without_a_thread_local_marker() {
        let a = pool();
        let b = pool();
        let context = HostContext {
            owner: Arc::downgrade(&a),
            worker: 1,
        };
        assert_eq!(worker_for(&a, Some(&context)), Some(1));
        assert_eq!(worker_for(&b, Some(&context)), None);
        let invalid = HostContext {
            owner: Arc::downgrade(&a),
            worker: usize::MAX,
        };
        assert_eq!(worker_for(&a, Some(&invalid)), None);
    }
}
