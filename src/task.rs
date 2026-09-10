//! A future turned into something the pool can run.
//!
//! # Why this is thirty lines and not three hundred
//!
//! It was three hundred: a state machine over `IDLE`/`RUNNING`/`NOTIFIED`/
//! `DONE`, a waker vtable, an output slot, a join waiter, and the `unsafe` to
//! hold it together. All of that is what `async-task` is, and `async-task` is
//! what `forte` uses to beat this crate. Keeping our own version of it was
//! carrying a liability to no measured end.
//!
//! What is left here is the seam: turning an `async_task::Runnable` into a
//! [`Job`] the pool can hold, which is one pointer and one function and no
//! allocation, because the `Runnable` *is* the allocation.

use alloc::sync::Arc;
use core::future::Future;
use core::ptr::NonNull;

use async_task::{Runnable, Task};
use st3::fanout::{Act, Job, Pool};

use crate::WorkerContext;

// Which pool worker this thread is, while it is running one.
//
// A raw pointer rather than an id, so a process with two pools cannot hand one
// pool's job to the other's worker. It is compared and never dereferenced.
#[cfg(feature = "std")]
std::thread_local! {
    static CURRENT: core::cell::Cell<Option<(*const Pool, usize)>> =
        const { core::cell::Cell::new(None) };
}

/// Mark this thread as `worker` of `pool` until the guard drops.
///
/// Used by the owned runtime and Executor::run_worker. A custom/no_std host
/// can instead supply WorkerContext to its executor.
#[cfg(feature = "std")]
pub(crate) fn mark_current(pool: &Arc<Pool>, worker: usize) -> CurrentGuard {
    let previous = CURRENT.with(|current| current.replace(Some((Arc::as_ptr(pool), worker))));
    CurrentGuard {
        previous,
        _thread_bound: core::marker::PhantomData,
    }
}

/// Restores a nested marker on drop, including on unwind. Must not cross threads.
#[cfg(feature = "std")]
pub(crate) struct CurrentGuard {
    previous: Option<(*const Pool, usize)>,
    _thread_bound: core::marker::PhantomData<alloc::rc::Rc<()>>,
}

#[cfg(feature = "std")]
impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(self.previous));
    }
}

/// This thread's worker id, if it is running `pool`.
#[cfg(feature = "std")]
pub(crate) fn current_worker(pool: &Arc<Pool>) -> Option<usize> {
    CURRENT.with(|current| match current.get() {
        Some((owner, worker)) if core::ptr::eq(owner, Arc::as_ptr(pool)) => Some(worker),
        _ => None,
    })
}

#[cfg(not(feature = "std"))]
fn current_worker(_pool: &Arc<Pool>) -> Option<usize> {
    None
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
        let Some(pool) = weak_pool.upgrade() else {
            // The executor/host owns pool lifetime, not a queued task.
            drop(runnable);
            return;
        };
        let pointer = runnable.into_raw();
        // SAFETY: `Job::from_raw` asks for a pointer that stays valid until the
        // job runs, work that is `Send`, an `execute` correct for the pointer,
        // and one job per pointer. `Runnable::into_raw` gives up ownership of
        // an allocation that lives until `from_raw` takes it back, which
        // `poll_once` does exactly once because running consumes the job;
        // `F` and `F::Output` are `Send`.
        let job = unsafe { Job::from_raw(pointer, poll_once) };
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
