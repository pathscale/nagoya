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
/// Called by [`crate::runtime::Runtime`] around its `Pool::run`. A caller
/// driving a pool by hand can call it too; without it nothing breaks, wakes
/// simply take the injector as they always did.
#[cfg(feature = "std")]
pub(crate) fn mark_current(pool: &Arc<Pool>, worker: usize) -> CurrentGuard {
    CURRENT.with(|current| current.set(Some((Arc::as_ptr(pool), worker))));
    CurrentGuard
}

/// Clears the current-worker marker on drop, including on unwind.
#[cfg(feature = "std")]
pub(crate) struct CurrentGuard;

#[cfg(feature = "std")]
impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| current.set(None));
    }
}

/// This thread's worker id, if it is running `pool`.
#[cfg(feature = "std")]
fn current_worker(pool: &Arc<Pool>) -> Option<usize> {
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
pub(crate) fn spawn<F>(future: F, pool: Arc<Pool>) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Called for the first poll and for every wake after it. The `Runnable` is
    // a single heap allocation that already holds the future and its state, so
    // turning it into a job is `into_raw`: no box, no closure, nothing new.
    let schedule = move |runnable: Runnable| {
        let pointer = runnable.into_raw();
        // SAFETY: `Job::from_raw` asks for a pointer that stays valid until the
        // job runs, work that is `Send`, an `execute` correct for the pointer,
        // and one job per pointer. `Runnable::into_raw` gives up ownership of
        // an allocation that lives until `from_raw` takes it back, which
        // `poll_once` does exactly once because running consumes the job;
        // `F` and `F::Output` are `Send`; and `Runnable::run` catches no panic,
        // which is the same as every other job this pool takes.
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
        match current_worker(&pool) {
            Some(worker) => pool.submit_local(worker, job),
            None => pool.submit_job(job),
        }
    };

    let (runnable, task) = async_task::spawn(future, schedule);
    runnable.schedule();
    task
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
