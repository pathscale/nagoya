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
        pool.submit_job(job);
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
