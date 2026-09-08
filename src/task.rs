//! A future turned into something the pool can run.
//!
//! `st3::fanout` takes `Box<dyn FnOnce() + Send>` and runs it once. A future is
//! polled many times, so the task submitted to the pool is not the future: it
//! is *one poll of it*, and the waker submits the next one.
//!
//! # Allocations
//!
//! **One per task, and none per poll.** It was four. The output slot used to be
//! its own `Arc` and the future its own `Box::pin`, and both now live inside
//! the task's single allocation; the fourth was a `Box` per poll for the
//! closure the pool took, and the pool no longer takes a closure.
//!
//! `st3::fanout::Job` is a thin pointer and the function that runs it, so the
//! task hands the pool the allocation it already lives in: `Arc::into_raw` on
//! the way out, `Arc::from_raw` on the way in. Nothing is allocated to schedule
//! a poll, however many times a future is woken.

use alloc::sync::Arc;
use core::future::Future;
use core::mem;
use core::cell::UnsafeCell;
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::sync::atomic::{AtomicU8, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use core::ptr::NonNull;
use spin::Mutex;
use st3::fanout::{Job, Pool};

/// Nobody is polling and nobody has asked for one.
const IDLE: u8 = 0;
/// A poll is queued or running.
const RUNNING: u8 = 1;
/// A wake arrived while a poll was running, so poll again before idling.
const NOTIFIED: u8 = 2;
/// The future finished; further wakes are dropped.
const DONE: u8 = 3;

/// What a [`crate::JoinHandle`] can ask of a task without knowing its future.
///
/// The handle is generic over the output alone and the task over the whole
/// future type, so something has to erase the difference. A trait object does
/// it without a second allocation: the handle holds another `Arc` to the task
/// that already exists.
pub(crate) trait Joinable<T>: Send + Sync {
    fn poll_output(&self, context: &Context<'_>) -> Poll<Option<T>>;
}

/// A spawned future, its output, and where to put the next poll.
pub(crate) struct RawTask<F: Future> {
    /// Polled in place. Taking it out and putting it back would move it, which
    /// is the one thing a `Future` may not survive; the `Arc` never moves, so a
    /// pointer into it is a valid pin.
    ///
    /// **No lock, because `state` already is one.** A thread reaches `run` only
    /// by winning the `IDLE -> RUNNING` compare-exchange in `schedule`, and
    /// nothing leaves `RUNNING` except that same thread. So the state machine
    /// already grants exclusive access to this cell for exactly the window in
    /// which it is touched, and a `Mutex` around it was a second lock enforcing
    /// what the first one had proved: two acquisitions per poll for nothing.
    future: UnsafeCell<Option<F>>,
    output: Mutex<Option<F::Output>>,
    waiter: Mutex<Option<Waker>>,
    finished: AtomicU8,
    state: AtomicU8,
    pool: Arc<Pool>,
}

// SAFETY: the `UnsafeCell` is what stops these being derived. Access to it is
// serialised by `state`, as the field's own comment sets out: one thread at a
// time, and the handover between them goes through an `AcqRel` compare-exchange
// on `state`, which is the edge that publishes the writes. `F` and `F::Output`
// are `Send`, so moving the task between threads moves them legally.
unsafe impl<F> Send for RawTask<F>
where
    F: Future + Send,
    F::Output: Send,
{
}
unsafe impl<F> Sync for RawTask<F>
where
    F: Future + Send,
    F::Output: Send,
{
}

impl<F> Joinable<F::Output> for RawTask<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn poll_output(&self, context: &Context<'_>) -> Poll<Option<F::Output>> {
        if self.finished.load(Ordering::Acquire) == 1 {
            return Poll::Ready(self.output.lock().take());
        }
        // Registered before the flag is read again, so a finish landing between
        // the two still finds a waker to call.
        *self.waiter.lock() = Some(context.waker().clone());
        if self.finished.load(Ordering::Acquire) == 1 {
            return Poll::Ready(self.output.lock().take());
        }
        Poll::Pending
    }
}

impl<F> RawTask<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    pub(crate) fn spawn(future: F, pool: Arc<Pool>) -> Arc<dyn Joinable<F::Output>> {
        let task = Arc::new(Self {
            future: UnsafeCell::new(Some(future)),
            output: Mutex::new(None),
            waiter: Mutex::new(None),
            finished: AtomicU8::new(0),
            state: AtomicU8::new(IDLE),
            pool,
        });
        task.clone().schedule();
        task
    }

    /// Put one poll of this task on the pool, unless one is already there.
    ///
    /// A wake arriving while a poll runs sets `NOTIFIED` instead of queueing a
    /// second poll. Without that the second poll would find the future already
    /// held for polling, do nothing, and the wake would be lost.
    fn schedule(self: Arc<Self>) {
        loop {
            match self.state.load(Ordering::Acquire) {
                DONE | NOTIFIED => return,
                RUNNING => {
                    if self
                        .state
                        .compare_exchange(RUNNING, NOTIFIED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return;
                    }
                }
                _ => {
                    if self
                        .state
                        .compare_exchange(IDLE, RUNNING, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        let pool = self.pool.clone();
                        // The task *is* the job. `Arc::into_raw` hands the
                        // pool the allocation this task already lives in, so a
                        // poll costs no allocation at all; the matching
                        // `Arc::from_raw` in `poll_once` takes the ownership
                        // back. This used to box a closure per poll.
                        let pointer = Arc::into_raw(self).cast::<()>().cast_mut();
                        // SAFETY: `Arc::into_raw` never returns null, the
                        // allocation stays alive because this holds the strong
                        // count it just gave up, and `poll_once::<F>` is
                        // written for exactly this pointer type. `run` catches
                        // nothing, but neither did the closure it replaces, and
                        // a panic in a future was already the caller's problem.
                        let job = unsafe {
                            Job::from_raw(NonNull::new_unchecked(pointer), poll_once::<F>)
                        };
                        pool.submit_job(job);
                        return;
                    }
                }
            }
        }
    }

    /// Poll until the future is pending with no wake outstanding, or done.
    fn run(self: Arc<Self>) {
        // **One waker for the whole run, and it owns nothing.** Built from a
        // borrowed pointer and never dropped, so it costs no reference count;
        // the vtable's `clone` takes a real one for any waker that outlives
        // this call. Building it inside the loop, which is what this replaces,
        // took a count up and put it back down on every poll.
        let waker = borrowed_waker(&self);
        let mut context = Context::from_waker(&waker);

        loop {
            let polled = {
                // SAFETY: this thread holds `RUNNING`, which is exclusive: the
                // only transition into it is the compare-exchange in
                // `schedule`, and the only transitions out are made below by
                // this thread. So nothing else may touch the cell now.
                let slot = unsafe { &mut *self.future.get() };
                let Some(future) = slot.as_mut() else {
                    self.state.store(IDLE, Ordering::Release);
                    return;
                };
                // SAFETY: the future lives inside an `Arc`, which never moves
                // it; it is never moved out of this slot while pollable; and
                // the only place it leaves is the `take` below, after it has
                // completed and may no longer be polled.
                let future = unsafe { Pin::new_unchecked(future) };
                future.poll(&mut context)
            };

            match polled {
                Poll::Ready(value) => {
                    // Dropped before `state` says `DONE`, so this thread is
                    // still the exclusive one when the future's destructor
                    // runs.
                    //
                    // SAFETY: as above, this thread still holds `RUNNING`.
                    drop(unsafe { (*self.future.get()).take() });
                    self.state.store(DONE, Ordering::Release);
                    self.finish(value);
                    return;
                }
                Poll::Pending => {
                    // A wake during the poll left NOTIFIED, and this takes it
                    // rather than idling on a wake already spent.
                    if self
                        .state
                        .compare_exchange(RUNNING, IDLE, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return;
                    }
                    self.state.store(RUNNING, Ordering::Release);
                }
            }
        }
    }

    fn finish(&self, value: F::Output) {
        *self.output.lock() = Some(value);
        self.finished.store(1, Ordering::Release);
        let waiter = self.waiter.lock().take();
        if let Some(waker) = waiter {
            waker.wake();
        }
    }
}

/// A `Waker` over a borrowed `Arc<RawTask>`, which takes no reference count.
///
/// `ManuallyDrop` is the whole trick: the returned waker is never dropped, so
/// the count it did not take is never given back. Anything that wants a waker
/// outliving this call clones it, and the vtable's `clone` takes a real count.
fn borrowed_waker<F>(task: &Arc<RawTask<F>>) -> ManuallyDrop<Waker>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let raw = RawWaker::new(Arc::as_ptr(task).cast(), vtable::<F>());
    // SAFETY: the vtable is written for exactly this pointer type. The pointer
    // is valid for as long as the caller's `Arc` is, which is the whole of
    // `run`, and the waker never outlives it because `ManuallyDrop` stops it
    // being dropped and nothing here moves it out.
    ManuallyDrop::new(unsafe { Waker::from_raw(raw) })
}

/// Run one poll of the task this pointer owns.
///
/// # Safety
///
/// `pointer` must be an `Arc<RawTask<F>>` handed over by `Arc::into_raw`, whose
/// strong count has not been given to anyone else.
unsafe fn poll_once<F>(pointer: NonNull<()>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // SAFETY: the caller's obligation, discharged at the one call site in
    // `schedule`, which pairs this function with a pointer of this type.
    let task = unsafe { Arc::from_raw(pointer.as_ptr().cast::<RawTask<F>>()) };
    task.run();
}

fn vtable<F>() -> &'static RawWakerVTable
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    unsafe fn clone<F>(pointer: *const ()) -> RawWaker
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = unsafe { Arc::from_raw(pointer.cast::<RawTask<F>>()) };
        let cloned = task.clone();
        mem::forget(task);
        RawWaker::new(Arc::into_raw(cloned).cast(), vtable::<F>())
    }

    unsafe fn wake<F>(pointer: *const ())
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        unsafe { Arc::from_raw(pointer.cast::<RawTask<F>>()) }.schedule();
    }

    unsafe fn wake_by_ref<F>(pointer: *const ())
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = unsafe { Arc::from_raw(pointer.cast::<RawTask<F>>()) };
        task.clone().schedule();
        mem::forget(task);
    }

    unsafe fn drop_it<F>(pointer: *const ())
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        drop(unsafe { Arc::from_raw(pointer.cast::<RawTask<F>>()) });
    }

    &RawWakerVTable::new(clone::<F>, wake::<F>, wake_by_ref::<F>, drop_it::<F>)
}
