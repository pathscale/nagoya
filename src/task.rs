//! A future turned into something the pool can run.
//!
//! `st3::fanout` takes `Box<dyn FnOnce() + Send>` and runs it once. A future is
//! polled many times, so the task submitted to the pool is not the future: it
//! is *one poll of it*, and the waker submits the next one.
//!
//! # Allocations
//!
//! Two per task: the task itself, and one `Box` per poll for the closure the
//! pool takes. It was four. The output slot used to be its own `Arc` and the
//! future its own `Box::pin`; both now live inside the task's single
//! allocation.
//!
//! The remaining `Box` per poll cannot be removed from here. `fanout::Task` is
//! `Box<dyn FnOnce()>`, so submitting anything means boxing it. A pool taking a
//! `(pointer, fn)` pair instead would need no allocation at all, and that is a
//! change to `ps-st3` rather than to this crate.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::sync::atomic::{AtomicU8, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use spin::Mutex;
use st3::fanout::Pool;

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
    future: Mutex<Option<F>>,
    output: Mutex<Option<F::Output>>,
    waiter: Mutex<Option<Waker>>,
    finished: AtomicU8,
    state: AtomicU8,
    pool: Arc<Pool>,
    worker: usize,
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
    pub(crate) fn spawn(future: F, pool: Arc<Pool>, worker: usize) -> Arc<dyn Joinable<F::Output>> {
        let task = Arc::new(Self {
            future: Mutex::new(Some(future)),
            output: Mutex::new(None),
            waiter: Mutex::new(None),
            finished: AtomicU8::new(0),
            state: AtomicU8::new(IDLE),
            pool,
            worker,
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
                        let worker = self.worker;
                        let pool = self.pool.clone();
                        pool.submit(worker, Box::new(move || self.run()));
                        return;
                    }
                }
            }
        }
    }

    /// Poll until the future is pending with no wake outstanding, or done.
    fn run(self: Arc<Self>) {
        loop {
            let waker = waker_of(self.clone());
            let mut context = Context::from_waker(&waker);

            let polled = {
                let mut slot = self.future.lock();
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
                    self.state.store(DONE, Ordering::Release);
                    drop(self.future.lock().take());
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

/// A waker over `Arc<RawTask>`, which is a thin pointer because the task type is
/// concrete here. An `Arc<dyn Joinable>` would be fat and would not fit the
/// `*const ()` a `RawWaker` carries.
fn waker_of<F>(task: Arc<RawTask<F>>) -> Waker
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let raw = RawWaker::new(Arc::into_raw(task).cast(), vtable::<F>());
    // SAFETY: the vtable below is written for exactly this pointer type, and
    // the pointer came from `Arc::into_raw`, so every entry has an `Arc` to act
    // on and the counts stay balanced.
    unsafe { Waker::from_raw(raw) }
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
