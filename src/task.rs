//! A future turned into something the pool can run.
//!
//! `st3::fanout` takes `Box<dyn FnOnce() + Send>` and runs it once. A future
//! is polled many times, so the task submitted to the pool is not the future:
//! it is *one poll of it*, and the waker submits the next one.

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

/// What a `JoinHandle` reads and a finished task writes.
pub(crate) struct Slot<T> {
    output: Mutex<Option<T>>,
    waiter: Mutex<Option<Waker>>,
    finished: AtomicU8,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            output: Mutex::new(None),
            waiter: Mutex::new(None),
            finished: AtomicU8::new(0),
        }
    }

    /// The output, once, or the waker to call when there is one.
    pub(crate) fn poll(&self, context: &Context<'_>) -> Poll<Option<T>> {
        if self.finished.load(Ordering::Acquire) == 1 {
            return Poll::Ready(self.output.lock().take());
        }
        // Registered before the flag is re-read, so a finish that lands between
        // the two still finds a waker to call.
        *self.waiter.lock() = Some(context.waker().clone());
        if self.finished.load(Ordering::Acquire) == 1 {
            return Poll::Ready(self.output.lock().take());
        }
        Poll::Pending
    }

    fn finish(&self, value: T) {
        *self.output.lock() = Some(value);
        self.finished.store(1, Ordering::Release);
        if let Some(waker) = self.waiter.lock().take() {
            waker.wake();
        }
    }
}

/// A spawned future, its output slot, and where to put the next poll.
pub(crate) struct RawTask<F: Future> {
    future: Mutex<Option<Pin<Box<F>>>>,
    slot: Arc<Slot<F::Output>>,
    pool: Arc<Pool>,
    worker: usize,
    state: AtomicU8,
}

impl<F> RawTask<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    pub(crate) fn spawn(future: F, pool: Arc<Pool>, worker: usize) -> Arc<Slot<F::Output>> {
        let slot = Arc::new(Slot::new());
        let task = Arc::new(Self {
            future: Mutex::new(Some(Box::pin(future))),
            slot: slot.clone(),
            pool,
            worker,
            state: AtomicU8::new(IDLE),
        });
        task.schedule();
        slot
    }

    /// Put one poll of this task on the pool, unless one is already there.
    ///
    /// A wake arriving while a poll runs sets `NOTIFIED` instead of queueing a
    /// second poll. Without that the second poll finds the future taken out of
    /// its slot, does nothing, and the wake is lost.
    fn schedule(self: &Arc<Self>) {
        loop {
            match self.state.load(Ordering::Acquire) {
                DONE => return,
                RUNNING => {
                    if self
                        .state
                        .compare_exchange(RUNNING, NOTIFIED, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return;
                    }
                }
                NOTIFIED => return,
                _ => {
                    if self
                        .state
                        .compare_exchange(IDLE, RUNNING, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        let task = self.clone();
                        let pool = self.pool.clone();
                        let worker = self.worker;
                        pool.submit(worker, Box::new(move || task.run()));
                        return;
                    }
                }
            }
        }
    }

    /// Poll until the future is pending with no wake outstanding, or done.
    fn run(self: Arc<Self>) {
        loop {
            let Some(mut future) = self.future.lock().take() else {
                self.state.store(IDLE, Ordering::Release);
                return;
            };

            let waker = waker_of(self.clone());
            let mut context = Context::from_waker(&waker);
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => {
                    self.state.store(DONE, Ordering::Release);
                    // Dropped before the waiter is woken, so a joiner that
                    // drops the handle does not race the future's destructor.
                    drop(future);
                    self.slot.finish(value);
                    return;
                }
                Poll::Pending => {
                    *self.future.lock() = Some(future);
                    // A wake during the poll leaves NOTIFIED, and the loop
                    // takes it rather than parking on a wake already spent.
                    match self.state.compare_exchange(
                        RUNNING,
                        IDLE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => return,
                        Err(_) => {
                            self.state.store(RUNNING, Ordering::Release);
                        }
                    }
                }
            }
        }
    }
}

/// A waker over `Arc<RawTask>`, which is a thin pointer because the task type
/// is concrete here. An `Arc<dyn Schedule>` would be fat and would not fit the
/// `*const ()` a `RawWaker` carries.
fn waker_of<F>(task: Arc<RawTask<F>>) -> Waker
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let raw = RawWaker::new(Arc::into_raw(task).cast(), vtable::<F>());
    // SAFETY: the vtable below is written for exactly this pointer type, and
    // the pointer came from `Arc::into_raw`, so every entry has an `Arc` to
    // act on and the counts stay balanced.
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
        let task = unsafe { Arc::from_raw(pointer.cast::<RawTask<F>>()) };
        task.schedule();
    }

    unsafe fn wake_by_ref<F>(pointer: *const ())
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = unsafe { Arc::from_raw(pointer.cast::<RawTask<F>>()) };
        task.schedule();
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
