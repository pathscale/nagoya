//! Driving one future on the thread that asked.
//!
//! This is the entry point every runtime has, and it needs no pool: one future,
//! one thread, polled until it is ready. What it does need is a way to wait
//! without spinning, and that is the only thing here that differs with and
//! without a `std`.

use alloc::sync::Arc;
use core::future::Future;
use core::mem;
use core::pin::pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// A one-shot signal from a waker to the thread inside `block_on`.
struct Signal {
    woken: AtomicBool,
    #[cfg(feature = "std")]
    thread: std::thread::Thread,
}

impl Signal {
    fn new() -> Self {
        Self {
            woken: AtomicBool::new(false),
            #[cfg(feature = "std")]
            thread: std::thread::current(),
        }
    }

    fn wake(&self) {
        self.woken.store(true, Ordering::Release);
        #[cfg(feature = "std")]
        self.thread.unpark();
    }

    /// Wait for a wake, and report whether one had already arrived.
    fn wait(&self) {
        #[cfg(feature = "std")]
        while !self.woken.swap(false, Ordering::AcqRel) {
            // `park` may return spuriously, which the loop absorbs, and a wake
            // that landed before the park leaves a permit, so this cannot miss
            // one.
            std::thread::park();
        }

        // Without a `std` there is no thread to park. Spinning is the honest
        // fallback: a caller on a target with an idle instruction should reach
        // for the pool and `Host::park` instead of this.
        #[cfg(not(feature = "std"))]
        while !self.woken.swap(false, Ordering::AcqRel) {
            core::hint::spin_loop();
        }
    }
}

fn waker_of(signal: Arc<Signal>) -> Waker {
    let raw = RawWaker::new(Arc::into_raw(signal).cast(), &VTABLE);
    // SAFETY: the vtable is written for this pointer type, the pointer came
    // from `Arc::into_raw`, and every entry balances the count it touches.
    unsafe { Waker::from_raw(raw) }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_it);

unsafe fn clone(pointer: *const ()) -> RawWaker {
    let signal = unsafe { Arc::from_raw(pointer.cast::<Signal>()) };
    let cloned = signal.clone();
    mem::forget(signal);
    RawWaker::new(Arc::into_raw(cloned).cast(), &VTABLE)
}

unsafe fn wake(pointer: *const ()) {
    unsafe { Arc::from_raw(pointer.cast::<Signal>()) }.wake();
}

unsafe fn wake_by_ref(pointer: *const ()) {
    let signal = unsafe { Arc::from_raw(pointer.cast::<Signal>()) };
    signal.wake();
    mem::forget(signal);
}

unsafe fn drop_it(pointer: *const ()) {
    drop(unsafe { Arc::from_raw(pointer.cast::<Signal>()) });
}

/// Run a future to completion on this thread, and give back its output.
///
/// The thread waits between polls rather than spinning, when there is a `std`
/// to wait with. Nothing is spawned and no pool is involved, so this works on
/// its own.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let signal = Arc::new(Signal::new());
    let waker = waker_of(signal.clone());
    let mut context = Context::from_waker(&waker);

    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        signal.wait();
    }
}

/// Run a future on this caller using a host-provided blocking wait.
///
/// Available without Rust std. The host must retain an unpark permit delivered
/// before park, as required by [`st3::fanout::Host`]. `waiter` selects a host
/// slot dedicated to this caller: do not share it with a pool worker or another
/// concurrent/nested block_on call. The host and slot must remain valid for
/// outstanding cloned wakers even after the future completes.
///
/// This does not start workers or drive timers. The embedding still supplies
/// those services; blocking a worker on its own exhausted pool can deadlock.
pub fn block_on_with_host<F: Future>(
    future: F,
    host: Arc<dyn st3::fanout::Host>,
    waiter: usize,
) -> F::Output {
    struct HostWake {
        host: Arc<dyn st3::fanout::Host>,
        waiter: usize,
    }
    impl alloc::task::Wake for HostWake {
        fn wake(self: Arc<Self>) {
            self.host.unpark(self.waiter);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.host.unpark(self.waiter);
        }
    }
    let signal = Arc::new(HostWake { host, waiter });
    let waker = Waker::from(signal.clone());
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        signal.host.park(waiter);
    }
}
