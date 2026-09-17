//! Running a future on the thread that owns the poller.
//!
//! # Why this exists
//!
//! [`Reactor`](super::Reactor) owns a thread. It returns from `kevent`, wakes
//! a waker, and the task polls somewhere else. That handoff costs a park and
//! an unpark, measured at about 10us on a Mac laptop, and it is paid on every
//! message. It buys something real - many tasks on many threads, all fed by
//! one poller - but for a single connection driven by one task it is pure
//! overhead, and it is most of the gap this crate had against tokio's
//! current-thread runtime, which polls on the thread that just returned from
//! the kernel.
//!
//! [`block_on`] is that arrangement: one thread, which waits for readiness and
//! then polls the future itself. No second thread, no handoff, no waker
//! crossing a thread boundary on the hot path.
//!
//! # When to use which
//!
//! `block_on` when one thread drives the connections it owns, which is the
//! shape a WebSocket server usually wants: a thread per core, each with its
//! own poller and its own set of connections, sharing nothing.
//!
//! [`Reactor`](super::Reactor) when tasks must run elsewhere: a pool sized
//! differently from the number of pollers, or work that has to move between
//! threads.

use alloc::sync::Arc;
use alloc::task::Wake;
use core::future::Future;
use core::pin::pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};

use super::driver::{Handle, Reactor};
use super::error::Result;

/// Woken when a task is ready to make progress again.
///
/// The flag is the whole mechanism: a wake from the poller's own thread just
/// sets it, and the loop below notices on the next pass. A wake from another
/// thread also has to interrupt the wait, which is what the handle is for.
struct LocalWaker {
    ready: AtomicBool,
    handle: Handle,
}

impl Wake for LocalWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Release);
        // If the wake came from the poller's thread, the loop is not waiting
        // and this is redundant but harmless. If it came from elsewhere, the
        // loop may be blocked in `kevent` with no event coming, and this is
        // what gets it out.
        let _ = self.handle.wake();
    }
}

/// Drive `future` to completion on this thread, waiting for readiness in place.
///
/// The reactor and the task share a thread, so a socket becoming readable and
/// the task reading from it happen without a thread handoff between them.
///
/// The reactor is created here and shut down when this returns. A caller
/// running several futures should use [`block_on_with`] and keep one reactor.
///
/// # Not [`crate::block_on`]
///
/// The crate root has one too, and it parks the thread on a condvar until a
/// waker says otherwise. That is right for a future waiting on a channel or a
/// timer and wrong for one waiting on a socket: nothing would ever wake it,
/// because no reactor is running to notice the socket became readable. This
/// one waits in `kevent`/`epoll_wait` instead, which is the same wait a
/// reactor thread would do, done here.
pub fn block_on<F: Future>(future: F) -> Result<F::Output> {
    let reactor = Reactor::local()?;
    let output = block_on_with(&reactor, future);
    reactor.shutdown();
    Ok(output)
}

/// Drive `future` on this thread using an existing reactor.
///
/// The reactor must have been created by [`Reactor::local`]: it has no thread
/// of its own, and this loop is what drives it.
pub fn block_on_with<F: Future>(reactor: &Reactor, future: F) -> F::Output {
    let mut future = pin!(future);

    let waker = Arc::new(LocalWaker {
        // Starts ready so the first pass polls rather than waiting for an
        // event that may already have happened.
        ready: AtomicBool::new(true),
        handle: reactor.handle(),
    });
    let raw = Waker::from(Arc::clone(&waker));
    let mut context = Context::from_waker(&raw);

    loop {
        if waker.ready.swap(false, Ordering::AcqRel) {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
        }

        // Nothing to poll: wait for the kernel. This is the same wait the
        // reactor thread would have done, done here instead, which is the
        // point of the whole module.
        if reactor.poll_once().is_err() {
            // The poller failed. Polling again would spin, so give the future
            // one more chance to finish on what it already has and then stop.
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            core::hint::spin_loop();
        }
    }
}
