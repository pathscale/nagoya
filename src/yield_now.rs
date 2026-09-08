//! A task giving its worker back.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

/// Let other work run, then continue.
///
/// A task that never suspends holds its worker until it finishes. Anything
/// long-running and CPU-bound should hand the worker back from time to time, or
/// it delays everything queued behind it, and this is how.
///
/// ```
/// # async fn example() {
/// for chunk in 0..1_000 {
///     # let _ = chunk;
///     // ... a slice of a long computation ...
///     nagoya::yield_now().await;
/// }
/// # }
/// ```
///
/// # What it costs, and when not to
///
/// The task wakes itself before returning `Pending`, so the poll that follows
/// goes through the whole scheduling path: a job onto the injector, and a
/// worker picking it back up. That is worth it every few microseconds of work
/// and wasteful every few nanoseconds.
///
/// **It has nothing to offer a [`par_for`](crate::par_for) body.** That body is
/// a closure, not a future, and cannot await anything. A parallel loop yields
/// between pieces instead, and [`ParFor::leaf`](crate::ParFor::leaf) is how
/// often.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// The future returned by [`yield_now`].
#[derive(Debug)]
#[must_use = "a yield that is not awaited does not yield"]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        // Woken before returning `Pending`, so this is a yield and not a stall:
        // the task is rescheduled at the back of the queue rather than waiting
        // for somebody else to wake it.
        context.waker().wake_by_ref();
        Poll::Pending
    }
}
