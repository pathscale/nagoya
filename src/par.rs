//! A parallel loop, as a task.
//!
//! # Why this is a future and not a blocking call
//!
//! `rayon::par_iter` blocks the calling thread until every piece is done, and
//! blocking needs somewhere to block: a condition variable, a futex, an
//! operating system. This crate does not have one, and giving it one is the
//! thing it exists to avoid.
//!
//! So a parallel loop here **is a task**. It splits, the pieces run on the
//! pool, and the loop itself is a future that completes when the last piece
//! does. Awaiting it costs the awaiting task nothing: the worker that was
//! running it goes and runs pieces of the loop instead.
//!
//! That is also the answer to what these two APIs have to do with each other.
//! A task is a thing that can suspend; a parallel loop is a thing that cannot.
//! Wrapping the loop in a task is what lets one runtime offer both, and it is
//! why `par_for_each` returns something you `await` rather than something that
//! parks your thread.
//!
//! # What it does not do
//!
//! The work is `'static`, so this cannot borrow a slice off the caller's stack
//! the way `rayon::scope` can. A future may be dropped before it completes, and
//! the pieces already handed to the pool would outlive the borrow. Closing that
//! needs either a scope that cannot be dropped early or the caller's promise
//! that it will not be, and neither is worth the unsafe until something asks
//! for it. Move what you need into the closure, or put it in an `Arc`.

use alloc::sync::Arc;
use core::future::Future;
use core::ops::Range;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};

use spin::Mutex;
use st3::fanout::Pool;

/// How small a piece has to be before it is run rather than split further.
///
/// **Not one item per worker.** An even split across eight workers is only
/// balanced if every item costs the same and every worker is free, and neither
/// is true: one slow item leaves seven workers idle with nothing left to steal.
/// Several pieces per worker is what gives stealing something to move, and it
/// is why `rayon` oversplits too.
const PIECES_PER_WORKER: usize = 8;

/// Run `body` for every index in `range`, splitting the range across the pool.
///
/// The returned future completes when the last index has been run. Nothing
/// happens until it is polled: the first poll is what hands the work over, so
/// a loop that is created and dropped costs one allocation and no work.
///
/// ```
/// # use core::sync::atomic::{AtomicUsize, Ordering};
/// # use alloc::sync::Arc;
/// # extern crate alloc;
/// # fn main() {}
/// # #[cfg(feature = "std")]
/// # fn example(pool: alloc::sync::Arc<st3::fanout::Pool>) {
/// let hits = Arc::new(AtomicUsize::new(0));
/// let counter = hits.clone();
/// let loop_ = nagoya::par_for_each(pool, 0..1_000, move |_| {
///     counter.fetch_add(1, Ordering::Relaxed);
/// });
/// nagoya::block_on(loop_);
/// assert_eq!(hits.load(Ordering::Relaxed), 1_000);
/// # }
/// ```
pub fn par_for_each<F>(pool: Arc<Pool>, range: Range<usize>, body: F) -> ParForEach
where
    F: Fn(usize) + Send + Sync + 'static,
{
    let len = range.end.saturating_sub(range.start);
    let leaf = (len / (pool.workers() * PIECES_PER_WORKER)).max(1);
    ParForEach {
        state: Some((
            Arc::new(Shared {
                body,
                // One outstanding piece: the whole range, not yet handed over.
                outstanding: AtomicUsize::new(1),
                finished: AtomicBool::new(false),
                waiter: Mutex::new(None),
            }) as Arc<dyn Split>,
            pool,
            range,
            leaf,
        )),
        started: None,
    }
}

/// The half of a running loop that a piece needs to see, with the closure's
/// type forgotten so every piece can name it.
trait Split: Send + Sync {
    /// Run `range`, splitting it while it is larger than `leaf`.
    fn work(self: Arc<Self>, pool: &Arc<Pool>, range: Range<usize>, leaf: usize);
    /// One piece finished. Completes the loop if it was the last.
    fn retire(&self);
    fn is_finished(&self) -> bool;
    fn park(&self, waker: &Waker);
}

struct Shared<F> {
    body: F,
    /// Pieces handed to the pool and not yet finished, plus one for the root
    /// until it has been handed over. Splitting adds one before it subtracts
    /// one, so this never reaches zero while there is work left to create.
    outstanding: AtomicUsize,
    finished: AtomicBool,
    waiter: Mutex<Option<Waker>>,
}

impl<F> Split for Shared<F>
where
    F: Fn(usize) + Send + Sync + 'static,
{
    fn work(self: Arc<Self>, pool: &Arc<Pool>, range: Range<usize>, leaf: usize) {
        let mut range = range;
        // Split iteratively rather than recursively, giving the right half to
        // the pool and keeping the left. Recursion here would put the whole
        // depth on a worker's stack, and a bare-metal target may not have one
        // to spare.
        while range.end - range.start > leaf {
            let middle = range.start + (range.end - range.start) / 2;
            let right = middle..range.end;
            range = range.start..middle;

            // Count the new piece *before* publishing it. The other order lets
            // the piece finish and retire against a count that has not been
            // raised yet, which would complete the loop early.
            self.outstanding.fetch_add(1, Ordering::AcqRel);
            let half = self.clone();
            let handle = pool.clone();
            pool.submit_fn(move || half.work(&handle, right, leaf));
        }
        for index in range {
            (self.body)(index);
        }
        self.retire();
    }

    fn retire(&self) {
        if self.outstanding.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        self.finished.store(true, Ordering::Release);
        let waiter = self.waiter.lock().take();
        if let Some(waker) = waiter {
            waker.wake();
        }
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    fn park(&self, waker: &Waker) {
        *self.waiter.lock() = Some(waker.clone());
    }
}

/// Everything a loop needs to start: the shared half, the pool to put pieces
/// on, the range still to cover, and how small a piece may get.
type Pending = (Arc<dyn Split>, Arc<Pool>, Range<usize>, usize);

/// A parallel loop that has not finished yet.
///
/// Created by [`par_for_each`]. Poll it to start the work and again to learn
/// that it is done.
pub struct ParForEach {
    /// Everything needed to start, taken on the first poll.
    state: Option<Pending>,
    started: Option<Arc<dyn Split>>,
}

impl Future for ParForEach {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();

        if let Some((shared, pool, range, leaf)) = this.state.take() {
            this.started = Some(shared.clone());
            // Hand the whole range to the pool rather than splitting it here.
            // Splitting is itself work, and doing it on the pool means it is
            // parallel too: each piece splits its own half.
            let handle = pool.clone();
            pool.submit_fn(move || shared.work(&handle, range, leaf));
        }

        let Some(shared) = this.started.as_ref() else {
            // Polled after it completed and was taken. A future polled past
            // completion may do anything sane; saying "done" is the sane one.
            return Poll::Ready(());
        };

        if shared.is_finished() {
            return Poll::Ready(());
        }
        // Registered before the flag is read again, so a piece that finishes
        // between the two still finds a waker to call.
        shared.park(context.waker());
        if shared.is_finished() {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}
