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
//! why `par_for` returns something you `await` rather than something that
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

#[cfg(feature = "std")]
use alloc::boxed::Box;
use alloc::sync::{Arc, Weak};
use core::future::Future;
use core::ops::Range;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};

use spin::Mutex;
use st3::fanout::Pool;

/// A switch that stops a parallel loop.
///
/// Cheap to clone and to read: one `Relaxed` load per piece, which is a few
/// hundred loads over a loop of any size, not one per item.
///
/// # What cancelling does and does not do
///
/// It stops work that **has not started**. A piece already running finishes its
/// current chunk, because the body is a closure and a closure cannot be
/// interrupted between two of its own instructions. So this bounds the work
/// still to come, not the work in flight, and the bound is one leaf chunk per
/// busy worker.
///
/// If that is too coarse, make the chunks smaller with
/// [`ParFor::leaf`]. If it is far too coarse, what you want is for the body
/// itself to check, and it can: clone this into the closure.
#[derive(Clone, Default)]
pub struct Cancel(Arc<AtomicBool>);

impl Cancel {
    /// A switch that has not been thrown.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop the loop. Idempotent, and callable from anywhere including from
    /// inside the loop's own body.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Whether the switch has been thrown.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

impl core::fmt::Debug for Cancel {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Cancel").field(&self.is_cancelled()).finish()
    }
}

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
/// With `std` and unwinding enabled, a body panic cancels remaining work and
/// is rethrown by the awaiting future after all published pieces retire.
/// Without `std`, panic handling is the host's responsibility.
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
/// let loop_ = nagoya::par_for(pool, 0..1_000, move |_| {
///     counter.fetch_add(1, Ordering::Relaxed);
/// });
/// nagoya::block_on(loop_);
/// assert_eq!(hits.load(Ordering::Relaxed), 1_000);
/// # }
/// ```
pub fn par_for<F>(pool: Arc<Pool>, range: Range<usize>, body: F) -> ParFor
where
    F: Fn(usize) + Send + Sync + 'static,
{
    let len = range.end.saturating_sub(range.start);
    let leaf = (len / (pool.workers() * PIECES_PER_WORKER)).max(1);
    let cancel = Cancel::new();
    ParFor {
        cancel: cancel.clone(),
        state: Some((
            Arc::new(Shared {
                body,
                // One outstanding piece: the whole range, not yet handed over.
                outstanding: AtomicUsize::new(1),
                finished: AtomicBool::new(false),
                waiter: Mutex::new(None),
                #[cfg(feature = "std")]
                panic: Mutex::new(None),
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
    fn work(self: Arc<Self>, pool: &Arc<Pool>, range: Range<usize>, leaf: usize, cancel: &Cancel);
    /// One piece finished. Completes the loop if it was the last.
    fn retire(&self);
    fn is_finished(&self) -> bool;
    fn park(&self, waker: &Waker);
    #[cfg(feature = "std")]
    fn record_panic(&self, payload: Box<dyn core::any::Any + Send>);
    #[cfg(feature = "std")]
    fn resume_panic(&self);
}

struct Shared<F> {
    body: F,
    /// Pieces handed to the pool and not yet finished, plus one for the root
    /// until it has been handed over. Splitting adds one before it subtracts
    /// one, so this never reaches zero while there is work left to create.
    outstanding: AtomicUsize,
    finished: AtomicBool,
    waiter: Mutex<Option<Waker>>,
    #[cfg(feature = "std")]
    panic: Mutex<Option<Box<dyn core::any::Any + Send>>>,
}

impl<F> Split for Shared<F>
where
    F: Fn(usize) + Send + Sync + 'static,
{
    fn work(self: Arc<Self>, pool: &Arc<Pool>, range: Range<usize>, leaf: usize, cancel: &Cancel) {
        // Checked once per piece, not once per item. A piece is the unit of
        // cancellation because it is the only boundary the runtime controls:
        // between two items the body is running and nothing here can interrupt
        // it.
        if cancel.is_cancelled() {
            return;
        }
        let mut range = range;
        // Split iteratively rather than recursively, giving the right half to
        // the pool and keeping the left. Recursion here would put the whole
        // split depth on one worker's stack, which is a stack overflow waiting
        // for a large enough range.
        while range.end.saturating_sub(range.start) > leaf {
            let middle = range.start + (range.end - range.start) / 2;
            let right = middle..range.end;
            range = range.start..middle;

            // Count the new piece *before* publishing it. The other order lets
            // the piece finish and retire against a count that has not been
            // raised yet, which would complete the loop early.
            self.outstanding.fetch_add(1, Ordering::AcqRel);
            let piece = Piece {
                shared: self.clone(),
                pool: Arc::downgrade(pool),
                range: right,
                leaf,
                cancel: cancel.clone(),
            };
            pool.submit_fn(move || piece.run());
        }
        for index in range {
            (self.body)(index);
        }
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
        let old = self.waiter.lock().replace(waker.clone());
        drop(old);
    }

    #[cfg(feature = "std")]
    fn record_panic(&self, payload: Box<dyn core::any::Any + Send>) {
        let mut pending = self.panic.lock();
        if pending.is_none() {
            *pending = Some(payload);
        } else {
            drop(pending);
            drop(payload);
        }
    }

    #[cfg(feature = "std")]
    fn resume_panic(&self) {
        let payload = self.panic.lock().take();
        if let Some(payload) = payload {
            std::panic::resume_unwind(payload);
        }
    }
}

// One counted piece, including while it is queued. Dropping an unrun closure
// must retire it too; keeping retirement only at the end of work() loses that
// accounting on panic, cancellation, and pool destruction.
struct Piece {
    shared: Arc<dyn Split>,
    pool: Weak<Pool>,
    range: Range<usize>,
    leaf: usize,
    cancel: Cancel,
}

impl Piece {
    fn run(self) {
        let Some(pool) = self.pool.upgrade() else {
            self.cancel.cancel();
            return;
        };
        #[cfg(feature = "std")]
        {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.shared
                    .clone()
                    .work(&pool, self.range.clone(), self.leaf, &self.cancel);
            }));
            if let Err(payload) = result {
                self.cancel.cancel();
                self.shared.record_panic(payload);
            }
        }
        #[cfg(not(feature = "std"))]
        self.shared
            .clone()
            .work(&pool, self.range.clone(), self.leaf, &self.cancel);
    }
}

impl Drop for Piece {
    fn drop(&mut self) {
        self.shared.retire();
    }
}

/// Everything a loop needs to start: the shared half, the pool to put pieces
/// on, the range still to cover, and how small a piece may get.
type Pending = (Arc<dyn Split>, Arc<Pool>, Range<usize>, usize);

/// A parallel loop that has not finished yet.
///
/// Created by [`par_for`]. Poll it to start the work and again to learn
/// that it is done.
pub struct ParFor {
    cancel: Cancel,
    /// Everything needed to start, taken on the first poll.
    state: Option<Pending>,
    started: Option<Arc<dyn Split>>,
}

impl ParFor {
    /// The switch that stops this loop.
    ///
    /// Clone it into the body to stop early on a result, hold it elsewhere to
    /// stop on a timeout, or ignore it and let [`Drop`] do the work.
    #[must_use]
    pub fn cancel(&self) -> Cancel {
        self.cancel.clone()
    }

    /// Use `cancel` as this loop's switch instead of its own.
    ///
    /// For a body that has to stop the loop it is inside: the token has to
    /// exist before the closure is built, so the loop cannot be the thing that
    /// creates it.
    ///
    /// Ignored once the loop has been polled.
    #[must_use]
    pub fn cancel_with(mut self, cancel: Cancel) -> Self {
        if self.state.is_some() {
            self.cancel = cancel;
        }
        self
    }

    /// How small a piece may get before it is run rather than split further.
    ///
    /// This is the loop's **cancellation granularity** as well as its
    /// scheduling granularity: a cancelled loop stops at the next piece
    /// boundary, so smaller pieces stop sooner and split more often. The
    /// default is the range divided by eight times the worker count.
    ///
    /// Ignored once the loop has been polled, because by then the work is out.
    #[must_use]
    pub fn leaf(mut self, items: usize) -> Self {
        if let Some((_, _, _, leaf)) = self.state.as_mut() {
            *leaf = items.max(1);
        }
        self
    }
}

/// **Dropping a `ParFor` cancels it.** That is what a future should do, and
/// what this did not do until it was written down: a `select!` that timed out
/// returned while the loop went on burning every worker it had.
///
/// Cancelling stops work that has not started. See [`Cancel`] for what that
/// does and does not bound.
impl Drop for ParFor {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Future for ParFor {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();

        if let Some((shared, pool, range, leaf)) = this.state.take() {
            this.started = Some(shared.clone());
            if range.is_empty() || this.cancel.is_cancelled() {
                shared.retire();
                return Poll::Ready(());
            }
            // Hand the whole range to the pool rather than splitting it here.
            // Splitting is itself work, and doing it on the pool means it is
            // parallel too: each piece splits its own half.
            let piece = Piece {
                shared,
                pool: Arc::downgrade(&pool),
                range,
                leaf,
                cancel: this.cancel.clone(),
            };
            pool.submit_fn(move || piece.run());
        }

        let Some(shared) = this.started.as_ref() else {
            // Polled after it completed and was taken. A future polled past
            // completion may do anything sane; saying "done" is the sane one.
            return Poll::Ready(());
        };

        if shared.is_finished() {
            #[cfg(feature = "std")]
            shared.resume_panic();
            return Poll::Ready(());
        }
        // Registered before the flag is read again, so a piece that finishes
        // between the two still finds a waker to call.
        shared.park(context.waker());
        if shared.is_finished() {
            #[cfg(feature = "std")]
            shared.resume_panic();
            return Poll::Ready(());
        }
        Poll::Pending
    }
}
