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
use core::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
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
    /// Which thread is currently inside the wait, if any.
    ///
    /// Zero means nobody. Otherwise it is the id of the thread blocked in
    /// `poll_once`, and a wake raised *by that same thread* needs no syscall:
    /// it is `dispatch` waking a task while the poller has already returned,
    /// and the loop is about to poll regardless.
    ///
    /// That case is the common one under `block_on`, not the rare one. Every
    /// readable event dispatches a wake on this thread, so the unconditional
    /// `handle.wake()` was a `kevent` per ready connection per batch, issued
    /// to interrupt a wait that was no longer running.
    waiting: AtomicU64,
}

/// This thread's id as a non-zero `u64`, for comparing against `waiting`.
///
/// `ThreadId::as_u64` is unstable, so the address of a thread local is used
/// instead: it is unique per thread, stable for the thread's life, and never
/// zero, which is what the sentinel needs.
fn thread_key() -> u64 {
    thread_local! {
        static KEY: u8 = const { 0 };
    }
    KEY.with(|key| core::ptr::from_ref(key) as u64)
}

impl Wake for LocalWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.store(true, Ordering::Release);
        // Raised from inside this thread's own wait: the poller has already
        // returned, the loop will poll next, and interrupting a `kevent` that
        // is not running would be a syscall for nothing.
        if self.waiting.load(Ordering::Acquire) == thread_key() {
            return;
        }
        // From another thread, or from outside the wait entirely: the loop may
        // be blocked in `kevent` with no event coming, and this is what gets
        // it out.
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
        waiting: AtomicU64::new(0),
    });
    let key = thread_key();
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
        //
        // Claimed across the whole call rather than only the blocking part,
        // because `poll_once` dispatches the wakes it collected before it
        // returns, and those are exactly the ones that must not pay a syscall
        // to interrupt a wait this thread has already left.
        waker.waiting.store(key, Ordering::Release);
        let outcome = reactor.poll_once();
        waker.waiting.store(0, Ordering::Release);
        if outcome.is_err() {
            // The poller failed. Polling again would spin, so give the future
            // one more chance to finish on what it already has and then stop.
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            core::hint::spin_loop();
        }
    }
}

/// A set of futures driven concurrently on one thread.
///
/// # Why this exists
///
/// [`block_on_with`] drives one future. A server with many connections has one
/// future per connection, and with nowhere to put them the caller writes the
/// combination by hand.
///
/// # What it costs to be idle
///
/// The version people write first polls every future on every pass, which is
/// quadratic in the wrong place: one readable socket costs a `recv` on every
/// *other* connection, each returning `EWOULDBLOCK`. Keeping a readiness flag
/// per future and checking them all is better but still walks the whole set
/// to find the one that moved.
///
/// Neither happens here. A waker carries its own index and pushes it onto the
/// set's ready queue, so waking is a push and polling drains what was pushed.
/// Nothing is scanned, and a connection that no data arrived for is not
/// touched at all: ten thousand idle tasks cost nothing while one is busy.
///
/// # What it costs to exist
///
/// One allocation per task, shared by its waker. The set holds a single
/// [`Arc`] of state that every waker points at, so the thing driving the set
/// is stored once rather than cloned into every task.
///
/// # Not a spawner
///
/// There is no global to spawn into and no handle to await. The set is a
/// future, it is owned by whoever created it, and it is ready once everything
/// in it has finished. More may be added whenever the set is not being polled,
/// including after it has drained, and dropping it drops them.
///
/// # Example
///
/// ```no_run
/// use nagoya::reactor::{block_on_with, Reactor, TaskSet};
///
/// let reactor = Reactor::local()?;
/// let mut tasks = TaskSet::new();
/// for _ in 0..8 {
///     tasks.push(async { /* one connection */ });
/// }
/// block_on_with(&reactor, tasks);
/// # Ok::<_, nagoya::reactor::Errno>(())
/// ```
#[derive(Default)]
pub struct TaskSet {
    /// One entry per task, kept for the life of the set.
    tasks: alloc::vec::Vec<Task>,
    /// State every waker shares, allocated on the first push.
    shared: Option<Arc<Shared>>,
    /// Slots still holding a future, so readiness is not a count.
    left: usize,
}

/// A future and the waker that puts it back on the chain.
///
/// The future is `None` once it has finished, but the entry itself is never
/// removed. Something else may still hold a clone of this task's waker - a
/// timer, a channel, another thread - and wake it after it is gone. That wake
/// puts the index back on the chain, and the walk has to be able to read the
/// link out of it and carry on to whatever is behind it.
struct Task {
    future: Option<core::pin::Pin<alloc::boxed::Box<dyn Future<Output = ()>>>>,
    waker: Waker,
    /// Held here rather than read through the waker so that walking the chain
    /// does not reach back into the shared state for every task.
    slot: Arc<Slot>,
}

/// The end of the ready chain.
const END: u32 = u32::MAX;

/// What every waker in one set points at.
///
/// Nothing here takes a lock. A wake happens on whatever thread finished the
/// I/O, often while the reactor is midway through dispatching a batch of them,
/// and a lock there would serialise exactly the part that should not be.
struct Shared {
    /// The most recently woken task, or [`END`].
    ///
    /// The rest of the chain is reached through the `next` field of each slot,
    /// so the queue costs no allocation and lives in the tasks themselves. It
    /// is indices rather than pointers, which is what keeps it safe code: a
    /// drained index is looked up in the set's own vector.
    head: AtomicU32,
    /// Whoever is driving the set. Stored once, not cloned into every task,
    /// and swapped without a lock.
    parent: futures_util::task::AtomicWaker,
}

/// One task's place in the set: its index, its link, and whether it is queued.
struct Slot {
    shared: Arc<Shared>,
    index: u32,
    /// The next woken task after this one, while this one is on the chain.
    next: AtomicU32,
    /// Set while the index is on the chain. A second wake before the task is
    /// polled is then free rather than a duplicate entry, which is what keeps
    /// a chatty socket from queueing itself once per event, and it is what
    /// makes the push below the only writer of `next` at that moment.
    queued: AtomicBool,
}

impl Wake for Slot {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.queued.swap(true, Ordering::AcqRel) {
            // Already waiting to be polled. The poll that is coming will see
            // whatever this wake was about.
            return;
        }
        // Won the right to queue this slot, so nothing else is writing `next`.
        let mut head = self.shared.head.load(Ordering::Relaxed);
        loop {
            self.next.store(head, Ordering::Relaxed);
            match self.shared.head.compare_exchange_weak(
                head,
                self.index,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => head = current,
            }
        }
        self.shared.parent.wake();
    }
}

impl TaskSet {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a future, which is polled once the set next is.
    pub fn push<F: Future<Output = ()> + 'static>(&mut self, future: F) {
        let shared = self
            .shared
            .get_or_insert_with(|| {
                Arc::new(Shared {
                    head: AtomicU32::new(END),
                    parent: futures_util::task::AtomicWaker::new(),
                })
            })
            .clone();
        let index = u32::try_from(self.tasks.len()).expect("a task set of under 4 billion tasks");
        let slot = Arc::new(Slot {
            shared: Arc::clone(&shared),
            index,
            next: AtomicU32::new(END),
            // A new task has never been polled, so it is owed one and goes
            // straight onto the chain rather than waiting to be discovered.
            queued: AtomicBool::new(true),
        });
        let mut head = shared.head.load(Ordering::Relaxed);
        loop {
            slot.next.store(head, Ordering::Relaxed);
            match shared.head.compare_exchange_weak(
                head,
                index,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => head = current,
            }
        }
        self.tasks.push(Task {
            future: Some(alloc::boxed::Box::pin(future)),
            waker: Waker::from(Arc::clone(&slot)),
            slot,
        });
        self.left += 1;
    }

    /// How many futures have not finished.
    #[must_use]
    pub fn len(&self) -> usize {
        self.left
    }

    /// Whether every future has finished, which includes never having any.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.left == 0
    }
}

impl Future for TaskSet {
    type Output = ();

    fn poll(self: core::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let Some(shared) = this.shared.clone() else {
            // Never had a task, so there is nothing to wait for.
            return Poll::Ready(());
        };

        // Recorded before draining, so a wake raised by one of the polls below
        // reaches whoever is driving the set now rather than whoever drove it
        // last time.
        shared.parent.register(context.waker());

        // The whole chain is taken at once rather than walked in place: a task
        // being polled may wake itself, and letting it push onto a chain that
        // is still being walked would spin forever on a busy socket. Anything
        // woken during this pass lands on the now empty chain and is picked up
        // on the next one.
        let mut cursor = shared.head.swap(END, Ordering::AcqRel);
        while cursor != END {
            let Some(task) = this.tasks.get_mut(cursor as usize) else {
                // Not an index this set ever handed out, so there is no link
                // to follow and nothing further can be recovered.
                break;
            };
            // Read before the poll: the task owns `next` again the moment it
            // is unqueued, and may overwrite it from inside its own wake.
            let following = task.slot.next.load(Ordering::Relaxed);
            // Cleared before the poll, not after: a wake that happens during
            // the poll has to be able to queue the task again.
            task.slot.queued.store(false, Ordering::Release);
            if let Some(future) = task.future.as_mut() {
                let mut context = Context::from_waker(&task.waker);
                if future.as_mut().poll(&mut context).is_ready() {
                    // Dropped here, which releases whatever the future held.
                    // The entry stays so that a late wake still has a link.
                    task.future = None;
                    this.left -= 1;
                }
            }
            cursor = following;
        }

        if this.left == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
