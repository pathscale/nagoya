//! Sleeping and timing out, without asking the pool for either.
//!
//! # Why this is not a `Host` method
//!
//! The obvious design is to let a worker park with a deadline, so an idle pool
//! wakes exactly when the next timer is due. That needs [`Host`] to grow a
//! timed park, and it cannot: `StdHost` parks on `atomic_wait::wait`, which has
//! no timeout, so the trait method would have nothing to call. Changing the
//! parking primitive to get one would trade a futex for a condition variable on
//! the path §7 of the WorkTable work just finished measuring, which is a real
//! cost paid by every pool whether it uses timers or not.
//!
//! So timers live beside the pool instead of inside it. A registry holds the
//! pending deadlines; something drives it and wakes what is due; waking goes
//! through the ordinary [`Waker`], which submits the task back through the
//! ordinary path. The pool learns nothing about time, and a program that never
//! sleeps pays nothing.
//!
//! # What drives it
//!
//! With `std`, a thread, started the first time anything sleeps. Never before:
//! a runtime that is only ever polled should not own a thread it does not use.
//!
//! Without `std` there is no thread and no clock, so the consumer supplies
//! both: [`set_clock`] once, then [`poll`] from a normal execution context.
//! A tick interrupt must only signal deferred work; it must not call `poll`,
//! register/drop timers, or invoke scheduler wakers. Those paths use locks and
//! allocation and are not interrupt-safe. The host's deferred loop processes
//! the signal and calls `poll(now_ns())` after returning from the interrupt.
//!
//! [`Host`]: st3::fanout::Host

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cmp::Ordering as CmpOrdering;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use spin::Mutex;

/// One pending timer, shared between the future and the registry.
///
/// The heap index is maintained under `PENDING`. Cancellation uses it to
/// remove an entry in O(log n), without scanning or retaining a tombstone.
#[derive(Debug)]
struct Slot {
    fired: AtomicBool,
    waker: Mutex<Option<Waker>>,
    index: AtomicUsize,
}

/// A deadline and what to wake at it.
struct Entry {
    deadline: u64,
    /// Breaks ties so equal deadlines fire in registration order rather than
    /// an arbitrary one, which keeps a paced loop from starving itself.
    seq: u64,
    slot: Arc<Slot>,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        (self.deadline, self.seq) == (other.deadline, other.seq)
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for Entry {
    /// Reversed, because the registry is a max-heap and the earliest deadline
    /// is the one wanted first.
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

const NOT_QUEUED: usize = usize::MAX;

// Indexed max-heap using Entry's reversed deadline ordering. Every move updates
// its slot index; index reads and writes are serialized by the registry lock.
#[derive(Default)]
struct TimerHeap {
    entries: Vec<Entry>,
}

impl TimerHeap {
    fn peek(&self) -> Option<&Entry> {
        self.entries.first()
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.entries.swap(a, b);
        self.entries[a].slot.index.store(a, Ordering::Relaxed);
        self.entries[b].slot.index.store(b, Ordering::Relaxed);
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if self.entries[index] <= self.entries[parent] {
                break;
            }
            self.swap(index, parent);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        // Checking for a child this way avoids overflowing 2 * index + 1.
        while index < self.entries.len() / 2 {
            let left = 2 * index + 1;
            let right = left + 1;
            let child = if right < self.entries.len() && self.entries[right] > self.entries[left] {
                right
            } else {
                left
            };
            if self.entries[index] >= self.entries[child] {
                break;
            }
            self.swap(index, child);
            index = child;
        }
    }

    fn push(&mut self, entry: Entry) {
        let index = self.entries.len();
        self.entries.push(entry);
        self.entries[index]
            .slot
            .index
            .store(index, Ordering::Relaxed);
        self.sift_up(index);
    }

    fn remove(&mut self, index: usize) -> Entry {
        let entry = self.entries.swap_remove(index);
        entry.slot.index.store(NOT_QUEUED, Ordering::Relaxed);
        if index < self.entries.len() {
            self.entries[index]
                .slot
                .index
                .store(index, Ordering::Relaxed);
            if index > 0 && self.entries[index] > self.entries[(index - 1) / 2] {
                self.sift_up(index);
            } else {
                self.sift_down(index);
            }
        }
        entry
    }

    fn pop(&mut self) -> Option<Entry> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.remove(0))
        }
    }
}

static PENDING: Mutex<Option<TimerHeap>> = Mutex::new(None);
static SEQ: AtomicU64 = AtomicU64::new(0);
static CLOCK: AtomicUsize = AtomicUsize::new(0);

/// Tell this module how to read a monotonic clock, in nanoseconds.
///
/// Required before any timer is used without `std`, where there is no clock to
/// default to. With `std`, the default is installed lazily on the first clock
/// read. A custom clock must be installed before then to take effect.
///
/// Only the first call takes effect, so a late caller cannot move time under a
/// timer that is already pending.
pub fn set_clock(clock: fn() -> u64) {
    let _ = CLOCK.compare_exchange(0, clock as usize, Ordering::Release, Ordering::Relaxed);
}

/// Now, in nanoseconds, on whatever clock this module was given.
///
/// # Panics
///
/// Without `std`, if [`set_clock`] has not been called. There is no monotonic
/// clock to fall back on and a timer that silently never fires is worse than a
/// panic that names the missing call.
#[must_use]
pub fn now_ns() -> u64 {
    // The install is behind the load, not in front of it. Reading the clock is
    // on the path of every `sleep` and every `timeout`, including the ones that
    // never arm, and installing unconditionally put a second `OnceLock` on that
    // path to answer a question already settled by the first call in the
    // process.
    #[cfg(feature = "std")]
    let raw = {
        let mut raw = CLOCK.load(Ordering::Acquire);
        if raw == 0 {
            driver::install_default_clock();
            raw = CLOCK.load(Ordering::Acquire);
        }
        raw
    };
    // Nothing to install: without `std` the consumer's `set_clock` is the only
    // way a clock ever arrives, so a zero here is the missing call, not a
    // default waiting to be taken.
    #[cfg(not(feature = "std"))]
    let raw = CLOCK.load(Ordering::Acquire);
    assert!(raw != 0, "nagoya::time::set_clock has not been called");
    // SAFETY: the only value ever stored is a `fn() -> u64` cast to `usize` by
    // `set_clock`, and the compare-exchange there means it is written once.
    let clock: fn() -> u64 = unsafe { core::mem::transmute::<usize, fn() -> u64>(raw) };
    clock()
}

/// Fire every timer due at `now`, and report when the next one is due.
///
/// The driver is a thread with `std`, or the host's deferred-work/idle loop
/// without it. This function takes ordinary spin locks, allocates, and invokes
/// arbitrary wakers. Do not call it from an interrupt, signal handler, or any
/// context that can preempt another timer operation on the same execution lane.
///
/// Returns `None` when nothing is pending, so a caller with nothing else to do
/// can sleep until something registers rather than spinning on an empty heap.
pub fn poll(now: u64) -> Option<u64> {
    // Wakers are collected and woken after the lock is released. A waker may
    // register another timer, and this lock is not reentrant.
    let mut due: Vec<Waker> = Vec::new();
    let next = {
        let mut guard = PENDING.lock();
        let heap = guard.as_mut()?;
        while let Some(entry) = heap.peek() {
            if entry.deadline > now {
                break;
            }
            let entry = heap.pop().expect("peeked");
            entry.slot.fired.store(true, Ordering::Release);
            // Bind before the entry drops. The guard borrows it, and the
            // scrutinee of an `if let` outlives the statement that produced it.
            let waker = entry.slot.waker.lock().take();
            if let Some(waker) = waker {
                due.push(waker);
            }
        }
        heap.peek().map(|entry| entry.deadline)
    };
    for waker in due {
        waker.wake();
    }
    next
}

/// Register `slot` to fire at `deadline`, and say whether it is now the
/// earliest pending timer.
fn arm(deadline: u64, slot: &Arc<Slot>) -> bool {
    let mut guard = PENDING.lock();
    let heap = guard.get_or_insert_with(TimerHeap::default);
    let earliest = heap.peek().is_none_or(|entry| deadline < entry.deadline);
    heap.push(Entry {
        deadline,
        seq: SEQ.fetch_add(1, Ordering::Relaxed),
        slot: Arc::clone(slot),
    });
    earliest
}

/// A future that is ready once its deadline has passed.
///
/// Dropping an armed sleep removes its heap entry in O(log n). Registration,
/// polling, and dropping must run outside interrupt/signal-handler contexts.
#[derive(Debug)]
pub struct Sleep {
    deadline: u64,
    /// `None` until this is first polled and found to still be in the future.
    ///
    /// **The allocation is deferred because most timers never need it.**
    /// `timeout` polls its inner future first and returns without touching the
    /// sleep whenever that future is ready, which in production is nearly every
    /// call. Allocating a slot in `sleep()` charged every one of those for a
    /// timer that was never armed: measured at 65 ns against tokio's 27 ns for
    /// the same arm-and-cancel loop.
    slot: Option<Arc<Slot>>,
}

impl Sleep {
    /// When this fires, on the clock [`now_ns`] reads.
    #[must_use]
    pub fn deadline(&self) -> u64 {
        self.deadline
    }
}

/// A future that is ready after `duration`.
///
/// The deadline is fixed here, at the call, not at the first poll, so a sleep
/// that sits unpolled in a `select!` arm does not quietly extend itself.
#[must_use]
pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(now_ns().saturating_add(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)))
}

/// A future that is ready once the clock reaches `deadline`.
#[must_use]
pub fn sleep_until(deadline: u64) -> Sleep {
    Sleep {
        deadline,
        slot: None,
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        match &this.slot {
            Some(slot) => {
                if slot.fired.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                // Re-registering matters: a task moved between wakes carries a
                // different waker, and the one held here would wake nothing.
                *slot.waker.lock() = Some(cx.waker().clone());
                // Checked again after registering, because the driver may have
                // fired between the load above and the store. Without this the
                // waker is consumed by a firing that already happened and the
                // sleep never returns.
                if slot.fired.load(Ordering::Acquire) {
                    return Poll::Ready(());
                }
                Poll::Pending
            }
            // **The deadline is checked before anything is allocated or
            // locked.** A sleep whose time has already passed is by far the
            // common case for a batch of timers awaited in order, and arming it
            // costs a full round trip through the driver to learn what the
            // clock could have said here: measured at 5495 us of overshoot on a
            // 10 ms deadline against tokio's 3844.
            None => {
                if now_ns() >= this.deadline {
                    return Poll::Ready(());
                }
                let slot = Arc::new(Slot {
                    fired: AtomicBool::new(false),
                    waker: Mutex::new(Some(cx.waker().clone())),
                    index: AtomicUsize::new(NOT_QUEUED),
                });
                let earliest = arm(this.deadline, &slot);
                this.slot = Some(slot);
                if earliest {
                    #[cfg(feature = "std")]
                    driver::nudge();
                }
                Poll::Pending
            }
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(slot) = &self.slot {
            let removed = {
                let mut guard = PENDING.lock();
                let index = slot.index.load(Ordering::Relaxed);
                if index == NOT_QUEUED {
                    None
                } else {
                    Some(
                        guard
                            .as_mut()
                            .expect("queued timer has a registry")
                            .remove(index),
                    )
                }
            };
            // Release heap ownership and the captured task outside PENDING.
            drop(removed);
            let waker = slot.waker.lock().take();
            drop(waker);
        }
    }
}

/// What [`timeout`] returns when the future did not finish in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl core::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("the future did not complete within its timeout")
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Elapsed {}

/// Run `future`, giving up after `duration`.
///
/// The inner future is polled first on every wake, so one that is ready at the
/// same moment its deadline passes completes rather than timing out. A timeout
/// is a failure to make progress, and a future that has made it should not be
/// punished for the order two wakes happened to arrive in.
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout {
        future,
        sleep: sleep(duration),
    }
}

/// The future [`timeout`] returns.
#[derive(Debug)]
pub struct Timeout<F> {
    future: F,
    sleep: Sleep,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: neither field is moved out, and the projections below are the
        // only access to them, so both stay pinned for as long as this is.
        let this = unsafe { self.get_unchecked_mut() };
        let future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(out) = future.poll(cx) {
            return Poll::Ready(Ok(out));
        }
        let sleep = unsafe { Pin::new_unchecked(&mut this.sleep) };
        match sleep.poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "std")]
mod driver {
    use super::{now_ns, poll, set_clock};
    use std::sync::{Condvar, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    static NUDGE: OnceLock<(Mutex<bool>, Condvar)> = OnceLock::new();
    static THREAD: OnceLock<()> = OnceLock::new();

    fn std_clock() -> u64 {
        let origin = ORIGIN.get_or_init(Instant::now);
        u64::try_from(origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// Point the registry at `Instant` unless a consumer already chose a clock.
    pub(super) fn install_default_clock() {
        let _ = ORIGIN.get_or_init(Instant::now);
        set_clock(std_clock);
    }

    fn nudge_pair() -> &'static (Mutex<bool>, Condvar) {
        NUDGE.get_or_init(|| (Mutex::new(false), Condvar::new()))
    }

    /// Wake the driver because a nearer deadline than it is waiting for now
    /// exists. Starts the driver if this is the first timer in the process.
    pub(super) fn nudge() {
        start();
        let (lock, signal) = nudge_pair();
        *lock.lock().expect("the nudge lock") = true;
        signal.notify_one();
    }

    fn start() {
        THREAD.get_or_init(|| {
            install_default_clock();
            std::thread::Builder::new()
                .name("nagoya-timer".into())
                .spawn(run)
                .expect("a timer thread");
        });
    }

    fn run() {
        let (lock, signal) = nudge_pair();
        loop {
            let next = poll(now_ns());
            let mut nudged = lock.lock().expect("the nudge lock");
            if *nudged {
                // A timer was armed while the heap above was being drained, so
                // `next` may already be stale. Go round rather than wait on it.
                *nudged = false;
                continue;
            }
            match next {
                Some(deadline) => {
                    let now = now_ns();
                    if deadline <= now {
                        continue;
                    }
                    let (guard, _) = signal
                        .wait_timeout(nudged, Duration::from_nanos(deadline - now))
                        .expect("the nudge lock");
                    nudged = guard;
                }
                // Nothing pending. Waiting rather than looping is what keeps an
                // idle timer thread off the CPU entirely.
                None => nudged = signal.wait(nudged).expect("the nudge lock"),
            }
            *nudged = false;
        }
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::{now_ns, sleep, timeout, Elapsed};
    use crate::block_on;
    use core::time::Duration;
    use std::time::Instant;

    fn slot() -> alloc::sync::Arc<super::Slot> {
        alloc::sync::Arc::new(super::Slot {
            fired: core::sync::atomic::AtomicBool::new(false),
            waker: spin::Mutex::new(None),
            index: core::sync::atomic::AtomicUsize::new(super::NOT_QUEUED),
        })
    }

    #[test]
    fn indexed_heap_removal_preserves_order_and_indices() {
        use core::sync::atomic::Ordering;
        let mut heap = super::TimerHeap::default();
        let mut slots = alloc::vec::Vec::new();
        for (seq, deadline) in [90, 10, 50, 10, 80, 20, 60, 30, 70, 40]
            .into_iter()
            .enumerate()
        {
            let slot = slot();
            heap.push(super::Entry {
                deadline,
                seq: seq as u64,
                slot: slot.clone(),
            });
            slots.push(slot);
        }
        for id in [2, 0, 7] {
            let index = slots[id].index.load(Ordering::Relaxed);
            let removed = heap.remove(index);
            assert!(alloc::sync::Arc::ptr_eq(&removed.slot, &slots[id]));
            assert_eq!(slots[id].index.load(Ordering::Relaxed), super::NOT_QUEUED);
            for (index, entry) in heap.entries.iter().enumerate() {
                assert_eq!(entry.slot.index.load(Ordering::Relaxed), index);
            }
        }
        let mut order = alloc::vec::Vec::new();
        while let Some(entry) = heap.pop() {
            assert_eq!(entry.slot.index.load(Ordering::Relaxed), super::NOT_QUEUED);
            order.push((entry.deadline, entry.seq));
        }
        assert_eq!(
            order,
            [
                (10, 1),
                (10, 3),
                (20, 5),
                (40, 9),
                (60, 6),
                (70, 8),
                (80, 4)
            ]
        );
    }

    #[test]
    fn dropping_an_armed_sleep_releases_registry_ownership_immediately() {
        use core::future::Future;
        use core::pin::Pin;
        use core::task::Context;
        use std::sync::Arc;
        use std::task::{Wake, Waker};
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }
        let waker = Waker::from(Arc::new(Noop));
        let mut sleep = super::sleep_until(u64::MAX);
        assert!(Pin::new(&mut sleep)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        let weak = Arc::downgrade(sleep.slot.as_ref().unwrap());
        drop(sleep);
        assert!(
            weak.upgrade().is_none(),
            "canceled timer is still retained by the registry"
        );
    }

    #[test]
    fn a_sleep_waits_at_least_its_duration() {
        let started = Instant::now();
        block_on(sleep(Duration::from_millis(50)));
        // Only the lower bound is asserted. The upper one is the scheduler's
        // and the machine's, and a test that fails when the box is busy is a
        // test that gets ignored.
        assert!(started.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn sleeps_fire_in_deadline_order_not_registration_order() {
        let order = block_on(async {
            let mut out = alloc::vec::Vec::new();
            let far = sleep(Duration::from_millis(60));
            let near = sleep(Duration::from_millis(10));
            near.await;
            out.push("near");
            far.await;
            out.push("far");
            out
        });
        assert_eq!(order, ["near", "far"]);
    }

    #[test]
    fn a_timeout_lets_a_finished_future_through() {
        let out = block_on(timeout(Duration::from_secs(30), async { 7 }));
        assert_eq!(out, Ok(7));
    }

    #[test]
    fn a_timeout_gives_up_on_one_that_does_not_finish() {
        let out = block_on(timeout(
            Duration::from_millis(20),
            sleep(Duration::from_secs(30)),
        ));
        assert_eq!(out, Err(Elapsed));
    }

    #[test]
    fn a_future_ready_at_its_deadline_wins() {
        // The inner future is polled first on every wake, so this completes
        // rather than racing its own timer. Same duration on both sides.
        let out = block_on(timeout(
            Duration::from_millis(20),
            sleep(Duration::from_millis(20)),
        ));
        assert_eq!(out, Ok(()));
    }

    #[test]
    fn dropping_a_sleep_disarms_it() {
        // A cancelled timer must not wake anything. Arming and dropping many
        // of them, then sleeping past all their deadlines, would deadlock or
        // panic in `poll` if a dead slot were still woken.
        block_on(async {
            for _ in 0..1_000 {
                let mut pending = sleep(Duration::from_millis(5));
                let _ = timeout(Duration::from_nanos(1), &mut pending).await;
            }
            sleep(Duration::from_millis(20)).await;
        });
    }

    #[test]
    fn the_clock_only_goes_forwards() {
        let first = now_ns();
        let second = now_ns();
        assert!(second >= first);
    }
}
