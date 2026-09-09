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
//! both: [`set_clock`] once, then [`poll`] from a tick interrupt or its own
//! loop. That is the whole no_std contract, and it is why the registry and the
//! driver are separate things in this file.
//!
//! [`Host`]: st3::fanout::Host

use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::collections::BinaryHeap;
use core::cmp::Ordering as CmpOrdering;
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use spin::Mutex;

/// One pending timer, shared between the future and the registry.
///
/// The future owns one end and the heap the other, so a cancelled timer can be
/// disarmed without finding and removing its heap entry: the entry survives to
/// its deadline and fires nothing.
#[derive(Debug)]
struct Slot {
    fired: AtomicBool,
    waker: Mutex<Option<Waker>>,
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
    /// Reversed, because `BinaryHeap` is a max-heap and the earliest deadline
    /// is the one wanted first.
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

static PENDING: Mutex<Option<BinaryHeap<Entry>>> = Mutex::new(None);
static SEQ: AtomicU64 = AtomicU64::new(0);
static CLOCK: AtomicUsize = AtomicUsize::new(0);

/// Tell this module how to read a monotonic clock, in nanoseconds.
///
/// Required before any timer is used without `std`, where there is no clock to
/// default to. With `std` this is already set and calling it replaces the
/// default, which is worth doing only to share an origin with something else.
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
    #[cfg(feature = "std")]
    driver::install_default_clock();
    let raw = CLOCK.load(Ordering::Acquire);
    assert!(raw != 0, "nagoya::time::set_clock has not been called");
    // SAFETY: the only value ever stored is a `fn() -> u64` cast to `usize` by
    // `set_clock`, and the compare-exchange there means it is written once.
    let clock: fn() -> u64 = unsafe { core::mem::transmute::<usize, fn() -> u64>(raw) };
    clock()
}

/// Fire every timer due at `now`, and report when the next one is due.
///
/// The driver for this is a thread with `std`. Without one, call it from a tick
/// interrupt or an idle loop; nothing else pumps the registry, and a timer that
/// is never polled never fires.
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
    let heap = guard.get_or_insert_with(BinaryHeap::new);
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
/// Dropping one disarms it. The heap entry survives until its deadline, holding
/// nothing and waking nothing, so a cancelled `timeout` costs one dead entry
/// for as long as it had left to run rather than forever.
#[derive(Debug)]
pub struct Sleep {
    deadline: u64,
    slot: Arc<Slot>,
    armed: bool,
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
        slot: Arc::new(Slot {
            fired: AtomicBool::new(false),
            waker: Mutex::new(None),
        }),
        armed: false,
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.slot.fired.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        // Registering the waker before the deadline check closes the race where
        // the driver fires between the two: it would find a waker and wake it,
        // and this poll returns pending to a task that is already scheduled.
        *this.slot.waker.lock() = Some(cx.waker().clone());
        if this.slot.fired.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        if !this.armed {
            this.armed = true;
            if arm(this.deadline, &this.slot) {
                #[cfg(feature = "std")]
                driver::nudge();
            }
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        // Disarm rather than unregister. Finding this entry in the heap costs a
        // linear scan under the lock; leaving it costs one wake of nothing.
        *self.slot.waker.lock() = None;
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
        let out = block_on(timeout(Duration::from_millis(20), sleep(Duration::from_secs(30))));
        assert_eq!(out, Err(Elapsed));
    }

    #[test]
    fn a_future_ready_at_its_deadline_wins() {
        // The inner future is polled first on every wake, so this completes
        // rather than racing its own timer. Same duration on both sides.
        let out = block_on(timeout(Duration::from_millis(20), sleep(Duration::from_millis(20))));
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
