//! Where a wakeup's time goes, for the part that is not the kernel.
//!
//! # Read the zeroing number with the correction below
//!
//! This reports that zeroing the 1024 entry event buffer costs 219 to 249ns a
//! wait, and that was used to argue for `MaybeUninit` in the poller. **That
//! conclusion was wrong and the change was reverted.** End to end on a quiet
//! machine it cost about 2.4us at p50, with the blocking floor unchanged.
//!
//! The measurement is right and the inference was not. Here the array stays hot
//! in cache across iterations, so `memset` is the whole cost. In the reactor the
//! wait blocks in the kernel in between, the stack goes cold, and that `memset`
//! was pre-touching pages the kernel is about to write. Removing it did not
//! delete the cost, it moved it inside `kevent` and turned sequential writes
//! into faults.
//!
//! The lesson is kept rather than the file deleted: a tight loop cannot price
//! anything whose real cost is a cold cache or a page fault.
//!
//! # What still stands
//!
//! Everything nagoya does outside the kernel per wait is about 35ns: the clock
//! read 21, the scratch handoff 9, the slots lookup 7. That is the budget any
//! proposed reactor micro-optimisation should be checked against, and it is why
//! the p90 gap to tokio is not in the bookkeeping.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const ROUNDS: usize = 500_000;

fn main() {
    event_buffer_cost();
    println!("per call, {ROUNDS} iterations:");

    // The clock read `service_timers` makes on every single wait, whether or
    // not any timer is armed.
    let start = Instant::now();
    for _ in 0..ROUNDS {
        std::hint::black_box(nagoya::now_ns());
    }
    println!(
        "  now_ns (every wait)        {:>7.1} ns",
        start.elapsed().as_nanos() as f64 / ROUNDS as f64
    );

    // An uncontended atomic load, the cheap half of service_timers.
    static DEADLINE: AtomicU64 = AtomicU64::new(u64::MAX);
    let start = Instant::now();
    for _ in 0..ROUNDS {
        std::hint::black_box(DEADLINE.load(Ordering::Acquire));
    }
    println!(
        "  atomic load                {:>7.1} ns",
        start.elapsed().as_nanos() as f64 / ROUNDS as f64
    );

    // The scratch buffer handoff: two uncontended mutex round trips per wait.
    let scratch =
        std::sync::Mutex::new((Vec::<u8>::with_capacity(64), Vec::<u8>::with_capacity(64)));
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let taken = {
            let mut guard = scratch.lock().expect("scratch");
            (core::mem::take(&mut guard.0), core::mem::take(&mut guard.1))
        };
        {
            let mut guard = scratch.lock().expect("scratch");
            guard.0 = taken.0;
            guard.1 = taken.1;
        }
    }
    println!(
        "  scratch take + put back    {:>7.1} ns",
        start.elapsed().as_nanos() as f64 / ROUNDS as f64
    );

    // The slots lookup dispatch does for each event, on a map with one entry.
    let slots: std::sync::Mutex<std::collections::HashMap<u64, u64>> =
        std::sync::Mutex::new(std::collections::HashMap::from([(0u64, 1u64)]));
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let guard = slots.lock().expect("slots");
        std::hint::black_box(guard.get(&0).copied());
    }
    println!(
        "  slots lock + lookup        {:>7.1} ns",
        start.elapsed().as_nanos() as f64 / ROUNDS as f64
    );
}

/// The event buffer `kevent` is handed on every wait.
///
/// Zeroed on the stack each time. At 1024 entries that is 32 KiB of memset and
/// eight pages touched per wakeup, which is the kind of cost that shows up in a
/// tail rather than a median because it evicts whatever else was warm.
///
/// Kept even though the conclusion it supported was wrong. The numbers below
/// are real and the change built on them, skipping the zeroing with
/// `MaybeUninit`, measured 2.4us *slower* end to end and was reverted. That is
/// the point worth keeping: a tight loop cannot price a cold cache or a page
/// fault, so a microbenchmark is where an optimisation starts and never where
/// it is decided.
///
/// `kevent` is a BSD type, so this is the one thing here that cannot run
/// everywhere. Reported as absent rather than skipped silently, and above all
/// not left to fail the build on Linux, which is what it did.
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
fn event_buffer_cost() {
    const CAPACITY: usize = 1024;
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let events: [libc::kevent; CAPACITY] = unsafe { core::mem::zeroed() };
        std::hint::black_box(&events);
    }
    println!(
        "  zero 1024 kevents          {:>7.1} ns",
        start.elapsed().as_nanos() as f64 / ROUNDS as f64
    );

    const SMALL: usize = 64;
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let events: [libc::kevent; SMALL] = unsafe { core::mem::zeroed() };
        std::hint::black_box(&events);
    }
    println!(
        "  zero 64 kevents            {:>7.1} ns",
        start.elapsed().as_nanos() as f64 / ROUNDS as f64
    );
}

/// Where there is no `kevent` there is no event buffer to price.
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
fn event_buffer_cost() {
    println!("  zero kevents                   n/a (kqueue only)");
}
