//! Where a wakeup's time goes, for the part that is not the kernel.
//!
//! The UDS benchmark leaves nagoya about 900ns behind tokio's current_thread
//! runtime at p90 while matching it at the median, and the syscall counters say
//! the work is already minimal: one `recv` and one poller wait per round trip
//! per end. So the gap is per wakeup cost rather than extra work, and this
//! times the pieces of `poll_once` that are not the `kevent` itself.
//!
//! Timed with nothing else running, so these are floors rather than what the
//! benchmark sees under contention.

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
