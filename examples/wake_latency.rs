//! How long one task waits when the pool has nothing else to do.
//!
//! Throughput and latency pull in opposite directions here. A worker that finds
//! no work backs off before looking again, and backing off longer is worth a
//! great deal of throughput because idle workers stop hammering the queue the
//! producer is trying to fill. What it costs is exactly this: a task that
//! arrives during a backoff waits for it to end.
//!
//! A parked worker is woken by `submit` directly and does not pay this. A
//! *spinning* worker is not in the sleeper bitmap, so nothing wakes it and it
//! finds the task on its next look. This measures that case, which is the one
//! that gets worse as the backoff grows.
//!
//! One task at a time, submitted to an idle pool, timed to completion.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Instant;

use st3::fanout::{Pool, StdHost, Tuning};

const SAMPLES: usize = 2_000;

fn tuning() -> Tuning {
    // Built from a preset rather than as a literal: `Tuning` is
    // `#[non_exhaustive]` from ps-st3 0.6, so a field it gains later is not a
    // breaking change and this example does not have to be edited again.
    Tuning::locality()
        .with_rounds_before_park(
            std::env::var("ROUNDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64),
        )
        .with_backoff_spins(
            std::env::var("BACKOFF")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1024),
        )
        .with_promote_every(64)
        .with_injector_batch(32)
}

fn workers() -> usize {
    std::env::var("W")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}

struct Done {
    lock: Mutex<bool>,
    signal: Condvar,
}

fn main() {
    let host = Arc::new(StdHost::new(workers()));
    let pool = Pool::with_tuning(workers(), 1024, host, tuning());
    let threads: Vec<_> = (0..workers())
        .map(|id| {
            let pool = pool.clone();
            let runner = pool.runner(id);
            thread::spawn(move || {
                let _ = pool.run(runner);
            })
        })
        .collect();

    // Let the pool settle into whatever idle state it settles into, so the
    // first sample is not measuring start-up.
    thread::sleep(std::time::Duration::from_millis(50));

    let mut samples = Vec::with_capacity(SAMPLES);
    let elapsed = Arc::new(AtomicU64::new(0));
    for _ in 0..SAMPLES {
        let done = Arc::new(Done {
            lock: Mutex::new(false),
            signal: Condvar::new(),
        });
        let signal = done.clone();
        let out = elapsed.clone();
        let start = Instant::now();
        pool.submit_fn(move || {
            out.store(
                u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                Ordering::Release,
            );
            *signal.lock.lock().expect("the lock") = true;
            signal.signal.notify_one();
        });
        let mut finished = done.lock.lock().expect("the lock");
        while !*finished {
            finished = done.signal.wait(finished).expect("the wait");
        }
        samples.push(elapsed.load(Ordering::Acquire));
        // Long enough that the workers are back to spinning or parked, which is
        // the state this is about.
        thread::sleep(std::time::Duration::from_micros(200));
    }

    pool.shut_down();
    for thread in threads {
        let _ = thread.join();
    }

    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() as f64 - 1.0) * q) as usize];
    println!(
        "{:>7} ns median   {:>8} ns p90   {:>9} ns p99   {:>9} ns max   ({SAMPLES} samples, {} workers)",
        at(0.5),
        at(0.9),
        at(0.99),
        samples[samples.len() - 1],
        workers()
    );
}
