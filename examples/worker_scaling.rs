//! Does this pool get slower as workers are added, with no I/O anywhere?
//!
//! # Why
//!
//! nago-wss measured its WebSocket echo two to three times faster on a pool of
//! two than on the default pool of one thread per core, at eight, thirty two
//! and five hundred and twelve connections alike. Nothing on the read path
//! explained it: one `recv` per message, no wasted wakeups, and the gap
//! survived removing the reactor sharding, the masking and the client harness.
//!
//! A pool that slows down as threads are added is contending on something. The
//! question this answers is whether that something is in the pool at all,
//! because everything measured so far had sockets in it. So this has none: the
//! tasks wake each other through channels and the only shared machinery left
//! is the executor and its injector.
//!
//! # What it runs
//!
//! `PAIRS` independent ping-pong pairs, each bouncing a turn back and forth
//! `ROUNDS` times through a pair of `Notify`. Every bounce is a wake and a
//! re-poll, which is the same shape a connection's read loop has and the
//! operation the injector is on the path of. If throughput falls as `workers`
//! rises here, the socket benchmarks were reporting the pool.
//!
//! ```text
//! cargo run --release --example worker_scaling
//! ```
//!
//! # What it found, and why the first reading of it was wrong
//!
//! On first run this looked like a clean reproduction of nago-wss's result:
//! peak at four workers, the default sixteen a third slower, and a `sample`
//! profile putting seventy per cent of a sixteen worker pool in `__ulock_wait`
//! against fourteen per cent at four. That is the table this comment used to
//! carry, and it does not survive repetition.
//!
//! Three runs of this probe at sixteen workers, unchanged code:
//!
//! ```text
//!   run 1   6.6M     run 2   9.4M     run 3  10.9M
//! ```
//!
//! A sixty five per cent spread, which is wider than every difference the
//! sweep is trying to measure. Run 2 has sixteen workers *beating* four. The
//! retry budget sweep below is worse: the default (4, 128) came first in one
//! run at 9.3M and (0, 0) came last at 8.0M, and on the next run (0, 0) came
//! first at 10.1M and the default dropped to 7.0M. Those are opposite
//! conclusions from the same binary.
//!
//! So this probe does not resolve what it was built to resolve, and the
//! profile above rested on a single `sample` of a quantity this variable.
//! Neither is evidence. What does reproduce is the measurement that started
//! this, in nago-wss, where four consecutive runs put two to four workers at
//! 286-343k messages a second and the default sixteen at 129-139k, monotonic
//! above four every time. The effect is real on that workload; this is simply
//! not the instrument that isolates it.
//!
//! Kept rather than deleted because the negative result is worth having: a
//! `Notify` ping-pong is not a usable proxy for the socket workload, and the
//! next attempt should either amortise far harder or measure the pool through
//! the thing that actually shows the effect.
//!
//! One thing the sweep did settle: the spin-before-park this was going to add
//! already exists. `Tuning::locality` is nagoya's default and it already
//! spins four rounds of 128 hints before parking, so there was no missing
//! backoff to write, and widening that budget over a 256x range moves nothing
//! outside noise.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nagoya::runtime::Runtime;
use nagoya::sync::Notify;
use st3::fanout::Tuning;

/// Concurrent pairs. Eight matches the connection count nago-wss found worst.
const PAIRS: usize = 8;
/// Bounces per pair.
const ROUNDS: usize = 2_000;
/// Samples per setting; the best is reported.
const SAMPLES: usize = 3;
/// Worker counts to sweep. Sixteen is `available_parallelism` on this machine.
const WORKERS: [usize; 6] = [1, 2, 4, 8, 12, 16];

/// The worker count the retry-budget sweep runs at: the default, which is the
/// setting the profile found spending seventy per cent of its time parked.
const DEFAULT_WORKERS: usize = 16;

/// Run one setting in a loop instead of sweeping, so a sampling profiler has
/// a steady target. `WORKER_SCALING_PIN=16` pins the worker count.
fn pinned() -> Option<usize> {
    std::env::var("WORKER_SCALING_PIN")
        .ok()
        .and_then(|value| value.parse().ok())
}

fn round(workers: usize) -> Duration {
    round_on(&Arc::new(Runtime::new(workers)))
}

/// Retry budgets to sweep: empty search rounds before a worker announces
/// sleep, and spin hints between those rounds. The default is four and 128.
const BUDGETS: [(u32, u32); 6] = [
    (0, 0),
    (4, 128),
    (8, 128),
    (16, 256),
    (64, 512),
    (256, 1024),
];

/// The same load at a fixed worker count, varying only the retry budget.
///
/// `Tuning::locality` already spins before parking and nagoya already uses it,
/// so the question the profile raises is not whether to add a backoff but
/// whether the default one is long enough. This answers that directly.
fn budget_round(workers: usize, rounds: u32, spins: u32) -> Duration {
    let tuning = Tuning::locality()
        .with_rounds_before_park(rounds)
        .with_backoff_spins(spins);
    round_on(&Arc::new(Runtime::with_tuning(workers, tuning, "sweep")))
}

/// One round on an existing runtime.
///
/// Separate from `round` because dropping a `Runtime` detaches its threads
/// rather than stopping them: a loop that builds one per iteration runs the
/// process out of threads, which is how the first attempt at profiling this
/// died rather than anything about the pool.
fn round_on(runtime: &Arc<Runtime>) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(PAIRS);
    for _ in 0..PAIRS {
        // Two notifies, so each side waits on the other: a bounce is a wake
        // and a re-poll, with no socket underneath it.
        let ping = Arc::new(Notify::new());
        let pong = Arc::new(Notify::new());

        let responder = {
            let ping = ping.clone();
            let pong = pong.clone();
            runtime.spawn(async move {
                for _ in 0..ROUNDS {
                    ping.notified().await;
                    pong.notify_one();
                }
            })
        };

        let driver = runtime.spawn(async move {
            for _ in 0..ROUNDS {
                // Arm the wait before waking the peer, so a notify that lands
                // first is still observed rather than missed.
                let mut waited = Box::pin(pong.notified());
                waited.as_mut().enable();
                ping.notify_one();
                waited.await;
            }
        });

        handles.push((driver, responder));
    }

    nagoya::block_on(async move {
        for (driver, responder) in handles {
            driver.await;
            responder.await;
        }
    });
    start.elapsed()
}

fn main() {
    if let Some(workers) = pinned() {
        // Long enough to sample, and reporting nothing: the profile is the
        // output here, not the number.
        let runtime = Arc::new(Runtime::new(workers));
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let _ = round_on(&runtime);
        }
        return;
    }

    let _ = round(4);

    println!("\n{PAIRS} ping-pong pairs, {ROUNDS} bounces each, best of {SAMPLES}\n");
    println!("  {:>8}  {:>16}", "workers", "bounces/s");

    let total = PAIRS * ROUNDS;
    for workers in WORKERS {
        let mut samples: Vec<Duration> = (0..SAMPLES).map(|_| round(workers)).collect();
        samples.sort_unstable();
        let rate = total as f64 / samples[0].as_secs_f64();
        println!("  {workers:>8}  {rate:>12.0} b/s");
    }

    println!("\nretry budget at {DEFAULT_WORKERS} workers, best of {SAMPLES}\n");
    println!("  {:>8}  {:>7}  {:>16}", "rounds", "spins", "bounces/s");
    for (rounds, spins) in BUDGETS {
        let mut samples: Vec<Duration> = (0..SAMPLES)
            .map(|_| budget_round(DEFAULT_WORKERS, rounds, spins))
            .collect();
        samples.sort_unstable();
        let rate = total as f64 / samples[0].as_secs_f64();
        println!("  {rounds:>8}  {spins:>7}  {rate:>12.0} b/s");
    }
    println!();
}
