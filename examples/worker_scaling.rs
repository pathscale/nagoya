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
//! # What it found
//!
//! Yes, and the pool is where it happens. Peak is four workers and the
//! default, one per core, is roughly a third slower than that:
//!
//! ```text
//!  workers       bounces/s
//!        1     4.7-6.3M
//!        2     6.0-8.9M
//!        4     9.4-11.0M
//!        8     7.6-8.6M
//!       12     6.7-7.8M
//!       16     6.6-6.8M
//! ```
//!
//! `WORKER_SCALING_PIN=<n>` holds one setting so a sampling profiler has a
//! steady target. `sample` on both, counting stacks under `Pool::run`:
//!
//! ```text
//!  workers   Pool::run   __ulock_wait   parked   ready_or_register   steal
//!        4       25440           3450      14%               10079       7
//!       16       89292          62382      70%               12619    1115
//! ```
//!
//! So it is not the injector: `SegQueue` and `submit_job` are a rounding error
//! at both sizes, which is what the obvious guess would have been given that
//! every spawn goes through one queue. It is park and unpark. Twelve extra
//! workers raise the useful work by a quarter and spend seventy per cent of
//! their time asleep in a futex, with stealing up a hundred and sixtyfold as
//! they scan for work that is not there.
//!
//! That is a scheduler question rather than a tuning one, and nothing here
//! changes a default on the strength of it: what a fix looks like depends on
//! whether the answer is fewer workers, a sleep that backs off, or a steal
//! that gives up sooner.

use std::sync::Arc;
use std::time::{Duration, Instant};

use nagoya::runtime::Runtime;
use nagoya::sync::Notify;

/// Concurrent pairs. Eight matches the connection count nago-wss found worst.
const PAIRS: usize = 8;
/// Bounces per pair.
const ROUNDS: usize = 2_000;
/// Samples per setting; the best is reported.
const SAMPLES: usize = 3;
/// Worker counts to sweep. Sixteen is `available_parallelism` on this machine.
const WORKERS: [usize; 6] = [1, 2, 4, 8, 12, 16];

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
    println!();
}
