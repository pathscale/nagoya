//! What the benchmark's own bookkeeping costs, before any scheduler runs.
//!
//! Every arm in `three_pools` and `vs_tokio` counts completions into one shared
//! `AtomicUsize`. That is one cache line, hit 100,000 times, and the cost of
//! hitting it depends on how many cores are hitting it. If that cost is a large
//! part of what those benchmarks report, then they are measuring their own
//! counter and the scheduler underneath is not what separates the arms.
//!
//! No pool, no tasks: threads and the counter alone.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

const TASKS: usize = 100_000;
const REPS: usize = 5;

fn median(mut times: Vec<f64>) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    times[times.len() / 2]
}

/// `threads` threads sharing one counter, `TASKS` increments between them.
fn shared(threads: usize) -> f64 {
    median(
        (0..REPS)
            .map(|_| {
                let counter = Arc::new(AtomicUsize::new(0));
                let each = TASKS / threads;
                let start = Instant::now();
                let handles: Vec<_> = (0..threads)
                    .map(|_| {
                        let counter = counter.clone();
                        thread::spawn(move || {
                            for _ in 0..each {
                                counter.fetch_add(1, Ordering::AcqRel);
                            }
                        })
                    })
                    .collect();
                for handle in handles {
                    let _ = handle.join();
                }
                start.elapsed().as_secs_f64()
            })
            .collect(),
    )
}

fn main() {
    println!("{TASKS} increments of one shared counter, median of {REPS}\n");
    for threads in [1usize, 2, 4, 8] {
        let time = shared(threads);
        println!(
            "  {threads} thread(s)   {:>7.1} ms   {:>10.0} increments/s   {:>5.0} ns each",
            time * 1e3,
            TASKS as f64 / time,
            time / TASKS as f64 * 1e9
        );
    }
}
