//! How fast can one thread hand work over, with nobody taking it?
//!
//! Every other benchmark here measures a producer and eight consumers together.
//! If the producer alone cannot beat the number those benchmarks report, then
//! the consumers are not the thing being measured and no amount of tuning them
//! will move it.
//!
//! No workers are started. The pool fills up and nothing runs.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use st3::fanout::{Job, Pool, StdHost};

const TASKS: usize = 100_000;
const REPS: usize = 5;

fn median(mut times: Vec<f64>) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    times[times.len() / 2]
}

fn report(label: &str, median: f64) {
    println!(
        "  {label:<34} {:>8.1} ms   {:>10.0} tasks/s   {:>6.0} ns each",
        median * 1e3,
        TASKS as f64 / median,
        median / TASKS as f64 * 1e9
    );
}

fn time(mut body: impl FnMut()) -> f64 {
    median(
        (0..REPS)
            .map(|_| {
                let start = Instant::now();
                body();
                start.elapsed().as_secs_f64()
            })
            .collect(),
    )
}

fn main() {
    println!("{TASKS} submits, no workers running, median of {REPS}\n");
    let counter = Arc::new(AtomicUsize::new(0));

    let boxed = time(|| {
        let pool = Pool::new(8, 1024, Arc::new(StdHost::new(8)));
        for n in 0..TASKS {
            let counter = counter.clone();
            pool.submit(
                n % 8,
                Box::new(move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                }),
            );
        }
    });
    let by_fn = time(|| {
        let pool = Pool::new(8, 1024, Arc::new(StdHost::new(8)));
        for _ in 0..TASKS {
            let counter = counter.clone();
            pool.submit_fn(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
    });
    let by_job = time(|| {
        let pool = Pool::new(8, 1024, Arc::new(StdHost::new(8)));
        for _ in 0..TASKS {
            let counter = counter.clone();
            pool.submit_job(Job::from_boxed(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            }));
        }
    });
    let spawned = time(|| {
        let pool = Pool::new(8, 1024, Arc::new(StdHost::new(8)));
        let executor = nagoya::Executor::new(pool);
        for _ in 0..TASKS {
            let counter = counter.clone();
            drop(executor.spawn(async move {
                counter.fetch_add(1, Ordering::Relaxed);
            }));
        }
    });

    report("pool.submit, a boxed closure", boxed);
    report("pool.submit_fn, a closure", by_fn);
    report("pool.submit_job, a job", by_job);
    report("executor.spawn, a future", spawned);
}
