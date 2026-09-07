//! nagoya against tokio, on the one thing they both do.
//!
//! # What this measures
//!
//! Spawning tasks and waiting for all of them to finish. Two shapes: tasks that
//! complete on their first poll, and tasks that yield first, which is what
//! exercises the wake path where a scheduler's design actually shows.
//!
//! # What this does not measure
//!
//! Everything else tokio does. There is no I/O here, no timers, no
//! cancellation, no `select!`, because nagoya has none of them. A runtime that
//! does more is not slower for doing more, and this number is not a verdict on
//! tokio; it is a check that the executor underneath nagoya is in the same
//! league before anyone builds on it.
//!
//! Arms are interleaved and the first pair is a **null calibration**: the same
//! implementation against itself, so the reported spread has a floor to be read
//! against.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use st3::fanout::{Pool, StdHost};

const WORKERS: usize = 8;
const TASKS: usize = 100_000;
const REPS: usize = 5;

/// Pends `left` times, waking itself each time.
struct Yields {
    left: usize,
}

impl std::future::Future for Yields {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.left == 0 {
            return std::task::Poll::Ready(());
        }
        self.left -= 1;
        context.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

fn nagoya_run(yields: usize) -> f64 {
    let host = Arc::new(StdHost::new(WORKERS));
    let pool = Pool::new(WORKERS, 1024, host);
    let threads: Vec<_> = (0..WORKERS)
        .map(|id| {
            let pool = pool.clone();
            let runner = pool.runner(id);
            thread::spawn(move || {
                let _ = pool.run(runner);
            })
        })
        .collect();
    let executor = nagoya::Executor::new(pool.clone());
    let done = Arc::new(AtomicUsize::new(0));
    let waiter = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));

    let start = Instant::now();
    for _ in 0..TASKS {
        let done = done.clone();
        let waiter = waiter.clone();
        executor.spawn(async move {
            Yields { left: yields }.await;
            if done.fetch_add(1, Ordering::AcqRel) + 1 == TASKS {
                let (lock, signal) = &*waiter;
                *lock.lock().expect("the lock") = true;
                signal.notify_one();
            }
        });
    }
    // Parked, not spun. A spinning main thread takes a core away from the
    // eight workers, and tokio's arm parks inside `block_on`, so spinning here
    // would be measuring the harness.
    {
        let (lock, signal) = &*waiter;
        let mut finished = lock.lock().expect("the lock");
        while !*finished {
            finished = signal.wait(finished).expect("the wait");
        }
    }
    let elapsed = start.elapsed().as_secs_f64();

    pool.shut_down();
    for thread in threads {
        let _ = thread.join();
    }
    elapsed
}

fn tokio_run(yields: usize) -> f64 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("a runtime");
    let done = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();
    runtime.block_on(async {
        let mut handles = Vec::with_capacity(TASKS);
        for _ in 0..TASKS {
            let done = done.clone();
            handles.push(tokio::spawn(async move {
                Yields { left: yields }.await;
                done.fetch_add(1, Ordering::Relaxed);
            }));
        }
        for handle in handles {
            handle.await.expect("a task");
        }
    });
    let elapsed = start.elapsed().as_secs_f64();
    assert_eq!(done.load(Ordering::Relaxed), TASKS);
    elapsed
}

fn report(label: &str, mut times: Vec<f64>) {
    times.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    let median = times[times.len() / 2];
    let spread = (times[times.len() - 1] - times[0]) / median * 100.0;
    println!(
        "  {label:<28} {:>8.1} ms   {:>10.0} tasks/s   spread {spread:>5.1}%",
        median * 1e3,
        TASKS as f64 / median
    );
}

fn main() {
    println!("{TASKS} tasks, {WORKERS} workers, median of {REPS}, arms interleaved\n");

    for (name, yields) in [("complete on first poll", 0), ("yield 4 times first", 4)] {
        println!("{name}:");
        let (mut a, mut b, mut null) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..REPS {
            null.push(nagoya_run(yields));
            a.push(nagoya_run(yields));
            b.push(tokio_run(yields));
        }
        report("nagoya (null calibration)", null);
        report("nagoya", a);
        report("tokio", b);
        println!();
    }
}
