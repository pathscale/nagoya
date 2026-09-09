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

use std::sync::atomic::{AtomicUsize, Ordering};
/// User + system CPU across every thread of this process, in seconds.
///
/// Wall time alone cannot tell a runtime that finished quickly from one that
/// finished quickly by burning eight cores to do it.
fn cpu_seconds() -> f64 {
    #[repr(C)]
    #[derive(Default)]
    struct Timeval {
        sec: i64,
        usec: i32,
        _pad: i32,
    }
    #[repr(C)]
    #[derive(Default)]
    struct Rusage {
        utime: Timeval,
        stime: Timeval,
        rest: [i64; 14],
    }
    unsafe extern "C" {
        fn getrusage(who: i32, usage: *mut Rusage) -> i32;
    }
    let mut usage = Rusage::default();
    // SAFETY: `who` is RUSAGE_SELF and the struct is the layout the platform
    // writes; the trailing fields are only read as opaque words.
    unsafe {
        getrusage(0, &raw mut usage);
    }
    usage.utime.sec as f64
        + f64::from(usage.utime.usec) / 1e6
        + usage.stime.sec as f64
        + f64::from(usage.stime.usec) / 1e6
}

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use st3::fanout::{Pool, StdHost, Tuning};

fn workers() -> usize {
    std::env::var("W")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}
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
    let host = Arc::new(StdHost::new(workers()));
    // Built from a preset rather than as a literal: `Tuning` is
    // `#[non_exhaustive]` from ps-st3 0.6, so a field it gains later is not a
    // breaking change and this example does not have to be edited again.
    let tuning = Tuning::locality()
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
        .with_promote_every(
            std::env::var("PROMOTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64),
        )
        .with_injector_batch(
            std::env::var("BATCH")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(32),
        );
    let pool = Pool::with_tuning(workers(), 1024, host, tuning);
    let threads: Vec<_> = (0..workers())
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
        .worker_threads(workers())
        .enable_all()
        .build()
        .expect("a runtime");
    let done = Arc::new(AtomicUsize::new(0));

    // Spawned from *outside* the runtime, on this thread, which is the same
    // cross-thread path nagoya's `submit` takes. Spawning inside `block_on`
    // instead puts every task on the current worker's own local queue with no
    // lock and no handoff, which is tokio's cheapest path and not the one the
    // other arm is being measured on.
    let start = Instant::now();
    let mut handles = Vec::with_capacity(TASKS);
    for _ in 0..TASKS {
        let done = done.clone();
        handles.push(runtime.spawn(async move {
            Yields { left: yields }.await;
            done.fetch_add(1, Ordering::Relaxed);
        }));
    }
    runtime.block_on(async {
        for handle in handles {
            handle.await.expect("a task");
        }
    });
    let elapsed = start.elapsed().as_secs_f64();
    assert_eq!(done.load(Ordering::Relaxed), TASKS);
    elapsed
}

fn report(label: &str, mut runs: Vec<(f64, f64)>) {
    runs.sort_by(|a, b| a.0.partial_cmp(&b.0).expect("no NaN"));
    let (median, cpu) = runs[runs.len() / 2];
    let spread = (runs[runs.len() - 1].0 - runs[0].0) / median * 100.0;
    let _ = spread;
    println!(
        "  {label:<28} {:>8.1} ms wall   {:>10.0} tasks/s   {:>8.1} ms cpu",
        median * 1e3,
        TASKS as f64 / median,
        cpu * 1e3
    );
}

/// Wall and CPU for one run of `body`.
fn measure(body: impl FnOnce() -> f64) -> (f64, f64) {
    let before = cpu_seconds();
    let wall = body();
    (wall, cpu_seconds() - before)
}

fn main() {
    println!(
        "{TASKS} tasks, {} workers, median of {REPS}, arms interleaved\n",
        workers()
    );

    for (name, yields) in [("complete on first poll", 0), ("yield 4 times first", 4)] {
        println!("{name}:");
        let (mut a, mut b, mut null) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..REPS {
            null.push(measure(|| nagoya_run(yields)));
            a.push(measure(|| nagoya_run(yields)));
            b.push(measure(|| tokio_run(yields)));
        }
        report("nagoya (null calibration)", null);
        report("nagoya", a);
        report("tokio", b);
        println!();
    }
}
