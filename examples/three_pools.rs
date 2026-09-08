//! Four ways to run 100,000 independent units of work on 8 threads.
//!
//! # What is and is not comparable here
//!
//! `rayon` is not an async runtime. It takes a closure, runs it once, and has
//! no waker, no poll and no notion of a task that suspends. So it can only
//! appear against the *closure* shape, and the honest reading is: rayon and
//! `st3::fanout` are the same kind of thing, and `tokio` and `nagoya` are the
//! same kind of thing built on top of that kind of thing.
//!
//! Every arm submits from a thread that is not one of the eight workers, which
//! is the cross-thread path. Submitting from inside a pool's own worker is a
//! different and much cheaper path in tokio and in rayon both.
//!
//! CPU is user + system across every thread, so an arm that finishes quickly by
//! burning eight cores cannot hide it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Instant;

use st3::fanout::{Pool, StdHost};

const TASKS: usize = 100_000;
const REPS: usize = 5;
fn workers() -> usize {
    std::env::var("W").ok().and_then(|v| v.parse().ok()).unwrap_or(8)
}

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

/// A counter that can be waited on without spinning, so the waiter does not
/// take a core away from the eight workers.
struct Finish {
    done: AtomicUsize,
    lock: Mutex<bool>,
    signal: Condvar,
}

impl Finish {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            done: AtomicUsize::new(0),
            lock: Mutex::new(false),
            signal: Condvar::new(),
        })
    }

    fn tick(&self) {
        if self.done.fetch_add(1, Ordering::AcqRel) + 1 == TASKS {
            *self.lock.lock().expect("the lock") = true;
            self.signal.notify_one();
        }
    }

    fn wait(&self) {
        let mut finished = self.lock.lock().expect("the lock");
        while !*finished {
            finished = self.signal.wait(finished).expect("the wait");
        }
    }
}

fn fanout_run() -> f64 {
    let host = Arc::new(StdHost::new(workers()));
    let pool = Pool::new(workers(), 1024, host);
    let threads: Vec<_> = (0..workers())
        .map(|id| {
            let pool = pool.clone();
            let runner = pool.runner(id);
            thread::spawn(move || {
                let _ = pool.run(runner);
            })
        })
        .collect();
    let finish = Finish::new();
    let start = Instant::now();
    for n in 0..TASKS {
        let finish = finish.clone();
        pool.submit(n % workers(), Box::new(move || finish.tick()));
    }
    finish.wait();
    let elapsed = start.elapsed().as_secs_f64();
    pool.shut_down();
    for thread in threads {
        let _ = thread.join();
    }
    elapsed
}

fn rayon_run() -> f64 {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers())
        .build()
        .expect("a pool");
    let finish = Finish::new();
    let start = Instant::now();
    for _ in 0..TASKS {
        let finish = finish.clone();
        pool.spawn(move || finish.tick());
    }
    finish.wait();
    start.elapsed().as_secs_f64()
}

fn tokio_run() -> f64 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers())
        .build()
        .expect("a runtime");
    let finish = Finish::new();
    let start = Instant::now();
    for _ in 0..TASKS {
        let finish = finish.clone();
        runtime.spawn(async move { finish.tick() });
    }
    finish.wait();
    start.elapsed().as_secs_f64()
}

fn nagoya_run() -> f64 {
    let host = Arc::new(StdHost::new(workers()));
    let pool = Pool::new(workers(), 1024, host);
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
    let finish = Finish::new();
    let start = Instant::now();
    for _ in 0..TASKS {
        let finish = finish.clone();
        executor.spawn(async move { finish.tick() });
    }
    finish.wait();
    let elapsed = start.elapsed().as_secs_f64();
    pool.shut_down();
    for thread in threads {
        let _ = thread.join();
    }
    elapsed
}

/// Rayon on its own ground, so the number above is not read as rayon's ceiling.
///
/// `par_iter` splits recursively *inside* the pool: the work never crosses the
/// injector, each worker takes half of what it finds, and the split stops when
/// a chunk is small enough to run straight through. That is the shape rayon
/// was built for, and no arm above shares it.
fn rayon_par_iter() -> f64 {
    use rayon::prelude::*;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers())
        .build()
        .expect("a pool");
    let finish = Finish::new();
    let start = Instant::now();
    pool.install(|| {
        (0..TASKS).into_par_iter().for_each(|_| finish.tick());
    });
    finish.wait();
    start.elapsed().as_secs_f64()
}

fn measure(body: impl FnOnce() -> f64) -> (f64, f64) {
    let before = cpu_seconds();
    let wall = body();
    (wall, cpu_seconds() - before)
}

fn report(label: &str, mut runs: Vec<(f64, f64)>) {
    runs.sort_by(|a, b| a.0.partial_cmp(&b.0).expect("no NaN"));
    let (median, cpu) = runs[runs.len() / 2];
    println!(
        "  {label:<26} {:>8.1} ms wall   {:>10.0} tasks/s   {:>8.1} ms cpu",
        median * 1e3,
        TASKS as f64 / median,
        cpu * 1e3
    );
}

fn main() {
    println!(
        "{TASKS} tasks, {} workers, median of {REPS}, arms interleaved, submitted from outside\n",
        workers()
    );
    let (mut fan, mut ray, mut tok, mut nag) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut par = Vec::new();
    for _ in 0..REPS {
        fan.push(measure(fanout_run));
        ray.push(measure(rayon_run));
        tok.push(measure(tokio_run));
        nag.push(measure(nagoya_run));
        par.push(measure(rayon_par_iter));
    }
    println!("a closure, run once:");
    report("rayon", ray);
    report("st3::fanout", fan);
    println!("\na future, polled to completion:");
    report("tokio", tok);
    report("nagoya (on st3::fanout)", nag);
    println!("\nfor scale, rayon doing what rayon is for:");
    report("rayon par_iter", par);
}
