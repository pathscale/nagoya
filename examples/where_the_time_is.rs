//! Is the executor slow, or is the pool under it slow?
//!
//! Three arms over the same 100,000 units of work:
//!
//! 1. **the pool alone** - `submit` a trivial boxed closure, no future, no
//!    waker, no executor. This is the ceiling nagoya can ever reach.
//! 2. **nagoya** - the same work as a spawned future.
//! 3. **tokio** - for scale.
//!
//! If arm 1 is close to arm 2, the executor is not the problem and no amount of
//! tuning it will help.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::Instant;

use st3::fanout::{Pool, StdHost};

static WORKERS_N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(8);
fn workers() -> usize { WORKERS_N.load(Ordering::Relaxed) }
const TASKS: usize = 100_000;
const REPS: usize = 5;

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

fn with_pool<R>(body: impl FnOnce(&Arc<Pool>) -> R) -> R {
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
    let out = body(&pool);
    pool.shut_down();
    for thread in threads {
        let _ = thread.join();
    }
    out
}

/// The pool with nothing on top: what a boxed closure costs, end to end.
fn pool_alone() -> f64 {
    with_pool(|pool| {
        let finish = Finish::new();
        let start = Instant::now();
        for n in 0..TASKS {
            let finish = finish.clone();
            pool.submit(n % workers(), Box::new(move || finish.tick()));
        }
        finish.wait();
        start.elapsed().as_secs_f64()
    })
}

/// The same work as a future, through nagoya.
fn through_nagoya() -> f64 {
    with_pool(|pool| {
        let executor = nagoya::Executor::new(pool.clone());
        let finish = Finish::new();
        let start = Instant::now();
        for _ in 0..TASKS {
            let finish = finish.clone();
            executor.spawn(async move { finish.tick() });
        }
        finish.wait();
        start.elapsed().as_secs_f64()
    })
}

fn through_tokio() -> f64 {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers())
        .build()
        .expect("a runtime");
    let finish = Finish::new();
    let start = Instant::now();
    runtime.block_on(async {
        for _ in 0..TASKS {
            let finish = finish.clone();
            tokio::spawn(async move { finish.tick() });
        }
        finish.wait();
    });
    start.elapsed().as_secs_f64()
}

fn report(label: &str, mut times: Vec<f64>) {
    times.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    let median = times[times.len() / 2];
    println!(
        "  {label:<22} {:>8.1} ms   {:>10.0} tasks/s   {:>7.0} ns each",
        median * 1e3,
        TASKS as f64 / median,
        median / TASKS as f64 * 1e9
    );
}

fn main() {
    println!("{TASKS} units, median of {REPS}, interleaved\n");
    for n in [1usize, 2, 4, 8] {
        WORKERS_N.store(n, Ordering::Relaxed);
        let (mut pool, mut tok) = (Vec::new(), Vec::new());
        for _ in 0..REPS {
            pool.push(pool_alone());
            tok.push(through_tokio());
        }
        println!("{n} worker(s):");
        report("  pool alone", pool);
        report("  tokio", tok);
    }
}
