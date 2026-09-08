//! nagoya's first-poll workload, in batches, so a sampler sees steady state.
//!
//! Batched rather than one huge spawn: the spawn loop outruns the workers, so a
//! single batch of the size this needs would hold millions of live tasks and
//! measure the allocator instead of the scheduler.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use st3::fanout::{Pool, StdHost};

fn main() {
    let workers = 8;
    let host = Arc::new(StdHost::new(workers));
    let pool = Pool::new(workers, 1024, host);
    let threads: Vec<_> = (0..workers)
        .map(|id| {
            let pool = pool.clone();
            let runner = pool.runner(id);
            thread::spawn(move || {
                let _ = pool.run(runner);
            })
        })
        .collect();
    let executor = nagoya::Executor::new(pool.clone());

    let batch = 100_000usize;
    let budget = Duration::from_secs(12);
    let start = Instant::now();
    let mut batches = 0usize;
    while start.elapsed() < budget {
        let done = Arc::new(AtomicUsize::new(0));
        for _ in 0..batch {
            let done = done.clone();
            executor.spawn(async move {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }
        while done.load(Ordering::Acquire) < batch {
            std::hint::spin_loop();
        }
        batches += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    pool.shut_down();
    for t in threads {
        let _ = t.join();
    }
    eprintln!(
        "nagoya: {batches} batches of {batch} in {elapsed:.1} s, {:.0} tasks/s",
        (batches * batch) as f64 / elapsed
    );
}
