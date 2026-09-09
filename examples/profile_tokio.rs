//! The same batched first-poll workload on tokio, so the two profiles compare.
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() {
    let workers = 8;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .expect("a runtime");

    let batch = 100_000usize;
    let budget = Duration::from_secs(12);
    let start = Instant::now();
    let mut batches = 0usize;
    while start.elapsed() < budget {
        let done = Arc::new(AtomicUsize::new(0));
        for _ in 0..batch {
            let done = done.clone();
            runtime.spawn(async move {
                done.fetch_add(1, Ordering::Relaxed);
            });
        }
        while done.load(Ordering::Acquire) < batch {
            std::hint::spin_loop();
        }
        batches += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "tokio: {batches} batches of {batch} in {elapsed:.1} s, {:.0} tasks/s",
        (batches * batch) as f64 / elapsed
    );
}
