//! Producer cost alone: submit with no workers running at all.
//!
//! If submitting into idle intakes is cheap, the collapse at eight workers is
//! on the worker side - stealing, waking, draining - and not in `submit`.
use std::sync::Arc;
use std::time::Instant;
use st3::fanout::{Pool, StdHost};

const TASKS: usize = 100_000;

fn main() {
    println!("{TASKS} submits, nobody running, median of 5\n");
    for workers in [1usize, 2, 4, 8] {
        let mut times = Vec::new();
        for _ in 0..5 {
            let host = Arc::new(StdHost::new(workers));
            let pool = Pool::new(workers, 1 << 17, host);
            let start = Instant::now();
            for n in 0..TASKS {
                pool.submit(n % workers, Box::new(|| {}));
            }
            times.push(start.elapsed().as_secs_f64());
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = times[2];
        println!(
            "  {workers} intake(s): {:>7.1} ms   {:>6.0} ns a submit",
            median * 1e3,
            median / TASKS as f64 * 1e9
        );
    }
}
