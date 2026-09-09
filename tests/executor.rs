//! What the executor has to do before any of it is worth measuring.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;

use nagoya::{block_on, Executor};
use st3::fanout::{Pool, StdHost};

/// A pool with real threads behind it, and a guard that stops them.
struct Running {
    executor: Executor,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Running {
    fn new(workers: usize) -> Self {
        let host = Arc::new(StdHost::new(workers));
        let pool = Pool::new(workers, 256, host);
        let threads = (0..workers)
            .map(|id| {
                let pool = pool.clone();
                let runner = pool.runner(id);
                // `run` owns the worker until shutdown and returns `true`
                // when it ends cleanly, so calling it in a `while` spins
                // forever afterwards. Once is the whole thread.
                thread::spawn(move || {
                    assert!(pool.run(runner), "this worker was already running");
                })
            })
            .collect();
        Self {
            executor: Executor::new(pool),
            threads,
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.executor.pool().shut_down();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

/// Pends `count` times before it is ready, waking itself each time.
///
/// This is the shape that exercises the part worth testing: every wake has to
/// put another poll on the pool, and the poll that follows has to find the
/// future where the last one left it.
struct Yields {
    left: usize,
}

impl Future for Yields {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<usize> {
        if self.left == 0 {
            return Poll::Ready(0);
        }
        self.left -= 1;
        context.waker().wake_by_ref();
        Poll::Pending
    }
}

#[test]
fn block_on_returns_a_ready_future() {
    assert_eq!(block_on(async { 7 }), 7);
}

#[test]
fn block_on_waits_for_a_wake_from_another_thread() {
    let running = Running::new(2);
    let handle = running.executor.spawn(async { 41 + 1 });
    assert_eq!(block_on(handle), Some(42));
}

#[test]
fn a_task_that_yields_still_finishes() {
    let running = Running::new(2);
    let handle = running.executor.spawn(async {
        Yields { left: 64 }.await;
        "done"
    });
    assert_eq!(block_on(handle), Some("done"));
}

#[test]
fn every_one_of_many_tasks_runs_exactly_once() {
    const TASKS: usize = 2_000;
    let running = Running::new(4);
    let counter = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..TASKS)
        .map(|n| {
            let counter = counter.clone();
            running.executor.spawn(async move {
                Yields { left: n % 4 }.await;
                counter.fetch_add(1, Ordering::Relaxed);
                n
            })
        })
        .collect();

    let mut seen = vec![false; TASKS];
    for handle in handles {
        let n = block_on(handle).expect("every task produces its output");
        assert!(!seen[n], "task {n} finished twice");
        seen[n] = true;
    }
    assert_eq!(counter.load(Ordering::Relaxed), TASKS);
}

/// A task spawning more tasks, which is where a pool that only ever ran what
/// the caller submitted would stall.
#[test]
fn a_task_can_spawn_and_await_another() {
    let running = Running::new(3);
    let executor = Arc::new(running.executor.clone_handle());
    let inner = executor.clone();

    let outer = executor.spawn(async move {
        let a = inner.spawn(async { 20 });
        let b = inner.spawn(async {
            Yields { left: 8 }.await;
            22
        });
        a.await.unwrap() + b.await.unwrap()
    });

    assert_eq!(block_on(outer), Some(42));
}

/// A parallel loop runs every index exactly once.
#[test]
fn a_parallel_loop_covers_its_range() {
    let running = Running::new(4);
    let seen: Arc<Vec<AtomicUsize>> = Arc::new((0..10_000).map(|_| AtomicUsize::new(0)).collect());
    let counter = seen.clone();
    block_on(nagoya::par_for(
        running.executor.pool().clone(),
        0..10_000,
        move |i| {
            counter[i].fetch_add(1, Ordering::Relaxed);
        },
    ));
    for (index, hits) in seen.iter().enumerate() {
        assert_eq!(hits.load(Ordering::Relaxed), 1, "index {index}");
    }
}

/// An empty range completes, rather than hanging on a count that never reaches
/// zero.
#[test]
fn an_empty_parallel_loop_finishes() {
    let running = Running::new(2);
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    block_on(nagoya::par_for(
        running.executor.pool().clone(),
        5..5,
        move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        },
    ));
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

/// A parallel loop awaited from inside a spawned task, which is the shape the
/// two APIs exist to combine.
#[test]
fn a_task_can_await_a_parallel_loop() {
    let running = Running::new(4);
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let pool = running.executor.pool().clone();
    block_on(running.executor.spawn(async move {
        nagoya::par_for(pool, 0..1_000, move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        })
        .await;
    }));
    assert_eq!(hits.load(Ordering::Relaxed), 1_000);
}

/// A cancelled loop stops, and stops soon rather than eventually.
#[test]
fn a_cancelled_loop_stops_early() {
    let running = Running::new(4);
    let done = Arc::new(AtomicUsize::new(0));
    let counter = done.clone();
    let loop_ = nagoya::par_for(running.executor.pool().clone(), 0..1_000_000, move |_| {
        counter.fetch_add(1, Ordering::Relaxed);
    })
    .leaf(64);
    let cancel = loop_.cancel();
    cancel.cancel();
    block_on(loop_);
    // Cancelled before the first poll, so nothing should have been handed out
    // at all, let alone run to a million.
    assert!(
        done.load(Ordering::Relaxed) < 1_000_000,
        "a cancelled loop ran every item"
    );
}

/// The body can stop the loop it is running in, which is how a parallel search
/// stops once it has found what it wanted.
#[test]
fn a_body_can_stop_its_own_loop() {
    let running = Running::new(4);
    let seen = Arc::new(AtomicUsize::new(0));
    let cancel = nagoya::Cancel::new();
    let (counter, trip) = (seen.clone(), cancel.clone());
    let loop_ = nagoya::par_for(running.executor.pool().clone(), 0..2_000_000, move |_| {
        if counter.fetch_add(1, Ordering::Relaxed) >= 1_000 {
            trip.cancel();
        }
    })
    .leaf(64)
    .cancel_with(cancel.clone());
    block_on(loop_);
    assert!(cancel.is_cancelled());
    let ran = seen.load(Ordering::Relaxed);
    assert!(ran >= 1_000, "stopped before it found anything: {ran}");
    assert!(ran < 2_000_000, "the body never stopped the loop: {ran}");
}

/// Dropping the future cancels the loop rather than leaving it running.
#[test]
fn dropping_a_loop_cancels_it() {
    let running = Running::new(4);
    let loop_ = nagoya::par_for(running.executor.pool().clone(), 0..1_000_000, |_| {});
    let cancel = loop_.cancel();
    assert!(!cancel.is_cancelled());
    drop(loop_);
    assert!(
        cancel.is_cancelled(),
        "dropping the future did not cancel it"
    );
}

/// A yielding task finishes, and lets others run while it does.
#[test]
fn a_task_can_yield_its_worker() {
    let running = Running::new(2);
    let order = Arc::new(AtomicUsize::new(0));
    let (first, second) = (order.clone(), order.clone());

    let long = running.executor.spawn(async move {
        for _ in 0..8 {
            nagoya::yield_now().await;
        }
        first.fetch_add(1, Ordering::AcqRel)
    });
    let short = running
        .executor
        .spawn(async move { second.fetch_add(1, Ordering::AcqRel) });

    let (long, short) = (block_on(long), block_on(short));
    assert_eq!(long, Some(1), "the yielding task did not let the other in");
    assert_eq!(short, Some(0));
}
