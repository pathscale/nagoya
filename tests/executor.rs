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
