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

/// `join` runs both halves and returns both results.
#[test]
fn join_runs_both_halves() {
    let running = Running::new(4);
    let registry = nagoya::Registry::new(running.executor.pool().clone());
    let (left, right) = registry.join(|| 6 * 7, || "both");
    assert_eq!(left, 42);
    assert_eq!(right, "both");
}

/// A scope may spawn work borrowing the caller's stack, and does not return
/// until that work has finished touching it.
#[test]
fn a_scope_waits_for_borrowed_work() {
    let running = Running::new(4);
    let registry = nagoya::Registry::new(running.executor.pool().clone());
    let mut cells = vec![0usize; 64];
    registry.scope(|scope| {
        for (index, cell) in cells.iter_mut().enumerate() {
            scope.spawn(move |_| *cell = index * 2);
        }
    });
    for (index, cell) in cells.iter().enumerate() {
        assert_eq!(*cell, index * 2, "cell {index}");
    }
}

/// The three hooks fire, and the deadlock one only when nothing is running.
#[test]
fn the_blocking_hooks_fire() {
    let running = Running::new(2);
    let acquired = Arc::new(AtomicUsize::new(0));
    let released = Arc::new(AtomicUsize::new(0));
    let deadlocked = Arc::new(AtomicUsize::new(0));
    let (a, r, d) = (acquired.clone(), released.clone(), deadlocked.clone());
    let registry = nagoya::Registry::with_hooks(
        running.executor.pool().clone(),
        nagoya::Hooks {
            on_acquire: Some(Arc::new(move || {
                a.fetch_add(1, Ordering::Relaxed);
            })),
            on_release: Some(Arc::new(move || {
                r.fetch_add(1, Ordering::Relaxed);
            })),
            on_deadlock: Some(Arc::new(move || {
                d.fetch_add(1, Ordering::Relaxed);
            })),
        },
    );

    registry.mark_blocked_and_wait(|| {});
    assert_eq!(released.load(Ordering::Relaxed), 1);
    assert_eq!(acquired.load(Ordering::Relaxed), 1);
    // Two workers, one blocked: one is still running, so no deadlock.
    assert_eq!(deadlocked.load(Ordering::Relaxed), 0);
    assert!(!registry.is_deadlocked());
}

/// Every thread blocked and none running is what the deadlock hook reports.
#[test]
fn every_thread_blocked_is_a_deadlock() {
    let running = Running::new(1);
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = seen.clone();
    let registry = nagoya::Registry::with_hooks(
        running.executor.pool().clone(),
        nagoya::Hooks {
            on_deadlock: Some(Arc::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
            })),
            ..nagoya::Hooks::default()
        },
    );
    // One worker, and it blocks: active reaches zero with one blocked.
    registry.mark_blocked_and_wait(|| {});
    assert_eq!(seen.load(Ordering::Relaxed), 1);
}
