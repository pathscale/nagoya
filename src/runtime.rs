//! A pool that owns its threads, for callers that have nowhere else to put them.
//!
//! # Why this exists in a crate whose point is not owning threads
//!
//! [`Executor`](crate::Executor) deliberately takes a pool whose threads the
//! caller provides. That is the property that lets this crate run without an
//! operating system, and it is not negotiable.
//!
//! But it pushes a real problem onto some callers. A storage engine has
//! background work that is *intrinsic to it* rather than to its user: a vacuum
//! sweep and a persistence writer are the engine's business, they must keep
//! running, and no caller should have to know they exist in order to hand over
//! a thread for them. Faced with that, the engine reached for `tokio::spawn`,
//! which works because tokio has an implicit global runtime to find. That one
//! call put the whole of `tokio` into a `no_std` dependency graph.
//!
//! So: this is the explicit version of the thing that was being got implicitly.
//! It owns threads, it needs `std`, and it says so in its name and its feature
//! gate, rather than arriving as a side effect of a `spawn`.
//!
//! **Off without `std`, and that is the honest outcome.** A build with no
//! threads has no background worker, and an engine that needs one has to say
//! what it does instead rather than pretend the problem is absent.

use alloc::sync::Arc;
use core::future::Future;

use st3::fanout::{Pool, StdHost, Tuning};

use crate::{Executor, JoinHandle};

/// A work-stealing pool that started its own threads.
///
/// Dropping this does **not** stop the threads: they are detached, because the
/// tasks on them are the ones nobody else is watching, and tearing them down
/// under a running sweep is worse than letting them run. A caller that wants
/// them stopped stops the work, not the runtime.
pub struct Runtime {
    executor: Executor,
}

impl Runtime {
    /// A runtime with `workers` threads.
    ///
    /// # Panics
    ///
    /// If a thread cannot be started. There is no useful way to continue: the
    /// caller asked for background execution and the platform refused it, and
    /// returning a runtime that silently runs nothing would be worse.
    #[must_use]
    pub fn new(workers: usize) -> Self {
        Self::with_tuning(workers, Tuning::default(), "nagoya")
    }

    /// A runtime with `workers` threads, at `tuning`, whose threads are named
    /// `{label}-{id}`.
    ///
    /// # Why this has to exist here rather than in the caller
    ///
    /// A caller can already build a [`Pool`] at any tuning and run it on its
    /// own threads. What it cannot do is [`crate::task::mark_current`], which
    /// is private, and **that marker is the only thing that makes
    /// `local_wakes` do anything**: without it a wake takes the injector no
    /// matter what the tuning says.
    ///
    /// So a pool built outside this module silently runs any locality-flavored
    /// tuning as if it were spread. That is not a small difference, it is the
    /// entire mechanism the tuning selects, and it is invisible: the pool
    /// works, the tuning is set, and the behaviour it asks for never happens.
    /// A comparison between two such pools would attribute to the scheduler a
    /// difference that was only ever a missing thread-local.
    ///
    /// # Panics
    ///
    /// If a thread cannot be started, for the reason [`Runtime::new`] gives.
    #[must_use]
    pub fn with_tuning(workers: usize, tuning: Tuning, label: &str) -> Self {
        let workers = workers.max(1);
        let host = Arc::new(StdHost::new(workers));
        let pool = Pool::with_tuning(workers, 1024, host, tuning);
        for id in 0..workers {
            let pool = pool.clone();
            let runner = pool.runner(id);
            std::thread::Builder::new()
                .name(alloc::format!("{label}-{id}"))
                .spawn(move || {
                    // Tells the scheduler that a wake happening on this thread
                    // belongs to worker `id`, so it can skip the injector. See
                    // `task::mark_current`.
                    let _current = crate::task::mark_current(&pool, id);
                    let _ = pool.run(runner);
                })
                .expect("a runtime thread");
        }
        Self {
            executor: Executor::new(pool),
        }
    }

    /// Run a future on this runtime's threads.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.executor.spawn(future)
    }

    /// The executor underneath, for a caller that wants to hand it on.
    #[must_use]
    pub fn executor(&self) -> &Executor {
        &self.executor
    }
}

/// Threads for the shared pool.
///
/// The machine's parallelism, which is also what `tokio::spawn` gave the
/// callers this replaces. Keeping the number the same is the point: changing
/// the thread count and the runtime in one step makes any measurement of the
/// swap unreadable.
fn shared_threads() -> usize {
    std::thread::available_parallelism().map_or(2, core::num::NonZeroUsize::get)
}

/// One pool per process, started on first use.
///
/// # Why this exists, given that an ambient runtime is what went wrong
///
/// This is the same shape as `tokio`'s implicit global, and that global is
/// precisely how a whole runtime got into a `no_std` dependency graph without
/// anyone writing it down. So it is worth being explicit about why it is back.
///
/// The alternative is each crate growing its own `OnceLock<Runtime>`. Two such
/// crates in one process start two pools and the process pays for both, with
/// neither able to steal the other's idle threads. A storage engine and
/// whatever else the binary links are not coordinating on this, and cannot.
/// One pool is the correct answer to that.
///
/// What makes it different from the thing it resembles: it is behind `std`, so
/// a `--no-default-features` build cannot reach it and cannot silently acquire
/// threads. That gate is exactly the one `tokio::spawn` did not have.
///
/// A caller that wants its own threads, its own count, or a pool it can stop
/// still builds a [`Runtime`] directly. This is the convenience, not the API.
pub fn background() -> &'static Runtime {
    static SHARED: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();
    SHARED.get_or_init(|| Runtime::new(shared_threads()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;
    use core::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn the_shared_pool_is_one_pool() {
        assert!(core::ptr::eq(background(), background()));
        let counter = Arc::new(AtomicUsize::new(0));
        let handles: alloc::vec::Vec<_> = (0..32)
            .map(|_| {
                let counter = counter.clone();
                background().spawn(async move {
                    counter.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        for handle in handles {
            block_on(handle);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 32);
    }

    #[test]
    fn a_runtime_runs_what_it_is_given() {
        let runtime = Runtime::new(2);
        let counter = Arc::new(AtomicUsize::new(0));
        let handles: alloc::vec::Vec<_> = (0..64)
            .map(|_| {
                let counter = counter.clone();
                runtime.spawn(async move {
                    counter.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        for handle in handles {
            block_on(handle);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 64);
    }
}

/// What [`Runtime::with_tuning`] exists for, demonstrated rather than asserted.
///
/// The claim is that a pool built outside this module cannot honour a
/// locality tuning, because [`crate::task::mark_current`] is private and that
/// marker is the whole mechanism. A test that only checked the marked pool
/// would pass just as well if the marker did nothing, so both are run.
#[cfg(test)]
mod local_wakes {
    use super::{Executor, Runtime, Tuning};
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use st3::fanout::{Pool, StdHost};
    use std::collections::HashSet;
    use std::sync::Mutex;

    const WORKERS: usize = 4;
    const YIELDS: usize = 400;

    /// Self-wakes `left` times, recording which thread ran each poll.
    ///
    /// The thread identity is the observable. `local_wakes` means a task that
    /// wakes itself goes back to the worker that was running it, so the set of
    /// threads that polled one task has exactly one member.
    struct RecordYields {
        left: usize,
        seen: Arc<Mutex<HashSet<std::thread::ThreadId>>>,
    }

    impl Future for RecordYields {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            self.seen
                .lock()
                .expect("the set holds no state a panic could corrupt")
                .insert(std::thread::current().id());
            if self.left == 0 {
                return Poll::Ready(());
            }
            self.left -= 1;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }

    fn threads_that_polled_one_task(executor: &Executor) -> usize {
        let seen = Arc::new(Mutex::new(HashSet::new()));
        let task = RecordYields {
            left: YIELDS,
            seen: seen.clone(),
        };
        crate::block_on(executor.spawn(task));
        let count = seen.lock().expect("the set").len();
        count
    }

    #[test]
    fn a_marked_pool_keeps_a_self_waking_task_on_one_worker() {
        let runtime = Runtime::with_tuning(WORKERS, Tuning::locality(), "test-marked");
        assert_eq!(
            threads_that_polled_one_task(runtime.executor()),
            1,
            "a locality-tuned runtime let a self-wake leave its worker, so the marker is not being set"
        );
    }

    /// The same tuning, on a pool this module did not build.
    ///
    /// This is what `WorkTable` was doing for every flavor but the default,
    /// and it is why `with_tuning` had to exist: the tuning is set, the pool
    /// works, and the behaviour it asks for silently never happens.
    #[test]
    fn an_unmarked_pool_cannot_honour_the_same_tuning() {
        let host = Arc::new(StdHost::new(WORKERS));
        let pool = Pool::with_tuning(WORKERS, 1024, host, Tuning::locality());
        let threads: Vec<_> = (0..WORKERS)
            .map(|id| {
                let pool = pool.clone();
                let runner = pool.runner(id);
                std::thread::spawn(move || {
                    // No `mark_current`, because it is private. That is the
                    // whole of the difference from the test above.
                    let _ = pool.run(runner);
                })
            })
            .collect();
        let executor = Executor::new(pool.clone());

        let count = threads_that_polled_one_task(&executor);

        pool.shut_down();
        for thread in threads {
            let _ = thread.join();
        }

        assert!(
            count > 1,
            "an unmarked pool kept the task on one worker, which would mean the marker is not the mechanism \
             `local_wakes` depends on and this whole API is unnecessary"
        );
    }
}
