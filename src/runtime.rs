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

use st3::fanout::{Pool, StdHost};

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
        let workers = workers.max(1);
        let host = Arc::new(StdHost::new(workers));
        let pool = Pool::new(workers, 1024, host);
        for id in 0..workers {
            let pool = pool.clone();
            let runner = pool.runner(id);
            std::thread::Builder::new()
                .name(alloc::format!("nagoya-{id}"))
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
