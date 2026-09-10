//! An async runtime that does not need an operating system.
//!
//! # What this is
//!
//! A `spawn`, a `JoinHandle` and a `block_on` over
//! [`st3::fanout`](https://docs.rs/ps-st3), which is a work-stealing pool
//! whose entire contact with a platform is three methods: park, unpark, and a
//! clock. Everything here is `no_std` and reaches nothing further.
//!
//! # What this is not
//!
//! Not Tokio. There is no I/O driver, and there will not be one in this crate:
//! an epoll or io_uring reactor is precisely the part that needs an operating
//! system, and being able to run without one is the only interesting thing
//! here. A caller that wants sockets brings its own reactor.
//!
//! # The surface worth having, measured rather than guessed
//!
//! Counted across this codebase's own use of Tokio, the scheduler is the
//! minority of it. In `a trading backend`:
//!
//! ```text
//! 34  sync::RwLock        13  time::interval    7  sync::Notify
//! 19  time::sleep         13  sync::Mutex       7  join!
//! 12  task::spawn_local   12  sync::OnceCell    6  sync::Semaphore
//! 11  time::Instant       10  select!           4  task::JoinHandle
//! ```
//!
//! Async `sync` primitives and `time` are two thirds of it, and `select!` and
//! `join!` are pure combinators that touch no runtime at all. All of them are
//! reachable without an operating system; only `time` needs even a clock, and
//! `Host::now_ns` already provides one. That is the order this should be built
//! in, and the executor below is only the part everything else needs first.

#![cfg_attr(not(feature = "std"), no_std)]
#![deny(missing_docs)]

extern crate alloc;

use alloc::sync::Arc;
use core::future::Future;
use core::pin::Pin;

use core::task::{Context, Poll};

use st3::fanout::Pool;

mod block_on;
pub mod io;
mod par;
// Owns threads, so it needs `std`. See its module comment for why a crate built
// not to own threads carries one that does.
#[cfg(feature = "std")]
pub mod runtime;
// The four async primitives a consumer was otherwise taking from `tokio::sync`.
// They are runtime-agnostic there too, which is exactly why the dependency was
// easy to acquire and hard to notice: see the module comment.
pub mod sync;
mod task;
mod time;
mod yield_now;

pub use block_on::block_on;
pub use par::{par_for, Cancel, ParFor};
/// The idle policy a pool runs with, from the queues underneath.
///
/// Re-exported because selecting one is a nagoya-level decision and every
/// caller that makes it would otherwise have to name `ps-st3` as a direct
/// dependency to spell the type, for one struct, in a graph that already
/// contains it transitively.
pub use st3::fanout::Tuning;
pub use time::{
    now_ns, poll as poll_timers, set_clock, sleep, sleep_until, timeout, Elapsed, Sleep, Timeout,
};
pub use yield_now::{yield_now, YieldNow};

/// A handle to a spawned task's output.
///
/// Awaiting it yields `Some(output)`, or `None` if the task was cancelled by
/// dropping the handle before it finished.
///
/// **Dropping this detaches the task; it does not cancel it.** That is Tokio's
/// behaviour and it was this crate's before, and it is worth stating because
/// the `async_task::Task` underneath does the opposite: dropping *that* cancels.
/// Preserving the documented behaviour cost one `Drop` impl and finding out the
/// hard way, when a benchmark that spawns and drops handles stopped finishing.
/// [`cancel`](JoinHandle::cancel) is how to ask for the other thing.
pub struct JoinHandle<T> {
    /// `None` only between `Drop` taking it and the handle going away.
    task: Option<async_task::Task<T>>,
}

impl<T> JoinHandle<T> {
    fn task(&mut self) -> &mut async_task::Task<T> {
        self.task
            .as_mut()
            .expect("the task is taken only by `Drop`")
    }

    /// Stop the task at its next suspension point and throw away its output.
    ///
    /// A task already past its last poll finishes anyway: cancelling is a
    /// request not to poll again, not a way to undo work already done.
    pub fn cancel(mut self) {
        if let Some(task) = self.task.take() {
            drop(task);
        }
    }

    /// Whether the task has finished, without waiting for it.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .expect("the task is taken only by `Drop`")
            .is_finished()
    }
}

/// Deliberately not derived: a derive would demand `T: Debug` from every
/// consumer that puts a handle in a `#[derive(Debug)]` struct, and a handle's
/// output type is exactly the thing they cannot see yet.
impl<T> core::fmt::Debug for JoinHandle<T> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("JoinHandle")
            .field(
                "finished",
                &self
                    .task
                    .as_ref()
                    .is_some_and(async_task::Task::is_finished),
            )
            .finish()
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.detach();
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(self.get_mut().task()).poll(context).map(Some)
    }
}

/// A pool with a place to put the next task.
///
/// Work goes to one injector that every worker drains, so there is nothing for
/// a spawn to choose: the pool decides who runs it.
pub struct Executor {
    pool: Arc<Pool>,
}

impl Executor {
    /// An executor over an existing pool.
    ///
    /// The pool is not started here. Its threads are the caller's to provide,
    /// which is what keeps this free of an operating system.
    #[must_use]
    pub fn new(pool: Arc<Pool>) -> Self {
        Self { pool }
    }

    /// Another handle to the same pool, for a task that spawns.
    ///
    /// Spawning from inside a task needs an executor the task can own, and this
    /// is one: another handle to the same pool.
    #[must_use]
    pub fn clone_handle(&self) -> Self {
        Self::new(self.pool.clone())
    }

    /// The pool this executor spawns onto.
    #[must_use]
    pub fn pool(&self) -> &Arc<Pool> {
        &self.pool
    }

    /// Run a future to completion somewhere on the pool.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle {
            task: Some(task::spawn(future, self.pool.clone())),
        }
    }
}
