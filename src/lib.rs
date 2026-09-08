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
mod task;

pub use block_on::block_on;

/// A handle to a spawned task's output.
///
/// Awaiting it yields `Some(output)`, or `None` if the output was already
/// taken. Unlike Tokio's, dropping this does **not** detach-and-forget any
/// differently from holding it: the task runs to completion either way, because
/// there is nothing here that could cancel it.
pub struct JoinHandle<T> {
    task: Arc<dyn task::Joinable<T>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.task.poll_output(context)
    }
}

/// A pool with a place to put the next task.
///
/// Spawning round-robins across workers. The pool steals, so a bad choice
/// costs a steal rather than a stall, and counting is cheaper than asking
/// which worker is least busy.
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
        Self {
            pool,
        }
    }

    /// Another handle to the same pool, for a task that spawns.
    ///
    /// Spawning from inside a task needs an executor the task can own, and
    /// this is one: the pool is shared, the round-robin counter is not, which
    /// costs nothing but a slightly different spread.
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
            task: task::RawTask::spawn(future, self.pool.clone()),
        }
    }
}
