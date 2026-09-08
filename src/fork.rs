//! Fork-join, scoped spawning, and the three hooks a parallel compiler needs.
//!
//! # What this is for
//!
//! `rustc`'s parallel front end runs queries on a work-stealing pool, and a
//! query can block on another query's result. That makes a cycle possible, and
//! a cycle among blocked threads is a deadlock the runtime has to *notice*
//! rather than hang on. `rustc_thread_pool` notices it with three callbacks and
//! two counters, and this is the same design:
//!
//! * **`on_release`** runs when a thread is about to block, so a jobserver can
//!   give the token back.
//! * **`on_acquire`** runs when it resumes, so the token can be reclaimed.
//! * **`on_deadlock`** runs when every thread is blocked and none is running,
//!   which is `active == 0 && blocked > 0`, exactly the predicate
//!   `rustc_thread_pool`'s `deadlock_check` uses.
//!
//! [`Registry::mark_blocked_and_wait`] is the whole protocol in one call:
//! count this thread as blocked, check for deadlock, release, wait, acquire.
//!
//! # How this differs from rayon, and why
//!
//! **A blocked thread here does not go and run other work.** `rayon::join`
//! blocks on a latch and, while blocked, keeps stealing; that needs a
//! thread-local handle to the worker the caller is running on, and this crate
//! has no such thing yet. So [`Registry::join`] runs one half on the calling
//! thread, gives the other to the pool, and then waits.
//!
//! That is a real difference and it costs a worker for the duration. It is also
//! exactly the case the hooks exist to make visible: a thread that blocks here
//! says so, and a deadlock detector or a jobserver sees it.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::marker::PhantomData;
use core::sync::atomic::{AtomicUsize, Ordering};

use spin::Mutex;
use st3::fanout::Pool;

/// A callback with no arguments, shared across threads.
pub type Hook = Arc<dyn Fn() + Send + Sync>;

/// The three callbacks a blocking-aware caller can install.
///
/// All optional. A pool with none of them behaves exactly as it did before,
/// and the counters are still kept so [`Registry::is_deadlocked`] can be asked.
#[derive(Clone, Default)]
pub struct Hooks {
    /// Runs when a thread resumes after blocking.
    pub on_acquire: Option<Hook>,
    /// Runs when a thread is about to block.
    pub on_release: Option<Hook>,
    /// Runs when every thread is blocked and none is running.
    pub on_deadlock: Option<Hook>,
}

impl core::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Hooks")
            .field("on_acquire", &self.on_acquire.is_some())
            .field("on_release", &self.on_release.is_some())
            .field("on_deadlock", &self.on_deadlock.is_some())
            .finish()
    }
}

/// How many threads are running and how many are blocked.
///
/// Under one lock rather than two atomics, because the deadlock predicate reads
/// both and has to see them agree. Two independent atomics can be observed
/// mid-transition as `active == 0 && blocked > 0` when in truth a thread is
/// between decrementing one and incrementing the other, and a spurious deadlock
/// report is worse than none. `rustc_thread_pool` takes a `Mutex` here too.
#[derive(Debug)]
struct Counts {
    active: usize,
    blocked: usize,
}

/// A pool, the hooks, and the count of who is blocked on it.
pub struct Registry {
    pool: Arc<Pool>,
    counts: Mutex<Counts>,
    hooks: Hooks,
}

impl core::fmt::Debug for Registry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let counts = self.counts.lock();
        f.debug_struct("Registry")
            .field("active", &counts.active)
            .field("blocked", &counts.blocked)
            .field("hooks", &self.hooks)
            .finish()
    }
}

impl Registry {
    /// A registry over `pool` with no hooks installed.
    #[must_use]
    pub fn new(pool: Arc<Pool>) -> Arc<Self> {
        Self::with_hooks(pool, Hooks::default())
    }

    /// A registry whose blocking is reported to `hooks`.
    #[must_use]
    pub fn with_hooks(pool: Arc<Pool>, hooks: Hooks) -> Arc<Self> {
        let active = pool.workers();
        Arc::new(Self {
            pool,
            counts: Mutex::new(Counts { active, blocked: 0 }),
            hooks,
        })
    }

    /// The pool this registry is over.
    #[must_use]
    pub fn pool(&self) -> &Arc<Pool> {
        &self.pool
    }

    /// Whether every thread is blocked and none is running.
    ///
    /// This is what [`Hooks::on_deadlock`] fires on. Reading it is a snapshot
    /// and it can be stale the moment it returns; it is here for a test or a
    /// diagnostic, not as something to make a decision on.
    #[must_use]
    pub fn is_deadlocked(&self) -> bool {
        let counts = self.counts.lock();
        counts.active == 0 && counts.blocked > 0
    }

    /// Block on `wait`, announcing it, and report a deadlock if this was the
    /// last running thread.
    ///
    /// The order is the one `rustc_thread_pool::mark_blocked_and_wait` uses and
    /// it matters: the count changes and the deadlock check happen **before**
    /// the thread releases and blocks, so a detector woken by `on_deadlock`
    /// sees a consistent picture rather than a thread that has announced
    /// nothing yet.
    pub fn mark_blocked_and_wait(&self, wait: impl FnOnce()) {
        {
            let mut counts = self.counts.lock();
            counts.active = counts.active.saturating_sub(1);
            counts.blocked += 1;
            let deadlocked = counts.active == 0 && counts.blocked > 0;
            // The lock is dropped before the callback. A handler that touches
            // this registry, which a deadlock handler is very likely to do,
            // would otherwise deadlock on the deadlock detector.
            drop(counts);
            if deadlocked {
                if let Some(hook) = &self.hooks.on_deadlock {
                    hook();
                }
            }
        }
        if let Some(hook) = &self.hooks.on_release {
            hook();
        }
        wait();
        if let Some(hook) = &self.hooks.on_acquire {
            hook();
        }
        self.mark_unblocked();
    }

    /// Count a thread as running again, having been counted as blocked.
    ///
    /// [`mark_blocked_and_wait`](Registry::mark_blocked_and_wait) already does
    /// this. It is public for a caller that unblocks a thread from the outside,
    /// which is how `rustc` resumes a query whose result arrived.
    pub fn mark_unblocked(&self) {
        let mut counts = self.counts.lock();
        counts.active += 1;
        counts.blocked = counts.blocked.saturating_sub(1);
    }

    /// Run both closures, `b` on the pool and `a` here, and return both results.
    ///
    /// **The calling thread blocks until `b` finishes**, announcing it through
    /// the hooks. See the module documentation for why it does not steal other
    /// work while it waits.
    pub fn join<A, B, RA, RB>(self: &Arc<Self>, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        let mut right = None;
        // One-element scope: `b` borrows the stack, `scope` guarantees it has
        // finished before returning, and `a` runs here meanwhile.
        let left = self.scope(|scope| {
            scope.spawn(|_| right = Some(b()));
            a()
        });
        (left, right.expect("the scope does not return until its work is done"))
    }

    /// Run `body` with a scope that can spawn work borrowing the caller's stack.
    ///
    /// # Why this is sound
    ///
    /// Work handed to the pool has to be `'static`, and the closures here are
    /// not: they borrow whatever `body`'s frame holds. What makes it safe is
    /// that **`scope` does not return until every piece it spawned has run**,
    /// including when `body` panics, so no job can outlive the frame it
    /// borrowed. The `unsafe` below is that promise, and the `Drop` guard is
    /// what keeps it during unwinding.
    pub fn scope<'scope, F, R>(self: &Arc<Self>, body: F) -> R
    where
        F: FnOnce(&Scope<'scope>) -> R,
    {
        let scope = Scope {
            registry: self.clone(),
            outstanding: AtomicUsize::new(0),
            marker: PhantomData,
        };
        // Waits in `Drop`, so an unwind out of `body` still waits rather than
        // freeing a frame the pool is still reading.
        let guard = Waiting { scope: &scope };
        let out = body(&scope);
        drop(guard);
        out
    }
}

/// Waits for a scope's work on the way out, panic or not.
struct Waiting<'a, 'scope> {
    scope: &'a Scope<'scope>,
}

impl Drop for Waiting<'_, '_> {
    fn drop(&mut self) {
        self.scope.wait();
    }
}

/// A place to put work that borrows the caller's stack.
///
/// Created by [`Registry::scope`].
pub struct Scope<'scope> {
    registry: Arc<Registry>,
    outstanding: AtomicUsize,
    /// Ties the scope to `'scope` so a spawned closure cannot outlive it.
    marker: PhantomData<&'scope mut &'scope ()>,
}

impl<'scope> Scope<'scope> {
    /// Put `body` on the pool. It may borrow anything outliving the scope.
    pub fn spawn<F>(&self, body: F)
    where
        F: FnOnce(&Scope<'scope>) + Send + 'scope,
    {
        self.outstanding.fetch_add(1, Ordering::AcqRel);

        // The pool takes `'static` work, and this is not `'static`. Erasing the
        // lifetime is sound only because the scope waits, which is what the
        // `Waiting` guard in `Registry::scope` enforces on every path out.
        let scope: &'scope Scope<'scope> = unsafe { &*core::ptr::from_ref(self) };
        let boxed: Box<dyn FnOnce() + Send + 'scope> = Box::new(move || {
            body(scope);
            scope.outstanding.fetch_sub(1, Ordering::AcqRel);
        });
        // SAFETY: the closure borrows only things that outlive `'scope`, and
        // `Registry::scope` does not return until `outstanding` reaches zero,
        // so every one of these has finished before any of that is dropped.
        let boxed: Box<dyn FnOnce() + Send + 'static> = unsafe { core::mem::transmute(boxed) };
        self.registry.pool.submit_fn(boxed);
    }

    /// The registry this scope spawns onto, for nested work.
    #[must_use]
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Block until everything spawned into this scope has run.
    fn wait(&self) {
        if self.outstanding.load(Ordering::Acquire) == 0 {
            return;
        }
        self.registry.mark_blocked_and_wait(|| {
            while self.outstanding.load(Ordering::Acquire) != 0 {
                core::hint::spin_loop();
            }
        });
    }
}
