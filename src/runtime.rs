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

use core::num::NonZeroUsize;
use st3::fanout::{Pool, StdHost, Tuning};

use crate::{Executor, JoinHandle};

/// A work-stealing pool that started its own threads.
///
/// Dropping this does **not** stop the threads: they are detached, because the
/// tasks on them are the ones nobody else is watching, and tearing them down
/// under a running sweep is worse than letting them run. A caller that wants
/// them stopped stops the work, not the runtime.
pub struct Runtime {
    /// The parking host, kept so its spurious-wake count can be read back.
    ///
    /// A worker that parks and wakes with no work to show for it is pure
    /// overhead, and this is the one number that says how often that happens.
    /// It is exact rather than sampled, which matters here: a profiler cannot
    /// see this workload at all, because the echo is short enough that every
    /// worker is parked in almost every wall-clock sample at any pool size.
    parking: Arc<StdHost>,
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
    /// Worker identity is installed automatically. Callers owning their own
    /// threads can instead use [`Executor::run_worker`]; custom/no_std hosts
    /// can supply [`crate::WorkerContext`] without creating an owned runtime.
    ///
    /// # Panics
    ///
    /// If a thread cannot be started, or `workers` exceeds `usize::BITS`,
    /// the underlying pool's worker bitmap capacity.
    #[must_use]
    pub fn with_tuning(workers: usize, tuning: Tuning, label: &str) -> Self {
        // Built directly rather than from `builder()`, whose default count asks
        // the platform for a number this is about to replace.
        Builder {
            workers,
            tuning,
            label: alloc::string::String::from(label),
            stack_size: None,
        }
        .build()
    }

    /// Everything [`Runtime::with_tuning`] sets, plus what it does not, such as
    /// the worker threads' stack size.
    ///
    /// Starts at what [`background`] is built with: one worker per fast core,
    /// the default tuning, the label `nagoya` and `std`'s default stack. So a
    /// caller that wants the shared pool's shape with one thing changed says
    /// only that thing.
    pub fn builder() -> Builder {
        Builder::new()
    }

    fn start(builder: &Builder) -> Self {
        let workers = builder.workers.max(1);
        assert!(
            workers <= usize::BITS as usize,
            "worker count exceeds the pool bitmap capacity"
        );
        let host = Arc::new(StdHost::new(workers));
        let parking = host.clone();
        let pool = Pool::with_tuning(workers, 1024, host, builder.tuning);
        let label = &builder.label;
        for id in 0..workers {
            let pool = pool.clone();
            let runner = pool.runner(id);
            let mut thread = std::thread::Builder::new().name(alloc::format!("{label}-{id}"));
            if let Some(bytes) = builder.stack_size {
                thread = thread.stack_size(bytes);
            }
            thread
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
            parking,
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

    /// Parks that woke with no signal to consume, since this runtime started.
    ///
    /// Spurious in the pool's sense: the worker slept, something woke it, and
    /// there was nothing for it. Rising much faster than the work does is what
    /// an oversized pool looks like from the inside.
    #[must_use]
    pub fn spurious_wakes(&self) -> u64 {
        self.parking.spurious()
    }

    /// The executor underneath, for a caller that wants to hand it on.
    #[must_use]
    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    /// The pool underneath, for the APIs that take one directly.
    ///
    /// [`crate::par_for`] takes an `Arc<Pool>` because it schedules closures
    /// without building async tasks.
    #[must_use]
    pub fn pool(&self) -> &Arc<Pool> {
        self.executor.pool()
    }
}

/// A [`Runtime`] described before it is started, from [`Runtime::builder`].
///
/// Exists for the settings that [`Runtime::with_tuning`] has no argument for.
/// Adding one there would break every caller; adding one here breaks none.
///
/// # Stack size
///
/// A worker's stack is where every task it polls runs, so it bounds how deep a
/// task can recurse. Unset, a worker gets what `std::thread` gives any thread:
/// `RUST_MIN_STACK` when the process has it, two MiB when it does not. That
/// makes the stack a property of whoever launched the process, which is wrong
/// for a caller whose tasks recurse by design and know how far. Such a caller
/// sets it here, and the environment stops mattering.
#[derive(Clone, Debug)]
#[must_use]
pub struct Builder {
    workers: usize,
    tuning: Tuning,
    label: alloc::string::String,
    stack_size: Option<usize>,
}

impl Builder {
    fn new() -> Self {
        Self {
            workers: shared_threads(),
            tuning: Tuning::default(),
            label: alloc::string::String::from("nagoya"),
            stack_size: None,
        }
    }

    /// Worker threads. Zero means one, as in [`Runtime::new`]. Unset, the
    /// count [`background`] uses.
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    /// The idle policy. [`Tuning::default`] unless set.
    pub fn tuning(mut self, tuning: Tuning) -> Self {
        self.tuning = tuning;
        self
    }

    /// Threads are named `{label}-{id}`. `nagoya` unless set.
    pub fn label(mut self, label: &str) -> Self {
        self.label = alloc::string::String::from(label);
        self
    }

    /// Each worker thread's stack, in bytes.
    ///
    /// Rounded up by the platform to its page size and minimum, as
    /// `std::thread::Builder::stack_size` is. See the type's documentation for
    /// what happens when it is not set.
    pub fn stack_size(mut self, bytes: usize) -> Self {
        self.stack_size = Some(bytes);
        self
    }

    /// Start the threads.
    ///
    /// # Panics
    ///
    /// As [`Runtime::with_tuning`] does: if a thread cannot be started, which
    /// includes a stack the platform will not allocate, or if the worker count
    /// exceeds `usize::BITS`.
    #[must_use]
    pub fn build(&self) -> Runtime {
        Runtime::start(self)
    }
}

/// Threads for the shared pool.
///
/// The machine's parallelism, except that on a machine with more than one
/// class of core it is the fast class's count rather than the total.
///
/// # Why not `available_parallelism`
///
/// On a heterogeneous CPU that number counts every core the scheduler will
/// hand out, and those are not interchangeable. This laptop reports sixteen:
/// twelve performance cores and four efficiency cores that run at a fraction
/// of the speed. A pool sized at sixteen therefore puts a quarter of its
/// workers somewhere a task takes several times longer, and work-stealing
/// does not rescue a task already running on a slow core, it only moves what
/// has not started.
///
/// So prefer the fast class where the platform names one. It is a floor on
/// nothing: a caller who wants every core still asks for it by building a
/// `Runtime` with the count it wants.
fn shared_threads() -> usize {
    supported_default_threads(
        performance_cores()
            .or_else(|| {
                std::thread::available_parallelism()
                    .ok()
                    .map(NonZeroUsize::get)
            })
            .unwrap_or(2),
    )
}

/// Cores in the fastest class, where the platform distinguishes classes.
///
/// `None` when it does not, or when the answer is not usable, in which case
/// the caller falls back to total parallelism.
#[cfg(target_vendor = "apple")]
fn performance_cores() -> Option<usize> {
    // `hw.perflevel0` is the fast class on Apple silicon, `perflevel1` the
    // efficiency one. Absent on Intel Macs, which are homogeneous, and the
    // failure there is the right one: fall back to the total.
    sysctl_usize(c"hw.perflevel0.logicalcpu").filter(|count| *count > 0)
}

/// The same query, on a platform that does not classify cores.
#[cfg(not(target_vendor = "apple"))]
fn performance_cores() -> Option<usize> {
    None
}

/// One integer `sysctl`, by name.
#[cfg(target_vendor = "apple")]
fn sysctl_usize(name: &core::ffi::CStr) -> Option<usize> {
    let mut value: core::ffi::c_int = 0;
    let mut size = core::mem::size_of::<core::ffi::c_int>();
    // SAFETY: `name` is a nul-terminated C string, and the out pointer and
    // its length describe the same `c_int` for the duration of the call.
    let status = unsafe {
        sysctlbyname(
            name.as_ptr(),
            core::ptr::from_mut(&mut value).cast(),
            &mut size,
            core::ptr::null_mut(),
            0,
        )
    };
    (status == 0 && value > 0).then_some(value as usize)
}

#[cfg(target_vendor = "apple")]
extern "C" {
    fn sysctlbyname(
        name: *const core::ffi::c_char,
        oldp: *mut core::ffi::c_void,
        oldlenp: *mut usize,
        newp: *mut core::ffi::c_void,
        newlen: usize,
    ) -> core::ffi::c_int;
}

fn supported_default_threads(available: usize) -> usize {
    available.clamp(1, usize::BITS as usize)
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
/// Route one reactor wake to the worker its descriptor belongs to.
///
/// Spreads descriptors over the pool by index while keeping each one on a
/// stable worker, so a task finds its own cache lines without every
/// connection sharing a queue.
pub(crate) fn route_reactor_wake(index: u64) -> Option<crate::task::RouteGuard> {
    // Only route onto a pool that is already running. Asking `background()`
    // for its width *starts* it, one thread per core, and the reactor is not
    // a caller that should be making that decision: a consumer with its own
    // `Runtime` was silently getting a second pool it never asked for, and
    // the modulus came from that pool's width rather than its own, so wakes
    // were routed to worker indices its runtime did not have.
    //
    // With no shared pool running there is nothing to route onto and the wake
    // goes wherever the waker sends it, which is the right answer rather than
    // a fallback.
    let pool = SHARED.get()?.pool();
    let worker = (index as usize) % pool.workers().max(1);
    Some(crate::task::route_wakes(worker))
}

/// A caller that wants its own threads, its own count, or a pool it can stop
/// still builds a [`Runtime`] directly. This is the convenience, not the API.
pub fn background() -> &'static Runtime {
    SHARED.get_or_init(|| Runtime::new(shared_threads()))
}

/// The shared pool, once something has asked for it.
///
/// Named rather than function-local so that [`route_reactor_wake`] can ask
/// whether it exists without bringing it into existence.
static SHARED: std::sync::OnceLock<Runtime> = std::sync::OnceLock::new();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;
    use core::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn automatic_worker_count_respects_the_bitmap_capacity() {
        assert_eq!(supported_default_threads(0), 1);
        assert_eq!(supported_default_threads(2), 2);
        assert_eq!(supported_default_threads(usize::MAX), usize::BITS as usize);
    }

    /// The default pool does not size itself onto the slow cores.
    ///
    /// Only asserts the relationship, not a count: the numbers are this
    /// machine's, and the property has to hold on a homogeneous one too,
    /// where the fast class is the whole machine and the two are equal.
    #[test]
    fn the_default_pool_prefers_the_fast_cores() {
        let total = std::thread::available_parallelism().map_or(2, core::num::NonZeroUsize::get);
        let chosen = shared_threads();

        assert!(chosen >= 1, "a pool needs a worker");
        assert!(
            chosen <= supported_default_threads(total),
            "chose {chosen} workers, more than the {total} cores the machine has"
        );

        if let Some(fast) = performance_cores() {
            assert_eq!(
                chosen,
                supported_default_threads(fast),
                "a machine reporting {fast} fast cores of {total} should size to the fast ones"
            );
        }
    }

    #[test]
    fn an_owned_runtime_installs_worker_identity() {
        let runtime = Runtime::with_tuning(1, Tuning::locality(), "identity-test");
        let pool = runtime.pool().clone();
        let task = runtime.spawn(async move { crate::task::current_worker(&pool) });
        let worker = block_on(task);
        runtime.pool().shut_down();
        assert_eq!(worker, Some(Some(0)));
    }

    /// A task that needs more stack than `std`'s two MiB default runs on a
    /// runtime that asked for more.
    ///
    /// Four MiB of locals overflows a default worker, so this fails without
    /// the setting unless the process was started with a large
    /// `RUST_MIN_STACK`, which is the dependence the setting exists to remove.
    #[test]
    fn a_built_runtime_gives_its_workers_the_stack_it_asked_for() {
        let runtime = Runtime::builder()
            .workers(1)
            .label("stack-test")
            .stack_size(16 * 1024 * 1024)
            .build();
        let task = runtime.spawn(async {
            let buffer = core::hint::black_box([1_u8; 4 * 1024 * 1024]);
            buffer.iter().map(|&byte| usize::from(byte)).sum::<usize>()
        });
        let sum = block_on(task);
        runtime.pool().shut_down();
        assert_eq!(sum, Some(4 * 1024 * 1024));
    }

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
