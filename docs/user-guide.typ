#set document(title: "Nagoya User Guide", author: "PathScale")
#set page(paper: "a4", margin: (x: 2.2cm, y: 2.4cm), numbering: "1")
#set text(font: ("Helvetica", "Arial"), size: 10pt)
#set par(leading: 0.62em)
#show heading: set block(above: 1.4em, below: 0.7em)
#show heading.where(level: 1): set text(size: 22pt, weight: "bold")
#show heading.where(level: 2): set text(size: 16pt, weight: "bold")
#show heading.where(level: 3): set text(size: 12pt, weight: "bold")
#show raw.where(block: true): it => block(
  fill: rgb("#f4f4f2"), inset: 9pt, radius: 3pt, width: 100%,
  breakable: false, text(size: 8pt, it),
)
#show link: set text(fill: rgb("#1a4f8a"))
= Nagoya user guide
<nagoya-user-guide>
This guide describes Nagoya 0.1.2, including `Executor::submit`.
That method is not available in 0.1.1.
Examples below are compiled as rustdoc tests, and examples that do not
require an external host also run as tests.

== Install and choose ownership
<install-and-choose-ownership>
Use `nagoya = "^0.1"` from crates.io, or
`nagoya = { path = "../nagoya" }` during local integration. Defaults
include `std`. `default-features = false` provides the executor, task
handles, parallel loops, synchronization, timers and portable I/O traits
without starting operating-system threads. It still requires `alloc` and
a host.

For ordinary applications start one owned runtime and pass its executor
to components. Use the process-wide `runtime::background()` only when
sharing a process-lifetime pool is intended. Repeatedly constructing
runtimes starts more worker pools; it does not reuse the background
runtime.

```rust
use nagoya::{block_on, runtime::Runtime};
let runtime = Runtime::new(2);
let task = runtime.spawn(async { 40 + 2 });
assert_eq!(block_on(task), Some(42));
runtime.pool().shut_down();
```

`Runtime::new(0)` uses one worker. Counts above `usize::BITS` panic
because the pool tracks workers in a bitmap. `background()` clamps the
machine's reported parallelism to that range. That reported count can
include slower efficiency cores; it is not a measurement of the best
worker count.

#strong[Lifetime:] dropping `Runtime` does not stop or join its detached
threads. Finish application work first. `runtime.pool().shut_down()`
requests pool shutdown; it is not an application task-drain barrier. If
joining worker threads matters, own the pool and worker handles as shown
below.

== Spawn, await, detach and cancel
<spawn-await-detach-and-cancel>
`Executor::spawn` and `Runtime::spawn` accept `Future + Send + 'static`
with `Send + 'static` output. Borrowed stack data must be moved into
owned values or an `Arc`. There is no `spawn_local` for non-`Send`
futures.

```rust
use nagoya::{block_on, runtime::Runtime};
use std::sync::Arc;
let runtime = Runtime::new(2);
let numbers = Arc::new(vec![2, 3, 5]);
let input = numbers.clone();
let task = runtime.executor().spawn(async move { input.iter().sum::<u64>() });
assert_eq!(block_on(task), Some(10));
runtime.pool().shut_down();
```

Awaiting a `JoinHandle<T>` returns `Option<T>`. `None` means the
runnable was canceled, for example when its pool was destroyed before
execution. With `std` and unwinding enabled, an async task panic is
rethrown when its handle is awaited; the worker survives. Abort-on-panic
builds cannot recover this way.

`handle.is_finished()` is an observation, not a join. Dropping the
handle #strong[detaches] the task. It keeps running; it is not canceled.
`handle.cancel()` consumes the handle and requests cancellation.
Cancellation cannot interrupt synchronous code already executing inside
a poll, and does not roll back side effects. Use explicit completion
signals when shutdown must wait.

`executor.clone_handle()` returns another handle to the same pool and
host context. It does not create workers. `executor.pool()` exposes the
`Arc<Pool>` for lower-level integration and `par_for`.

== Submit a synchronous closure
<submit-a-synchronous-closure>
`Executor::submit` runs an owned `FnOnce() + Send + 'static` closure. It
creates no async task or waker and returns no join handle. Choose it for
short work that cannot suspend. A closure can still allocate its capture
storage inside the pool; "no async task" is not "no allocation of any
kind."

```rust
use nagoya::runtime::Runtime;
use std::sync::mpsc;
use std::time::Duration;
let runtime = Runtime::new(2);
let (send, receive) = mpsc::channel();
runtime.executor().submit(move || send.send(42).unwrap());
assert_eq!(receive.recv_timeout(Duration::from_secs(2)).unwrap(), 42);
runtime.pool().shut_down();
```

The channel in this example is the completion contract. `submit` itself
has no cancellation, output, panic-reporting or drain handle. Pool
shutdown may prevent queued closures from running, so submit only while
the pool is live. Do not rely on async-task panic isolation for a raw
submitted closure.

== Drive a future and yield cooperatively
<drive-a-future-and-yield-cooperatively>
`block_on(future)` drives that future on the calling thread. It does not
create a pool, install a socket reactor, or turn blocking code
asynchronous. With `std` it parks on a waker; without `std` the
fallback polling loop spins. Use `block_on_with_host(future, host, waiter)`
for a blocking caller without Rust std. Await child tasks inside a worker instead
of blocking a worker waiting for work that needs the same exhausted
pool.

```rust
nagoya::block_on(async {
    nagoya::yield_now().await;
    assert_eq!(6 * 7, 42);
});
```

`yield_now()` returns a `YieldNow` future that yields once. It provides
a scheduling opportunity, not fairness, priority, or a promise that a
specific other task will run. CPU loops should expose useful work
boundaries.

== Parallel ranges and cancellation granularity
<parallel-ranges-and-cancellation-granularity>
`par_for(pool, start..end, body)` returns a lazy `ParFor` future. Work
starts when polled. The body accepts a `usize` index and is synchronous;
it must be safe to execute concurrently. Empty or reversed ranges finish
without work.

```rust
use nagoya::{block_on, par_for, runtime::Runtime};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
let runtime = Runtime::new(2);
let sum = Arc::new(AtomicUsize::new(0));
let output = sum.clone();
block_on(par_for(runtime.pool().clone(), 0..1000, move |i| {
    output.fetch_add(i, Ordering::Relaxed);
}).leaf(64));
assert_eq!(sum.load(Ordering::Relaxed), 999 * 1000 / 2);
runtime.pool().shut_down();
```

`.leaf(items)` controls the minimum split size, clamped to at least one.
Default is the range divided by eight times the worker count. Smaller
leaves increase scheduling overhead and improve cancellation
responsiveness. `.leaf` and `.cancel_with` have no effect after the loop
has started.

```rust
use nagoya::{block_on, par_for, Cancel, runtime::Runtime};
let runtime = Runtime::new(2);
let token = Cancel::new();
token.cancel();
let work = par_for(runtime.pool().clone(), 0..100, |_| {
    panic!("a canceled loop must not begin this body")
}).cancel_with(token.clone());
assert!(token.is_cancelled());
block_on(work);
runtime.pool().shut_down();
```

`work.cancel()` returns a cloneable `Cancel` token; `token.cancel()`
requests stopping. Dropping `ParFor` also cancels it, unlike dropping a
task handle. Already-running pieces may finish. Cancellation is checked
at piece boundaries and cannot undo completed writes. A panicking
parallel body stops its piece; do not treat partial output as a
successfully completed dataset.

== Timers and deadlines
<timers-and-deadlines>
`sleep(Duration)` sets a relative delay. `sleep_until(u64)` takes an
absolute nanosecond deadline in #strong[Nagoya's monotonic clock
domain], not a Unix timestamp or `std::time::Instant`. `now_ns()` reads
that domain; `Sleep::deadline()` exposes the selected deadline. Timers
may complete late when the machine or executor is busy. They are not
real-time guarantees.

```rust
use std::time::Duration;
nagoya::block_on(async {
    let sleep = nagoya::sleep(Duration::from_millis(1));
    let deadline = sleep.deadline();
    sleep.await;
    assert!(nagoya::now_ns() >= deadline);
    nagoya::sleep_until(nagoya::now_ns()).await;
    assert_eq!(nagoya::timeout(Duration::from_secs(1), async { 42 }).await, Ok(42));
});
```

`timeout(duration, future)` returns `Result<F::Output, Elapsed>`. A
future ready when polled at the deadline wins over the timer. Expiry
drops the wrapped future, not arbitrary external work it launched. In
particular, timing out a join handle detaches its task when the handle
is dropped; use an application cancellation token if the task must stop.
A long synchronous poll cannot be preempted by a timeout on that same
task.

With `std`, the timer driver starts on demand. Without `std`, install a
monotonic nanosecond clock using `set_clock(fn() -> u64)` before any
timer, then have a deferred-work or idle loop call `poll_timers(now)`.
It fires due wakers and returns the next deadline, or `None` when no
timers are pending. Only the first clock installation wins, including
lazy default installation. Do not replace a running process's clock
origin.

`poll_timers` takes locks, allocates and invokes wakers. It must not run
in an interrupt or signal handler that can preempt a timer operation.
Clock reads must use the same origin as submitted absolute deadlines.

== Synchronization
<synchronization>
These primitives are runtime-agnostic. They work on Nagoya or another
executor; importing them does not install a reactor. Pending acquisition
futures unregister on cancellation so abandoned waiters do not retain
wakers.

=== RwLock
<rwlock>
`RwLock::new(value)` offers `read().await` / `write().await` and
nonblocking `try_read()` / `try_write()` returning `Option<Guard>`.
Dropping guards releases access. `get_mut()` requires exclusive access
to the lock itself; `into_inner()` consumes it. Readers can share; a
writer excludes other access.

```rust
use nagoya::sync::RwLock;
use std::sync::Arc;
nagoya::block_on(async {
    let lock = Arc::new(RwLock::new(1usize));
    *lock.write().await += 1;
    assert_eq!(*lock.read().await, 2);
    let owned = lock.clone().read_owned().await;
    assert_eq!(*owned, 2);
    assert!(lock.try_write().is_none());
    drop(owned);
    *lock.clone().write_owned().await = 3;
});
```

The owned APIs are `read_owned`, `write_owned`, `try_read_owned` and
`try_write_owned`, with an `Arc<Self>` receiver. Their guards keep the
lock alive. Borrowed guards are `RwLockReadGuard` / `RwLockWriteGuard`\;
owned guards are `OwnedRwLockReadGuard` / `OwnedRwLockWriteGuard`. Avoid
holding guards across unrelated awaits, reentrant acquisition or slow
blocking I/O. There is no mutex, condition variable, channel or async
once-cell API here.

=== Notify
<notify>
`Notify::new()` / `Default` creates a notification source.
`notify_one()` wakes one waiter or retains one spare permit. Repeated
calls without waiters do not form a counting semaphore.
`notify_waiters()` wakes the current set and does not retain a permit
for future waiters.

```rust
let signal = nagoya::sync::Notify::new();
signal.notify_one();
nagoya::block_on(signal.notified());
```

Use `notified()` to create a `Notified` future. For a queue with
multiple consumers, create and pin it, call `enable()` #strong[before]
checking the queue, then await if the queue is empty. This registers the
waiter before a producer can signal between the predicate check and the
await. Recheck the predicate after every notification.
`Pin<&mut Notified>::enable()` reports whether the notification is
already ready.

=== Barrier
<barrier>
`Barrier::new(n)` releases a generation once all `n` callers reach
`wait().await`\; its bool result marks one leader. It is reusable across
generations. A barrier of one returns immediately. Every intended
participant must arrive: cancellation is not a replacement for an
arrival.

```rust
assert!(nagoya::block_on(nagoya::sync::Barrier::new(1).wait()));
```

=== Semaphore
<semaphore>
`Semaphore::new(n)` limits concurrent work. `acquire().await` returns a
`SemaphorePermit`\; `try_acquire()` is nonblocking. Dropping a permit
returns it. `permit.forget()` consumes it without returning capacity.
`available_permits()` is a snapshot; `add_permits(n)` adds capacity and
wakes waiters. There is no close protocol or multi-permit acquisition
API.

```rust
let semaphore = nagoya::sync::Semaphore::new(2);
let permit = semaphore.try_acquire().unwrap();
assert_eq!(semaphore.available_permits(), 1);
drop(permit);
assert_eq!(semaphore.available_permits(), 2);
```

== Files and portable I/O
<files-and-portable-io>
`nagoya::io::{Read, Write, Seek, File}` are portable traits. `Read`
supplies `read`, `read_exact`, and `read_to_end`\; `Write` supplies
`write`, `write_all`, and `flush`. `Seek::seek` uses
`SeekFrom::{Start, End, Current}`. `File` adds `length`, `set_length`,
`sync_all`, and `sync_data`. `Error::kind()` returns the crate's
portable `ErrorKind`\; `Error::new(kind)` lets a custom host construct
one. Short reads/writes are handled by the `*_exact` / `*_all` helpers;
EOF remains an error for `read_exact`.

With `std`, `HostFile::new(std::fs::File)` adapts a file and `get_ref()`
exposes the underlying handle. Free functions are `open`, `create`,
`open_or_create`, `append`, `read`, `write`, `metadata`, `remove_file`,
`remove_dir_all`, `rename` and `create_dir_all`. `metadata` returns file
length, not a full metadata object. `create` truncates; `open_or_create`
preserves an existing file. File methods block the calling thread
despite their async signatures. Put long disk work on threads owned for
that purpose.

// doctest: no_run
```rust
use nagoya::io::{Read, Write, Seek, SeekFrom, File};
nagoya::block_on(async {
    let mut file = nagoya::io::create("example.data").await?;
    file.write_all(b"hello").await?;
    file.flush().await?;
    file.sync_data().await?;
    file.seek(SeekFrom::Start(0)).await?;
    let mut bytes = [0u8; 5];
    file.read_exact(&mut bytes).await?;
    assert_eq!(&bytes, b"hello");
    Ok::<_, nagoya::io::Error>(())
})?;
```

`flush` is not a durable-storage guarantee. Choose `sync_data` or
`sync_all` at an application-defined durability boundary; neither
creates transactional multi-file atomicity. Network filesystems may have
additional semantics.

== Hosts, worker identity and no\_std
<hosts-worker-identity-and-no_std>
`Executor::new(Arc<Pool>)` does not start workers. An embedding host
owns pool construction and scheduling. `Executor::run_worker(runner)`
installs the worker identity for the duration of `Pool::run` on std
hosts. Calling the pool directly bypasses this marker and can change
local-wake routing.

```rust
use nagoya::Executor;
use st3::fanout::{Pool, StdHost};
use std::{sync::Arc, thread};
let pool = Pool::new(1, 256, Arc::new(StdHost::new(1)));
let executor = Executor::new(pool.clone());
let worker_executor = executor.clone_handle();
let runner = pool.runner(0);
let worker = thread::spawn(move || worker_executor.run_worker(runner));
assert_eq!(nagoya::block_on(executor.spawn(async { 42 })), Some(42));
pool.shut_down();
assert!(worker.join().unwrap());
```

This lower-level example needs a direct dependency on `ps-st3` with
`fanout` and `host`\; its library import name is `st3`. Use a registry
patch when testing a local ps-st3 so every dependency refers to the same
crate instance.

`Executor::with_worker_context(pool, Arc<dyn WorkerContext>)` is the
custom host route. Implement `current_worker(&Pool) -> Option<usize>`
according to the trait signature in the API reference: report an ID only
while executing on a worker of that exact pool. Returning an unrelated
pool's ID can route work to the wrong lane. No\_std hosts provide
scheduling, allocation and the timer-driving contract; disabling `std`
does not supply these services.

Hosts may also implement `with_current_worker(&mut dyn FnMut(&Pool, usize))`
to visit a pool borrowed from the active worker scope. Keep an Arc lease in
that scope, call the visitor synchronously, and never retain it. Nagoya checks
the pool and worker ID before consuming a runnable. This avoids a pool-wide
reference-count update on each local wake. The default method preserves the
`current_worker` fallback. Queued tasks retain only a weak pool reference,
so they cannot keep their containing pool alive in a cycle.

=== Blocking without Rust std

A host may use libc and OS wait primitives without linking Rust std. Enable
`ps-st3`'s `atomic-host` feature with default features off and construct
`AtomicHost::new(workers, clock)`. Its clock is a `fn() -> u64` returning
monotonic nanoseconds. Workers block through atomic-wait platform calls and
wake permits; there is no periodic timeout. `spurious()` counts platform
returns without a signal. This host does not create threads or drive timers.

A blocking caller also needs a wait mechanism. `block_on_with_host` accepts
an `Arc<dyn Host>` and a dedicated waiter slot, available with or without
Rust std. Reserve that slot for this caller; do not reuse a worker's slot or
share it between simultaneous or nested blocking calls. Cloned wakers keep the
host alive and may signal after completion, so the slot must remain valid.

```rust
use nagoya::block_on_with_host;
use st3::fanout::AtomicHost;
use std::sync::Arc;
// This example uses no timers, so its clock is unused.
let caller = Arc::new(AtomicHost::new(1, || 0));
let answer = block_on_with_host(async {
    nagoya::yield_now().await;
    42
}, caller, 0);
assert_eq!(answer, 42);
```

For a portable embedding, replace the example's std Arc import with
`alloc::sync::Arc`, supply its monotonic clock, and arrange worker lifecycle.
The suite's `portable-runtime` example provides a no_std Unix clock and
pthread TLS implementation of `WorkerContext`. Without worker identity,
self-wakes use the shared injector and can cost more CPU. Identity is a host
service; it does not require Rust's thread-local macro.

== Tuning and measurement
<tuning-and-measurement>
`Runtime::with_tuning(workers, tuning, label)` names worker threads
`label-id` and installs worker identity. `Tuning` is re-exported from
ps-st3. The default is locality with four empty search rounds and 128 spin
hints per round before host parking. Use `Tuning::default()` or its named presets and change one
public field at a time. Worker count, queue batching, local wake
routing, private-task sharing, and idle spin/backoff policy trade
throughput, tail latency and CPU use.

The Rust setters are `with_rounds_before_park`, `with_backoff_spins`,
`with_injector_batch`, `with_lifo_run_limit`, `with_local_wakes`,
`with_promote_every`, `with_share_displaced` and `with_stealable_inbox`.
The last exposes displaced jobs through a stealable FIFO inbox, while retaining
a warm LIFO slot that becomes stealable at its fairness quota. It takes precedence over `share_displaced`; it has no
effect when local wake routing is disabled. Smaller LIFO quotas offer other
work more frequent opportunities. A quota boundary with a ready local job does
not count as idle work.

Start with `Tuning::default()`. It is the strongest measured generic policy in
the release grids and keeps its locality advantage without the long-read weakness
of the immediate-parking policy.

The opt-in Rust preset `Tuning::almost_tokio()` combines a three-poll warm
slot with a stealable FIFO inbox and parks after an unsuccessful work search,
without idle spin rounds. It preserves no_std support and adds no DSL grammar.
This is a Nagoya policy, not the Tokio runtime. Dated benchmark reports called
it `parking`; the earlier aggressive-spin `almost_tokio` preset was retired.
For example:

```rust
let rt = nagoya::runtime::Runtime::with_tuning(
    8, nagoya::Tuning::almost_tokio(), "almost_tokio");
```

WorkTable's six named flavors are WorkTable registry policy, not six
different Nagoya executors. Distinct flavor pools can interfere through
idle spinning, so performance comparisons should isolate them in child
processes. Report CPU alongside throughput and latency.
`perf-benchmarks` contains `orderbook-arrival`, `runtime-flavours`,
`null-submit-cost`, `null-wake-latency` and `null-timer-latency` for
these questions.

== Interoperability and limitations
<interoperability-and-limitations>
Futures combinators such as `join!` and `select!` do not inherently
require a particular runtime. Socket clients can: Tokio-based networking
expects a Tokio reactor. Polling a reqwest future on Nagoya does not
install that reactor. Use a synchronous HTTP client on a dedicated
blocking thread, or keep an explicitly owned Tokio runtime for the
component that needs it. Cancellation, errors and shutdown must cross
that boundary explicitly.

There is no network reactor, interval API, blocking-task pool, task
priority, spawn-local executor, scoped borrowing API, or runtime
drop-and-join contract. Do not infer these from similar names in Tokio.
This guide and the public rustdoc describe what exists; benchmarks
describe only the workloads run.
