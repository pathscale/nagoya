# One process, one injected execution domain

Source baseline: nagoya master `4cae808eccec503107bd8a9769f385e0ddbdbfdd`,
version 0.1.16. The owner's 2026-10-14 ruling overrides contradictory adaptive
compilation language in the coordinator documents. Suspected attack means
throttle or kill that agent. These changes introduce no compile tier.

Start once, before libraries submit work or poll a pending timer:

```rust
let builder = nagoya::runtime::Runtime::builder().all_cores()?;
let runtime = builder.install_background()?;
let readiness = runtime.readiness()?;
let executor = runtime.executor().clone_handle();
```

Enable the `reactor` feature. Inject this executor, its `Arc<Pool>` and this
readiness handle throughout chuzz. `nagoya::spawn` and `background()` find the
configured shared pool. Do not call `Runtime::new`, `Builder::build`,
`Reactor::start`, `Reactor::sharded` or resolver-owned blocking thread helpers
from application/library components in this topology. Existing APIs remain
available for other embeddings. Platform main does only OS-required work.

`Builder::all_cores` is the documented core-selection knob. It uses the
process's available logical CPU count, including performance and efficiency
cores, and rejects a count above ps-st3's `usize::BITS` bitmap capacity. It
does not pin OS threads or guarantee that an OS schedules one on each core.
An embedding needing affinity must supply that separately inside nagoya.
Today's unchanged default is `Builder::new -> shared_threads` and
`background -> Runtime::new(shared_threads())` in `src/runtime.rs`.
`shared_threads` tries `performance_cores` first. On Apple that reads
`hw.perflevel0.logicalcpu`; only its fallback uses `available_parallelism`.
An explicit `workers` call after `all_cores` overrides the count.

Nagoya owns N pool worker threads plus one explicitly accounted OS
readiness/deadline driver. The driver waits in kqueue/epoll and runs no agent
code. No application thread, secondary executor or per-agent runtime is
required. The timer backend must be bound before first timer admission;
late binding is a named error and never silently creates two timer drivers.

# Exclusive turns and typed ownership

`Owner<T,K,B>` requires the actual value T to be Send, without requiring Sync.
Only admission handles clone. They retain the same typed key and authority
Arcs, and share one atomic admission gate. Successful admission Acquire-takes
an owned T into a private lease. `dispatch` reserves CPU credits before the
async task is scheduled. The worker executes one synchronous callback with
exclusive `&mut T`, reconciles CPU, then puts T back before Release-clearing
the gate. The join becomes ready only after that publication. Queued work,
running work, cancellation and unwinding all retain or return the one lease.
Busy or budget-denied submission returns the original callback and captures.

The slot uses an audited `UnsafeCell<Option<T>>` and a conditional Sync
implementation for T: Send. This synchronizes movement of an already-Send
value; it does not assert Send for any DOM, realm, native frame or platform
field. Agent fields containing Rc or thread-affine objects must be refactored
before using this primitive. There is no Mutex or dyn Any in owner transport.
Immutable inputs, outputs, source and code products can be owned typed Arcs.

`dispatch_to` is an optional initial queue preference for deterministic
placement work, not lifetime affinity. Stealing can move queued work. End all
worker-specific borrows, native frames, code-reader pins and publication
guards before returning a turn. A bounded native suspension must retain roots
and owned continuation/code records, then acquire new worker guards on entry.
This primitive does not make arbitrary native execution resumable.

# CPU fairness and concurrency at every level

`CpuBudget<K>` and `CpuPermit` are policy hooks. The supplied `CpuCredits`
implementation uses `Arc<CreditAccount<K>>` as the stable accounting key.
Give agents from one site and all of their parse, compile, style, layout,
paint, decode, network and marking helpers that same account. Its atomic
credit balance and in-flight cap apply across every worker. Migration,
reentry, new registrations and agent construction must reuse the account;
they must never construct fresh site credit. Retain policy accounts across
worker-instance teardown while the policy key remains alive.

Each reservation removes CPU nanoseconds before enqueueing. Entry/exit read
the host's current-thread CPU clock, with no await between them. Waiting and
queue residence are not charged as CPU. Cancellation before entry refunds
everything; unused reservations are refunded; overshoot becomes negative
debt. Replenishment pays debt first and caps spendable credit. The host must
provide a monotonic CPU clock, never guest time or elapsed wall time.

`CpuCredits::dispatch` admits attributed synchronous helpers on the same
executor. `par_for` already uses the same Arc<Pool>, but does not itself call
the new CPU hook. Reserve each bounded chunk before submitting it, or run
chunks through attributed helper dispatch. Do not reserve once around an
unbounded parallel range. Agent turns remain serial while independent agents
and immutable helper DAG inputs can execute concurrently from the first
integration. Chuzz supplies the DAG, revision validation and deficit/deadline
ordering, foreground dependency priority, control/progress capacity and
site-to-agent selection. Admission hooks alone do not implement that policy.

After denial, call `CreditAccount::wait_for` with the retained request size.
It registers then rechecks balance and in-flight state without attempting a
reservation or emitting its own rollback notification. After it returns, retry
the retained callback; another real admission may win first. `changed` remains
a lower-level notification hook for register/recheck of accounting state.
Refunds and explicit refill events wake waiters. Refill is one retained due deadline or a class/input
event, never a scan. A background site has a shared in-flight limit; trusted
foreground/control policy may use separate reserved authority. Native
safepoints must bound CPU overshoot and termination. Cooperative dispatch
cannot preempt a monolithic callback or a stuck OS call.

# Timer and readiness delivery

`Runtime::readiness` starts one existing `Reactor::start` and retains it for
the process. `time::set_timer_reactor` binds its Handle before timer backend
selection. First pending `Sleep::poll` still registers in the existing indexed
timer heap; earliest-arm notification calls `Handle::timer_armed`. The driver
caches deadlines, services only due timers and blocks in `Poller::wait` until
an I/O edge, the actual deadline or an arm/shutdown notification. Publication
uses swap-before-drain and fetch_min-after-drain so a racing earlier deadline
is retained. A cancelled earliest timer may cause one stale deadline wake,
but no periodic inspection.

`ReadySource` is one movable consumer; cloneable `ReadyHandle`s publish reason
bits before notification. Register/recheck retains wakes racing with idle
suspension. Bits coalesce eligibility changes, not distinct events or data.
Keep ordered inputs in bounded typed owned records or correctly owned ps-spsc
endpoints. `ReadyHandle::wake_at` rounds delivery up to a caller-provided
coarse quantum and arms one Sleep task on the same executor. Retain and cancel
its join when a deadline changes. No tick, interval or readiness probe runs.

For automatic tab sleep, chuzz records each window's five-minute last-view
deadline, arms the next relevant eligibility deadline and rechecks the whole
window agent on wake. An active same-origin popup keeps its shared agent live.
Only after exclusive turn exit, bounded teardown and helper-reader drain may
the caller revoke the incarnation and release heap/cage state. Sleeping tab
records and service-worker registrations retain policy and bounded metadata,
not an eager agent or heap. A delivery bit alone never authorizes discard.

For service workers, real readiness/completion events or admitted scheduled
pings retain a broker event. Compute a coarse hash-staggered next wake from
the event due time and retained site minimum gap, then arm one deadline.
Use one site authority to atomically admit across all registrations. Trusted
events can be immediate; normal batching and throttled 900-second gaps are
chuzz policy. New events joining a wake do not reset its CPU account. Root
waitUntil obligations until completion or a named cap failure, then discard
idle worker state. Foreground fetch fallback/deadlines remain explicit.
All exposed guest clocks remain coarse and frozen across fairness continuations;
the scheduler and credit hooks use trusted host clocks.

Sockets already use `Handle::register`, owned `Registration`,
`take_readable_or_park`, `take_writable_or_park`, `poll_readable`,
`poll_writable`, and generation-qualified readiness. Reuse these, draining
actual I/O until EWOULDBLOCK before parking; do not periodically check a file
descriptor. Immutable typed completions can wake `ReadyHandle` after their
owned payload and generation are published.

# Existing seams and release impact

Built on `Runtime::builder`, `Builder::workers`, `Builder::build`,
`Runtime::executor`, `Executor::clone_handle`, `Executor::spawn`,
`Executor::pool`, `WorkerContext::current_worker`, private
`task::current_worker`, `task::spawn_on`, `async_task::Builder::spawn`,
`Runnable::schedule/into_raw/from_raw/run`, `JoinHandle`,
ps-st3 `Pool::submit_local/submit_job`, `Job::from_raw`, `Act`,
`Reactor::start/handle`, `Handle::timer_armed/wake/register`,
`Poller::wait/wake`, `sleep_until`, `Sleep`, private
`time::arm/fire_due`, `Notify::notified/notify_one/notify_waiters` and
`Notified::enable`. Task allocations and wakers are reused across waits;
one new owner-turn task is allocated per admitted turn. No new scheduler,
timer heap, general channel or thread pool is introduced.

The published crate is 0.1.16. These APIs are additive and legacy defaults
remain; no minor bump is required for these signatures. Edition 2024 is a
deliberate toolchain change, with the existing Rust 1.85 floor supporting it.
The owner must use a 0.2 minor bump if later work changes defaults, removes
existing constructors or changes a published contract. Path plus caret
ps-st3 dependencies use the sibling in-house checkout locally and retain a
registry version for publication. No lockfile is supplied.

The example witness and named production mutants are supplied for separate
owner-run functional acceptance. No build, unit test, benchmark or functional
execution was performed in the read-only lane. SEARCH checks establish patch
applicability, not compiler acceptance or concurrency correctness.
