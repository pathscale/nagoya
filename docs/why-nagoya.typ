#set document(title: "Why Nagoya", author: "PathScale")
#set page(paper: "a4", margin: (x: 2.2cm, y: 2cm), numbering: "1")
#set text(font: ("Helvetica", "Arial"), size: 10pt, fill: rgb("#172833"))
#set par(leading: 0.65em)
#show heading: set block(above: 1.3em, below: 0.6em)
#show heading.where(level: 2): set text(size: 16pt)
#show raw.where(block: true): it => block(fill: rgb("#eff4f5"), inset: 10pt, radius: 3pt, width: 100%, breakable: false, text(size: 8pt, it))
#show link: set text(fill: rgb("#176a7a"))

#text(size: 10pt, weight: "bold", fill: rgb("#176a7a"))[PATHSCALE / NAGOYA]
#v(0.5cm)
#text(size: 32pt, weight: "bold")[Keep the work together.]
#v(0.3cm)
#text(size: 15pt)[Futures, CPU work and parallel ranges. One runtime you can own.]
#v(0.4cm)

A request waits for input, transforms data, fans out computation and waits again.
Nagoya gives those stages a common execution home. Spawn a future, submit a
synchronous closure or split a range across workers, using the same pool.

This is the direction a next generation runtime should take: make the execution
model fit the whole application, and let the application decide how it lives on
the machine.

== One pool, three useful entry points

`spawn` handles work that can suspend. `submit` accepts a synchronous closure
without wrapping it in an async task. `par_for` distributes a range with an
explicit minimum leaf size. They share the scheduler and worker budget.

```rust
use nagoya::{block_on, par_for, runtime::Runtime};
use std::sync::{mpsc, Arc, atomic::{AtomicUsize, Ordering}};

let runtime = Runtime::new(4);
assert_eq!(block_on(runtime.spawn(async { 21 * 2 })), Some(42));

let (send, receive) = mpsc::channel();
runtime.executor().submit(move || send.send(42).unwrap());
assert_eq!(receive.recv().unwrap(), 42);

let total = Arc::new(AtomicUsize::new(0));
let output = total.clone();
block_on(par_for(runtime.pool().clone(), 0..1000, move |i| {
    output.fetch_add(i, Ordering::Relaxed);
}).leaf(64));
assert_eq!(total.load(Ordering::Relaxed), 499_500);
runtime.pool().shut_down();
```

== The host stays in charge

Use an owned `Runtime` in an ordinary Rust application, or supply the executor's
host yourself. The core supports `no_std` with `alloc`: the host supplies worker
execution, parking and time. Timers and synchronization belong to the runtime;
an operating-system I/O reactor is not a mandatory dependency of its core.

Scheduler tuning is explicit. Worker count, polling, spinning and parking are
decisions you can inspect and measure. That matters when a small latency gain
costs a large amount of CPU, or when the runtime must coexist with another
system on the same machine.

#pagebreak()
#text(size: 10pt, weight: "bold", fill: rgb("#176a7a"))[WHY NAGOYA / MEASURED BEHAVIOR]
== Keep useful work moving

The release comparison runs the same generated WorkTable, portable locks and
cooperative yield on Nagoya and Tokio. With Nagoya's explicit LowLatency tuning,
the geometric mean throughput ratios across eight and sixteen workers,
1/4/8/16 clients and two independent sweeps were:

#table(
  columns: (2fr, 1fr), inset: 8pt, stroke: rgb("#d1dcdf"),
  table.header([*Workload*], [*Nagoya / Tokio*]),
  [50% reads, 50% updates], [*1.65×*],
  [Read, then modify in place], [*2.12×*],
  [Read only], [0.95×],
)

The benefit appears where work hands data and synchronization to its successor.
Across all 48 cells, this tuning recorded 1.49× throughput and 2.01× throughput
per CPU core, using geometric means. It was within 10% of the best Nagoya policy
in 46 cells. Tokio retained a material advantage in four-client reads; this is
a workload comparison, not a claim that one executor always wins.

== Quiet when there is nothing to do

A runtime should expose what low latency costs. The OS parker can block workers
without Rust std and without periodic timeout wakeups. A portable host can use
libc for its clock and thread identity, then share the same wake-permit protocol
as an ordinary Rust application.

Short gaps between bursts deserve their own measurement. In the portable-host
probe, aggressive spinning used several CPU cores between sparse jobs; immediate
parking used a small fraction of one core with a comparable median in one tested
case. Choose and measure the tradeoff your application needs. A fully quiet
pool and an intermittently busy pool are different tests.

== A common foundation for mixed workloads

Tokio's integrated networking ecosystem is valuable. Rayon makes data-parallel
computation approachable. Nagoya's proposition is a common execution substrate
for applications that need asynchronous coordination and CPU work together,
including hosts where a conventional operating-system runtime is the wrong fit.

Sharing a pool avoids requiring a separate executor merely to change work shape.
It also makes ownership visible: long blocking calls still occupy workers, and
CPU loops still need useful work boundaries. An application chooses its pool
budget, completion contracts and shutdown sequence explicitly.

== Start with the workload you own

Use the #link("user-guide.pdf")[Nagoya user guide] for task lifetime, cancellation,
timers, synchronization, portable I/O and custom hosts, with executable examples.
Version 0.1.2 introduces `Executor::submit`; use the release checkout until that
version is published. Compare your own latency distribution and CPU use before
choosing a tuning profile.

#v(0.35cm)
#text(size: 8pt, fill: rgb("#526873"))[
  *Measurement note.* Apple M4 Max, macOS arm64, Rust 1.98.0, 12 September 2026.
  Table comparison: 20,000 rows, 60,000 operations per client, fifteen samples
  after warmup in each cell. Six isolated Nagoya policies plus Tokio, two sweeps.
  LowLatency is the explicitly selected comparison policy. Ratios use the same
  workload, client and worker counts, not unrelated peak scores. Results apply
  to this machine and do not establish a cross-platform or end-to-end guarantee.
  #link("https://github.com/pathscale/perf-benchmarks/blob/fix/two-ps-st3-in-one-graph/data/apple-m4-max-darwin-arm64/2026-09-12-ready-work-runtime-baseline.md")[Table report and provenance].
  #link("https://github.com/pathscale/perf-benchmarks/blob/fix/two-ps-st3-in-one-graph/data/apple-m4-max-darwin-arm64/2026-09-12-runtime-diagnostics.md")[Portable-host and placement diagnostics].
]
