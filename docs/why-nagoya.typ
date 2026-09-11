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
== Low queue latency, with the cost visible

In the suite's paced-arrival experiment, each job performs the same calibrated
CPU work. The runtime comparison measures submission-to-start latency, before
that work runs. At 25% of the benchmark's estimated eight-worker capacity:

#table(
  columns: (1.5fr, 1fr, 1fr, 1fr), inset: 8pt,
  stroke: rgb("#d1dcdf"),
  table.header([*Runtime / call*], [*Median*], [*p99*], [*CPU cores*]),
  [Nagoya `submit`], [1.25 µs], [3.88 µs], [8.89],
  [Nagoya `spawn`], [1.33 µs], [4.79 µs], [8.66],
  [Tokio], [1.38 µs], [19.63 µs], [3.55],
  [Rayon], [0.75 µs], [14.88 µs], [7.90],
)

Nagoya's closure path had about *five times lower p99 queue latency than Tokio*
in this configuration. Rayon had the lower median. Nagoya's latency-oriented
tuning consumed substantially more CPU than Tokio; the CPU column includes the
producer and runtime workers. These are useful tradeoffs, not a universal ranking.

At the 50% point, Nagoya `submit` recorded 5.92 µs p99, versus 68.38 µs for
Tokio and 17.13 µs for Rayon. At 75%, its p99 rose to 57.58 µs while Nagoya
`spawn` recorded 12.29 µs. API choice and offered load both matter. The suite
keeps both call paths so the favorable case does not stand in for the whole curve.

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
  *Measurement note.* Apple M4 Max, macOS arm64, 11 September 2026.
  `orderbook-arrival`: eight workers; 20,000 jobs per point; median of three
  interleaved rounds; calibrated job cost 1,592 ns. Percentages use estimated
  capacity, not measured CPU utilization. This is one local full-suite run,
  not a cross-machine guarantee or end-to-end request benchmark.
  #link("https://github.com/pathscale/perf-benchmarks/blob/fix/two-ps-st3-in-one-graph/data/apple-m4-max-darwin-arm64/2026-09-11-210249-full.md")[Report and provenance].
  #link("https://github.com/pathscale/perf-benchmarks/blob/fix/two-ps-st3-in-one-graph/benchmarks/orderbook-arrival.rs")[Benchmark and tuning].
]
