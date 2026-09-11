# nagoya

The [user guide](docs/user-guide.md) covers task and worker lifetimes, submit,
parallel loops, cancellation, timers, synchronization, files, custom hosts,
tuning and interoperability. Its Rust examples are compiled as rustdoc tests.

An async runtime that does not need an operating system.

`spawn`, `JoinHandle`, `block_on` and a parallel `par_for`, over
[`ps-st3`](https://crates.io/crates/ps-st3)'s work-stealing pool, whose entire
contact with a platform is three methods: park, unpark, and a clock.

```toml
nagoya = "0.1"
```

**The API will churn.** 0.1.0 exists so other projects have something to depend
on, not because the surface is settled.

## Not Tokio

There is no I/O driver and there will not be one here: an epoll or io_uring
reactor is exactly the part that needs an operating system, and running without
one is the only interesting thing about this. A caller that wants sockets brings
its own reactor.

### Clients that require Tokio

A Tokio-based HTTP client can compile and then panic when polled on a worker
without Tokio's reactor context. Starting another ordinary thread does not
install that context. There are three integration choices:

- Use a blocking HTTP client on a thread dedicated to blocking work.
- Own a Tokio runtime for the networking component and communicate explicitly
  across the boundary, including errors, cancellation and shutdown.
- Require Tokio for a feature that cannot operate without its reactor.

WorkTable's S3 adapter uses blocking `ureq` calls behind its async interface.
Those calls occupy the polling thread. That choice removes the reactor
requirement; it does not make network I/O nonblocking or establish a universal
performance advantage. See the [interoperability guide](docs/user-guide.md#interoperability-and-limitations).

## Two shapes, one runtime

A task is a thing that can suspend. A parallel loop is a thing that cannot, and
that restriction is what makes it fast: the range splits recursively on the pool
and never becomes one queue entry per item.

```rust,ignore
// A task.
let handle = executor.spawn(async move { work().await });

// A parallel loop, which is *also* a task: awaiting it suspends the caller
// rather than blocking a thread, so the worker goes and runs pieces of it.
nagoya::par_for(pool, 0..1_000_000, |i| compute(i)).await;
```

`rayon::install` blocks the calling thread, and blocking needs a condition
variable or a futex. `par_for` returns a future instead, which is what lets one
runtime offer both shapes without an operating system underneath either.

## Where it stands

100,000 tasks, 8 workers, median of 5, arms interleaved, submitted from outside
the pool, on an M4 Max. Reproduce with `cargo run --release --example three_pools`.

| a future, polled to completion | wall | tasks/s | cpu |
| --- | ---: | ---: | ---: |
| forte | 20.8 ms | 4,797,678 | 74.5 ms |
| tokio | 27.6 ms | 3,621,341 | 88.8 ms |
| **nagoya** | **29.6 ms** | **3,378,659** | **270.9 ms** |

| a parallel loop | wall | items/s | cpu |
| --- | ---: | ---: | ---: |
| **nagoya `par_for`** | **2.7 ms** | **37,048,467** | **26.2 ms** |
| rayon `par_iter` | 3.6 ms | 27,491,409 | 29.3 ms |

Read the second table with its footnote: a parallel loop splits 100,000 items
into a few hundred pieces, so it is not scheduling 100,000 of anything. It is
not comparable to the first table, and neither is rayon's row to tokio's.

Where this loses is CPU. nagoya's workers spin where forte's sleep, and closing
that costs wake latency. `st3::fanout::Tuning` exposes the trade, and the
sweep behind its defaults - along with every variant that lost - is recorded
outside this repository.

## `no_std`

`default-features = false` drops `std` and with it `Runtime` and the parker
`block_on` uses. Verified against `aarch64-unknown-none`, not merely believed.

## Licence

MIT or Apache-2.0, at your option.
