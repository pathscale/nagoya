# nagoya

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


### What that means for `reqwest` and friends

The rule above is easy to agree with and easy to trip over, because the failure
arrives at runtime rather than at compile time:

```
thread 'nagoya-0' panicked:
there is no reactor running, must be called from the context of a Tokio 1.x runtime
```

`reqwest`, and most async HTTP clients, do not merely *use* a reactor — they look
for **tokio's**, through a thread-local runtime handle. So there is nothing
nagoya can provide that would satisfy them. Adding an I/O driver here would not
fix it, and neither does spawning a thread.

There are three honest ways out, in the order worth trying them.

**1. Use a blocking client.** Async HTTP exists to multiplex thousands of
connections onto a few threads. A worker that makes one request at a time and is
already on its own thread wants a blocking call, and gets it for less. `ureq`,
`attohttpc` and `minreq` all do this with no runtime at all.

This is what WorkTable did in September 2026. Its S3 support made four calls — a
PUT and three GETs against presigned URLs, no streaming, no multipart, no auth
headers, because the signature is in the URL. Swapping `reqwest` for `ureq`:

| | before | after |
|---|---:|---:|
| crates in the build | 198 | 170 |
| what the feature cost | 91 crates | 63 |
| tokio present | yes | **no** |
| the panicking test | panicked | passes |

The `async fn` signatures stayed, so no call site changed; the blocking happens
inside them. That is not a hack, it is the same thing WorkTable's filesystem
layer already does deliberately — neither `tokio::fs` nor `async-fs` performs
asynchronous file I/O either, both hand a blocking `std::fs` call to a thread
pool, and WorkTable measured 12,316 rows/sec through `tokio::fs` against 74,728
blocking. Pretending to be async cost 6x.

**2. Run tokio beside nagoya, deliberately.** If a blocking client genuinely will
not do — you need HTTP/2 multiplexing, or streaming bodies, or a vendor SDK with
no blocking variant — then own a tokio runtime explicitly for that work and hand
requests to it over a channel. Do it because you decided to, not because a
dependency decided for you, and keep it off the path nagoya schedules.

**3. Require tokio for that feature.** If a Cargo feature cannot work without a
tokio reactor, make it say so and refuse otherwise. A build that fails is better
than a worker that panics in production, and a silent incompatibility between a
feature flag and a runtime choice is the worst of the three.

**What not to do:** add an I/O driver to nagoya to satisfy a client that is
looking for tokio's. It will not find it, and the reason there is no reactor here
is the reason this runtime exists.

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
