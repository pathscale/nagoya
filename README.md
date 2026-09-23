# Nagoya

Futures, synchronous closures and parallel ranges on one pool, with a core that
supports `no_std` plus `alloc`. The embedding can supply OS services through
libc without enabling Rust std.

- [User guide](docs/user-guide.typ): installation, every public callsite, task
  lifetime, cancellation, timers, synchronization, I/O, host integration and tuning.
- [Why Nagoya](docs/why-nagoya.typ): design, examples and measured tradeoffs.

Typst is the canonical documentation source. Run `sh scripts/build-guide.sh`
to create both PDFs in `docs/`; Rust examples are extracted and checked by
`cargo test --doc`.

Version 0.1.14 adds `Runtime::builder()`, which takes what `with_tuning` does
plus the worker threads' stack size. Without it a worker's stack is whatever
`RUST_MIN_STACK` says, or two MiB, so a caller whose tasks recurse deeply had
to set an environment variable to run them. `new`, `with_tuning` and
`background()` are unchanged.

Version 0.1.12 adds `process`, behind a feature of the same name: `Command`,
`Child` and the child's pipes, spelled as `tokio::process` spells them, with
the pipes as `futures-io` readers and writers. `spawn`, `output` and `status`
take the reactor `Handle` to register on, as a socket does: there is no
process-wide reactor and nothing starts a thread behind the caller's back. On a
reactor with a thread of its own they finish under `nagoya::block_on`,
`nagoya::spawn` or any other executor. A child's exit is a
`pidfd` on Linux and an `EVFILT_PROC` kqueue on macOS and the BSDs, so nothing
polls `waitpid`.

Version 0.1.4 adds `reactor`, an off-by-default readiness driver, and with it
the first implementation of `io::Stream`. It is epoll or kqueue behind a
feature that implies `std`, on the same terms as `runtime`: a `no_std` build
does not compile it and has no sockets, and a default build does not acquire a
reactor thread by depending on this crate. Nothing here polls a set of sources
to ask which is ready; a descriptor owns its waker slot and readiness wakes the
task parked on it.

Version 0.1.3 added `io::Stream`, a byte stream trait for consumers whose
futures must not be `Send` and whose errors have to be able to say "not ready
yet". It sits beside `io::Read` and `io::Write`, which describe a file and
cannot express either.

Version 0.1.2 added `Executor::submit` and `block_on_with_host`. It accepts
ps-st3 through `^0.6`; that release was validated with 0.6.2. Use `cargo update
-p ps-st3` in an existing checkout to pick up the scheduler fixes.

`block_on` uses a spinning fallback without std. Use `block_on_with_host`
with a dedicated host wait slot when the caller should block. See the guide
for the separate worker identity, clock and timer-driving contracts.
