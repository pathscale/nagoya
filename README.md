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

Version 0.1.2 adds `Executor::submit` and `block_on_with_host`. It requires
ps-st3 0.6.2. Until those versions are published, use the release checkouts and
one consistent Cargo patch graph. Merge and publish dependencies first.

`block_on` uses a spinning fallback without std. Use `block_on_with_host`
with a dedicated host wait slot when the caller should block. See the guide
for the separate worker identity, clock and timer-driving contracts.
