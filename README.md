# nagoya

An async runtime that does not need an operating system.

`spawn`, `JoinHandle` and `block_on` over [`ps-st3`](https://crates.io/crates/ps-st3)'s
work-stealing pool, whose entire contact with a platform is three methods: park,
unpark, and a clock.

**Not Tokio.** There is no I/O driver and there will not be one here: an epoll or
io_uring reactor is exactly the part that needs an operating system, and running
without one is the only interesting thing about this. A caller that wants sockets
brings its own reactor.

Nothing is published. `0.0.0`, `publish = false`.
