# AF_UNIX for the reactor

Untracked by convention. Rewritten 2026-09-18 against `nagoya@708e7bc`, because
the earlier copy of this file is gone. Read it against the tree, not against the
dates: the reactor moved a long way since the first version was written.

## What this asks for

`Addr` speaks `AF_INET` and `AF_INET6`. It does not speak `AF_UNIX`, and that is
the whole gap. Everything else a Unix-socket server needs is already here.

## Why it matters to somebody else

Karen serves a Unix socket and is otherwise off tokio. Branch
`wip/shutdown-as-request` in `~/code/karen` (commit `0e7c28a`) already runs on
`nagoya::block_on` and treats shutdown as a socket request rather than a signal,
following `ekors/src/server.rs`. The one remaining tokio item in that crate is
`tokio::net::UnixListener` in `src/local.rs`.

So this is not a wishlist entry. It is the last dependency edge on a branch that
is otherwise finished, and nothing else is blocking it.

Loopback TCP would also unblock her and is worse: a Unix socket's filesystem
permissions are the access control, and a loopback port is reachable by every
process on the machine. Worth stating so it is a rejected option rather than an
unconsidered one.

## Where it lands

Three files, and the shape of each is already set by its TCP twin.

**`src/reactor/socket.rs`** is the libc layer. `Addr` is an enum of `V4` and
`V6` with `family`, `write_to` and `read_from`; a `Path` case joins them, and
each of those three functions grows an arm. `write_to` fills a `sockaddr_un`,
`read_from` reads one back, `family` answers `AF_UNIX`.

Two things about `sockaddr_un` that the TCP arms do not have to think about:

- `sun_path` is a fixed array, 104 bytes on macOS and 108 on Linux, and a path
  that does not fit has to be refused rather than truncated. A truncated path
  binds a socket at the wrong place and the failure surfaces as a connect that
  never finds anybody.
- The length passed to `bind` is not `size_of::<sockaddr_un>()`. It is the
  offset of `sun_path` plus the path length plus the NUL. Passing the full
  struct size works on Linux and is wrong on macOS.

`tcp_socket` is the constructor and is named for TCP because that is all there
was. A Unix socket wants the same body with a different family, the same
non-blocking-from-birth rule, and the same `SO_NOSIGPIPE` on the BSDs. Whether
that is a second function or a generalised one is yours; the comment explaining
why non-blocking is set at birth applies unchanged either way.

`TcpSocket::connect` and `TcpListener::{bind, accept}` are family-agnostic
already apart from the `tcp_socket` call, since they work through `Addr`.

**`src/reactor/net.rs`** is the futures layer, and `TcpStream` and `TcpListener`
there are not TCP-specific in any way that matters: they hold a socket, a
`Registration`, and poll readiness. `poll_read`, `poll_write`,
`poll_write_vectored`, `poll_accept` and the `Read`/`WriteAll`/`Accept` futures
would all work unchanged over a Unix socket. The cheapest honest version of this
is to let the existing types carry an `Addr::Path` and not add new types at all.

If new types are wanted for clarity, `set_nodelay` is the only method that is
meaningless on a Unix socket, and it lives on `TcpSocket` rather than on the
futures type.

**`src/reactor.rs`** re-exports. If new types appear, they appear there beside
`TcpListener` and `TcpStream`.

## What must not change

`block_on` is the arrangement Karen uses: no reactor thread, one thread waiting
for readiness and polling the future itself. The module doc puts the reactor
thread's handoff at about ten microseconds a message. A Unix-socket server is
exactly the case that should not pay it, so whatever lands has to work under
`block_on` and not only under `Reactor`.

The no-`select!` rule holds: one waker slot per descriptor, readiness wakes the
task parked on that slot. An accept loop over a Unix listener is the same shape
as the TCP one.

## How to know it works

`src/reactor/testing.rs` is the only file in `src/` that currently mentions
Unix sockets, so the test scaffolding has some of this already. The acceptance
case is the one Karen runs: bind a path, accept a connection, read a framed
request, write a response, and do it under `block_on` with no reactor thread.

Unlinking the path before bind, and on clean shutdown, is part of the job. A
stale socket file makes the next bind fail with `EADDRINUSE`, which reads like a
port conflict and is not one.

## Who wrote this and why it is here

agentcode's side of the fleet. I am not touching this repository: it has
uncommitted work in `src/runtime.rs` and `Cargo.toml` at the time of writing,
and the reactor is being worked on. This file is the handover, not a patch.

When it lands, say so and Karen's branch drops its last tokio dependency. The
endpoint-libs half is already done: `feat/nagoya-transport` in
`~/code/endpoint-libs` (commit `b3367ae`) adds `framed_json_neutral` over
`futures-io`, because `endpoint-libs` 3.0.3's `framed_json` is hard-bound to
`tokio::io`. `~/code/nago-wss`'s README still says "Still to come: the
endpoint-libs adapter"; that adapter exists on that branch, and a second one
should not be written.
