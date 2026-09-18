//! Unix-domain sockets over the reactor, through the public API.
//!
//! The acceptance case for `AF_UNIX` support, and it is deliberately the one a
//! consumer runs rather than a demonstration that `bind` returns `Ok`: a length
//! delimited request goes in, a response comes back, and it happens under
//! `block_on_with` on a reactor with **no thread of its own**. That arrangement
//! is the point of the feature, since a Unix socket is exactly the case that
//! should not pay a reactor thread's handoff, and a test against `Reactor::start`
//! would not have exercised it.
//!
//! Both directions are here. Serving is the obvious half; connecting to a path
//! is the other one, and the half that gets skipped because it looks free.

use std::io::Write as _;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use nagoya::io::{Stream, StreamError};
use nagoya::reactor::{block_on_with, Addr, Reactor, TcpListener, TcpStream, UnixPath};

/// A socket path that removes itself.
///
/// Unlinking is part of the job rather than housekeeping: a socket file left by
/// a process that died makes the next `bind` fail with `EADDRINUSE`, which
/// reads like a port conflict and is not one. The file is removed before the
/// bind and again on the way out, so a test that fails mid-way does not break
/// the next run.
struct SocketPath(PathBuf);

impl SocketPath {
    /// A path in the temporary directory, unique to this process and call.
    fn new(label: &str) -> Self {
        /// Distinguishes two paths made in the same process. Not a clock: the
        /// reactor's test build has no business reading one, and a counter
        /// cannot collide with itself.
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let serial = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "nagoya-{label}-{}-{serial}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        Self(path)
    }

    fn as_path(&self) -> &Path {
        &self.0
    }

    /// The address to bind or connect to.
    fn addr(&self) -> Addr {
        Addr::path(self.0.as_os_str().as_bytes()).expect("temporary path fits in sun_path")
    }
}

impl Drop for SocketPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Write a length-delimited frame the blocking way, for the far end of a test.
fn write_frame(sink: &mut impl std::io::Write, payload: &[u8]) {
    sink.write_all(&(payload.len() as u32).to_be_bytes())
        .expect("length");
    sink.write_all(payload).expect("payload");
    sink.flush().expect("flush");
}

/// Read one back.
fn read_frame(source: &mut impl std::io::Read) -> Vec<u8> {
    let mut length = [0u8; 4];
    source.read_exact(&mut length).expect("length");
    let mut payload = vec![0u8; u32::from_be_bytes(length) as usize];
    source.read_exact(&mut payload).expect("payload");
    payload
}

/// Serve a Unix socket: bind a path, accept, read a framed request, answer it.
///
/// The peer is an ordinary blocking `UnixStream` on another thread, so what is
/// under test is this crate's side rather than two copies of it agreeing with
/// each other. Its result comes back through `join`.
#[test]
fn serves_a_framed_request_over_a_unix_socket() {
    let path = SocketPath::new("serve");
    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let listener = TcpListener::bind(path.addr(), &handle).expect("bind");

    // Bound before the peer exists, so the connect below cannot race it and
    // nothing has to be signalled between the two threads.
    let peer_path = path.as_path().to_owned();
    let peer = std::thread::spawn(move || {
        let mut stream = std::os::unix::net::UnixStream::connect(&peer_path).expect("connect");
        write_frame(&mut stream, b"ping");
        read_frame(&mut stream)
    });

    let accepted = block_on_with(&reactor, async {
        let (mut stream, addr) = listener.accept().await.expect("accept");

        let mut length = [0u8; 4];
        stream.read_exact(&mut length).await.expect("length");
        let mut payload = vec![0u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut payload).await.expect("payload");

        let mut response = (4u32).to_be_bytes().to_vec();
        response.extend_from_slice(b"pong");
        stream.write_all(&response).await.expect("respond");

        (addr, payload)
    });

    let answer = peer.join().expect("peer");
    assert_eq!(answer, b"pong", "wrong response");

    let (addr, request) = accepted;
    assert_eq!(request, b"ping", "wrong request");

    // The regression this whole arm exists for. A client that never called
    // `bind` has no address, which is every ordinary Unix client, and decoding
    // that as "a family this crate does not speak" would fail every accept.
    match addr {
        Addr::Path(peer) => assert!(peer.is_unnamed(), "unbound peer reported {peer:?}"),
        other => panic!("accepted a {other:?} on a Unix listener"),
    }
}

/// Connect to a Unix socket: the client half, which Karen needs as well.
///
/// The far end is bound on this thread before the serving thread is spawned, so
/// again there is nothing to synchronise: the socket is listening by the time
/// anything could connect to it.
#[test]
fn connects_to_a_unix_socket_and_gets_an_answer() {
    let path = SocketPath::new("connect");
    let far_end = std::os::unix::net::UnixListener::bind(path.as_path()).expect("bind");

    let server = std::thread::spawn(move || {
        let (mut stream, _) = far_end.accept().expect("accept");
        let request = read_frame(&mut stream);
        write_frame(&mut stream, b"answered");
        request
    });

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();

    let response = block_on_with(&reactor, async {
        let mut stream = TcpStream::connect(path.addr(), &handle)
            .await
            .expect("connect");

        let mut request = (7u32).to_be_bytes().to_vec();
        request.extend_from_slice(b"request");
        stream.write_all(&request).await.expect("write");

        let mut length = [0u8; 4];
        stream.read_exact(&mut length).await.expect("length");
        let mut payload = vec![0u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut payload).await.expect("payload");
        payload
    });

    let request = server.join().expect("server");
    assert_eq!(request, b"request", "wrong request arrived");
    assert_eq!(response, b"answered", "wrong response");
}

/// A bound path survives the trip through `sockaddr_un` and back.
///
/// The length handed to `bind` is the offset of `sun_path` plus the path plus
/// its terminator, not the size of the struct. The wrong one is accepted by
/// Linux and rejected by macOS, so this is what catches it on either.
#[test]
fn a_bound_path_reads_back_as_itself() {
    let path = SocketPath::new("roundtrip");
    let reactor = Reactor::local().expect("reactor");
    let listener = TcpListener::bind(path.addr(), &reactor.handle()).expect("bind");

    let local = listener.local_addr().expect("local_addr");
    assert_eq!(local, path.addr(), "bound address changed in the kernel");
    match local {
        Addr::Path(unix) => {
            assert_eq!(
                unix.as_path_bytes(),
                Some(path.as_path().as_os_str().as_bytes()),
                "path came back different"
            );
            assert!(!unix.is_unnamed());
            assert!(!unix.is_abstract());
        }
        other => panic!("a Unix listener reported {other:?}"),
    }
    // A Unix socket has no port, and says so rather than inventing one.
    assert_eq!(local.port(), 0);
}

/// A path that does not fit is refused, not truncated.
///
/// Truncation would bind a socket somewhere nobody is looking, and the failure
/// would surface much later as a connect that finds nothing.
#[test]
fn an_impossible_path_is_refused_at_construction() {
    // The boundary is the claim worth pinning: `sun_path` holds the path and
    // its terminator, so the longest usable path is one byte shorter than the
    // array. Off by one here is a path that binds somewhere else.
    let longest = vec![b'a'; nagoya::reactor::UNIX_PATH_CAPACITY - 1];
    assert!(
        UnixPath::new(&longest).is_ok(),
        "the longest path that fits was refused"
    );
    let too_long = vec![b'a'; nagoya::reactor::UNIX_PATH_CAPACITY];
    assert!(
        UnixPath::new(&too_long).is_err(),
        "an over-long path was accepted"
    );

    // A NUL inside the path would have the kernel stop at it and bind to a
    // prefix, which is truncation by another name.
    assert!(
        UnixPath::new(b"/tmp/a\0b").is_err(),
        "an interior NUL was accepted"
    );
    assert!(UnixPath::new(b"").is_err(), "an empty path was accepted");
}

/// `read_exact` reports a stream that ends mid-frame rather than a short count.
///
/// The provided method is the reason a framing consumer does not write the
/// accumulate loop itself, so the failure it exists to name is worth pinning.
#[test]
fn read_exact_names_a_stream_that_ends_early() {
    let path = SocketPath::new("eof");
    let far_end = std::os::unix::net::UnixListener::bind(path.as_path()).expect("bind");

    let server = std::thread::spawn(move || {
        let (mut stream, _) = far_end.accept().expect("accept");
        // A header promising eight bytes, then two, then the connection goes.
        stream.write_all(&(8u32).to_be_bytes()).expect("length");
        stream.write_all(b"xy").expect("partial");
    });

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();

    let outcome: Result<(), StreamError> = block_on_with(&reactor, async {
        let mut stream = TcpStream::connect(path.addr(), &handle)
            .await
            .expect("connect");
        let mut length = [0u8; 4];
        stream.read_exact(&mut length).await.expect("length");
        let mut payload = vec![0u8; u32::from_be_bytes(length) as usize];
        let outcome = stream.read_exact(&mut payload).await;
        outcome
    });

    server.join().expect("server");
    assert_eq!(outcome, Err(StreamError::UNEXPECTED_EOF));
}

/// A peer that writes and hangs up at once is still seen to hang up.
///
/// This is the shape of every one-shot client: connect, say one thing, go. Both
/// pollers can report the last of the data and the hang-up on a single edge,
/// and a read that does not fill its buffer consumes that edge, so a reader
/// that then waits for another one waits for something that has already
/// happened. The hang-up is latched apart from readability for exactly this.
///
/// Being a race, it does not fail every time: it hung about one run in three
/// before the latch, and passed forty runs after it. It hangs rather than
/// failing, which is why it is pinned here instead of left for a consumer to
/// meet as a connection that never closes and a task that never ends.
#[test]
fn sees_a_peer_that_hung_up_with_its_last_write() {
    let path = SocketPath::new("hangup");
    let far_end = std::os::unix::net::UnixListener::bind(path.as_path()).expect("bind");

    let server = std::thread::spawn(move || {
        let (mut stream, _) = far_end.accept().expect("accept");
        stream.write_all(b"abcdef").expect("write");
        stream.flush().expect("flush");
        // Dropped here, so the data and the hang-up are one edge.
    });

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();

    let seen = block_on_with(&reactor, async {
        let mut stream = TcpStream::connect(path.addr(), &handle)
            .await
            .expect("connect");
        let mut seen = Vec::new();
        loop {
            // Deliberately larger than anything that will arrive, so every
            // read is a short read and the readable bit is consumed each time.
            let mut buffer = [0u8; 64];
            let read = Stream::read(&mut stream, &mut buffer).await.expect("read");
            if read == 0 {
                return seen;
            }
            seen.extend_from_slice(&buffer[..read]);
        }
    });

    server.join().expect("server");
    assert_eq!(seen, b"abcdef");
}

/// Connecting to a Unix socket whose peer has already gone still completes.
///
/// The deterministic half of the same family of bug. A Unix-domain connect
/// finishes inside the `connect` call, so there is no later writable edge to
/// wait for; the old code waited anyway and asked `getpeername` whether the
/// handshake was done. That question stops being answerable the moment the
/// peer closes, so a server that answers and exits left the client waiting on
/// an edge that had already gone by.
///
/// Sequenced rather than raced: the far end is fully finished, joined, before
/// the connect is awaited at all.
#[test]
fn a_connect_completes_even_if_the_peer_left_first() {
    let path = SocketPath::new("departed");
    let far_end = std::os::unix::net::UnixListener::bind(path.as_path()).expect("bind");

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();

    // Started, not awaited: the descriptor is connected from here on, which is
    // what lets the far end finish before anything asks about the handshake.
    let mut stream = TcpStream::connect_started(path.addr(), &handle).expect("connect");

    let server = std::thread::spawn(move || {
        let (mut peer, _) = far_end.accept().expect("accept");
        peer.write_all(b"gone").expect("write");
    });
    // The peer is now closed and its thread is finished, so `getpeername` on
    // this socket has nothing left to report.
    server.join().expect("server");

    let seen = block_on_with(&reactor, async {
        stream.connected().await.expect("connected");
        let mut buffer = [0u8; 16];
        let read = Stream::read(&mut stream, &mut buffer).await.expect("read");
        buffer[..read].to_vec()
    });

    assert_eq!(seen, b"gone");
}
