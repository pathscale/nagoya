//! Sockets over the reactor, through the public API.
//!
//! These live here rather than inside `src/reactor/net.rs` because they need
//! nothing the crate does not already export: a listener, a stream, a reactor
//! and `block_on`. A test that only uses the public surface is an integration
//! test, and keeping it beside the implementation only makes the
//! implementation longer to read.

use nagoya::reactor::{Addr, Reactor, TcpListener, TcpStream};

/// A socket is usable through the trait, not just beside it.
///
/// This is written as a generic function on purpose: it compiles only if
/// `TcpStream` really satisfies `io::Stream`, and it runs only if the
/// forwarding is right. An inherent method with the same name would
/// satisfy neither.
#[test]
fn a_socket_reads_and_writes_through_the_stream_trait() {
    async fn echo_once<S: nagoya::io::Stream>(stream: &mut S) -> usize {
        let mut buffer = [0u8; 8];
        let read = stream.read(&mut buffer).await.expect("read");
        stream.write_all(&buffer[..read]).await.expect("write");
        read
    }

    let reactor = Reactor::start().expect("reactor");
    let handle = reactor.handle();
    let listener = TcpListener::bind(Addr::localhost(0), &handle).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let client_handle = handle.clone();
    let client = std::thread::spawn(move || {
        nagoya::block_on(async move {
            let mut stream = TcpStream::connect(addr, &client_handle)
                .await
                .expect("connect");
            // Named through the trait on this side, so both directions
            // are the trait's methods rather than the inherent ones that
            // happen to share their names.
            nagoya::io::Stream::write_all(&mut stream, b"trait")
                .await
                .expect("write");
            let mut buffer = [0u8; 8];
            let read = nagoya::io::Stream::read(&mut stream, &mut buffer)
                .await
                .expect("read");
            buffer[..read].to_vec()
        })
    });

    let served = nagoya::block_on(async {
        let (mut stream, _) = listener.accept().await.expect("accept");
        echo_once(&mut stream).await
    });

    assert_eq!(served, 5);
    assert_eq!(client.join().expect("client"), b"trait");
}
/// Port zero: the kernel picks a free one, which `local_addr` reports.
fn local() -> Addr {
    Addr::localhost(0)
}

#[test]
fn a_connection_round_trips_through_the_reactor() {
    let reactor = Reactor::start().expect("reactor");
    let handle = reactor.handle();

    let listener = TcpListener::bind(local(), &handle).expect("bind");
    let addr = listener.local_addr().expect("addr");

    // The client runs on another thread so the accept below has something
    // to accept. Both sides go through the reactor.
    let client_handle = handle.clone();
    let client = std::thread::spawn(move || {
        nagoya::block_on(async move {
            let mut stream = TcpStream::connect(addr, &client_handle)
                .await
                .expect("connect");
            stream.write_all(b"ping").await.expect("write");
            let mut buffer = [0u8; 4];
            stream.read(&mut buffer).await.expect("read");
            buffer
        })
    });

    let echoed = nagoya::block_on(async {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buffer = [0u8; 4];
        let read = stream.read(&mut buffer).await.expect("read");
        assert_eq!(read, 4, "short read");
        assert_eq!(&buffer, b"ping");
        stream.write_all(b"pong").await.expect("write");
        buffer
    });
    assert_eq!(&echoed, b"ping");

    let received = client.join().expect("client thread");
    assert_eq!(&received, b"pong", "client did not receive the reply");
}

#[test]
fn a_large_write_completes_across_multiple_wakeups() {
    // Bigger than any socket send buffer, so the write necessarily blocks
    // partway and has to be resumed by a writability wakeup. This is the
    // path a naive write_all gets wrong.
    const SIZE: usize = 4 * 1024 * 1024;

    let reactor = Reactor::start().expect("reactor");
    let handle = reactor.handle();
    let listener = TcpListener::bind(local(), &handle).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let client_handle = handle.clone();
    let client = std::thread::spawn(move || {
        nagoya::block_on(async move {
            let mut stream = TcpStream::connect(addr, &client_handle)
                .await
                .expect("connect");
            let payload = vec![0xABu8; SIZE];
            stream.write_all(&payload).await.expect("write");
        })
    });

    let total = nagoya::block_on(async {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buffer = vec![0u8; 64 * 1024];
        let mut total = 0usize;
        while total < SIZE {
            let read = stream.read(&mut buffer).await.expect("read");
            if read == 0 {
                break;
            }
            assert!(
                buffer[..read].iter().all(|byte| *byte == 0xAB),
                "payload corrupted in transit"
            );
            total += read;
        }
        total
    });

    client.join().expect("client thread");
    assert_eq!(total, SIZE, "did not receive the whole payload");
}

#[test]
fn a_closed_peer_reads_zero() {
    let reactor = Reactor::start().expect("reactor");
    let handle = reactor.handle();
    let listener = TcpListener::bind(local(), &handle).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let client_handle = handle.clone();
    let client = std::thread::spawn(move || {
        nagoya::block_on(async move {
            // Connect properly, then hang up. Dropping a socket whose
            // handshake is still in flight would be a different test, and
            // a racy one: the server might never see a connection at all.
            let stream = TcpStream::connect(addr, &client_handle)
                .await
                .expect("connect");
            drop(stream);
        });
    });

    // On its own thread with a bound, because the failure this can hit is
    // a wake that never arrives: the peer's FIN can land before the server
    // registers interest, and edge triggered readiness that has already
    // passed is not redelivered. A test that waits forever for it does not
    // fail, it holds the runner until the job is killed, which is what it
    // did on CI. Bounded, the same bug is a failure in under a second.
    let server = std::thread::spawn(move || {
        nagoya::block_on(async {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buffer = [0u8; 16];
            stream.read(&mut buffer).await.expect("read")
        })
    });

    // Poll the handle rather than joining it: join would wait forever for
    // the wake that may never arrive, which is the failure being guarded
    // against. The thread is left running on timeout and dies with the
    // process, which is the right trade in a test that has already failed.
    let deadline = std::time::Instant::now() + core::time::Duration::from_secs(5);
    while !server.is_finished() {
        assert!(
            std::time::Instant::now() < deadline,
            "a hung up peer should wake the read, not hang it"
        );
        std::thread::yield_now();
    }
    let read = server.join().expect("server thread");

    client.join().expect("client thread");
    assert_eq!(read, 0, "a hung up peer should read zero");
}
