//! The thread local reactor, through the public API.
//!
//! Moved out of the implementation file: these use only what the crate
//! exports, so they are integration tests and belong beside the other ones
//! rather than at the bottom of the module they exercise.

use nagoya::reactor::{block_on_with, Reactor};

use nagoya::reactor::socket::Addr;
use nagoya::reactor::TcpListener;

#[test]
fn drives_a_connection_on_one_thread() {
    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let listener = TcpListener::bind(Addr::localhost(0), &handle).expect("bind");
    let addr = listener.local_addr().expect("addr");

    // The peer is an ordinary blocking socket on another thread, so this
    // test is about the local loop rather than about two of them.
    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let mut stream =
            std::net::TcpStream::connect(std::net::SocketAddr::from(([127, 0, 0, 1], addr.port())))
                .expect("connect");
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).expect("read");
        stream.write_all(&byte).expect("write");
    });

    let echoed = block_on_with(&reactor, async {
        let (mut stream, _) = listener.accept().await.expect("accept");
        stream.write_all(b"x").await.expect("write");
        let mut byte = [0u8; 1];
        let read = stream.read(&mut byte).await.expect("read");
        assert_eq!(read, 1, "short read");
        byte
    });

    peer.join().expect("peer");
    assert_eq!(&echoed, b"x");
}
