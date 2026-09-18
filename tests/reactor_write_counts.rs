//! What the write path costs in syscalls, counted rather than timed.
//!
//! Alone in its own file on purpose. The counters are process wide and the
//! harness runs the tests within one binary in parallel, so a neighbour that
//! writes to a socket does not make this fail loudly: it inflates the count
//! silently, which is worse than a flake. Cargo runs each test binary in its
//! own process, so a file with one test in it is the isolation this needs.

#![cfg(feature = "syscall-counters")]

use nagoya::reactor::{block_on_with, counters, Addr, Reactor, TcpStream};
use std::io::Read as _;
use std::os::unix::ffi::OsStrExt as _;

/// A write that fills the send buffer does not pay a rejected `sendmsg`.
///
/// The write path used to learn the buffer was full by being told so: one
/// `sendmsg` that succeeded, then a second whose only purpose was to return
/// `EWOULDBLOCK` and establish what the first had already proved. That is half
/// of every write on a saturated socket, and it is the write side twin of the
/// wasted `recv` the read path used to make.
#[test]
fn filling_the_send_buffer_costs_no_rejected_write() {
    let path = std::env::temp_dir().join(format!("nagoya-write-count-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = std::os::unix::net::UnixListener::bind(&path).expect("bind");

    // Written in chunks the size of the send buffer, which is the case that
    // used to cost a rejected write every time: the kernel takes the chunk
    // whole, so nothing in the return value says the buffer is now full.
    let capacity = 8 * 1024;
    let chunks = 64;

    let reader = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut sink = vec![0u8; 64 * 1024];
        let mut seen = 0;
        while seen < capacity * chunks {
            match stream.read(&mut sink) {
                Ok(0) | Err(_) => break,
                Ok(n) => seen += n,
            }
        }
        seen
    });

    let addr = Addr::path(path.as_os_str().as_bytes()).expect("path fits");
    let socket = loop {
        match nagoya::reactor::socket::TcpSocket::connect(addr) {
            Ok(socket) => break socket,
            Err(_) => std::thread::yield_now(),
        }
    };

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let stream = TcpStream::from_socket(socket, &handle).expect("register");

    let _ = counters::take_writes();
    block_on_with(&reactor, async {
        let mut stream = stream;
        let chunk = vec![0xABu8; capacity];
        for _ in 0..chunks {
            stream.write_all(&chunk).await.expect("write");
        }
    });
    let (sends, rejected) = counters::take_writes();

    let seen = reader.join().expect("reader");
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        seen,
        capacity * chunks,
        "the reader should have seen it all"
    );

    // Not asserted as zero, deliberately. The peer drains on another thread,
    // so a writable edge can arrive while this side still believes it has
    // room, and the write that follows is refused. That race is inherent to
    // two parties observing one socket, and in a release build it is not even
    // deterministic.
    //
    // The bug this guards against is neither rare nor racy: it refused one
    // write for every write that succeeded, because the only way the old code
    // could learn the buffer was full was to be told so by the kernel. A tenth
    // of the chunks sits far from both, measured at 63 of 64 before the fix
    // and between 0 and 2 after it.
    let ceiling = chunks as u64 / 10;
    assert!(
        rejected <= ceiling,
        "{rejected} writes refused out of {chunks}, expected at most {ceiling}: \
         a refusal per write means fullness is being learned from the kernel \
         rather than from what the last write returned"
    );
    assert!(
        sends <= chunks as u64 + ceiling,
        "{sends} sendmsg for {chunks} chunks, expected close to one each"
    );
}
