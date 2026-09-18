//! Why a framed reader costs more than one `recv` per message.
//!
//! `poll_read` puts readiness back when a read filled its buffer, on the
//! grounds that a full buffer has not proved the socket empty. For a streaming
//! reader whose buffer is larger than the traffic that is true and free,
//! because the read almost never fills it.
//!
//! A framed reader is the opposite case. `read_exact` asks for exactly the
//! header and then exactly the body, so *every* read fills its buffer exactly,
//! readiness goes back every time, and the next read issues a `recv` that can
//! only return `EWOULDBLOCK`. One wasted syscall per read, on the path a
//! WebSocket server takes several times per frame.
//!
//! Each end is counted on its own, with the other end in a child process,
//! because the counters are process wide and one number for both ends cannot
//! say which is paying.

use std::os::unix::ffi::OsStrExt;

const MESSAGE: usize = 64;
const ITERATIONS: usize = 20_000;

fn main() {
    // The child arm: be the echo server on the given path, then exit.
    if let Some(path) = std::env::args().nth(1) {
        serve(&path);
        return;
    }

    println!("{ITERATIONS} round trips of {MESSAGE} bytes, client end counted alone\n");
    println!(
        "{:<44} {:>9} {:>15} {:>9}",
        "client read buffer", "recv/rt", "EWOULDBLOCK/rt", "wait/rt"
    );
    for (label, size) in [
        ("exactly the message, as read_exact asks", MESSAGE),
        ("twice the message", MESSAGE * 2),
    ] {
        let (recv, block, wait) = client(size);
        println!("{label:<44} {recv:>9.2} {block:>15.2} {wait:>9.2}");
    }
}

/// Run the client end here and the server in a child, so the counters see one
/// end only.
fn client(read_buffer: usize) -> (f64, f64, f64) {
    use nagoya::reactor::{block_on_with, Addr, Reactor, TcpStream};

    let path = std::env::temp_dir().join(format!(
        "nagoya-readshape-{}-{read_buffer}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let mut child = std::process::Command::new(std::env::current_exe().expect("exe"))
        .arg(&path)
        .spawn()
        .expect("spawn server");

    // The connect itself is the readiness signal for "the child has bound":
    // no flag, no sleep, and this is setup rather than measured region.
    let addr = Addr::path(path.as_os_str().as_bytes()).expect("path fits");
    let socket = loop {
        match nagoya::reactor::socket::TcpSocket::connect(addr) {
            Ok(socket) => break socket,
            Err(_) => std::thread::yield_now(),
        }
    };

    let _ = nagoya::reactor::counters::take();

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    block_on_with(&reactor, async {
        let mut stream = TcpStream::from_socket(socket, &handle).expect("register");
        let outgoing = [0xABu8; MESSAGE];
        let mut incoming = vec![0u8; read_buffer];
        for _ in 0..ITERATIONS {
            stream.write_all(&outgoing).await.expect("client write");
            let mut filled = 0;
            while filled < MESSAGE {
                let read = stream.read(&mut incoming[filled..]).await.expect("read");
                assert!(read > 0, "peer closed early");
                filled += read;
            }
        }
    });

    let (recv, block, wait) = nagoya::reactor::counters::take();
    child.wait().expect("server exit");
    let _ = std::fs::remove_file(&path);

    let per = ITERATIONS as f64;
    (recv as f64 / per, block as f64 / per, wait as f64 / per)
}

/// The child process: echo `ITERATIONS` messages and exit.
fn serve(path: &str) {
    use nagoya::io::Stream as _;
    use nagoya::reactor::socket::TcpListener as RawListener;
    use nagoya::reactor::{block_on_with, Addr, Reactor, TcpListener};

    let addr = Addr::path(path.as_bytes()).expect("path fits");
    let listener = RawListener::bind(addr, 128).expect("bind");
    let reactor = Reactor::local().expect("reactor");
    let listener = TcpListener::from_listener(listener, &reactor.handle()).expect("register");
    block_on_with(&reactor, async {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut buffer = [0u8; MESSAGE];
        for _ in 0..ITERATIONS {
            if stream.read_exact(&mut buffer).await.is_err() {
                return;
            }
            stream.write_all(&buffer).await.expect("write");
        }
    });
}
