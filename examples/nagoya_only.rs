//! The nagoya echo arm on its own, in two shapes, long enough to profile.
//!
//! `uds_scale` runs every arm in sequence, so sampling it catches whichever
//! arm happened to be running. This runs only nagoya, at the connection count
//! where it is furthest behind tokio, so a profile is unambiguous about whose
//! code it is looking at.
//!
//! # The two shapes
//!
//! `barrier` is what `uds_scale` measures today: one future, which writes to
//! every connection and then reads from every connection in order. All the
//! connections have a message in flight at once, so it looks like concurrency,
//! but the reads are a barrier. Connection 3's reply cannot be consumed until
//! connection 0's has been, and no connection starts its next message until
//! the slowest one has finished this one. A round costs the maximum latency of
//! its connections, not the average, and a descheduled peer thread stalls all
//! of them.
//!
//! `join` is the shape the tokio arm gets from `spawn_local`, which this crate
//! spells [`TaskSet`](nagoya::reactor::TaskSet): one future per connection,
//! each with its own waker, each polled only when its own socket is ready. A
//! fast connection runs ahead of a slow one.
//!
//! The runtime, the reactor, the syscalls and the peer are identical between
//! them. Only the pacing differs, which is what makes the difference between
//! the two numbers a property of the benchmark rather than of the crate.

use std::io::{Read as _, Write as _};
use std::os::unix::ffi::OsStrExt;

const SMALL: usize = 64;
const MESSAGES: usize = 50_000;
const CONNECTIONS: usize = 8;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 3 && args[1] == "serve" {
        return child(&args[2]);
    }
    let mode = args.get(1).cloned().unwrap_or_else(|| "both".into());

    if mode == "both" || mode == "barrier" {
        for round in 0..3 {
            println!("barrier [r{}] {:.0} msg/s", round + 1, run(false));
        }
    }
    if mode == "both" || mode == "join" {
        for round in 0..3 {
            println!("join    [r{}] {:.0} msg/s", round + 1, run(true));
        }
    }
}

fn run(join: bool) -> f64 {
    use nagoya::io::Stream as _;
    use nagoya::reactor::{block_on_with, Addr, Reactor, TaskSet, TcpStream};

    let path = std::env::temp_dir().join(format!(
        "nagoya-only-{}-{}.sock",
        std::process::id(),
        u32::from(join)
    ));
    let _ = std::fs::remove_file(&path);
    let mut child = std::process::Command::new(std::env::current_exe().expect("exe"))
        .arg("serve")
        .arg(&path)
        .spawn()
        .expect("spawn");
    while !path.exists() {
        std::thread::yield_now();
    }

    let addr = Addr::path(path.as_os_str().as_bytes()).expect("path fits");
    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let sockets: Vec<_> = (0..CONNECTIONS)
        .map(|_| loop {
            match nagoya::reactor::socket::TcpSocket::connect(addr) {
                Ok(socket) => break socket,
                Err(_) => std::thread::yield_now(),
            }
        })
        .collect();

    let start = std::time::Instant::now();
    if join {
        let mut tasks = TaskSet::new();
        for socket in sockets {
            let mut stream = TcpStream::from_socket(socket, &handle).expect("register");
            tasks.push(async move {
                let outgoing = [0xABu8; SMALL];
                let mut incoming = [0u8; SMALL];
                for _ in 0..MESSAGES {
                    stream.write_all(&outgoing).await.expect("write");
                    stream.read_exact(&mut incoming).await.expect("read");
                }
            });
        }
        block_on_with(&reactor, tasks);
    } else {
        block_on_with(&reactor, async {
            let mut streams: Vec<TcpStream> = sockets
                .into_iter()
                .map(|socket| TcpStream::from_socket(socket, &handle).expect("register"))
                .collect();
            let outgoing = [0xABu8; SMALL];
            let mut incoming = [0u8; SMALL];
            for _ in 0..MESSAGES {
                for stream in streams.iter_mut() {
                    stream.write_all(&outgoing).await.expect("write");
                }
                for stream in streams.iter_mut() {
                    stream.read_exact(&mut incoming).await.expect("read");
                }
            }
        });
    }
    let elapsed = start.elapsed();
    child.wait().expect("child");
    let _ = std::fs::remove_file(&path);
    (CONNECTIONS * MESSAGES) as f64 / elapsed.as_secs_f64()
}

fn child(path: &str) {
    let listener = std::os::unix::net::UnixListener::bind(path).expect("bind");
    let mut workers = Vec::new();
    for _ in 0..CONNECTIONS {
        let (mut stream, _) = listener.accept().expect("accept");
        workers.push(std::thread::spawn(move || {
            let mut buffer = [0u8; SMALL];
            for _ in 0..MESSAGES {
                if stream.read_exact(&mut buffer).is_err() {
                    return;
                }
                if stream.write_all(&buffer).is_err() {
                    return;
                }
            }
        }));
    }
    for worker in workers {
        let _ = worker.join();
    }
}
