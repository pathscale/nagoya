//! Unix sockets under concurrency and at volume: nagoya against tuned tokio.
//!
//! # Why this exists separately from `uds_vs_tokio`
//!
//! That one measures a single connection's round trip latency. This one
//! measures the two things it cannot: how throughput scales with connection
//! count, and how fast bytes move when the messages are large. A runtime can
//! win one and lose the others, and the sibling WebSocket benchmark does
//! exactly that, so quoting the latency number alone would be picking the
//! favourable measurement.
//!
//! # Giving tokio its best shot
//!
//! The comparison is only worth anything if the tokio arm is the one a
//! competent tokio user would write, so:
//!
//! - **`LocalSet` with `spawn_local`** for the single threaded arm, which is
//!   what avoids the `Send` bound and some atomic traffic on a per connection
//!   task. Plain `block_on` with a `!Send` future does not exercise the
//!   scheduler at all once there is more than one connection.
//! - **`event_interval` tuned**, not left at the default. tokio checks the I/O
//!   driver every `event_interval` task polls; the default of 61 is chosen for
//!   fairness under mixed load, and a pure I/O ping pong wants it lower.
//! - **Worker count matched to the load** for the multi threaded arm rather
//!   than defaulting to every core, because sixteen workers fighting over one
//!   connection is a configuration nobody would ship.
//!
//! Both arms get the same connection count, the same message size, the same
//! echo protocol and the same process topology. The server is a child process
//! in every case, so a runtime is never measured against itself across a
//! thread boundary it did not have to cross.
//!
//! # Reading the output
//!
//! Every configuration runs `ROUNDS` times and all of them are printed. The
//! sibling websocket benchmark swings by a factor of two on unchanged code, so
//! a single run is not evidence and the spread is part of the result.

use std::io::{Read as _, Write as _};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Connection counts swept.
const CONNECTIONS: [usize; 4] = [1, 8, 32, 128];

/// Messages per connection in the concurrency sweep.
const MESSAGES: usize = 4_000;

/// Message size for the concurrency sweep: small, so the number is about
/// wakeups and scheduling rather than memory bandwidth.
const SMALL: usize = 64;

/// Total bytes each direction in the throughput arm, per chunk size.
const VOLUME: usize = 128 * 1024 * 1024;

/// Chunk sizes swept by the throughput arm.
///
/// The socket send buffer is what makes this a sweep rather than a constant.
/// On this machine `net.local.stream.sendspace` is 8 KiB, so a chunk at or
/// under that goes out in one `send` and the arm measures moving bytes. A
/// chunk above it cannot: `write_all` partially sends, gets `EWOULDBLOCK`,
/// parks, waits for the socket to become writable and resumes, once per 8 KiB.
/// At 64 KiB that is seven re-arms per chunk, and the number stops being
/// bandwidth and starts being the cost of the writable path.
///
/// Both are worth knowing. Quoting only the second one as "throughput", which
/// is what this benchmark used to do, is not.
const CHUNKS: [usize; 5] = [4 * 1024, 8 * 1024, 16 * 1024, 64 * 1024, 256 * 1024];

/// How many times each configuration is repeated.
const ROUNDS: usize = 3;

fn main() {
    // Child mode: `<path> <mode> <connections>`.
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 4 {
        let path = &args[1];
        let connections: usize = args[3].parse().expect("connections");
        match args[2].as_str() {
            "echo" => child_echo(path, connections),
            "sink" => child_sink(path),
            other => panic!("unknown child mode {other}"),
        }
        return;
    }

    // The syscall counts come first when they are compiled in, and then the
    // sweep runs anyway: returning early instead left every arm below it dead
    // code under `--all-features`, which is a warning the gate rejects and a
    // worse answer besides, since the counts are most useful read next to the
    // throughput they are supposed to explain.
    #[cfg(feature = "syscall-counters")]
    {
        count_nagoya();
        count_throughput();
    }
    concurrency_sweep();
    throughput();
}

fn concurrency_sweep() {
    println!("== concurrency: {MESSAGES} messages of {SMALL} bytes per connection ==");
    println!("messages per second, summed across connections, higher is better\n");
    println!(
        "{:<26} {:>12} {:>12} {:>12} {:>12}",
        "arm", "1 conn", "8 conns", "32 conns", "128 conns"
    );

    /// One benchmark arm: a label, and something that returns messages per
    /// second for a connection count.
    type Arm = (&'static str, fn(usize) -> f64);

    let arms: [Arm; 4] = [
        ("nagoya", nagoya_echo),
        ("tokio current+LocalSet", tokio_echo_local),
        ("tokio multi, tuned", tokio_echo_multi),
        ("blocking std (floor)", blocking_echo),
    ];

    for round in 0..ROUNDS {
        for (label, arm) in arms {
            let mut cells = String::new();
            for connections in CONNECTIONS {
                let rate = arm(connections);
                cells.push_str(&format!("{:>12.0}", rate));
            }
            println!("{:<26}{cells}", format!("{label} [r{}]", round + 1));
        }
        println!();
    }
}

fn throughput() {
    println!(
        "== throughput: {} MiB one way, by chunk size ==",
        VOLUME / (1024 * 1024)
    );
    println!("MiB per second, higher is better");
    println!("the send buffer is 8 KiB, so anything above it pays a re-arm per 8 KiB\n");

    print!("{:<26}", "arm");
    for chunk in CHUNKS {
        print!("{:>11}", format!("{} KiB", chunk / 1024));
    }
    println!();

    type Arm = (&'static str, fn(usize) -> f64);
    let arms: [Arm; 3] = [
        ("nagoya", nagoya_throughput),
        ("tokio current+LocalSet", tokio_throughput),
        ("blocking std (floor)", blocking_throughput),
    ];
    for round in 0..ROUNDS {
        for (label, arm) in arms {
            let mut cells = String::new();
            for chunk in CHUNKS {
                cells.push_str(&format!("{:>11.0}", arm(chunk)));
            }
            println!("{:<26}{cells}", format!("{label} [r{}]", round + 1));
        }
        println!();
    }
}

/// A socket path that removes itself.
struct SocketPath(PathBuf);

impl SocketPath {
    fn new(label: &str) -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let serial = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "nagoya-scale-{label}-{}-{serial}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        Self(path)
    }

    fn as_path(&self) -> &Path {
        &self.0
    }
}

impl Drop for SocketPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Spawn the child server and wait until it is accepting.
///
/// The connect is the readiness signal, so there is no flag and no sleep.
fn start_child(path: &Path, mode: &str, connections: usize) -> std::process::Child {
    let child = std::process::Command::new(std::env::current_exe().expect("exe"))
        .arg(path)
        .arg(mode)
        .arg(connections.to_string())
        .spawn()
        .expect("spawn child");
    // Wait for the bind by trying to connect once and dropping it costs a
    // connection slot, so instead just wait for the path to appear and let the
    // real connects retry.
    while !path.exists() {
        std::thread::yield_now();
    }
    child
}

/// Connect `count` blocking sockets, retrying while the child finishes binding.
fn connect_all(path: &Path, count: usize) -> Vec<std::os::unix::net::UnixStream> {
    (0..count)
        .map(|_| loop {
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(stream) => break stream,
                Err(_) => std::thread::yield_now(),
            }
        })
        .collect()
}

// --- nagoya ---------------------------------------------------------------

/// nagoya driving `connections` echo clients on one thread, one reactor.
fn nagoya_echo(connections: usize) -> f64 {
    use nagoya::io::Stream as _;
    use nagoya::reactor::{block_on_with, Addr, Reactor, TaskSet, TcpStream};

    let path = SocketPath::new("nagoya-echo");
    let mut child = start_child(path.as_path(), "echo", connections);
    let addr = Addr::path(path.as_path().as_os_str().as_bytes()).expect("path fits");

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();

    // Connected before timing, so the measured region is messages only.
    let sockets: Vec<_> = (0..connections)
        .map(|_| loop {
            match nagoya::reactor::socket::TcpSocket::connect(addr) {
                Ok(socket) => break socket,
                Err(_) => std::thread::yield_now(),
            }
        })
        .collect();

    // One task per connection, which is the shape the tokio arm gets from
    // `spawn_local`. The earlier version of this arm was a single future that
    // wrote to every connection and then read from every connection in order,
    // and that is a barrier, not concurrency: connection three's reply could
    // not be consumed until connection zero's had been, and nobody started
    // their next message until the slowest peer thread had answered. It cost
    // about a quarter of the throughput and none of it was the runtime's.
    // Timed from here, not from `block_on_with`: the tokio arm adopts its
    // sockets and spawns its tasks inside its own timed region, so this one
    // has to pay for registration too or the comparison is tilted.
    let start = Instant::now();
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
    let elapsed = start.elapsed();

    child.wait().expect("child");
    (connections * MESSAGES) as f64 / elapsed.as_secs_f64()
}

/// nagoya streaming `VOLUME` bytes and counting them in.
fn nagoya_throughput(chunk_size: usize) -> f64 {
    use nagoya::reactor::{block_on_with, Addr, Reactor, TcpStream};

    let path = SocketPath::new("nagoya-sink");
    let mut child = start_child(path.as_path(), "sink", 1);
    let addr = Addr::path(path.as_path().as_os_str().as_bytes()).expect("path fits");

    let socket = loop {
        match nagoya::reactor::socket::TcpSocket::connect(addr) {
            Ok(socket) => break socket,
            Err(_) => std::thread::yield_now(),
        }
    };

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let start = Instant::now();
    block_on_with(&reactor, async {
        let mut stream = TcpStream::from_socket(socket, &handle).expect("register");
        let chunk = vec![0xABu8; chunk_size];
        let mut sent = 0;
        while sent < VOLUME {
            stream.write_all(&chunk).await.expect("write");
            sent += chunk_size;
        }
    });
    let elapsed = start.elapsed();

    child.wait().expect("child");
    VOLUME as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

// --- tokio ----------------------------------------------------------------

/// tokio single threaded, with `LocalSet` and `spawn_local` per connection.
fn tokio_echo_local(connections: usize) -> f64 {
    let path = SocketPath::new("tokio-local");
    let mut child = start_child(path.as_path(), "echo", connections);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        // The default of 61 favours fairness under mixed load. A pure I/O
        // workload wants the driver checked more often, and this is the knob
        // a tokio user tuning for this shape would reach for.
        .event_interval(8)
        .build()
        .expect("runtime");

    let std_streams = connect_all(path.as_path(), connections);
    for stream in &std_streams {
        stream.set_nonblocking(true).expect("nonblocking");
    }

    let local = tokio::task::LocalSet::new();
    let start = Instant::now();
    runtime.block_on(local.run_until(async move {
        let mut handles = Vec::with_capacity(connections);
        for stream in std_streams {
            let stream = tokio::net::UnixStream::from_std(stream).expect("adopt");
            handles.push(tokio::task::spawn_local(echo_client_tokio(stream)));
        }
        for handle in handles {
            handle.await.expect("client task");
        }
    }));
    let elapsed = start.elapsed();

    child.wait().expect("child");
    (connections * MESSAGES) as f64 / elapsed.as_secs_f64()
}

/// tokio multi threaded, workers matched to the connection count.
fn tokio_echo_multi(connections: usize) -> f64 {
    let path = SocketPath::new("tokio-multi");
    let mut child = start_child(path.as_path(), "echo", connections);

    // Never more workers than there is work, and never more than the machine
    // has. Sixteen workers on one connection is a configuration nobody ships.
    let workers = connections.min(
        std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(4),
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_io()
        .event_interval(8)
        .build()
        .expect("runtime");

    let std_streams = connect_all(path.as_path(), connections);
    for stream in &std_streams {
        stream.set_nonblocking(true).expect("nonblocking");
    }

    let start = Instant::now();
    runtime.block_on(async move {
        let mut handles = Vec::with_capacity(connections);
        for stream in std_streams {
            let stream = tokio::net::UnixStream::from_std(stream).expect("adopt");
            handles.push(tokio::spawn(echo_client_tokio(stream)));
        }
        for handle in handles {
            handle.await.expect("client task");
        }
    });
    let elapsed = start.elapsed();

    child.wait().expect("child");
    (connections * MESSAGES) as f64 / elapsed.as_secs_f64()
}

async fn echo_client_tokio(mut stream: tokio::net::UnixStream) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let outgoing = [0xABu8; SMALL];
    let mut incoming = [0u8; SMALL];
    for _ in 0..MESSAGES {
        stream.write_all(&outgoing).await.expect("write");
        stream.read_exact(&mut incoming).await.expect("read");
    }
}

fn tokio_throughput(chunk_size: usize) -> f64 {
    use tokio::io::AsyncWriteExt as _;

    let path = SocketPath::new("tokio-sink");
    let mut child = start_child(path.as_path(), "sink", 1);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .event_interval(8)
        .build()
        .expect("runtime");

    let std_stream = connect_all(path.as_path(), 1).pop().expect("stream");
    std_stream.set_nonblocking(true).expect("nonblocking");

    let start = Instant::now();
    runtime.block_on(async move {
        let mut stream = tokio::net::UnixStream::from_std(std_stream).expect("adopt");
        let chunk = vec![0xABu8; chunk_size];
        let mut sent = 0;
        while sent < VOLUME {
            stream.write_all(&chunk).await.expect("write");
            sent += chunk_size;
        }
    });
    let elapsed = start.elapsed();

    child.wait().expect("child");
    VOLUME as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

// --- blocking floor -------------------------------------------------------

/// One thread per connection, no runtime. The floor at low counts and the
/// cautionary tale at high ones.
fn blocking_echo(connections: usize) -> f64 {
    let path = SocketPath::new("blocking-echo");
    let mut child = start_child(path.as_path(), "echo", connections);
    let streams = connect_all(path.as_path(), connections);

    let start = Instant::now();
    let workers: Vec<_> = streams
        .into_iter()
        .map(|mut stream| {
            std::thread::spawn(move || {
                let outgoing = [0xABu8; SMALL];
                let mut incoming = [0u8; SMALL];
                for _ in 0..MESSAGES {
                    stream.write_all(&outgoing).expect("write");
                    stream.read_exact(&mut incoming).expect("read");
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("worker");
    }
    let elapsed = start.elapsed();

    child.wait().expect("child");
    (connections * MESSAGES) as f64 / elapsed.as_secs_f64()
}

fn blocking_throughput(chunk_size: usize) -> f64 {
    let path = SocketPath::new("blocking-sink");
    let mut child = start_child(path.as_path(), "sink", 1);
    let mut stream = connect_all(path.as_path(), 1).pop().expect("stream");

    let start = Instant::now();
    let chunk = vec![0xABu8; chunk_size];
    let mut sent = 0;
    while sent < VOLUME {
        stream.write_all(&chunk).expect("write");
        sent += chunk_size;
    }
    drop(stream);
    let elapsed = start.elapsed();

    child.wait().expect("child");
    VOLUME as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

// --- the child process ----------------------------------------------------

/// Echo server: one blocking thread per connection.
///
/// Deliberately the same server for every arm, and deliberately blocking, so
/// that what varies between arms is only the client's runtime. A server built
/// on one of the runtimes would put that runtime on both ends of its own
/// measurement.
fn child_echo(path: &str, connections: usize) {
    let listener = std::os::unix::net::UnixListener::bind(path).expect("bind");
    let mut workers = Vec::with_capacity(connections);
    for _ in 0..connections {
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

/// Sink: read until the peer stops, counting nothing.
fn child_sink(path: &str) {
    let listener = std::os::unix::net::UnixListener::bind(path).expect("bind");
    let (mut stream, _) = listener.accept().expect("accept");
    // Sized to the largest chunk swept, and the same for every arm and every
    // chunk size: the reader is not what is being varied.
    let mut buffer = vec![0u8; CHUNKS[CHUNKS.len() - 1]];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Syscalls per message for the nagoya arm, at each connection count.
///
/// The concurrency table says nagoya is behind; this says whether that is
/// extra work or the cost of the work it already does. Only nagoya can be
/// counted, because the counters are this crate's.
#[cfg(feature = "syscall-counters")]
pub fn count_nagoya() {
    println!("\n== nagoya syscalls per message, client end ==\n");
    println!(
        "{:>10} {:>10} {:>15} {:>10} {:>10}",
        "conns", "recv/msg", "EWOULDBLOCK/msg", "wait/msg", "wake/msg"
    );
    for connections in CONNECTIONS {
        let _ = nagoya::reactor::counters::take();
        nagoya_echo(connections);
        let (recv, block, wait) = nagoya::reactor::counters::take();
        let wakes = nagoya::reactor::counters::WAKE.swap(0, std::sync::atomic::Ordering::Relaxed);
        let messages = (connections * MESSAGES) as f64;
        println!(
            "{connections:>10} {:>10.2} {:>15.2} {:>10.2} {:>10.2}",
            recv as f64 / messages,
            block as f64 / messages,
            wait as f64 / messages,
            wakes as f64 / messages
        );
    }
}

/// Syscalls per MiB written, for the one way stream, at each chunk size.
///
/// The question this answers is whether the throughput gap is syscall count
/// or syscall cost. The send buffer is 8 KiB, so the writer cannot get more
/// than that ahead of the reader whatever the chunk size, and the floor is
/// therefore one `sendmsg` per 8 KiB no matter how large a chunk is handed
/// down. A `sendmsg` count above `chunk / 8 KiB` per chunk is work that is
/// not required; so is a wait count above the number of times the buffer
/// actually filled.
#[cfg(feature = "syscall-counters")]
pub fn count_throughput() {
    println!("== syscalls per MiB written, nagoya ==");
    println!("send buffer is 8 KiB, so the floor is 128 sendmsg per MiB\n");
    println!(
        "{:<12} {:>10} {:>14} {:>10} {:>12}",
        "chunk", "MiB/s", "sendmsg/MiB", "wblock/MiB", "waits/MiB"
    );
    for chunk in CHUNKS {
        let _ = nagoya::reactor::counters::take();
        let _ = nagoya::reactor::counters::take_writes();
        let rate = nagoya_throughput(chunk);
        let (_, _, waits) = nagoya::reactor::counters::take();
        let (sends, would_block) = nagoya::reactor::counters::take_writes();
        let mib = (VOLUME / (1024 * 1024)) as f64;
        println!(
            "{:<12} {rate:>10.0} {:>14.1} {:>10.1} {:>12.1}",
            format!("{} KiB", chunk / 1024),
            sends as f64 / mib,
            would_block as f64 / mib,
            waits as f64 / mib,
        );
    }
    println!();
}
