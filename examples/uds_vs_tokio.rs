//! Unix-domain sockets: nagoya against tokio, and against the floor.
//!
//! # What this measures
//!
//! Request/response round trip latency on one Unix socket connection. A client
//! writes a fixed size message, a server on another thread reads it and writes
//! it straight back, and the client reads the reply. That is deliberately the
//! shape Karen has, an RPC over `<state_dir>/*.sock`, rather than a streaming
//! throughput figure which a Unix socket wins by memcpy speed and which says
//! nothing about how fast a request is answered.
//!
//! Round trip means the number includes both directions and both wakeups. It
//! is not halved into a "one way latency", because nothing here measured one
//! way and the halving would be an assumption presented as a measurement.
//!
//! # The arms
//!
//! - **nagoya**, on a reactor with no thread of its own, driven by
//!   `block_on_with` on the same thread as the task. This is the arrangement
//!   the AF_UNIX work exists for and the one Karen uses.
//! - **tokio current_thread**, which is tokio's fastest shape for one
//!   connection: no work stealing, no cross worker handoff.
//! - **tokio multi_thread**, the default anyone gets by writing `#[tokio::main]`.
//! - **blocking std**, no runtime at all. Two threads, two blocking sockets.
//!   Not a competitor, a floor: it is the least a round trip can cost on this
//!   machine, and every async number should be read against it.
//! - **nagoya TCP loopback**, the same nagoya code over `127.0.0.1`. The delta
//!   against the nagoya Unix arm is what the address family is worth, measured
//!   rather than assumed, on identical code.
//!
//! # How it is run
//!
//! Arms are interleaved and repeated, and the first pair is a **null
//! calibration**: nagoya against itself. Whatever spread that pair shows is
//! noise, because it is the same implementation twice, and no difference
//! smaller than it means anything. This matters here more than usual: the
//! sibling websocket benchmark swings ninety thousand to one hundred and eighty
//! thousand messages a second on unchanged code, so a single run of anything is
//! not evidence.
//!
//! Percentiles rather than a mean. A mean round trip hides exactly the thing a
//! request/response service is judged on, which is what the slow ones cost.

use std::io::{Read as _, Write as _};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Bytes each way. Small, because this is a latency measurement and a large
/// message would turn it into a memory bandwidth measurement.
const MESSAGE: usize = 64;

/// Round trips per sample. Large enough that the timing loop dominates the
/// setup, small enough to run every arm several times.
const ITERATIONS: usize = 20_000;

/// Round trips discarded at the start of each sample.
///
/// The first few pay for page faults, the connection's first wakeups and the
/// allocator settling, none of which repeat.
const WARMUP: usize = 2_000;

/// How many times the whole set of arms is run.
const ROUNDS: usize = 3;

fn main() {
    let mut results: Vec<(String, Vec<u64>)> = Vec::new();

    for round in 0..ROUNDS {
        // The null calibration goes first, so the noise floor is on screen
        // before any comparison is.
        results.push((
            format!("null: nagoya uds (a) [round {}]", round + 1),
            nagoya_unix(),
        ));
        results.push((
            format!("null: nagoya uds (b) [round {}]", round + 1),
            nagoya_unix(),
        ));

        results.push((format!("nagoya uds [round {}]", round + 1), nagoya_unix()));
        results.push((
            format!("tokio uds current_thread [round {}]", round + 1),
            tokio_unix(false),
        ));
        results.push((
            format!("tokio uds multi_thread [round {}]", round + 1),
            tokio_unix(true),
        ));
        results.push((
            format!("blocking std uds [round {}]", round + 1),
            blocking_unix(),
        ));
        results.push((
            format!("nagoya tcp loopback [round {}]", round + 1),
            nagoya_tcp(),
        ));
    }

    report(&results);
    report_syscalls();
}

/// What the latency table cannot answer: whether a gap is syscall count or
/// syscall cost.
///
/// Only nagoya can be counted here, because the counters are this crate's. That
/// is still the useful half: if nagoya is already at the theoretical minimum of
/// one `recv` and one wait per round trip, the tail is cost or scheduling and
/// not work, and looking for a redundant syscall would be looking for something
/// that is not there.
#[cfg(feature = "syscall-counters")]
fn report_syscalls() {
    let timed = (ITERATIONS - WARMUP) as f64;

    // Cleared, then one more sample, so the numbers belong to that sample
    // alone rather than to every arm run above it.
    let _ = nagoya::reactor::counters::take();
    nagoya_unix();
    let (recv, would_block, wait) = nagoya::reactor::counters::take();
    println!("\nnagoya uds syscalls, both ends, over {timed} timed round trips:");
    println!(
        "  recv        {recv:>9}  ({:.2} per round trip)",
        recv as f64 / timed
    );
    println!(
        "  EWOULDBLOCK {would_block:>9}  ({:.2} per round trip)",
        would_block as f64 / timed
    );
    println!(
        "  poller wait {wait:>9}  ({:.2} per round trip)",
        wait as f64 / timed
    );

    let _ = nagoya::reactor::counters::take();
    nagoya_tcp();
    let (recv, would_block, wait) = nagoya::reactor::counters::take();
    println!("\nnagoya tcp loopback syscalls, same basis:");
    println!(
        "  recv        {recv:>9}  ({:.2} per round trip)",
        recv as f64 / timed
    );
    println!(
        "  EWOULDBLOCK {would_block:>9}  ({:.2} per round trip)",
        would_block as f64 / timed
    );
    println!(
        "  poller wait {wait:>9}  ({:.2} per round trip)",
        wait as f64 / timed
    );
}

#[cfg(not(feature = "syscall-counters"))]
fn report_syscalls() {
    println!("\n(build with --features syscall-counters for the syscall breakdown)");
}

/// Print every sample, then the per arm summary across rounds.
fn report(results: &[(String, Vec<u64>)]) {
    println!(
        "{MESSAGE} byte round trips, {} timed per sample ({WARMUP} discarded), {ROUNDS} rounds\n",
        ITERATIONS - WARMUP
    );
    println!(
        "{:<38} {:>10} {:>10} {:>10} {:>10}",
        "sample", "p50 ns", "p90 ns", "p99 ns", "max ns"
    );
    for (label, nanos) in results {
        let mut sorted = nanos.clone();
        sorted.sort_unstable();
        println!(
            "{:<38} {:>10} {:>10} {:>10} {:>10}",
            label,
            percentile(&sorted, 0.50),
            percentile(&sorted, 0.90),
            percentile(&sorted, 0.99),
            sorted.last().copied().unwrap_or(0),
        );
    }

    println!("\nbest p50 per arm across {ROUNDS} rounds:");
    let mut arms: Vec<&str> = Vec::new();
    for (label, _) in results {
        let arm = label.split(" [round").next().unwrap_or(label);
        if !arms.contains(&arm) {
            arms.push(arm);
        }
    }
    for arm in arms {
        let mut best = u64::MAX;
        let mut worst = 0;
        for (label, nanos) in results {
            if label.split(" [round").next() != Some(arm) {
                continue;
            }
            let mut sorted = nanos.clone();
            sorted.sort_unstable();
            let p50 = percentile(&sorted, 0.50);
            best = best.min(p50);
            worst = worst.max(p50);
        }
        println!("  {arm:<38} best {best:>8} ns   worst {worst:>8} ns");
    }
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}

/// A socket path that removes itself, and is unique per call.
struct SocketPath(PathBuf);

impl SocketPath {
    fn new(label: &str) -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let serial = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "nagoya-bench-{label}-{}-{serial}.sock",
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

/// nagoya over AF_UNIX, on a reactor with no thread of its own.
fn nagoya_unix() -> Vec<u64> {
    use nagoya::reactor::socket::TcpListener as RawListener;
    use nagoya::reactor::{block_on_with, Addr, Reactor, TcpListener, TcpStream};

    let path = SocketPath::new("nagoya-uds");
    let addr = Addr::path(path.as_path().as_os_str().as_bytes()).expect("path fits");

    // Bound here, before the server thread exists, so the client below cannot
    // arrive before the socket is listening and nothing has to be signalled.
    let listener = RawListener::bind(addr, 128).expect("bind");

    let server = std::thread::spawn(move || {
        let reactor = Reactor::local().expect("reactor");
        let listener = TcpListener::from_listener(listener, &reactor.handle()).expect("register");
        block_on_with(&reactor, async {
            let (mut stream, _) = listener.accept().await.expect("accept");
            echo_loop_nagoya(&mut stream).await;
        });
    });

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let nanos = block_on_with(&reactor, async {
        let mut stream = TcpStream::connect(addr, &handle).await.expect("connect");
        round_trips_nagoya(&mut stream).await
    });

    server.join().expect("server");
    nanos
}

/// The same nagoya code over loopback TCP, for the family delta.
fn nagoya_tcp() -> Vec<u64> {
    use nagoya::reactor::socket::TcpListener as RawListener;
    use nagoya::reactor::{block_on_with, Addr, Reactor, TcpListener, TcpStream};

    let listener = RawListener::bind(Addr::localhost(0), 128).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let server = std::thread::spawn(move || {
        let reactor = Reactor::local().expect("reactor");
        let listener = TcpListener::from_listener(listener, &reactor.handle()).expect("register");
        block_on_with(&reactor, async {
            let (mut stream, _) = listener.accept().await.expect("accept");
            echo_loop_nagoya(&mut stream).await;
        });
    });

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let nanos = block_on_with(&reactor, async {
        let mut stream = TcpStream::connect(addr, &handle).await.expect("connect");
        round_trips_nagoya(&mut stream).await
    });

    server.join().expect("server");
    nanos
}

async fn echo_loop_nagoya(stream: &mut nagoya::reactor::TcpStream) {
    use nagoya::io::Stream as _;
    let mut buffer = [0u8; MESSAGE];
    for _ in 0..ITERATIONS {
        stream.read_exact(&mut buffer).await.expect("server read");
        stream.write_all(&buffer).await.expect("server write");
    }
}

async fn round_trips_nagoya(stream: &mut nagoya::reactor::TcpStream) -> Vec<u64> {
    use nagoya::io::Stream as _;
    let outgoing = [0xABu8; MESSAGE];
    let mut incoming = [0u8; MESSAGE];
    let mut nanos = Vec::with_capacity(ITERATIONS - WARMUP);
    for iteration in 0..ITERATIONS {
        let start = Instant::now();
        stream.write_all(&outgoing).await.expect("client write");
        stream.read_exact(&mut incoming).await.expect("client read");
        if iteration >= WARMUP {
            nanos.push(start.elapsed().as_nanos() as u64);
        }
    }
    nanos
}

/// tokio over AF_UNIX. `multi` picks the default multi threaded runtime over
/// the single threaded one.
fn tokio_unix(multi: bool) -> Vec<u64> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let path = SocketPath::new(if multi { "tokio-mt" } else { "tokio-ct" });
    let listener = std::os::unix::net::UnixListener::bind(path.as_path()).expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");

    let server = std::thread::spawn(move || {
        let runtime = build_runtime(multi);
        runtime.block_on(async move {
            let listener = tokio::net::UnixListener::from_std(listener).expect("adopt");
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buffer = [0u8; MESSAGE];
            for _ in 0..ITERATIONS {
                stream.read_exact(&mut buffer).await.expect("server read");
                stream.write_all(&buffer).await.expect("server write");
            }
        });
    });

    let client_path = path.as_path().to_owned();
    let runtime = build_runtime(multi);
    let nanos = runtime.block_on(async move {
        let mut stream = tokio::net::UnixStream::connect(&client_path)
            .await
            .expect("connect");
        let outgoing = [0xABu8; MESSAGE];
        let mut incoming = [0u8; MESSAGE];
        let mut nanos = Vec::with_capacity(ITERATIONS - WARMUP);
        for iteration in 0..ITERATIONS {
            let start = Instant::now();
            stream.write_all(&outgoing).await.expect("client write");
            stream.read_exact(&mut incoming).await.expect("client read");
            if iteration >= WARMUP {
                nanos.push(start.elapsed().as_nanos() as u64);
            }
        }
        nanos
    });

    server.join().expect("server");
    nanos
}

fn build_runtime(multi: bool) -> tokio::runtime::Runtime {
    if multi {
        tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .build()
            .expect("runtime")
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("runtime")
    }
}

/// Two threads and two blocking sockets. The floor, not a competitor.
fn blocking_unix() -> Vec<u64> {
    let path = SocketPath::new("blocking");
    let listener = std::os::unix::net::UnixListener::bind(path.as_path()).expect("bind");

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buffer = [0u8; MESSAGE];
        for _ in 0..ITERATIONS {
            stream.read_exact(&mut buffer).expect("server read");
            stream.write_all(&buffer).expect("server write");
        }
    });

    let mut stream = std::os::unix::net::UnixStream::connect(path.as_path()).expect("connect");
    let outgoing = [0xABu8; MESSAGE];
    let mut incoming = [0u8; MESSAGE];
    let mut nanos = Vec::with_capacity(ITERATIONS - WARMUP);
    for iteration in 0..ITERATIONS {
        let start = Instant::now();
        stream.write_all(&outgoing).expect("client write");
        stream.read_exact(&mut incoming).expect("client read");
        if iteration >= WARMUP {
            nanos.push(start.elapsed().as_nanos() as u64);
        }
    }

    server.join().expect("server");
    nanos
}
