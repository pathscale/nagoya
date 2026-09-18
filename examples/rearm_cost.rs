//! What the exact-fill re-arm should cost, measured before choosing.
//!
//! `poll_read` re-arms readability when a read filled its buffer, which makes a
//! framed reader pay a `recv` that can only return `EWOULDBLOCK`. Two ways to
//! avoid that, and the choice should be a measurement rather than a preference:
//!
//! - **FIONREAD**: ask the kernel how much is left, and re-arm only if there
//!   is something. One `ioctl` on the exact-fill path instead of one `recv` on
//!   the next read.
//! - **leave it**: keep paying the wasted `recv`.
//!
//! Both are syscalls, so the question is purely which is cheaper on this
//! machine. Timed on a socket with a known amount of data pending, so neither
//! call blocks and only their own cost is in the number.

use std::os::unix::io::AsRawFd;
use std::time::Instant;

const ROUNDS: usize = 200_000;

fn main() {
    let (a, b) = std::os::unix::net::UnixStream::pair().expect("pair");
    a.set_nonblocking(true).expect("nonblocking");
    let fd = a.as_raw_fd();

    // Leave plenty pending so neither call is measuring an empty socket.
    use std::io::Write as _;
    let mut writer = b;
    writer.set_nonblocking(true).expect("nonblocking writer");
    let payload = vec![0u8; 4096];
    // Non-blocking, and we stop at the first refusal, so this cannot wedge on
    // a socketpair whose reader never drains.
    while writer.write(&payload).is_ok() {}

    // FIONREAD on a socket that has data.
    let start = Instant::now();
    let mut pending: libc::c_int = 0;
    for _ in 0..ROUNDS {
        unsafe {
            libc::ioctl(fd, libc::FIONREAD, std::ptr::addr_of_mut!(pending));
        }
        std::hint::black_box(pending);
    }
    let fionread = start.elapsed().as_nanos() as f64 / ROUNDS as f64;

    // A zero length recv, the cheapest possible stand-in for the wasted call.
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let got = unsafe { libc::recv(fd, std::ptr::null_mut(), 0, 0) };
        std::hint::black_box(got);
    }
    let empty_recv = start.elapsed().as_nanos() as f64 / ROUNDS as f64;

    // The real wasted call: a recv into a real buffer on a drained socket.
    let (c, d) = std::os::unix::net::UnixStream::pair().expect("pair");
    c.set_nonblocking(true).expect("nonblocking");
    drop(d);
    let drained = c.as_raw_fd();
    let mut scratch = [0u8; 64];
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let got = unsafe { libc::recv(drained, scratch.as_mut_ptr().cast(), scratch.len(), 0) };
        std::hint::black_box(got);
    }
    let wasted_recv = start.elapsed().as_nanos() as f64 / ROUNDS as f64;

    println!("per call, {ROUNDS} iterations:");
    println!("  FIONREAD ioctl            {fionread:>7.1} ns");
    println!("  recv(len 0)               {empty_recv:>7.1} ns");
    println!("  recv on a drained socket  {wasted_recv:>7.1} ns");
}
