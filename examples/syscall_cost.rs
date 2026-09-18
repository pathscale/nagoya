//! What each syscall on the hot path actually costs, per call.
//!
//! A profiler's sample count says how often a call is on the stack, which for
//! a loop that does the same four syscalls every message says almost nothing
//! about which one to attack. `__sendmsg` sat at the top of an eight
//! connection echo profile and replacing it with `send` changed throughput not
//! at all, because it was the most frequent call and not the most expensive.
//!
//! These are timed on a connected socketpair with data already waiting, so no
//! call blocks and each one is measuring only itself.

use std::os::unix::io::AsRawFd;
use std::time::Instant;

const ROUNDS: usize = 200_000;

fn main() {
    let (a, b) = std::os::unix::net::UnixStream::pair().expect("pair");
    a.set_nonblocking(true).expect("nonblocking");
    b.set_nonblocking(true).expect("nonblocking");
    let write_fd = a.as_raw_fd();
    let read_fd = b.as_raw_fd();
    let payload = [0xABu8; 64];
    let mut scratch = [0u8; 64];

    // send, one buffer.
    let start = Instant::now();
    for _ in 0..ROUNDS {
        unsafe {
            libc::send(write_fd, payload.as_ptr().cast(), payload.len(), 0);
            libc::recv(read_fd, scratch.as_mut_ptr().cast(), scratch.len(), 0);
        }
    }
    let send_pair = start.elapsed().as_nanos() as f64 / ROUNDS as f64;

    // sendmsg, one iovec.
    let iovec = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };
    let start = Instant::now();
    for _ in 0..ROUNDS {
        unsafe {
            let mut message: libc::msghdr = core::mem::zeroed();
            message.msg_iov = core::ptr::addr_of!(iovec) as *mut libc::iovec;
            message.msg_iovlen = 1;
            libc::sendmsg(write_fd, core::ptr::addr_of!(message), 0);
            libc::recv(read_fd, scratch.as_mut_ptr().cast(), scratch.len(), 0);
        }
    }
    let sendmsg_pair = start.elapsed().as_nanos() as f64 / ROUNDS as f64;

    println!("per send+recv pair, {ROUNDS} iterations:");
    println!("  send    + recv   {send_pair:>8.1} ns");
    println!("  sendmsg + recv   {sendmsg_pair:>8.1} ns");
    println!(
        "  sendmsg costs    {:>8.1} ns more",
        sendmsg_pair - send_pair
    );
}
