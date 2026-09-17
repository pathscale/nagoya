//! Socket helpers shared by the reactor's integration tests.
//!
//! These live in one place rather than in each test module because they are the
//! only `unsafe` the tests need, and duplicating raw `socketpair` plumbing per
//! module is how one copy quietly drifts from the others.

// Test scaffolding that has to call the kernel to make a real socket. The
// crate-wide deny is lifted here for the same reason as in `poller`.
#![allow(unsafe_code, dead_code)]

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// A connected pair of non-blocking Unix sockets.
pub fn socket_pair() -> (OwnedFd, OwnedFd) {
    let mut fds = [0i32; 2];
    // SAFETY: `fds` is a live array of two ints, which is what socketpair fills.
    let result = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    assert_eq!(result, 0, "socketpair failed");
    for fd in fds {
        // SAFETY: `fd` is a descriptor socketpair just returned.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
    // SAFETY: both descriptors are fresh and owned exclusively here.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

/// Write a single byte, to make the other end readable.
pub fn write_byte(fd: &OwnedFd) {
    let byte = b"x";
    // SAFETY: writing one byte from a live buffer to a valid descriptor.
    let written = unsafe { libc::write(fd.as_raw_fd(), byte.as_ptr().cast::<libc::c_void>(), 1) };
    assert_eq!(written, 1, "write failed");
}
