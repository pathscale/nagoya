//! Errors from the kernel, as a code rather than a boxed type.
//!
//! The type is nagoya's [`StreamError`](crate::io::StreamError). It carries
//! the platform's own number, which is what a reactor needs: the question
//! asked of every failed read is whether it means "not ready yet", and that is
//! a comparison rather than a downcast.
//!
//! Taking it from nagoya rather than restating it is what lets a TLS session
//! and this crate's socket satisfy the same trait with no conversion between
//! them.

pub use crate::io::StreamError as Errno;

/// The result of a syscall.
pub type Result<T> = core::result::Result<T, Errno>;

/// Codes this crate reports for itself, spelled out rather than taken from
/// libc where the value is the same on every target it builds for.
pub mod codes {
    use super::Errno;

    /// A write had nowhere to go: the peer is gone.
    pub const BROKEN_PIPE: Errno = Errno(32);

    /// The peer reset the connection.
    pub const CONNECTION_RESET: Errno = Errno(54);
}

/// Whether an `accept` failure concerns only the connection being accepted.
///
/// A peer that resets between the readiness notification and the accept call
/// produces one of these. Failing the listener on one would let any client
/// take a server down by connecting and immediately resetting.
///
/// A free function rather than a method, because the error type is nagoya's
/// and this is a question only a socket asks.
#[must_use]
pub fn transient_accept(error: Errno) -> bool {
    // ECONNABORTED, ECONNRESET, ECONNREFUSED. Spelled out because the values
    // differ between Linux and the BSDs and this crate has libc to ask.
    error.0 == libc::ECONNABORTED || error.0 == libc::ECONNRESET || error.0 == libc::ECONNREFUSED
}

/// The last error a syscall left in this thread's `errno`.
///
/// libc's, because this crate has libc and nagoya does not: nagoya can name
/// the constants but cannot read the location.
/// Four spellings, and the split is not Linux against the rest: Linux and
/// DragonFly say `__errno_location`, apple and FreeBSD say `__error`, and the
/// NetBSD-likes say `__errno`. `signal.rs` needs the same cell and gates it
/// the same way.
#[allow(unsafe_code)]
pub fn last() -> Errno {
    #[cfg(any(target_os = "linux", target_os = "dragonfly"))]
    use libc::__errno_location as errno_location;
    #[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
    use libc::__error as errno_location;
    #[cfg(any(target_os = "netbsd", target_os = "openbsd"))]
    use libc::__errno as errno_location;

    // SAFETY: the call returns a pointer to this thread's errno, valid for
    // the life of the thread.
    Errno(unsafe { *errno_location() })
}
