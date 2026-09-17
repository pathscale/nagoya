//! Sockets over libc, without `std::net`.
//!
//! # Why this exists
//!
//! `std::net` is not slow. Measured against raw `send`/`recv` on this machine
//! the difference was inside the noise, and `TcpStream::read` really is a
//! `recv` and an error map with nothing between them. Speed is not the reason
//! this module is here.
//!
//! The reasons are the interface and the dependency.
//!
//! **The interface is the wrong shape for a reactor.** Non-blocking is a
//! hidden mode rather than a type: `set_nonblocking(true)` flips a flag and
//! leaves a `TcpStream` that looks exactly like a blocking one, so nothing
//! stops a blocking read on the reactor thread, which would stall every
//! connection sharing it. `WouldBlock` arrives as an `io::Error`, when "not
//! ready" is the ordinary state of a reactive socket and the signal the whole
//! design turns on. And `io::Read::read` demands initialised memory, so
//! filling a buffer means zeroing bytes the kernel is about to overwrite.
//!
//! **The dependency is the other half.** Everything else in this crate builds
//! without Rust's standard library; the socket layer was the last thing
//! reaching for it. Wrapping libc keeps that reachable, because a crate
//! wrapping libc is still a crate that links no `std` of its own: the platform
//! already provides libc, and on macOS `std` itself goes through libSystem.
//!
//! # What it is not
//!
//! Not a general sockets library. There is no UDP, no dual-stack fallback, no
//! name resolution: an address arrives already resolved. What the fleet does
//! not use is not here.

// Calling the kernel is this module's entire purpose.
#![allow(unsafe_code)]

use core::mem;

use super::error::{Errno, Result};

/// An owned file descriptor.
///
/// Closes on drop. The whole reason to have this rather than a bare `i32` is
/// that a descriptor is a resource, and a leaked one is a connection the peer
/// thinks is open forever.
#[derive(Debug)]
pub struct Fd(i32);

impl Fd {
    /// Take ownership of a raw descriptor.
    ///
    /// # Safety
    ///
    /// `fd` must be an open descriptor that nothing else will close.
    pub unsafe fn from_raw(fd: i32) -> Self {
        Self(fd)
    }

    /// The underlying descriptor, still owned by this value.
    #[inline]
    pub fn raw(&self) -> i32 {
        self.0
    }

    /// Give up ownership without closing.
    pub fn into_raw(self) -> i32 {
        let raw = self.0;
        mem::forget(self);
        raw
    }
}

impl Drop for Fd {
    fn drop(&mut self) {
        // A failing close is not actionable: the descriptor is gone either
        // way, and on every platform this targets a retry on EINTR would risk
        // closing a descriptor another thread has already been handed.
        unsafe { libc::close(self.0) };
    }
}

/// An IPv4 or IPv6 socket address.
///
/// Deliberately not `std::net::SocketAddr`. Carrying that type would mean
/// converting at this boundary in both directions for a value that is a port
/// and some bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Addr {
    /// An IPv4 address and port.
    V4([u8; 4], u16),
    /// An IPv6 address and port.
    V6([u8; 16], u16),
}

impl Addr {
    /// The loopback address on `port`.
    pub const fn localhost(port: u16) -> Self {
        Self::V4([127, 0, 0, 1], port)
    }

    /// The port this address names.
    pub const fn port(&self) -> u16 {
        match self {
            Self::V4(_, port) | Self::V6(_, port) => *port,
        }
    }

    /// The address family constant for this address.
    const fn family(&self) -> libc::c_int {
        match self {
            Self::V4(..) => libc::AF_INET,
            Self::V6(..) => libc::AF_INET6,
        }
    }

    /// Write this address into a `sockaddr_storage`, returning its length.
    fn write_to(&self, storage: &mut libc::sockaddr_storage) -> libc::socklen_t {
        match self {
            Self::V4(octets, port) => {
                let addr = storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in;
                // SAFETY: `sockaddr_in` is smaller than `sockaddr_storage`,
                // which exists precisely to be written through as any family.
                unsafe {
                    (*addr).sin_family = libc::AF_INET as libc::sa_family_t;
                    (*addr).sin_port = port.to_be();
                    (*addr).sin_addr.s_addr = u32::from_ne_bytes(*octets);
                }
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
            }
            Self::V6(octets, port) => {
                let addr = storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in6;
                // SAFETY: as above, for the v6 layout.
                unsafe {
                    (*addr).sin6_family = libc::AF_INET6 as libc::sa_family_t;
                    (*addr).sin6_port = port.to_be();
                    (*addr).sin6_addr.s6_addr = *octets;
                }
                mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
            }
        }
    }

    /// Read an address back out of a `sockaddr_storage`.
    fn read_from(storage: &libc::sockaddr_storage) -> Option<Self> {
        match libc::c_int::from(storage.ss_family) {
            libc::AF_INET => {
                let addr = storage as *const libc::sockaddr_storage as *const libc::sockaddr_in;
                // SAFETY: the family field says this is a `sockaddr_in`.
                unsafe {
                    Some(Self::V4(
                        (*addr).sin_addr.s_addr.to_ne_bytes(),
                        u16::from_be((*addr).sin_port),
                    ))
                }
            }
            libc::AF_INET6 => {
                let addr = storage as *const libc::sockaddr_storage as *const libc::sockaddr_in6;
                // SAFETY: the family field says this is a `sockaddr_in6`.
                unsafe {
                    Some(Self::V6(
                        (*addr).sin6_addr.s6_addr,
                        u16::from_be((*addr).sin6_port),
                    ))
                }
            }
            // A family this crate does not speak. Reported as absent rather
            // than guessed at.
            _ => None,
        }
    }
}

/// Turn a negative return into an error, the way every one of these calls
/// reports failure.
#[inline]
fn check(value: libc::c_int) -> Result<libc::c_int> {
    if value < 0 {
        Err(crate::reactor::error::last())
    } else {
        Ok(value)
    }
}

/// Create a non-blocking TCP socket for `addr`'s family.
///
/// Non-blocking from birth rather than set afterwards: a socket that is
/// briefly blocking is a socket that can briefly stall the reactor.
fn tcp_socket(addr: &Addr) -> Result<Fd> {
    // SOCK_NONBLOCK and SOCK_CLOEXEC are Linux extensions to `socket`; the
    // BSDs need two more calls. Both paths end at the same place.
    #[cfg(target_os = "linux")]
    let raw = check(unsafe {
        libc::socket(
            addr.family(),
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    })?;

    #[cfg(not(target_os = "linux"))]
    let raw = {
        let raw = check(unsafe { libc::socket(addr.family(), libc::SOCK_STREAM, 0) })?;
        // SAFETY: `raw` is a descriptor the call above just returned.
        unsafe {
            let flags = libc::fcntl(raw, libc::F_GETFL);
            libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK);
            libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC);
        }
        raw
    };

    // SAFETY: `raw` is a fresh descriptor owned exclusively here.
    let fd = unsafe { Fd::from_raw(raw) };

    // SIGPIPE on a closed peer would kill the process. Every socket in this
    // crate wants the error return instead. macOS spells it as a socket
    // option; Linux has no equivalent and uses MSG_NOSIGNAL per call, which
    // `send_vectored` passes.
    #[cfg(not(target_os = "linux"))]
    {
        let on: libc::c_int = 1;
        // SAFETY: SO_NOSIGPIPE takes an int, which is what is passed.
        unsafe {
            libc::setsockopt(
                fd.raw(),
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                core::ptr::addr_of!(on).cast(),
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }

    Ok(fd)
}

/// Set a boolean socket option.
fn set_flag(fd: i32, level: libc::c_int, name: libc::c_int, on: bool) -> Result<()> {
    let value: libc::c_int = i32::from(on);
    // SAFETY: every option used here takes an int.
    check(unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            core::ptr::addr_of!(value).cast(),
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    })?;
    Ok(())
}

/// Ask the kernel for one of a socket's own addresses.
fn addr_of(fd: i32, peer: bool) -> Result<Addr> {
    // SAFETY: an all-zero `sockaddr_storage` is valid; the kernel fills it.
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let call = if peer {
        libc::getpeername
    } else {
        libc::getsockname
    };
    // SAFETY: `storage` and `len` are live locals of the right types.
    check(unsafe {
        call(
            fd,
            core::ptr::addr_of_mut!(storage).cast(),
            core::ptr::addr_of_mut!(len),
        )
    })?;
    Addr::read_from(&storage).ok_or(Errno(libc::EAFNOSUPPORT))
}

/// A connected TCP socket.
#[derive(Debug)]
pub struct TcpSocket(Fd);

impl TcpSocket {
    /// Start connecting to `addr`.
    ///
    /// The socket is non-blocking, so this returns as soon as the handshake is
    /// under way rather than when it completes: `connect` reports `EINPROGRESS`
    /// and the socket becomes *writable* once it finishes. The caller waits for
    /// that edge and then checks [`Self::connect_error`].
    pub fn connect(addr: Addr) -> Result<Self> {
        let fd = tcp_socket(&addr)?;
        // SAFETY: zeroed storage, then written by `write_to` for this family.
        let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let len = addr.write_to(&mut storage);

        // SAFETY: `storage` holds a valid address of `len` bytes.
        let result = unsafe { libc::connect(fd.raw(), core::ptr::addr_of!(storage).cast(), len) };
        if result < 0 {
            let error = crate::reactor::error::last();
            // Expected: the handshake is in flight.
            if error.0 != libc::EINPROGRESS {
                return Err(error);
            }
        }

        let socket = Self(fd);
        socket.set_nodelay(true)?;
        Ok(socket)
    }

    /// Whether an in-flight connect has failed, once the socket is writable.
    ///
    /// A failed non-blocking connect does not report through `connect`; it
    /// leaves the reason in `SO_ERROR` and makes the socket writable, which is
    /// indistinguishable from success without this check.
    pub fn connect_error(&self) -> Result<()> {
        let mut value: libc::c_int = 0;
        let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: SO_ERROR writes an int, and that is what is passed.
        check(unsafe {
            libc::getsockopt(
                self.raw(),
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                core::ptr::addr_of_mut!(value).cast(),
                core::ptr::addr_of_mut!(len),
            )
        })?;
        if value == 0 {
            Ok(())
        } else {
            Err(Errno(value))
        }
    }

    /// Adopt an existing descriptor, which must already be non-blocking.
    ///
    /// # Safety
    ///
    /// `fd` must be an open, non-blocking, connected TCP socket that nothing
    /// else will close.
    pub unsafe fn from_raw(fd: i32) -> Self {
        Self(Fd::from_raw(fd))
    }

    /// The descriptor, for registering with the poller.
    #[inline]
    pub fn raw(&self) -> i32 {
        self.0.raw()
    }

    /// Turn Nagle's algorithm off.
    ///
    /// A framed protocol and Nagle interact badly: a small frame written on its
    /// own waits for an acknowledgement that is itself waiting for more data.
    pub fn set_nodelay(&self, on: bool) -> Result<()> {
        set_flag(self.raw(), libc::IPPROTO_TCP, libc::TCP_NODELAY, on)
    }

    /// This socket's own address.
    pub fn local_addr(&self) -> Result<Addr> {
        addr_of(self.raw(), false)
    }

    /// The address of the peer.
    pub fn peer_addr(&self) -> Result<Addr> {
        addr_of(self.raw(), true)
    }

    /// Receive into `buffer`, which need not be initialised.
    ///
    /// This is the call `std::io::Read` cannot express. It takes a pointer and
    /// a length rather than an initialised slice, so a growable buffer is
    /// filled without first writing zeros the kernel immediately overwrites.
    ///
    /// # Safety
    ///
    /// `pointer` must be valid for writes of `len` bytes.
    pub unsafe fn recv(&self, pointer: *mut u8, len: usize) -> Result<usize> {
        let read = libc::recv(self.raw(), pointer.cast(), len, 0);
        if read < 0 {
            Err(crate::reactor::error::last())
        } else {
            Ok(read as usize)
        }
    }

    /// Send two buffers as one segment, without joining them.
    ///
    /// A frame is a short header and a payload the caller already owns.
    /// Concatenating them to get one `send` copies the payload for the sake of
    /// at most fourteen leading bytes; this hands the kernel both addresses.
    pub fn send_vectored(&self, first: &[u8], second: &[u8]) -> Result<usize> {
        let iovecs = [
            libc::iovec {
                iov_base: first.as_ptr() as *mut libc::c_void,
                iov_len: first.len(),
            },
            libc::iovec {
                iov_base: second.as_ptr() as *mut libc::c_void,
                iov_len: second.len(),
            },
        ];

        // An empty leading slice would have the kernel walk a zero length
        // entry for nothing; skipping it is one fewer iovec to process on the
        // common path where the header is already out.
        let (start, count) = if first.is_empty() { (1, 1) } else { (0, 2) };

        // SAFETY: `msghdr` is a plain C struct; zero is a valid starting value
        // and every field used is set below. The pointer offset is in bounds:
        // `start` is 0 or 1 and `iovecs` has two entries.
        let mut message: libc::msghdr = unsafe { mem::zeroed() };
        message.msg_iov = unsafe { iovecs.as_ptr().add(start) as *mut libc::iovec };
        message.msg_iovlen = count as _;

        // MSG_NOSIGNAL is how Linux asks for EPIPE instead of SIGPIPE. The
        // BSDs have no such flag and use SO_NOSIGPIPE, set when the socket was
        // created.
        #[cfg(target_os = "linux")]
        let flags = libc::MSG_NOSIGNAL;
        #[cfg(not(target_os = "linux"))]
        let flags = 0;

        // SAFETY: `message` points at `iovecs`, a live local, and describes
        // `count` entries starting at `start`, all within it.
        let sent = unsafe { libc::sendmsg(self.raw(), core::ptr::addr_of!(message), flags) };
        if sent < 0 {
            Err(crate::reactor::error::last())
        } else {
            Ok(sent as usize)
        }
    }
}

/// A listening TCP socket.
#[derive(Debug)]
pub struct TcpListener(Fd);

impl TcpListener {
    /// Bind to `addr` and start listening.
    pub fn bind(addr: Addr, backlog: i32) -> Result<Self> {
        let fd = tcp_socket(&addr)?;
        // Without this, a restart fails to bind while the previous socket's
        // connections drain through TIME_WAIT.
        set_flag(fd.raw(), libc::SOL_SOCKET, libc::SO_REUSEADDR, true)?;

        // SAFETY: zeroed storage, then written by `write_to`.
        let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let len = addr.write_to(&mut storage);
        // SAFETY: `storage` holds a valid address of `len` bytes.
        check(unsafe { libc::bind(fd.raw(), core::ptr::addr_of!(storage).cast(), len) })?;
        check(unsafe { libc::listen(fd.raw(), backlog) })?;

        Ok(Self(fd))
    }

    /// The descriptor, for registering with the poller.
    #[inline]
    pub fn raw(&self) -> i32 {
        self.0.raw()
    }

    /// The address this listener is bound to.
    ///
    /// Worth asking for even when the bind address was chosen by the caller: a
    /// port of zero means "any", and this is how to learn which one arrived.
    pub fn local_addr(&self) -> Result<Addr> {
        addr_of(self.raw(), false)
    }

    /// Accept one connection.
    ///
    /// The accepted socket is non-blocking, like everything else here.
    pub fn accept(&self) -> Result<(TcpSocket, Addr)> {
        // SAFETY: zeroed storage the kernel fills with the peer address.
        let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

        // `accept4` takes the flags directly on Linux; elsewhere they are set
        // afterwards, which is a race only in the sense that the descriptor is
        // briefly blocking before anyone can use it.
        #[cfg(target_os = "linux")]
        let raw = check(unsafe {
            libc::accept4(
                self.raw(),
                core::ptr::addr_of_mut!(storage).cast(),
                core::ptr::addr_of_mut!(len),
                libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            )
        })?;

        #[cfg(not(target_os = "linux"))]
        let raw = {
            let raw = check(unsafe {
                libc::accept(
                    self.raw(),
                    core::ptr::addr_of_mut!(storage).cast(),
                    core::ptr::addr_of_mut!(len),
                )
            })?;
            // SAFETY: `raw` is the descriptor just accepted.
            unsafe {
                let flags = libc::fcntl(raw, libc::F_GETFL);
                libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK);
                libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC);
                let on: libc::c_int = 1;
                libc::setsockopt(
                    raw,
                    libc::SOL_SOCKET,
                    libc::SO_NOSIGPIPE,
                    core::ptr::addr_of!(on).cast(),
                    mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
            raw
        };

        let addr = Addr::read_from(&storage).ok_or(Errno(libc::EAFNOSUPPORT))?;
        // SAFETY: `raw` is a fresh, non-blocking, connected descriptor.
        let socket = unsafe { TcpSocket::from_raw(raw) };
        socket.set_nodelay(true)?;
        Ok((socket, addr))
    }
}
