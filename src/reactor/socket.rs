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
//! Not a general sockets library. There is no UDP and no dual-stack fallback.
//! Name resolution is [`resolve`](super::resolve); an address arrives at this
//! module already resolved. What the fleet does not use is not here.

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

/// Where `sun_path` starts inside a `sockaddr_un`.
///
/// Two on both targets, for different reasons: Linux has a single `u16`
/// `sun_family`, the BSDs have a `u8` `sun_len` and a `u8` `sun_family`. Asked
/// of the real layout rather than written down, because the one number that
/// matters here is the one the running kernel agrees with.
const SUN_PATH_OFFSET: usize = mem::offset_of!(libc::sockaddr_un, sun_path);

/// How many bytes of `sun_path` there are: 104 on macOS, 108 on Linux.
///
/// Public because it is the limit a caller has to respect, and a caller that
/// cannot name the limit finds out by having [`UnixPath::new`] refuse.
pub const UNIX_PATH_CAPACITY: usize = mem::size_of::<libc::sockaddr_un>() - SUN_PATH_OFFSET;

/// The name of a Unix-domain socket, or the absence of one.
///
/// Three things live in here, because `sun_path` is one field that means three
/// things and splitting them into three `Addr` variants would put the weight of
/// that on every match over an address that is usually TCP:
///
/// - **A filesystem path**, the ordinary case, stored without its terminator.
/// - **Unnamed**, a length of zero. Not an edge case: a client that never
///   called `bind` has no address at all, which is every ordinary Unix client,
///   so this is what almost every [`accept`](TcpListener::accept) reports.
/// - **A Linux abstract name**, a leading NUL and then bytes that are not a
///   filesystem path and may contain NULs of their own.
///
/// The bytes are inline rather than behind a pointer so that [`Addr`] stays
/// `Copy` and this module keeps allocating nothing at all.
#[derive(Clone, Copy)]
pub struct UnixPath {
    /// The `sun_path` bytes exactly as the kernel wants them, zero padded.
    bytes: [u8; UNIX_PATH_CAPACITY],
    /// How many of them are meaningful. A `u8` holds 108 comfortably.
    len: u8,
}

impl UnixPath {
    /// A socket with no name, which is what an unbound peer has.
    #[must_use]
    pub const fn unnamed() -> Self {
        Self {
            bytes: [0; UNIX_PATH_CAPACITY],
            len: 0,
        }
    }

    /// A filesystem path.
    ///
    /// Refused rather than truncated when it does not fit: a truncated path
    /// binds a socket somewhere nobody is looking, and the failure surfaces
    /// much later as a connect that finds nothing. An interior NUL is refused
    /// for the same reason, since the kernel would stop at it.
    ///
    /// With `std`, the bytes of a `Path` come from
    /// `std::os::unix::ffi::OsStrExt::as_bytes`.
    ///
    /// # Errors
    ///
    /// `EINVAL` for an empty path or one containing a NUL, `ENAMETOOLONG` for
    /// one that does not fit in `sun_path` alongside its terminator.
    pub fn new(path: &[u8]) -> Result<Self> {
        if path.is_empty() || path.contains(&0) {
            return Err(Errno(libc::EINVAL));
        }
        // One byte is kept back for the NUL the kernel reads the path up to.
        if path.len() > UNIX_PATH_CAPACITY - 1 {
            return Err(Errno(libc::ENAMETOOLONG));
        }
        let mut bytes = [0; UNIX_PATH_CAPACITY];
        bytes[..path.len()].copy_from_slice(path);
        Ok(Self {
            bytes,
            len: path.len() as u8,
        })
    }

    /// A Linux abstract socket name, which lives in no filesystem.
    ///
    /// Linux only, and deliberately not offered elsewhere: no other target has
    /// abstract sockets, so a portable-looking constructor would compile
    /// everywhere and fail to bind on macOS. The name is not NUL terminated
    /// and may contain NULs.
    ///
    /// # Errors
    ///
    /// `EINVAL` for an empty name, `ENAMETOOLONG` for one that does not fit
    /// after the leading NUL that marks it as abstract.
    #[cfg(target_os = "linux")]
    pub fn abstract_name(name: &[u8]) -> Result<Self> {
        if name.is_empty() {
            return Err(Errno(libc::EINVAL));
        }
        if name.len() > UNIX_PATH_CAPACITY - 1 {
            return Err(Errno(libc::ENAMETOOLONG));
        }
        let mut bytes = [0; UNIX_PATH_CAPACITY];
        bytes[1..=name.len()].copy_from_slice(name);
        Ok(Self {
            bytes,
            len: (name.len() + 1) as u8,
        })
    }

    /// Whether this is the absence of a name.
    #[must_use]
    pub const fn is_unnamed(&self) -> bool {
        self.len == 0
    }

    /// Whether this is a Linux abstract name rather than a filesystem path.
    #[must_use]
    pub const fn is_abstract(&self) -> bool {
        self.len > 0 && self.bytes[0] == 0
    }

    /// The filesystem path, if that is what this is.
    ///
    /// `None` for an unnamed socket and for an abstract one, because neither
    /// names anything on disk and handing back bytes that look like a path
    /// would have a caller unlink a file that was never created.
    #[must_use]
    pub fn as_path_bytes(&self) -> Option<&[u8]> {
        if self.is_unnamed() || self.is_abstract() {
            None
        } else {
            Some(&self.bytes[..self.len as usize])
        }
    }

    /// The abstract name without its leading NUL, if that is what this is.
    #[must_use]
    pub fn as_abstract_name(&self) -> Option<&[u8]> {
        if self.is_abstract() {
            Some(&self.bytes[1..self.len as usize])
        } else {
            None
        }
    }

    /// The `sun_path` bytes as the kernel wants them, terminator excluded.
    #[must_use]
    pub fn encoded(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// The length to hand `bind` and `connect`.
    ///
    /// Not `size_of::<sockaddr_un>()`. Linux accepts the whole-struct size and
    /// macOS rejects it, so the wrong one passes its own tests on the wrong
    /// machine. A filesystem path counts its terminator, an abstract name has
    /// none to count, and an unnamed socket is the family and nothing after it.
    const fn sockaddr_len(&self) -> libc::socklen_t {
        let len = self.len as usize;
        let total = if len == 0 {
            SUN_PATH_OFFSET
        } else if self.bytes[0] == 0 {
            SUN_PATH_OFFSET + len
        } else {
            SUN_PATH_OFFSET + len + 1
        };
        total as libc::socklen_t
    }
}

impl PartialEq for UnixPath {
    /// Compares the meaningful bytes only.
    ///
    /// The padding past `len` is always zero, so a derive would agree today.
    /// Written out because it would stop agreeing the first time anything
    /// built one of these without clearing the tail.
    fn eq(&self, other: &Self) -> bool {
        self.encoded() == other.encoded()
    }
}

impl Eq for UnixPath {}

impl core::fmt::Debug for UnixPath {
    /// Says which of the three things it is, rather than printing 104 bytes.
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if self.is_unnamed() {
            formatter.write_str("UnixPath(unnamed)")
        } else if let Some(name) = self.as_abstract_name() {
            write!(formatter, "UnixPath(abstract {:?})", Bytes(name))
        } else {
            write!(formatter, "UnixPath({:?})", Bytes(self.encoded()))
        }
    }
}

/// Bytes printed as text where they are text, so a path reads as a path.
struct Bytes<'a>(&'a [u8]);

impl core::fmt::Debug for Bytes<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match core::str::from_utf8(self.0) {
            Ok(text) => write!(formatter, "{text:?}"),
            Err(_) => write!(formatter, "{:?}", self.0),
        }
    }
}

/// A socket address: IPv4, IPv6, or a Unix-domain path.
///
/// Deliberately not `std::net::SocketAddr`. Carrying that type would mean
/// converting at this boundary in both directions for a value that is a port
/// and some bytes, and it has nowhere to put the Unix case at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Addr {
    /// An IPv4 address and port.
    V4([u8; 4], u16),
    /// An IPv6 address and port.
    V6([u8; 16], u16),
    /// A Unix-domain socket's name, or its lack of one.
    Path(UnixPath),
}

impl Addr {
    /// The loopback address on `port`.
    pub const fn localhost(port: u16) -> Self {
        Self::V4([127, 0, 0, 1], port)
    }

    /// A Unix-domain address for a filesystem path.
    ///
    /// # Errors
    ///
    /// As [`UnixPath::new`].
    pub fn path(path: &[u8]) -> Result<Self> {
        Ok(Self::Path(UnixPath::new(path)?))
    }

    /// The port this address names, and zero for a Unix socket.
    ///
    /// A Unix socket has no port and never will; zero is the answer rather
    /// than an `Option` because every caller of this is about to format a
    /// number and none of them want a second code path for the case where the
    /// transport is a file.
    pub const fn port(&self) -> u16 {
        match self {
            Self::V4(_, port) | Self::V6(_, port) => *port,
            Self::Path(_) => 0,
        }
    }

    /// The address family constant for this address.
    const fn family(&self) -> libc::c_int {
        match self {
            Self::V4(..) => libc::AF_INET,
            Self::V6(..) => libc::AF_INET6,
            Self::Path(_) => libc::AF_UNIX,
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
            Self::Path(path) => {
                let addr = storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_un;
                let encoded = path.encoded();
                // SAFETY: `sockaddr_un` is smaller than `sockaddr_storage`, and
                // `encoded` is at most `UNIX_PATH_CAPACITY` bytes, which is the
                // length of `sun_path`. `sun_len` on the BSDs is left at zero
                // like every other caller leaves it: the kernel reads the
                // length from the `socklen_t` argument, not from the struct.
                unsafe {
                    (*addr).sun_family = libc::AF_UNIX as libc::sa_family_t;
                    core::ptr::copy_nonoverlapping(
                        encoded.as_ptr(),
                        core::ptr::addr_of_mut!((*addr).sun_path).cast::<u8>(),
                        encoded.len(),
                    );
                }
                path.sockaddr_len()
            }
        }
    }

    /// Read an address back out of a `sockaddr_storage` the kernel filled.
    ///
    /// `len` is what the kernel reported writing, and it is not optional for
    /// `AF_UNIX`: an unnamed peer and an abstract name both start with a NUL
    /// byte, and only the length tells them apart.
    fn read_from(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<Self> {
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
            libc::AF_UNIX => {
                // Only what the kernel says it wrote, clamped because a
                // reported length may include padding past `sun_path` on some
                // kernels and this is about to index with it.
                let reported = (len as usize)
                    .saturating_sub(SUN_PATH_OFFSET)
                    .min(UNIX_PATH_CAPACITY);
                let addr = storage as *const libc::sockaddr_storage as *const libc::sockaddr_un;
                let mut bytes = [0u8; UNIX_PATH_CAPACITY];
                // SAFETY: the family field says this is a `sockaddr_un`, and
                // `reported` is clamped to the length of its `sun_path`.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        core::ptr::addr_of!((*addr).sun_path).cast::<u8>(),
                        bytes.as_mut_ptr(),
                        reported,
                    );
                }

                let len = if reported == 0 {
                    // The family and nothing after it: an unbound peer, which
                    // is the ordinary case rather than a failure to decode.
                    0
                } else if bytes[0] == 0 {
                    // A leading NUL is a Linux abstract name, taken exactly as
                    // reported since it has no terminator and may contain NULs.
                    // Nowhere else has abstract sockets, so there the same
                    // bytes are a kernel reporting slack after an unnamed peer,
                    // and reading them as a name would invent one.
                    if cfg!(target_os = "linux") {
                        reported
                    } else {
                        0
                    }
                } else {
                    // A filesystem path is NUL terminated inside `sun_path`,
                    // and the reported length may or may not count that byte.
                    bytes[..reported]
                        .iter()
                        .position(|byte| *byte == 0)
                        .unwrap_or(reported)
                };

                // Whatever was not kept is not part of the name, and leaving it
                // behind would make two equal addresses compare unequal.
                bytes[len..].fill(0);
                Some(Self::Path(UnixPath {
                    bytes,
                    len: len as u8,
                }))
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

/// Create a non-blocking stream socket for `addr`'s family.
///
/// Non-blocking from birth rather than set afterwards: a socket that is
/// briefly blocking is a socket that can briefly stall the reactor.
///
/// Named for the family it is given rather than for TCP, because nothing in it
/// is TCP-specific: `AF_UNIX` wants the same `SOCK_STREAM`, the same
/// non-blocking-at-birth rule, and `SO_NOSIGPIPE` for the same reason, since
/// writing to a Unix socket whose peer has gone raises `SIGPIPE` exactly as a
/// TCP one does.
fn stream_socket(addr: &Addr) -> Result<Fd> {
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
    Addr::read_from(&storage, len).ok_or(Errno(libc::EAFNOSUPPORT))
}

/// A connected stream socket: TCP, or Unix-domain.
///
/// Still named for TCP because that is what almost every one of these is, and
/// because renaming it would churn every caller in the fleet for a type whose
/// only family-dependent method is [`set_nodelay`](Self::set_nodelay).
#[derive(Debug)]
pub struct TcpSocket(Fd);

impl TcpSocket {
    /// Start connecting to `addr`.
    ///
    /// The socket is non-blocking, so this returns as soon as the handshake is
    /// under way rather than when it completes: `connect` reports `EINPROGRESS`
    /// and the socket becomes *writable* once it finishes. The caller waits for
    /// that edge and then checks [`Self::connect_error`].
    ///
    /// A Unix-domain connect does not go in flight at all: it succeeds
    /// outright, or fails immediately with `ENOENT` for a path with no socket
    /// file or `ECONNREFUSED` for one with nothing listening. Both fall out of
    /// the same code; a caller written to TCP's shape should not read an
    /// instant answer as a bug.
    pub fn connect(addr: Addr) -> Result<Self> {
        let fd = stream_socket(&addr)?;
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
        // `TCP_NODELAY` lives at `IPPROTO_TCP`, and asking for it on an
        // `AF_UNIX` descriptor fails `ENOPROTOOPT`. Unconditionally, the `?`
        // would turn a connect that actually worked into an error naming a
        // socket option, which points nowhere near the cause.
        if addr.family() != libc::AF_UNIX {
            socket.set_nodelay(true)?;
        }
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
    ///
    /// TCP only. On a Unix-domain socket this fails `ENOPROTOOPT`, which is the
    /// honest answer: there is no Nagle to turn off.
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

    /// How many bytes are waiting to be read, if the kernel will say.
    ///
    /// `None` means it would not, and the caller should assume there may be
    /// more rather than fewer. That bias is the whole safety property here: a
    /// wrong "nothing left" loses a wakeup and hangs a connection, while a
    /// wrong "something left" costs one `recv` that returns `EWOULDBLOCK`,
    /// which is exactly what this call exists to avoid and no worse than not
    /// having made it.
    ///
    /// `FIONREAD` is on both targets and is measurably cheaper than the `recv`
    /// it replaces, 123ns against 176ns on this machine.
    pub fn pending(&self) -> Option<usize> {
        let mut waiting: libc::c_int = 0;
        // SAFETY: FIONREAD writes one `c_int`, which is what is passed.
        let result =
            unsafe { libc::ioctl(self.raw(), libc::FIONREAD, core::ptr::addr_of_mut!(waiting)) };
        if result < 0 || waiting < 0 {
            None
        } else {
            Some(waiting as usize)
        }
    }

    /// The configured send buffer size, as the kernel reports it.
    ///
    /// Read once when a stream is registered and then kept. Asking per write
    /// would cost exactly the syscall this is meant to save, and the answer
    /// does not change underneath a connection.
    ///
    /// The write side has no equivalent of [`Self::pending`] to lean on:
    /// `FIONWRITE` is a TCP facility and returns nothing for a Unix socket,
    /// which is measured, not assumed. The capacity alone is still enough for
    /// the case that matters, because a write of at least this many bytes that
    /// the kernel took whole has necessarily filled the buffer.
    pub fn send_buffer(&self) -> Option<usize> {
        let mut size: libc::c_int = 0;
        let mut len = core::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: `size` and `len` are live, and `len` describes `size`.
        let result = unsafe {
            libc::getsockopt(
                self.raw(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                core::ptr::addr_of_mut!(size).cast(),
                core::ptr::addr_of_mut!(len),
            )
        };
        if result < 0 || size < 0 {
            None
        } else {
            Some(size as usize)
        }
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
        #[cfg(feature = "syscall-counters")]
        crate::reactor::counters::RECV.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        let read = libc::recv(self.raw(), pointer.cast(), len, 0);
        if read < 0 {
            let error = crate::reactor::error::last();
            #[cfg(feature = "syscall-counters")]
            if error.would_block() {
                crate::reactor::counters::RECV_WOULD_BLOCK
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
            }
            Err(error)
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
        #[cfg(feature = "syscall-counters")]
        crate::reactor::counters::SEND.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
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
            let error = crate::reactor::error::last();
            #[cfg(feature = "syscall-counters")]
            if error.would_block() {
                crate::reactor::counters::SEND_WOULD_BLOCK
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
            }
            Err(error)
        } else {
            Ok(sent as usize)
        }
    }
}

/// A listening stream socket: TCP, or Unix-domain.
#[derive(Debug)]
pub struct TcpListener {
    fd: Fd,
    /// The family bound, kept because `accept` has to behave differently for
    /// `AF_UNIX` and asking the kernel would be a `getsockname` per connection.
    family: libc::c_int,
}

impl TcpListener {
    /// Bind to `addr` and start listening.
    ///
    /// A Unix-domain bind creates the socket file and fails `EADDRINUSE` if one
    /// is already there, including one left behind by a process that died. The
    /// caller unlinks the path first and on clean shutdown; nothing here does
    /// it, because a bind that quietly removes whatever it finds would remove a
    /// live server's socket.
    pub fn bind(addr: Addr, backlog: i32) -> Result<Self> {
        let family = addr.family();
        let fd = stream_socket(&addr)?;
        // Without this, a restart fails to bind while the previous socket's
        // connections drain through TIME_WAIT. There is no TIME_WAIT on a Unix
        // socket and no option to ask about, so it is not asked for.
        if family != libc::AF_UNIX {
            set_flag(fd.raw(), libc::SOL_SOCKET, libc::SO_REUSEADDR, true)?;
        }

        // SAFETY: zeroed storage, then written by `write_to`.
        let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let len = addr.write_to(&mut storage);
        // SAFETY: `storage` holds a valid address of `len` bytes.
        check(unsafe { libc::bind(fd.raw(), core::ptr::addr_of!(storage).cast(), len) })?;
        check(unsafe { libc::listen(fd.raw(), backlog) })?;

        Ok(Self { fd, family })
    }

    /// The descriptor, for registering with the poller.
    #[inline]
    pub fn raw(&self) -> i32 {
        self.fd.raw()
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

        let addr = match Addr::read_from(&storage, len) {
            Some(addr) => addr,
            // A Unix client that never called `bind` has no address, and that
            // is every ordinary Unix client rather than an edge case. Some
            // kernels do not even set the family on the way out, so failing
            // here would fail every accept with an errno about address
            // families, which names nothing that went wrong.
            None if self.family == libc::AF_UNIX => Addr::Path(UnixPath::unnamed()),
            None => return Err(Errno(libc::EAFNOSUPPORT)),
        };
        // SAFETY: `raw` is a fresh, non-blocking, connected descriptor.
        let socket = unsafe { TcpSocket::from_raw(raw) };
        // As in `connect`: TCP's option, and fatal on a Unix socket.
        if self.family != libc::AF_UNIX {
            socket.set_nodelay(true)?;
        }
        Ok((socket, addr))
    }
}
