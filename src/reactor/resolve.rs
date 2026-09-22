//! Turning a hostname into addresses, and connecting to the first that answers.
//!
//! `getaddrinfo` is the platform's resolver. There is no safe binding for it,
//! so the unsafe in this module is that call and the walk of the list it
//! returns, and nothing else. It lives here rather than in a protocol crate
//! because anything that connects to a name needs it, and a second copy is
//! where the dual-stack bug gets written again: take the first address, and
//! `localhost` fails whenever that first address is the family the listener
//! is not on.

#![allow(unsafe_code)]

use std::ffi::CString;

use super::error::{Errno, Result};
use super::net::TcpStream;
use super::socket::Addr;
use super::Handle;

/// Why [`resolve`] produced no addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveError {
    /// `host` contained an interior nul, so it cannot be passed to `getaddrinfo`.
    Nul,
    /// `getaddrinfo` failed. The number is the platform's `EAI_*` code, which
    /// is not an errno and must not be compared with one.
    Failed(i32),
    /// The call succeeded and returned nothing this crate can connect to.
    Empty,
}

impl core::fmt::Display for ResolveError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Nul => formatter.write_str("host has a nul byte"),
            Self::Failed(code) => write!(formatter, "getaddrinfo failed ({code})"),
            Self::Empty => formatter.write_str("resolver returned no address"),
        }
    }
}

impl std::error::Error for ResolveError {}

/// Resolve `host` to every address the platform offers, at `port`.
///
/// All of them, in the resolver's order, rather than just the first. A host
/// with both an A and an AAAA record is ordinary, and on a machine where only
/// one family actually works, taking the first answer fails outright. The
/// caller tries them in turn ([`connect_any`]).
///
/// Uses `getaddrinfo`, so it honours the hosts file, search domains and
/// whatever else the machine is configured with. The port is the caller's:
/// the service string is not passed, and each returned address carries `port`
/// rather than whatever the resolver would have filled in.
///
/// **This blocks the calling thread, and there is nowhere here to put it.**
/// `getaddrinfo` is synchronous and can sit in DNS for as long as the
/// resolver's timeout, which is seconds, not milliseconds. This crate has no
/// `spawn_blocking`. The arrangement the reactor documentation recommends is
/// the one where the polling thread *is* the reactor thread, so a call to
/// this function from inside a task stalls every other descriptor on that
/// reactor for the whole lookup. It is a plain `fn` and not an `async fn`
/// precisely so that this is visible at the call site rather than hidden
/// behind an await.
///
/// Resolve before entering the reactor where the name is known up front,
/// which is the common case for a client that connects to a configured host.
/// A caller that must resolve names while a reactor is running needs a thread
/// of its own for it; this function will not provide one.
pub fn resolve(host: &str, port: u16) -> core::result::Result<Vec<Addr>, ResolveError> {
    let name = CString::new(host).map_err(|_| ResolveError::Nul)?;

    // SAFETY: an all-zero `addrinfo` is a valid set of hints.
    let mut hints: libc::addrinfo = unsafe { core::mem::zeroed() };
    hints.ai_family = libc::AF_UNSPEC;
    hints.ai_socktype = libc::SOCK_STREAM;

    let mut result: *mut libc::addrinfo = core::ptr::null_mut();
    // SAFETY: `name` is a live C string, `hints` a live local, and `result`
    // receives a list that is freed below when the call succeeds.
    let status = unsafe {
        libc::getaddrinfo(
            name.as_ptr(),
            core::ptr::null(),
            core::ptr::addr_of!(hints),
            core::ptr::addr_of_mut!(result),
        )
    };
    if status != 0 {
        // A failed call leaves `result` undefined. Do not free it.
        return Err(ResolveError::Failed(status));
    }
    if result.is_null() {
        return Err(ResolveError::Empty);
    }

    let mut found = Vec::new();
    let mut cursor = result;
    while !cursor.is_null() {
        // SAFETY: the list is well formed until the null terminator, and this
        // walk finishes before `freeaddrinfo` below.
        let entry = unsafe { &*cursor };
        match entry.ai_family {
            libc::AF_INET => {
                // SAFETY: the family says this is a `sockaddr_in`.
                let addr = unsafe { &*(entry.ai_addr as *const libc::sockaddr_in) };
                found.push(Addr::V4(addr.sin_addr.s_addr.to_ne_bytes(), port));
            }
            libc::AF_INET6 => {
                // SAFETY: the family says this is a `sockaddr_in6`.
                let addr = unsafe { &*(entry.ai_addr as *const libc::sockaddr_in6) };
                found.push(Addr::V6(addr.sin6_addr.s6_addr, port));
            }
            // A family this crate does not speak, skipped rather than guessed at.
            _ => {}
        }
        cursor = entry.ai_next;
    }
    // SAFETY: `result` came from a successful `getaddrinfo` and is freed once.
    unsafe { libc::freeaddrinfo(result) };

    if found.is_empty() {
        return Err(ResolveError::Empty);
    }
    Ok(found)
}

/// Connect to the first address in `addrs` that accepts a connection.
///
/// Sequential rather than raced. Happy eyeballs buys latency on a dual-stacked
/// network; what is needed here is only that a host answering on one family
/// is reachable. An empty list fails with `ECONNREFUSED`, the same answer as
/// a list whose every attempt was refused.
pub async fn connect_any(addrs: &[Addr], handle: &Handle) -> Result<TcpStream> {
    let mut last = Errno(libc::ECONNREFUSED);
    for addr in addrs {
        match TcpStream::connect(*addr, handle).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last = error,
        }
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_nul_in_the_host_is_refused_before_the_resolver() {
        let error = resolve("local\0host", 80).unwrap_err();
        assert_eq!(error, ResolveError::Nul);
    }

    #[test]
    fn an_address_literal_comes_back_with_the_callers_port() {
        let addrs = resolve("127.0.0.1", 9).expect("resolve");
        assert!(
            addrs
                .iter()
                .any(|addr| *addr == Addr::V4([127, 0, 0, 1], 9)),
            "127.0.0.1 did not resolve to itself: {addrs:?}"
        );
    }

    #[test]
    fn localhost_keeps_every_family_and_the_port() {
        let addrs = resolve("localhost", 1234).expect("localhost did not resolve");
        assert!(!addrs.is_empty(), "no addresses");
        assert!(addrs.iter().all(|addr| addr.port() == 1234));
        let v4 = addrs.iter().any(|addr| matches!(addr, Addr::V4(..)));
        let v6 = addrs.iter().any(|addr| matches!(addr, Addr::V6(..)));
        assert!(v4 || v6, "localhost resolved to neither family: {addrs:?}");
    }
}
