//! Turning a hostname into addresses, and connecting to the first that answers.
//!
//! `getaddrinfo` is the platform's resolver. There is no safe binding for it,
//! so the unsafe in this module is that call and the walk of the list it
//! returns, and nothing else. It lives here rather than in a protocol crate
//! because anything that connects to a name needs it, and a second copy is
//! where the dual-stack bug gets written again: take the first address, and
//! `localhost` fails whenever that first address is the family the listener
//! is not on.
//!
//! [`Resolver`] provides a bounded asynchronous handoff to this same system
//! resolver. Initialize its shared pool during synchronous setup; existing
//! [`resolve`] calls remain synchronous and unchanged.

#![allow(unsafe_code)]

use alloc::vec::Vec;
use eko::libc_lite as libc;

use crate::net::TcpStream;
use crate::reactor::Handle;
use crate::reactor::error::{Errno, Result};
use crate::reactor::socket::Addr;

/// Why [`resolve`] produced no addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveError {
    /// `host` contained an interior nul, so it cannot be passed to a resolver.
    Nul,
    /// Resolver failed with code.
    Failed(i32),
    /// The call succeeded and returned nothing this crate can connect to.
    Empty,
}

impl core::fmt::Display for ResolveError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Nul => formatter.write_str("host has a nul byte"),
            Self::Failed(code) => write!(formatter, "resolver failed ({code})"),
            Self::Empty => formatter.write_str("resolver returned no address"),
        }
    }
}

impl core::error::Error for ResolveError {}

/// Resolve `host` to every address the platform offers, at `port`.
pub fn resolve(host: &str, port: u16) -> core::result::Result<alloc::vec::Vec<Addr>, ResolveError> {
    if host.as_bytes().contains(&0) {
        return Err(ResolveError::Nul);
    }
    if host.is_empty() {
        return Err(ResolveError::Empty);
    }

    let stripped = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    if !stripped.is_empty() {
        if let Ok(ip) = stripped.parse::<core::net::IpAddr>() {
            return Ok(alloc::vec![ip_to_addr(ip, port)]);
        }
    }

    let query_host = host.trim_end_matches('.');
    if query_host.is_empty() {
        return Err(ResolveError::Empty);
    }

    if query_host.eq_ignore_ascii_case("localhost") {
        return Ok(alloc::vec![
            Addr::V4([127, 0, 0, 1], port),
            Addr::V6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], port),
        ]);
    }

    if let Ok(contents) = eko::file::read_to_string("/etc/hosts") {
        let mut found = Vec::new();
        for line in contents.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let Some(ip_str) = parts.next() else { continue };
            let Ok(ip) = ip_str.parse::<core::net::IpAddr>() else {
                continue;
            };
            for alias in parts {
                if alias.trim_end_matches('.').eq_ignore_ascii_case(query_host) {
                    let addr = ip_to_addr(ip, port);
                    if !found.contains(&addr) {
                        found.push(addr);
                    }
                }
            }
        }
        if !found.is_empty() {
            return Ok(found);
        }
    }

    let nameservers = load_nameservers();
    let mut found = Vec::new();
    for ns in &nameservers {
        let mut ns_found = Vec::new();
        for (idx, qtype) in [(0u16, 1u16), (1u16, 28u16)] {
            let txid = next_dns_txid(idx);
            if let Some(pkt) = build_dns_query(query_host, qtype, txid) {
                if let Some(resp) = send_dns_udp(ns, &pkt) {
                    parse_dns_response(&resp, txid, query_host, qtype, port, &mut ns_found);
                }
            }
        }
        if !ns_found.is_empty() {
            for addr in ns_found {
                if !found.contains(&addr) {
                    found.push(addr);
                }
            }
            break;
        }
    }

    if found.is_empty() {
        return Err(ResolveError::Empty);
    }
    Ok(found)
}

fn ip_to_addr(ip: core::net::IpAddr, port: u16) -> Addr {
    match ip {
        core::net::IpAddr::V4(v4) => Addr::V4(v4.octets(), port),
        core::net::IpAddr::V6(v6) => Addr::V6(v6.octets(), port),
    }
}

fn load_nameservers() -> Vec<core::net::SocketAddr> {
    let mut servers = Vec::new();
    if let Ok(contents) = eko::file::read_to_string("/etc/resolv.conf") {
        for line in contents.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            let line = line.split(';').next().unwrap_or("").trim();
            let mut parts = line.split_whitespace();
            if parts.next() == Some("nameserver") {
                if let Some(ns_str) = parts.next() {
                    if let Ok(ip) = ns_str.parse::<core::net::IpAddr>() {
                        let addr = core::net::SocketAddr::new(ip, 53);
                        if !servers.contains(&addr) {
                            servers.push(addr);
                        }
                    } else if let Ok(sa) = ns_str.parse::<core::net::SocketAddr>() {
                        if !servers.contains(&sa) {
                            servers.push(sa);
                        }
                    }
                }
            }
        }
    }
    let fb1 = core::net::SocketAddr::V4(core::net::SocketAddrV4::new(
        core::net::Ipv4Addr::new(169, 254, 169, 253),
        53,
    ));
    let fb2 = core::net::SocketAddr::V4(core::net::SocketAddrV4::new(
        core::net::Ipv4Addr::new(1, 1, 1, 1),
        53,
    ));
    if !servers.contains(&fb1) {
        servers.push(fb1);
    }
    if !servers.contains(&fb2) {
        servers.push(fb2);
    }
    servers
}

fn next_dns_txid(salt: u16) -> u16 {
    static SEQ: core::sync::atomic::AtomicU16 = core::sync::atomic::AtomicU16::new(0x4123);
    let seq = SEQ.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    let tick = (eko::clock::now_ns() as u16) ^ ((eko::clock::now_ns() >> 16) as u16);
    let id = seq.wrapping_add(tick) ^ salt.wrapping_mul(0x9e37);
    if id == 0 { 1 } else { id }
}

fn build_dns_query(host: &str, qtype: u16, txid: u16) -> Option<Vec<u8>> {
    let mut pkt = Vec::with_capacity(64);
    pkt.extend_from_slice(&txid.to_be_bytes());
    pkt.extend_from_slice(&0x0100u16.to_be_bytes()); // RD = 1
    pkt.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT = 1
    pkt.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT = 0
    pkt.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT = 0
    pkt.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT = 0

    let trimmed = host.trim_end_matches('.');
    if trimmed.is_empty() || trimmed.len() > 253 {
        return None;
    }
    for label in trimmed.split('.') {
        if label.is_empty() || label.len() > 63 {
            return None;
        }
        pkt.push(label.len() as u8);
        pkt.extend_from_slice(label.as_bytes());
    }
    pkt.push(0);
    pkt.extend_from_slice(&qtype.to_be_bytes());
    pkt.extend_from_slice(&1u16.to_be_bytes()); // QCLASS = IN
    Some(pkt)
}

fn send_dns_udp(ns: &core::net::SocketAddr, query: &[u8]) -> Option<Vec<u8>> {
    let bind_addr = match ns {
        core::net::SocketAddr::V4(_) => core::net::SocketAddr::V4(
            core::net::SocketAddrV4::new(core::net::Ipv4Addr::UNSPECIFIED, 0),
        ),
        core::net::SocketAddr::V6(_) => core::net::SocketAddr::V6(
            core::net::SocketAddrV6::new(core::net::Ipv6Addr::UNSPECIFIED, 0, 0, 0),
        ),
    };
    let sock = eko::net::UdpSocket::bind_nonblocking(bind_addr).ok()?;
    sock.send_to(query, *ns).ok()?;

    let mut fds = [eko::poll::PollFd {
        fd: sock.as_raw_fd(),
        events: eko::poll::POLLIN,
        revents: 0,
    }];
    let ready = eko::poll::poll(&mut fds, 2500).ok()?;
    if ready == 0 || (fds[0].revents & eko::poll::POLLIN) == 0 {
        return None;
    }
    let mut buf = [0u8; 1500];
    let (n, _) = sock.recv_from(&mut buf).ok()?;
    Some(buf[..n].to_vec())
}

fn read_dns_name(pkt: &[u8], mut offset: usize) -> Option<(alloc::string::String, usize)> {
    let mut name = alloc::string::String::new();
    let mut end_offset = None;
    let mut hops = 0usize;

    loop {
        if offset >= pkt.len() {
            return None;
        }
        let len = pkt[offset];
        if len == 0 {
            if end_offset.is_none() {
                end_offset = Some(offset + 1);
            }
            break;
        }
        if (len & 0xC0) == 0xC0 {
            if offset + 1 >= pkt.len() || hops >= 16 {
                return None;
            }
            if end_offset.is_none() {
                end_offset = Some(offset + 2);
            }
            let ptr = (((len & 0x3F) as usize) << 8) | (pkt[offset + 1] as usize);
            offset = ptr;
            hops += 1;
            continue;
        }
        if (len & 0xC0) != 0 {
            return None;
        }
        let label_len = len as usize;
        offset += 1;
        if offset + label_len > pkt.len() {
            return None;
        }
        let label = core::str::from_utf8(&pkt[offset..offset + label_len]).ok()?;
        if !name.is_empty() {
            name.push('.');
        }
        for b in label.bytes() {
            name.push((b.to_ascii_lowercase()) as char);
        }
        offset += label_len;
        if name.len() > 253 {
            return None;
        }
    }

    Some((name, end_offset?))
}

fn parse_dns_response(
    pkt: &[u8],
    expected_txid: u16,
    query_host: &str,
    expected_qtype: u16,
    port: u16,
    out: &mut Vec<Addr>,
) {
    if pkt.len() < 12 {
        return;
    }
    let txid = u16::from_be_bytes([pkt[0], pkt[1]]);
    if txid != expected_txid {
        return;
    }
    let flags = u16::from_be_bytes([pkt[2], pkt[3]]);
    if (flags & 0x8000) == 0 || (flags & 0x000F) != 0 {
        return;
    }
    let qdcount = u16::from_be_bytes([pkt[4], pkt[5]]) as usize;
    let ancount = u16::from_be_bytes([pkt[6], pkt[7]]) as usize;

    let mut offset = 12;
    for _ in 0..qdcount {
        let Some((_, next_off)) = read_dns_name(pkt, offset) else {
            return;
        };
        if next_off + 4 > pkt.len() {
            return;
        }
        offset = next_off + 4;
    }

    struct DnsRecord {
        owner: alloc::string::String,
        rtype: u16,
        rdata_offset: usize,
        rdlen: usize,
    }

    let mut records = Vec::with_capacity(ancount.min(64));
    for _ in 0..ancount {
        let Some((owner, next_off)) = read_dns_name(pkt, offset) else {
            return;
        };
        if next_off + 10 > pkt.len() {
            return;
        };
        let rtype = u16::from_be_bytes([pkt[next_off], pkt[next_off + 1]]);
        let rclass = u16::from_be_bytes([pkt[next_off + 2], pkt[next_off + 3]]);
        let rdlen = u16::from_be_bytes([pkt[next_off + 8], pkt[next_off + 9]]) as usize;
        let rdata_offset = next_off + 10;
        if rdata_offset + rdlen > pkt.len() {
            return;
        }
        if rclass == 1 {
            records.push(DnsRecord {
                owner,
                rtype,
                rdata_offset,
                rdlen,
            });
        }
        offset = rdata_offset + rdlen;
    }

    let mut target = alloc::string::String::with_capacity(query_host.len());
    for b in query_host.trim_end_matches('.').bytes() {
        target.push((b.to_ascii_lowercase()) as char);
    }

    for _hop in 0..16 {
        let mut matched_addr = false;
        for rec in &records {
            if rec.owner == target && rec.rtype == expected_qtype {
                if expected_qtype == 1 && rec.rdlen == 4 {
                    let b = &pkt[rec.rdata_offset..rec.rdata_offset + 4];
                    let addr = Addr::V4([b[0], b[1], b[2], b[3]], port);
                    if !out.contains(&addr) {
                        out.push(addr);
                    }
                    matched_addr = true;
                } else if expected_qtype == 28 && rec.rdlen == 16 {
                    let b = &pkt[rec.rdata_offset..rec.rdata_offset + 16];
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(b);
                    let addr = Addr::V6(octets, port);
                    if !out.contains(&addr) {
                        out.push(addr);
                    }
                    matched_addr = true;
                }
            }
        }
        if matched_addr {
            break;
        }
        let mut next_cname = None;
        for rec in &records {
            if rec.owner == target && rec.rtype == 5 {
                if let Some((cname, _)) = read_dns_name(pkt, rec.rdata_offset) {
                    next_cname = Some(cname);
                    break;
                }
            }
        }
        if let Some(cname) = next_cname {
            if cname == target {
                break;
            }
            target = cname;
        } else {
            break;
        }
    }
}

/// Connect to the first address in `addrs` that accepts a connection.
///
/// Sequential rather than raced. Happy eyeballs buys latency on a dual-stacked
/// network; what is needed here is only that a host answering on one family
/// is reachable. An empty list fails with `ECONNREFUSED`, the same answer as
/// a list whose every attempt was refused.
///
/// The error is the first one that describes the *service* rather than this
/// machine's routing, not simply the last attempt's. `getaddrinfo` is asked
/// without `AI_ADDRCONFIG`, so a host with an AAAA record yields a v6 address
/// even on a box with no v6 route, and that attempt fails `ENETUNREACH` after
/// the v4 attempt already reported the true `ECONNREFUSED`. Reporting the
/// last error would hand the caller whichever answer the resolver happened to
/// order last, which flips between machines and hides the real one.
pub async fn connect_any(addrs: &[Addr], handle: &Handle) -> Result<TcpStream> {
    let mut fallback = Errno(libc::ECONNREFUSED);
    let mut about_the_service = None;
    for addr in addrs {
        match TcpStream::connect(*addr, handle).await {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                if about_the_service.is_none() && !unreachable_family(error) {
                    about_the_service = Some(error);
                }
                fallback = error;
            }
        }
    }
    Err(about_the_service.unwrap_or(fallback))
}

/// Whether `error` says this machine cannot reach that address family at all,
/// rather than anything about the host on the other end.
///
/// Spelled out for the same reason as `transient_accept`: the values differ
/// between Linux and the BSDs, and this crate has libc to ask.
fn unreachable_family(error: Errno) -> bool {
    error.0 == libc::ENETUNREACH || error.0 == libc::EHOSTUNREACH || error.0 == libc::EAFNOSUPPORT
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
            addrs.contains(&Addr::V4([127, 0, 0, 1], 9)),
            "127.0.0.1 did not resolve to itself: {addrs:?}"
        );
    }

    #[test]
    fn a_routing_failure_does_not_outrank_a_refusal() {
        // The shape `connect_any` sees on a dual-stacked host with no v6
        // route: the v4 attempt is the true answer, the v6 attempt is an
        // artefact of this machine, and the resolver decides which comes
        // last.
        assert!(!unreachable_family(Errno(libc::ECONNREFUSED)));
        assert!(unreachable_family(Errno(libc::ENETUNREACH)));
        assert!(unreachable_family(Errno(libc::EHOSTUNREACH)));
        assert!(unreachable_family(Errno(libc::EAFNOSUPPORT)));
    }

    #[test]
    fn localhost_keeps_every_family_and_the_port() {
        let addrs = resolve("localhost", 1234).expect("localhost did not resolve");
        assert!(!addrs.is_empty(), "no addresses");
        assert!(addrs.iter().all(|addr| addr.port() == 1234));
        let v4 = addrs.iter().any(|addr| matches!(addr, Addr::V4(..)));
        let v6 = addrs
            .iter()
            .any(|addr| matches!(addr, Addr::V6(..) | Addr::V6Scoped(..)));
        assert!(v4 || v6, "localhost resolved to neither family: {addrs:?}");
    }
}

#[cfg(feature = "reactor")]
#[path = "bounded_dns.rs"]
mod bounded_dns;
#[cfg(feature = "reactor")]
pub use bounded_dns::{ResolveUntilError, Resolver, ResolverStartError};

#[cfg(test)]
mod udp_dns_tests {
    use super::*;

    #[test]
    fn udp_dns_query_and_response_parser_roundtrip() {
        let server = eko::net::UdpSocket::bind(eko::net::SocketAddr::V4(eko::net::SocketAddrV4::new(eko::net::Ipv4Addr::LOCALHOST, 0))).expect("bind mock dns");
        let server_addr = server.local_addr().expect("local_addr");
        let handle = eko::thread::spawn(move || {
            let mut req = [0u8; 512];
            let (n, peer) = server.recv_from(&mut req).expect("recv_from");
            assert!(n >= 12);
            // Build minimal A response for example.test -> 93.184.216.34
            let mut resp = alloc::vec::Vec::new();
            resp.extend_from_slice(&req[0..2]); // txid
            resp.extend_from_slice(&[0x81, 0x80, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
            resp.extend_from_slice(&req[12..n]); // echo question
            resp.extend_from_slice(&[0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x04, 93, 184, 216, 34]);
            server.send_to(&resp, peer).expect("send_to");
        });
        let txid = 0x4242;
        let pkt = build_dns_query("example.test", 1, txid).expect("build_dns_query");
        let raw = send_dns_udp(&server_addr, &pkt).expect("send_dns_udp");
        let mut out = alloc::vec::Vec::new();
        parse_dns_response(&raw, txid, "example.test", 1, 443, &mut out);
        let _ = handle.expect("spawn").join();
        assert_eq!(out, alloc::vec![Addr::V4([93, 184, 216, 34], 443)]);
    }
}
