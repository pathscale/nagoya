//! The readiness poller: kqueue on the BSDs, epoll on Linux.
//!
//! # What this is
//!
//! One object that answers a single question: which of these file descriptors
//! can be read or written right now, and wake me when that changes. It knows
//! nothing about WebSockets, futures or tasks.
//!
//! # Why the two platforms share one type rather than one trait
//!
//! The interfaces differ in shape (kqueue takes a batch of changes and returns
//! a batch of events, epoll takes one change per call) but the *use* is
//! identical, and a trait here would buy an abstraction with exactly one
//! implementation live per target. The `cfg` is at the bottom, in the two
//! private modules, and the surface above it is identical on both.
//!
//! # Edge triggered
//!
//! Both backends are set up edge triggered, which is what a future-driven
//! reactor wants: a level triggered readiness would re-fire continuously for a
//! descriptor nobody is currently reading, and the loop would spin. Edge
//! triggering means a wakeup arrives when readiness *changes*, so the
//! obligation on the caller is to read or write until `EWOULDBLOCK` before
//! waiting again. Every I/O path in this crate does that.

// Calling the kernel is this module's entire purpose, so the crate-wide `deny`
// is lifted here and nowhere else. Every block below names the invariant it is
// relying on.
#![allow(unsafe_code)]

use alloc::vec::Vec;

use super::error::Result;

/// What a caller is waiting for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interest {
    /// Wake when the descriptor becomes readable.
    pub readable: bool,
    /// Wake when the descriptor becomes writable.
    pub writable: bool,
}

impl Interest {
    /// Readable only.
    pub const READABLE: Self = Self {
        readable: true,
        writable: false,
    };
    /// Writable only.
    pub const WRITABLE: Self = Self {
        readable: false,
        writable: true,
    };
    /// Both directions.
    pub const BOTH: Self = Self {
        readable: true,
        writable: true,
    };
}

/// A readiness notification for one descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// The token registered alongside the descriptor.
    pub token: u64,
    /// The descriptor may be readable. Treat as a hint: read until it blocks.
    pub readable: bool,
    /// The descriptor may be writable. Treat as a hint: write until it blocks.
    pub writable: bool,
    /// The peer hung up, or the descriptor failed.
    ///
    /// Reported beside `readable` rather than folded into it, because the two
    /// have different lifetimes. Readability is consumed by a read; a hang-up
    /// is permanent. Both pollers can deliver a hang-up on the *same* edge as
    /// the last of the data, so a reader that takes a short read and consumes
    /// the readable bit swallows the hang-up with it, then parks for an edge
    /// that has already happened: a task that never finishes and a connection
    /// that is never closed.
    pub hangup: bool,
}

/// A readiness poller over a set of file descriptors.
#[derive(Debug)]
pub struct Poller(sys::Poller);

impl Poller {
    /// Create a poller.
    pub fn new() -> Result<Self> {
        sys::Poller::new().map(Self)
    }

    /// Start watching `fd`, reporting `token` on every event for it.
    ///
    /// # Safety of the token
    ///
    /// The token is an opaque `u64` chosen by the caller, not a pointer. A
    /// stale event for a closed descriptor therefore resolves to a token that
    /// is simply absent from the caller's table, rather than to a dangling
    /// reference.
    pub fn add(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
        self.0.add(fd, token, interest)
    }

    /// Change what `fd` is being watched for.
    pub fn modify(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
        self.0.modify(fd, token, interest)
    }

    /// Stop watching `fd`.
    ///
    /// Closing a descriptor also removes it from the kernel's set, so this is
    /// only needed when the descriptor outlives its registration.
    pub fn remove(&self, fd: i32) -> Result<()> {
        self.0.remove(fd)
    }

    /// Wait for readiness, appending events to `out`.
    ///
    /// `timeout_ns` of `None` blocks indefinitely. A return of zero events is
    /// normal: it means the timeout expired, or the wait was interrupted by a
    /// signal, or the poller was woken by [`Self::wake`].
    pub fn wait(&self, out: &mut Vec<Event>, timeout_ns: Option<u64>) -> Result<()> {
        #[cfg(feature = "syscall-counters")]
        crate::reactor::counters::WAIT.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        self.0.wait(out, timeout_ns)
    }

    /// Wake a thread blocked in [`Self::wait`].
    ///
    /// This is what lets a newly registered timer or a freshly spawned
    /// connection interrupt a wait that was about to sleep for a long time.
    /// Safe to call from any thread.
    pub fn wake(&self) -> Result<()> {
        self.0.wake()
    }
}

// --- eko ------------------------------------------------------------------

#[cfg(target_os = "none")]
mod sys {
    use alloc::vec::Vec;

    use super::super::error::{Errno, Result};
    use super::{Event, Interest};
    use eko::epoll::{EPOLLERR, EPOLLHUP, EPOLLIN, EPOLLOUT, EPOLLRDHUP};
    use eko::reactive::{ReactiveRing, UkReactiveEvent};

    pub(super) struct Poller {
        ring: ReactiveRing,
    }

    impl core::fmt::Debug for Poller {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.debug_struct("Poller")
                .field("capacity", &self.ring.capacity())
                .finish()
        }
    }

    impl Poller {
        pub(super) fn new() -> Result<Self> {
            let ring = ReactiveRing::create(0).map_err(|e| Errno(e.0))?;
            Ok(Self { ring })
        }

        fn mask(interest: Interest) -> u32 {
            let mut mask = 0u32;
            if interest.readable {
                mask |= EPOLLIN | EPOLLRDHUP;
            }
            if interest.writable {
                mask |= EPOLLOUT;
            }
            mask
        }

        pub(super) fn add(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
            self.ring
                .add(fd, token, Self::mask(interest))
                .map_err(|e| Errno(e.0))
        }

        pub(super) fn modify(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
            self.ring
                .modify(fd, token, Self::mask(interest))
                .map_err(|e| Errno(e.0))
        }

        pub(super) fn remove(&self, fd: i32) -> Result<()> {
            self.ring.remove(fd).map_err(|e| Errno(e.0))
        }

        pub(super) fn wait(&self, out: &mut Vec<Event>, timeout_ns: Option<u64>) -> Result<()> {
            const CAPACITY: usize = 256;
            let mut events = [UkReactiveEvent::default(); CAPACITY];

            let (count, _user_wake, _overflow) = match self.ring.wait_timeout_ns(&mut events, timeout_ns) {
                Ok(res) => res,
                Err(error) => {
                    let errno = Errno(error.0);
                    if errno.interrupted() {
                        return Ok(());
                    }
                    return Err(errno);
                }
            };

            for event in &events[..count] {
                let flags = event.flags;
                let failed = (flags & (EPOLLHUP | EPOLLERR)) != 0;
                out.push(Event {
                    token: event.token,
                    readable: failed || (flags & EPOLLIN) != 0,
                    writable: failed || (flags & EPOLLOUT) != 0,
                    hangup: failed || (flags & EPOLLRDHUP) != 0,
                });
            }

            Ok(())
        }

        pub(super) fn wake(&self) -> Result<()> {
            self.ring.wake().map_err(|e| Errno(e.0))
        }
    }
}

#[cfg(not(target_os = "none"))]
mod sys {
    use alloc::vec::Vec;

    use super::super::error::{Errno, Result};
    use super::{Event, Interest};
    use eko::epoll::{
        EPOLLERR, EPOLLET, EPOLLHUP, EPOLLIN, EPOLLOUT, EPOLLRDHUP, Epoll, EpollEvent, WakerFd,
    };

    const WAKE_TOKEN: u64 = u64::MAX;

    #[derive(Debug)]
    pub(super) struct Poller {
        epoll: Epoll,
        wake: WakerFd,
    }

    impl Poller {
        pub(super) fn new() -> Result<Self> {
            let epoll = Epoll::new().map_err(|e| Errno(e.0))?;
            let wake = WakerFd::new().map_err(|e| Errno(e.0))?;
            epoll
                .add(wake.as_raw_fd(), WAKE_TOKEN, EPOLLIN)
                .map_err(|e| Errno(e.0))?;
            Ok(Self { epoll, wake })
        }

        fn mask(interest: Interest) -> u32 {
            let mut mask = EPOLLET;
            if interest.readable {
                mask |= EPOLLIN | EPOLLRDHUP;
            }
            if interest.writable {
                mask |= EPOLLOUT;
            }
            mask
        }

        pub(super) fn add(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
            self.epoll
                .add(fd, token, Self::mask(interest))
                .map_err(|e| Errno(e.0))
        }

        pub(super) fn modify(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
            self.epoll
                .modify(fd, token, Self::mask(interest))
                .map_err(|e| Errno(e.0))
        }

        pub(super) fn remove(&self, fd: i32) -> Result<()> {
            self.epoll.delete(fd).map_err(|e| Errno(e.0))
        }

        pub(super) fn wait(&self, out: &mut Vec<Event>, timeout_ns: Option<u64>) -> Result<()> {
            const CAPACITY: usize = 256;
            let mut events = [EpollEvent::default(); CAPACITY];

            let timeout_ms = match timeout_ns {
                None => -1,
                Some(0) => 0,
                Some(ns) => {
                    let ms = ns.div_ceil(1_000_000);
                    ms.min(i32::MAX as u64) as i32
                }
            };

            let count = match self.epoll.wait(&mut events, timeout_ms) {
                Ok(n) => n,
                Err(error) => {
                    let errno = Errno(error.0);
                    if errno.interrupted() {
                        return Ok(());
                    }
                    return Err(errno);
                }
            };

            for event in &events[..count] {
                if event.token == WAKE_TOKEN {
                    let _ = self.wake.drain();
                    continue;
                }
                let flags = event.events;
                let failed = (flags & (EPOLLHUP | EPOLLERR)) != 0;
                out.push(Event {
                    token: event.token,
                    readable: failed || (flags & EPOLLIN) != 0,
                    writable: failed || (flags & EPOLLOUT) != 0,
                    hangup: failed || (flags & EPOLLRDHUP) != 0,
                });
            }
            Ok(())
        }

        pub(super) fn wake(&self) -> Result<()> {
            self.wake.wake().map_err(|e| Errno(e.0))
        }
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::reactor::testing::{socket_pair, write_byte};
    use std::os::fd::AsRawFd;
    use std::time::{Duration, Instant};

    #[test]
    fn reports_readability_only_after_data_arrives() {
        let poller = Poller::new().expect("poller");
        let (a, b) = socket_pair();
        poller
            .add(a.as_raw_fd(), 42, Interest::READABLE)
            .expect("add");

        // Nothing written yet: the wait must time out rather than report.
        let mut events = Vec::new();
        poller.wait(&mut events, Some(50_000_000)).expect("wait");
        assert!(events.is_empty(), "readable with no data pending");

        write_byte(&b);
        poller.wait(&mut events, Some(500_000_000)).expect("wait");
        assert_eq!(events.len(), 1, "no event after write");
        assert_eq!(events[0].token, 42);
        assert!(events[0].readable);
    }

    #[test]
    fn wake_interrupts_a_blocking_wait() {
        use std::sync::Arc;

        let poller = Arc::new(Poller::new().expect("poller"));
        let waker = Arc::clone(&poller);

        // Wake from another thread while this one is blocked indefinitely. If
        // `wake` did not work this test would hang rather than fail, so the
        // elapsed assertion below bounds it.
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            waker.wake().expect("wake");
        });

        let start = Instant::now();
        let mut events = Vec::new();
        poller.wait(&mut events, None).expect("wait");
        let elapsed = start.elapsed();

        assert!(events.is_empty(), "wake produced a spurious event");
        assert!(
            elapsed < Duration::from_secs(5),
            "wait was not interrupted: {elapsed:?}"
        );
    }

    #[test]
    fn a_wake_before_the_wait_is_not_lost() {
        // The race that matters: something registers work and wakes the reactor
        // in the window before it blocks. That wake must still be delivered, or
        // the reactor sleeps on a queue that already has work in it.
        let poller = Poller::new().expect("poller");
        poller.wake().expect("wake");

        let start = Instant::now();
        let mut events = Vec::new();
        poller.wait(&mut events, None).expect("wait");

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "an earlier wake was swallowed"
        );
    }

    #[test]
    fn modify_can_narrow_an_interest() {
        let poller = Poller::new().expect("poller");
        let (a, b) = socket_pair();

        // A fresh socket is writable, so registering both directions reports it.
        poller.add(a.as_raw_fd(), 7, Interest::BOTH).expect("add");
        let mut events = Vec::new();
        poller.wait(&mut events, Some(200_000_000)).expect("wait");
        assert!(
            events.iter().any(|event| event.writable),
            "expected writability on a fresh socket"
        );

        // Narrowing to readable must drop the write filter, not leave it live.
        poller
            .modify(a.as_raw_fd(), 7, Interest::READABLE)
            .expect("modify");
        events.clear();
        poller.wait(&mut events, Some(50_000_000)).expect("wait");
        assert!(
            !events.iter().any(|event| event.writable),
            "write interest survived being narrowed"
        );

        // Reads still arrive.
        write_byte(&b);
        events.clear();
        poller.wait(&mut events, Some(500_000_000)).expect("wait");
        assert!(
            events.iter().any(|event| event.readable),
            "lost readability"
        );
    }

    #[test]
    fn remove_stops_delivery() {
        let poller = Poller::new().expect("poller");
        let (a, b) = socket_pair();
        poller
            .add(a.as_raw_fd(), 1, Interest::READABLE)
            .expect("add");
        poller.remove(a.as_raw_fd()).expect("remove");

        write_byte(&b);
        let mut events = Vec::new();
        poller.wait(&mut events, Some(100_000_000)).expect("wait");
        assert!(events.is_empty(), "event delivered after remove");
    }

    #[test]
    fn a_timeout_of_zero_returns_immediately() {
        let poller = Poller::new().expect("poller");
        let start = Instant::now();
        let mut events = Vec::new();
        poller.wait(&mut events, Some(0)).expect("wait");
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "zero timeout blocked"
        );
    }
}
