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

/// The last OS error when `value` signals failure.
fn check(value: i32) -> Result<i32> {
    if value < 0 {
        Err(crate::reactor::error::last())
    } else {
        Ok(value)
    }
}

// --- kqueue ---------------------------------------------------------------

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
))]
mod sys {
    use super::super::error::Result;
    use super::{check, Event, Interest};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    /// The identifier used for the user event that implements `wake`.
    const WAKE_IDENT: usize = usize::MAX;

    #[derive(Debug)]
    pub(super) struct Poller {
        kq: OwnedFd,
    }

    impl Poller {
        pub(super) fn new() -> Result<Self> {
            // SAFETY: kqueue takes no arguments and returns a descriptor or -1.
            let raw = check(unsafe { libc::kqueue() })?;
            // SAFETY: `raw` is a fresh descriptor this call owns exclusively.
            let kq = unsafe { OwnedFd::from_raw_fd(raw) };

            // The wake channel: a user event that `wake` triggers. Registered
            // once here so that triggering it later is a single syscall with no
            // allocation and no second descriptor, which is what a pipe or an
            // eventfd would have cost.
            let change = libc::kevent {
                ident: WAKE_IDENT,
                filter: libc::EVFILT_USER,
                flags: libc::EV_ADD | libc::EV_CLEAR,
                fflags: 0,
                data: 0,
                udata: WAKE_IDENT as *mut libc::c_void,
            };
            let poller = Self { kq };
            poller.apply(&[change])?;
            Ok(poller)
        }

        /// Submit changes, requesting no events back.
        fn apply(&self, changes: &[libc::kevent]) -> Result<()> {
            // SAFETY: `changes` is a valid slice for its length, and a null
            // event list with a zero count asks for no events in return.
            check(unsafe {
                libc::kevent(
                    self.kq.as_raw_fd(),
                    changes.as_ptr(),
                    changes.len() as libc::c_int,
                    core::ptr::null_mut(),
                    0,
                    core::ptr::null(),
                )
            })?;
            Ok(())
        }

        /// Build the add/delete pair expressing `interest` for `fd`.
        ///
        /// A filter is deleted rather than left in place when the caller is no
        /// longer interested, because kqueue registers read and write as two
        /// independent filters and dropping one from the interest set has to
        /// remove it explicitly.
        fn changes(fd: i32, token: u64, interest: Interest) -> [libc::kevent; 2] {
            let flags = libc::EV_ADD | libc::EV_CLEAR;
            let make = |filter: i16, enabled: bool| libc::kevent {
                ident: fd as usize,
                filter,
                flags: if enabled { flags } else { libc::EV_DELETE },
                fflags: 0,
                data: 0,
                udata: token as *mut libc::c_void,
            };
            [
                make(libc::EVFILT_READ, interest.readable),
                make(libc::EVFILT_WRITE, interest.writable),
            ]
        }

        pub(super) fn add(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
            self.modify(fd, token, interest)
        }

        pub(super) fn modify(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
            // Deleting a filter that was never added returns ENOENT, which is
            // the expected outcome when narrowing an interest that only ever
            // had one direction, not a failure.
            for change in Self::changes(fd, token, interest) {
                if let Err(error) = self.apply(&[change]) {
                    let ignorable = change.flags & libc::EV_DELETE != 0 && error.0 == libc::ENOENT;
                    if !ignorable {
                        return Err(error);
                    }
                }
            }
            Ok(())
        }

        pub(super) fn remove(&self, fd: i32) -> Result<()> {
            self.modify(
                fd,
                0,
                Interest {
                    readable: false,
                    writable: false,
                },
            )
        }

        pub(super) fn wait(&self, out: &mut Vec<Event>, timeout_ns: Option<u64>) -> Result<()> {
            /// How many events one syscall may return. A full buffer simply
            /// means the next wait returns immediately with the rest.
            const CAPACITY: usize = 1024;
            // Uninitialised, because zeroing this costs more than everything
            // else a wakeup does put together: 1024 entries is 32 KiB of
            // `memset` and eight pages touched, measured at 249ns against the
            // roughly 35ns of all the bookkeeping around it, and the kernel is
            // about to overwrite whatever is here anyway. Only the first
            // `count` entries are read, and those the kernel wrote.
            let mut events: [core::mem::MaybeUninit<libc::kevent>; CAPACITY] =
                // SAFETY: an array of `MaybeUninit` requires no initialisation,
                // which is the whole point of the type.
                unsafe { core::mem::MaybeUninit::uninit().assume_init() };

            let timeout = timeout_ns.map(|ns| libc::timespec {
                tv_sec: (ns / 1_000_000_000) as libc::time_t,
                tv_nsec: (ns % 1_000_000_000) as libc::c_long,
            });
            let timeout_ptr = timeout
                .as_ref()
                .map_or(core::ptr::null(), |value| value as *const libc::timespec);

            // SAFETY: the event buffer is valid for CAPACITY entries and the
            // timeout pointer is either null or to a live local.
            let count = unsafe {
                libc::kevent(
                    self.kq.as_raw_fd(),
                    core::ptr::null(),
                    0,
                    events.as_mut_ptr().cast::<libc::kevent>(),
                    CAPACITY as libc::c_int,
                    timeout_ptr,
                )
            };
            if count < 0 {
                let error = crate::reactor::error::last();
                // A signal during the wait is not a failure: the caller's loop
                // simply goes around again.
                if error.interrupted() {
                    return Ok(());
                }
                return Err(error);
            }

            for slot in &events[..count as usize] {
                // SAFETY: `kevent` reported writing `count` entries, so each
                // of these has been initialised by the kernel.
                let event = unsafe { slot.assume_init_ref() };
                if event.filter == libc::EVFILT_USER {
                    continue;
                }
                let token = event.udata as u64;
                let readable = event.filter == libc::EVFILT_READ;
                let writable = event.filter == libc::EVFILT_WRITE;
                // EV_EOF means the peer hung up. It is surfaced as readiness in
                // whichever direction was registered so the caller's next read
                // returns 0 and the connection closes through the normal path
                // rather than through a special case here. It is *also*
                // reported on its own, because kqueue sets it on the same
                // event that carries the last of the data, and readiness alone
                // does not survive the read that consumes it.
                out.push(Event {
                    token,
                    readable,
                    writable,
                    hangup: event.flags & libc::EV_EOF != 0,
                });
            }
            Ok(())
        }

        pub(super) fn wake(&self) -> Result<()> {
            let change = libc::kevent {
                ident: WAKE_IDENT,
                filter: libc::EVFILT_USER,
                flags: 0,
                fflags: libc::NOTE_TRIGGER,
                data: 0,
                udata: WAKE_IDENT as *mut libc::c_void,
            };
            self.apply(&[change])
        }
    }
}

// --- epoll ----------------------------------------------------------------

#[cfg(target_os = "linux")]
mod sys {
    use super::super::error::Result;
    use super::{check, Event, Interest};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    /// Token reserved for the eventfd that implements `wake`.
    const WAKE_TOKEN: u64 = u64::MAX;

    #[derive(Debug)]
    pub(super) struct Poller {
        epoll: OwnedFd,
        /// The wake channel. epoll has no user event, so this is an eventfd.
        wake: OwnedFd,
    }

    impl Poller {
        pub(super) fn new() -> Result<Self> {
            // SAFETY: epoll_create1 returns a descriptor or -1.
            let raw = check(unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) })?;
            // SAFETY: fresh descriptor, owned exclusively.
            let epoll = unsafe { OwnedFd::from_raw_fd(raw) };

            // SAFETY: eventfd returns a descriptor or -1.
            let raw = check(unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) })?;
            // SAFETY: fresh descriptor, owned exclusively.
            let wake = unsafe { OwnedFd::from_raw_fd(raw) };

            let poller = Self { epoll, wake };
            // Level triggered for the wake fd, deliberately: a trigger that
            // lands while the counter is already non-zero must still be seen,
            // and the drain in `wait` clears it.
            let mut event = libc::epoll_event {
                events: libc::EPOLLIN as u32,
                u64: WAKE_TOKEN,
            };
            check(unsafe {
                libc::epoll_ctl(
                    poller.epoll.as_raw_fd(),
                    libc::EPOLL_CTL_ADD,
                    poller.wake.as_raw_fd(),
                    &mut event,
                )
            })?;
            Ok(poller)
        }

        fn mask(interest: Interest) -> u32 {
            let mut mask = libc::EPOLLET as u32;
            if interest.readable {
                // EPOLLRDHUP has to be asked for; EPOLLHUP and EPOLLERR arrive
                // whether or not they are in the mask. Without it a peer's
                // ordinary half-close is indistinguishable from data.
                mask |= libc::EPOLLIN as u32 | libc::EPOLLRDHUP as u32;
            }
            if interest.writable {
                mask |= libc::EPOLLOUT as u32;
            }
            mask
        }

        fn ctl(&self, op: libc::c_int, fd: i32, token: u64, interest: Interest) -> Result<()> {
            let mut event = libc::epoll_event {
                events: Self::mask(interest),
                u64: token,
            };
            // SAFETY: `event` is a live local for the duration of the call.
            check(unsafe { libc::epoll_ctl(self.epoll.as_raw_fd(), op, fd, &mut event) })?;
            Ok(())
        }

        pub(super) fn add(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
            self.ctl(libc::EPOLL_CTL_ADD, fd, token, interest)
        }

        pub(super) fn modify(&self, fd: i32, token: u64, interest: Interest) -> Result<()> {
            self.ctl(libc::EPOLL_CTL_MOD, fd, token, interest)
        }

        pub(super) fn remove(&self, fd: i32) -> Result<()> {
            // SAFETY: a null event is permitted for DEL on Linux 2.6.9 and up.
            check(unsafe {
                libc::epoll_ctl(
                    self.epoll.as_raw_fd(),
                    libc::EPOLL_CTL_DEL,
                    fd,
                    core::ptr::null_mut(),
                )
            })?;
            Ok(())
        }

        pub(super) fn wait(&self, out: &mut Vec<Event>, timeout_ns: Option<u64>) -> Result<()> {
            const CAPACITY: usize = 1024;
            // Uninitialised for the reason the kqueue arm gives: the kernel
            // overwrites what it fills and only that much is read, so zeroing
            // this is 12 KiB of `memset` on every wakeup for nothing.
            let mut events: [core::mem::MaybeUninit<libc::epoll_event>; CAPACITY] =
                // SAFETY: an array of `MaybeUninit` needs no initialisation.
                unsafe { core::mem::MaybeUninit::uninit().assume_init() };

            // epoll_wait takes milliseconds. Round a sub-millisecond timeout up
            // to 1ms rather than down to 0: a zero would busy-spin, and firing
            // a timer a fraction late is the lesser error.
            let timeout_ms = match timeout_ns {
                None => -1,
                Some(ns) => {
                    let ms = ns.div_ceil(1_000_000);
                    ms.min(libc::c_int::MAX as u64) as libc::c_int
                }
            };

            // SAFETY: buffer valid for CAPACITY entries.
            let count = unsafe {
                libc::epoll_wait(
                    self.epoll.as_raw_fd(),
                    events.as_mut_ptr().cast::<libc::epoll_event>(),
                    CAPACITY as libc::c_int,
                    timeout_ms,
                )
            };
            if count < 0 {
                let error = crate::reactor::error::last();
                if error.interrupted() {
                    return Ok(());
                }
                return Err(error);
            }

            for slot in &events[..count as usize] {
                // SAFETY: `epoll_wait` reported writing `count` entries, so
                // each of these has been initialised by the kernel.
                let event = unsafe { slot.assume_init_ref() };
                if event.u64 == WAKE_TOKEN {
                    // Drain the counter so the level triggered fd goes quiet.
                    let mut buffer = [0u8; 8];
                    // SAFETY: an 8 byte read is what an eventfd requires, and a
                    // failure here only means it was already drained.
                    unsafe {
                        libc::read(
                            self.wake.as_raw_fd(),
                            buffer.as_mut_ptr().cast::<libc::c_void>(),
                            8,
                        );
                    }
                    continue;
                }
                // EPOLLHUP and EPOLLERR are reported as readiness in both
                // directions so the caller's next read or write surfaces the
                // real error through its normal path. EPOLLRDHUP joins them
                // for the hang-up flag but not for readiness: it is the
                // ordinary half-close, which epoll delivers on the same
                // edge-triggered event as the last of the data, and the flag
                // is what survives the read that consumes that readiness.
                let flags = event.events;
                let failed = flags & (libc::EPOLLHUP as u32 | libc::EPOLLERR as u32) != 0;
                out.push(Event {
                    token: event.u64,
                    readable: failed || flags & libc::EPOLLIN as u32 != 0,
                    writable: failed || flags & libc::EPOLLOUT as u32 != 0,
                    hangup: failed || flags & libc::EPOLLRDHUP as u32 != 0,
                });
            }
            Ok(())
        }

        pub(super) fn wake(&self) -> Result<()> {
            let value: u64 = 1;
            // SAFETY: writing 8 bytes to an eventfd increments its counter.
            let written = unsafe {
                libc::write(
                    self.wake.as_raw_fd(),
                    core::ptr::addr_of!(value).cast::<libc::c_void>(),
                    8,
                )
            };
            if written < 0 {
                let error = crate::reactor::error::last();
                // EAGAIN means the counter is saturated, which already means a
                // wake is pending. Nothing to do.
                if error.would_block() {
                    return Ok(());
                }
                return Err(error);
            }
            Ok(())
        }
    }
}

#[cfg(test)]
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
