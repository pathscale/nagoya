//! Unix signals as a readable descriptor.
//!
//! [`Registration`](super::Registration) watches a file descriptor. On the
//! BSDs `EVFILT_SIGNAL` does not name one: its identifier is the signal
//! number, and the poller would report the event with neither readable nor
//! writable set. `signalfd` is a descriptor, so it registers the same way a
//! socket does. The BSDs have no `signalfd`. A pipe written from a handler is
//! a descriptor too, and that is the whole of the other path.
//!
//! # What outlives the waiter
//!
//! Only `sigaction` stops the default action, and it stops it for every
//! thread at once. A signal mask does not: `pthread_sigmask` changes the
//! calling thread and nothing else, so while any other thread is running with
//! the disposition still `SIG_DFL`, a process-directed `kill` is delivered
//! there and terminates the process no matter what this thread has masked.
//! The BSD path is ordered around that. The pipe is created and its write end
//! published first, then `sigaction` installs the handler; from that
//! instruction on no thread can take the default action, and the handler
//! never runs before it has somewhere to write. Nothing is blocked to get
//! there, because blocking would not have helped and would only have widened
//! the window it appeared to close. A signal arriving between the handler and
//! the poller registration is a byte already in the pipe, and the
//! registration starts readable.
//!
//! The pipe is not closed when the waiter is dropped. A handler that has
//! already loaded the write end's number can still call `write`, and closing
//! that descriptor would let the kernel hand the number out again underneath
//! it. One pipe per signal number stays open for the process, and the next
//! waiter reuses it. Drop puts the previous disposition back. There the block
//! is real work rather than theatre: the swap goes through `SIG_IGN` with the
//! signal blocked on this thread, because restoring the default first and
//! then letting a still-pending `SIGINT` through would terminate the process.
//!
//! On Linux there is no handler. `signalfd` delivers a signal only while that
//! signal stays blocked, so every waiter blocks it on the calling thread,
//! including one that finds the descriptor already made, and the block is not
//! lifted on drop. The per-thread scope bites here and cannot be engineered
//! away: this thread and any thread it creates afterwards inherit the block,
//! a thread that already existed does not, and a signal delivered to one of
//! those takes the default action instead of the descriptor.
//!
//! A signal the program had already blocked on the calling thread is left
//! blocked. That state was deliberate and is not this module's to hand back,
//! so the mask is restored only where this module was the one that took it.
//!
//! One waiter per signal number. A second [`Signal`] for the same number
//! gets `EBUSY`.

#![allow(unsafe_code)]

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use crate::reactor::driver::{Handle, Registration};
use crate::reactor::error::{Errno, Result};
use crate::reactor::poller::Interest;

/// Which signal a [`Signal`] waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalKind(i32);

impl SignalKind {
    /// `SIGHUP`. The default action is to terminate the process.
    #[must_use]
    pub const fn hangup() -> Self {
        Self(libc::SIGHUP)
    }

    /// `SIGINT`. The default action is to terminate the process.
    #[must_use]
    pub const fn interrupt() -> Self {
        Self(libc::SIGINT)
    }

    /// `SIGTERM`. The default action is to terminate the process.
    #[must_use]
    pub const fn terminate() -> Self {
        Self(libc::SIGTERM)
    }

    /// An arbitrary signal number.
    ///
    /// [`Signal::new`] accepts `1..32`. `SIGKILL` and `SIGSTOP` are in that
    /// range and are rejected by the kernel, because a process is not allowed
    /// to catch or block them.
    #[must_use]
    pub const fn from_raw(signo: i32) -> Self {
        Self(signo)
    }

    /// The signal number passed to the kernel.
    #[must_use]
    pub const fn as_raw(self) -> i32 {
        self.0
    }
}

/// A waiter for one unix signal.
///
/// The future from [`recv`](Self::recv) completes once each time the signal
/// is delivered. While a waiter exists, `SIGINT` does not terminate the
/// process: on the BSDs for any thread, because a handler is installed, and
/// on Linux for this thread and the ones it goes on to create, because those
/// are the threads that carry the block `signalfd` needs.
///
/// On Linux the signal stays blocked after drop. `signalfd` only delivers a
/// signal that is blocked, and lifting the block would hand a pending
/// `SIGINT` to the default action. On the BSDs drop restores the disposition
/// [`new`](Self::new) replaced.
#[derive(Debug)]
pub struct Signal {
    kind: SignalKind,
    /// Before `claim`, because fields drop in declaration order and the
    /// descriptor has to leave the poller before anyone else can be handed
    /// it. The descriptor is process-lifetime and shared, so the next waiter
    /// registers the same number; a claim released first would let that
    /// registration happen and then be torn out by this one's removal, which
    /// on kqueue is a silent delete and a wait that never completes.
    registration: Registration,
    /// Held so a second waiter for this number fails. Released last of all:
    /// after the disposition has been put back and after the descriptor is
    /// out of the poller. The descriptor itself is not owned here.
    claim: Claim,
    fd: i32,
}

impl Signal {
    /// Wait for `kind` on `handle`'s reactor.
    ///
    /// Fails with `EBUSY` when a waiter for this signal number already exists,
    /// and with `EINVAL` when the number is outside `1..32` or the kernel
    /// refuses to catch it.
    pub fn new(kind: SignalKind, handle: &Handle) -> Result<Self> {
        let claim = Claim::acquire(kind.as_raw())?;
        let fd = match platform::arm(kind.as_raw()) {
            Ok(fd) => fd,
            Err(error) => {
                drop(claim);
                return Err(error);
            }
        };
        let registration = match handle.register(fd, Interest::READABLE) {
            Ok(registration) => registration,
            Err(error) => {
                platform::disarm(kind.as_raw());
                drop(claim);
                return Err(error);
            }
        };
        if let Err(error) = platform::committed(kind.as_raw()) {
            platform::disarm(kind.as_raw());
            // The same order the fields drop in, and for the same reason: the
            // claim is what keeps the next waiter out, so it cannot go while
            // the descriptor is still in the poller waiting to be removed.
            drop(registration);
            drop(claim);
            return Err(error);
        }
        Ok(Self {
            kind,
            registration,
            claim,
            fd,
        })
    }

    /// The signal this waiter was created for.
    #[must_use]
    pub fn kind(&self) -> SignalKind {
        self.kind
    }

    /// Complete the next time this signal is delivered.
    ///
    /// A delivery that happened after [`new`](Self::new) and before this is
    /// polled is not lost: it is already a byte in the descriptor, and the
    /// registration starts readable because an edge that fired before `add`
    /// will not fire again.
    pub fn recv(&mut self) -> Recv<'_> {
        Recv { signal: self }
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<SignalKind>> {
        // The same contract as a socket read. The flag starts set, so a
        // signal that arrived before anyone polled is still observed, and a
        // later one parks the waker under the same lock that observes the flag.
        if !self.registration.take_readable_or_park(cx.waker()) {
            return Poll::Pending;
        }
        loop {
            match platform::read_one(self.fd) {
                Ok(kind) => {
                    // One event does not prove the descriptor empty. Two
                    // deliveries queued before this read share one edge, and
                    // consuming it here would leave the second with nothing
                    // to wake on.
                    self.registration.mark_readable();
                    return Poll::Ready(Ok(kind));
                }
                Err(error) if error.would_block() => {
                    self.registration.poll_readable(cx.waker());
                    return Poll::Pending;
                }
                Err(error) if error.interrupted() => continue,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
    }
}

impl Drop for Signal {
    fn drop(&mut self) {
        // While the claim is still held, so a new waiter cannot install over
        // the restore. The fields drop after this and in their declared
        // order: the registration takes the descriptor back out of the
        // poller, and only then is the claim released. The descriptor stays
        // open.
        platform::disarm(self.claim.0);
    }
}

/// A poll of [`Signal::recv`].
#[must_use = "a signal is not waited for until the future is polled"]
pub struct Recv<'a> {
    signal: &'a mut Signal,
}

impl Future for Recv<'_> {
    type Output = Result<SignalKind>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().signal.poll_recv(cx)
    }
}

/// The set of signal numbers that currently have a waiter.
static CLAIMED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct Claim(i32);

impl Claim {
    fn acquire(sig: i32) -> Result<Self> {
        // SIGKILL and SIGSTOP cannot be caught, and `sigaction` says so on
        // the BSDs. On Linux `signalfd` silently drops them from its mask and
        // succeeds, which would hand back a waiter that can never fire. Refused
        // here, before either platform is asked, so both answer the same.
        if !(1..32).contains(&sig) || sig == libc::SIGKILL || sig == libc::SIGSTOP {
            return Err(Errno(libc::EINVAL));
        }
        let bit = 1u64 << sig;
        let previous = CLAIMED.fetch_or(bit, Ordering::AcqRel);
        if previous & bit != 0 {
            return Err(Errno(libc::EBUSY));
        }
        Ok(Self(sig))
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        let bit = !(1u64 << self.0);
        CLAIMED.fetch_and(bit, Ordering::Release);
    }
}

fn check(value: i32) -> Result<i32> {
    if value < 0 {
        Err(crate::reactor::error::last())
    } else {
        Ok(value)
    }
}

/// Block `sig` on this thread, reporting whether it was already blocked.
///
/// The old mask is asked for rather than discarded, because a program is
/// entitled to have blocked `SIGHUP` on this thread on purpose and would not
/// expect creating a waiter to hand it back. The answer is the one bit that
/// matters, so it is reduced to a `bool` here and the set is not kept.
fn block_one(sig: i32) -> Result<bool> {
    // SAFETY: `set` and `previous` are live locals. `sigemptyset` and
    // `sigaddset` write theirs and do not retain it, `pthread_sigmask` copies
    // the set before returning and fills `previous` in place.
    unsafe {
        let mut set = core::mem::zeroed::<libc::sigset_t>();
        check(libc::sigemptyset(&mut set))?;
        check(libc::sigaddset(&mut set, sig))?;
        let mut previous = core::mem::zeroed::<libc::sigset_t>();
        let status = libc::pthread_sigmask(libc::SIG_BLOCK, &set, &mut previous);
        if status != 0 {
            return Err(Errno(status));
        }
        Ok(check(libc::sigismember(&previous, sig))? != 0)
    }
}

fn unblock_one(sig: i32) -> Result<()> {
    // SAFETY: as `block_one`, and the old mask is genuinely not wanted here.
    unsafe {
        let mut set = core::mem::zeroed::<libc::sigset_t>();
        check(libc::sigemptyset(&mut set))?;
        check(libc::sigaddset(&mut set, sig))?;
        let status = libc::pthread_sigmask(libc::SIG_UNBLOCK, &set, core::ptr::null_mut());
        if status != 0 {
            return Err(Errno(status));
        }
    }
    Ok(())
}

/// Undo a [`block_one`], given what it reported.
///
/// A caller that found the signal already blocked leaves it blocked. Only the
/// block this module took is the block this module lifts.
fn restore_mask(sig: i32, was_blocked: bool) -> Result<()> {
    if was_blocked {
        return Ok(());
    }
    unblock_one(sig)
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{block_one, check, restore_mask, Errno, Result, SignalKind};
    use std::sync::atomic::{AtomicI32, Ordering};

    /// One `signalfd` per signal number, open for the process.
    ///
    /// Reused by the next waiter. Closing it is unnecessary, and a signal
    /// that arrives while nobody is waiting stays blocked and readable on
    /// this descriptor rather than taking the default action.
    static FDS: [AtomicI32; 32] = [const { AtomicI32::new(-1) }; 32];

    pub(super) fn arm(sig: i32) -> Result<i32> {
        // Before the cache is consulted, not after. The descriptor is shared
        // between waiters but the mask is not: it belongs to whichever thread
        // called, and `signalfd` reports nothing that thread is still allowed
        // to receive. A second waiter created on another thread would find
        // the descriptor already made, return here with its own mask
        // untouched and its disposition still `SIG_DFL`, and be killed by the
        // very signal a live waiter was sitting on the descriptor for.
        let was_blocked = block_one(sig)?;
        let existing = FDS[sig as usize].load(Ordering::Acquire);
        if existing >= 0 {
            return Ok(existing);
        }
        match create(sig) {
            Ok(fd) => {
                FDS[sig as usize].store(fd, Ordering::Release);
                Ok(fd)
            }
            Err(error) => {
                // Nothing is waiting, so the block would only swallow the
                // signal. Put the mask back.
                let _ = restore_mask(sig, was_blocked);
                Err(error)
            }
        }
    }

    fn create(sig: i32) -> Result<i32> {
        // SAFETY: `set` is a live local. `signalfd` copies it.
        unsafe {
            let mut set = core::mem::zeroed::<libc::sigset_t>();
            check(libc::sigemptyset(&mut set))?;
            check(libc::sigaddset(&mut set, sig))?;
            check(libc::signalfd(
                -1,
                &set,
                libc::SFD_NONBLOCK | libc::SFD_CLOEXEC,
            ))
        }
    }

    pub(super) fn committed(_sig: i32) -> Result<()> {
        // Stays blocked. `signalfd` does not deliver a signal that a thread
        // is allowed to receive by the default action.
        Ok(())
    }

    pub(super) fn disarm(_sig: i32) {
        // The block outlives the waiter. Lifting it would deliver a pending
        // `SIGINT` to the default action, which terminates the process.
    }

    pub(super) fn read_one(fd: i32) -> Result<SignalKind> {
        // SAFETY: `info` is a live local of the size `signalfd` writes.
        let mut info: libc::signalfd_siginfo = unsafe { core::mem::zeroed() };
        let read = unsafe {
            libc::read(
                fd,
                core::ptr::addr_of_mut!(info).cast::<libc::c_void>(),
                core::mem::size_of::<libc::signalfd_siginfo>(),
            )
        };
        if read < 0 {
            return Err(crate::reactor::error::last());
        }
        if read == 0 {
            return Err(Errno(libc::EPIPE));
        }
        if read as usize != core::mem::size_of::<libc::signalfd_siginfo>() {
            return Err(Errno(libc::EIO));
        }
        Ok(SignalKind::from_raw(info.ssi_signo as i32))
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{block_one, check, restore_mask, unblock_one, Errno, Result, SignalKind};
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Mutex;

    struct Ends {
        read: AtomicI32,
        write: AtomicI32,
    }

    /// The pipe for each signal number. Both ends stay open for the process:
    /// the handler publishes the write end through an atomic and may be
    /// inside `write` after a waiter has decided to go away.
    static PIPES: [Ends; 32] = [const {
        Ends {
            read: AtomicI32::new(-1),
            write: AtomicI32::new(-1),
        }
    }; 32];

    struct Saved {
        action: libc::sigaction,
    }

    static SAVED: Mutex<[Option<Saved>; 32]> = Mutex::new([const { None }; 32]);

    /// This thread's `errno` cell.
    ///
    /// Three spellings across the family this branch compiles for, and the
    /// choice is per target rather than per BSD: apple and FreeBSD say
    /// `__error`, DragonFly says `__errno_location` like Linux, and the
    /// NetBSD-likes say `__errno`.
    fn errno_slot() -> *mut libc::c_int {
        #[cfg(any(target_os = "netbsd", target_os = "openbsd"))]
        use libc::__errno as errno_location;
        #[cfg(target_os = "dragonfly")]
        use libc::__errno_location as errno_location;
        #[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
        use libc::__error as errno_location;

        // SAFETY: returns a pointer to the calling thread's own errno, valid
        // for the life of that thread. Getting the pointer is safe; what is
        // done through it is the caller's problem.
        unsafe { errno_location() }
    }

    /// Write the signal number into that signal's pipe.
    ///
    /// `write` of one byte to a non-blocking pipe is async-signal-safe. The
    /// atomic load is one instruction. Nothing else is allowed here: the
    /// handler runs on whichever stack was interrupted.
    ///
    /// `errno` is saved and put back around it because that write does fail:
    /// a pipe nobody is draining fills, and the failure is `EAGAIN`. The
    /// handler interrupts arbitrary code between any two instructions, and
    /// code that has just made a syscall and not yet read `errno` would see
    /// this one instead of its own.
    extern "C" fn write_signal(sig: libc::c_int) {
        let slot = errno_slot();
        // SAFETY: this thread's own errno cell, live for the life of the
        // thread. Reading it and putting the value back is exactly what the
        // interrupted code expects to still be able to do afterwards.
        let saved = unsafe { *slot };
        if (1..32).contains(&sig) {
            let fd = PIPES[sig as usize].write.load(Ordering::Acquire);
            if fd >= 0 {
                let byte = sig as u8;
                unsafe {
                    libc::write(fd, core::ptr::addr_of!(byte).cast::<libc::c_void>(), 1);
                }
            }
        }
        unsafe { *slot = saved };
    }

    pub(super) fn arm(sig: i32) -> Result<i32> {
        // Nothing is blocked here, and deliberately so. The default action is
        // what has to be displaced, only `sigaction` displaces it, and it
        // displaces it for every thread at once; a `pthread_sigmask` would
        // cover this thread alone and leave a process-directed `kill` free to
        // be delivered to any other running thread, where the disposition is
        // still `SIG_DFL`. Blocking first would therefore not close the
        // window, it would open a wider one: a pipe and four `fcntl`s worth
        // of time in which the code looks safe and is not. So the pipe is
        // built and its write end published first, and then one `sigaction`
        // ends the exposure outright.
        install(sig)
    }

    fn install(sig: i32) -> Result<i32> {
        // Published before the `sigaction` below and never after it: the
        // handler loads this slot and gives up when it is negative, so a
        // handler installed first would drop the one delivery it exists to
        // catch. Nothing reads it until then, so a failure past this point
        // leaves a write end with no handler behind it, which is inert.
        let write = ensure_pipe(sig)?;
        PIPES[sig as usize].write.store(write, Ordering::Release);
        let read = PIPES[sig as usize].read.load(Ordering::Acquire);
        if read < 0 {
            return Err(Errno(libc::EBADF));
        }

        // SAFETY: `next` and `previous` are live locals. `sigaction` copies
        // them before returning. `SA_SIGINFO` is off, so the kernel calls
        // `write_signal` as `fn(c_int)` and the signal number is the argument.
        let mut next: libc::sigaction = unsafe { core::mem::zeroed() };
        next.sa_sigaction = write_signal as *const () as libc::sighandler_t;
        next.sa_flags = libc::SA_RESTART;
        unsafe {
            check(libc::sigemptyset(&mut next.sa_mask))?;
            let mut previous: libc::sigaction = core::mem::zeroed();
            check(libc::sigaction(sig, &next, &mut previous))?;
            SAVED.lock().expect("signal disposition poisoned")[sig as usize] =
                Some(Saved { action: previous });
        }
        Ok(read)
    }

    fn ensure_pipe(sig: i32) -> Result<i32> {
        let slot = &PIPES[sig as usize];
        let existing = slot.write.load(Ordering::Acquire);
        if existing >= 0 {
            return Ok(existing);
        }
        let mut ends = [0; 2];
        check(unsafe { libc::pipe(ends.as_mut_ptr()) })?;
        for fd in ends {
            if let Err(error) = set_nonblock_cloexec(fd) {
                unsafe {
                    libc::close(ends[0]);
                    libc::close(ends[1]);
                }
                return Err(error);
            }
        }
        slot.read.store(ends[0], Ordering::Release);
        Ok(ends[1])
    }

    fn set_nonblock_cloexec(fd: i32) -> Result<()> {
        // SAFETY: `fd` is a pipe end this call just created and still owns.
        unsafe {
            let flags = check(libc::fcntl(fd, libc::F_GETFL))?;
            check(libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK))?;
            check(libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC))?;
        }
        Ok(())
    }

    pub(super) fn committed(_sig: i32) -> Result<()> {
        // Nothing left to do. `arm` never masked anything, because the
        // handler and not the mask is what keeps the default action away, so
        // there is no block here to lift and no moment at which the waiter
        // becomes armed later than it already was.
        Ok(())
    }

    pub(super) fn disarm(sig: i32) {
        let saved = {
            let mut guard = SAVED.lock().expect("signal disposition poisoned");
            guard[sig as usize].take()
        };
        let Some(saved) = saved else {
            return;
        };
        // Here the block earns its place, unlike in `arm`: it is not standing
        // in for the handler, it is holding this one thread still while the
        // disposition goes from ours to `SIG_IGN` to the program's, so that a
        // delivery cannot land on the default action mid-swap.
        let was_blocked = block_one(sig).unwrap_or(true);
        let write = PIPES[sig as usize].write.swap(-1, Ordering::AcqRel);
        unsafe {
            let mut ignore: libc::sigaction = core::mem::zeroed();
            ignore.sa_sigaction = libc::SIG_IGN;
            let _ = libc::sigemptyset(&mut ignore.sa_mask);
            let _ = libc::sigaction(sig, &ignore, core::ptr::null_mut());
        }
        if !was_blocked {
            // POSIX drops a pending signal when the action becomes `SIG_IGN`.
            // The unblock and reblock under ignore is the same thing for a
            // kernel that only drops it on delivery, and without it a
            // `SIGINT` still pending would run the restored default action.
            // Skipped when the program had the signal blocked already:
            // letting it through would be undoing a decision that was not
            // ours, and no handler of ours ever ran for it anyway.
            let _ = unblock_one(sig);
            let _ = block_one(sig);
        }
        unsafe {
            let _ = libc::sigaction(sig, &saved.action, core::ptr::null_mut());
        }
        if write >= 0 {
            PIPES[sig as usize].write.store(write, Ordering::Release);
        }
        let _ = restore_mask(sig, was_blocked);
    }

    pub(super) fn read_one(fd: i32) -> Result<SignalKind> {
        let mut byte = 0u8;
        // SAFETY: one byte, live for the call.
        let read =
            unsafe { libc::read(fd, core::ptr::addr_of_mut!(byte).cast::<libc::c_void>(), 1) };
        if read < 0 {
            return Err(crate::reactor::error::last());
        }
        if read == 0 {
            return Err(Errno(libc::EPIPE));
        }
        Ok(SignalKind::from_raw(i32::from(byte)))
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use super::{Signal, SignalKind};
    use crate::reactor::{block_on_with, Reactor};

    /// Disposition is process-wide. Two tests installing handlers at once
    /// would restore each other's.
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn current_handler(sig: i32) -> libc::sighandler_t {
        let mut action: libc::sigaction = unsafe { core::mem::zeroed() };
        unsafe {
            libc::sigaction(sig, core::ptr::null(), &mut action);
        }
        action.sa_sigaction
    }

    #[test]
    fn a_number_outside_1_to_31_is_rejected() {
        let reactor = Reactor::local().expect("reactor");
        let handle = reactor.handle();
        for number in [0, 32, 64] {
            let error = Signal::new(SignalKind::from_raw(number), &handle).expect_err("rejected");
            assert_eq!(error.0, libc::EINVAL, "signal {number}");
        }
    }

    #[test]
    fn a_signal_the_kernel_will_not_catch_is_rejected_and_can_be_asked_again() {
        let _guard = lock();
        let reactor = Reactor::local().expect("reactor");
        let handle = reactor.handle();
        for _ in 0..2 {
            let error =
                Signal::new(SignalKind::from_raw(libc::SIGKILL), &handle).expect_err("SIGKILL");
            assert_eq!(error.0, libc::EINVAL);
        }
    }

    #[test]
    fn a_second_waiter_for_the_same_signal_is_busy() {
        let _guard = lock();
        let reactor = Reactor::local().expect("reactor");
        let handle = reactor.handle();
        let first = Signal::new(SignalKind::hangup(), &handle).expect("first");
        let error = Signal::new(SignalKind::hangup(), &handle).expect_err("second");
        assert_eq!(error.0, libc::EBUSY);
        drop(first);
        // The claim has to move with the value. A drop that kept it would
        // make the only way to wait again be to restart the process.
        let second = Signal::new(SignalKind::hangup(), &handle);
        assert!(second.is_ok(), "waiter after drop: {second:?}");
    }

    #[test]
    fn interrupt_terminate_and_hangup_are_delivered_and_do_not_kill_the_process() {
        let _guard = lock();
        let reactor = Reactor::local().expect("reactor");
        let handle = reactor.handle();
        for kind in [
            SignalKind::hangup(),
            SignalKind::interrupt(),
            SignalKind::terminate(),
        ] {
            let before = current_handler(kind.as_raw());
            let mut signal = Signal::new(kind, &handle).expect("signal");
            // Synchronous: the handler runs before `raise` returns, so the
            // byte is in the pipe and the process is still here.
            unsafe { libc::raise(kind.as_raw()) };
            let got = block_on_with(&reactor, signal.recv()).expect("recv");
            assert_eq!(got, kind);
            drop(signal);
            assert_eq!(
                current_handler(kind.as_raw()),
                before,
                "drop did not restore the disposition for {}",
                kind.as_raw()
            );
        }
    }

    #[test]
    fn a_parked_waiter_observes_the_signal_through_the_reactor() {
        let _guard = lock();
        let reactor = Reactor::local().expect("reactor");
        let mut signal = Signal::new(SignalKind::terminate(), &reactor.handle()).expect("signal");
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut waiting = std::pin::pin!(signal.recv());
        assert!(waiting.as_mut().poll(&mut context).is_pending());

        unsafe { libc::raise(libc::SIGTERM) };

        let mut saw = false;
        for _ in 0..100 {
            reactor.poll_once_timeout(Some(0)).expect("poll");
            match waiting.as_mut().poll(&mut context) {
                Poll::Ready(result) => {
                    assert_eq!(result.expect("recv"), SignalKind::terminate());
                    saw = true;
                    break;
                }
                Poll::Pending => std::thread::yield_now(),
            }
        }
        assert!(saw, "a parked waiter did not observe SIGTERM");
    }

    #[test]
    fn two_signals_do_not_complete_each_other() {
        let _guard = lock();
        let reactor = Reactor::local().expect("reactor");
        let handle = reactor.handle();
        let mut interrupt = Signal::new(SignalKind::interrupt(), &handle).expect("int");
        let mut terminate = Signal::new(SignalKind::terminate(), &handle).expect("term");

        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut term = std::pin::pin!(terminate.recv());
        assert!(term.as_mut().poll(&mut context).is_pending());

        unsafe { libc::raise(libc::SIGINT) };
        let got = block_on_with(&reactor, interrupt.recv()).expect("int");
        assert_eq!(got, SignalKind::interrupt());
        assert!(
            term.as_mut().poll(&mut context).is_pending(),
            "SIGTERM completed because SIGINT was delivered"
        );
    }

    #[test]
    fn a_delivery_from_another_thread_wakes_a_blocked_wait() {
        let _guard = lock();
        let reactor = Reactor::local().expect("reactor");
        let mut signal = Signal::new(SignalKind::hangup(), &reactor.handle()).expect("signal");
        // Aimed at this thread, the waiter. Not `raise` on the other thread:
        // that targets the raising thread, which on Linux inherited the block
        // `Signal::new` took, so the signal stayed pending there and
        // `signalfd`, which reports only its reader's and process-wide
        // signals, never saw it. The test hung until CI cancelled the job.
        let waiter = unsafe { libc::pthread_self() } as usize;
        let started = AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while !started.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
                // The waiter needs to reach the kernel wait. A signal that
                // lands before that is still delivered, by being pending
                // already; the sleep is what makes this the parked case.
                std::thread::sleep(Duration::from_millis(50));
                let status = unsafe { libc::pthread_kill(waiter as libc::pthread_t, libc::SIGHUP) };
                assert_eq!(status, 0, "pthread_kill");
            });
            started.store(true, Ordering::Release);
            // Bounded, so a regression fails here in seconds rather than
            // holding the test lock until CI gives up on the job.
            let got = block_on_with(
                &reactor,
                crate::timeout(Duration::from_secs(5), signal.recv()),
            )
            .expect("no SIGHUP within 5s")
            .expect("recv");
            assert_eq!(got, SignalKind::hangup());
        });
    }
}

/// The same waiter, at the path tokio puts it.
///
/// tokio spells this `tokio::signal::unix::{signal, SignalKind, Signal}`, and
/// a consumer being moved off it arrives looking for that. The one difference
/// the name cannot carry is the `Handle`: tokio's `signal(kind)` reaches a
/// process-global driver its runtime is already turning, and a nagoya
/// [`Signal`] is registered on one reactor and only fires while that reactor
/// is polled, so there is nowhere to infer it from and it has to be passed.
pub mod unix {
    pub use super::{Signal, SignalKind};

    /// Wait for `kind` on the reactor behind `handle`.
    ///
    /// `tokio::signal::unix::signal(kind)` with the reactor named, and the
    /// same thing as [`Signal::new`].
    pub fn signal(kind: SignalKind, handle: &crate::reactor::Handle) -> super::Result<Signal> {
        Signal::new(kind, handle)
    }
}
