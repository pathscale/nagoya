//! Child processes, spelled the way `tokio::process` spells them.
//!
//! [`Command`] builds and spawns, [`Child`] waits for and kills, and
//! [`ChildStdin`], [`ChildStdout`] and [`ChildStderr`] are the pipes, as
//! `futures-io` readers and writers. A caller moving off tokio changes the
//! import path and the `AsyncRead` it names, and nothing else.
//!
//! # No handle to pass
//!
//! Every other reactor type in this crate is registered on a reactor the
//! caller names, because a caller that runs [`block_on`](crate::reactor::block_on)
//! on its own poller wants its descriptors there. A child process is not that
//! kind of caller. It is spawned from build scripts, from tests and from
//! whatever executor a library happens to be polled by, and the future it
//! hands back has to finish under all of them: `nagoya::block_on`,
//! `nagoya::spawn`, and an executor this crate has never heard of.
//!
//! So the pipes and the exit notification register on one reactor per
//! process, started with [`Reactor::start`] the first time anything here is
//! spawned and kept in a static for the rest of the process. It never drops,
//! on purpose: dropping a `Reactor` stops its thread, and every descriptor
//! registered on it would then wait for an edge nobody is left to deliver. A
//! build that never spawns a child never starts the thread.
//!
//! # Knowing that a child exited, without asking
//!
//! Nothing here polls `waitpid` on a timer or sleeps in a loop. The exit is a
//! descriptor that turns readable, registered like a socket:
//!
//! - On Linux that descriptor is a `pidfd` from `pidfd_open`, which the kernel
//!   makes readable when the process terminates.
//! - On macOS and the BSDs it is a kqueue of the child's own, holding one
//!   `EVFILT_PROC` filter for `NOTE_EXIT`. A kqueue is itself a descriptor that
//!   is readable while it has an event pending, so the reactor watches it with
//!   an ordinary read filter. `EVFILT_PROC` could not go on the reactor's own
//!   kqueue directly: its identifier is a pid rather than a descriptor, and a
//!   [`Registration`] removes itself by descriptor number on drop, which for a
//!   pid would delete the filters of whatever unrelated descriptor happened to
//!   share the number.
//!
//! Either way it costs one descriptor per live child and no thread. The one
//! exception is a Linux kernel older than 5.3, or a sandbox that refuses
//! `pidfd_open`: there a child gets a thread of its own, blocked in `waitid`
//! with `WNOWAIT` so that it observes the exit without reaping it, which then
//! wakes the waiter. That is a thread per child, but only where the kernel
//! offers nothing better, and it still never polls.
//!
//! The status itself always comes from `waitpid`, through
//! [`std::process::Child::try_wait`]. The descriptor only says when to ask.
//!
//! # Cancel safety, and children nobody waited for
//!
//! [`Child::wait`] is cancel safe. Dropping the future part way leaves a waker
//! parked that wakes nothing of consequence, and the next `wait` parks its own.
//! The status is kept once reaped, so every later `wait` returns it at once.
//!
//! A [`Child`] dropped before its exit was reaped is handed to the reactor,
//! which reaps it when it exits, so dropping a handle does not leave a zombie
//! behind for the life of the process. [`Command::kill_on_drop`] sends
//! `SIGKILL` first. Both are what tokio does.
//!
//! # A write to a child that has gone
//!
//! Is `EPIPE`, provided `SIGPIPE` is ignored, which a Rust `main` arranges
//! before it runs. A program whose entry point is not Rust's and that leaves
//! `SIGPIPE` at its default is terminated by that write instead, exactly as it
//! would be writing to the same pipe with `std`.

// Pipes, `fcntl`, `pidfd_open` and `kevent` are the kernel's, so this module
// lifts the crate-wide deny the way `net` and `signal` do. Every block names
// the invariant it relies on.
#![allow(unsafe_code)]

use std::ffi::OsStr;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll, Wake, Waker};

use crate::reactor::driver::{Handle, Reactor, Registration};
use crate::reactor::poller::Interest;

pub use std::process::{ExitStatus, Output, Stdio};

/// The reactor every pipe and every exit notification in this module uses.
///
/// Started on first use and never dropped: a static is not dropped at exit,
/// and that is the property wanted, because a dropped `Reactor` stops its
/// thread and strands every registration on it.
static REACTOR: OnceLock<Reactor> = OnceLock::new();

/// A handle to the process-wide reactor, starting it if this is the first ask.
///
/// Not `get_or_init`, because starting a reactor can fail and a failure has to
/// come back as an error from `spawn` rather than a panic inside a lazy
/// initialiser. Two threads spawning their first child at the same moment can
/// both start one; the loser's is dropped here, which stops and joins its
/// thread before anything was ever registered on it.
fn reactor() -> io::Result<Handle> {
    if let Some(reactor) = REACTOR.get() {
        return Ok(reactor.handle());
    }
    let started = Reactor::start()?;
    if let Err(lost) = REACTOR.set(started) {
        drop(lost);
    }
    match REACTOR.get() {
        Some(reactor) => Ok(reactor.handle()),
        None => Err(io::Error::other("the process reactor was not stored")),
    }
}

/// A process to spawn, and how.
///
/// A wrapper over [`std::process::Command`] with tokio's method names, which
/// are `std`'s names. The only setting of its own is
/// [`kill_on_drop`](Self::kill_on_drop); everything else is passed straight
/// through, so [`as_std_mut`](Self::as_std_mut) reaches whatever a platform
/// extension trait adds.
#[derive(Debug)]
pub struct Command {
    std: std::process::Command,
    kill_on_drop: bool,
}

impl Command {
    /// A command to run `program`, with no arguments and the parent's
    /// environment, working directory and standard streams.
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            std: std::process::Command::new(program),
            kill_on_drop: false,
        }
    }

    /// Add one argument.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.std.arg(arg);
        self
    }

    /// Add several arguments, in order.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.std.args(args);
        self
    }

    /// Set one environment variable for the child.
    pub fn env<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.std.env(key, value);
        self
    }

    /// Set several environment variables for the child.
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.std.envs(vars);
        self
    }

    /// Remove one environment variable from what the child inherits.
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        self.std.env_remove(key);
        self
    }

    /// Start the child with an empty environment, apart from what is set
    /// after this.
    pub fn env_clear(&mut self) -> &mut Self {
        self.std.env_clear();
        self
    }

    /// The directory the child starts in.
    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.std.current_dir(dir);
        self
    }

    /// What the child's standard input is connected to.
    ///
    /// [`Stdio::piped`] gives the parent a [`ChildStdin`] in [`Child::stdin`].
    pub fn stdin(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.std.stdin(cfg);
        self
    }

    /// What the child's standard output is connected to.
    ///
    /// [`Stdio::piped`] gives the parent a [`ChildStdout`] in
    /// [`Child::stdout`].
    pub fn stdout(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.std.stdout(cfg);
        self
    }

    /// What the child's standard error is connected to.
    ///
    /// [`Stdio::piped`] gives the parent a [`ChildStderr`] in
    /// [`Child::stderr`].
    pub fn stderr(&mut self, cfg: impl Into<Stdio>) -> &mut Self {
        self.std.stderr(cfg);
        self
    }

    /// Whether dropping the [`Child`] before it has been reaped sends it
    /// `SIGKILL`.
    ///
    /// Off by default, as in tokio. Either way a dropped child is reaped once
    /// it exits; this only decides whether it is asked to exit first.
    pub fn kill_on_drop(&mut self, kill_on_drop: bool) -> &mut Self {
        self.kill_on_drop = kill_on_drop;
        self
    }

    /// Whether [`kill_on_drop`](Self::kill_on_drop) is set.
    #[must_use]
    pub fn get_kill_on_drop(&self) -> bool {
        self.kill_on_drop
    }

    /// The `std` command underneath.
    #[must_use]
    pub fn as_std(&self) -> &std::process::Command {
        &self.std
    }

    /// The `std` command underneath, for a setting this type does not
    /// forward, such as the ones in `std::os::unix::process::CommandExt`.
    pub fn as_std_mut(&mut self) -> &mut std::process::Command {
        &mut self.std
    }

    /// Start the child.
    ///
    /// Returns as soon as it is running, with its pipes registered and its
    /// exit watched. The reactor is started before the child is, so a
    /// reactor that cannot start spawns nothing. A child that did start but
    /// whose pipes or exit could not be registered is killed and reaped
    /// before the error is returned, rather than left running with nobody
    /// holding it.
    pub fn spawn(&mut self) -> io::Result<Child> {
        let handle = reactor()?;
        let mut child = self.std.spawn()?;
        match adopt(&mut child, &handle) {
            Ok((stdin, stdout, stderr, watch)) => Ok(Child {
                stdin,
                stdout,
                stderr,
                process: Some(Process { watch, child }),
                status: None,
                kill_on_drop: self.kill_on_drop,
            }),
            Err(error) => {
                // The kill is bounded and so is the wait after it: `SIGKILL`
                // cannot be caught, so the child is already on its way out.
                let _ = child.kill();
                let _ = child.wait();
                Err(error)
            }
        }
    }

    /// Run the child to completion and collect what it wrote.
    ///
    /// Standard output and standard error are captured whatever they were set
    /// to, as tokio does; standard input is left as configured.
    pub async fn output(&mut self) -> io::Result<Output> {
        self.std.stdout(Stdio::piped());
        self.std.stderr(Stdio::piped());
        let child = self.spawn();
        child?.wait_with_output().await
    }

    /// Run the child to completion and return how it exited.
    ///
    /// Any pipes configured are closed on the parent's side before waiting,
    /// because a child blocked writing to a pipe nobody reads, or reading one
    /// nobody writes, would never exit.
    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        let mut child = self.spawn()?;
        child.stdin = None;
        child.stdout = None;
        child.stderr = None;
        child.wait().await
    }
}

impl From<std::process::Command> for Command {
    fn from(std: std::process::Command) -> Self {
        Self {
            std,
            kill_on_drop: false,
        }
    }
}

/// The parent's ends of whichever pipes were asked for, and the exit watch.
type Adopted = (
    Option<ChildStdin>,
    Option<ChildStdout>,
    Option<ChildStderr>,
    Watch,
);

/// Take `child`'s pipes and exit notification onto the reactor.
fn adopt(child: &mut std::process::Child, handle: &Handle) -> io::Result<Adopted> {
    let stdin = match child.stdin.take() {
        Some(pipe) => Some(ChildStdin {
            pipe: Some(Pipe::new(OwnedFd::from(pipe), Interest::WRITABLE, handle)?),
        }),
        None => None,
    };
    let stdout = match child.stdout.take() {
        Some(pipe) => Some(ChildStdout {
            pipe: Pipe::new(OwnedFd::from(pipe), Interest::READABLE, handle)?,
        }),
        None => None,
    };
    let stderr = match child.stderr.take() {
        Some(pipe) => Some(ChildStderr {
            pipe: Pipe::new(OwnedFd::from(pipe), Interest::READABLE, handle)?,
        }),
        None => None,
    };
    let watch = Watch::new(child.id(), handle)?;
    Ok((stdin, stdout, stderr, watch))
}

/// A running, or exited, child process.
///
/// The pipes are public fields, as in tokio and `std`, so they can be taken
/// and moved into other tasks while this handle stays behind to wait.
#[derive(Debug)]
pub struct Child {
    /// The parent's end of the child's standard input, if it was piped.
    pub stdin: Option<ChildStdin>,
    /// The parent's end of the child's standard output, if it was piped.
    pub stdout: Option<ChildStdout>,
    /// The parent's end of the child's standard error, if it was piped.
    pub stderr: Option<ChildStderr>,
    /// The process and its exit watch, until it has been reaped.
    ///
    /// `None` exactly when `status` is `Some`. Dropped on reaping, which
    /// closes the watch's descriptor and makes [`Child::id`] answer `None`:
    /// once reaped the pid belongs to the kernel again and may already name
    /// an unrelated process.
    process: Option<Process>,
    /// How the child exited, once that has been observed.
    status: Option<ExitStatus>,
    kill_on_drop: bool,
}

impl Child {
    /// The child's process id, or `None` once it has been reaped.
    #[must_use]
    pub fn id(&self) -> Option<u32> {
        self.process.as_ref().map(|process| process.child.id())
    }

    /// Wait for the child to exit, and return how it did.
    ///
    /// Closes [`stdin`](Self::stdin) first, as tokio does: a child reading its
    /// input to the end would otherwise never exit while the parent held the
    /// write end open and waited.
    ///
    /// Cancel safe. A future dropped part way loses nothing, and a later call
    /// waits again from where things stand. Once the child has been reaped
    /// every call returns the same status immediately.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.stdin = None;
        std::future::poll_fn(|cx| self.poll_wait(cx)).await
    }

    /// Return the exit status if the child has exited, without waiting.
    ///
    /// Reaps the child when it has exited, so a later [`wait`](Self::wait)
    /// returns the same status without asking the kernel again.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        let Some(process) = self.process.as_mut() else {
            return Err(unreachable_state());
        };
        match process.child.try_wait()? {
            Some(status) => {
                self.reaped(status);
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    /// Send the child `SIGKILL`, without waiting for it to exit.
    ///
    /// Fails with `InvalidInput` once the child has been reaped, as tokio's
    /// does: the pid may name another process by then, and signalling it is
    /// not something to do by accident.
    pub fn start_kill(&mut self) -> io::Result<()> {
        match self.process.as_mut() {
            Some(process) => process.child.kill(),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid argument: can't kill an exited process",
            )),
        }
    }

    /// Send the child `SIGKILL` and wait for it to exit.
    ///
    /// The status is kept, so a [`wait`](Self::wait) afterwards returns it and
    /// reports the signal.
    pub async fn kill(&mut self) -> io::Result<()> {
        self.start_kill()?;
        self.wait().await?;
        Ok(())
    }

    /// Wait for the child to exit, collecting everything it wrote to the
    /// pipes that were set up for its standard output and standard error.
    ///
    /// Both are read at the same time as each other, since a child that
    /// fills one pipe while the parent is still reading the other stops
    /// until that one is read. Standard input is closed first. A stream that
    /// was not piped, or was already taken, comes back empty.
    pub async fn wait_with_output(mut self) -> io::Result<Output> {
        self.stdin = None;
        let mut stdout_pipe = self.stdout.take();
        let mut stderr_pipe = self.stderr.take();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        std::future::poll_fn(|cx| {
            let out = drain(cx, &mut stdout_pipe, &mut stdout);
            let err = drain(cx, &mut stderr_pipe, &mut stderr);
            match (out, err) {
                (Poll::Ready(Err(error)), _) | (_, Poll::Ready(Err(error))) => {
                    Poll::Ready(Err(error))
                }
                (Poll::Ready(Ok(())), Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
                _ => Poll::Pending,
            }
        })
        .await?;
        let status = self.wait().await?;
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }

    fn poll_wait(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<ExitStatus>> {
        if let Some(status) = self.status {
            return Poll::Ready(Ok(status));
        }
        let Some(process) = self.process.as_mut() else {
            return Poll::Ready(Err(unreachable_state()));
        };
        match process.poll_exit(cx) {
            Poll::Ready(Ok(status)) => {
                self.reaped(status);
                Poll::Ready(Ok(status))
            }
            other => other,
        }
    }

    /// Keep `status` and let go of the process, which closes its watch.
    fn reaped(&mut self, status: ExitStatus) {
        self.status = Some(status);
        self.process = None;
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        let Some(mut process) = self.process.take() else {
            return;
        };
        if self.kill_on_drop {
            let _ = process.child.kill();
        }
        Orphan::adopt(process);
    }
}

/// The error for a [`Child`] that has neither a status nor a process.
///
/// Every path that drops the process records the status first, so this is
/// not expected to be seen. It is an error rather than a panic because a
/// wrong answer from one child is not a reason to take down the caller.
fn unreachable_state() -> io::Error {
    io::Error::other("child has neither an exit status nor a process")
}

/// Read `pipe` to the end into `into`, forgetting the pipe once it is there.
fn drain<R>(cx: &mut Context<'_>, pipe: &mut Option<R>, into: &mut Vec<u8>) -> Poll<io::Result<()>>
where
    R: futures_io::AsyncRead + Unpin,
{
    let Some(reader) = pipe.as_mut() else {
        return Poll::Ready(Ok(()));
    };
    let mut chunk = [0u8; 8192];
    loop {
        match futures_io::AsyncRead::poll_read(Pin::new(&mut *reader), cx, &mut chunk) {
            Poll::Ready(Ok(0)) => {
                *pipe = None;
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Ok(read)) => into.extend_from_slice(&chunk[..read]),
            Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
    }
}

/// A child that has not been reaped, and the means of knowing when to.
#[derive(Debug)]
struct Process {
    watch: Watch,
    child: std::process::Child,
}

impl Process {
    /// Reap the child if it has exited, or park `cx`'s waker for its exit.
    ///
    /// `waitpid` is asked first and the watch second, then round again if the
    /// watch says something happened. That order is what makes a lost wakeup
    /// impossible: the watch parks the waker under the same lock that
    /// observes the flag the reactor sets, so an exit landing after the
    /// `waitpid` either sets the flag before the park, and is seen by going
    /// round, or finds the waker already parked and wakes it.
    fn poll_exit(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<ExitStatus>> {
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Poll::Ready(Ok(status)),
                Ok(None) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Poll::Ready(Err(error)),
            }
            match self.watch.take_or_park(cx.waker()) {
                Readiness::Parked => return Poll::Pending,
                Readiness::Check => {}
                // The kernel has already said the process is past the point
                // where it could be watched, so it is inside `exit` and the
                // only thing left is for it to become waitable. That is
                // bounded by the rest of its own exit, not by anything it
                // could still choose to do, so the blocking wait is short.
                Readiness::Exiting => return Poll::Ready(self.child.wait()),
            }
        }
    }
}

/// What a [`Watch`] had to say.
enum Readiness {
    /// Nothing yet. The waker is parked and will be woken on exit.
    Parked,
    /// The child may have exited. Ask `waitpid`.
    Check,
    /// The child was already exiting when the watch was set up.
    Exiting,
}

/// How a child's exit reaches a waker.
#[derive(Debug)]
enum Watch {
    /// A descriptor that turns readable when the child exits: a `pidfd` on
    /// Linux, a kqueue holding one `EVFILT_PROC` filter elsewhere.
    Descriptor {
        /// First, so it leaves the poller before the descriptor it names is
        /// closed and its number handed to somebody else.
        registration: Registration,
        /// Held and never read. Closing it is what ends the watch.
        _fd: OwnedFd,
    },
    /// A thread blocked in `waitid`, where there is no `pidfd`.
    #[cfg(target_os = "linux")]
    Thread(Arc<ThreadExit>),
    /// The kernel refused to watch a process that was already exiting.
    Exiting,
}

impl Watch {
    fn new(pid: u32, handle: &Handle) -> io::Result<Self> {
        match sys::watch(pid) {
            Ok(Some(fd)) => {
                let registration = handle.register(fd.as_raw_fd(), Interest::READABLE)?;
                Ok(Self::Descriptor {
                    registration,
                    _fd: fd,
                })
            }
            Ok(None) => Ok(Self::Exiting),
            // No `pidfd`: an old kernel says `ENOSYS`, a seccomp profile
            // that predates the call says `EPERM` or `ENOSYS`. Either way
            // the fallback is the thread, not a failed spawn.
            #[cfg(target_os = "linux")]
            Err(_) => ThreadExit::watch(pid),
            #[cfg(not(target_os = "linux"))]
            Err(error) => Err(error),
        }
    }

    fn take_or_park(&self, waker: &Waker) -> Readiness {
        match self {
            // Readable from the start, as every registration is, so the
            // first answer is always `Check`. That costs one `waitpid` and
            // is what catches an exit that happened before the registration
            // existed and whose edge will therefore never be delivered.
            Self::Descriptor { registration, .. } => {
                if registration.take_readable_or_park(waker) {
                    Readiness::Check
                } else {
                    Readiness::Parked
                }
            }
            #[cfg(target_os = "linux")]
            Self::Thread(exit) => {
                if exit.observed_or_park(waker) {
                    Readiness::Check
                } else {
                    Readiness::Parked
                }
            }
            Self::Exiting => Readiness::Exiting,
        }
    }
}

/// The exit a fallback thread observed, and the waker to tell.
#[cfg(target_os = "linux")]
#[derive(Debug, Default)]
struct ThreadExit {
    state: Mutex<ThreadExitState>,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Default)]
struct ThreadExitState {
    /// Latched, never cleared: an exit does not un-happen.
    exited: bool,
    waker: Option<Waker>,
}

#[cfg(target_os = "linux")]
impl ThreadExit {
    /// Start a thread that waits for `pid` to exit without reaping it.
    ///
    /// `WNOWAIT` is the whole point: the thread only learns that the child is
    /// waitable, and the reaping stays with `std`'s `try_wait`, so the status
    /// is taken exactly once and by the owner of the [`Child`].
    fn watch(pid: u32) -> io::Result<Watch> {
        let exit = Arc::new(Self::default());
        let observer = Arc::clone(&exit);
        let id: libc::id_t = pid;
        std::thread::Builder::new()
            .name("nagoya-process-exit".into())
            .spawn(move || {
                loop {
                    // SAFETY: `info` is a live local of the type `waitid`
                    // fills, and an all-zero `siginfo_t` is a valid value.
                    let mut info: libc::siginfo_t = unsafe { core::mem::zeroed() };
                    // SAFETY: `info` outlives the call. `WNOWAIT` leaves the
                    // child waitable, so this reaps nothing.
                    let result = unsafe {
                        libc::waitid(libc::P_PID, id, &mut info, libc::WEXITED | libc::WNOWAIT)
                    };
                    // Anything but an interrupted wait ends the watch. An
                    // error other than `EINTR` means the child is no longer
                    // this process's to wait for, and `try_wait` reports that
                    // properly once the waiter is woken to ask.
                    if result == 0
                        || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted
                    {
                        break;
                    }
                }
                observer.exited();
            })?;
        Ok(Watch::Thread(exit))
    }

    fn exited(&self) {
        // Taken under the lock and woken outside it: a waker may run
        // arbitrary code, including a poll that takes this lock.
        let waker = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.exited = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Whether the exit has been seen, parking `waker` under the same lock
    /// if it has not, so an exit cannot land between the answer and the park.
    fn observed_or_park(&self, waker: &Waker) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.exited {
            return true;
        }
        match &state.waker {
            Some(existing) if existing.will_wake(waker) => {}
            _ => state.waker = Some(waker.clone()),
        }
        false
    }
}

/// A child whose [`Child`] was dropped before it was reaped.
///
/// Its own waker: the reactor wakes it when the child exits, on the reactor
/// thread, and waking it reaps. No executor is involved, which matters
/// because a dropped handle has no task left to poll it. The waker parked
/// in the registration holds the orphan and the orphan holds the
/// registration, and that cycle is what keeps it alive until the exit. The
/// reactor takes the waker out of the slot before waking it, which breaks the
/// cycle, and reaping drops the rest.
struct Orphan {
    process: Mutex<Option<Process>>,
}

impl Orphan {
    fn adopt(process: Process) {
        let orphan = Arc::new(Self {
            process: Mutex::new(Some(process)),
        });
        orphan.reap_or_park();
    }

    fn reap_or_park(self: &Arc<Self>) {
        let waker = Waker::from(Arc::clone(self));
        let mut context = Context::from_waker(&waker);
        let mut process = self.process.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(running) = process.as_mut() else {
            return;
        };
        // Reaped, or failed in a way a later try would fail the same way.
        // Either way there is nothing left to learn and the watch can go.
        if running.poll_exit(&mut context).is_ready() {
            *process = None;
        }
    }
}

impl Wake for Orphan {
    fn wake(self: Arc<Self>) {
        self.reap_or_park();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.reap_or_park();
    }
}

/// The parent's end of one pipe, registered with the process reactor.
#[derive(Debug)]
struct Pipe {
    /// First, for the reason [`Watch::Descriptor`] gives.
    registration: Registration,
    fd: OwnedFd,
}

impl Pipe {
    fn new(fd: OwnedFd, interest: Interest, handle: &Handle) -> io::Result<Self> {
        // The parent's end only. A pipe's two ends are separate open file
        // descriptions, so the child's end keeps the blocking mode a child
        // expects, and only this side learns to say `EAGAIN`.
        set_nonblocking(fd.as_raw_fd())?;
        let registration = handle.register(fd.as_raw_fd(), interest)?;
        Ok(Self { registration, fd })
    }

    /// Read into `buffer`, parking `cx`'s waker if the pipe is empty.
    ///
    /// The socket contract from `net`, with one difference in how the wait is
    /// entered: after `EAGAIN` this goes back round through
    /// `take_readable_or_park` rather than parking unconditionally, so an edge
    /// that arrived between the read and the park is taken rather than
    /// slept through.
    fn poll_read(&self, cx: &mut Context<'_>, buffer: &mut [u8]) -> Poll<io::Result<usize>> {
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            if !self.registration.take_readable_or_park(cx.waker()) {
                return Poll::Pending;
            }
            match read_fd(self.fd.as_raw_fd(), buffer) {
                Ok(read) => {
                    // A read that filled the buffer has not shown the pipe
                    // empty, and end of file stays end of file, so both keep
                    // readiness. A short read from a pipe has shown it empty:
                    // the kernel hands over everything that is there.
                    if read == 0 || read == buffer.len() {
                        self.registration.mark_readable();
                    }
                    return Poll::Ready(Ok(read));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => {
                    // Nothing was learned about readiness, so it is put back
                    // for the retry or for the next call.
                    self.registration.mark_readable();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Poll::Ready(Err(error));
                    }
                }
            }
        }
    }

    /// Write from `buffer`, parking `cx`'s waker if the pipe is full.
    fn poll_write(&self, cx: &mut Context<'_>, buffer: &[u8]) -> Poll<io::Result<usize>> {
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            if !self.registration.take_writable_or_park(cx.waker()) {
                return Poll::Pending;
            }
            match write_fd(self.fd.as_raw_fd(), buffer) {
                Ok(written) => {
                    // Readiness is always put back after a write that
                    // succeeded, and only `EAGAIN` consumes it. A short write
                    // to a pipe usually means it is full, but not always, and
                    // guessing wrong would wait for an edge that has already
                    // gone by. The price is one rejected write per time the
                    // pipe fills.
                    self.registration.mark_writable();
                    return Poll::Ready(Ok(written));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => {
                    self.registration.mark_writable();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Poll::Ready(Err(error));
                    }
                }
            }
        }
    }
}

/// The parent's end of a child's standard input.
///
/// [`close`](futures_io::AsyncWrite::poll_close) closes the pipe, which is
/// how the child sees end of file. Dropping this does the same.
#[derive(Debug)]
pub struct ChildStdin {
    /// `None` once closed.
    pipe: Option<Pipe>,
}

/// The parent's end of a child's standard output.
#[derive(Debug)]
pub struct ChildStdout {
    pipe: Pipe,
}

/// The parent's end of a child's standard error.
#[derive(Debug)]
pub struct ChildStderr {
    pipe: Pipe,
}

impl futures_io::AsyncWrite for ChildStdin {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &self.get_mut().pipe {
            Some(pipe) => pipe.poll_write(cx, buffer),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the child's standard input was already closed",
            ))),
        }
    }

    /// Nothing to flush: every write goes straight to the pipe.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Dropping the pipe deregisters it and then closes the descriptor.
        // Closing twice is not an error, as a second `close` of a socket that
        // has shut down is not one either.
        self.get_mut().pipe = None;
        Poll::Ready(Ok(()))
    }
}

impl futures_io::AsyncRead for ChildStdout {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().pipe.poll_read(cx, buffer)
    }
}

impl futures_io::AsyncRead for ChildStderr {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().pipe.poll_read(cx, buffer)
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is a descriptor the caller owns. `F_GETFL` and `F_SETFL`
    // read and write its status flags and touch no memory.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// One `read`, with -1 turned into the error it stands for.
fn read_fd(fd: RawFd, buffer: &mut [u8]) -> io::Result<usize> {
    let pointer = buffer.as_mut_ptr().cast::<libc::c_void>();
    // SAFETY: `pointer` is a live initialised slice of `buffer.len()` bytes,
    // and `read` writes at most that many into it.
    let read = unsafe { libc::read(fd, pointer, buffer.len()) };
    usize::try_from(read).map_err(|_| io::Error::last_os_error())
}

/// One `write`, with -1 turned into the error it stands for.
fn write_fd(fd: RawFd, buffer: &[u8]) -> io::Result<usize> {
    let pointer = buffer.as_ptr().cast::<libc::c_void>();
    // SAFETY: `pointer` is a live slice of `buffer.len()` bytes, and `write`
    // reads at most that many from it.
    let written = unsafe { libc::write(fd, pointer, buffer.len()) };
    usize::try_from(written).map_err(|_| io::Error::last_os_error())
}

// --- pidfd ----------------------------------------------------------------

#[cfg(target_os = "linux")]
mod sys {
    use std::io;
    use std::os::fd::{FromRawFd, OwnedFd, RawFd};

    /// A `pidfd` for `pid`, or `None` if there is no such process to watch.
    ///
    /// Through `syscall` rather than a libc wrapper, because glibc only grew
    /// one in 2.36 and the call itself has been there since Linux 5.3. The
    /// descriptor is close-on-exec without being asked.
    pub(super) fn watch(pid: u32) -> io::Result<Option<OwnedFd>> {
        let pid =
            libc::pid_t::try_from(pid).map_err(|_| io::Error::from_raw_os_error(libc::ESRCH))?;
        let flags: libc::c_uint = 0;
        // SAFETY: `pidfd_open` takes a pid and a flags word, touches no
        // memory of ours, and returns a new descriptor or -1.
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, flags) };
        if raw < 0 {
            let error = io::Error::last_os_error();
            // A zombie still has a pid to open, so this only happens to a
            // child something else has already reaped. `try_wait` will say
            // so, which is a better error than one from here.
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(None);
            }
            return Err(error);
        }
        let raw = RawFd::try_from(raw).map_err(|_| io::Error::from_raw_os_error(libc::EBADF))?;
        // SAFETY: a fresh descriptor the kernel just returned, owned by
        // nothing else.
        Ok(Some(unsafe { OwnedFd::from_raw_fd(raw) }))
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
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    /// A kqueue that turns readable when `pid` exits, or `None` if the
    /// process is already too far into exiting to be watched.
    ///
    /// Not close-on-exec, and it does not need to be: a kqueue is not
    /// inherited across `fork` at all, so no child ever holds one.
    pub(super) fn watch(pid: u32) -> io::Result<Option<OwnedFd>> {
        // SAFETY: kqueue takes no arguments and returns a descriptor or -1.
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a fresh descriptor this call owns exclusively.
        let queue = unsafe { OwnedFd::from_raw_fd(raw) };

        // Zeroed and then filled rather than written as a literal, because
        // the struct grows fields on some of the BSDs and a literal would
        // stop compiling there.
        // SAFETY: `kevent` is a plain C struct of integers and a pointer, and
        // all zeroes is a valid value of it.
        let mut change: libc::kevent = unsafe { core::mem::zeroed() };
        change.ident = pid as libc::uintptr_t;
        change.filter = libc::EVFILT_PROC;
        change.flags = libc::EV_ADD;
        change.fflags = libc::NOTE_EXIT;
        // SAFETY: one live change, no event list, and a null timeout, which
        // with no events asked for does not wait.
        let result = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &change,
                1,
                core::ptr::null_mut(),
                0,
                core::ptr::null(),
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            // The kernel stops handing out references to a process early in
            // its exit, before it becomes a zombie, and refuses the filter
            // from then on. The filter would have fired anyway, so this is
            // the exit being reported, only sooner.
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(None);
            }
            return Err(error);
        }
        Ok(Some(queue))
    }
}
