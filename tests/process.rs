//! Child processes, through the public API, under three executors.
//!
//! The executor is the variable that matters here. A child's pipes and its
//! exit are registered on the reactor the caller passes in, which has a thread
//! of its own, so the claim being tested is that the futures finish whoever
//! polls them: `nagoya::block_on`, a task on `nagoya::spawn`, and tokio's
//! current thread runtime, which knows nothing about nagoya's reactor at all.
//!
//! Every test starts its own reactor and keeps it for the whole test. Nothing
//! is shared between them, so no test can leave state behind for another.
//!
//! Every program is a real one from `/bin` or `/usr/bin`, because what could
//! go wrong is the kernel's half: a pipe that never reports end of file, an
//! exit that never turns a descriptor readable.

use std::future::Future;
use std::os::unix::process::ExitStatusExt;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use futures_util::{AsyncReadExt, AsyncWriteExt};
use nagoya::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use nagoya::reactor::Reactor;

/// How long any one of these may take before it counts as hung.
///
/// Generous against the milliseconds each needs, and well inside the ten
/// seconds a test is allowed, so a failure here is a hang rather than a slow
/// machine.
const BOUND: Duration = Duration::from_secs(5);

/// A reactor for one test, with a thread of its own polling it.
fn reactor() -> Reactor {
    Reactor::start().expect("reactor")
}

/// Run `future` on tokio's current thread runtime.
///
/// Built without `enable_io` or `enable_time` on purpose: nothing tokio drives
/// is involved, so if these futures finish here they finish on their own.
fn on_tokio<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

#[cfg(not(target_os = "linux"))]
#[test]
fn cloexec_spawn_releases_pending_network_polls() {
    use nagoya::net::{TcpListener, TcpStream};
    use nagoya::reactor::Addr;
    use std::io::Read as _;
    use std::net::Shutdown;
    use std::os::fd::AsRawFd as _;
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::task::Wake;

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let reactor = Reactor::local().expect("local reactor");
    let handle = reactor.handle();
    let listener = TcpListener::bind(Addr::localhost(0), &handle).expect("listen");
    let addr = listener.local_addr().expect("listener address");
    let _peer = std::net::TcpStream::connect(("127.0.0.1", addr.port())).expect("queue peer");
    reactor
        .poll_once_timeout(Some(0))
        .expect("drain listener readiness");

    let (mut control, child_control) = UnixStream::pair().expect("spawn control");
    control.set_read_timeout(Some(BOUND)).expect("read timeout");
    let child_fd = child_control.as_raw_fd();
    let mut command = Command::new("/usr/bin/true");
    // SAFETY: The child uses only async-signal-safe syscalls on the inherited
    // socket before exec. The parent keeps that descriptor open through spawn.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            let byte = 1u8;
            if libc::write(child_fd, core::ptr::addr_of!(byte).cast(), 1) != 1 {
                return Err(std::io::Error::last_os_error());
            }
            let mut byte = 0u8;
            loop {
                if libc::read(child_fd, core::ptr::addr_of_mut!(byte).cast(), 1) >= 0 {
                    return Ok(());
                }
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        });
    }

    let accept_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let connect_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let accept_waker = Waker::from(Arc::clone(&accept_wakes));
    let connect_waker = Waker::from(Arc::clone(&connect_wakes));
    let mut accept = pin!(listener.accept());
    let mut connect = pin!(TcpStream::connect(addr, &handle));
    let child_reactor = Reactor::local().expect("child reactor");
    let child_handle = child_reactor.handle();

    let mut child = std::thread::scope(|scope| {
        let spawn = scope.spawn(|| command.spawn(&child_handle));
        if let Err(error) = control.read_exact(&mut [0]) {
            control.shutdown(Shutdown::Write).expect("release child");
            panic!("child did not enter pre_exec: {error}");
        }

        let (release, released) = mpsc::channel();
        let watchdog = scope.spawn(move || {
            let polled_during_spawn = released.recv_timeout(BOUND).is_ok();
            control.shutdown(Shutdown::Write).expect("release child");
            polled_during_spawn
        });
        let accept_pending = accept
            .as_mut()
            .poll(&mut Context::from_waker(&accept_waker))
            .is_pending();
        let connect_pending = connect
            .as_mut()
            .poll(&mut Context::from_waker(&connect_waker))
            .is_pending();
        let _ = release.send(());
        assert!(watchdog.join().expect("watchdog"), "network poll blocked");
        assert!(accept_pending && connect_pending);
        spawn.join().expect("spawn thread").expect("spawn")
    });

    // The network reactor has not been polled since spawn began. Its
    // notification must be pending, and neither task ran on the spawn thread.
    assert_eq!(accept_wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(connect_wakes.0.load(Ordering::Relaxed), 0);
    let deadline = Instant::now() + BOUND;
    while accept_wakes.0.load(Ordering::Relaxed) == 0
        || connect_wakes.0.load(Ordering::Relaxed) == 0
    {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "gate release did not wake both polls");
        reactor
            .poll_once_timeout(Some(left.as_nanos() as u64))
            .expect("reactor notification");
        assert!(Instant::now() < deadline, "gate release exceeded deadline");
    }
    let status = nagoya::reactor::block_on_with(&child_reactor, child.wait()).expect("wait");
    assert!(status.success());
    nagoya::reactor::block_on_with(&reactor, async {
        accept.as_mut().await.expect("accept after spawn");
        connect.as_mut().await.expect("connect after spawn");
    });
}

#[test]
fn the_handles_are_send() {
    fn send<T: Send>() {}
    send::<Child>();
    send::<ChildStdin>();
    send::<ChildStdout>();
    send::<ChildStderr>();
    send::<Command>();
}

#[test]
fn echo_is_read_to_end_of_file() {
    let reactor = reactor();
    let started = Instant::now();
    let (output, status) = nagoya::block_on(async {
        let mut child = Command::new("/bin/echo")
            .arg("hello")
            .stdout(Stdio::piped())
            .spawn(&reactor.handle())
            .expect("spawn echo");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut output = Vec::new();
        stdout.read_to_end(&mut output).await.expect("read");
        (output, child.wait().await.expect("wait"))
    });
    assert_eq!(output, b"hello\n");
    assert!(status.success(), "echo exited {status:?}");
    assert!(started.elapsed() < BOUND, "took {:?}", started.elapsed());
}

#[test]
fn cat_gives_back_what_it_was_given_once_its_input_is_closed() {
    let reactor = reactor();
    let handle = reactor.handle();
    let started = Instant::now();
    let joined = nagoya::block_on(nagoya::spawn(async move {
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn(&handle)
            .expect("spawn cat");
        let mut stdin = child.stdin.take().expect("piped stdin");
        stdin.write_all(b"round trip").await.expect("write");
        // Without this `cat` never sees end of file, so it never exits and
        // its output never ends: the read below would hang.
        stdin.close().await.expect("close");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut output = Vec::new();
        stdout.read_to_end(&mut output).await.expect("read");
        let status = child.wait().await.expect("wait");
        (output, status)
    }));
    let (output, status) = joined.expect("task ran");
    assert_eq!(output, b"round trip");
    assert!(status.success(), "cat exited {status:?}");
    assert!(started.elapsed() < BOUND, "took {:?}", started.elapsed());
}

#[test]
fn a_write_after_close_is_an_error_rather_than_a_hang() {
    let reactor = reactor();
    nagoya::block_on(async {
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn(&reactor.handle())
            .expect("spawn cat");
        let mut stdin = child.stdin.take().expect("piped stdin");
        stdin.close().await.expect("close");
        assert!(stdin.write_all(b"late").await.is_err());
        assert!(child.wait().await.expect("wait").success());
    });
}

#[test]
fn false_reports_failure_under_a_foreign_executor() {
    let reactor = reactor();
    let started = Instant::now();
    let status = on_tokio(Command::new("/usr/bin/false").status(&reactor.handle()));
    let status = status.expect("status");
    assert!(!status.success(), "false succeeded");
    assert_eq!(status.code(), Some(1));
    assert!(started.elapsed() < BOUND, "took {:?}", started.elapsed());
}

#[test]
fn output_collects_both_streams() {
    let reactor = reactor();
    let output = on_tokio(
        Command::new("/bin/sh")
            .arg("-c")
            .arg("echo out; echo err 1>&2; exit 3")
            .output(&reactor.handle()),
    )
    .expect("output");
    assert_eq!(output.stdout, b"out\n");
    assert_eq!(output.stderr, b"err\n");
    assert_eq!(output.status.code(), Some(3));
}

#[test]
fn kill_returns_promptly_and_wait_reports_the_signal() {
    let reactor = reactor();
    let started = Instant::now();
    nagoya::block_on(async {
        let mut child = Command::new("/bin/sleep")
            .arg("30")
            .spawn(&reactor.handle())
            .expect("spawn sleep");
        assert!(child.id().is_some());
        child.kill().await.expect("kill");
        let status = child.wait().await.expect("wait after kill");
        assert_eq!(status.signal(), Some(9), "status {status:?}");
        // Reaped, so the pid is no longer this handle's to name.
        assert_eq!(child.id(), None);
        assert!(child.start_kill().is_err());
    });
    assert!(started.elapsed() < BOUND, "took {:?}", started.elapsed());
}

#[test]
fn a_process_group_is_signalled_as_one() {
    let reactor = reactor();
    let started = Instant::now();
    nagoya::block_on(async {
        // The shell starts a sleep of its own and waits for it. Killing the
        // group has to end both, or the wait below never returns.
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg("/bin/sleep 30; exit 0")
            .process_group(0)
            .spawn(&reactor.handle())
            .expect("spawn sh");
        let group = child.id().expect("running") as libc::pid_t;
        // SAFETY: a negative pid addresses the process group this test made.
        assert_eq!(unsafe { libc::kill(-group, libc::SIGKILL) }, 0);
        let status = child.wait().await.expect("wait after group kill");
        assert_eq!(status.signal(), Some(9), "status {status:?}");
    });
    assert!(started.elapsed() < BOUND, "took {:?}", started.elapsed());
}

#[test]
fn a_wait_dropped_part_way_can_be_waited_again() {
    let reactor = reactor();
    let started = Instant::now();
    let mut child = Command::new("/bin/sleep")
        .arg("0.2")
        .spawn(&reactor.handle())
        .expect("spawn sleep");
    {
        // Polled once, so its waker is parked with the reactor, and then
        // dropped while the child is still running.
        let mut context = Context::from_waker(Waker::noop());
        let mut waiting = pin!(child.wait());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
    }
    assert!(child.try_wait().expect("try_wait").is_none());
    let status = nagoya::block_on(child.wait()).expect("second wait");
    assert!(status.success(), "sleep exited {status:?}");
    // Kept once reaped: a third wait answers without the kernel.
    let again = nagoya::block_on(child.wait()).expect("third wait");
    assert_eq!(again, status);
    assert!(started.elapsed() < BOUND, "took {:?}", started.elapsed());
}

#[test]
fn kill_on_drop_kills_and_reaps() {
    let reactor = reactor();
    let child = Command::new("/bin/sleep")
        .arg("30")
        .kill_on_drop(true)
        .spawn(&reactor.handle())
        .expect("spawn sleep");
    let pid = child.id().expect("running") as libc::pid_t;
    drop(child);

    // The reactor reaps it on its own thread when the exit arrives, so this
    // has to wait for that to happen rather than expect it already has. A
    // pid that `kill(pid, 0)` cannot find is one that was both killed and
    // reaped; a zombie would still be found. The reactor is held until the
    // loop ends, because it is what does the reaping.
    let started = Instant::now();
    loop {
        // SAFETY: signal 0 checks for existence and delivers nothing.
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        if !alive {
            break;
        }
        assert!(
            started.elapsed() < BOUND,
            "pid {pid} still exists after kill_on_drop"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(reactor);
}

#[test]
fn many_children_at_once_all_finish() {
    let reactor = reactor();
    let started = Instant::now();
    let mut handles = Vec::new();
    for index in 0..16 {
        let handle = reactor.handle();
        handles.push(nagoya::spawn(async move {
            let output = Command::new("/bin/echo")
                .arg(index.to_string())
                .output(&handle)
                .await
                .expect("output");
            assert!(output.status.success());
            String::from_utf8(output.stdout).expect("utf-8")
        }));
    }
    for (index, handle) in handles.into_iter().enumerate() {
        let line = nagoya::block_on(handle).expect("task ran");
        assert_eq!(line, format!("{index}\n"));
    }
    assert!(started.elapsed() < BOUND, "took {:?}", started.elapsed());
}

#[test]
fn a_missing_program_is_an_error_from_spawn() {
    let reactor = reactor();
    let error = Command::new("/nonexistent/nagoya-process-test")
        .spawn(&reactor.handle())
        .expect_err("spawned something that does not exist");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
}

/// Polled by hand, so the pending and ready states are both observed rather
/// than inferred from the test finishing.
#[test]
fn wait_is_pending_until_the_exit_and_ready_after() {
    let reactor = reactor();
    let mut child = Command::new("/bin/sleep")
        .arg("0.1")
        .spawn(&reactor.handle())
        .expect("spawn sleep");
    let mut context = Context::from_waker(Waker::noop());
    let mut waiting = pin!(child.wait());
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    let started = Instant::now();
    let status = loop {
        if let Poll::Ready(status) = waiting.as_mut().poll(&mut context) {
            break status.expect("wait");
        }
        assert!(started.elapsed() < BOUND, "never became ready");
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success());
}
