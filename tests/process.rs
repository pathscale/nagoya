//! Child processes, through the public API, under three executors.
//!
//! The executor is the variable that matters here. A child's pipes and its
//! exit are registered on a reactor the module starts for itself, so the
//! claim being tested is that the futures finish whoever polls them:
//! `nagoya::block_on`, a task on `nagoya::spawn`, and tokio's current thread
//! runtime, which knows nothing about nagoya's reactor at all.
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

/// How long any one of these may take before it counts as hung.
///
/// Generous against the milliseconds each needs, and well inside the ten
/// seconds a test is allowed, so a failure here is a hang rather than a slow
/// machine.
const BOUND: Duration = Duration::from_secs(5);

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
    let started = Instant::now();
    let (output, status) = nagoya::block_on(async {
        let mut child = Command::new("/bin/echo")
            .arg("hello")
            .stdout(Stdio::piped())
            .spawn()
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
    let started = Instant::now();
    let joined = nagoya::block_on(nagoya::spawn(async {
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
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
    nagoya::block_on(async {
        let mut child = Command::new("/bin/cat")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn cat");
        let mut stdin = child.stdin.take().expect("piped stdin");
        stdin.close().await.expect("close");
        assert!(stdin.write_all(b"late").await.is_err());
        assert!(child.wait().await.expect("wait").success());
    });
}

#[test]
fn false_reports_failure_under_a_foreign_executor() {
    let started = Instant::now();
    let status = on_tokio(Command::new("/usr/bin/false").status());
    let status = status.expect("status");
    assert!(!status.success(), "false succeeded");
    assert_eq!(status.code(), Some(1));
    assert!(started.elapsed() < BOUND, "took {:?}", started.elapsed());
}

#[test]
fn output_collects_both_streams() {
    let output = on_tokio(
        Command::new("/bin/sh")
            .arg("-c")
            .arg("echo out; echo err 1>&2; exit 3")
            .output(),
    )
    .expect("output");
    assert_eq!(output.stdout, b"out\n");
    assert_eq!(output.stderr, b"err\n");
    assert_eq!(output.status.code(), Some(3));
}

#[test]
fn kill_returns_promptly_and_wait_reports_the_signal() {
    let started = Instant::now();
    nagoya::block_on(async {
        let mut child = Command::new("/bin/sleep")
            .arg("30")
            .spawn()
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
fn a_wait_dropped_part_way_can_be_waited_again() {
    let started = Instant::now();
    let mut child = Command::new("/bin/sleep")
        .arg("0.2")
        .spawn()
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
    let child = Command::new("/bin/sleep")
        .arg("30")
        .kill_on_drop(true)
        .spawn()
        .expect("spawn sleep");
    let pid = child.id().expect("running") as libc::pid_t;
    drop(child);

    // The reactor reaps it on its own thread when the exit arrives, so this
    // has to wait for that to happen rather than expect it already has. A
    // pid that `kill(pid, 0)` cannot find is one that was both killed and
    // reaped; a zombie would still be found.
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
}

#[test]
fn many_children_at_once_all_finish() {
    let started = Instant::now();
    let mut handles = Vec::new();
    for index in 0..16 {
        handles.push(nagoya::spawn(async move {
            let output = Command::new("/bin/echo")
                .arg(index.to_string())
                .output()
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
    let error = Command::new("/nonexistent/nagoya-process-test")
        .spawn()
        .expect_err("spawned something that does not exist");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
}

/// Polled by hand, so the pending and ready states are both observed rather
/// than inferred from the test finishing.
#[test]
fn wait_is_pending_until_the_exit_and_ready_after() {
    let mut child = Command::new("/bin/sleep")
        .arg("0.1")
        .spawn()
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
