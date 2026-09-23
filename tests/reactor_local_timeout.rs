//! A `timeout` expiring under a local reactor that has no I/O to wake it.
//!
//! Timers are fired by whichever gets there first: the timer thread, or the
//! local reactor servicing the heap just before it blocks. The second used to
//! hang. The wake it raised landed on its own thread inside `poll_once`, where
//! the waker skips the syscall, and the reactor then blocked in the kernel with
//! no deadline left and nothing to interrupt it.
//!
//! Which of the two wins is a race in real time, so the clock here is one the
//! test controls: it moves only on the reactor's thread. The timer thread sees
//! a deadline that never arrives and cannot be the one to fire it, so the
//! reactor always is, and a regression hangs every run rather than some.
//!
//! Its own binary because only the first `set_clock` in a process counts.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::cell::Cell;
use std::sync::mpsc;
use std::time::Duration;

use nagoya::reactor::{block_on_with, Reactor};
use nagoya::{now_ns, set_clock, timeout, Elapsed};

/// Where the clock starts. Any non-zero reading will do.
const START: u64 = 1_000_000_000;
/// How far the reactor's thread jumps: well past any deadline armed here.
const JUMP: u64 = 1_000_000_000;

thread_local! {
    /// Set on the reactor's thread once its timer is armed.
    static JUMPED: Cell<bool> = const { Cell::new(false) };
}

fn clock() -> u64 {
    if JUMPED.with(Cell::get) {
        START + JUMP
    } else {
        START
    }
}

/// Polls `inner`, and once it is pending, and so has armed its timer, moves
/// this thread's clock past the deadline.
struct JumpOnceArmed<F> {
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for JumpOnceArmed<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let out = self.inner.as_mut().poll(cx);
        if out.is_pending() {
            JUMPED.with(|jumped| jumped.set(true));
        }
        out
    }
}

#[test]
fn a_timeout_the_local_reactor_fires_itself_is_not_slept_through() {
    set_clock(clock);
    assert_eq!(now_ns(), START, "another clock was installed first");

    // On a thread of its own so that a hang is a failed receive here, in
    // seconds, rather than a test that never ends.
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let reactor = Reactor::local().expect("reactor");
        let never = core::future::pending::<()>();
        let future = JumpOnceArmed {
            inner: Box::pin(timeout(Duration::from_millis(20), never)),
        };
        let out = block_on_with(&reactor, future);
        let _ = send.send(out);
    });

    let out = receive
        .recv_timeout(Duration::from_secs(5))
        .expect("block_on_with did not return: the local reactor slept through its own timeout");
    assert_eq!(out, Err(Elapsed));
}
