//! The thread local reactor, through the public API.
//!
//! Moved out of the implementation file: these use only what the crate
//! exports, so they are integration tests and belong beside the other ones
//! rather than at the bottom of the module they exercise.

use nagoya::reactor::{block_on_with, Reactor};

use nagoya::reactor::socket::Addr;
use nagoya::reactor::TcpListener;

#[test]
fn drives_a_connection_on_one_thread() {
    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let listener = TcpListener::bind(Addr::localhost(0), &handle).expect("bind");
    let addr = listener.local_addr().expect("addr");

    // The peer is an ordinary blocking socket on another thread, so this
    // test is about the local loop rather than about two of them.
    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let mut stream =
            std::net::TcpStream::connect(std::net::SocketAddr::from(([127, 0, 0, 1], addr.port())))
                .expect("connect");
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).expect("read");
        stream.write_all(&byte).expect("write");
    });

    let echoed = block_on_with(&reactor, async {
        let (mut stream, _) = listener.accept().await.expect("accept");
        stream.write_all(b"x").await.expect("write");
        let mut byte = [0u8; 1];
        let read = stream.read(&mut byte).await.expect("read");
        assert_eq!(read, 1, "short read");
        byte
    });

    peer.join().expect("peer");
    assert_eq!(&echoed, b"x");
}

/// An empty set has nothing to wait for, so it finishes on its first poll.
#[test]
fn an_empty_task_set_is_already_done() {
    use nagoya::reactor::TaskSet;

    let tasks = TaskSet::new();
    assert!(tasks.is_empty(), "a set with no tasks should be empty");
    assert_eq!(tasks.len(), 0);
    nagoya::block_on(tasks);
}

/// Tasks make progress independently rather than in lockstep.
///
/// Each task yields a different number of times and records itself when it is
/// done, so the finishing order is the observable evidence: if the set polled
/// everything to completion one at a time, or advanced them in rounds gated on
/// the slowest, the order would follow the order they were pushed instead of
/// the amount of work each one has.
#[test]
fn tasks_in_a_set_run_at_their_own_pace() {
    use alloc_free::{Rc, RefCell};
    use nagoya::reactor::TaskSet;

    mod alloc_free {
        pub use core::cell::RefCell;
        pub use std::rc::Rc;
    }

    let order = Rc::new(RefCell::new(Vec::new()));
    let mut tasks = TaskSet::new();
    for (label, yields) in [("slow", 8), ("fast", 1), ("middling", 4)] {
        let order = Rc::clone(&order);
        tasks.push(async move {
            for _ in 0..yields {
                nagoya::yield_now().await;
            }
            order.borrow_mut().push(label);
        });
    }
    assert_eq!(tasks.len(), 3, "three tasks pushed");

    nagoya::block_on(tasks);

    assert_eq!(
        order.borrow().as_slice(),
        ["fast", "middling", "slow"],
        "finishing order should follow how much work each task had"
    );
}

/// A set is still usable once everything in it has finished.
#[test]
fn a_drained_set_takes_more_work() {
    use nagoya::reactor::TaskSet;

    let done: std::rc::Rc<core::cell::RefCell<Vec<&'static str>>> = Default::default();

    let mut tasks = TaskSet::new();
    {
        let done = std::rc::Rc::clone(&done);
        tasks.push(async move {
            nagoya::yield_now().await;
            done.borrow_mut().push("first");
        });
    }
    nagoya::block_on(&mut tasks);
    assert_eq!(done.borrow().as_slice(), ["first"]);
    assert!(tasks.is_empty(), "the set should have drained");

    let done_again = std::rc::Rc::clone(&done);
    tasks.push(async move {
        done_again.borrow_mut().push("second");
    });
    assert_eq!(tasks.len(), 1, "a drained set still accepts work");
    nagoya::block_on(&mut tasks);
    assert_eq!(done.borrow().as_slice(), ["first", "second"]);
}

/// Waking one task does not poll the others.
///
/// This is the property the whole design is for. A set that kept a readiness
/// flag per task and checked them all would pass every other test here and
/// fail this one, because it would walk the idle task on every pass looking
/// for something to do.
#[test]
fn an_idle_task_is_not_polled_when_another_is_woken() {
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use nagoya::reactor::TaskSet;

    /// Counts how many times it was polled and never finishes on its own.
    struct Idle(std::rc::Rc<core::cell::Cell<usize>>);

    impl Future for Idle {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            self.0.set(self.0.get() + 1);
            // No waker kept anywhere, so nothing will ever wake this: the only
            // polls it can get are ones the set handed out uninvited.
            Poll::Pending
        }
    }

    let polls = std::rc::Rc::new(core::cell::Cell::new(0));
    let mut tasks = TaskSet::new();
    tasks.push(Idle(std::rc::Rc::clone(&polls)));
    // Yields repeatedly, so the set is polled many times over while the idle
    // task beside it has nothing to say.
    tasks.push(async {
        for _ in 0..64 {
            nagoya::yield_now().await;
        }
    });

    // The set never completes, because the idle task never does, so it is
    // driven by hand for a fixed number of passes rather than awaited.
    nagoya::block_on(async move {
        let mut tasks = core::pin::pin!(tasks);
        for _ in 0..200 {
            core::future::poll_fn(|context| {
                let _ = tasks.as_mut().poll(context);
                Poll::Ready(())
            })
            .await;
            nagoya::yield_now().await;
        }
    });

    assert_eq!(
        polls.get(),
        1,
        "the idle task should have been polled once, when it was added, and never again"
    );
}

/// Several connections echo at once on one thread, each its own task.
#[test]
fn a_task_set_drives_many_connections() {
    use nagoya::io::Stream as _;
    use nagoya::reactor::TaskSet;

    const CONNECTIONS: u8 = 4;

    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let listener = TcpListener::bind(Addr::localhost(0), &handle).expect("bind");
    let addr = listener.local_addr().expect("addr");

    let peers = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let mut streams: Vec<_> = (0..CONNECTIONS)
            .map(|_| {
                std::net::TcpStream::connect(std::net::SocketAddr::from((
                    [127, 0, 0, 1],
                    addr.port(),
                )))
                .expect("connect")
            })
            .collect();
        // Answered in reverse, so the first task to be pushed is the last one
        // that can finish. A set that insisted on order would deadlock here.
        for stream in streams.iter_mut().rev() {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).expect("read");
            stream.write_all(&[byte[0] + 1]).expect("write");
        }
    });

    let echoed: std::rc::Rc<core::cell::RefCell<Vec<u8>>> = Default::default();
    let accepted = block_on_with(&reactor, async {
        let mut streams = Vec::new();
        for _ in 0..CONNECTIONS {
            let (stream, _) = listener.accept().await.expect("accept");
            streams.push(stream);
        }
        streams
    });

    let mut tasks = TaskSet::new();
    for (index, mut stream) in accepted.into_iter().enumerate() {
        let echoed = std::rc::Rc::clone(&echoed);
        tasks.push(async move {
            let sent = index as u8;
            stream.write_all(&[sent]).await.expect("write");
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await.expect("read");
            echoed.borrow_mut().push(byte[0]);
        });
    }
    block_on_with(&reactor, tasks);
    peers.join().expect("peers");

    let mut got = echoed.borrow().clone();
    got.sort_unstable();
    assert_eq!(
        got,
        (0..CONNECTIONS).map(|index| index + 1).collect::<Vec<_>>(),
        "every connection should have been echoed"
    );
}

/// A wake that arrives after its task finished does not strand the others.
///
/// Whatever a task was waiting on may outlive it: a timer that fires late, a
/// channel whose sender has not noticed, another thread mid wake. That waker
/// still puts the finished task's place onto the set's ready chain, and the
/// set has to be able to read the link out of it and carry on to whatever is
/// queued behind it. Losing the rest of the chain there is a hang, not a
/// wasted poll, so it is worth a test of its own.
#[test]
fn a_wake_after_a_task_finished_does_not_strand_the_rest() {
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use nagoya::reactor::TaskSet;

    /// Hands its waker out on the first poll and finishes immediately.
    struct Leaks(std::sync::Arc<std::sync::Mutex<Option<core::task::Waker>>>);

    impl Future for Leaks {
        type Output = ();
        fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
            *self.0.lock().expect("lock") = Some(context.waker().clone());
            Poll::Ready(())
        }
    }

    let leaked: std::sync::Arc<std::sync::Mutex<Option<core::task::Waker>>> = Default::default();
    let ran = std::rc::Rc::new(core::cell::Cell::new(false));

    let mut tasks = TaskSet::new();
    // Finishes first, and leaves its waker behind.
    tasks.push(Leaks(std::sync::Arc::clone(&leaked)));
    // Has to still run after the stale wake lands in front of it.
    {
        let ran = std::rc::Rc::clone(&ran);
        tasks.push(async move {
            for _ in 0..4 {
                nagoya::yield_now().await;
            }
            ran.set(true);
        });
    }

    nagoya::block_on(async move {
        let mut tasks = core::pin::pin!(tasks);
        for pass in 0..64 {
            // From the second pass on, the finished task's waker is fired
            // every time, so a dead index is on the chain ahead of the live
            // task for the whole run rather than once by luck.
            if pass > 0 {
                if let Some(waker) = leaked.lock().expect("lock").as_ref() {
                    waker.wake_by_ref();
                }
            }
            core::future::poll_fn(|context| {
                let _ = tasks.as_mut().poll(context);
                Poll::Ready(())
            })
            .await;
            nagoya::yield_now().await;
        }
    });

    assert!(
        ran.get(),
        "a task queued behind a finished one should still be polled"
    );
}
