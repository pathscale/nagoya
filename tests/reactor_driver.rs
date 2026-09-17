//! The reactor driver, through the public API.
//!
//! All of it lives here: the driver's tests needed nothing private except a
//! reader for the cached timer deadline, which is now `Reactor::next_deadline`
//! because whether arming a timer moves it, and whether socket traffic leaves
//! it alone, is behaviour rather than an implementation detail.

mod support;

use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::Waker;
use nagoya::reactor::{Interest, Reactor};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;
use support::{socket_pair, write_byte};

/// A waker that runs a closure. Local to this file: the crate does not export
/// one, and a test that needs to observe a wake needs exactly this much.
fn waker_fn<F: Fn() + Send + Sync + 'static>(f: F) -> Waker {
    struct Fun<F>(F);
    impl<F: Fn() + Send + Sync + 'static> std::task::Wake for Fun<F> {
        fn wake(self: Arc<Self>) {
            (self.0)();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            (self.0)();
        }
    }
    Waker::from(Arc::new(Fun(f)))
}

#[test]
fn a_sharded_reactor_serves_every_shard() {
    let sharded = Reactor::sharded(4).expect("sharded");
    assert_eq!(sharded.shards(), 4);

    let mut threads = Vec::with_capacity(8);
    for _ in 0..8 {
        let handle = sharded.handle();
        threads.push(std::thread::spawn(move || {
            let (a, b) = socket_pair();
            let registration = handle
                .register(b.as_raw_fd(), Interest::READABLE)
                .expect("register");
            let flag = Arc::new(AtomicBool::new(false));
            let waker = {
                let flag = Arc::clone(&flag);
                waker_fn(move || flag.store(true, Ordering::Release))
            };
            registration.poll_readable(&waker);
            write_byte(&a);
            // Spin briefly rather than park: this is about whether the
            // shard delivers at all, not how fast.
            for _ in 0..10_000 {
                if flag.load(Ordering::Acquire) {
                    break;
                }
                std::thread::yield_now();
            }
            flag.load(Ordering::Acquire)
        }));
    }

    // Bounded by the spin above rather than by a timeout here: each thread
    // gives up after ten thousand yields and reports what it saw, so a
    // shard that never delivers is a false rather than a hang.
    for thread in threads {
        let woke = thread.join().expect("a shard thread panicked");
        assert!(woke, "a descriptor on one shard never woke");
    }
}

/// A waker that calls back into `poll_once` must not deadlock.
///
/// `dispatch` invokes wakers with no reactor lock held, precisely because
/// a waker runs arbitrary code. `poll_once` holds scratch across
/// `dispatch`, so a waker that re-enters it locks a non reentrant mutex
/// this thread already owns and blocks forever.
///
/// The whole test runs on a spawned thread so a regression fails the run
/// rather than hanging it: the deadlock is on one thread, so the main
/// thread stays alive to time it out and say what happened.

#[test]
fn a_waker_may_reenter_poll_once() {
    let worker = std::thread::spawn(move || {
        let reactor = Reactor::local().expect("reactor");
        let handle = reactor.handle();
        let (a, b) = socket_pair();

        let registration = handle
            .register(b.as_raw_fd(), Interest::READABLE)
            .expect("register");

        // The waker re-enters the reactor, which is what a future doing
        // any further I/O from its own poll would cause.
        let reactor = Arc::new(reactor);
        let inner = Arc::clone(&reactor);
        let waker = waker_fn(move || {
            // One nested call is enough: if scratch is held across
            // dispatch this never returns.
            //
            // `poll_once_timeout(0)` rather than `poll_once`: the nested
            // call has no event waiting for it, and a blocking wait would
            // park in the kernel forever for reasons that have nothing to
            // do with the lock this test is about.
            let _ = inner.poll_once_timeout(Some(0));
        });
        registration.poll_readable(&waker);

        write_byte(&a);
        reactor.poll_once().expect("poll");
    });

    // Poll the handle rather than joining it: join would wait out the
    // deadlock this test exists to catch, turning a failure into a hang.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !worker.is_finished() {
        assert!(
            std::time::Instant::now() < deadline,
            "poll_once deadlocked when a waker re-entered it"
        );
        std::thread::yield_now();
    }
    worker.join().expect("worker thread");
}

#[test]
fn an_armed_timer_lowers_the_cached_deadline_and_a_later_one_does_not() {
    // The cache is what keeps timer servicing off the socket path, so its
    // update rule is worth pinning down: sooner replaces, later is ignored.
    let reactor = Reactor::start().expect("reactor");
    let handle = reactor.handle();

    let now = nagoya::now_ns();
    let soon = now + 60_000_000_000;
    let later = soon + 60_000_000_000;

    handle.timer_armed(later).expect("arm");
    // Not asserted directly against the atomic from outside, because that
    // is the thread's to own; arming a sooner one must still take effect.
    handle.timer_armed(soon).expect("arm");

    // Arming something further out than what is cached must be a no-op
    // rather than pushing the deadline back.
    handle.timer_armed(later).expect("arm");

    // Nothing should have fired: both deadlines are a minute away.
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        reactor.next_deadline() <= soon,
        "a later timer pushed the deadline back"
    );
}

#[test]
fn socket_events_do_not_disturb_the_timer_deadline() {
    // The point of the cache: a flood of readiness must not drag the timer
    // wheel along with it. If the loop consulted the wheel on every wakeup
    // this deadline would be recomputed and the assertion would fail.
    let reactor = Reactor::start().expect("reactor");
    let handle = reactor.handle();

    let far = nagoya::now_ns() + 3_600_000_000_000;
    handle.timer_armed(far).expect("arm");

    let (a, b) = socket_pair();
    let registration = handle
        .register(a.as_raw_fd(), Interest::READABLE)
        .expect("register");

    // Generate real readiness events repeatedly.
    for _ in 0..50 {
        let waker = waker_fn(|| {});
        registration.poll_readable(&waker);
        write_byte(&b);
        let mut drain = [0u8; 8];
        // SAFETY: reading into a live local buffer from a valid descriptor.
        #[allow(unsafe_code)]
        unsafe {
            libc::read(a.as_raw_fd(), drain.as_mut_ptr().cast::<libc::c_void>(), 8);
        }
    }
    std::thread::sleep(Duration::from_millis(100));

    assert_eq!(
        reactor.next_deadline(),
        far,
        "socket traffic moved the timer deadline"
    );
}

#[test]
fn wakes_a_reader_when_data_arrives() {
    // A local reactor, driven by this thread: `poll_once_timeout` blocks
    // in the kernel until an event arrives and then returns, so readiness
    // is what the call hands back rather than something read out of a
    // waker afterwards. No flag, no channel, no spinning.
    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let (a, b) = socket_pair();

    let registration = handle
        .register(a.as_raw_fd(), Interest::READABLE)
        .expect("register");

    let waker = waker_fn(|| {});

    // A fresh registration starts readable and says so once. The poller is
    // edge triggered, so it cannot report state, only transitions, and any
    // data that arrived before the descriptor was added produced an edge that
    // is already gone. Answering "maybe" and letting the caller find out with
    // one `recv` is the only answer that cannot lose a wake.
    assert!(
        registration.take_readable_or_park(&waker),
        "a new registration must assume it may be readable"
    );

    // Taken, so the next question parks: nothing has arrived since.
    assert!(
        !registration.take_readable_or_park(&waker),
        "readable twice with no edge in between"
    );

    write_byte(&b);
    reactor.poll_once().expect("poll");

    assert!(
        registration.take_readable_or_park(&waker),
        "reader was never woken"
    );
}

#[test]
fn a_dropped_registration_stops_waking() {
    let reactor = Reactor::local().expect("reactor");
    let handle = reactor.handle();
    let (a, b) = socket_pair();

    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&wakes);
    let waker = waker_fn(move || {
        counter.fetch_add(1, Ordering::Release);
    });
    let descriptor = a.as_raw_fd();
    {
        let registration = handle
            .register(descriptor, Interest::READABLE)
            .expect("register");
        // Twice: the first takes the readiness a new registration starts
        // with, the second parks the waker this test is about.
        let _ = registration.take_readable_or_park(&waker);
        let _ = registration.take_readable_or_park(&waker);
    }

    write_byte(&b);
    // Bounded so a reactor with nothing to deliver returns rather than
    // waiting in the kernel for an event that is not coming.
    reactor.poll_once_timeout(Some(0)).expect("poll");

    // The dropped registration's waker must not have been called. A fresh
    // registration of the same descriptor starts readable, as every
    // registration does, so readiness is not what distinguishes them: the
    // waker is. This one was never woken, and the count says so.
    assert_eq!(
        wakes.load(Ordering::Acquire),
        0,
        "a dropped registration still woke its task"
    );
}

#[test]
fn shuts_down_promptly_while_blocked() {
    let reactor = Reactor::start().expect("reactor");
    // The thread is now blocked in `wait` with no timers and no
    // descriptors, so this only returns if `wake` breaks it out.
    let start = std::time::Instant::now();
    reactor.shutdown();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "shutdown did not interrupt a blocked reactor"
    );
}
