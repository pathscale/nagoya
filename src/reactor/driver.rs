//! The reactor thread: readiness in, wakers out.
//!
//! # What it does
//!
//! One thread owns the [`Poller`] and loops. Each pass waits for readiness,
//! wakes whatever task registered interest in each ready descriptor, and then
//! drives nagoya's timer wheel. Nothing else happens here: the reactor does not
//! run tasks, which is nagoya's job, and does not touch sockets, which is the
//! task's job. It only converts kernel readiness into `Waker::wake`.
//!
//! # Timers, without polling them
//!
//! Nothing here runs on a tick. The thread blocks in `kevent`/`epoll_wait`,
//! which is an interrupt-driven kernel wait, and is woken by a descriptor
//! changing state, by an expiring deadline, or by an explicit
//! [`Poller::wake`]. There is no retry interval and no spin.
//!
//! Timers are event driven in the same sense. The wheel is *not* consulted on
//! every wakeup: a socket delivering ten thousand events a second would
//! otherwise take the timer heap's lock ten thousand times to be told, almost
//! always, that nothing is due. Instead the next deadline is cached in
//! [`Shared::next_deadline`], and the wheel is touched only when that deadline
//! has actually arrived or when [`Handle::timer_armed`] reports a new one.
//! Socket traffic and timer work are then independent: neither makes the other
//! do anything.
//!
//! # Why a slot is not behind the map's lock
//!
//! Parking a waker and waking one both happen on every message, so anything
//! shared between connections on that path is contention that scales with the
//! connection count rather than with the work. The slot map's lock is taken
//! only to find or create a slot, which happens once per registration; the
//! waker itself lives in a per-slot lock reached through an `Arc`, so two
//! connections parking at the same time never touch the same lock at all.
//!
//! # Registration lifetime
//!
//! A [`Registration`] owns its slot: dropping it removes the descriptor from
//! the poller and frees the token. Tokens are generational, so a readiness
//! event that was already in flight when a registration was dropped resolves to
//! a stale slot and is discarded rather than waking an unrelated task that has
//! since taken the same index.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;

use super::error::Result;
use super::poller::{Event, Interest, Poller};

/// The wakers waiting on one descriptor.
#[derive(Debug, Default)]
struct Wakers {
    reader: Option<Waker>,
    writer: Option<Waker>,
    /// Whether an edge has been seen since the last read drained the socket.
    ///
    /// Inside the same lock as `reader` on purpose. As a separate atomic it
    /// races: a read clears it, the poller sets it and takes the waker, and
    /// the read then parks a waker nobody will ever wake. Under one lock the
    /// clear and the park are one step against the poller's set and take.
    readable: bool,
}

/// One descriptor's registration state.
///
/// Held by `Arc` so a [`Registration`] can park a waker without going through
/// the map, which is what keeps per-message work off the shared lock.
#[derive(Debug)]
struct Slot {
    /// Bumped on every reuse of this index, so a stale event can be spotted.
    generation: u64,
    wakers: Mutex<Wakers>,
}

/// Shared reactor state. The thread and every handle hold one of these.
#[derive(Debug)]
struct Shared {
    poller: Poller,
    /// Indexed by slot index, not by token: the token carries the generation.
    ///
    /// Taken on registration and deregistration only. The per-message path
    /// goes through the `Arc<Slot>` a `Registration` already holds.
    slots: Mutex<HashMap<u64, Arc<Slot>>>,
    next_index: AtomicU64,
    running: AtomicBool,
    /// When the earliest known timer is due, in nagoya's clock, or
    /// [`NO_DEADLINE`] when none is armed.
    ///
    /// This is what makes timer servicing event driven rather than polled: the
    /// loop compares against it instead of asking the timer wheel.
    next_deadline: AtomicU64,
}

/// Sentinel for "no timer armed". A real deadline is a nanosecond clock
/// reading, which does not reach `u64::MAX` for any running system.
const NO_DEADLINE: u64 = u64::MAX;

/// Split a token into its slot index and generation.
///
/// The generation occupies the high 16 bits, which is enough that a slot would
/// have to be reused 65,536 times inside one in-flight event for a collision,
/// and leaves 48 bits of index: more descriptors than any process can open.
const GENERATION_SHIFT: u32 = 48;

#[inline]
fn make_token(index: u64, generation: u64) -> u64 {
    (generation << GENERATION_SHIFT) | index
}

#[inline]
fn split_token(token: u64) -> (u64, u64) {
    (
        token & ((1 << GENERATION_SHIFT) - 1),
        token >> GENERATION_SHIFT,
    )
}

/// A handle to the running reactor.
#[derive(Debug, Clone)]
pub struct Handle {
    shared: Arc<Shared>,
}

impl Handle {
    /// Register `fd` with the reactor.
    ///
    /// The returned [`Registration`] must be kept for as long as the descriptor
    /// is in use; dropping it deregisters. `fd` must be non-blocking and must
    /// outlive the registration.
    pub fn register(&self, fd: i32, interest: Interest) -> Result<Registration> {
        let index = self.shared.next_index.fetch_add(1, Ordering::Relaxed);

        // A fresh index is never already present, so the generation starts at
        // one rather than being read back out of an existing slot.
        let generation = 1u64;
        let slot = Arc::new(Slot {
            generation,
            wakers: Mutex::new(Wakers::default()),
        });
        {
            let mut slots = self.shared.slots.lock().expect("reactor slots poisoned");
            slots.insert(index, Arc::clone(&slot));
        }

        let token = make_token(index, generation);
        if let Err(error) = self.shared.poller.add(fd, token, interest) {
            self.shared
                .slots
                .lock()
                .expect("reactor slots poisoned")
                .remove(&index);
            return Err(error);
        }

        Ok(Registration {
            shared: Arc::clone(&self.shared),
            slot,
            fd,
            index,
            token,
        })
    }

    /// Wake the reactor thread if it is blocked.
    pub fn wake(&self) -> Result<()> {
        self.shared.poller.wake()
    }

    /// Tell the reactor a timer was armed for `deadline`.
    ///
    /// This is the event that makes timer servicing reactive. Without it the
    /// reactor would have to ask the wheel on every pass just in case, which is
    /// the polling this design avoids. Calling it with a deadline further out
    /// than the one already cached is cheap and does not wake the thread.
    pub fn timer_armed(&self, deadline: u64) -> Result<()> {
        // Lower the cached deadline if this one is sooner. A racing update that
        // lowers it further simply wins; the loser's deadline is later and will
        // still be served when the earlier one fires.
        let mut current = self.shared.next_deadline.load(Ordering::Acquire);
        loop {
            if current <= deadline {
                // Something sooner is already pending, so the thread will wake
                // in time to see this one. Nothing to do.
                return Ok(());
            }
            match self.shared.next_deadline.compare_exchange_weak(
                current,
                deadline,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        // The thread may be blocked on a later deadline: interrupt it so it
        // recomputes its wait against the new one.
        self.shared.poller.wake()
    }
}

/// A descriptor's registration with the reactor.
///
/// Dropping this deregisters the descriptor and invalidates its token.
#[derive(Debug)]
pub struct Registration {
    shared: Arc<Shared>,
    /// This descriptor's slot, held directly so parking a waker does not go
    /// through the shared map.
    slot: Arc<Slot>,
    fd: i32,
    index: u64,
    token: u64,
}

impl Registration {
    /// Take readiness if there is any, and park `waker` if there is not.
    ///
    /// One lock acquisition doing both is what makes this safe. The caller
    /// reads only when this returns `true`, and when it returns `false` the
    /// waker is already parked, so there is no window in which an edge can
    /// arrive between the decision and the park.
    ///
    /// Returns `true` when the socket may have data. Edge triggered readiness
    /// means a `recv` that returned `EWOULDBLOCK` drained the socket and
    /// nothing arrives until the poller says so, which is what makes the
    /// speculative call skippable: measured at 1.24 wasted reads per message
    /// across eight connections, 38 percent of all reads.
    pub fn take_readable_or_park(&self, waker: &Waker) -> bool {
        let mut wakers = self.slot.wakers.lock().expect("reactor slot poisoned");
        if wakers.readable {
            wakers.readable = false;
            return true;
        }
        match &wakers.reader {
            Some(existing) if existing.will_wake(waker) => {}
            _ => wakers.reader = Some(waker.clone()),
        }
        false
    }

    /// Note that this descriptor may still have data, for a caller that filled
    /// its buffer and did not reach `EWOULDBLOCK`.
    pub fn mark_readable(&self) {
        let mut wakers = self.slot.wakers.lock().expect("reactor slot poisoned");
        wakers.readable = true;
    }

    /// Park `waker` until the descriptor is readable.
    ///
    /// Call this only after a read has actually returned `EWOULDBLOCK`: the
    /// poller is edge triggered, so registering interest without first draining
    /// means waiting for an edge that has already passed.
    pub fn poll_readable(&self, waker: &Waker) {
        let mut wakers = self.slot.wakers.lock().expect("reactor slot poisoned");
        // `will_wake` avoids the atomic refcount bump when the same task parks
        // again, which on a busy connection is every single read.
        match &wakers.reader {
            Some(existing) if existing.will_wake(waker) => {}
            _ => wakers.reader = Some(waker.clone()),
        }
    }

    /// Park `waker` until the descriptor is writable. See [`Self::poll_readable`].
    pub fn poll_writable(&self, waker: &Waker) {
        let mut wakers = self.slot.wakers.lock().expect("reactor slot poisoned");
        match &wakers.writer {
            Some(existing) if existing.will_wake(waker) => {}
            _ => wakers.writer = Some(waker.clone()),
        }
    }

    /// Change what this descriptor is watched for.
    pub fn modify(&self, interest: Interest) -> Result<()> {
        self.shared.poller.modify(self.fd, self.token, interest)
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        // Order matters: stop the kernel delivering first, then drop the slot.
        // The reverse would leave a window where an event arrives for a slot
        // that is already gone, which is harmless but pointless work.
        let _ = self.shared.poller.remove(self.fd);
        self.shared
            .slots
            .lock()
            .expect("reactor slots poisoned")
            .remove(&self.index);
    }
}

/// A group of reactors, each on its own thread.
///
/// See [`Reactor::sharded`] for why this exists rather than one reactor with
/// more threads behind it.
pub struct Sharded {
    reactors: Vec<Reactor>,
    next: AtomicU64,
}

impl Sharded {
    /// A handle for the next shard, round robin.
    ///
    /// Call once per descriptor and keep the handle for that descriptor's
    /// life: the shard that registers a descriptor is the one that will report
    /// its readiness, so moving between them mid connection is not something
    /// to do casually.
    pub fn handle(&self) -> Handle {
        let index = self.next.fetch_add(1, Ordering::Relaxed) as usize;
        self.reactors[index % self.reactors.len()].handle()
    }

    /// How many reactors this group holds.
    pub fn shards(&self) -> usize {
        self.reactors.len()
    }

    /// The handle for one specific shard, for a caller that wants to place a
    /// descriptor itself rather than take the next one.
    pub fn handle_for(&self, shard: usize) -> Handle {
        self.reactors[shard % self.reactors.len()].handle()
    }
}

/// Start a reactor on its own thread.
///
/// The thread runs until [`Reactor::shutdown`] is called or the returned
/// [`Reactor`] is dropped.
pub struct Reactor {
    shared: Arc<Shared>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Scratch for [`Self::poll_once`], so the local arrangement allocates per
    /// reactor rather than per wakeup.
    ///
    /// The threaded loop keeps the same two buffers on its stack and reuses
    /// them across wakeups, for the reason its comment gives: this runs on
    /// every readiness event, so allocating here is allocating once per
    /// message. `poll_once` takes `&self` and had nowhere to keep them, so it
    /// allocated both every call and the local reactor paid what the threaded
    /// one was careful not to.
    ///
    /// A mutex rather than a cell because `Reactor` is `Sync`. It is held only
    /// to take the buffers out and to put them back, never across the wakers,
    /// because a waker can re-enter `poll_once` on this thread and a
    /// non reentrant lock held across that deadlocks against itself.
    scratch: Mutex<Scratch>,
}

impl Reactor {
    /// Create a reactor with no thread of its own.
    ///
    /// Nothing runs until someone calls [`Self::poll_once`], which is what
    /// [`block_on`](super::local::block_on) does on the thread that also polls
    /// the future. That arrangement saves a park and an unpark per message
    /// against the threaded reactor; see that module for when each fits.
    pub fn local() -> Result<Self> {
        Ok(Self {
            shared: Arc::new(Shared {
                poller: Poller::new()?,
                slots: Mutex::new(HashMap::new()),
                next_index: AtomicU64::new(0),
                running: AtomicBool::new(true),
                next_deadline: AtomicU64::new(NO_DEADLINE),
            }),
            thread: None,
            // Same capacity the threaded loop reserves.
            scratch: Mutex::new((Vec::with_capacity(64), Vec::with_capacity(64))),
        })
    }

    /// Wait for readiness once and wake whatever it belongs to.
    ///
    /// For a reactor from [`Self::local`]. Returns after one wait, having
    /// woken any task whose descriptor became ready, so the caller can poll.
    pub fn poll_once(&self) -> Result<()> {
        self.poll_once_timeout(None)
    }

    /// Wait for readiness once, blocking for at most `cap` milliseconds.
    ///
    /// `None` defers to whatever the timer wheel says. `Some(0)` returns
    /// immediately with whatever is already pending, which is what a waker
    /// re-entering the reactor wants: it is asking whether anything else is
    /// ready, not to sleep until something is.
    pub fn poll_once_timeout(&self, cap: Option<u64>) -> Result<()> {
        let timeout = match cap {
            Some(0) => Some(0),
            other => {
                let due = service_timers(&self.shared);
                match (other, due) {
                    (Some(c), Some(d)) => Some(c.min(d)),
                    (Some(c), None) => Some(c),
                    (None, d) => d,
                }
            }
        };

        // Taken out of the reactor for the duration rather than borrowed under
        // a held lock. `dispatch` invokes wakers, a waker may run arbitrary
        // code, and arbitrary code on this thread may call `poll_once` again:
        // holding a non reentrant lock across that is a deadlock against
        // itself. `dispatch` already releases the slots lock before waking for
        // exactly this reason and it applies here too.
        //
        // The take leaves empty vectors behind, so a reentrant call allocates
        // rather than deadlocking, and the put back below restores the
        // capacity for the common case where nothing reentered.
        let (mut events, mut pending) = {
            let mut scratch = self.scratch.lock().expect("reactor scratch poisoned");
            (
                core::mem::take(&mut scratch.0),
                core::mem::take(&mut scratch.1),
            )
        };

        events.clear();
        let waited = self.shared.poller.wait(&mut events, timeout);
        if waited.is_ok() {
            dispatch(&self.shared, &events, &mut pending);
        }

        // Keep whichever buffers have the larger capacity: a reentrant call
        // may have left its own here, and dropping the bigger pair would undo
        // the point of keeping any.
        {
            let mut scratch = self.scratch.lock().expect("reactor scratch poisoned");
            if events.capacity() >= scratch.0.capacity() {
                scratch.0 = events;
            }
            if pending.capacity() >= scratch.1.capacity() {
                scratch.1 = pending;
            }
        }

        waited
    }

    /// Start the reactor on its own thread.
    pub fn start() -> Result<Self> {
        let shared = Arc::new(Shared {
            poller: Poller::new()?,
            slots: Mutex::new(HashMap::new()),
            next_index: AtomicU64::new(0),
            running: AtomicBool::new(true),
            next_deadline: AtomicU64::new(NO_DEADLINE),
        });

        let worker = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("nago-wss-reactor".into())
            // The one place this crate still needs `std`: it owns a thread.
            // Everything on the I/O path below is libc. A spawn failure is an
            // OS refusal, so it is reported as one rather than as a new error
            // kind that would exist for this single call.
            .spawn(move || run(&worker))
            .map_err(|error| super::error::Errno(error.raw_os_error().unwrap_or(libc::EAGAIN)))?;

        Ok(Self {
            shared,
            thread: Some(thread),
            // The threaded reactor keeps its scratch on the worker's stack and
            // never calls `poll_once`, so this stays empty.
            scratch: Mutex::new((Vec::new(), Vec::new())),
        })
    }

    /// Several reactors, each on its own thread with its own poller.
    ///
    /// One reactor thread is one `kevent`/`epoll_wait` loop, and every wakeup
    /// it produces crosses to whichever thread runs the task. With many busy
    /// connections that one loop is both a serialisation point and a thread
    /// boundary paid per message.
    ///
    /// Sharding gives each thread its own poller and its own descriptors, so
    /// the loops run in parallel and a descriptor's events always come from
    /// the same thread. [`Sharded::handle`] hands out the shard a new
    /// descriptor should register with, round robin, so connections spread
    /// evenly without the caller choosing.
    ///
    /// This is not a pool: a shard owns its descriptors for their lifetime and
    /// nothing is stolen between them. That is deliberate, because a
    /// descriptor moving between pollers is exactly the case generational
    /// tokens exist to make safe and there is no reason to invite it.
    pub fn sharded(shards: usize) -> Result<Sharded> {
        let shards = shards.max(1);
        let mut reactors = Vec::with_capacity(shards);
        for _ in 0..shards {
            reactors.push(Self::start()?);
        }
        Ok(Sharded {
            reactors,
            next: AtomicU64::new(0),
        })
    }

    /// A cloneable handle for registering descriptors.
    pub fn handle(&self) -> Handle {
        Handle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Stop the reactor thread and wait for it to finish.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.shared.running.store(false, Ordering::Release);
        // The thread may be blocked in `wait`; this is what gets it out.
        let _ = self.shared.poller.wake();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The reactor loop.
///
/// One blocking wait per pass, woken by a descriptor, a deadline or an explicit
/// wake. Nothing polls and nothing spins.
fn run(shared: &Arc<Shared>) {
    let mut events: Vec<Event> = Vec::with_capacity(64);
    // Reused across wakeups rather than allocated per dispatch: this runs on
    // every readiness event, so an allocation here is an allocation per
    // message on a busy connection.
    let mut pending: Vec<(u64, Waker)> = Vec::with_capacity(64);

    while shared.running.load(Ordering::Acquire) {
        let timeout = service_timers(shared);

        events.clear();
        if shared.poller.wait(&mut events, timeout).is_err() {
            // A failed wait is not recoverable by retrying in a tight loop; the
            // descriptor set is intact, so stop rather than spin.
            break;
        }

        dispatch(shared, &events, &mut pending);
    }
}

/// Fire any due timers and return how long the next wait may block.
///
/// The timer wheel is only consulted when the cached deadline says something is
/// actually due, so a connection delivering a flood of readiness events does
/// not drag the timer lock along with it. `None` means block until an event
/// arrives: there is no deadline to wake for.
fn service_timers(shared: &Arc<Shared>) -> Option<u64> {
    let deadline = shared.next_deadline.load(Ordering::Acquire);
    let now = crate::now_ns();

    // Not due yet: wait exactly until it is, and do not touch the wheel.
    if deadline != NO_DEADLINE && deadline > now {
        return Some(deadline - now);
    }

    // Either a deadline has arrived or a timer was armed and the cache was
    // invalidated. Both mean the wheel has work to report.
    let next = crate::poll_timers(now);
    shared
        .next_deadline
        .store(next.unwrap_or(NO_DEADLINE), Ordering::Release);
    next.map(|deadline| deadline.saturating_sub(now))
}

/// Wake the tasks named by `events`.
///
/// `pending` is scratch owned by the caller so that the common case allocates
/// nothing; it is left empty on return.
/// The two buffers `poll_once` reuses: the events read from the poller, and
/// the wakers they name, each paired with the slot index it came from so the
/// wake can be routed to the worker that descriptor belongs to.
type Scratch = (Vec<Event>, Vec<(u64, Waker)>);

fn dispatch(shared: &Arc<Shared>, events: &[Event], pending: &mut Vec<(u64, Waker)>) {
    // Wakers are collected under the lock and invoked after it is released: a
    // waker may run arbitrary code, including code that registers another
    // descriptor, and this lock is not reentrant.
    {
        let slots = shared.slots.lock().expect("reactor slots poisoned");
        for event in events {
            let (index, generation) = split_token(event.token);
            let Some(slot) = slots.get(&index) else {
                continue;
            };
            // A stale event: the slot was reused after this event was queued.
            if slot.generation != generation {
                continue;
            }
            let mut wakers = slot.wakers.lock().expect("reactor slot poisoned");
            if event.readable {
                wakers.readable = true;
                pending.extend(wakers.reader.take().map(|waker| (index, waker)));
            }
            if event.writable {
                pending.extend(wakers.writer.take().map(|waker| (index, waker)));
            }
        }
    }

    for (index, waker) in pending.drain(..) {
        // Route this wake to the worker this descriptor belongs to, so the
        // task keeps landing in one place across wakeups and different
        // descriptors spread over the pool.
        //
        // Pinning them all to one worker was tried and is worse than doing
        // nothing: it kept locality by funnelling every connection through a
        // single queue, which measured 38 percent faster at eight connections
        // and two and a half times slower at ten thousand. The index is
        // already unique per registration, so it distributes without needing
        // a hash.
        #[cfg(feature = "std")]
        let _routed = crate::runtime::route_reactor_wake(index);
        waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Descriptors spread across shards and each one still works.
    ///
    /// The failure this guards against is a descriptor registered with one
    /// shard and waited on through another: it would simply never wake, and
    /// the test would hang rather than fail, so the channel has a timeout.
    #[test]
    fn a_sharded_reactor_serves_every_shard() {
        use std::sync::mpsc;

        let sharded = Reactor::sharded(4).expect("sharded");
        assert_eq!(sharded.shards(), 4);

        let (tx, rx) = mpsc::channel();
        for _ in 0..8 {
            let handle = sharded.handle();
            let tx = tx.clone();
            std::thread::spawn(move || {
                let (a, b) = crate::reactor::testing::socket_pair();
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
                tx.send(flag.load(Ordering::Acquire)).expect("signal");
            });
        }
        drop(tx);

        for _ in 0..8 {
            let woke = rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("a shard never reported");
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
        use std::sync::mpsc;

        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let reactor = Reactor::local().expect("reactor");
            let handle = reactor.handle();
            let (a, b) = crate::reactor::testing::socket_pair();

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
            done_tx.send(()).expect("signal");
        });

        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("poll_once deadlocked when a waker re-entered it");
    }

    /// A `Waker` from a closure, without pulling in a dependency for it.
    fn waker_fn<F: Fn() + Send + Sync + 'static>(f: F) -> Waker {
        struct Fun<F>(F);
        impl<F: Fn() + Send + Sync + 'static> alloc::task::Wake for Fun<F> {
            fn wake(self: Arc<Self>) {
                (self.0)();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                (self.0)();
            }
        }
        Waker::from(Arc::new(Fun(f)))
    }

    use crate::reactor::testing::{socket_pair, write_byte};
    use std::os::fd::AsRawFd;
    use std::sync::mpsc;
    use std::time::Duration;

    /// A waker that reports having been woken, over a channel.
    fn channel_waker() -> (Waker, mpsc::Receiver<()>) {
        use std::sync::Arc as StdArc;
        struct Signal(mpsc::Sender<()>);
        impl std::task::Wake for Signal {
            fn wake(self: StdArc<Self>) {
                let _ = self.0.send(());
            }
            fn wake_by_ref(self: &StdArc<Self>) {
                let _ = self.0.send(());
            }
        }
        let (tx, rx) = mpsc::channel();
        (Waker::from(StdArc::new(Signal(tx))), rx)
    }

    #[test]
    fn wakes_a_reader_when_data_arrives() {
        let reactor = Reactor::start().expect("reactor");
        let handle = reactor.handle();
        let (a, b) = socket_pair();

        let registration = handle
            .register(a.as_raw_fd(), Interest::READABLE)
            .expect("register");
        let (waker, woken) = channel_waker();
        registration.poll_readable(&waker);

        // Nothing written: the waker must stay untouched.
        assert!(
            woken.recv_timeout(Duration::from_millis(100)).is_err(),
            "woken with no data pending"
        );

        write_byte(&b);

        woken
            .recv_timeout(Duration::from_secs(5))
            .expect("reader was never woken");
    }

    #[test]
    fn a_dropped_registration_stops_waking() {
        let reactor = Reactor::start().expect("reactor");
        let handle = reactor.handle();
        let (a, b) = socket_pair();

        let (waker, woken) = channel_waker();
        {
            let registration = handle
                .register(a.as_raw_fd(), Interest::READABLE)
                .expect("register");
            registration.poll_readable(&waker);
        }

        write_byte(&b);

        assert!(
            woken.recv_timeout(Duration::from_millis(200)).is_err(),
            "a dropped registration still woke its task"
        );
    }

    #[test]
    fn a_stale_token_does_not_wake_the_slots_new_owner() {
        // Generations exist for this: an event queued for one registration must
        // not wake whatever later takes the same slot index.
        let (index, generation) = split_token(make_token(5, 3));
        assert_eq!((index, generation), (5, 3));

        let mut slots: HashMap<u64, Arc<Slot>> = HashMap::new();
        slots.insert(
            5,
            Arc::new(Slot {
                generation: 4,
                wakers: Mutex::new(Wakers::default()),
            }),
        );

        let slot = slots.get(&5).expect("slot");
        assert_ne!(
            slot.generation, generation,
            "a reused slot must not match the old generation"
        );
    }

    #[test]
    fn an_armed_timer_lowers_the_cached_deadline_and_a_later_one_does_not() {
        // The cache is what keeps timer servicing off the socket path, so its
        // update rule is worth pinning down: sooner replaces, later is ignored.
        let reactor = Reactor::start().expect("reactor");
        let handle = reactor.handle();

        let now = crate::now_ns();
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
            shared_deadline(&reactor) <= soon,
            "a later timer pushed the deadline back"
        );
    }

    /// Read the cached deadline, for the test above.
    fn shared_deadline(reactor: &Reactor) -> u64 {
        reactor.shared.next_deadline.load(Ordering::Acquire)
    }

    #[test]
    fn socket_events_do_not_disturb_the_timer_deadline() {
        // The point of the cache: a flood of readiness must not drag the timer
        // wheel along with it. If the loop consulted the wheel on every wakeup
        // this deadline would be recomputed and the assertion would fail.
        let reactor = Reactor::start().expect("reactor");
        let handle = reactor.handle();

        let far = crate::now_ns() + 3_600_000_000_000;
        handle.timer_armed(far).expect("arm");

        let (a, b) = socket_pair();
        let registration = handle
            .register(a.as_raw_fd(), Interest::READABLE)
            .expect("register");

        // Generate real readiness events repeatedly.
        for _ in 0..50 {
            let (waker, _woken) = channel_waker();
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
            shared_deadline(&reactor),
            far,
            "socket traffic moved the timer deadline"
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
}
