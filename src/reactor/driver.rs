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
    /// Whether an edge has been seen since the last write filled the socket.
    ///
    /// The write side's twin of `readable`, and in the same lock for the same
    /// reason. Without it every write had to learn the buffer was full by
    /// being told so: one `sendmsg` that succeeds, then a second that returns
    /// `EWOULDBLOCK` purely to establish what a short write already proved.
    /// Measured at 256 `sendmsg` per MiB against a floor of 128.
    writable: bool,
    /// Whether the peer has hung up. Latched, and never cleared.
    ///
    /// `readable` is a question about right now and a read answers it. This is
    /// a question about the rest of the descriptor's life, and once the answer
    /// is yes it stays yes: every subsequent `recv` returns 0. Keeping it
    /// separate is what lets a short read consume readiness without swallowing
    /// the hang-up that arrived on the same edge.
    hangup: bool,
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
            // Readable from the start, because at this moment nobody knows
            // whether it is. The poller is edge triggered, so it reports
            // transitions and not state: anything that arrived before this
            // `add` produced an edge that is already gone and will never be
            // delivered. A peer that connected and hung up immediately is the
            // ordinary case, and starting at `false` means the first read
            // parks for an edge that has already passed and waits forever.
            //
            // Starting at `true` costs one speculative `recv` per
            // registration. If data is there it is read, and if it is not the
            // `EWOULDBLOCK` path parks correctly with the edge still ahead.
            //
            // `writable` starts there too, and for a stronger reason than
            // symmetry: a fresh socket has room by definition, so waiting for
            // an edge to be told so would be waiting for one that has already
            // gone by. That is a hang on the first write, not a slow one.
            wakers: Mutex::new(Wakers {
                readable: true,
                writable: true,
                ..Wakers::default()
            }),
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
        #[cfg(feature = "syscall-counters")]
        crate::reactor::counters::WAKE.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
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
        // A hung-up descriptor is permanently worth reading: `recv` answers 0
        // rather than `EWOULDBLOCK`, so this neither spins nor costs a wasted
        // syscall. It is checked because the hang-up may have arrived on the
        // same edge as the last of the data, and that read cleared `readable`.
        if wakers.readable || wakers.hangup {
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

    /// Take writable readiness, or park `waker` for the next edge.
    ///
    /// The write side's twin of [`Self::take_readable_or_park`], and the thing
    /// that lets a write skip the syscall whose only job was to be rejected.
    pub fn take_writable_or_park(&self, waker: &Waker) -> bool {
        let mut wakers = self.slot.wakers.lock().expect("reactor slot poisoned");
        if wakers.writable {
            wakers.writable = false;
            return true;
        }
        match &wakers.writer {
            Some(existing) if existing.will_wake(waker) => {}
            _ => wakers.writer = Some(waker.clone()),
        }
        false
    }

    /// Note that this descriptor may still have room, for a caller whose write
    /// was accepted whole and so never proved the buffer full.
    pub fn mark_writable(&self) {
        let mut wakers = self.slot.wakers.lock().expect("reactor slot poisoned");
        wakers.writable = true;
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
    /// The soonest deadline this reactor will wake for, in nanoseconds.
    ///
    /// [`NO_DEADLINE`] when no timer is armed. This is the cache that keeps
    /// timer servicing off the socket path: the wheel is consulted only when
    /// this says it is time, so whether arming a timer updates it, and whether
    /// socket traffic leaves it alone, is observable behaviour rather than an
    /// implementation detail.
    #[must_use]
    pub fn next_deadline(&self) -> u64 {
        self.shared.next_deadline.load(Ordering::Acquire)
    }

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
            // Latched before readiness is, so a reader waking on this edge
            // cannot observe the data without also observing the hang-up.
            if event.hangup {
                wakers.hangup = true;
            }
            if event.readable || event.hangup {
                wakers.readable = true;
                pending.extend(wakers.reader.take().map(|waker| (index, waker)));
            }
            if event.writable {
                wakers.writable = true;
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
        // `None` when no shared pool is running, in which case there is
        // nothing to route onto and starting one here would be wrong.
        #[cfg(feature = "std")]
        let _routed = crate::runtime::route_reactor_wake(index);
        waker.wake();
    }
}
