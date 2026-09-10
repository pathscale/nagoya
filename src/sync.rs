//! Async synchronisation, for a crate that does not want a runtime.
//!
//! # Why this is here and not taken from tokio
//!
//! `tokio::sync` is waker-based and runtime-agnostic: its `RwLock`, `Semaphore`
//! and `Notify` genuinely work on any executor, which is why a storage engine
//! could sit on nagoya's file layer and still lock with tokio's primitives, and
//! pass its tests doing so.
//!
//! It is still the wrong dependency. Taking those types means taking `tokio`,
//! and `tokio` is `std` and unconditional: `cargo tree --no-default-features
//! -e normal -i tokio` on WorkTable showed it linked even with every feature
//! off, because the uses were never gated. A crate whose point is to have no
//! runtime cannot reach into one for a lock.
//!
//! So these are the four types that were actually used, and nothing else. Not a
//! general-purpose library: `RwLock`, `Semaphore`, `Notify`, and the owned
//! guards, because those are what a table needed.
//!
//! # What these are not
//!
//! **Not fair.** A waiter is woken and then races for the lock with anyone
//! arriving at that moment, so a writer can be overtaken. `tokio::sync::RwLock`
//! is write-preferring and does not do this. Nothing here depends on fairness
//! today, and saying so is cheaper than implementing it; if a caller starts to
//! depend on it, that is the moment to write the queue properly rather than to
//! discover it in production.
//!
//! **Requires `alloc`.** Contended waits use reusable indexed slots.
//! Uncontended acquisition does not allocate a waiter.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::cell::UnsafeCell;
use core::future::Future;
use core::ops::{Deref, DerefMut};
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};

use spin::Mutex;

#[derive(Clone, Copy, PartialEq, Eq)]
enum WakeKind {
    Queued,
    One,
    All,
}

struct Waiter {
    previous: Option<usize>,
    next: Option<usize>,
    kind: WakeKind,
    waker: Option<Waker>,
}

enum WaiterSlot {
    Live(Waiter),
    Free(Option<usize>),
}

// Stable slot indices belong to futures, not wakers. A granted slot remains
// live until its future acknowledges or cancels it. This permits O(1) removal
// without an intrusive pointer, a per-waiter Arc, or a scan on cancellation.
struct WaitQueue {
    slots: Vec<WaiterSlot>,
    free: Option<usize>,
    head: Option<usize>,
    tail: Option<usize>,
}

impl WaitQueue {
    const fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: None,
            head: None,
            tail: None,
        }
    }

    fn get(&self, id: usize) -> &Waiter {
        match &self.slots[id] {
            WaiterSlot::Live(waiter) => waiter,
            WaiterSlot::Free(_) => unreachable!("registration already released"),
        }
    }

    fn get_mut(&mut self, id: usize) -> &mut Waiter {
        match &mut self.slots[id] {
            WaiterSlot::Live(waiter) => waiter,
            WaiterSlot::Free(_) => unreachable!("registration already released"),
        }
    }

    fn enqueue(&mut self, id: usize) {
        let tail = self.tail;
        let waiter = self.get_mut(id);
        waiter.previous = tail;
        waiter.next = None;
        waiter.kind = WakeKind::Queued;
        if let Some(tail) = tail {
            self.get_mut(tail).next = Some(id);
        } else {
            self.head = Some(id);
        }
        self.tail = Some(id);
    }

    fn unlink(&mut self, id: usize) {
        let waiter = self.get(id);
        let (previous, next) = (waiter.previous, waiter.next);
        if let Some(previous) = previous {
            self.get_mut(previous).next = next;
        } else {
            self.head = next;
        }
        if let Some(next) = next {
            self.get_mut(next).previous = previous;
        } else {
            self.tail = previous;
        }
        let waiter = self.get_mut(id);
        waiter.previous = None;
        waiter.next = None;
    }

    // Return any replaced waker so its destructor runs outside the queue lock.
    fn register(&mut self, token: &mut Option<usize>, waker: Option<&Waker>) -> Option<Waker> {
        let id = match *token {
            Some(id) => {
                if self.get(id).kind != WakeKind::Queued {
                    self.enqueue(id);
                }
                id
            }
            None => {
                let waiter = Waiter {
                    previous: None,
                    next: None,
                    kind: WakeKind::Queued,
                    waker: None,
                };
                let id = if let Some(id) = self.free {
                    self.free = match &self.slots[id] {
                        WaiterSlot::Free(next) => *next,
                        WaiterSlot::Live(_) => unreachable!("free-list entry is live"),
                    };
                    self.slots[id] = WaiterSlot::Live(waiter);
                    id
                } else {
                    self.slots.push(WaiterSlot::Live(waiter));
                    self.slots.len() - 1
                };
                *token = Some(id);
                self.enqueue(id);
                id
            }
        };
        let waiter = self.get_mut(id);
        if let Some(waker) = waker {
            if !waiter
                .waker
                .as_ref()
                .is_some_and(|old| old.will_wake(waker))
            {
                return waiter.waker.replace(waker.clone());
            }
        }
        None
    }

    fn remove(&mut self, token: &mut Option<usize>) -> Option<Waiter> {
        let id = token.take()?;
        if self.get(id).kind == WakeKind::Queued {
            self.unlink(id);
        }
        let old = core::mem::replace(&mut self.slots[id], WaiterSlot::Free(self.free));
        self.free = Some(id);
        match old {
            WaiterSlot::Live(waiter) => Some(waiter),
            WaiterSlot::Free(_) => unreachable!("registration already released"),
        }
    }

    // Outer Option means a waiter was granted, even if enable() has not
    // supplied a waker yet.
    fn grant_one(&mut self) -> Option<Option<Waker>> {
        let id = self.head?;
        self.unlink(id);
        let waiter = self.get_mut(id);
        waiter.kind = WakeKind::One;
        Some(waiter.waker.take())
    }

    fn grant_all(&mut self) -> Vec<Waker> {
        let mut wakers = Vec::new();
        while let Some(id) = self.head {
            self.unlink(id);
            let waiter = self.get_mut(id);
            waiter.kind = WakeKind::All;
            if let Some(waker) = waiter.waker.take() {
                wakers.push(waker);
            }
        }
        wakers
    }
}

struct Waiters(Mutex<WaitQueue>);

impl Waiters {
    const fn new() -> Self {
        Self(Mutex::new(WaitQueue::new()))
    }

    fn register(&self, token: &mut Option<usize>, waker: &Waker) {
        let old = self.0.lock().register(token, Some(waker));
        drop(old);
    }

    fn remove(&self, token: &mut Option<usize>) -> Option<WakeKind> {
        if token.is_none() {
            return None;
        }
        let removed = self.0.lock().remove(token);
        removed.map(|waiter| waiter.kind)
    }

    fn wake_one(&self) {
        let waker = self.0.lock().grant_one().flatten();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn wake_all(&self) {
        let wakers = self.0.lock().grant_all();
        for waker in wakers {
            waker.wake();
        }
    }
}

/// A one-shot wake-up that remembers a notification with no waiter.
///
/// The permit is the whole point: a producer that signals before the consumer
/// has parked must not lose the signal, or the consumer sleeps forever holding
/// work that was already announced.
pub struct Notify {
    permit: AtomicBool,
    /// Bumped by [`Notify::notify_waiters`], which leaves no permit.
    ///
    /// Snapshotted at construction so broadcasts before the first poll are
    /// observed. Single notifications are owned by registered waiter slots.
    generation: AtomicUsize,
    waiters: Waiters,
}

impl core::fmt::Debug for Notify {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Notify")
            .field("permit", &self.permit.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl Notify {
    /// A `Notify` holding no permit and no waiters.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            permit: AtomicBool::new(false),
            generation: AtomicUsize::new(0),
            waiters: Waiters::new(),
        }
    }

    /// Wake one waiter, or leave a permit for the next one to arrive.
    pub fn notify_one(&self) {
        let waker = {
            let mut queue = self.waiters.0.lock();
            self.grant_one(&mut queue)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn grant_one(&self, queue: &mut WaitQueue) -> Option<Waker> {
        match queue.grant_one() {
            Some(waker) => waker,
            None => {
                // Registration and this fallback share a lock: a waiter
                // cannot appear between the empty check and permit store.
                self.permit.store(true, Ordering::Release);
                None
            }
        }
    }

    /// Wake every current waiter. Leaves no permit.
    pub fn notify_waiters(&self) {
        let wakers = {
            let mut queue = self.waiters.0.lock();
            self.generation.fetch_add(1, Ordering::AcqRel);
            queue.grant_all()
        };
        for waker in wakers {
            waker.wake();
        }
    }

    /// Wait for a notification, taking a stored permit if one is there.
    pub fn notified(&self) -> Notified<'_> {
        Notified {
            notify: self,
            generation: self.generation.load(Ordering::Acquire),
            registration: None,
            done: false,
            reserved_one: false,
        }
    }
}

/// The future returned by [`Notify::notified`].
pub struct Notified<'a> {
    notify: &'a Notify,
    /// The broadcast generation when this was created.
    generation: usize,
    registration: Option<usize>,
    done: bool,
    // enable() may reserve a notification before poll delivers it. Dropping
    // during that window must transfer the notification, not consume it.
    reserved_one: bool,
}

impl Notified<'_> {
    /// Register before checking external state, reserving a notification if
    /// one is already available. Returns whether this future is now ready.
    pub fn enable(self: Pin<&mut Self>) -> bool {
        self.get_mut().ready_or_register(None)
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.ready_or_register(Some(context.waker())) {
            this.reserved_one = false;
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

impl Notified<'_> {
    fn ready_or_register(&mut self, waker: Option<&Waker>) -> bool {
        if self.done {
            return true;
        }
        if self.registration.is_none()
            && self.notify.generation.load(Ordering::Acquire) != self.generation
        {
            self.done = true;
            return true;
        }
        // The uncontended stored-permit path does not allocate or lock.
        if self.registration.is_none()
            && self
                .notify
                .permit
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.done = true;
            self.reserved_one = true;
            return true;
        }
        let mut queue = self.notify.waiters.0.lock();
        let assigned = self
            .registration
            .map(|id| queue.get(id).kind)
            .filter(|kind| *kind != WakeKind::Queued);
        let notification = if assigned.is_some() {
            assigned
        } else if self.notify.generation.load(Ordering::Acquire) != self.generation {
            Some(WakeKind::All)
        } else if self.notify.permit.swap(false, Ordering::AcqRel) {
            Some(WakeKind::One)
        } else {
            None
        };
        if let Some(kind) = notification {
            let removed = queue.remove(&mut self.registration);
            self.done = true;
            self.reserved_one = kind == WakeKind::One;
            drop(queue);
            drop(removed);
            return true;
        }
        let old = queue.register(&mut self.registration, waker);
        drop(queue);
        drop(old);
        false
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        if self.reserved_one {
            self.notify.notify_one();
            return;
        }
        if self.registration.is_none() {
            return;
        }
        let (removed, waker) = {
            let mut queue = self.notify.waiters.0.lock();
            let removed = queue.remove(&mut self.registration);
            let waker = if removed
                .as_ref()
                .is_some_and(|entry| entry.kind == WakeKind::One)
            {
                self.notify.grant_one(&mut queue)
            } else {
                None
            };
            (removed, waker)
        };
        drop(removed);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// A rendezvous for a fixed number of tasks.
///
/// Every waiter blocks until `n` of them have arrived, then all are released
/// together. Reusable: the next `n` arrivals rendezvous again.
///
/// The benchmark case is what this is for. A harness that starts `n` client
/// tasks and times them has to release them at one instant, or the first
/// task's warm-up is measured against the last task's steady state and the
/// number means nothing. A barrier is how that instant is defined.
pub struct Barrier {
    n: usize,
    state: Mutex<BarrierState>,
    notify: Notify,
}

struct BarrierState {
    arrived: usize,
    /// Bumped on each release, so a waiter can tell "my group has gone" from
    /// "someone else's group has". Without it a task that is slow to be
    /// scheduled after the wake rejoins the *next* rendezvous and hangs.
    generation: usize,
}

impl Barrier {
    /// A barrier that releases once `n` tasks have arrived.
    ///
    /// `n` of 0 or 1 never blocks, which is what a single-threaded run of a
    /// harness wants rather than a special case at every call site.
    #[must_use]
    pub fn new(n: usize) -> Self {
        Self {
            n,
            state: Mutex::new(BarrierState {
                arrived: 0,
                generation: 0,
            }),
            notify: Notify::new(),
        }
    }

    /// Wait for the rest of the group.
    ///
    /// Returns `true` for exactly one waiter per rendezvous, the one whose
    /// arrival completed it. Callers use that to elect a task to do the
    /// once-per-round work without a second primitive.
    pub async fn wait(&self) -> bool {
        let generation = {
            let mut state = self.state.lock();
            state.arrived += 1;
            if state.arrived >= self.n {
                state.arrived = 0;
                state.generation = state.generation.wrapping_add(1);
                drop(state);
                self.notify.notify_waiters();
                return true;
            }
            state.generation
        };

        loop {
            // Register before re-reading the generation. `notify_waiters`
            // retains no permit, so a release landing between the read and the
            // await would be lost and this task would wait for a rendezvous
            // that already happened.
            let notified = self.notify.notified();
            let mut notified = core::pin::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().generation != generation {
                return false;
            }
            notified.await;
        }
    }
}

impl core::fmt::Debug for Barrier {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Barrier")
            .field("n", &self.n)
            .finish_non_exhaustive()
    }
}

/// A counting semaphore.
pub struct Semaphore {
    permits: AtomicUsize,
    waiters: Waiters,
}

impl Semaphore {
    /// A semaphore starting with `permits` available.
    #[must_use]
    pub const fn new(permits: usize) -> Self {
        Self {
            permits: AtomicUsize::new(permits),
            waiters: Waiters::new(),
        }
    }

    /// Permits available right now.
    #[must_use]
    pub fn available_permits(&self) -> usize {
        self.permits.load(Ordering::Acquire)
    }

    /// Take a permit without waiting, if one is free.
    pub fn try_acquire(&self) -> Option<SemaphorePermit<'_>> {
        let mut current = self.permits.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return None;
            }
            match self.permits.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(SemaphorePermit { semaphore: self }),
                Err(seen) => current = seen,
            }
        }
    }

    /// Wait for a permit.
    pub fn acquire(&self) -> Acquire<'_> {
        Acquire {
            semaphore: self,
            registration: None,
        }
    }

    /// Hand the semaphore `n` more permits than it was created with.
    ///
    /// The counterpart to [`SemaphorePermit::forget`]: together they let a
    /// caller move a permit's *ownership* somewhere the borrow could not
    /// follow, which is how a semaphore gets used as a one-shot gate rather
    /// than as a pool of N interchangeable slots.
    pub fn add_permits(&self, n: usize) {
        if n == 0 {
            return;
        }
        self.permits.fetch_add(n, Ordering::AcqRel);
        // One waker per permit. Waking only one and letting it cascade is
        // wrong here: a woken task takes exactly one permit, so the rest would
        // sleep through permits that are already available.
        for _ in 0..n {
            self.waiters.wake_one();
        }
    }

    fn release(&self) {
        self.permits.fetch_add(1, Ordering::AcqRel);
        self.waiters.wake_one();
    }
}

impl core::fmt::Debug for Semaphore {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Semaphore")
            .field("available_permits", &self.available_permits())
            .finish_non_exhaustive()
    }
}

/// A held semaphore permit, returned on drop.
pub struct SemaphorePermit<'a> {
    semaphore: &'a Semaphore,
}

impl SemaphorePermit<'_> {
    /// Drop the permit without returning it, permanently shrinking the
    /// semaphore by one.
    ///
    /// Pairs with [`Semaphore::add_permits`]. Forgetting every permit turns
    /// the semaphore into a signal, where the count is a number of events that
    /// happened rather than a number of slots that are free.
    pub fn forget(self) {
        core::mem::forget(self);
    }
}

impl core::fmt::Debug for SemaphorePermit<'_> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("SemaphorePermit")
            .finish_non_exhaustive()
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        self.semaphore.release();
    }
}

/// The future returned by [`Semaphore::acquire`].
pub struct Acquire<'a> {
    semaphore: &'a Semaphore,
    registration: Option<usize>,
}

impl<'a> Future for Acquire<'a> {
    type Output = SemaphorePermit<'a>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(permit) = this.semaphore.try_acquire() {
            this.semaphore.waiters.remove(&mut this.registration);
            return Poll::Ready(permit);
        }
        this.semaphore
            .waiters
            .register(&mut this.registration, context.waker());
        if let Some(permit) = this.semaphore.try_acquire() {
            this.semaphore.waiters.remove(&mut this.registration);
            return Poll::Ready(permit);
        }
        Poll::Pending
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        if self.semaphore.waiters.remove(&mut self.registration) == Some(WakeKind::One)
            && self.semaphore.available_permits() != 0
        {
            self.semaphore.waiters.wake_one();
        }
    }
}

/// State value meaning "one writer holds this"; any other value is the number
/// of readers.
const WRITER: usize = usize::MAX;

/// An async reader-writer lock.
pub struct RwLock<T: ?Sized> {
    state: AtomicUsize,
    waiters: Waiters,
    value: UnsafeCell<T>,
}

// SAFETY: access to `value` is gated by `state`, which admits either one writer
// or any number of readers and never both. `T: Send` is required to move the
// value between threads; `T: Sync` because readers hand out `&T` concurrently.
unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    /// An unlocked `RwLock` holding `value`.
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: Waiters::new(),
            value: UnsafeCell::new(value),
        }
    }

    /// The value, consuming the lock.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Exclusive access without a lock, because `&mut self` is already proof
    /// that nobody else holds one.
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }

    fn try_read_raw(&self) -> bool {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current == WRITER {
                return false;
            }
            match self.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(seen) => current = seen,
            }
        }
    }

    fn try_write_raw(&self) -> bool {
        self.state
            .compare_exchange(0, WRITER, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn release_read(&self) {
        // The last reader out wakes a waiting writer. Waking on every release
        // would be correct and wasteful; waking on none would stall the writer.
        if self.state.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.waiters.wake_all();
        }
    }

    fn release_write(&self) {
        self.state.store(0, Ordering::Release);
        // Every waiter, because the next holder may be a batch of readers and
        // waking one of those leaves the rest asleep behind an unlocked lock.
        self.waiters.wake_all();
    }

    /// Take a read lock without waiting, if no writer holds it.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        self.try_read_raw().then(|| RwLockReadGuard { lock: self })
    }

    /// Take a write lock without waiting, if the lock is free.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        self.try_write_raw()
            .then(|| RwLockWriteGuard { lock: self })
    }

    /// Wait for a read lock.
    pub fn read(&self) -> Read<'_, T> {
        Read {
            lock: self,
            registration: None,
        }
    }

    /// Wait for a write lock.
    pub fn write(&self) -> Write<'_, T> {
        Write {
            lock: self,
            registration: None,
        }
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Wait for a read lock that borrows the `Arc` rather than the lock.
    ///
    /// The guard a caller can hold across an await without naming a lifetime,
    /// which is what `tokio::sync`'s owned guards were used for here.
    pub fn read_owned(self: Arc<Self>) -> ReadOwned<T> {
        ReadOwned {
            lock: Some(self),
            registration: None,
        }
    }

    /// The write half of [`Self::read_owned`].
    pub fn write_owned(self: Arc<Self>) -> WriteOwned<T> {
        WriteOwned {
            lock: Some(self),
            registration: None,
        }
    }

    /// An owned read lock, or `None` if a writer holds it.
    pub fn try_read_owned(self: Arc<Self>) -> Option<OwnedRwLockReadGuard<T>> {
        self.try_read_raw()
            .then(|| OwnedRwLockReadGuard { lock: self })
    }

    /// An owned write lock, or `None` if the lock is held at all.
    pub fn try_write_owned(self: Arc<Self>) -> Option<OwnedRwLockWriteGuard<T>> {
        self.try_write_raw()
            .then(|| OwnedRwLockWriteGuard { lock: self })
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized> core::fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // The value is deliberately not shown: reading it would need the lock,
        // and a `Debug` that can block is a debugging hazard rather than a help.
        let state = self.state.load(Ordering::Relaxed);
        let held = if state == WRITER {
            "write"
        } else if state == 0 {
            "free"
        } else {
            "read"
        };
        f.debug_struct("RwLock")
            .field("held", &held)
            .finish_non_exhaustive()
    }
}

/// A held read lock.
pub struct RwLockReadGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: a read guard exists only while `state` counts this reader, and
        // a writer cannot hold the lock at the same time.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.release_read();
    }
}

/// A held write lock.
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: a write guard is the only guard that can exist.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as above, and exclusively.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.release_write();
    }
}

/// A read lock that owns its `Arc`.
pub struct OwnedRwLockReadGuard<T: ?Sized> {
    lock: Arc<RwLock<T>>,
}

impl<T: ?Sized> Deref for OwnedRwLockReadGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: as `RwLockReadGuard`.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for OwnedRwLockReadGuard<T> {
    fn drop(&mut self) {
        self.lock.release_read();
    }
}

/// A write lock that owns its `Arc`.
pub struct OwnedRwLockWriteGuard<T: ?Sized> {
    lock: Arc<RwLock<T>>,
}

impl<T: ?Sized> Deref for OwnedRwLockWriteGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: as `RwLockWriteGuard`.
        unsafe { &*self.lock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for OwnedRwLockWriteGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: as `RwLockWriteGuard`.
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T: ?Sized> Drop for OwnedRwLockWriteGuard<T> {
    fn drop(&mut self) {
        self.lock.release_write();
    }
}

/// The future returned by [`RwLock::read`].
pub struct Read<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    registration: Option<usize>,
}

impl<'a, T: ?Sized> Future for Read<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.lock.try_read_raw() {
            this.lock.waiters.remove(&mut this.registration);
            return Poll::Ready(RwLockReadGuard { lock: this.lock });
        }
        this.lock
            .waiters
            .register(&mut this.registration, context.waker());
        if this.lock.try_read_raw() {
            this.lock.waiters.remove(&mut this.registration);
            return Poll::Ready(RwLockReadGuard { lock: this.lock });
        }
        Poll::Pending
    }
}

/// The future returned by [`RwLock::write`].
pub struct Write<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
    registration: Option<usize>,
}

impl<'a, T: ?Sized> Future for Write<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.lock.try_write_raw() {
            this.lock.waiters.remove(&mut this.registration);
            return Poll::Ready(RwLockWriteGuard { lock: this.lock });
        }
        this.lock
            .waiters
            .register(&mut this.registration, context.waker());
        if this.lock.try_write_raw() {
            this.lock.waiters.remove(&mut this.registration);
            return Poll::Ready(RwLockWriteGuard { lock: this.lock });
        }
        Poll::Pending
    }
}

/// The future returned by [`RwLock::read_owned`].
pub struct ReadOwned<T: ?Sized> {
    lock: Option<Arc<RwLock<T>>>,
    registration: Option<usize>,
}

impl<T: ?Sized> Future for ReadOwned<T> {
    type Output = OwnedRwLockReadGuard<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let lock = this.lock.as_ref().expect("polled after completion");
        if lock.try_read_raw() {
            lock.waiters.remove(&mut this.registration);
            let lock = this.lock.take().expect("just checked");
            return Poll::Ready(OwnedRwLockReadGuard { lock });
        }
        lock.waiters
            .register(&mut this.registration, context.waker());
        if lock.try_read_raw() {
            lock.waiters.remove(&mut this.registration);
            let lock = this.lock.take().expect("just checked");
            return Poll::Ready(OwnedRwLockReadGuard { lock });
        }
        Poll::Pending
    }
}

/// The future returned by [`RwLock::write_owned`].
pub struct WriteOwned<T: ?Sized> {
    lock: Option<Arc<RwLock<T>>>,
    registration: Option<usize>,
}

impl<T: ?Sized> Future for WriteOwned<T> {
    type Output = OwnedRwLockWriteGuard<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let lock = this.lock.as_ref().expect("polled after completion");
        if lock.try_write_raw() {
            lock.waiters.remove(&mut this.registration);
            let lock = this.lock.take().expect("just checked");
            return Poll::Ready(OwnedRwLockWriteGuard { lock });
        }
        lock.waiters
            .register(&mut this.registration, context.waker());
        if lock.try_write_raw() {
            lock.waiters.remove(&mut this.registration);
            let lock = this.lock.take().expect("just checked");
            return Poll::Ready(OwnedRwLockWriteGuard { lock });
        }
        Poll::Pending
    }
}

impl<T: ?Sized> Drop for Read<'_, T> {
    fn drop(&mut self) {
        self.lock.waiters.remove(&mut self.registration);
    }
}

impl<T: ?Sized> Drop for Write<'_, T> {
    fn drop(&mut self) {
        self.lock.waiters.remove(&mut self.registration);
    }
}

impl<T: ?Sized> Drop for ReadOwned<T> {
    fn drop(&mut self) {
        if let Some(lock) = &self.lock {
            lock.waiters.remove(&mut self.registration);
        }
    }
}

impl<T: ?Sized> Drop for WriteOwned<T> {
    fn drop(&mut self) {
        if let Some(lock) = &self.lock {
            lock.waiters.remove(&mut self.registration);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_on;
    use alloc::vec::Vec;

    #[test]
    fn a_notification_before_the_wait_is_not_lost() {
        let notify = Notify::new();
        notify.notify_one();
        // Would hang if the permit were not remembered.
        block_on(notify.notified());
    }

    #[test]
    fn readers_share_and_a_writer_excludes() {
        let lock = RwLock::new(5u32);
        let a = lock.try_read().expect("first reader");
        let b = lock.try_read().expect("second reader");
        assert_eq!(*a, 5);
        assert_eq!(*b, 5);
        assert!(lock.try_write().is_none(), "a writer must not join readers");
        drop(a);
        assert!(lock.try_write().is_none(), "one reader still holds it");
        drop(b);
        let mut w = lock.try_write().expect("the last reader left");
        *w = 6;
        drop(w);
        assert_eq!(*block_on(lock.read()), 6);
    }

    #[test]
    fn a_write_guard_excludes_readers() {
        let lock = RwLock::new(0u32);
        let w = lock.try_write().expect("free");
        assert!(lock.try_read().is_none());
        drop(w);
        assert!(lock.try_read().is_some());
    }

    #[test]
    fn a_semaphore_hands_out_exactly_its_permits() {
        let semaphore = Semaphore::new(2);
        let a = semaphore.try_acquire().expect("first");
        let b = semaphore.try_acquire().expect("second");
        assert!(semaphore.try_acquire().is_none());
        drop(a);
        let c = semaphore.try_acquire().expect("one was returned");
        drop(b);
        drop(c);
        assert_eq!(semaphore.available_permits(), 2);
    }

    #[cfg(feature = "std")]
    #[test]
    fn a_barrier_releases_only_once_everyone_has_arrived() {
        use crate::runtime::Runtime;

        let runtime = Runtime::new(4);
        let barrier = Arc::new(Barrier::new(4));
        let passed = Arc::new(AtomicUsize::new(0));
        // Counted before and after, so a barrier that let anyone through early
        // is visible rather than merely suspected.
        let leaders = Arc::new(AtomicUsize::new(0));

        let handles: alloc::vec::Vec<_> = (0..4)
            .map(|_| {
                let barrier = barrier.clone();
                let passed = passed.clone();
                let leaders = leaders.clone();
                runtime.spawn(async move {
                    if barrier.wait().await {
                        leaders.fetch_add(1, Ordering::Relaxed);
                    }
                    passed.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        for handle in handles {
            block_on(handle);
        }

        assert_eq!(passed.load(Ordering::Relaxed), 4);
        assert_eq!(
            leaders.load(Ordering::Relaxed),
            1,
            "exactly one waiter leads a rendezvous"
        );
    }

    #[test]
    fn a_barrier_of_one_never_blocks() {
        let barrier = Barrier::new(1);
        assert!(block_on(barrier.wait()));
        // Reusable: a second rendezvous behaves like the first.
        assert!(block_on(barrier.wait()));
    }

    #[test]
    fn a_forgotten_permit_does_not_come_back() {
        let semaphore = Semaphore::new(1);
        block_on(semaphore.acquire()).forget();
        assert_eq!(semaphore.available_permits(), 0);
        assert!(semaphore.try_acquire().is_none());
    }

    #[test]
    fn added_permits_are_a_signal_a_forgetting_waiter_can_count() {
        // The gate shape: `add_permits` says an event happened, the waiter
        // consumes exactly one and forgets it, so the count tracks events
        // rather than returning to a fixed pool.
        let semaphore = Semaphore::new(0);
        semaphore.add_permits(3);
        assert_eq!(semaphore.available_permits(), 3);
        for _ in 0..3 {
            block_on(semaphore.acquire()).forget();
        }
        assert_eq!(semaphore.available_permits(), 0);
        assert!(semaphore.try_acquire().is_none());
    }

    #[test]
    fn owned_guards_outlive_the_borrow() {
        let lock = Arc::new(RwLock::new(Vec::<u32>::new()));
        {
            let mut w = block_on(lock.clone().write_owned());
            w.push(1);
        }
        let r = block_on(lock.clone().read_owned());
        assert_eq!(&*r, &[1]);
    }
}
