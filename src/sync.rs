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
//! **Not `no_std`-free of allocation.** The waiter lists are `VecDeque<Waker>`,
//! so this needs `alloc`, which the rest of the crate needs anyway.

use alloc::collections::VecDeque;
use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::future::Future;
use core::ops::{Deref, DerefMut};
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};

use spin::Mutex;

/// The waiter list every primitive here shares.
///
/// A spin lock rather than a blocking one because it is held for the length of
/// a push or a pop and nothing else, and because a blocking mutex would need
/// the very thing this module exists to avoid.
#[derive(Default)]
struct Waiters(Mutex<VecDeque<Waker>>);

impl Waiters {
    const fn new() -> Self {
        Self(Mutex::new(VecDeque::new()))
    }

    fn push(&self, waker: &Waker) {
        let mut queue = self.0.lock();
        // Replacing an equivalent waker rather than pushing a second one: a
        // future polled twice before it completes would otherwise leave a stale
        // entry that wakes a task which has already moved on.
        if queue.iter().any(|w| w.will_wake(waker)) {
            return;
        }
        queue.push_back(waker.clone());
    }

    fn wake_one(&self) {
        let waker = self.0.lock().pop_front();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn wake_all(&self) {
        // Drained under the lock, woken outside it: `wake` runs arbitrary
        // scheduler code, and holding a spin lock across that invites the
        // woken task to spin on the lock its waker still holds.
        let drained: VecDeque<Waker> = core::mem::take(&mut *self.0.lock());
        for waker in drained {
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
    /// A `Notified` snapshots this when it is created, so a broadcast landing
    /// between creation and the first poll is still observed. That is the race
    /// `tokio`'s `Notified::enable` exists for, and taking it at construction
    /// costs one relaxed load instead of an intrusive list.
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
        self.permit.store(true, Ordering::Release);
        self.waiters.wake_one();
    }

    /// Wake every current waiter. Leaves no permit.
    pub fn notify_waiters(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.waiters.wake_all();
    }

    /// Wait for a notification, taking a stored permit if one is there.
    pub fn notified(&self) -> Notified<'_> {
        Notified {
            notify: self,
            generation: self.generation.load(Ordering::Acquire),
        }
    }
}

/// The future returned by [`Notify::notified`].
pub struct Notified<'a> {
    notify: &'a Notify,
    /// The broadcast generation when this was created.
    generation: usize,
}

impl Notified<'_> {
    /// Whether a notification is already waiting for this future.
    ///
    /// **This is not `tokio`'s `enable` and does not need to be.** There, the
    /// future must be linked into the waiter list before the caller reads any
    /// state, or a notification in between is lost. Here the two ways of
    /// notifying are both already durable across that window: `notify_one`
    /// leaves a permit that outlives it, and `notify_waiters` bumps a
    /// generation this future snapshotted when it was created. So the race the
    /// call guards against cannot happen, and the method exists to keep the
    /// registration point visible at the call site, which is worth having.
    pub fn enable(self: Pin<&mut Self>) -> bool {
        self.notify.permit.load(Ordering::Acquire)
            || self.notify.generation.load(Ordering::Acquire) != self.generation
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.is_notified() {
            return Poll::Ready(());
        }
        // Registered before the second look, so a notification landing between
        // the two still finds a waker to call.
        self.notify.waiters.push(context.waker());
        if self.is_notified() {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

impl Notified<'_> {
    /// Take a permit, or observe that a broadcast has happened since creation.
    fn is_notified(&self) -> bool {
        if self
            .notify
            .permit
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
        self.notify.generation.load(Ordering::Acquire) != self.generation
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
        Acquire { semaphore: self }
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
        formatter.debug_struct("SemaphorePermit").finish_non_exhaustive()
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
}

impl<'a> Future for Acquire<'a> {
    type Output = SemaphorePermit<'a>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(permit) = self.semaphore.try_acquire() {
            return Poll::Ready(permit);
        }
        self.semaphore.waiters.push(context.waker());
        if let Some(permit) = self.semaphore.try_acquire() {
            return Poll::Ready(permit);
        }
        Poll::Pending
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
        Read { lock: self }
    }

    /// Wait for a write lock.
    pub fn write(&self) -> Write<'_, T> {
        Write { lock: self }
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Wait for a read lock that borrows the `Arc` rather than the lock.
    ///
    /// The guard a caller can hold across an await without naming a lifetime,
    /// which is what `tokio::sync`'s owned guards were used for here.
    pub fn read_owned(self: Arc<Self>) -> ReadOwned<T> {
        ReadOwned { lock: Some(self) }
    }

    /// The write half of [`Self::read_owned`].
    pub fn write_owned(self: Arc<Self>) -> WriteOwned<T> {
        WriteOwned { lock: Some(self) }
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
}

impl<'a, T: ?Sized> Future for Read<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.lock.try_read_raw() {
            return Poll::Ready(RwLockReadGuard { lock: self.lock });
        }
        self.lock.waiters.push(context.waker());
        if self.lock.try_read_raw() {
            return Poll::Ready(RwLockReadGuard { lock: self.lock });
        }
        Poll::Pending
    }
}

/// The future returned by [`RwLock::write`].
pub struct Write<'a, T: ?Sized> {
    lock: &'a RwLock<T>,
}

impl<'a, T: ?Sized> Future for Write<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.lock.try_write_raw() {
            return Poll::Ready(RwLockWriteGuard { lock: self.lock });
        }
        self.lock.waiters.push(context.waker());
        if self.lock.try_write_raw() {
            return Poll::Ready(RwLockWriteGuard { lock: self.lock });
        }
        Poll::Pending
    }
}

/// The future returned by [`RwLock::read_owned`].
pub struct ReadOwned<T: ?Sized> {
    lock: Option<Arc<RwLock<T>>>,
}

impl<T: ?Sized> Future for ReadOwned<T> {
    type Output = OwnedRwLockReadGuard<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let lock = this.lock.as_ref().expect("polled after completion");
        if lock.try_read_raw() {
            let lock = this.lock.take().expect("just checked");
            return Poll::Ready(OwnedRwLockReadGuard { lock });
        }
        lock.waiters.push(context.waker());
        if lock.try_read_raw() {
            let lock = this.lock.take().expect("just checked");
            return Poll::Ready(OwnedRwLockReadGuard { lock });
        }
        Poll::Pending
    }
}

/// The future returned by [`RwLock::write_owned`].
pub struct WriteOwned<T: ?Sized> {
    lock: Option<Arc<RwLock<T>>>,
}

impl<T: ?Sized> Future for WriteOwned<T> {
    type Output = OwnedRwLockWriteGuard<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let lock = this.lock.as_ref().expect("polled after completion");
        if lock.try_write_raw() {
            let lock = this.lock.take().expect("just checked");
            return Poll::Ready(OwnedRwLockWriteGuard { lock });
        }
        lock.waiters.push(context.waker());
        if lock.try_write_raw() {
            let lock = this.lock.take().expect("just checked");
            return Poll::Ready(OwnedRwLockWriteGuard { lock });
        }
        Poll::Pending
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
