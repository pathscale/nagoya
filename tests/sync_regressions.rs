//! Deterministic polling tests: no executor scheduling or sleeps are involved.
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use nagoya::sync::{Notify, RwLock, Semaphore};

#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn counter() -> (Arc<Count>, Waker) {
    let count = Arc::new(Count::default());
    let waker = Waker::from(count.clone());
    (count, waker)
}

fn poll<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

#[test]
fn canceling_the_first_acquirer_does_not_consume_the_only_wake() {
    let semaphore = Semaphore::new(0);
    let (a_count, a_waker) = counter();
    let (b_count, b_waker) = counter();
    let mut a = Box::pin(semaphore.acquire());
    let mut b = Box::pin(semaphore.acquire());
    assert!(poll(a.as_mut(), &a_waker).is_pending());
    assert!(poll(b.as_mut(), &b_waker).is_pending());
    drop(a);
    semaphore.add_permits(1);
    assert_eq!(a_count.0.load(Ordering::Relaxed), 0);
    assert_eq!(b_count.0.load(Ordering::Relaxed), 1);
    assert!(poll(b.as_mut(), &b_waker).is_ready());
}

#[test]
fn canceling_an_awakened_acquirer_hands_the_wake_on() {
    let semaphore = Semaphore::new(0);
    let (_, a_waker) = counter();
    let (b_count, b_waker) = counter();
    let mut a = Box::pin(semaphore.acquire());
    let mut b = Box::pin(semaphore.acquire());
    assert!(poll(a.as_mut(), &a_waker).is_pending());
    assert!(poll(b.as_mut(), &b_waker).is_pending());
    semaphore.add_permits(1);
    drop(a);
    assert_eq!(b_count.0.load(Ordering::Relaxed), 1);
    assert!(poll(b.as_mut(), &b_waker).is_ready());
}

#[test]
fn two_notifications_belong_to_two_registered_waiters() {
    let notify = Notify::new();
    // A task may await multiple futures with the very same waker.
    let (_, waker) = counter();
    let mut a = Box::pin(notify.notified());
    let mut b = Box::pin(notify.notified());
    assert!(poll(a.as_mut(), &waker).is_pending());
    assert!(poll(b.as_mut(), &waker).is_pending());
    notify.notify_one();
    notify.notify_one();
    assert!(poll(a.as_mut(), &waker).is_ready());
    assert!(poll(b.as_mut(), &waker).is_ready());
    assert!(poll(Box::pin(notify.notified()).as_mut(), &waker).is_pending());
}

#[test]
fn enable_registers_each_waiter_without_needing_a_waker() {
    let notify = Notify::new();
    let (_, waker) = counter();
    let mut a = Box::pin(notify.notified());
    let mut b = Box::pin(notify.notified());
    assert!(!a.as_mut().enable());
    assert!(!b.as_mut().enable());
    notify.notify_one();
    notify.notify_one();
    assert!(poll(a.as_mut(), &waker).is_ready());
    assert!(poll(b.as_mut(), &waker).is_ready());
}

#[test]
fn canceling_a_notified_waiter_transfers_its_notification() {
    let notify = Notify::new();
    let (_, a_waker) = counter();
    let (b_count, b_waker) = counter();
    let mut a = Box::pin(notify.notified());
    let mut b = Box::pin(notify.notified());
    assert!(poll(a.as_mut(), &a_waker).is_pending());
    assert!(poll(b.as_mut(), &b_waker).is_pending());
    notify.notify_one();
    drop(a);
    assert_eq!(b_count.0.load(Ordering::Relaxed), 1);
    assert!(poll(b.as_mut(), &b_waker).is_ready());
}

#[test]
fn canceled_assigned_notification_becomes_a_spare_permit() {
    let notify = Notify::new();
    let (_, waker) = counter();
    let mut a = Box::pin(notify.notified());
    assert!(!a.as_mut().enable());
    notify.notify_one();
    drop(a);
    assert!(poll(Box::pin(notify.notified()).as_mut(), &waker).is_ready());
    assert!(poll(Box::pin(notify.notified()).as_mut(), &waker).is_pending());
}

#[test]
fn dropping_an_enabled_ready_future_returns_its_reserved_permit() {
    let notify = Notify::new();
    let (_, waker) = counter();
    notify.notify_one();
    let mut a = Box::pin(notify.notified());
    assert!(a.as_mut().enable());
    // enable acknowledges readiness but has not delivered the result to poll.
    drop(a);
    assert!(poll(Box::pin(notify.notified()).as_mut(), &waker).is_ready());
    assert!(poll(Box::pin(notify.notified()).as_mut(), &waker).is_pending());
}

#[test]
fn a_repolled_waiter_updates_its_waker_without_duplicate_registration() {
    let semaphore = Semaphore::new(0);
    let (old_count, old) = counter();
    let (new_count, new) = counter();
    let mut acquire = Box::pin(semaphore.acquire());
    assert!(poll(acquire.as_mut(), &old).is_pending());
    assert!(poll(acquire.as_mut(), &new).is_pending());
    semaphore.add_permits(1);
    assert_eq!(old_count.0.load(Ordering::Relaxed), 0);
    assert_eq!(new_count.0.load(Ordering::Relaxed), 1);
    assert!(poll(acquire.as_mut(), &new).is_ready());
}

#[test]
fn broadcasts_reach_old_futures_but_not_new_ones() {
    let notify = Notify::new();
    let (_, waker) = counter();
    let mut unpolled = Box::pin(notify.notified());
    let mut registered = Box::pin(notify.notified());
    assert!(poll(registered.as_mut(), &waker).is_pending());
    notify.notify_waiters();
    assert!(poll(unpolled.as_mut(), &waker).is_ready());
    assert!(poll(registered.as_mut(), &waker).is_ready());
    assert!(poll(Box::pin(notify.notified()).as_mut(), &waker).is_pending());
}

#[test]
fn an_old_broadcast_does_not_consume_a_new_spare_notification() {
    let notify = Notify::new();
    let (_, waker) = counter();
    let mut old = Box::pin(notify.notified());
    notify.notify_waiters();
    notify.notify_one();
    assert!(poll(old.as_mut(), &waker).is_ready());
    assert!(poll(Box::pin(notify.notified()).as_mut(), &waker).is_ready());
    assert!(poll(Box::pin(notify.notified()).as_mut(), &waker).is_pending());
}

#[test]
fn canceling_lock_waits_releases_their_wakers_and_reuses_slots() {
    let lock = Arc::new(RwLock::new(0));
    let held = lock.try_write().unwrap();
    let (count, waker) = counter();
    for _ in 0..128 {
        let mut read = Box::pin(lock.clone().read_owned());
        assert!(poll(read.as_mut(), &waker).is_pending());
        drop(read);
        assert_eq!(Arc::strong_count(&count), 2);
    }
    drop(held);
    assert_eq!(count.0.load(Ordering::Relaxed), 0);
    assert!(lock.try_write().is_some());
}
