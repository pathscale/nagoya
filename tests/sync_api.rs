//! The synchronisation primitives, through the public API.
//!
//! Moved out of the implementation file: these use only what the crate
//! exports, so they are integration tests and belong beside the other ones
//! rather than at the bottom of the module they exercise.

use core::sync::atomic::{AtomicUsize, Ordering};
use nagoya::sync::{Barrier, Notify, RwLock, Semaphore};
use std::sync::Arc;

use nagoya::block_on;

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
    use nagoya::runtime::Runtime;

    let runtime = Runtime::new(4);
    let barrier = Arc::new(Barrier::new(4));
    let passed = Arc::new(AtomicUsize::new(0));
    // Counted before and after, so a barrier that let anyone through early
    // is visible rather than merely suspected.
    let leaders = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..4)
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
