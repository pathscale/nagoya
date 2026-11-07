//! Bounded handoff for the synchronous system name resolver.
//!
//! One process-wide pool owns two workers and admits eight queued lookups.
//! Initialize it during synchronous setup, outside reactor polling. A deadline
//! abandons the result; it cannot interrupt libc. A stalled libc lookup retains
//! its worker, so repeated timeouts do not create additional threads.
use crate::reactor::socket::Addr;
use crate::resolve::{self, ResolveError};
use crate::time::{self, Sleep};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_util::task::AtomicWaker;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;

const WORKERS: usize = 2;
const WAITING: usize = 8;
type ResolverFn = Arc<dyn Fn(&str, u16) -> Result<Vec<Addr>, ResolveError> + Send + Sync>;

/// Failure to start the process-wide system resolver workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolverStartError {
    message: String,
}
impl core::fmt::Display for ResolverStartError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ResolverStartError {}

/// Failure to accept a DNS result before the absolute deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveUntilError {
    /// The monotonic deadline elapsed before a result was accepted.
    Deadline,
    /// The bounded waiting queue was full or momentarily locked.
    Busy,
    /// The broker closed or the reply endpoint disconnected.
    Closed,
    /// The system resolver returned its original typed error.
    Resolve(ResolveError),
}

impl core::fmt::Display for ResolveUntilError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Deadline => f.write_str("DNS deadline elapsed"),
            Self::Busy => f.write_str("DNS admission is busy"),
            Self::Closed => f.write_str("DNS resolver is closed"),
            Self::Resolve(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ResolveUntilError {}

struct Mailbox {
    value: Mutex<Option<Result<Vec<Addr>, ResolveError>>>,
    disconnected: AtomicBool,
    waker: AtomicWaker,
}
struct Reply {
    mailbox: Arc<Mailbox>,
    abandoned: Arc<AtomicBool>,
}
struct ReplySender {
    mailbox: Arc<Mailbox>,
    abandoned: Arc<AtomicBool>,
}
impl ReplySender {
    fn send(self, result: Result<Vec<Addr>, ResolveError>) {
        if !self.abandoned.load(Ordering::Acquire) {
            if let Ok(mut slot) = self.mailbox.value.lock() {
                *slot = Some(result);
            }
        }
        self.mailbox.waker.wake();
    }
}
impl Drop for ReplySender {
    fn drop(&mut self) {
        self.mailbox.disconnected.store(true, Ordering::Release);
        self.mailbox.waker.wake();
    }
}
struct Job {
    host: String,
    port: u16,
    deadline_ns: u64,
    reply: ReplySender,
    abandoned: Arc<AtomicBool>,
}
struct Inner {
    queue: Mutex<VecDeque<Job>>,
    available: Condvar,
    closed: AtomicBool,
    resolver: ResolverFn,
}

/// Process-wide bounded system DNS resolver.
pub struct Resolver {
    inner: Arc<Inner>,
}
static SHARED: OnceLock<Result<Resolver, ResolverStartError>> = OnceLock::new();
impl Resolver {
    /// Start or retrieve the process-wide resolver. Call during synchronous setup.
    pub fn shared() -> Result<&'static Self, ResolverStartError> {
        match SHARED.get_or_init(|| Self::start(Arc::new(resolve::resolve))) {
            Ok(resolver) => Ok(resolver),
            Err(error) => Err(error.clone()),
        }
    }

    /// Resolve a name and accept a result only before `deadline_ns`.
    ///
    /// The deadline uses [`crate::time::now_ns`]'s monotonic nanosecond clock.
    /// Dropping the future abandons admission or delivery without waiting for
    /// libc. Timers remain cooperative with the executor. Busy admission is an
    /// immediate error; callers must not retry in an unbounded polling loop.
    pub async fn resolve_until(
        &self,
        host: &str,
        port: u16,
        deadline_ns: u64,
    ) -> Result<Vec<Addr>, ResolveUntilError> {
        ResolveUntil {
            resolver: self,
            host,
            port,
            deadline_ns,
            reply: None,
            timer: Box::pin(time::sleep_until(deadline_ns)),
            done: false,
        }
        .await
    }

    fn start(resolver: ResolverFn) -> Result<Self, ResolverStartError> {
        let inner = Arc::new(Inner {
            queue: Mutex::new(VecDeque::with_capacity(WAITING)),
            available: Condvar::new(),
            closed: AtomicBool::new(false),
            resolver,
        });
        let mut handles = Vec::with_capacity(WORKERS);
        for index in 0..WORKERS {
            let worker_inner = Arc::clone(&inner);
            match thread::Builder::new()
                .name(format!("nagoya-dns-{index}"))
                .spawn(move || worker(worker_inner))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    // Publish the predicate under the same mutex workers use for wait.
                    if let Ok(_queue) = inner.queue.lock() {
                        inner.closed.store(true, Ordering::Release);
                    }
                    inner.available.notify_all();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(ResolverStartError {
                        message: format!("could not start DNS worker: {error}"),
                    });
                }
            }
        }
        drop(handles); // Shared workers are detached and process-lifetime.
        Ok(Self { inner })
    }

    /// Permanently close admission and drain queued requests without joining workers.
    /// A call already inside libc finishes on its worker; this resolver cannot restart.
    pub fn close(&self) {
        let drained = match self.inner.queue.lock() {
            Ok(mut queue) => {
                self.inner.closed.store(true, Ordering::Release);
                queue.drain(..).collect::<Vec<_>>()
            }
            Err(poisoned) => {
                self.inner.closed.store(true, Ordering::Release);
                poisoned.into_inner().drain(..).collect::<Vec<_>>()
            }
        };
        self.inner.available.notify_all();
        drop(drained); // Endpoint drops and wakes are outside the queue lock.
    }

    fn enqueue(
        &self,
        host: String,
        port: u16,
        deadline_ns: u64,
    ) -> Result<Reply, ResolveUntilError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(ResolveUntilError::Closed);
        }
        let mailbox = Arc::new(Mailbox {
            value: Mutex::new(None),
            disconnected: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        });
        let abandoned = Arc::new(AtomicBool::new(false));
        let job = Job {
            host,
            port,
            deadline_ns,
            reply: ReplySender {
                mailbox: Arc::clone(&mailbox),
                abandoned: Arc::clone(&abandoned),
            },
            abandoned: Arc::clone(&abandoned),
        };
        let mut queue = match self.inner.queue.try_lock() {
            Ok(q) => q,
            Err(std::sync::TryLockError::Poisoned(_)) => return Err(ResolveUntilError::Closed),
            Err(std::sync::TryLockError::WouldBlock) => return Err(ResolveUntilError::Busy),
        };
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(ResolveUntilError::Closed);
        }
        if queue.len() == WAITING {
            return Err(ResolveUntilError::Busy);
        }
        queue.push_back(job);
        drop(queue);
        self.inner.available.notify_one();
        Ok(Reply { mailbox, abandoned })
    }
}

struct ResolveUntil<'a> {
    resolver: &'a Resolver,
    host: &'a str,
    port: u16,
    deadline_ns: u64,
    reply: Option<Reply>,
    timer: Pin<Box<Sleep>>,
    done: bool,
}
impl ResolveUntil<'_> {
    fn terminal(
        &mut self,
        outcome: Result<Vec<Addr>, ResolveUntilError>,
    ) -> Poll<Result<Vec<Addr>, ResolveUntilError>> {
        self.done = true;
        if time::now_ns() >= self.deadline_ns {
            self.abandon();
            Poll::Ready(Err(ResolveUntilError::Deadline))
        } else {
            Poll::Ready(outcome)
        }
    }
    fn abandon(&mut self) {
        if let Some(reply) = self.reply.take() {
            reply.abandoned.store(true, Ordering::Release);
            drop(reply.mailbox.waker.take());
        }
    }
}
impl Drop for ResolveUntil<'_> {
    fn drop(&mut self) {
        self.abandon();
    }
}
impl Future for ResolveUntil<'_> {
    type Output = Result<Vec<Addr>, ResolveUntilError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if this.done {
            return this.terminal(Err(ResolveUntilError::Closed));
        }
        if time::now_ns() >= this.deadline_ns {
            this.abandon();
            this.done = true;
            return Poll::Ready(Err(ResolveUntilError::Deadline));
        }
        if this.reply.is_none() {
            match this
                .resolver
                .enqueue(this.host.to_owned(), this.port, this.deadline_ns)
            {
                Ok(reply) => this.reply = Some(reply),
                Err(error) => return this.terminal(Err(error)),
            }
        }
        // try_lock only on the reactor. Register then recheck closes the lost-wakeup window.
        match poll_mailbox(this.reply.as_ref().unwrap(), cx) {
            Some(Ok(result)) => return this.terminal(result.map_err(ResolveUntilError::Resolve)),
            Some(Err(())) => return this.terminal(Err(ResolveUntilError::Closed)),
            None => {}
        }
        if this.timer.as_mut().poll(cx).is_ready() || time::now_ns() >= this.deadline_ns {
            this.abandon();
            this.done = true;
            return Poll::Ready(Err(ResolveUntilError::Deadline));
        }
        Poll::Pending
    }
}
fn poll_mailbox(
    reply: &Reply,
    cx: &mut Context<'_>,
) -> Option<Result<Result<Vec<Addr>, ResolveError>, ()>> {
    let mut registered = false;
    for _ in 0..2 {
        let mut slot = match reply.mailbox.value.try_lock() {
            Ok(slot) => slot,
            Err(std::sync::TryLockError::WouldBlock) => {
                if !registered {
                    reply.mailbox.waker.register(cx.waker());
                    registered = true;
                    continue;
                } else {
                    return None;
                }
            }
            Err(std::sync::TryLockError::Poisoned(_)) => return Some(Err(())),
        };
        if let Some(value) = slot.take() {
            return Some(Ok(value));
        }
        if reply.mailbox.disconnected.load(Ordering::Acquire) {
            return Some(Err(()));
        }
        drop(slot);
        if !registered {
            reply.mailbox.waker.register(cx.waker());
            registered = true;
        }
    }
    None
}

fn worker(inner: Arc<Inner>) {
    loop {
        let job = {
            let mut queue = match inner.queue.lock() {
                Ok(q) => q,
                Err(_) => return,
            };
            while queue.is_empty() && !inner.closed.load(Ordering::Acquire) {
                queue = match inner.available.wait(queue) {
                    Ok(q) => q,
                    Err(_) => return,
                };
            }
            if inner.closed.load(Ordering::Acquire) {
                return;
            }
            queue.pop_front()
        };
        let Some(job) = job else { continue };
        if job.abandoned.load(Ordering::Acquire) || time::now_ns() >= job.deadline_ns {
            drop(job.reply);
            continue;
        }
        let result = (inner.resolver)(&job.host, job.port);
        if !job.abandoned.load(Ordering::Acquire) && time::now_ns() < job.deadline_ns {
            job.reply.send(result);
        } else {
            drop(job.reply);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Wake, Waker};
    use std::time::Duration;
    struct Signal(std::sync::mpsc::Sender<()>);
    impl Wake for Signal {
        fn wake(self: Arc<Self>) {
            let _ = self.0.send(());
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.0.send(());
        }
    }
    fn context() -> (Context<'static>, std::sync::mpsc::Receiver<()>) {
        let (tx, rx) = std::sync::mpsc::channel();
        let waker = Box::leak(Box::new(Waker::from(Arc::new(Signal(tx)))));
        (Context::from_waker(waker), rx)
    }
    fn addrs(port: u16) -> Result<Vec<Addr>, ResolveError> {
        Ok(vec![
            Addr::V4([127, 0, 0, 1], port),
            Addr::V6Scoped([0; 16], port, 3),
        ])
    }
    fn send_assert<T: Send>(_: T) {}

    fn receive_entries(
        receiver: &std::sync::mpsc::Receiver<u16>,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<u16>, std::sync::mpsc::RecvTimeoutError> {
        let deadline = std::time::Instant::now() + timeout;
        let mut ports = Vec::with_capacity(count);
        for _ in 0..count {
            ports.push(
                receiver
                    .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))?,
            );
        }
        Ok(ports)
    }

    struct EntryReleaseGate {
        entered: std::sync::mpsc::Sender<u16>,
        releases: Mutex<std::collections::HashMap<u16, std::sync::mpsc::Receiver<()>>>,
    }
    impl EntryReleaseGate {
        fn publish_and_wait(&self, port: u16) {
            let _ = self.entered.send(port);
            self.wait_for_release(port);
        }

        fn wait_for_release(&self, port: u16) {
            self.releases
                .lock()
                .unwrap()
                .remove(&port)
                .expect("each held worker has a release channel")
                .recv_timeout(Duration::from_secs(5))
                .expect("held worker receives bounded release");
        }
    }

    struct WorkerReleaseGuard<'a> {
        resolver: &'a Resolver,
        senders: Vec<std::sync::mpsc::Sender<()>>,
    }
    impl WorkerReleaseGuard<'_> {
        fn release(&self) {
            for sender in &self.senders {
                let _ = sender.send(());
            }
        }
    }
    impl Drop for WorkerReleaseGuard<'_> {
        fn drop(&mut self) {
            self.release();
            self.resolver.close();
        }
    }

    #[test]
    fn expired_and_closed_requests_never_call_the_system_resolver() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker_calls = calls.clone();
        let resolver = Resolver::start(Arc::new(move |_, port| {
            worker_calls.fetch_add(1, Ordering::SeqCst);
            addrs(port)
        }))
        .unwrap();
        assert_eq!(
            crate::block_on(resolver.resolve_until("unused", 80, time::now_ns())),
            Err(ResolveUntilError::Deadline)
        );
        resolver.close();
        assert_eq!(
            crate::block_on(resolver.resolve_until(
                "unused",
                80,
                time::now_ns().saturating_add(2_000_000_000)
            )),
            Err(ResolveUntilError::Closed)
        );
        assert_eq!(
            crate::block_on(resolver.resolve_until("unused", 80, time::now_ns())),
            Err(ResolveUntilError::Deadline)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    fn admit<'a>(
        resolver: &'a Resolver,
        host: &'a str,
        port: u16,
        deadline: u64,
        cx: &mut Context<'_>,
    ) -> Pin<Box<ResolveUntil<'a>>> {
        loop {
            assert!(time::now_ns() < deadline, "admission deadline");
            let mut future = Box::pin(ResolveUntil {
                resolver,
                host,
                port,
                deadline_ns: deadline,
                reply: None,
                timer: Box::pin(time::sleep_until(deadline)),
                done: false,
            });
            match future.as_mut().poll(cx) {
                Poll::Pending => return future,
                Poll::Ready(Err(ResolveUntilError::Busy)) => std::thread::yield_now(),
                other => panic!("unexpected admission: {other:?}"),
            }
        }
    }

    #[test]
    fn async_surface_is_send_and_mailbox_wakes_actual_future() {
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let resolver = Resolver::start(Arc::new(move |_, port| {
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            addrs(port)
        }))
        .expect("workers");
        send_assert(resolver.resolve_until(
            "fixture.invalid",
            443,
            time::now_ns().saturating_add(2_000_000_000),
        ));
        let (mut cx, wakes) = context();
        let deadline = time::now_ns().saturating_add(2_000_000_000);
        let mut future = admit(&resolver, "fixture.invalid", 443, deadline, &mut cx);
        release_tx.send(()).unwrap();
        wakes
            .recv_timeout(Duration::from_secs(1))
            .expect("mailbox wake");
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(found)) => assert_eq!(found, addrs(443).unwrap()),
            other => panic!("unexpected result: {other:?}"),
        }
        resolver.close();
    }

    #[test]
    fn saturation_expiry_and_recovery_keep_worker_bound() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let mut release_senders = Vec::with_capacity(WORKERS);
        let mut release_receivers = std::collections::HashMap::with_capacity(WORKERS);
        for port in 1..=WORKERS as u16 {
            let (sender, receiver) = std::sync::mpsc::channel();
            release_senders.push(sender);
            release_receivers.insert(port, receiver);
        }
        let gate = Arc::new(EntryReleaseGate {
            entered: entered_tx,
            releases: Mutex::new(release_receivers),
        });
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let a = active.clone();
        let p = peak.clone();
        let c = calls.clone();
        let worker_gate = Arc::clone(&gate);
        let resolver = Resolver::start(Arc::new(move |_, port| {
            if c.fetch_add(1, Ordering::SeqCst) < WORKERS {
                let now = a.fetch_add(1, Ordering::SeqCst) + 1;
                p.fetch_max(now, Ordering::SeqCst);
                // Teardown can close the entry receiver after sending release.
                // A late worker must still consume that buffered release signal.
                worker_gate.publish_and_wait(port);
                a.fetch_sub(1, Ordering::SeqCst);
            }
            addrs(port)
        }))
        .expect("workers");
        let release_guard = WorkerReleaseGuard {
            resolver: &resolver,
            senders: release_senders,
        };
        let (mut cx, _) = context();
        let deadline = time::now_ns().saturating_add(5_000_000_000);
        let mut active_futures = Vec::new();
        for port in 1..=WORKERS as u16 {
            active_futures.push(admit(&resolver, "held", port, deadline, &mut cx));
        }
        let mut entered_ports = receive_entries(&entered_rx, WORKERS, Duration::from_secs(2))
            .expect("all resolver workers enter before the fixture timeout");
        entered_ports.sort_unstable();
        assert_eq!(entered_ports, (1..=WORKERS as u16).collect::<Vec<_>>());
        let mut queued = Vec::new();
        for port in 10..(10 + WAITING as u16) {
            let mut f = Box::pin(ResolveUntil {
                resolver: &resolver,
                host: "queued",
                port,
                deadline_ns: deadline,
                reply: None,
                timer: Box::pin(time::sleep_until(deadline)),
                done: false,
            });
            assert!(f.as_mut().poll(&mut cx).is_pending());
            queued.push(f);
        }
        let mut overflow = Box::pin(ResolveUntil {
            resolver: &resolver,
            host: "overflow",
            port: 99,
            deadline_ns: deadline,
            reply: None,
            timer: Box::pin(time::sleep_until(deadline)),
            done: false,
        });
        assert!(matches!(
            overflow.as_mut().poll(&mut cx),
            Poll::Ready(Err(ResolveUntilError::Busy))
        ));
        assert_eq!(peak.load(Ordering::SeqCst), WORKERS);
        let queued_mailboxes: Vec<_> = queued
            .iter()
            .map(|future| Arc::clone(&future.reply.as_ref().unwrap().mailbox))
            .collect();
        drop(queued); // Dropped queued requests must be skipped when workers are released.
        assert!(
            queued_mailboxes
                .iter()
                .all(|mailbox| mailbox.waker.take().is_none()),
            "cancelled requests must release task wakers while workers remain held"
        );
        release_guard.release();
        let mut finished = vec![false; WORKERS];
        while time::now_ns() < deadline {
            for (index, f) in active_futures.iter_mut().enumerate() {
                if !finished[index] {
                    finished[index] = matches!(f.as_mut().poll(&mut cx), Poll::Ready(Ok(_)));
                }
            }
            if finished.iter().all(|done| *done) {
                break;
            }
            std::thread::yield_now();
        }
        assert!(
            finished.iter().all(|done| *done),
            "active futures did not complete"
        );
        assert_eq!(calls.load(Ordering::SeqCst), WORKERS);
        let recovery_deadline = time::now_ns().saturating_add(2_000_000_000);
        while time::now_ns() < recovery_deadline {
            let mut recovery = Box::pin(resolver.resolve_until("fresh", 443, recovery_deadline));
            loop {
                if time::now_ns() >= recovery_deadline {
                    break;
                }
                match recovery.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(found)) => {
                        assert_eq!(found, addrs(443).unwrap());
                        assert_eq!(calls.load(Ordering::SeqCst), WORKERS + 1);
                        return;
                    }
                    Poll::Ready(Err(ResolveUntilError::Busy)) => break,
                    Poll::Ready(other) => panic!("unexpected recovery result: {other:?}"),
                    Poll::Pending => {
                        if time::now_ns() >= recovery_deadline {
                            break;
                        }
                        std::thread::yield_now();
                    }
                }
            }
        }
        panic!("workers did not recover within bounded polling");
    }

    #[test]
    fn suppressed_entry_is_detected_within_the_two_second_gate() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (called_tx, called_rx) = std::sync::mpsc::channel();
        let mut release_senders = Vec::with_capacity(WORKERS);
        let mut release_receivers = std::collections::HashMap::with_capacity(WORKERS);
        for port in 1..=WORKERS as u16 {
            let (sender, receiver) = std::sync::mpsc::channel();
            release_senders.push(sender);
            release_receivers.insert(port, receiver);
        }
        let gate = Arc::new(EntryReleaseGate {
            entered: entered_tx,
            releases: Mutex::new(release_receivers),
        });
        let worker_gate = Arc::clone(&gate);
        let resolver = Resolver::start(Arc::new(move |_, port| {
            let _ = called_tx.send(port);
            if port == 2 {
                worker_gate.wait_for_release(port);
            } else {
                worker_gate.publish_and_wait(port);
            }
            addrs(port)
        }))
        .expect("workers");
        let release_guard = WorkerReleaseGuard {
            resolver: &resolver,
            senders: release_senders,
        };
        let (mut cx, _) = context();
        let deadline = time::now_ns().saturating_add(5_000_000_000);
        let _first = admit(&resolver, "fault-injected", 1, deadline, &mut cx);
        let _second = admit(&resolver, "fault-injected", 2, deadline, &mut cx);
        let mut called = vec![
            called_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("first callback"),
            called_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("second callback"),
        ];
        called.sort_unstable();
        assert_eq!(called, vec![1, 2]);
        let started = std::time::Instant::now();
        let result = receive_entries(&entered_rx, WORKERS, Duration::from_secs(2));
        assert!(matches!(
            result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(started.elapsed() <= Duration::from_secs(3));
        release_guard.release();
        resolver.close();
    }

    #[test]
    fn late_entry_after_teardown_still_consumes_release() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let start_rx = Mutex::new(start_rx);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut release_receivers = std::collections::HashMap::new();
        release_receivers.insert(1, release_rx);
        let gate = Arc::new(EntryReleaseGate {
            entered: entered_tx,
            releases: Mutex::new(release_receivers),
        });
        let worker_gate = Arc::clone(&gate);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (exited_tx, exited_rx) = std::sync::mpsc::channel();
        let resolver = Resolver::start(Arc::new(move |_, port| {
            if port == 1 {
                let _ = started_tx.send(());
                start_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(3))
                    .expect("test resumes callback after teardown");
                worker_gate.publish_and_wait(port);
                let _ = exited_tx.send(());
            }
            addrs(port)
        }))
        .expect("workers");
        let (mut cx, _) = context();
        let deadline = time::now_ns().saturating_add(5_000_000_000);
        let future = admit(&resolver, "late-entry", 1, deadline, &mut cx);
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("callback started before dropping its admitted future");
        drop(future);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = WorkerReleaseGuard {
                resolver: &resolver,
                senders: vec![release_tx],
            };
            panic!("simulate fixture unwind before explicit release");
        }));
        assert!(result.is_err());
        drop(entered_rx);
        start_tx.send(()).unwrap();
        exited_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("late worker exits after receiver teardown");
    }
}
