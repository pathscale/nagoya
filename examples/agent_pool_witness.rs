//! Functional CLI acceptance for the injected pool and owner contracts.

use nagoya::credits::{CpuCredits, CreditAccount};
use nagoya::owner::{DispatchDenied, Owner};
use nagoya::readiness::ReadySource;
use nagoya::runtime::Runtime;
use std::cell::Cell;
use std::io::{Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

fn fail(name: &str) -> ! {
    eprintln!("FAIL {name}");
    std::process::exit(1)
}

fn require(condition: bool, name: &str) {
    if !condition { fail(name); }
}

fn thread_cpu_ns() -> u64 {
    let mut value = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: value is a writable timespec; this reads the current worker.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) };
    require(result == 0, "WorkerCpuClockUnavailable");
    (value.tv_sec as u64).saturating_mul(1_000_000_000).saturating_add(value.tv_nsec as u64)
}

struct Agent {
    turns: Cell<u64>,
    active: Arc<AtomicUsize>,
    input: Arc<str>,
}

impl Agent {
    fn turn(&mut self) -> u64 {
        require(self.active.fetch_add(1, Ordering::AcqRel) == 0,
            "ExclusiveOwnerDuplicateAdmission");
        let mut digest = 0xcbf29ce484222325_u64;
        for byte in self.input.bytes() {
            digest = (digest ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
        self.turns.set(self.turns.get() + 1);
        self.active.fetch_sub(1, Ordering::AcqRel);
        std::hint::black_box(digest)
    }
}

type Account = CreditAccount<Arc<str>>;
type AgentOwner = Owner<Agent, Account, CpuCredits>;

fn owner(input: Arc<str>) -> (AgentOwner, Arc<Account>) {
    let account = Arc::new(CreditAccount::new(Arc::from("https://witness.invalid"),
        NonZeroU64::new(1_000_000_000).unwrap(), NonZeroUsize::new(2).unwrap()).unwrap());
    let value = Agent { turns: Cell::new(0), active: Arc::new(AtomicUsize::new(0)), input };
    (Owner::new(value, account.clone(), Arc::new(CpuCredits::new(thread_cpu_ns))), account)
}

fn exclusive(runtime: &Runtime, input: Arc<str>) {
    let (owner, _) = owner(input);
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    let first = owner.dispatch(runtime.executor(), 1_000_000, move |agent| {
        let mut delivery = [0_u8; 1];
        // One bounded witness rendezvous, released by CLI coordinator below.
        // The ownership gate remains held while queued or executing.
        reader.read_exact(&mut delivery).unwrap();
        require(delivery == [7], "OwnerInputDelivery");
        agent.turn();
        agent.turns.get()
    }).unwrap_or_else(|_| fail("OwnerFirstAdmission"));
    let second = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        owner.dispatch(runtime.executor(), 1_000_000, |agent| agent.turn())
    }));
    writer.write_all(&[7]).unwrap();
    require(nagoya::block_on(first) == Some(1), "OwnerTurnOutput");
    match second {
        Ok(Err(rejected)) if rejected.reason == DispatchDenied::Busy => {}
        Ok(Ok(handle)) => {
            let _ = nagoya::block_on(handle);
            fail("ExclusiveOwnerDuplicateAdmission");
        }
        _ => fail("ExclusiveOwnerDuplicateAdmission"),
    }
    let next = owner.dispatch(runtime.executor(), 1_000_000, |agent| {
        agent.turn();
        agent.turns.get()
    }).unwrap_or_else(|_| fail("OwnerHandoffAdmission"));
    require(nagoya::block_on(next) == Some(2), "OwnerHandoffOutput");
    println!("PASS exclusive: one admission, owned state retained through two turns");
}

fn migration(runtime: &Runtime, input: Arc<str>) {
    let width = runtime.pool().workers();
    require(width >= 2, "MigrationRequiresTwoWorkers");
    let (owner, account) = owner(input);
    let observer = runtime.executor().clone_handle();
    let first = owner.dispatch_to(runtime.executor(), Some(0), 1_000_000, move |agent| {
        agent.turn();
        observer.current_worker().unwrap()
    }).unwrap_or_else(|_| fail("MigrationFirstAdmission"));
    let first_worker = nagoya::block_on(first).unwrap();
    let mut migrated = None;
    for attempt in 0..64 {
        let worker = (first_worker + 1 + attempt) % width;
        let observer = runtime.executor().clone_handle();
        let next = owner.dispatch_to(runtime.executor(), Some(worker), 1_000_000, move |agent| {
            agent.turn();
            observer.current_worker().unwrap()
        }).unwrap_or_else(|_| fail("MigrationTurnAdmission"));
        let actual = nagoya::block_on(next).unwrap();
        if actual != first_worker { migrated = Some(actual); break; }
    }
    let second_worker = migrated.unwrap_or_else(|| fail("MigrationNotObserved"));
    require(Arc::ptr_eq(owner.key(), &account), "MigrationChangedOwnerKey");
    let remaining = account.balance();
    require(remaining < 1_000_000_000, "CpuConsumptionNotObserved");
    let denied_credits = if remaining > 0 { remaining as u64 + 1 } else { 1 };
    let rejected = owner.dispatch_to(runtime.executor(), Some(first_worker), denied_credits,
        |agent| agent.turn());
    match rejected {
        Err(rejected) if rejected.reason == DispatchDenied::Budget => {}
        Ok(handle) => {
            let _ = nagoya::block_on(handle);
            fail("BudgetResetByMigration");
        }
        _ => fail("BudgetResetByMigration"),
    }
    let helper = CpuCredits::new(thread_cpu_ns).dispatch(runtime.executor(), &account,
        denied_credits, || 5_u64);
    match helper {
        Err(rejected) if rejected.reason == DispatchDenied::Budget => {}
        Ok(handle) => { let _ = nagoya::block_on(handle); fail("HelperBudgetEscapedOwnerKey"); }
        _ => fail("HelperBudgetEscapedOwnerKey"),
    }
    println!("PASS migration: worker {first_worker} -> {second_worker}, debt and helper cap retained");
}

fn timer(runtime: &Runtime) {
    let mut source = ReadySource::new();
    let producer = source.handle();
    let start = nagoya::now_ns();
    let waits = nagoya::reactor::counters::WAIT.load(Ordering::Acquire);
    let delivery = producer.wake_at(runtime.executor(), start + 500_000_000,
        NonZeroU64::new(50_000_000).unwrap(), 4).unwrap();
    let receive = runtime.spawn(async move { source.wait().await });
    require(nagoya::block_on(receive) == Some(4), "TimerReadyReasonLost");
    require(nagoya::now_ns() >= start + 500_000_000, "TimerDeliveredEarly");
    require(nagoya::block_on(delivery).is_some(), "TimerDeliveryCancelled");
    let waits = nagoya::reactor::counters::WAIT.load(Ordering::Acquire) - waits;
    // A quiet one-shot deadline has only arm/expiry waits. A planted 1 ms
    // inspection interval produces hundreds of kernel waits before delivery.
    require(waits <= 16, "TimerFiresByPolling");
    println!("PASS timer: coarse deadline delivered with {waits} event/deadline waits");
}

fn readiness(runtime: &Runtime, input: Arc<str>) {
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let registration = runtime.readiness().unwrap().register(reader.as_raw_fd(),
        nagoya::reactor::Interest::READABLE).unwrap();
    let expected = input.len();
    let received = runtime.spawn(async move {
        let mut output = Vec::with_capacity(expected);
        let mut chunk = [0_u8; 4096];
        while output.len() < expected {
            let count = futures_util::future::poll_fn(|cx| {
                match reader.read(&mut chunk) {
                    Ok(count) => Poll::Ready(count),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        registration.poll_readable(cx.waker());
                        Poll::Pending
                    }
                    Err(_) => fail("ReadinessReadFailure"),
                }
            }).await;
            require(count != 0, "ReadinessUnexpectedEof");
            output.extend_from_slice(&chunk[..count]);
        }
        output
    });
    let payload = input.clone();
    let sent = runtime.spawn(async move {
        nagoya::sleep(std::time::Duration::from_millis(25)).await;
        writer.write_all(payload.as_bytes()).unwrap();
    });
    let output = nagoya::block_on(received).unwrap();
    require(output == input.as_bytes(), "ReadinessInputMismatch");
    require(nagoya::block_on(sent).is_some(), "ReadinessWriterCancelled");
    println!("PASS readiness: delivered {}", String::from_utf8(output).unwrap());
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| fail("MissingWitnessMode"));
    let input: Arc<str> = Arc::from(args.next().unwrap_or_else(|| fail("MissingWitnessInput")));
    require(!input.is_empty() && input.len() <= 4096, "WitnessInputSize");
    let runtime = Runtime::builder().all_cores().unwrap().install_background().unwrap();
    runtime.readiness().unwrap();
    match mode.as_str() {
        "exclusive" => exclusive(runtime, input),
        "migration" => migration(runtime, input),
        "timer" => timer(runtime),
        "readiness" => readiness(runtime, input),
        _ => fail("UnknownWitnessMode"),
    }
}
