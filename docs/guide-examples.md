Rust examples from the Typst user guide.

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::{block_on, runtime::Runtime};
let runtime = Runtime::new(2);
let task = runtime.spawn(async { 40 + 2 });
assert_eq!(block_on(task), Some(42));
runtime.pool().shut_down();
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::{block_on, runtime::Runtime};
use std::sync::Arc;
let runtime = Runtime::new(2);
let numbers = Arc::new(vec![2, 3, 5]);
let input = numbers.clone();
let task = runtime.executor().spawn(async move { input.iter().sum::<u64>() });
assert_eq!(block_on(task), Some(10));
runtime.pool().shut_down();
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::runtime::Runtime;
use std::sync::mpsc;
use std::time::Duration;
let runtime = Runtime::new(2);
let (send, receive) = mpsc::channel();
runtime.executor().submit(move || send.send(42).unwrap());
assert_eq!(receive.recv_timeout(Duration::from_secs(2)).unwrap(), 42);
runtime.pool().shut_down();
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
nagoya::block_on(async {
    nagoya::yield_now().await;
    assert_eq!(6 * 7, 42);
});
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::{block_on, par_for, runtime::Runtime};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
let runtime = Runtime::new(2);
let sum = Arc::new(AtomicUsize::new(0));
let output = sum.clone();
block_on(par_for(runtime.pool().clone(), 0..1000, move |i| {
    output.fetch_add(i, Ordering::Relaxed);
}).leaf(64));
assert_eq!(sum.load(Ordering::Relaxed), 999 * 1000 / 2);
runtime.pool().shut_down();
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::{block_on, par_for, Cancel, runtime::Runtime};
let runtime = Runtime::new(2);
let token = Cancel::new();
token.cancel();
let work = par_for(runtime.pool().clone(), 0..100, |_| {
    panic!("a canceled loop must not begin this body")
}).cancel_with(token.clone());
assert!(token.is_cancelled());
block_on(work);
runtime.pool().shut_down();
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use std::time::Duration;
nagoya::block_on(async {
    let sleep = nagoya::sleep(Duration::from_millis(1));
    let deadline = sleep.deadline();
    sleep.await;
    assert!(nagoya::now_ns() >= deadline);
    nagoya::sleep_until(nagoya::now_ns()).await;
    assert_eq!(nagoya::timeout(Duration::from_secs(1), async { 42 }).await, Ok(42));
});
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::sync::RwLock;
use std::sync::Arc;
nagoya::block_on(async {
    let lock = Arc::new(RwLock::new(1usize));
    *lock.write().await += 1;
    assert_eq!(*lock.read().await, 2);
    let owned = lock.clone().read_owned().await;
    assert_eq!(*owned, 2);
    assert!(lock.try_write().is_none());
    drop(owned);
    *lock.clone().write_owned().await = 3;
});
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let signal = nagoya::sync::Notify::new();
signal.notify_one();
nagoya::block_on(signal.notified());
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
assert!(nagoya::block_on(nagoya::sync::Barrier::new(1).wait()));
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let semaphore = nagoya::sync::Semaphore::new(2);
let permit = semaphore.try_acquire().unwrap();
assert_eq!(semaphore.available_permits(), 1);
drop(permit);
assert_eq!(semaphore.available_permits(), 2);
# Ok(())
# }
```

```rust,no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::io::{Read, Write, Seek, SeekFrom, File};
nagoya::block_on(async {
    let mut file = nagoya::io::create("example.data").await?;
    file.write_all(b"hello").await?;
    file.flush().await?;
    file.sync_data().await?;
    file.seek(SeekFrom::Start(0)).await?;
    let mut bytes = [0u8; 5];
    file.read_exact(&mut bytes).await?;
    assert_eq!(&bytes, b"hello");
    Ok::<_, nagoya::io::Error>(())
})?;
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::Executor;
use st3::fanout::{Pool, StdHost};
use std::{sync::Arc, thread};
let pool = Pool::new(1, 256, Arc::new(StdHost::new(1)));
let executor = Executor::new(pool.clone());
let worker_executor = executor.clone_handle();
let runner = pool.runner(0);
let worker = thread::spawn(move || worker_executor.run_worker(runner));
assert_eq!(nagoya::block_on(executor.spawn(async { 42 })), Some(42));
pool.shut_down();
assert!(worker.join().unwrap());
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::block_on_with_host;
use st3::fanout::AtomicHost;
use std::sync::Arc;
// This example uses no timers, so its clock is unused.
let caller = Arc::new(AtomicHost::new(1, || 0));
let answer = block_on_with_host(async {
    nagoya::yield_now().await;
    42
}, caller, 0);
assert_eq!(answer, 42);
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let rt = nagoya::runtime::Runtime::with_tuning(
    8, nagoya::Tuning::almost_tokio(), "almost_tokio");
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let rt = nagoya::runtime::Runtime::builder()
    .workers(2)
    .label("deep")
    .stack_size(16 * 1024 * 1024)
    .build();
rt.pool().shut_down();
# Ok(())
# }
```

```rust
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use nagoya::{block_on, par_for, runtime::Runtime};
use std::sync::{mpsc, Arc, atomic::{AtomicUsize, Ordering}};

let runtime = Runtime::new(4);
assert_eq!(block_on(runtime.spawn(async { 21 * 2 })), Some(42));

let (send, receive) = mpsc::channel();
runtime.executor().submit(move || send.send(42).unwrap());
assert_eq!(receive.recv().unwrap(), 42);

let total = Arc::new(AtomicUsize::new(0));
let output = total.clone();
block_on(par_for(runtime.pool().clone(), 0..1000, move |i| {
    output.fetch_add(i, Ordering::Relaxed);
}).leaf(64));
assert_eq!(total.load(Ordering::Relaxed), 499_500);
runtime.pool().shut_down();
# Ok(())
# }
```

