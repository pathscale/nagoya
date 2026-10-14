# One process, one injected execution domain

Source baseline: nagoya master `4cae808eccec503107bd8a9769f385e0ddbdbfdd`,
version 0.1.16. The owner's 2026-10-14 ruling overrides contradictory adaptive
compilation language in the coordinator documents. Suspected attack means
throttle or kill that agent. These changes introduce no compile tier.

Start once, before libraries submit work or poll a pending timer:

```rust
let builder = nagoya::runtime::Runtime::builder().all_cores()?;
let runtime = builder.install_background()?;
let readiness = runtime.readiness()?;
let executor = runtime.executor().clone_handle();
