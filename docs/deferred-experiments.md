# Scoped fork experiment review

The 0.1.2 alpha includes owned tasks and synchronous submission. It does not include the borrowed scope experiment in [e176030](https://github.com/pathscale/nagoya/commit/e176030). That source remains on the remote branch for future work; it must not be presented as a supported runtime feature.

Review on 11 September 2026 found three release blockers in that experiment:

- A panic in a spawned closure bypasses the outstanding-work decrement. Scope teardown can wait forever, including during unwinding. Completion accounting needs an unwind-safe guard.
- A panic in a blocking hook or wait callback bypasses the registry unblocking step. Counts no longer describe worker availability. Restoring those counts needs an unwind-safe guard and a defined callback panic policy.
- Scope waits spin without helping queued jobs. Nested joins can occupy every worker while their children remain queued. Deadlock notification alone does not provide execution progress.

The unsafe lifetime erasure also needs a complete soundness review covering escaping scopes, nested borrowing, panic propagation, submission failure, and shutdown before this can become public API. Tests must establish both borrowed-lifetime safety and progress on a saturated small pool. The user guide accurately excludes scoped borrowing from this alpha.
