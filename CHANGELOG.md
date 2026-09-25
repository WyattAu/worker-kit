# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project adheres to [Semantic Versioning](https://semver.org).

## [0.1.0] — 2026-09-26

### Added

- `WorkerSupervisor::new(ShutdownGuard)` → `.register(JobSpec)` →
  `.run()`: registers periodic jobs, runs their loops, drains on
  shutdown (30 s cap), and reports a name-ordered `RunReport`.
- `Job` closure contract (`Fn(JobContext) -> BoxFuture<Result<(),
  JobError>>`), with panics caught and counted as failures.
- `Trigger::Interval(Duration)` with full-jitter delays
  (`JitterPolicy`, default fraction 0.2, per-worker `SmallRng` seeded
  from wall-clock nanos) and coalescing: a run outlasting its interval
  fires the next immediately after completion, never stacked.
- `Trigger::Cron(String)` behind the default-off `cron` feature —
  parsed eagerly at registration, evaluated in UTC (documented DST
  limitations), `Trigger::next_fire_after` exposed for tests and
  capacity planning.
- Failure budgets: consecutive failures (default 5, `u32`) mark a job
  `Degraded` in status while it keeps running; a success resets.
- `breaker` feature (default ON): per-job estate `breaker = "2"`
  circuit breaker wrapping the closure — clustered failures pause via
  open/half-open, paused attempts consume neither the failure budget
  nor the breaker's budget, `breaker_state`/`breaker_metrics`
  accessors. The fire match is total under any breaker feature
  unification (wildcard arm, per the outbox-kit 0.1.1 lesson).
- `leader` feature (default OFF): `Lease` trait (async
  acquire/renew) + `MemoryLease` (always wins) + `RedisLease`
  (`redis` feature, `SET NX PX` acquisition and an atomic
  compare-and-expire renewal over a `ConnectionManager`). Non-leaders
  skip their fires — recorded, never failures.
- Live `status()` snapshots (fires, failures, skips, paused,
  consecutive failures, degraded, last error).
- Hermetic test suite (coalescing, jitter-bound proptest, degraded
  budgets, breaker pause/half-open, leader skips, drain < 2 s, report
  exactness, name validation, cron monotonicity) plus an
  `#[ignore]`-gated Docker testcontainer suite for the Redis lease.
- Criterion benches for the jitter draw, name validation, and cron
  next-fire computation.

[0.1.0]: https://github.com/WyattAu/worker-kit/releases/tag/v0.1.0
