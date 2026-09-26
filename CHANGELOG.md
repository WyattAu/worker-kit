# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

## [0.2.0] - 2026-09-26

### Added
- `JobSpec::fire_at_start` — immediate first fire before the first
  scheduled tick (startup-pass jobs; tokio-interval semantics opt-in).
- `JobSpec::drain_pass` — one final fire after shutdown is observed, for
  flush-then-exit jobs (the closure sees the guard signalled; gate on it
  only for non-drain passes).
- `Supervisor::seed(u64)` — deterministic jitter sequence across runs
  (XOR'd with the worker name so distinct jobs decorrelate).
- `JobRunSummary::skips` / `::paused` — leadership/breaker outcomes are
  now auditable in the terminal report.
- `JobSpec::use_breaker` — per-job breaker opt-out for local-DB sweeps
  where pausing on clustered failures is wrong.

### Fixed
- Dropped the forced `timeout` feature on the breaker dependency: it
  unified graph-wide and broke hosts with non-total breaker matches
  (estate-integration round-4 finding). This crate's fire match remains
  total under any unification.

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
