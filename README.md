# worker-kit

Periodic job scheduling for Rust services — jittered intervals, graceful
drain, breaker-aware failure budgets, optional leader election.

`worker-kit` runs background jobs on a cadence without a scheduler
service: register `JobSpec`s on a `WorkerSupervisor`, hand it the
estate's [`shutdown-kit::ShutdownGuard`], and `run`. Every job loop
sleeps to its next (jittered) tick, fires through the estate's circuit
breaker, and records outcomes; on shutdown the supervisor drains
gracefully and returns a `RunReport`.

```rust
use std::sync::Arc;
use std::time::Duration;

use shutdown_kit::ShutdownGuard;
use worker_kit::{Job, JobError, JobSpec, Trigger, WorkerSupervisor};

let guard = ShutdownGuard::new();
let mut supervisor = WorkerSupervisor::new(guard.clone());

let sweep: Job = Arc::new(|_ctx| Box::pin(async { Ok::<(), JobError>(()) }));
supervisor
    .register(JobSpec::new(
        "sweep",
        Trigger::Interval(Duration::from_secs(30)),
        sweep,
    ))
    .expect("valid name");

let supervisor = Arc::new(supervisor);
let runner = tokio::spawn(supervisor.run());

// ... later, on shutdown:
guard.shutdown();
let report = runner.await.expect("supervisor task");
for job in report.per_job {
    println!("{}: {} fires, {} failures", job.name, job.fires, job.failures);
}
```

## Design

- **Jittered cadences.** Every wait is stretched *full-jitter* — uniform
  in `[0, fraction × gap]`, default fraction `0.2` — so fleets of
  workers do not stampede a shared downstream on the same boundary. The
  per-worker RNG seeds from wall-clock nanos (`clock_seed`); jitter is
  schedule decorrelation, **not security**, and the delay *bound* is
  deterministic for tests and capacity planning
  (`JitterPolicy::jitter_seeded`).
- **Coalescing, not stacking.** A run that outlasts its gap fires the
  next tick *immediately after completion* — never stacked, never
  skipped into a backlog. After an overrun the schedule re-anchors to
  the completion time.
- **Failure budgets, visibly.** `failure_budget` consecutive failures
  (default 5) mark a job `Degraded` — visible in `status()`, still
  running. A success resets the budget.
- **Breaker-aware fires.** With the default `breaker` feature, each
  job's closure runs through the estate's `breaker = "2"` circuit
  breaker: clustered failures pause fires entirely (the closure is *not
  invoked* — paused attempts consume neither the failure budget nor the
  breaker's budget), half-open probes test recovery, a success closes.
  Panics are caught and count as failures — the supervisor outlives
  buggy jobs.
- **Optional leader election.** With the `leader` feature, `leader:
  true` jobs fire only on the lease holder: a `Lease` with
  `acquire`/`renew` — in-process (`MemoryLease`) or Redis `SET NX PX`
  (`RedisLease`, `redis` feature). Non-leaders *skip* their fires
  (recorded, never failures).
- **Graceful drain.** `run` parks until shutdown, then waits up to 30 s
  (configurable) for in-flight fires, and reports per-job totals in
  name order.

## Features

| Feature  | Default | Enables                                          |
|----------|---------|--------------------------------------------------|
| `breaker` | yes    | per-job estate `breaker = "2"` circuit breaker   |
| `cron`    | no     | `Trigger::Cron` via the `cron` crate             |
| `leader`  | no     | `Lease`, `MemoryLease`                           |
| `redis`   | no     | `RedisLease` (implies `leader`)                  |

## Triggers

- `Trigger::Interval(Duration)` — fixed cadence, jittered + coalesced.
  The first fire is one (jittered) interval after `run` starts.
- `Trigger::Cron(String)` (`cron` feature) — a cron expression in the
  `cron` crate's 6- or 7-field syntax (`sec min hour dom mon dow
  [year]`), e.g. `"0 0 6 * * *"` for 06:00:00 UTC daily. Parsed
  eagerly at registration; a bad expression is a typed registration
  error.

### Cron and DST

`Trigger::Cron` evaluates expressions in **UTC**: around a DST
transition the local meaning of a fixed UTC hour shifts by the zone's
offset, and the ambiguous local hour is simply not represented —
schedules that must track local civil time need a tz-aware scheduler.
This kit deliberately does not guess time zones.

## Observability

- `supervisor.status()` — live per-job snapshot (fires, failures,
  skips, paused, consecutive failures, degraded, last error).
- `supervisor.breaker_state(name)` / `supervisor.breaker_metrics()` —
  per-job circuit state and metrics (`breaker` feature).
- `RunReport` — per-job totals after a graceful drain, name-ordered.

## Redis leases

```toml
worker-kit = { version = "0.1", features = ["redis"] }
```

```rust
use std::sync::Arc;
use std::time::Duration;
use worker_kit::RedisLease;

let lease = RedisLease::connect("redis://127.0.0.1:6379", "worker-kit:leader")
    .await
    .expect("redis");
let mut supervisor = WorkerSupervisor::new(guard);
supervisor.lease(Arc::new(lease)).lease_ttl(Duration::from_secs(30));
```

Acquisition is `SET key holder NX PX ttl` (single winner); renewal is
an atomic compare-and-expire script. Backend errors report "not leader
now" rather than erroring — losing leadership merely skips fires, which
is the safe failure direction. Note that a Redis failover can briefly
admit two leaders (the old holder's key may survive on a lagging
replica); strictly-single-instance jobs should pair leadership with
their own fencing.

## Testing hermetically

The Redis suite uses a Docker testcontainer and is `#[ignore]`-gated:

```sh
cargo test                          # hermetic — no Docker needed
cargo test --test redis_lease -- --ignored --nocapture   # with Docker
```

## MSRV

Rust 1.85 (edition 2021).

## License

MIT OR Apache-2.0 — see [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE).

[`shutdown-kit::ShutdownGuard`]: https://crates.io/crates/shutdown-kit
