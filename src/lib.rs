//! Periodic job scheduling for Rust services — jittered intervals,
//! graceful drain, breaker-aware failure budgets, optional leader
//! election.
//!
//! `worker-kit` runs background jobs on a cadence without a scheduler
//! service: register [`JobSpec`]s on a [`WorkerSupervisor`], hand it the
//! estate's [`ShutdownGuard`](shutdown_kit::ShutdownGuard), and `run`.
//! Every job loop sleeps to its next (jittered) tick, fires through the
//! estate's circuit breaker, and records outcomes; on shutdown the
//! supervisor drains gracefully and returns a [`RunReport`].
//!
//! # Design
//!
//! - **Jittered cadences.** Every wait is stretched **full-jitter** —
//!   uniform in `[0, fraction × gap]`, default fraction 0.2 — so fleets
//!   of workers do not stampede a shared downstream on the same
//!   boundary. The per-worker RNG seeds from wall-clock nanos
//!   ([`clock_seed`]); jitter is schedule decorrelation, not security,
//!   and the delay *bound* is deterministic for tests and capacity
//!   planning ([`JitterPolicy::jitter_seeded`]).
//! - **Coalescing, not stacking.** A run that outlasts its gap fires
//!   the next tick **immediately after completion** — never stacked.
//!   After an overrun the schedule re-anchors to the completion time.
//! - **Failure budgets, visibly.** `failure_budget` consecutive
//!   failures (default 5) mark a job
//!   [`Degraded`](JobStatus::degraded) — visible in status, still
//!   running. A success resets the budget.
//! - **Breaker-aware fires.** With the default `breaker` feature, each
//!   job's closure runs through the estate's `breaker = "2"` circuit
//!   breaker: clustered failures pause fires entirely (the closure is
//!   *not invoked* — paused attempts consume neither the failure budget
//!   nor the breaker's budget), half-open probes test recovery, a
//!   success closes. Panics are caught and count as failures — the
//!   supervisor outlives buggy jobs.
//! - **Optional leader election.** With the `leader` feature, `leader:
//!   true` jobs fire only on the lease holder: a [`Lease`] with
//!   [`acquire`](Lease::acquire)/[`renew`](Lease::renew) — in-process
//!   ([`MemoryLease`]) or Redis `SET NX PX` ([`RedisLease`], `redis`
//!   feature). Non-leaders skip their fires (recorded, never failures).
//! - **Graceful drain.** `run` parks until shutdown, then waits up to
//!   [`DEFAULT_DRAIN_CAP`] (30 s) for in-flight fires, and reports
//!   per-job totals in name order.
//!
//! # Triggers
//!
#![cfg_attr(
    feature = "cron",
    doc = "| [`Trigger::Interval`] | (default) | fixed cadence, jittered + coalesced |
| [`Trigger::Cron`] | `cron` | cron expression, evaluated in UTC |
"
)]
#![cfg_attr(
    not(feature = "cron"),
    doc = "| [`Trigger::Interval`] | (default) | fixed cadence, jittered + coalesced |
| [`Trigger::Cron`] | `cron` | cron expression, evaluated in UTC (enable the feature) |
"
)]
//!
//! # Features
//!
//! | Feature | Default | Enables |
//! |---|---|---|
//! | `breaker` | yes | per-job estate `breaker = "2"` circuit breaker |
//! | `cron` | no | [`Trigger::Cron`] via the `cron` crate |
//! | `leader` | no | [`Lease`], [`MemoryLease`] |
//! | `redis` | no | [`RedisLease`] (implies `leader`) |
//!
//! # Example
//!
//! Two jobs — one sweep every 30 s, one always-failing rollup that
//! degrades visibly after its budget — run until shutdown:
//!
//! ```
//! # #[cfg(feature = "breaker")] fn main() {
//! #     let rt = tokio::runtime::Builder::new_current_thread()
//! #         .enable_all()
//! #         .build()
//! #         .unwrap();
//! #     rt.block_on(async {
//! #         demo().await;
//! #     });
//! # }
//! # #[cfg(not(feature = "breaker"))]
//! # fn main() {}
//! # #[cfg(feature = "breaker")]
//! # async fn demo() {
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicU32, Ordering};
//! use std::time::Duration;
//!
//! use shutdown_kit::ShutdownGuard;
//! use worker_kit::{Job, JobError, JobSpec, Trigger, WorkerSupervisor};
//!
//! let guard = ShutdownGuard::new();
//! let mut supervisor = WorkerSupervisor::new(guard.clone());
//!
//! // A healthy sweep.
//! let sweep: Job = Arc::new(|_ctx| Box::pin(async { Ok::<(), JobError>(()) }));
//! supervisor
//!     .register(JobSpec::new(
//!         "sweep",
//!         Trigger::Interval(Duration::from_secs(30)),
//!         sweep,
//!     ))
//!     .unwrap();
//!
//! // A rollup whose backend is down: degrades after 5 consecutive
//! // failures (and keeps running; the breaker pauses it once the
//! // failures cluster).
//! let attempts = Arc::new(AtomicU32::new(0));
//! let rollup: Job = {
//!     let attempts = Arc::clone(&attempts);
//!     Arc::new(move |_ctx| {
//!         let attempts = Arc::clone(&attempts);
//!         Box::pin(async move {
//!             attempts.fetch_add(1, Ordering::Relaxed);
//!             Err(JobError::msg("rollup backend down"))
//!         })
//!     })
//! };
//! supervisor
//!     .register(JobSpec {
//!         name: "metrics-rollup".to_owned(),
//!         trigger: Trigger::Interval(Duration::from_secs(10)),
//!         closure: rollup,
//!         failure_budget: 5,
//!         leader: false,
//!     })
//!     .unwrap();
//!
//! let supervisor = Arc::new(supervisor);
//! let runner = tokio::spawn(Arc::clone(&supervisor).run());
//!
//! // ... the jobs run on their cadences; status is observable live ...
//! let _ = supervisor.status();
//!
//! // On shutdown: drain and report.
//! guard.shutdown();
//! let report = runner.await.unwrap();
//! assert_eq!(report.per_job.len(), 2);
//! assert_eq!(report.per_job[0].name, "metrics-rollup");
//! # }
//! ```
//!
//! # Cron and DST
//!
//! `Trigger::Cron` (the `cron` feature) evaluates expressions in
//! **UTC**: `0 0 6 * * *` means 06:00:00 UTC, every day, year-round.
//! Around a DST transition the local meaning of a fixed UTC hour shifts
//! by the zone's offset, and the ambiguous local hour is simply not
//! represented — schedules that must track local civil time need a
//! tz-aware scheduler. Expressions use the `cron` crate's 6- or
//! 7-field syntax (`sec min hour dom mon dow [year]`).
//!
//! # Roadmap
//!
//! - Database-backed leases (Postgres advisory locks) alongside the
//!   Redis lease.
//! - Per-job breaker configuration (today the supervisor-level config
//!   applies to every job).
//! - Breaker metrics export through `metrics-kit`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
mod job;
#[cfg(feature = "leader")]
mod leader;
mod runner;
mod supervisor;
mod trigger;

pub use crate::error::{is_valid_name, JobError, RegisterError, NAME_MAX_LEN};
pub use crate::job::{Job, JobContext, JobSpec, DEFAULT_FAILURE_BUDGET};
#[cfg(feature = "redis")]
pub use crate::leader::RedisLease;
#[cfg(feature = "leader")]
pub use crate::leader::{Lease, MemoryLease};
#[cfg(feature = "leader")]
pub use crate::supervisor::DEFAULT_LEASE_TTL;
pub use crate::supervisor::{
    JobRunSummary, JobStatus, RunReport, WorkerSupervisor, DEFAULT_DRAIN_CAP,
};
pub use crate::trigger::{clock_seed, JitterPolicy, Trigger, DEFAULT_JITTER_FRACTION};
