//! The [`WorkerSupervisor`]: registers jobs, runs their loops, drains
//! on shutdown, and reports.

#[cfg(feature = "breaker")]
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use shutdown_kit::ShutdownGuard;
use tokio::task::JoinSet;

use crate::error::RegisterError;
use crate::job::JobSpec;
use crate::runner::JobRunner;
use crate::trigger::JitterPolicy;

#[cfg(feature = "breaker")]
use breaker::CircuitBreakerConfig;

#[cfg(feature = "leader")]
use crate::leader::Lease;

/// Default drain cap: how long `run` waits for in-flight jobs to finish
/// after shutdown before abandoning them.
pub const DEFAULT_DRAIN_CAP: Duration = Duration::from_secs(30);

/// Default leadership lease TTL (`leader` feature): renewed before each
/// fire.
#[cfg(feature = "leader")]
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

/// The default leader holder id fragment, from the wall-clock nanos —
/// unique enough per supervisor process instance.
fn holder_id() -> String {
    format!("worker-kit-{}", crate::trigger::clock_seed())
}

/// One job's live snapshot, from [`WorkerSupervisor::status`].
#[derive(Debug, Clone, PartialEq)]
pub struct JobStatus {
    /// The job's registered name.
    pub name: String,
    /// The registered trigger.
    pub trigger: crate::trigger::Trigger,
    /// Fires of the job closure (breaker-paused attempts are *not*
    /// fires; leader-skipped ticks are *not* fires).
    pub fires: u64,
    /// Completed fires that failed (returned `Err` or panicked).
    pub failures: u64,
    /// Leader-skipped ticks (`leader: true` jobs without leadership).
    pub skips: u64,
    /// Fire attempts the breaker paused (`breaker` feature).
    pub paused: u64,
    /// Consecutive failures since the last success.
    pub consecutive_failures: u32,
    /// `true` once consecutive failures reached the job's failure
    /// budget. The job keeps running (and a success clears this).
    pub degraded: bool,
    /// The most recent failure message (`Err` text, or the static
    /// panic message — panic payloads carry no `Display`).
    pub last_error: Option<String>,
}

/// One job's totals at the end of [`WorkerSupervisor::run`].
#[derive(Debug, Clone, PartialEq)]
pub struct JobRunSummary {
    /// The job's registered name.
    pub name: String,
    /// Fires of the job closure.
    pub fires: u64,
    /// Failed fires.
    pub failures: u64,
    /// Whether the job ended past its failure budget.
    pub degraded: bool,
    /// The most recent failure message.
    pub last_error: Option<String>,
}

/// What [`WorkerSupervisor::run`] returns after a graceful drain.
///
/// `per_job` is ordered by job name (the `BTreeMap` order), so reports
/// are stable across runs and machines.
#[derive(Debug, Clone, PartialEq)]
pub struct RunReport {
    /// Per-job summaries, ordered by job name.
    pub per_job: Vec<JobRunSummary>,
}

/// Registers periodic jobs, runs them until shutdown, drains, and
/// reports.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use shutdown_kit::ShutdownGuard;
/// use worker_kit::{JobError, JobSpec, Trigger, WorkerSupervisor};
///
/// # async fn demo() {
/// let guard = ShutdownGuard::new();
/// let mut supervisor = WorkerSupervisor::new(guard.clone());
///
/// let sweep: worker_kit::Job = Arc::new(|ctx| {
///     Box::pin(async move {
///         // ... periodic work ...
///         let _ = &ctx.worker_name;
///         Ok::<(), JobError>(())
///     })
/// });
/// supervisor
///     .register(JobSpec::new(
///         "sweep",
///         Trigger::Interval(Duration::from_secs(60)),
///         sweep,
///     ))
///     .expect("valid name");
///
/// // Elsewhere (a signal handler, health server, ...):
/// let report = {
///     let runner = tokio::spawn({
///         let supervisor = Arc::new(supervisor);
///         async move { supervisor.run().await }
///     });
///     guard.shutdown();
///     runner.await.expect("supervisor task")
/// };
/// for job in report.per_job {
///     println!("{}: {} fires, {} failures", job.name, job.fires, job.failures);
/// }
/// # }
/// ```
///
/// # Scheduling semantics
///
/// - **Jitter.** Every wait is stretched full-jitter by up to
///   [`JitterPolicy::default`] of the gap (default 0.2 — configurable
///   via [`jitter`](Self::jitter)). This decorrelates workers across
///   processes; the seed is wall-clock nanos per worker (not
///   security-sensitive — documented in [`JitterPolicy`]).
/// - **Coalescing.** A run outlasting its gap fires the next tick
///   **immediately after completion** — never stacked, never skipped
///   into a backlog. After an overrun the schedule re-anchors to the
///   completion time.
/// - **First fire.** One (jittered) schedule step after `run` starts —
///   jobs do not fire before their first period elapses.
/// - **Drain.** On shutdown each loop finishes its in-flight fire and
///   exits; `run` waits up to [`DEFAULT_DRAIN_CAP`] (configurable via
///   [`drain_cap`](Self::drain_cap)) and reports what completed. A job
///   still running past the cap is abandoned (its partial counts are
///   still reported).
pub struct WorkerSupervisor {
    shutdown: ShutdownGuard,
    jitter: JitterPolicy,
    drain_cap: Duration,
    holder: String,
    runners: Vec<Arc<JobRunner>>,
    #[cfg(feature = "breaker")]
    breaker_config: CircuitBreakerConfig,
    #[cfg(feature = "leader")]
    lease: Option<Arc<dyn Lease>>,
    #[cfg(feature = "leader")]
    lease_ttl: Duration,
}

impl WorkerSupervisor {
    /// A supervisor with the documented defaults.
    #[must_use]
    pub fn new(shutdown: ShutdownGuard) -> Self {
        Self {
            shutdown,
            jitter: JitterPolicy::default(),
            drain_cap: DEFAULT_DRAIN_CAP,
            holder: holder_id(),
            runners: Vec::new(),
            #[cfg(feature = "breaker")]
            breaker_config: default_breaker_config(),
            #[cfg(feature = "leader")]
            lease: None,
            #[cfg(feature = "leader")]
            lease_ttl: DEFAULT_LEASE_TTL,
        }
    }

    /// Set the jitter policy (default: full-jitter at fraction 0.2).
    pub fn jitter(&mut self, jitter: JitterPolicy) -> &mut Self {
        self.jitter = jitter;
        self
    }

    /// Set the drain cap (default [`DEFAULT_DRAIN_CAP`]).
    pub fn drain_cap(&mut self, drain_cap: Duration) -> &mut Self {
        self.drain_cap = drain_cap;
        self
    }

    /// Set the breaker configuration used for every registered job
    /// (`breaker` feature; default: the estate-standard breaker — trip
    /// on 5 consecutive failures or a 50 % rate over 10 calls, open
    /// 30 s, then admit 3 half-open probes).
    #[cfg(feature = "breaker")]
    pub fn breaker_config(&mut self, breaker_config: CircuitBreakerConfig) -> &mut Self {
        self.breaker_config = breaker_config;
        self
    }

    /// Set the leadership lease used by `leader: true` jobs (`leader`
    /// feature). Without a lease, leadership is a no-op (the
    /// `MemoryLease` always wins).
    #[cfg(feature = "leader")]
    pub fn lease(&mut self, lease: Arc<dyn Lease>) -> &mut Self {
        self.lease = Some(lease);
        self
    }

    /// Set the leadership lease TTL (default [`DEFAULT_LEASE_TTL`];
    /// `leader` feature). Renewed before each fire.
    #[cfg(feature = "leader")]
    pub fn lease_ttl(&mut self, lease_ttl: Duration) -> &mut Self {
        self.lease_ttl = lease_ttl;
        self
    }

    /// Register a job. The name is validated eagerly
    /// (`[a-z0-9_.-]{1,64}` — else
    /// [`RegisterError::InvalidName`]), and with the `cron` feature the
    /// trigger's cron expression is parsed eagerly (else
    /// [`RegisterError::InvalidCron`]).
    ///
    /// # Errors
    /// [`RegisterError::InvalidName`] / [`RegisterError::InvalidCron`].
    pub fn register(&mut self, spec: JobSpec) -> Result<&mut Self, RegisterError> {
        if !crate::error::is_valid_name(&spec.name) {
            return Err(RegisterError::InvalidName { name: spec.name });
        }
        spec.trigger.validate()?;

        let runner = JobRunner::new(spec, self.jitter.clone());
        #[cfg(feature = "breaker")]
        let runner = runner.with_breaker(self.breaker_config.clone());
        #[cfg(feature = "leader")]
        let runner = runner.with_lease(self.lease.clone(), self.lease_ttl);
        self.runners.push(Arc::new(runner));
        Ok(self)
    }

    /// A clone of the supervisor's shutdown guard, for callers that
    /// want to trigger the drain without holding the original.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownGuard {
        self.shutdown.clone()
    }

    /// Live status snapshots for every registered job, ordered by name.
    #[must_use]
    pub fn status(&self) -> Vec<JobStatus> {
        let mut statuses: Vec<JobStatus> = self.runners.iter().map(|r| r.status()).collect();
        statuses.sort_by(|a, b| a.name.cmp(&b.name));
        statuses
    }

    /// The named job's breaker state (`breaker` feature): `Closed`,
    /// `Open` (fires paused), or `HalfOpen` (probing for recovery).
    /// `None` if no job is registered under `name`.
    #[cfg(feature = "breaker")]
    #[must_use]
    pub fn breaker_state(&self, name: &str) -> Option<breaker::State> {
        self.runners
            .iter()
            .find(|r| r.name() == name)
            .and_then(|r| r.breaker_state())
    }

    /// Every job's breaker metrics snapshot, by name, ordered by name
    /// (`breaker` feature).
    #[cfg(feature = "breaker")]
    #[must_use]
    pub fn breaker_metrics(&self) -> BTreeMap<String, breaker::CircuitMetrics> {
        self.runners
            .iter()
            .map(|r| (r.name().to_owned(), r.breaker_metrics()))
            .filter_map(|(name, metrics)| metrics.map(|m| (name, m)))
            .collect()
    }

    /// Run every registered job until shutdown, then drain (up to the
    /// drain cap) and report. See the [type docs](Self) for the
    /// scheduling semantics.
    pub async fn run(self: Arc<Self>) -> RunReport {
        let mut loops = JoinSet::new();
        for (index, runner) in self.runners.iter().enumerate() {
            let runner = Arc::clone(runner);
            let shutdown = self.shutdown.clone();
            let holder = self.holder.clone();
            loops.spawn(async move { (index, runner.run_loop(shutdown, holder).await) });
        }

        // Park until shutdown is signalled (or every loop somehow ends).
        self.shutdown.wait_for_shutdown().await;

        // Graceful drain: every loop finishes its in-flight fire and
        // returns its summary — bounded by the drain cap. Loops still
        // running past the cap are abandoned; their partial counters are
        // synthesized from the live stats so no job vanishes from the
        // report.
        let mut summaries: Vec<Option<JobRunSummary>> =
            (0..self.runners.len()).map(|_| None).collect();
        let drain = async {
            while let Some(finished) = loops.join_next().await {
                if let Ok((index, summary)) = finished {
                    // `index` comes from enumerate over `runners`, so the
                    // slot always exists — `get_mut` keeps the deny-listed
                    // indexing syntax out anyway.
                    if let Some(slot) = summaries.get_mut(index) {
                        *slot = Some(summary);
                    }
                }
            }
        };
        if tokio::time::timeout(self.drain_cap, drain).await.is_err() {
            loops.abort_all();
        }
        for (runner, slot) in self.runners.iter().zip(summaries.iter_mut()) {
            // Closure (not a fn path): `runners` holds `Arc<JobRunner>`.
            slot.get_or_insert_with(|| runner.summary());
        }
        let mut per_job: Vec<JobRunSummary> = summaries.into_iter().flatten().collect();
        // BTreeMap order: sorted by name (stable, so duplicate names keep
        // their registration order).
        per_job.sort_by(|a, b| a.name.cmp(&b.name));
        RunReport { per_job }
    }
}

/// The estate-standard breaker for jobs (`breaker` feature): trip on 5
/// consecutive failures or a 50 % failure rate over the last 10 calls,
/// stay open 30 s, then admit 3 half-open probes; one success closes.
#[cfg(feature = "breaker")]
fn default_breaker_config() -> CircuitBreakerConfig {
    breaker::CircuitBreakerConfig::standard()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    // Exact f64 literals compared for the documented defaults.
    #![allow(clippy::float_cmp)]
    use super::*;
    use crate::error::JobError;
    use crate::job::JobContext;

    fn noop(name: &str) -> JobSpec {
        JobSpec::new(
            name,
            crate::trigger::Trigger::Interval(Duration::from_millis(10)),
            Arc::new(|_ctx: JobContext| Box::pin(async { Ok::<(), JobError>(()) })),
        )
    }

    #[test]
    fn new_uses_documented_defaults() {
        let supervisor = WorkerSupervisor::new(ShutdownGuard::new());
        assert_eq!(supervisor.drain_cap, DEFAULT_DRAIN_CAP);
        assert_eq!(supervisor.drain_cap, Duration::from_secs(30));
        assert_eq!(supervisor.jitter.fraction, 0.2);
        assert!(supervisor.status().is_empty());
    }

    #[test]
    fn register_rejects_invalid_names() {
        let mut supervisor = WorkerSupervisor::new(ShutdownGuard::new());
        let Err(err) = supervisor.register(noop("INVALID NAME")) else {
            panic!("must reject invalid names");
        };
        assert!(matches!(err, RegisterError::InvalidName { .. }));
        assert!(supervisor.register(noop("ok-name")).is_ok());
        assert_eq!(supervisor.status().len(), 1);
    }

    #[test]
    fn register_is_chainable() {
        let mut supervisor = WorkerSupervisor::new(ShutdownGuard::new());
        supervisor
            .register(noop("a"))
            .and_then(|s| s.register(noop("b")).map(|_| ()))
            .expect("both names valid");
        assert_eq!(supervisor.status().len(), 2);
    }

    #[test]
    fn status_is_ordered_by_name() {
        let mut supervisor = WorkerSupervisor::new(ShutdownGuard::new());
        for name in ["zeta", "alpha", "mid"] {
            supervisor.register(noop(name)).expect("valid");
        }
        let names: Vec<String> = supervisor.status().iter().map(|s| s.name.clone()).collect();
        assert_eq!(names, ["alpha", "mid", "zeta"]);
    }

    #[cfg(feature = "breaker")]
    #[test]
    fn breaker_accessors_report_per_job() {
        let mut supervisor = WorkerSupervisor::new(ShutdownGuard::new());
        supervisor.register(noop("job-a")).expect("valid");
        assert_eq!(
            supervisor.breaker_state("job-a"),
            Some(breaker::State::Closed),
            "a fresh job's breaker is closed"
        );
        assert_eq!(supervisor.breaker_state("missing"), None);
        let metrics = supervisor.breaker_metrics();
        assert_eq!(metrics.len(), 1);
        assert!(metrics.contains_key("job-a"));
        let _ = metrics;
    }

    #[tokio::test]
    async fn run_with_no_jobs_reports_empty_after_shutdown() {
        let guard = ShutdownGuard::new();
        let supervisor = Arc::new(WorkerSupervisor::new(guard.clone()));
        let runner = tokio::spawn(Arc::clone(&supervisor).run());
        guard.shutdown();
        let report = tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("drains immediately with no jobs")
            .expect("join");
        assert!(report.per_job.is_empty());
    }
}
