//! The job contract: the closure type, the per-fire context, and the
//! registration spec.

use std::sync::Arc;

use futures::future::BoxFuture;
use shutdown_kit::ShutdownGuard;

use crate::error::JobError;
use crate::trigger::Trigger;

/// Default consecutive failures before a job reports
/// [`JobStatus::degraded`](crate::JobStatus::degraded).
pub const DEFAULT_FAILURE_BUDGET: u32 = 5;

/// The job closure: given a [`JobContext`], do the work.
///
/// Object-safe and async via [`BoxFuture`], so anything from a quick
/// sweep to a long-running reindex fits. Every returned `Err` counts as
/// a failure (failure budget → breaker); a **panic** is caught by the
/// runner and counts as a failure too — the supervisor outlives buggy
/// jobs. Long jobs should watch
/// [`JobContext::shutdown`](JobContext::shutdown) and return early on
/// drain.
pub type Job = Arc<dyn Fn(JobContext) -> BoxFuture<'static, Result<(), JobError>> + Send + Sync>;

/// Everything a fire is told: who it is, why it fired, and how to notice
/// shutdown.
///
/// The `shutdown` guard is a clone of the supervisor's — jobs running
/// long stretches should `select!` on
/// [`wait_for_shutdown`](ShutdownGuard::wait_for_shutdown) so a drain
/// (30 s cap) does not cut them off mid-flight.
#[derive(Clone)]
pub struct JobContext {
    /// The registered name of the job firing — handy for logging and
    /// per-job metric labels.
    pub worker_name: String,
    /// The trigger that caused this fire (clone of the registered
    /// trigger), so one closure can serve several cadences or adapt to
    /// interval vs cron.
    pub trigger: Trigger,
    /// Cooperative shutdown signal; resolved when the supervisor drains.
    pub shutdown: ShutdownGuard,
}

impl std::fmt::Debug for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobContext")
            .field("worker_name", &self.worker_name)
            .field("trigger", &self.trigger)
            .field("shutdown_signalled", &self.shutdown.is_shutdown())
            .finish()
    }
}

/// A job registration: name, trigger, closure, failure budget, and
/// whether leadership is required to fire.
///
/// Construct with a struct literal or [`JobSpec::new`] (which fills the
/// documented defaults):
///
/// ```
/// use std::time::Duration;
/// use worker_kit::{JobError, JobSpec, Trigger};
///
/// let spec = JobSpec {
///     name: "metrics-rollup".to_owned(),
///     trigger: Trigger::Interval(Duration::from_secs(60)),
///     closure: std::sync::Arc::new(|_ctx| {
///         Box::pin(async { Ok::<(), JobError>(()) })
///     }),
///     failure_budget: 5,
///     leader: false,
/// };
/// assert_eq!(spec.failure_budget, 5);
/// ```
pub struct JobSpec {
    /// Unique-ish job name: `[a-z0-9_.-]{1,64}`. Duplicate names are
    /// allowed and register as independent workers (reports list both
    /// entries), but give them distinct names for clean observability.
    pub name: String,
    /// What starts each fire.
    pub trigger: Trigger,
    /// The work ([`Job`]).
    pub closure: Job,
    /// Consecutive failures after which the job reports `Degraded`
    /// (and keeps running). Default [`DEFAULT_FAILURE_BUDGET`].
    pub failure_budget: u32,
    /// Require leadership to fire (leader feature). Non-leaders skip
    /// their fires — recorded in
    /// [`JobStatus::skips`](crate::JobStatus::skips), never counted as
    /// failures. Without a lease configured, leadership is a no-op
    /// (the `MemoryLease` always wins).
    pub leader: bool,
}

impl JobSpec {
    /// A spec with the documented defaults: failure budget
    /// [`DEFAULT_FAILURE_BUDGET`], no leadership requirement.
    pub fn new(name: impl Into<String>, trigger: Trigger, closure: Job) -> Self {
        Self {
            name: name.into(),
            trigger,
            closure,
            failure_budget: DEFAULT_FAILURE_BUDGET,
            leader: false,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::time::Duration;

    fn noop() -> Job {
        Arc::new(|_ctx: JobContext| Box::pin(async { Ok::<(), JobError>(()) }))
    }

    #[test]
    fn new_fills_documented_defaults() {
        let spec = JobSpec::new("sweep", Trigger::Interval(Duration::from_secs(1)), noop());
        assert_eq!(spec.name, "sweep");
        assert_eq!(spec.failure_budget, DEFAULT_FAILURE_BUDGET);
        assert_eq!(spec.failure_budget, 5);
        assert!(!spec.leader);
    }

    #[test]
    fn context_is_cloneable_and_debuggable() {
        let ctx = JobContext {
            worker_name: "sweep".to_owned(),
            trigger: Trigger::Interval(Duration::from_secs(1)),
            shutdown: ShutdownGuard::new(),
        };
        let clone = ctx.clone();
        assert_eq!(clone.worker_name, "sweep");
        assert!(format!("{ctx:?}").contains("sweep"));
    }
}
