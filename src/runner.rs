//! The per-job runner: one async loop per registered job — schedule the
//! next tick with jitter, gate on leadership, fire through the breaker,
//! and record outcomes.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use rand::rngs::SmallRng;
use rand::SeedableRng;
use shutdown_kit::ShutdownGuard;

#[cfg(feature = "breaker")]
use breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError};

#[cfg(feature = "breaker")]
use crate::error::JobError;
use crate::job::{Job, JobContext, JobSpec};
use crate::supervisor::{JobRunSummary, JobStatus};
use crate::trigger::{clock_seed, JitterPolicy, SCHEDULE_PARK};

#[cfg(feature = "leader")]
use crate::leader::{Lease, MemoryLease};

/// Recorded when a job panics. The panic payload (`Box<dyn Any + Send>`)
/// has **no `Display`**, so this static message is all that can be
/// recorded (the real detail goes to the panic hook's stderr).
const JOB_PANIC_MESSAGE: &str = "job panicked (panic payload has no Display)";

/// Recorded for any other breaker-rejected fire — the payload here also
/// carries no `Display` (see the wildcard arm in
/// [`JobRunner::invoke_through_breaker`]).
#[cfg(feature = "breaker")]
const JOB_UNCLASSIFIED_MESSAGE: &str =
    "job rejected by unclassified breaker error (timed out, or a future variant)";

/// What one fire did.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FireOutcome {
    /// The job returned `Ok`.
    Success,
    /// The job returned `Err`, or panicked — with the recorded message.
    Failure(String),
    /// The breaker refused to run the job (open, or half-open with no
    /// probe slot): not a fire of the closure, not a failure — a pause.
    /// Only reachable with the `breaker` feature.
    #[cfg(feature = "breaker")]
    Paused,
}

/// The outcome counters for one job. All `Relaxed`: the counters are
/// advisory snapshots for status/reporting, with no cross-variable
/// invariants a stricter ordering would buy.
#[derive(Default)]
struct Stats {
    fires: AtomicU64,
    failures: AtomicU64,
    skips: AtomicU64,
    paused: AtomicU64,
    consecutive: AtomicU32,
    degraded: AtomicBool,
    last_error: StdMutex<Option<String>>,
}

/// One registered job's loop. Shared as `Arc<JobRunner>` between the
/// spawned loop task and the supervisor's status/report methods.
pub(crate) struct JobRunner {
    spec: JobSpec,
    jitter: JitterPolicy,
    #[cfg(feature = "breaker")]
    breaker: Option<CircuitBreaker>,
    #[cfg(feature = "leader")]
    lease: Option<Arc<dyn Lease>>,
    #[cfg(feature = "leader")]
    lease_ttl: Duration,
    #[cfg(feature = "leader")]
    leader_held: AtomicBool,
    stats: Stats,
}

impl JobRunner {
    pub(crate) fn new(spec: JobSpec, jitter: JitterPolicy) -> Self {
        Self {
            spec,
            jitter,
            #[cfg(feature = "breaker")]
            breaker: None,
            #[cfg(feature = "leader")]
            lease: None,
            #[cfg(feature = "leader")]
            lease_ttl: crate::supervisor::DEFAULT_LEASE_TTL,
            #[cfg(feature = "leader")]
            leader_held: AtomicBool::new(false),
            stats: Stats::default(),
        }
    }

    /// Attach the per-job breaker (estate `breaker = "2"`).
    #[cfg(feature = "breaker")]
    pub(crate) fn with_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.breaker = Some(CircuitBreaker::new(config));
        self
    }

    /// Attach the leadership lease for `leader: true` jobs.
    #[cfg(feature = "leader")]
    pub(crate) fn with_lease(mut self, lease: Option<Arc<dyn Lease>>, ttl: Duration) -> Self {
        self.lease = lease;
        self.lease_ttl = ttl;
        self
    }

    /// The registered job name (breaker accessors match on it; dead
    /// without the `breaker` feature).
    #[cfg_attr(not(feature = "breaker"), allow(dead_code))]
    pub(crate) fn name(&self) -> &str {
        &self.spec.name
    }

    /// Snapshot for the run report.
    pub(crate) fn summary(&self) -> JobRunSummary {
        JobRunSummary {
            name: self.spec.name.clone(),
            fires: self.stats.fires.load(Ordering::Relaxed),
            failures: self.stats.failures.load(Ordering::Relaxed),
            degraded: self.stats.degraded.load(Ordering::Relaxed),
            last_error: self.last_error(),
        }
    }

    /// Snapshot for live status.
    pub(crate) fn status(&self) -> JobStatus {
        JobStatus {
            name: self.spec.name.clone(),
            trigger: self.spec.trigger.clone(),
            fires: self.stats.fires.load(Ordering::Relaxed),
            failures: self.stats.failures.load(Ordering::Relaxed),
            skips: self.stats.skips.load(Ordering::Relaxed),
            paused: self.stats.paused.load(Ordering::Relaxed),
            consecutive_failures: self.stats.consecutive.load(Ordering::Relaxed),
            degraded: self.stats.degraded.load(Ordering::Relaxed),
            last_error: self.last_error(),
        }
    }

    /// The job breaker's current state, if the breaker feature is on.
    #[cfg(feature = "breaker")]
    pub(crate) fn breaker_state(&self) -> Option<breaker::State> {
        self.breaker.as_ref().map(CircuitBreaker::state)
    }

    /// The job breaker's metrics snapshot, if the breaker feature is on.
    #[cfg(feature = "breaker")]
    pub(crate) fn breaker_metrics(&self) -> Option<breaker::CircuitMetrics> {
        self.breaker.as_ref().map(CircuitBreaker::metrics)
    }

    fn last_error(&self) -> Option<String> {
        self.stats
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn record_last_error(&self, message: String) {
        *self
            .stats
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(message);
    }

    /// The job loop: sleep to the next (jittered) tick, fire, repeat —
    /// until shutdown. Returns the summary the supervisor reports.
    pub(crate) async fn run_loop(
        self: Arc<Self>,
        shutdown: ShutdownGuard,
        holder: String,
    ) -> JobRunSummary {
        // Per-worker RNG seeded from wall-clock nanos: jitter is schedule
        // decorrelation, not security — see `JitterPolicy`'s docs.
        let mut rng = SmallRng::seed_from_u64(clock_seed());
        let mut shutdown_rx = shutdown.watch_receiver();

        // First tick: one schedule step from the supervisor's start.
        let mut tick = first_tick(&self.spec.trigger);

        loop {
            let gap = tick
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO);
            let fire_at = tick + self.jitter.jitter(gap, &mut rng);

            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow_and_update() {
                        break;
                    }
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(std_instant_at(
                    fire_at,
                ))) => {
                    self.fire(&shutdown, &holder).await;
                    // Coalescing: the schedule keeps ticking, but a run
                    // that outlasted its gap fires the next immediately
                    // after completion — never stacked.
                    let completed = SystemTime::now();
                    tick = next_tick_or_park(&self.spec.trigger, tick, completed);
                    if completed >= tick {
                        tick = completed;
                    }
                }
            }
        }

        self.summary()
    }

    /// One fire: leader gate, run the closure (panics caught, breaker
    /// wrapped), record the outcome.
    #[cfg_attr(not(feature = "leader"), allow(unused_variables))]
    async fn fire(&self, shutdown: &ShutdownGuard, holder: &str) {
        // Leader gate: non-leaders skip the fire — recorded, never a
        // failure.
        #[cfg(feature = "leader")]
        if self.spec.leader {
            let lease: Arc<dyn Lease> = self.lease.clone().unwrap_or_else(|| Arc::new(MemoryLease));
            // Held holders renew; a failed renew falls back to acquire so
            // a lapsed leader can take over again once the lease is free
            // (if another holder took it, the `SET NX` acquire fails and
            // the fire is a skip — the safe direction).
            let elected = if self.leader_held.load(Ordering::Relaxed) {
                lease.renew(holder, self.lease_ttl).await
                    || lease.acquire(holder, self.lease_ttl).await
            } else {
                lease.acquire(holder, self.lease_ttl).await
            };
            if !elected {
                self.stats.skips.fetch_add(1, Ordering::Relaxed);
                return;
            }
            self.leader_held.store(true, Ordering::Relaxed);
        }

        let ctx = JobContext {
            worker_name: self.spec.name.clone(),
            trigger: self.spec.trigger.clone(),
            shutdown: shutdown.clone(),
        };

        match self.invoke(ctx).await {
            FireOutcome::Success => {
                // The closure ran: count the fire, reset the budget.
                self.stats.fires.fetch_add(1, Ordering::Relaxed);
                self.stats.consecutive.store(0, Ordering::Relaxed);
                self.stats.degraded.store(false, Ordering::Relaxed);
            }
            FireOutcome::Failure(message) => {
                // The closure ran and failed: fire + failure, and the
                // consecutive-failure budget may degrade the job.
                self.stats.fires.fetch_add(1, Ordering::Relaxed);
                self.stats.failures.fetch_add(1, Ordering::Relaxed);
                let consecutive = self.stats.consecutive.fetch_add(1, Ordering::Relaxed) + 1;
                if u64::from(consecutive) >= u64::from(self.spec.failure_budget) {
                    self.stats.degraded.store(true, Ordering::Relaxed);
                }
                self.record_last_error(message);
            }
            #[cfg(feature = "breaker")]
            FireOutcome::Paused => {
                // The breaker refused the attempt: not a fire, not a
                // failure — a pause.
                self.stats.paused.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Run the closure — through the per-job breaker when the `breaker`
    /// feature is on, with panics caught either way.
    async fn invoke(&self, ctx: JobContext) -> FireOutcome {
        let job = Arc::clone(&self.spec.closure);
        #[cfg(feature = "breaker")]
        if let Some(breaker) = &self.breaker {
            return Self::invoke_through_breaker(breaker, job, ctx).await;
        }
        Self::invoke_plain(job, ctx).await
    }

    /// The plain path (no breaker): panics caught, outcomes recorded.
    async fn invoke_plain(job: Job, ctx: JobContext) -> FireOutcome {
        match futures::FutureExt::catch_unwind(AssertUnwindSafe(job(ctx))).await {
            Ok(Ok(())) => FireOutcome::Success,
            Ok(Err(job_error)) => FireOutcome::Failure(job_error.to_string()),
            // Panic payload: no `Display` — a static message is all that
            // can be recorded.
            Err(_) => FireOutcome::Failure(JOB_PANIC_MESSAGE.to_owned()),
        }
    }

    /// The breaker-wrapped path (estate `breaker = "2"`). The operation
    /// maps the job's outcome into the breaker's error slot — job `Err`
    /// **and** panics count as breaker failures, so persistent failures
    /// trip the circuit (pausing fires via open/half-open). A bare
    /// `catch_unwind` future would leave job errors in the `Ok`
    /// position, and the breaker would record every failure as a
    /// success — the trap this mapping avoids.
    #[cfg(feature = "breaker")]
    async fn invoke_through_breaker(
        breaker: &CircuitBreaker,
        job: Job,
        ctx: JobContext,
    ) -> FireOutcome {
        let outcome = breaker
            .call(|| async {
                match futures::FutureExt::catch_unwind(AssertUnwindSafe(job(ctx))).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(job_error)) => Err(job_error),
                    // Panic payload: no `Display` — a static message is
                    // all that can be carried.
                    Err(_) => Err(JobError::msg(JOB_PANIC_MESSAGE)),
                }
            })
            .await;
        match outcome {
            Ok(()) => FireOutcome::Success,
            Err(CircuitBreakerError::CircuitOpen | CircuitBreakerError::Rejected) => {
                // Paused (open) or probe slots taken (half-open): the
                // closure is not invoked, and neither the failure budget
                // nor the breaker's own budget moves. The breaker, not
                // the job budget, owns this pause.
                FireOutcome::Paused
            }
            Err(CircuitBreakerError::Failure(job_error)) => {
                FireOutcome::Failure(job_error.to_string())
            }
            // Feature-unification safety: breaker's additive `timeout`
            // feature appends a `Timeout` variant to
            // `CircuitBreakerError`, and any host enabling it anywhere in
            // the graph unifies it into this crate's build (the
            // outbox-kit 0.1.0 regression class). A timed-out job is a
            // failure; the variant carries no payload to display, so the
            // message is static and the match stays total under ANY
            // unification.
            Err(_) => FireOutcome::Failure(JOB_UNCLASSIFIED_MESSAGE.to_owned()),
        }
    }
}

/// First schedule tick: one step from now (`None` from an exhausted
/// schedule parks and re-evaluates).
fn first_tick(trigger: &crate::trigger::Trigger) -> SystemTime {
    let now = SystemTime::now();
    trigger
        .next_fire_after(now)
        .unwrap_or_else(|| now + SCHEDULE_PARK)
}

/// The next grid tick after `tick`; an exhausted schedule parks.
fn next_tick_or_park(
    trigger: &crate::trigger::Trigger,
    tick: SystemTime,
    completed: SystemTime,
) -> SystemTime {
    trigger
        .next_fire_after(tick)
        .unwrap_or_else(|| completed + SCHEDULE_PARK)
}

/// `SystemTime` → `std::time::Instant` by explicit offset arithmetic
/// from *now* (the monotonic-clock anchor). `Instant::from(SystemTime)`
/// is avoided deliberately — its availability is platform-dependent.
fn std_instant_at(at: SystemTime) -> Instant {
    let now_system = SystemTime::now();
    let now_instant = Instant::now();
    match at.duration_since(now_system) {
        Ok(ahead) => now_instant.checked_add(ahead).unwrap_or(now_instant),
        Err(behind) => now_instant
            .checked_sub(behind.duration())
            .unwrap_or(now_instant),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::error::JobError;
    use crate::job::Job;
    use futures::future::BoxFuture;
    use std::sync::atomic::AtomicU32;

    /// A `Job` from a plain async closure.
    fn job<F, T>(f: F) -> Job
    where
        F: Fn(JobContext) -> T + Send + Sync + 'static,
        T: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
    {
        Arc::new(move |ctx: JobContext| Box::pin(f(ctx)) as BoxFuture<'static, _>)
    }

    fn runner(job_closure: Job, budget: u32) -> JobRunner {
        let spec = JobSpec {
            name: "test-job".to_owned(),
            trigger: crate::trigger::Trigger::Interval(Duration::from_millis(10)),
            closure: job_closure,
            failure_budget: budget,
            leader: false,
        };
        JobRunner::new(spec, JitterPolicy::new(0.0))
    }

    #[tokio::test]
    async fn failures_trip_degraded_at_the_budget() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_job = Arc::clone(&calls);
        let job_runner = runner(
            job(move |_ctx| {
                calls_for_job.fetch_add(1, Ordering::Relaxed);
                async { Err(JobError::msg("down")) }
            }),
            3,
        );
        let guard = ShutdownGuard::new();

        for _ in 0..3 {
            job_runner.fire(&guard, "holder").await;
        }
        let status = job_runner.status();
        assert_eq!(status.fires, 3);
        assert_eq!(status.failures, 3);
        assert_eq!(status.consecutive_failures, 3);
        assert!(status.degraded, "budget 3 tripped by the 3rd failure");
        assert_eq!(status.last_error.as_deref(), Some("down"));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert_eq!(status.paused, 0);
        assert_eq!(status.skips, 0);
    }

    #[tokio::test]
    async fn a_success_resets_degraded() {
        let flip = Arc::new(AtomicU32::new(0));
        let flip_for_job = Arc::clone(&flip);
        let job_runner = runner(
            job(move |_ctx| {
                let n = flip_for_job.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n < 2 {
                        Err(JobError::msg("down"))
                    } else {
                        Ok(())
                    }
                }
            }),
            2,
        );
        let guard = ShutdownGuard::new();

        job_runner.fire(&guard, "holder").await;
        job_runner.fire(&guard, "holder").await;
        assert!(job_runner.status().degraded);

        job_runner.fire(&guard, "holder").await;
        let status = job_runner.status();
        assert!(!status.degraded, "a success clears degradation");
        assert_eq!(status.consecutive_failures, 0);
        assert_eq!(status.failures, 2);
        assert_eq!(status.fires, 3);
        // last_error stays as the most recent failure for diagnosis.
        assert_eq!(status.last_error.as_deref(), Some("down"));
    }

    #[tokio::test]
    async fn panics_count_as_failures_with_a_static_message() {
        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_job = Arc::clone(&calls);
        let job_runner = runner(
            job(move |_ctx: JobContext| {
                let n = calls_for_job.fetch_add(1, Ordering::Relaxed);
                async move {
                    assert!(n != 0, "job blew up on its first invocation");
                    Ok::<(), JobError>(())
                }
            }),
            10,
        );
        let guard = ShutdownGuard::new();

        job_runner.fire(&guard, "holder").await;
        let status = job_runner.status();
        assert_eq!(status.fires, 1);
        assert_eq!(status.failures, 1);
        assert_eq!(
            status.last_error.as_deref(),
            Some(JOB_PANIC_MESSAGE),
            "the panic payload has no Display — the message must be static"
        );
    }

    #[tokio::test]
    async fn summary_matches_status_counters() {
        let job_runner = runner(job(|_ctx: JobContext| async { Ok::<(), JobError>(()) }), 5);
        let guard = ShutdownGuard::new();
        job_runner.fire(&guard, "holder").await;
        job_runner.fire(&guard, "holder").await;

        let summary = job_runner.summary();
        assert_eq!(summary.name, "test-job");
        assert_eq!(summary.fires, 2);
        assert_eq!(summary.failures, 0);
        assert!(!summary.degraded);
        assert_eq!(summary.last_error, None);
    }

    #[cfg(feature = "breaker")]
    #[cfg(feature = "leader")]
    mod leader_tests {
        use super::*;

        /// Renewals fail while `others_hold` is set — as if another
        /// instance took the lease. Acquisition always succeeds when the
        /// lease is free (the `SET NX` shape).
        struct LapsingLease {
            others_hold: AtomicBool,
        }

        impl LapsingLease {
            fn new() -> Self {
                Self {
                    others_hold: AtomicBool::new(false),
                }
            }
        }

        #[async_trait::async_trait]
        impl crate::leader::Lease for LapsingLease {
            async fn acquire(&self, _holder: &str, _ttl: Duration) -> bool {
                !self.others_hold.load(Ordering::Relaxed)
            }
            async fn renew(&self, _holder: &str, _ttl: Duration) -> bool {
                !self.others_hold.load(Ordering::Relaxed)
            }
        }

        #[tokio::test]
        async fn a_lapsed_leader_reacquires_instead_of_stalling() {
            let lease = Arc::new(LapsingLease::new());
            let job_runner = JobRunner::new(
                JobSpec {
                    name: "crowned".to_owned(),
                    trigger: crate::trigger::Trigger::Interval(Duration::from_millis(10)),
                    closure: Arc::new(|_ctx: JobContext| {
                        Box::pin(async { Ok::<(), JobError>(()) })
                    }),
                    failure_budget: 5,
                    leader: true,
                },
                JitterPolicy::new(0.0),
            )
            .with_lease(Some(lease.clone()), Duration::from_secs(30));
            let guard = ShutdownGuard::new();

            // Fire 1: elected by acquisition, fires.
            job_runner.fire(&guard, "holder").await;
            assert_eq!(job_runner.status().fires, 1);

            // Fire 2: the lease was taken over — renew fails, the SET-NX
            // acquire fails too: a recorded skip.
            lease.others_hold.store(true, Ordering::Relaxed);
            job_runner.fire(&guard, "holder").await;
            let status = job_runner.status();
            assert_eq!(status.fires, 1, "a non-leader never fires");
            assert_eq!(status.skips, 1);

            // Fire 3: the other holder is gone. The failed renew must
            // fall back to acquire — a stalled "renew forever" leader
            // would never fire again even with the lease free.
            lease.others_hold.store(false, Ordering::Relaxed);
            job_runner.fire(&guard, "holder").await;
            let status = job_runner.status();
            assert_eq!(status.fires, 2, "the lapsed leader reacquired");
            assert_eq!(status.skips, 1);
        }
    }

    #[cfg(feature = "breaker")]
    mod breaker_tests {
        use super::*;
        use breaker::{BackoffStrategy, CircuitBreakerConfig};

        fn tiny_breaker() -> CircuitBreakerConfig {
            CircuitBreakerConfig::builder()
                .consecutive_failures(2)
                .failure_rate_threshold(1.0)
                .sliding_window_size(10)
                .backoff(BackoffStrategy::Fixed(Duration::from_millis(50)))
                .half_open_max_calls(1)
                .success_threshold(1)
                .build()
        }

        #[tokio::test]
        async fn open_breaker_pauses_instead_of_firing() {
            let job_runner = runner(
                job(|_ctx: JobContext| async { Err(JobError::msg("down")) }),
                50,
            )
            .with_breaker(tiny_breaker());
            let guard = ShutdownGuard::new();

            // Two failures trip the tiny breaker; the third fire attempt
            // is paused, not fired.
            job_runner.fire(&guard, "holder").await;
            job_runner.fire(&guard, "holder").await;
            assert_eq!(
                job_runner.breaker_state(),
                Some(breaker::State::Open),
                "two consecutive failures must open the tiny breaker"
            );

            job_runner.fire(&guard, "holder").await;
            let status = job_runner.status();
            assert_eq!(status.fires, 2, "paused fires are not fires");
            assert_eq!(status.failures, 2, "paused fires are not failures");
            assert_eq!(status.paused, 1, "the open breaker pauses the fire");

            // The breaker's own metrics saw exactly the two real failures.
            let metrics = job_runner.breaker_metrics().expect("breaker attached");
            assert_eq!(metrics.total_failures, 2);
        }

        #[tokio::test]
        async fn half_open_probe_recovers_to_closed() {
            let flip = Arc::new(AtomicU32::new(0));
            let flip_for_job = Arc::clone(&flip);
            let job_runner = runner(
                job(move |_ctx| {
                    let n = flip_for_job.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if n < 2 {
                            Err(JobError::msg("down"))
                        } else {
                            Ok(())
                        }
                    }
                }),
                50,
            )
            .with_breaker(tiny_breaker());
            let guard = ShutdownGuard::new();

            job_runner.fire(&guard, "holder").await;
            job_runner.fire(&guard, "holder").await;
            assert_eq!(job_runner.breaker_state(), Some(breaker::State::Open));

            // Ride out the 50 ms fixed cooldown; the next fire is a
            // half-open probe that succeeds and closes the circuit.
            tokio::time::sleep(Duration::from_millis(60)).await;
            assert_eq!(
                job_runner.breaker_state(),
                Some(breaker::State::HalfOpen),
                "after the open wait the breaker probes"
            );
            job_runner.fire(&guard, "holder").await;
            assert_eq!(
                job_runner.breaker_state(),
                Some(breaker::State::Closed),
                "a successful probe must close the circuit"
            );
            let status = job_runner.status();
            assert_eq!(status.fires, 3);
            assert_eq!(status.paused, 0, "no fire was paused in this run");
            assert_eq!(status.failures, 2);
        }
    }
}
