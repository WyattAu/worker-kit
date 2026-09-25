//! Hermetic integration tests: supervisor lifecycle, coalescing,
//! degraded budgets, breaker pauses, leader skips, drain timing, and
//! report exactness. No network, no Docker.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use futures::future::BoxFuture;
use shutdown_kit::ShutdownGuard;
use worker_kit::{JitterPolicy, Job, JobContext, JobError, JobSpec, Trigger, WorkerSupervisor};

/// A `Job` from a plain async closure.
fn boxed<F, T>(f: F) -> Job
where
    F: Fn(JobContext) -> T + Send + Sync + 'static,
    T: std::future::Future<Output = Result<(), JobError>> + Send + 'static,
{
    Arc::new(move |ctx: JobContext| Box::pin(f(ctx)) as BoxFuture<'static, Result<(), JobError>>)
}

/// A supervisor with deterministic (zero) jitter unless asked otherwise.
fn supervisor(jittered: bool) -> (ShutdownGuard, WorkerSupervisor) {
    let guard = ShutdownGuard::new();
    let mut supervisor = WorkerSupervisor::new(guard.clone());
    if !jittered {
        supervisor.jitter(JitterPolicy::new(0.0));
    }
    (guard, supervisor)
}

/// Poll `cond` until it holds or `timeout` elapses (then fail with
/// `msg`). Yields to the runtime between polls so the supervisor's
/// loops actually run.
async fn wait_until(timeout: Duration, msg: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting: {msg}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn status_of(supervisor: &WorkerSupervisor, name: &str) -> worker_kit::JobStatus {
    supervisor
        .status()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no status for {name}"))
}

fn millis_since(start: SystemTime) -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(start)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

#[test]
fn name_validation_is_typed_and_eager() {
    let (guard, mut supervisor) = supervisor(false);
    let spec = |name: &str| {
        JobSpec::new(
            name,
            Trigger::Interval(Duration::from_millis(10)),
            boxed(|_ctx| async { Ok::<(), JobError>(()) }),
        )
    };

    let too_long = "a".repeat(worker_kit::NAME_MAX_LEN + 1);
    for bad in ["", "UPPER", "has space", "bang!", too_long.as_str()] {
        let Err(err) = supervisor.register(spec(bad)) else {
            panic!("must reject {bad:?}");
        };
        assert!(
            matches!(err, worker_kit::RegisterError::InvalidName { ref name } if name == bad),
            "the typed error must carry the rejected name"
        );
    }
    for good in ["a", "metrics-rollup", "nightly_reindex.v2"] {
        supervisor.register(spec(good)).expect("must accept");
    }
    guard.shutdown();
}

#[tokio::test]
async fn interval_coalescing_fires_the_next_immediately_without_stacking() {
    const RUN_MS: u64 = 120;
    let (guard, mut supervisor) = supervisor(false);
    supervisor
        .register(JobSpec::new(
            "sleeper",
            Trigger::Interval(Duration::from_millis(50)),
            boxed(|_ctx| async move {
                tokio::time::sleep(Duration::from_millis(RUN_MS)).await;
                Ok::<(), JobError>(())
            }),
        ))
        .expect("valid name");

    let supervisor = Arc::new(supervisor);
    let runner = tokio::spawn(Arc::clone(&supervisor).run());
    let started = SystemTime::now();

    tokio::time::sleep(Duration::from_millis(700)).await;
    let elapsed_ms = millis_since(started);
    let fires = status_of(&supervisor, "sleeper").fires;

    // No stacking: every fire consumes at least the 120 ms run, so the
    // fire count cannot exceed the elapsed time divided by the run
    // length (plus slop for the very first scheduled tick).
    assert!(
        fires * RUN_MS <= elapsed_ms + 150,
        "{fires} fires of {RUN_MS} ms cannot fit in {elapsed_ms} ms without stacking"
    );
    // Liveness: it kept running on the coalesced cadence.
    assert!(fires >= 3, "expected several coalesced fires, got {fires}");

    guard.shutdown();
    let report = tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("drains under 2 s")
        .expect("join");
    assert_eq!(report.per_job.len(), 1);
    assert!(
        report.per_job[0].fires >= fires,
        "the report must account for every fire observed in status"
    );
}

#[tokio::test]
async fn degraded_after_the_failure_budget_but_still_running() {
    let (guard, mut supervisor) = supervisor(false);
    supervisor
        .register(JobSpec {
            name: "doomed".to_owned(),
            trigger: Trigger::Interval(Duration::from_millis(15)),
            closure: boxed(|_ctx| async { Err(JobError::msg("down")) }),
            failure_budget: 3,
            leader: false,
        })
        .expect("valid name");

    let supervisor = Arc::new(supervisor);
    let runner = tokio::spawn(Arc::clone(&supervisor).run());

    wait_until(
        Duration::from_secs(5),
        "degrade at 3 consecutive failures",
        || status_of(&supervisor, "doomed").degraded,
    )
    .await;

    // It keeps running (and failing) past the budget.
    let before = status_of(&supervisor, "doomed").fires;
    tokio::time::sleep(Duration::from_millis(120)).await;
    let after = status_of(&supervisor, "doomed");
    assert!(after.fires > before, "a degraded job keeps running");
    assert_eq!(after.failures, after.fires, "every fire failed");
    assert!(after.consecutive_failures >= 3);
    assert_eq!(after.last_error.as_deref(), Some("down"));

    guard.shutdown();
    let report = tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("drains under 2 s")
        .expect("join");
    let summary = &report.per_job[0];
    assert!(summary.degraded);
    assert_eq!(summary.failures, summary.fires);
    assert_eq!(summary.last_error.as_deref(), Some("down"));
}

#[tokio::test]
async fn the_first_fire_waits_one_full_interval() {
    const INTERVAL_MS: u64 = 150;
    let (guard, mut supervisor) = supervisor(false);
    let first_fire_ms = Arc::new(AtomicU64::new(0));
    let stamp = Arc::clone(&first_fire_ms);
    let started = SystemTime::now();
    supervisor
        .register(JobSpec::new(
            "late-starter",
            Trigger::Interval(Duration::from_millis(INTERVAL_MS)),
            boxed(move |_ctx| {
                stamp.fetch_max(millis_since(started), Ordering::Relaxed);
                async { Ok::<(), JobError>(()) }
            }),
        ))
        .expect("valid name");

    let supervisor = Arc::new(supervisor);
    let runner = tokio::spawn(Arc::clone(&supervisor).run());

    wait_until(Duration::from_secs(3), "first fire", || {
        supervisor.status()[0].fires >= 1
    })
    .await;

    let fired_at = first_fire_ms.load(Ordering::Relaxed);
    assert!(
        fired_at >= INTERVAL_MS.saturating_sub(20),
        "first fire at {fired_at} ms must wait the {INTERVAL_MS} ms interval (jitter 0)"
    );

    guard.shutdown();
    tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("drains under 2 s")
        .expect("join");
}

#[cfg(feature = "breaker")]
mod breaker_tests {
    use super::*;
    use breaker::BackoffStrategy;

    fn tiny_breaker(open_wait: Duration) -> breaker::CircuitBreakerConfig {
        breaker::CircuitBreakerConfig::builder()
            .consecutive_failures(2)
            .failure_rate_threshold(1.0)
            .sliding_window_size(10)
            .backoff(BackoffStrategy::Fixed(open_wait))
            .half_open_max_calls(1)
            .success_threshold(1)
            .build()
    }

    #[tokio::test]
    async fn breaker_pauses_then_half_open_recovers() {
        let (guard, mut supervisor) = supervisor(false);
        supervisor.breaker_config(tiny_breaker(Duration::from_millis(150)));

        let calls = Arc::new(AtomicU32::new(0));
        let calls_for_job = Arc::clone(&calls);
        supervisor
            .register(JobSpec {
                name: "flaky".to_owned(),
                trigger: Trigger::Interval(Duration::from_millis(20)),
                closure: boxed(move |_ctx| {
                    let n = calls_for_job.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if n < 3 {
                            Err(JobError::msg(format!("failure {n}")))
                        } else {
                            Ok::<(), JobError>(())
                        }
                    }
                }),
                failure_budget: 50,
                leader: false,
            })
            .expect("valid name");

        let supervisor = Arc::new(supervisor);
        let runner = tokio::spawn(Arc::clone(&supervisor).run());

        // Two consecutive failures trip the tiny breaker.
        wait_until(Duration::from_secs(5), "breaker opens", || {
            supervisor.breaker_state("flaky") == Some(breaker::State::Open)
        })
        .await;
        assert_eq!(
            status_of(&supervisor, "flaky").fires,
            2,
            "the trip is the second consecutive failure"
        );

        // While open: ticks keep arriving but the closure is not invoked
        // — paused, not fired, not failed.
        let before = status_of(&supervisor, "flaky");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let during = status_of(&supervisor, "flaky");
        assert_eq!(during.fires, before.fires, "an open breaker pauses fires");
        assert_eq!(
            during.failures, before.failures,
            "paused attempts are not failures"
        );
        assert!(
            during.paused > before.paused,
            "paused attempts are recorded"
        );

        // After the fixed 150 ms open wait a half-open probe runs, fails
        // (call 3), re-trips, and a later probe succeeds (call 4) and
        // closes the circuit.
        wait_until(
            Duration::from_secs(5),
            "breaker closes after probes",
            || {
                supervisor.breaker_state("flaky") == Some(breaker::State::Closed)
                    && status_of(&supervisor, "flaky").fires >= 4
            },
        )
        .await;

        let status = status_of(&supervisor, "flaky");
        assert_eq!(status.failures, 3, "exactly three real failures");
        assert_eq!(calls.load(Ordering::Relaxed), 3 + 1, "3 failures + 1 probe");
        assert!(!status.degraded, "budget 50 never trips");
        let metrics = supervisor.breaker_metrics();
        assert_eq!(metrics["flaky"].total_failures, 3);

        guard.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("drains under 2 s")
            .expect("join");
    }
}

#[cfg(feature = "leader")]
mod leader_tests {
    use super::*;
    use std::time::Duration as StdDuration;

    struct NeverLease;

    #[async_trait::async_trait]
    impl worker_kit::Lease for NeverLease {
        async fn acquire(&self, _holder: &str, _ttl: StdDuration) -> bool {
            false
        }
        async fn renew(&self, _holder: &str, _ttl: StdDuration) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn non_leaders_skip_and_leaders_fire() {
        let (guard, mut supervisor) = supervisor(false);
        supervisor.lease(Arc::new(NeverLease));
        let spec = |name: &str, leader: bool| JobSpec {
            name: name.to_owned(),
            trigger: Trigger::Interval(Duration::from_millis(15)),
            closure: boxed(|_ctx| async { Ok::<(), JobError>(()) }),
            failure_budget: 5,
            leader,
        };
        supervisor.register(spec("crowned", true)).expect("valid");
        supervisor.register(spec("commoner", false)).expect("valid");

        let supervisor = Arc::new(supervisor);
        let runner = tokio::spawn(Arc::clone(&supervisor).run());

        wait_until(Duration::from_secs(3), "skips are recorded", || {
            status_of(&supervisor, "crowned").skips > 0
        })
        .await;

        let crowned = status_of(&supervisor, "crowned");
        assert_eq!(crowned.fires, 0, "a non-leader never fires");
        assert_eq!(crowned.failures, 0, "a skip is not a failure");
        let commoner = status_of(&supervisor, "commoner");
        assert!(commoner.fires > 0, "non-leader jobs fire regardless");
        assert_eq!(commoner.skips, 0);

        guard.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("drains under 2 s")
            .expect("join");
    }

    #[tokio::test]
    async fn memory_lease_always_wins_so_leader_jobs_fire() {
        let (guard, mut supervisor) = supervisor(false);
        supervisor.lease(Arc::new(worker_kit::MemoryLease));
        supervisor
            .register(JobSpec {
                name: "crowned".to_owned(),
                trigger: Trigger::Interval(Duration::from_millis(15)),
                closure: boxed(|_ctx| async { Ok::<(), JobError>(()) }),
                failure_budget: 5,
                leader: true,
            })
            .expect("valid");

        let supervisor = Arc::new(supervisor);
        let runner = tokio::spawn(Arc::clone(&supervisor).run());

        wait_until(Duration::from_secs(3), "the leader fires", || {
            status_of(&supervisor, "crowned").fires > 0
        })
        .await;
        assert_eq!(status_of(&supervisor, "crowned").skips, 0);

        guard.shutdown();
        tokio::time::timeout(Duration::from_secs(2), runner)
            .await
            .expect("drains under 2 s")
            .expect("join");
    }
}

#[tokio::test]
async fn drain_completes_in_flight_work_under_two_seconds() {
    let (guard, mut supervisor) = supervisor(false);
    let completed = Arc::new(AtomicU32::new(0));
    let completed_for_job = Arc::clone(&completed);
    // The run (250 ms) fits inside the interval (300 ms), so after the
    // first fire completes the next tick is a full interval away —
    // shutting down in that window must drain exactly one in-flight run.
    supervisor
        .register(JobSpec::new(
            "slowpoke",
            Trigger::Interval(Duration::from_millis(300)),
            boxed(move |_ctx| {
                let completed = Arc::clone(&completed_for_job);
                async move {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    completed.fetch_add(1, Ordering::Relaxed);
                    Ok::<(), JobError>(())
                }
            }),
        ))
        .expect("valid name");

    let supervisor = Arc::new(supervisor);
    let runner = tokio::spawn(Arc::clone(&supervisor).run());

    // Wait for the first fire to *start*, then signal shutdown while it
    // is still sleeping (250 ms run vs 50 ms poll).
    wait_until(Duration::from_secs(3), "first fire starts", || {
        status_of(&supervisor, "slowpoke").fires >= 1
    })
    .await;

    let drain_started = Instant::now();
    guard.shutdown();
    let report = tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("drain must complete under 2 s")
        .expect("join");
    assert!(
        drain_started.elapsed() < Duration::from_secs(2),
        "drain took {:?}",
        drain_started.elapsed()
    );
    // Graceful: the in-flight run finished and was reported.
    assert_eq!(completed.load(Ordering::Relaxed), 1);
    assert_eq!(report.per_job[0].fires, 1);
    assert_eq!(report.per_job[0].failures, 0);
}

#[tokio::test]
async fn run_report_is_exact_and_name_ordered() {
    let (guard, mut supervisor) = supervisor(false);
    let calls = Arc::new(AtomicU32::new(0));
    let calls_for_job = Arc::clone(&calls);
    supervisor
        .register(JobSpec {
            name: "flaky".to_owned(),
            trigger: Trigger::Interval(Duration::from_millis(15)),
            closure: boxed(move |_ctx| {
                let n = calls_for_job.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n == 0 {
                        Err(JobError::msg("boom 1"))
                    } else if n == 1 {
                        Err(JobError::msg("boom 2"))
                    } else {
                        Ok::<(), JobError>(())
                    }
                }
            }),
            failure_budget: 10,
            leader: false,
        })
        .expect("valid");
    supervisor
        .register(JobSpec::new(
            "steady",
            Trigger::Interval(Duration::from_millis(15)),
            boxed(|_ctx| async { Ok::<(), JobError>(()) }),
        ))
        .expect("valid");

    let supervisor = Arc::new(supervisor);
    let runner = tokio::spawn(Arc::clone(&supervisor).run());

    wait_until(Duration::from_secs(3), "flaky recovers", || {
        status_of(&supervisor, "flaky").fires >= 3
    })
    .await;

    guard.shutdown();
    let report = tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("drains under 2 s")
        .expect("join");

    // Name-ordered (BTreeMap order), every registered job present.
    let names: Vec<&str> = report.per_job.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["flaky", "steady"]);

    // Exactness: the runner's fires equal the closure's own invocation
    // count — every fire counted exactly once, none fabricated.
    let flaky = &report.per_job[0];
    assert_eq!(flaky.fires, u64::from(calls.load(Ordering::Relaxed)));
    assert_eq!(flaky.failures, 2, "exactly the first two calls failed");
    assert!(!flaky.degraded, "budget 10 is far above 2 failures");
    assert_eq!(flaky.last_error.as_deref(), Some("boom 2"));

    let steady = &report.per_job[1];
    assert!(steady.fires >= 1);
    assert_eq!(steady.failures, 0);
    assert!(!steady.degraded);
    assert_eq!(steady.last_error, None);
}

#[tokio::test]
async fn duplicate_names_register_independent_workers() {
    let (guard, mut supervisor) = supervisor(false);
    let spec = || {
        JobSpec::new(
            "twin",
            Trigger::Interval(Duration::from_millis(25)),
            boxed(|_ctx| async { Ok::<(), JobError>(()) }),
        )
    };
    supervisor.register(spec()).expect("valid");
    supervisor.register(spec()).expect("duplicates allowed");

    let supervisor = Arc::new(supervisor);
    let runner = tokio::spawn(Arc::clone(&supervisor).run());

    wait_until(Duration::from_secs(3), "both twins fire", || {
        supervisor
            .status()
            .iter()
            .filter(|s| s.name == "twin")
            .map(|s| s.fires)
            .sum::<u64>()
            >= 2
    })
    .await;

    guard.shutdown();
    let report = tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("drains under 2 s")
        .expect("join");
    assert_eq!(report.per_job.len(), 2, "both entries are reported");
    assert_eq!(report.per_job[0].name, "twin");
    assert_eq!(report.per_job[1].name, "twin");
}
