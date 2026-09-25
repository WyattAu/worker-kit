//! Cron trigger tests (`cron` feature): parse validation at
//! registration, next-fire monotonicity, and UTC-fixed semantics (the
//! documented DST stance). Pure computation — no sleeping, no flakiness.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, TimeZone, Utc};
use futures::future::BoxFuture;
use shutdown_kit::ShutdownGuard;
use worker_kit::{JobContext, JobError, JobSpec, RegisterError, Trigger, WorkerSupervisor};

fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
        .single()
        .expect("valid test timestamp")
}

fn trigger(expr: &str) -> Trigger {
    Trigger::Cron(expr.to_owned())
}

fn next(trigger: &Trigger, after: DateTime<Utc>) -> DateTime<Utc> {
    trigger
        .next_fire_after(after.into())
        .expect("cron schedules always have a next tick")
        .into()
}

fn noop_spec(name: &str, expr: &str) -> JobSpec {
    JobSpec {
        name: name.to_owned(),
        trigger: trigger(expr),
        closure: Arc::new(|_ctx: JobContext| {
            Box::pin(async { Ok::<(), JobError>(()) }) as BoxFuture<'static, Result<(), JobError>>
        }),
        failure_budget: 5,
        leader: false,
    }
}

#[test]
fn next_fire_is_strictly_monotonic_and_on_the_grid() {
    let t = trigger("0/2 * * * * *"); // every 2 s (seconds 0, 2, 4, ...)
    let cursor = utc(2026, 9, 26, 12, 0, 3);
    let mut fired = next(&t, cursor); // :03 -> :04 (the first gap to the grid is partial)
    assert_eq!(fired - cursor, chrono::Duration::seconds(1));
    for _ in 0..10 {
        let following = next(&t, fired);
        assert!(
            following > fired,
            "next fire must be strictly after the anchor"
        );
        assert_eq!(
            following - fired,
            chrono::Duration::seconds(2),
            "consecutive fires are 2 s apart"
        );
        fired = following;
    }
}

#[test]
fn next_fire_is_monotone_in_the_anchor() {
    // For any two anchors a < b: next(a) <= next(b) — scheduling can
    // never move backwards when a run overruns its gap.
    let t = trigger("0/7 * * * * *"); // every 7 s
    let base = utc(2026, 1, 31, 23, 59, 50);
    let mut prev_next: Option<DateTime<Utc>> = None;
    for s in 0..30_u32 {
        let anchor = base + chrono::Duration::seconds(i64::from(s));
        let fired = next(&t, anchor);
        assert!(fired > anchor);
        if let Some(prev) = prev_next {
            assert!(
                fired >= prev,
                "a later anchor must never schedule an earlier fire"
            );
        }
        prev_next = Some(fired);
    }
}

#[test]
fn daily_schedule_lands_on_the_utc_wall_clock() {
    let t = trigger("0 0 6 * * *"); // 06:00:00 UTC daily
    let fired = next(&t, utc(2026, 9, 26, 12, 0, 0));
    assert_eq!(fired, utc(2026, 9, 27, 6, 0, 0));
}

#[test]
fn utc_semantics_hold_across_a_dst_transition() {
    // US DST spring-forward: 2026-03-08, 02:00 local (New York) jumps to
    // 03:00. In UTC nothing happens — the documented stance: cron here
    // is UTC-fixed, so hourly ticks stay exactly 3600 s apart.
    let t = trigger("0 0 * * * *");
    let first = next(&t, utc(2026, 3, 8, 5, 30, 0));
    let second = next(&t, first);
    assert_eq!(first, utc(2026, 3, 8, 6, 0, 0));
    assert_eq!(second - first, chrono::Duration::seconds(3600));
}

#[test]
fn invalid_cron_fails_registration_eagerly() {
    let guard = ShutdownGuard::new();
    let mut supervisor = WorkerSupervisor::new(guard.clone());

    let Err(err) = supervisor.register(noop_spec("cronned", "not a schedule")) else {
        panic!("garbage must be rejected at registration");
    };
    assert!(
        matches!(&err, RegisterError::InvalidCron { expr, source }
            if expr == "not a schedule" && !source.to_string().is_empty()),
        "expected InvalidCron carrying the expression, got {err:?}"
    );
    assert!(
        supervisor
            .register(noop_spec("cronned", "61 * * * * *"))
            .is_err(),
        "second 61 is out of range"
    );
    assert!(
        supervisor
            .register(noop_spec("cronned", "0 0 6 * * *"))
            .is_ok(),
        "a valid expression registers"
    );

    guard.shutdown();
}

#[test]
fn interval_triggers_compute_next_fire_too() {
    // Same public path the runner uses, exercised for Interval.
    let t = Trigger::Interval(Duration::from_secs(10));
    let anchor: SystemTime = utc(2026, 9, 26, 0, 0, 0).into();
    let fired = t.next_fire_after(anchor).expect("interval ticks");
    assert_eq!(
        fired.duration_since(anchor).expect("positive"),
        Duration::from_secs(10)
    );
}
