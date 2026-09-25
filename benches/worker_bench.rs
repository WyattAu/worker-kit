//! Criterion benches — smoke tests for the hot paths (the CI bench job
//! runs `cargo bench -- --test` as a sanity check, not a gate).

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

use worker_kit::{is_valid_name, JitterPolicy};

/// The jitter draw happens once per scheduled fire per worker.
fn bench_jitter(c: &mut Criterion) {
    let policy = JitterPolicy::default();
    let mut group = c.benchmark_group("jitter");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("jitter_seeded/30s_gap", |b| {
        b.iter(|| policy.jitter_seeded(Duration::from_secs(30), 42));
    });
    group.finish();
}

/// Name validation runs once per registration.
fn bench_name_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("name_validation");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("is_valid_name/typical", |b| {
        b.iter(|| is_valid_name("metrics-rollup.v2"));
    });
    group.bench_function("is_valid_name/invalid", |b| {
        b.iter(|| is_valid_name("INVALID NAME!"));
    });
    group.finish();
}

/// `Trigger::next_fire_after` for a cron expression (parse + next query)
/// happens once per scheduled fire for cron jobs.
#[cfg(feature = "cron")]
fn bench_cron_next_fire(c: &mut Criterion) {
    use std::time::SystemTime;

    use worker_kit::Trigger;

    let trigger = Trigger::Cron("0 0/5 * * * *".to_owned()); // every 5 min
    let mut group = c.benchmark_group("cron");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("next_fire_after/every_5_min", |b| {
        b.iter(|| trigger.next_fire_after(SystemTime::now()));
    });
    group.finish();
}

#[cfg(feature = "cron")]
criterion_group!(
    benches,
    bench_jitter,
    bench_name_validation,
    bench_cron_next_fire
);
#[cfg(not(feature = "cron"))]
criterion_group!(benches, bench_jitter, bench_name_validation);
criterion_main!(benches);
