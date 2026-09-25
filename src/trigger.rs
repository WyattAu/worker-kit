//! Triggers (what starts a fire) and the full-jitter policy applied to
//! the wait before each fire.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

/// Smallest interval we honor. A `Trigger::Interval` below this is
/// clamped up: a zero interval would pin a core with back-to-back fires.
pub(crate) const MIN_INTERVAL: Duration = Duration::from_millis(1);

/// Default jitter fraction: waits are stretched by up to 20 % of the
/// gap between fires, drawn full-jitter (uniform over `[0, fraction ×
/// gap]`). See [`JitterPolicy`].
pub const DEFAULT_JITTER_FRACTION: f64 = 0.2;

/// When the schedule gives us nothing to compute (an exhausted cron
/// schedule), park the loop on this wait and re-evaluate.
pub(crate) const SCHEDULE_PARK: Duration = Duration::from_secs(60);

/// What starts a job.
///
/// - [`Trigger::Interval`] — a fixed cadence with [full
///   jitter](crate::JitterPolicy) and [coalescing](crate::WorkerSupervisor)
///   (a run outlasting its interval fires the next immediately after
///   completion — never stacked).
/// - [`Trigger::Cron`] (`cron` feature) — a cron expression evaluated in
///   **UTC**. Hours are wall-clock UTC: around a DST transition the local
///   meaning of a fixed UTC hour shifts by the offset, and the ambiguous
///   local hour is simply not represented. Schedules that must track
///   local civil time need a tz-aware scheduler — this kit deliberately
///   does not guess time zones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// Fire every `interval`, starting one (jittered) interval after the
    /// supervisor begins. Intervals below 1 ms are clamped to 1 ms.
    Interval(Duration),
    /// Fire on the cron schedule — a 6- or 7-field expression in the
    /// `cron` crate's syntax (`sec min hour dom mon dow [year]`, e.g.
    /// `"0 0 6 * * *"` for 06:00:00 UTC daily). Parsed at registration.
    #[cfg(feature = "cron")]
    Cron(String),
}

impl Trigger {
    /// The next un-jittered fire strictly after `after` — the schedule
    /// grid the runner jitters on top of. Public for tests, dashboards,
    /// and capacity planning: for [`Trigger::Interval`] this is
    /// `after + interval` (clamped to 1 ms); for [`Trigger::Cron`] it
    /// is the next UTC tick.
    ///
    /// `None` means "nothing schedulable" (an exhausted cron schedule).
    #[must_use]
    pub fn next_fire_after(&self, after: SystemTime) -> Option<SystemTime> {
        match self {
            Trigger::Interval(interval) => Some(after + (*interval).max(MIN_INTERVAL)),
            #[cfg(feature = "cron")]
            Trigger::Cron(expr) => {
                // Wall-clock (UTC) arithmetic on SystemTime, converted to
                // a monotonic Instant only at sleep time via explicit
                // offset arithmetic — `Instant::from(SystemTime)` is
                // avoided deliberately (portability of its trait bounds).
                let dt: chrono::DateTime<chrono::Utc> = after.into();
                let schedule: cron::Schedule = expr.parse().ok()?;
                schedule.after(&dt).next().map(SystemTime::from)
            }
        }
    }

    /// Parse and validate the trigger eagerly. Called at registration so
    /// a bad cron expression fails at startup.
    pub(crate) fn validate(&self) -> Result<(), crate::error::RegisterError> {
        #[cfg(feature = "cron")]
        if let Trigger::Cron(expr) = self {
            return expr
                .parse::<cron::Schedule>()
                .map(|_: cron::Schedule| ())
                .map_err(|source| crate::error::RegisterError::InvalidCron {
                    expr: expr.clone(),
                    source,
                });
        }
        Ok(())
    }
}

/// Full-jitter policy for the wait before each fire.
///
/// The gap between two schedule ticks (the interval, or the gap to the
/// next cron tick) is stretched by a delay drawn **full-jitter** —
/// uniformly from `[0, fraction × gap]`. Jitter decorrelates workers
/// that would otherwise all fire on the same boundary and stampede a
/// shared downstream; the default fraction of
/// [`DEFAULT_JITTER_FRACTION`] (0.2) keeps the cadence recognizable
/// while spreading the starts.
///
/// # Seeding
///
/// Each worker seeds its own [`SmallRng`] from the wall-clock nanos at
/// startup ([`clock_seed`]). The seed is **not** security-sensitive —
/// this is schedule jitter, not key material — and it is **not
/// reproducible across runs** by design: the only contract is the
/// *bound*, which [`jitter_seeded`] exposes deterministically for tests
/// and capacity planning.
#[derive(Debug, Clone, PartialEq)]
pub struct JitterPolicy {
    /// Fraction of the gap used as the jitter window, clamped to
    /// `[0.0, 1.0]` (`NaN` becomes `0.0`). `0.0` disables jitter.
    pub fraction: f64,
}

impl Default for JitterPolicy {
    fn default() -> Self {
        Self {
            fraction: DEFAULT_JITTER_FRACTION,
        }
    }
}

impl JitterPolicy {
    /// A policy with the given fraction, clamped to `[0.0, 1.0]` (`NaN`
    /// becomes `0.0`).
    #[must_use]
    pub fn new(fraction: f64) -> Self {
        Self {
            fraction: clamp_fraction(fraction),
        }
    }

    /// The jitter window for a gap of `gap`: `fraction × gap`,
    /// saturated at the gap itself.
    #[must_use]
    pub fn window(&self, gap: Duration) -> Duration {
        let scaled = gap.mul_f64(self.fraction);
        scaled.min(gap)
    }

    /// Full-jitter delay for a gap, drawn from the given RNG: uniform in
    /// `[0, fraction × gap]`.
    #[must_use]
    pub fn jitter(&self, gap: Duration, rng: &mut impl RngExt) -> Duration {
        let window = self.window(gap);
        if window.is_zero() {
            return Duration::ZERO;
        }
        rng.random_range(Duration::ZERO..=window)
    }

    /// Deterministic full-jitter delay for a gap from an explicit seed —
    /// same `(gap, seed)` reproduces the same delay. Production draws
    /// use [`jitter`](Self::jitter) from the per-worker RNG; this variant
    /// exists for tests and capacity planning.
    #[must_use]
    pub fn jitter_seeded(&self, gap: Duration, seed: u64) -> Duration {
        self.jitter(gap, &mut SmallRng::seed_from_u64(seed))
    }
}

/// Clamp a jitter fraction into `[0.0, 1.0]`; `NaN` becomes `0.0`.
fn clamp_fraction(fraction: f64) -> f64 {
    if fraction.is_nan() {
        0.0
    } else {
        fraction.clamp(0.0, 1.0)
    }
}

/// A per-worker RNG seed from the wall-clock nanos, pushed through the
/// `SplitMix64` finalizer so consecutive starts do not share low bits.
///
/// Not security-sensitive: the only property needed is that two workers
/// starting in the same millisecond still (virtually always) differ.
#[must_use]
pub fn clock_seed() -> u64 {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
        // u128 nanos truncated to u64: the low 64 bits already span
        // ~584 years of monotonic wall time — far more entropy than
        // a jitter seed needs.
        #[allow(clippy::cast_possible_truncation)]
        {
            d.as_nanos() as u64
        }
    });
    mix64(nanos)
}

/// The `SplitMix64` finalizer — a cheap, well-mixed scramble (not a PRNG
/// by itself).
fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    // Exact f64 literals compared for the documented defaults.
    #![allow(clippy::float_cmp)]
    use super::*;

    #[test]
    fn default_policy_matches_documented_fraction() {
        let policy = JitterPolicy::default();
        assert_eq!(policy.fraction, DEFAULT_JITTER_FRACTION);
        assert_eq!(DEFAULT_JITTER_FRACTION, 0.2);
    }

    #[test]
    fn new_clamps_fraction() {
        assert_eq!(JitterPolicy::new(-1.0).fraction, 0.0);
        assert_eq!(JitterPolicy::new(0.5).fraction, 0.5);
        assert_eq!(JitterPolicy::new(7.0).fraction, 1.0);
        assert_eq!(JitterPolicy::new(f64::NAN).fraction, 0.0);
    }

    #[test]
    fn window_is_fraction_of_gap_saturated() {
        let policy = JitterPolicy::new(0.25);
        assert_eq!(
            policy.window(Duration::from_secs(8)),
            Duration::from_secs(2)
        );
        // fraction > 1 saturates at the gap itself.
        assert_eq!(
            JitterPolicy::new(2.0).window(Duration::from_secs(4)),
            Duration::from_secs(4)
        );
        assert_eq!(
            JitterPolicy::new(0.9).window(Duration::ZERO),
            Duration::ZERO
        );
    }

    #[test]
    fn jitter_seeded_is_deterministic_and_bounded() {
        let policy = JitterPolicy::default();
        let gap = Duration::from_secs(30);
        let bound = policy.window(gap);
        for seed in 0..64_u64 {
            let delay = policy.jitter_seeded(gap, seed);
            assert!(delay <= bound, "seed {seed} left the window: {delay:?}");
        }
        assert_eq!(
            policy.jitter_seeded(gap, 42),
            policy.jitter_seeded(gap, 42),
            "same (gap, seed) must reproduce the delay"
        );
        assert_ne!(
            policy.jitter_seeded(gap, 42),
            policy.jitter_seeded(gap, 43),
            "distinct seeds must (virtually always) differ"
        );
    }

    #[test]
    fn zero_fraction_is_exact() {
        let policy = JitterPolicy::new(0.0);
        assert_eq!(
            policy.jitter_seeded(Duration::from_secs(5), 7),
            Duration::ZERO
        );
    }

    #[test]
    fn clock_seed_is_mixing() {
        let a = clock_seed();
        let b = clock_seed();
        // Two calls in the same process must (virtually always) differ...
        assert_ne!(a, b);
        // ...and the finalizer must scramble structured inputs.
        assert_ne!(mix64(0), 0);
        assert_eq!(mix64(123), mix64(123));
    }

    #[test]
    fn interval_tick_is_clamped_to_minimum() {
        let trigger = Trigger::Interval(Duration::ZERO);
        let now = SystemTime::now();
        let next = trigger.next_fire_after(now).expect("interval ticks");
        let gap = next.duration_since(now).expect("next is after now");
        assert_eq!(gap, MIN_INTERVAL, "zero interval clamps to 1 ms");
    }

    // Property: jittered delays never leave `[0, fraction × gap]` and are
    // deterministic per `(gap, seed)` — the bound is the entire contract
    // production code may rely on (the seed is not reproducible).
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(1000))]
        #[test]
        fn jitter_stays_within_the_window(gap_ms in 1_u64..=60_000, fraction in 0.0_f64..=1.0, seed in 0_u64..1_000) {
            let policy = JitterPolicy::new(fraction);
            let gap = Duration::from_millis(gap_ms);
            let bound = policy.window(gap);
            proptest::prop_assert!(bound <= gap, "window must not exceed the gap");
            let delay = policy.jitter_seeded(gap, seed);
            proptest::prop_assert!(delay <= bound, "jitter {delay:?} left the window {bound:?}");
            proptest::prop_assert_eq!(delay, policy.jitter_seeded(gap, seed), "must be deterministic per seed");
        }
    }

    #[cfg(feature = "cron")]
    mod cron_tests {
        use super::*;
        use chrono::TimeZone;

        fn utc_secs(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> SystemTime {
            chrono::Utc
                .with_ymd_and_hms(y, mo, d, h, mi, s)
                .single()
                .expect("valid test timestamp")
                .into()
        }

        #[test]
        fn cron_tick_is_strictly_future_and_on_schedule() {
            let trigger = Trigger::Cron("0/15 * * * * *".to_owned()); // every 15 s
            let base = utc_secs(2026, 9, 26, 12, 0, 3);
            let next = trigger.next_fire_after(base).expect("next tick");
            let gap = next.duration_since(base).expect("strictly future");
            assert_eq!(gap, Duration::from_secs(12), "next quarter-hour mark");
            let after_that = trigger.next_fire_after(next).expect("next tick");
            assert_eq!(
                after_that.duration_since(next).expect("positive"),
                Duration::from_secs(15)
            );
        }

        #[test]
        fn cron_validation_rejects_garbage() {
            assert!(Trigger::Cron("not a schedule".to_owned())
                .validate()
                .is_err());
            assert!(
                Trigger::Cron("61 * * * * *".to_owned()).validate().is_err(),
                "second 61 is out of range"
            );
            assert!(Trigger::Cron("0 0 6 * * *".to_owned()).validate().is_ok());
        }

        #[test]
        fn interval_validation_always_passes() {
            assert!(Trigger::Interval(Duration::ZERO).validate().is_ok());
            assert!(Trigger::Interval(Duration::from_secs(1)).validate().is_ok());
        }
    }
}
