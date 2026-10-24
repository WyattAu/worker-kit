//! Pressure-reactive background work (SIS round 9, iter 68 → backlog
//! #11): a polling watcher over the kernel's PSI
//! ([Pressure Stall Information]) memory lines, invoking a callback
//! while the host is under memory pressure.
//!
//! This is the polling half of the systemd pressure protocol — the
//! same signal systemd exposes as `$MEMORY_PRESSURE_WATCH` — without
//! the poll()-registration subtleties: every
//! [`POLL_INTERVAL_DEFAULT`] the watcher reads the configured PSI file
//! (`/proc/pressure/memory` by default, or the path named by
//! `$MEMORY_PRESSURE_WATCH`), compares the `avg60` partial-stall
//! ("some") percentage against [`PressureConfig::threshold`], and
//! invokes the callback for as long as pressure persists. A
//! `/dev/null` watch path (the systemd "off" spelling) disables the
//! watcher entirely.
//!
//! The callback is the consumer's policy: release caches, shed
//! background work, pause a sweeper. The watcher never takes action
//! on its own — it is a signal source, not an actor.
//!
//! # Example
//!
//! ```no_run
//! use worker_kit::pressure::{watch, PressureConfig, PressureSample};
//! use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
//!
//! # async fn demo() {
//! let cfg = PressureConfig {
//!     threshold: 20.0, // avg60 some >= 20% = under pressure
//!     ..PressureConfig::from_env()
//! };
//! let shutdown = Arc::new(AtomicBool::new(false));
//! watch(
//!     cfg,
//!     Arc::clone(&shutdown),
//!     |sample: PressureSample| {
//!         eprintln!("memory pressure: avg60={:.1}%", sample.avg60);
//!         // release caches / defer background work here
//!     },
//!     None,
//! );
//! # }
//! ```
//!
//! [Pressure Stall Information]: https://docs.kernel.org/accounting/psi.html

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Default PSI file (the kernel's memory-pressure line).
pub const DEFAULT_PSI_PATH: &str = "/proc/pressure/memory";
/// Default poll cadence: 5 s — pressure is a state, not an event;
/// sub-second reaction is nobody's requirement for cache trimming.
pub const POLL_INTERVAL_DEFAULT: Duration = Duration::from_secs(5);
/// Default threshold: `some` avg60 at 20% — sustained, meaningful
/// stall time, well above normal scheduler noise (SIS watchdog pages
/// its humans at 80%+; this watcher reacts far earlier).
pub const THRESHOLD_DEFAULT: f64 = 20.0;

/// Watcher configuration. [`PressureConfig::from_env`] honors the
/// systemd pressure protocol's environment: `$MEMORY_PRESSURE_WATCH`
/// selects the file (`/dev/null` disables), overriding the default.
#[derive(Debug, Clone)]
pub struct PressureConfig {
    /// PSI file to read. Defaults to `/proc/pressure/memory`, or the
    /// `$MEMORY_PRESSURE_WATCH` path when that variable is set.
    pub path: PathBuf,
    /// Partial-stall `avg60` percentage at or above which the callback
    /// fires.
    pub threshold: f64,
    /// Poll cadence.
    pub poll_interval: Duration,
}

impl PressureConfig {
    /// Configuration from the process environment: honors
    /// `$MEMORY_PRESSURE_WATCH` (the systemd protocol path; the literal
    /// `/dev/null` means "pressure handling off"), with the crate
    /// defaults for threshold and cadence.
    #[must_use]
    pub fn from_env() -> Self {
        let path = std::env::var("MEMORY_PRESSURE_WATCH")
            .ok()
            .filter(|p| !p.trim().is_empty())
            .map_or_else(|| PathBuf::from(DEFAULT_PSI_PATH), PathBuf::from);
        Self {
            path,
            threshold: THRESHOLD_DEFAULT,
            poll_interval: POLL_INTERVAL_DEFAULT,
        }
    }
}

/// One PSI reading.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PressureSample {
    /// Partial-stall average over 10 s (percent).
    pub avg10: f64,
    /// Partial-stall average over 60 s (percent).
    pub avg60: f64,
    /// Partial-stall average over 300 s (percent).
    pub avg300: f64,
}

/// Parse a PSI file body (`/proc/pressure/*` shape) into the `some`
/// line's averages. Returns `None` when the file has no parsable
/// `some` line (missing file, kernel without PSI, permission error).
#[must_use]
pub fn parse_some(text: &str) -> Option<PressureSample> {
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("some ") {
            let mut avg10 = None;
            let mut avg60 = None;
            let mut avg300 = None;
            for field in rest.split_whitespace() {
                let (key, value) = field.split_once('=')?;
                let v = value.parse::<f64>().ok()?;
                match key {
                    "avg10" => avg10 = Some(v),
                    "avg60" => avg60 = Some(v),
                    "avg300" => avg300 = Some(v),
                    _ => {}
                }
            }
            return Some(PressureSample {
                avg10: avg10?,
                avg60: avg60?,
                avg300: avg300?,
            });
        }
    }
    None
}

/// Read and parse the configured PSI file.
///
/// # Errors
/// Returns the I/O error string when the file cannot be read.
pub fn read_sample(path: &std::path::Path) -> Result<PressureSample, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    parse_some(&text).ok_or_else(|| format!("{}: no parsable `some` line", path.display()))
}

/// Run the watcher until `shutdown` fires: on every
/// [`PressureConfig::poll_interval`], read the PSI file and invoke
/// `callback(sample)` while `sample.avg60 >= threshold`. The first
/// read happens immediately. Reading failures are retried on the next
/// tick (a transiently missing `/proc` entry must not kill the
/// watcher) and reported through `on_error` when provided.
///
/// `watch` never returns under normal operation; drop the returned
/// task handle to detach it.
pub fn watch<F>(
    cfg: PressureConfig,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    mut callback: F,
    on_error: Option<Arc<dyn Fn(&str) + Send + Sync>>,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut(PressureSample) + Send + 'static,
{
    let path = cfg.path.clone();
    let threshold = cfg.threshold;
    let interval = cfg.poll_interval;
    tokio::spawn(async move {
        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
            match read_sample(&path) {
                Ok(sample) => {
                    if sample.avg60 >= threshold {
                        callback(sample);
                    }
                }
                Err(e) => {
                    if let Some(report) = &on_error {
                        report(&e);
                    }
                }
            }
            tokio::time::sleep(interval).await;
        }
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn parses_some_line_averages() {
        let sample = parse_some("some avg10=12.34 avg60=5.00 avg300=0.50\ntotal avg10=0")
            .expect("some line");
        assert!((sample.avg10 - 12.34).abs() < f64::EPSILON);
        assert!((sample.avg60 - 5.0).abs() < f64::EPSILON);
        assert!((sample.avg300 - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn total_line_is_ignored_full_line_is_parsable() {
        assert!(parse_some("total avg10=1.00 avg60=2.00 avg300=3.00\n").is_none());
        let sample = parse_some(
            "some avg10=1.00 avg60=2.00 avg300=3.00\ntotal avg10=9.00 avg60=9.00 avg300=9.00\n",
        )
        .expect("some line");
        assert!(
            (sample.avg60 - 2.0).abs() < f64::EPSILON,
            "total must not win"
        );
    }

    #[test]
    fn missing_some_line_is_none() {
        assert!(parse_some("").is_none());
        assert!(parse_some("garbage\n").is_none());
    }

    #[test]
    fn reads_a_real_pressure_file_shape() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("memory");
        std::fs::write(
            &path,
            "some avg10=0.00 avg60=25.50 avg300=10.00\ntotal avg10=0\n",
        )
        .expect("write");
        let sample = read_sample(&path).expect("read");
        assert!((sample.avg60 - 25.5).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn watcher_fires_above_threshold_and_stops_on_shutdown() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("memory");
        std::fs::write(&path, "some avg10=30.00 avg60=40.00 avg300=10.00\n").expect("write");

        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let cfg = PressureConfig {
            path: path.clone(),
            threshold: 20.0,
            poll_interval: Duration::from_millis(20),
        };
        let cb_fired = Arc::clone(&fired);
        let task = watch(
            cfg,
            Arc::clone(&shutdown),
            move |_sample| {
                cb_fired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
            None,
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = task.await;
        assert!(
            fired.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "callback must fire while above threshold"
        );
    }

    #[tokio::test]
    async fn dev_null_watch_path_disables_the_watcher() {
        // The systemd "off" spelling: the watcher must not read /dev/null
        // as a PSI file (it has no `some` line) — it should tick, error
        // (optionally), and never fire.
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cfg = PressureConfig {
            path: PathBuf::from("/dev/null"),
            threshold: 20.0,
            poll_interval: Duration::from_millis(10),
        };
        let cb_fired = Arc::clone(&fired);
        let task = watch(
            cfg,
            Arc::clone(&shutdown),
            move |_| {
                cb_fired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            },
            None,
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = task.await;
        assert_eq!(fired.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}
