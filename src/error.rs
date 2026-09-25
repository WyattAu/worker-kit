//! Errors: the job-closure error type and the typed registration errors.

use std::error::Error;
use std::fmt;

/// Maximum length of a job name. Names matching `[a-z0-9_.-]{1,64}` are
/// accepted by [`WorkerSupervisor::register`](crate::WorkerSupervisor::register)
/// and are safe in metrics labels, log keys, and Redis keys.
pub const NAME_MAX_LEN: usize = 64;

/// Whether `name` is a valid job name: 1–64 chars of `[a-z0-9_.-]`.
///
/// Exposed for validators and dashboards; [`register`](crate::WorkerSupervisor::register)
/// enforces the same rule and returns
/// [`RegisterError::InvalidName`](crate::RegisterError::InvalidName) on
/// violation.
#[must_use]
pub fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= NAME_MAX_LEN
        && name.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.' || c == '-'
        })
}

/// The error type job closures return. A thin, `Display`-able wrapper
/// around any `std::error::Error + Send + Sync` value (the `anyhow`
/// shape, without the backtrace machinery): `?` on any concrete error
/// converts into it.
///
/// Not part of the failure budget by itself — the runner counts every
/// `Err` the same way, whether it came from `?`, `JobError::msg`, or a
/// panic (which becomes a fixed static message, since panic payloads
/// carry no `Display`).
#[derive(Debug)]
pub struct JobError {
    inner: Box<dyn Error + Send + Sync>,
}

impl JobError {
    /// Wrap an error value.
    pub fn new(err: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self { inner: err.into() }
    }

    /// Build from a message.
    pub fn msg(msg: impl fmt::Display + Send + Sync + 'static) -> Self {
        Self {
            inner: msg.to_string().into(),
        }
    }

    /// The wrapped error value, for callers that want the concrete type
    /// back.
    #[must_use]
    pub fn into_inner(self) -> Box<dyn Error + Send + Sync> {
        self.inner
    }
}

impl fmt::Display for JobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

// Any concrete `Error + Send + Sync + 'static` converts with `?`. (This is
// the `anyhow` blanket: it stays coherent because `JobError` itself does
// not implement `std::error::Error`.)
impl<E: Error + Send + Sync + 'static> From<E> for JobError {
    fn from(err: E) -> Self {
        Self {
            inner: Box::new(err),
        }
    }
}

/// Why [`WorkerSupervisor::register`](crate::WorkerSupervisor::register)
/// refused a job spec.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// The job name violated `[a-z0-9_.-]{1,64}` — see
    /// [`is_valid_name`].
    #[error("invalid job name {name:?}: must be 1..={NAME_MAX_LEN} chars of [a-z0-9_.-]")]
    InvalidName {
        /// The rejected name, verbatim.
        name: String,
    },
    /// The cron expression did not parse (`cron` feature). Registration
    /// parses eagerly so a bad schedule fails at startup, not at the
    /// first tick.
    #[cfg(feature = "cron")]
    #[error("invalid cron expression {expr:?}")]
    InvalidCron {
        /// The rejected expression, verbatim.
        expr: String,
        /// The parse error from the `cron` crate.
        #[source]
        source: cron::error::Error,
    },
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn valid_names_pass() {
        for name in ["a", "metrics-rollup", "nightly_reindex.v2", "x-9_z.w"] {
            assert!(is_valid_name(name), "{name:?} must be valid");
        }
    }

    #[test]
    fn invalid_names_fail() {
        for name in [
            "",
            "UPPER",
            "has space",
            "slash/ed",
            "bang!",
            "unicode-é",
            "tab\t",
        ] {
            assert!(!is_valid_name(name), "{name:?} must be invalid");
        }
    }

    #[test]
    fn name_length_bounds() {
        assert!(is_valid_name(&"a".repeat(NAME_MAX_LEN)));
        assert!(!is_valid_name(&"a".repeat(NAME_MAX_LEN + 1)));
    }

    #[test]
    fn invalid_name_error_display() {
        let err = RegisterError::InvalidName {
            name: "BAD!".to_owned(),
        };
        assert!(err.to_string().contains("BAD!"));
        assert!(err.to_string().contains("invalid job name"));
    }

    #[test]
    fn job_error_display_and_conversions() {
        let err = JobError::msg("boom 1");
        assert_eq!(err.to_string(), "boom 1");

        let from_io: JobError = std::io::Error::other("io down").into();
        assert_eq!(from_io.to_string(), "io down");

        let msg = JobError::msg("keep me");
        assert_eq!(msg.into_inner().to_string(), "keep me");
    }
}
