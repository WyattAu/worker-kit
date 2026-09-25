//! Leader election (feature `leader`): a minimal lease abstraction so
//! only one supervisor instance fires `leader: true` jobs.
//!
//! The contract is deliberately tiny — [`Lease`] with
//! [`acquire`](Lease::acquire)/[`renew`](Lease::renew) — so anything
//! from a single-node no-op ([`MemoryLease`]) to a Redis `SET NX PX`
//! ([`RedisLease`], `redis` feature) or a database row fits. Renewal is
//! checked before each fire; a lease that lapses simply makes the next
//! fire a skip until re-acquired, which is the safe failure direction
//! (a skipped fire, not two leaders firing).

use std::time::Duration;

/// A leadership lease.
///
/// All methods are infallible by contract: a backend error means "not
/// leader right now" (`false`), never a panic — losing leadership
/// pauses fires, which is always safe. Implementations should be cheap
/// to call before every fire.
#[async_trait::async_trait]
pub trait Lease: Send + Sync {
    /// Try to take the lease for `holder` for at least `ttl`. Returns
    /// `true` if this holder holds it now.
    async fn acquire(&self, holder: &str, ttl: Duration) -> bool;

    /// Extend the lease for `holder` (who must already hold it). Returns
    /// `true` if this holder still holds it.
    async fn renew(&self, holder: &str, ttl: Duration) -> bool;
}

/// A lease that always wins: single-process leader semantics (and the
/// default when `leader: true` jobs are registered without a lease).
#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryLease;

#[async_trait::async_trait]
impl Lease for MemoryLease {
    async fn acquire(&self, _holder: &str, _ttl: Duration) -> bool {
        true
    }

    async fn renew(&self, _holder: &str, _ttl: Duration) -> bool {
        true
    }
}

#[cfg(feature = "redis")]
mod redis_lease;

#[cfg(feature = "redis")]
pub use redis_lease::RedisLease;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[tokio::test]
    async fn memory_lease_always_wins() {
        let lease = MemoryLease;
        assert!(lease.acquire("holder-a", Duration::from_secs(1)).await);
        assert!(lease.renew("holder-a", Duration::from_secs(1)).await);
        assert!(lease.acquire("holder-b", Duration::from_secs(1)).await);
    }
}
