//! Redis lease tests (`redis` feature). Every test here is
//! `#[ignore]`-gated: it needs Docker for the testcontainer, so the
//! normal `cargo test` gate never requires it. Run explicitly with:
//!
//! ```text
//! cargo test --test redis_lease -- --ignored --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
use std::time::Duration;

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;
use worker_kit::{Lease, RedisLease};

async fn lease_at(container_port: u16, key: &str) -> RedisLease {
    let url = format!("redis://127.0.0.1:{container_port}");
    RedisLease::connect(&url, key)
        .await
        .expect("redis lease connects")
}

#[tokio::test]
#[ignore = "requires Docker (redis testcontainer)"]
async fn acquire_is_exclusive_and_renew_is_holder_scoped() {
    let container = Redis::default()
        .start()
        .await
        .expect("redis container starts");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("mapped port");
    let lease_a = lease_at(port, "worker-kit:test").await;
    let lease_b = lease_at(port, "worker-kit:test").await;

    // One winner.
    assert!(lease_a.acquire("holder-a", Duration::from_secs(5)).await);
    assert!(
        !lease_b.acquire("holder-b", Duration::from_secs(5)).await,
        "a held lease cannot be taken"
    );

    // Renewal is holder-scoped: the holder extends, the outsider cannot.
    assert!(lease_a.renew("holder-a", Duration::from_secs(5)).await);
    assert!(!lease_b.renew("holder-b", Duration::from_secs(5)).await);

    // After expiry (PX lapses) the outsider takes over.
    tokio::time::sleep(Duration::from_millis(5_200)).await;
    assert!(
        lease_b.acquire("holder-b", Duration::from_secs(5)).await,
        "an expired lease is acquirable"
    );
    assert!(!lease_a.renew("holder-a", Duration::from_secs(5)).await);
}

#[tokio::test]
#[ignore = "requires Docker (redis testcontainer)"]
async fn lease_survives_reconnects_via_connection_manager() {
    // The ConnectionManager re-establishes the connection on failure; a
    // dropped server must not wedge the lease into permanent false.
    let container = Redis::default()
        .start()
        .await
        .expect("redis container starts");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("mapped port");
    let lease = lease_at(port, "worker-kit:reconnect").await;

    assert!(lease.acquire("holder-a", Duration::from_secs(30)).await);
    // Repeated successful operations exercise the shared connection.
    for _ in 0..3 {
        assert!(lease.renew("holder-a", Duration::from_secs(30)).await);
    }
}
