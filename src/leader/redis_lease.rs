//! Redis-backed lease: `SET NX PX` acquisition with a compare-and-
//! expire renewal, over a `ConnectionManager` (the connection survives
//! reconnects, which is exactly what a long-lived leader wants).

use std::time::Duration;

use redis::aio::ConnectionManager;

use super::Lease;

/// Lua renewal: extend the TTL only if we still hold the key. Atomic —
/// no `GET`/`PEXPIRE` race window where a lapsed lease gets extended by
/// the wrong holder.
const RENEW_LUA: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then \
return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";

/// A [`Lease`] on a Redis key: acquisition is `SET key holder NX PX ttl`
/// (single winner; losers get `nil`), renewal is the atomic
/// compare-and-expire script above.
///
/// Backend errors report `false` ("not leader now") rather than
/// erroring: losing leadership merely skips fires. Note that a Redis
/// failover can briefly admit two leaders (the old holder's key may
/// survive on a lagging replica) — jobs that must be strictly single-
/// instance should pair leadership with their own fencing.
pub struct RedisLease {
    conn: ConnectionManager,
    key: String,
}

impl RedisLease {
    /// Connect to `url` and lease `key`.
    ///
    /// # Errors
    /// Connection failure (bad URL, unreachable server).
    pub async fn connect(url: &str, key: impl Into<String>) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn,
            key: key.into(),
        })
    }

    /// `PX` argument: milliseconds as `i64`, saturating (`pexpire`
    /// wants `i64`; a `u128`-range TTL saturates rather than errors).
    fn ttl_ms(ttl: Duration) -> i64 {
        i64::try_from(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)).unwrap_or(i64::MAX)
    }
}

#[async_trait::async_trait]
impl Lease for RedisLease {
    async fn acquire(&self, holder: &str, ttl: Duration) -> bool {
        redis::cmd("SET")
            .arg(&self.key)
            .arg(holder)
            .arg("NX")
            .arg("PX")
            .arg(Self::ttl_ms(ttl))
            .query_async::<Option<String>>(&mut self.conn.clone())
            .await
            .is_ok_and(|reply| reply.is_some())
    }

    async fn renew(&self, holder: &str, ttl: Duration) -> bool {
        redis::Script::new(RENEW_LUA)
            .key(&self.key)
            .arg(holder)
            .arg(Self::ttl_ms(ttl))
            .invoke_async::<i64>(&mut self.conn.clone())
            .await
            .is_ok_and(|extended| extended == 1)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    /// `pexpire` takes `i64`; the conversion must saturate, not panic,
    /// for every duration a caller can construct.
    #[test]
    fn ttl_ms_saturates_and_is_precise_in_range() {
        assert_eq!(RedisLease::ttl_ms(Duration::from_millis(1_500)), 1_500);
        assert_eq!(
            RedisLease::ttl_ms(Duration::from_secs(u64::MAX / 1_000)),
            i64::MAX
        );
        assert_eq!(RedisLease::ttl_ms(Duration::MAX), i64::MAX);
    }

    #[test]
    fn renew_script_compares_then_extends() {
        // The script's contract: 1 iff the key still holds our value.
        assert!(RENEW_LUA.contains("get', KEYS[1]"));
        assert!(RENEW_LUA.contains("pexpire', KEYS[1], ARGV[2]"));
    }
}
