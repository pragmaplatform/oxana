use crate::OxanaError;
use deadpool_redis::redis;
use uuid::Uuid;

pub struct Throttler {
    redis_pool: deadpool_redis::Pool,
    key: String,
    limit: u64,
    window_ms: i64,
}

#[derive(Debug)]
pub struct ThrottlerState {
    pub requests: u64,
    pub is_allowed: bool,
    pub throttled_for: Option<i64>,
}

impl Throttler {
    /// A throttler over the sorted set at `key`, which the storage builds from
    /// its throttler prefix and the queue name.
    pub fn new(redis_pool: deadpool_redis::Pool, key: String, limit: u64, window_ms: i64) -> Self {
        Throttler {
            redis_pool,
            key,
            limit,
            window_ms,
        }
    }

    pub async fn consume(&self, cost: Option<u64>) -> Result<ThrottlerState, OxanaError> {
        let mut redis = self.redis_pool.get().await?;
        let current_time = u64::try_from(chrono::Utc::now().timestamp_micros())?;
        let state = self.state_w_conn(&mut redis).await?;

        if state.is_allowed {
            let effective_cost = cost.unwrap_or(1);

            if effective_cost == 0 {
                return Ok(state);
            }

            let members: Vec<(u64, String)> = (0..effective_cost)
                .map(|_| (current_time, Uuid::new_v4().to_string()))
                .collect();

            // The window is expressed in milliseconds; EXPIRE would round a
            // sub-second window down to zero and a 1500ms window to a second.
            let mut pipe = redis::pipe();
            pipe.zadd_multiple(&self.key, &members)
                .pexpire(&self.key, self.window_ms);
            let (updated, _): (u64, ()) = pipe.query_async(&mut redis).await?;

            Ok(ThrottlerState {
                requests: state.requests + updated,
                ..state
            })
        } else {
            Ok(state)
        }
    }

    pub async fn state(&self) -> Result<ThrottlerState, OxanaError> {
        let mut redis = self.redis_pool.get().await?;
        self.state_w_conn(&mut redis).await
    }

    async fn state_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
    ) -> Result<ThrottlerState, OxanaError> {
        let now = chrono::Utc::now().timestamp_micros();
        let window_start = now - self.window_micros();

        let (_, first, request_count): ((), Vec<(String, f64)>, u64) = redis::pipe()
            .zrembyscore(&self.key, 0, window_start)
            .zrange_withscores(&self.key, 0, 0)
            .zcard(&self.key)
            .query_async(&mut *redis)
            .await?;

        let accurate_window_start = if let Some((_, score)) = first.first() {
            Some(*score as i64)
        } else {
            None
        };

        let is_allowed = request_count < self.limit;

        let throttled_for_micros = if is_allowed {
            None
        } else {
            accurate_window_start.map(|start| now - start + self.window_micros())
        };

        let throttled_for = throttled_for_micros.map(|micros| micros / 1000 + 1);

        Ok(ThrottlerState {
            requests: request_count,
            is_allowed,
            throttled_for,
        })
    }

    fn window_micros(&self) -> i64 {
        self.window_ms * 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helper::*;
    use deadpool_redis::redis::AsyncCommands;
    use std::time::Duration;
    use testresult::TestResult;

    #[tokio::test]
    async fn test_consume() -> TestResult {
        let pool = redis_pool().await?;
        let key = random_string();
        let rate_limiter = Throttler::new(pool, key, 2, 60000);
        assert!(rate_limiter.consume(None).await?.is_allowed);
        assert!(rate_limiter.consume(None).await?.is_allowed);
        let state = rate_limiter.consume(None).await?;
        assert!(!state.is_allowed);
        assert!(state.throttled_for.is_some());
        assert!(state.throttled_for.unwrap_or(0) >= 60000);
        let state = rate_limiter.consume(None).await?;
        assert!(!state.is_allowed);
        assert!(state.throttled_for.is_some());
        assert!(state.throttled_for.unwrap_or(0) >= 60000);

        Ok(())
    }

    #[tokio::test]
    async fn sub_second_window_still_limits() -> TestResult {
        let pool = redis_pool().await?;
        let key = random_string();
        let rate_limiter = Throttler::new(pool.clone(), key.clone(), 1, 500);
        assert!(rate_limiter.consume(None).await?.is_allowed);

        // The window is stored with millisecond precision: a 500ms window
        // must neither expire at once nor be rounded down to zero.
        let ttl_ms: i64 = pool.get().await?.pttl(&key).await?;
        assert!((1..=500).contains(&ttl_ms), "ttl was {ttl_ms}ms");

        let state = rate_limiter.consume(None).await?;
        assert!(!state.is_allowed);
        assert!(
            state
                .throttled_for
                .is_some_and(|ms| (1..1000).contains(&ms))
        );

        tokio::time::sleep(Duration::from_millis(550)).await;
        assert!(rate_limiter.consume(None).await?.is_allowed);

        Ok(())
    }

    #[tokio::test]
    async fn window_keeps_its_fractional_second() -> TestResult {
        let pool = redis_pool().await?;
        let key = random_string();
        let rate_limiter = Throttler::new(pool.clone(), key.clone(), 1, 1500);
        assert!(rate_limiter.consume(None).await?.is_allowed);

        let ttl_ms: i64 = pool.get().await?.pttl(&key).await?;
        assert!((1001..=1500).contains(&ttl_ms), "ttl was {ttl_ms}ms");

        Ok(())
    }

    #[tokio::test]
    async fn test_consume_with_cost() -> TestResult {
        let pool = redis_pool().await?;
        let key = random_string();
        let rate_limiter = Throttler::new(pool, key, 4, 60000);
        assert!(rate_limiter.consume(Some(2)).await?.is_allowed);
        assert!(rate_limiter.consume(Some(2)).await?.is_allowed);
        let state = rate_limiter.consume(Some(1)).await?;
        assert!(!state.is_allowed);

        Ok(())
    }
}
