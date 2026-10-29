use std::time::Duration;

use crate::{OxanaError, PoolRetryConfig, Storage, storage_internal::StorageInternal};

const DEFAULT_REDIS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);

#[must_use]
pub struct StorageBuilder {
    namespace: Option<String>,
    max_pool_size: Option<usize>,
    timeouts: Option<StorageBuilderTimeouts>,
    claimed_connection_retries: Option<PoolRetryConfig>,
    redis_response_timeout: Option<Duration>,
    dead_job_limit: Option<std::num::NonZeroUsize>,
}

#[derive(Clone, Copy)]
pub struct StorageBuilderTimeouts {
    pub wait: Option<std::time::Duration>,
    pub create: Option<std::time::Duration>,
    pub recycle: Option<std::time::Duration>,
}

impl StorageBuilderTimeouts {
    pub fn new(timeout: std::time::Duration) -> Self {
        Self {
            wait: Some(timeout),
            create: Some(timeout),
            recycle: Some(timeout),
        }
    }
}

impl Default for StorageBuilderTimeouts {
    fn default() -> Self {
        Self {
            wait: Some(std::time::Duration::from_millis(300)),
            create: Some(std::time::Duration::from_millis(300)),
            recycle: Some(std::time::Duration::from_millis(300)),
        }
    }
}

impl From<StorageBuilderTimeouts> for deadpool_redis::Timeouts {
    fn from(value: StorageBuilderTimeouts) -> Self {
        deadpool_redis::Timeouts {
            wait: value.wait,
            create: value.create,
            recycle: value.recycle,
        }
    }
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBuilder {
    pub fn new() -> Self {
        Self {
            namespace: None,
            max_pool_size: None,
            timeouts: None,
            claimed_connection_retries: Some(PoolRetryConfig::default()),
            redis_response_timeout: None,
            dead_job_limit: std::num::NonZeroUsize::new(1000),
        }
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Maximum retained dead payloads, shared by every queue in this namespace.
    /// Defaults to 1,000. Use `None` to retain all dead jobs until explicitly
    /// revived or removed; monitor Redis memory when disabling the limit.
    /// Configure the same limit on every consumer of the namespace.
    pub fn dead_job_limit(mut self, limit: Option<std::num::NonZeroUsize>) -> Self {
        self.dead_job_limit = limit;
        self
    }

    pub fn max_pool_size(mut self, max_pool_size: usize) -> Self {
        self.max_pool_size = Some(max_pool_size);
        self
    }

    pub fn timeouts(mut self, timeouts: StorageBuilderTimeouts) -> Self {
        self.timeouts = Some(timeouts);
        self
    }

    /// Configures connection-acquisition retries before claimed-job start,
    /// acknowledgment, retry, and dead-letter transitions, including supplied pools.
    /// Defaults to `Some(PoolRetryConfig::default())`; `None` disables retries
    /// and uses the pool's own checkout deadlines.
    ///
    /// The retry window covers the first attempt and backoff. Heartbeats and
    /// other pool access retain their existing behavior. Commands already sent
    /// are never replayed. Invalid durations are rejected when building storage.
    pub fn claimed_connection_retries(mut self, config: Option<PoolRetryConfig>) -> Self {
        self.claimed_connection_retries = config;
        self
    }

    pub fn redis_response_timeout(mut self, timeout: Duration) -> Self {
        self.redis_response_timeout = Some(timeout);
        self
    }

    pub fn build_from_redis_url(self, url: impl Into<String>) -> Result<Storage, OxanaError> {
        let pool = Self::build_redis_pool(
            url,
            self.max_pool_size.unwrap_or(50),
            self.timeouts.unwrap_or_default(),
            self.redis_response_timeout
                .unwrap_or(DEFAULT_REDIS_RESPONSE_TIMEOUT),
        )?;

        self.build_from_pool(pool)
    }

    pub fn build_from_redis_urls(
        self,
        url: impl Into<String>,
        stats_url: impl Into<String>,
    ) -> Result<Storage, OxanaError> {
        let max_pool_size = self.max_pool_size.unwrap_or(50);
        let timeouts = self.timeouts.unwrap_or_default();
        let redis_response_timeout = self
            .redis_response_timeout
            .unwrap_or(DEFAULT_REDIS_RESPONSE_TIMEOUT);
        let pool = Self::build_redis_pool(url, max_pool_size, timeouts, redis_response_timeout)?;
        let stats_pool =
            Self::build_redis_pool(stats_url, max_pool_size, timeouts, redis_response_timeout)?;

        self.build_from_pools(pool, stats_pool)
    }

    fn build_redis_pool(
        url: impl Into<String>,
        max_pool_size: usize,
        timeouts: StorageBuilderTimeouts,
        redis_response_timeout: Duration,
    ) -> Result<deadpool_redis::Pool, OxanaError> {
        // Configure the connection before setup commands run; a post-create hook
        // is too late to override Redis's default 500ms response timeout.
        let manager = deadpool_redis::Manager::new_with_config(
            url.into(),
            deadpool_redis::redis::AsyncConnectionConfig::new()
                .set_connection_timeout(timeouts.create)
                .set_response_timeout(Some(redis_response_timeout)),
        )?;
        let config = deadpool_redis::PoolConfig {
            max_size: max_pool_size,
            timeouts: timeouts.into(),
            queue_mode: Default::default(),
        };

        Ok(deadpool_redis::Pool::builder(manager)
            .config(config)
            .runtime(deadpool_redis::Runtime::Tokio1)
            .build()?)
    }

    pub fn build_from_env(self) -> Result<Storage, OxanaError> {
        self.build_from_env_var("REDIS_URL")
    }

    pub fn build_from_env_var(self, var_name: &str) -> Result<Storage, OxanaError> {
        // Do not interpolate the VarError: its NotUnicode variant embeds the
        // variable's value, which may contain Redis credentials.
        let url = std::env::var(var_name).map_err(|err| {
            let reason = match err {
                std::env::VarError::NotPresent => "is not set",
                std::env::VarError::NotUnicode(_) => "is not valid unicode",
            };
            OxanaError::ConfigError(format!("{var_name} {reason}"))
        })?;
        match std::env::var("REDIS_STATS_URL") {
            Ok(stats_url) => self.build_from_redis_urls(url, stats_url),
            Err(_) => self.build_from_redis_url(url),
        }
    }

    /// Builds a storage from a pre-configured pool.
    ///
    /// Note that the builder's `max_pool_size`, `timeouts`, and
    /// `redis_response_timeout` settings do not apply here — the provided pool
    /// is used as-is.
    pub fn build_from_pool(self, pool: deadpool_redis::Pool) -> Result<Storage, OxanaError> {
        if let Some(config) = self.claimed_connection_retries {
            config.validate()?;
        }
        let internal = StorageInternal::new(pool, self.namespace)
            .with_claimed_connection_retries(self.claimed_connection_retries)
            .with_dead_job_limit(self.dead_job_limit.map(std::num::NonZeroUsize::get));
        Ok(Storage::new(internal))
    }

    /// Builds a storage from pre-configured pools.
    ///
    /// Note that the builder's `max_pool_size`, `timeouts`, and
    /// `redis_response_timeout` settings do not apply here — the provided pools
    /// are used as-is.
    pub fn build_from_pools(
        self,
        pool: deadpool_redis::Pool,
        stats_pool: deadpool_redis::Pool,
    ) -> Result<Storage, OxanaError> {
        if let Some(config) = self.claimed_connection_retries {
            config.validate()?;
        }
        let internal = StorageInternal::with_stats_pool(pool, stats_pool, self.namespace)
            .with_claimed_connection_retries(self.claimed_connection_retries)
            .with_dead_job_limit(self.dead_job_limit.map(std::num::NonZeroUsize::get));
        Ok(Storage::new(internal))
    }
}

#[cfg(test)]
mod tests {
    use super::{Duration, StorageBuilder, StorageBuilderTimeouts};

    #[tokio::test]
    async fn configured_response_timeout_applies_during_connection_setup() -> testresult::TestResult
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("redis://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = [0; 1024];
            assert!(stream.read(&mut request).await? > 0);
            tokio::time::sleep(Duration::from_secs(1)).await;
            stream.write_all(b"+OK\r\n+OK\r\n").await
        });
        let pool = StorageBuilder::build_redis_pool(
            url,
            1,
            StorageBuilderTimeouts::new(Duration::from_secs(2)),
            Duration::from_secs(2),
        )?;
        let connection = pool.get().await;
        let response = server.await?;
        connection?;
        response?;
        Ok(())
    }

    #[test]
    fn build_from_redis_url_uses_primary_pool_for_stats() {
        let storage = StorageBuilder::new()
            .build_from_redis_url("redis://127.0.0.1/0")
            .expect("storage should build");

        assert!(!storage.internal.has_dedicated_stats_pool());
    }

    #[test]
    fn build_from_redis_urls_uses_dedicated_stats_pool() {
        let storage = StorageBuilder::new()
            .build_from_redis_urls("redis://127.0.0.1/0", "redis://127.0.0.1/1")
            .expect("storage should build");

        assert!(storage.internal.has_dedicated_stats_pool());
    }

    #[test]
    fn build_from_env_var_missing_returns_config_error() {
        let err = StorageBuilder::new()
            .build_from_env_var("OXANA_TEST_DEFINITELY_UNSET_REDIS_URL")
            .err()
            .expect("missing env var should be an error, not a panic");

        assert!(matches!(err, crate::OxanaError::ConfigError(_)));
    }
}
