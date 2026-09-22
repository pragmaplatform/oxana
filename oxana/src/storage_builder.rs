use deadpool_redis::Hook;
use std::time::Duration;

use crate::{OxanaError, Storage, storage_internal::StorageInternal, storage_keys::StorageKeys};

const DEFAULT_REDIS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);

#[must_use]
pub struct StorageBuilder {
    namespace: Option<String>,
    keys: Option<StorageKeys>,
    max_pool_size: Option<usize>,
    timeouts: Option<StorageBuilderTimeouts>,
    redis_response_timeout: Option<Duration>,
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
            keys: None,
            max_pool_size: None,
            timeouts: None,
            redis_response_timeout: None,
        }
    }

    /// Sets the namespace every Redis key is built under, in the default
    /// layout. See [`keys`](Self::keys) to change the layout itself.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Sets the Redis key layout, taking precedence over
    /// [`namespace`](Self::namespace). [`StorageKeys::new`] reproduces the
    /// default layout for a namespace, and its `with_*` methods override one
    /// key or prefix each.
    pub fn keys(mut self, keys: StorageKeys) -> Self {
        self.keys = Some(keys);
        self
    }

    fn storage_keys(&self) -> StorageKeys {
        self.keys
            .clone()
            .unwrap_or_else(|| StorageKeys::new(self.namespace.clone().unwrap_or_default()))
    }

    pub fn max_pool_size(mut self, max_pool_size: usize) -> Self {
        self.max_pool_size = Some(max_pool_size);
        self
    }

    pub fn timeouts(mut self, timeouts: StorageBuilderTimeouts) -> Self {
        self.timeouts = Some(timeouts);
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

        Ok(Storage::new(StorageInternal::new(
            pool,
            self.storage_keys(),
        )))
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

        Ok(Storage::new(StorageInternal::with_stats_pool(
            pool,
            stats_pool,
            self.storage_keys(),
        )))
    }

    fn build_redis_pool(
        url: impl Into<String>,
        max_pool_size: usize,
        timeouts: StorageBuilderTimeouts,
        redis_response_timeout: Duration,
    ) -> Result<deadpool_redis::Pool, OxanaError> {
        let mut cfg = deadpool_redis::Config::from_url(url);
        cfg.pool = Some(deadpool_redis::PoolConfig {
            max_size: max_pool_size,
            timeouts: timeouts.into(),
            queue_mode: Default::default(),
        });

        Ok(cfg
            .builder()?
            .post_create(Hook::sync_fn(move |conn, _| {
                // redis 1.0 introduced a default 500ms response_timeout on
                // MultiplexedConnection. This causes blocking Redis commands
                // (BLMOVE, BLPOP, etc.) to time out prematurely. Disable it
                // so that command-level timeouts govern blocking behavior.
                conn.set_response_timeout(redis_response_timeout);
                Ok(())
            }))
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
        let internal = StorageInternal::new(pool, self.storage_keys());
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
        let internal = StorageInternal::with_stats_pool(pool, stats_pool, self.storage_keys());
        Ok(Storage::new(internal))
    }
}

#[cfg(test)]
mod tests {
    use super::StorageBuilder;

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
