/// The Redis keys a [`Storage`](crate::Storage) reads and writes.
///
/// [`StorageKeys::new`] builds the default layout for a namespace, which is
/// what [`StorageBuilder::namespace`](crate::StorageBuilder::namespace) uses.
/// Each `with_*` method overrides one key or prefix, for operators who scope
/// Redis ACLs or `SCAN`s to one structure, or who run several deployments on
/// one logical database. Pass the result to
/// [`StorageBuilder::keys`](crate::StorageBuilder::keys).
///
/// Prefixes are extended with `:<name>` by the storage: a queue list is
/// `<queue_prefix>:<queue>`, a processing list `<processing_prefix>:<process>`.
///
/// # Examples
///
/// ```rust
/// use oxana::{Storage, StorageKeys};
///
/// let keys = StorageKeys::new("app")
///     .with_queue_prefix("app:pending")
///     .with_throttler_prefix("app:throttle");
/// let storage = Storage::builder().keys(keys).build_from_env()?;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageKeys {
    /// Normalized namespace prefix applied to every Redis key
    /// (e.g. `oxana` or `oxana:<custom>`).
    pub(crate) namespace: String,
    /// Redis hash that stores serialized `JobEnvelope` values keyed by `JobId`.
    pub(crate) jobs: String,
    /// Redis list acting as the dead-letter queue containing JSON `JobEnvelope`s.
    pub(crate) dead: String,
    /// Redis sorted set (ZSET) of job IDs scheduled for future execution,
    /// scored by their target timestamp in microseconds.
    pub(crate) schedule: String,
    /// Redis sorted set (ZSET) of job IDs queued for retry,
    /// scored by the retry timestamp in microseconds.
    pub(crate) retry: String,
    /// Prefix for Redis list keys that hold enqueued job IDs
    /// (actual keys look like `{queue_prefix}:<queue>`).
    pub(crate) queue_prefix: String,
    /// Prefix for Redis list keys tracking jobs currently processed by a worker
    /// process (keys look like `{processing_queue_prefix}:<process_id>`).
    pub(crate) processing_queue_prefix: String,
    /// Redis sorted set (ZSET) of active process IDs scored by their last heartbeat.
    pub(crate) processes: String,
    /// Redis hash that stores serialized `Process` metadata keyed by process ID.
    pub(crate) processes_data: String,
    /// Redis hash that stores per-queue counters (processed, succeeded, panicked,
    /// failed) keyed as `<queue_full_key>:<metric>`.
    pub(crate) stats: String,
    /// Redis hash that stores serialized runtime queue configuration keyed by
    /// queue name.
    pub(crate) queue_configs: String,
    /// Prefix for Redis keys that store Sidekiq-style job execution metrics.
    pub(crate) metrics_prefix: String,
    /// Prefix for the sorted sets that hold throttle windows
    /// (keys look like `{throttler_prefix}:<queue>`).
    pub(crate) throttler_prefix: String,
}

/// The throttler prefix every release so far has used, outside the namespace.
const DEFAULT_THROTTLER_PREFIX: &str = "oxana:throttler";

impl StorageKeys {
    /// Builds the default layout for a namespace: every key is
    /// `<namespace>:<structure>`, except the throttle windows, which stay
    /// under `oxana:throttler` as they always have. An empty namespace falls
    /// back to `oxanus`.
    pub fn new(namespace: impl Into<String>) -> Self {
        let namespace = namespace.into();
        let namespace = if namespace.is_empty() {
            "oxanus".to_string()
        } else {
            namespace
        };

        Self {
            jobs: format!("{namespace}:jobs"),
            dead: format!("{namespace}:dead"),
            schedule: format!("{namespace}:schedule"),
            retry: format!("{namespace}:retry"),
            queue_prefix: format!("{namespace}:queue"),
            processing_queue_prefix: format!("{namespace}:processing"),
            processes: format!("{namespace}:processes"),
            processes_data: format!("{namespace}:processes_data"),
            stats: format!("{namespace}:stats"),
            queue_configs: format!("{namespace}:queue_configs"),
            metrics_prefix: format!("{namespace}:metrics"),
            throttler_prefix: DEFAULT_THROTTLER_PREFIX.to_string(),
            namespace,
        }
    }

    /// The namespace the layout was built from. It names the storage in logs
    /// and is what [`Storage::namespace`](crate::Storage::namespace) returns.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The hash of stored jobs, keyed by job ID.
    pub fn jobs(&self) -> &str {
        &self.jobs
    }

    /// The dead-letter list.
    pub fn dead(&self) -> &str {
        &self.dead
    }

    /// The sorted set of scheduled job IDs.
    pub fn schedule(&self) -> &str {
        &self.schedule
    }

    /// The sorted set of job IDs waiting to be retried.
    pub fn retry(&self) -> &str {
        &self.retry
    }

    /// The prefix of the pending lists, one per queue.
    pub fn queue_prefix(&self) -> &str {
        &self.queue_prefix
    }

    /// The prefix of the processing lists, one per process.
    pub fn processing_prefix(&self) -> &str {
        &self.processing_queue_prefix
    }

    /// The sorted set of live process IDs, scored by last heartbeat.
    pub fn processes(&self) -> &str {
        &self.processes
    }

    /// The hash of process records, keyed by process ID.
    pub fn processes_data(&self) -> &str {
        &self.processes_data
    }

    /// The hash of per-queue counters.
    pub fn stats(&self) -> &str {
        &self.stats
    }

    /// The hash of runtime queue configuration, keyed by queue name.
    pub fn queue_configs(&self) -> &str {
        &self.queue_configs
    }

    /// The prefix of the per-minute job metrics keys.
    pub fn metrics_prefix(&self) -> &str {
        &self.metrics_prefix
    }

    /// The prefix of the throttle windows, one per throttled queue.
    pub fn throttler_prefix(&self) -> &str {
        &self.throttler_prefix
    }

    /// Overrides the hash of stored jobs.
    #[must_use]
    pub fn with_jobs(mut self, key: impl Into<String>) -> Self {
        self.jobs = key.into();
        self
    }

    /// Overrides the dead-letter list.
    #[must_use]
    pub fn with_dead(mut self, key: impl Into<String>) -> Self {
        self.dead = key.into();
        self
    }

    /// Overrides the sorted set of scheduled job IDs.
    #[must_use]
    pub fn with_schedule(mut self, key: impl Into<String>) -> Self {
        self.schedule = key.into();
        self
    }

    /// Overrides the sorted set of job IDs waiting to be retried.
    #[must_use]
    pub fn with_retry(mut self, key: impl Into<String>) -> Self {
        self.retry = key.into();
        self
    }

    /// Overrides the prefix of the pending lists.
    #[must_use]
    pub fn with_queue_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.queue_prefix = prefix.into();
        self
    }

    /// Overrides the prefix of the processing lists.
    #[must_use]
    pub fn with_processing_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.processing_queue_prefix = prefix.into();
        self
    }

    /// Overrides the sorted set of live process IDs.
    #[must_use]
    pub fn with_processes(mut self, key: impl Into<String>) -> Self {
        self.processes = key.into();
        self
    }

    /// Overrides the hash of process records.
    #[must_use]
    pub fn with_processes_data(mut self, key: impl Into<String>) -> Self {
        self.processes_data = key.into();
        self
    }

    /// Overrides the hash of per-queue counters.
    #[must_use]
    pub fn with_stats(mut self, key: impl Into<String>) -> Self {
        self.stats = key.into();
        self
    }

    /// Overrides the hash of runtime queue configuration.
    #[must_use]
    pub fn with_queue_configs(mut self, key: impl Into<String>) -> Self {
        self.queue_configs = key.into();
        self
    }

    /// Overrides the prefix of the per-minute job metrics keys.
    #[must_use]
    pub fn with_metrics_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.metrics_prefix = prefix.into();
        self
    }

    /// Overrides the prefix of the throttle windows, for example to bring
    /// them under the namespace.
    #[must_use]
    pub fn with_throttler_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.throttler_prefix = prefix.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::StorageKeys;

    #[test]
    fn default_layout_is_unchanged() {
        let keys = StorageKeys::new("app");
        assert_eq!(keys.namespace(), "app");
        assert_eq!(keys.jobs(), "app:jobs");
        assert_eq!(keys.dead(), "app:dead");
        assert_eq!(keys.schedule(), "app:schedule");
        assert_eq!(keys.retry(), "app:retry");
        assert_eq!(keys.queue_prefix(), "app:queue");
        assert_eq!(keys.processing_prefix(), "app:processing");
        assert_eq!(keys.processes(), "app:processes");
        assert_eq!(keys.processes_data(), "app:processes_data");
        assert_eq!(keys.stats(), "app:stats");
        assert_eq!(keys.queue_configs(), "app:queue_configs");
        assert_eq!(keys.metrics_prefix(), "app:metrics");
        assert_eq!(keys.throttler_prefix(), "oxana:throttler");

        let keys = StorageKeys::new("");
        assert_eq!(keys.namespace(), "oxanus");
        assert_eq!(keys.jobs(), "oxanus:jobs");
        assert_eq!(keys.throttler_prefix(), "oxana:throttler");
    }

    #[test]
    fn overrides_change_one_field_each() {
        let keys = StorageKeys::new("app")
            .with_queue_prefix("app:pending")
            .with_processes_data("app:process-records")
            .with_metrics_prefix("app:metrics-v2")
            .with_throttler_prefix("app:throttle");
        assert_eq!(keys.queue_prefix(), "app:pending");
        assert_eq!(keys.processes_data(), "app:process-records");
        assert_eq!(keys.metrics_prefix(), "app:metrics-v2");
        assert_eq!(keys.throttler_prefix(), "app:throttle");
        assert_eq!(keys.jobs(), "app:jobs");
        assert_eq!(keys.processes(), "app:processes");
        assert_eq!(keys.namespace(), "app");
    }
}
