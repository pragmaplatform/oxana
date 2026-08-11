use chrono::{DateTime, Utc};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::{
    config::DEFAULT_DEAD_PROCESS_THRESHOLD,
    error::OxanaError,
    job_envelope::{JobEnvelope, JobId, UniqueJobId},
    metrics::{
        JobMetricsDetail, JobMetricsQuery, JobMetricsSnapshot, MetricIdentity,
        QueueLengthMetricsSnapshot,
    },
    queue::{Queue, QueueConcurrency, QueueKind, QueueRuntimeConfig, QueueState},
    runtime::RuntimeBuilder,
    stats::{Process, QueueStats, Stats},
    storage_builder::StorageBuilder,
    storage_internal::StorageInternal,
    storage_types::QueueListOpts,
    worker::Job,
};

#[cfg(feature = "prometheus")]
use crate::prometheus::PrometheusMetrics;

/// Storage provides the main interface for job management in Oxana.
///
/// It handles all job operations including enqueueing, scheduling, and monitoring.
/// Storage instances are created using the [`Storage::builder()`] method.
///
/// # Examples
///
/// ```rust
/// use oxana::{Storage, Queue, Job};
///
/// async fn example() -> Result<(), oxana::OxanaError> {
///     let storage = Storage::builder().build_from_env()?;
///
///     // Enqueue a job
///     storage.enqueue(MyQueue, MyJob { data: "hello" }).await?;
///
///     // Schedule a job for later
///     storage.enqueue_in(MyQueue, MyJob { data: "delayed" }, 300).await?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Storage {
    pub(crate) internal: StorageInternal,
    monitoring_settings: Arc<StorageMonitoringSettings>,
}

impl Storage {
    pub(crate) fn new(internal: StorageInternal) -> Self {
        Self {
            internal,
            monitoring_settings: Arc::new(StorageMonitoringSettings::default()),
        }
    }

    pub(crate) fn set_dead_process_threshold(&self, threshold: Duration) {
        *self
            .monitoring_settings
            .dead_process_threshold
            .write()
            .expect("monitoring settings lock poisoned") = threshold;
    }

    pub(crate) fn dead_process_threshold(&self) -> Duration {
        *self
            .monitoring_settings
            .dead_process_threshold
            .read()
            .expect("monitoring settings lock poisoned")
    }

    /// Creates a new [`StorageBuilder`] for configuring and building a Storage instance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use oxana::Storage;
    ///
    /// let builder = Storage::builder();
    /// let storage = builder.build_from_env()?;
    /// ```
    pub fn builder() -> StorageBuilder {
        StorageBuilder::new()
    }

    /// Builds a storage handle from the `REDIS_URL` environment variable.
    pub fn from_env() -> Result<Self, OxanaError> {
        Self::builder().build_from_env()
    }

    /// Builds a storage handle from a Redis URL.
    pub fn from_url(url: impl Into<String>) -> Result<Self, OxanaError> {
        Self::builder().build_from_redis_url(url)
    }

    /// Starts configuring a typed worker runtime for this storage.
    pub fn runtime<DT>(&self, ctx: DT) -> RuntimeBuilder<DT>
    where
        DT: Clone + Send + Sync + 'static,
    {
        RuntimeBuilder::new(self.clone(), ctx)
    }

    /// Enqueues a job to be processed immediately.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to enqueue the job to
    /// * `job` - The job to enqueue (must implement [`Job`])
    ///
    /// # Returns
    ///
    /// A [`JobId`] that can be used to track the job, or an [`OxanaError`] if the operation fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use oxana::{Storage, Queue, Job};
    ///
    /// async fn example(storage: &Storage) -> Result<(), oxana::OxanaError> {
    ///     let job_id = storage.enqueue(MyQueue, MyJob { data: "hello" }).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn enqueue<T: Job + 'static>(
        &self,
        queue: impl Queue,
        job: T,
    ) -> Result<JobId, OxanaError> {
        self.enqueue_in(queue, job, 0).await
    }

    /// Enqueues multiple jobs to the same queue for immediate processing.
    ///
    /// The returned job IDs are in the same order as the input jobs. Unique-job
    /// conflict handling is the same as [`enqueue`](Self::enqueue): skipped jobs
    /// return their existing deterministic job ID, and replace conflicts update
    /// the stored job before enqueueing it again.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to enqueue the jobs to
    /// * `jobs` - The jobs to enqueue (each must implement [`Job`])
    ///
    /// # Returns
    ///
    /// A vector of [`JobId`]s that can be used to track the jobs, or an
    /// [`OxanaError`] if the operation fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use oxana::{Storage, Queue, Job};
    ///
    /// async fn example(storage: &Storage) -> Result<(), oxana::OxanaError> {
    ///     let job_ids = storage
    ///         .enqueue_list(
    ///             MyQueue,
    ///             vec![
    ///                 MyJob { data: "first" },
    ///                 MyJob { data: "second" },
    ///             ],
    ///         )
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn enqueue_list<T, Jobs>(
        &self,
        queue: impl Queue,
        jobs: Jobs,
    ) -> Result<Vec<JobId>, OxanaError>
    where
        T: Job + 'static,
        Jobs: IntoIterator<Item = T>,
    {
        let queue_key = queue.key();
        let envelopes = jobs
            .into_iter()
            .map(|job| JobEnvelope::new(queue_key.clone(), job))
            .collect::<Result<Vec<_>, _>>()?;

        tracing::trace!(
            queue = queue_key,
            count = envelopes.len(),
            "Enqueuing job list"
        );

        self.internal.enqueue_list(envelopes).await
    }

    /// Enqueues a job to be processed after a specified delay.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to enqueue the job to
    /// * `job` - The job to enqueue (must implement [`Job`])
    /// * `delay` - The delay in seconds before the job should be processed
    ///
    /// # Returns
    ///
    /// A [`JobId`] that can be used to track the job, or an [`OxanaError`] if the operation fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use oxana::{Storage, Queue, Job};
    ///
    /// async fn example(storage: &Storage) -> Result<(), oxana::OxanaError> {
    ///     // Schedule a job to run in 5 minutes
    ///     let job_id = storage.enqueue_in(MyQueue, MyJob { data: "delayed" }, 300).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn enqueue_in<T: Job + 'static>(
        &self,
        queue: impl Queue,
        job: T,
        delay: u64,
    ) -> Result<JobId, OxanaError> {
        let envelope = JobEnvelope::new(queue.key().clone(), job)?;

        tracing::trace!("Enqueuing job: {:?}", envelope);

        if delay > 0 {
            self.internal.enqueue_in(envelope, delay).await
        } else {
            self.internal.enqueue(envelope).await
        }
    }

    /// Schedules a job to run at a specific time.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to enqueue the job to
    /// * `job` - The job to enqueue (must implement [`Job`])
    /// * `time` - The UTC timestamp when the job should become available
    ///
    /// # Returns
    ///
    /// A [`JobId`] that can be used to track the job, or an [`OxanaError`] if the operation fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use chrono::{Duration, Utc};
    /// use oxana::{Storage, Queue, Job};
    ///
    /// async fn example(storage: &Storage) -> Result<(), oxana::OxanaError> {
    ///     let time = Utc::now() + Duration::minutes(5);
    ///     let job_id = storage.enqueue_at(MyQueue, MyJob { data: "scheduled" }, time).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn enqueue_at<T: Job + 'static>(
        &self,
        queue: impl Queue,
        job: T,
        time: DateTime<Utc>,
    ) -> Result<JobId, OxanaError> {
        let envelope = JobEnvelope::new_scheduled(queue.key().clone(), job, time)?;

        tracing::trace!("Scheduling job {:?} at {}", envelope, time);

        self.internal.enqueue_at(envelope).await
    }

    /// Returns the number of jobs currently enqueued in the specified queue.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to count jobs for
    ///
    /// # Returns
    ///
    /// The number of enqueued jobs, or an [`OxanaError`] if the operation fails.
    pub async fn enqueued_count(&self, queue: impl Queue) -> Result<usize, OxanaError> {
        self.internal.enqueued_count(&queue.key()).await
    }

    /// Returns the latency of the queue (The age of the oldest job in the queue).
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to get the latency for
    ///
    /// # Returns
    ///
    /// The latency of the queue in milliseconds, or an [`OxanaError`] if the operation fails.
    pub async fn latency_ms(&self, queue: impl Queue) -> Result<f64, OxanaError> {
        self.internal.latency_ms(&queue.key()).await
    }

    /// Returns the number of jobs that have failed and moved to the dead queue.
    ///
    /// # Returns
    ///
    /// The number of dead jobs, or an [`OxanaError`] if the operation fails.
    pub async fn dead_count(&self) -> Result<usize, OxanaError> {
        self.internal.dead_count().await
    }

    /// Returns the number of jobs that are currently being retried.
    ///
    /// # Returns
    ///
    /// The number of retrying jobs, or an [`OxanaError`] if the operation fails.
    pub async fn retries_count(&self) -> Result<usize, OxanaError> {
        self.internal.retries_count().await
    }

    /// Returns the number of jobs that are scheduled for future execution.
    ///
    /// # Returns
    ///
    /// The number of scheduled jobs, or an [`OxanaError`] if the operation fails.
    pub async fn scheduled_count(&self) -> Result<usize, OxanaError> {
        self.internal.scheduled_count().await
    }

    /// Returns the number of jobs that are currently enqueued or scheduled for future execution.
    ///
    /// # Returns
    ///
    /// The number of jobs, or an [`OxanaError`] if the operation fails.
    pub async fn jobs_count(&self) -> Result<usize, OxanaError> {
        self.internal.jobs_count().await
    }

    /// Deletes a job by its ID.
    ///
    /// Removes the job from both the job store and the processing queue.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the job to delete
    ///
    /// # Returns
    ///
    /// An [`OxanaError`] if the operation fails.
    pub async fn delete_job(&self, id: &JobId) -> Result<(), OxanaError> {
        self.internal.delete_job(id).await
    }

    /// Deletes a unique job identified by either its [`JobId`] or its [`Job`] value.
    ///
    /// # Errors
    ///
    /// Returns [`OxanaError::ConfigError`] when the provided job does not define
    /// a unique ID.
    pub async fn delete_unique_job<T: UniqueJobId>(&self, job: &T) -> Result<(), OxanaError> {
        let id = job.unique_job_id().ok_or_else(|| {
            OxanaError::ConfigError("delete_unique_job requires a job with a unique ID".to_string())
        })?;
        self.delete_job(&id).await
    }

    /// Returns a job by its ID.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the job to return
    ///
    /// # Returns
    ///
    /// The job envelope when present, or [`None`] if the job no longer exists.
    pub async fn get_job(&self, id: &JobId) -> Result<Option<JobEnvelope>, OxanaError> {
        self.internal.get_job(id).await
    }

    /// Returns per-queue statistics for queues matching the given patterns.
    ///
    /// Each pattern is matched against queue names using the same glob syntax
    /// as Redis SCAN (e.g. `"email*"`, `"*"`).
    ///
    /// # Arguments
    ///
    /// * `patterns` - Glob patterns to match queue names against
    ///
    /// # Returns
    ///
    /// Statistics for matching queues, or an [`OxanaError`] if the operation fails.
    pub async fn stats_queues_for(&self, patterns: &[&str]) -> Result<Vec<QueueStats>, OxanaError> {
        self.internal.stats_queues_for(patterns).await
    }

    /// Returns per-queue statistics for all queues.
    ///
    /// # Returns
    ///
    /// Statistics for all queues, or an [`OxanaError`] if the operation fails.
    pub async fn stats_queues(&self) -> Result<Vec<QueueStats>, OxanaError> {
        self.internal.stats_queues().await
    }

    /// Returns the full stats including global aggregates, processes, and per-queue stats.
    ///
    /// # Returns
    ///
    /// The full stats, or an [`OxanaError`] if the operation fails.
    pub async fn stats(&self) -> Result<Stats, OxanaError> {
        self.internal.stats(self.dead_process_threshold()).await
    }

    /// Returns Sidekiq-style job execution metrics for all workers.
    ///
    /// Metrics are retained for up to 24 hours. The query defaults to 60 minutes
    /// and is clamped to 1440 minutes.
    pub async fn job_metrics(
        &self,
        query: JobMetricsQuery,
    ) -> Result<JobMetricsSnapshot, OxanaError> {
        self.internal.job_metrics(query).await
    }

    /// Returns Sidekiq-style job execution metrics for a single worker.
    ///
    /// Job counters count every job. Execution counters, execution time, and
    /// histogram data count each worker execution once, so a batch worker
    /// contributes one execution sample for the whole batch.
    pub async fn job_metrics_for(
        &self,
        identity: &MetricIdentity,
        query: JobMetricsQuery,
    ) -> Result<JobMetricsDetail, OxanaError> {
        self.internal.job_metrics_for(identity, query).await
    }

    /// Returns per-minute queue length samples for active queues.
    ///
    /// Samples are recorded by workers during their periodic queue length refresh
    /// and retained for the same window as job execution metrics.
    pub async fn queue_length_metrics(
        &self,
        query: JobMetricsQuery,
    ) -> Result<QueueLengthMetricsSnapshot, OxanaError> {
        self.internal.queue_length_metrics(query).await
    }

    /// Returns the effective persisted runtime config for a queue, if one exists.
    pub async fn queue_config(
        &self,
        queue: impl Queue,
    ) -> Result<Option<QueueRuntimeConfig>, OxanaError> {
        let queue_key = queue.key();
        let concurrency = queue.config().concurrency;
        Ok(self
            .internal
            .queue_config(&queue_key)
            .await?
            .map(|config| concurrency.effective_runtime_config(config)))
    }

    /// Returns persisted runtime configs for the provided queue keys.
    pub async fn queue_configs(
        &self,
        queues: &[String],
    ) -> Result<std::collections::HashMap<String, QueueRuntimeConfig>, OxanaError> {
        self.internal.queue_configs(queues).await
    }

    /// Stores the full runtime config for a queue.
    pub async fn set_queue_config(
        &self,
        queue: impl Queue,
        config: &QueueRuntimeConfig,
    ) -> Result<(), OxanaError> {
        let queue_key = queue.key();
        validate_runtime_concurrency(&queue_key, queue.config().concurrency, config)?;
        self.internal.set_queue_config(&queue_key, config).await
    }

    /// Removes any persisted runtime config for a queue.
    pub async fn reset_queue_config(&self, queue: impl Queue) -> Result<(), OxanaError> {
        self.internal.reset_queue_config(&queue.key()).await
    }

    /// Updates the runtime concurrency for a queue without restarting workers.
    pub async fn set_queue_concurrency(
        &self,
        queue: impl Queue,
        concurrency: usize,
    ) -> Result<(), OxanaError> {
        let queue_key = queue.key();
        let queue_config = queue.config();
        let queue_concurrency = queue_config.concurrency;
        validate_dynamic_concurrency(&queue_key, queue_concurrency)?;

        let default_concurrency = self
            .runtime_concurrency_reset_default(&queue_key, &queue_config.kind, queue_concurrency)
            .await?;
        let existing_config = self.internal.queue_config(&queue_key).await?;

        if concurrency == default_concurrency && existing_config.is_none() {
            return Ok(());
        }

        let mut config = existing_config.unwrap_or_default();
        config.concurrency = (concurrency != default_concurrency).then_some(concurrency);

        self.internal.set_queue_config(&queue_key, &config).await
    }

    async fn runtime_concurrency_reset_default(
        &self,
        queue_key: &str,
        queue_kind: &QueueKind,
        queue_concurrency: QueueConcurrency,
    ) -> Result<usize, OxanaError> {
        let configured_default = queue_concurrency.default_concurrency();
        let QueueKind::Dynamic { prefix, .. } = queue_kind else {
            return Ok(configured_default);
        };

        if queue_key == prefix {
            return Ok(configured_default);
        }

        let base_config = self
            .internal
            .queue_config_or_default(prefix, queue_concurrency.stored_runtime_default())
            .await?;

        Ok(base_config.concurrency.unwrap_or(configured_default))
    }

    /// Updates the runtime processing state for a queue.
    pub async fn set_queue_state(
        &self,
        queue: impl Queue,
        state: QueueState,
    ) -> Result<(), OxanaError> {
        let queue_key = queue.key();
        let concurrency = queue.config().concurrency;
        if concurrency.is_dynamic() {
            return self.internal.set_queue_state(&queue_key, state).await;
        }

        let mut config = self
            .internal
            .queue_config(&queue_key)
            .await?
            .unwrap_or_default();
        config.concurrency = None;
        config.state = state;

        self.internal.set_queue_config(&queue_key, &config).await
    }

    /// Pauses job processing for a queue. Enqueueing is not affected.
    pub async fn pause_queue(&self, queue: impl Queue) -> Result<(), OxanaError> {
        self.set_queue_state(queue, QueueState::Paused).await
    }

    /// Resumes job processing for a paused queue.
    pub async fn unpause_queue(&self, queue: impl Queue) -> Result<(), OxanaError> {
        self.set_queue_state(queue, QueueState::Active).await
    }

    /// Returns the list of processes that are currently running.
    ///
    /// # Returns
    ///
    /// The list of processes, or an [`OxanaError`] if the operation fails.
    pub async fn processes(&self) -> Result<Vec<Process>, OxanaError> {
        self.internal.processes(self.dead_process_threshold()).await
    }

    /// Returns the namespace this storage instance is using.
    ///
    /// # Returns
    ///
    /// The namespace string.
    pub fn namespace(&self) -> &str {
        self.internal.namespace()
    }

    /// Returns a list of jobs currently enqueued in the specified queue.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to list jobs from
    /// * `opts` - Pagination options controlling count and offset
    ///
    /// # Returns
    ///
    /// A vector of [`JobEnvelope`]s, or an [`OxanaError`] if the operation fails.
    pub async fn list_queue_jobs(
        &self,
        queue: impl Queue,
        opts: &QueueListOpts,
    ) -> Result<Vec<JobEnvelope>, OxanaError> {
        self.internal.list_queue_jobs(&queue.key(), opts).await
    }

    /// Returns a list of dead jobs.
    ///
    /// # Arguments
    ///
    /// * `opts` - Pagination options controlling count and offset
    ///
    /// # Returns
    ///
    /// A vector of [`JobEnvelope`]s, or an [`OxanaError`] if the operation fails.
    pub async fn list_dead(&self, opts: &QueueListOpts) -> Result<Vec<JobEnvelope>, OxanaError> {
        self.internal.list_dead(opts).await
    }

    /// Returns a list of jobs pending retry.
    ///
    /// # Arguments
    ///
    /// * `opts` - Pagination options controlling count and offset
    ///
    /// # Returns
    ///
    /// A vector of [`JobEnvelope`]s, or an [`OxanaError`] if the operation fails.
    pub async fn list_retries(&self, opts: &QueueListOpts) -> Result<Vec<JobEnvelope>, OxanaError> {
        self.internal.list_retries(opts).await
    }

    /// Moves all pending retry jobs back to their queues for immediate processing.
    ///
    /// # Returns
    ///
    /// The number of retry jobs enqueued immediately, or an [`OxanaError`] if the operation fails.
    pub async fn retry_all_now(&self) -> Result<usize, OxanaError> {
        self.internal.retry_all_now().await
    }

    /// Returns a list of jobs scheduled for future execution.
    ///
    /// # Arguments
    ///
    /// * `opts` - Pagination options controlling count and offset
    ///
    /// # Returns
    ///
    /// A vector of [`JobEnvelope`]s, or an [`OxanaError`] if the operation fails.
    pub async fn list_scheduled(
        &self,
        opts: &QueueListOpts,
    ) -> Result<Vec<JobEnvelope>, OxanaError> {
        self.internal.list_scheduled(opts).await
    }

    /// Enqueues a pre-built job envelope for immediate processing.
    ///
    /// Unlike [`enqueue`](Self::enqueue), this accepts a raw [`JobEnvelope`] directly,
    /// which is useful for re-enqueueing jobs from the dead queue or other sources
    /// where the original worker type is not available.
    ///
    /// # Arguments
    ///
    /// * `envelope` - The job envelope to enqueue
    ///
    /// # Returns
    ///
    /// The [`JobId`] of the enqueued job, or an [`OxanaError`] if the operation fails.
    pub async fn enqueue_envelope(&self, envelope: JobEnvelope) -> Result<JobId, OxanaError> {
        self.internal.enqueue(envelope).await
    }

    /// Removes all jobs from the specified queue.
    ///
    /// # Arguments
    ///
    /// * `queue` - The queue to wipe
    ///
    /// # Returns
    ///
    /// An [`OxanaError`] if the operation fails.
    pub async fn wipe_queue(&self, queue: impl Queue) -> Result<(), OxanaError> {
        self.internal.wipe_queue(&queue.key()).await
    }

    /// Removes all jobs from the dead queue.
    ///
    /// # Returns
    ///
    /// An [`OxanaError`] if the operation fails.
    pub async fn wipe_dead(&self) -> Result<(), OxanaError> {
        self.internal.wipe_dead().await
    }

    /// Moves all dead jobs back to their queues for immediate processing.
    ///
    /// # Returns
    ///
    /// The number of dead jobs revived, or an [`OxanaError`] if the operation fails.
    pub async fn revive_all_dead(&self) -> Result<usize, OxanaError> {
        self.internal.revive_all_dead().await
    }

    /// Returns Prometheus metrics based on the current stats.
    ///
    /// # Returns
    ///
    /// A [`PrometheusMetrics`] instance containing all current metrics,
    /// or an [`OxanaError`] if fetching stats fails.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use oxana::Storage;
    ///
    /// async fn example(storage: &Storage) -> Result<(), oxana::OxanaError> {
    ///     let metrics = storage.metrics().await?;
    ///     let output = metrics.encode_to_string()?;
    ///     println!("{}", output);
    ///     Ok(())
    /// }
    /// ```
    #[cfg(feature = "prometheus")]
    pub async fn metrics(&self) -> Result<PrometheusMetrics, OxanaError> {
        let stats = self.stats().await?;
        Ok(PrometheusMetrics::from_stats(&stats))
    }
}

fn validate_runtime_concurrency(
    queue_key: &str,
    concurrency: QueueConcurrency,
    config: &QueueRuntimeConfig,
) -> Result<(), OxanaError> {
    if config.concurrency.is_some() {
        validate_dynamic_concurrency(queue_key, concurrency)?;
    }

    Ok(())
}

fn validate_dynamic_concurrency(
    queue_key: &str,
    concurrency: QueueConcurrency,
) -> Result<(), OxanaError> {
    if concurrency.is_dynamic() {
        Ok(())
    } else {
        Err(OxanaError::ConfigError(format!(
            "Queue {queue_key} has fixed concurrency and cannot be overridden"
        )))
    }
}

struct StorageMonitoringSettings {
    dead_process_threshold: RwLock<Duration>,
}

impl Default for StorageMonitoringSettings {
    fn default() -> Self {
        Self {
            dead_process_threshold: RwLock::new(DEFAULT_DEAD_PROCESS_THRESHOLD),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_storage() -> Storage {
        Storage::builder()
            .build_from_redis_url("redis://127.0.0.1/0")
            .expect("test storage should build")
    }

    #[test]
    fn runtime_settings_defaults_match_existing_behavior() {
        let settings = test_storage().runtime(()).settings();

        assert_eq!(settings.heartbeat_interval, Duration::from_millis(500));
        assert_eq!(settings.dead_process_threshold, Duration::from_secs(5));
        assert_eq!(settings.resurrect_scan_interval, Duration::from_secs(2));
        assert_eq!(settings.redis_failure_tolerance, 30);
        assert_eq!(settings.retry_poll_interval, Duration::from_millis(300));
        assert_eq!(settings.schedule_poll_interval, Duration::from_millis(300));
        assert_eq!(settings.cron_initial_offset, Duration::from_secs(3));
        assert_eq!(settings.cron_lookahead, Duration::from_secs(30));
        assert_eq!(settings.cron_tick_interval, Duration::from_secs(1));
        assert_eq!(settings.dequeue_timeout, Duration::from_secs(10));
        assert_eq!(settings.dispatcher_idle_sleep, Duration::from_secs(1));
        assert_eq!(
            settings.throttled_queue_fallback_wait,
            Duration::from_millis(100)
        );
        assert_eq!(settings.shutdown_timeout, Duration::from_secs(180));
        assert_eq!(settings.exit_when_processed, None);
    }

    #[test]
    fn runtime_setting_setters_override_defaults() {
        let storage = test_storage();
        let runtime = storage
            .runtime(())
            .heartbeat_interval(Duration::from_millis(11))
            .dead_process_threshold(Duration::from_millis(12))
            .resurrect_scan_interval(Duration::from_millis(13))
            .redis_failure_tolerance(14)
            .retry_poll_interval(Duration::from_millis(15))
            .schedule_poll_interval(Duration::from_millis(16))
            .cron_initial_offset(Duration::from_millis(17))
            .cron_lookahead(Duration::from_millis(18))
            .cron_tick_interval(Duration::from_millis(19))
            .dequeue_timeout(Duration::from_millis(20))
            .dispatcher_idle_sleep(Duration::from_millis(21))
            .throttled_queue_fallback_wait(Duration::from_millis(22))
            .shutdown_timeout(Duration::from_millis(23))
            .exit_when_processed(24);

        let settings = runtime.settings();

        assert_eq!(settings.heartbeat_interval, Duration::from_millis(11));
        assert_eq!(settings.dead_process_threshold, Duration::from_millis(12));
        assert_eq!(settings.resurrect_scan_interval, Duration::from_millis(13));
        assert_eq!(settings.redis_failure_tolerance, 14);
        assert_eq!(settings.retry_poll_interval, Duration::from_millis(15));
        assert_eq!(settings.schedule_poll_interval, Duration::from_millis(16));
        assert_eq!(settings.cron_initial_offset, Duration::from_millis(17));
        assert_eq!(settings.cron_lookahead, Duration::from_millis(18));
        assert_eq!(settings.cron_tick_interval, Duration::from_millis(19));
        assert_eq!(settings.dequeue_timeout, Duration::from_millis(20));
        assert_eq!(settings.dispatcher_idle_sleep, Duration::from_millis(21));
        assert_eq!(
            settings.throttled_queue_fallback_wait,
            Duration::from_millis(22)
        );
        assert_eq!(settings.shutdown_timeout, Duration::from_millis(23));
        assert_eq!(settings.exit_when_processed, Some(24));
        assert_eq!(storage.dead_process_threshold(), Duration::from_millis(12));
    }

    #[test]
    fn cron_lookahead_allows_values_above_the_default() {
        let settings = test_storage()
            .runtime(())
            .cron_lookahead(Duration::from_secs(60))
            .settings();

        assert_eq!(settings.cron_lookahead, Duration::from_secs(60));
    }

    #[test]
    fn runtime_loop_cadence_setters_reject_zero() {
        fn assert_zero_duration_panics(f: impl FnOnce() + std::panic::UnwindSafe) {
            let panic = std::panic::catch_unwind(f)
                .expect_err("zero duration should be rejected before the runtime starts");
            let message = panic
                .downcast::<String>()
                .expect("panic payload should be a string");
            assert!(message.contains("must be greater than zero"));
        }

        assert_zero_duration_panics(|| {
            let _runtime = test_storage()
                .runtime(())
                .heartbeat_interval(Duration::ZERO);
        });
        assert_zero_duration_panics(|| {
            let _runtime = test_storage()
                .runtime(())
                .resurrect_scan_interval(Duration::ZERO);
        });
        assert_zero_duration_panics(|| {
            let _runtime = test_storage()
                .runtime(())
                .retry_poll_interval(Duration::ZERO);
        });
        assert_zero_duration_panics(|| {
            let _runtime = test_storage()
                .runtime(())
                .schedule_poll_interval(Duration::ZERO);
        });
        assert_zero_duration_panics(|| {
            let _runtime = test_storage()
                .runtime(())
                .cron_tick_interval(Duration::ZERO);
        });
        assert_zero_duration_panics(|| {
            let _runtime = test_storage().runtime(()).dequeue_timeout(Duration::ZERO);
        });
        assert_zero_duration_panics(|| {
            let _runtime = test_storage()
                .runtime(())
                .dispatcher_idle_sleep(Duration::ZERO);
        });
        assert_zero_duration_panics(|| {
            let _runtime = test_storage()
                .runtime(())
                .throttled_queue_fallback_wait(Duration::ZERO);
        });
    }
}
