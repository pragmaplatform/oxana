use deadpool_redis::redis::{self, AsyncCommands};
use futures::TryStreamExt;
use std::{
    collections::{HashMap, HashSet},
    num::NonZero,
    sync::Arc,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

use crate::{
    OxanaError,
    job_envelope::{JobConflictStrategy, JobEnvelope, JobId},
    metrics::*,
    queue::{QueueRuntimeConfig, QueueState},
    result_collector::QueueResultStats,
    stats::{
        DynamicQueueStats, Process, QueueRateStats, QueueStats, Stats, StatsGlobal, StatsProcessing,
    },
    storage_keys::StorageKeys,
    storage_types::QueueListOpts,
    worker_registry::CronJob,
};

const JOB_EXPIRE_TIME: i64 = 7 * 24 * 3600; // 7 days
const SCAN_BATCH_SIZE: usize = 500;
const ENQUEUE_LIST_CHUNK_SIZE: usize = 100;
const QUEUE_LENGTH_SNAPSHOT_TTL_SECS: i64 = 120;

#[derive(Debug, PartialEq, Eq)]
enum UniqueCronAction {
    Enqueue,
    Wait,
    Skip,
}

fn unique_cron_action(
    existing_job: bool,
    previously_observed_existing_job: bool,
    scheduled_at: i64,
    now: i64,
) -> UniqueCronAction {
    if now >= scheduled_at && (existing_job || previously_observed_existing_job) {
        UniqueCronAction::Skip
    } else if existing_job {
        UniqueCronAction::Wait
    } else {
        UniqueCronAction::Enqueue
    }
}

fn unix_timestamp_secs_f64() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn enqueue_list_chunks(envelopes: &[JobEnvelope]) -> std::slice::Chunks<'_, JobEnvelope> {
    envelopes.chunks(ENQUEUE_LIST_CHUNK_SIZE)
}

#[derive(Clone)]
pub(crate) struct StorageInternal {
    pool: deadpool_redis::Pool,
    stats_pool: Option<deadpool_redis::Pool>,
    keys: StorageKeys,
    started_at: i64,
    consecutive_redis_failures: Arc<AtomicU32>,
}

enum JobEnqueueAction {
    Default,
    Skip,
    Replace { existing_queue: String },
}

#[derive(Clone, Copy)]
enum JobDestination {
    Queue,
    Schedule,
}

#[derive(Default)]
struct QueueLengthSnapshot {
    enqueued: Option<i64>,
    refreshed_at: Option<i64>,
}

struct QueueStatsInputs {
    stats: HashMap<String, i64>,
    queue_length_rate_hashes: Vec<HashMap<String, i64>>,
    queue_counter_totals: HashMap<String, QueueCounterTotals>,
}

impl StorageInternal {
    pub fn new(pool: deadpool_redis::Pool, namespace: Option<String>) -> Self {
        Self::with_optional_stats_pool(pool, None, namespace)
    }

    pub fn with_stats_pool(
        pool: deadpool_redis::Pool,
        stats_pool: deadpool_redis::Pool,
        namespace: Option<String>,
    ) -> Self {
        Self::with_optional_stats_pool(pool, Some(stats_pool), namespace)
    }

    fn with_optional_stats_pool(
        pool: deadpool_redis::Pool,
        stats_pool: Option<deadpool_redis::Pool>,
        namespace: Option<String>,
    ) -> Self {
        let keys = StorageKeys::new(namespace.unwrap_or_default());
        Self {
            pool,
            stats_pool,
            keys,
            started_at: chrono::Utc::now().timestamp(),
            consecutive_redis_failures: Arc::new(AtomicU32::new(0)),
        }
    }

    fn record_redis_success(&self) {
        if self.consecutive_redis_failures.load(Ordering::Relaxed) != 0 {
            self.consecutive_redis_failures.store(0, Ordering::Relaxed);
        }
    }

    fn record_redis_failure(&self, err: &OxanaError, failure_tolerance: u32) -> bool {
        let count = self
            .consecutive_redis_failures
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        tracing::warn!(error = %err, consecutive_failures = count, "Transient Redis error");
        count >= failure_tolerance
    }

    /// Wraps a Redis operation result with resilience tracking.
    /// Returns `Ok(Some(value))` on success, `Ok(None)` on transient failure,
    /// or `Err(e)` if the consecutive failure threshold has been exceeded.
    pub(crate) fn track_redis_result<T>(
        &self,
        result: Result<T, OxanaError>,
        failure_tolerance: u32,
    ) -> Result<Option<T>, OxanaError> {
        match result {
            Ok(val) => {
                self.record_redis_success();
                Ok(Some(val))
            }
            Err(e) => {
                if self.record_redis_failure(&e, failure_tolerance) {
                    Err(e)
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn namespace(&self) -> &str {
        &self.keys.namespace
    }

    #[cfg(test)]
    pub(crate) fn has_dedicated_stats_pool(&self) -> bool {
        self.stats_pool.is_some()
    }

    pub async fn pool(&self) -> Result<deadpool_redis::Pool, OxanaError> {
        Ok(self.pool.clone())
    }

    pub async fn connection(&self) -> Result<deadpool_redis::Connection, OxanaError> {
        Self::connection_from_pool(&self.pool).await
    }

    async fn stats_connection(&self) -> Result<deadpool_redis::Connection, OxanaError> {
        let pool = self.stats_pool.as_ref().unwrap_or(&self.pool);
        Self::connection_from_pool(pool).await
    }

    async fn connection_from_pool(
        pool: &deadpool_redis::Pool,
    ) -> Result<deadpool_redis::Connection, OxanaError> {
        pool.get().await.map_err(OxanaError::DeadpoolRedisPoolError)
    }

    async fn scan_keys_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        pattern: &str,
    ) -> Result<HashSet<String>, OxanaError> {
        let opts = redis::ScanOptions::default()
            .with_pattern(pattern)
            .with_count(SCAN_BATCH_SIZE);
        let iter = redis.scan_options::<String>(opts).await?;
        let keys: Vec<String> = iter.try_collect().await?;
        Ok(keys.into_iter().collect())
    }

    #[cfg(test)]
    pub async fn queue_keys(&self, pattern: &str) -> Result<HashSet<String>, OxanaError> {
        let mut conn = self.connection().await?;
        self.scan_keys_w_conn(&mut conn, &self.namespace_queue(pattern))
            .await
    }

    pub async fn queue_config_or_default(
        &self,
        queue: &str,
        default_config: QueueRuntimeConfig,
    ) -> Result<QueueRuntimeConfig, OxanaError> {
        let mut redis = self.connection().await?;
        self.queue_config_or_default_w_conn(&mut redis, queue, default_config)
            .await
    }

    async fn queue_config_or_default_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        queue: &str,
        default_config: QueueRuntimeConfig,
    ) -> Result<QueueRuntimeConfig, OxanaError> {
        let config = self.queue_config_w_conn(redis, queue).await?;
        Ok(config
            .map(|config| config.with_defaults(default_config.clone()))
            .unwrap_or(default_config))
    }

    pub async fn queue_config(
        &self,
        queue: &str,
    ) -> Result<Option<QueueRuntimeConfig>, OxanaError> {
        let mut redis = self.connection().await?;
        self.queue_config_w_conn(&mut redis, queue).await
    }

    async fn queue_config_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        queue: &str,
    ) -> Result<Option<QueueRuntimeConfig>, OxanaError> {
        let config: Option<String> = (*redis).hget(&self.keys.queue_configs, queue).await?;
        config
            .map(|config| serde_json::from_str(&config).map_err(OxanaError::from))
            .transpose()
    }

    pub async fn queue_configs(
        &self,
        queues: &[String],
    ) -> Result<HashMap<String, QueueRuntimeConfig>, OxanaError> {
        if queues.is_empty() {
            return Ok(HashMap::new());
        }

        let mut redis = self.connection().await?;
        let configs: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(&self.keys.queue_configs)
            .arg(queues)
            .query_async(&mut redis)
            .await?;
        let mut map = HashMap::new();

        for (queue, config) in queues.iter().zip(configs) {
            if let Some(config) = config {
                map.insert(queue.clone(), serde_json::from_str(&config)?);
            }
        }

        Ok(map)
    }

    pub async fn set_queue_config(
        &self,
        queue: &str,
        config: &QueueRuntimeConfig,
    ) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let _: () = (*redis)
            .hset(
                &self.keys.queue_configs,
                queue,
                serde_json::to_string(config)?,
            )
            .await?;
        Ok(())
    }

    pub async fn reset_queue_config(&self, queue: &str) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let _: () = (*redis).hdel(&self.keys.queue_configs, queue).await?;
        Ok(())
    }

    pub async fn set_queue_state(&self, queue: &str, state: QueueState) -> Result<(), OxanaError> {
        let mut config = self.queue_config(queue).await?.unwrap_or_default();
        config.state = state;
        self.set_queue_config(queue, &config).await
    }

    pub(crate) async fn queues(&self, pattern: &str) -> Result<Vec<String>, OxanaError> {
        let mut redis = self.connection().await?;
        self.queues_w_conn(&mut redis, pattern).await
    }

    async fn queues_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        pattern: &str,
    ) -> Result<Vec<String>, OxanaError> {
        let queue_keys = self
            .scan_keys_w_conn(redis, &self.namespace_queue(pattern))
            .await?;
        // remove namespace prefix from beginning of each key
        let prefix = format!("{}:", self.keys.queue_prefix);
        let queues = queue_keys
            .into_iter()
            .filter_map(|key| key.strip_prefix(&prefix).map(ToOwned::to_owned))
            .collect();
        Ok(queues)
    }

    pub async fn enqueue(&self, envelope: JobEnvelope) -> Result<JobId, OxanaError> {
        let mut redis = self.connection().await?;
        self.enqueue_w_conn(&mut redis, envelope, JobDestination::Queue)
            .await
    }

    pub async fn enqueue_list(
        &self,
        envelopes: Vec<JobEnvelope>,
    ) -> Result<Vec<JobId>, OxanaError> {
        if envelopes.is_empty() {
            return Ok(Vec::new());
        }

        let mut redis = self.connection().await?;
        self.enqueue_list_w_conn(&mut redis, envelopes, JobDestination::Queue)
            .await
    }

    async fn enqueue_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        envelope: JobEnvelope,
        destination: JobDestination,
    ) -> Result<JobId, OxanaError> {
        let mut pipe = redis::pipe();

        match self.job_enqueue_action(redis, &envelope).await? {
            JobEnqueueAction::Skip => {
                tracing::warn!("Unique job {} already exists, skipping", envelope.id);

                return Ok(envelope.id);
            }
            JobEnqueueAction::Replace { existing_queue } => {
                tracing::warn!("Unique job {} already exists, replacing", envelope.id);

                self.append_replace(&mut pipe, &existing_queue, &envelope, destination)?;
            }
            JobEnqueueAction::Default => {
                self.append_enqueue(&mut pipe, &envelope, destination)?;
            }
        }

        let _: () = pipe.query_async(&mut *redis).await?;

        Ok(envelope.id)
    }

    async fn enqueue_list_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        envelopes: Vec<JobEnvelope>,
        destination: JobDestination,
    ) -> Result<Vec<JobId>, OxanaError> {
        let mut job_ids = Vec::with_capacity(envelopes.len());
        let mut staged_unique_queues: HashMap<JobId, String> = HashMap::new();

        for chunk in enqueue_list_chunks(&envelopes) {
            let mut pipe = redis::pipe();
            let mut has_writes = false;

            for envelope in chunk {
                let job_id = envelope.id.clone();
                let action = self
                    .enqueue_list_action(redis, envelope, &staged_unique_queues)
                    .await?;

                let wrote = match action {
                    JobEnqueueAction::Skip => {
                        tracing::warn!("Unique job {} already exists, skipping", envelope.id);
                        false
                    }
                    JobEnqueueAction::Replace { existing_queue } => {
                        tracing::warn!("Unique job {} already exists, replacing", envelope.id);

                        self.append_replace(&mut pipe, &existing_queue, envelope, destination)?;
                        true
                    }
                    JobEnqueueAction::Default => {
                        self.append_enqueue(&mut pipe, envelope, destination)?;
                        true
                    }
                };

                if wrote {
                    if envelope.meta.unique {
                        staged_unique_queues.insert(job_id.clone(), envelope.queue.clone());
                    }
                    has_writes = true;
                }

                job_ids.push(job_id);
            }

            if has_writes {
                let _: () = pipe.query_async(&mut *redis).await?;
            }
        }

        Ok(job_ids)
    }

    async fn enqueue_list_action(
        &self,
        redis: &mut deadpool_redis::Connection,
        envelope: &JobEnvelope,
        staged_unique_queues: &HashMap<JobId, String>,
    ) -> Result<JobEnqueueAction, OxanaError> {
        if !envelope.meta.unique {
            return Ok(JobEnqueueAction::Default);
        }

        let Some(existing_queue) = staged_unique_queues.get(&envelope.id) else {
            return self.job_enqueue_action(redis, envelope).await;
        };

        match envelope.meta.on_conflict.as_ref() {
            Some(JobConflictStrategy::Replace) => Ok(JobEnqueueAction::Replace {
                existing_queue: existing_queue.clone(),
            }),
            Some(JobConflictStrategy::Skip) | None => Ok(JobEnqueueAction::Skip),
        }
    }

    fn append_enqueue(
        &self,
        pipe: &mut redis::Pipeline,
        envelope: &JobEnvelope,
        destination: JobDestination,
    ) -> Result<(), OxanaError> {
        pipe.hset(
            &self.keys.jobs,
            &envelope.id,
            serde_json::to_string(envelope)?,
        );
        self.push_to_destination(pipe, envelope, destination);
        Ok(())
    }

    fn push_to_destination(
        &self,
        pipe: &mut redis::Pipeline,
        envelope: &JobEnvelope,
        destination: JobDestination,
    ) {
        match destination {
            JobDestination::Queue => {
                pipe.lpush(self.namespace_queue(&envelope.queue), &envelope.id);
            }
            JobDestination::Schedule => {
                pipe.zadd(
                    &self.keys.schedule,
                    &envelope.id,
                    envelope.meta.scheduled_at,
                );
            }
        }
    }

    async fn job_enqueue_action(
        &self,
        redis: &mut deadpool_redis::Connection,
        envelope: &JobEnvelope,
    ) -> Result<JobEnqueueAction, OxanaError> {
        if !envelope.meta.unique {
            return Ok(JobEnqueueAction::Default);
        }

        let exists: bool = redis.hexists(&self.keys.jobs, &envelope.id).await?;

        if exists {
            match envelope.meta.on_conflict {
                Some(JobConflictStrategy::Skip) | None => Ok(JobEnqueueAction::Skip),
                Some(JobConflictStrategy::Replace) => {
                    match self.get_job_w_conn(redis, &envelope.id).await? {
                        Some(existing) => Ok(JobEnqueueAction::Replace {
                            existing_queue: existing.queue,
                        }),
                        None => Ok(JobEnqueueAction::Default),
                    }
                }
            }
        } else {
            Ok(JobEnqueueAction::Default)
        }
    }

    fn append_replace(
        &self,
        pipe: &mut redis::Pipeline,
        existing_queue: &str,
        envelope: &JobEnvelope,
        destination: JobDestination,
    ) -> Result<(), OxanaError> {
        let old_queue = self.namespace_queue(existing_queue);

        pipe.hset(
            &self.keys.jobs,
            &envelope.id,
            serde_json::to_string(envelope)?,
        )
        .zrem(&self.keys.schedule, &envelope.id)
        .zrem(&self.keys.retry, &envelope.id)
        .lrem(old_queue, 0, &envelope.id);

        if existing_queue != envelope.queue.as_str() {
            pipe.lrem(self.namespace_queue(&envelope.queue), 0, &envelope.id);
        }

        self.push_to_destination(pipe, envelope, destination);

        Ok(())
    }

    pub async fn enqueue_in(
        &self,
        envelope: JobEnvelope,
        delay_s: u64,
    ) -> Result<JobId, OxanaError> {
        if delay_s == 0 {
            self.enqueue(envelope).await
        } else {
            let time = chrono::Utc::now() + chrono::Duration::seconds(delay_s as i64);
            self.enqueue_at(envelope.with_scheduled_at(time)).await
        }
    }

    pub async fn enqueue_at(&self, envelope: JobEnvelope) -> Result<JobId, OxanaError> {
        if envelope.meta.scheduled_at <= chrono::Utc::now().timestamp_micros() {
            return self.enqueue(envelope).await;
        }

        let mut redis = self.connection().await?;
        self.enqueue_w_conn(&mut redis, envelope, JobDestination::Schedule)
            .await
    }

    pub async fn retry_in(
        &self,
        job_id: JobId,
        delay_s: u64,
        error: String,
    ) -> Result<(), OxanaError> {
        let updated_envelope = self
            .get_job(&job_id)
            .await?
            .ok_or(OxanaError::JobNotFound)?
            .with_retries_incremented(error);

        let now = chrono::Utc::now().timestamp_micros() as u64;

        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .hset(
                &self.keys.jobs,
                &updated_envelope.id,
                serde_json::to_string(&updated_envelope)?,
            )
            .zadd(
                &self.keys.retry,
                updated_envelope.id,
                now + delay_s * 1_000_000,
            )
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn dequeue(&self, queue: &str) -> Result<Option<JobId>, OxanaError> {
        let mut redis = self.connection().await?;
        let job_id: Option<JobId> = redis
            .lmove(
                self.namespace_queue(queue),
                self.current_processing_queue(),
                redis::Direction::Right,
                redis::Direction::Left,
            )
            .await?;
        Ok(job_id)
    }

    pub async fn get_job(&self, id: &JobId) -> Result<Option<JobEnvelope>, OxanaError> {
        let mut redis = self.connection().await?;
        self.get_job_w_conn(&mut redis, id).await
    }

    async fn get_job_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        id: &JobId,
    ) -> Result<Option<JobEnvelope>, OxanaError> {
        let envelope: Option<String> = redis.hget(&self.keys.jobs, id).await?;
        match envelope {
            Some(envelope) => Ok(Some(serde_json::from_str(&envelope)?)),
            None => Ok(None),
        }
    }

    pub async fn update_job(&self, envelope: &JobEnvelope) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .hset(
                &self.keys.jobs,
                &envelope.id,
                serde_json::to_string(envelope)?,
            )
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn update_jobs(&self, envelopes: &[JobEnvelope]) -> Result<(), OxanaError> {
        if envelopes.is_empty() {
            return Ok(());
        }

        let items = envelopes
            .iter()
            .map(|envelope| Ok((envelope.id.as_str(), serde_json::to_string(envelope)?)))
            .collect::<Result<Vec<_>, OxanaError>>()?;

        let mut redis = self.connection().await?;
        let _: () = redis.hset_multiple(&self.keys.jobs, &items).await?;
        Ok(())
    }

    pub async fn update_state(
        &self,
        id: &JobId,
        state: serde_json::Value,
    ) -> Result<(), OxanaError> {
        let mut envelope = match self.get_job(id).await? {
            Some(envelope) => envelope,
            None => return Err(OxanaError::JobNotFound),
        };
        envelope.meta.state = Some(state);
        self.update_job(&envelope).await
    }

    pub async fn set_started_at_batch(
        &self,
        envelopes: &mut [JobEnvelope],
    ) -> Result<(), OxanaError> {
        let now = chrono::Utc::now().timestamp_micros();
        for envelope in envelopes.iter_mut() {
            envelope.meta.started_at = Some(now);
        }
        self.update_jobs(envelopes).await?;
        Ok(())
    }

    pub async fn delete_job(&self, id: &JobId) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let raw_envelope: Option<String> = redis.hget(&self.keys.jobs, id).await?;
        let queue = raw_envelope
            .as_deref()
            .and_then(|envelope| serde_json::from_str::<serde_json::Value>(envelope).ok())
            .and_then(|envelope| {
                envelope
                    .get("queue")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        let queue_keys = if let Some(queue) = queue {
            HashSet::from([self.namespace_queue(&queue)])
        } else if raw_envelope.is_some() {
            self.scan_keys_w_conn(&mut redis, &self.namespace_queue("*"))
                .await?
        } else {
            HashSet::new()
        };
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hdel(&self.keys.jobs, id)
            .zrem(&self.keys.schedule, id)
            .zrem(&self.keys.retry, id)
            .lrem(self.current_processing_queue(), 0, id);

        for queue_key in queue_keys {
            pipe.lrem(queue_key, 0, id);
        }

        let _: () = pipe.query_async(&mut redis).await?;
        Ok(())
    }

    pub async fn get_many(&self, ids: &[JobId]) -> Result<Vec<JobEnvelope>, OxanaError> {
        let mut redis = self.connection().await?;
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&self.keys.jobs);
        cmd.arg(ids);
        let envelopes_str: Vec<Option<String>> = cmd.query_async(&mut redis).await?;
        let mut envelopes: Vec<JobEnvelope> = vec![];
        for envelope_str in envelopes_str.into_iter().flatten() {
            envelopes.push(serde_json::from_str(&envelope_str)?);
        }
        Ok(envelopes)
    }

    pub async fn kill(&self, envelope: &JobEnvelope, error: String) -> Result<(), OxanaError> {
        let envelope = envelope.clone().with_error(error);
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .lrem(self.current_processing_queue(), 1, &envelope.id)
            .hdel(&self.keys.jobs, &envelope.id)
            .lpush(&self.keys.dead, &serde_json::to_string(&envelope)?)
            .ltrim(&self.keys.dead, 0, 999)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn finish_with_success(&self, envelope: &JobEnvelope) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .hdel(&self.keys.jobs, &envelope.id)
            .lrem(self.current_processing_queue(), 1, &envelope.id)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn finish_with_success_batch(
        &self,
        envelopes: &[JobEnvelope],
    ) -> Result<(), OxanaError> {
        if envelopes.is_empty() {
            return Ok(());
        }

        let job_ids = envelopes
            .iter()
            .map(|envelope| envelope.id.as_str())
            .collect::<Vec<_>>();
        let processing_queue = self.current_processing_queue();
        let mut pipe = redis::pipe();
        pipe.hdel(&self.keys.jobs, &job_ids);
        for job_id in job_ids {
            pipe.lrem(&processing_queue, 1, job_id);
        }

        let mut redis = self.connection().await?;
        let _: () = pipe.query_async(&mut redis).await?;
        Ok(())
    }

    pub async fn finish_with_failure(&self, envelope: &JobEnvelope) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let _: () = redis::pipe()
            .lrem(self.current_processing_queue(), 1, &envelope.id)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn enqueue_scheduled(&self, schedule_queue: &str) -> Result<usize, OxanaError> {
        let now = chrono::Utc::now().timestamp_micros();
        let mut redis = self.connection().await?;
        let job_ids: Vec<JobId> = redis.zrangebyscore(schedule_queue, 0, now).await?;

        self.claim_and_enqueue_scheduled_job_ids(&mut redis, schedule_queue, job_ids)
            .await
    }

    async fn claim_and_enqueue_scheduled_job_ids(
        &self,
        redis: &mut deadpool_redis::Connection,
        schedule_queue: &str,
        job_ids: Vec<JobId>,
    ) -> Result<usize, OxanaError> {
        if job_ids.is_empty() {
            return Ok(0);
        }

        let mut claim_pipe = redis::pipe();
        for job_id in &job_ids {
            claim_pipe.zrem(schedule_queue, job_id);
        }
        let claimed: Vec<u32> = claim_pipe.query_async(&mut *redis).await?;

        let claimed_job_ids: Vec<JobId> = job_ids
            .into_iter()
            .zip(claimed)
            .filter(|(_, removed)| *removed > 0)
            .map(|(id, _)| id)
            .collect();

        if claimed_job_ids.is_empty() {
            return Ok(0);
        }

        let envelopes = self.get_many(&claimed_job_ids).await?;
        let envelopes_count = envelopes.len();

        let mut enqueue_pipe = redis::pipe();
        let mut map: HashMap<&str, Vec<&str>> = HashMap::new();

        for envelope in envelopes.iter() {
            map.entry(&envelope.queue)
                .or_default()
                .push(envelope.id.as_str());
        }

        for (queue, job_ids) in map {
            enqueue_pipe.lpush(self.namespace_queue(queue), job_ids);
        }

        let _: () = enqueue_pipe.query_async(&mut *redis).await?;

        Ok(envelopes_count)
    }

    pub async fn retry_all_now(&self) -> Result<usize, OxanaError> {
        let mut redis = self.connection().await?;
        let job_ids: Vec<JobId> = (*redis).zrange(&self.keys.retry, 0, -1).await?;

        self.claim_and_enqueue_scheduled_job_ids(&mut redis, &self.keys.retry, job_ids)
            .await
    }

    pub async fn list_queue_jobs(
        &self,
        queue: &str,
        opts: &QueueListOpts,
    ) -> Result<Vec<JobEnvelope>, OxanaError> {
        let mut redis = self.connection().await?;
        let start = opts.offset as isize;
        let stop = (opts.offset + opts.count).saturating_sub(1) as isize;
        let job_ids: Vec<JobId> = (*redis)
            .lrange(self.namespace_queue(queue), start, stop)
            .await?;

        if job_ids.is_empty() {
            return Ok(vec![]);
        }

        self.get_many(&job_ids).await
    }

    pub async fn wipe_queue(&self, queue: &str) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let queue_key = self.namespace_queue(queue);

        let job_ids: Vec<JobId> = (*redis).lrange(&queue_key, 0, -1).await?;

        if !job_ids.is_empty() {
            let mut pipe = redis::pipe();
            pipe.hdel(&self.keys.jobs, &job_ids);
            pipe.del(&queue_key);
            let _: () = pipe.query_async(&mut redis).await?;
        } else {
            let _: () = (*redis).del(&queue_key).await?;
        }

        Ok(())
    }

    pub async fn wipe_dead(&self) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let _: () = (*redis).del(&self.keys.dead).await?;
        Ok(())
    }

    pub async fn revive_all_dead(&self) -> Result<usize, OxanaError> {
        let claim_key = format!("{}:reviving:{}", self.keys.dead, uuid::Uuid::new_v4());
        let mut redis = self.connection().await?;
        let claimed: i32 = redis::cmd("EVAL")
            .arg(
                r"
            if redis.call('EXISTS', KEYS[1]) == 0 then
                return 0
            end
            redis.call('RENAME', KEYS[1], KEYS[2])
            return 1
            ",
            )
            .arg(2)
            .arg(&self.keys.dead)
            .arg(&claim_key)
            .query_async(&mut redis)
            .await?;

        if claimed == 0 {
            return Ok(0);
        }

        let entries: Vec<String> = (*redis).lrange(&claim_key, 0, -1).await?;
        drop(redis);

        let mut revived_count = 0;
        let mut entries = entries.into_iter();
        while let Some(entry) = entries.next() {
            let Ok(job) = serde_json::from_str::<JobEnvelope>(&entry) else {
                continue;
            };

            if let Err(error) = self.enqueue(job).await {
                let mut entries_to_restore = vec![entry];
                entries_to_restore.extend(entries);
                if let Err(restore_error) = self
                    .restore_claimed_dead_entries(&claim_key, entries_to_restore)
                    .await
                {
                    tracing::error!("Failed to restore claimed dead jobs: {}", restore_error);
                }
                return Err(error);
            }

            revived_count += 1;
        }

        let mut redis = self.connection().await?;
        let _: () = (*redis).del(&claim_key).await?;

        Ok(revived_count)
    }

    async fn restore_claimed_dead_entries(
        &self,
        claim_key: &str,
        entries: Vec<String>,
    ) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;

        if entries.is_empty() {
            let _: () = (*redis).del(claim_key).await?;
            return Ok(());
        }

        let _: () = redis::pipe()
            .rpush(&self.keys.dead, entries)
            .del(claim_key)
            .query_async(&mut redis)
            .await?;

        Ok(())
    }

    pub async fn list_dead(&self, opts: &QueueListOpts) -> Result<Vec<JobEnvelope>, OxanaError> {
        let mut redis = self.connection().await?;
        let start = opts.offset as isize;
        let stop = (opts.offset + opts.count).saturating_sub(1) as isize;
        let entries: Vec<String> = (*redis).lrange(&self.keys.dead, start, stop).await?;

        let jobs: Vec<JobEnvelope> = entries
            .into_iter()
            .filter_map(|s| serde_json::from_str::<JobEnvelope>(&s).ok())
            .collect();

        Ok(jobs)
    }

    pub async fn list_retries(&self, opts: &QueueListOpts) -> Result<Vec<JobEnvelope>, OxanaError> {
        let mut redis = self.connection().await?;
        let start = opts.offset as isize;
        let stop = (opts.offset + opts.count).saturating_sub(1) as isize;
        let job_ids: Vec<JobId> = (*redis).zrange(&self.keys.retry, start, stop).await?;

        if job_ids.is_empty() {
            return Ok(vec![]);
        }

        self.get_many(&job_ids).await
    }

    pub async fn list_scheduled(
        &self,
        opts: &QueueListOpts,
    ) -> Result<Vec<JobEnvelope>, OxanaError> {
        let mut redis = self.connection().await?;
        let start = opts.offset as isize;
        let stop = (opts.offset + opts.count).saturating_sub(1) as isize;
        let job_ids: Vec<JobId> = (*redis).zrange(&self.keys.schedule, start, stop).await?;

        if job_ids.is_empty() {
            return Ok(vec![]);
        }

        self.get_many(&job_ids).await
    }

    pub async fn enqueued_count(&self, queue: &str) -> Result<usize, OxanaError> {
        let mut redis = self.connection().await?;
        self.enqueued_count_w_conn(&mut redis, queue).await
    }

    async fn enqueued_count_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        queue: &str,
    ) -> Result<usize, OxanaError> {
        let count: i64 = (*redis).llen(self.namespace_queue(queue)).await?;
        Ok(count as usize)
    }

    async fn latency_s_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        queue: &str,
    ) -> Result<f64, OxanaError> {
        self.latency_micros_w_conn(redis, queue)
            .await
            .map(|latency| latency / 1_000_000.0)
    }

    pub async fn latency_ms(&self, queue: &str) -> Result<f64, OxanaError> {
        self.latency_micros(queue)
            .await
            .map(|latency| latency / 1_000.0)
    }

    pub async fn latency_micros(&self, queue: &str) -> Result<f64, OxanaError> {
        let mut redis = self.connection().await?;
        self.latency_micros_w_conn(&mut redis, queue).await
    }

    async fn latency_micros_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        queue: &str,
    ) -> Result<f64, OxanaError> {
        let result: Option<String> = (*redis).lindex(self.namespace_queue(queue), -1).await?;
        match result.as_ref() {
            Some(job_id) => {
                let envelope = self.get_job_w_conn(redis, job_id).await?;
                Ok(envelope.map_or(0.0, |envelope| {
                    let now = chrono::Utc::now().timestamp_micros();
                    (now - envelope.meta.effective_scheduled_at_micros()) as f64
                }))
            }
            None => Ok(0.0),
        }
    }

    pub async fn dead_count(&self) -> Result<usize, OxanaError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).llen(&self.keys.dead).await?;
        Ok(count as usize)
    }

    pub async fn retries_count(&self) -> Result<usize, OxanaError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).zcard(&self.keys.retry).await?;
        Ok(count as usize)
    }

    pub async fn scheduled_count(&self) -> Result<usize, OxanaError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).zcard(&self.keys.schedule).await?;
        Ok(count as usize)
    }

    pub async fn jobs_count(&self) -> Result<usize, OxanaError> {
        let mut redis = self.connection().await?;
        let count: i64 = (*redis).hlen(&self.keys.jobs).await?;
        Ok(count as usize)
    }

    pub async fn stats_queues_for(&self, patterns: &[&str]) -> Result<Vec<QueueStats>, OxanaError> {
        let mut redis = self.connection().await?;
        let mut matched_queues = Vec::new();
        for pattern in patterns {
            matched_queues.extend(self.queues_w_conn(&mut redis, pattern).await?);
        }
        matched_queues.sort();
        matched_queues.dedup();

        self.build_queue_stats(&mut redis, &matched_queues, true)
            .await
    }

    pub async fn stats_queues(&self) -> Result<Vec<QueueStats>, OxanaError> {
        let mut redis = self.connection().await?;
        let queues = self.queues_w_conn(&mut redis, "*").await?;
        self.build_queue_stats(&mut redis, &queues, false).await
    }

    pub async fn stats(&self, dead_process_threshold: Duration) -> Result<Stats, OxanaError> {
        let mut redis = self.connection().await?;

        let queues = self.queues_w_conn(&mut redis, "*").await?;
        let values = self.build_queue_stats(&mut redis, &queues, false).await?;

        let mut processed_count_total = 0;
        let mut enqueued_count_total = 0;
        let mut failed_count_total = 0;
        let mut latency_s_max: f64 = 0.0;

        for value in &values {
            if value.latency_s > latency_s_max {
                latency_s_max = value.latency_s;
            }
            for dq in &value.queues {
                if dq.latency_s > latency_s_max {
                    latency_s_max = dq.latency_s;
                }
            }
            processed_count_total += value.processed;
            enqueued_count_total += value.enqueued;
            failed_count_total += value.failed;
        }

        let processes = self.processes(dead_process_threshold).await?;

        let mut processing = vec![];

        for process in processes.iter() {
            let processing_queue = self.processing_queue(&process.id());
            let job_ids: Vec<String> = (*redis).lrange(&processing_queue, 0, -1).await?;

            for job_id in job_ids {
                if let Some(envelope) = self.get_job(&job_id).await? {
                    processing.push(StatsProcessing {
                        process_id: process.id(),
                        job_envelope: envelope,
                    });
                }
            }
        }

        Ok(Stats {
            global: StatsGlobal {
                jobs: self.jobs_count().await?,
                enqueued: enqueued_count_total,
                processed: processed_count_total,
                failed: failed_count_total,
                dead: self.dead_count().await?,
                scheduled: self.scheduled_count().await?,
                retries: self.retries_count().await?,
                latency_s_max,
            },
            processing,
            processes,
            queues: values,
        })
    }

    async fn build_queue_stats(
        &self,
        redis: &mut deadpool_redis::Connection,
        queues: &[String],
        filter: bool,
    ) -> Result<Vec<QueueStats>, OxanaError> {
        let now = chrono::Utc::now().timestamp();
        let rate_minutes = metric_minutes(now, JobMetricsQuery::new(QUEUE_RATE_WINDOW_MINUTES));
        let inputs = self.queue_stats_inputs(redis, &rate_minutes).await?;

        let mut map = HashMap::new();
        let mut queue_values: Vec<(String, String, i64)> = Vec::new();
        let mut queue_length_snapshots: HashMap<String, QueueLengthSnapshot> = HashMap::new();

        for queue in queues {
            queue_values.push((queue.clone(), "processed".to_string(), 0));
        }

        for (key, value) in inputs.stats {
            let (queue_full_key, stat_key) = match Self::stats_key_parts(&key) {
                Some(parts) => parts,
                None => continue,
            };

            if filter && !Self::stats_key_matches_filter(queue_full_key, queues) {
                continue;
            }

            match stat_key {
                "enqueued" => {
                    queue_length_snapshots
                        .entry(queue_full_key.to_string())
                        .or_default()
                        .enqueued = Some(value);
                }
                "enqueued_at" => {
                    queue_length_snapshots
                        .entry(queue_full_key.to_string())
                        .or_default()
                        .refreshed_at = Some(value);
                }
                _ => {
                    queue_values.push((queue_full_key.to_string(), stat_key.to_string(), value));
                }
            }
        }

        for (queue_full_key, stat_key, value) in queue_values {
            let Some((queue_key, queue_dynamic_key)) = Self::split_queue_stats_key(&queue_full_key)
            else {
                continue;
            };

            let queue_stats = map
                .entry(queue_key.to_string())
                .or_insert_with(|| QueueStats {
                    key: queue_key.to_string(),
                    enqueued: 0,
                    processed: 0,
                    succeeded: 0,
                    panicked: 0,
                    failed: 0,
                    latency_s: 0.0,
                    rate: QueueRateStats::default(),
                    queues: vec![],
                });

            if let Some(queue_dynamic_key) = queue_dynamic_key {
                if !queue_stats
                    .queues
                    .iter_mut()
                    .any(|q| q.suffix == queue_dynamic_key)
                {
                    queue_stats.queues.push(DynamicQueueStats {
                        suffix: queue_dynamic_key.to_string(),
                        enqueued: 0,
                        processed: 0,
                        succeeded: 0,
                        panicked: 0,
                        failed: 0,
                        latency_s: 0.0,
                        rate: QueueRateStats::default(),
                    });
                }

                if let Some(existing) = queue_stats
                    .queues
                    .iter_mut()
                    .find(|q| q.suffix == queue_dynamic_key)
                {
                    match stat_key.as_str() {
                        "processed" => existing.processed += value,
                        "succeeded" => existing.succeeded += value,
                        "panicked" => existing.panicked += value,
                        "failed" => existing.failed += value,
                        _ => {}
                    }
                }
            }

            match stat_key.as_str() {
                "processed" => queue_stats.processed += value,
                "succeeded" => queue_stats.succeeded += value,
                "panicked" => queue_stats.panicked += value,
                "failed" => queue_stats.failed += value,
                _ => {}
            }
        }

        for queue_full_key in queue_length_snapshots.keys() {
            if Self::fresh_queue_length_snapshot(&queue_length_snapshots, queue_full_key, now)
                .is_some_and(|enqueued| enqueued > 0)
            {
                Self::ensure_queue_stats_entry(&mut map, queue_full_key);
            }
        }

        let mut values: Vec<QueueStats> = map.into_values().collect();

        for value in values.iter_mut() {
            if value.queues.is_empty() {
                value.enqueued = match Self::fresh_queue_length_snapshot(
                    &queue_length_snapshots,
                    &value.key,
                    now,
                ) {
                    Some(enqueued) => enqueued,
                    None => self.enqueued_count_w_conn(redis, &value.key).await?,
                };
                value.latency_s = self.latency_s_w_conn(redis, &value.key).await?;
                value.rate = Self::queue_rate_stats(
                    &value.key,
                    value.enqueued,
                    &inputs.queue_length_rate_hashes,
                    &inputs.queue_counter_totals,
                );
            } else {
                for dynamic_queue in value.queues.iter_mut() {
                    let dynamic_queue_key = format!("{}#{}", value.key, dynamic_queue.suffix);
                    let enqueued = match Self::fresh_queue_length_snapshot(
                        &queue_length_snapshots,
                        &dynamic_queue_key,
                        now,
                    ) {
                        Some(enqueued) => enqueued,
                        None => {
                            self.enqueued_count_w_conn(redis, &dynamic_queue_key)
                                .await?
                        }
                    };
                    let latency_s = self.latency_s_w_conn(redis, &dynamic_queue_key).await?;

                    dynamic_queue.enqueued = enqueued;
                    dynamic_queue.latency_s = latency_s;
                    dynamic_queue.rate = Self::queue_rate_stats(
                        &dynamic_queue_key,
                        dynamic_queue.enqueued,
                        &inputs.queue_length_rate_hashes,
                        &inputs.queue_counter_totals,
                    );

                    if value.latency_s < latency_s {
                        value.latency_s = latency_s;
                    }
                    value.enqueued += enqueued;
                }
                value.rate = QueueRateStats::aggregate(
                    QUEUE_RATE_WINDOW_MINUTES,
                    value.enqueued,
                    value.queues.iter().map(|queue| queue.rate),
                );
            }

            value.queues.sort_by(|a, b| a.suffix.cmp(&b.suffix));
        }

        values.sort_by(|a, b| a.key.cmp(&b.key));

        Ok(values)
    }

    async fn queue_stats_inputs(
        &self,
        redis: &mut deadpool_redis::Connection,
        rate_minutes: &[i64],
    ) -> Result<QueueStatsInputs, OxanaError> {
        if self.stats_pool.is_some() {
            let mut stats_redis = self.stats_connection().await?;
            self.queue_stats_inputs_w_conn(&mut stats_redis, rate_minutes)
                .await
        } else {
            self.queue_stats_inputs_w_conn(redis, rate_minutes).await
        }
    }

    async fn queue_stats_inputs_w_conn(
        &self,
        redis: &mut deadpool_redis::Connection,
        rate_minutes: &[i64],
    ) -> Result<QueueStatsInputs, OxanaError> {
        let stats: HashMap<String, i64> = (*redis).hgetall(&self.keys.stats).await?;
        let queue_length_rate_hashes = self.queue_length_metric_hashes(redis, rate_minutes).await?;
        let queue_counter_rate_hashes = self
            .queue_counter_metric_hashes(redis, rate_minutes)
            .await?;
        let queue_counter_totals = aggregate_queue_counter_hashes(queue_counter_rate_hashes);

        Ok(QueueStatsInputs {
            stats,
            queue_length_rate_hashes,
            queue_counter_totals,
        })
    }

    fn stats_key_parts(key: &str) -> Option<(&str, &str)> {
        key.rsplit_once(':')
    }

    fn split_queue_stats_key(queue_full_key: &str) -> Option<(&str, Option<&str>)> {
        let mut queue_key_parts = queue_full_key.splitn(2, '#');
        let queue_key = queue_key_parts.next()?;
        Some((queue_key, queue_key_parts.next()))
    }

    fn ensure_queue_stats_entry(map: &mut HashMap<String, QueueStats>, queue_full_key: &str) {
        let Some((queue_key, queue_dynamic_key)) = Self::split_queue_stats_key(queue_full_key)
        else {
            return;
        };

        let queue_stats = map
            .entry(queue_key.to_string())
            .or_insert_with(|| QueueStats {
                key: queue_key.to_string(),
                enqueued: 0,
                processed: 0,
                succeeded: 0,
                panicked: 0,
                failed: 0,
                latency_s: 0.0,
                rate: QueueRateStats::default(),
                queues: vec![],
            });

        if let Some(queue_dynamic_key) = queue_dynamic_key
            && !queue_stats
                .queues
                .iter()
                .any(|q| q.suffix == queue_dynamic_key)
        {
            queue_stats.queues.push(DynamicQueueStats {
                suffix: queue_dynamic_key.to_string(),
                enqueued: 0,
                processed: 0,
                succeeded: 0,
                panicked: 0,
                failed: 0,
                latency_s: 0.0,
                rate: QueueRateStats::default(),
            });
        }
    }

    fn stats_key_matches_filter(queue_full_key: &str, queues: &[String]) -> bool {
        let base_key = queue_full_key
            .split_once('#')
            .map_or(queue_full_key, |(base, _)| base);

        queues.iter().any(|queue| {
            let queue_base = queue
                .split_once('#')
                .map_or(queue.as_str(), |(base, _)| base);
            queue_base == base_key || queue.as_str() == queue_full_key
        })
    }

    fn fresh_queue_length_snapshot(
        queue_length_snapshots: &HashMap<String, QueueLengthSnapshot>,
        queue: &str,
        now: i64,
    ) -> Option<usize> {
        let snapshot = queue_length_snapshots.get(queue)?;
        let refreshed_at = snapshot.refreshed_at?;
        if now.saturating_sub(refreshed_at) > QUEUE_LENGTH_SNAPSHOT_TTL_SECS {
            return None;
        }

        usize::try_from(snapshot.enqueued?).ok()
    }

    fn queue_rate_stats(
        queue: &str,
        enqueued: usize,
        queue_length_hashes: &[HashMap<String, i64>],
        queue_counter_totals: &HashMap<String, QueueCounterTotals>,
    ) -> QueueRateStats {
        let window_start_enqueued = queue_length_hashes
            .first()
            .and_then(|hash| hash.get(queue))
            .and_then(|value| usize::try_from(*value).ok())
            .unwrap_or_default();
        let counters = queue_counter_totals.get(queue).copied().unwrap_or_default();

        QueueRateStats::calculate(
            QUEUE_RATE_WINDOW_MINUTES,
            enqueued,
            window_start_enqueued,
            counters.processed,
            counters.succeeded,
            counters.failed,
        )
    }

    pub(crate) async fn flush_result_stats(
        &self,
        stats: &HashMap<String, QueueResultStats>,
    ) -> Result<(), OxanaError> {
        if stats.is_empty() {
            return Ok(());
        }

        let mut redis = self.stats_connection().await?;
        let mut pipe = redis::pipe();
        let mut has_commands = false;

        for (queue, queue_stats) in stats {
            for (stat_name, value) in [
                ("processed", queue_stats.processed),
                ("succeeded", queue_stats.succeeded),
                ("panicked", queue_stats.panicked),
                ("failed", queue_stats.failed),
            ] {
                if value > 0 {
                    pipe.hincr(&self.keys.stats, format!("{queue}:{stat_name}"), value);
                    has_commands = true;
                }
            }
        }

        if has_commands {
            let _: () = pipe.query_async(&mut redis).await?;
        }

        Ok(())
    }

    pub(crate) async fn refresh_queue_length_stats(
        &self,
        queues: &[String],
    ) -> Result<HashMap<String, i64>, OxanaError> {
        if queues.is_empty() {
            return Ok(HashMap::new());
        }

        let mut redis = self.connection().await?;
        let mut queues = queues.to_vec();
        queues.sort();
        queues.dedup();
        let mut lengths = HashMap::with_capacity(queues.len());

        for queue in &queues {
            let count: i64 = (*redis).llen(self.namespace_queue(queue)).await?;
            lengths.insert(queue.clone(), count);
        }

        let refreshed_at = chrono::Utc::now().timestamp();

        if self.stats_pool.is_some() {
            let mut stats_redis = self.stats_connection().await?;
            self.write_queue_length_stats(&mut stats_redis, &lengths, refreshed_at)
                .await?;
        } else {
            self.write_queue_length_stats(&mut redis, &lengths, refreshed_at)
                .await?;
        }

        Ok(lengths)
    }

    async fn write_queue_length_stats(
        &self,
        redis: &mut deadpool_redis::Connection,
        lengths: &HashMap<String, i64>,
        refreshed_at: i64,
    ) -> Result<(), OxanaError> {
        let queue_length_key = self.metrics_queue_length_key(refreshed_at.div_euclid(60));
        let mut pipe = redis::pipe();

        for (queue, count) in lengths {
            pipe.hset(&self.keys.stats, format!("{queue}:enqueued"), *count)
                .hset(
                    &self.keys.stats,
                    format!("{queue}:enqueued_at"),
                    refreshed_at,
                )
                .hset(&queue_length_key, queue, *count);
        }
        pipe.expire(queue_length_key, METRICS_RETENTION_SECS);

        let _: () = pipe.query_async(redis).await?;

        Ok(())
    }

    pub async fn flush_job_metrics(&self, buffer: &JobMetricsBuffer) -> Result<(), OxanaError> {
        if buffer.is_empty() {
            return Ok(());
        }

        let mut redis = self.stats_connection().await?;
        let mut pipe = redis::pipe();

        for (minute, identity, metrics) in buffer.records() {
            let counter_key = self.metrics_counter_key(minute);
            for (metric, value) in metrics.increments() {
                if value > 0 {
                    pipe.hincr(
                        &counter_key,
                        identity.metric_field(metric),
                        redis_metric_increment(value),
                    );
                }
            }
            pipe.expire(&counter_key, METRICS_RETENTION_SECS);

            let histogram_args = histogram_bitfield_increment_args(&metrics.histogram);
            if histogram_args.len() > 2 {
                let histogram_key = self.metrics_histogram_key(identity, minute);
                let mut cmd = redis::cmd("BITFIELD");
                cmd.arg(&histogram_key);
                for arg in histogram_args {
                    cmd.arg(arg);
                }
                pipe.add_command(cmd).ignore();
                pipe.expire(histogram_key, METRICS_RETENTION_SECS);
            }
        }

        for (minute, queue, metrics) in buffer.queue_records() {
            let counter_key = self.metrics_queue_counter_key(minute);

            for (metric, value) in metrics.increments() {
                if value > 0 {
                    pipe.hincr(
                        &counter_key,
                        queue_metric_field(queue, metric),
                        redis_metric_increment(value),
                    );
                }
            }
            pipe.expire(&counter_key, METRICS_RETENTION_SECS);
        }

        let _: () = pipe.query_async(&mut redis).await?;

        Ok(())
    }

    pub async fn job_metrics(
        &self,
        query: JobMetricsQuery,
    ) -> Result<JobMetricsSnapshot, OxanaError> {
        let mut redis = self.stats_connection().await?;
        let minutes = metric_minutes(chrono::Utc::now().timestamp(), query);
        let hashes = self.metrics_counter_hashes(&mut redis, &minutes).await?;
        let aggregation = aggregate_counter_hashes(&minutes, hashes, None);
        let starts_at = minutes.first().copied().unwrap_or_default() * 60;
        let ends_at = minutes.last().copied().unwrap_or_default() * 60;

        Ok(JobMetricsSnapshot {
            starts_at,
            ends_at,
            minutes: minutes.len(),
            totals: aggregation.totals,
            series: aggregation.series,
            workers: aggregation.workers,
        })
    }

    pub async fn job_metrics_for(
        &self,
        identity: &MetricIdentity,
        query: JobMetricsQuery,
    ) -> Result<JobMetricsDetail, OxanaError> {
        let mut redis = self.stats_connection().await?;
        let minutes = metric_minutes(chrono::Utc::now().timestamp(), query);
        let hashes = self
            .metrics_counter_hashes_for(&mut redis, &minutes, identity)
            .await?;
        let aggregation = aggregate_counter_hashes(&minutes, hashes, Some(identity));
        let histogram = self
            .metrics_histogram_counts_for(&mut redis, &minutes, identity)
            .await?;
        let starts_at = minutes.first().copied().unwrap_or_default() * 60;
        let ends_at = minutes.last().copied().unwrap_or_default() * 60;

        Ok(JobMetricsDetail {
            identity: identity.clone(),
            starts_at,
            ends_at,
            minutes: minutes.len(),
            totals: aggregation.totals,
            series: aggregation.series,
            histogram: histogram_buckets_from_counts(&histogram),
        })
    }

    pub async fn queue_length_metrics(
        &self,
        query: JobMetricsQuery,
    ) -> Result<QueueLengthMetricsSnapshot, OxanaError> {
        let mut redis = self.stats_connection().await?;
        let minutes = metric_minutes(chrono::Utc::now().timestamp(), query);
        let hashes = self
            .queue_length_metric_hashes(&mut redis, &minutes)
            .await?;
        let starts_at = minutes.first().copied().unwrap_or_default() * 60;
        let ends_at = minutes.last().copied().unwrap_or_default() * 60;

        Ok(QueueLengthMetricsSnapshot {
            starts_at,
            ends_at,
            minutes: minutes.len(),
            queues: queue_length_series_from_hashes(&minutes, hashes),
        })
    }

    pub async fn retry_loop(
        &self,
        cancel_token: CancellationToken,
        poll_interval: Duration,
        failure_tolerance: u32,
    ) -> Result<(), OxanaError> {
        tracing::info!("Starting retry loop");

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(poll_interval) => {
                    self.track_redis_result(self.enqueue_scheduled(&self.keys.retry).await, failure_tolerance)?;
                }
            }
        }
    }

    pub async fn schedule_loop(
        &self,
        cancel_token: CancellationToken,
        poll_interval: Duration,
        failure_tolerance: u32,
    ) -> Result<(), OxanaError> {
        tracing::info!("Starting schedule loop");

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(poll_interval) => {
                    self.track_redis_result(self.enqueue_scheduled(&self.keys.schedule).await, failure_tolerance)?;
                }
            }
        }
    }

    pub async fn cleanup_loop(
        &self,
        cancel_token: CancellationToken,
        failure_tolerance: u32,
    ) -> Result<(), OxanaError> {
        tracing::info!("Starting cleanup loop");

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(600)) => {
                    self.track_redis_result(self.cleanup().await, failure_tolerance)?;
                }
            }
        }
    }

    pub async fn ping_loop(
        &self,
        cancel_token: CancellationToken,
        heartbeat_interval: Duration,
        failure_tolerance: u32,
    ) -> Result<(), OxanaError> {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(heartbeat_interval) => {
                    self.track_redis_result(self.ping().await, failure_tolerance)?;
                }
            }
        }
    }

    pub async fn ping(&self) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let process = self.current_process();
        let _: () = redis::pipe()
            .zadd(
                &self.keys.processes,
                process.id(),
                unix_timestamp_secs_f64(),
            )
            .hset(
                &self.keys.processes_data,
                process.id(),
                serde_json::to_string(&process)?,
            )
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    pub async fn cleanup(&self) -> Result<usize, OxanaError> {
        let mut redis = self.connection().await?;

        let job_ids_to_clean = {
            let now = chrono::Utc::now().timestamp();
            let mut job_ids = vec![];
            let mut cursor = 0;

            loop {
                let mut cmd = redis::cmd("HSCAN");
                cmd.arg(&self.keys.jobs).arg(cursor);
                let result: (u64, Vec<String>) = cmd.query_async(&mut redis).await?;
                let (new_cursor, items) = result;

                let mut iter = items.into_iter();
                while let (Some(job_id), Some(job_str)) = (iter.next(), iter.next()) {
                    let parsed: Result<JobEnvelope, _> = serde_json::from_str(&job_str);

                    match parsed {
                        Ok(job_envelope) => {
                            if job_envelope.meta.created_at_secs() < now - JOB_EXPIRE_TIME {
                                job_ids.push(job_id);
                            }
                        }
                        Err(_) => job_ids.push(job_id),
                    }
                }

                cursor = new_cursor;

                if cursor == 0 {
                    break;
                }
            }

            job_ids
        };

        let count = job_ids_to_clean.len();
        if count > 0 {
            tracing::info!("Cleaning up {count} expired jobs");
            let () = redis.hdel(&self.keys.jobs, job_ids_to_clean).await?;
        }

        Ok(count)
    }

    pub async fn get_process_data(&self, id: &str) -> Result<Option<Process>, OxanaError> {
        let mut redis = self.connection().await?;
        let process_str: Option<String> = (*redis).hget(&self.keys.processes_data, id).await?;
        match process_str {
            Some(process_str) => Ok(Some(serde_json::from_str(&process_str)?)),
            None => Ok(None),
        }
    }

    pub async fn processes(
        &self,
        dead_process_threshold: Duration,
    ) -> Result<Vec<Process>, OxanaError> {
        let mut redis = self.connection().await?;
        let process_ids: Vec<String> = (*redis)
            .zrangebyscore(
                &self.keys.processes,
                unix_timestamp_secs_f64() - dead_process_threshold.as_secs_f64(),
                unix_timestamp_secs_f64(),
            )
            .await?;

        let mut processes = vec![];

        for process_id in process_ids {
            if let Some(process) = self.get_process_data(&process_id).await? {
                processes.push(process);
            }
        }

        Ok(processes)
    }

    pub async fn resurrect_loop(
        &self,
        cancel_token: CancellationToken,
        scan_interval: Duration,
        dead_process_threshold: Duration,
        failure_tolerance: u32,
    ) -> Result<(), OxanaError> {
        tracing::info!("Starting resurrect loop");

        // Route the initial ping through the failure tolerance so a transient
        // Redis error at startup does not abort the whole runtime.
        loop {
            if cancel_token.is_cancelled() {
                return Ok(());
            }
            if self
                .track_redis_result(self.ping().await, failure_tolerance)?
                .is_some()
            {
                break;
            }
            tokio::time::sleep(scan_interval).await;
        }

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Ok(());
                }
                _ = tokio::time::sleep(scan_interval) => {
                    self.track_redis_result(self.resurrect(dead_process_threshold).await, failure_tolerance)?;
                }
            }
        }
    }

    pub async fn cron_job_loop<F>(
        &self,
        cancel_token: CancellationToken,
        settings: crate::config::RuntimeSettings,
        cron_job: CronJob,
        mut envelope_factory: F,
    ) -> Result<(), OxanaError>
    where
        F: FnMut(i64) -> Result<(JobEnvelope, bool), OxanaError> + Send,
    {
        // Clamp instead of panicking: out-of-range offsets overflow both
        // chrono::Duration and the DateTime addition.
        let initial_offset = chrono::Duration::from_std(settings.cron_initial_offset)
            .unwrap_or(chrono::Duration::MAX);
        let start = chrono::Utc::now()
            .checked_add_signed(initial_offset)
            .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC);
        let iterator = cron_job.schedule.after(&start);

        let mut previous: Option<chrono::DateTime<chrono::Utc>> = None;

        for next in iterator {
            loop {
                if cancel_token.is_cancelled() {
                    return Ok(());
                }

                let now = chrono::Utc::now();
                let lookahead = chrono::Duration::from_std(settings.cron_lookahead)
                    .unwrap_or(chrono::Duration::MAX);
                let max_schedule_time = now
                    .checked_add_signed(lookahead)
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC);

                if next > max_schedule_time {
                    tokio::time::sleep(settings.cron_tick_interval).await;
                    continue;
                }

                if let Some(previous) = previous
                    && previous > now
                {
                    tokio::time::sleep(settings.cron_tick_interval).await;
                    continue;
                }

                break;
            }

            let scheduled_at = next.timestamp_micros();
            let (envelope, declared_unique) = envelope_factory(scheduled_at)?;

            let should_enqueue = if declared_unique {
                let mut observed_existing_job = false;
                loop {
                    if cancel_token.is_cancelled() {
                        return Ok(());
                    }
                    let action = match self.track_redis_result(
                        self.get_job(&envelope.id).await,
                        settings.redis_failure_tolerance,
                    )? {
                        Some(existing) => {
                            let existing_job = existing.is_some();
                            let action = unique_cron_action(
                                existing_job,
                                observed_existing_job,
                                scheduled_at,
                                chrono::Utc::now().timestamp_micros(),
                            );
                            observed_existing_job |= existing_job;
                            action
                        }
                        None => UniqueCronAction::Wait,
                    };
                    match action {
                        UniqueCronAction::Skip => {
                            tracing::warn!(
                                "Unique cron job {} is still active at its next occurrence, skipping",
                                envelope.id
                            );
                            break false;
                        }
                        UniqueCronAction::Wait => {
                            tokio::time::sleep(settings.cron_tick_interval).await;
                        }
                        UniqueCronAction::Enqueue => break true,
                    }
                }
            } else {
                true
            };

            if should_enqueue {
                loop {
                    if cancel_token.is_cancelled() {
                        return Ok(());
                    }
                    match self.track_redis_result(
                        self.enqueue_at(envelope.clone()).await,
                        settings.redis_failure_tolerance,
                    )? {
                        Some(_) => break,
                        None => {
                            tokio::time::sleep(settings.cron_tick_interval).await;
                        }
                    }
                }
            }

            previous = Some(next);
        }

        Ok(())
    }

    pub async fn resurrect(&self, dead_process_threshold: Duration) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        for process_id in self
            .dead_process_ids(&mut redis, dead_process_threshold)
            .await?
        {
            tracing::info!("Dead process detected: {}", process_id);

            let processing_queue = self.processing_queue(&process_id);
            let mut resurrected_count = 0;

            loop {
                let job_ids: Vec<String> = (*redis)
                    .lpop(&processing_queue, Some(NonZero::new(10).unwrap()))
                    .await?;

                if job_ids.is_empty() {
                    break;
                }

                resurrected_count += job_ids.len();

                for job_id in job_ids {
                    match self.get_job_w_conn(&mut redis, &job_id).await? {
                        Some(envelope) => {
                            if envelope.meta.resurrect {
                                tracing::info!(
                                    job_id = job_id,
                                    queue = envelope.queue,
                                    job = envelope.job.name,
                                    "Resurrecting job"
                                );
                                let _: () = (*redis)
                                    .lpush(self.namespace_queue(&envelope.queue), &envelope.id)
                                    .await?;
                            } else {
                                tracing::info!(
                                    job_id = job_id,
                                    queue = envelope.queue,
                                    job = envelope.job.name,
                                    "Skipping resurrection (resurrect=false), deleting job"
                                );
                                let _: () = (*redis).hdel(&self.keys.jobs, &envelope.id).await?;
                            }
                        }
                        None => tracing::warn!("Job {} not found", job_id),
                    }
                }
            }

            let _: () = redis::pipe()
                .zrem(&self.keys.processes, &process_id)
                .hdel(&self.keys.processes_data, &process_id)
                .query_async(&mut redis)
                .await?;

            if resurrected_count > 0 {
                tracing::info!(
                    "Resurrected process: {} ({} jobs)",
                    process_id,
                    resurrected_count
                );
            }
        }

        Ok(())
    }

    pub async fn self_cleanup(&self) -> Result<(), OxanaError> {
        let mut redis = self.connection().await?;
        let process = self.current_process();
        let _: () = redis::pipe()
            .zrem(&self.keys.processes, process.id())
            .hdel(&self.keys.processes_data, process.id())
            .query_async(&mut redis)
            .await?;

        Ok(())
    }

    fn processing_queue(&self, process_id: &str) -> String {
        format!("{}:{}", self.keys.processing_queue_prefix, process_id)
    }

    fn current_processing_queue(&self) -> String {
        self.processing_queue(&self.current_process().id())
    }

    #[cfg(test)]
    async fn currently_processing_job_ids(&self) -> Result<Vec<String>, OxanaError> {
        let mut redis = self.connection().await?;
        let job_id: Option<String> = (*redis).lindex(self.current_processing_queue(), 0).await?;
        Ok(job_id.into_iter().collect())
    }

    fn current_process(&self) -> Process {
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let pid = std::process::id();
        Process {
            hostname,
            pid,
            heartbeat_at: chrono::Utc::now().timestamp(),
            started_at: self.started_at,
        }
    }

    fn namespace_queue(&self, queue: &str) -> String {
        if queue.starts_with(self.keys.namespace.as_str()) {
            queue.to_string()
        } else {
            format!("{}:{}", self.keys.queue_prefix, queue)
        }
    }

    fn metrics_counter_key(&self, minute: i64) -> String {
        format!("{}:j:{minute}", self.keys.metrics_prefix)
    }

    fn metrics_queue_length_key(&self, minute: i64) -> String {
        format!("{}:q:{minute}", self.keys.metrics_prefix)
    }

    fn metrics_queue_counter_key(&self, minute: i64) -> String {
        format!("{}:qc:{minute}", self.keys.metrics_prefix)
    }

    fn metrics_histogram_key(&self, identity: &MetricIdentity, minute: i64) -> String {
        format!(
            "{}:h:{}:{minute}",
            self.keys.metrics_prefix,
            identity.field_key()
        )
    }

    async fn metrics_counter_hashes(
        &self,
        redis: &mut deadpool_redis::Connection,
        minutes: &[i64],
    ) -> Result<Vec<HashMap<String, i64>>, OxanaError> {
        if minutes.is_empty() {
            return Ok(Vec::new());
        }

        let mut pipe = redis::pipe();
        for minute in minutes {
            pipe.hgetall(self.metrics_counter_key(*minute));
        }
        Ok(pipe.query_async(redis).await?)
    }

    async fn queue_length_metric_hashes(
        &self,
        redis: &mut deadpool_redis::Connection,
        minutes: &[i64],
    ) -> Result<Vec<HashMap<String, i64>>, OxanaError> {
        if minutes.is_empty() {
            return Ok(Vec::new());
        }

        let mut pipe = redis::pipe();
        for minute in minutes {
            pipe.hgetall(self.metrics_queue_length_key(*minute));
        }
        Ok(pipe.query_async(redis).await?)
    }

    async fn queue_counter_metric_hashes(
        &self,
        redis: &mut deadpool_redis::Connection,
        minutes: &[i64],
    ) -> Result<Vec<HashMap<String, i64>>, OxanaError> {
        if minutes.is_empty() {
            return Ok(Vec::new());
        }

        let mut pipe = redis::pipe();
        for minute in minutes {
            pipe.hgetall(self.metrics_queue_counter_key(*minute));
        }
        Ok(pipe.query_async(redis).await?)
    }

    async fn metrics_counter_hashes_for(
        &self,
        redis: &mut deadpool_redis::Connection,
        minutes: &[i64],
        identity: &MetricIdentity,
    ) -> Result<Vec<HashMap<String, i64>>, OxanaError> {
        if minutes.is_empty() {
            return Ok(Vec::new());
        }

        let fields = JOB_METRIC_KEYS.map(|metric| identity.metric_field(metric));
        let mut pipe = redis::pipe();
        for minute in minutes {
            let counter_key = self.metrics_counter_key(*minute);
            for field in &fields {
                pipe.hget(&counter_key, field);
            }
        }

        let values: Vec<Option<i64>> = pipe.query_async(redis).await?;
        let mut hashes = vec![HashMap::new(); minutes.len()];

        for (idx, values) in values.chunks(fields.len()).enumerate() {
            let Some(hash) = hashes.get_mut(idx) else {
                continue;
            };
            for (field, value) in fields.iter().zip(values.iter()) {
                if let Some(value) = value {
                    hash.insert(field.clone(), *value);
                }
            }
        }

        Ok(hashes)
    }

    async fn metrics_histogram_counts_for(
        &self,
        redis: &mut deadpool_redis::Connection,
        minutes: &[i64],
        identity: &MetricIdentity,
    ) -> Result<[u64; HISTOGRAM_BUCKET_COUNT], OxanaError> {
        if minutes.is_empty() {
            return Ok([0; HISTOGRAM_BUCKET_COUNT]);
        }

        let fetch_args = histogram_bitfield_fetch_args();
        let mut pipe = redis::pipe();
        for minute in minutes {
            let mut cmd = redis::cmd("BITFIELD");
            cmd.arg(self.metrics_histogram_key(identity, *minute));
            for arg in &fetch_args {
                cmd.arg(arg);
            }
            pipe.add_command(cmd);
        }

        let values: Vec<Vec<Option<u64>>> = pipe.query_async(redis).await?;
        let mut totals = [0_u64; HISTOGRAM_BUCKET_COUNT];

        for bucket_values in values {
            for (idx, value) in bucket_values.into_iter().enumerate() {
                let Some(total) = totals.get_mut(idx) else {
                    continue;
                };
                *total = total.saturating_add(value.unwrap_or_default());
            }
        }

        Ok(totals)
    }

    async fn dead_process_ids(
        &self,
        redis: &mut deadpool_redis::Connection,
        dead_process_threshold: Duration,
    ) -> Result<Vec<String>, OxanaError> {
        let process_ids: Vec<(String, f64)> = (*redis)
            .zrange_withscores(&self.keys.processes, 0, -1)
            .await?;

        let active_process_ids: HashSet<String> =
            process_ids.iter().map(|(id, _)| id.clone()).collect();
        let mut dead_process_ids = Vec::new();
        let mut seen_dead = HashSet::new();
        let threshold = unix_timestamp_secs_f64() - dead_process_threshold.as_secs_f64();

        for (process_id, score) in process_ids {
            if score < threshold && seen_dead.insert(process_id.clone()) {
                dead_process_ids.push(process_id);
            }
        }

        let all_processing_queues = self
            .scan_keys_w_conn(redis, &format!("{}:*", self.keys.processing_queue_prefix))
            .await?;

        for processing_queue in all_processing_queues {
            let process_id = match processing_queue.rsplit(':').next() {
                Some(process_id) => process_id.to_string(),
                None => continue,
            };

            if !active_process_ids.contains(&process_id) && seen_dead.insert(process_id.clone()) {
                dead_process_ids.push(process_id);
            }
        }

        Ok(dead_process_ids)
    }
}

fn redis_metric_increment(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helper::{random_string, redis_pool};
    use rand::RngExt;
    use serde::Serialize;
    use testresult::TestResult;

    #[derive(Serialize)]
    struct TestJob {}

    impl crate::worker::Job for TestJob {}

    #[test]
    fn unique_cron_skips_an_observed_occurrence_if_it_vanishes_after_due() {
        assert_eq!(
            unique_cron_action(true, false, 2_000, 1_000),
            UniqueCronAction::Wait
        );
        assert_eq!(
            unique_cron_action(true, true, 2_000, 2_000),
            UniqueCronAction::Skip
        );
        assert_eq!(
            unique_cron_action(false, false, 2_000, 1_000),
            UniqueCronAction::Enqueue
        );
        assert_eq!(
            unique_cron_action(false, true, 2_000, 2_000),
            UniqueCronAction::Skip
        );
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    async fn assert_latency_ms_in_measurement_window(
        storage: &StorageInternal,
        queue: &str,
        scheduled_at_micros: i64,
    ) -> Result<f64, OxanaError> {
        let before = chrono::Utc::now().timestamp_micros();
        let latency_ms = storage.latency_ms(queue).await?;
        let after = chrono::Utc::now().timestamp_micros();

        let min_ms = (before - scheduled_at_micros) as f64 / 1_000.0;
        let max_ms = (after - scheduled_at_micros) as f64 / 1_000.0;
        assert!(
            (min_ms..=max_ms).contains(&latency_ms),
            "expected latency_ms {latency_ms} to be between {min_ms} and {max_ms}"
        );

        Ok(latency_ms)
    }

    async fn assert_latency_micros_in_measurement_window(
        storage: &StorageInternal,
        queue: &str,
        scheduled_at_micros: i64,
    ) -> Result<f64, OxanaError> {
        let before = chrono::Utc::now().timestamp_micros();
        let latency_micros = storage.latency_micros(queue).await?;
        let after = chrono::Utc::now().timestamp_micros();

        let min_micros = (before - scheduled_at_micros) as f64;
        let max_micros = (after - scheduled_at_micros) as f64;
        assert!(
            (min_micros..=max_micros).contains(&latency_micros),
            "expected latency_micros {latency_micros} to be between {min_micros} and {max_micros}"
        );

        Ok(latency_micros)
    }

    #[test]
    fn enqueue_list_chunks_batches_by_one_hundred() -> TestResult {
        let queue = random_string();
        let envelopes = (0..(ENQUEUE_LIST_CHUNK_SIZE * 2 + 1))
            .map(|_| JobEnvelope::new(queue.clone(), TestJob {}))
            .collect::<Result<Vec<_>, _>>()?;

        let chunk_sizes = enqueue_list_chunks(&envelopes)
            .map(<[JobEnvelope]>::len)
            .collect::<Vec<_>>();

        assert_eq!(ENQUEUE_LIST_CHUNK_SIZE, 100);
        assert_eq!(chunk_sizes, vec![100, 100, 1]);

        Ok(())
    }

    #[tokio::test]
    async fn redis_failure_tolerance_controls_escalation() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));

        assert!(
            storage
                .track_redis_result::<()>(
                    Err(OxanaError::GenericError("transient".to_string())),
                    2,
                )?
                .is_none()
        );
        assert!(
            storage
                .track_redis_result::<()>(
                    Err(OxanaError::GenericError("transient".to_string())),
                    2,
                )
                .is_err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ping() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        storage.ping().await?;

        let process = storage.current_process();
        let process_data = storage.get_process_data(&process.id()).await?;
        assert!(process_data.is_some());
        let process = process_data.unwrap();
        assert_eq!(
            process.hostname,
            gethostname::gethostname().to_string_lossy().to_string()
        );
        assert_eq!(process.pid, std::process::id());
        assert!(process.heartbeat_at > chrono::Utc::now().timestamp() - 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_latency() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let mut envelope = JobEnvelope::new(queue.clone(), TestJob {})?;
        let now = chrono::Utc::now();
        let actual_latency = 7777;
        let past = now.timestamp_micros() - actual_latency * 1_000;
        envelope.meta.created_at = past;
        envelope.meta.scheduled_at = past;
        storage.enqueue(envelope).await?;

        assert_latency_ms_in_measurement_window(&storage, &queue, past).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_latency_multiple_jobs() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let latency_ms = storage.latency_ms(&queue).await?;
        assert_eq!(latency_ms, 0.0);

        let latency_micros = storage.latency_micros(&queue).await?;
        assert_eq!(latency_micros, 0.0);

        let mut envelope = JobEnvelope::new(queue.clone(), TestJob {})?;
        let now = chrono::Utc::now();
        let actual_latency_ms = 7777;
        let actual_latency_micros = actual_latency_ms * 1_000;
        let past = now.timestamp_micros() - actual_latency_micros;
        envelope.meta.created_at = past;
        envelope.meta.scheduled_at = past;
        storage.enqueue(envelope).await?;

        assert_latency_ms_in_measurement_window(&storage, &queue, past).await?;
        assert_latency_micros_in_measurement_window(&storage, &queue, past).await?;

        let mut envelope2 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let actual_latency_ms2 = 5000;
        let actual_latency_micros2 = actual_latency_ms2 * 1_000;
        let past2 = now.timestamp_micros() - actual_latency_micros2;
        envelope2.meta.created_at = past2;
        envelope2.meta.scheduled_at = past2;
        storage.enqueue(envelope2).await?;

        assert_latency_ms_in_measurement_window(&storage, &queue, past).await?;
        assert_latency_micros_in_measurement_window(&storage, &queue, past).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let mut expired_envelope1 = JobEnvelope::new(queue.clone(), TestJob {})?;
        expired_envelope1.meta.created_at =
            (chrono::Utc::now().timestamp() - JOB_EXPIRE_TIME - 1) * 1000000;
        let mut expired_envelope2 = JobEnvelope::new(queue.clone(), TestJob {})?;
        expired_envelope2.meta.created_at =
            (chrono::Utc::now().timestamp() - JOB_EXPIRE_TIME - 1) * 1000000;

        let active_envelope = JobEnvelope::new(queue.clone(), TestJob {})?;

        storage.enqueue(expired_envelope1.clone()).await?;
        storage.enqueue(expired_envelope2.clone()).await?;
        storage.enqueue(active_envelope.clone()).await?;

        assert!(storage.get_job(&expired_envelope1.id).await?.is_some());
        assert!(storage.get_job(&expired_envelope2.id).await?.is_some());
        assert!(storage.get_job(&active_envelope.id).await?.is_some());

        assert_eq!(storage.cleanup().await?, 2);

        assert!(storage.get_job(&expired_envelope1.id).await?.is_none());
        assert!(storage.get_job(&expired_envelope2.id).await?.is_none());
        assert!(storage.get_job(&active_envelope.id).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_empty() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));

        assert_eq!(storage.cleanup().await?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_resurrect() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope = JobEnvelope::new(queue.clone(), TestJob {})?;

        storage.enqueue(envelope.clone()).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        let job_id = storage.dequeue(&queue).await?;

        assert_eq!(job_id, Some(envelope.id));

        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert_eq!(
            storage.currently_processing_job_ids().await?,
            vec![job_id.unwrap()]
        );

        let mut redis = storage.connection().await?;

        // fake ping in the past
        let _: () = redis
            .zadd(
                &storage.keys.processes,
                storage.current_process().id(),
                unix_timestamp_secs_f64() - 6.0,
            )
            .await?;

        storage.resurrect(Duration::from_secs(5)).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_resurrect_unique_job() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope = JobEnvelope::new_cron(
            queue.clone(),
            "CronWorker-1234567890".to_string(),
            &TestJob {},
            1234567890,
            true,
        )?
        .0;

        assert!(envelope.meta.unique);
        assert_eq!(envelope.meta.on_conflict, Some(JobConflictStrategy::Skip));

        storage.enqueue(envelope.clone()).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());
        assert!(storage.get_job(&envelope.id).await?.is_some());

        let job_id = storage.dequeue(&queue).await?;

        assert_eq!(job_id, Some(envelope.id.clone()));

        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert_eq!(
            storage.currently_processing_job_ids().await?,
            vec![job_id.expect("job_id should be Some")]
        );

        let mut redis = storage.connection().await?;

        let _: () = redis
            .zadd(
                &storage.keys.processes,
                storage.current_process().id(),
                unix_timestamp_secs_f64() - 6.0,
            )
            .await?;

        storage.resurrect(Duration::from_secs(5)).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());
        assert!(
            storage.get_job(&envelope.id).await?.is_some(),
            "unique job should still exist in jobs hash after resurrection"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_resurrect_when_process_is_missing() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope = JobEnvelope::new(queue.clone(), TestJob {})?;

        storage.enqueue(envelope.clone()).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        let job_id = storage.dequeue(&queue).await?;

        assert_eq!(job_id, Some(envelope.id));

        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert_eq!(
            storage.currently_processing_job_ids().await?,
            vec![job_id.unwrap()]
        );

        storage.resurrect(Duration::from_secs(5)).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 1);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_queue_discovery_only_returns_queue_keys() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue_a = storage.namespace_queue("prefix-a");
        let queue_b = storage.namespace_queue("prefix#fast");
        let queue_c = storage.namespace_queue("other");
        let unrelated_key = format!("{}:misc", storage.keys.namespace);

        let mut redis = storage.connection().await?;
        let _: () = redis.lpush(&queue_a, "job-a").await?;
        let _: () = redis.lpush(&queue_b, "job-b").await?;
        let _: () = redis.lpush(&queue_c, "job-c").await?;
        let _: () = redis.set(&unrelated_key, "not-a-queue").await?;
        let _: () = redis.hset(&storage.keys.jobs, "job-1", "payload").await?;

        let queue_keys = storage.queue_keys("prefix*").await?;
        assert_eq!(
            queue_keys,
            HashSet::from([queue_a.clone(), queue_b.clone()])
        );

        let mut queues = storage.queues("*").await?;
        queues.sort();
        assert_eq!(
            queues,
            vec![
                "other".to_string(),
                "prefix#fast".to_string(),
                "prefix-a".to_string(),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_queue_config_defaults_and_updates() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let default_config = QueueRuntimeConfig::new(3);
        let config = storage
            .queue_config_or_default(&queue, default_config.clone())
            .await?;
        assert_eq!(config, default_config);
        assert_eq!(storage.queue_config(&queue).await?, None);
        assert!(
            storage
                .queue_configs(std::slice::from_ref(&queue))
                .await?
                .is_empty()
        );

        let changed_default = storage
            .queue_config_or_default(&queue, QueueRuntimeConfig::new(5))
            .await?;
        assert_eq!(changed_default.concurrency, Some(5));
        assert_eq!(changed_default.state, QueueState::Active);
        assert_eq!(storage.queue_config(&queue).await?, None);

        storage
            .set_queue_config(&queue, &QueueRuntimeConfig::new(7))
            .await?;
        storage.set_queue_state(&queue, QueueState::Paused).await?;

        let config = storage
            .queue_config(&queue)
            .await?
            .expect("queue config should exist");
        assert_eq!(config.concurrency, Some(7));
        assert_eq!(config.state, QueueState::Paused);

        let preserved = storage
            .queue_config_or_default(&queue, QueueRuntimeConfig::new(1))
            .await?;
        assert_eq!(preserved.concurrency, Some(7));
        assert_eq!(preserved.state, QueueState::Paused);

        let configs = storage.queue_configs(std::slice::from_ref(&queue)).await?;
        assert_eq!(configs.get(&queue), Some(&preserved));

        storage.reset_queue_config(&queue).await?;
        assert_eq!(storage.queue_config(&queue).await?, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_queue_state_update_without_config_does_not_store_concurrency() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        storage.set_queue_state(&queue, QueueState::Paused).await?;

        let stored = storage
            .queue_config(&queue)
            .await?
            .expect("queue config should exist");
        assert_eq!(stored.concurrency, None);
        assert_eq!(stored.state, QueueState::Paused);

        let runtime_config = storage
            .queue_config_or_default(&queue, QueueRuntimeConfig::new(4))
            .await?;
        assert_eq!(runtime_config.concurrency, Some(4));
        assert_eq!(runtime_config.state, QueueState::Paused);

        let stored = storage
            .queue_config(&queue)
            .await?
            .expect("queue config should still exist");
        assert_eq!(stored.concurrency, None);
        assert_eq!(stored.state, QueueState::Paused);

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_result_stats_batches_counts() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let static_queue = random_string();
        let dynamic_prefix = random_string();
        let dynamic_queue = format!("{dynamic_prefix}#fast");

        let mut updates = HashMap::new();
        updates.insert(
            static_queue.clone(),
            QueueResultStats {
                processed: 3,
                succeeded: 1,
                panicked: 1,
                failed: 2,
            },
        );
        updates.insert(
            dynamic_queue,
            QueueResultStats {
                processed: 2,
                succeeded: 2,
                panicked: 0,
                failed: 0,
            },
        );

        storage.flush_result_stats(&updates).await?;

        let stats = storage.stats(std::time::Duration::from_secs(5)).await?;
        let static_stats = stats
            .queues
            .iter()
            .find(|queue| queue.key == static_queue)
            .expect("static queue stats should exist");
        assert_eq!(static_stats.processed, 3);
        assert_eq!(static_stats.succeeded, 1);
        assert_eq!(static_stats.panicked, 1);
        assert_eq!(static_stats.failed, 2);

        let dynamic_stats = stats
            .queues
            .iter()
            .find(|queue| queue.key == dynamic_prefix)
            .expect("dynamic queue stats should exist");
        assert_eq!(dynamic_stats.processed, 2);
        assert_eq!(dynamic_stats.succeeded, 2);
        assert_eq!(dynamic_stats.panicked, 0);
        assert_eq!(dynamic_stats.failed, 0);
        assert_eq!(dynamic_stats.queues.len(), 1);
        let dynamic_queue_stats = dynamic_stats
            .queues
            .first()
            .expect("fast dynamic queue stats should exist");
        assert_eq!(dynamic_queue_stats.suffix, "fast");
        assert_eq!(dynamic_queue_stats.processed, 2);
        assert_eq!(dynamic_queue_stats.succeeded, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_include_queue_rates_from_historical_counters() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let static_queue = random_string();
        let dynamic_prefix = random_string();
        let dynamic_queue_a = format!("{dynamic_prefix}#a");
        let dynamic_queue_b = format!("{dynamic_prefix}#b");

        let mut metrics = JobMetricsBuffer::default();
        metrics.record(&crate::result_collector::WorkerResult {
            kind: crate::result_collector::WorkerResultKind::Success,
            worker_name: "Worker".to_string(),
            queue: static_queue.clone(),
            execution_ms: 10,
            job_count: 4,
        });
        metrics.record(&crate::result_collector::WorkerResult {
            kind: crate::result_collector::WorkerResultKind::Failed,
            worker_name: "Worker".to_string(),
            queue: static_queue.clone(),
            execution_ms: 10,
            job_count: 2,
        });
        metrics.record(&crate::result_collector::WorkerResult {
            kind: crate::result_collector::WorkerResultKind::Success,
            worker_name: "Worker".to_string(),
            queue: dynamic_queue_a.clone(),
            execution_ms: 10,
            job_count: 4,
        });
        metrics.record(&crate::result_collector::WorkerResult {
            kind: crate::result_collector::WorkerResultKind::Panicked,
            worker_name: "Worker".to_string(),
            queue: dynamic_queue_b.clone(),
            execution_ms: 10,
            job_count: 2,
        });
        storage.flush_job_metrics(&metrics).await?;

        let mut updates = HashMap::new();
        updates.insert(
            static_queue.clone(),
            QueueResultStats {
                processed: 6,
                succeeded: 4,
                panicked: 0,
                failed: 2,
            },
        );
        updates.insert(
            dynamic_queue_a.clone(),
            QueueResultStats {
                processed: 4,
                succeeded: 4,
                panicked: 0,
                failed: 0,
            },
        );
        updates.insert(
            dynamic_queue_b.clone(),
            QueueResultStats {
                processed: 2,
                succeeded: 0,
                panicked: 2,
                failed: 2,
            },
        );
        storage.flush_result_stats(&updates).await?;

        for _ in 0..3 {
            storage
                .enqueue(JobEnvelope::new(static_queue.clone(), TestJob {})?)
                .await?;
        }
        for _ in 0..2 {
            storage
                .enqueue(JobEnvelope::new(dynamic_queue_a.clone(), TestJob {})?)
                .await?;
        }
        storage
            .enqueue(JobEnvelope::new(dynamic_queue_b.clone(), TestJob {})?)
            .await?;

        let stats = storage.stats(std::time::Duration::from_secs(5)).await?;
        let static_stats = stats
            .queues
            .iter()
            .find(|queue| queue.key == static_queue)
            .expect("static queue stats should exist");
        assert_close(static_stats.rate.processed_per_minute, 0.6);
        assert_close(static_stats.rate.succeeded_per_minute, 0.4);
        assert_close(static_stats.rate.failed_per_minute, 0.2);
        assert_close(static_stats.rate.growth_per_minute, 0.3);
        assert_close(static_stats.rate.effective_drain_per_minute, 0.0);
        assert!(static_stats.rate.eta_s.is_none());

        let dynamic_stats = stats
            .queues
            .iter()
            .find(|queue| queue.key == dynamic_prefix)
            .expect("dynamic queue stats should exist");
        assert_close(dynamic_stats.rate.processed_per_minute, 0.6);
        assert_close(dynamic_stats.rate.succeeded_per_minute, 0.4);
        assert_close(dynamic_stats.rate.failed_per_minute, 0.2);
        assert_close(dynamic_stats.rate.growth_per_minute, 0.3);
        assert_close(dynamic_stats.rate.effective_drain_per_minute, 0.0);
        assert!(dynamic_stats.rate.eta_s.is_none());

        let dynamic_queue_stats = dynamic_stats
            .queues
            .iter()
            .find(|queue| queue.suffix == "b")
            .expect("dynamic sub-queue stats should exist");
        assert_close(dynamic_queue_stats.rate.processed_per_minute, 0.2);
        assert_close(dynamic_queue_stats.rate.succeeded_per_minute, 0.0);
        assert_close(dynamic_queue_stats.rate.failed_per_minute, 0.2);
        assert_close(dynamic_queue_stats.rate.growth_per_minute, 0.1);
        assert_close(dynamic_queue_stats.rate.effective_drain_per_minute, 0.0);
        assert!(dynamic_queue_stats.rate.eta_s.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_refresh_queue_length_stats_reports_llen() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let static_queue = random_string();
        let dynamic_prefix = random_string();
        let dynamic_queue = format!("{dynamic_prefix}#fast");

        storage
            .enqueue(JobEnvelope::new(static_queue.clone(), TestJob {})?)
            .await?;
        storage
            .enqueue(JobEnvelope::new(static_queue.clone(), TestJob {})?)
            .await?;
        storage
            .enqueue(JobEnvelope::new(dynamic_queue.clone(), TestJob {})?)
            .await?;
        storage
            .enqueue(JobEnvelope::new(dynamic_queue.clone(), TestJob {})?)
            .await?;
        storage
            .enqueue(JobEnvelope::new(dynamic_queue.clone(), TestJob {})?)
            .await?;

        let reported_lengths = storage
            .refresh_queue_length_stats(&[static_queue.clone(), dynamic_queue.clone()])
            .await?;
        assert_eq!(reported_lengths.get(&static_queue), Some(&2));
        assert_eq!(reported_lengths.get(&dynamic_queue), Some(&3));

        let mut redis = storage.connection().await?;
        let static_enqueued: i64 = (*redis)
            .hget(&storage.keys.stats, format!("{static_queue}:enqueued"))
            .await?;
        let static_refreshed_at: i64 = (*redis)
            .hget(&storage.keys.stats, format!("{static_queue}:enqueued_at"))
            .await?;
        assert_eq!(static_enqueued, 2);
        assert!(static_refreshed_at >= chrono::Utc::now().timestamp() - 3);

        let stats = storage.stats(std::time::Duration::from_secs(5)).await?;
        let static_stats = stats
            .queues
            .iter()
            .find(|queue| queue.key == static_queue)
            .expect("static queue stats should exist");
        assert_eq!(static_stats.enqueued, 2);

        let dynamic_stats = stats
            .queues
            .iter()
            .find(|queue| queue.key == dynamic_prefix)
            .expect("dynamic queue stats should exist");
        assert_eq!(dynamic_stats.enqueued, 3);
        assert_eq!(dynamic_stats.queues.len(), 1);
        let dynamic_queue_stats = dynamic_stats
            .queues
            .first()
            .expect("fast dynamic queue stats should exist");
        assert_eq!(dynamic_queue_stats.suffix, "fast");
        assert_eq!(dynamic_queue_stats.enqueued, 3);

        let queue_lengths = storage
            .queue_length_metrics(JobMetricsQuery::new(2))
            .await?;
        let static_series = queue_lengths
            .queues
            .iter()
            .find(|series| series.queue == static_queue)
            .expect("static queue length series should exist");
        assert_eq!(
            static_series
                .series
                .iter()
                .map(|point| point.enqueued)
                .max(),
            Some(2)
        );
        let dynamic_series = queue_lengths
            .queues
            .iter()
            .find(|series| series.queue == dynamic_queue)
            .expect("dynamic queue length series should exist");
        assert_eq!(
            dynamic_series
                .series
                .iter()
                .map(|point| point.enqueued)
                .max(),
            Some(3)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_use_fresh_queue_length_snapshots() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let static_queue = random_string();
        let dynamic_prefix = random_string();
        let dynamic_queue_a = format!("{dynamic_prefix}#a");
        let dynamic_queue_b = format!("{dynamic_prefix}#b");
        let refreshed_at = chrono::Utc::now().timestamp();

        let mut redis = storage.connection().await?;
        let _: () = redis::pipe()
            .hset(&storage.keys.stats, format!("{static_queue}:enqueued"), 42)
            .hset(
                &storage.keys.stats,
                format!("{static_queue}:enqueued_at"),
                refreshed_at,
            )
            .hset(
                &storage.keys.stats,
                format!("{dynamic_queue_a}:enqueued"),
                3,
            )
            .hset(
                &storage.keys.stats,
                format!("{dynamic_queue_a}:enqueued_at"),
                refreshed_at,
            )
            .hset(
                &storage.keys.stats,
                format!("{dynamic_queue_b}:enqueued"),
                4,
            )
            .hset(
                &storage.keys.stats,
                format!("{dynamic_queue_b}:enqueued_at"),
                refreshed_at,
            )
            .query_async(&mut redis)
            .await?;

        let stats = storage.stats(std::time::Duration::from_secs(5)).await?;
        assert_eq!(stats.global.enqueued, 49);

        let static_stats = stats
            .queues
            .iter()
            .find(|queue| queue.key == static_queue)
            .expect("static queue stats should exist");
        assert_eq!(static_stats.enqueued, 42);

        let dynamic_stats = stats
            .queues
            .iter()
            .find(|queue| queue.key == dynamic_prefix)
            .expect("dynamic queue stats should exist");
        assert_eq!(dynamic_stats.enqueued, 7);
        assert_eq!(dynamic_stats.queues.len(), 2);
        let first_dynamic_queue = dynamic_stats
            .queues
            .first()
            .expect("first dynamic queue stats should exist");
        assert_eq!(first_dynamic_queue.suffix, "a");
        assert_eq!(first_dynamic_queue.enqueued, 3);
        let second_dynamic_queue = dynamic_stats
            .queues
            .get(1)
            .expect("second dynamic queue stats should exist");
        assert_eq!(second_dynamic_queue.suffix, "b");
        assert_eq!(second_dynamic_queue.enqueued, 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_ignore_zero_queue_length_snapshots_without_other_stats() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let static_queue = random_string();
        let dynamic_prefix = random_string();
        let dynamic_queue = format!("{dynamic_prefix}#empty");
        let refreshed_at = chrono::Utc::now().timestamp();

        let mut redis = storage.connection().await?;
        let _: () = redis::pipe()
            .hset(&storage.keys.stats, format!("{static_queue}:enqueued"), 0)
            .hset(
                &storage.keys.stats,
                format!("{static_queue}:enqueued_at"),
                refreshed_at,
            )
            .hset(&storage.keys.stats, format!("{dynamic_queue}:enqueued"), 0)
            .hset(
                &storage.keys.stats,
                format!("{dynamic_queue}:enqueued_at"),
                refreshed_at,
            )
            .query_async(&mut redis)
            .await?;

        let stats = storage.stats(std::time::Duration::from_secs(5)).await?;

        assert_eq!(stats.global.enqueued, 0);
        assert!(
            !stats
                .queues
                .iter()
                .any(|queue_stats| queue_stats.key == static_queue)
        );
        assert!(
            !stats
                .queues
                .iter()
                .any(|queue_stats| queue_stats.key == dynamic_prefix)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_stats_use_live_llen_when_queue_length_snapshot_is_stale() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        storage
            .enqueue(JobEnvelope::new(queue.clone(), TestJob {})?)
            .await?;
        storage
            .enqueue(JobEnvelope::new(queue.clone(), TestJob {})?)
            .await?;

        let stale_refreshed_at =
            chrono::Utc::now().timestamp() - QUEUE_LENGTH_SNAPSHOT_TTL_SECS - 1;
        let mut redis = storage.connection().await?;
        let _: () = redis::pipe()
            .hset(&storage.keys.stats, format!("{queue}:enqueued"), 42)
            .hset(
                &storage.keys.stats,
                format!("{queue}:enqueued_at"),
                stale_refreshed_at,
            )
            .query_async(&mut redis)
            .await?;

        let stats = storage.stats(std::time::Duration::from_secs(5)).await?;
        let queue_stats = stats
            .queues
            .iter()
            .find(|queue_stats| queue_stats.key == queue)
            .expect("queue stats should exist");
        assert_eq!(queue_stats.enqueued, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_many_with_missing_jobs() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let envelope1 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let envelope2 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let envelope3 = JobEnvelope::new(queue.clone(), TestJob {})?;

        storage.enqueue(envelope1.clone()).await?;
        storage.enqueue(envelope2.clone()).await?;
        storage.enqueue(envelope3.clone()).await?;

        storage.delete_job(&envelope2.id).await?;

        let job_ids = vec![
            envelope1.id.clone(),
            envelope2.id.clone(),
            envelope3.id.clone(),
        ];
        let envelopes = storage.get_many(&job_ids).await?;

        assert_eq!(envelopes.len(), 2);

        let returned_ids: Vec<String> = envelopes.iter().map(|e| e.id.clone()).collect();
        assert!(returned_ids.contains(&envelope1.id));
        assert!(!returned_ids.contains(&envelope2.id));
        assert!(returned_ids.contains(&envelope3.id));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_job_cleans_up_corrupt_job_memberships() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope = JobEnvelope::new(queue.clone(), TestJob {})?;
        let job_id = envelope.id.clone();

        storage.enqueue(envelope).await?;

        let mut redis = storage.connection().await?;
        let _: () = redis::pipe()
            .hset(&storage.keys.jobs, &job_id, "{invalid json")
            .zadd(&storage.keys.schedule, &job_id, 1)
            .zadd(&storage.keys.retry, &job_id, 1)
            .lpush(storage.current_processing_queue(), &job_id)
            .query_async(&mut redis)
            .await?;

        storage.delete_job(&job_id).await?;

        let payload_exists: bool = redis.hexists(&storage.keys.jobs, &job_id).await?;
        assert!(!payload_exists);
        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert_eq!(storage.scheduled_count().await?, 0);
        assert_eq!(storage.retries_count().await?, 0);
        assert!(storage.currently_processing_job_ids().await?.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_list_queue_jobs() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let envelope1 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let envelope2 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let envelope3 = JobEnvelope::new(queue.clone(), TestJob {})?;

        storage.enqueue(envelope1.clone()).await?;
        storage.enqueue(envelope2.clone()).await?;
        storage.enqueue(envelope3.clone()).await?;

        let opts = QueueListOpts {
            count: 10,
            offset: 0,
        };
        let jobs = storage.list_queue_jobs(&queue, &opts).await?;
        assert_eq!(jobs.len(), 3);

        let opts = QueueListOpts {
            count: 2,
            offset: 0,
        };
        let jobs = storage.list_queue_jobs(&queue, &opts).await?;
        assert_eq!(jobs.len(), 2);

        let opts = QueueListOpts {
            count: 10,
            offset: 1,
        };
        let jobs = storage.list_queue_jobs(&queue, &opts).await?;
        assert_eq!(jobs.len(), 2);

        let opts = QueueListOpts {
            count: 10,
            offset: 5,
        };
        let jobs = storage.list_queue_jobs(&queue, &opts).await?;
        assert!(jobs.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_wipe_queue() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let envelope1 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let envelope2 = JobEnvelope::new(queue.clone(), TestJob {})?;

        storage.enqueue(envelope1.clone()).await?;
        storage.enqueue(envelope2.clone()).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 2);
        assert!(storage.get_job(&envelope1.id).await?.is_some());
        assert!(storage.get_job(&envelope2.id).await?.is_some());

        storage.wipe_queue(&queue).await?;

        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert!(storage.get_job(&envelope1.id).await?.is_none());
        assert!(storage.get_job(&envelope2.id).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_wipe_dead() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope1 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let envelope2 = JobEnvelope::new(queue, TestJob {})?;

        storage.kill(&envelope1, "first failed".to_string()).await?;
        storage
            .kill(&envelope2, "second failed".to_string())
            .await?;

        assert_eq!(storage.dead_count().await?, 2);

        storage.wipe_dead().await?;

        assert_eq!(storage.dead_count().await?, 0);
        assert!(
            storage
                .list_dead(&QueueListOpts {
                    count: 10,
                    offset: 0,
                })
                .await?
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_revive_all_dead() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope1 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let envelope2 = JobEnvelope::new(queue.clone(), TestJob {})?;

        storage.kill(&envelope1, "first failed".to_string()).await?;
        storage
            .kill(&envelope2, "second failed".to_string())
            .await?;

        assert_eq!(storage.dead_count().await?, 2);
        assert_eq!(storage.enqueued_count(&queue).await?, 0);

        let revived = storage.revive_all_dead().await?;

        assert_eq!(revived, 2);
        assert_eq!(storage.dead_count().await?, 0);
        assert_eq!(storage.enqueued_count(&queue).await?, 2);
        assert!(storage.get_job(&envelope1.id).await?.is_some());
        assert!(storage.get_job(&envelope2.id).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_claimed_dead_entries_skips_already_revived_entries() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let revived = JobEnvelope::new(queue, TestJob {})?;
        let remaining = JobEnvelope::new(random_string(), TestJob {})?;
        let revived_entry = serde_json::to_string(&revived)?;
        let remaining_entry = serde_json::to_string(&remaining)?;
        let claim_key = format!("{}:reviving:{}", storage.keys.dead, uuid::Uuid::new_v4());

        let mut redis = storage.connection().await?;
        let _: () = (*redis)
            .rpush(&claim_key, &[&revived_entry, &remaining_entry])
            .await?;
        drop(redis);

        storage
            .restore_claimed_dead_entries(&claim_key, vec![remaining_entry.clone()])
            .await?;

        let mut redis = storage.connection().await?;
        let claim_exists: bool = (*redis).exists(&claim_key).await?;
        let restored: Vec<String> = (*redis).lrange(&storage.keys.dead, 0, -1).await?;
        drop(redis);

        assert!(!claim_exists);
        assert_eq!(restored, vec![remaining_entry]);
        assert!(!restored.contains(&revived_entry));

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_all_now_enqueues_all_pending_retries() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope1 = JobEnvelope::new(queue.clone(), TestJob {})?;
        let envelope2 = JobEnvelope::new(queue.clone(), TestJob {})?;

        storage.enqueue(envelope1.clone()).await?;
        storage.enqueue(envelope2.clone()).await?;
        assert_eq!(storage.dequeue(&queue).await?, Some(envelope1.id.clone()));
        assert_eq!(storage.dequeue(&queue).await?, Some(envelope2.id.clone()));
        storage.finish_with_failure(&envelope1).await?;
        storage.finish_with_failure(&envelope2).await?;
        storage
            .retry_in(envelope1.id.clone(), 3_600, "first failed".to_string())
            .await?;
        storage
            .retry_in(envelope2.id.clone(), 3_600, "second failed".to_string())
            .await?;

        assert_eq!(storage.retries_count().await?, 2);
        assert_eq!(storage.enqueued_count(&queue).await?, 0);

        let retried = storage.retry_all_now().await?;

        assert_eq!(retried, 2);
        assert_eq!(storage.retries_count().await?, 0);
        assert_eq!(storage.enqueued_count(&queue).await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_started_at_batch() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let mut envelopes = vec![
            JobEnvelope::new(queue.clone(), TestJob {})?,
            JobEnvelope::new(queue.clone(), TestJob {})?,
        ];
        for envelope in &envelopes {
            assert!(envelope.meta.started_at.is_none());
            storage.enqueue(envelope.clone()).await?;
        }

        let before = chrono::Utc::now().timestamp_micros();
        storage.set_started_at_batch(&mut envelopes).await?;
        let after = chrono::Utc::now().timestamp_micros();

        let started_at = envelopes
            .first()
            .expect("batch should contain an envelope")
            .meta
            .started_at
            .expect("started_at should be set");
        assert!(started_at >= before);
        assert!(started_at <= after);

        for envelope in &envelopes {
            assert_eq!(envelope.meta.started_at, Some(started_at));
            let persisted = storage
                .get_job(&envelope.id)
                .await?
                .expect("job should exist");
            assert_eq!(persisted.meta.started_at, Some(started_at));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_finish_with_success_batch() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let envelopes = vec![
            JobEnvelope::new(queue.clone(), TestJob {})?,
            JobEnvelope::new(queue.clone(), TestJob {})?,
        ];
        for envelope in &envelopes {
            storage.enqueue(envelope.clone()).await?;
        }

        for envelope in &envelopes {
            assert_eq!(storage.dequeue(&queue).await?, Some(envelope.id.clone()));
        }

        let mut redis = storage.connection().await?;
        let processing_before: Vec<JobId> = (*redis)
            .lrange(storage.current_processing_queue(), 0, -1)
            .await?;
        assert_eq!(processing_before.len(), 2);
        drop(redis);

        storage.finish_with_success_batch(&envelopes).await?;

        for envelope in &envelopes {
            assert!(storage.get_job(&envelope.id).await?.is_none());
        }
        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert_eq!(storage.jobs_count().await?, 0);

        let mut redis = storage.connection().await?;
        let processing_after: Vec<JobId> = (*redis)
            .lrange(storage.current_processing_queue(), 0, -1)
            .await?;
        assert!(processing_after.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_envelope() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let now = chrono::Utc::now().timestamp_micros();
        let id = uuid::Uuid::new_v4().to_string();
        let envelope = JobEnvelope {
            id: id.clone(),
            queue: queue.clone(),
            job: crate::job_envelope::JobData {
                name: "MyWorker".to_string(),
                args: serde_json::json!({"key": "value"}),
            },
            meta: crate::job_envelope::JobMeta {
                id: id.clone(),
                retries: 0,
                unique: false,
                on_conflict: None,
                created_at: now,
                scheduled_at: now,
                started_at: None,
                state: None,
                resurrect: true,
                error: None,
                throttle_cost: None,
            },
        };

        let returned_id = storage.enqueue(envelope).await?;
        assert_eq!(returned_id, id);
        assert_eq!(storage.enqueued_count(&queue).await?, 1);

        let job = storage.get_job(&id).await?.expect("job should exist");
        assert_eq!(job.queue, queue);
        assert_eq!(job.job.name, "MyWorker");
        assert_eq!(job.job.args, serde_json::json!({"key": "value"}));
        assert_eq!(job.meta.retries, 0);
        assert!(job.meta.error.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_in_envelope() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let now = chrono::Utc::now().timestamp_micros();
        let id = uuid::Uuid::new_v4().to_string();
        let envelope = JobEnvelope {
            id: id.clone(),
            queue: queue.clone(),
            job: crate::job_envelope::JobData {
                name: "MyWorker".to_string(),
                args: serde_json::json!({"key": "value"}),
            },
            meta: crate::job_envelope::JobMeta {
                id: id.clone(),
                retries: 0,
                unique: false,
                on_conflict: None,
                created_at: now,
                scheduled_at: now,
                started_at: None,
                state: None,
                resurrect: true,
                error: None,
                throttle_cost: None,
            },
        };
        let delay_s = rand::rng().random_range(1..3600);
        let delay = chrono::Duration::seconds(delay_s)
            .num_microseconds()
            .unwrap();

        let before = chrono::Utc::now().timestamp_micros();
        let returned_id = storage.enqueue_in(envelope, delay_s as u64).await?;
        let after = chrono::Utc::now().timestamp_micros();
        assert_eq!(returned_id, id);
        assert_eq!(storage.enqueued_count(&queue).await?, 0);
        assert_eq!(storage.scheduled_count().await?, 1);

        let job = storage.get_job(&id).await?.expect("job should exist");
        assert_eq!(job.queue, queue);
        assert_eq!(job.job.name, "MyWorker");
        assert_eq!(job.job.args, serde_json::json!({"key": "value"}));

        assert!(job.meta.scheduled_at >= (before + delay));
        assert!(job.meta.scheduled_at <= (after + delay));
        assert_eq!(job.meta.retries, 0);
        assert!(job.meta.error.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_at_unique_skip_keeps_existing_occurrence() -> TestResult {
        #[derive(Serialize)]
        struct UniqueTestJob;

        impl crate::worker::Job for UniqueTestJob {
            fn unique_id(&self) -> Option<String> {
                Some("singleton".to_string())
            }
        }

        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();
        let envelope = JobEnvelope::new(queue, UniqueTestJob)?;
        let first_at = chrono::Utc::now() + chrono::Duration::seconds(60);
        let second_at = first_at + chrono::Duration::seconds(60);

        storage
            .enqueue_at(envelope.clone().with_scheduled_at(first_at))
            .await?;
        storage
            .enqueue_at(envelope.clone().with_scheduled_at(second_at))
            .await?;

        assert_eq!(storage.scheduled_count().await?, 1);
        let job = storage
            .get_job(&envelope.id)
            .await?
            .expect("first occurrence should remain scheduled");
        assert_eq!(job.meta.scheduled_at, first_at.timestamp_micros());

        Ok(())
    }

    #[tokio::test]
    async fn test_enqueue_at_unique_replace_envelope() -> TestResult {
        let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
        let queue = random_string();

        let now = chrono::Utc::now().timestamp_micros();
        let id = uuid::Uuid::new_v4().to_string();
        let envelope = JobEnvelope {
            id: id.clone(),
            queue: queue.clone(),
            job: crate::job_envelope::JobData {
                name: "MyWorker".to_string(),
                args: serde_json::json!({"key": "value"}),
            },
            meta: crate::job_envelope::JobMeta {
                id: id.clone(),
                retries: 0,
                unique: true,
                on_conflict: Some(JobConflictStrategy::Replace),
                created_at: now,
                scheduled_at: now,
                started_at: None,
                state: None,
                resurrect: true,
                error: None,
                throttle_cost: None,
            },
        };

        let first_at = chrono::Utc::now() + chrono::Duration::seconds(60);
        let second_at = first_at + chrono::Duration::seconds(60);

        storage
            .enqueue_at(envelope.clone().with_scheduled_at(first_at))
            .await?;
        let returned_id = storage
            .enqueue_at(envelope.with_scheduled_at(second_at))
            .await?;

        assert_eq!(returned_id, id);
        assert_eq!(storage.scheduled_count().await?, 1);
        assert_eq!(storage.enqueued_count(&queue).await?, 0);

        let job = storage.get_job(&id).await?.expect("job should exist");
        assert_eq!(job.meta.scheduled_at, second_at.timestamp_micros());

        Ok(())
    }

    #[tokio::test]
    async fn test_envelope_new_scheduled() -> TestResult {
        let queue = random_string();
        let delay_s = rand::rng().random_range(1..3600);
        let scheduled_at = chrono::Utc::now() + chrono::Duration::seconds(delay_s);

        let envelope = JobEnvelope::new_scheduled(queue.clone(), TestJob {}, scheduled_at)?;

        assert_eq!(envelope.meta.scheduled_at, scheduled_at.timestamp_micros());

        Ok(())
    }
}
