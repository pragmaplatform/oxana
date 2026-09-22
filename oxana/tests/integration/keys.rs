use crate::shared::*;
use deadpool_redis::redis::AsyncCommands;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use testresult::TestResult;

#[derive(Serialize)]
struct ThrottledQueue;

impl oxana::Queue for ThrottledQueue {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("layout_throttled").throttle(oxana::QueueThrottle {
            window_ms: 60_000,
            limit: 100,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LayoutJob {
    fail: bool,
}

impl oxana::Job for LayoutJob {}

#[derive(Clone)]
struct LayoutState {
    redis: deadpool_redis::Pool,
    pattern: String,
    seen: Arc<Mutex<BTreeSet<String>>>,
}

struct LayoutWorker(LayoutState);

impl oxana::FromContext<LayoutState> for LayoutWorker {
    fn from_context(ctx: &LayoutState) -> Self {
        Self(ctx.clone())
    }
}

#[async_trait::async_trait]
impl oxana::Worker<LayoutJob> for LayoutWorker {
    type Error = WorkerError;

    async fn process(&self, job: LayoutJob, _ctx: &oxana::JobContext) -> Result<(), WorkerError> {
        // Snapshot the keyspace while this job is held in a processing list.
        self.0.snapshot().await?;
        if job.fail {
            return Err(WorkerError::Generic("expected failure".to_string()));
        }
        Ok(())
    }

    fn max_retries(&self, _job: &LayoutJob) -> u32 {
        1
    }

    fn retry_delay(&self, _job: &LayoutJob, _retries: u32) -> u64 {
        1
    }
}

impl LayoutState {
    async fn snapshot(&self) -> Result<(), WorkerError> {
        let keys = scan(&self.redis, &self.pattern).await?;
        self.seen.lock().unwrap().extend(keys);
        Ok(())
    }
}

async fn scan(pool: &deadpool_redis::Pool, pattern: &str) -> Result<Vec<String>, WorkerError> {
    let mut redis = pool.get().await?;
    let iter = redis
        .scan_options::<String>(
            deadpool_redis::redis::ScanOptions::default()
                .with_pattern(pattern)
                .with_count(1000),
        )
        .await?;
    Ok(iter.try_collect().await?)
}

#[tokio::test]
async fn test_custom_key_layout_is_used_by_every_code_path() -> TestResult {
    let redis_pool = setup();
    let namespace = format!("ns{}", random_string());
    let prefix = format!("custom-{}", random_string());
    let keys = oxana::StorageKeys::new(namespace.clone())
        .with_jobs(format!("{prefix}:j"))
        .with_dead(format!("{prefix}:d"))
        .with_schedule(format!("{prefix}:s"))
        .with_retry(format!("{prefix}:r"))
        .with_queue_prefix(format!("{prefix}:q"))
        .with_processing_prefix(format!("{prefix}:p"))
        .with_processes(format!("{prefix}:procs"))
        .with_processes_data(format!("{prefix}:procdata"))
        .with_stats(format!("{prefix}:st"))
        .with_queue_configs(format!("{prefix}:qc"))
        .with_metrics_prefix(format!("{prefix}:m"))
        .with_throttler_prefix(format!("{prefix}:t"));
    let expected_prefixes = [
        keys.jobs().to_string(),
        keys.dead().to_string(),
        keys.schedule().to_string(),
        keys.retry().to_string(),
        keys.queue_prefix().to_string(),
        keys.processing_prefix().to_string(),
        keys.processes().to_string(),
        keys.processes_data().to_string(),
        keys.stats().to_string(),
        keys.queue_configs().to_string(),
        keys.metrics_prefix().to_string(),
        keys.throttler_prefix().to_string(),
    ];
    let storage = oxana::Storage::builder()
        .keys(keys)
        .build_from_pool(redis_pool.clone())?;
    assert_eq!(storage.namespace(), namespace);
    let state = LayoutState {
        redis: redis_pool.clone(),
        pattern: format!("{prefix}:*"),
        seen: Arc::new(Mutex::new(BTreeSet::new())),
    };

    // Enqueue, schedule, and a runtime queue config, before anything runs.
    storage
        .enqueue(ThrottledQueue, LayoutJob { fail: false })
        .await?;
    storage
        .enqueue(ThrottledQueue, LayoutJob { fail: true })
        .await?;
    storage
        .enqueue_in(ThrottledQueue, LayoutJob { fail: false }, 3600)
        .await?;
    storage
        .set_queue_config(ThrottledQueue, &oxana::QueueRuntimeConfig::default())
        .await?;
    assert_eq!(storage.enqueued_count(ThrottledQueue).await?, 2);
    assert_eq!(storage.scheduled_count().await?, 1);
    assert_eq!(storage.jobs_count().await?, 3);
    state.snapshot().await?;

    // Dequeue, processing lists, processes, throttler, retry, dead, stats,
    // metrics: the failing job runs twice before it is killed.
    let runtime = storage
        .runtime(state.clone())
        .queue::<ThrottledQueue>()
        .worker::<LayoutWorker, LayoutJob>()
        .dequeue_timeout(std::time::Duration::from_millis(25))
        .exit_when_processed(3);
    let mut runner = tokio::spawn(runtime.run());
    // Keep scanning while it runs, so short-lived keys (the retry set) are seen.
    let stats = loop {
        state.snapshot().await?;
        match tokio::time::timeout(std::time::Duration::from_millis(50), &mut runner).await {
            Ok(stats) => break stats??,
            Err(_) => continue,
        }
    };
    assert_eq!(stats.processed, 3);
    assert_eq!(storage.dead_count().await?, 1);
    state.snapshot().await?;
    let list = oxana::QueueListOpts {
        count: 10,
        offset: 0,
    };
    assert_eq!(storage.list_dead(&list).await?.len(), 1);
    assert_eq!(storage.list_scheduled(&list).await?.len(), 1);
    assert_eq!(storage.stats().await?.queues.len(), 1);
    assert!(
        storage
            .stats_queues()
            .await?
            .iter()
            .any(|q| q.processed == 3)
    );
    assert!(storage.queue_config(ThrottledQueue).await?.is_some());
    assert!(
        storage
            .job_metrics(oxana::JobMetricsQuery::new(5))
            .await?
            .totals
            .processed
            >= 3
    );

    // Every structure was written under its own key.
    let seen = state.seen.lock().unwrap().clone();
    for expected in &expected_prefixes {
        assert!(
            seen.iter()
                .any(|key| key == expected || key.starts_with(&format!("{expected}:"))),
            "no key observed under {expected}: {seen:?}"
        );
    }
    // And nothing was written under the namespace or the default throttler prefix.
    assert!(
        scan(&redis_pool, &format!("{namespace}*"))
            .await?
            .is_empty(),
        "keys built outside StorageKeys"
    );
    assert!(
        scan(&redis_pool, "oxana:throttler:*layout_throttled*")
            .await?
            .is_empty(),
        "throttler escaped the custom layout"
    );

    Ok(())
}

#[tokio::test]
async fn test_default_key_layout_matches_builder_namespace() -> TestResult {
    let redis_pool = setup();
    let namespace = random_string();
    let keys = oxana::StorageKeys::new(namespace.clone());
    let storage = oxana::Storage::builder()
        .keys(keys.clone())
        .build_from_pool(redis_pool.clone())?;
    storage.enqueue(QueueOne, WorkerNoopJob {}).await?;

    let mut redis = redis_pool.get().await?;
    let filed: i64 = redis.hlen(format!("{namespace}:jobs")).await?;
    assert_eq!(filed, 1);
    let queued: i64 = redis.llen(format!("{namespace}:queue:one")).await?;
    assert_eq!(queued, 1);
    assert_eq!(keys.queue_prefix(), format!("{namespace}:queue"));
    assert_eq!(keys.throttler_prefix(), "oxana:throttler");

    Ok(())
}
