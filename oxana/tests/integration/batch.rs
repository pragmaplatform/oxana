use crate::shared::*;
use deadpool_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
struct FooBatchJob {
    values_key: String,
    calls_key: String,
    value: String,
}

#[derive(oxana::Worker)]
#[oxana(context = WorkerState, error = WorkerError)]
#[oxana(registry = None)]
#[oxana(batch_size = 3, batch_timeout_ms = 100)]
struct FooBatchWorker {
    state: WorkerState,
}

impl FooBatchWorker {
    async fn process_batch(
        &self,
        jobs: Vec<oxana::BatchItem<FooBatchJob>>,
    ) -> Result<(), WorkerError> {
        if let Some(first) = jobs.first() {
            let mut redis = self.state.redis.get().await?;
            let values = jobs
                .iter()
                .map(|item| item.job.value.clone())
                .collect::<Vec<_>>();
            let _: usize = redis.rpush(&first.job.values_key, values).await?;
            let _: i64 = redis.incr(&first.job.calls_key, 1).await?;
            let _: bool = redis.expire(&first.job.values_key, 3).await?;
            let _: bool = redis.expire(&first.job.calls_key, 3).await?;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
struct ContextBatchJob {
    seen_key: String,
    value: String,
}

#[derive(oxana::Worker)]
#[oxana(context = WorkerState, error = WorkerError)]
#[oxana(registry = None)]
#[oxana(batch_size = 2, batch_timeout_ms = 100)]
struct ContextBatchWorker {
    state: WorkerState,
}

impl ContextBatchWorker {
    async fn process_batch(
        &self,
        jobs: Vec<oxana::BatchItem<ContextBatchJob>>,
    ) -> Result<(), WorkerError> {
        let mut redis = self.state.redis.get().await?;
        for oxana::BatchItem { job, ctx } in jobs {
            let _: usize = redis.hset(&job.seen_key, job.value, ctx.meta.id).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
struct RetryBatchJob {}

#[derive(oxana::Worker)]
#[oxana(context = WorkerState, error = WorkerError)]
#[oxana(registry = None)]
#[oxana(max_retries = 1, retry_delay = 0)]
#[oxana(batch_size = 2, batch_timeout_ms = 100)]
struct RetryBatchWorker;

impl RetryBatchWorker {
    async fn process_batch(
        &self,
        jobs: Vec<oxana::BatchItem<RetryBatchJob>>,
    ) -> Result<(), WorkerError> {
        if jobs.iter().any(|item| item.ctx.meta.retries == 0) {
            return Err(WorkerError::Generic("retry batch once".to_string()));
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
struct PanicBatchJob {}

#[derive(oxana::Worker)]
#[oxana(context = WorkerState, error = WorkerError)]
#[oxana(registry = None)]
#[oxana(max_retries = 0)]
#[oxana(batch_size = 2, batch_timeout_ms = 100)]
struct PanicBatchWorker;

impl PanicBatchWorker {
    async fn process_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<PanicBatchJob>>,
    ) -> Result<(), WorkerError> {
        panic!("batch panic");
    }
}

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
struct SizingBatchJob {
    sizes_key: String,
}

#[derive(oxana::Worker)]
#[oxana(context = WorkerState, error = WorkerError)]
#[oxana(registry = None)]
#[oxana(batch_size = 2, batch_timeout_ms = 100)]
struct SizingBatchWorker {
    state: WorkerState,
}

impl SizingBatchWorker {
    async fn process_batch(
        &self,
        jobs: Vec<oxana::BatchItem<SizingBatchJob>>,
    ) -> Result<(), WorkerError> {
        if let Some(first) = jobs.first() {
            let mut redis = self.state.redis.get().await?;
            let _: usize = redis.rpush(&first.job.sizes_key, jobs.len()).await?;
            let _: bool = redis.expire(&first.job.sizes_key, 3).await?;
        }

        Ok(())
    }
}

#[derive(Serialize)]
struct QueueBatch;

impl oxana::Queue for QueueBatch {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("batch").concurrency(3)
    }
}

#[derive(Serialize)]
struct QueueBatchHighConcurrency;

impl oxana::Queue for QueueBatchHighConcurrency {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("batch_high_concurrency").concurrency(5)
    }
}

#[tokio::test]
pub async fn test_batch_worker_processes_full_batch_once() -> TestResult {
    let redis_pool = setup();
    let mut redis_conn = redis_pool.get().await?;

    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueBatch>()
        .worker::<FooBatchWorker, FooBatchJob>()
        .exit_when_processed(3);

    let values_key = uuid::Uuid::new_v4().to_string();
    let calls_key = uuid::Uuid::new_v4().to_string();

    for value in ["a", "b", "c"] {
        storage
            .enqueue(
                QueueBatch,
                FooBatchJob {
                    values_key: values_key.clone(),
                    calls_key: calls_key.clone(),
                    value: value.to_string(),
                },
            )
            .await?;
    }

    runtime.run().await?;

    let calls: Option<i64> = redis_conn.get(calls_key).await?;
    let values: Vec<String> = redis_conn.lrange(values_key, 0, -1).await?;

    assert_eq!(calls, Some(1));
    assert_eq!(values.len(), 3);

    Ok(())
}

#[tokio::test]
pub async fn test_batch_worker_flushes_partial_batch_after_timeout() -> TestResult {
    let redis_pool = setup();
    let mut redis_conn = redis_pool.get().await?;

    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueBatch>()
        .worker::<FooBatchWorker, FooBatchJob>()
        .exit_when_processed(2);

    let values_key = uuid::Uuid::new_v4().to_string();
    let calls_key = uuid::Uuid::new_v4().to_string();

    for value in ["a", "b"] {
        storage
            .enqueue(
                QueueBatch,
                FooBatchJob {
                    values_key: values_key.clone(),
                    calls_key: calls_key.clone(),
                    value: value.to_string(),
                },
            )
            .await?;
    }

    runtime.run().await?;

    let calls: Option<i64> = redis_conn.get(calls_key).await?;
    let values: Vec<String> = redis_conn.lrange(values_key, 0, -1).await?;

    assert_eq!(calls, Some(1));
    assert_eq!(values.len(), 2);

    Ok(())
}

#[tokio::test]
pub async fn test_batch_worker_kills_only_invalid_jobs_in_mixed_batch() -> TestResult {
    let redis_pool = setup();
    let mut redis_conn = redis_pool.get().await?;

    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueBatch>()
        .worker::<FooBatchWorker, FooBatchJob>()
        .exit_when_processed(2);

    let values_key = uuid::Uuid::new_v4().to_string();
    let calls_key = uuid::Uuid::new_v4().to_string();

    storage
        .enqueue_envelope(invalid_batch_envelope(
            values_key.clone(),
            calls_key.clone(),
        ))
        .await?;

    for value in ["a", "b"] {
        storage
            .enqueue(
                QueueBatch,
                FooBatchJob {
                    values_key: values_key.clone(),
                    calls_key: calls_key.clone(),
                    value: value.to_string(),
                },
            )
            .await?;
    }

    runtime.run().await?;

    let calls: Option<i64> = redis_conn.get(calls_key).await?;
    let values: Vec<String> = redis_conn.lrange(values_key, 0, -1).await?;
    let dead = storage
        .list_dead(&oxana::QueueListOpts {
            count: 10,
            offset: 0,
        })
        .await?;

    assert_eq!(calls, Some(1));
    assert_eq!(values.len(), 2);
    assert_eq!(dead.len(), 1);

    Ok(())
}

#[tokio::test]
pub async fn test_batch_worker_retries_each_job_after_batch_error() -> TestResult {
    let redis_pool = setup();
    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueBatch>()
        .worker::<RetryBatchWorker, RetryBatchJob>()
        .exit_when_processed(4);

    storage.enqueue(QueueBatch, RetryBatchJob {}).await?;
    storage.enqueue(QueueBatch, RetryBatchJob {}).await?;

    let stats = runtime.run().await?;

    assert_eq!(stats.processed, 4);
    assert_eq!(stats.failed, 2);
    assert_eq!(stats.succeeded, 2);
    assert_eq!(stats.panicked, 0);
    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}

#[tokio::test]
pub async fn test_batch_worker_panic_marks_each_job_panicked() -> TestResult {
    let redis_pool = setup();
    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueBatch>()
        .worker::<PanicBatchWorker, PanicBatchJob>()
        .exit_when_processed(2);

    storage.enqueue(QueueBatch, PanicBatchJob {}).await?;
    storage.enqueue(QueueBatch, PanicBatchJob {}).await?;

    let stats = runtime.run().await?;

    assert_eq!(stats.processed, 2);
    assert_eq!(stats.failed, 2);
    assert_eq!(stats.panicked, 2);
    assert_eq!(stats.succeeded, 0);
    assert_eq!(storage.dead_count().await?, 2);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}

#[tokio::test]
pub async fn test_batch_worker_never_exceeds_batch_size_with_higher_queue_concurrency() -> TestResult
{
    let redis_pool = setup();
    let mut redis_conn = redis_pool.get().await?;

    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueBatchHighConcurrency>()
        .worker::<SizingBatchWorker, SizingBatchJob>()
        .exit_when_processed(5);

    let sizes_key = uuid::Uuid::new_v4().to_string();
    for _ in 0..5 {
        storage
            .enqueue(
                QueueBatchHighConcurrency,
                SizingBatchJob {
                    sizes_key: sizes_key.clone(),
                },
            )
            .await?;
    }

    runtime.run().await?;

    let sizes: Vec<usize> = redis_conn.lrange(sizes_key, 0, -1).await?;

    assert!(sizes.len() > 1);
    assert_eq!(sizes.iter().sum::<usize>(), 5);
    assert!(sizes.iter().all(|size| *size <= 2));

    Ok(())
}

#[tokio::test]
pub async fn test_batch_worker_kills_all_invalid_jobs_without_processing() -> TestResult {
    let redis_pool = setup();

    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueBatch>()
        .worker::<FooBatchWorker, FooBatchJob>()
        .shutdown_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            Ok(())
        });

    let values_key = uuid::Uuid::new_v4().to_string();
    let calls_key = uuid::Uuid::new_v4().to_string();

    for _ in 0..3 {
        storage
            .enqueue_envelope(invalid_batch_envelope(
                values_key.clone(),
                calls_key.clone(),
            ))
            .await?;
    }

    let stats = runtime.run().await?;

    assert_eq!(stats.processed, 0);
    assert_eq!(storage.dead_count().await?, 3);
    assert_eq!(storage.enqueued_count(QueueBatch).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}

#[tokio::test]
pub async fn test_batch_worker_receives_context_per_job() -> TestResult {
    let redis_pool = setup();
    let mut redis_conn = redis_pool.get().await?;

    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueBatch>()
        .worker::<ContextBatchWorker, ContextBatchJob>()
        .exit_when_processed(2);

    let seen_key = uuid::Uuid::new_v4().to_string();
    let first_id = storage
        .enqueue(
            QueueBatch,
            ContextBatchJob {
                seen_key: seen_key.clone(),
                value: "first".to_string(),
            },
        )
        .await?;
    let second_id = storage
        .enqueue(
            QueueBatch,
            ContextBatchJob {
                seen_key: seen_key.clone(),
                value: "second".to_string(),
            },
        )
        .await?;

    runtime.run().await?;

    let first_seen: Option<String> = redis_conn.hget(&seen_key, "first").await?;
    let second_seen: Option<String> = redis_conn.hget(&seen_key, "second").await?;

    assert_eq!(first_seen, Some(first_id));
    assert_eq!(second_seen, Some(second_id));

    Ok(())
}

fn invalid_batch_envelope(values_key: String, calls_key: String) -> oxana::JobEnvelope {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp_micros();

    oxana::JobEnvelope {
        id: id.clone(),
        queue: "batch".to_string(),
        job: oxana::JobData {
            name: std::any::type_name::<FooBatchJob>().to_string(),
            args: serde_json::json!({
                "values_key": values_key,
                "calls_key": calls_key
            }),
        },
        meta: oxana::JobMeta {
            id,
            on_demand: None,
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
    }
}
