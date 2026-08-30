use crate::shared::*;
use deadpool_redis::redis::AsyncCommands;
use oxana::Queue as _;
use std::sync::Arc;
use std::time::Duration;
use testresult::TestResult;
use tokio::sync::{Notify, oneshot};

#[derive(serde::Serialize)]
struct QueueTwo;

impl oxana::Queue for QueueTwo {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("two")
    }
}

#[derive(serde::Serialize)]
struct DynamicConcurrencyQueue;

impl oxana::Queue for DynamicConcurrencyQueue {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("dynamic_concurrency").dynamic_concurrency(3)
    }
}

#[derive(serde::Serialize)]
struct DynamicTenantBase;

impl oxana::Queue for DynamicTenantBase {
    fn key(&self) -> String {
        "tenant".to_string()
    }

    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_dynamic("tenant").dynamic_concurrency(3)
    }
}

#[derive(serde::Serialize)]
struct DynamicTenantQueue {
    tenant: String,
}

impl oxana::Queue for DynamicTenantQueue {
    fn key(&self) -> String {
        format!(
            "tenant#{}",
            oxana::value_to_queue_key(serde_json::to_value(self).unwrap_or_default())
        )
    }

    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_dynamic("tenant").dynamic_concurrency(3)
    }
}

#[derive(Clone)]
struct ShutdownDrainState {
    started: Arc<Notify>,
    finished: Arc<Notify>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct ShutdownProgressJob;

impl oxana::Job for ShutdownProgressJob {
    fn should_resurrect() -> bool {
        false
    }
}

struct ShutdownProgressWorker {
    state: ShutdownDrainState,
}

impl oxana::FromContext<ShutdownDrainState> for ShutdownProgressWorker {
    fn from_context(ctx: &ShutdownDrainState) -> Self {
        Self { state: ctx.clone() }
    }
}

#[async_trait::async_trait]
impl oxana::Worker<ShutdownProgressJob> for ShutdownProgressWorker {
    type Error = oxana::OxanaError;

    async fn run_batch(
        &self,
        jobs: Vec<oxana::BatchItem<ShutdownProgressJob>>,
    ) -> Result<(), oxana::OxanaError> {
        let job = jobs
            .into_iter()
            .next()
            .expect("shutdown progress worker receives one job");
        self.state.started.notify_one();
        tokio::time::sleep(Duration::from_millis(8500)).await;
        job.ctx.state.update_progress((1, 1)).await?;
        self.state.finished.notify_one();
        Ok(())
    }

    fn max_retries(&self, _job: &ShutdownProgressJob) -> u32 {
        0
    }
}

#[tokio::test]
pub async fn test_standard() -> TestResult {
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
        .queue::<QueueOne>()
        .worker::<WorkerRedisSet, WorkerRedisSetJob>()
        .exit_when_processed(1);

    let random_key = uuid::Uuid::new_v4().to_string();
    let random_value = uuid::Uuid::new_v4().to_string();

    storage
        .enqueue(
            QueueOne,
            WorkerRedisSetJob {
                key: random_key.clone(),
                value: random_value.clone(),
            },
        )
        .await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    runtime.run().await?;

    let value: Option<String> = redis_conn.get(random_key).await?;

    assert_eq!(value, Some(random_value));
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}

#[tokio::test]
pub async fn test_runtime_only_processes_selected_queues() -> TestResult {
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
        .queue::<QueueOne>()
        .queue::<QueueTwo>()
        .worker::<WorkerRedisSet, WorkerRedisSetJob>()
        .only_queue::<QueueOne>()
        .exit_when_processed(1);
    let selected_key = random_string();
    let excluded_key = random_string();

    storage
        .enqueue(
            QueueOne,
            WorkerRedisSetJob {
                key: selected_key.clone(),
                value: "selected".to_string(),
            },
        )
        .await?;
    storage
        .enqueue(
            QueueTwo,
            WorkerRedisSetJob {
                key: excluded_key.clone(),
                value: "excluded".to_string(),
            },
        )
        .await?;

    runtime.run().await?;

    let selected: Option<String> = redis_conn.get(selected_key).await?;
    let excluded: Option<String> = redis_conn.get(excluded_key).await?;
    assert_eq!(selected.as_deref(), Some("selected"));
    assert_eq!(excluded, None);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.enqueued_count(QueueTwo).await?, 1);

    Ok(())
}

#[tokio::test]
pub async fn test_enqueue_list() -> TestResult {
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
        .queue::<QueueOne>()
        .worker::<WorkerRedisSet, WorkerRedisSetJob>()
        .exit_when_processed(3);

    let key1 = random_string();
    let key2 = random_string();
    let key3 = random_string();

    let job_ids = storage
        .enqueue_list(
            QueueOne,
            vec![
                WorkerRedisSetJob {
                    key: key1.clone(),
                    value: "first".to_string(),
                },
                WorkerRedisSetJob {
                    key: key2.clone(),
                    value: "second".to_string(),
                },
                WorkerRedisSetJob {
                    key: key3.clone(),
                    value: "third".to_string(),
                },
            ],
        )
        .await?;

    assert_eq!(job_ids.len(), 3);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 3);

    runtime.run().await?;

    let value1: Option<String> = redis_conn.get(key1).await?;
    let value2: Option<String> = redis_conn.get(key2).await?;
    let value3: Option<String> = redis_conn.get(key3).await?;

    assert_eq!(value1, Some("first".to_string()));
    assert_eq!(value2, Some("second".to_string()));
    assert_eq!(value3, Some("third".to_string()));
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}

#[tokio::test]
pub async fn test_shutdown_keeps_heartbeat_until_workers_finish() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let state = ShutdownDrainState {
        started: Arc::new(Notify::new()),
        finished: Arc::new(Notify::new()),
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let runtime = storage
        .runtime(state.clone())
        .queue::<QueueOne>()
        .worker::<ShutdownProgressWorker, ShutdownProgressJob>()
        .shutdown_on(async move {
            shutdown_rx
                .await
                .map_err(|_| std::io::Error::other("shutdown sender dropped"))
        });
    let job_id = storage.enqueue(QueueOne, ShutdownProgressJob).await?;
    let old_worker = tokio::spawn(async move { runtime.run().await });

    state.started.notified().await;
    shutdown_tx
        .send(())
        .expect("old worker shutdown receiver should be alive");

    tokio::time::sleep(Duration::from_secs(6)).await;

    let new_runtime = storage
        .runtime(state.clone())
        .queue::<QueueOne>()
        .worker::<ShutdownProgressWorker, ShutdownProgressJob>()
        .shutdown_on(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(())
        });
    let new_worker = tokio::spawn(async move { new_runtime.run().await });

    tokio::time::timeout(Duration::from_secs(5), new_worker).await???;
    tokio::time::timeout(Duration::from_secs(5), state.finished.notified()).await?;
    tokio::time::timeout(Duration::from_secs(5), old_worker).await???;

    assert!(storage.get_job(&job_id).await?.is_none());
    assert_eq!(storage.dead_count().await?, 0);

    Ok(())
}

#[tokio::test]
pub async fn test_paused_queue_resumes_after_runtime_config_update() -> TestResult {
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
        .queue::<QueueOne>()
        .worker::<WorkerRedisSet, WorkerRedisSetJob>()
        .exit_when_processed(1);
    storage
        .set_queue_state(QueueOne, oxana::QueueState::Paused)
        .await?;

    let random_key = uuid::Uuid::new_v4().to_string();
    let random_value = uuid::Uuid::new_v4().to_string();

    storage
        .enqueue(
            QueueOne,
            WorkerRedisSetJob {
                key: random_key.clone(),
                value: random_value.clone(),
            },
        )
        .await?;

    let handle = tokio::spawn(async move { runtime.run().await });

    tokio::time::sleep(Duration::from_millis(500)).await;
    let value: Option<String> = redis_conn.get(&random_key).await?;
    assert_eq!(value, None);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    storage.unpause_queue(QueueOne).await?;

    tokio::time::timeout(Duration::from_secs(5), handle).await???;
    let value: Option<String> = redis_conn.get(random_key).await?;

    assert_eq!(value, Some(random_value));
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);

    Ok(())
}

#[tokio::test]
pub async fn test_fixed_queue_rejects_runtime_concurrency_override() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;

    let error = storage
        .set_queue_concurrency(QueueOne, 2)
        .await
        .expect_err("fixed queues should reject runtime concurrency overrides");

    assert!(matches!(error, oxana::OxanaError::ConfigError(_)));

    Ok(())
}

#[tokio::test]
pub async fn test_dynamic_queue_unsets_runtime_concurrency_when_default_is_set() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let queue_key = "dynamic_concurrency".to_string();

    storage
        .set_queue_concurrency(DynamicConcurrencyQueue, 3)
        .await?;
    let configs = storage
        .queue_configs(std::slice::from_ref(&queue_key))
        .await?;
    assert!(configs.is_empty());

    storage
        .set_queue_concurrency(DynamicConcurrencyQueue, 5)
        .await?;
    let configs = storage
        .queue_configs(std::slice::from_ref(&queue_key))
        .await?;
    assert_eq!(
        configs
            .get(&queue_key)
            .and_then(|config| config.concurrency),
        Some(5)
    );

    storage
        .set_queue_state(DynamicConcurrencyQueue, oxana::QueueState::Paused)
        .await?;
    storage
        .set_queue_concurrency(DynamicConcurrencyQueue, 3)
        .await?;

    let configs = storage
        .queue_configs(std::slice::from_ref(&queue_key))
        .await?;
    let config = configs
        .get(&queue_key)
        .expect("queue state should preserve runtime config");
    assert_eq!(config.concurrency, None);
    assert_eq!(config.state, oxana::QueueState::Paused);

    storage.reset_queue_config(DynamicConcurrencyQueue).await?;
    let configs = storage
        .queue_configs(std::slice::from_ref(&queue_key))
        .await?;
    assert!(configs.is_empty());

    Ok(())
}

#[tokio::test]
pub async fn test_dynamic_child_queue_unsets_runtime_concurrency_when_inherited_default_is_set()
-> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let child_queue = DynamicTenantQueue {
        tenant: "acme".to_string(),
    };
    let child_queue_key = child_queue.key();

    storage.set_queue_concurrency(DynamicTenantBase, 5).await?;
    storage.set_queue_concurrency(child_queue, 5).await?;

    let configs = storage
        .queue_configs(std::slice::from_ref(&child_queue_key))
        .await?;
    assert!(configs.is_empty());

    let child_queue = DynamicTenantQueue {
        tenant: "acme".to_string(),
    };
    storage.set_queue_concurrency(child_queue, 9).await?;
    let configs = storage
        .queue_configs(std::slice::from_ref(&child_queue_key))
        .await?;
    assert_eq!(
        configs
            .get(&child_queue_key)
            .and_then(|config| config.concurrency),
        Some(9)
    );

    let child_queue = DynamicTenantQueue {
        tenant: "acme".to_string(),
    };
    storage.set_queue_concurrency(child_queue, 5).await?;
    let configs = storage
        .queue_configs(std::slice::from_ref(&child_queue_key))
        .await?;
    let config = configs
        .get(&child_queue_key)
        .expect("child runtime config should remain after clearing override");
    assert_eq!(config.concurrency, None);

    Ok(())
}

fn legacy_worker_named_envelope(queue: &str) -> oxana::JobEnvelope {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().timestamp_micros();

    oxana::JobEnvelope {
        id: id.clone(),
        queue: queue.to_string(),
        job: oxana::JobData {
            // 1.x envelopes carry the worker type name, not the job name.
            name: std::any::type_name::<WorkerNoop>().to_string(),
            args: serde_json::json!({}),
        },
        meta: oxana::JobMeta {
            id,
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

#[tokio::test]
pub async fn test_legacy_worker_named_envelope_is_processed() -> TestResult {
    let redis_pool = setup();

    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let runtime = storage
        .runtime(())
        .queue::<QueueOne>()
        .worker::<WorkerNoop, WorkerNoopJob>()
        .dequeue_timeout(Duration::from_millis(50))
        .exit_when_processed(1);

    storage
        .enqueue_envelope(legacy_worker_named_envelope("one"))
        .await?;

    let stats = runtime.run().await?;

    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);

    Ok(())
}
