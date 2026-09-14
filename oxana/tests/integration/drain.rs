use crate::shared::*;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

#[derive(Serialize)]
struct QueueDynamic(i32);

#[derive(Serialize)]
struct QueueStatic;

impl oxana::Queue for QueueDynamic {
    fn key(&self) -> String {
        format!(
            "dynamic#{}",
            oxana::value_to_queue_key(serde_json::to_value(self).unwrap_or_default())
        )
    }

    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_dynamic("dynamic")
    }
}

impl oxana::Queue for QueueStatic {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("static")
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DrainFailJob;

impl oxana::Job for DrainFailJob {}

struct DrainFailWorker;

impl oxana::FromContext<()> for DrainFailWorker {
    fn from_context(_ctx: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<DrainFailJob> for DrainFailWorker {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<DrainFailJob>>,
    ) -> Result<(), WorkerError> {
        Err(WorkerError::Generic("drain failed".to_string()))
    }
}

#[tokio::test]
pub async fn test_drain() -> TestResult {
    let redis_pool = setup();
    let ctx = ();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueDynamic>()
        .queue::<QueueStatic>()
        .worker::<WorkerNoop, WorkerNoopJob>()
        .exit_when_processed(2);

    storage.enqueue(QueueDynamic(1), WorkerNoopJob {}).await?;
    storage.enqueue(QueueDynamic(2), WorkerNoopJob {}).await?;
    storage.enqueue(QueueStatic, WorkerNoopJob {}).await?;
    storage.enqueue(QueueStatic, WorkerNoopJob {}).await?;

    assert_eq!(storage.jobs_count().await?, 4);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = runtime.drain(QueueDynamic(1)).await?;

    assert_eq!(storage.jobs_count().await?, 3);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = runtime.drain(QueueDynamic(2)).await?;

    assert_eq!(storage.jobs_count().await?, 2);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = runtime.drain(QueueStatic).await?;

    assert_eq!(storage.jobs_count().await?, 0);
    assert_eq!(stats.processed, 2);
    assert_eq!(stats.succeeded, 2);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 0);

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct DrainSlowJob;

impl oxana::Job for DrainSlowJob {}

struct DrainSlowWorker;

impl oxana::FromContext<()> for DrainSlowWorker {
    fn from_context(_ctx: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<DrainSlowJob> for DrainSlowWorker {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<DrainSlowJob>>,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        Ok(())
    }
}

/// A drain holds a processing list for as long as a job runs. A worker sweeping
/// the same namespace reads that list as a dead process's unless the drain is
/// registered, and would put the job back on its queue while the drain still
/// runs it.
#[tokio::test]
pub async fn test_drain_registers_its_process_while_jobs_run() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(())
        .queue::<QueueStatic>()
        .worker::<DrainSlowWorker, DrainSlowJob>();

    storage.enqueue(QueueStatic, DrainSlowJob).await?;
    assert!(storage.processes().await?.is_empty());

    let observed = storage.clone();
    let observer = tokio::spawn(async move {
        for _ in 0..40 {
            if !observed.processes().await.unwrap_or_default().is_empty() {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        false
    });

    let stats = runtime.drain(QueueStatic).await?;

    assert_eq!(stats.succeeded, 1);
    assert!(
        observer.await?,
        "the drain ran a job without registering its process"
    );
    assert!(
        storage.processes().await?.is_empty(),
        "the drain left its process record behind"
    );

    Ok(())
}

#[tokio::test]
pub async fn test_drain_uses_custom_error_formatter() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(())
        .queue::<QueueStatic>()
        .worker::<DrainFailWorker, DrainFailJob>()
        .error_formatter(|error| format!("diagnostic:\n{error:?}"));

    storage.enqueue(QueueStatic, DrainFailJob).await?;

    let stats = runtime.drain(QueueStatic).await?;
    let dead = storage
        .list_dead(&oxana::QueueListOpts {
            count: 1,
            offset: 0,
        })
        .await?;

    assert_eq!(stats.failed, 1);
    assert_eq!(
        dead[0].meta.error.as_deref(),
        Some("diagnostic:\nGeneric(\"drain failed\")")
    );

    Ok(())
}
