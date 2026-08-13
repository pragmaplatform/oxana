use serde::{Deserialize, Serialize};
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerFailJob {}

pub struct WorkerFail;

impl oxana::Job for WorkerFailJob {}

impl oxana::FromContext<()> for WorkerFail {
    fn from_context(_ctx: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<WorkerFailJob> for WorkerFail {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<WorkerFailJob>>,
    ) -> Result<(), WorkerError> {
        Err(WorkerError::Generic(
            "I have nothing to live for...".to_string(),
        ))
    }

    fn retry_delay(&self, _job: &WorkerFailJob, _retries: u32) -> u64 {
        0
    }
    fn max_retries(&self, _job: &WorkerFailJob) -> u32 {
        0
    }
}

#[tokio::test]
pub async fn test_dead() -> TestResult {
    let redis_pool = setup();
    let ctx = ();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueOne>()
        .worker::<WorkerFail, WorkerFailJob>()
        .exit_when_processed(1);

    storage.enqueue(QueueOne, WorkerFailJob {}).await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    runtime.run().await?;

    assert_eq!(storage.dead_count().await?, 1);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    let dead = storage
        .list_dead(&oxana::QueueListOpts {
            count: 1,
            offset: 0,
        })
        .await?;
    assert_eq!(
        dead[0].meta.error.as_deref(),
        Some("Generic(\"I have nothing to live for...\")")
    );

    Ok(())
}

#[tokio::test]
pub async fn test_dead_uses_custom_error_formatter() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(())
        .queue::<QueueOne>()
        .worker::<WorkerFail, WorkerFailJob>()
        .error_formatter(|error| format!("diagnostic:\n{error:?}"))
        .exit_when_processed(1);

    storage.enqueue(QueueOne, WorkerFailJob {}).await?;
    runtime.run().await?;

    let dead = storage
        .list_dead(&oxana::QueueListOpts {
            count: 1,
            offset: 0,
        })
        .await?;
    assert_eq!(
        dead[0].meta.error.as_deref(),
        Some("diagnostic:\nGeneric(\"I have nothing to live for...\")")
    );

    Ok(())
}
