use serde::{Deserialize, Serialize};
use std::time::Duration;
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerTerminalJob {}

pub struct WorkerTerminal;

impl oxana::Job for WorkerTerminalJob {}

impl oxana::FromContext<()> for WorkerTerminal {
    fn from_context(_ctx: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<WorkerTerminalJob> for WorkerTerminal {
    type Error = oxana::BoxError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<WorkerTerminalJob>>,
    ) -> Result<(), Self::Error> {
        Err(oxana::Terminal("invalid recipient".to_string()).into())
    }

    fn retry_delay(&self, _job: &WorkerTerminalJob, _retries: u32) -> u64 {
        0
    }

    fn max_retries(&self, _job: &WorkerTerminalJob) -> u32 {
        5
    }
}

#[tokio::test]
pub async fn test_terminal_error_kills_job_on_first_run() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(())
        .queue::<QueueOne>()
        .worker::<WorkerTerminal, WorkerTerminalJob>()
        .dequeue_timeout(Duration::from_millis(50))
        .exit_when_processed(1);

    let id = storage.enqueue(QueueOne, WorkerTerminalJob {}).await?;
    runtime.run().await?;

    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.dead_count().await?, 1);
    assert_eq!(storage.jobs_count().await?, 0);

    let dead = storage
        .list_dead(&oxana::QueueListOpts {
            count: 1,
            offset: 0,
        })
        .await?;
    assert_eq!(dead[0].id, id);
    assert_eq!(dead[0].meta.retries, 0);
    assert!(
        dead[0]
            .meta
            .error
            .as_deref()
            .is_some_and(|error| error.contains("invalid recipient")),
        "dead job should keep the terminal reason, got {:?}",
        dead[0].meta.error
    );

    Ok(())
}

/// A worker-defined error that wraps `Terminal` as its source.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("send rejected")]
    Rejected(#[source] oxana::Terminal),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerWrappedTerminalJob {}

pub struct WorkerWrappedTerminal;

impl oxana::Job for WorkerWrappedTerminalJob {}

impl oxana::FromContext<()> for WorkerWrappedTerminal {
    fn from_context(_ctx: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<WorkerWrappedTerminalJob> for WorkerWrappedTerminal {
    type Error = SendError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<WorkerWrappedTerminalJob>>,
    ) -> Result<(), SendError> {
        Err(SendError::Rejected(oxana::Terminal(
            "server refused".to_string(),
        )))
    }

    fn retry_delay(&self, _job: &WorkerWrappedTerminalJob, _retries: u32) -> u64 {
        0
    }

    fn max_retries(&self, _job: &WorkerWrappedTerminalJob) -> u32 {
        5
    }
}

#[tokio::test]
pub async fn test_terminal_error_found_in_source_chain_kills_job() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(())
        .queue::<QueueOne>()
        .worker::<WorkerWrappedTerminal, WorkerWrappedTerminalJob>()
        .dequeue_timeout(Duration::from_millis(50))
        .exit_when_processed(1);

    storage.enqueue(QueueOne, WorkerWrappedTerminalJob {}).await?;
    runtime.run().await?;

    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.dead_count().await?, 1);

    Ok(())
}
