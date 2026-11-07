use deadpool_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerSnoozeJob {
    pub attempts_key: String,
}

pub struct WorkerSnooze {
    state: WorkerState,
}

impl oxana::Job for WorkerSnoozeJob {}

impl oxana::FromContext<WorkerState> for WorkerSnooze {
    fn from_context(ctx: &WorkerState) -> Self {
        Self { state: ctx.clone() }
    }
}

#[async_trait::async_trait]
impl oxana::Worker<WorkerSnoozeJob> for WorkerSnooze {
    type Error = oxana::BoxError;

    async fn process(
        &self,
        job: WorkerSnoozeJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), Self::Error> {
        let mut redis = self.state.redis.get().await?;
        let attempt: i64 = redis.incr(&job.attempts_key, 1).await?;
        if attempt == 1 {
            return Err(oxana::Snooze(Duration::from_secs(60)).into());
        }
        Ok(())
    }

    fn retry_delay(&self, _job: &WorkerSnoozeJob, _retries: u32) -> u64 {
        0
    }

    fn max_retries(&self, _job: &WorkerSnoozeJob) -> u32 {
        0
    }
}

#[tokio::test]
pub async fn test_snooze_reschedules_same_job_without_spending_a_retry() -> TestResult {
    let redis_pool = setup();
    let ctx = WorkerState {
        redis: redis_pool.clone(),
    };
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueOne>()
        .worker::<WorkerSnooze, WorkerSnoozeJob>()
        .dequeue_timeout(Duration::from_millis(50))
        .exit_when_processed(1);

    let attempts_key = uuid::Uuid::new_v4().to_string();
    let before = chrono::Utc::now().timestamp_micros();
    let id = storage
        .enqueue(QueueOne, WorkerSnoozeJob { attempts_key })
        .await?;
    runtime.run().await?;

    // max_retries is 0, so a counted attempt would have killed the job.
    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.scheduled_count().await?, 1);

    let scheduled = storage
        .list_scheduled(&oxana::QueueListOpts {
            count: 1,
            offset: 0,
        })
        .await?;
    assert_eq!(scheduled[0].id, id);
    assert_eq!(scheduled[0].meta.retries, 0);
    assert!(
        scheduled[0].meta.scheduled_at >= before + 60_000_000,
        "job should be scheduled at least 60s ahead"
    );

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerShortSnoozeJob {
    pub attempts_key: String,
}

pub struct WorkerShortSnooze {
    state: WorkerState,
}

impl oxana::Job for WorkerShortSnoozeJob {}

impl oxana::FromContext<WorkerState> for WorkerShortSnooze {
    fn from_context(ctx: &WorkerState) -> Self {
        Self { state: ctx.clone() }
    }
}

#[async_trait::async_trait]
impl oxana::Worker<WorkerShortSnoozeJob> for WorkerShortSnooze {
    type Error = oxana::BoxError;

    async fn process(
        &self,
        job: WorkerShortSnoozeJob,
        ctx: &oxana::JobContext,
    ) -> Result<(), Self::Error> {
        assert_eq!(ctx.meta.retries, 0, "snooze must not count a retry");
        let mut redis = self.state.redis.get().await?;
        let attempt: i64 = redis.incr(&job.attempts_key, 1).await?;
        if attempt == 1 {
            return Err(oxana::Snooze(Duration::from_millis(500)).into());
        }
        Ok(())
    }

    fn max_retries(&self, _job: &WorkerShortSnoozeJob) -> u32 {
        0
    }
}

#[tokio::test]
pub async fn test_snoozed_job_runs_again_after_delay() -> TestResult {
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
        .queue::<QueueOne>()
        .worker::<WorkerShortSnooze, WorkerShortSnoozeJob>()
        .dequeue_timeout(Duration::from_millis(50))
        .exit_when_processed(2);

    let attempts_key = uuid::Uuid::new_v4().to_string();
    storage
        .enqueue(
            QueueOne,
            WorkerShortSnoozeJob {
                attempts_key: attempts_key.clone(),
            },
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(30), runtime.run()).await??;

    let attempts: i64 = redis_conn.get(&attempts_key).await?;
    assert_eq!(attempts, 2);
    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    Ok(())
}
