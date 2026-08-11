use deadpool_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerUniqueSkipJob {
    pub id: i32,
    pub key: String,
    pub value: i32,
}

pub struct WorkerUniqueSkip {
    state: WorkerState,
}

impl oxana::Job for WorkerUniqueSkipJob {
    fn unique_id(&self) -> Option<String> {
        Some(format!("unique:{}", self.id))
    }
    fn on_conflict(&self) -> oxana::JobConflictStrategy {
        oxana::JobConflictStrategy::Skip
    }
}

impl oxana::FromContext<WorkerState> for WorkerUniqueSkip {
    fn from_context(ctx: &WorkerState) -> Self {
        Self { state: ctx.clone() }
    }
}

#[async_trait::async_trait]
impl oxana::Worker<WorkerUniqueSkipJob> for WorkerUniqueSkip {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        jobs: Vec<oxana::BatchItem<WorkerUniqueSkipJob>>,
    ) -> Result<(), WorkerError> {
        let mut redis = self.state.redis.get().await?;
        for item in jobs {
            let job = item.job;
            let _: () = redis.set_ex(&job.key, job.value.to_string(), 3).await?;
        }
        Ok(())
    }

    fn retry_delay(&self, _job: &WorkerUniqueSkipJob, _retries: u32) -> u64 {
        0
    }
    fn max_retries(&self, _job: &WorkerUniqueSkipJob) -> u32 {
        0
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerUniqueReplaceJob {
    pub id: i32,
    pub key: String,
    pub value: i32,
}

pub struct WorkerUniqueReplace {
    state: WorkerState,
}

impl oxana::Job for WorkerUniqueReplaceJob {
    fn unique_id(&self) -> Option<String> {
        Some(format!("unique:{}", self.id))
    }
    fn on_conflict(&self) -> oxana::JobConflictStrategy {
        oxana::JobConflictStrategy::Replace
    }
}

impl oxana::FromContext<WorkerState> for WorkerUniqueReplace {
    fn from_context(ctx: &WorkerState) -> Self {
        Self { state: ctx.clone() }
    }
}

#[async_trait::async_trait]
impl oxana::Worker<WorkerUniqueReplaceJob> for WorkerUniqueReplace {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        jobs: Vec<oxana::BatchItem<WorkerUniqueReplaceJob>>,
    ) -> Result<(), WorkerError> {
        let mut redis = self.state.redis.get().await?;
        for item in jobs {
            let job = item.job;
            let _: () = redis.set_ex(&job.key, job.value.to_string(), 3).await?;
        }
        Ok(())
    }

    fn retry_delay(&self, _job: &WorkerUniqueReplaceJob, _retries: u32) -> u64 {
        0
    }
    fn max_retries(&self, _job: &WorkerUniqueReplaceJob) -> u32 {
        0
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerUniqueReplaceRetryJob {
    pub id: i32,
    pub marker: i32,
}

pub struct WorkerUniqueReplaceRetry;

impl oxana::Job for WorkerUniqueReplaceRetryJob {
    fn unique_id(&self) -> Option<String> {
        Some(format!("unique:{}", self.id))
    }

    fn on_conflict(&self) -> oxana::JobConflictStrategy {
        oxana::JobConflictStrategy::Replace
    }
}

impl oxana::FromContext<WorkerState> for WorkerUniqueReplaceRetry {
    fn from_context(_ctx: &WorkerState) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<WorkerUniqueReplaceRetryJob> for WorkerUniqueReplaceRetry {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<WorkerUniqueReplaceRetryJob>>,
    ) -> Result<(), WorkerError> {
        Err(WorkerError::Generic("retry later".to_string()))
    }

    fn retry_delay(&self, _job: &WorkerUniqueReplaceRetryJob, _retries: u32) -> u64 {
        60
    }

    fn max_retries(&self, _job: &WorkerUniqueReplaceRetryJob) -> u32 {
        1
    }
}

#[tokio::test]
pub async fn test_unique_skip() -> TestResult {
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
        .worker::<WorkerUniqueSkip, WorkerUniqueSkipJob>()
        .exit_when_processed(2);
    let key1 = random_string();
    let key2 = random_string();

    storage
        .enqueue(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 1,
                key: key1.clone(),
                value: 1,
            },
        )
        .await?;
    storage
        .enqueue(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 1,
                key: key1.clone(),
                value: 2,
            },
        )
        .await?;
    storage
        .enqueue(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 2,
                key: key2.clone(),
                value: 3,
            },
        )
        .await?;
    storage
        .enqueue(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 2,
                key: key2.clone(),
                value: 4,
            },
        )
        .await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 2);

    runtime.run().await?;

    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    let value: Option<i32> = redis_conn.get(key1).await?;
    assert_eq!(value, Some(1));
    let value: Option<i32> = redis_conn.get(key2).await?;
    assert_eq!(value, Some(3));

    Ok(())
}

#[tokio::test]
pub async fn test_enqueue_list_unique_skip_deduplicates_within_batch() -> TestResult {
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
        .worker::<WorkerUniqueSkip, WorkerUniqueSkipJob>()
        .exit_when_processed(1);
    let key = random_string();

    let job_ids = storage
        .enqueue_list(
            QueueOne,
            vec![
                WorkerUniqueSkipJob {
                    id: 1,
                    key: key.clone(),
                    value: 1,
                },
                WorkerUniqueSkipJob {
                    id: 1,
                    key: key.clone(),
                    value: 2,
                },
            ],
        )
        .await?;

    assert_eq!(job_ids.len(), 2);
    assert_eq!(job_ids[0], job_ids[1]);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    runtime.run().await?;

    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    let value: Option<i32> = redis_conn.get(key).await?;
    assert_eq!(value, Some(1));

    Ok(())
}

#[tokio::test]
pub async fn test_enqueue_list_unique_skip_deduplicates_across_chunks() -> TestResult {
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
        .worker::<WorkerUniqueSkip, WorkerUniqueSkipJob>()
        .exit_when_processed(100);
    let duplicate_key = random_string();

    let jobs = (0..101)
        .map(|idx| WorkerUniqueSkipJob {
            id: if idx == 100 { 99 } else { idx },
            key: if idx >= 99 {
                duplicate_key.clone()
            } else {
                random_string()
            },
            value: idx,
        })
        .collect::<Vec<_>>();

    let job_ids = storage.enqueue_list(QueueOne, jobs).await?;

    assert_eq!(job_ids.len(), 101);
    assert_eq!(job_ids[99], job_ids[100]);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 100);

    runtime.run().await?;

    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    let value: Option<i32> = redis_conn.get(duplicate_key).await?;
    assert_eq!(value, Some(99));

    Ok(())
}

#[tokio::test]
pub async fn test_enqueue_list_unique_replace_deduplicates_within_batch() -> TestResult {
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
        .worker::<WorkerUniqueReplace, WorkerUniqueReplaceJob>()
        .exit_when_processed(1);
    let key = random_string();

    let job_ids = storage
        .enqueue_list(
            QueueOne,
            vec![
                WorkerUniqueReplaceJob {
                    id: 1,
                    key: key.clone(),
                    value: 1,
                },
                WorkerUniqueReplaceJob {
                    id: 1,
                    key: key.clone(),
                    value: 2,
                },
            ],
        )
        .await?;

    assert_eq!(job_ids.len(), 2);
    assert_eq!(job_ids[0], job_ids[1]);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    runtime.run().await?;

    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    let value: Option<i32> = redis_conn.get(key).await?;
    assert_eq!(value, Some(2));

    Ok(())
}

#[tokio::test]
pub async fn test_enqueue_list_unique_replace_deduplicates_across_chunks() -> TestResult {
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
        .worker::<WorkerUniqueReplace, WorkerUniqueReplaceJob>()
        .exit_when_processed(100);
    let duplicate_key = random_string();

    let jobs = (0..101)
        .map(|idx| WorkerUniqueReplaceJob {
            id: if idx == 100 { 99 } else { idx },
            key: if idx >= 99 {
                duplicate_key.clone()
            } else {
                random_string()
            },
            value: idx,
        })
        .collect::<Vec<_>>();

    let job_ids = storage.enqueue_list(QueueOne, jobs).await?;

    assert_eq!(job_ids.len(), 101);
    assert_eq!(job_ids[99], job_ids[100]);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 100);

    runtime.run().await?;

    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    let value: Option<i32> = redis_conn.get(duplicate_key).await?;
    assert_eq!(value, Some(100));

    Ok(())
}

#[tokio::test]
pub async fn test_unique_replace_clears_stale_retry_entry() -> TestResult {
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
        .worker::<WorkerUniqueReplaceRetry, WorkerUniqueReplaceRetryJob>()
        .exit_when_processed(1);

    let job_id = storage
        .enqueue(QueueOne, WorkerUniqueReplaceRetryJob { id: 1, marker: 1 })
        .await?;

    runtime.run().await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.retries_count().await?, 1);

    let retry_jobs = storage
        .list_retries(&oxana::QueueListOpts {
            count: 10,
            offset: 0,
        })
        .await?;
    let retried = retry_jobs
        .iter()
        .find(|job| job.id == job_id)
        .expect("job should be pending retry");
    assert_eq!(retried.meta.retries, 1);

    storage
        .enqueue(QueueOne, WorkerUniqueReplaceRetryJob { id: 1, marker: 2 })
        .await?;

    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    let replacement = storage
        .get_job(&job_id)
        .await?
        .expect("replacement should still exist");
    assert_eq!(replacement.meta.retries, 0);
    assert_eq!(
        replacement.job.args.get("marker"),
        Some(&serde_json::json!(2))
    );

    let retry_jobs = storage
        .list_retries(&oxana::QueueListOpts {
            count: 10,
            offset: 0,
        })
        .await?;
    assert!(retry_jobs.is_empty());

    Ok(())
}

#[tokio::test]
pub async fn test_unique_replace() -> TestResult {
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
        .worker::<WorkerUniqueReplace, WorkerUniqueReplaceJob>()
        .exit_when_processed(2);

    let key1 = random_string();
    let key2 = random_string();

    storage
        .enqueue(
            QueueOne,
            WorkerUniqueReplaceJob {
                id: 1,
                key: key1.clone(),
                value: 1,
            },
        )
        .await?;
    storage
        .enqueue(
            QueueOne,
            WorkerUniqueReplaceJob {
                id: 1,
                key: key1.clone(),
                value: 2,
            },
        )
        .await?;
    storage
        .enqueue(
            QueueOne,
            WorkerUniqueReplaceJob {
                id: 2,
                key: key2.clone(),
                value: 3,
            },
        )
        .await?;
    storage
        .enqueue(
            QueueOne,
            WorkerUniqueReplaceJob {
                id: 2,
                key: key2.clone(),
                value: 4,
            },
        )
        .await?;

    assert_eq!(storage.enqueued_count(QueueOne).await?, 2);

    runtime.run().await?;

    assert_eq!(storage.dead_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);

    let value: Option<i32> = redis_conn.get(key1).await?;
    assert_eq!(value, Some(2));
    let value: Option<i32> = redis_conn.get(key2).await?;
    assert_eq!(value, Some(4));

    Ok(())
}

#[tokio::test]
pub async fn test_delete_unique_job_accepts_job_id_or_job() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;

    let first_job_id = storage
        .enqueue(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 1,
                key: random_string(),
                value: 1,
            },
        )
        .await?;
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);
    storage.delete_unique_job(&first_job_id).await?;
    assert!(storage.get_job(&first_job_id).await?.is_none());
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);

    let second_job_id = storage
        .enqueue(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 2,
                key: random_string(),
                value: 2,
            },
        )
        .await?;
    let second_job = WorkerUniqueSkipJob {
        id: 2,
        key: random_string(),
        value: 3,
    };
    storage.delete_unique_job(&second_job).await?;
    assert!(storage.get_job(&second_job_id).await?.is_none());
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);

    let re_enqueued_job_id = storage
        .enqueue(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 2,
                key: random_string(),
                value: 4,
            },
        )
        .await?;
    assert_eq!(re_enqueued_job_id, second_job_id);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    Ok(())
}

#[tokio::test]
pub async fn test_delete_unique_job_removes_scheduled_membership() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;

    let job_id = storage
        .enqueue_in(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 1,
                key: random_string(),
                value: 1,
            },
            60,
        )
        .await?;
    assert_eq!(storage.scheduled_count().await?, 1);

    storage.delete_unique_job(&job_id).await?;

    assert!(storage.get_job(&job_id).await?.is_none());
    assert_eq!(storage.scheduled_count().await?, 0);

    let re_enqueued_job_id = storage
        .enqueue_in(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 1,
                key: random_string(),
                value: 2,
            },
            60,
        )
        .await?;
    assert_eq!(re_enqueued_job_id, job_id);
    assert_eq!(storage.scheduled_count().await?, 1);

    Ok(())
}

#[tokio::test]
pub async fn test_delete_unique_job_removes_retry_membership() -> TestResult {
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
        .worker::<WorkerUniqueReplaceRetry, WorkerUniqueReplaceRetryJob>()
        .exit_when_processed(1);

    let job_id = storage
        .enqueue(QueueOne, WorkerUniqueReplaceRetryJob { id: 1, marker: 1 })
        .await?;
    runtime.run().await?;
    assert_eq!(storage.retries_count().await?, 1);

    storage.delete_unique_job(&job_id).await?;

    assert!(storage.get_job(&job_id).await?.is_none());
    assert_eq!(storage.retries_count().await?, 0);

    let re_enqueued_job_id = storage
        .enqueue(QueueOne, WorkerUniqueReplaceRetryJob { id: 1, marker: 2 })
        .await?;
    assert_eq!(re_enqueued_job_id, job_id);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    Ok(())
}

#[tokio::test]
pub async fn test_delete_unique_job_rejects_non_unique_job() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;

    let error = storage
        .delete_unique_job(&WorkerNoopJob {})
        .await
        .expect_err("non-unique jobs cannot be resolved to a deterministic ID");

    assert!(matches!(error, oxana::OxanaError::ConfigError(_)));

    Ok(())
}
