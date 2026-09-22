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

#[tokio::test]
pub async fn test_enqueue_at_unique_skip_preserves_original_schedule() -> TestResult {
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(setup())?;
    let first_at = chrono::Utc::now() + chrono::Duration::seconds(60);
    let between_at = first_at + chrono::Duration::seconds(30);
    let second_at = first_at + chrono::Duration::seconds(60);
    let key = random_string();

    let first_id = storage
        .enqueue_at(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 1,
                key: key.clone(),
                value: 1,
            },
            first_at,
        )
        .await?;
    let skipped_id = storage
        .enqueue_at(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 1,
                key,
                value: 2,
            },
            second_at,
        )
        .await?;
    let between_id = storage
        .enqueue_at(QueueOne, WorkerNoopJob {}, between_at)
        .await?;

    assert_eq!(skipped_id, first_id);
    let scheduled = storage
        .list_scheduled(&oxana::QueueListOpts {
            count: 10,
            offset: 0,
        })
        .await?;
    assert_eq!(scheduled.len(), 2);
    assert_eq!(scheduled[0].id, first_id);
    assert_eq!(scheduled[0].meta.scheduled_at, first_at.timestamp_micros());
    assert_eq!(scheduled[0].job.args["value"], 1);
    assert_eq!(scheduled[1].id, between_id);

    Ok(())
}

#[tokio::test]
pub async fn test_enqueue_at_unique_replace_uses_new_schedule() -> TestResult {
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(setup())?;
    let first_at = chrono::Utc::now() + chrono::Duration::seconds(60);
    let between_at = first_at + chrono::Duration::seconds(30);
    let second_at = first_at + chrono::Duration::seconds(60);
    let key = random_string();

    let first_id = storage
        .enqueue_at(
            QueueOne,
            WorkerUniqueReplaceJob {
                id: 1,
                key: key.clone(),
                value: 1,
            },
            first_at,
        )
        .await?;
    let replaced_id = storage
        .enqueue_at(
            QueueOne,
            WorkerUniqueReplaceJob {
                id: 1,
                key,
                value: 2,
            },
            second_at,
        )
        .await?;
    let between_id = storage
        .enqueue_at(QueueOne, WorkerNoopJob {}, between_at)
        .await?;

    assert_eq!(replaced_id, first_id);
    let scheduled = storage
        .list_scheduled(&oxana::QueueListOpts {
            count: 10,
            offset: 0,
        })
        .await?;
    assert_eq!(scheduled.len(), 2);
    assert_eq!(scheduled[0].id, between_id);
    assert_eq!(scheduled[1].id, first_id);
    assert_eq!(scheduled[1].meta.scheduled_at, second_at.timestamp_micros());
    assert_eq!(scheduled[1].job.args["value"], 2);

    Ok(())
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

#[tokio::test]
pub async fn test_try_enqueue_reports_duplicate_and_replacement() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let key = random_string();
    let skip_job = || WorkerUniqueSkipJob {
        id: 1,
        key: key.clone(),
        value: 1,
    };
    let replace_job = || WorkerUniqueReplaceJob {
        id: 2,
        key: key.clone(),
        value: 1,
    };

    let first = storage.try_enqueue(QueueOne, skip_job()).await?;
    let oxana::EnqueueOutcome::Enqueued(first_id) = &first else {
        panic!("first push must be filed, got {first:?}");
    };
    assert_eq!(first.job_id(), first_id);
    assert!(first.is_enqueued());

    let second = storage.try_enqueue(QueueOne, skip_job()).await?;
    assert_eq!(
        second,
        oxana::EnqueueOutcome::Duplicate {
            existing: first_id.clone()
        }
    );
    assert!(!second.is_enqueued());
    assert_eq!(second.job_id(), first_id);
    // The plain API keeps returning the id either way.
    assert_eq!(&storage.enqueue(QueueOne, skip_job()).await?, first_id);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    let first = storage.try_enqueue(QueueOne, replace_job()).await?;
    let oxana::EnqueueOutcome::Enqueued(replace_id) = first else {
        panic!("first push must be filed, got {first:?}");
    };
    let second = storage.try_enqueue(QueueOne, replace_job()).await?;
    assert_eq!(second, oxana::EnqueueOutcome::Replaced(replace_id));
    assert!(second.is_enqueued());
    assert_eq!(storage.enqueued_count(QueueOne).await?, 2);

    let scheduled = storage
        .try_enqueue_in(
            QueueOne,
            WorkerUniqueSkipJob {
                id: 3,
                key: key.clone(),
                value: 1,
            },
            60,
        )
        .await?;
    assert!(matches!(scheduled, oxana::EnqueueOutcome::Enqueued(_)));
    assert_eq!(storage.scheduled_count().await?, 1);

    let outcomes = storage
        .try_enqueue_list(
            QueueOne,
            [
                skip_job(),
                WorkerUniqueSkipJob {
                    id: 4,
                    key: String::new(),
                    value: 1,
                },
            ],
        )
        .await?;
    assert!(
        matches!(
            outcomes.as_slice(),
            [
                oxana::EnqueueOutcome::Duplicate { .. },
                oxana::EnqueueOutcome::Enqueued(_)
            ]
        ),
        "{outcomes:?}"
    );
    let outcomes = storage.try_enqueue_list(QueueOne, [replace_job()]).await?;
    assert!(
        matches!(outcomes.as_slice(), [oxana::EnqueueOutcome::Replaced(_)]),
        "{outcomes:?}"
    );

    Ok(())
}

#[tokio::test]
pub async fn test_concurrent_unique_pushes_enqueue_exactly_one() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let namespace = storage.namespace().to_string();
    let mut redis = redis_pool.get().await?;

    for round in 0..20 {
        let key = random_string();
        let mut pushes = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let storage = storage.clone();
            let key = key.clone();
            pushes.spawn(async move {
                storage
                    .try_enqueue(
                        QueueOne,
                        WorkerUniqueSkipJob {
                            id: round,
                            key,
                            value: 1,
                        },
                    )
                    .await
            });
        }
        let mut enqueued = 0;
        let mut duplicates = 0;
        while let Some(outcome) = pushes.join_next().await {
            match outcome?? {
                oxana::EnqueueOutcome::Enqueued(_) => enqueued += 1,
                oxana::EnqueueOutcome::Duplicate { .. } => duplicates += 1,
                other => panic!("unexpected outcome {other:?}"),
            }
        }
        assert_eq!((enqueued, duplicates), (1, 15), "round {round}");

        let queued: Vec<String> = redis
            .lrange(format!("{namespace}:queue:one"), 0, -1)
            .await?;
        assert_eq!(
            queued.len(),
            round as usize + 1,
            "round {round}: {queued:?}"
        );
        assert_eq!(storage.jobs_count().await?, round as usize + 1);
    }

    Ok(())
}

#[tokio::test]
pub async fn test_cancel_and_push_on_one_unique_key_never_drops_the_push_silently() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let namespace = storage.namespace().to_string();
    let mut redis = redis_pool.get().await?;
    let job = || WorkerUniqueSkipJob {
        id: 1,
        key: "key".to_string(),
        value: 1,
    };
    let job_id = storage.enqueue(QueueOne, job()).await?;
    let queue_key = format!("{namespace}:queue:one");

    let mut reported_duplicates = 0;
    for round in 0..50 {
        let cancel = {
            let storage = storage.clone();
            let job_id = job_id.clone();
            tokio::spawn(async move { storage.delete_unique_job(&job_id).await })
        };
        let push = {
            let storage = storage.clone();
            tokio::spawn(async move { storage.try_enqueue(QueueOne, job()).await })
        };
        cancel.await??;
        let outcome = push.await??;

        let filed: bool = redis.hexists(format!("{namespace}:jobs"), &job_id).await?;
        let queued: Vec<String> = redis.lrange(&queue_key, 0, -1).await?;
        match outcome {
            // The push landed after the cancel: the job must be filed, once.
            oxana::EnqueueOutcome::Enqueued(_) => {
                assert!(filed, "round {round}: reported enqueued but not filed");
                assert_eq!(queued, vec![job_id.clone()], "round {round}");
            }
            // The push saw the job before the cancel removed it: the caller is
            // told, rather than handed an id of a job that no longer exists.
            oxana::EnqueueOutcome::Duplicate { existing } => {
                assert_eq!(existing, job_id);
                assert!(
                    !filed,
                    "round {round}: duplicate reported but job still filed"
                );
                assert!(queued.is_empty(), "round {round}: {queued:?}");
                reported_duplicates += 1;
                storage.enqueue(QueueOne, job()).await?;
            }
            other => panic!("unexpected outcome {other:?}"),
        }
    }
    tracing::info!(
        reported_duplicates,
        "cancel+push rounds that reported a duplicate"
    );

    Ok(())
}
