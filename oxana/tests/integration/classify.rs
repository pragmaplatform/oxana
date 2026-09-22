use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use testresult::TestResult;

use crate::shared::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct ClassifiedJob {
    /// Whether the worker should decide, during the attempt, that the payload
    /// can never succeed.
    permanent: bool,
}

impl oxana::Job for ClassifiedJob {}

#[derive(Debug, thiserror::Error)]
pub enum ClassifiedError {
    #[error("malformed payload")]
    Malformed,
    #[error("upstream unavailable")]
    Transient,
}

#[derive(Clone, Default)]
pub struct Attempts(Arc<AtomicUsize>);

pub struct ClassifyingWorker(Attempts);

impl oxana::FromContext<Attempts> for ClassifyingWorker {
    fn from_context(ctx: &Attempts) -> Self {
        Self(ctx.clone())
    }
}

#[async_trait::async_trait]
impl oxana::Worker<ClassifiedJob> for ClassifyingWorker {
    type Error = ClassifiedError;

    async fn process(
        &self,
        job: ClassifiedJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), Self::Error> {
        self.0.0.fetch_add(1, Ordering::SeqCst);
        if job.permanent {
            Err(ClassifiedError::Malformed)
        } else {
            Err(ClassifiedError::Transient)
        }
    }

    fn classify(&self, job: &ClassifiedJob, error: &Self::Error) -> oxana::FailureKind {
        assert!(job.permanent == matches!(error, ClassifiedError::Malformed));
        match error {
            ClassifiedError::Malformed => oxana::FailureKind::DeadLetter,
            ClassifiedError::Transient => oxana::FailureKind::Retry,
        }
    }

    fn max_retries(&self, _job: &ClassifiedJob) -> u32 {
        3
    }

    fn retry_delay(&self, _job: &ClassifiedJob, _retries: u32) -> u64 {
        0
    }
}

/// Same handler, no `classify`: the default retries to the budget.
pub struct DefaultWorker(Attempts);

impl oxana::FromContext<Attempts> for DefaultWorker {
    fn from_context(ctx: &Attempts) -> Self {
        Self(ctx.clone())
    }
}

#[async_trait::async_trait]
impl oxana::Worker<ClassifiedJob> for DefaultWorker {
    type Error = ClassifiedError;

    async fn process(
        &self,
        _job: ClassifiedJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), Self::Error> {
        self.0.0.fetch_add(1, Ordering::SeqCst);
        Err(ClassifiedError::Malformed)
    }

    fn max_retries(&self, _job: &ClassifiedJob) -> u32 {
        3
    }

    fn retry_delay(&self, _job: &ClassifiedJob, _retries: u32) -> u64 {
        0
    }
}

#[tokio::test]
pub async fn test_dead_letter_classification_skips_the_retry_budget() -> TestResult {
    let redis_pool = setup();
    let attempts = Attempts::default();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let reports = Arc::new(std::sync::Mutex::new(Vec::new()));
    let runtime = storage
        .runtime(attempts.clone())
        .queue::<QueueOne>()
        .worker::<ClassifyingWorker, ClassifiedJob>()
        .failure_reporter({
            let reports = Arc::clone(&reports);
            move |report| {
                reports
                    .lock()
                    .unwrap()
                    .push((report.metadata.will_retry, report.metadata.terminal));
            }
        })
        .exit_when_processed(1);
    let job_id = storage
        .enqueue(QueueOne, ClassifiedJob { permanent: true })
        .await?;

    let stats = tokio::time::timeout(std::time::Duration::from_secs(5), runtime.run()).await??;

    assert_eq!(stats.processed, 1);
    assert_eq!(stats.failed, 1);
    assert_eq!(
        attempts.0.load(Ordering::SeqCst),
        1,
        "handler must run once"
    );
    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.dead_count().await?, 1);
    assert!(storage.get_job(&job_id).await?.is_none());
    let dead = storage
        .list_dead(&oxana::QueueListOpts {
            count: 1,
            offset: 0,
        })
        .await?;
    assert_eq!(dead[0].id, job_id);
    assert_eq!(dead[0].meta.retries, 0);
    assert_eq!(dead[0].meta.error.as_deref(), Some("Malformed"));
    assert_eq!(*reports.lock().unwrap(), vec![(false, true)]);

    Ok(())
}

#[tokio::test]
pub async fn test_retry_classification_keeps_the_retry_budget() -> TestResult {
    let redis_pool = setup();
    let attempts = Attempts::default();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let runtime = storage
        .runtime(attempts.clone())
        .queue::<QueueOne>()
        .worker::<ClassifyingWorker, ClassifiedJob>()
        .dequeue_timeout(std::time::Duration::from_millis(25))
        .exit_when_processed(4);
    storage
        .enqueue(QueueOne, ClassifiedJob { permanent: false })
        .await?;

    let stats = tokio::time::timeout(std::time::Duration::from_secs(10), runtime.run()).await??;

    assert_eq!(stats.processed, 4);
    assert_eq!(attempts.0.load(Ordering::SeqCst), 4);
    assert_eq!(storage.dead_count().await?, 1);

    Ok(())
}

#[tokio::test]
pub async fn test_default_classification_retries_to_the_budget() -> TestResult {
    let redis_pool = setup();
    let attempts = Attempts::default();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let runtime = storage
        .runtime(attempts.clone())
        .queue::<QueueOne>()
        .worker::<DefaultWorker, ClassifiedJob>()
        .dequeue_timeout(std::time::Duration::from_millis(25))
        .exit_when_processed(4);
    storage
        .enqueue(QueueOne, ClassifiedJob { permanent: true })
        .await?;

    let stats = tokio::time::timeout(std::time::Duration::from_secs(10), runtime.run()).await??;

    assert_eq!(stats.processed, 4);
    assert_eq!(attempts.0.load(Ordering::SeqCst), 4);
    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.dead_count().await?, 1);

    Ok(())
}
