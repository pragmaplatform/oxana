//! Recovery and callback coverage that also compiles without Oxana's macros or Sentry.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxana::{BatchItem, WorkerFailure, WorkerFailureMetadata};
use serde::{Deserialize, Serialize};
use testresult::TestResult;

#[derive(Debug, Serialize, Deserialize)]
struct PanicJob {
    retry: bool,
}
impl oxana::Job for PanicJob {}

struct PanicWorker<const BATCH: usize>;
impl<const BATCH: usize> oxana::FromContext<()> for PanicWorker<BATCH> {
    fn from_context(_: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl<const BATCH: usize> oxana::Worker<PanicJob> for PanicWorker<BATCH> {
    type Error = std::io::Error;

    async fn run_batch(&self, jobs: Vec<BatchItem<PanicJob>>) -> Result<(), Self::Error> {
        assert_eq!(jobs.len(), BATCH);
        if jobs.iter().any(|job| job.ctx.meta.retries == 0) {
            panic!("recoverable worker panic");
        }
        for job in jobs {
            assert_eq!(
                job.ctx.meta.error.as_deref(),
                Some("recoverable worker panic")
            );
        }
        Ok(())
    }

    fn max_retries(&self, job: &PanicJob) -> u32 {
        u32::from(job.retry)
    }

    fn retry_delay(&self, _: &PanicJob, _: u32) -> u64 {
        0
    }

    fn batch_config() -> Option<oxana::WorkerBatchConfig> {
        (BATCH > 1).then(|| oxana::WorkerBatchConfig::new(BATCH, Duration::from_millis(100)))
    }
}

struct PanicQueue;
impl oxana::Queue for PanicQueue {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("panics").concurrency(2)
    }
}

async fn verify_recovery<const BATCH: usize>() -> TestResult {
    for retry in [false, true] {
        for custom in [false, true] {
            let pool = deadpool_redis::Config::from_url(std::env::var("REDIS_URL")?)
                .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
            let storage = oxana::Storage::builder()
                .namespace(uuid::Uuid::new_v4().to_string())
                .build_from_pool(pool)?;
            let reports = Arc::new(Mutex::new(Vec::<WorkerFailureMetadata>::new()));
            let mut runtime = storage
                .runtime(())
                .queue::<PanicQueue>()
                .worker::<PanicWorker<BATCH>, PanicJob>()
                .dequeue_timeout(Duration::from_millis(50))
                .exit_when_processed((BATCH * if retry { 2 } else { 1 }) as u64);
            if custom {
                let reports = Arc::clone(&reports);
                runtime = runtime.failure_reporter(move |report| {
                    assert!(matches!(
                        report.failure,
                        WorkerFailure::Panic {
                            message: "recoverable worker panic"
                        }
                    ));
                    reports
                        .lock()
                        .expect("reports")
                        .push(report.metadata.clone());
                });
            }
            let mut job_ids = Vec::new();
            for _ in 0..BATCH {
                job_ids.push(storage.enqueue(PanicQueue, PanicJob { retry }).await?);
            }
            let stats = tokio::time::timeout(Duration::from_secs(15), runtime.run()).await??;
            assert_eq!(stats.panicked, BATCH as u64);
            assert_eq!(stats.failed, BATCH as u64);
            assert_eq!(stats.succeeded, if retry { BATCH as u64 } else { 0 });
            assert_eq!(stats.processed, (BATCH * if retry { 2 } else { 1 }) as u64);
            assert_eq!(storage.retries_count().await?, 0);
            assert_eq!(storage.jobs_count().await?, 0);
            let dead = storage
                .list_dead(&oxana::QueueListOpts {
                    count: 10,
                    offset: 0,
                })
                .await?;
            assert_eq!(dead.len(), if retry { 0 } else { BATCH });
            for job in dead {
                assert_eq!(job.meta.error.as_deref(), Some("recoverable worker panic"));
            }
            let reports = reports.lock().expect("reports");
            assert_eq!(reports.len(), usize::from(custom));
            if let Some(report) = reports.first() {
                assert_eq!(report.batch_size, BATCH);
                assert_eq!(report.jobs.len(), BATCH);
                assert_eq!(report.will_retry, retry);
                assert_eq!(report.terminal, !retry);
                for job in &report.jobs {
                    assert!(job_ids.contains(&job.job_id));
                    assert_eq!(job.retry_count, 0);
                    assert_eq!(job.max_retries, u32::from(retry));
                    assert_eq!(job.will_retry, retry);
                    assert_eq!(job.terminal, !retry);
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn single_job_panic_recovery_and_custom_reporting() -> TestResult {
    verify_recovery::<1>().await
}

#[tokio::test]
async fn batch_panic_recovery_and_custom_reporting() -> TestResult {
    verify_recovery::<2>().await
}
