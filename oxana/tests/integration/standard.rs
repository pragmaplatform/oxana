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
    executions: Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct ShutdownProgressJob;

impl oxana::Job for ShutdownProgressJob {
    fn should_resurrect() -> bool {
        true
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
        self.state
            .executions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
pub async fn test_runtime_does_not_process_excluded_queues() -> TestResult {
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
        .except_queue::<QueueTwo>()
        .exit_when_processed(1);
    let included_key = random_string();
    let excluded_key = random_string();

    storage
        .enqueue(
            QueueOne,
            WorkerRedisSetJob {
                key: included_key.clone(),
                value: "included".to_string(),
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

    let included: Option<String> = redis_conn.get(included_key).await?;
    let excluded: Option<String> = redis_conn.get(excluded_key).await?;
    assert_eq!(included.as_deref(), Some("included"));
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
        .build_from_pool(redis_pool.clone())?;
    let state = ShutdownDrainState {
        started: Arc::new(Notify::new()),
        finished: Arc::new(Notify::new()),
        executions: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let runtime = storage
        .runtime(state.clone())
        .queue::<QueueOne>()
        .worker::<ShutdownProgressWorker, ShutdownProgressJob>()
        .shutdown_timeout(Duration::from_secs(12))
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

    let replacement = oxana::Storage::builder()
        .namespace(storage.namespace().to_string())
        .build_from_pool(redis_pool)?;
    let new_runtime = replacement
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
    assert_eq!(
        state.executions.load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    Ok(())
}

type CapturedReport = Arc<std::sync::Mutex<Option<oxana::ShutdownTimeoutReport>>>;

/// A slot and the `on_shutdown_timeout` callback that fills it.
fn capture_shutdown_report() -> (
    CapturedReport,
    impl Fn(&oxana::ShutdownTimeoutReport) + Send + Sync + 'static,
) {
    let report: CapturedReport = Arc::default();
    let slot = Arc::clone(&report);
    (report, move |timeout: &oxana::ShutdownTimeoutReport| {
        slot.lock().unwrap().replace(timeout.clone());
    })
}

trait TakeReport {
    fn take(&self) -> Option<oxana::ShutdownTimeoutReport>;
}

impl TakeReport for CapturedReport {
    fn take(&self) -> Option<oxana::ShutdownTimeoutReport> {
        self.lock().unwrap().take()
    }
}

#[derive(Clone)]
struct BlockedShutdownState {
    started: tokio::sync::mpsc::UnboundedSender<usize>,
    dropped: tokio::sync::mpsc::UnboundedSender<()>,
    release: Arc<tokio::sync::Mutex<oneshot::Receiver<()>>>,
    lifetime: Arc<()>,
}

struct ShutdownDropGuard(tokio::sync::mpsc::UnboundedSender<()>);

impl Drop for ShutdownDropGuard {
    fn drop(&mut self) {
        self.0.send(()).ok();
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct BlockedShutdownJob;

impl oxana::Job for BlockedShutdownJob {
    fn should_resurrect() -> bool {
        true
    }
}

struct BlockedShutdownWorker<const BATCH: bool>(BlockedShutdownState);

impl<const BATCH: bool> oxana::FromContext<BlockedShutdownState> for BlockedShutdownWorker<BATCH> {
    fn from_context(ctx: &BlockedShutdownState) -> Self {
        Self(ctx.clone())
    }
}

#[async_trait::async_trait]
impl<const BATCH: bool> oxana::Worker<BlockedShutdownJob> for BlockedShutdownWorker<BATCH> {
    type Error = std::io::Error;

    fn batch_config() -> Option<oxana::WorkerBatchConfig> {
        BATCH.then(|| oxana::WorkerBatchConfig::new(2, Duration::from_secs(30)))
    }

    async fn run_batch(
        &self,
        jobs: Vec<oxana::BatchItem<BlockedShutdownJob>>,
    ) -> Result<(), Self::Error> {
        let _guard = ShutdownDropGuard(self.0.dropped.clone());
        self.0.started.send(jobs.len()).unwrap();
        (&mut *self.0.release.lock().await).await.unwrap();
        Ok(())
    }
}

struct RecoveredShutdownWorker;

impl oxana::FromContext<()> for RecoveredShutdownWorker {
    fn from_context((): &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<BlockedShutdownJob> for RecoveredShutdownWorker {
    type Error = std::io::Error;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<BlockedShutdownJob>>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum ShutdownTrigger {
    Signal,
    CoordinatorFailure,
    BackgroundFailure,
}

async fn bounded_shutdown<const BATCH: bool>(trigger: ShutdownTrigger) -> TestResult {
    let pool = setup();
    let namespace = random_string();
    let storage = oxana::Storage::builder()
        .namespace(namespace.clone())
        .build_from_pool(pool.clone())?;
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = oneshot::channel();
    let state = BlockedShutdownState {
        started: started_tx,
        dropped: dropped_tx,
        release: Arc::new(tokio::sync::Mutex::new(release_rx)),
        lifetime: Arc::new(()),
    };
    let lifetime = Arc::downgrade(&state.lifetime);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let deadline = Duration::from_millis(300);
    let (report, capture) = capture_shutdown_report();
    let runtime = storage
        .runtime(state)
        .queue_with_concurrency::<QueueOne>(2)
        .queue_with_concurrency::<QueueTwo>(2)
        .worker::<BlockedShutdownWorker<BATCH>, BlockedShutdownJob>()
        .heartbeat_interval(Duration::from_millis(25))
        .shutdown_timeout(deadline)
        .redis_failure_tolerance(1)
        .on_shutdown_timeout(capture)
        .shutdown_on(async move {
            shutdown_rx.await.unwrap();
            Ok(())
        });
    let batch_size = if BATCH { 2 } else { 1 };
    let mut job_ids = Vec::new();
    for _ in 0..batch_size {
        job_ids.push(storage.enqueue(QueueOne, BlockedShutdownJob).await?);
        job_ids.push(storage.enqueue(QueueTwo, BlockedShutdownJob).await?);
    }
    let mut runner = tokio::spawn(runtime.run());
    for _ in 0..2 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), started_rx.recv()).await?,
            Some(batch_size)
        );
    }

    let mut redis = pool.get().await?;
    let processes_key = format!("{namespace}:processes");
    let old_processes: Vec<String> = redis.zrange(&processes_key, 0, -1).await?;
    assert_eq!(old_processes.len(), 1);
    let started = tokio::time::Instant::now();
    match trigger {
        ShutdownTrigger::Signal => shutdown_tx.send(()).unwrap(),
        ShutdownTrigger::CoordinatorFailure => {
            // The queue config watcher deterministically fails parsing this value.
            let _: () = redis
                .hset(format!("{namespace}:queue_configs"), "one", "invalid-json")
                .await?;
        }
        ShutdownTrigger::BackgroundFailure => {
            // The schedule loop must read a sorted set; inject a Redis WRONGTYPE
            // failure without taking down Redis or touching another namespace.
            let _: () = redis
                .set(format!("{namespace}:schedule"), "wrong-type")
                .await?;
        }
    }

    // Borrow the separately owned task: timing out cannot cancel the runtime
    // and manufacture the worker drop that this regression is checking.
    let overhead = if !matches!(trigger, ShutdownTrigger::Signal) {
        Duration::from_millis(1500) // Includes the config watcher's 1s poll.
    } else {
        Duration::from_millis(250)
    };
    let outcome = tokio::time::timeout(deadline + overhead, &mut runner).await;
    let result = match outcome {
        Ok(result) => result?,
        Err(error) => {
            // Failure cleanup only; the test still returns the timeout error.
            runner.abort();
            let _ = runner.await;
            return Err(error.into());
        }
    };
    assert!(
        started.elapsed() >= deadline,
        "workers must get time to drain"
    );
    match trigger {
        ShutdownTrigger::Signal => assert!(
            matches!(result, Err(oxana::OxanaError::ShutdownTimeout)),
            "{result:?}"
        ),
        ShutdownTrigger::CoordinatorFailure => {
            let Err(oxana::OxanaError::JsonError(error)) = result else {
                panic!("expected the initiating queue config error, got {result:?}");
            };
            assert_eq!(error.to_string(), "expected value at line 1 column 1");
        }
        ShutdownTrigger::BackgroundFailure => {
            let Err(oxana::OxanaError::DeadpoolRedisError(error)) = result else {
                panic!("expected the initiating Redis error, got {result:?}");
            };
            assert_eq!(error.code(), Some("WRONGTYPE"));
        }
    }
    for _ in 0..2 {
        assert_eq!(
            dropped_rx.try_recv(),
            Ok(()),
            "worker must be dropped before run returns"
        );
    }
    assert!(lifetime.upgrade().is_none(), "runtime context leaked");
    // The channel was never released, including while checking cancellation.
    drop(release_tx);

    for job_id in &job_ids {
        assert!(storage.get_job(job_id).await?.unwrap().meta.resurrect);
    }
    assert_eq!(storage.dead_count().await?, 0);
    let processing_key = format!("{namespace}:processing:{}", old_processes[0]);
    let interrupted: Vec<String> = redis.lrange(&processing_key, 0, -1).await?;
    assert_eq!(interrupted.len(), job_ids.len());
    for job_id in &job_ids {
        assert!(interrupted.contains(job_id));
    }
    // The cancelled jobs are reported to the caller, read from Redis.
    let report = report.take().expect("timeout must be reported");
    assert!(report.read);
    let mut reported = report.interrupted.clone();
    reported.sort();
    let mut expected = interrupted.clone();
    expected.sort();
    assert_eq!(reported, expected);
    let heartbeat: Option<f64> = redis.zscore(&processes_key, &old_processes[0]).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let later: Option<f64> = redis.zscore(&processes_key, &old_processes[0]).await?;
    assert_eq!(
        heartbeat, later,
        "heartbeat must stop after execution stops"
    );

    storage.reset_queue_config(QueueOne).await?;
    if matches!(trigger, ShutdownTrigger::BackgroundFailure) {
        let _: () = redis.del(format!("{namespace}:schedule")).await?;
    }
    // Building a new Storage gives the replacement a distinct process identity.
    let replacement = oxana::Storage::builder()
        .namespace(namespace)
        .build_from_pool(pool)?;
    let runtime = replacement
        .runtime(())
        .queue::<QueueOne>()
        .queue::<QueueTwo>()
        .worker::<RecoveredShutdownWorker, BlockedShutdownJob>()
        .heartbeat_interval(Duration::from_millis(25))
        .dead_process_threshold(Duration::from_millis(200))
        .resurrect_scan_interval(Duration::from_millis(25))
        .dequeue_timeout(Duration::from_millis(25))
        .exit_when_processed(job_ids.len() as u64);
    let mut replacement_runner = tokio::spawn(runtime.run());
    let stats = tokio::time::timeout(Duration::from_secs(5), &mut replacement_runner).await???;
    assert_eq!(stats.processed, job_ids.len() as u64);
    for job_id in &job_ids {
        assert!(replacement.get_job(job_id).await?.is_none());
    }
    assert_eq!(replacement.dead_count().await?, 0);
    Ok(())
}

#[tokio::test]
async fn test_shutdown_deadline_cancels_workers_and_recovers_jobs() -> TestResult {
    bounded_shutdown::<false>(ShutdownTrigger::Signal).await
}

#[tokio::test]
async fn test_shutdown_deadline_cancels_batches_and_recovers_jobs() -> TestResult {
    bounded_shutdown::<true>(ShutdownTrigger::Signal).await
}

#[tokio::test]
async fn test_shutdown_deadline_preserves_coordinator_error() -> TestResult {
    bounded_shutdown::<false>(ShutdownTrigger::CoordinatorFailure).await
}

#[tokio::test]
async fn test_shutdown_deadline_preserves_error_with_blocked_batches() -> TestResult {
    bounded_shutdown::<true>(ShutdownTrigger::CoordinatorFailure).await
}

#[tokio::test]
async fn test_shutdown_deadline_preserves_background_redis_error() -> TestResult {
    bounded_shutdown::<true>(ShutdownTrigger::BackgroundFailure).await
}

#[derive(Clone)]
struct CutCompletionState {
    pool: deadpool_redis::Pool,
    started: Arc<Notify>,
    go: Arc<Notify>,
    hold: Duration,
}

struct CutCompletionWorker(CutCompletionState);

impl oxana::FromContext<CutCompletionState> for CutCompletionWorker {
    fn from_context(ctx: &CutCompletionState) -> Self {
        Self(ctx.clone())
    }
}

#[async_trait::async_trait]
impl oxana::Worker<BlockedShutdownJob> for CutCompletionWorker {
    type Error = std::io::Error;

    async fn process(
        &self,
        _job: BlockedShutdownJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), Self::Error> {
        self.0.started.notify_one();
        self.0.go.notified().await;
        // Take the pool's only connection with us: the handler returns, but
        // the runtime cannot write the completion until the connection is back.
        let connection = self.0.pool.get().await.map_err(std::io::Error::other)?;
        let hold = self.0.hold;
        tokio::spawn(async move {
            let _connection = connection;
            tokio::time::sleep(hold).await;
        });
        Ok(())
    }
}

#[tokio::test]
async fn test_shutdown_timeout_reports_a_job_whose_completion_was_cut() -> TestResult {
    setup();
    let namespace = random_string();
    let mut cfg = deadpool_redis::Config::from_url(std::env::var("REDIS_URL")?);
    cfg.pool = Some(deadpool_redis::PoolConfig {
        max_size: 1,
        timeouts: deadpool_redis::Timeouts {
            wait: Some(Duration::from_secs(5)),
            create: Some(Duration::from_secs(1)),
            recycle: Some(Duration::from_secs(1)),
        },
        ..Default::default()
    });
    let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
    let storage = oxana::Storage::builder()
        .namespace(namespace.clone())
        .build_from_pool(pool.clone())?;
    let state = CutCompletionState {
        pool: pool.clone(),
        started: Arc::new(Notify::new()),
        go: Arc::new(Notify::new()),
        hold: Duration::from_millis(1500),
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (report, capture) = capture_shutdown_report();
    let runtime = storage
        .runtime(state.clone())
        .queue::<QueueOne>()
        .worker::<CutCompletionWorker, BlockedShutdownJob>()
        .heartbeat_interval(Duration::from_millis(50))
        .shutdown_timeout(Duration::from_millis(300))
        .on_shutdown_timeout(capture)
        .shutdown_on(async move {
            shutdown_rx.await.unwrap();
            Ok(())
        });
    let job_id = storage.enqueue(QueueOne, BlockedShutdownJob).await?;
    let runner = tokio::spawn(runtime.run());

    tokio::time::timeout(Duration::from_secs(5), state.started.notified()).await?;
    shutdown_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    state.go.notify_one();

    let result = tokio::time::timeout(Duration::from_secs(10), runner).await??;
    assert!(
        matches!(result, Err(oxana::OxanaError::ShutdownTimeout)),
        "{result:?}"
    );

    // The handler returned, but the completion never reached Redis: the job
    // is still filed for the next process to run again, and the caller is told.
    assert!(storage.get_job(&job_id).await?.is_some());
    let mut redis = crate::shared::redis_pool().get().await?;
    let processes: Vec<String> = redis
        .zrange(format!("{namespace}:processes"), 0, -1)
        .await?;
    assert_eq!(processes.len(), 1, "process record must be kept");
    let interrupted: Vec<String> = redis
        .lrange(format!("{namespace}:processing:{}", processes[0]), 0, -1)
        .await?;
    assert_eq!(interrupted, vec![job_id.clone()]);
    let report = report.take().expect("timeout must be reported");
    assert!(report.read, "the processing list must be read, not guessed");
    assert_eq!(report.interrupted, vec![job_id]);

    Ok(())
}

#[tokio::test]
async fn test_clean_drain_reports_no_shutdown_timeout() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let ctx = WorkerState { redis: redis_pool };
    storage
        .enqueue(
            QueueOne,
            WorkerRedisSetJob {
                key: random_string(),
                value: "value".to_string(),
            },
        )
        .await?;
    let reported = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueOne>()
        .worker::<WorkerRedisSet, WorkerRedisSetJob>()
        .shutdown_timeout(Duration::from_secs(5))
        .on_shutdown_timeout({
            let reported = Arc::clone(&reported);
            move |_| reported.store(true, std::sync::atomic::Ordering::SeqCst)
        })
        .exit_when_processed(1);

    let stats = tokio::time::timeout(Duration::from_secs(5), runtime.run()).await??;

    assert_eq!(stats.processed, 1);
    assert!(!reported.load(std::sync::atomic::Ordering::SeqCst));

    Ok(())
}

#[tokio::test]
async fn test_shutdown_timeout_beyond_instant_range_drains_without_deadline() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let ctx = WorkerState { redis: redis_pool };
    let job_id = storage
        .enqueue(
            QueueOne,
            WorkerRedisSetJob {
                key: random_string(),
                value: "value".to_string(),
            },
        )
        .await?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueOne>()
        .worker::<WorkerRedisSet, WorkerRedisSetJob>()
        // "Wait for the jobs however long": too large to add to an Instant.
        .shutdown_timeout(Duration::from_secs(u64::MAX))
        .exit_when_processed(1);

    let stats = tokio::time::timeout(Duration::from_secs(5), runtime.run()).await??;

    assert_eq!(stats.processed, 1);
    assert!(storage.get_job(&job_id).await?.is_none());

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
