use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use deadpool_redis::PoolConfig;
use divan::counter::ItemsCount;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

fn main() {
    divan::main();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerNoopJob {
    pub sleep_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Generic error: {0}")]
    GenericError(String),
}

#[derive(Debug, Clone)]
pub struct EndToEndContext {}

#[derive(oxana::Worker)]
#[oxana(
    job = WorkerNoopJob,
    context = EndToEndContext,
    error = ServiceError,
    registry = None
)]
pub struct EndToEndWorker;

impl oxana::Job for WorkerNoopJob {}

impl EndToEndWorker {
    async fn process(
        &self,
        job: WorkerNoopJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), ServiceError> {
        tokio::time::sleep(std::time::Duration::from_millis(job.sleep_ms)).await;
        Ok(())
    }
}

type SteadyStateContext = Arc<SteadyStateControl>;

#[derive(oxana::Worker)]
#[oxana(
    job = WorkerNoopJob,
    context = SteadyStateContext,
    error = ServiceError,
    registry = None
)]
pub struct SteadyStateWorker {
    control: SteadyStateContext,
}

impl SteadyStateWorker {
    async fn process(
        &self,
        job: WorkerNoopJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), ServiceError> {
        self.control.wait_for_measurement().await;
        tokio::time::sleep(std::time::Duration::from_millis(job.sleep_ms)).await;
        self.control.record_completion();
        Ok(())
    }
}

#[derive(Serialize)]
pub struct QueueOne;

impl oxana::Queue for QueueOne {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig {
            kind: oxana::QueueKind::Static {
                key: "one".to_string(),
            },
            concurrency: oxana::QueueConcurrency::Fixed(1),
            throttle: None,
        }
    }
}

const DEFAULT_JOBS_COUNT: u64 = 1000;
const SAMPLE_COUNT: u32 = 5;

#[derive(Clone, Copy)]
struct BenchmarkCase {
    concurrency: usize,
    jobs_count: u64,
}

impl BenchmarkCase {
    const fn new(concurrency: usize, jobs_count: u64) -> Self {
        Self {
            concurrency,
            jobs_count,
        }
    }
}

impl fmt::Display for BenchmarkCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} workers, {} jobs", self.concurrency, self.jobs_count)
    }
}

const ZERO_MS_CASES: &[BenchmarkCase] = &[
    BenchmarkCase::new(1, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(2, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(4, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(8, 8000),
    BenchmarkCase::new(12, 8000),
    BenchmarkCase::new(16, 8000),
    BenchmarkCase::new(512, 8000),
];

const ONE_MS_CASES: &[BenchmarkCase] = &[
    BenchmarkCase::new(1, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(2, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(4, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(8, 6000),
    BenchmarkCase::new(12, 8000),
    BenchmarkCase::new(16, 8000),
    BenchmarkCase::new(512, 8000),
];

const TWO_MS_CASES: &[BenchmarkCase] = &[
    BenchmarkCase::new(1, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(2, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(4, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(8, 5000),
    BenchmarkCase::new(12, 7000),
    BenchmarkCase::new(16, 8000),
    BenchmarkCase::new(512, 8000),
];

const TEN_MS_CASES: &[BenchmarkCase] = &[
    BenchmarkCase::new(1, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(2, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(4, DEFAULT_JOBS_COUNT),
    BenchmarkCase::new(8, 2000),
    BenchmarkCase::new(12, 3000),
    BenchmarkCase::new(16, 4000),
    BenchmarkCase::new(512, 8000),
];

struct EndToEndInput {
    storage: oxana::Storage,
    case: BenchmarkCase,
}

macro_rules! bench_end_to_end_jobs {
    ($name:ident, $sleep_ms:expr, $cases:expr) => {
        #[divan::bench(args = $cases, sample_size = 1, sample_count = SAMPLE_COUNT)]
        fn $name(bencher: divan::Bencher, case: BenchmarkCase) {
            let rt = &tokio::runtime::Runtime::new().unwrap();
            let storage = build_storage();

            bencher
                .with_inputs(|| {
                    rt.block_on(async {
                        setup(&storage, case.jobs_count, $sleep_ms).await.unwrap();
                    });
                    EndToEndInput {
                        storage: storage.clone(),
                        case,
                    }
                })
                .input_counter(|input| ItemsCount::new(input.case.jobs_count))
                .bench_local_values(|input| {
                    rt.block_on(async {
                        execute_end_to_end(input.storage, input.case).await.unwrap();
                    })
                });
        }
    };
}

mod end_to_end {
    use super::*;

    bench_end_to_end_jobs!(jobs_taking_0_ms, 0, ZERO_MS_CASES);
    bench_end_to_end_jobs!(jobs_taking_1_ms, 1, ONE_MS_CASES);
    bench_end_to_end_jobs!(jobs_taking_2_ms, 2, TWO_MS_CASES);
    bench_end_to_end_jobs!(jobs_taking_10_ms, 10, TEN_MS_CASES);
}

macro_rules! bench_steady_state_jobs {
    ($name:ident, $sleep_ms:expr, $cases:expr) => {
        #[divan::bench(args = $cases, sample_size = 1, sample_count = SAMPLE_COUNT)]
        fn $name(bencher: divan::Bencher, case: BenchmarkCase) {
            let rt = &tokio::runtime::Runtime::new().unwrap();
            let storage = build_storage();

            bencher
                .with_inputs(|| {
                    rt.block_on(SteadyStateFixture::prepare(
                        storage.clone(),
                        case,
                        $sleep_ms,
                    ))
                })
                .input_counter(|fixture| ItemsCount::new(fixture.case.jobs_count))
                .bench_local_values(|fixture| {
                    rt.block_on(fixture.measure());
                    SteadyStateCleanup {
                        rt,
                        fixture: Some(fixture),
                    }
                });
        }
    };
}

mod steady_state {
    use super::*;

    bench_steady_state_jobs!(jobs_taking_0_ms, 0, ZERO_MS_CASES);
    bench_steady_state_jobs!(jobs_taking_1_ms, 1, ONE_MS_CASES);
    bench_steady_state_jobs!(jobs_taking_2_ms, 2, TWO_MS_CASES);
    bench_steady_state_jobs!(jobs_taking_10_ms, 10, TEN_MS_CASES);
}

struct SteadyStateControl {
    concurrency: usize,
    jobs_count: u64,
    started: AtomicUsize,
    completed: AtomicU64,
    ready: Semaphore,
    gate: Semaphore,
    finished: Semaphore,
}

impl SteadyStateControl {
    fn new(case: BenchmarkCase) -> Self {
        Self {
            concurrency: case.concurrency,
            jobs_count: case.jobs_count,
            started: AtomicUsize::new(0),
            completed: AtomicU64::new(0),
            ready: Semaphore::new(0),
            gate: Semaphore::new(0),
            finished: Semaphore::new(0),
        }
    }

    async fn wait_for_measurement(&self) {
        // Hold the first worker cohort at the gate so runtime startup and queue
        // discovery finish before Divan starts timing.
        if self.started.fetch_add(1, Ordering::Relaxed) + 1 == self.concurrency {
            self.ready.add_permits(1);
        }

        self.gate
            .acquire()
            .await
            .expect("steady-state benchmark gate was closed")
            .forget();
    }

    async fn wait_until_ready(&self) {
        self.ready
            .acquire()
            .await
            .expect("steady-state benchmark readiness semaphore was closed")
            .forget();
    }

    fn start_measurement(&self) {
        self.gate.add_permits(self.jobs_count as usize);
    }

    fn record_completion(&self) {
        if self.completed.fetch_add(1, Ordering::Relaxed) + 1 == self.jobs_count {
            self.finished.add_permits(1);
        }
    }

    async fn wait_until_finished(&self) {
        self.finished
            .acquire()
            .await
            .expect("steady-state benchmark completion semaphore was closed")
            .forget();
    }
}

struct SteadyStateFixture {
    case: BenchmarkCase,
    control: SteadyStateContext,
    runtime: JoinHandle<Result<oxana::RunStats, oxana::OxanaError>>,
}

impl SteadyStateFixture {
    async fn prepare(storage: oxana::Storage, case: BenchmarkCase, sleep_ms: u64) -> Self {
        setup(&storage, case.jobs_count, sleep_ms).await.unwrap();

        let control = Arc::new(SteadyStateControl::new(case));
        let runtime = storage
            .runtime(Arc::clone(&control))
            .queue_with_concurrency::<QueueOne>(case.concurrency)
            .worker::<SteadyStateWorker, WorkerNoopJob>()
            .exit_when_processed(case.jobs_count);
        let mut runtime = tokio::spawn(runtime.run());

        tokio::select! {
            () = control.wait_until_ready() => {}
            result = &mut runtime => {
                panic!("steady-state benchmark runtime exited during setup: {result:?}");
            }
        }

        Self {
            case,
            control,
            runtime,
        }
    }

    async fn measure(&self) {
        self.control.start_measurement();
        self.control.wait_until_finished().await;
    }
}

struct SteadyStateCleanup<'a> {
    rt: &'a tokio::runtime::Runtime,
    fixture: Option<SteadyStateFixture>,
}

impl Drop for SteadyStateCleanup<'_> {
    fn drop(&mut self) {
        // Divan drops benchmark outputs after stopping its timer. Waiting for
        // Oxana's result accounting and shutdown here keeps both out of the
        // steady-state throughput measurement.
        let SteadyStateFixture { case, runtime, .. } = self
            .fixture
            .take()
            .expect("steady-state benchmark fixture was already cleaned up");

        let stats = self
            .rt
            .block_on(runtime)
            .expect("steady-state benchmark runtime task failed")
            .expect("steady-state benchmark runtime failed");
        assert_stats(&stats, case.jobs_count);
    }
}

async fn setup(
    storage: &oxana::Storage,
    jobs_count: u64,
    sleep_ms: u64,
) -> Result<(), oxana::OxanaError> {
    storage
        .enqueue_list(
            QueueOne,
            (0..jobs_count).map(|_| WorkerNoopJob { sleep_ms }),
        )
        .await?;

    Ok(())
}

async fn execute_end_to_end(
    storage: oxana::Storage,
    case: BenchmarkCase,
) -> Result<(), oxana::OxanaError> {
    let runtime = storage
        .runtime(EndToEndContext {})
        .queue_with_concurrency::<QueueOne>(case.concurrency)
        .worker::<EndToEndWorker, WorkerNoopJob>()
        .exit_when_processed(case.jobs_count);

    let stats = runtime.run().await?;
    assert_stats(&stats, case.jobs_count);

    Ok(())
}

fn assert_stats(stats: &oxana::RunStats, jobs_count: u64) {
    assert_eq!(stats.processed, jobs_count);
    assert_eq!(stats.succeeded, jobs_count);
    assert_eq!(stats.failed, 0);
}

fn redis_pool() -> deadpool_redis::Pool {
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL is not set");
    let mut cfg = deadpool_redis::Config::from_url(redis_url);
    cfg.pool = Some(PoolConfig {
        max_size: 512,
        ..Default::default()
    });
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("Failed to create Redis pool")
}

fn build_storage() -> oxana::Storage {
    dotenvy::from_filename(".env.test").ok();
    oxana::Storage::builder()
        .build_from_pool(redis_pool())
        .expect("Failed to build storage")
}
