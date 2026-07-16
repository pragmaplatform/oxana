use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

use crate::WorkerBatchConfig;
use crate::context::ContextValue;
use crate::error::OxanaError;
use crate::executor::{ExecutionError, ExecutionOutcome};
use crate::job_envelope::JobEnvelope;
use crate::queue::{QueueConcurrency, QueueConfig, QueueKind, QueueRuntimeConfig};
use crate::result_collector::{WorkerResult, WorkerResultKind};
use crate::runtime::Runtime;
use crate::semaphores_map::{QueueControlsMap, QueuePermit};
use crate::worker_event::WorkerJob;
use crate::{
    dispatcher, executor,
    result_collector::{self, Stats},
};

pub async fn run<DT>(
    config: Arc<Runtime<DT>>,
    stats: Arc<Mutex<Stats>>,
    ctx: ContextValue<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let channel_capacity = queue_config.concurrency.default_concurrency().max(1);
    let (result_tx, result_rx) = mpsc::channel::<WorkerResult>(channel_capacity);
    let (job_tx, mut job_rx) = mpsc::channel::<WorkerJob>(channel_capacity);
    let (batch_error_tx, mut batch_error_rx) = mpsc::channel::<OxanaError>(channel_capacity);
    let (queue_task_error_tx, mut queue_task_error_rx) =
        mpsc::channel::<OxanaError>(channel_capacity);
    let queue_controls = Arc::new(QueueControlsMap::new());
    let mut joinset = JoinSet::new();
    let mut batchers: HashMap<BatchKey, mpsc::Sender<PendingJob>> = HashMap::new();

    joinset.spawn(result_collector::run(
        result_rx,
        Arc::clone(&config),
        Arc::clone(&stats),
    ));
    joinset.spawn(run_queue_watcher(
        Arc::clone(&config),
        queue_config.clone(),
        job_tx.clone(),
        Arc::clone(&queue_controls),
        queue_task_error_tx,
    ));

    loop {
        tokio::select! {
            job = job_rx.recv() => {
                if let Some(job) = job {
                    route_job(
                        Arc::clone(&config),
                        ctx.clone(),
                        result_tx.clone(),
                        batch_error_tx.clone(),
                        job,
                        &mut batchers,
                        &mut joinset,
                    )
                    .await?;
                }
            }
            Some(task_result) = joinset.join_next() => {
                task_result??;
            }
            batch_error = batch_error_rx.recv() => {
                if let Some(error) = batch_error {
                    return Err(error);
                }
            }
            queue_task_error = queue_task_error_rx.recv() => {
                match queue_task_error {
                    Some(error) => return Err(error),
                    None if config.cancel_token.is_cancelled() => break,
                    None => {
                        return Err(OxanaError::GenericError(
                            "Queue task monitor closed unexpectedly".to_string(),
                        ));
                    }
                }
            }
            _ = config.cancel_token.cancelled() => {
                break;
            }
        }
    }

    drop(batchers);
    wait_for_workers_to_finish(config, Arc::clone(&queue_controls)).await;
    drop(result_tx);
    drop(batch_error_tx);

    while let Some(task_result) = joinset.join_next().await {
        task_result??;
    }

    Ok(())
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct BatchKey {
    worker: String,
    queue: String,
}

impl BatchKey {
    fn from_envelope(envelope: &JobEnvelope) -> Self {
        Self {
            worker: envelope.job.name.clone(),
            queue: envelope.queue.clone(),
        }
    }
}

struct PendingJob {
    envelope: JobEnvelope,
    permit: QueuePermit,
}

async fn route_job<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batch_error_tx: mpsc::Sender<OxanaError>,
    job_event: WorkerJob,
    batchers: &mut HashMap<BatchKey, mpsc::Sender<PendingJob>>,
    joinset: &mut JoinSet<Result<(), OxanaError>>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let pending = match load_pending_job(Arc::clone(&config), job_event).await? {
        Some(pending) => pending,
        None => return Ok(()),
    };

    if let Some(batch_config) = config.registry.batch_config(&pending.envelope.job.name) {
        send_to_batcher(
            config,
            ctx,
            result_tx,
            batch_error_tx,
            batchers,
            pending,
            batch_config,
        )
        .await;
        return Ok(());
    }

    joinset.spawn(process_pending_job(config, ctx, result_tx, pending));

    Ok(())
}

async fn load_pending_job<DT>(
    config: Arc<Runtime<DT>>,
    job_event: WorkerJob,
) -> Result<Option<PendingJob>, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    tracing::trace!("Processing job: {:?}", job_event);

    let envelope: JobEnvelope = match config.storage.internal.get_job(&job_event.job_id).await {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            tracing::warn!("Job {} not found", job_event.job_id);
            if let Err(e) = config.storage.internal.delete_job(&job_event.job_id).await {
                #[cfg(feature = "sentry")]
                sentry_core::capture_error(&e);
                tracing::error!("Failed to delete job: {}", e);
            }
            return Ok(None);
        }
        Err(e) => {
            #[cfg(feature = "sentry")]
            sentry_core::capture_error(&e);
            tracing::error!("Failed to get job envelope: {}", e);
            return Ok(None);
        }
    };

    tracing::debug!(
        job_id = envelope.id,
        latency_ms = envelope.meta.latency_millis(),
        envelope = %serde_json::to_value(&envelope)?,
        "Received envelope"
    );

    Ok(Some(PendingJob {
        envelope,
        permit: job_event.permit,
    }))
}

async fn process_pending_job<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    pending: PendingJob,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut envelope = pending.envelope;
    let job = match config
        .registry
        .build(&envelope.job.name, envelope.job.args.clone(), &ctx.0)
    {
        Ok(job) => job,
        Err(e) => {
            let err_msg = format!("Invalid job: {} - {}", envelope.job.name, e);
            tracing::error!("{}", err_msg);
            if let Err(e) = config.storage.internal.kill(&envelope, err_msg).await {
                #[cfg(feature = "sentry")]
                sentry_core::capture_error(&e);
                tracing::error!("Failed to kill job: {}", e);
            }
            return Ok(());
        }
    };

    let result = executor::run(Arc::clone(&config), job, &mut envelope).await?;
    drop(pending.permit);
    process_result(result_tx, result, vec![envelope]).await;

    Ok(())
}

async fn send_to_batcher<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batch_error_tx: mpsc::Sender<OxanaError>,
    batchers: &mut HashMap<BatchKey, mpsc::Sender<PendingJob>>,
    pending: PendingJob,
    batch_config: WorkerBatchConfig,
) where
    DT: Send + Sync + Clone + 'static,
{
    let key = BatchKey::from_envelope(&pending.envelope);
    let mut pending = pending;

    loop {
        let tx = batchers
            .entry(key.clone())
            .or_insert_with(|| {
                spawn_batcher(
                    Arc::clone(&config),
                    ctx.clone(),
                    result_tx.clone(),
                    batch_error_tx.clone(),
                    batch_config.clone(),
                )
            })
            .clone();

        match tx.send(pending).await {
            Ok(()) => break,
            Err(err) => {
                pending = err.0;
                batchers.remove(&key);
            }
        }
    }
}

fn spawn_batcher<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batch_error_tx: mpsc::Sender<OxanaError>,
    batch_config: WorkerBatchConfig,
) -> mpsc::Sender<PendingJob>
where
    DT: Send + Sync + Clone + 'static,
{
    let batch_size = batch_config.size();
    let (tx, rx) = mpsc::channel(batch_size * 2);
    tokio::spawn(async move {
        if let Err(e) = run_batcher(
            config,
            ctx,
            result_tx,
            batch_error_tx.clone(),
            batch_config,
            rx,
        )
        .await
        {
            tracing::error!(error = %e, "Batcher exited with error");
            batch_error_tx.send(e).await.ok();
        }
    });
    tx
}

async fn run_batcher<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batch_error_tx: mpsc::Sender<OxanaError>,
    batch_config: WorkerBatchConfig,
    mut rx: mpsc::Receiver<PendingJob>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let batch_size = batch_config.size();
    let mut pending = Vec::with_capacity(batch_size);

    loop {
        tokio::select! {
            job = rx.recv(), if pending.is_empty() => {
                match job {
                    Some(job) => pending.push(job),
                    None => return Ok(()),
                }
            }
            _ = config.cancel_token.cancelled(), if pending.is_empty() => {
                flush_batcher(
                    Arc::clone(&config),
                    ctx.clone(),
                    result_tx.clone(),
                    batch_error_tx.clone(),
                    &mut rx,
                    &mut pending,
                    batch_size,
                )
                .await;
                return Ok(());
            }
        }

        if pending.len() >= batch_size || batch_config.timeout().is_zero() {
            spawn_batch(
                Arc::clone(&config),
                ctx.clone(),
                result_tx.clone(),
                batch_error_tx.clone(),
                &mut pending,
            );
            continue;
        }

        let timeout = tokio::time::sleep(batch_config.timeout());
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                job = rx.recv() => {
                    match job {
                        Some(job) => {
                            pending.push(job);
                            if pending.len() >= batch_size {
                                break;
                            }
                        }
                        None => {
                            spawn_batch(
                                Arc::clone(&config),
                                ctx.clone(),
                                result_tx.clone(),
                                batch_error_tx.clone(),
                                &mut pending,
                            );
                            return Ok(());
                        }
                    }
                }
                _ = &mut timeout => {
                    break;
                }
                _ = config.cancel_token.cancelled() => {
                    flush_batcher(
                        Arc::clone(&config),
                        ctx.clone(),
                        result_tx.clone(),
                        batch_error_tx.clone(),
                        &mut rx,
                        &mut pending,
                        batch_size,
                    )
                    .await;
                    return Ok(());
                }
            }
        }

        spawn_batch(
            Arc::clone(&config),
            ctx.clone(),
            result_tx.clone(),
            batch_error_tx.clone(),
            &mut pending,
        );
    }
}

async fn flush_batcher<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batch_error_tx: mpsc::Sender<OxanaError>,
    rx: &mut mpsc::Receiver<PendingJob>,
    pending: &mut Vec<PendingJob>,
    batch_size: usize,
) where
    DT: Send + Sync + Clone + 'static,
{
    rx.close();

    while let Some(job) = rx.recv().await {
        pending.push(job);
        if pending.len() >= batch_size {
            spawn_batch(
                Arc::clone(&config),
                ctx.clone(),
                result_tx.clone(),
                batch_error_tx.clone(),
                pending,
            );
        }
    }

    spawn_batch(config, ctx, result_tx, batch_error_tx, pending);
}

fn spawn_batch<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batch_error_tx: mpsc::Sender<OxanaError>,
    pending: &mut Vec<PendingJob>,
) where
    DT: Send + Sync + Clone + 'static,
{
    if pending.is_empty() {
        return;
    }

    let batch = std::mem::take(pending);
    tokio::spawn(async move {
        if let Err(e) = process_pending_batch(config, ctx, result_tx, batch).await {
            tracing::error!(error = %e, "Failed to process job batch");
            batch_error_tx.send(e).await.ok();
        }
    });
}

async fn process_pending_batch<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    pending: Vec<PendingJob>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut envelopes = Vec::with_capacity(pending.len());
    let mut permits = Vec::with_capacity(pending.len());
    for pending_job in pending {
        envelopes.push(pending_job.envelope);
        permits.push(pending_job.permit);
    }

    let Some(first_envelope) = envelopes.first() else {
        return Ok(());
    };
    let worker_name = first_envelope.job.name.clone();
    let args = envelopes
        .iter()
        .map(|envelope| envelope.job.args.clone())
        .collect();
    let batch = match config.registry.build_batch(&worker_name, args, &ctx.0) {
        Ok(batch) => batch,
        Err(e) => {
            let err_msg = format!("Invalid job batch: {worker_name} - {e}");
            tracing::error!("{}", err_msg);
            for envelope in &envelopes {
                if let Err(e) = config
                    .storage
                    .internal
                    .kill(envelope, err_msg.clone())
                    .await
                {
                    #[cfg(feature = "sentry")]
                    sentry_core::capture_error(&e);
                    tracing::error!("Failed to kill job: {}", e);
                }
            }
            return Ok(());
        }
    };

    let invalid_by_index = invalid_jobs_by_index(batch.invalid, envelopes.len());
    if !invalid_by_index.is_empty() {
        let mut valid_envelopes =
            Vec::with_capacity(envelopes.len().saturating_sub(invalid_by_index.len()));
        let mut valid_permits =
            Vec::with_capacity(permits.len().saturating_sub(invalid_by_index.len()));

        for (index, (envelope, permit)) in envelopes.into_iter().zip(permits).enumerate() {
            if let Some(error) = invalid_by_index.get(&index) {
                let err_msg = format!("Invalid job: {worker_name} - {error}");
                tracing::error!("{}", err_msg);

                if let Err(e) = config.storage.internal.kill(&envelope, err_msg).await {
                    #[cfg(feature = "sentry")]
                    sentry_core::capture_error(&e);
                    tracing::error!("Failed to kill job: {}", e);
                }

                drop(permit);
            } else {
                valid_envelopes.push(envelope);
                valid_permits.push(permit);
            }
        }

        envelopes = valid_envelopes;
        permits = valid_permits;
    }

    let Some(job) = batch.job else {
        return Ok(());
    };

    let outcome = executor::run_batch(Arc::clone(&config), job, &mut envelopes).await?;
    drop(permits);
    process_result(result_tx, outcome, envelopes).await;

    Ok(())
}

fn invalid_jobs_by_index(
    invalid_jobs: Vec<crate::worker_registry::InvalidBatchJob>,
    envelope_count: usize,
) -> HashMap<usize, String> {
    let mut invalid_by_index = HashMap::new();
    for invalid in invalid_jobs {
        if invalid.index < envelope_count {
            invalid_by_index
                .entry(invalid.index)
                .or_insert(invalid.error);
        }
    }
    invalid_by_index
}

async fn process_result(
    result_tx: mpsc::Sender<WorkerResult>,
    outcome: ExecutionOutcome,
    envelopes: Vec<JobEnvelope>,
) {
    let kind = match outcome.result {
        Ok(()) => WorkerResultKind::Success,
        Err(e) => match e {
            ExecutionError::NotPanic => WorkerResultKind::Failed,
            ExecutionError::Panic() => WorkerResultKind::Panicked,
        },
    };

    let job_count = u64::try_from(envelopes.len()).unwrap_or(u64::MAX);
    let Some(first_envelope) = envelopes.into_iter().next() else {
        tracing::warn!("Worker result with no envelopes, dropping {:?}", kind);
        return;
    };

    result_tx
        .send(WorkerResult {
            kind,
            worker_name: first_envelope.job.name,
            queue: first_envelope.queue,
            execution_ms: outcome.duration_ms,
            job_count,
        })
        .await
        .ok();
}

async fn run_queue_watcher<DT>(
    config: Arc<Runtime<DT>>,
    queue_config: QueueConfig,
    job_tx: mpsc::Sender<WorkerJob>,
    queue_controls: Arc<QueueControlsMap>,
    queue_task_error_tx: mpsc::Sender<OxanaError>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut tracked_queues = HashSet::new();
    let queue_concurrency = queue_config.concurrency;
    let default_runtime_config = queue_concurrency.stored_runtime_default();
    let base_queue_key = queue_config.key_or_prefix();

    loop {
        let all_queues: HashSet<String> = match &queue_config.kind {
            QueueKind::Static { key } => HashSet::from([key.clone()]),
            QueueKind::Dynamic { prefix, .. } => config
                .storage
                .internal
                .track_redis_result(
                    config.storage.internal.queues(&format!("{prefix}*")).await,
                    config.settings.redis_failure_tolerance,
                )?
                .map(|queues| queues.into_iter().collect())
                .unwrap_or_default(),
        };
        let new_queues: HashSet<String> = all_queues.difference(&tracked_queues).cloned().collect();

        for queue in new_queues {
            tracing::info!(
                queue = queue,
                config.throttle = format!("{:?}", queue_config.throttle),
                "Tracking queue"
            );

            let runtime_config = runtime_queue_config(
                &config,
                &queue,
                &base_queue_key,
                default_runtime_config.clone(),
            )
            .await?;
            let runtime_config = queue_concurrency.effective_runtime_config(runtime_config);
            let queue_control = queue_controls
                .get_or_create(queue.clone(), runtime_config.clone())
                .await;
            queue_control.apply_config(runtime_config);

            let watcher_config = Arc::clone(&config);
            let watcher_queue = queue.clone();
            let watcher_control = Arc::clone(&queue_control);
            let watcher_base_queue_key = base_queue_key.clone();
            let watcher_default_runtime_config = default_runtime_config.clone();
            spawn_queue_task(
                "queue config watcher",
                queue_task_error_tx.clone(),
                run_queue_config_watcher(
                    watcher_config,
                    watcher_queue,
                    watcher_base_queue_key,
                    watcher_default_runtime_config,
                    queue_concurrency,
                    watcher_control,
                ),
            );

            let dispatcher_config = Arc::clone(&config);
            let dispatcher_queue_config = queue_config.clone();
            let dispatcher_job_tx = job_tx.clone();
            let dispatcher_queue_control = Arc::clone(&queue_control);
            let dispatcher_queue = queue.clone();
            spawn_queue_task(
                "dispatcher",
                queue_task_error_tx.clone(),
                dispatcher::run(
                    dispatcher_config,
                    dispatcher_queue_config,
                    dispatcher_queue,
                    dispatcher_job_tx,
                    dispatcher_queue_control,
                ),
            );

            tracked_queues.insert(queue);
        }

        if config.cancel_token.is_cancelled() {
            return Ok(());
        } else if let QueueKind::Dynamic {
            discovery_interval, ..
        } = queue_config.kind
        {
            tokio::time::sleep(discovery_interval).await;
        } else {
            return Ok(());
        }
    }
}

fn spawn_queue_task<F>(task: &'static str, error_tx: mpsc::Sender<OxanaError>, future: F)
where
    F: Future<Output = Result<(), OxanaError>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = future.await {
            tracing::error!(task, error = %error, "Queue task exited with error");
            let _ = error_tx.try_send(error);
        }
    });
}

async fn runtime_queue_config<DT>(
    config: &Runtime<DT>,
    queue_key: &str,
    base_queue_key: &str,
    default_runtime_config: QueueRuntimeConfig,
) -> Result<QueueRuntimeConfig, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    if queue_key == base_queue_key {
        return config
            .storage
            .internal
            .queue_config_or_default(queue_key, default_runtime_config)
            .await;
    }

    let base_runtime_config = config
        .storage
        .internal
        .queue_config_or_default(base_queue_key, default_runtime_config)
        .await?;

    config
        .storage
        .internal
        .queue_config_or_default(queue_key, base_runtime_config)
        .await
}

async fn run_queue_config_watcher<DT>(
    config: Arc<Runtime<DT>>,
    queue_key: String,
    base_queue_key: String,
    default_runtime_config: QueueRuntimeConfig,
    queue_concurrency: QueueConcurrency,
    queue_control: Arc<crate::semaphores_map::QueueControl>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = config.cancel_token.cancelled() => {
                return Ok(());
            }
            _ = interval.tick() => {
                if let Some(runtime_config) = config.storage.internal.track_redis_result(
                    runtime_queue_config(
                        &config,
                        &queue_key,
                        &base_queue_key,
                        default_runtime_config.clone(),
                    )
                    .await,
                    config.settings.redis_failure_tolerance,
                )? {
                    let runtime_config = queue_concurrency.effective_runtime_config(runtime_config);
                    queue_control.apply_config(runtime_config);
                }
            }
        }
    }
}

async fn wait_for_workers_to_finish<DT>(
    config: Arc<Runtime<DT>>,
    queue_controls: Arc<QueueControlsMap>,
) where
    DT: Send + Sync + Clone + 'static,
{
    let t_start = std::time::Instant::now();
    let mut ticks = 0;

    loop {
        ticks += 1;

        let busy_count = queue_controls.busy_count().await;
        if busy_count == 0 {
            break;
        }

        if ticks % 200 == 0 {
            tracing::info!("Waiting for {} workers to finish...", busy_count);
        }

        if t_start.elapsed() > config.settings.shutdown_timeout {
            tracing::error!("Shutdown timeout reached");
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingJob, process_pending_batch, spawn_queue_task};
    use crate::QueueRuntimeConfig;
    use crate::config::{Config, RuntimeSettings};
    use crate::context::ContextValue;
    use crate::runtime::Runtime;
    use crate::semaphores_map::QueueControlsMap;
    use crate::test_helper::{random_string, redis_pool};
    use crate::worker_registry::{self, BatchBuild, InvalidBatchJob, WorkerConfigKind};
    use crate::{Job, JobEnvelope, Storage, Worker, WorkerConfig};
    use serde::{Deserialize, Serialize};
    use std::{sync::Arc, time::Duration};
    use testresult::TestResult;
    use tokio::sync::mpsc;

    #[derive(Debug, Serialize, Deserialize)]
    struct UnsortedInvalidJob;

    impl Job for UnsortedInvalidJob {}

    struct UnsortedInvalidWorker;

    impl crate::FromContext<()> for UnsortedInvalidWorker {
        fn from_context(_ctx: &()) -> Self {
            Self
        }
    }

    #[async_trait::async_trait]
    impl Worker<UnsortedInvalidJob> for UnsortedInvalidWorker {
        type Error = std::io::Error;

        async fn run_batch(
            &self,
            _jobs: Vec<crate::BatchItem<UnsortedInvalidJob>>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn unsorted_invalid_batch_factory(
        _values: Vec<serde_json::Value>,
        _ctx: &(),
    ) -> Result<BatchBuild, crate::OxanaError> {
        Ok(BatchBuild {
            job: None,
            invalid: vec![
                InvalidBatchJob {
                    index: 1,
                    error: "second invalid".to_string(),
                },
                InvalidBatchJob {
                    index: 0,
                    error: "first invalid".to_string(),
                },
            ],
        })
    }

    #[tokio::test]
    async fn process_pending_batch_handles_unsorted_invalid_indexes() -> TestResult {
        let pool = redis_pool().await?;
        let storage = Storage::builder()
            .namespace(random_string())
            .build_from_pool(pool)?;
        let queue = random_string();
        let mut config = Config::new();
        config.register_worker_with(WorkerConfig {
            name: UnsortedInvalidJob::name().to_string(),
            legacy_names: Vec::new(),
            factory: worker_registry::job_factory::<UnsortedInvalidWorker, UnsortedInvalidJob, ()>,
            batch_factory: unsorted_invalid_batch_factory,
            batch_config: Some(crate::WorkerBatchConfig::new(
                2,
                std::time::Duration::from_millis(100),
            )),
            on_demand: None,
            kind: WorkerConfigKind::Normal,
        });

        let envelopes = vec![
            JobEnvelope::new(queue.clone(), UnsortedInvalidJob)?,
            JobEnvelope::new(queue.clone(), UnsortedInvalidJob)?,
        ];
        for envelope in &envelopes {
            storage.internal.enqueue(envelope.clone()).await?;
        }
        for _ in &envelopes {
            storage.internal.dequeue(&queue).await?;
        }

        let queue_controls = QueueControlsMap::new();
        let queue_control = queue_controls
            .get_or_create(queue.clone(), QueueRuntimeConfig::new(envelopes.len()))
            .await;
        let mut pending = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            pending.push(PendingJob {
                envelope,
                permit: queue_control.acquire().await,
            });
        }
        let (result_tx, _result_rx) = mpsc::channel(1);

        process_pending_batch(
            Arc::new(Runtime::new(
                storage.clone(),
                config,
                RuntimeSettings::new(),
            )),
            ContextValue::new(()),
            result_tx,
            pending,
        )
        .await?;

        assert_eq!(storage.dead_count().await?, 2);
        assert_eq!(storage.jobs_count().await?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn queue_task_errors_are_forwarded() -> TestResult {
        let (error_tx, mut error_rx) = mpsc::channel(1);

        spawn_queue_task("test queue task", error_tx, async {
            Err(crate::OxanaError::GenericError(
                "dispatcher failed".to_string(),
            ))
        });

        let error = tokio::time::timeout(Duration::from_secs(1), error_rx.recv())
            .await?
            .expect("queue task error should be forwarded");

        assert!(matches!(
            error,
            crate::OxanaError::GenericError(message) if message == "dispatcher failed"
        ));

        Ok(())
    }
}
