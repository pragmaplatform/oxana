use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, RuntimeSettings};
use crate::context::ContextValue;
use crate::coordinator;
use crate::error::OxanaError;
use crate::job_envelope::JobId;
use crate::queue::QueueConfig;
use crate::result_collector::Stats;
use crate::runtime::{Runtime, ShutdownTimeoutReport};
use crate::storage::Storage;
use crate::worker_registry::CronJob;

pub(crate) async fn run<DT>(
    storage: Storage,
    config: Config<DT>,
    settings: RuntimeSettings,
    ctx: ContextValue<DT>,
) -> Result<Stats, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    tracing::info!("Starting worker (namespace: {})", storage.namespace());

    let runtime = Runtime::new(storage, config, settings);
    let shutdown_signal = runtime.settings.consume_shutdown_signal();
    let runtime: Arc<Runtime<DT>> = Arc::new(runtime);
    // Dropping run() also cancels descendants whose spawn handles live outside
    // the launcher's JoinSets.
    let _force_cancel_on_drop = runtime.force_cancel_token.clone().drop_guard();
    let mut joinset = JoinSet::new();
    let mut ping_joinset = JoinSet::new();
    let mut coordinator_joinset = JoinSet::new();
    let stats = Arc::new(Mutex::new(Stats::default()));
    let ping_cancel_token = CancellationToken::new();

    ping_joinset.spawn(ping_loop(Arc::clone(&runtime), ping_cancel_token.clone()));
    joinset.spawn(retry_loop(Arc::clone(&runtime)));
    joinset.spawn(schedule_loop(Arc::clone(&runtime)));
    joinset.spawn(resurrect_loop(Arc::clone(&runtime)));
    joinset.spawn(cron_loop(Arc::clone(&runtime)));
    joinset.spawn(cleanup_loop(Arc::clone(&runtime)));

    for queue_config in &runtime.queues {
        if !runtime.settings.runs_queue(queue_config) {
            continue;
        }
        coordinator_joinset.spawn(coordinator_loop(
            Arc::clone(&runtime),
            Arc::clone(&stats),
            ctx.clone(),
            queue_config.clone(),
        ));
    }

    tokio::select! {
        Some(task_result) = joinset.join_next() => {
            record_task_result(&runtime, task_result);
        }
        Some(task_result) = coordinator_joinset.join_next() => {
            record_task_result(&runtime, task_result);
        }
        Some(task_result) = ping_joinset.join_next() => {
            record_task_result(&runtime, task_result);
        }
        _ = runtime.cancel_token.cancelled() => {}
        _ = shutdown_signal => {
            tracing::info!("Received shutdown signal");
        }
    }

    tracing::info!("Shutting down");
    runtime.cancel_token.cancel();
    runtime.tasks.close();

    let drain = async {
        while let Some(task_result) = coordinator_joinset.join_next().await {
            record_task_result(&runtime, task_result);
        }
        while let Some(task_result) = joinset.join_next().await {
            record_task_result(&runtime, task_result);
        }
        // Descendants can outlive their coordinator (notably batch workers).
        // Keep heartbeating until every execution future has been dropped.
        runtime.tasks.wait().await;
        ping_cancel_token.cancel();
        while let Some(task_result) = ping_joinset.join_next().await {
            record_task_result(&runtime, task_result);
        }

        // Only remove registration after execution and heartbeats have stopped.
        // This does not delete processing lists, so interrupted jobs survive.
        if let Err(error) = runtime.storage.internal.self_cleanup().await {
            runtime.fail(error);
        }
    };
    // `timeout` clamps a duration too large to add to an Instant, so a
    // "wait however long" timeout drains without a deadline instead of panicking.
    let drained = tokio::time::timeout(runtime.settings.shutdown_timeout, drain).await;

    if drained.is_err() {
        // A deadline is configuration, not a fault: warn, not error.
        tracing::warn!("Shutdown timeout reached; cancelling remaining owned tasks");
        runtime.force_cancel_token.cancel();
        abort_and_join(&runtime, &mut coordinator_joinset).await;
        abort_and_join(&runtime, &mut joinset).await;
        // Aborting a parent only requests cancellation of its JoinSet children.
        // Wait for their futures to actually drop before stopping heartbeats.
        runtime.tasks.wait().await;
        ping_cancel_token.cancel();
        abort_and_join(&runtime, &mut ping_joinset).await;
        tracing::warn!("Forced cancellation complete; interrupted jobs retained for resurrection");
        // Registration is left to expire naturally, and the replacement
        // process will recover the processing list. Reading that list is the
        // one Redis round trip made after the deadline, and it is bounded.
        report_shutdown_timeout(&runtime).await;
    }

    debug_assert!(runtime.tasks.is_empty());
    debug_assert_eq!(Arc::strong_count(&runtime), 1, "runtime tasks leaked");
    let error = runtime
        .take_shutdown_error()
        .or_else(|| drained.is_err().then_some(OxanaError::ShutdownTimeout));
    let stats = Arc::try_unwrap(stats)
        .expect("Failed to unwrap Arc - there are still references to stats")
        .into_inner();

    match error {
        None => {
            tracing::info!("Gracefully shut down");
            Ok(stats)
        }
        Some(error @ OxanaError::ShutdownTimeout) => {
            tracing::warn!(error = %error, "Shut down at the deadline");
            Err(error)
        }
        Some(error) => {
            tracing::error!(error = %error, "Shut down with error");
            Err(error)
        }
    }
}

/// How long a timed-out shutdown waits to read the interrupted jobs.
const SHUTDOWN_REPORT_TIMEOUT: Duration = Duration::from_secs(5);

/// Reads which jobs this process still holds and hands them to the caller's
/// `on_shutdown_timeout` callback, logging each one.
async fn report_shutdown_timeout<DT>(runtime: &Runtime<DT>)
where
    DT: Send + Sync + Clone + 'static,
{
    let storage = &runtime.storage.internal;
    let interrupted = match tokio::time::timeout(
        SHUTDOWN_REPORT_TIMEOUT,
        storage.processing_job_ids(),
    )
    .await
    {
        Ok(Ok(interrupted)) => Some(interrupted),
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "Could not read the interrupted jobs after the shutdown timeout");
            None
        }
        Err(_) => {
            tracing::warn!("Timed out reading the interrupted jobs after the shutdown timeout");
            None
        }
    };
    let report = ShutdownTimeoutReport {
        read: interrupted.is_some(),
        interrupted: interrupted.unwrap_or_default(),
    };

    if !report.interrupted.is_empty() {
        // Best effort: the queue names make the log useful, but the ids are
        // the report, and it must not depend on a second read.
        let queues: HashMap<JobId, String> = match tokio::time::timeout(
            SHUTDOWN_REPORT_TIMEOUT,
            storage.get_many(&report.interrupted),
        )
        .await
        {
            Ok(Ok(envelopes)) => envelopes
                .into_iter()
                .map(|envelope| (envelope.id, envelope.queue))
                .collect(),
            Ok(Err(_)) | Err(_) => HashMap::new(),
        };
        for job_id in &report.interrupted {
            tracing::warn!(
                job_id = job_id,
                queue = queues.get(job_id).map(String::as_str).unwrap_or_default(),
                "Job interrupted by the shutdown timeout; retained for resurrection"
            );
        }
    }

    if let Some(reporter) = &runtime.settings.shutdown_timeout_reporter {
        reporter(&report);
    }
}

async fn abort_and_join<DT>(runtime: &Runtime<DT>, tasks: &mut JoinSet<Result<(), OxanaError>>) {
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        // Intentional cancellation must not obscure a failure already observed,
        // but preserve real errors from tasks that finished before the abort.
        if !matches!(&result, Err(error) if error.is_cancelled()) {
            record_task_result(runtime, result);
        }
    }
}

fn record_task_result<DT>(
    runtime: &Runtime<DT>,
    result: Result<Result<(), OxanaError>, tokio::task::JoinError>,
) {
    if let Err(error) = result.map_err(OxanaError::from).and_then(|result| result) {
        runtime.fail(error);
    }
}

async fn retry_loop<DT>(runtime: Arc<Runtime<DT>>) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    runtime
        .storage
        .internal
        .retry_loop(
            runtime.cancel_token.clone(),
            runtime.settings.retry_poll_interval,
            runtime.settings.redis_failure_tolerance,
        )
        .await?;

    tracing::trace!("Retry loop finished");

    Ok(())
}

async fn cleanup_loop<DT>(runtime: Arc<Runtime<DT>>) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    runtime
        .storage
        .internal
        .cleanup_loop(
            runtime.cancel_token.clone(),
            runtime.settings.redis_failure_tolerance,
        )
        .await?;

    tracing::trace!("Cleanup loop finished");

    Ok(())
}

async fn schedule_loop<DT>(runtime: Arc<Runtime<DT>>) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    runtime
        .storage
        .internal
        .schedule_loop(
            runtime.cancel_token.clone(),
            runtime.settings.schedule_poll_interval,
            runtime.settings.redis_failure_tolerance,
        )
        .await?;

    tracing::trace!("Schedule loop finished");

    Ok(())
}

async fn ping_loop<DT>(
    runtime: Arc<Runtime<DT>>,
    cancel_token: CancellationToken,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    runtime
        .storage
        .internal
        .ping_loop(
            cancel_token,
            runtime.settings.heartbeat_interval,
            runtime.settings.redis_failure_tolerance,
        )
        .await?;

    tracing::trace!("Ping loop finished");

    Ok(())
}

/// Registers this process, once for the whole runtime.
///
/// Returns `false` when the runtime was cancelled before a ping landed. Every
/// task that must not act before the process is registered awaits this, so a
/// Redis outage at startup is retried by one task rather than by each of them,
/// which would burn the shared failure tolerance that much faster.
async fn register<DT>(runtime: &Runtime<DT>) -> Result<bool, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    runtime
        .registered
        .get_or_try_init(|| {
            runtime.storage.internal.register(
                &runtime.cancel_token,
                runtime.settings.heartbeat_interval,
                runtime.settings.redis_failure_tolerance,
            )
        })
        .await
        .copied()
}

async fn coordinator_loop<DT>(
    runtime: Arc<Runtime<DT>>,
    stats: Arc<Mutex<Stats>>,
    ctx: ContextValue<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    // Claim nothing before this process is registered: a processing list with
    // no process record is a dead process's as far as a peer's sweep can tell,
    // and it would put the job back on the queue while it still runs.
    if !register(&runtime).await? {
        return Ok(());
    }

    coordinator::run(runtime, stats, ctx, queue_config).await?;

    tracing::trace!("Coordinator finished");

    Ok(())
}

async fn resurrect_loop<DT>(runtime: Arc<Runtime<DT>>) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    // Sweeping before this process is registered would let it resurrect the
    // jobs it is about to claim itself.
    if !register(&runtime).await? {
        return Ok(());
    }

    runtime
        .storage
        .internal
        .resurrect_loop(
            runtime.cancel_token.clone(),
            runtime.settings.resurrect_scan_interval,
            runtime.settings.dead_process_threshold,
            runtime.settings.redis_failure_tolerance,
        )
        .await?;

    tracing::trace!("Resurrect loop finished");

    Ok(())
}

async fn cron_loop<DT>(runtime: Arc<Runtime<DT>>) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut set = JoinSet::new();

    for (name, cron_job) in &runtime.registry.schedules {
        if !runtime.settings.runs_static_queue(&cron_job.queue_key) {
            continue;
        }
        set.spawn(runtime.tasks.track_future(cron_job_loop(
            Arc::clone(&runtime),
            name.clone(),
            cron_job.clone(),
        )));
    }

    if set.is_empty() {
        runtime.cancel_token.cancelled().await;
    } else {
        while let Some(result) = set.join_next().await {
            result??;
        }
    }
    Ok(())
}

async fn cron_job_loop<DT>(
    runtime: Arc<Runtime<DT>>,
    job_name: String,
    cron_job: CronJob,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let storage = runtime.storage.internal.clone();
    let registry = runtime.registry.clone();
    storage
        .cron_job_loop(
            runtime.cancel_token.clone(),
            runtime.settings.clone(),
            cron_job,
            |scheduled_at| registry.cron_envelope(&job_name, scheduled_at),
        )
        .await?;

    tracing::trace!("Cron job loop finished for {}", job_name);

    Ok(())
}
