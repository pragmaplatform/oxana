use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, RuntimeSettings};
use crate::context::ContextValue;
use crate::coordinator;
use crate::error::OxanaError;
use crate::result_collector::Stats;
use crate::runtime::Runtime;
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
    let mut joinset = JoinSet::new();
    let mut coordinator_joinset = JoinSet::new();
    let stats = Arc::new(Mutex::new(Stats::default()));
    let ping_cancel_token = CancellationToken::new();

    joinset.spawn(ping_loop(Arc::clone(&runtime), ping_cancel_token.clone()));
    joinset.spawn(retry_loop(Arc::clone(&runtime)));
    joinset.spawn(schedule_loop(Arc::clone(&runtime)));
    joinset.spawn(resurrect_loop(Arc::clone(&runtime)));
    joinset.spawn(cron_loop(Arc::clone(&runtime)));
    joinset.spawn(cleanup_loop(Arc::clone(&runtime)));

    for queue_config in &runtime.queues {
        coordinator_joinset.spawn(coordinator::run(
            Arc::clone(&runtime),
            Arc::clone(&stats),
            ctx.clone(),
            queue_config.clone(),
        ));
    }

    let mut result = Ok(());

    tokio::select! {
        Some(task_result) = joinset.join_next() => {
            result = task_result?;

            if result.is_ok() {
                tracing::info!("Background task unexpectedly finished");
            }

            runtime.cancel_token.cancel();
        }
        Some(task_result) = coordinator_joinset.join_next() => {
            result = task_result?;

            if result.is_ok() {
                tracing::info!("Background task unexpectedly finished");
            }

            runtime.cancel_token.cancel();
        }
        _ = runtime.cancel_token.cancelled() => {}
        _ = shutdown_signal => {
            tracing::info!("Received shutdown signal");
            runtime.cancel_token.cancel();
        }
    }

    tracing::info!("Shutting down");

    while let Some(task_result) = coordinator_joinset.join_next().await {
        let task_result = task_result?;
        if result.is_ok()
            && let Err(e) = task_result
        {
            result = Err(e);
        }
    }
    ping_cancel_token.cancel();
    while let Some(task_result) = joinset.join_next().await {
        let task_result = task_result?;
        if result.is_ok()
            && let Err(e) = task_result
        {
            result = Err(e);
        }
    }

    runtime.storage.internal.self_cleanup().await?;

    let stats = Arc::try_unwrap(stats)
        .expect("Failed to unwrap Arc - there are still references to stats")
        .into_inner();

    match result {
        Ok(()) => {
            tracing::info!("Gracefully shut down");
            Ok(stats)
        }
        Err(e) => {
            tracing::error!("Gracefully shut down with errors");
            Err(e)
        }
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

async fn resurrect_loop<DT>(runtime: Arc<Runtime<DT>>) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
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
        set.spawn(cron_job_loop(
            Arc::clone(&runtime),
            name.clone(),
            cron_job.clone(),
        ));
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
