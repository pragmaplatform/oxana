use tokio_util::sync::CancellationToken;

use crate::config::{Config, RuntimeSettings};
use crate::context::ContextValue;
use crate::error::OxanaError;
use crate::job_state::JobState;
use crate::{JobContext, JobId, Queue, Storage};

enum ProcessJobResult {
    Success,
    Failed,
    Missing,
}

#[derive(Default, Debug)]
pub struct DrainStats {
    pub processed: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub missing: u64,
}

/// Drains a queue of jobs.
///
/// This function will drain a queue of jobs, processing them one by one.
///
/// It is useful in development or testing to process a queue of jobs without running the full worker.
///
/// # Arguments
///
/// * `storage` - The job storage to drain from
/// * `config` - The worker configuration, including queue and worker registrations
/// * `settings` - Runtime settings, including worker error formatting
/// * `ctx` - The context value that will be shared across all worker instances
/// * `queue` - The queue to drain
///
/// # Returns
///
/// Returns statistics about the drain operation, or an [`OxanaError`] if the operation fails.
pub async fn drain<DT>(
    storage: &Storage,
    config: &Config<DT>,
    settings: &RuntimeSettings,
    ctx: ContextValue<DT>,
    queue: impl Queue,
) -> Result<DrainStats, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let queue_key = queue.key();

    // Claim nothing before this process is registered, and keep heartbeating
    // for as long as jobs run. A processing list whose process has no record —
    // or a record older than `dead_process_threshold`, five seconds by default
    // — belongs to a dead process as far as a worker's sweep can tell, and the
    // sweep would put the job back on its queue while the drain still runs it.
    storage.internal.ping().await?;

    let heartbeat_cancel = CancellationToken::new();
    let heartbeat = tokio::spawn({
        let storage = storage.clone();
        let cancel_token = heartbeat_cancel.clone();
        let heartbeat_interval = settings.heartbeat_interval;
        let failure_tolerance = settings.redis_failure_tolerance;
        async move {
            storage
                .internal
                .ping_loop(cancel_token, heartbeat_interval, failure_tolerance)
                .await
        }
    });

    let stats = drain_queue(storage, config, settings, ctx, &queue_key).await;

    heartbeat_cancel.cancel();
    if let Err(e) = heartbeat.await {
        tracing::error!("Drain heartbeat task failed: {}", e);
    }

    // Drop the record before returning, so that a drain that gave up holding a
    // job leaves its processing list to be resurrected rather than a stale
    // record that only goes cold after `dead_process_threshold`.
    storage.internal.self_cleanup().await?;

    stats
}

async fn drain_queue<DT>(
    storage: &Storage,
    config: &Config<DT>,
    settings: &RuntimeSettings,
    ctx: ContextValue<DT>,
    queue_key: &str,
) -> Result<DrainStats, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut stats = DrainStats::default();

    while let Some(job_id) = storage.internal.dequeue(queue_key).await? {
        let result = process_job(storage, config, settings, ctx.clone(), job_id).await?;
        match result {
            ProcessJobResult::Success => stats.succeeded += 1,
            ProcessJobResult::Failed => stats.failed += 1,
            ProcessJobResult::Missing => stats.missing += 1,
        }
        stats.processed += 1;
    }

    Ok(stats)
}

async fn process_job<DT>(
    storage: &Storage,
    config: &Config<DT>,
    settings: &RuntimeSettings,
    ctx: ContextValue<DT>,
    job_id: JobId,
) -> Result<ProcessJobResult, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut envelope = match storage.internal.get_job(&job_id).await? {
        Some(envelope) => envelope,
        None => return Ok(ProcessJobResult::Missing),
    };

    let job = config
        .registry
        .build(&envelope.job.name, envelope.job.args.clone(), &ctx.0)?;

    let should_resume = job.should_resume();
    if !should_resume {
        envelope.meta.state = None;
        storage.internal.update_job(&envelope).await?;
    }

    let job_ctx = JobContext {
        meta: envelope.meta.clone(),
        state: JobState::new(storage.clone(), job_id, envelope.meta.state.clone()),
    };

    let job_result = job.process(vec![job_ctx]).await;

    match job_result {
        Ok(()) => {
            storage.internal.finish_with_success(&envelope).await?;
            Ok(ProcessJobResult::Success)
        }
        Err(e) => {
            tracing::error!("Job failed: {}", e);
            storage.internal.finish_with_failure(&envelope).await?;
            storage
                .internal
                .kill(&envelope, settings.format_error(e.as_ref()))
                .await?;
            Ok(ProcessJobResult::Failed)
        }
    }
}
