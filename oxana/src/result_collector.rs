use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, Instant, MissedTickBehavior};

use crate::{OxanaError, metrics::JobMetricsBuffer, runtime::Runtime};

const METRICS_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const STATS_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const QUEUE_LENGTH_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default, Debug)]
pub struct Stats {
    pub processed: u64,
    pub succeeded: u64,
    pub panicked: u64,
    pub failed: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WorkerResult {
    pub kind: WorkerResultKind,
    pub worker_name: String,
    pub queue: String,
    pub execution_ms: u64,
    pub job_count: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkerResultKind {
    Success,
    Panicked,
    Failed,
    /// Postponed by the worker with `Snooze`: neither a success nor a failure.
    Snoozed,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct QueueResultStats {
    pub(crate) processed: i64,
    pub(crate) succeeded: i64,
    pub(crate) panicked: i64,
    pub(crate) failed: i64,
}

impl QueueResultStats {
    fn record(&mut self, result: &WorkerResult) {
        let count = i64::try_from(result.job_count).unwrap_or(i64::MAX);
        self.processed = self.processed.saturating_add(count);
        match result.kind {
            WorkerResultKind::Success => {
                self.succeeded = self.succeeded.saturating_add(count);
            }
            WorkerResultKind::Panicked => {
                self.panicked = self.panicked.saturating_add(count);
                self.failed = self.failed.saturating_add(count);
            }
            WorkerResultKind::Failed => {
                self.failed = self.failed.saturating_add(count);
            }
            WorkerResultKind::Snoozed => {}
        }
    }
}

pub async fn run<DT>(
    mut rx: mpsc::Receiver<WorkerResult>,
    config: Arc<Runtime<DT>>,
    stats: Arc<Mutex<Stats>>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut metrics = JobMetricsBuffer::default();
    let mut pending_stats = HashMap::new();
    let mut active_queues = HashSet::new();
    let mut metrics_flush_interval = tokio::time::interval_at(
        Instant::now() + METRICS_FLUSH_INTERVAL,
        METRICS_FLUSH_INTERVAL,
    );
    let mut stats_flush_interval =
        tokio::time::interval_at(Instant::now() + STATS_FLUSH_INTERVAL, STATS_FLUSH_INTERVAL);
    let mut queue_length_refresh_interval = tokio::time::interval_at(
        Instant::now() + QUEUE_LENGTH_REFRESH_INTERVAL,
        QUEUE_LENGTH_REFRESH_INTERVAL,
    );
    metrics_flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    stats_flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    queue_length_refresh_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Some(result) => {
                        metrics.record(&result);
                        update_stats(&config, Arc::clone(&stats), &mut pending_stats, &mut active_queues, &result).await?;
                    }
                    None => {
                        flush_before_exit(&config, &mut metrics, &mut pending_stats, &mut active_queues).await?;
                        return Ok(());
                    }
                }
            }
            _ = metrics_flush_interval.tick() => {
                flush_metrics(&config, &mut metrics).await?;
            }
            _ = stats_flush_interval.tick() => {
                flush_pending_stats(&config, &mut pending_stats).await?;
            }
            _ = queue_length_refresh_interval.tick() => {
                refresh_active_queue_lengths(&config, &mut active_queues).await?;
            }
        }
    }
}

async fn update_stats<DT>(
    config: &Runtime<DT>,
    stats: Arc<Mutex<Stats>>,
    pending_stats: &mut HashMap<String, QueueResultStats>,
    active_queues: &mut HashSet<String>,
    result: &WorkerResult,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let processed = {
        let mut stats = stats.lock().await;
        stats.processed = stats.processed.saturating_add(result.job_count);
        match result.kind {
            WorkerResultKind::Success => {
                stats.succeeded = stats.succeeded.saturating_add(result.job_count);
            }
            WorkerResultKind::Panicked => {
                stats.panicked = stats.panicked.saturating_add(result.job_count);
                stats.failed = stats.failed.saturating_add(result.job_count);
            }
            WorkerResultKind::Failed => {
                stats.failed = stats.failed.saturating_add(result.job_count);
            }
            WorkerResultKind::Snoozed => {}
        }

        stats.processed
    };

    pending_stats
        .entry(result.queue.clone())
        .or_default()
        .record(result);
    active_queues.insert(result.queue.clone());

    if let Some(exit_when_processed) = config.settings.exit_when_processed
        && processed >= exit_when_processed
    {
        config.cancel_token.cancel();
    }

    Ok(())
}

async fn flush_before_exit<DT>(
    config: &Runtime<DT>,
    metrics: &mut JobMetricsBuffer,
    pending_stats: &mut HashMap<String, QueueResultStats>,
    active_queues: &mut HashSet<String>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    flush_metrics(config, metrics).await?;
    flush_pending_stats(config, pending_stats).await?;
    refresh_active_queue_lengths(config, active_queues).await
}

async fn flush_metrics<DT>(
    config: &Runtime<DT>,
    metrics: &mut JobMetricsBuffer,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    if metrics.is_empty() {
        return Ok(());
    }

    if config
        .storage
        .internal
        .track_redis_result(
            config.storage.internal.flush_job_metrics(metrics).await,
            config.settings.redis_failure_tolerance,
        )?
        .is_some()
    {
        metrics.clear();
    }

    Ok(())
}

async fn flush_pending_stats<DT>(
    config: &Runtime<DT>,
    pending_stats: &mut HashMap<String, QueueResultStats>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    if pending_stats.is_empty() {
        return Ok(());
    }

    if config
        .storage
        .internal
        .track_redis_result(
            config
                .storage
                .internal
                .flush_result_stats(pending_stats)
                .await,
            config.settings.redis_failure_tolerance,
        )?
        .is_some()
    {
        pending_stats.clear();
    }

    Ok(())
}

async fn refresh_active_queue_lengths<DT>(
    config: &Runtime<DT>,
    active_queues: &mut HashSet<String>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    if active_queues.is_empty() {
        return Ok(());
    }

    let queues: Vec<String> = active_queues.iter().cloned().collect();
    let lengths = config.storage.internal.track_redis_result(
        config
            .storage
            .internal
            .refresh_queue_length_stats(&queues)
            .await,
        config.settings.redis_failure_tolerance,
    )?;
    prune_active_queues(active_queues, lengths.as_ref());

    Ok(())
}

fn prune_active_queues(
    active_queues: &mut HashSet<String>,
    lengths: Option<&HashMap<String, i64>>,
) {
    let Some(lengths) = lengths else {
        return;
    };

    active_queues.retain(|queue| lengths.get(queue).is_some_and(|count| *count > 0));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, RuntimeSettings};
    use crate::metrics::{JobMetricsQuery, MetricIdentity};
    use crate::test_helper::{random_string, redis_pool};
    use crate::{Storage, runtime::Runtime};
    use testresult::TestResult;

    #[tokio::test]
    async fn result_collector_drains_results_after_cancellation() -> TestResult {
        let storage = Storage::builder()
            .namespace(random_string())
            .build_from_pool(redis_pool().await?)?;
        let mut settings = RuntimeSettings::new();
        settings.exit_when_processed = Some(1);
        let config: Arc<Runtime<()>> =
            Arc::new(Runtime::new(storage.clone(), Config::new(), settings));
        let stats = Arc::new(Mutex::new(Stats::default()));
        let (tx, rx) = mpsc::channel(2);
        let collector = tokio::spawn(run(rx, Arc::clone(&config), Arc::clone(&stats)));

        tx.send(WorkerResult {
            kind: WorkerResultKind::Success,
            worker_name: "CollectorWorker".to_string(),
            queue: "collector".to_string(),
            execution_ms: 25,
            job_count: 1,
        })
        .await
        .expect("collector should receive first result");

        tokio::time::timeout(Duration::from_secs(1), config.cancel_token.cancelled())
            .await
            .expect("first result should trigger cancellation");

        tx.send(WorkerResult {
            kind: WorkerResultKind::Success,
            worker_name: "CollectorWorker".to_string(),
            queue: "collector".to_string(),
            execution_ms: 50,
            job_count: 1,
        })
        .await
        .expect("collector should keep receiving after cancellation");
        drop(tx);

        collector.await??;

        let stats = stats.lock().await;
        assert_eq!(stats.processed, 2);
        assert_eq!(stats.succeeded, 2);
        drop(stats);

        let metrics = storage
            .job_metrics_for(
                &MetricIdentity {
                    worker: "CollectorWorker".to_string(),
                },
                JobMetricsQuery::default(),
            )
            .await?;
        assert_eq!(metrics.totals.processed, 2);
        assert_eq!(metrics.totals.succeeded, 2);
        assert_eq!(metrics.totals.successful_executions, 2);
        assert_eq!(metrics.totals.execution_ms, 75);

        Ok(())
    }

    #[test]
    fn active_queue_pruning_drops_zero_length_queue() {
        let queue = "empty".to_string();
        let mut active_queues = HashSet::from([queue.clone()]);
        let lengths = HashMap::from([(queue.clone(), 0)]);

        prune_active_queues(&mut active_queues, Some(&lengths));

        assert!(!active_queues.contains(&queue));
    }

    #[test]
    fn active_queue_pruning_keeps_nonzero_queue() {
        let queue = "busy".to_string();
        let mut active_queues = HashSet::from([queue.clone()]);
        let lengths = HashMap::from([(queue.clone(), 1)]);

        prune_active_queues(&mut active_queues, Some(&lengths));

        assert!(active_queues.contains(&queue));
    }

    #[test]
    fn active_queue_pruning_skips_pruning_when_refresh_failed() {
        let queue = "unknown".to_string();
        let mut active_queues = HashSet::from([queue.clone()]);

        prune_active_queues(&mut active_queues, None);

        assert!(active_queues.contains(&queue));
    }
}
