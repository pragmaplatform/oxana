//! Sidekiq-style execution metrics for Oxana workers.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::result_collector::{WorkerResult, WorkerResultKind};

pub(crate) const METRICS_RETENTION_SECS: i64 = 24 * 60 * 60;
pub(crate) const DEFAULT_METRIC_MINUTES: usize = 60;
pub(crate) const MAX_METRIC_MINUTES: usize = 24 * 60;
pub(crate) const QUEUE_RATE_WINDOW_MINUTES: usize = 10;
pub(crate) const HISTOGRAM_BUCKET_COUNT: usize = 14;

/// Maximum execution time represented by each histogram bucket.
pub const HISTOGRAM_BUCKET_INTERVALS_MS: [u64; HISTOGRAM_BUCKET_COUNT] = [
    25,
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000,
    30000,
    60000,
    120000,
    300000,
    u64::MAX,
];

/// Display labels for [`HISTOGRAM_BUCKET_INTERVALS_MS`].
pub const HISTOGRAM_BUCKET_LABELS: [&str; HISTOGRAM_BUCKET_COUNT] = [
    "25ms", "50ms", "100ms", "250ms", "500ms", "1s", "2.5s", "5s", "10s", "30s", "60s", "120s",
    "5min", "Slow",
];

pub(crate) const METRIC_PROCESSED_JOBS: &str = "p";
pub(crate) const METRIC_FAILED_JOBS: &str = "f";
pub(crate) const METRIC_PANICKED_JOBS: &str = "pn";
pub(crate) const METRIC_SUCCESSFUL_EXECUTIONS: &str = "xs";
pub(crate) const METRIC_FAILED_EXECUTIONS: &str = "xf";
pub(crate) const METRIC_PANICKED_EXECUTIONS: &str = "xpn";
pub(crate) const METRIC_EXECUTION_MS: &str = "ms";
pub(crate) const JOB_METRIC_KEYS: [&str; 7] = [
    METRIC_PROCESSED_JOBS,
    METRIC_FAILED_JOBS,
    METRIC_PANICKED_JOBS,
    METRIC_SUCCESSFUL_EXECUTIONS,
    METRIC_FAILED_EXECUTIONS,
    METRIC_PANICKED_EXECUTIONS,
    METRIC_EXECUTION_MS,
];
pub(crate) const QUEUE_METRIC_PROCESSED_JOBS: &str = "p";
pub(crate) const QUEUE_METRIC_SUCCEEDED_JOBS: &str = "s";
pub(crate) const QUEUE_METRIC_FAILED_JOBS: &str = "f";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MetricIdentity {
    pub worker: String,
}

impl MetricIdentity {
    pub(crate) fn from_worker_result(result: &WorkerResult) -> Self {
        Self {
            worker: result.worker_name.clone(),
        }
    }

    pub(crate) fn field_key(&self) -> String {
        format!("{}:{}", self.worker.len(), self.worker)
    }

    pub(crate) fn from_field_key(key: &str) -> Option<Self> {
        let (worker_len, rest) = key.split_once(':')?;
        let worker_len = worker_len.parse::<usize>().ok()?;

        if rest.len() < worker_len || !rest.is_char_boundary(worker_len) {
            return None;
        }

        let (worker, _) = rest.split_at(worker_len);
        Some(Self {
            worker: worker.to_string(),
        })
    }

    pub(crate) fn metric_field(&self, metric: &str) -> String {
        format!("{}|{metric}", self.field_key())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct JobMetricsQuery {
    pub minutes: usize,
}

impl JobMetricsQuery {
    #[must_use]
    pub fn new(minutes: usize) -> Self {
        Self { minutes }
    }

    #[must_use]
    pub fn effective_minutes(&self) -> usize {
        if self.minutes == 0 {
            DEFAULT_METRIC_MINUTES
        } else {
            self.minutes.min(MAX_METRIC_MINUTES)
        }
    }
}

impl Default for JobMetricsQuery {
    fn default() -> Self {
        Self {
            minutes: DEFAULT_METRIC_MINUTES,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobMetricsTotals {
    /// Jobs completed, including successes, failures, and panics. Batch jobs count individually.
    pub processed: u64,
    /// Jobs completed successfully. Batch jobs count individually.
    pub succeeded: u64,
    /// Jobs that failed or panicked. Batch jobs count individually.
    pub failed: u64,
    /// Jobs that panicked. Batch jobs count individually.
    pub panicked: u64,
    /// Successful worker executions. Batch workers count once per batch.
    pub successful_executions: u64,
    /// Worker executions that failed or panicked. Batch workers count once per batch.
    pub failed_executions: u64,
    /// Panicked worker executions. Batch workers count once per batch.
    pub panicked_executions: u64,
    /// Total duration for successful worker executions. Batch duration counts once per batch.
    pub execution_ms: u64,
}

macro_rules! impl_job_metric_aggregation {
    ($($ty:ty),* $(,)?) => {$(
        impl $ty {
            #[must_use]
            pub fn average_execution_ms(&self) -> f64 {
                if self.successful_executions == 0 {
                    0.0
                } else {
                    self.execution_ms as f64 / self.successful_executions as f64
                }
            }

            #[must_use]
            pub fn failed_executions_without_panics(&self) -> u64 {
                self.failed_executions
                    .saturating_sub(self.panicked_executions)
            }

            fn add_metric(&mut self, metric: &str, value: u64) {
                match metric {
                    METRIC_PROCESSED_JOBS => self.processed = self.processed.saturating_add(value),
                    METRIC_FAILED_JOBS => self.failed = self.failed.saturating_add(value),
                    METRIC_PANICKED_JOBS => self.panicked = self.panicked.saturating_add(value),
                    METRIC_SUCCESSFUL_EXECUTIONS => {
                        self.successful_executions =
                            self.successful_executions.saturating_add(value);
                    }
                    METRIC_FAILED_EXECUTIONS => {
                        self.failed_executions = self.failed_executions.saturating_add(value);
                    }
                    METRIC_PANICKED_EXECUTIONS => {
                        self.panicked_executions = self.panicked_executions.saturating_add(value);
                    }
                    METRIC_EXECUTION_MS => {
                        self.execution_ms = self.execution_ms.saturating_add(value);
                    }
                    _ => {}
                }
            }

            fn finalize(&mut self) {
                self.succeeded = self.processed.saturating_sub(self.failed);
                if self.successful_executions + self.failed_executions + self.panicked_executions
                    == 0
                {
                    self.successful_executions = self.succeeded;
                    self.failed_executions = self.failed;
                    self.panicked_executions = self.panicked;
                }
            }
        }
    )*};
}

impl_job_metric_aggregation!(JobMetricsTotals, JobMetricsPoint);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobMetricsPoint {
    /// Start timestamp for this minute bucket.
    pub timestamp: i64,
    /// Jobs completed, including successes, failures, and panics. Batch jobs count individually.
    pub processed: u64,
    /// Jobs completed successfully. Batch jobs count individually.
    pub succeeded: u64,
    /// Jobs that failed or panicked. Batch jobs count individually.
    pub failed: u64,
    /// Jobs that panicked. Batch jobs count individually.
    pub panicked: u64,
    /// Successful worker executions. Batch workers count once per batch.
    pub successful_executions: u64,
    /// Worker executions that failed or panicked. Batch workers count once per batch.
    pub failed_executions: u64,
    /// Panicked worker executions. Batch workers count once per batch.
    pub panicked_executions: u64,
    /// Total duration for successful worker executions. Batch duration counts once per batch.
    pub execution_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetricsSummary {
    /// Worker these metrics belong to.
    pub identity: MetricIdentity,
    /// Totals for this worker over the queried window.
    pub totals: JobMetricsTotals,
    /// Per-minute points for this worker over the queried window.
    pub series: Vec<JobMetricsPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetricsSnapshot {
    /// Inclusive start timestamp for the queried window.
    pub starts_at: i64,
    /// Inclusive end timestamp for the queried window.
    pub ends_at: i64,
    /// Number of minute buckets returned.
    pub minutes: usize,
    /// Totals across all workers in the queried window.
    pub totals: JobMetricsTotals,
    /// Per-minute totals across all workers in the queried window.
    pub series: Vec<JobMetricsPoint>,
    /// Per-worker summaries sorted by total execution time descending.
    pub workers: Vec<WorkerMetricsSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetricsHistogramBucket {
    pub label: String,
    pub upper_bound_ms: Option<u64>,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetricsDetail {
    /// Worker these metrics belong to.
    pub identity: MetricIdentity,
    /// Inclusive start timestamp for the queried window.
    pub starts_at: i64,
    /// Inclusive end timestamp for the queried window.
    pub ends_at: i64,
    /// Number of minute buckets returned.
    pub minutes: usize,
    /// Totals for this worker in the queried window.
    pub totals: JobMetricsTotals,
    /// Per-minute points for this worker in the queried window.
    pub series: Vec<JobMetricsPoint>,
    /// Execution-time histogram for successful worker executions.
    pub histogram: Vec<JobMetricsHistogramBucket>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueLengthMetricsPoint {
    /// Start timestamp for this minute bucket.
    pub timestamp: i64,
    /// Jobs enqueued in this queue when the sample was recorded.
    pub enqueued: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueLengthMetricsSeries {
    /// Queue name.
    pub queue: String,
    /// Per-minute queue length samples.
    pub series: Vec<QueueLengthMetricsPoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueLengthMetricsSnapshot {
    /// Inclusive start timestamp for the queried window.
    pub starts_at: i64,
    /// Inclusive end timestamp for the queried window.
    pub ends_at: i64,
    /// Number of minute buckets returned.
    pub minutes: usize,
    /// Per-queue length samples sorted by peak queue length descending.
    pub queues: Vec<QueueLengthMetricsSeries>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct QueueCounterTotals {
    pub(crate) processed: u64,
    pub(crate) succeeded: u64,
    pub(crate) failed: u64,
}

impl QueueCounterTotals {
    pub(crate) fn increments(&self) -> impl Iterator<Item = (&'static str, u64)> {
        [
            (QUEUE_METRIC_PROCESSED_JOBS, self.processed),
            (QUEUE_METRIC_SUCCEEDED_JOBS, self.succeeded),
            (QUEUE_METRIC_FAILED_JOBS, self.failed),
        ]
        .into_iter()
    }

    fn add_metric(&mut self, metric: &str, value: u64) {
        match metric {
            QUEUE_METRIC_PROCESSED_JOBS => {
                self.processed = self.processed.saturating_add(value);
            }
            QUEUE_METRIC_SUCCEEDED_JOBS => {
                self.succeeded = self.succeeded.saturating_add(value);
            }
            QUEUE_METRIC_FAILED_JOBS => {
                self.failed = self.failed.saturating_add(value);
            }
            _ => {}
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct JobMetricsBuffer {
    entries: HashMap<(i64, MetricIdentity), PendingJobMetrics>,
    queue_entries: HashMap<(i64, String), QueueCounterTotals>,
}

impl JobMetricsBuffer {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.queue_entries.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.queue_entries.clear();
    }

    pub(crate) fn record(&mut self, result: &WorkerResult) {
        let minute = chrono::Utc::now().timestamp().div_euclid(60);
        let identity = MetricIdentity::from_worker_result(result);
        let metrics = self.entries.entry((minute, identity)).or_default();
        let queue_metrics = self
            .queue_entries
            .entry((minute, result.queue.clone()))
            .or_default();

        metrics.processed = metrics.processed.saturating_add(result.job_count);
        queue_metrics.processed = queue_metrics.processed.saturating_add(result.job_count);

        match result.kind {
            WorkerResultKind::Success => {
                metrics.successful_executions = metrics.successful_executions.saturating_add(1);
                metrics.execution_ms = metrics.execution_ms.saturating_add(result.execution_ms);
                queue_metrics.succeeded = queue_metrics.succeeded.saturating_add(result.job_count);
                let bucket = histogram_bucket_index(result.execution_ms);
                if let Some(count) = metrics.histogram.get_mut(bucket) {
                    *count = count.saturating_add(1);
                }
            }
            WorkerResultKind::Panicked => {
                metrics.failed = metrics.failed.saturating_add(result.job_count);
                metrics.panicked = metrics.panicked.saturating_add(result.job_count);
                metrics.failed_executions = metrics.failed_executions.saturating_add(1);
                metrics.panicked_executions = metrics.panicked_executions.saturating_add(1);
                queue_metrics.failed = queue_metrics.failed.saturating_add(result.job_count);
            }
            WorkerResultKind::Failed => {
                metrics.failed = metrics.failed.saturating_add(result.job_count);
                metrics.failed_executions = metrics.failed_executions.saturating_add(1);
                queue_metrics.failed = queue_metrics.failed.saturating_add(result.job_count);
            }
        }
    }

    pub(crate) fn records(
        &self,
    ) -> impl Iterator<Item = (i64, &MetricIdentity, &PendingJobMetrics)> {
        self.entries
            .iter()
            .map(|((minute, identity), metrics)| (*minute, identity, metrics))
    }

    pub(crate) fn queue_records(&self) -> impl Iterator<Item = (i64, &str, &QueueCounterTotals)> {
        self.queue_entries
            .iter()
            .map(|((minute, queue), metrics)| (*minute, queue.as_str(), metrics))
    }
}

#[derive(Clone, Default)]
pub(crate) struct PendingJobMetrics {
    pub(crate) processed: u64,
    pub(crate) failed: u64,
    pub(crate) panicked: u64,
    pub(crate) successful_executions: u64,
    pub(crate) failed_executions: u64,
    pub(crate) panicked_executions: u64,
    pub(crate) execution_ms: u64,
    pub(crate) histogram: [u64; HISTOGRAM_BUCKET_COUNT],
}

impl PendingJobMetrics {
    pub(crate) fn increments(&self) -> impl Iterator<Item = (&'static str, u64)> {
        [
            (METRIC_PROCESSED_JOBS, self.processed),
            (METRIC_FAILED_JOBS, self.failed),
            (METRIC_PANICKED_JOBS, self.panicked),
            (METRIC_SUCCESSFUL_EXECUTIONS, self.successful_executions),
            (METRIC_FAILED_EXECUTIONS, self.failed_executions),
            (METRIC_PANICKED_EXECUTIONS, self.panicked_executions),
            (METRIC_EXECUTION_MS, self.execution_ms),
        ]
        .into_iter()
    }
}

#[derive(Default)]
pub(crate) struct JobMetricsAggregation {
    pub(crate) totals: JobMetricsTotals,
    pub(crate) series: Vec<JobMetricsPoint>,
    pub(crate) workers: Vec<WorkerMetricsSummary>,
}

struct WorkerMetricsSummaryBuilder {
    totals: JobMetricsTotals,
    series: Vec<JobMetricsPoint>,
}

#[must_use]
pub(crate) fn metric_minutes(now_ts: i64, query: JobMetricsQuery) -> Vec<i64> {
    let minutes = query.effective_minutes();
    let end_minute = now_ts.div_euclid(60);
    let start_minute = end_minute - i64::try_from(minutes).unwrap_or(i64::MAX) + 1;
    (start_minute..=end_minute).collect()
}

#[must_use]
pub(crate) fn aggregate_counter_hashes(
    minutes: &[i64],
    hashes: Vec<HashMap<String, i64>>,
    filter: Option<&MetricIdentity>,
) -> JobMetricsAggregation {
    let mut aggregation = JobMetricsAggregation {
        series: minutes
            .iter()
            .map(|minute| JobMetricsPoint {
                timestamp: minute * 60,
                ..JobMetricsPoint::default()
            })
            .collect(),
        ..JobMetricsAggregation::default()
    };
    let worker_series_template: Vec<JobMetricsPoint> = minutes
        .iter()
        .map(|minute| JobMetricsPoint {
            timestamp: minute * 60,
            ..JobMetricsPoint::default()
        })
        .collect();
    let mut workers: HashMap<MetricIdentity, WorkerMetricsSummaryBuilder> = HashMap::new();

    for (idx, hash) in hashes.into_iter().enumerate() {
        let Some(point) = aggregation.series.get_mut(idx) else {
            continue;
        };

        for (field, raw_value) in hash {
            let Some((identity, metric)) = split_metric_field(&field) else {
                continue;
            };
            if filter.is_some_and(|expected| expected != &identity) {
                continue;
            }

            let value = u64::try_from(raw_value).unwrap_or_default();
            point.add_metric(metric, value);
            aggregation.totals.add_metric(metric, value);
            let worker_summary =
                workers
                    .entry(identity)
                    .or_insert_with(|| WorkerMetricsSummaryBuilder {
                        totals: JobMetricsTotals::default(),
                        series: worker_series_template.clone(),
                    });
            worker_summary.totals.add_metric(metric, value);
            if let Some(worker_point) = worker_summary.series.get_mut(idx) {
                worker_point.add_metric(metric, value);
            }
        }

        point.finalize();
    }

    aggregation.totals.finalize();
    aggregation.workers = workers
        .into_iter()
        .map(|(identity, mut summary)| {
            summary.totals.finalize();
            for point in &mut summary.series {
                point.finalize();
            }
            WorkerMetricsSummary {
                identity,
                totals: summary.totals,
                series: summary.series,
            }
        })
        .collect();
    aggregation.workers.sort_by(|a, b| {
        b.totals
            .execution_ms
            .cmp(&a.totals.execution_ms)
            .then_with(|| b.totals.processed.cmp(&a.totals.processed))
            .then_with(|| a.identity.worker.cmp(&b.identity.worker))
    });

    aggregation
}

#[must_use]
pub(crate) fn queue_metric_field(queue: &str, metric: &str) -> String {
    format!("{}:{queue}|{metric}", queue.len())
}

#[must_use]
pub(crate) fn aggregate_queue_counter_hashes(
    hashes: Vec<HashMap<String, i64>>,
) -> HashMap<String, QueueCounterTotals> {
    let mut queues = HashMap::new();

    for hash in hashes {
        for (field, raw_value) in hash {
            let Some((queue, metric)) = split_queue_metric_field(&field) else {
                continue;
            };
            let value = u64::try_from(raw_value).unwrap_or_default();
            queues
                .entry(queue)
                .or_insert_with(QueueCounterTotals::default)
                .add_metric(metric, value);
        }
    }

    queues
}

#[must_use]
pub(crate) fn queue_length_series_from_hashes(
    minutes: &[i64],
    hashes: Vec<HashMap<String, i64>>,
) -> Vec<QueueLengthMetricsSeries> {
    let series_template: Vec<QueueLengthMetricsPoint> = minutes
        .iter()
        .map(|minute| QueueLengthMetricsPoint {
            timestamp: minute * 60,
            enqueued: 0,
        })
        .collect();
    let mut queues: HashMap<String, Vec<QueueLengthMetricsPoint>> = HashMap::new();

    for (idx, hash) in hashes.into_iter().enumerate() {
        for (queue, raw_value) in hash {
            let value = u64::try_from(raw_value).unwrap_or_default();
            let series = queues
                .entry(queue)
                .or_insert_with(|| series_template.clone());
            if let Some(point) = series.get_mut(idx) {
                point.enqueued = value;
            }
        }
    }

    let mut queues: Vec<QueueLengthMetricsSeries> = queues
        .into_iter()
        .map(|(queue, series)| QueueLengthMetricsSeries { queue, series })
        .collect();

    queues.sort_by(|a, b| {
        let a_peak = a
            .series
            .iter()
            .map(|point| point.enqueued)
            .max()
            .unwrap_or_default();
        let b_peak = b
            .series
            .iter()
            .map(|point| point.enqueued)
            .max()
            .unwrap_or_default();

        b_peak.cmp(&a_peak).then_with(|| a.queue.cmp(&b.queue))
    });

    queues
}

#[must_use]
pub(crate) fn histogram_bucket_index(duration_ms: u64) -> usize {
    HISTOGRAM_BUCKET_INTERVALS_MS
        .iter()
        .position(|upper| duration_ms < *upper)
        .unwrap_or(HISTOGRAM_BUCKET_COUNT - 1)
}

#[must_use]
pub(crate) fn histogram_bitfield_increment_args(
    buckets: &[u64; HISTOGRAM_BUCKET_COUNT],
) -> Vec<String> {
    let mut args = vec!["OVERFLOW".to_string(), "SAT".to_string()];
    for (idx, value) in buckets.iter().enumerate() {
        if *value == 0 {
            continue;
        }
        args.push("INCRBY".to_string());
        args.push("u16".to_string());
        args.push(format!("#{idx}"));
        args.push(value.to_string());
    }
    args
}

#[must_use]
pub(crate) fn histogram_bitfield_fetch_args() -> Vec<String> {
    let mut args = Vec::with_capacity(HISTOGRAM_BUCKET_COUNT * 3);
    for idx in 0..HISTOGRAM_BUCKET_COUNT {
        args.push("GET".to_string());
        args.push("u16".to_string());
        args.push(format!("#{idx}"));
    }
    args
}

#[must_use]
pub(crate) fn histogram_buckets_from_counts(
    counts: &[u64; HISTOGRAM_BUCKET_COUNT],
) -> Vec<JobMetricsHistogramBucket> {
    HISTOGRAM_BUCKET_LABELS
        .iter()
        .zip(counts.iter())
        .enumerate()
        .map(|(idx, (label, count))| JobMetricsHistogramBucket {
            label: (*label).to_string(),
            upper_bound_ms: (idx < HISTOGRAM_BUCKET_COUNT - 1)
                .then(|| HISTOGRAM_BUCKET_INTERVALS_MS.get(idx).copied())
                .flatten(),
            count: *count,
        })
        .collect()
}

fn split_metric_field(field: &str) -> Option<(MetricIdentity, &str)> {
    let (identity_key, metric) = field.rsplit_once('|')?;
    if JOB_METRIC_KEYS.contains(&metric) {
        MetricIdentity::from_field_key(identity_key).map(|id| (id, metric))
    } else {
        None
    }
}

fn split_queue_metric_field(field: &str) -> Option<(String, &str)> {
    let (queue_key, metric) = field.rsplit_once('|')?;
    match metric {
        QUEUE_METRIC_PROCESSED_JOBS | QUEUE_METRIC_SUCCEEDED_JOBS | QUEUE_METRIC_FAILED_JOBS => {
            let (queue_len, queue) = queue_key.split_once(':')?;
            let queue_len = queue_len.parse::<usize>().ok()?;

            if queue.len() != queue_len || !queue.is_char_boundary(queue_len) {
                return None;
            }

            Some((queue.to_string(), metric))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn histogram_bucket_boundaries_use_configured_thresholds() {
        assert_eq!(histogram_bucket_index(0), 0);
        assert_eq!(histogram_bucket_index(24), 0);
        assert_eq!(histogram_bucket_index(25), 1);
        assert_eq!(histogram_bucket_index(49), 1);
        assert_eq!(histogram_bucket_index(50), 2);
        assert_eq!(histogram_bucket_index(299_999), 12);
        assert_eq!(histogram_bucket_index(300_000), 13);
    }

    #[test]
    fn bitfield_increment_args_use_saturated_u16_counters() {
        let mut buckets = [0_u64; HISTOGRAM_BUCKET_COUNT];
        *buckets
            .first_mut()
            .expect("histogram should include the first bucket") = 2;
        *buckets
            .get_mut(13)
            .expect("histogram should include the last bucket") = 70_000;

        let args = histogram_bitfield_increment_args(&buckets);

        assert_eq!(
            args,
            vec![
                "OVERFLOW", "SAT", "INCRBY", "u16", "#0", "2", "INCRBY", "u16", "#13", "70000",
            ]
        );
    }

    #[test]
    fn metric_increment_iterators_preserve_key_order() {
        let job_metrics = PendingJobMetrics {
            processed: 1,
            failed: 2,
            panicked: 3,
            successful_executions: 4,
            failed_executions: 5,
            panicked_executions: 6,
            execution_ms: 7,
            ..PendingJobMetrics::default()
        };
        let increments = job_metrics.increments().collect::<Vec<_>>();
        assert_eq!(
            increments.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            JOB_METRIC_KEYS
        );
        assert_eq!(
            increments
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 7]
        );

        let queue_counters = QueueCounterTotals {
            processed: 8,
            succeeded: 9,
            failed: 10,
        };
        assert_eq!(
            queue_counters.increments().collect::<Vec<_>>(),
            vec![
                (QUEUE_METRIC_PROCESSED_JOBS, 8),
                (QUEUE_METRIC_SUCCEEDED_JOBS, 9),
                (QUEUE_METRIC_FAILED_JOBS, 10),
            ]
        );
    }

    #[test]
    fn query_aggregation_computes_totals_and_clamps_minutes() {
        let identity = MetricIdentity {
            worker: "WorkerA".to_string(),
        };
        let other = MetricIdentity {
            worker: "WorkerB".to_string(),
        };
        let minutes = metric_minutes(10_000, JobMetricsQuery::new(10_000));
        assert_eq!(minutes.len(), MAX_METRIC_MINUTES);

        let mut hashes = vec![HashMap::new(); minutes.len()];
        let first_hash = hashes
            .get_mut(0)
            .expect("query should include a first minute bucket");
        first_hash.insert(identity.metric_field(METRIC_PROCESSED_JOBS), 3);
        first_hash.insert(identity.metric_field(METRIC_FAILED_JOBS), 1);
        first_hash.insert(identity.metric_field(METRIC_SUCCESSFUL_EXECUTIONS), 2);
        first_hash.insert(identity.metric_field(METRIC_FAILED_EXECUTIONS), 1);
        first_hash.insert(identity.metric_field(METRIC_EXECUTION_MS), 250);

        let second_hash = hashes
            .get_mut(1)
            .expect("query should include a second minute bucket");
        second_hash.insert(other.metric_field(METRIC_PROCESSED_JOBS), 2);
        second_hash.insert(other.metric_field(METRIC_FAILED_JOBS), 1);
        second_hash.insert(other.metric_field(METRIC_PANICKED_JOBS), 1);
        second_hash.insert(other.metric_field(METRIC_SUCCESSFUL_EXECUTIONS), 1);
        second_hash.insert(other.metric_field(METRIC_FAILED_EXECUTIONS), 1);
        second_hash.insert(other.metric_field(METRIC_PANICKED_EXECUTIONS), 1);
        second_hash.insert(other.metric_field(METRIC_EXECUTION_MS), 100);

        let aggregation = aggregate_counter_hashes(&minutes, hashes.clone(), None);
        assert_eq!(aggregation.totals.processed, 5);
        assert_eq!(aggregation.totals.failed, 2);
        assert_eq!(aggregation.totals.panicked, 1);
        assert_eq!(aggregation.totals.succeeded, 3);
        assert_eq!(aggregation.totals.successful_executions, 3);
        assert_eq!(aggregation.totals.failed_executions, 2);
        assert_eq!(aggregation.totals.panicked_executions, 1);
        assert_eq!(aggregation.totals.failed_executions_without_panics(), 1);
        assert_eq!(aggregation.totals.execution_ms, 350);
        assert_eq!(aggregation.workers.len(), 2);
        let first_worker = aggregation
            .workers
            .first()
            .expect("first worker summary should exist");
        let first_point = first_worker
            .series
            .first()
            .expect("first worker should have a first series point");
        assert_eq!(first_worker.series.len(), MAX_METRIC_MINUTES);
        assert_eq!(first_point.processed, 3);
        assert_eq!(first_point.failed, 1);
        assert_eq!(first_point.succeeded, 2);
        assert_eq!(first_point.successful_executions, 2);
        assert_eq!(first_point.execution_ms, 250);

        let second_worker = aggregation
            .workers
            .get(1)
            .expect("second worker summary should exist");
        assert_eq!(second_worker.totals.panicked, 1);
        assert_eq!(second_worker.totals.failed_executions, 1);
        assert_eq!(second_worker.totals.panicked_executions, 1);
        assert_eq!(second_worker.totals.failed_executions_without_panics(), 0);

        let filtered = aggregate_counter_hashes(&minutes, hashes, Some(&identity));
        assert_eq!(filtered.totals.processed, 3);
        assert_eq!(filtered.totals.failed, 1);
        assert_eq!(filtered.totals.succeeded, 2);
        assert_eq!(filtered.totals.successful_executions, 2);
        assert_eq!(filtered.totals.execution_ms, 250);
        assert_eq!(filtered.workers.len(), 1);
        let filtered_worker = filtered
            .workers
            .first()
            .expect("filtered worker summary should exist");
        let empty_point = filtered_worker
            .series
            .get(1)
            .expect("filtered worker should have a second series point");
        assert_eq!(empty_point.processed, 0);
    }

    #[test]
    fn metric_identity_round_trips_with_delimiters() {
        let identity = MetricIdentity {
            worker: "crate::worker|Name".to_string(),
        };

        assert_eq!(
            MetricIdentity::from_field_key(&identity.field_key()),
            Some(identity)
        );
    }

    #[test]
    fn queue_counter_hashes_aggregate_with_missing_fields_as_zero() {
        let queue = "critical:tenant|fast";
        let mut hashes = vec![HashMap::new(), HashMap::new()];
        let second_hash = hashes
            .get_mut(1)
            .expect("query should include a second minute bucket");
        second_hash.insert(queue_metric_field(queue, QUEUE_METRIC_PROCESSED_JOBS), 5);
        second_hash.insert(queue_metric_field(queue, QUEUE_METRIC_FAILED_JOBS), 2);

        let counters = aggregate_queue_counter_hashes(hashes);
        let totals = counters
            .get(queue)
            .expect("queue counters should be aggregated");

        assert_eq!(totals.processed, 5);
        assert_eq!(totals.succeeded, 0);
        assert_eq!(totals.failed, 2);
    }

    #[test]
    fn job_metrics_buffer_records_queue_counters() {
        let mut buffer = JobMetricsBuffer::default();

        buffer.record(&WorkerResult {
            kind: WorkerResultKind::Success,
            worker_name: "Worker".to_string(),
            queue: "default".to_string(),
            execution_ms: 10,
            job_count: 3,
        });
        buffer.record(&WorkerResult {
            kind: WorkerResultKind::Panicked,
            worker_name: "Worker".to_string(),
            queue: "default".to_string(),
            execution_ms: 10,
            job_count: 1,
        });

        let queue_records = buffer.queue_records().collect::<Vec<_>>();
        assert_eq!(queue_records.len(), 1);
        let (_, queue, counters) = queue_records
            .first()
            .expect("queue counter record should exist");
        assert_eq!(*queue, "default");
        assert_eq!(counters.processed, 4);
        assert_eq!(counters.succeeded, 3);
        assert_eq!(counters.failed, 1);
    }

    #[test]
    fn queue_length_series_aggregates_and_sorts_by_peak_length() {
        let minutes = vec![10, 11, 12];
        let mut hashes = vec![HashMap::new(); minutes.len()];
        hashes
            .first_mut()
            .expect("query should include a first minute bucket")
            .insert("default".to_string(), 2);
        hashes
            .get_mut(1)
            .expect("query should include a second minute bucket")
            .insert("critical".to_string(), 7);
        let third_hash = hashes
            .get_mut(2)
            .expect("query should include a third minute bucket");
        third_hash.insert("default".to_string(), 4);
        third_hash.insert("critical".to_string(), 0);

        let queues = queue_length_series_from_hashes(&minutes, hashes);

        assert_eq!(queues.len(), 2);
        let critical = queues
            .first()
            .expect("critical queue length series should exist");
        assert_eq!(critical.queue, "critical");
        assert_eq!(
            critical
                .series
                .first()
                .expect("critical series should include the first point")
                .timestamp,
            600
        );
        assert_eq!(
            critical
                .series
                .iter()
                .map(|point| point.enqueued)
                .collect::<Vec<_>>(),
            vec![0, 7, 0]
        );

        let default = queues
            .get(1)
            .expect("default queue length series should exist");
        assert_eq!(default.queue, "default");
        assert_eq!(
            default
                .series
                .iter()
                .map(|point| point.enqueued)
                .collect::<Vec<_>>(),
            vec![2, 0, 4]
        );
    }

    #[test]
    fn metric_identity_reads_legacy_worker_queue_keys_as_worker_only() {
        let legacy_key = "6:Workerdefault";

        assert_eq!(
            MetricIdentity::from_field_key(legacy_key),
            Some(MetricIdentity {
                worker: "Worker".to_string(),
            })
        );
    }
}
