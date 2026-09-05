use askama::Template;
use askama_web::WebTemplate;
use std::collections::HashMap;

use crate::filters;
use crate::pagination::JobPage;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum QueueConcurrencyStatus {
    Fixed,
    Default { default: usize },
    Override { default: usize },
}

#[derive(Debug, Clone)]
pub(crate) struct QueueRuntimeConfigView {
    pub config: oxana::QueueRuntimeConfig,
    pub concurrency_status: QueueConcurrencyStatus,
}

impl QueueRuntimeConfigView {
    fn concurrency_label(&self) -> String {
        self.config
            .concurrency
            .map_or_else(|| "-".to_string(), |concurrency| concurrency.to_string())
    }

    fn concurrency_default_label(&self) -> String {
        match self.concurrency_status {
            QueueConcurrencyStatus::Fixed => self.concurrency_label(),
            QueueConcurrencyStatus::Default { default }
            | QueueConcurrencyStatus::Override { default } => default.to_string(),
        }
    }

    fn can_change_concurrency(&self) -> bool {
        !matches!(self.concurrency_status, QueueConcurrencyStatus::Fixed)
    }

    fn has_concurrency_override(&self) -> bool {
        matches!(
            self.concurrency_status,
            QueueConcurrencyStatus::Override { .. }
        )
    }
}

fn busy_for(stats: &oxana::Stats, key: &str) -> usize {
    stats
        .processing
        .iter()
        .filter(|p| p.job_envelope.queue == key)
        .count()
}

fn busy_for_process(stats: &oxana::Stats, process: &oxana::Process) -> usize {
    let id = process.id();
    stats
        .processing
        .iter()
        .filter(|p| p.process_id == id)
        .count()
}

fn concurrency_for(queue_configs: &HashMap<String, QueueRuntimeConfigView>, key: &str) -> String {
    queue_configs.get(key).map_or_else(
        || "-".to_string(),
        QueueRuntimeConfigView::concurrency_label,
    )
}

fn default_concurrency_for(
    queue_configs: &HashMap<String, QueueRuntimeConfigView>,
    key: &str,
) -> String {
    queue_configs
        .get(key)
        .map_or_else(String::new, |config| config.concurrency_default_label())
}

fn has_concurrency_override_for(
    queue_configs: &HashMap<String, QueueRuntimeConfigView>,
    key: &str,
) -> bool {
    queue_configs
        .get(key)
        .is_some_and(QueueRuntimeConfigView::has_concurrency_override)
}

fn state_for(queue_configs: &HashMap<String, QueueRuntimeConfigView>, key: &str) -> String {
    queue_configs.get(key).map_or_else(
        || "Active".to_string(),
        |config| config.config.state.label().to_string(),
    )
}

fn state_class_for(
    queue_configs: &HashMap<String, QueueRuntimeConfigView>,
    key: &str,
) -> &'static str {
    match queue_configs.get(key).map(|config| config.config.state) {
        Some(oxana::QueueState::Paused) => "text-yellow-300",
        _ => "text-green-300",
    }
}

fn queue_action_path(base_path: &str, key: &str, action: &str) -> String {
    format!(
        "{}/queues/{}/{}",
        base_path,
        urlencoding::encode(key),
        action
    )
}

fn metrics_chart_bucket_minutes(window_minutes: usize) -> usize {
    match window_minutes {
        0..=120 => 1,
        121..=480 => 5,
        _ => 15,
    }
}

fn downsample_job_metrics_points(
    points: &[oxana::JobMetricsPoint],
    window_minutes: usize,
) -> Vec<oxana::JobMetricsPoint> {
    let bucket_minutes = metrics_chart_bucket_minutes(window_minutes);
    if bucket_minutes == 1 {
        return points.to_vec();
    }

    points
        .chunks(bucket_minutes)
        .map(|chunk| {
            let timestamp = chunk.first().map_or(0, |point| point.timestamp);
            chunk.iter().fold(
                oxana::JobMetricsPoint {
                    timestamp,
                    ..Default::default()
                },
                |mut sum, point| {
                    sum.processed = sum.processed.saturating_add(point.processed);
                    sum.succeeded = sum.succeeded.saturating_add(point.succeeded);
                    sum.failed = sum.failed.saturating_add(point.failed);
                    sum.panicked = sum.panicked.saturating_add(point.panicked);
                    sum.successful_executions = sum
                        .successful_executions
                        .saturating_add(point.successful_executions);
                    sum.failed_executions = sum
                        .failed_executions
                        .saturating_add(point.failed_executions);
                    sum.panicked_executions = sum
                        .panicked_executions
                        .saturating_add(point.panicked_executions);
                    sum.execution_ms = sum.execution_ms.saturating_add(point.execution_ms);
                    sum
                },
            )
        })
        .collect()
}

macro_rules! impl_queue_view_helpers {
    ($($ty:ty),* $(,)?) => {$(
        impl $ty {
            pub fn concurrency_for(&self, key: &str) -> String {
                concurrency_for(&self.queue_configs, key)
            }

            pub fn default_concurrency_for(&self, key: &str) -> String {
                default_concurrency_for(&self.queue_configs, key)
            }

            pub fn has_concurrency_override_for(&self, key: &str) -> bool {
                has_concurrency_override_for(&self.queue_configs, key)
            }

            pub fn state_for(&self, key: &str) -> String {
                state_for(&self.queue_configs, key)
            }

            pub fn state_class_for(&self, key: &str) -> &'static str {
                state_class_for(&self.queue_configs, key)
            }

            pub fn dynamic_queue_key(&self, key: &str, suffix: &str) -> String {
                format!("{key}#{suffix}")
            }
        }
    )*};
}

macro_rules! impl_busy_helpers {
    ($($ty:ty),* $(,)?) => {$(
        impl $ty {
            pub fn busy_for(&self, key: &str) -> usize {
                busy_for(&self.stats, key)
            }

            pub fn busy_for_process(&self, process: &oxana::Process) -> usize {
                busy_for_process(&self.stats, process)
            }
        }
    )*};
}

impl_queue_view_helpers!(DashboardTemplate, BusyTemplate, QueuesTemplate);
impl_busy_helpers!(DashboardTemplate, BusyTemplate);

#[derive(Template, WebTemplate)]
#[template(path = "dashboard.html")]
pub(crate) struct DashboardTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub stats: oxana::Stats,
    pub queue_configs: HashMap<String, QueueRuntimeConfigView>,
}

#[derive(Template, WebTemplate)]
#[template(path = "busy.html")]
pub(crate) struct BusyTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub stats: oxana::Stats,
    pub queue_configs: HashMap<String, QueueRuntimeConfigView>,
}

#[derive(Template, WebTemplate)]
#[template(path = "queues.html")]
pub(crate) struct QueuesTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub stats: oxana::Stats,
    pub queue_configs: HashMap<String, QueueRuntimeConfigView>,
    pub queue_lengths: oxana::QueueLengthMetricsSnapshot,
    pub sort: String,
    pub dir: String,
}

impl QueuesTemplate {
    pub fn next_dir(&self, col: &str) -> &'static str {
        if self.sort == col {
            if self.dir == "desc" { "asc" } else { "desc" }
        } else {
            "desc"
        }
    }

    pub fn sort_arrow(&self, col: &str) -> &'static str {
        if self.sort == col {
            if self.dir == "asc" {
                " \u{25B2}"
            } else {
                " \u{25BC}"
            }
        } else {
            ""
        }
    }

    pub fn sort_href(&self, col: &str) -> String {
        format!(
            "{}/queues?sort={}&dir={}&minutes={}",
            self.base_path,
            col,
            self.next_dir(col),
            self.queue_lengths.minutes
        )
    }

    pub fn queue_length_chart_data_json(&self) -> String {
        let timestamps: Vec<i64> = self.queue_lengths.queues.first().map_or_else(
            || {
                (0..self.queue_lengths.minutes)
                    .map(|idx| self.queue_lengths.starts_at + i64::try_from(idx).unwrap_or(0) * 60)
                    .collect()
            },
            |queue| queue.series.iter().map(|point| point.timestamp).collect(),
        );
        let series: Vec<serde_json::Value> = self
            .queue_lengths
            .queues
            .iter()
            .map(|queue| {
                let data: Vec<u64> = queue.series.iter().map(|point| point.enqueued).collect();
                serde_json::json!({
                    "label": queue.queue.clone(),
                    "data": data,
                })
            })
            .collect();

        serde_json::json!({
            "timestamps": timestamps,
            "series": series,
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{QueueConcurrencyStatus, QueueRuntimeConfigView, QueuesTemplate};
    use askama::Template;
    use std::collections::HashMap;

    fn queues_template(queue_lengths: oxana::QueueLengthMetricsSnapshot) -> QueuesTemplate {
        QueuesTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/queues",
            stats: oxana::Stats {
                global: oxana::StatsGlobal {
                    jobs: 0,
                    enqueued: 0,
                    processed: 0,
                    failed: 0,
                    dead: 0,
                    scheduled: 0,
                    retries: 0,
                    latency_s_max: 0.0,
                },
                processes: Vec::new(),
                processing: Vec::new(),
                queues: Vec::new(),
            },
            queue_configs: HashMap::new(),
            queue_lengths,
            sort: "key".to_string(),
            dir: "asc".to_string(),
        }
    }

    fn queue_stats(key: &str) -> oxana::QueueStats {
        oxana::QueueStats {
            key: key.to_string(),
            enqueued: 0,
            processed: 0,
            succeeded: 0,
            panicked: 0,
            failed: 0,
            latency_s: 0.0,
            rate: oxana::QueueRateStats::default(),
            queues: Vec::new(),
        }
    }

    #[test]
    fn queue_sort_href_preserves_chart_window() {
        let template = queues_template(oxana::QueueLengthMetricsSnapshot {
            starts_at: 0,
            ends_at: 0,
            minutes: 120,
            queues: Vec::new(),
        });

        assert_eq!(
            template.sort_href("enqueued"),
            "/admin/queues?sort=enqueued&dir=desc&minutes=120"
        );
    }

    #[test]
    fn queue_length_chart_data_json_uses_window_when_no_queues_exist() {
        let template = queues_template(oxana::QueueLengthMetricsSnapshot {
            starts_at: 60,
            ends_at: 120,
            minutes: 2,
            queues: Vec::new(),
        });
        let payload: serde_json::Value =
            serde_json::from_str(&template.queue_length_chart_data_json()).unwrap();

        assert_eq!(payload["timestamps"], serde_json::json!([60, 120]));
        assert_eq!(payload["series"], serde_json::json!([]));
    }

    #[test]
    fn queue_length_chart_data_json_serializes_queue_series() {
        let template = queues_template(oxana::QueueLengthMetricsSnapshot {
            starts_at: 60,
            ends_at: 120,
            minutes: 2,
            queues: vec![oxana::QueueLengthMetricsSeries {
                queue: "default".to_string(),
                series: vec![
                    oxana::QueueLengthMetricsPoint {
                        timestamp: 60,
                        enqueued: 2,
                    },
                    oxana::QueueLengthMetricsPoint {
                        timestamp: 120,
                        enqueued: 5,
                    },
                ],
            }],
        });
        let payload: serde_json::Value =
            serde_json::from_str(&template.queue_length_chart_data_json()).unwrap();

        assert_eq!(payload["timestamps"], serde_json::json!([60, 120]));
        assert_eq!(
            payload["series"],
            serde_json::json!([{ "label": "default", "data": [2, 5] }])
        );
    }

    #[test]
    fn queues_template_renders_compact_concurrency_override_without_change_form() {
        let mut template = queues_template(oxana::QueueLengthMetricsSnapshot {
            starts_at: 0,
            ends_at: 0,
            minutes: 60,
            queues: Vec::new(),
        });
        template.stats.queues = vec![queue_stats("emails")];
        template.queue_configs.insert(
            "emails".to_string(),
            QueueRuntimeConfigView {
                config: oxana::QueueRuntimeConfig::new(5),
                concurrency_status: QueueConcurrencyStatus::Override { default: 2 },
            },
        );

        let rendered = template.render().unwrap();

        assert!(rendered.contains(
            "5 <span class=\"text-gray-500\">(<span class=\"line-through\">2</span>)</span>"
        ));
        assert!(!rendered.contains("action=\"/admin/queues/emails/concurrency\""));
        assert!(!rendered.contains("data-default-concurrency=\"2\""));
    }
}

#[derive(Template, WebTemplate)]
#[template(path = "metrics.html")]
pub(crate) struct MetricsTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub metrics: oxana::JobMetricsSnapshot,
    pub table_workers: Vec<oxana::WorkerMetricsSummary>,
    pub sort: String,
    pub dir: String,
}

impl MetricsTemplate {
    pub fn has_sort(&self) -> bool {
        !self.sort.is_empty()
    }

    fn effective_sort(&self) -> &str {
        if self.sort.is_empty() {
            "total_time"
        } else {
            &self.sort
        }
    }

    pub fn next_dir(&self, col: &str) -> &'static str {
        if self.effective_sort() == col {
            if self.dir == "desc" { "asc" } else { "desc" }
        } else {
            "desc"
        }
    }

    pub fn sort_arrow(&self, col: &str) -> &'static str {
        if self.effective_sort() == col {
            if self.dir == "asc" {
                " \u{25B2}"
            } else {
                " \u{25BC}"
            }
        } else {
            ""
        }
    }

    pub fn sort_href(&self, col: &str) -> String {
        format!(
            "{}/metrics?sort={}&dir={}&minutes={}",
            self.base_path,
            col,
            self.next_dir(col),
            self.metrics.minutes
        )
    }

    pub fn metric_identity_label(&self, identity: &oxana::MetricIdentity) -> String {
        metric_identity_label(identity)
    }

    pub fn execution_chart_data_json(&self) -> String {
        self.summary_chart_data_json(|point| point.execution_ms as f64 / 1000.0)
    }

    pub fn processed_chart_data_json(&self) -> String {
        self.summary_chart_data_json(|point| point.processed as f64)
    }

    fn summary_chart_data_json(&self, value: impl Fn(&oxana::JobMetricsPoint) -> f64) -> String {
        let summary_points =
            downsample_job_metrics_points(&self.metrics.series, self.metrics.minutes);
        let timestamps: Vec<i64> = summary_points.iter().map(|point| point.timestamp).collect();
        let series: Vec<serde_json::Value> = self
            .metrics
            .workers
            .iter()
            .map(|worker| {
                let points = downsample_job_metrics_points(&worker.series, self.metrics.minutes);
                let data: Vec<f64> = points.iter().map(&value).collect();
                serde_json::json!({
                    "label": metric_identity_label(&worker.identity),
                    "fullLabel": worker.identity.worker,
                    "data": data,
                })
            })
            .collect();

        serde_json::json!({
            "timestamps": timestamps,
            "series": series,
        })
        .to_string()
    }
}

#[cfg(test)]
mod metrics_template_tests {
    use super::{MetricDetailTemplate, MetricsTemplate, metrics_chart_bucket_minutes};

    fn metrics_template(sort: &str, dir: &str) -> MetricsTemplate {
        MetricsTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/metrics",
            metrics: oxana::JobMetricsSnapshot {
                starts_at: 0,
                ends_at: 0,
                minutes: 120,
                totals: oxana::JobMetricsTotals::default(),
                series: Vec::new(),
                workers: Vec::new(),
            },
            table_workers: Vec::new(),
            sort: sort.to_string(),
            dir: dir.to_string(),
        }
    }

    fn metric_detail_template(
        minutes: usize,
        series: Vec<oxana::JobMetricsPoint>,
    ) -> MetricDetailTemplate {
        MetricDetailTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/metrics",
            metrics: oxana::JobMetricsDetail {
                identity: oxana::MetricIdentity {
                    worker: "Worker".to_string(),
                },
                starts_at: 0,
                ends_at: 0,
                minutes,
                totals: oxana::JobMetricsTotals::default(),
                series,
                histogram: Vec::new(),
            },
        }
    }

    fn metric_point(
        idx: usize,
        processed: u64,
        successful_executions: u64,
        failed_executions_without_panics: u64,
        panicked_executions: u64,
        execution_ms: u64,
    ) -> oxana::JobMetricsPoint {
        let failed_executions =
            failed_executions_without_panics.saturating_add(panicked_executions);
        oxana::JobMetricsPoint {
            timestamp: 60 + i64::try_from(idx).unwrap_or(0) * 60,
            processed,
            succeeded: processed.saturating_sub(failed_executions),
            failed: failed_executions,
            panicked: panicked_executions,
            successful_executions,
            failed_executions,
            panicked_executions,
            execution_ms,
        }
    }

    fn worker_metrics(worker: &str, execution_ms: u64) -> oxana::WorkerMetricsSummary {
        oxana::WorkerMetricsSummary {
            identity: oxana::MetricIdentity {
                worker: worker.to_string(),
            },
            totals: oxana::JobMetricsTotals {
                execution_ms,
                successful_executions: 1,
                ..oxana::JobMetricsTotals::default()
            },
            series: vec![metric_point(0, 1, 1, 0, 0, execution_ms)],
        }
    }

    #[test]
    fn metrics_chart_bucket_policy_matches_selected_windows() {
        assert_eq!(metrics_chart_bucket_minutes(60), 1);
        assert_eq!(metrics_chart_bucket_minutes(120), 1);
        assert_eq!(metrics_chart_bucket_minutes(240), 5);
        assert_eq!(metrics_chart_bucket_minutes(480), 5);
        assert_eq!(metrics_chart_bucket_minutes(1440), 15);
    }

    #[test]
    fn metrics_sort_href_preserves_chart_window_and_defaults_descending() {
        let template = metrics_template("", "desc");

        assert_eq!(
            template.sort_href("processed"),
            "/admin/metrics?sort=processed&dir=desc&minutes=120"
        );
    }

    #[test]
    fn metrics_sort_href_toggles_current_column() {
        let template = metrics_template("processed", "desc");

        assert_eq!(
            template.sort_href("processed"),
            "/admin/metrics?sort=processed&dir=asc&minutes=120"
        );
    }

    #[test]
    fn metrics_default_sort_shows_total_time_indicator() {
        let template = metrics_template("", "desc");

        assert_eq!(template.sort_arrow("total_time"), " \u{25BC}");
        assert_eq!(
            template.sort_href("total_time"),
            "/admin/metrics?sort=total_time&dir=asc&minutes=120"
        );
    }

    #[test]
    fn metrics_chart_data_uses_snapshot_worker_order() {
        let mut template = metrics_template("processed", "desc");
        template.metrics.series = vec![oxana::JobMetricsPoint {
            timestamp: 60,
            ..oxana::JobMetricsPoint::default()
        }];
        template.metrics.workers = vec![
            worker_metrics("ChartFirst", 100),
            worker_metrics("ChartSecond", 200),
        ];
        template.table_workers = vec![
            worker_metrics("TableFirst", 300),
            worker_metrics("TableSecond", 400),
        ];

        let payload: serde_json::Value =
            serde_json::from_str(&template.execution_chart_data_json()).unwrap();

        assert_eq!(payload["series"][0]["fullLabel"], "ChartFirst");
        assert_eq!(payload["series"][1]["fullLabel"], "ChartSecond");
    }

    #[test]
    fn metrics_chart_data_downsamples_four_hour_window_to_five_minute_buckets() {
        let mut template = metrics_template("processed", "desc");
        template.metrics.minutes = 240;
        let series: Vec<oxana::JobMetricsPoint> = (0..10)
            .map(|idx| {
                let value = u64::try_from(idx + 1).unwrap_or(0);
                metric_point(idx, value, 1, 0, 0, value * 1000)
            })
            .collect();
        template.metrics.series = series.clone();
        template.metrics.workers = vec![oxana::WorkerMetricsSummary {
            identity: oxana::MetricIdentity {
                worker: "Worker".to_string(),
            },
            totals: oxana::JobMetricsTotals::default(),
            series,
        }];

        let execution_payload: serde_json::Value =
            serde_json::from_str(&template.execution_chart_data_json()).unwrap();
        let processed_payload: serde_json::Value =
            serde_json::from_str(&template.processed_chart_data_json()).unwrap();

        assert_eq!(
            execution_payload["timestamps"],
            serde_json::json!([60, 360])
        );
        assert_eq!(
            processed_payload["timestamps"],
            serde_json::json!([60, 360])
        );
        assert_eq!(
            execution_payload["series"][0]["data"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_f64().unwrap())
                .collect::<Vec<_>>(),
            vec![15.0, 40.0]
        );
        assert_eq!(
            processed_payload["series"][0]["data"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_f64().unwrap())
                .collect::<Vec<_>>(),
            vec![15.0, 40.0]
        );
    }

    #[test]
    fn metric_detail_chart_data_downsamples_24_hour_window_to_fifteen_minute_buckets() {
        let template = metric_detail_template(
            1440,
            (0..30)
                .map(|idx| {
                    if idx < 15 {
                        metric_point(idx, 1, 1, 0, 0, 100)
                    } else {
                        metric_point(idx, 2, 2, 0, 0, 600)
                    }
                })
                .collect(),
        );

        let average_payload: serde_json::Value =
            serde_json::from_str(&template.detail_average_chart_data_json()).unwrap();
        let total_payload: serde_json::Value =
            serde_json::from_str(&template.detail_total_chart_data_json()).unwrap();

        assert_eq!(average_payload[0], serde_json::json!([60, 960]));
        assert_eq!(
            average_payload[1]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_f64().unwrap())
                .collect::<Vec<_>>(),
            vec![100.0, 300.0]
        );
        assert_eq!(total_payload[0], serde_json::json!([60, 960]));
        assert_eq!(total_payload[1], serde_json::json!([15, 30]));
        assert_eq!(total_payload[2], serde_json::json!([0, 0]));
        assert_eq!(total_payload[3], serde_json::json!([0, 0]));
    }
}

fn metric_identity_label(identity: &oxana::MetricIdentity) -> String {
    identity
        .worker
        .rsplit("::")
        .next()
        .unwrap_or(&identity.worker)
        .to_string()
}

#[derive(Template, WebTemplate)]
#[template(path = "metric_detail.html")]
pub(crate) struct MetricDetailTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub metrics: oxana::JobMetricsDetail,
}

impl MetricDetailTemplate {
    pub fn detail_average_chart_data_json(&self) -> String {
        let points = downsample_job_metrics_points(&self.metrics.series, self.metrics.minutes);
        let timestamps: Vec<i64> = points.iter().map(|point| point.timestamp).collect();
        let average_ms: Vec<f64> = points
            .iter()
            .map(oxana::JobMetricsPoint::average_execution_ms)
            .collect();

        serde_json::json!([timestamps, average_ms]).to_string()
    }

    pub fn detail_total_chart_data_json(&self) -> String {
        let points = downsample_job_metrics_points(&self.metrics.series, self.metrics.minutes);
        let timestamps: Vec<i64> = points.iter().map(|point| point.timestamp).collect();
        let succeeded: Vec<u64> = points
            .iter()
            .map(|point| point.successful_executions)
            .collect();
        let failed_without_panics: Vec<u64> = points
            .iter()
            .map(oxana::JobMetricsPoint::failed_executions_without_panics)
            .collect();
        let panicked: Vec<u64> = points
            .iter()
            .map(|point| point.panicked_executions)
            .collect();

        serde_json::json!([timestamps, succeeded, failed_without_panics, panicked]).to_string()
    }

    pub fn max_histogram_count(&self) -> u64 {
        self.metrics
            .histogram
            .iter()
            .map(|bucket| bucket.count)
            .max()
            .unwrap_or(0)
    }

    pub fn histogram_width(&self, count: &u64) -> String {
        let max = self.max_histogram_count();
        if max == 0 {
            "0%".to_string()
        } else {
            format!("{:.1}%", (*count as f64 / max as f64) * 100.0)
        }
    }
}

pub(crate) enum CronRow {
    Group { name: String, depth: usize },
    Worker { view: CronWorkerView, depth: usize },
}

impl CronRow {
    fn depth(&self) -> usize {
        match self {
            Self::Group { depth, .. } | Self::Worker { depth, .. } => *depth,
        }
    }

    pub fn indent_px(&self) -> usize {
        self.depth() * 20
    }
}

#[derive(Clone)]
pub(crate) struct CronWorkerView {
    pub name: String,
    pub short_name: String,
    pub schedule: String,
    pub queue_key: String,
    pub next_run_micros: Option<i64>,
}

#[derive(Template, WebTemplate)]
#[template(path = "cron.html")]
pub(crate) struct CronTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub rows: Vec<CronRow>,
    pub total: usize,
    pub enqueued: bool,
}

#[derive(Clone)]
pub(crate) struct OnDemandJobView {
    pub name: String,
    pub short_name: String,
    pub args_template_json: String,
}

pub(crate) enum OnDemandRow {
    Group { name: String, depth: usize },
    Job { view: OnDemandJobView, depth: usize },
}

impl OnDemandRow {
    fn depth(&self) -> usize {
        match self {
            Self::Group { depth, .. } | Self::Job { depth, .. } => *depth,
        }
    }

    pub fn indent_px(&self) -> usize {
        self.depth() * 20
    }
}

#[derive(Clone)]
pub(crate) struct OnDemandQueueView {
    pub key: String,
    pub selected: bool,
}

#[derive(Template, WebTemplate)]
#[template(path = "on_demand.html")]
pub(crate) struct OnDemandTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub rows: Vec<OnDemandRow>,
    pub queues: Vec<OnDemandQueueView>,
    pub total: usize,
    pub scheduled: bool,
    pub invalid_json: bool,
}

#[derive(Template, WebTemplate)]
#[template(path = "queue_detail.html")]
pub(crate) struct QueueDetailTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub queue_key: String,
    pub queue_stats: Option<oxana::QueueStats>,
    pub queue_config: QueueRuntimeConfigView,
    pub active_jobs: Vec<oxana::StatsProcessing>,
    pub busy: usize,
    pub page: JobPage,
}

impl QueueDetailTemplate {
    pub fn state_label(&self) -> &'static str {
        self.queue_config.config.state.label()
    }

    pub fn state_class(&self) -> &'static str {
        match self.queue_config.config.state {
            oxana::QueueState::Active => "text-green-300 border-green-800 bg-green-900/30",
            oxana::QueueState::Paused => "text-yellow-300 border-yellow-800 bg-yellow-900/30",
        }
    }

    pub fn is_paused(&self) -> bool {
        matches!(self.queue_config.config.state, oxana::QueueState::Paused)
    }

    pub fn concurrency_label(&self) -> String {
        self.queue_config.concurrency_label()
    }

    pub fn concurrency_default_label(&self) -> String {
        self.queue_config.concurrency_default_label()
    }

    pub fn can_change_concurrency(&self) -> bool {
        self.queue_config.can_change_concurrency()
    }

    pub fn has_concurrency_override(&self) -> bool {
        self.queue_config.has_concurrency_override()
    }

    pub fn queue_action_path(&self, action: &str) -> String {
        queue_action_path(&self.base_path, &self.queue_key, action)
    }
}

pub(crate) enum JobListKind {
    Dead,
    Retries,
    Scheduled,
}

impl JobListKind {
    pub fn path(&self) -> &'static str {
        match self {
            Self::Dead => "/dead",
            Self::Retries => "/retries",
            Self::Scheduled => "/scheduled",
        }
    }

    pub fn title(&self) -> &'static str {
        match self {
            Self::Dead => "Dead Jobs",
            Self::Retries => "Retries",
            Self::Scheduled => "Scheduled",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Dead => "dead jobs",
            Self::Retries => "retrying jobs",
            Self::Scheduled => "scheduled jobs",
        }
    }

    pub fn empty_label(&self) -> &'static str {
        match self {
            Self::Dead => "No dead jobs",
            Self::Retries => "No retrying jobs",
            Self::Scheduled => "No scheduled jobs",
        }
    }

    pub fn border_class(&self) -> &'static str {
        match self {
            Self::Dead => "border-red-900/50",
            Self::Retries => "border-orange-900/50",
            Self::Scheduled => "border-yellow-900/50",
        }
    }

    pub fn is_dead(&self) -> bool {
        matches!(self, Self::Dead)
    }

    pub fn is_retries(&self) -> bool {
        matches!(self, Self::Retries)
    }

    pub fn dot_class(&self) -> &'static str {
        match self {
            Self::Dead => "bg-red-400",
            Self::Retries => "bg-orange-400",
            Self::Scheduled => "bg-yellow-400",
        }
    }
}

#[derive(Template, WebTemplate)]
#[template(path = "global_jobs.html")]
pub(crate) struct GlobalJobsTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub kind: JobListKind,
    pub page: JobPage,
}

#[derive(Template, WebTemplate)]
#[template(path = "job_detail.html")]
pub(crate) struct JobDetailTemplate {
    pub base_path: String,
    pub active_tab: &'static str,
    pub job_id: String,
    pub job: Option<oxana::JobEnvelope>,
    pub is_dead: bool,
}

#[cfg(test)]
mod job_card_tests {
    use super::{GlobalJobsTemplate, JobListKind};
    use crate::pagination::JobPage;
    use askama::Template;
    use serde_json::json;

    fn job_envelope(args: serde_json::Value) -> oxana::JobEnvelope {
        job_envelope_with_id("job-1", args)
    }

    fn job_envelope_with_id(id: &str, args: serde_json::Value) -> oxana::JobEnvelope {
        oxana::JobEnvelope {
            id: id.to_string(),
            queue: "default".to_string(),
            job: oxana::JobData {
                name: "crate::ImportGame".to_string(),
                args,
            },
            meta: oxana::JobMeta {
                id: id.to_string(),
                retries: 0,
                unique: false,
                on_conflict: None,
                created_at: 1_000_000,
                scheduled_at: 1_000_000,
                started_at: None,
                state: None,
                resurrect: true,
                error: None,
                throttle_cost: None,
            },
        }
    }

    #[test]
    fn global_job_pagination_uses_lookahead_and_preserves_links() {
        for kind in [
            JobListKind::Scheduled,
            JobListKind::Dead,
            JobListKind::Retries,
        ] {
            let jobs = (1..=51)
                .map(|id| job_envelope_with_id(&format!("job-{id}"), json!({})))
                .collect();
            let path = kind.path();
            let template = GlobalJobsTemplate {
                base_path: "/admin".to_string(),
                active_tab: path,
                kind,
                page: JobPage::new(2, 101, jobs),
            };
            let rendered = template.render().unwrap();

            assert!(rendered.contains("href=\"/admin/jobs/job-50\""));
            assert!(!rendered.contains("href=\"/admin/jobs/job-51\""));
            assert!(rendered.contains("href=\"?page=1\""));
            assert!(rendered.contains("href=\"?page=3\""));
            assert!(rendered.contains("51&ndash;100 of 101"));
        }
    }

    #[test]
    fn pagination_normalizes_zero_and_does_not_infer_next_page_from_total() {
        let opts = JobPage::list_opts(0);
        assert_eq!((opts.offset, opts.count), (0, 51));
        assert_eq!(JobPage::list_opts(2).offset, 50);

        let page = JobPage::new(0, 100, vec![job_envelope(json!({}))]);
        assert_eq!(page.number, 1);
        assert_eq!((page.range_start(), page.range_end()), (1, 1));
        assert!(!page.has_next);

        let page = JobPage::new(2, 50, vec![job_envelope(json!({}))]);
        assert_eq!(page.range_end(), 50);
    }

    #[test]
    fn global_job_cards_escape_progress_notes_and_keep_other_state_visible() {
        for (state, expected) in [
            (json!([1, 4, "<b>Importing</b>"]), "1 / 4 (25%)"),
            (json!({"checkpoint": "resume-here"}), "resume-here"),
        ] {
            let mut job = job_envelope(json!({}));
            job.meta.state = Some(state);
            let template = GlobalJobsTemplate {
                base_path: "/admin".to_string(),
                active_tab: "/scheduled",
                kind: JobListKind::Scheduled,
                page: JobPage::new(1, 1, vec![job]),
            };
            let rendered = template.render().unwrap();
            assert!(rendered.contains(expected));
            assert!(!rendered.contains("<b>Importing</b>"));
            if expected == "1 / 4 (25%)" {
                assert!(rendered.contains("&#60;b&#62;Importing&#60;/b&#62;"));
                assert!(rendered.contains("style=\"width: 25%\""));
                assert!(rendered.contains("ETA —"));
            }
        }
    }

    #[test]
    fn global_job_cards_render_simple_arg_pills() {
        let template = GlobalJobsTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/scheduled",
            kind: JobListKind::Scheduled,
            page: JobPage::new(
                1,
                1,
                vec![job_envelope(json!({
                    "game_id": 12345,
                    "dry_run": false,
                }))],
            ),
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("title=\"game_id: 12345\""));
        assert!(rendered.contains("title=\"dry_run: false\""));
        assert!(
            !rendered.contains(
                "<summary class=\"text-xs text-gray-500 cursor-pointer hover:text-gray-300\">Arguments</summary>"
            )
        );
    }

    #[test]
    fn global_job_cards_render_pills_and_json_args_when_any_arg_is_complex() {
        let template = GlobalJobsTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/scheduled",
            kind: JobListKind::Scheduled,
            page: JobPage::new(
                1,
                1,
                vec![job_envelope(json!({
                    "game_id": 12345,
                    "metadata": { "season": 2026 },
                }))],
            ),
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("title=\"game_id: 12345\""));
        assert!(!rendered.contains("title=\"metadata:"));
        assert!(
            rendered.contains(
                "<summary class=\"text-xs text-gray-500 cursor-pointer hover:text-gray-300\">Arguments</summary>"
            )
        );
    }

    #[test]
    fn global_job_cards_encode_slashes_in_job_links() {
        let template = GlobalJobsTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/scheduled",
            kind: JobListKind::Scheduled,
            page: JobPage::new(
                1,
                1,
                vec![job_envelope_with_id("crate::Worker/type-123", json!({}))],
            ),
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("href=\"/admin/jobs/crate%3A%3AWorker%2Ftype-123\""));
    }

    #[test]
    fn dead_jobs_template_renders_revive_all_action() {
        let template = GlobalJobsTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/dead",
            kind: JobListKind::Dead,
            page: JobPage::new(1, 1, vec![job_envelope(json!({}))]),
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("action=\"/admin/dead/revive_all\""));
        assert!(rendered.contains(">Revive All</button>"));
        assert!(rendered.contains("confirmReviveAllDead()"));
    }

    #[test]
    fn retries_template_renders_retry_all_now_action() {
        let template = GlobalJobsTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/retries",
            kind: JobListKind::Retries,
            page: JobPage::new(1, 1, vec![job_envelope(json!({}))]),
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("action=\"/admin/retries/retry_all_now\""));
        assert!(rendered.contains(">Retry All Now</button>"));
        assert!(rendered.contains("confirmRetryAllNow()"));
    }
}

#[cfg(test)]
mod cron_tests {
    use super::{CronRow, CronTemplate, CronWorkerView};
    use askama::Template;

    #[test]
    fn cron_template_renders_enqueue_now_form_at_end_of_row() {
        let template = CronTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/cron",
            rows: vec![CronRow::Worker {
                view: CronWorkerView {
                    name: "crate::workers::EmailCronWorker".to_string(),
                    short_name: "EmailCronWorker".to_string(),
                    schedule: "0 * * * * *".to_string(),
                    queue_key: "default".to_string(),
                    next_run_micros: None,
                },
                depth: 0,
            }],
            total: 1,
            enqueued: false,
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("<th class=\"pb-3 text-right\">Action</th>"));
        assert!(rendered.contains("action=\"/admin/cron/enqueue\""));
        assert!(rendered.contains("name=\"name\" value=\"crate::workers::EmailCronWorker\""));
        assert!(rendered.contains("Enqueue now"));
    }

    #[test]
    fn cron_template_shows_notice_after_successful_enqueue() {
        let template = CronTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/cron",
            rows: Vec::new(),
            total: 0,
            enqueued: true,
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("Cron job enqueued."));
        assert!(rendered.contains("data-auto-dismiss-notice"));
    }
}

#[cfg(test)]
mod on_demand_tests {
    use super::{OnDemandJobView, OnDemandQueueView, OnDemandRow, OnDemandTemplate};
    use askama::Template;

    #[test]
    fn on_demand_template_renders_empty_state() {
        let template = OnDemandTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/on-demand",
            rows: Vec::new(),
            queues: vec![OnDemandQueueView {
                key: "default".to_string(),
                selected: true,
            }],
            total: 0,
            scheduled: false,
            invalid_json: false,
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("No on-demand jobs registered"));
    }

    #[test]
    fn on_demand_template_renders_no_static_queues_state() {
        let template = OnDemandTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/on-demand",
            rows: vec![OnDemandRow::Job {
                view: OnDemandJobView {
                    name: "crate::EmailWorker".to_string(),
                    short_name: "EmailWorker".to_string(),
                    args_template_json: "{}".to_string(),
                },
                depth: 0,
            }],
            queues: Vec::new(),
            total: 1,
            scheduled: false,
            invalid_json: false,
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("No static queues registered"));
    }

    #[test]
    fn on_demand_template_prefills_enqueue_form() {
        let template = OnDemandTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/on-demand",
            rows: vec![
                OnDemandRow::Group {
                    name: "crate".to_string(),
                    depth: 0,
                },
                OnDemandRow::Job {
                    view: OnDemandJobView {
                        name: "crate::email::EmailWorker".to_string(),
                        short_name: "email::EmailWorker".to_string(),
                        args_template_json: "{\n  \"payload\": \"\"\n}".to_string(),
                    },
                    depth: 1,
                },
            ],
            queues: vec![OnDemandQueueView {
                key: "default".to_string(),
                selected: true,
            }],
            total: 1,
            scheduled: false,
            invalid_json: false,
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("action=\"/admin/on-demand/enqueue\""));
        assert!(rendered.contains("<option value=\"default\" selected>default</option>"));
        assert!(rendered.contains("crate"));
        assert!(rendered.contains("email::EmailWorker"));
        assert!(rendered.contains("payload"));
    }

    #[test]
    fn on_demand_template_shows_notice_after_successful_enqueue() {
        let template = OnDemandTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/on-demand",
            rows: Vec::new(),
            queues: Vec::new(),
            total: 0,
            scheduled: true,
            invalid_json: false,
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("Job scheduled."));
        assert!(rendered.contains("data-auto-dismiss-notice"));
    }

    #[test]
    fn on_demand_template_shows_notice_after_invalid_json() {
        let template = OnDemandTemplate {
            base_path: "/admin".to_string(),
            active_tab: "/on-demand",
            rows: Vec::new(),
            queues: Vec::new(),
            total: 0,
            scheduled: false,
            invalid_json: true,
        };

        let rendered = template.render().unwrap();

        assert!(rendered.contains("Invalid JSON."));
        assert!(rendered.contains("data-auto-dismiss-notice"));
    }
}
