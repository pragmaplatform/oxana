//! Prometheus metrics integration for Oxana.
//!
//! This module provides Prometheus metrics based on the [`Stats`] from the storage.
//!
//! # Example
//!
//! ```rust,ignore
//! use oxana::Storage;
//!
//! async fn example(storage: &Storage) -> Result<(), oxana::OxanaError> {
//!     let metrics = storage.metrics().await?;
//!
//!     // Encode metrics to text format
//!     let output = metrics.encode_to_string()?;
//!     println!("{}", output);
//!     Ok(())
//! }
//! ```

use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{family::Family, gauge::Gauge},
    registry::{Metric, Registry},
};
use std::sync::atomic::{AtomicI64, AtomicU64};

use crate::stats::Stats;

fn register_i64(registry: &mut Registry, name: &str, help: &str, value: i64) {
    let gauge = Gauge::<i64, AtomicI64>::default();
    gauge.set(value);
    registry.register(name, help, gauge);
}

fn register_f64(registry: &mut Registry, name: &str, help: &str, value: f64) {
    let gauge = Gauge::<f64, AtomicU64>::default();
    gauge.set(value);
    registry.register(name, help, gauge);
}

fn register_family<L, M>(registry: &mut Registry, name: &str, help: &str, family: Family<L, M>)
where
    Family<L, M>: Metric,
{
    registry.register(name, help, family);
}

macro_rules! register_families {
    (
        $registry:expr, $labels:ty, $values:expr,
        |$item:pat_param| $labels_value:expr;
        $($family:ident: $gauge:ty = $name:literal => ($help:literal, $value:expr);)+)
    => {
        $(let $family = Family::<$labels, $gauge>::default();)+
        for $item in $values {
            let labels = $labels_value;
            $($family.get_or_create(&labels).set($value);)+
        }
        $(register_family($registry, $name, $help, $family);)+
    };
}

/// Label set for queue-level metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct QueueLabels {
    /// The queue key/name.
    pub queue: String,
}

/// Label set for dynamic sub-queue metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DynamicQueueLabels {
    /// The parent queue key/name.
    pub queue: String,
    /// The dynamic queue suffix.
    pub suffix: String,
}

/// Label set for process-level metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ProcessLabels {
    /// The hostname of the process.
    pub hostname: String,
    /// The process ID.
    pub pid: String,
}

/// Prometheus metrics for Oxana job queue.
///
/// This struct holds all the Prometheus metrics and the registry.
/// Use [`PrometheusMetrics::from_stats()`] to create an instance from storage stats.
pub struct PrometheusMetrics {
    registry: Registry,
}

impl PrometheusMetrics {
    /// Creates a new [`PrometheusMetrics`] instance from the provided stats.
    #[must_use]
    pub fn from_stats(stats: &Stats) -> Self {
        Self::from_stats_with_prefix(stats, "oxana")
    }

    /// Creates a new [`PrometheusMetrics`] instance from the provided stats with a custom prefix.
    #[must_use]
    pub fn from_stats_with_prefix(stats: &Stats, prefix: &str) -> Self {
        let mut registry = Registry::with_prefix(prefix);

        #[rustfmt::skip]
        let global_metrics = [
            ("jobs_total", "Total number of jobs (enqueued + scheduled)", stats.global.jobs as i64),
            ("enqueued_total", "Total number of jobs currently enqueued", stats.global.enqueued as i64),
            ("processed_total", "Total number of jobs processed", stats.global.processed),
            ("failed_total", "Total number of jobs failed", stats.global.failed),
            ("dead_total", "Total number of dead jobs", stats.global.dead as i64),
            ("scheduled_total", "Total number of scheduled jobs", stats.global.scheduled as i64),
            ("retries_total", "Total number of jobs in retry queue", stats.global.retries as i64),
        ];
        for (name, help, value) in global_metrics {
            register_i64(&mut registry, name, help, value);
        }
        register_f64(
            &mut registry,
            "latency_max_seconds",
            "Maximum latency across all queues in seconds",
            stats.global.latency_s_max,
        );

        register_families!(
            &mut registry, QueueLabels, &stats.queues,
            |queue| QueueLabels { queue: queue.key.clone() };
            queue_enqueued: Gauge<i64, AtomicI64> = "queue_enqueued" => ("Number of jobs enqueued per queue", queue.enqueued as i64);
            queue_processed: Gauge<i64, AtomicI64> = "queue_processed_total" => ("Total number of jobs processed per queue", queue.processed);
            queue_succeeded: Gauge<i64, AtomicI64> = "queue_succeeded_total" => ("Total number of jobs succeeded per queue", queue.succeeded);
            queue_panicked: Gauge<i64, AtomicI64> = "queue_panicked_total" => ("Total number of jobs panicked per queue", queue.panicked);
            queue_failed: Gauge<i64, AtomicI64> = "queue_failed_total" => ("Total number of jobs failed per queue", queue.failed);
            queue_latency: Gauge<f64, AtomicU64> = "queue_latency_seconds" => ("Current latency per queue in seconds", queue.latency_s);
        );

        register_families!(
            &mut registry, DynamicQueueLabels,
            stats.queues.iter().flat_map(|queue| queue.queues.iter().map(move |dynamic_queue| (queue, dynamic_queue))),
            |(queue, dynamic_queue)| DynamicQueueLabels {
                queue: queue.key.clone(),
                suffix: dynamic_queue.suffix.clone(),
            };
            dynamic_queue_enqueued: Gauge<i64, AtomicI64> = "dynamic_queue_enqueued" => ("Number of jobs enqueued per dynamic sub-queue", dynamic_queue.enqueued as i64);
            dynamic_queue_processed: Gauge<i64, AtomicI64> = "dynamic_queue_processed_total" => ("Total number of jobs processed per dynamic sub-queue", dynamic_queue.processed);
            dynamic_queue_succeeded: Gauge<i64, AtomicI64> = "dynamic_queue_succeeded_total" => ("Total number of jobs succeeded per dynamic sub-queue", dynamic_queue.succeeded);
            dynamic_queue_panicked: Gauge<i64, AtomicI64> = "dynamic_queue_panicked_total" => ("Total number of jobs panicked per dynamic sub-queue", dynamic_queue.panicked);
            dynamic_queue_failed: Gauge<i64, AtomicI64> = "dynamic_queue_failed_total" => ("Total number of jobs failed per dynamic sub-queue", dynamic_queue.failed);
            dynamic_queue_latency: Gauge<f64, AtomicU64> = "dynamic_queue_latency_seconds" => ("Current latency per dynamic sub-queue in seconds", dynamic_queue.latency_s);
        );

        register_families!(
            &mut registry, ProcessLabels, &stats.processes,
            |process| ProcessLabels {
                hostname: process.hostname.clone(),
                pid: process.pid.to_string(),
            };
            process_heartbeat: Gauge<i64, AtomicI64> = "process_heartbeat_timestamp_seconds" => ("Last heartbeat timestamp per process", process.heartbeat_at);
            process_started: Gauge<i64, AtomicI64> = "process_started_timestamp_seconds" => ("Start timestamp per process", process.started_at);
        );

        register_i64(
            &mut registry,
            "processes_count",
            "Number of active Oxana processes",
            stats.processes.len() as i64,
        );

        Self { registry }
    }

    /// Returns a reference to the underlying Prometheus registry.
    ///
    /// This can be used for custom encoding or to add additional metrics.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Encodes the metrics to the `OpenMetrics` text format.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails.
    pub fn encode(&self, writer: &mut String) -> Result<(), std::fmt::Error> {
        encode(writer, &self.registry)
    }

    /// Encodes the metrics and returns them as a string.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails.
    pub fn encode_to_string(&self) -> Result<String, std::fmt::Error> {
        let mut buffer = String::new();
        self.encode(&mut buffer)?;
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{
        DynamicQueueStats, Process, QueueRateStats, QueueStats, Stats, StatsGlobal,
    };

    fn create_test_stats() -> Stats {
        Stats {
            global: StatsGlobal {
                jobs: 100,
                enqueued: 50,
                processed: 200,
                failed: 10,
                dead: 5,
                scheduled: 30,
                retries: 10,
                latency_s_max: 2.5,
            },
            processes: vec![Process {
                hostname: "test-host".to_string(),
                pid: 12345,
                heartbeat_at: 1700000000,
                started_at: 1699999000,
            }],
            processing: vec![],
            queues: vec![
                QueueStats {
                    key: "default".to_string(),
                    enqueued: 30,
                    processed: 150,
                    succeeded: 140,
                    panicked: 2,
                    failed: 8,
                    latency_s: 1.5,
                    rate: QueueRateStats::default(),
                    queues: vec![],
                },
                QueueStats {
                    key: "priority".to_string(),
                    enqueued: 20,
                    processed: 50,
                    succeeded: 48,
                    panicked: 0,
                    failed: 2,
                    latency_s: 0.5,
                    rate: QueueRateStats::default(),
                    queues: vec![DynamicQueueStats {
                        suffix: "user_123".to_string(),
                        enqueued: 5,
                        processed: 10,
                        succeeded: 9,
                        panicked: 0,
                        failed: 1,
                        latency_s: 0.3,
                        rate: QueueRateStats::default(),
                    }],
                },
            ],
        }
    }

    #[test]
    fn test_prometheus_metrics_from_stats() {
        let stats = create_test_stats();
        let metrics = PrometheusMetrics::from_stats(&stats);

        // Verify metrics can be encoded
        let output = metrics.encode_to_string().expect("encoding should succeed");
        assert!(!output.is_empty());
    }

    #[test]
    fn test_prometheus_metrics_with_prefix() {
        let stats = create_test_stats();
        let metrics = PrometheusMetrics::from_stats_with_prefix(&stats, "my_app");
        let output = metrics.encode_to_string().expect("encoding should succeed");
        assert!(output.contains("my_app_"));
    }

    #[test]
    fn test_prometheus_metrics_encode() {
        let stats = create_test_stats();
        let metrics = PrometheusMetrics::from_stats(&stats);

        let output = metrics.encode_to_string().expect("encoding should succeed");

        // Check that metrics are present in the output
        assert!(output.contains("oxana_jobs_total"));
        assert!(output.contains("oxana_enqueued_total"));
        assert!(output.contains("oxana_processed_total"));
        assert!(output.contains("oxana_failed_total"));
        assert!(output.contains("oxana_dead_total"));
        assert!(output.contains("oxana_scheduled_total"));
        assert!(output.contains("oxana_retries_total"));
        assert!(output.contains("oxana_queue_enqueued"));
        assert!(output.contains("oxana_processes_count"));

        // Check that queue labels are present
        assert!(output.contains("queue=\"default\""));
        assert!(output.contains("queue=\"priority\""));

        // Check that dynamic queue labels are present
        assert!(output.contains("suffix=\"user_123\""));

        // Check that process labels are present
        assert!(output.contains("hostname=\"test-host\""));
        assert!(output.contains("pid=\"12345\""));
    }
}
