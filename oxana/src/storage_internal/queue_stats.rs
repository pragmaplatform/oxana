//! Pure aggregation of queue counters and cached lengths before live Redis reads.

use std::collections::HashMap;

use crate::{
    metrics::{QUEUE_RATE_WINDOW_MINUTES, QueueCounterTotals},
    result_collector::QueueResultStats,
    stats::{DynamicQueueStats, QueueRateStats, QueueStats},
};

pub(super) const QUEUE_LENGTH_SNAPSHOT_TTL_SECS: i64 = 120;

#[derive(Default)]
struct QueueLengthSnapshot {
    enqueued: Option<i64>,
    refreshed_at: Option<i64>,
}

impl QueueLengthSnapshot {
    fn fresh_enqueued(&self, now: i64) -> Option<usize> {
        if now.saturating_sub(self.refreshed_at?) > QUEUE_LENGTH_SNAPSHOT_TTL_SECS {
            return None;
        }

        usize::try_from(self.enqueued?).ok()
    }
}

pub(super) fn aggregate_queue_stats(
    queues: &[String],
    stats: HashMap<String, i64>,
    filter: bool,
    now: i64,
) -> (Vec<QueueStats>, HashMap<String, usize>) {
    let mut counters: HashMap<String, QueueResultStats> = queues
        .iter()
        .map(|queue| (queue.clone(), QueueResultStats::default()))
        .collect();
    let mut snapshots: HashMap<String, QueueLengthSnapshot> = HashMap::new();

    for (key, value) in stats {
        let Some((queue, stat)) = key.rsplit_once(':') else {
            continue;
        };
        if filter
            && !queues
                .iter()
                .any(|selected| queue_base(selected) == queue_base(queue))
        {
            continue;
        }

        match stat {
            "enqueued" => snapshots.entry(queue.to_string()).or_default().enqueued = Some(value),
            "enqueued_at" => {
                snapshots.entry(queue.to_string()).or_default().refreshed_at = Some(value);
            }
            _ => {
                // Even an unrecognized counter keeps a historical queue visible.
                let counts = counters.entry(queue.to_string()).or_default();
                match stat {
                    "processed" => counts.processed += value,
                    "succeeded" => counts.succeeded += value,
                    "panicked" => counts.panicked += value,
                    "failed" => counts.failed += value,
                    _ => {}
                }
            }
        }
    }

    let enqueued_snapshots: HashMap<String, usize> = snapshots
        .into_iter()
        .filter_map(|(queue, snapshot)| Some((queue, snapshot.fresh_enqueued(now)?)))
        .collect();
    for (queue, enqueued) in &enqueued_snapshots {
        // A zero snapshot can serve an existing queue, but cannot create one.
        if *enqueued > 0 {
            counters.entry(queue.clone()).or_default();
        }
    }

    let mut groups: HashMap<String, QueueStats> = HashMap::new();
    for (queue, counts) in counters {
        let base = queue_base(&queue);
        let parent = groups
            .entry(base.to_string())
            .or_insert_with(|| QueueStats {
                key: base.to_string(),
                enqueued: 0,
                processed: 0,
                succeeded: 0,
                panicked: 0,
                failed: 0,
                latency_s: 0.0,
                rate: QueueRateStats::default(),
                queues: vec![],
            });
        parent.processed += counts.processed;
        parent.succeeded += counts.succeeded;
        parent.panicked += counts.panicked;
        parent.failed += counts.failed;

        if let Some((_, suffix)) = queue.split_once('#') {
            parent.queues.push(DynamicQueueStats {
                suffix: suffix.to_string(),
                enqueued: 0,
                processed: counts.processed,
                succeeded: counts.succeeded,
                panicked: counts.panicked,
                failed: counts.failed,
                latency_s: 0.0,
                rate: QueueRateStats::default(),
            });
        }
    }

    (groups.into_values().collect(), enqueued_snapshots)
}

fn queue_base(queue: &str) -> &str {
    queue.split_once('#').map_or(queue, |(base, _)| base)
}

pub(super) fn queue_rate_stats(
    queue: &str,
    enqueued: usize,
    queue_length_hashes: &[HashMap<String, i64>],
    queue_counter_totals: &HashMap<String, QueueCounterTotals>,
) -> QueueRateStats {
    let window_start_enqueued = queue_length_hashes
        .first()
        .and_then(|hash| hash.get(queue))
        .and_then(|value| usize::try_from(*value).ok())
        .unwrap_or_default();
    let counters = queue_counter_totals.get(queue).copied().unwrap_or_default();

    QueueRateStats::calculate(
        QUEUE_RATE_WINDOW_MINUTES,
        enqueued,
        window_start_enqueued,
        counters.processed,
        counters.succeeded,
        counters.failed,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(entries: &[(&str, i64)]) -> HashMap<String, i64> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), *value))
            .collect()
    }

    fn queue<'a>(queues: &'a [QueueStats], key: &str) -> &'a QueueStats {
        queues
            .iter()
            .find(|queue| queue.key == key)
            .expect("queue should exist")
    }

    #[test]
    fn groups_live_and_historical_queues_without_duplicate_children() {
        let (queues, snapshots) = aggregate_queue_stats(
            &["active".into(), "batch#north".into(), "batch#north".into()],
            stats(&[
                ("static:processed", 10),
                ("batch:processed", 2),
                ("batch#north:processed", 4),
                ("batch#north:succeeded", 3),
                ("batch#north:panicked", 1),
                ("batch#north:failed", 1),
                ("batch#south:processed", 6),
                ("batch#south:succeeded", 6),
                ("tenant:jobs#a#b:processed", 7),
                ("legacy:unknown", 99),
                ("malformed", 99),
            ]),
            false,
            1_000,
        );

        assert!(snapshots.is_empty());
        assert_eq!(queues.len(), 5);
        assert_eq!(queue(&queues, "active").processed, 0);
        assert_eq!(queue(&queues, "static").processed, 10);
        assert_eq!(queue(&queues, "legacy").processed, 0);

        let batch = queue(&queues, "batch");
        assert_eq!(
            (
                batch.processed,
                batch.succeeded,
                batch.panicked,
                batch.failed
            ),
            (12, 9, 1, 1)
        );
        assert_eq!(batch.queues.len(), 2);
        let north = batch
            .queues
            .iter()
            .find(|queue| queue.suffix == "north")
            .unwrap();
        assert_eq!(
            (
                north.processed,
                north.succeeded,
                north.panicked,
                north.failed
            ),
            (4, 3, 1, 1)
        );

        let tenant = queue(&queues, "tenant:jobs");
        assert_eq!(tenant.processed, 7);
        assert_eq!(tenant.queues.len(), 1);
        assert_eq!(tenant.queues.first().unwrap().suffix, "a#b");
    }

    #[test]
    fn filtering_keeps_sibling_counters_and_snapshots() {
        let stored = stats(&[
            ("batch:processed", 9),
            ("batch#south:processed", 4),
            ("batch#east:enqueued", 2),
            ("batch#east:enqueued_at", 1_000),
            ("other:processed", 99),
            ("other:enqueued", 99),
            ("other:enqueued_at", 1_000),
        ]);
        let (queues, snapshots) =
            aggregate_queue_stats(&["batch#north".into()], stored.clone(), true, 1_000);

        assert_eq!(queues.len(), 1);
        let batch = queue(&queues, "batch");
        assert_eq!(batch.processed, 13);
        let mut suffixes: Vec<_> = batch
            .queues
            .iter()
            .map(|queue| queue.suffix.as_str())
            .collect();
        suffixes.sort_unstable();
        assert_eq!(suffixes, ["east", "north", "south"]);
        assert_eq!(snapshots, HashMap::from([("batch#east".into(), 2)]));

        let (queues, snapshots) = aggregate_queue_stats(&[], stored, true, 1_000);
        assert!(queues.is_empty());
        assert!(snapshots.is_empty());
    }

    #[test]
    fn only_positive_fresh_snapshots_create_queues() {
        let (queues, snapshots) = aggregate_queue_stats(
            &["live".into()],
            stats(&[
                ("live:enqueued", 0),
                ("live:enqueued_at", 1_000),
                ("historical:processed", 3),
                ("historical:enqueued", 0),
                ("historical:enqueued_at", 1_000),
                ("snapshot#busy:enqueued", 5),
                ("snapshot#busy:enqueued_at", 1_000),
                ("snapshot#empty:enqueued", 0),
                ("snapshot#empty:enqueued_at", 1_000),
                ("empty:enqueued", 0),
                ("empty:enqueued_at", 1_000),
                ("stale:enqueued", 7),
                ("stale:enqueued_at", 879),
                ("incomplete:enqueued", 8),
                ("negative:enqueued", -1),
                ("negative:enqueued_at", 1_000),
            ]),
            false,
            1_000,
        );

        assert_eq!(queues.len(), 3);
        assert_eq!(queue(&queues, "live").processed, 0);
        assert_eq!(queue(&queues, "historical").processed, 3);
        let snapshot = queue(&queues, "snapshot");
        assert_eq!(snapshot.queues.len(), 1);
        assert_eq!(snapshot.queues.first().unwrap().suffix, "busy");
        assert_eq!(
            snapshots,
            HashMap::from([
                ("live".into(), 0),
                ("historical".into(), 0),
                ("snapshot#busy".into(), 5),
                ("snapshot#empty".into(), 0),
                ("empty".into(), 0),
            ])
        );
    }

    #[test]
    fn snapshot_freshness_includes_the_ttl_boundary_and_future_timestamps() {
        for (enqueued, refreshed_at, expected) in [
            (Some(5), Some(880), Some(5)),
            (Some(5), Some(879), None),
            (Some(5), Some(1_001), Some(5)),
            (Some(0), Some(1_000), Some(0)),
            (Some(-1), Some(1_000), None),
            (None, Some(1_000), None),
            (Some(5), None, None),
        ] {
            let snapshot = QueueLengthSnapshot {
                enqueued,
                refreshed_at,
            };
            assert_eq!(snapshot.fresh_enqueued(1_000), expected);
        }
    }
}
