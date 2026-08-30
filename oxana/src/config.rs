use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::failure::{ExecutionSentryHub, FailureReporterFn, WorkerFailureReport, report_failure};
use crate::queue::QueueConfig;
use crate::storage_types::{
    Catalog, CronWorkerInfo, OnDemandJobInfo, QueueInfo, QueueThrottleInfo, WorkerInfo,
};
use crate::worker::{FromContext, Job, Worker};
use crate::worker_registry::{self, WorkerConfig, WorkerConfigKind, WorkerRegistry};

pub(crate) const DEFAULT_DEAD_PROCESS_THRESHOLD: Duration = Duration::from_secs(5);

pub(crate) type RetryDelayOverrideFn =
    dyn Fn(&(dyn std::error::Error + Send + Sync + 'static), u32, u64) -> Option<u64> + Send + Sync;
pub(crate) type ErrorFormatterFn =
    dyn Fn(&(dyn std::error::Error + Send + Sync + 'static)) -> String + Send + Sync;

type ShutdownSignal =
    Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send + Sync + 'static>>;

#[derive(Clone)]
pub(crate) struct RuntimeSettings {
    pub(crate) exit_when_processed: Option<u64>,
    shutdown_signal: Arc<Mutex<Option<ShutdownSignal>>>,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) retry_delay_override: Option<Arc<RetryDelayOverrideFn>>,
    pub(crate) error_formatter: Option<Arc<ErrorFormatterFn>>,
    pub(crate) failure_reporter: Option<Arc<FailureReporterFn>>,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) dead_process_threshold: Duration,
    pub(crate) resurrect_scan_interval: Duration,
    pub(crate) redis_failure_tolerance: u32,
    pub(crate) retry_poll_interval: Duration,
    pub(crate) schedule_poll_interval: Duration,
    pub(crate) cron_initial_offset: Duration,
    pub(crate) cron_lookahead: Duration,
    pub(crate) cron_tick_interval: Duration,
    pub(crate) dequeue_timeout: Duration,
    pub(crate) dispatcher_idle_sleep: Duration,
    pub(crate) throttled_queue_fallback_wait: Duration,
    pub(crate) queue_allowlist: HashSet<QueueConfig>,
}

impl RuntimeSettings {
    pub(crate) fn new() -> Self {
        Self {
            exit_when_processed: None,
            shutdown_signal: Arc::new(Mutex::new(Some(Box::pin(default_shutdown_signal())))),
            shutdown_timeout: Duration::from_secs(180),
            retry_delay_override: None,
            error_formatter: None,
            failure_reporter: None,
            heartbeat_interval: Duration::from_millis(500),
            dead_process_threshold: DEFAULT_DEAD_PROCESS_THRESHOLD,
            resurrect_scan_interval: Duration::from_secs(2),
            redis_failure_tolerance: 30,
            retry_poll_interval: Duration::from_millis(300),
            schedule_poll_interval: Duration::from_millis(300),
            cron_initial_offset: Duration::from_secs(3),
            cron_lookahead: Duration::from_secs(30),
            cron_tick_interval: Duration::from_secs(1),
            dequeue_timeout: Duration::from_secs(10),
            dispatcher_idle_sleep: Duration::from_secs(1),
            throttled_queue_fallback_wait: Duration::from_millis(100),
            queue_allowlist: HashSet::new(),
        }
    }

    pub(crate) fn runs_queue(&self, queue: &QueueConfig) -> bool {
        self.queue_allowlist.is_empty() || self.queue_allowlist.contains(queue)
    }

    pub(crate) fn runs_static_queue(&self, queue_key: &str) -> bool {
        self.queue_allowlist.is_empty()
            || self
                .queue_allowlist
                .iter()
                .any(|queue| queue.static_key().as_deref() == Some(queue_key))
    }

    pub(crate) fn replace_shutdown_signal(
        &mut self,
        fut: impl Future<Output = Result<(), std::io::Error>> + Send + Sync + 'static,
    ) {
        *self
            .shutdown_signal
            .lock()
            .expect("shutdown signal mutex poisoned") = Some(Box::pin(fut));
    }

    pub(crate) fn consume_shutdown_signal(&self) -> ShutdownSignal {
        self.shutdown_signal
            .lock()
            .expect("shutdown signal mutex poisoned")
            .take()
            .unwrap_or_else(no_signal)
    }

    pub(crate) fn format_error(
        &self,
        error: &(dyn std::error::Error + Send + Sync + 'static),
    ) -> String {
        self.error_formatter
            .as_ref()
            .map_or_else(|| format!("{error:?}"), |formatter| formatter(error))
    }

    pub(crate) fn report_failure(
        &self,
        report: WorkerFailureReport<'_>,
        execution_hub: &ExecutionSentryHub,
    ) {
        report_failure(self.failure_reporter.as_ref(), report, execution_hub);
    }
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub(crate) struct Config<DT> {
    pub(crate) registry: WorkerRegistry<DT>,
    pub(crate) queues: HashSet<QueueConfig>,
}

impl<DT> Config<DT> {
    /// Creates an empty registration container for workers and queues.
    pub fn new() -> Self {
        Self {
            registry: WorkerRegistry::new(),
            queues: HashSet::new(),
        }
    }

    /// Registers a queue from a [`QueueConfig`].
    ///
    /// Registering the same queue key again replaces the previous
    /// configuration (last write wins).
    pub fn register_queue_with(&mut self, config: QueueConfig) {
        if self.queues.replace(config).is_some() {
            tracing::debug!("Replacing existing queue registration");
        }
    }

    /// Registers a queue only if its key is not registered yet, keeping any
    /// explicit configuration. Used for implicit registrations (e.g. cron
    /// queues) so they never override user-provided queue configs.
    fn register_default_queue(&mut self, config: QueueConfig) {
        if !self.queues.contains(&config) {
            self.queues.insert(config);
        }
    }

    /// Registers a worker and its associated job type.
    pub fn register_worker<W, A>(mut self) -> Self
    where
        W: Worker<A> + FromContext<DT> + 'static,
        A: Job + serde::de::DeserializeOwned + Send + 'static,
        DT: Clone + Send + Sync + 'static,
    {
        let name = A::name().to_string();
        let factory = worker_registry::job_factory::<W, A, DT>;
        let batch_factory = worker_registry::job_batch_factory::<W, A, DT>;
        let kind = <W as Worker<A>>::to_config();
        let batch_config = W::batch_config();
        let on_demand = A::on_demand_args_template().map(|args_template| {
            worker_registry::OnDemandJobRegistration {
                args_template,
                enqueue_factory: worker_registry::job_envelope_factory::<A>,
            }
        });

        if let WorkerConfigKind::Cron { .. } = &kind {
            assert!(
                serde_json::from_value::<A>(serde_json::json!({})).is_ok(),
                "{name}: Cron job args must be deserializable from empty JSON `{{}}`. \
                 Use `#[serde(default)]` on all fields or define the args struct with no fields."
            );

            if let Some(queue_config) = <W as Worker<A>>::cron_queue_config() {
                self.register_default_queue(queue_config);
            }
        }

        self.registry.register_worker_with(
            WorkerConfig {
                name,
                legacy_names: vec![std::any::type_name::<W>().to_owned()],
                factory,
                batch_factory,
                batch_config,
                on_demand,
                kind,
            },
            Some(worker_registry::cron_envelope_factory::<A>),
        );
        self
    }

    /// Registers a worker from a [`WorkerConfig`].
    pub fn register_worker_with(&mut self, config: WorkerConfig<DT>) {
        self.registry.register_worker_with(config, None);
    }

    /// Returns a catalog of all registered workers.
    #[cfg(test)]
    pub fn catalog(&self) -> Catalog {
        self.catalog_with_queues(&HashSet::new())
    }

    pub(crate) fn catalog_with_queues(&self, extra_queues: &HashSet<QueueConfig>) -> Catalog {
        let mut cron_workers: Vec<CronWorkerInfo> = self
            .registry
            .schedules
            .iter()
            .map(|(name, cron_job)| CronWorkerInfo {
                name: name.clone(),
                schedule: cron_job.schedule.clone(),
                queue_key: cron_job.queue_key.clone(),
                resurrect: cron_job.resurrect,
            })
            .collect();
        cron_workers.sort_by(|a, b| a.name.cmp(&b.name));

        let mut workers: Vec<WorkerInfo> = self
            .registry
            .worker_names()
            .into_iter()
            .filter(|name| !self.registry.schedules.contains_key(*name))
            .map(|name| WorkerInfo {
                name: name.to_string(),
            })
            .collect();
        workers.sort_by(|a, b| a.name.cmp(&b.name));

        let mut queues: Vec<QueueInfo> = self
            .queues
            .union(extra_queues)
            .map(|q| {
                let (key, dynamic) = match &q.kind {
                    crate::queue::QueueKind::Static { key } => (key.clone(), false),
                    crate::queue::QueueKind::Dynamic { prefix, .. } => (prefix.clone(), true),
                };
                QueueInfo {
                    key,
                    dynamic,
                    concurrency: q.concurrency.default_concurrency(),
                    dynamic_concurrency: q.concurrency.is_dynamic(),
                    throttle: q.throttle.as_ref().map(|t| QueueThrottleInfo {
                        window_ms: t.window_ms,
                        limit: t.limit,
                    }),
                }
            })
            .collect();
        queues.sort_by(|a, b| a.key.cmp(&b.key));

        let mut on_demand_jobs: Vec<OnDemandJobInfo> = self
            .registry
            .on_demand_jobs
            .iter()
            .map(|(name, registration)| OnDemandJobInfo {
                name: name.clone(),
                args_template: registration.args_template.clone(),
                enqueue_factory: registration.enqueue_factory,
            })
            .collect();
        on_demand_jobs.sort_by(|a, b| a.name.cmp(&b.name));

        Catalog {
            workers,
            cron_workers,
            queues,
            on_demand_jobs,
        }
    }
}

impl<DT> Default for Config<DT> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_family = "unix")]
async fn default_shutdown_signal() -> Result<(), std::io::Error> {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    tokio::select! {
        _ = ctrl_c => Ok(()),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(target_os = "windows")]
async fn default_shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

fn no_signal() -> Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send + Sync + 'static>>
{
    // A consumed shutdown slot must never fire; completing immediately would
    // trigger an instant graceful shutdown instead.
    Box::pin(std::future::pending())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as oxana;
    use serde::{Deserialize, Serialize};
    use std::io::Error as WorkerError;

    macro_rules! impl_test_worker {
        ($worker:ty, $job:ty) => {
            impl oxana::FromContext<()> for $worker {
                fn from_context(_ctx: &()) -> Self {
                    Self
                }
            }

            #[async_trait::async_trait]
            impl oxana::Worker<$job> for $worker {
                type Error = WorkerError;

                async fn run_batch(
                    &self,
                    _jobs: Vec<oxana::BatchItem<$job>>,
                ) -> Result<(), Self::Error> {
                    Ok(())
                }
            }
        };
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct PlainJob {
        value: String,
    }

    struct PlainWorker;

    impl oxana::Job for PlainJob {}

    impl_test_worker!(PlainWorker, PlainJob);

    #[derive(Debug, Serialize, Deserialize)]
    struct ZetaJob {
        value: String,
    }

    struct ZetaWorker;

    impl oxana::Job for ZetaJob {
        fn on_demand_args_template() -> Option<serde_json::Value> {
            Some(serde_json::json!({
                "value": "",
            }))
        }
    }

    impl_test_worker!(ZetaWorker, ZetaJob);

    #[derive(Debug, Serialize, Deserialize)]
    struct AlphaJob {
        id: u64,
        cost: u64,
    }

    impl AlphaJob {
        fn throttle_cost(&self) -> Option<u64> {
            Some(self.cost)
        }
    }

    struct AlphaWorker;

    impl oxana::Job for AlphaJob {
        fn unique_id(&self) -> Option<String> {
            Some(format!("alpha_{}", self.id))
        }

        fn on_conflict(&self) -> oxana::JobConflictStrategy {
            oxana::JobConflictStrategy::Replace
        }

        fn throttle_cost(&self) -> Option<u64> {
            Self::throttle_cost(self)
        }

        fn on_demand_args_template() -> Option<serde_json::Value> {
            Some(serde_json::json!({
                "id": 0,
                "cost": 0,
            }))
        }
    }

    impl_test_worker!(AlphaWorker, AlphaJob);

    #[test]
    #[should_panic(expected = "manual cron worker registration is not supported")]
    fn manual_cron_worker_config_requires_typed_factory() {
        let mut config = Config::<()>::new();
        config.register_worker_with(WorkerConfig {
            name: PlainJob::name().to_string(),
            legacy_names: Vec::new(),
            factory: worker_registry::job_factory::<PlainWorker, PlainJob, ()>,
            batch_factory: worker_registry::job_batch_factory::<PlainWorker, PlainJob, ()>,
            batch_config: None,
            on_demand: None,
            kind: WorkerConfigKind::Cron {
                schedule: "*/5 * * * * *".to_string(),
                queue_key: "default".to_string(),
                resurrect: true,
            },
        });
    }

    #[test]
    fn worker_registration_accepts_canonical_and_legacy_worker_names() {
        let config = Config::<()>::new().register_worker::<PlainWorker, PlainJob>();
        let job = serde_json::json!({
            "value": "hello",
        });

        let canonical = config
            .registry
            .build(PlainJob::name(), job.clone(), &())
            .expect("canonical job name should build");
        assert_eq!(canonical.len(), 1);

        let legacy = config
            .registry
            .build(std::any::type_name::<PlainWorker>(), job.clone(), &())
            .expect("legacy worker name should build");
        assert_eq!(legacy.len(), 1);

        let batch = config
            .registry
            .build_batch(std::any::type_name::<PlainWorker>(), vec![job], &())
            .expect("legacy worker name should build a batch");
        assert_eq!(batch.job.expect("batch should contain one job").len(), 1);

        let worker_names = config
            .catalog()
            .workers
            .into_iter()
            .map(|worker| worker.name)
            .collect::<Vec<_>>();
        assert_eq!(worker_names, vec![PlainJob::name().to_string()]);
    }

    #[test]
    fn cron_worker_uses_canonical_catalog_and_legacy_consumer_alias() {
        #[derive(Debug, Serialize, Deserialize)]
        struct CronCompatJob {}

        impl oxana::Job for CronCompatJob {}

        #[derive(Serialize)]
        struct CronCompatQueue;

        impl oxana::Queue for CronCompatQueue {
            fn to_config() -> oxana::QueueConfig {
                oxana::QueueConfig::as_static("cron_compat")
            }
        }

        struct CronCompatWorker;

        impl oxana::FromContext<()> for CronCompatWorker {
            fn from_context(_ctx: &()) -> Self {
                Self
            }
        }

        #[async_trait::async_trait]
        impl oxana::Worker<CronCompatJob> for CronCompatWorker {
            type Error = WorkerError;

            async fn run_batch(
                &self,
                _jobs: Vec<oxana::BatchItem<CronCompatJob>>,
            ) -> Result<(), Self::Error> {
                Ok(())
            }

            fn cron_schedule() -> Option<String> {
                Some("*/5 * * * * *".to_string())
            }

            fn cron_queue_config() -> Option<oxana::QueueConfig> {
                Some(<CronCompatQueue as oxana::Queue>::to_config())
            }
        }

        let config = Config::<()>::new().register_worker::<CronCompatWorker, CronCompatJob>();

        assert!(
            config
                .registry
                .schedules
                .contains_key(CronCompatJob::name())
        );
        assert!(
            !config
                .registry
                .schedules
                .contains_key(std::any::type_name::<CronCompatWorker>())
        );

        let catalog = config.catalog();
        assert_eq!(catalog.cron_workers.len(), 1);
        assert_eq!(
            catalog
                .cron_workers
                .first()
                .expect("one cron worker should be cataloged")
                .name
                .as_str(),
            CronCompatJob::name()
        );

        let legacy = config
            .registry
            .build(
                std::any::type_name::<CronCompatWorker>(),
                serde_json::json!({}),
                &(),
            )
            .expect("legacy cron worker name should build");
        assert_eq!(legacy.len(), 1);
    }

    #[test]
    fn cron_envelopes_compute_unique_id_from_each_job_instance() {
        use std::sync::atomic::{AtomicU64, Ordering};

        static WORKER_BUILDS: AtomicU64 = AtomicU64::new(0);

        fn next_id() -> u64 {
            static NEXT_ID: AtomicU64 = AtomicU64::new(1);
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct DynamicCronJob {
            #[serde(default = "next_id")]
            id: u64,
        }

        impl oxana::Job for DynamicCronJob {
            fn unique_id(&self) -> Option<String> {
                Some(self.id.to_string())
            }
        }

        #[derive(Serialize)]
        struct DynamicCronQueue;

        impl oxana::Queue for DynamicCronQueue {
            fn to_config() -> oxana::QueueConfig {
                oxana::QueueConfig::as_static("dynamic_cron")
            }
        }

        struct DynamicCronWorker;

        impl oxana::FromContext<()> for DynamicCronWorker {
            fn from_context(_ctx: &()) -> Self {
                WORKER_BUILDS.fetch_add(1, Ordering::Relaxed);
                Self
            }
        }

        #[async_trait::async_trait]
        impl oxana::Worker<DynamicCronJob> for DynamicCronWorker {
            type Error = WorkerError;

            async fn run_batch(
                &self,
                _jobs: Vec<oxana::BatchItem<DynamicCronJob>>,
            ) -> Result<(), Self::Error> {
                Ok(())
            }

            fn cron_schedule() -> Option<String> {
                Some("*/5 * * * * *".to_string())
            }

            fn cron_queue_config() -> Option<oxana::QueueConfig> {
                Some(<DynamicCronQueue as oxana::Queue>::to_config())
            }
        }

        let config = Config::<()>::new().register_worker::<DynamicCronWorker, DynamicCronJob>();
        let first = config
            .registry
            .cron_envelope(DynamicCronJob::name(), 123)
            .expect("first cron envelope should build")
            .0;
        let second = config
            .registry
            .cron_envelope(DynamicCronJob::name(), 456)
            .expect("second cron envelope should build")
            .0;

        let first_arg_id = first
            .job
            .args
            .get("id")
            .and_then(serde_json::Value::as_u64)
            .expect("first cron args should contain an id");
        let second_arg_id = second
            .job
            .args
            .get("id")
            .and_then(serde_json::Value::as_u64)
            .expect("second cron args should contain an id");
        assert_ne!(first_arg_id, second_arg_id);
        assert_eq!(
            first.id,
            format!("{}/{first_arg_id}", DynamicCronJob::name())
        );
        assert_eq!(
            second.id,
            format!("{}/{second_arg_id}", DynamicCronJob::name())
        );
        assert_eq!(
            WORKER_BUILDS.load(Ordering::Relaxed),
            0,
            "building cron envelopes must not construct workers"
        );
    }

    #[test]
    fn catalog_lists_only_on_demand_jobs_sorted() {
        let config = Config::<()>::new()
            .register_worker::<PlainWorker, PlainJob>()
            .register_worker::<ZetaWorker, ZetaJob>()
            .register_worker::<AlphaWorker, AlphaJob>();

        let names = config
            .catalog()
            .on_demand_jobs
            .into_iter()
            .map(|job| job.name)
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                std::any::type_name::<AlphaJob>().to_string(),
                std::any::type_name::<ZetaJob>().to_string(),
            ]
        );
    }

    #[test]
    fn on_demand_factory_preserves_job_hooks() {
        let catalog = Config::<()>::new()
            .register_worker::<AlphaWorker, AlphaJob>()
            .catalog();
        let job = catalog
            .on_demand_jobs
            .iter()
            .find(|job| job.name == std::any::type_name::<AlphaJob>())
            .expect("alpha job should be registered as on-demand");

        let envelope = job
            .enqueue_envelope(
                "manual".to_string(),
                serde_json::json!({
                    "id": 7,
                    "cost": 3,
                }),
            )
            .expect("on-demand factory should build typed envelope");

        assert_eq!(envelope.queue, "manual");
        assert_eq!(envelope.id, format!("{}/alpha_7", AlphaJob::name()));
        assert!(envelope.meta.unique);
        assert_eq!(
            envelope.meta.on_conflict,
            Some(oxana::JobConflictStrategy::Replace)
        );
        assert_eq!(envelope.meta.throttle_cost, Some(3));
    }
}
