use std::any::TypeId;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::{Config, ErrorFormatterFn, RetryDelayOverrideFn, RuntimeSettings};
use crate::context::ContextValue;
use crate::drainer::{self, DrainStats};
use crate::error::OxanaError;
use crate::queue::{Queue, QueueConcurrency, require_non_zero_duration};
use crate::result_collector::Stats as RunStats;
use crate::storage::Storage;
use crate::storage_types::Catalog;
use crate::worker::{FromContext, Job, Worker};
use crate::worker_group::WorkerGroups;

#[cfg(feature = "registry")]
use crate::registry::RegisterComponents;

pub struct RuntimeBuilder<DT>
where
    DT: Clone + Send + Sync + 'static,
{
    storage: Storage,
    config: Config<DT>,
    settings: RuntimeSettings,
    ctx: ContextValue<DT>,
    only_groups: HashSet<TypeId>,
    excluded_groups: HashSet<TypeId>,
}

impl<DT> RuntimeBuilder<DT>
where
    DT: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(storage: Storage, ctx: DT) -> Self {
        Self {
            storage,
            config: Config::new(),
            settings: RuntimeSettings::new(),
            ctx: ContextValue::new(ctx),
            only_groups: HashSet::new(),
            excluded_groups: HashSet::new(),
        }
    }

    /// Returns the storage handle used by this runtime.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Registers all components from a derived component registry.
    #[cfg(feature = "registry")]
    pub fn register<R>(self) -> Self
    where
        R: RegisterComponents<Context = DT>,
    {
        R::register_components(self)
    }

    /// Registers a queue from a [`crate::QueueConfig`].
    pub fn queue_with(mut self, config: crate::QueueConfig) -> Self {
        self.config.register_queue_with(config);
        self
    }

    /// Registers a queue by type.
    pub fn queue<Q>(self) -> Self
    where
        Q: Queue,
    {
        self.queue_with(Q::to_config())
    }

    /// Registers a queue by type with a custom fixed concurrency limit.
    ///
    /// # Panics
    ///
    /// Panics if the concurrency is zero, since a zero-permit queue would
    /// silently never process jobs.
    pub fn queue_with_concurrency<Q>(self, concurrency: usize) -> Self
    where
        Q: Queue,
    {
        assert!(concurrency > 0, "concurrency must be greater than zero");
        let mut config = Q::to_config();
        config.concurrency = QueueConcurrency::Fixed(concurrency);
        self.queue_with(config)
    }

    /// Registers a worker for a job type.
    pub fn worker<W, A>(mut self) -> Self
    where
        W: Worker<A> + FromContext<DT> + 'static,
        A: Job + serde::de::DeserializeOwned + Send + 'static,
    {
        self.config = self.config.register_worker::<W, A>();
        self
    }

    /// Registers a worker from a [`crate::WorkerConfig`].
    pub fn worker_with(mut self, worker: crate::WorkerConfig<DT>) -> Self {
        self.config.register_worker_with(worker);
        self
    }

    /// Runs only workers belonging to at least one of the supplied groups.
    ///
    /// Calls accumulate, and ungrouped workers are excluded when this filter is present.
    pub fn only_groups<G>(mut self, groups: G) -> Self
    where
        G: WorkerGroups,
    {
        self.only_groups.extend(groups.group_ids());
        self
    }

    /// Excludes workers belonging to any of the supplied groups.
    ///
    /// Calls accumulate, and ungrouped workers remain enabled.
    pub fn exclude_groups<G>(mut self, groups: G) -> Self
    where
        G: WorkerGroups,
    {
        self.excluded_groups.extend(groups.group_ids());
        self
    }

    /// Stops processing after the given number of jobs have been processed. Useful for tests.
    pub fn exit_when_processed(mut self, processed: u64) -> Self {
        self.settings.exit_when_processed = Some(processed);
        self
    }

    /// Sets a future that triggers graceful shutdown when it completes.
    ///
    /// Defaults to listening for SIGTERM/SIGINT on Unix and Ctrl+C on Windows.
    pub fn shutdown_on(
        mut self,
        fut: impl Future<Output = Result<(), std::io::Error>> + Send + Sync + 'static,
    ) -> Self {
        self.settings.replace_shutdown_signal(fut);
        self
    }

    /// Sets Ctrl-C as the shutdown trigger.
    ///
    /// Note that this replaces the default signal listener, which also handles
    /// SIGTERM on Unix. Keep the default if you deploy behind an orchestrator
    /// that stops processes with SIGTERM.
    pub fn shutdown_on_ctrl_c(self) -> Self {
        self.shutdown_on(tokio::signal::ctrl_c())
    }

    /// Sets the maximum time to wait for in-flight workers during shutdown.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.settings.shutdown_timeout = timeout;
        self
    }

    /// Sets a global callback to override the retry delay when a job fails.
    ///
    /// The `'static` bound on the error trait object is what allows
    /// `error.downcast_ref::<ConcreteError>()` inside the callback.
    pub fn retry_delay_override(
        mut self,
        f: impl Fn(&(dyn std::error::Error + Send + Sync + 'static), u32, u64) -> Option<u64>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.settings.retry_delay_override = Some(Arc::new(f) as Arc<RetryDelayOverrideFn>);
        self
    }

    /// Sets a global formatter for worker errors before they are stored on retry or dead jobs.
    ///
    /// By default, errors use their [`std::fmt::Debug`] representation so error types that capture
    /// backtraces can include them. The `'static` bound on
    /// the error trait object allows `error.downcast_ref::<ConcreteError>()` inside the formatter.
    pub fn error_formatter(
        mut self,
        f: impl Fn(&(dyn std::error::Error + Send + Sync + 'static)) -> String + Send + Sync + 'static,
    ) -> Self {
        self.settings.error_formatter = Some(Arc::new(f) as Arc<ErrorFormatterFn>);
        self
    }

    /// Sets how often this process records a liveness heartbeat. Defaults to 500ms.
    ///
    /// # Panics
    ///
    /// Panics if the interval is zero.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.settings.heartbeat_interval =
            require_non_zero_duration("heartbeat_interval", interval);
        self
    }

    /// Sets how long a process can miss heartbeats before it is considered dead
    /// and its in-flight jobs become eligible for resurrection. Defaults to 5s.
    ///
    /// Must be comfortably larger than [`Self::heartbeat_interval`], otherwise
    /// live processes are treated as dead and their in-flight jobs are
    /// re-enqueued while still running.
    ///
    /// Note that this also updates the monitoring settings of the underlying
    /// [`Storage`] handle (and all of its clones), so `Storage::stats()` and
    /// `Storage::processes()` use the same liveness window.
    ///
    /// # Panics
    ///
    /// Panics if the threshold is zero.
    pub fn dead_process_threshold(mut self, threshold: Duration) -> Self {
        let threshold = require_non_zero_duration("dead_process_threshold", threshold);
        self.settings.dead_process_threshold = threshold;
        self.storage.set_dead_process_threshold(threshold);
        self
    }

    /// Sets how often to scan for dead processes and resurrect their jobs.
    /// Defaults to 2s.
    ///
    /// # Panics
    ///
    /// Panics if the interval is zero.
    pub fn resurrect_scan_interval(mut self, interval: Duration) -> Self {
        self.settings.resurrect_scan_interval =
            require_non_zero_duration("resurrect_scan_interval", interval);
        self
    }

    /// Sets how many consecutive Redis failures the background loops tolerate
    /// before shutting the runtime down. Defaults to 30.
    pub fn redis_failure_tolerance(mut self, tolerance: u32) -> Self {
        self.settings.redis_failure_tolerance = tolerance;
        self
    }

    /// Sets how often to poll for due retries. Defaults to 300ms.
    ///
    /// # Panics
    ///
    /// Panics if the interval is zero.
    pub fn retry_poll_interval(mut self, interval: Duration) -> Self {
        self.settings.retry_poll_interval =
            require_non_zero_duration("retry_poll_interval", interval);
        self
    }

    /// Sets how often to poll for due scheduled jobs. Defaults to 300ms.
    ///
    /// # Panics
    ///
    /// Panics if the interval is zero.
    pub fn schedule_poll_interval(mut self, interval: Duration) -> Self {
        self.settings.schedule_poll_interval =
            require_non_zero_duration("schedule_poll_interval", interval);
        self
    }

    /// Sets how long to wait after startup before scheduling cron jobs.
    /// Defaults to 3s.
    pub fn cron_initial_offset(mut self, offset: Duration) -> Self {
        self.settings.cron_initial_offset = offset;
        self
    }

    /// Sets how far ahead cron occurrences are scheduled. Defaults to 30 seconds.
    pub fn cron_lookahead(mut self, lookahead: Duration) -> Self {
        self.settings.cron_lookahead = lookahead;
        self
    }

    /// Sets how often the cron loop checks for occurrences to schedule.
    /// Defaults to 1s.
    ///
    /// # Panics
    ///
    /// Panics if the interval is zero.
    pub fn cron_tick_interval(mut self, interval: Duration) -> Self {
        self.settings.cron_tick_interval =
            require_non_zero_duration("cron_tick_interval", interval);
        self
    }

    /// Sets how long a dispatcher sleeps after polling an empty queue, which
    /// bounds the pickup latency of jobs enqueued while a queue is idle.
    /// Defaults to 10s.
    ///
    /// # Panics
    ///
    /// Panics if the timeout is zero.
    pub fn dequeue_timeout(mut self, timeout: Duration) -> Self {
        self.settings.dequeue_timeout = require_non_zero_duration("dequeue_timeout", timeout);
        self
    }

    /// Sets how long a dispatcher backs off after a tolerated Redis failure
    /// before polling again. Defaults to 1s.
    ///
    /// # Panics
    ///
    /// Panics if the sleep is zero.
    pub fn dispatcher_idle_sleep(mut self, sleep: Duration) -> Self {
        self.settings.dispatcher_idle_sleep =
            require_non_zero_duration("dispatcher_idle_sleep", sleep);
        self
    }

    /// Sets how long a throttled queue waits before re-checking its throttle
    /// window when no explicit throttle delay is available. Defaults to 100ms.
    ///
    /// # Panics
    ///
    /// Panics if the wait is zero.
    pub fn throttled_queue_fallback_wait(mut self, wait: Duration) -> Self {
        self.settings.throttled_queue_fallback_wait =
            require_non_zero_duration("throttled_queue_fallback_wait", wait);
        self
    }

    /// Returns a catalog of all registered workers and queues.
    pub fn catalog(&self) -> Catalog {
        self.config.catalog_with_queues(&Default::default())
    }

    /// Runs the Oxana worker system.
    pub async fn run(mut self) -> Result<RunStats, OxanaError> {
        if self.settings.dead_process_threshold <= self.settings.heartbeat_interval {
            tracing::warn!(
                dead_process_threshold_ms = self.settings.dead_process_threshold.as_millis(),
                heartbeat_interval_ms = self.settings.heartbeat_interval.as_millis(),
                "dead_process_threshold should be larger than heartbeat_interval; \
                 live processes may be treated as dead and their in-flight jobs \
                 re-enqueued while still running"
            );
        }
        self.config
            .apply_group_filters(&self.only_groups, &self.excluded_groups);
        if self.config.registry.worker_names().is_empty() {
            return Err(OxanaError::ConfigError(
                "no workers are enabled for this runtime".to_string(),
            ));
        }
        crate::launcher::run(self.storage, self.config, self.settings, self.ctx).await
    }

    /// Drains a queue of jobs using this runtime's registrations.
    pub async fn drain(&self, queue: impl Queue) -> Result<DrainStats, OxanaError> {
        drainer::drain(
            &self.storage,
            &self.config,
            &self.settings,
            self.ctx.clone(),
            queue,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) fn settings(&self) -> RuntimeSettings {
        self.settings.clone()
    }
}

pub(crate) struct Runtime<DT> {
    pub(crate) config: Config<DT>,
    pub(crate) settings: RuntimeSettings,
    pub(crate) storage: Storage,
    pub(crate) cancel_token: CancellationToken,
}

impl<DT> Runtime<DT> {
    pub(crate) fn new(storage: Storage, config: Config<DT>, settings: RuntimeSettings) -> Self {
        Self {
            config,
            settings,
            storage,
            cancel_token: CancellationToken::new(),
        }
    }
}

impl<DT> Deref for Runtime<DT> {
    type Target = Config<DT>;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}
