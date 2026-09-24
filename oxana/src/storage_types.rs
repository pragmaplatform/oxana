use serde::{Deserialize, Serialize};

use crate::worker_registry::JobEnvelopeFactory;
use crate::{JobEnvelope, OxanaError};

/// Options for listing jobs in a queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueListOpts {
    /// Maximum number of jobs to return.
    pub count: usize,
    /// Number of jobs to skip from the start.
    pub offset: usize,
}

/// Catalog of all registered workers and queues.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    /// Regular (non-cron) workers.
    pub workers: Vec<WorkerInfo>,
    /// Cron workers with schedule information.
    pub cron_workers: Vec<CronWorkerInfo>,
    /// Registered queues.
    pub queues: Vec<QueueInfo>,
    /// Jobs explicitly exposed for manual enqueueing from the web dashboard.
    pub on_demand_jobs: Vec<OnDemandJobInfo>,
}

/// Information about an on-demand job exposed in the web dashboard.
#[derive(Debug, Clone)]
pub struct OnDemandJobInfo {
    /// The worker name (Rust type path).
    pub name: String,
    /// Editable JSON template used to prefill the enqueue form.
    pub args_template: serde_json::Value,
    pub(crate) enqueue_factory: JobEnvelopeFactory,
}

impl OnDemandJobInfo {
    /// Builds an enqueueable envelope using the registered job type.
    pub fn enqueue_envelope(
        &self,
        queue: String,
        args: serde_json::Value,
    ) -> Result<JobEnvelope, OxanaError> {
        let mut envelope = (self.enqueue_factory)(queue, args)?;
        envelope.meta.on_demand = Some(true);
        Ok(envelope)
    }
}

/// Information about a registered queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    /// The queue key or prefix.
    pub key: String,
    /// Whether this is a dynamic queue.
    pub dynamic: bool,
    /// The concurrency limit for this queue.
    pub concurrency: usize,
    /// Whether queue concurrency can be changed at runtime.
    pub dynamic_concurrency: bool,
    /// Throttle configuration, if any.
    pub throttle: Option<QueueThrottleInfo>,
}

/// Throttle configuration for a queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueThrottleInfo {
    /// Throttle window in milliseconds.
    pub window_ms: i64,
    /// Maximum number of jobs allowed within the window.
    pub limit: u64,
}

/// Information about a registered worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// The worker name (Rust type path).
    pub name: String,
}

/// Information about a registered cron worker.
#[derive(Debug, Clone)]
pub struct CronWorkerInfo {
    /// The worker name (Rust type path).
    pub name: String,
    /// The cron schedule expression.
    pub schedule: cron::Schedule,
    /// The queue key this worker runs on.
    pub queue_key: String,
    /// Whether jobs for this worker should be resurrected if a process dies.
    pub resurrect: bool,
}
