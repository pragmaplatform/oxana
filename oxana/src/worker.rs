use crate::{
    QueueConfig, WorkerConfigKind, context::JobContext, job_envelope::JobConflictStrategy,
};

#[derive(Debug, Clone)]
pub struct WorkerBatchConfig {
    size: usize,
    timeout: std::time::Duration,
}

impl WorkerBatchConfig {
    pub fn new(size: usize, timeout: std::time::Duration) -> Self {
        assert!(size > 0, "batch size must be greater than zero");
        Self { size, timeout }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn timeout(&self) -> std::time::Duration {
        self.timeout
    }
}

pub trait Job: Send + serde::Serialize {
    #[doc(hidden)]
    const REPLACES_ON_CONFLICT: bool = false;

    fn name() -> &'static str
    where
        Self: Sized + 'static,
    {
        std::any::type_name::<Self>()
    }

    fn unique_id(&self) -> Option<String> {
        None
    }

    fn on_conflict(&self) -> JobConflictStrategy {
        JobConflictStrategy::Skip
    }

    fn should_resurrect() -> bool
    where
        Self: Sized,
    {
        true
    }

    fn should_resume() -> bool
    where
        Self: Sized,
    {
        true
    }

    fn throttle_cost(&self) -> Option<u64> {
        None
    }

    fn on_demand_args_template() -> Option<serde_json::Value>
    where
        Self: Sized,
    {
        None
    }
}

#[derive(Clone)]
pub struct BatchItem<Args> {
    pub job: Args,
    pub ctx: JobContext,
}

#[async_trait::async_trait]
pub trait Worker<Args: Send + 'static>: Send + Sync {
    type Error: IntoWorkerError + Send + Sync + 'static;

    async fn process(&self, job: Args, ctx: &JobContext) -> Result<(), Self::Error> {
        self.run_batch(vec![BatchItem {
            job,
            ctx: ctx.clone(),
        }])
        .await
    }

    async fn run_batch(&self, _jobs: Vec<BatchItem<Args>>) -> Result<(), Self::Error> {
        panic!(
            "Worker::run_batch is not implemented for worker `{}` and job `{}`; \
             implement `process` for single-job workers or `run_batch` for batch workers",
            std::any::type_name::<Self>(),
            std::any::type_name::<Args>()
        );
    }

    fn max_retries(&self, _job: &Args) -> u32 {
        2
    }

    fn retry_delay(&self, _job: &Args, retries: u32) -> u64 {
        // 0 -> 25 seconds
        // 1 -> 125 seconds
        // 2 -> 625 seconds
        // 3 -> 3125 seconds
        // 4 -> 15625 seconds
        // 5 -> 78125 seconds
        // 6 -> 390625 seconds
        // 7 -> 1953125 seconds
        u64::pow(5, retries + 2)
    }

    /// 6 part cron schedule: "* * * * * *"
    fn cron_schedule() -> Option<String>
    where
        Self: Sized,
    {
        None
    }

    fn cron_queue_config() -> Option<QueueConfig>
    where
        Self: Sized,
    {
        None
    }

    fn batch_config() -> Option<WorkerBatchConfig>
    where
        Self: Sized,
    {
        None
    }

    fn to_config() -> WorkerConfigKind
    where
        Self: Sized,
        Args: Job,
    {
        if let Some(schedule) = Self::cron_schedule() {
            let queue_config = Self::cron_queue_config()
                .expect("Cron worker must define cron_queue_config (use #[oxana(cron(schedule = \"...\", queue = MyQueue))])");
            let queue_key = queue_config.static_key().expect(
                "Cron workers must use static queues. Dynamic queues are not supported for cron workers.",
            );
            return WorkerConfigKind::Cron {
                schedule,
                queue_key,
                resurrect: Args::should_resurrect(),
            };
        }
        WorkerConfigKind::Normal
    }
}

pub trait FromContext<T> {
    fn from_context(ctx: &T) -> Self;
}

#[async_trait::async_trait]
pub trait Processable: Send {
    async fn process(self: Box<Self>, contexts: Vec<JobContext>) -> Result<(), WorkerError>;
    fn len(&self) -> usize;
    fn job_name(&self) -> &'static str;
    fn worker_name(&self) -> &'static str;
    fn max_retries(&self, index: usize) -> u32;
    fn retry_delay(&self, index: usize, retries: u32) -> u64;
    fn should_resume(&self) -> bool {
        true
    }
}

pub(crate) type WorkerError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type BoxedProcessable = Box<dyn Processable>;

/// Type-erased worker error, used as the default [`Worker::Error`] type.
///
/// Note that `BoxError` intentionally does not implement [`std::error::Error`]:
/// doing so would conflict with the blanket `From<E: Error>` impl (via the
/// reflexive `From<BoxError>`), which is what lets `?` convert any error type
/// in worker code.
#[derive(Debug)]
pub struct BoxError(WorkerError);

impl std::fmt::Display for BoxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<E> From<E> for BoxError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(error: E) -> Self {
        Self(Box::new(error))
    }
}

pub trait IntoWorkerError {
    fn into_worker_error(self) -> WorkerError;
}

impl<E> IntoWorkerError for E
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_worker_error(self) -> WorkerError {
        Box::new(self)
    }
}

impl IntoWorkerError for BoxError {
    fn into_worker_error(self) -> WorkerError {
        self.0
    }
}

pub(crate) struct BoundJob<W, A> {
    pub worker: W,
    pub job: A,
}

pub(crate) struct BoundBatchJob<W, A> {
    pub worker: W,
    pub jobs: Vec<A>,
}

#[async_trait::async_trait]
impl<W, A> Processable for BoundJob<W, A>
where
    W: Worker<A> + Send + Sync + 'static,
    A: Job + Send + 'static,
{
    async fn process(self: Box<Self>, contexts: Vec<JobContext>) -> Result<(), WorkerError> {
        assert_eq!(contexts.len(), 1, "single job must have one context");
        let ctx = contexts
            .into_iter()
            .next()
            .expect("single job context exists after length check");
        self.worker
            .process(self.job, &ctx)
            .await
            .map_err(IntoWorkerError::into_worker_error)
    }

    fn len(&self) -> usize {
        1
    }

    fn job_name(&self) -> &'static str {
        A::name()
    }

    fn worker_name(&self) -> &'static str {
        std::any::type_name::<W>()
    }

    fn max_retries(&self, index: usize) -> u32 {
        assert_eq!(index, 0, "single job index must be zero");
        self.worker.max_retries(&self.job)
    }

    fn retry_delay(&self, index: usize, retries: u32) -> u64 {
        assert_eq!(index, 0, "single job index must be zero");
        self.worker.retry_delay(&self.job, retries)
    }

    fn should_resume(&self) -> bool {
        A::should_resume()
    }
}

#[async_trait::async_trait]
impl<W, A> Processable for BoundBatchJob<W, A>
where
    W: Worker<A> + Send + Sync + 'static,
    A: Job + Send + 'static,
{
    async fn process(self: Box<Self>, contexts: Vec<JobContext>) -> Result<(), WorkerError> {
        assert_eq!(
            self.jobs.len(),
            contexts.len(),
            "batch jobs and contexts must have the same length"
        );
        let items = self
            .jobs
            .into_iter()
            .zip(contexts)
            .map(|(job, ctx)| BatchItem { job, ctx })
            .collect();
        self.worker
            .run_batch(items)
            .await
            .map_err(IntoWorkerError::into_worker_error)
    }

    fn len(&self) -> usize {
        self.jobs.len()
    }

    fn job_name(&self) -> &'static str {
        A::name()
    }

    fn worker_name(&self) -> &'static str {
        std::any::type_name::<W>()
    }

    fn max_retries(&self, index: usize) -> u32 {
        let job = self.jobs.get(index).expect("batch job index out of bounds");
        self.worker.max_retries(job)
    }

    fn retry_delay(&self, index: usize, retries: u32) -> u64 {
        let job = self.jobs.get(index).expect("batch job index out of bounds");
        self.worker.retry_delay(job, retries)
    }

    fn should_resume(&self) -> bool {
        A::should_resume()
    }
}

#[cfg(test)]
mod processable_tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct LogJob;

    impl Job for LogJob {}

    struct LogWorker;

    #[async_trait::async_trait]
    impl Worker<LogJob> for LogWorker {
        type Error = BoxError;

        async fn process(&self, _job: LogJob, _ctx: &JobContext) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn bound_job_exposes_job_and_worker_names() {
        let processable = BoundJob {
            worker: LogWorker,
            job: LogJob,
        };

        assert_eq!(processable.job_name(), std::any::type_name::<LogJob>());
        assert_eq!(
            processable.worker_name(),
            std::any::type_name::<LogWorker>()
        );
    }
}

#[cfg(feature = "macros")]
#[cfg(test)]
mod tests {
    use super::{Job, JobConflictStrategy};
    use crate::{self as oxana, JobEnvelope};
    use serde::{Deserialize, Serialize};
    use std::io::Error as WorkerError;

    #[derive(Clone, Default)]
    struct WorkerContext {}

    #[derive(oxana::Registry)]
    #[allow(dead_code)]
    struct ComponentRegistry(oxana::ComponentRegistry<WorkerContext>);

    #[derive(oxana::Registry)]
    #[allow(dead_code)]
    struct ComponentRegistryFmt(oxana::ComponentRegistry<WorkerContext>);

    #[tokio::test]
    async fn test_define_worker_with_macro() {
        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        struct TestJob {}

        #[derive(oxana::Worker)]
        struct TestWorker;

        impl TestWorker {
            async fn process(
                &self,
                _job: TestJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), WorkerError> {
                Ok(())
            }
        }

        assert_eq!(
            oxana::Worker::<TestJob>::max_retries(&TestWorker, &TestJob {}),
            2
        );
        assert_eq!(TestJob::name(), std::any::type_name::<TestJob>());

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(on_conflict = Replace)]
        struct TestWorkerCustomErrorJob {}

        #[derive(oxana::Worker)]
        #[oxana(error = std::fmt::Error, registry = ComponentRegistryFmt)]
        #[oxana(max_retries = 3, retry_delay = 10)]
        struct TestWorkerCustomError;

        impl TestWorkerCustomError {
            async fn process(
                &self,
                _job: TestWorkerCustomErrorJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), std::fmt::Error> {
                use std::fmt::Write;
                let mut s = String::new();
                write!(&mut s, "hi")
            }
        }

        assert_eq!(
            oxana::Worker::<TestWorkerCustomErrorJob>::max_retries(
                &TestWorkerCustomError,
                &TestWorkerCustomErrorJob {}
            ),
            3
        );
        assert_eq!(
            oxana::Worker::<TestWorkerCustomErrorJob>::retry_delay(
                &TestWorkerCustomError,
                &TestWorkerCustomErrorJob {},
                1
            ),
            10
        );
        assert_eq!(
            TestWorkerCustomErrorJob {}.on_conflict(),
            JobConflictStrategy::Replace
        );

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(unique_id = "test_worker_{id}")]
        struct TestWorkerUniqueIdJob {
            id: i32,
            _1: i32,
        }

        #[derive(oxana::Worker)]
        struct TestWorkerUniqueId;

        impl TestWorkerUniqueId {
            async fn process(
                &self,
                _job: TestWorkerUniqueIdJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), WorkerError> {
                Ok(())
            }
        }

        assert_eq!(
            oxana::Worker::<TestWorkerUniqueIdJob>::max_retries(
                &TestWorkerUniqueId,
                &TestWorkerUniqueIdJob { id: 0, _1: 0 }
            ),
            2
        );
        assert_eq!(
            oxana::Job::unique_id(&TestWorkerUniqueIdJob { id: 1, _1: 0 }),
            Some("test_worker_1".to_string())
        );
        assert_eq!(
            oxana::Job::unique_id(&TestWorkerUniqueIdJob { id: 12, _1: 0 }),
            Some("test_worker_12".to_string())
        );

        #[derive(Debug, Serialize, Deserialize, Default)]
        struct NestedTask {
            name: String,
        }

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(unique_id(fmt = "test_worker_{id}_{task}", id = self.id, task = self.task.name))]
        struct TestWorkerNestedUniqueIdJob {
            id: i32,
            task: NestedTask,
        }

        #[derive(oxana::Worker)]
        struct TestWorkerNestedUniqueId;

        impl TestWorkerNestedUniqueId {
            async fn process(
                &self,
                _job: TestWorkerNestedUniqueIdJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), WorkerError> {
                Ok(())
            }
        }

        assert_eq!(
            oxana::Job::unique_id(&TestWorkerNestedUniqueIdJob {
                id: 1,
                task: NestedTask {
                    name: "task1".to_owned(),
                }
            }),
            Some("test_worker_1_task1".to_string())
        );
        assert_eq!(
            oxana::Job::unique_id(&TestWorkerNestedUniqueIdJob {
                id: 2,
                task: NestedTask {
                    name: "task2".to_owned(),
                }
            }),
            Some("test_worker_2_task2".to_string())
        );

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(unique_id = Self::unique_id)]
        #[oxana(throttle_cost = Self::throttle_cost)]
        struct TestWorkerCustomUniqueIdJob {
            id: i32,
            task: NestedTask,
            cost: u64,
        }

        impl TestWorkerCustomUniqueIdJob {
            fn unique_id(&self) -> Option<String> {
                Some(format!("worker_id_{}_task_{}", self.id, self.task.name))
            }

            fn throttle_cost(&self) -> Option<u64> {
                Some(self.cost)
            }
        }

        #[derive(oxana::Worker)]
        #[oxana(retry_delay = Self::retry_delay)]
        #[oxana(max_retries = Self::max_retries)]
        struct TestWorkerCustomUniqueId;

        impl TestWorkerCustomUniqueId {
            async fn process(
                &self,
                _job: TestWorkerCustomUniqueIdJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), WorkerError> {
                Ok(())
            }

            fn retry_delay(&self, _job: &TestWorkerCustomUniqueIdJob, retries: u32) -> u64 {
                retries as u64 * 2
            }

            fn max_retries(&self, _job: &TestWorkerCustomUniqueIdJob) -> u32 {
                9
            }
        }

        assert_eq!(
            oxana::Job::unique_id(&TestWorkerCustomUniqueIdJob {
                id: 1,
                task: NestedTask {
                    name: "11".to_owned(),
                },
                cost: 3,
            }),
            Some("worker_id_1_task_11".to_string())
        );
        let job2 = TestWorkerCustomUniqueIdJob {
            id: 2,
            task: NestedTask {
                name: "22".to_owned(),
            },
            cost: 5,
        };
        assert_eq!(
            oxana::Job::unique_id(&job2),
            Some("worker_id_2_task_22".to_string())
        );
        assert_eq!(oxana::Job::throttle_cost(&job2), Some(5));
        let worker = TestWorkerCustomUniqueId;
        assert_eq!(
            oxana::Worker::<TestWorkerCustomUniqueIdJob>::retry_delay(&worker, &job2, 1),
            2
        );
        assert_eq!(
            oxana::Worker::<TestWorkerCustomUniqueIdJob>::retry_delay(&worker, &job2, 2),
            4
        );
        assert_eq!(
            oxana::Worker::<TestWorkerCustomUniqueIdJob>::max_retries(&worker, &job2),
            9
        );
        let envelope = JobEnvelope::new(
            "default".to_owned(),
            TestWorkerCustomUniqueIdJob {
                id: 3,
                task: NestedTask {
                    name: "33".to_owned(),
                },
                cost: 7,
            },
        )
        .expect("job-owned throttle_cost should populate the envelope");
        assert_eq!(envelope.meta.throttle_cost, Some(7));

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(unique_id = TestWorkerExplicitJobHooksJob::unique_id)]
        #[oxana(throttle_cost = TestWorkerExplicitJobHooksJob::throttle_cost)]
        struct TestWorkerExplicitJobHooksJob {
            id: i32,
            cost: u64,
        }

        impl TestWorkerExplicitJobHooksJob {
            fn unique_id(&self) -> Option<String> {
                Some(format!("explicit_job_{}", self.id))
            }

            fn throttle_cost(&self) -> Option<u64> {
                Some(self.cost)
            }
        }

        #[derive(oxana::Worker)]
        struct TestWorkerExplicitJobHooks;

        impl TestWorkerExplicitJobHooks {
            async fn process(
                &self,
                _job: TestWorkerExplicitJobHooksJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), WorkerError> {
                Ok(())
            }
        }

        let explicit_job = TestWorkerExplicitJobHooksJob { id: 4, cost: 9 };
        assert_eq!(
            oxana::Job::unique_id(&explicit_job),
            Some("explicit_job_4".to_string())
        );
        assert_eq!(oxana::Job::throttle_cost(&explicit_job), Some(9));
        let explicit_envelope = JobEnvelope::new(
            "default".to_owned(),
            TestWorkerExplicitJobHooksJob { id: 5, cost: 11 },
        )
        .expect("explicit job hook paths should still populate the envelope");
        assert_eq!(explicit_envelope.meta.throttle_cost, Some(11));

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        struct TestWorkerBatchJob {
            value: u32,
        }

        #[derive(oxana::Worker)]
        #[oxana(batch_size = 25, batch_timeout_ms = 150)]
        struct TestWorkerBatch;

        impl TestWorkerBatch {
            async fn process_batch(
                &self,
                _jobs: Vec<oxana::BatchItem<TestWorkerBatchJob>>,
            ) -> Result<(), WorkerError> {
                Ok(())
            }
        }

        let batch_config = <TestWorkerBatch as oxana::Worker<TestWorkerBatchJob>>::batch_config()
            .expect("batch attributes should generate worker batch config");
        assert_eq!(batch_config.size(), 25);
        assert_eq!(
            batch_config.timeout(),
            std::time::Duration::from_millis(150)
        );
    }

    #[tokio::test]
    async fn default_worker_run_batch_panics_instead_of_recursing() {
        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        struct MissingWorkerHookJob;

        struct MissingWorkerHook;

        impl oxana::FromContext<()> for MissingWorkerHook {
            fn from_context(_ctx: &()) -> Self {
                Self
            }
        }

        #[async_trait::async_trait]
        impl oxana::Worker<MissingWorkerHookJob> for MissingWorkerHook {
            type Error = WorkerError;
        }

        let envelope = JobEnvelope::new("default".to_owned(), MissingWorkerHookJob)
            .expect("test job should serialize");
        let ctx = oxana::JobContext {
            meta: envelope.meta.clone(),
            state: crate::JobState::new(
                oxana::Storage::builder()
                    .build_from_redis_url("redis://127.0.0.1/0")
                    .expect("test storage should build"),
                envelope.id,
                envelope.meta.state,
            ),
        };

        let join = tokio::spawn(async move {
            <MissingWorkerHook as oxana::Worker<MissingWorkerHookJob>>::process(
                &MissingWorkerHook,
                MissingWorkerHookJob,
                &ctx,
            )
            .await
        })
        .await;

        let panic = join.expect_err("missing worker hook should panic");
        let message = panic
            .try_into_panic()
            .expect("join error should contain panic payload")
            .downcast::<String>()
            .expect("panic payload should be a string");
        assert!(message.contains("Worker::run_batch is not implemented"));
        assert!(message.contains("MissingWorkerHook"));
        assert!(message.contains("MissingWorkerHookJob"));
    }

    #[tokio::test]
    async fn test_define_cron_worker_with_macro() {
        use crate as oxana;
        use crate::Queue;
        use std::io::Error as WorkerError;

        #[derive(Serialize, oxana::Queue)]
        struct DefaultQueue;

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(unique_id = "test_cron", on_conflict = Skip)]
        struct TestCronJob {}

        #[derive(oxana::Worker)]
        #[oxana(cron(schedule = "*/1 * * * * *", queue = DefaultQueue))]
        struct TestCronWorker;

        impl TestCronWorker {
            async fn process(
                &self,
                _job: TestCronJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), WorkerError> {
                Ok(())
            }
        }

        assert_eq!(
            <TestCronWorker as oxana::Worker<TestCronJob>>::cron_schedule(),
            Some("*/1 * * * * *".to_string())
        );
        assert_eq!(
            <TestCronWorker as oxana::Worker<TestCronJob>>::cron_queue_config(),
            Some(DefaultQueue::to_config()),
        );
        assert!(<TestCronJob as oxana::Job>::should_resurrect());
    }

    #[tokio::test]
    async fn test_define_worker_with_resurrect_false() {
        use crate as oxana;
        use std::io::Error as WorkerError;

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(resurrect = false)]
        struct NoResurrectJob {}

        #[derive(oxana::Worker)]
        struct NoResurrectWorker;

        impl NoResurrectWorker {
            async fn process(
                &self,
                _job: NoResurrectJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), WorkerError> {
                Ok(())
            }
        }

        assert!(!<NoResurrectJob as oxana::Job>::should_resurrect());

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        struct DefaultResurrectJob {}

        #[derive(oxana::Worker)]
        struct DefaultResurrectWorker;

        impl DefaultResurrectWorker {
            async fn process(
                &self,
                _job: DefaultResurrectJob,
                _ctx: &oxana::JobContext,
            ) -> Result<(), WorkerError> {
                Ok(())
            }
        }

        assert!(<DefaultResurrectJob as oxana::Job>::should_resurrect());
    }

    #[test]
    fn test_define_job_with_resume_false() {
        use crate as oxana;

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(resume = false)]
        struct NoResumeJob {}

        assert!(!<NoResumeJob as oxana::Job>::should_resume());

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        struct DefaultResumeJob {}

        assert!(<DefaultResumeJob as oxana::Job>::should_resume());
    }

    #[test]
    fn test_on_demand_args_template_macro() {
        use crate as oxana;
        use serde_json::json;
        use std::collections::HashMap;

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        struct DefaultOffJob {
            value: String,
        }

        assert_eq!(DefaultOffJob::on_demand_args_template(), None);

        #[derive(Debug, Serialize, Deserialize)]
        struct NestedTask {
            name: String,
        }

        #[repr(transparent)]
        #[derive(Debug, Serialize, Deserialize)]
        struct CustomerId(i32);

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(on_demand)]
        #[serde(rename_all = "camelCase")]
        struct NamedOnDemandJob {
            name: String,
            count: u32,
            enabled: bool,
            ratio: f64,
            optional: Option<String>,
            tags: Vec<String>,
            labels: HashMap<String, String>,
            nested: NestedTask,
            customer_id: CustomerId,
            #[serde(rename = "custom_id")]
            renamed_id: u64,
            #[serde(skip)]
            #[allow(dead_code)]
            skipped: String,
        }

        assert_eq!(
            NamedOnDemandJob::on_demand_args_template(),
            Some(json!({
                "name": "",
                "count": 0,
                "enabled": false,
                "ratio": 0.0,
                "optional": null,
                "tags": [],
                "labels": {},
                "nested": {},
                "customerId": 0,
                "custom_id": 0,
            }))
        );

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(on_demand)]
        struct TupleOnDemandJob(String, u64, Option<bool>, Vec<String>);

        assert_eq!(
            TupleOnDemandJob::on_demand_args_template(),
            Some(json!(["", 0, null, []]))
        );

        #[derive(Debug, Serialize, Deserialize, oxana::Job)]
        #[oxana(on_demand)]
        struct UnitOnDemandJob;

        assert_eq!(
            UnitOnDemandJob::on_demand_args_template(),
            Some(serde_json::Value::Null)
        );
    }
}
