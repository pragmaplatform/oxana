use crate::shared::{WorkerError, WorkerState as WorkerContext, random_string, setup};
use deadpool_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

#[derive(oxana::Registry)]
struct ComponentRegistry(oxana::ComponentRegistry<WorkerContext>);

#[derive(Serialize, oxana::Queue)]
#[oxana(key = "two")]
struct QueueTwo;

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
#[oxana(on_demand)]
pub struct WorkerCounterJob {
    pub key: String,
}

#[derive(oxana::Worker)]
pub struct WorkerCounter {
    ctx: WorkerContext,
}

impl WorkerCounter {
    async fn process(
        &self,
        job: WorkerCounterJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), WorkerError> {
        let mut redis = self.ctx.redis.get().await?;
        let _: () = redis.incr(&job.key, 1).await?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
pub struct CronWorkerCounterJob {}

#[derive(oxana::Worker)]
#[oxana(cron(schedule = "* * * * * *", queue = QueueTwo))]
pub struct CronWorkerCounter {
    ctx: WorkerContext,
}

impl CronWorkerCounter {
    async fn process(
        &self,
        _job: CronWorkerCounterJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), WorkerError> {
        let mut redis = self.ctx.redis.get().await?;
        let _: () = redis.incr("test_worker:counter", 1).await?;
        Ok(())
    }
}

#[test]
fn registry_cron_workers_keep_typed_registration() {
    let component = oxana::iterate_components::<ComponentRegistry>()
        .find(|component| component.0.type_name == stringify!(CronWorkerCounter))
        .expect("cron worker should be present in the component registry");

    assert!(matches!(
        (component.0.definition)(),
        oxana::ComponentDefinition::WorkerRegistration(_)
    ));
}

#[tokio::test]
pub async fn test_registry() -> TestResult {
    let redis_pool = setup();
    let mut redis_conn = redis_pool.get().await?;
    let _: i64 = redis_conn.del("test_worker:counter").await?;

    let ctx = WorkerContext {
        redis: redis_pool.clone(),
    };

    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;

    let runtime = storage
        .runtime(ctx)
        .register::<ComponentRegistry>()
        .exit_when_processed(2);

    // no need to manually register, here we verify they were registered
    let catalog = runtime.catalog();
    assert!(catalog.queues.iter().any(|queue| queue.key == "two"));
    assert!(
        catalog
            .workers
            .iter()
            .any(|worker| worker.name == std::any::type_name::<WorkerCounterJob>())
    );
    assert!(
        catalog
            .cron_workers
            .iter()
            .any(|worker| worker.name == std::any::type_name::<CronWorkerCounterJob>())
    );
    assert!(catalog.on_demand_jobs.iter().any(|job| {
        job.name == std::any::type_name::<WorkerCounterJob>()
            && job.args_template == serde_json::json!({ "key": "" })
    }));

    storage
        .enqueue(
            QueueTwo,
            WorkerCounterJob {
                key: "test_worker:counter".to_owned(),
            },
        )
        .await?;

    runtime.run().await?;

    let mut redis_conn = redis_pool.get().await?;
    let value: Option<i64> = redis_conn.get("test_worker:counter").await?;

    assert_eq!(value, Some(2));

    Ok(())
}
