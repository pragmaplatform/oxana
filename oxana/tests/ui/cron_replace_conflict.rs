use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct WorkerContext;

#[derive(Debug, thiserror::Error)]
enum WorkerError {}

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
#[oxana(unique_id = "singleton", on_conflict = Replace)]
struct ReplaceCronJob {}

#[derive(Serialize, oxana::Queue)]
#[oxana(registry = None)]
struct ReplaceCronQueue;

#[derive(oxana::Worker)]
#[oxana(registry = None)]
#[oxana(cron(schedule = "*/5 * * * * *", queue = ReplaceCronQueue))]
struct ReplaceCronWorker;

impl ReplaceCronWorker {
    async fn process(
        &self,
        _job: ReplaceCronJob,
        _ctx: &oxana::JobContext,
    ) -> Result<(), WorkerError> {
        Ok(())
    }
}

fn main() {}
