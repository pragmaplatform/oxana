//! Run with Redis available:
//! REDIS_URL=redis://127.0.0.1:6379 cargo run -p oxana --example terminal_and_snooze

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[derive(Clone)]
struct WorkerContext;

#[derive(oxana::Registry)]
struct ComponentRegistry(oxana::ComponentRegistry<WorkerContext>);

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
struct MessageJob {
    recipient: String,
}

#[derive(oxana::Worker)]
#[oxana(max_retries = 5)]
struct MessageWorker;

impl MessageWorker {
    async fn process(
        &self,
        job: MessageJob,
        ctx: &oxana::JobContext,
    ) -> Result<(), oxana::BoxError> {
        println!(
            "Running job {} with retries={}",
            ctx.meta.id, ctx.meta.retries
        );

        if job.recipient.is_empty() {
            // Permanent failure: skip all five retries and keep the reason in Dead.
            return Err(oxana::Terminal("recipient is empty".into()).into());
        }

        match ctx.state.get::<String>().await? {
            None => {
                // Simulate preparing work that needs a brief wait before completion.
                ctx.state.update("message prepared").await?;
                println!("Snoozing job {} for one second", ctx.meta.id);
                Err(oxana::Snooze(Duration::from_secs(1)).into())
            }
            Some(saved) => {
                // Snooze keeps the job ID, saved state, and retry count unchanged.
                assert_eq!(saved, "message prepared");
                assert_eq!(ctx.meta.retries, 0);
                println!(
                    "Resumed job {}: {saved}; completed for {}",
                    ctx.meta.id, job.recipient
                );
                Ok(())
            }
        }
    }
}

#[derive(Serialize, oxana::Queue)]
#[oxana(key = "messages", concurrency = 1)]
struct MessageQueue;

#[tokio::main]
async fn main() -> Result<(), oxana::OxanaError> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    // Keep repeated runs separate from each other and from other examples.
    let storage = oxana::Storage::builder()
        .namespace(format!("terminal-and-snooze-{}", uuid::Uuid::new_v4()))
        .build_from_env()?;
    let runtime = storage
        .runtime(WorkerContext)
        .register::<ComponentRegistry>()
        .shutdown_on_ctrl_c()
        // One terminal failure, one snooze, and one successful resumed run.
        .exit_when_processed(3);

    let terminal_id = storage
        .enqueue(
            MessageQueue,
            MessageJob {
                recipient: String::new(),
            },
        )
        .await?;
    storage
        .enqueue(
            MessageQueue,
            MessageJob {
                recipient: "reader@example.com".into(),
            },
        )
        .await?;

    let stats = runtime.run().await?;
    assert_eq!((stats.processed, stats.succeeded, stats.failed), (3, 1, 1));
    let dead = storage
        .list_dead(&oxana::QueueListOpts {
            count: 1,
            offset: 0,
        })
        .await?;
    assert_eq!(dead[0].id, terminal_id);
    assert_eq!(dead[0].meta.retries, 0);
    println!(
        "Terminal job stopped without retries: {:?}",
        dead[0].meta.error
    );
    println!("Finished: 3 processed, 1 succeeded, 1 failed; the snooze counts only as processed.");
    Ok(())
}
