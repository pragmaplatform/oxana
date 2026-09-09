// cargo run --package oxana-web-topcoat --example web

use serde::Serialize;

#[derive(oxana::Registry)]
struct ComponentRegistry(oxana::ComponentRegistry<WorkerContext>);

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error(transparent)]
    Oxana(#[from] oxana::OxanaError),
}

#[derive(Debug, Clone)]
struct WorkerContext {}

mod demo {
    pub(crate) mod crm {
        pub(crate) mod customers {
            use crate::{ComponentRegistry, WorkerContext, WorkerError};
            use serde::{Deserialize, Serialize};

            #[derive(Debug, Serialize, Deserialize, oxana::Job)]
            #[oxana(on_demand)]
            pub(crate) struct SyncCustomerProfileJob {
                pub(crate) duration_ms: u64,
            }

            #[derive(oxana::Worker)]
            #[oxana(max_retries = 0)]
            struct SyncCustomerProfileWorker;

            impl SyncCustomerProfileWorker {
                async fn process(
                    &self,
                    job: SyncCustomerProfileJob,
                    _ctx: &oxana::JobContext,
                ) -> Result<(), WorkerError> {
                    tokio::time::sleep(std::time::Duration::from_millis(job.duration_ms)).await;
                    Ok(())
                }
            }
        }
    }

    pub(crate) mod billing {
        pub(crate) mod invoices {
            use crate::{ComponentRegistry, WorkerContext, WorkerError};
            use serde::{Deserialize, Serialize};

            #[derive(Debug, Serialize, Deserialize, oxana::Job)]
            #[oxana(on_demand)]
            pub(crate) struct GenerateInvoiceJob {
                pub(crate) duration_ms: u64,
            }

            #[derive(oxana::Worker)]
            #[oxana(max_retries = 0)]
            struct GenerateInvoiceWorker;

            impl GenerateInvoiceWorker {
                async fn process(
                    &self,
                    job: GenerateInvoiceJob,
                    _ctx: &oxana::JobContext,
                ) -> Result<(), WorkerError> {
                    tokio::time::sleep(std::time::Duration::from_millis(job.duration_ms)).await;
                    Ok(())
                }
            }
        }
    }

    pub(crate) mod messaging {
        pub(crate) mod receipts {
            use crate::{ComponentRegistry, WorkerContext, WorkerError};
            use serde::{Deserialize, Serialize};

            #[derive(Debug, Serialize, Deserialize, oxana::Job)]
            pub(crate) struct SendReceiptEmailJob {
                pub(crate) duration_ms: u64,
            }

            #[derive(oxana::Worker)]
            #[oxana(max_retries = 0)]
            struct SendReceiptEmailWorker;

            impl SendReceiptEmailWorker {
                async fn process(
                    &self,
                    job: SendReceiptEmailJob,
                    _ctx: &oxana::JobContext,
                ) -> Result<(), WorkerError> {
                    tokio::time::sleep(std::time::Duration::from_millis(job.duration_ms)).await;
                    Ok(())
                }
            }
        }
    }

    pub(crate) mod maintenance {
        pub(crate) mod cleanup {
            use crate::{ComponentRegistry, DefaultQueue, WorkerContext, WorkerError};
            use serde::{Deserialize, Serialize};

            #[derive(Debug, Serialize, Deserialize, oxana::Job)]
            pub(crate) struct CleanupExpiredSessionsJob {}

            #[derive(oxana::Worker)]
            #[oxana(max_retries = 0)]
            #[oxana(cron(schedule = "*/30 * * * * *", queue = DefaultQueue))]
            struct CleanupExpiredSessionsWorker;

            impl CleanupExpiredSessionsWorker {
                async fn process(
                    &self,
                    _job: CleanupExpiredSessionsJob,
                    _ctx: &oxana::JobContext,
                ) -> Result<(), WorkerError> {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    Ok(())
                }
            }
        }

        pub(crate) mod progress {
            use crate::{ComponentRegistry, WorkerContext, WorkerError};
            use serde::{Deserialize, Serialize};

            #[derive(Debug, Serialize, Deserialize, oxana::Job)]
            #[oxana(on_demand)]
            pub(crate) struct LongRunningProgressJob {
                pub(crate) duration_s: i64,
            }

            #[derive(oxana::Worker)]
            #[oxana(max_retries = 0)]
            struct LongRunningProgressWorker;

            impl LongRunningProgressWorker {
                async fn process(
                    &self,
                    job: LongRunningProgressJob,
                    ctx: &oxana::JobContext,
                ) -> Result<(), WorkerError> {
                    ctx.state
                        .update_progress((
                            0,
                            job.duration_s,
                            "Starting long-running job".to_string(),
                        ))
                        .await?;

                    for second in 1..=job.duration_s {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        ctx.state
                            .update_progress((
                                second,
                                job.duration_s,
                                Some(format!("Completed second {second} of {}", job.duration_s)),
                            ))
                            .await?;
                    }

                    Ok(())
                }
            }
        }
    }
}

use demo::billing::invoices::GenerateInvoiceJob;
use demo::crm::customers::SyncCustomerProfileJob;
use demo::maintenance::progress::LongRunningProgressJob;
use demo::messaging::receipts::SendReceiptEmailJob;

#[derive(Serialize, oxana::Queue)]
#[oxana(key = "default", concurrency = 5)]
struct DefaultQueue;

#[derive(Serialize, oxana::Queue)]
#[oxana(key = "priority", concurrency = 5)]
struct PriorityQueue;

#[derive(Serialize, oxana::Queue)]
#[oxana(key = "progress", concurrency = 1)]
struct ProgressQueue;

#[derive(Serialize)]
struct TenantQueueBase;

impl oxana::Queue for TenantQueueBase {
    fn key(&self) -> String {
        "tenant".to_string()
    }

    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_dynamic("tenant").dynamic_concurrency(1)
    }
}

#[derive(Serialize, oxana::Queue)]
#[oxana(prefix = "tenant", concurrency = Dynamic(1))]
struct TenantQueue {
    tenant: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = oxana::Storage::builder().build_from_env()?;
    let worker_ctx = WorkerContext {};
    let runtime = storage
        .runtime(worker_ctx)
        .register::<ComponentRegistry>()
        .shutdown_on_ctrl_c();
    let catalog = runtime.catalog();

    seed_sample_jobs(&storage).await?;

    let mut worker = tokio::spawn(async move {
        if let Err(err) = runtime.run().await {
            eprintln!("Oxana worker exited with error: {err}");
        }
    });

    let base_path = "/oxana";
    let oxana_state =
        oxana_web_topcoat::OxanaWebState::new(storage, catalog, base_path.to_string());

    let app = axum::Router::new().nest(base_path, oxana_web_topcoat::router(oxana_state));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("Oxana web UI available at http://localhost:3000{base_path}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    if !worker.is_finished() {
        match tokio::time::timeout(std::time::Duration::from_secs(2), &mut worker).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) if err.is_cancelled() => {}
            Ok(Err(err)) => return Err(Box::new(err) as Box<dyn std::error::Error>),
            Err(_) => {
                worker.abort();
                match worker.await {
                    Ok(()) => {}
                    Err(err) if err.is_cancelled() => {}
                    Err(err) => return Err(Box::new(err) as Box<dyn std::error::Error>),
                }
            }
        }
    }

    Ok(())
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        eprintln!("Failed to listen for Ctrl-C: {err}");
    }
    println!("Shutting down Oxana web example");
}

async fn seed_sample_jobs(storage: &oxana::Storage) -> Result<(), oxana::OxanaError> {
    for _ in 0..3 {
        storage
            .enqueue(PriorityQueue, SyncCustomerProfileJob { duration_ms: 0 })
            .await?;
    }
    for _ in 0..2 {
        storage
            .enqueue(DefaultQueue, SyncCustomerProfileJob { duration_ms: 0 })
            .await?;
    }
    for _ in 0..2 {
        storage
            .enqueue(DefaultQueue, GenerateInvoiceJob { duration_ms: 0 })
            .await?;
    }
    for _ in 0..3 {
        storage
            .enqueue(DefaultQueue, SendReceiptEmailJob { duration_ms: 0 })
            .await?;
    }

    for duration_ms in [25, 50, 100, 250, 500, 1000, 2500, 1000] {
        storage
            .enqueue(PriorityQueue, SyncCustomerProfileJob { duration_ms })
            .await?;
    }
    for duration_ms in [25, 50, 100, 250, 500, 1000, 2500, 2000] {
        storage
            .enqueue(DefaultQueue, SyncCustomerProfileJob { duration_ms })
            .await?;
    }
    for duration_ms in [25, 50, 100, 250, 500, 1000, 2500, 2000] {
        storage
            .enqueue(DefaultQueue, GenerateInvoiceJob { duration_ms })
            .await?;
    }
    for duration_ms in [25, 50, 100, 250, 500, 1000, 2500, 2000] {
        storage
            .enqueue(DefaultQueue, SendReceiptEmailJob { duration_ms })
            .await?;
    }

    storage
        .enqueue(
            PriorityQueue,
            SyncCustomerProfileJob {
                duration_ms: seconds(15),
            },
        )
        .await?;
    storage
        .enqueue(
            DefaultQueue,
            SyncCustomerProfileJob {
                duration_ms: seconds(30),
            },
        )
        .await?;
    storage
        .enqueue(
            DefaultQueue,
            GenerateInvoiceJob {
                duration_ms: seconds(45),
            },
        )
        .await?;
    storage
        .enqueue(
            DefaultQueue,
            SendReceiptEmailJob {
                duration_ms: seconds(60),
            },
        )
        .await?;
    storage
        .enqueue(
            PriorityQueue,
            SyncCustomerProfileJob {
                duration_ms: seconds(75),
            },
        )
        .await?;
    storage
        .enqueue(
            DefaultQueue,
            SyncCustomerProfileJob {
                duration_ms: seconds(90),
            },
        )
        .await?;
    storage
        .enqueue(
            DefaultQueue,
            GenerateInvoiceJob {
                duration_ms: seconds(105),
            },
        )
        .await?;
    storage
        .enqueue(
            DefaultQueue,
            SendReceiptEmailJob {
                duration_ms: seconds(120),
            },
        )
        .await?;
    storage
        .enqueue(
            PriorityQueue,
            SyncCustomerProfileJob {
                duration_ms: seconds(135),
            },
        )
        .await?;
    storage
        .enqueue(
            PriorityQueue,
            SyncCustomerProfileJob {
                duration_ms: seconds(100),
            },
        )
        .await?;
    seed_dynamic_tenant_jobs(storage).await?;
    storage
        .enqueue(ProgressQueue, LongRunningProgressJob { duration_s: 150 })
        .await?;

    println!("Seeded sample jobs across default, priority, and tenant dynamic queues");

    Ok(())
}

async fn seed_dynamic_tenant_jobs(storage: &oxana::Storage) -> Result<(), oxana::OxanaError> {
    storage.set_queue_concurrency(TenantQueueBase, 2).await?;
    storage
        .set_queue_concurrency(TenantQueue { tenant: "acme" }, 3)
        .await?;
    storage
        .set_queue_state(TenantQueue { tenant: "globex" }, oxana::QueueState::Paused)
        .await?;

    for duration_ms in [seconds(45), seconds(90), seconds(135), seconds(180)] {
        storage
            .enqueue(
                TenantQueue { tenant: "acme" },
                SyncCustomerProfileJob { duration_ms },
            )
            .await?;
    }

    for duration_ms in [seconds(60), seconds(120), seconds(180)] {
        storage
            .enqueue(
                TenantQueue { tenant: "globex" },
                GenerateInvoiceJob { duration_ms },
            )
            .await?;
    }

    for duration_ms in [seconds(30), seconds(75), seconds(150)] {
        storage
            .enqueue(
                TenantQueue { tenant: "initech" },
                SendReceiptEmailJob { duration_ms },
            )
            .await?;
    }

    Ok(())
}

fn seconds(value: u64) -> u64 {
    value * 1000
}
