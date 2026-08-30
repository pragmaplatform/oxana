# Oxana

[![Build Status](https://img.shields.io/github/actions/workflow/status/pragmaplatform/oxana/test.yml?branch=main)](https://github.com/pragmaplatform/oxana/actions)
[![Latest Version](https://img.shields.io/crates/v/oxana.svg)](https://crates.io/crates/oxana)
[![docs.rs](https://img.shields.io/static/v1?label=docs.rs&message=oxana&color=blue&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K)](https://docs.rs/oxana/latest)

Oxana is a Redis-backed job processing library for Rust. It powers the background job infrastructure behind [Player.gg](https://player.gg) and [Firstlook.gg](https://firstlook.gg), serving hundreds of studios and millions of players.

Oxana focuses on simplicity and depth over breadth - one backend, done well.

<p align="center">
  <picture>
    <img alt="Oxana Web Dashboard" src="https://raw.githubusercontent.com/pragmaplatform/oxana/refs/heads/main/web.png">
  </picture>
</p>

## Key Features

- **Isolated Queues** - separate queues with independent concurrency and configuration
- **Retries** - automatic retry with configurable backoff
- **Scheduled Jobs** - run jobs at specific times or after delays
- **Cron Jobs** - periodic jobs using cron expressions
- **Batch Processing** - process jobs together with size and timeout based batching
- **Dynamic Queues** - create and manage queues at runtime
- **Runtime Queue Controls** - pause queues and adjust dynamic concurrency without restarting workers
- **Throttling** - rate-limit job processing per queue
- **Unique Jobs** - deduplicate jobs so only one instance runs at a time
- **Resumable Jobs** - resume from where a job left off on retry
- **Resilient Jobs** - survive worker crashes and restarts
- **Graceful Shutdown** - clean shutdown with in-progress job handling
- **Web Dashboard** - built-in UI for monitoring jobs, queues, metrics, and cron - pure Rust, no JS toolchain
- **Prometheus Metrics** - export queue and job metrics for monitoring
- **Well Tested** - comprehensive integration test suite

## Quick Start

```bash
cargo add oxana@2.1.3 --features registry
```

The `registry` feature (not enabled by default) powers `#[derive(oxana::Registry)]` and `runtime.register::<...>()` used below; without it, register queues and workers explicitly with `runtime.queue::<...>()` and `runtime.worker::<...>()`.

```rust
use oxana::Storage;
use serde::{Serialize, Deserialize};

#[derive(oxana::Registry)]
struct ComponentRegistry(oxana::ComponentRegistry<MyContext>);

#[derive(Debug, thiserror::Error)]
enum MyError {}

#[derive(Debug, Clone)]
struct MyContext {}

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
struct MyJob {
    data: String,
}

#[derive(oxana::Worker)]
#[oxana(context = MyContext)]
struct MyWorker;

impl MyWorker {
    async fn process(&self, job: MyJob, _ctx: &oxana::JobContext) -> Result<(), MyError> {
        println!("Processing: {}", job.data);
        Ok(())
    }
}

#[derive(oxana::Queue)]
#[oxana(key = "my_queue", concurrency = 2)]
struct MyQueue;

#[tokio::main]
async fn main() -> Result<(), oxana::OxanaError> {
    let storage = Storage::from_env()?;
    let runtime = storage
        .runtime(MyContext {})
        .register::<ComponentRegistry>();

    storage.enqueue(MyQueue, MyJob { data: "hello".into() }).await?;
    runtime.run().await?;
    Ok(())
}
```

For more detailed usage examples, check out the [examples directory](https://github.com/pragmaplatform/oxana/tree/main/oxana/examples).

## Web Dashboard

The `oxana-web` crate provides a built-in dashboard for monitoring jobs, queues, worker metrics, and cron schedules. It integrates as a nested axum router.

```rust
use oxana_web::OxanaWebState;

let runtime = storage
    .runtime(MyContext {})
    .register::<ComponentRegistry>();
let catalog = runtime.catalog();

let oxana_router = oxana_web::router(OxanaWebState::new(
    storage.clone(),
    catalog,
    "/oxana".to_string(),
));

let app = your_app_router().nest("/oxana", oxana_router);
```

The dashboard exposes these pages:

- `/` - Overview with job stats
- `/busy` - Currently processing jobs
- `/queues` - All queues with stats
- `/queues/{queue_key}` - Jobs in a specific queue
- `/jobs/{job_id}` - Details for a specific job
- `/metrics` - Worker execution metrics
- `/metrics/job?worker=...` - Metrics for a specific worker
- `/cron` - Cron job schedules
- `/on-demand` - Manually enqueue registered on-demand jobs
- `/scheduled` - Scheduled jobs
- `/retries` - Jobs pending retry
- `/dead` - Dead letter queue

It also provides management actions for pausing and unpausing queues, changing dynamic queue concurrency from queue detail pages, wiping queues, and deleting individual jobs.

## Core Concepts

### Jobs and Workers

Jobs carry the data that gets enqueued and define enqueue-time metadata. Workers define the processing logic for jobs. Use the `#[derive(oxana::Job)]` and `#[derive(oxana::Worker)]` macros or implement the traits manually.

| `Job` attributes (enqueue-time) | `Worker` attributes (execution-time) |
| --- | --- |
| `#[oxana(unique_id = "worker_{id}")]` - define unique job identifiers | `#[oxana(job = MyJob)]` - override inferred `FooWorker` -> `FooJob` binding |
| `#[oxana(on_conflict = Skip)]` - handle unique job conflicts (Skip or Replace) | `#[oxana(context = MyContext)]` - set worker context type |
| `#[oxana(resurrect = false)]` - disable crash resurrection for this job type | `#[oxana(error = MyError)]` - set worker error type |
| `#[oxana(resume = false)]` - reset prior-attempt job state on retry | `#[oxana(registry = MyRegistry)]` - choose component registry |
| `#[oxana(throttle_cost = 2)]` - set per-job throttle cost | `#[oxana(max_retries = 3)]` - set maximum retry attempts |
| `#[oxana(on_demand)]` - expose the job in the web dashboard for manual enqueueing | `#[oxana(retry_delay = 5)]` - set retry delay in seconds |
|  | `#[oxana(cron(schedule = "*/5 * * * * *", queue = MyQueue))]` - schedule periodic jobs |
|  | `#[oxana(batch_size = 100, batch_timeout_ms = 500)]` - process jobs in batches |

Cron jobs may define a `unique_id` to prevent overlapping occurrences. Their conflict strategy
must be `Skip`; `on_conflict = Replace` is rejected because replacing an in-flight occurrence can
corrupt the following occurrence.

For job hooks, `Self::...` resolves to the job type. For worker hooks, `Self::...` resolves to the worker type.

On-demand argument templates infer editable placeholders from field types. Numeric primitives and common numeric ID newtypes named `*Id` or `*ID` are prefilled with `0`.

Batch workers use all-or-nothing result semantics: if `process_batch` returns `Ok(())`, every job in the batch is marked successful; if it returns an error or panics, every job in that batch follows the normal retry or failure path. Batch handlers should therefore be idempotent, or should only commit external side effects after the whole batch is ready to succeed.

### Queues

Queues are the channels through which jobs flow. Use the `#[derive(oxana::Queue)]` macro or implement the `Queue` trait manually. Static queues do not need `Serialize`; dynamic queues need serializable fields so Oxana can derive each runtime queue key.

Queues can be:

- **Static**: Defined at compile time with a fixed key
- **Dynamic**: Created at runtime with each instance being a separate queue (requires struct fields)

Queue attributes:

- `#[oxana(key = "my_queue")]` - Set static queue key
- `#[oxana(prefix = "dynamic")]` - Set prefix for dynamic queues
- `#[oxana(concurrency = 2)]` - Set a fixed concurrency limit
- `#[oxana(concurrency = Dynamic(2))]` - Set a runtime-adjustable concurrency limit with default `2`
- `#[oxana(throttle(window_ms = 2000, limit = 5))]` - Configure throttling
- `#[oxana(discovery_interval_ms = 250)]` - Set how often new dynamic queues are discovered (dynamic queues only)

### Component Registry

The component registry automatically discovers and registers all workers and queues in your application. Use `#[derive(oxana::Registry)]` to create a registry and `storage.runtime(ctx).register::<ComponentRegistry>()` to register them on a typed runtime.

### Storage

`Storage` provides the interface for job persistence - enqueueing, scheduling, state management, and queue monitoring.

Build it with `Storage::from_env()` or `Storage::builder().build_from_env()`, which read the `REDIS_URL` environment variable.
Set `REDIS_STATS_URL` to store counters and metrics in a separate Redis instance; when it is not set, stats use `REDIS_URL`.
Call `storage.runtime(ctx)` to create a typed worker runtime, register queues and workers on that runtime, then call `runtime.run().await`.

### Context

The context provides shared state and utilities to workers. It can include:

- Database connections
- Configuration
- Shared resources
- Job state (for resumable jobs)

Workers can persist job state with `ctx.state.update(...)` and read the state from
the current attempt with `ctx.state.get::<T>()`. This is useful for resumable jobs
that need to continue from the last completed item after a retry. Jobs resume
state by default; use `#[oxana(resume = false)]` to clear prior-attempt state
before each retry.

For long-running jobs, use `ctx.state.update_progress(...)` to store progress in a
structured format. The web dashboard renders a progress bar when `total` is set:

```rust
ctx.state
    .update_progress((cursor, total, Some("importing users".to_string())))
    .await?;
```

`update_progress` accepts a cursor value, `(cursor, total)`, or
`(cursor, total, note)`. Cursor-only state remains useful for resumable jobs and is
shown as normal state in the dashboard. `ctx.state.progress().await?` reloads the
latest stored progress for the current job.

### Runtime Configuration

Runtime configuration is done on the typed runtime builder, which allows you to:

- Automatically register queues and workers via the component registry
- Set up graceful shutdown
- Configure exit conditions and runtime timing/backoff knobs
- Customize how worker errors are stored on retry and dead jobs

To run only selected queues while keeping every queue and worker registered,
call `only_queue` once for each queue to process:

```rust
let runtime = storage
    .runtime(ctx)
    .register::<Components>()
    .only_queue::<PriorityQueue>()
    .only_queue::<EmailQueue>();

runtime.run().await?;
```

Selecting a dynamic queue processes all of its discovered subqueues. Cron jobs
for unselected queues are not scheduled. Without any `only_queue` calls, all
registered queues run as usual.

`Storage` remains the enqueueing and monitoring handle; `RuntimeBuilder<C>` is the worker setup and execution handle for app context type `C`.

Worker errors use their `Debug` representation by default so error types that capture backtraces
can include them in the dashboard. Applications can override the stored representation when needed:

```rust
let runtime = storage
    .runtime(ctx)
    .error_formatter(|error| error.to_string());
```

With the `sentry` feature enabled, failed executions are reported once by default with isolated
`oxana.*` tags and an `oxana` context containing job IDs, queue/job/worker names, batch size, and
per-job arguments and retry state. Applications that need an error-specific integration can replace
the capture while retaining the original concrete worker error:

```rust
let runtime = storage.runtime(ctx).failure_reporter(|report| {
    match report.failure {
        oxana::WorkerFailure::Error(error) => {
            // Downcast to your application's worker error and call a richer
            // integration, such as sentry_anyhow::capture_anyhow(...).
            report_application_error(error, report.metadata);
        }
        oxana::WorkerFailure::Panic { message } => {
            report_application_panic(message, report.metadata);
        }
        _ => {}
    }
});
```

The callback is also available without the `sentry` feature. Sentry's panic integration runs before
Oxana catches a worker panic, so Oxana suppresses that pre-catch event on the isolated worker hub and
reports the caught panic once after selecting the default or custom reporter. This prevents both a
duplicate event and argument capture before custom redaction can run.

Serialized job arguments are included in failure metadata and default Sentry events. Arguments can
contain sensitive application data; custom reporters receive the metadata without Oxana attaching
it automatically, allowing them to redact or omit arguments before capture.

Backtraces must be enabled before the process starts (for example with `RUST_BACKTRACE=1`) and
captured by the application's error type. Diagnostic output may contain sensitive details, so only
enable backtraces where access to stored job errors is appropriately restricted.

### Error Handling

Oxana uses a custom `OxanaError` type that covers all library error cases. Workers can define their own error type that implements `std::error::Error`.

### Prometheus Metrics

Enable the `prometheus` feature to expose metrics:

```rust
let metrics = storage.metrics().await?;
let output = metrics.encode_to_string()?;
// Serve `output` on your metrics endpoint
```

## Comparison with Similar Libraries

| Feature | Oxana | [Apalis](https://crates.io/crates/apalis) | [rusty-sidekiq](https://crates.io/crates/rusty-sidekiq) | [Fang](https://crates.io/crates/fang) |
|---|---|---|---|---|
| Backend | Redis | Redis, Postgres, `SQLite`, `MySQL`, AMQP, NATS | Redis | Postgres, `SQLite`, `MySQL` |
| Retries | Yes | Yes (tower layer) | Yes | Yes |
| Scheduled Jobs | Yes | Yes | Yes | Yes |
| Cron | Yes | Yes | Yes | Yes |
| Unique Jobs | Yes | No | Yes | Yes |
| Throttling | Yes | No | No | No |
| Dynamic Queues | Yes | No | No | No |
| Resumable Jobs | Yes | No | No | No |
| Graceful Shutdown | Yes | Yes | Partial | No |
| Web UI | Yes | Yes (apalis-board) | No (uses Ruby Sidekiq UI) | No |
| License | MIT | MIT | MIT | MIT |

**Oxana** focuses on depth with a single Redis backend rather than breadth across multiple backends. It is the only Rust job library offering resumable jobs and combines unique jobs, throttling, and a built-in web dashboard in one package.

**Apalis** offers the most backend options and integrates with the tower middleware ecosystem, making it highly extensible. It suits projects that need backend flexibility or already use tower layers. However, its breadth of abstraction can come at the cost of reliability and debuggability in production.

**rusty-sidekiq** is wire-compatible with Ruby Sidekiq, making it ideal for teams migrating from or coexisting with Ruby services. It can share queues with Ruby Sidekiq workers and use the existing Sidekiq web UI.

**Fang** is SQL-database-backed (no Redis dependency) with both async and threaded execution modes. A good fit for projects that prefer Postgres/SQLite over Redis.

## License

Oxana is licensed under the MIT License.
