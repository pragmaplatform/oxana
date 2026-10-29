# Oxana

[![Build Status](https://img.shields.io/github/actions/workflow/status/pragmaplatform/oxana/test.yml?branch=main)](https://github.com/pragmaplatform/oxana/actions)
[![Latest Version](https://img.shields.io/crates/v/oxana.svg)](https://crates.io/crates/oxana)
[![docs.rs](https://img.shields.io/docsrs/oxana)](https://docs.rs/oxana/latest)

Oxana is a Redis-backed job processing library for Rust. It powers the background job infrastructure behind [Player.gg](https://player.gg) and [Firstlook.gg](https://firstlook.gg), serving hundreds of studios and millions of players.

Use it to enqueue typed jobs, run workers with shared application state, and monitor queues through a Rust web dashboard. Jobs can be retried or recovered after a worker stops, so handlers should tolerate repeated execution.

<p align="center">
  <picture>
    <img alt="Oxana Web Dashboard" src="https://raw.githubusercontent.com/pragmaplatform/oxana/refs/heads/main/web.png">
  </picture>
</p>

## Key Features

- **Queue controls**: static and dynamic queues with independent concurrency, pause/resume, and rate limits.
- **Scheduling**: immediate, delayed, scheduled, and cron jobs.
- **Retries and recovery**: configurable retry backoff, dead jobs, crash recovery, and graceful shutdown.
- **Unique jobs**: deduplicate by job ID, with skip or replace policies.
- **Resumable jobs**: persist application checkpoints and progress between attempts.
- **Batch processing**: collect jobs by batch size and timeout.
- **Monitoring**: an Axum web dashboard, Prometheus metrics, Sentry error reporting (enabled by default), and a separate read-only MCP server.

[Quick start](#quick-start) · [Core concepts](#core-concepts) · [Monitoring](#web-dashboard) · [Comparison](#comparison-with-similar-libraries) · [API docs](https://docs.rs/oxana/latest/oxana/) · [Examples](https://github.com/pragmaplatform/oxana/tree/main/oxana/examples)

## Quick Start

Requires Rust 1.89 or newer and a running Redis instance. In a new Rust binary project, add the dependencies used below:

```bash
cargo add oxana@2.5.0 --features registry
cargo add serde --features derive
cargo add tokio --features macros,rt-multi-thread
cargo add async-trait thiserror
```

Set the Redis connection URL:

```bash
export REDIS_URL=redis://127.0.0.1:6379
```

The opt-in `registry` feature discovers the queues and workers below. Put this code in `src/main.rs`:

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
#[oxana(context = MyContext, error = MyError)]
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

Run `cargo run` to enqueue and process the job. The runtime keeps listening for more work until shutdown: SIGINT or SIGTERM on Unix, or Ctrl+C on Windows.

Examples: [scheduling](https://github.com/pragmaplatform/oxana/blob/main/oxana/examples/scheduled.rs), [cron](https://github.com/pragmaplatform/oxana/blob/main/oxana/examples/cron.rs), [unique jobs](https://github.com/pragmaplatform/oxana/blob/main/oxana/examples/unique.rs), [dynamic queues](https://github.com/pragmaplatform/oxana/blob/main/oxana/examples/dynamic.rs), [throttling](https://github.com/pragmaplatform/oxana/blob/main/oxana/examples/throttled.rs), [batches](https://github.com/pragmaplatform/oxana/blob/main/oxana/examples/batch.rs), and [resumable jobs](https://github.com/pragmaplatform/oxana/blob/main/oxana/examples/resumable.rs).

## Web Dashboard

The separate `oxana-web` crate provides a dashboard for jobs, queues, worker metrics, and cron schedules, with no separate JavaScript build step. Add `oxana-web` to your Axum application and nest its router:

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

The dashboard shows active jobs, queues, worker metrics, cron schedules, scheduled jobs, retries, and dead jobs. Management actions include:

- Manually trigger cron jobs or enqueue jobs marked `#[oxana(on_demand)]`.
- Pause and resume queues, or change dynamic queue concurrency.
- Retry all pending retries immediately or revive all dead jobs.
- Delete individual jobs or clear queues.

Routes below are relative to the mount path (`/oxana` in the example):

- `/` — overview and job statistics
- `/busy` — jobs currently being processed
- `/queues` and `/queues/{queue_key}` — queue statistics and queued jobs
- `/jobs/{job_id}` — job details, including state, progress, and errors
- `/metrics` and `/metrics/job?worker=...` — execution metrics and worker details
- `/cron` — cron schedules
- `/on-demand` — manually enqueue registered on-demand jobs
- `/scheduled`, `/retries`, and `/dead` — scheduled jobs, pending retries, and dead jobs

See the [web example](https://github.com/pragmaplatform/oxana/blob/main/oxana-web/examples/web.rs) for a complete application. The router exposes job data and management actions; apply your application's authentication and access controls when mounting it.

For monitoring through MCP clients, use the separate [oxana-mcp crate](https://github.com/pragmaplatform/oxana/tree/main/oxana-mcp). It serves bearer-authenticated, read-only tools for queue statistics, jobs, processes, and metrics.

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

With the `registry` feature enabled, derives register components with their selected registry. Use `#[derive(oxana::Registry)]` to define the registry and `storage.runtime(ctx).register::<ComponentRegistry>()` to add its components to a typed runtime. Without this feature, register queues and workers explicitly with `.queue::<MyQueue>()` and `.worker::<MyWorker, MyJob>()`.

### Storage

`Storage` provides the interface for job persistence - enqueueing, scheduling, state management, and queue monitoring.

Build it with `Storage::from_env()` or `Storage::builder().build_from_env()`, which read the `REDIS_URL` environment variable.
Set `REDIS_STATS_URL` to store counters and metrics in a separate Redis instance; when it is not set, stats use `REDIS_URL`.
Call `storage.runtime(ctx)` to create a typed worker runtime, register queues and workers on that runtime, then call `runtime.run().await`.

Use `enqueue` for immediate work, `enqueue_in` or `enqueue_at` for delayed work, and `enqueue_list` for bulk enqueueing. For unique jobs, `try_enqueue` returns `Some(job_id)` when inserted or replaced and `None` when skipped. Storage failures return an error.

### Redis Connection Retries

Claimed-job start, acknowledgment, retry, and dead-letter transitions retry
transient connection-acquisition failures by default. The total checkout budget
is two seconds or twice the longest configured pool timeout, whichever is
greater. It includes the first attempt and backoff; unset pool timeouts do not
extend it. Backoff starts at 100 ms and doubles up to one second, with each delay
randomly selected between half and all of that ceiling.

Override the policy when needed:

```rust
use std::time::Duration;
use oxana::{PoolRetryConfig, Storage};

let storage = Storage::builder()
    .claimed_connection_retries(Some(PoolRetryConfig {
        retry_window: Some(Duration::from_secs(5)),
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(1),
    }))
    .build_from_env()?;
```

`PoolRetryConfig::retry_window: None` keeps the automatic timeout-derived budget.
`claimed_connection_retries(None)` disables this policy and uses the pool's own
checkout deadlines. The policy also applies to supplied primary pools and new
runtime/drain process identities. It does not apply to heartbeats, enqueueing,
statistics, or other ordinary pool access. Commands already sent are never
replayed, and job retry counts are unaffected by acquiring a connection.

Claims and execution slots remain occupied while waiting. Larger windows delay
shutdown after a persistent acquisition failure. Execution remains at least once.

### Context

Application context and job context have separate roles. Pass shared application resources, such as database pools and configuration, to `storage.runtime(app_context)`.

The `Worker` derive supports unit or empty workers, and workers with exactly one named field of the context type, which receives a clone of the context. For other layouts, implement `Worker<Args>` and `FromContext<C>` manually instead of using the derive.

Each execution also receives a `JobContext` with job metadata and persistent state.

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

#### Queue Selection

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

To run every registered queue except specific ones, call `except_queue` once
for each queue to exclude:

```rust
let runtime = storage
    .runtime(ctx)
    .register::<Components>()
    .except_queue::<LowPriorityQueue>()
    .except_queue::<MaintenanceQueue>();

runtime.run().await?;
```

Excluding a dynamic queue excludes all of its discovered subqueues. Cron jobs
for excluded queues are not scheduled.

`Storage` remains the enqueueing and monitoring handle; `RuntimeBuilder<C>` is the worker setup and execution handle for app context type `C`.

#### Shutdown

The default shutdown listener handles SIGTERM/SIGINT on Unix and Ctrl+C on Windows. Shutdown stops fetching new work and lets workers drain. A shared 180-second deadline covers workers, background tasks, and process deregistration. Use `.shutdown_timeout(duration)` to change it or `.disable_shutdown_timeout()` to wait indefinitely.

At the deadline, unfinished asynchronous tasks are cancelled and interrupted jobs remain eligible for recovery unless resurrection is disabled for their job type. `run()` returns `OxanaError::ShutdownTimeout` after an ordinary shutdown signal, or preserves the initiating error if a runtime failure triggered shutdown.

### Error and Panic Reporting

Library operations return `OxanaError`. Set a worker's associated error type with `#[oxana(error = MyError)]`; otherwise the derive uses `oxana::BoxError`. Custom error types must satisfy `IntoWorkerError + Send + Sync + 'static`; types implementing `std::error::Error + Send + Sync + 'static` meet that requirement automatically.

Worker errors use their `Debug` representation by default so error types that capture backtraces
can include them in the dashboard. Applications can override the stored representation when needed:

```rust
let runtime = storage
    .runtime(ctx)
    .error_formatter(|error| error.to_string());
```

The default `sentry` feature reports returned worker errors. Enable `sentry-panic` to also report worker panics. Panic catching, failure storage, retries, and custom `failure_reporter` callbacks work with either setting and with Sentry disabled.

With the `sentry` feature enabled, returned worker errors are reported once by default with isolated
`oxana.*` tags and an `oxana` context containing job IDs, queue/job/worker names, batch size, and
per-job arguments and retry state. To customize events for returned errors while retaining this
metadata and worker scope, use `sentry_error_event_builder`:

```rust
let runtime = storage.runtime(ctx).sentry_error_event_builder(|error| {
    match error.downcast_ref::<ApplicationError>() {
        Some(error) => error.sentry_event(),
        None => sentry_core::event_from_error(error),
    }
});
```

Return an event without capturing it; Oxana submits it once. The default builder uses
`sentry_core::event_from_error`, which formats errors with `Debug`. Applications avoiding native
backtrace resolution should build events from safe messages and recorded locations, and also use
safe `Debug` output or an `error_formatter` for the dashboard. The event builder runs only for
returned errors when a Sentry client is active.

Applications that need to replace all reporting, including panic reporting, can instead use
`failure_reporter`. It takes precedence over `sentry_error_event_builder`:

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

The callback receives panic failures exactly once per execution (including an entire failed
batch), regardless of Sentry features. `sentry_error_event_builder` never runs for panics.

Applications own their global Sentry panic hook. Oxana neither installs one nor enables the
application's Sentry panic integration. With `sentry`, Oxana suppresses automatic hook events
and other events emitted during unwinding on its isolated worker hub. Without `sentry-panic`,
these events are discarded without retaining a panic-event buffer. With `sentry-panic`, Oxana
retains the first hook event and reports it after the catch, preserving its stacktrace and
breadcrumbs and attaching job/retry metadata with the handled `oxana.worker_panic` mechanism.
If there is no hook event, Oxana builds a fallback event. A custom reporter always replaces
built-in reporting and discards any retained event. Panics outside the worker scope are unaffected.

Cargo features are additive: check `cargo tree -e features -i oxana` in the application to ensure
no dependency enables `oxana/sentry-panic`. `oxana-web` and `oxana-mcp` use Oxana's defaults,
which do not enable panic reporting.

Serialized job arguments are included in failure metadata and default Sentry events. Arguments can
contain sensitive application data; custom reporters receive the metadata without Oxana attaching
it automatically, allowing them to redact or omit arguments before capture.

Backtraces must be enabled before the process starts (for example with `RUST_BACKTRACE=1`) and
captured by the application's error type. Diagnostic output may contain sensitive details, so only
enable backtraces where access to stored job errors is appropriately restricted.

### Prometheus Metrics

Enable the `prometheus` feature to expose metrics:

```rust
let metrics = storage.metrics().await?;
let output = metrics.encode_to_string()?;
// Serve `output` on your metrics endpoint
```

## Comparison with Similar Libraries

These libraries differ in storage, worker APIs, and integrations. The comparison below reflects the linked project documentation checked on 2026-10-29; backend and feature choices can affect availability.

| Library | Storage | Worker model and documented capabilities |
| --- | --- | --- |
| **Oxana 2.5** | Redis | Typed jobs and workers; static and dynamic queues; per-queue throttling; unique jobs; persisted job state and progress; batch processing; `oxana-web` dashboard and read-only MCP tools. |
| **[Apalis 1.0](https://docs.rs/apalis/latest/apalis/)** | Redis, PostgreSQL, SQLite, MySQL, and additional queue backends | Async task handlers with Tower middleware; rate limits; deduplication; resumable jobs and workflows; `apalis-board` web UI. Capabilities vary by backend and crate. |
| **[rusty-sidekiq 0.15](https://docs.rs/crate/rusty-sidekiq/latest)** | Redis | Rust client and workers compatible with Ruby Sidekiq; server middleware; cron; unique jobs with a configurable TTL based on worker, queue, and arguments. |
| **[Fang 0.11](https://docs.rs/fang/0.11.0/fang/)** | PostgreSQL, SQLite, MySQL | Async or threaded workers; scheduled and cron tasks; unique tasks; task-type filtering; configurable retry backoff. |

Oxana fits applications that want Redis queues with built-in queue controls, job checkpoints, and monitoring. Apalis offers backend choice, Tower middleware, and workflow composition. rusty-sidekiq is useful when Rust and Ruby services need to exchange Sidekiq jobs. Fang keeps the queue in a SQL database and supports both async and threaded execution.

## License

Oxana is licensed under the MIT License.
