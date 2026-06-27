# Oxanus

> Deprecation notice: `oxanus` has been renamed to `oxana`. This package line is only kept to direct existing users to `oxana` v2. New projects should depend on `oxana = "2"` and, if they use the dashboard, `oxana-web = "2"`.

## Migration to Oxana v2

Update package names:

```bash
cargo remove oxanus oxanus-web
cargo add oxana@2
cargo add oxana-web@2
```

Then update Rust paths and macro attributes:

- `oxanus::` -> `oxana::`
- `oxanus_web::` -> `oxana_web::`
- `#[oxanus(...)]` -> `#[oxana(...)]`
- `OxanusError` -> `OxanaError`

See the `oxana` v2 crate for current documentation: <https://crates.io/crates/oxana>.

[![Build Status](https://img.shields.io/github/actions/workflow/status/pragmaplatform/oxanus/test.yml?branch=main)](https://github.com/pragmaplatform/oxanus/actions)
[![Latest Version](https://img.shields.io/crates/v/oxanus.svg)](https://crates.io/crates/oxanus)
[![docs.rs](https://img.shields.io/static/v1?label=docs.rs&message=oxanus&color=blue&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K)](https://docs.rs/oxanus/latest)

Oxanus is a Redis-backed job processing library for Rust. It powers the background job infrastructure behind [Player.gg](https://player.gg) and [Firstlook.gg](https://firstlook.gg), serving hundreds of studios and millions of players.

Oxanus focuses on simplicity and depth over breadth - one backend, done well.

<p align="center">
  <picture>
    <img alt="Oxanus Web Dashboard" src="https://raw.githubusercontent.com/pragmaplatform/oxanus/refs/heads/main/web.png">
  </picture>
</p>

## Key Features

- **Isolated Queues** - separate queues with independent concurrency and configuration
- **Retries** - automatic retry with configurable backoff
- **Scheduled Jobs** - run jobs at specific times or after delays
- **Cron Jobs** - periodic jobs using cron expressions
- **Dynamic Queues** - create and manage queues at runtime
- **Throttling** - rate-limit job processing per queue
- **Unique Jobs** - deduplicate jobs so only one instance runs at a time
- **Resumable Jobs** - resume from where a job left off on retry
- **Resilient Jobs** - survive worker crashes and restarts
- **Graceful Shutdown** - clean shutdown with in-progress job handling
- **Web Dashboard** - built-in UI for monitoring jobs, queues, and cron - pure Rust, no JS toolchain
- **Prometheus Metrics** - export queue and job metrics for monitoring
- **Well Tested** - comprehensive integration test suite

## Quick Start

```bash
cargo add oxanus
```

```rust
use oxanus::Storage;
use serde::{Serialize, Deserialize};

#[derive(oxanus::Registry)]
struct ComponentRegistry(oxanus::ComponentRegistry<MyContext, MyError>);

#[derive(Debug, thiserror::Error)]
enum MyError {}

#[derive(Debug, Clone)]
struct MyContext {}

#[derive(Debug, Serialize, Deserialize)]
struct MyJob {
    data: String,
}

#[derive(oxanus::Worker)]
struct MyWorker;

impl MyWorker {
    async fn process(&self, job: &MyJob, _ctx: &oxanus::JobContext) -> Result<(), MyError> {
        println!("Processing: {}", job.data);
        Ok(())
    }
}

#[derive(Serialize, oxanus::Queue)]
#[oxanus(key = "my_queue", concurrency = 2)]
struct MyQueue;

#[tokio::main]
async fn main() -> Result<(), oxanus::OxanusError> {
    let ctx = oxanus::ContextValue::new(MyContext {});
    let storage = Storage::builder().build_from_env()?;
    let config = ComponentRegistry::build_config(&storage)
        .with_graceful_shutdown(tokio::signal::ctrl_c());

    storage.enqueue(MyQueue, MyJob { data: "hello".into() }).await?;
    oxanus::run(config, ctx).await?;
    Ok(())
}
```

For more detailed usage examples, check out the [examples directory](https://github.com/pragmaplatform/oxanus/tree/main/oxanus/examples).

## Web Dashboard

The `oxanus-web` crate provides a built-in dashboard for monitoring jobs, queues, and cron schedules. It integrates as a nested axum router.

```rust
use oxanus_web::OxanusWebState;

let config = ComponentRegistry::build_config(&storage)
    .with_graceful_shutdown(tokio::signal::ctrl_c());

let oxanus_router = oxanus_web::router(OxanusWebState::new(
    config.storage.clone(),
    config.catalog(),
    "/oxanus".to_string(),
));

let app = your_app_router().nest("/oxanus", oxanus_router);
```

The dashboard exposes these pages:

- `/` - Overview with job stats
- `/busy` - Currently processing jobs
- `/queues` - All queues with stats
- `/queues/{queue_key}` - Jobs in a specific queue
- `/cron` - Cron job schedules
- `/scheduled` - Scheduled jobs
- `/retries` - Jobs pending retry
- `/dead` - Dead letter queue

It also provides management actions for wiping queues and deleting individual jobs.

## Core Concepts

### Workers

Workers define the processing logic for jobs. Use the `#[derive(oxanus::Worker)]` macro or implement the `Worker` trait manually.

Worker attributes:

- `#[oxanus(max_retries = 3)]` - Set maximum retry attempts
- `#[oxanus(retry_delay = 5)]` - Set retry delay in seconds
- `#[oxanus(unique_id = "worker_{id}")]` - Define unique job identifiers
- `#[oxanus(on_conflict = Skip)]` - Handle job conflicts (Skip or Replace)
- `#[oxanus(cron(schedule = "*/5 * * * * *", queue = MyQueue))]` - Schedule periodic jobs

### Queues

Queues are the channels through which jobs flow. Use the `#[derive(oxanus::Queue)]` macro or implement the `Queue` trait manually.

Queues can be:

- **Static**: Defined at compile time with a fixed key
- **Dynamic**: Created at runtime with each instance being a separate queue (requires struct fields)

Queue attributes:

- `#[oxanus(key = "my_queue")]` - Set static queue key
- `#[oxanus(prefix = "dynamic")]` - Set prefix for dynamic queues
- `#[oxanus(concurrency = 2)]` - Set concurrency limit
- `#[oxanus(throttle(window_ms = 2000, limit = 5))]` - Configure throttling

### Component Registry

The component registry automatically discovers and registers all workers and queues in your application. Use `#[derive(oxanus::Registry)]` to create a registry and `ComponentRegistry::build_config()` to build the configuration.

### Storage

`Storage` provides the interface for job persistence - enqueueing, scheduling, state management, and queue monitoring.

Build it with `Storage::builder().build_from_env()` which reads the `REDIS_URL` environment variable.

### Context

The context provides shared state and utilities to workers. It can include:

- Database connections
- Configuration
- Shared resources
- Job state (for resumable jobs)

### Configuration

Configuration is done through the `Config` builder, which allows you to:

- Automatically register queues and workers via the component registry
- Set up graceful shutdown
- Configure exit conditions

### Error Handling

Oxanus uses a custom `OxanusError` type that covers all library error cases. Workers can define their own error type that implements `std::error::Error`.

### Prometheus Metrics

Enable the `prometheus` feature to expose metrics:

```rust
let metrics = storage.metrics().await?;
let output = metrics.encode_to_string()?;
// Serve `output` on your metrics endpoint
```

## Comparison with Similar Libraries

| Feature | Oxanus | [Apalis](https://crates.io/crates/apalis) | [rusty-sidekiq](https://crates.io/crates/rusty-sidekiq) | [Fang](https://crates.io/crates/fang) |
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

**Oxanus** focuses on depth with a single Redis backend rather than breadth across multiple backends. It is the only Rust job library offering resumable jobs and combines unique jobs, throttling, and a built-in web dashboard in one package.

**Apalis** offers the most backend options and integrates with the tower middleware ecosystem, making it highly extensible. It suits projects that need backend flexibility or already use tower layers. However, its breadth of abstraction can come at the cost of reliability and debuggability in production.

**rusty-sidekiq** is wire-compatible with Ruby Sidekiq, making it ideal for teams migrating from or coexisting with Ruby services. It can share queues with Ruby Sidekiq workers and use the existing Sidekiq web UI.

**Fang** is SQL-database-backed (no Redis dependency) with both async and threaded execution modes. A good fit for projects that prefer Postgres/SQLite over Redis.
