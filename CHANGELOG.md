# Changelog

All notable changes to this project will be documented in this file.

## [2.0.0]

### Deprecated

- Deprecate the `oxanus`, `oxanus-macros`, and `oxanus-web` crates. The project has been renamed to `oxana`; migrate to `oxana = "2"` and `oxana-web = "2"`.
- Add Cargo build warnings so existing `oxanus` users see the migration notice during builds without changing runtime behavior.

## [1.1.1]

### Fixed

- Clamp scheduled jobs to the current time when a past timestamp is provided.

## [1.1.0]

### Added

- Scheduled jobs example

### Changed

- Update dependencies, including `deadpool-redis` to 0.23, and adjust Redis connection response timeout handling for blocking commands

### Fixed

- Store `scheduled_at` in job metadata when scheduling jobs with `enqueue_at`

## [1.0.5]

### Added

- Per-queue stats API: `stats_queues()` and `stats_queues_for(patterns)` for fetching queue-level statistics independently

## [1.0.4]

### Added

- BSD support for shutdown signal handling ([#41](https://github.com/pragmaplatform/oxanus/issues/41))

## [1.0.3]

### Added

- Global retry delay override in Config

## [1.0.2]

### Improved

- Use LINDEX instead of LRANGE for single-element list access

## [1.0.1]

### Fixed

- Fix duplicate job scheduling across concurrent instances

## [1.0.0]

### Added

- Public release

## [0.10.0]

**Breaking release.** See [MIGRATION.md](MIGRATION.md) for the upgrade guide.

### Added

- **Job/Worker separation**: job data (`Job` trait) is now separate from processing logic (`Worker<Args>` trait)
- `FromContext` trait for injecting app state into workers (auto-derived for unit and single-field structs)
- `JobContext` replaces generic `Context<T>`
- `ContextValue::new(x)` replaces `Context::value(x)`
- `#[oxanus(job = Type)]` attribute with `{Name}Job` convention default (strips `Worker` suffix)

### Changed

- `#[derive(oxanus::Worker)]` now generates both `Job` and `Worker<Args>` impls
- `config.register_worker::<W, J>()` replaces `config.register_worker::<W>()`
- `storage.enqueue(queue, job)` now takes the job struct, not the worker struct
- Cron `queue` attribute is now required at compile time (was runtime panic)
- `job_envelope::Job` struct renamed to `JobData`
- `Processable`/`BoxedProcessable` removed from public API

### Removed

- `Context<T>` generic context wrapper
- `register_cron_worker` (use `register_worker` with cron attributes)
- `test_helper.rs` (replaced by inline test utilities)

## [0.9.7]

### Added

- Add stat cards to queues tab

### Fixed

- Retry cron job enqueue on transient Redis failure
- Handle transient Redis errors without full shutdown

## [0.9.6]

### Fixed

- Show stats tiles on dynamic sub-queue detail pages

## [0.9.5]

### Added

- Make queue name clickable in job cards linking to queue detail page
- Show dynamic child queues in Active Queues sections on dashboard and busy pages

## [0.9.4]

### Added

- Queue stats tiles to queue detail page
- Add latency tile to overview dashboard

## [0.9.3]

### Added

- Truncate long errors and add Copy Error Info button in web dashboard

### Fixed

- Fix panic instrumentation: trace was missing `success` value instead of recording `false`

## [0.9.2]

### Added

- Web UI dashboard (`oxanus-web` crate) for monitoring jobs, queues, and cron
- Revive button to dead jobs dashboard
- Link enqueued stat box to queues tab in web dashboard

### Changed

- Eliminate redundant Redis fetch in job execution hot path
- Deduplicate relative time filter and pre-compute concurrency map

## [0.9.1]

### Fixed

- Fix queue latency calculation by falling back to `created_at` when `scheduled_at` is zero

## [0.9.0]

### Added

- Throttling: support custom cost per job
- Throttling: skip Redis calls when throttle cost is zero
- Store error message on dead and retried jobs
- `JobMeta`: don't serialize None fields

### Changed

- Relax sentry version requirement
- Update dependencies

## [0.8.20]

### Added

- Conductor workspace setup and archive scripts

## [0.8.19]

### Added

- Failed count to global stats

### Changed

- Tweak dead trimming

## [0.8.18]

### Fixed

- Fix resurrect orphaning (with regression test)

## [0.8.17]

### Added

- Track `started_at` for a job

## [0.8.16]

### Added

- Expose `delete_job`
- Error when updating state of non-existing job

## [0.8.15]

### Added

- Add resurrect flag

## [0.8.14]

### Changed

- Rename catalog function

## [0.8.13]

### Added

- Include queues in catalog

## [0.8.12]

### Added

- Workers catalog

## [0.8.11]

### Added

- `list_scheduled` function

## [0.8.10]

### Fixed

- Fix `list_dead` and `list_retries`

## [0.8.9]

### Added

- `list_dead` and `list_retries` functions

## [0.8.8]

### Added

- Export `JobEnvelope`

## [0.8.7]

### Added

- `list_queue_jobs` and `wipe_queue` functions

## [0.8.6]

### Added

- Export stats types

## [0.8.4]

### Added

- `JobMeta`: add `created_at`/`scheduled_at` functions returning datetime

### Changed

- Revert back to deadpool-redis 0.22

## [0.8.2]

### Added

- Export `JobState` and `JobMeta`

### Changed

- Update dependencies

## [0.8.1]

### Added

- Prometheus metrics

### Fixed

- Fix reported latency

## [0.8.0]

### Added

- Worker derive macro
- Queue derive macro
- Component registry with macros
- Allow unique id to be `None`
- Allow register cron worker with schedule override
- `has_registered_cron_worker` function
- `max_retries` support custom function

### Changed

- Change Cron worker API and update examples
- Namespace `unique_id` with job name
- Restore `register_cron_worker` API
- Update dependencies

## [0.7.0]

### Added

- Report process' `started_at`

## [0.6.3]

### Changed

- Update dependencies

## [0.6.2]

### Changed

- Upgrade dependencies (Redis to 1.0)

## [0.6.0]

### Changed

- Rework storage builder

## [0.5.2]

### Added

- Implement on-conflict strategy for unique jobs

## [0.5.1]

### Changed

- Enqueue immediately with `enqueue_at` when time is lower than now

## [0.5.0]

### Added

- Expose `enqueue_at` in public interface

### Fixed

- Fix cron/unique jobs not being retried

## [0.4.0]

### Added

- Optimize connection management
- Optimize `enqueue_scheduled` with pipeline

### Changed

- Switch time types from `u64` to `i64`
- Improve tracing
- Change age to latency and add `scheduled_at`

### Fixed

- Fix cleanup timestamp check

## [0.3.x]

### Added

- Implement shutdown timeout
- Add cron job validation
- Firehose (optional, disabled by default)
- Tracing instrument feature
- Multi-crate workspace structure (`oxanus` + `oxanus-api`)
- Latency max to global stats
- Global stats enhancements
- Process reporting and stats expansion
- Basic instrumentation
- Resumable jobs via job state
- `from_env_var` for storage builder
- `latency_ms` to storage
- Record basic global stats for all queues
- Add drain

### Changed

- Improve error handling
- Improve storage builder
- Reliability improvements
- Rework stats with grouping of dynamic queues

### Fixed

- Fix handling of critical failure
- Fix double in dynamic queue key
- Account for job not existing anymore when retrying
- Fix performance regression
- Fix key prefixes
- Fix windows compatibility
- Fix macos compatibility

## [0.2.0]

### Added

- Support for cron jobs
- Namespace storage support
- Redis pool (deadpool-redis)
- Dynamic queues support
- Sentry integration (minimal)
- Graceful panic handling
- Dead queue trimming

### Changed

- Rename `WorkerContext` to `Context`
- Clean up public API
- Accept any future as shutdown signal

## [0.1.0]

### Added

- Initial release
- Job processing with Redis backend
- Scheduling and retrying
- Throttling
- Unique jobs
- Job expiration
- Resilient jobs
- Graceful shutdown
- Dynamic queues
- Concurrency control per queue
