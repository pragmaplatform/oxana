# AGENTS

This file provides guidance to any coding agent (Claude, ChatGPT, etc.) when working with code in this repository.

## Development Commands

### Building and Testing

- `cargo build` - Build the project
- `cargo test` - Run all tests (unit and integration tests)
- `cargo test <test_name>` - Run specific tests by name
- `cargo bench` - Run benchmarks (uses divan benchmarking framework)
- `cargo check --all` - Quick syntax and type checking without building
- `cargo fmt --all` - Format the codebase (`cargo fmt --all -- --check` to verify only)
- `cargo clippy --all-features --workspace` - Run Rust linter with extensive warnings enabled
- `cargo doc` - Generate documentation

### Package Structure

This is a Rust workspace with three crates:

- `oxana/` - Main job processing library
- `oxana-macros/` - Proc macros for Oxana
- `oxana-web/` - Web UI dashboard for monitoring jobs, queues, and cron

## Architecture Overview

Oxana is a job processing library built around several core components:

### Core Components

- **Storage**: Main interface for job management, handles enqueueing, scheduling, and monitoring
- **Config**: Configuration builder that registers queues and workers, manages graceful shutdown
- **Context**: Provides shared state and utilities to workers
- **Worker**: Trait defining job processing logic
- **Queue**: Channels through which jobs flow (static or dynamic)
- **JobEnvelope**: Wrapper containing job data and metadata
- **Coordinator**: Orchestrates job processing across queues
- **Dispatcher**: Routes jobs to appropriate workers
- **Executor**: Handles actual job execution

### Key Design Patterns

- Uses Redis as the backing store (via deadpool-redis)
- Graceful shutdown handling with signal management (SIGTERM/SIGINT on Unix, Ctrl+C on Windows)
- Comprehensive error handling with custom `OxanaError` type
- Extensive Clippy linting rules enforced for code quality
- Supports both static queues (compile-time) and dynamic queues (runtime)
- Job uniqueness, throttling, retries, and scheduling capabilities

### Multi-crate Structure

The workspace is organized as:

- Root `Cargo.toml` defines workspace members and shared package metadata
- `oxana/` contains the main library implementation
- `oxana-macros/` contains proc macros
- `oxana-web/` contains the web UI dashboard (uses askama templates + axum)

### Testing

- Unit tests are co-located with source code
- Integration tests are in `oxana/tests/integration/`
- Test utilities are in `test_helper.rs`
- Uses `testresult` crate for test error handling
- Benchmarks use the divan framework in `oxana/benches/`

### Examples

Comprehensive examples are provided in `oxana/examples/` covering:

- Basic usage (`minimal.rs`)
- Cron scheduling (`cron.rs`)
- Dynamic queues (`dynamic.rs`)
- Throttling (`throttled.rs`)
- Unique jobs (`unique.rs`)
- Resumable jobs (`resumable.rs`)

## Skill routing

When the user's request matches an available skill, invoke it via the Skill tool. When in doubt, invoke the skill.

Key routing rules:
- Product ideas/brainstorming → invoke /office-hours
- Strategy/scope → invoke /plan-ceo-review
- Architecture → invoke /plan-eng-review
- Design system/plan review → invoke /design-consultation or /plan-design-review
- Full review pipeline → invoke /autoplan
- Bugs/errors → invoke /investigate
- QA/testing site behavior → invoke /qa or /qa-only
- Code review/diff check → invoke /review
- Visual polish → invoke /design-review
- Ship/deploy/PR → invoke /ship or /land-and-deploy
- Save progress → invoke /context-save
- Resume context → invoke /context-restore
