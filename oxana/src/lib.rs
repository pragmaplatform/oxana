#![doc = include_str!("../README.md")]
#![deny(unsafe_code)]
#![warn(
    clippy::all,
    clippy::await_holding_lock,
    clippy::char_lit_as_u8,
    clippy::checked_conversions,
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::debug_assert_with_mut_call,
    clippy::doc_markdown,
    clippy::empty_enums,
    clippy::enum_glob_use,
    clippy::exit,
    clippy::expl_impl_clone_on_copy,
    clippy::explicit_deref_methods,
    clippy::explicit_into_iter_loop,
    clippy::fallible_impl_from,
    clippy::filter_map_next,
    clippy::flat_map_option,
    clippy::float_cmp_const,
    clippy::fn_params_excessive_bools,
    clippy::if_let_mutex,
    clippy::implicit_clone,
    clippy::imprecise_flops,
    clippy::indexing_slicing,
    clippy::inefficient_to_string,
    clippy::invalid_upcast_comparisons,
    clippy::large_digit_groups,
    clippy::large_stack_arrays,
    clippy::large_types_passed_by_value,
    clippy::let_unit_value,
    clippy::linkedlist,
    clippy::lossy_float_literal,
    clippy::macro_use_imports,
    clippy::manual_ok_or,
    clippy::map_err_ignore,
    clippy::map_flatten,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::match_wild_err_arm,
    clippy::match_wildcard_for_single_variants,
    clippy::mem_forget,
    clippy::missing_enforced_import_renames,
    clippy::mut_mut,
    clippy::mutex_integer,
    clippy::needless_borrow,
    clippy::needless_continue,
    clippy::needless_for_each,
    clippy::option_option,
    clippy::path_buf_push_overwrite,
    clippy::ptr_as_ptr,
    clippy::rc_mutex,
    clippy::ref_option_ref,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::same_functions_in_if_condition,
    clippy::semicolon_if_nothing_returned,
    clippy::single_match_else,
    clippy::string_add_assign,
    clippy::string_add,
    clippy::string_lit_as_bytes,
    clippy::todo,
    clippy::trait_duplication_in_bounds,
    clippy::unimplemented,
    clippy::unnested_or_patterns,
    clippy::unused_self,
    clippy::useless_transmute,
    clippy::verbose_file_reads,
    clippy::zero_sized_map_values,
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    unexpected_cfgs
)]
#![allow(clippy::unused_self, clippy::single_match_else, clippy::todo)]

mod config;
mod context;
mod coordinator;
mod dispatcher;
mod drainer;
mod error;
mod executor;
mod failure;
mod job_envelope;
mod job_state;
mod launcher;
mod metrics;
mod queue;
mod result_collector;
mod runtime;
mod semaphores_map;
mod stats;
mod storage;
mod storage_builder;
mod storage_internal;
mod storage_keys;
mod storage_types;
mod throttler;
mod worker;
mod worker_event;
mod worker_registry;

#[cfg(feature = "registry")]
mod registry;

#[cfg(feature = "prometheus")]
pub mod prometheus;

#[cfg(test)]
mod test_helper;

pub use crate::context::JobContext;
pub use crate::drainer::DrainStats;
pub use crate::error::OxanaError;
pub use crate::failure::{
    FailedJobMetadata, WorkerFailure, WorkerFailureMetadata, WorkerFailureReport,
};
pub use crate::job_envelope::{
    JobConflictStrategy, JobData, JobEnvelope, JobId, JobMeta, UniqueJobId,
};
pub use crate::job_state::{JobProgress, JobProgressIterator, JobState};
pub use crate::metrics::*;
pub use crate::queue::{
    Queue, QueueConcurrency, QueueConfig, QueueKind, QueueRuntimeConfig, QueueState, QueueThrottle,
    value_to_queue_key,
};
pub use crate::result_collector::Stats as RunStats;
pub use crate::runtime::RuntimeBuilder;
pub use crate::stats::*;
pub use crate::storage::Storage;
pub use crate::storage_builder::{StorageBuilder, StorageBuilderTimeouts};
pub use crate::storage_types::*;
pub use crate::worker::{
    BatchItem, BoxError, FromContext, IntoWorkerError, Job, Worker, WorkerBatchConfig,
};
pub use crate::worker_registry::{
    OnDemandJobRegistration, WorkerConfig, WorkerConfigKind, job_batch_factory,
    job_envelope_factory, job_factory,
};

#[cfg(feature = "registry")]
pub use registry::*;

#[cfg(feature = "macros")]
pub use oxana_macros::{Job, Queue, Registry, Worker};
