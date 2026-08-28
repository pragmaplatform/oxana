mod job;
mod queue;
mod registry;
mod worker;
mod worker_group;

use job::*;
use queue::*;
use registry::*;
use worker::*;
use worker_group::*;

use proc_macro::TokenStream;
use proc_macro_error2::proc_macro_error;
use syn::{DeriveInput, parse_macro_input};

/// Generates impl for `oxana::Queue`.
///
/// Example usage:
/// ```ignore
/// #[derive(Serialize, oxana::Queue)]
/// #[oxana(key = "my_queue")]
/// #[oxana(concurrency = 2)]
/// #[oxana(throttle(window_ms = 3, limit = 4))]
/// pub struct MyQueue;
/// ```
#[proc_macro_error]
#[proc_macro_derive(Queue, attributes(oxana))]
pub fn derive_queue(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    expand_derive_queue(input).into()
}

/// Generates impl for `oxana::Job`.
///
/// Example usage:
/// ```ignore
/// #[derive(Serialize, oxana::Job)]
/// #[oxana(on_conflict = Replace)]
/// #[oxana(unique_id = "foo_{id}")]
/// #[oxana(resume = false)]
/// #[oxana(throttle_cost = Self::throttle_cost)]
/// struct TestJob {
///     id: i32,
///     cost: u64,
/// }
///
/// impl TestJob {
///     fn throttle_cost(&self) -> Option<u64> {
///         Some(self.cost)
///     }
/// }
///
/// #[derive(oxana::Worker)]
/// struct TestWorker;
/// ```
#[proc_macro_error]
#[proc_macro_derive(Job, attributes(oxana))]
pub fn derive_job(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    expand_derive_job(input).into()
}

/// Generates impl for `oxana::Worker`.
///
/// Example usage:
/// ```ignore
/// #[derive(oxana::Worker)]
/// #[oxana(max_retries = 3)]
/// #[oxana(batch_size = 10, batch_timeout_ms = 500)]
/// struct TestWorkerUniqueId;
/// ```
#[proc_macro_error]
#[proc_macro_derive(Worker, attributes(oxana))]
pub fn derive_worker(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    expand_derive_worker(input).into()
}

/// Generates an implementation of the `oxana::WorkerGroup` marker trait.
///
/// Worker groups must be unit structs.
#[proc_macro_error]
#[proc_macro_derive(WorkerGroup)]
pub fn derive_worker_group(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    expand_derive_worker_group(input).into()
}

/// Helper to define a component registry.
#[proc_macro_error]
#[proc_macro_derive(Registry, attributes(oxana))]
pub fn derive_registry(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    expand_derive_registry(input).into()
}
