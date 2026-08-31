use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use crate::job_envelope::JobEnvelope;
use crate::job_state::JobState;
use crate::runtime::Runtime;
use crate::worker::{BoxedProcessable, WorkerError};
use crate::{
    FailedJobMetadata, JobContext, OxanaError, WorkerFailure, WorkerFailureMetadata,
    WorkerFailureReport,
};

#[derive(Debug)]
enum ExecutionResult {
    NotPanic(Result<(), WorkerError>),
    Panic(String),
}

struct ProcessResult {
    result: ExecutionResult,
    sentry_hub: crate::failure::ExecutionSentryHub,
}

pub(crate) enum ExecutionError {
    NotPanic,
    Panic(),
}

pub(crate) struct ExecutionOutcome {
    pub(crate) result: Result<(), ExecutionError>,
    pub(crate) duration_ms: u64,
}

#[derive(Clone, Copy)]
struct ExecutionNames {
    job: &'static str,
    worker: &'static str,
}

pub async fn run<DT>(
    config: Arc<Runtime<DT>>,
    worker: BoxedProcessable,
    envelope: &mut JobEnvelope,
) -> Result<ExecutionOutcome, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    run_batch(config, worker, std::slice::from_mut(envelope)).await
}

pub async fn run_batch<DT>(
    config: Arc<Runtime<DT>>,
    worker: BoxedProcessable,
    envelopes: &mut [JobEnvelope],
) -> Result<ExecutionOutcome, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    if envelopes.is_empty() {
        return Ok(ExecutionOutcome {
            result: Ok(()),
            duration_ms: 0,
        });
    }

    if worker.len() != envelopes.len() {
        return Err(OxanaError::GenericError(format!(
            "Batch worker has {} jobs but received {} envelopes",
            worker.len(),
            envelopes.len()
        )));
    }

    let policies: Vec<JobExecutionPolicy> = envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| execution_policy(&worker, index, envelope))
        .collect();

    if !worker.should_resume() {
        for envelope in envelopes.iter_mut() {
            envelope.meta.state = None;
        }
    }

    config
        .storage
        .internal
        .set_started_at_batch(envelopes)
        .await?;

    let first_envelope = envelopes
        .first()
        .expect("envelopes is not empty because it was checked above");
    let queue = first_envelope.queue.clone();
    let names = ExecutionNames {
        job: worker.job_name(),
        worker: worker.worker_name(),
    };
    if envelopes.len() == 1 {
        tracing::info!(
            job_id = first_envelope.id,
            queue = queue,
            job = names.job,
            worker = names.worker,
            latency_ms = first_envelope.meta.latency_millis(),
            "Job started"
        );
    } else {
        tracing::info!(
            batch_size = envelopes.len(),
            queue = queue,
            job = names.job,
            worker = names.worker,
            "Job batch started"
        );
    }
    let start = std::time::Instant::now();
    let job_contexts = job_contexts(&config.storage, envelopes);

    let process_result = run_process(worker, job_contexts, first_envelope).await;

    let duration = start.elapsed();
    let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    let is_err = !matches!(process_result.result, ExecutionResult::NotPanic(Ok(_)));
    if envelopes.len() == 1 {
        tracing::info!(
            job_id = first_envelope.id,
            queue = queue,
            job = names.job,
            worker = names.worker,
            success = !is_err,
            duration = duration_ms,
            retries = first_envelope.meta.retries,
            "Job finished"
        );
    } else {
        tracing::info!(
            batch_size = envelopes.len(),
            queue = queue,
            job = names.job,
            worker = names.worker,
            success = !is_err,
            duration = duration_ms,
            "Job batch finished"
        );
    }

    let result =
        finish_batch_result(config.as_ref(), process_result, envelopes, &policies, names).await;
    Ok(ExecutionOutcome {
        result,
        duration_ms,
    })
}

struct JobExecutionPolicy {
    max_retries: u32,
    retry_delay: u64,
}

fn execution_policy(
    worker: &BoxedProcessable,
    index: usize,
    envelope: &JobEnvelope,
) -> JobExecutionPolicy {
    JobExecutionPolicy {
        max_retries: worker.max_retries(index),
        retry_delay: worker.retry_delay(index, envelope.meta.retries),
    }
}

async fn run_process(
    worker: BoxedProcessable,
    job_contexts: Vec<JobContext>,
    envelope: &JobEnvelope,
) -> ProcessResult {
    let future = AssertUnwindSafe(process(worker, job_contexts, envelope)).catch_unwind();
    let (result, sentry_hub) = crate::failure::with_execution_sentry_hub(future).await;
    let result = match result {
        Ok(result) => ExecutionResult::NotPanic(result),
        Err(panic) => ExecutionResult::Panic(panic_message(panic)),
    };
    ProcessResult { result, sentry_hub }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic occurred".to_string()
    }
}

async fn finish_batch_result<DT>(
    config: &Runtime<DT>,
    process_result: ProcessResult,
    envelopes: &[JobEnvelope],
    policies: &[JobExecutionPolicy],
    names: ExecutionNames,
) -> Result<(), ExecutionError>
where
    DT: Send + Sync + Clone + 'static,
{
    let ProcessResult { result, sentry_hub } = process_result;
    match result {
        ExecutionResult::NotPanic(Ok(())) => {
            if let Err(e) = config
                .storage
                .internal
                .finish_with_success_batch(envelopes)
                .await
            {
                tracing::error!("Failed to finish job batch: {}", e);
            }
            Ok(())
        }
        ExecutionResult::NotPanic(Err(e)) => {
            let failure_metadata = failure_metadata(envelopes, policies, names);
            config.settings.report_failure(
                WorkerFailureReport {
                    failure: WorkerFailure::Error(e.as_ref()),
                    metadata: &failure_metadata,
                },
                &sentry_hub,
            );

            if let Some(envelope) = envelopes.first() {
                if envelopes.len() == 1 {
                    tracing::error!(
                        job_id = envelope.id,
                        queue = envelope.queue,
                        job = names.job,
                        worker = names.worker,
                        "Job failed"
                    );
                } else {
                    tracing::error!(
                        batch_size = envelopes.len(),
                        queue = envelope.queue,
                        job = names.job,
                        worker = names.worker,
                        "Job batch failed"
                    );
                }
            }

            let err_msg = config.settings.format_error(e.as_ref());
            for (envelope, policy) in envelopes.iter().zip(policies.iter()) {
                handle_err(
                    config,
                    &err_msg,
                    envelope,
                    retry_delay(config, e.as_ref(), envelope, policy),
                    policy.max_retries,
                )
                .await;
            }

            Err(ExecutionError::NotPanic)
        }
        ExecutionResult::Panic(panic_msg) => {
            let failure_metadata = failure_metadata(envelopes, policies, names);
            config.settings.report_failure(
                WorkerFailureReport {
                    failure: WorkerFailure::Panic {
                        message: &panic_msg,
                    },
                    metadata: &failure_metadata,
                },
                &sentry_hub,
            );

            for (envelope, policy) in envelopes.iter().zip(policies.iter()) {
                handle_err(
                    config,
                    &panic_msg,
                    envelope,
                    policy.retry_delay,
                    policy.max_retries,
                )
                .await;
            }

            Err(ExecutionError::Panic())
        }
    }
}

fn failure_metadata(
    envelopes: &[JobEnvelope],
    policies: &[JobExecutionPolicy],
    names: ExecutionNames,
) -> WorkerFailureMetadata {
    let jobs = envelopes
        .iter()
        .zip(policies)
        .map(|(envelope, policy)| {
            let will_retry = envelope.meta.retries < policy.max_retries;
            FailedJobMetadata {
                job_id: envelope.id.clone(),
                args: envelope.job.args.clone(),
                retry_count: envelope.meta.retries,
                max_retries: policy.max_retries,
                will_retry,
                terminal: !will_retry,
            }
        })
        .collect::<Vec<_>>();
    let will_retry = jobs.iter().any(|job| job.will_retry);

    WorkerFailureMetadata {
        batch_size: jobs.len(),
        jobs,
        queue: envelopes
            .first()
            .map_or_else(String::new, |envelope| envelope.queue.clone()),
        job_name: names.job.to_string(),
        worker_name: names.worker.to_string(),
        will_retry,
        terminal: !will_retry,
    }
}

fn retry_delay<DT>(
    config: &Runtime<DT>,
    error: &(dyn std::error::Error + Send + Sync + 'static),
    envelope: &JobEnvelope,
    policy: &JobExecutionPolicy,
) -> u64 {
    config
        .settings
        .retry_delay_override
        .as_ref()
        .and_then(|f| f(error, envelope.meta.retries, policy.retry_delay))
        .unwrap_or(policy.retry_delay)
}

#[cfg_attr(feature = "tracing-instrument", tracing::instrument(skip_all, name = "job", fields(
    job_id = envelope.id,
    queue = envelope.queue,
    job = worker.job_name(),
    worker = worker.worker_name(),
    args = %envelope.job.args,
    retries = envelope.meta.retries,
    latency_ms = envelope.meta.latency_millis(),
    success = false,
)))]
async fn process(
    worker: BoxedProcessable,
    job_contexts: Vec<JobContext>,
    #[cfg_attr(not(feature = "tracing-instrument"), allow(unused_variables))]
    envelope: &JobEnvelope,
) -> Result<(), WorkerError> {
    #[cfg(feature = "tracing-instrument")]
    let span = tracing::Span::current();

    let result = worker.process(job_contexts).await;

    #[cfg(feature = "tracing-instrument")]
    span.record("success", result.is_ok());

    result
}

fn job_context(storage: &crate::Storage, envelope: &JobEnvelope) -> JobContext {
    JobContext {
        meta: envelope.meta.clone(),
        state: JobState::new(
            storage.clone(),
            envelope.id.clone(),
            envelope.meta.state.clone(),
        ),
    }
}

fn job_contexts(storage: &crate::Storage, envelopes: &[JobEnvelope]) -> Vec<JobContext> {
    envelopes
        .iter()
        .map(|envelope| job_context(storage, envelope))
        .collect()
}
async fn handle_err<DT>(
    config: &Runtime<DT>,
    err_msg: &str,
    envelope: &JobEnvelope,
    retry_delay: u64,
    max_retries: u32,
) where
    DT: Send + Sync + Clone + 'static,
{
    if envelope.meta.retries < max_retries {
        if let Err(e) = config.storage.internal.finish_with_failure(envelope).await {
            tracing::error!("Failed to finish job: {}", e);
        }
        if let Err(e) = config
            .storage
            .internal
            .retry_in(envelope.id.clone(), retry_delay, err_msg.to_string())
            .await
        {
            tracing::error!("Failed to retry job: {}", e);
        }
    } else {
        tracing::error!(
            "Job {} failed after {} retries: {}",
            envelope.id,
            max_retries,
            err_msg
        );
        if let Err(e) = config
            .storage
            .internal
            .kill(envelope, err_msg.to_string())
            .await
        {
            tracing::error!("Failed to kill job: {}", e);
        }
    }
}
