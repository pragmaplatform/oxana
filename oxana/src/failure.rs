use std::error::Error;
use std::future::Future;
use std::sync::Arc;

use serde::Serialize;

use crate::JobId;

/// The cause of a failed worker execution.
///
/// Returned errors retain their original concrete type. A reporter can use
/// [`Error::downcast_ref`] before passing the error to a type-specific
/// integration such as `sentry-anyhow`.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum WorkerFailure<'a> {
    /// The error returned by the worker.
    Error(&'a (dyn Error + Send + Sync + 'static)),
    /// A panic caught while polling the worker future.
    Panic {
        /// The string panic payload, or a fallback for non-string payloads.
        message: &'a str,
    },
}

impl WorkerFailure<'_> {
    #[cfg(feature = "sentry")]
    fn kind(self) -> &'static str {
        match self {
            Self::Error(_) => "error",
            Self::Panic { .. } => "panic",
        }
    }
}

/// Retry metadata for one job in a failed worker execution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub struct FailedJobMetadata {
    /// The job ID.
    pub job_id: JobId,
    /// The job's serialized arguments.
    pub args: serde_json::Value,
    /// The number of retries already performed before this execution.
    pub retry_count: u32,
    /// The maximum number of retries allowed for this job.
    pub max_retries: u32,
    /// Whether this job will be retried after the failure.
    pub will_retry: bool,
}

/// Execution metadata attached to a worker failure report.
///
/// Batch jobs retain their individual IDs, serialized arguments, and retry
/// states in [`Self::jobs`]. Job arguments can contain sensitive application
/// data; configure a custom reporter when they need to be filtered or omitted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub struct WorkerFailureMetadata {
    /// Metadata for every job affected by this execution failure.
    pub jobs: Vec<FailedJobMetadata>,
    /// The queue key from which the jobs were dequeued.
    pub queue: String,
    /// The registered job type name.
    pub job_name: String,
    /// The concrete worker type name.
    pub worker_name: String,
    /// Whether at least one affected job will be retried.
    pub will_retry: bool,
}

/// A worker failure and its execution metadata.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct WorkerFailureReport<'a> {
    /// The returned error or caught panic.
    pub failure: WorkerFailure<'a>,
    /// Job, worker, queue, batch, and retry metadata for the execution.
    pub metadata: &'a WorkerFailureMetadata,
}

pub(crate) type FailureReporterFn = dyn for<'a> Fn(WorkerFailureReport<'a>) + Send + Sync + 'static;

pub(crate) struct ExecutionSentryHub {
    #[cfg(feature = "sentry")]
    hub: Arc<sentry_core::Hub>,
}

pub(crate) fn execution_sentry_hub() -> ExecutionSentryHub {
    #[cfg(feature = "sentry")]
    {
        let hub = Arc::new(sentry_core::Hub::new_from_top(sentry_core::Hub::current()));
        hub.configure_scope(|scope| {
            // Sentry's panic hook runs before Oxana's catch_unwind completes.
            // Suppress events emitted during that unwind so the caught panic
            // can be reported once, after custom reporter selection and with
            // failure-only metadata applied.
            scope.add_event_processor(|event| {
                if std::thread::panicking() {
                    None
                } else {
                    Some(event)
                }
            });
        });
        ExecutionSentryHub { hub }
    }

    #[cfg(not(feature = "sentry"))]
    {
        ExecutionSentryHub {}
    }
}

pub(crate) fn report_failure(
    reporter: Option<&Arc<FailureReporterFn>>,
    report: WorkerFailureReport<'_>,
    execution_hub: &ExecutionSentryHub,
) {
    if let Some(reporter) = reporter {
        on_execution_sentry_hub(execution_hub, || reporter(report));
        return;
    }

    #[cfg(feature = "sentry")]
    default_sentry_report(report, execution_hub);
}

#[cfg(feature = "sentry")]
fn on_execution_sentry_hub<R>(
    execution_hub: &ExecutionSentryHub,
    callback: impl FnOnce() -> R,
) -> R {
    sentry_core::Hub::run(Arc::clone(&execution_hub.hub), callback)
}

#[cfg(not(feature = "sentry"))]
fn on_execution_sentry_hub<R>(
    _execution_hub: &ExecutionSentryHub,
    callback: impl FnOnce() -> R,
) -> R {
    callback()
}

pub(crate) async fn with_execution_sentry_hub<F>(future: F) -> (F::Output, ExecutionSentryHub)
where
    F: Future,
{
    let execution_hub = execution_sentry_hub();

    #[cfg(feature = "sentry")]
    {
        use sentry_core::SentryFutureExt;

        let output = future.bind_hub(Arc::clone(&execution_hub.hub)).await;
        (output, execution_hub)
    }

    #[cfg(not(feature = "sentry"))]
    {
        (future.await, execution_hub)
    }
}

#[cfg(feature = "sentry")]
fn default_sentry_report(report: WorkerFailureReport<'_>, execution_hub: &ExecutionSentryHub) {
    with_failure_sentry_scope(report, execution_hub, || match report.failure {
        WorkerFailure::Error(error) => {
            sentry_core::capture_error(error);
        }
        WorkerFailure::Panic { message } => {
            use sentry_core::protocol::{Event, Exception, Mechanism};

            sentry_core::capture_event(Event {
                exception: vec![Exception {
                    ty: "panic".to_string(),
                    value: Some(message.to_string()),
                    mechanism: Some(Mechanism {
                        ty: "oxana.worker_panic".to_string(),
                        handled: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]
                .into(),
                level: sentry_core::Level::Error,
                ..Default::default()
            });
        }
    });
}

#[cfg(feature = "sentry")]
fn with_failure_sentry_scope<R>(
    report: WorkerFailureReport<'_>,
    execution_hub: &ExecutionSentryHub,
    callback: impl FnOnce() -> R,
) -> R {
    sentry_core::Hub::run(Arc::clone(&execution_hub.hub), || {
        sentry_core::with_scope(
            |scope| {
                configure_sentry_scope(scope, report.metadata);
                scope.set_tag("oxana.failure_kind", report.failure.kind());
            },
            callback,
        )
    })
}

#[cfg(feature = "sentry")]
fn configure_sentry_scope(scope: &mut sentry_core::Scope, metadata: &WorkerFailureMetadata) {
    scope.set_tag("oxana.queue", &metadata.queue);
    scope.set_tag("oxana.job", &metadata.job_name);
    scope.set_tag("oxana.worker", &metadata.worker_name);
    scope.set_tag("oxana.batch_size", metadata.jobs.len());
    scope.set_tag("oxana.will_retry", metadata.will_retry);

    if let [job] = metadata.jobs.as_slice() {
        scope.set_tag("oxana.job_id", &job.job_id);
        scope.set_tag("oxana.retry_count", job.retry_count);
        scope.set_tag("oxana.max_retries", job.max_retries);
    }

    let context = serde_json::to_value(metadata)
        .expect("worker failure metadata contains only serializable values");
    let context = context
        .as_object()
        .expect("worker failure metadata serializes as an object")
        .clone()
        .into_iter()
        .collect();
    scope.set_context("oxana", sentry_core::protocol::Context::Other(context));
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    #[derive(Debug)]
    struct ConcreteWorkerError;

    impl std::fmt::Display for ConcreteWorkerError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("concrete worker error")
        }
    }

    impl Error for ConcreteWorkerError {}

    fn job(job_id: &str, retry_count: u32, max_retries: u32) -> FailedJobMetadata {
        let will_retry = retry_count < max_retries;
        FailedJobMetadata {
            job_id: job_id.to_string(),
            args: serde_json::json!({ "source": job_id }),
            retry_count,
            max_retries,
            will_retry,
        }
    }

    fn metadata(queue: &str, jobs: Vec<FailedJobMetadata>) -> WorkerFailureMetadata {
        let will_retry = jobs.iter().any(|job| job.will_retry);
        WorkerFailureMetadata {
            jobs,
            queue: queue.to_string(),
            job_name: "test::EmailJob".to_string(),
            worker_name: "test::EmailWorker".to_string(),
            will_retry,
        }
    }

    fn report_failure(reporter: Option<&Arc<FailureReporterFn>>, report: WorkerFailureReport<'_>) {
        let execution_hub = execution_sentry_hub();
        super::report_failure(reporter, report, &execution_hub);
    }

    #[test]
    fn custom_reporter_replaces_default_and_receives_concrete_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let saw_concrete_error = Arc::new(AtomicBool::new(false));
        let saw_arguments = Arc::new(AtomicBool::new(false));
        let calls_for_reporter = Arc::clone(&calls);
        let saw_for_reporter = Arc::clone(&saw_concrete_error);
        let saw_arguments_for_reporter = Arc::clone(&saw_arguments);
        let reporter: Arc<FailureReporterFn> = Arc::new(move |report| {
            calls_for_reporter.fetch_add(1, Ordering::SeqCst);
            saw_arguments_for_reporter.store(
                report.metadata.jobs[0].args == serde_json::json!({ "source": "custom-id" }),
                Ordering::SeqCst,
            );
            if let WorkerFailure::Error(error) = report.failure {
                let saw_concrete = error.downcast_ref::<ConcreteWorkerError>().is_some();
                saw_for_reporter.store(saw_concrete, Ordering::SeqCst);
                #[cfg(feature = "sentry")]
                if saw_concrete {
                    sentry_core::capture_message("custom capture", sentry_core::Level::Warning);
                }
            }
        });
        let error = ConcreteWorkerError;
        let metadata = metadata("custom", vec![job("custom-id", 0, 1)]);

        #[cfg(feature = "sentry")]
        let events = sentry_core::test::with_captured_events(|| {
            report_failure(
                Some(&reporter),
                WorkerFailureReport {
                    failure: WorkerFailure::Error(&error),
                    metadata: &metadata,
                },
            );
        });

        #[cfg(not(feature = "sentry"))]
        report_failure(
            Some(&reporter),
            WorkerFailureReport {
                failure: WorkerFailure::Error(&error),
                metadata: &metadata,
            },
        );

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(saw_concrete_error.load(Ordering::SeqCst));
        assert!(saw_arguments.load(Ordering::SeqCst));
        #[cfg(feature = "sentry")]
        {
            assert_eq!(events.len(), 1, "the default capture must be replaced");
            assert_eq!(events[0].message.as_deref(), Some("custom capture"));
            assert!(!events[0].tags.contains_key("oxana.job_id"));
            assert!(!events[0].contexts.contains_key("oxana"));
        }
    }

    #[cfg(feature = "sentry")]
    fn oxana_context<'a>(
        event: &'a sentry_core::protocol::Event<'_>,
    ) -> &'a std::collections::BTreeMap<String, serde_json::Value> {
        match event.contexts.get("oxana") {
            Some(sentry_core::protocol::Context::Other(context)) => context,
            context => panic!("expected Oxana context, got {context:?}"),
        }
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn returned_error_creates_one_enriched_report() {
        let error = ConcreteWorkerError;
        let metadata = metadata("mailers", vec![job("job-123", 1, 3)]);
        let events = sentry_core::test::with_captured_events(|| {
            report_failure(
                None,
                WorkerFailureReport {
                    failure: WorkerFailure::Error(&error),
                    metadata: &metadata,
                },
            );
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(
            event.tags.get("oxana.queue").map(String::as_str),
            Some("mailers")
        );
        assert_eq!(
            event.tags.get("oxana.job").map(String::as_str),
            Some("test::EmailJob")
        );
        assert_eq!(
            event.tags.get("oxana.worker").map(String::as_str),
            Some("test::EmailWorker")
        );
        assert_eq!(
            event.tags.get("oxana.job_id").map(String::as_str),
            Some("job-123")
        );
        assert_eq!(
            event.tags.get("oxana.retry_count").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            event.tags.get("oxana.max_retries").map(String::as_str),
            Some("3")
        );
        assert_eq!(
            event.tags.get("oxana.batch_size").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            event.tags.get("oxana.will_retry").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            event.tags.get("oxana.failure_kind").map(String::as_str),
            Some("error")
        );

        let context = oxana_context(event);
        assert_eq!(context.get("queue"), Some(&serde_json::json!("mailers")));
        assert_eq!(
            context.get("jobs").and_then(serde_json::Value::as_array),
            Some(&vec![serde_json::json!({
                "job_id": "job-123",
                "args": { "source": "job-123" },
                "retry_count": 1,
                "max_retries": 3,
                "will_retry": true,
            })])
        );
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn final_report_reuses_worker_scope_without_exposing_args_to_worker_events() {
        let error = ConcreteWorkerError;
        let metadata = metadata("mailers", vec![job("job-123", 1, 3)]);
        let events = sentry_core::test::with_captured_events(|| {
            let ((), execution_hub) =
                futures::executor::block_on(with_execution_sentry_hub(async {
                    sentry_core::configure_scope(|scope| {
                        scope.set_tag("worker.context", "preserved");
                        scope.set_user(Some(sentry_core::User {
                            id: Some("worker-user".to_string()),
                            ..Default::default()
                        }));
                    });
                    sentry_core::add_breadcrumb(sentry_core::Breadcrumb {
                        message: Some("worker breadcrumb".to_string()),
                        ..Default::default()
                    });
                    sentry_core::capture_message("worker diagnostic", sentry_core::Level::Info);
                }));

            super::report_failure(
                None,
                WorkerFailureReport {
                    failure: WorkerFailure::Error(&error),
                    metadata: &metadata,
                },
                &execution_hub,
            );
        });

        assert_eq!(events.len(), 2);
        let diagnostic = events
            .iter()
            .find(|event| event.message.as_deref() == Some("worker diagnostic"))
            .expect("worker diagnostic event");
        assert!(!diagnostic.contexts.contains_key("oxana"));
        assert!(!diagnostic.tags.keys().any(|key| key.starts_with("oxana.")));

        let failure = events
            .iter()
            .find(|event| !event.exception.is_empty())
            .expect("worker failure event");
        assert_eq!(failure.tags["worker.context"], "preserved");
        assert_eq!(failure.tags["oxana.job_id"], "job-123");
        assert_eq!(
            failure.user.as_ref().and_then(|user| user.id.as_deref()),
            Some("worker-user")
        );
        assert!(
            failure
                .breadcrumbs
                .iter()
                .any(|breadcrumb| { breadcrumb.message.as_deref() == Some("worker breadcrumb") })
        );
        assert_eq!(
            oxana_context(failure)["jobs"][0]["args"]["source"],
            "job-123"
        );
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn batch_report_contains_per_job_retry_metadata() {
        let error = ConcreteWorkerError;
        let metadata = metadata("batch", vec![job("retrying", 0, 2), job("terminal", 2, 2)]);
        let events = sentry_core::test::with_captured_events(|| {
            report_failure(
                None,
                WorkerFailureReport {
                    failure: WorkerFailure::Error(&error),
                    metadata: &metadata,
                },
            );
        });

        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(
            event.tags.get("oxana.batch_size").map(String::as_str),
            Some("2")
        );
        assert!(!event.tags.contains_key("oxana.retry_count"));
        assert!(!event.tags.contains_key("oxana.max_retries"));
        assert_eq!(
            event.tags.get("oxana.will_retry").map(String::as_str),
            Some("true")
        );
        assert!(!event.tags.contains_key("oxana.job_id"));

        let jobs = oxana_context(event)
            .get("jobs")
            .and_then(serde_json::Value::as_array)
            .expect("batch jobs metadata");
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0]["job_id"], "retrying");
        assert_eq!(jobs[0]["args"], serde_json::json!({ "source": "retrying" }));
        assert_eq!(jobs[0]["will_retry"], true);
        assert_eq!(jobs[1]["job_id"], "terminal");
        assert_eq!(jobs[1]["args"], serde_json::json!({ "source": "terminal" }));
        assert_eq!(jobs[1]["will_retry"], false);
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn retrying_and_non_retrying_executions_are_filterable() {
        let error = ConcreteWorkerError;
        let retrying = metadata("retrying", vec![job("retrying", 0, 1)]);
        let terminal = metadata("terminal", vec![job("terminal", 1, 1)]);
        let mut events = sentry_core::test::with_captured_events(|| {
            for metadata in [&retrying, &terminal] {
                report_failure(
                    None,
                    WorkerFailureReport {
                        failure: WorkerFailure::Error(&error),
                        metadata,
                    },
                );
            }
        });
        events.sort_by(|left, right| left.tags["oxana.queue"].cmp(&right.tags["oxana.queue"]));

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].tags["oxana.queue"], "retrying");
        assert_eq!(events[0].tags["oxana.will_retry"], "true");
        assert_eq!(events[1].tags["oxana.queue"], "terminal");
        assert_eq!(events[1].tags["oxana.will_retry"], "false");
    }

    #[cfg(feature = "sentry")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_reports_do_not_leak_scope_metadata() {
        use sentry_core::SentryFutureExt;

        let transport = sentry_core::test::TestTransport::new();
        let options = sentry_core::ClientOptions::new()
            .dsn("https://public@sentry.invalid/1")
            .transport(Arc::clone(&transport));
        let hub = Arc::new(sentry_core::Hub::new(
            Some(Arc::new(options.into())),
            Arc::new(Default::default()),
        ));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let spawn_report = |queue: &'static str, job_id: &'static str| {
            let barrier = Arc::clone(&barrier);
            let hub = Arc::clone(&hub);
            tokio::spawn(
                async move {
                    let error = ConcreteWorkerError;
                    let metadata = metadata(queue, vec![job(job_id, 0, 1)]);
                    barrier.wait().await;
                    report_failure(
                        None,
                        WorkerFailureReport {
                            failure: WorkerFailure::Error(&error),
                            metadata: &metadata,
                        },
                    );
                }
                .bind_hub(hub),
            )
        };

        let first = spawn_report("queue-a", "job-a");
        let second = spawn_report("queue-b", "job-b");
        first.await.expect("first report task");
        second.await.expect("second report task");

        let mut events = transport.fetch_and_clear_events();
        events.sort_by(|left, right| left.tags["oxana.queue"].cmp(&right.tags["oxana.queue"]));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].tags["oxana.queue"], "queue-a");
        assert_eq!(events[0].tags["oxana.job_id"], "job-a");
        assert_eq!(oxana_context(&events[0])["queue"], "queue-a");
        assert_eq!(events[1].tags["oxana.queue"], "queue-b");
        assert_eq!(events[1].tags["oxana.job_id"], "job-b");
        assert_eq!(oxana_context(&events[1])["queue"], "queue-b");
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn panic_integration_waits_for_custom_reporter_before_capture() {
        use futures::FutureExt;
        use std::panic::AssertUnwindSafe;

        let metadata = metadata("panics", vec![job("panic-id", 0, 0)]);
        let reporter: Arc<FailureReporterFn> = Arc::new(|report| {
            assert!(matches!(report.failure, WorkerFailure::Panic { .. }));
            assert_eq!(report.metadata.jobs[0].args["source"], "panic-id");
            sentry_core::capture_message("redacted panic", sentry_core::Level::Error);
        });
        let options = sentry_core::ClientOptions::new()
            .add_integration(sentry_panic::PanicIntegration::default());
        let events = sentry_core::test::with_captured_events_options(
            || {
                let (result, execution_hub) =
                    futures::executor::block_on(with_execution_sentry_hub(
                        AssertUnwindSafe(async { panic!("worker panicked") }).catch_unwind(),
                    ));
                assert!(result.is_err());
                super::report_failure(
                    Some(&reporter),
                    WorkerFailureReport {
                        failure: WorkerFailure::Panic {
                            message: "worker panicked",
                        },
                        metadata: &metadata,
                    },
                    &execution_hub,
                );
            },
            options,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message.as_deref(), Some("redacted panic"));
        assert!(events[0].exception.is_empty());
        assert!(!events[0].contexts.contains_key("oxana"));
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn panic_without_panic_integration_is_reported_once_by_oxana() {
        let metadata = metadata("panics", vec![job("panic-id", 0, 0)]);
        let events = sentry_core::test::with_captured_events(|| {
            report_failure(
                None,
                WorkerFailureReport {
                    failure: WorkerFailure::Panic {
                        message: "worker panicked",
                    },
                    metadata: &metadata,
                },
            );
        });

        assert_eq!(events.len(), 1);
        assert!(events[0].message.is_none());
        assert_eq!(events[0].exception[0].ty, "panic");
        assert_eq!(
            events[0].exception[0].value.as_deref(),
            Some("worker panicked")
        );
        assert_eq!(
            events[0].exception[0]
                .mechanism
                .as_ref()
                .and_then(|mechanism| mechanism.handled),
            Some(true)
        );
        assert_eq!(events[0].tags["oxana.failure_kind"], "panic");
    }
}
