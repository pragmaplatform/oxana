use std::error::Error;
use std::future::Future;
#[cfg(any(feature = "sentry", test))]
use std::sync::Arc;
#[cfg(feature = "sentry-panic")]
use std::sync::Mutex;

use serde::Serialize;

use crate::JobId;
use crate::config::RuntimeSettings;

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
    /// Whether this failure is terminal for this job.
    pub terminal: bool,
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
    /// The number of jobs processed by the failed execution.
    pub batch_size: usize,
    /// Whether at least one affected job will be retried.
    pub will_retry: bool,
    /// Whether the failure is terminal for every affected job.
    pub terminal: bool,
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
    #[cfg(feature = "sentry-panic")]
    panic_event: Arc<Mutex<Option<sentry_core::protocol::Event<'static>>>>,
}

pub(crate) fn execution_sentry_hub() -> ExecutionSentryHub {
    #[cfg(feature = "sentry")]
    {
        let hub = Arc::new(sentry_core::Hub::new_from_top(sentry_core::Hub::current()));
        #[cfg(feature = "sentry-panic")]
        let panic_event = Arc::new(Mutex::new(None));
        #[cfg(feature = "sentry-panic")]
        let panic_event_for_processor = Arc::clone(&panic_event);
        hub.configure_scope(|scope| {
            // Sentry's panic hook runs before Oxana's catch_unwind completes.
            // Suppress events during unwinding until reporter selection,
            // including captures from destructors that could duplicate the
            // panic hook event. Only retain that first hook event (and its
            // stacktrace) when built-in panic reporting is enabled.
            scope.add_event_processor(move |event| {
                if std::thread::panicking() {
                    #[cfg(feature = "sentry-panic")]
                    if is_panic_integration_event(&event) {
                        let mut panic_event = panic_event_for_processor
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if panic_event.is_none() {
                            *panic_event = Some(event);
                        }
                    }
                    None
                } else {
                    Some(event)
                }
            });
        });
        ExecutionSentryHub {
            hub,
            #[cfg(feature = "sentry-panic")]
            panic_event,
        }
    }

    #[cfg(not(feature = "sentry"))]
    {
        ExecutionSentryHub {}
    }
}

#[cfg(feature = "sentry-panic")]
fn is_panic_integration_event(event: &sentry_core::protocol::Event<'_>) -> bool {
    event.exception.iter().any(|exception| {
        exception
            .mechanism
            .as_ref()
            .is_some_and(|mechanism| mechanism.ty == "panic")
    })
}

#[cfg(feature = "sentry-panic")]
impl ExecutionSentryHub {
    fn take_panic_event(&self) -> Option<sentry_core::protocol::Event<'static>> {
        self.panic_event
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

pub(crate) fn report_failure(
    settings: &RuntimeSettings,
    report: WorkerFailureReport<'_>,
    execution_hub: &ExecutionSentryHub,
) {
    if let Some(reporter) = &settings.failure_reporter {
        on_execution_sentry_hub(execution_hub, || reporter(report));
        return;
    }

    #[cfg(feature = "sentry")]
    default_sentry_report(settings, report, execution_hub);
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
fn default_sentry_report(
    settings: &RuntimeSettings,
    report: WorkerFailureReport<'_>,
    execution_hub: &ExecutionSentryHub,
) {
    match report.failure {
        WorkerFailure::Error(error) => with_failure_sentry_scope(report, execution_hub, || {
            sentry_core::Hub::with_active(|hub| {
                let event = settings.sentry_error_event_builder.as_ref().map_or_else(
                    || sentry_core::event_from_error(error),
                    |builder| builder(error),
                );
                hub.capture_event(event);
            });
        }),
        #[cfg(feature = "sentry-panic")]
        WorkerFailure::Panic { message } => {
            if let Some(event) = execution_hub.take_panic_event() {
                capture_panic_event(report, execution_hub, mark_panic_event_handled(event));
            } else {
                with_failure_sentry_scope(report, execution_hub, || {
                    sentry_core::capture_event(fallback_panic_event(message));
                });
            }
        }
        #[cfg(not(feature = "sentry-panic"))]
        WorkerFailure::Panic { .. } => {}
    }
}

#[cfg(feature = "sentry-panic")]
fn mark_panic_event_handled(
    mut event: sentry_core::protocol::Event<'static>,
) -> sentry_core::protocol::Event<'static> {
    if let Some(exception) = event.exception.first_mut() {
        let mechanism = exception
            .mechanism
            .get_or_insert_with(sentry_core::protocol::Mechanism::default);
        mechanism.ty = "oxana.worker_panic".to_string();
        mechanism.handled = Some(true);
    }
    event.level = sentry_core::Level::Error;
    event
}

#[cfg(feature = "sentry-panic")]
fn fallback_panic_event(message: &str) -> sentry_core::protocol::Event<'static> {
    use sentry_core::protocol::{Event, Exception, Mechanism};

    Event {
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
    }
}

#[cfg(feature = "sentry-panic")]
fn capture_panic_event(
    report: WorkerFailureReport<'_>,
    execution_hub: &ExecutionSentryHub,
    event: sentry_core::protocol::Event<'static>,
) {
    // The retained event already contains the execution scope that Sentry
    // applied before our processor intercepted it. Capture it through a fresh
    // scope on the same client to avoid duplicating worker breadcrumbs while
    // adding failure-only metadata.
    let reporting_hub = Arc::new(sentry_core::Hub::new(
        execution_hub.hub.client(),
        Arc::new(sentry_core::Scope::default()),
    ));
    with_failure_sentry_scope_on_hub(report, reporting_hub, || {
        sentry_core::capture_event(event);
    });
}

#[cfg(feature = "sentry")]
fn with_failure_sentry_scope<R>(
    report: WorkerFailureReport<'_>,
    execution_hub: &ExecutionSentryHub,
    callback: impl FnOnce() -> R,
) -> R {
    with_failure_sentry_scope_on_hub(report, Arc::clone(&execution_hub.hub), callback)
}

#[cfg(feature = "sentry")]
fn with_failure_sentry_scope_on_hub<R>(
    report: WorkerFailureReport<'_>,
    hub: Arc<sentry_core::Hub>,
    callback: impl FnOnce() -> R,
) -> R {
    sentry_core::Hub::run(hub, || {
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
    scope.set_tag("oxana.batch_size", metadata.batch_size);
    scope.set_tag("oxana.will_retry", metadata.will_retry);
    scope.set_tag("oxana.terminal", metadata.terminal);

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

    impl Error for ConcreteWorkerError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&RootCause)
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("root cause")]
    struct RootCause;

    fn job(job_id: &str, retry_count: u32, max_retries: u32) -> FailedJobMetadata {
        let will_retry = retry_count < max_retries;
        FailedJobMetadata {
            job_id: job_id.to_string(),
            args: serde_json::json!({ "source": job_id }),
            retry_count,
            max_retries,
            will_retry,
            terminal: !will_retry,
        }
    }

    fn metadata(queue: &str, jobs: Vec<FailedJobMetadata>) -> WorkerFailureMetadata {
        let will_retry = jobs.iter().any(|job| job.will_retry);
        WorkerFailureMetadata {
            batch_size: jobs.len(),
            jobs,
            queue: queue.to_string(),
            job_name: "test::EmailJob".to_string(),
            worker_name: "test::EmailWorker".to_string(),
            will_retry,
            terminal: !will_retry,
        }
    }

    fn item<T>(items: &[T], index: usize) -> &T {
        items.get(index).expect("test item")
    }

    fn json_field<'a>(value: &'a serde_json::Value, key: &str) -> &'a serde_json::Value {
        value.get(key).expect("JSON field")
    }

    fn report_failure(reporter: Option<&Arc<FailureReporterFn>>, report: WorkerFailureReport<'_>) {
        let execution_hub = execution_sentry_hub();
        let mut settings = RuntimeSettings::default();
        settings.failure_reporter = reporter.cloned();
        super::report_failure(&settings, report, &execution_hub);
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
            let reported_job = item(&report.metadata.jobs, 0);
            saw_arguments_for_reporter.store(
                reported_job.args == serde_json::json!({ "source": "custom-id" }),
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
            let event = item(&events, 0);
            assert_eq!(event.message.as_deref(), Some("custom capture"));
            assert!(!event.tags.contains_key("oxana.job_id"));
            assert!(!event.contexts.contains_key("oxana"));
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
    fn tag<'a>(event: &'a sentry_core::protocol::Event<'_>, key: &str) -> &'a str {
        event.tags.get(key).map(String::as_str).expect("event tag")
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn error_event_builder_avoids_debug_and_preserves_scope_and_metadata() {
        struct DisplayOnlyError;
        impl std::fmt::Debug for DisplayOnlyError {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("error Debug must not be called");
            }
        }
        impl std::fmt::Display for DisplayOnlyError {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("safe message")
            }
        }
        impl Error for DisplayOnlyError {}

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_builder = Arc::clone(&calls);
        let mut settings = RuntimeSettings::default();
        settings.sentry_error_event_builder = Some(Arc::new(move |error| {
            calls_for_builder.fetch_add(1, Ordering::SeqCst);
            assert!(error.downcast_ref::<DisplayOnlyError>().is_some());
            sentry_core::protocol::Event {
                message: Some(error.to_string()),
                exception: vec![sentry_core::protocol::Exception {
                    ty: "ApplicationError".to_string(),
                    stacktrace: Some(sentry_core::protocol::Stacktrace {
                        frames: vec![sentry_core::protocol::Frame {
                            function: Some("service::send_mail".to_string()),
                            filename: Some("service.rs".to_string()),
                            lineno: Some(42),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }]
                .into(),
                level: sentry_core::Level::Error,
                ..Default::default()
            }
        }));
        let metadata = metadata("mailers", vec![job("job-123", 1, 3)]);
        let events = sentry_core::test::with_captured_events(|| {
            let ((), execution_hub) =
                futures::executor::block_on(with_execution_sentry_hub(async {
                    sentry_core::configure_scope(|scope| {
                        scope.set_tag("worker.context", "preserved");
                    });
                    sentry_core::add_breadcrumb(sentry_core::Breadcrumb {
                        message: Some("before failure".to_string()),
                        ..Default::default()
                    });
                }));
            settings.report_failure(
                WorkerFailureReport {
                    failure: WorkerFailure::Error(&DisplayOnlyError),
                    metadata: &metadata,
                },
                &execution_hub,
            );
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(events.len(), 1);
        let event = item(&events, 0);
        assert_eq!(event.message.as_deref(), Some("safe message"));
        let stacktrace = item(event.exception.as_ref(), 0)
            .stacktrace
            .as_ref()
            .expect("recorded application call chain");
        let frame = item(&stacktrace.frames, 0);
        assert_eq!(frame.function.as_deref(), Some("service::send_mail"));
        assert_eq!(frame.filename.as_deref(), Some("service.rs"));
        assert_eq!(frame.lineno, Some(42));
        assert_eq!(tag(event, "worker.context"), "preserved");
        assert_eq!(tag(event, "oxana.job_id"), "job-123");
        assert_eq!(tag(event, "oxana.retry_count"), "1");
        assert!(oxana_context(event).contains_key("jobs"));
        assert_eq!(event.breadcrumbs.len(), 1);
        assert_eq!(
            item(event.breadcrumbs.as_ref(), 0).message.as_deref(),
            Some("before failure")
        );
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn failure_reporter_takes_precedence_over_error_event_builder() {
        let mut settings = RuntimeSettings::default();
        settings.failure_reporter = Some(Arc::new(|_| {
            sentry_core::capture_message("custom report", sentry_core::Level::Error);
        }));
        settings.sentry_error_event_builder =
            Some(Arc::new(|_| panic!("builder must be bypassed")));
        let metadata = metadata("custom", vec![job("job-123", 0, 1)]);
        let events = sentry_core::test::with_captured_events(|| {
            settings.report_failure(
                WorkerFailureReport {
                    failure: WorkerFailure::Error(&ConcreteWorkerError),
                    metadata: &metadata,
                },
                &execution_sentry_hub(),
            );
        });
        assert_eq!(events.len(), 1);
        assert_eq!(item(&events, 0).message.as_deref(), Some("custom report"));
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
        let event = item(&events, 0);
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

        assert_eq!(event.exception.len(), 2);
        assert_eq!(
            item(event.exception.as_ref(), 0).value.as_deref(),
            Some("root cause")
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
                "terminal": false,
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
                &RuntimeSettings::default(),
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
        assert_eq!(tag(failure, "worker.context"), "preserved");
        assert_eq!(tag(failure, "oxana.job_id"), "job-123");
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
        let jobs = oxana_context(failure)
            .get("jobs")
            .and_then(serde_json::Value::as_array)
            .expect("failure jobs");
        let args = json_field(item(jobs, 0), "args");
        assert_eq!(json_field(args, "source"), "job-123");
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
        let event = item(&events, 0);
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
        let retrying_job = item(jobs, 0);
        assert_eq!(json_field(retrying_job, "job_id"), "retrying");
        assert_eq!(
            json_field(retrying_job, "args"),
            &serde_json::json!({ "source": "retrying" })
        );
        assert_eq!(json_field(retrying_job, "will_retry"), true);
        assert_eq!(json_field(retrying_job, "terminal"), false);
        let terminal_job = item(jobs, 1);
        assert_eq!(json_field(terminal_job, "job_id"), "terminal");
        assert_eq!(
            json_field(terminal_job, "args"),
            &serde_json::json!({ "source": "terminal" })
        );
        assert_eq!(json_field(terminal_job, "will_retry"), false);
        assert_eq!(json_field(terminal_job, "terminal"), true);
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
        events.sort_by(|left, right| tag(left, "oxana.queue").cmp(tag(right, "oxana.queue")));

        assert_eq!(events.len(), 2);
        let retrying_event = item(&events, 0);
        assert_eq!(tag(retrying_event, "oxana.queue"), "retrying");
        assert_eq!(tag(retrying_event, "oxana.will_retry"), "true");
        assert_eq!(tag(retrying_event, "oxana.terminal"), "false");
        let terminal_event = item(&events, 1);
        assert_eq!(tag(terminal_event, "oxana.queue"), "terminal");
        assert_eq!(tag(terminal_event, "oxana.will_retry"), "false");
        assert_eq!(tag(terminal_event, "oxana.terminal"), "true");
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
        events.sort_by(|left, right| tag(left, "oxana.queue").cmp(tag(right, "oxana.queue")));
        assert_eq!(events.len(), 2);
        let first_event = item(&events, 0);
        assert_eq!(tag(first_event, "oxana.queue"), "queue-a");
        assert_eq!(tag(first_event, "oxana.job_id"), "job-a");
        assert_eq!(
            oxana_context(first_event).get("queue"),
            Some(&serde_json::json!("queue-a"))
        );
        let second_event = item(&events, 1);
        assert_eq!(tag(second_event, "oxana.queue"), "queue-b");
        assert_eq!(tag(second_event, "oxana.job_id"), "job-b");
        assert_eq!(
            oxana_context(second_event).get("queue"),
            Some(&serde_json::json!("queue-b"))
        );
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn panic_reporting_respects_features_and_reporter_during_unwind() {
        use futures::FutureExt;
        use std::panic::AssertUnwindSafe;

        // Destructors may emit both ordinary diagnostics and panic-like events
        // during unwinding. Neither may bypass reporter selection or replace
        // the original hook event and its stacktrace.
        struct ReportOnDrop(bool);
        impl Drop for ReportOnDrop {
            fn drop(&mut self) {
                sentry_core::capture_message("unwind diagnostic", sentry_core::Level::Error);
                if !self.0 {
                    return;
                }
                sentry_core::capture_event(sentry_core::protocol::Event {
                    exception: vec![sentry_core::protocol::Exception {
                        ty: "panic".to_string(),
                        value: Some("duplicate from destructor".to_string()),
                        mechanism: Some(sentry_core::protocol::Mechanism {
                            ty: "panic".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]
                    .into(),
                    ..Default::default()
                });
            }
        }

        for with_hook in [false, true] {
            for custom_reporter in [false, true] {
                for batch_size in [1, 2] {
                    let metadata = metadata(
                        "panics",
                        (0..batch_size)
                            .map(|i| job(&format!("panic-{i}"), i, 1))
                            .collect(),
                    );
                    let calls = Arc::new(AtomicUsize::new(0));
                    let mut settings = RuntimeSettings::default();
                    settings.sentry_error_event_builder = Some(Arc::new(|_| {
                        panic!("error builder must never handle panics");
                    }));
                    if custom_reporter {
                        let calls = Arc::clone(&calls);
                        settings.failure_reporter = Some(Arc::new(move |report| {
                            calls.fetch_add(1, Ordering::SeqCst);
                            assert!(matches!(
                                report.failure,
                                WorkerFailure::Panic {
                                    message: "worker panicked"
                                }
                            ));
                            assert_eq!(report.metadata.batch_size, batch_size as usize);
                            sentry_core::capture_message("custom panic", sentry_core::Level::Error);
                        }));
                    }
                    let mut options = sentry_core::ClientOptions::new();
                    if with_hook {
                        options =
                            options.add_integration(sentry_panic::PanicIntegration::default());
                    }
                    let events = sentry_core::test::with_captured_events_options(
                        || {
                            let (result, execution_hub) =
                                futures::executor::block_on(with_execution_sentry_hub(
                                    AssertUnwindSafe(async {
                                        sentry_core::configure_scope(|scope| {
                                            scope.set_tag("worker.context", "preserved");
                                        });
                                        sentry_core::add_breadcrumb(sentry_core::Breadcrumb {
                                            message: Some("before panic".to_string()),
                                            ..Default::default()
                                        });
                                        let _report_on_drop = ReportOnDrop(with_hook);
                                        panic!("worker panicked");
                                    })
                                    .catch_unwind(),
                                ));
                            assert!(result.is_err());
                            super::report_failure(
                                &settings,
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
                    assert_eq!(calls.load(Ordering::SeqCst), usize::from(custom_reporter));
                    let expected = usize::from(custom_reporter || cfg!(feature = "sentry-panic"));
                    assert_eq!(
                        events.len(),
                        expected,
                        "hook={with_hook}, custom={custom_reporter}, batch={batch_size}"
                    );
                    if custom_reporter {
                        assert_eq!(item(&events, 0).message.as_deref(), Some("custom panic"));
                    } else if cfg!(feature = "sentry-panic") {
                        let event = item(&events, 0);
                        let exception = item(event.exception.as_ref(), 0);
                        assert_eq!(exception.value.as_deref(), Some("worker panicked"));
                        let mechanism = exception.mechanism.as_ref().expect("panic mechanism");
                        assert_eq!(mechanism.ty, "oxana.worker_panic");
                        assert_eq!(mechanism.handled, Some(true));
                        if with_hook {
                            assert!(
                                !exception
                                    .stacktrace
                                    .as_ref()
                                    .expect("hook stacktrace")
                                    .frames
                                    .is_empty()
                            );
                        }
                        assert_eq!(tag(event, "worker.context"), "preserved");
                        assert_eq!(tag(event, "oxana.failure_kind"), "panic");
                        assert_eq!(tag(event, "oxana.batch_size"), batch_size.to_string());
                        assert_eq!(
                            oxana_context(event).get("jobs"),
                            Some(&serde_json::to_value(&metadata.jobs).expect("jobs"))
                        );
                        assert_eq!(event.breadcrumbs.len(), 1);
                    }
                }
            }
        }
    }

    #[cfg(feature = "sentry")]
    #[test]
    fn panic_hook_outside_worker_scope_is_unaffected() {
        let events = sentry_core::test::with_captured_events_options(
            || {
                futures::executor::block_on(with_execution_sentry_hub(async {}));
                assert!(std::panic::catch_unwind(|| panic!("application panic")).is_err());
            },
            sentry_core::ClientOptions::new()
                .add_integration(sentry_panic::PanicIntegration::default()),
        );
        assert_eq!(events.len(), 1);
        let mechanism = item(item(&events, 0).exception.as_ref(), 0)
            .mechanism
            .as_ref()
            .expect("application mechanism");
        assert_eq!(mechanism.ty, "panic");
        assert_eq!(mechanism.handled, Some(false));
    }
}
