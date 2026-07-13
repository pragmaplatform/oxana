use axum::{
    Form,
    extract::{Extension, Path, Query},
    response::{IntoResponse, Redirect, Response},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use crate::JOBS_PER_PAGE;
use crate::OxanaWebState;
use crate::error::OxanaWebError;
use crate::templates::{
    BusyTemplate, CronRow, CronTemplate, CronWorkerView, DashboardTemplate, GlobalJobsTemplate,
    JobDetailTemplate, JobListKind, MetricDetailTemplate, MetricsTemplate, OnDemandJobView,
    OnDemandQueueView, OnDemandRow, OnDemandTemplate, QueueConcurrencyStatus, QueueDetailTemplate,
    QueueRuntimeConfigView, QueuesTemplate,
};

const DEFAULT_QUEUE_SORT: &str = "enqueued";
const DEFAULT_QUEUE_SORT_DIRECTION: &str = "desc";

pub(crate) async fn dashboard(
    Extension(state): Extension<OxanaWebState>,
) -> Result<DashboardTemplate, OxanaWebError> {
    let stats = state.storage.stats().await?;
    let queue_configs = queue_config_map(&state.storage, &state.catalog, &stats).await?;

    Ok(DashboardTemplate {
        base_path: state.base_path,
        active_tab: "",
        stats,
        queue_configs,
    })
}

pub(crate) async fn busy(
    Extension(state): Extension<OxanaWebState>,
) -> Result<BusyTemplate, OxanaWebError> {
    let stats = state.storage.stats().await?;
    let queue_configs = queue_config_map(&state.storage, &state.catalog, &stats).await?;

    Ok(BusyTemplate {
        base_path: state.base_path,
        active_tab: "/busy",
        stats,
        queue_configs,
    })
}

pub(crate) async fn queues_list(
    Extension(state): Extension<OxanaWebState>,
    Query(params): Query<QueuesParams>,
) -> Result<QueuesTemplate, OxanaWebError> {
    let mut stats = state.storage.stats().await?;
    let queue_configs = queue_config_map(&state.storage, &state.catalog, &stats).await?;
    let query = oxana::JobMetricsQuery::new(params.minutes.unwrap_or(0));
    let queue_lengths = state.storage.queue_length_metrics(query).await?;

    let (sort, dir) = sort_queues_for_params(&mut stats.queues, &queue_configs, &params);

    Ok(QueuesTemplate {
        base_path: state.base_path,
        active_tab: "/queues",
        stats,
        queue_configs,
        queue_lengths,
        sort: sort.to_string(),
        dir: dir.to_string(),
    })
}

pub(crate) async fn metrics(
    Extension(state): Extension<OxanaWebState>,
    Query(params): Query<MetricsParams>,
) -> Result<MetricsTemplate, OxanaWebError> {
    let query = oxana::JobMetricsQuery::new(params.minutes.unwrap_or(0));
    let metrics = state.storage.job_metrics(query).await?;
    let sort = params
        .sort
        .as_deref()
        .filter(|sort| valid_metric_sort(sort))
        .unwrap_or("");
    let dir = if !sort.is_empty() && params.dir.as_deref() == Some("asc") {
        "asc"
    } else {
        "desc"
    };
    let sort_key = if sort.is_empty() { "total_time" } else { sort };
    let table_workers = sorted_worker_metrics(&metrics.workers, sort_key, dir == "desc");

    Ok(MetricsTemplate {
        base_path: state.base_path,
        active_tab: "/metrics",
        metrics,
        table_workers,
        sort: sort.to_string(),
        dir: dir.to_string(),
    })
}

pub(crate) async fn metric_detail(
    Extension(state): Extension<OxanaWebState>,
    Query(params): Query<MetricDetailParams>,
) -> Result<MetricDetailTemplate, OxanaWebError> {
    let identity = oxana::MetricIdentity {
        worker: params.worker,
    };
    let metrics = state
        .storage
        .job_metrics_for(
            &identity,
            oxana::JobMetricsQuery::new(params.minutes.unwrap_or(0)),
        )
        .await?;

    Ok(MetricDetailTemplate {
        base_path: state.base_path,
        active_tab: "/metrics",
        metrics,
    })
}

pub(crate) async fn cron_jobs(
    Extension(state): Extension<OxanaWebState>,
    Query(params): Query<CronParams>,
) -> CronTemplate {
    let now = chrono::Utc::now();
    let rows = build_cron_rows(&state.catalog.cron_workers, &now);
    let total = state.catalog.cron_workers.len();

    CronTemplate {
        base_path: state.base_path,
        active_tab: "/cron",
        rows,
        total,
        enqueued: params.enqueued.as_deref() == Some("1"),
    }
}

pub(crate) async fn enqueue_cron_job(
    Extension(state): Extension<OxanaWebState>,
    Form(form): Form<CronEnqueueJobForm>,
) -> Result<Redirect, OxanaWebError> {
    let envelope = cron_envelope_from_form(&state.catalog, &form)?;
    state.storage.enqueue_envelope(envelope).await?;

    Ok(cron_enqueued_redirect(&state.base_path))
}

pub(crate) async fn on_demand_jobs(
    Extension(state): Extension<OxanaWebState>,
    Query(params): Query<OnDemandParams>,
) -> OnDemandTemplate {
    let rows = build_on_demand_rows(&state.catalog.on_demand_jobs);
    let queues = build_on_demand_queue_views(&state.catalog.queues);

    OnDemandTemplate {
        base_path: state.base_path,
        active_tab: "/on-demand",
        total: state.catalog.on_demand_jobs.len(),
        rows,
        queues,
        scheduled: params.scheduled.as_deref() == Some("1"),
        invalid_json: params.invalid_json.as_deref() == Some("1"),
    }
}

pub(crate) async fn enqueue_on_demand_job(
    Extension(state): Extension<OxanaWebState>,
    Form(form): Form<OnDemandEnqueueJobForm>,
) -> Result<Redirect, OxanaWebError> {
    let envelope = match on_demand_envelope_from_form(&state.catalog, &form) {
        Ok(envelope) => envelope,
        Err(oxana::OxanaError::JsonError(_)) => {
            return Ok(Redirect::to(&format!(
                "{}/on-demand?invalid_json=1",
                state.base_path
            )));
        }
        Err(error) => return Err(error.into()),
    };

    state.storage.enqueue_envelope(envelope).await?;

    Ok(Redirect::to(&format!(
        "{}/on-demand?scheduled=1",
        state.base_path
    )))
}

pub(crate) async fn scheduled_jobs(
    Extension(state): Extension<OxanaWebState>,
    Query(params): Query<PaginationParams>,
) -> Result<GlobalJobsTemplate, OxanaWebError> {
    let page = params.page.max(1);
    let opts = list_opts(page);

    let total = state.storage.scheduled_count().await?;
    let mut jobs = state.storage.list_scheduled(&opts).await?;

    let has_next = jobs.len() > JOBS_PER_PAGE;
    jobs.truncate(JOBS_PER_PAGE);

    Ok(GlobalJobsTemplate {
        base_path: state.base_path,
        active_tab: "/scheduled",
        kind: JobListKind::Scheduled,
        jobs,
        page,
        total,
        has_next,
    })
}

pub(crate) async fn dead_jobs(
    Extension(state): Extension<OxanaWebState>,
    Query(params): Query<PaginationParams>,
) -> Result<GlobalJobsTemplate, OxanaWebError> {
    let page = params.page.max(1);
    let opts = list_opts(page);

    let total = state.storage.dead_count().await?;
    let mut jobs = state.storage.list_dead(&opts).await?;

    let has_next = jobs.len() > JOBS_PER_PAGE;
    jobs.truncate(JOBS_PER_PAGE);

    Ok(GlobalJobsTemplate {
        base_path: state.base_path,
        active_tab: "/dead",
        kind: JobListKind::Dead,
        jobs,
        page,
        total,
        has_next,
    })
}

pub(crate) async fn wipe_dead(
    Extension(state): Extension<OxanaWebState>,
) -> Result<Redirect, OxanaWebError> {
    state.storage.wipe_dead().await?;

    Ok(Redirect::to(&format!("{}/dead", state.base_path)))
}

pub(crate) async fn revive_all_dead(
    Extension(state): Extension<OxanaWebState>,
) -> Result<Redirect, OxanaWebError> {
    state.storage.revive_all_dead().await?;

    Ok(Redirect::to(&format!("{}/dead", state.base_path)))
}

pub(crate) async fn retry_jobs(
    Extension(state): Extension<OxanaWebState>,
    Query(params): Query<PaginationParams>,
) -> Result<GlobalJobsTemplate, OxanaWebError> {
    let page = params.page.max(1);
    let opts = list_opts(page);

    let total = state.storage.retries_count().await?;
    let mut jobs = state.storage.list_retries(&opts).await?;

    let has_next = jobs.len() > JOBS_PER_PAGE;
    jobs.truncate(JOBS_PER_PAGE);

    Ok(GlobalJobsTemplate {
        base_path: state.base_path,
        active_tab: "/retries",
        kind: JobListKind::Retries,
        jobs,
        page,
        total,
        has_next,
    })
}

pub(crate) async fn retry_all_now(
    Extension(state): Extension<OxanaWebState>,
) -> Result<Redirect, OxanaWebError> {
    state.storage.retry_all_now().await?;

    Ok(Redirect::to(&format!("{}/retries", state.base_path)))
}

pub(crate) async fn job_detail(
    Extension(state): Extension<OxanaWebState>,
    Path(job_id): Path<String>,
) -> Result<JobDetailTemplate, OxanaWebError> {
    let (job, is_dead) = find_job(&state.storage, &job_id).await?;

    Ok(JobDetailTemplate {
        base_path: state.base_path,
        active_tab: "/jobs",
        job_id,
        job,
        is_dead,
    })
}

pub(crate) async fn queue_detail(
    Extension(state): Extension<OxanaWebState>,
    Path(queue_key): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Response, OxanaWebError> {
    if is_dynamic_parent_queue(&state.catalog, &queue_key) {
        return Ok(Redirect::to(&format!("{}/queues", state.base_path)).into_response());
    }

    let page = params.page.max(1);
    let opts = list_opts(page);

    let stats = state.storage.stats().await?;
    let queue_config = queue_runtime_config_for(&state, &queue_key).await?;
    let live_enqueued = state
        .storage
        .enqueued_count(RawQueue::fixed(queue_key.clone()))
        .await?;
    let queue_stats = queue_detail_queue_stats(&stats, &queue_key, live_enqueued);
    let total = live_enqueued;
    let active_jobs = stats
        .processing
        .iter()
        .filter(|p| p.job_envelope.queue == queue_key)
        .cloned()
        .collect::<Vec<_>>();
    let busy = active_jobs.len();

    let mut jobs = state
        .storage
        .list_queue_jobs(RawQueue::fixed(queue_key.clone()), &opts)
        .await?;

    let has_next = jobs.len() > JOBS_PER_PAGE;
    jobs.truncate(JOBS_PER_PAGE);

    Ok(QueueDetailTemplate {
        base_path: state.base_path,
        active_tab: "/queues",
        queue_key,
        queue_stats,
        queue_config,
        active_jobs,
        busy,
        jobs,
        page,
        total,
        has_next,
    }
    .into_response())
}

fn is_dynamic_parent_queue(catalog: &oxana::Catalog, queue_key: &str) -> bool {
    !queue_key.contains('#')
        && catalog
            .queues
            .iter()
            .any(|queue| queue.key == queue_key && queue.dynamic)
}

fn queue_detail_queue_stats(
    stats: &oxana::Stats,
    queue_key: &str,
    live_enqueued: usize,
) -> Option<oxana::QueueStats> {
    if let Some(queue_stats) = stats.queues.iter().find(|q| q.key == queue_key).cloned() {
        return Some(queue_stats_with_live_enqueued(queue_stats, live_enqueued));
    }

    let (prefix, suffix) = queue_key.split_once('#')?;
    let parent = stats.queues.iter().find(|q| q.key == prefix)?;
    let dynamic_queue = parent.queues.iter().find(|q| q.suffix == suffix)?;

    Some(queue_stats_with_live_enqueued(
        oxana::QueueStats {
            key: queue_key.to_string(),
            enqueued: dynamic_queue.enqueued,
            processed: dynamic_queue.processed,
            succeeded: dynamic_queue.succeeded,
            panicked: dynamic_queue.panicked,
            failed: dynamic_queue.failed,
            latency_s: dynamic_queue.latency_s,
            rate: dynamic_queue.rate,
            queues: Vec::new(),
        },
        live_enqueued,
    ))
}

fn queue_stats_with_live_enqueued(
    mut queue_stats: oxana::QueueStats,
    live_enqueued: usize,
) -> oxana::QueueStats {
    let effective_drain_per_minute = queue_stats.rate.effective_drain_per_minute;
    queue_stats.rate.eta_s = if live_enqueued == 0 {
        Some(0.0)
    } else if effective_drain_per_minute > 0.0 {
        Some(live_enqueued as f64 / (effective_drain_per_minute / 60.0))
    } else {
        None
    };
    queue_stats.enqueued = live_enqueued;
    queue_stats
}

async fn find_job(
    storage: &oxana::Storage,
    job_id: &str,
) -> Result<(Option<oxana::JobEnvelope>, bool), oxana::OxanaError> {
    let job_id = job_id.to_string();
    if let Some(job) = storage.get_job(&job_id).await? {
        return Ok((Some(job), false));
    }

    let dead_count = storage.dead_count().await?;
    if dead_count == 0 {
        return Ok((None, false));
    }

    let dead_jobs = storage
        .list_dead(&oxana::QueueListOpts {
            count: dead_count,
            offset: 0,
        })
        .await?;
    let Some(dead_job) = dead_jobs.into_iter().find(|job| job.id == job_id) else {
        return Ok((None, false));
    };

    Ok((Some(dead_job), true))
}

pub(crate) async fn wipe_queue(
    Extension(state): Extension<OxanaWebState>,
    Path(queue_key): Path<String>,
) -> Result<Redirect, OxanaWebError> {
    state
        .storage
        .wipe_queue(RawQueue::fixed(queue_key.clone()))
        .await?;

    Ok(Redirect::to(&format!(
        "{}/queues/{}",
        state.base_path,
        urlencoding::encode(&queue_key)
    )))
}

pub(crate) async fn pause_queue(
    Extension(state): Extension<OxanaWebState>,
    Path(queue_key): Path<String>,
) -> Result<Redirect, OxanaWebError> {
    set_queue_state(&state, &queue_key, oxana::QueueState::Paused).await?;

    Ok(queue_redirect(&state.base_path, &queue_key))
}

pub(crate) async fn unpause_queue(
    Extension(state): Extension<OxanaWebState>,
    Path(queue_key): Path<String>,
) -> Result<Redirect, OxanaWebError> {
    set_queue_state(&state, &queue_key, oxana::QueueState::Active).await?;

    Ok(queue_redirect(&state.base_path, &queue_key))
}

pub(crate) async fn set_queue_concurrency(
    Extension(state): Extension<OxanaWebState>,
    Path(queue_key): Path<String>,
    Form(form): Form<QueueConcurrencyForm>,
) -> Result<Redirect, OxanaWebError> {
    if form.concurrency == 0 {
        return Err(oxana::OxanaError::ConfigError(
            "Queue concurrency must be at least 1".to_string(),
        )
        .into());
    }

    state
        .storage
        .set_queue_concurrency(
            RawQueue::from_catalog(&state.catalog, &queue_key),
            form.concurrency,
        )
        .await?;

    Ok(queue_redirect(&state.base_path, &queue_key))
}

pub(crate) async fn delete_job(
    Extension(state): Extension<OxanaWebState>,
    Path((queue_key, job_id)): Path<(String, String)>,
) -> Result<Redirect, OxanaWebError> {
    state.storage.delete_job(&job_id).await?;

    Ok(Redirect::to(&format!(
        "{}/queues/{}",
        state.base_path,
        urlencoding::encode(&queue_key)
    )))
}

pub(crate) async fn enqueue_job(
    Extension(state): Extension<OxanaWebState>,
    Form(form): Form<EnqueueJobForm>,
) -> Result<Redirect, OxanaWebError> {
    let envelope = immediate_envelope(
        form.queue,
        form.name,
        parse_json(&form.args)?,
        parse_optional_json(form.state.as_deref())?,
        true,
    );

    state.storage.enqueue_envelope(envelope).await?;

    let redirect = form.redirect.as_deref().unwrap_or("/dead");
    Ok(Redirect::to(&format!("{}{}", state.base_path, redirect)))
}

// --- Helpers ---

#[derive(Serialize)]
struct RawQueue {
    key: String,
    #[serde(skip_serializing)]
    base_key: String,
    #[serde(skip_serializing)]
    dynamic: bool,
    #[serde(skip_serializing)]
    concurrency: oxana::QueueConcurrency,
}

impl RawQueue {
    fn fixed(key: String) -> Self {
        Self {
            base_key: key.clone(),
            key,
            dynamic: false,
            concurrency: oxana::QueueConcurrency::Fixed(1),
        }
    }

    fn from_catalog(catalog: &oxana::Catalog, key: &str) -> Self {
        let base_key = base_queue_key(key);
        let queue_info = catalog.queues.iter().find(|queue| queue.key == base_key);
        let concurrency = queue_info.map_or(oxana::QueueConcurrency::Fixed(1), |queue| {
            if queue.dynamic_concurrency {
                oxana::QueueConcurrency::Dynamic {
                    default: queue.concurrency,
                }
            } else {
                oxana::QueueConcurrency::Fixed(queue.concurrency)
            }
        });

        Self {
            key: key.to_string(),
            base_key: base_key.to_string(),
            dynamic: queue_info.is_some_and(|queue| queue.dynamic),
            concurrency,
        }
    }
}

impl oxana::Queue for RawQueue {
    fn key(&self) -> String {
        self.key.clone()
    }

    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("")
    }

    fn config(&self) -> oxana::QueueConfig {
        let config = if self.dynamic {
            oxana::QueueConfig::as_dynamic(self.base_key.clone())
        } else {
            oxana::QueueConfig::as_static(self.key.clone())
        };

        match self.concurrency {
            oxana::QueueConcurrency::Fixed(concurrency) => config.concurrency(concurrency),
            oxana::QueueConcurrency::Dynamic { default } => config.dynamic_concurrency(default),
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct PaginationParams {
    #[serde(default = "default_page")]
    page: usize,
}

fn default_page() -> usize {
    1
}

#[derive(Deserialize)]
pub(crate) struct QueuesParams {
    #[serde(default)]
    sort: Option<String>,
    #[serde(default)]
    dir: Option<String>,
    #[serde(default)]
    minutes: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct MetricsParams {
    #[serde(default)]
    sort: Option<String>,
    #[serde(default)]
    dir: Option<String>,
    #[serde(default)]
    minutes: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct MetricDetailParams {
    worker: String,
    #[serde(default)]
    minutes: Option<usize>,
}

#[derive(Deserialize)]
pub(crate) struct EnqueueJobForm {
    queue: String,
    name: String,
    args: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    redirect: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct CronEnqueueJobForm {
    name: String,
}

#[derive(Deserialize)]
pub(crate) struct OnDemandEnqueueJobForm {
    queue: String,
    name: String,
    args: String,
}

#[derive(Deserialize)]
pub(crate) struct QueueConcurrencyForm {
    concurrency: usize,
}

#[derive(Deserialize)]
pub(crate) struct OnDemandParams {
    #[serde(default)]
    scheduled: Option<String>,
    #[serde(default)]
    invalid_json: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct CronParams {
    #[serde(default)]
    enqueued: Option<String>,
}

fn list_opts(page: usize) -> oxana::QueueListOpts {
    oxana::QueueListOpts {
        count: JOBS_PER_PAGE + 1,
        offset: (page - 1) * JOBS_PER_PAGE,
    }
}

async fn queue_config_map(
    storage: &oxana::Storage,
    catalog: &oxana::Catalog,
    stats: &oxana::Stats,
) -> Result<HashMap<String, QueueRuntimeConfigView>, oxana::OxanaError> {
    let mut keys = queue_config_keys(catalog, stats);
    keys.sort();
    keys.dedup();

    let stored_configs = storage.queue_configs(&keys).await?;

    Ok(keys
        .into_iter()
        .map(|key| {
            let config = queue_runtime_config_view_from_map(catalog, &stored_configs, &key);
            (key, config)
        })
        .collect())
}

fn queue_config_keys(catalog: &oxana::Catalog, stats: &oxana::Stats) -> Vec<String> {
    let mut keys = catalog
        .queues
        .iter()
        .map(|queue| queue.key.clone())
        .collect::<Vec<_>>();

    for queue in &stats.queues {
        keys.push(queue.key.clone());
        keys.extend(
            queue
                .queues
                .iter()
                .map(|dynamic_queue| format!("{}#{}", queue.key, dynamic_queue.suffix)),
        );
    }

    keys
}

fn merge_queue_runtime_config(
    catalog: &oxana::Catalog,
    queue_key: &str,
    mut config: oxana::QueueRuntimeConfig,
    default_config: oxana::QueueRuntimeConfig,
) -> oxana::QueueRuntimeConfig {
    if !queue_uses_dynamic_concurrency(catalog, queue_key) {
        config.concurrency = None;
    }

    config.with_defaults(default_config)
}

fn queue_runtime_config_from_map(
    catalog: &oxana::Catalog,
    stored_configs: &HashMap<String, oxana::QueueRuntimeConfig>,
    queue_key: &str,
) -> oxana::QueueRuntimeConfig {
    let base_key = base_queue_key(queue_key);
    let base_default = default_queue_runtime_config(catalog, base_key);
    let base_config = stored_configs
        .get(base_key)
        .cloned()
        .map(|config| merge_queue_runtime_config(catalog, base_key, config, base_default.clone()))
        .unwrap_or(base_default);

    if queue_key == base_key {
        return base_config;
    }

    stored_configs
        .get(queue_key)
        .cloned()
        .map(|config| merge_queue_runtime_config(catalog, queue_key, config, base_config.clone()))
        .unwrap_or(base_config)
}

fn queue_runtime_config_view_from_map(
    catalog: &oxana::Catalog,
    stored_configs: &HashMap<String, oxana::QueueRuntimeConfig>,
    queue_key: &str,
) -> QueueRuntimeConfigView {
    let config = queue_runtime_config_from_map(catalog, stored_configs, queue_key);
    let concurrency_status = queue_concurrency_status_from_map(catalog, stored_configs, queue_key);

    QueueRuntimeConfigView {
        config,
        concurrency_status,
    }
}

fn queue_concurrency_status_from_map(
    catalog: &oxana::Catalog,
    stored_configs: &HashMap<String, oxana::QueueRuntimeConfig>,
    queue_key: &str,
) -> QueueConcurrencyStatus {
    if !queue_uses_dynamic_concurrency(catalog, queue_key) {
        return QueueConcurrencyStatus::Fixed;
    }

    let base_key = base_queue_key(queue_key);
    let configured_default = default_queue_runtime_config(catalog, base_key)
        .concurrency
        .unwrap_or(1);
    let direct_concurrency = stored_configs
        .get(queue_key)
        .and_then(|config| config.concurrency);

    if queue_key == base_key {
        return match direct_concurrency {
            Some(concurrency) if concurrency != configured_default => {
                QueueConcurrencyStatus::Override {
                    default: configured_default,
                }
            }
            _ => QueueConcurrencyStatus::Default {
                default: configured_default,
            },
        };
    }

    let inherited_default = queue_runtime_config_from_map(catalog, stored_configs, base_key)
        .concurrency
        .unwrap_or(configured_default);

    if let Some(concurrency) = direct_concurrency {
        return if concurrency != inherited_default {
            QueueConcurrencyStatus::Override {
                default: inherited_default,
            }
        } else {
            QueueConcurrencyStatus::Default {
                default: inherited_default,
            }
        };
    }

    QueueConcurrencyStatus::Default {
        default: inherited_default,
    }
}

fn queue_uses_dynamic_concurrency(catalog: &oxana::Catalog, queue_key: &str) -> bool {
    let base_key = base_queue_key(queue_key);
    catalog
        .queues
        .iter()
        .find(|queue| queue.key == base_key)
        .is_some_and(|queue| queue.dynamic_concurrency)
}

async fn queue_runtime_config_for(
    state: &OxanaWebState,
    queue_key: &str,
) -> Result<QueueRuntimeConfigView, oxana::OxanaError> {
    let keys = queue_runtime_config_keys(queue_key);
    let stored_configs = state.storage.queue_configs(&keys).await?;

    Ok(queue_runtime_config_view_from_map(
        &state.catalog,
        &stored_configs,
        queue_key,
    ))
}

fn queue_runtime_config_keys(queue_key: &str) -> Vec<String> {
    let base_key = base_queue_key(queue_key);
    if base_key == queue_key {
        vec![base_key.to_string()]
    } else {
        vec![base_key.to_string(), queue_key.to_string()]
    }
}

fn default_queue_runtime_config(
    catalog: &oxana::Catalog,
    queue_key: &str,
) -> oxana::QueueRuntimeConfig {
    let base_key = base_queue_key(queue_key);

    catalog
        .queues
        .iter()
        .find(|queue| queue.key == base_key)
        .map_or_else(
            || oxana::QueueRuntimeConfig::new(1),
            |queue| oxana::QueueRuntimeConfig::new(queue.concurrency),
        )
}

fn base_queue_key(queue_key: &str) -> &str {
    queue_key
        .split_once('#')
        .map_or(queue_key, |(base, _)| base)
}

async fn set_queue_state(
    state: &OxanaWebState,
    queue_key: &str,
    queue_state: oxana::QueueState,
) -> Result<(), oxana::OxanaError> {
    state
        .storage
        .set_queue_state(
            RawQueue::from_catalog(&state.catalog, queue_key),
            queue_state,
        )
        .await
}

fn queue_redirect(base_path: &str, queue_key: &str) -> Redirect {
    Redirect::to(&format!(
        "{}/queues/{}",
        base_path,
        urlencoding::encode(queue_key)
    ))
}

fn sort_queues(
    queues: &mut [oxana::QueueStats],
    queue_configs: &HashMap<String, QueueRuntimeConfigView>,
    sort: &str,
    desc: bool,
) {
    queues.sort_by(|a, b| {
        let cmp = if sort == "eta" {
            compare_eta(a.rate.eta_s, b.rate.eta_s, desc)
        } else {
            let cmp = match sort {
                "enqueued" => a.enqueued.cmp(&b.enqueued),
                "processed" => a.processed.cmp(&b.processed),
                "succeeded" => a.succeeded.cmp(&b.succeeded),
                "failed" => a.failed.cmp(&b.failed),
                "panicked" => a.panicked.cmp(&b.panicked),
                "rate" => a
                    .rate
                    .processed_per_minute
                    .partial_cmp(&b.rate.processed_per_minute)
                    .unwrap_or(std::cmp::Ordering::Equal),
                "concurrency" => {
                    let ca = queue_configs
                        .get(&a.key)
                        .and_then(|config| config.config.concurrency)
                        .unwrap_or(0);
                    let cb = queue_configs
                        .get(&b.key)
                        .and_then(|config| config.config.concurrency)
                        .unwrap_or(0);
                    ca.cmp(&cb)
                }
                "state" => queue_state_order(queue_configs, &a.key)
                    .cmp(&queue_state_order(queue_configs, &b.key)),
                "latency" => a
                    .latency_s
                    .partial_cmp(&b.latency_s)
                    .unwrap_or(std::cmp::Ordering::Equal),
                _ => a.key.cmp(&b.key),
            };
            if desc { cmp.reverse() } else { cmp }
        };
        cmp.then_with(|| a.key.cmp(&b.key))
    });
}

fn sort_queues_for_params<'params>(
    queues: &mut [oxana::QueueStats],
    queue_configs: &HashMap<String, QueueRuntimeConfigView>,
    params: &'params QueuesParams,
) -> (&'params str, &'params str) {
    let sort = params.sort.as_deref().unwrap_or(DEFAULT_QUEUE_SORT);
    let default_dir = if params.sort.is_none() {
        DEFAULT_QUEUE_SORT_DIRECTION
    } else {
        "asc"
    };
    let dir = params.dir.as_deref().unwrap_or(default_dir);

    sort_queues(queues, queue_configs, sort, dir == "desc");

    (sort, dir)
}

fn queue_state_order(queue_configs: &HashMap<String, QueueRuntimeConfigView>, key: &str) -> u8 {
    match queue_configs.get(key).map(|config| config.config.state) {
        Some(oxana::QueueState::Paused) => 1,
        _ => 0,
    }
}

fn valid_metric_sort(sort: &str) -> bool {
    matches!(
        sort,
        "processed" | "succeeded" | "failed" | "total_time" | "avg_time"
    )
}

fn sorted_worker_metrics(
    workers: &[oxana::WorkerMetricsSummary],
    sort: &str,
    desc: bool,
) -> Vec<oxana::WorkerMetricsSummary> {
    let mut workers = workers.to_vec();

    workers.sort_by(|a, b| {
        let cmp = match sort {
            "processed" => a.totals.processed.cmp(&b.totals.processed),
            "succeeded" => a.totals.succeeded.cmp(&b.totals.succeeded),
            "failed" => a.totals.failed.cmp(&b.totals.failed),
            "avg_time" => a
                .totals
                .average_execution_ms()
                .partial_cmp(&b.totals.average_execution_ms())
                .unwrap_or(std::cmp::Ordering::Equal),
            _ => a.totals.execution_ms.cmp(&b.totals.execution_ms),
        };
        let cmp = if desc { cmp.reverse() } else { cmp };

        cmp.then_with(|| a.identity.worker.cmp(&b.identity.worker))
    });

    workers
}

fn compare_eta(a: Option<f64>, b: Option<f64>, desc: bool) -> std::cmp::Ordering {
    let cmp = match (a, b) {
        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    };

    if desc { cmp.reverse() } else { cmp }
}

fn on_demand_envelope_from_form(
    catalog: &oxana::Catalog,
    form: &OnDemandEnqueueJobForm,
) -> Result<oxana::JobEnvelope, oxana::OxanaError> {
    if !catalog
        .queues
        .iter()
        .any(|queue| !queue.dynamic && queue.key == form.queue)
    {
        return Err(oxana::OxanaError::GenericError(format!(
            "Queue {} is not a registered static queue",
            form.queue
        )));
    }

    let job = catalog
        .on_demand_jobs
        .iter()
        .find(|job| job.name == form.name)
        .ok_or_else(|| {
            oxana::OxanaError::GenericError(format!(
                "Job {} is not registered for on-demand enqueue",
                form.name
            ))
        })?;

    job.enqueue_envelope(form.queue.clone(), parse_json(&form.args)?)
}

fn cron_envelope_from_form(
    catalog: &oxana::Catalog,
    form: &CronEnqueueJobForm,
) -> Result<oxana::JobEnvelope, oxana::OxanaError> {
    let job = catalog
        .cron_workers
        .iter()
        .find(|job| job.name == form.name)
        .ok_or_else(|| {
            oxana::OxanaError::GenericError(format!("Cron job {} is not registered", form.name))
        })?;

    Ok(immediate_envelope(
        job.queue_key.clone(),
        job.name.clone(),
        serde_json::json!({}),
        None,
        job.resurrect,
    ))
}

fn parse_json(input: &str) -> Result<serde_json::Value, oxana::OxanaError> {
    serde_json::from_str(input).map_err(oxana::OxanaError::from)
}

fn parse_optional_json(
    input: Option<&str>,
) -> Result<Option<serde_json::Value>, oxana::OxanaError> {
    input
        .filter(|value| !value.is_empty())
        .map(parse_json)
        .transpose()
}

fn cron_enqueued_redirect(base_path: &str) -> Redirect {
    Redirect::to(&format!("{base_path}/cron?enqueued=1"))
}

fn immediate_envelope(
    queue: String,
    name: String,
    args: serde_json::Value,
    state: Option<serde_json::Value>,
    resurrect: bool,
) -> oxana::JobEnvelope {
    let now = chrono::Utc::now().timestamp_micros();
    let id = uuid::Uuid::new_v4().to_string();

    oxana::JobEnvelope {
        id: id.clone(),
        queue,
        job: oxana::JobData { name, args },
        meta: oxana::JobMeta {
            id,
            retries: 0,
            unique: false,
            on_conflict: None,
            created_at: now,
            scheduled_at: now,
            started_at: None,
            state,
            resurrect,
            error: None,
            throttle_cost: None,
        },
    }
}

struct TypeTreeNode<T> {
    children: BTreeMap<String, TypeTreeNode<T>>,
    entries: Vec<T>,
}

impl<T> TypeTreeNode<T> {
    fn new() -> Self {
        Self {
            children: BTreeMap::new(),
            entries: Vec::new(),
        }
    }
}

fn type_tree_parts(name: &str) -> (Vec<&str>, String) {
    let segments: Vec<&str> = name.split("::").collect();
    let split = segments.len().saturating_sub(2);

    (segments[..split].to_vec(), segments[split..].join("::"))
}

fn insert_type_tree_entry<T>(root: &mut TypeTreeNode<T>, group_segments: &[&str], entry: T) {
    let mut node = root;
    for seg in group_segments {
        node = node
            .children
            .entry((*seg).to_string())
            .or_insert_with(TypeTreeNode::new);
    }

    node.entries.push(entry);
}

fn build_cron_rows(
    cron_workers: &[oxana::CronWorkerInfo],
    now: &chrono::DateTime<chrono::Utc>,
) -> Vec<CronRow> {
    let mut root = TypeTreeNode::new();

    for cw in cron_workers {
        let (group_segments, leaf_name) = type_tree_parts(&cw.name);
        insert_type_tree_entry(
            &mut root,
            &group_segments,
            CronWorkerView {
                name: cw.name.clone(),
                short_name: leaf_name,
                schedule: cw.schedule.to_string(),
                queue_key: cw.queue_key.clone(),
                next_run_micros: cw
                    .schedule
                    .after(now)
                    .next()
                    .map(|dt| dt.timestamp_micros()),
            },
        );
    }

    fn flatten(node: &TypeTreeNode<CronWorkerView>, depth: usize, rows: &mut Vec<CronRow>) {
        for (name, child) in &node.children {
            rows.push(CronRow::Group {
                name: name.clone(),
                depth,
            });
            flatten(child, depth + 1, rows);
        }
        for worker in &node.entries {
            rows.push(CronRow::Worker {
                view: worker.clone(),
                depth,
            });
        }
    }

    let mut rows = Vec::new();
    flatten(&root, 0, &mut rows);
    rows
}

fn build_on_demand_rows(on_demand_jobs: &[oxana::OnDemandJobInfo]) -> Vec<OnDemandRow> {
    let mut root = TypeTreeNode::new();

    for job in on_demand_jobs {
        let (group_segments, leaf_name) = type_tree_parts(&job.name);
        insert_type_tree_entry(
            &mut root,
            &group_segments,
            OnDemandJobView {
                name: job.name.clone(),
                short_name: leaf_name,
                args_template_json: serde_json::to_string_pretty(&job.args_template)
                    .unwrap_or_else(|_| job.args_template.to_string()),
            },
        );
    }

    fn flatten(node: &TypeTreeNode<OnDemandJobView>, depth: usize, rows: &mut Vec<OnDemandRow>) {
        for (name, child) in &node.children {
            rows.push(OnDemandRow::Group {
                name: name.clone(),
                depth,
            });
            flatten(child, depth + 1, rows);
        }
        for job in &node.entries {
            rows.push(OnDemandRow::Job {
                view: job.clone(),
                depth,
            });
        }
    }

    let mut rows = Vec::new();
    flatten(&root, 0, &mut rows);
    rows
}

fn build_on_demand_queue_views(queues: &[oxana::QueueInfo]) -> Vec<OnDemandQueueView> {
    let selected_queue = ["on_demand", "default", "main"]
        .into_iter()
        .find(|candidate| {
            queues
                .iter()
                .any(|queue| !queue.dynamic && queue.key == *candidate)
        });

    queues
        .iter()
        .filter(|queue| !queue.dynamic)
        .map(|queue| OnDemandQueueView {
            key: queue.key.clone(),
            selected: selected_queue == Some(queue.key.as_str()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        CronEnqueueJobForm, OnDemandEnqueueJobForm, RawQueue, build_on_demand_queue_views,
        cron_enqueued_redirect, cron_envelope_from_form, enqueue_on_demand_job,
        is_dynamic_parent_queue, on_demand_envelope_from_form, queue_concurrency_status_from_map,
        queue_detail_queue_stats, queue_runtime_config_from_map,
        queue_runtime_config_view_from_map, sort_queues, sorted_worker_metrics,
    };
    use crate::OxanaWebState;
    use crate::templates::{QueueConcurrencyStatus, QueueRuntimeConfigView};
    use axum::Form;
    use axum::extract::Extension;
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    use oxana::Queue as _;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::io::Error as WorkerError;

    const CRON_WORKER_NAME: &str = "test::CleanupCronWorker";

    #[test]
    fn queues_default_sort_is_enqueued_descending() {
        let mut lightly_loaded = queue_with_eta("lightly-loaded", None);
        lightly_loaded.enqueued = 2;
        let mut beta = queue_with_eta("beta", None);
        beta.enqueued = 20;
        let mut alpha = queue_with_eta("alpha", None);
        alpha.enqueued = 20;
        let mut queues = vec![lightly_loaded, beta, alpha];
        let params = super::QueuesParams {
            sort: None,
            dir: None,
            minutes: None,
        };

        let (sort, dir) = super::sort_queues_for_params(&mut queues, &HashMap::new(), &params);

        let keys = queues
            .iter()
            .map(|queue| queue.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!((sort, dir), ("enqueued", "desc"));
        assert_eq!(keys, vec!["alpha", "beta", "lightly-loaded"]);
    }

    #[test]
    fn explicit_queue_sort_without_direction_remains_ascending() {
        let mut queues = vec![queue_with_eta("beta", None), queue_with_eta("alpha", None)];
        let params = super::QueuesParams {
            sort: Some("key".to_string()),
            dir: None,
            minutes: None,
        };

        let (sort, dir) = super::sort_queues_for_params(&mut queues, &HashMap::new(), &params);

        let keys = queues
            .iter()
            .map(|queue| queue.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!((sort, dir), ("key", "asc"));
        assert_eq!(keys, vec!["alpha", "beta"]);
    }

    #[derive(Serialize)]
    struct StaticQueue;

    impl oxana::Queue for StaticQueue {
        fn to_config() -> oxana::QueueConfig {
            oxana::QueueConfig::as_static("default")
        }
    }

    #[derive(Serialize)]
    struct DynamicQueue {
        tenant: String,
    }

    impl oxana::Queue for DynamicQueue {
        fn key(&self) -> String {
            format!(
                "tenant#{}",
                oxana::value_to_queue_key(serde_json::to_value(self).unwrap_or_default())
            )
        }

        fn to_config() -> oxana::QueueConfig {
            oxana::QueueConfig::as_dynamic("tenant")
        }
    }

    #[derive(Debug, Serialize, Deserialize, oxana::Job)]
    #[oxana(on_demand)]
    #[oxana(unique_id = "on_demand_{id}")]
    struct OnDemandJob {
        id: u64,
        payload: String,
    }

    struct OnDemandWorker;

    impl oxana::FromContext<()> for OnDemandWorker {
        fn from_context(_ctx: &()) -> Self {
            Self
        }
    }

    #[async_trait::async_trait]
    impl oxana::Worker<OnDemandJob> for OnDemandWorker {
        type Error = WorkerError;

        async fn run_batch(
            &self,
            _jobs: Vec<oxana::BatchItem<OnDemandJob>>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn on_demand_catalog() -> oxana::Catalog {
        let storage = oxana::Storage::builder()
            .build_from_redis_url("redis://127.0.0.1/0")
            .expect("test storage should build");

        storage
            .runtime(())
            .queue::<StaticQueue>()
            .queue::<DynamicQueue>()
            .worker::<OnDemandWorker, OnDemandJob>()
            .catalog()
    }

    fn cron_catalog() -> oxana::Catalog {
        oxana::Catalog {
            workers: Vec::new(),
            cron_workers: vec![oxana::CronWorkerInfo {
                name: CRON_WORKER_NAME.to_string(),
                schedule: "*/5 * * * * *".parse().unwrap(),
                queue_key: "default".to_string(),
                resurrect: false,
            }],
            queues: Vec::new(),
            on_demand_jobs: Vec::new(),
        }
    }

    fn queue_with_eta(key: &str, eta_s: Option<f64>) -> oxana::QueueStats {
        oxana::QueueStats {
            key: key.to_string(),
            enqueued: 0,
            processed: 0,
            succeeded: 0,
            panicked: 0,
            failed: 0,
            latency_s: 0.0,
            rate: oxana::QueueRateStats {
                eta_s,
                ..oxana::QueueRateStats::default()
            },
            queues: Vec::new(),
        }
    }

    fn stats_with_queues(queues: Vec<oxana::QueueStats>) -> oxana::Stats {
        oxana::Stats {
            global: oxana::StatsGlobal {
                jobs: 0,
                enqueued: 0,
                processed: 0,
                failed: 0,
                dead: 0,
                scheduled: 0,
                retries: 0,
                latency_s_max: 0.0,
            },
            processes: Vec::new(),
            processing: Vec::new(),
            queues,
        }
    }

    fn queue_info(key: &str, dynamic: bool) -> oxana::QueueInfo {
        oxana::QueueInfo {
            key: key.to_string(),
            dynamic,
            concurrency: 1,
            dynamic_concurrency: false,
            throttle: None,
        }
    }

    fn queue_config_view(config: oxana::QueueRuntimeConfig) -> QueueRuntimeConfigView {
        QueueRuntimeConfigView {
            config,
            concurrency_status: QueueConcurrencyStatus::Fixed,
        }
    }

    #[test]
    fn dynamic_parent_queue_detail_redirect_only_applies_to_parent() {
        let catalog = oxana::Catalog {
            workers: Vec::new(),
            cron_workers: Vec::new(),
            queues: vec![queue_info("default", false), queue_info("tenant", true)],
            on_demand_jobs: Vec::new(),
        };

        assert!(is_dynamic_parent_queue(&catalog, "tenant"));
        assert!(!is_dynamic_parent_queue(&catalog, "tenant#acme"));
        assert!(!is_dynamic_parent_queue(&catalog, "default"));
    }

    #[test]
    fn queue_detail_queue_stats_uses_live_enqueued_for_static_queue() {
        let mut queue = queue_with_eta("default", None);
        queue.enqueued = 42;
        queue.processed = 7;
        queue.rate.effective_drain_per_minute = 2.0;
        queue.rate.eta_s = Some(1_260.0);
        let stats = stats_with_queues(vec![queue]);

        let detail_stats = queue_detail_queue_stats(&stats, "default", 2)
            .expect("static queue stats should exist");

        assert_eq!(detail_stats.key, "default");
        assert_eq!(detail_stats.enqueued, 2);
        assert_eq!(detail_stats.processed, 7);
        assert_eq!(detail_stats.rate.eta_s, Some(60.0));
    }

    #[test]
    fn queue_detail_queue_stats_uses_live_enqueued_for_dynamic_child_queue() {
        let mut parent = queue_with_eta("tenant", None);
        parent.enqueued = 42;
        parent.queues.push(oxana::DynamicQueueStats {
            suffix: "acme".to_string(),
            enqueued: 42,
            processed: 11,
            succeeded: 10,
            panicked: 0,
            failed: 1,
            latency_s: 3.5,
            rate: oxana::QueueRateStats::default(),
        });
        let stats = stats_with_queues(vec![parent]);

        let detail_stats = queue_detail_queue_stats(&stats, "tenant#acme", 3)
            .expect("dynamic child queue stats should exist");

        assert_eq!(detail_stats.key, "tenant#acme");
        assert_eq!(detail_stats.enqueued, 3);
        assert_eq!(detail_stats.processed, 11);
        assert_eq!(detail_stats.succeeded, 10);
        assert_eq!(detail_stats.failed, 1);
        assert_eq!(detail_stats.latency_s, 3.5);
        assert!(detail_stats.queues.is_empty());
    }

    #[test]
    fn raw_queue_from_catalog_preserves_dynamic_concurrency_mode() {
        let mut dynamic_queue = queue_info("tenant", true);
        dynamic_queue.concurrency = 7;
        dynamic_queue.dynamic_concurrency = true;
        let catalog = oxana::Catalog {
            workers: Vec::new(),
            cron_workers: Vec::new(),
            queues: vec![dynamic_queue],
            on_demand_jobs: Vec::new(),
        };

        let queue = RawQueue::from_catalog(&catalog, "tenant#acme");
        assert_eq!(queue.key(), "tenant#acme");
        assert_eq!(
            queue.config().concurrency,
            oxana::QueueConcurrency::Dynamic { default: 7 }
        );
        assert!(matches!(
            queue.config().kind,
            oxana::QueueKind::Dynamic { ref prefix, .. } if prefix == "tenant"
        ));
    }

    #[test]
    fn dynamic_child_queue_inherits_base_runtime_override() {
        let mut dynamic_queue = queue_info("tenant", true);
        dynamic_queue.concurrency = 3;
        dynamic_queue.dynamic_concurrency = true;
        let catalog = oxana::Catalog {
            workers: Vec::new(),
            cron_workers: Vec::new(),
            queues: vec![dynamic_queue],
            on_demand_jobs: Vec::new(),
        };
        let stored_configs = HashMap::from([(
            "tenant".to_string(),
            oxana::QueueRuntimeConfig {
                concurrency: Some(5),
                state: oxana::QueueState::Paused,
            },
        )]);

        let config = queue_runtime_config_from_map(&catalog, &stored_configs, "tenant#acme");

        assert_eq!(config.concurrency, Some(5));
        assert_eq!(config.state, oxana::QueueState::Paused);
    }

    #[test]
    fn dynamic_child_queue_override_wins_over_base_runtime_override() {
        let mut dynamic_queue = queue_info("tenant", true);
        dynamic_queue.concurrency = 3;
        dynamic_queue.dynamic_concurrency = true;
        let catalog = oxana::Catalog {
            workers: Vec::new(),
            cron_workers: Vec::new(),
            queues: vec![dynamic_queue],
            on_demand_jobs: Vec::new(),
        };
        let stored_configs = HashMap::from([
            (
                "tenant".to_string(),
                oxana::QueueRuntimeConfig {
                    concurrency: Some(5),
                    state: oxana::QueueState::Paused,
                },
            ),
            (
                "tenant#acme".to_string(),
                oxana::QueueRuntimeConfig {
                    concurrency: Some(9),
                    state: oxana::QueueState::Active,
                },
            ),
        ]);

        let config = queue_runtime_config_from_map(&catalog, &stored_configs, "tenant#acme");

        assert_eq!(config.concurrency, Some(9));
        assert_eq!(config.state, oxana::QueueState::Active);
    }

    #[test]
    fn dynamic_base_queue_reports_runtime_concurrency_override() {
        let mut dynamic_queue = queue_info("tenant", true);
        dynamic_queue.concurrency = 3;
        dynamic_queue.dynamic_concurrency = true;
        let catalog = oxana::Catalog {
            workers: Vec::new(),
            cron_workers: Vec::new(),
            queues: vec![dynamic_queue],
            on_demand_jobs: Vec::new(),
        };
        let stored_configs = HashMap::from([(
            "tenant".to_string(),
            oxana::QueueRuntimeConfig {
                concurrency: Some(5),
                state: oxana::QueueState::Active,
            },
        )]);

        let view = queue_runtime_config_view_from_map(&catalog, &stored_configs, "tenant");

        assert_eq!(view.config.concurrency, Some(5));
        assert_eq!(
            view.concurrency_status,
            QueueConcurrencyStatus::Override { default: 3 }
        );
    }

    #[test]
    fn dynamic_child_queue_treats_inherited_base_concurrency_as_default() {
        let mut dynamic_queue = queue_info("tenant", true);
        dynamic_queue.concurrency = 3;
        dynamic_queue.dynamic_concurrency = true;
        let catalog = oxana::Catalog {
            workers: Vec::new(),
            cron_workers: Vec::new(),
            queues: vec![dynamic_queue],
            on_demand_jobs: Vec::new(),
        };
        let stored_configs = HashMap::from([(
            "tenant".to_string(),
            oxana::QueueRuntimeConfig {
                concurrency: Some(5),
                state: oxana::QueueState::Active,
            },
        )]);

        let status = queue_concurrency_status_from_map(&catalog, &stored_configs, "tenant#acme");

        assert_eq!(status, QueueConcurrencyStatus::Default { default: 5 });
    }

    #[test]
    fn dynamic_child_queue_reports_direct_override_defaulting_to_base_concurrency() {
        let mut dynamic_queue = queue_info("tenant", true);
        dynamic_queue.concurrency = 3;
        dynamic_queue.dynamic_concurrency = true;
        let catalog = oxana::Catalog {
            workers: Vec::new(),
            cron_workers: Vec::new(),
            queues: vec![dynamic_queue],
            on_demand_jobs: Vec::new(),
        };
        let stored_configs = HashMap::from([
            (
                "tenant".to_string(),
                oxana::QueueRuntimeConfig {
                    concurrency: Some(5),
                    state: oxana::QueueState::Active,
                },
            ),
            (
                "tenant#acme".to_string(),
                oxana::QueueRuntimeConfig {
                    concurrency: Some(9),
                    state: oxana::QueueState::Active,
                },
            ),
        ]);

        let status = queue_concurrency_status_from_map(&catalog, &stored_configs, "tenant#acme");

        assert_eq!(status, QueueConcurrencyStatus::Override { default: 5 });
    }

    #[test]
    fn eta_sort_ascending_places_unknown_last() {
        let mut queues = vec![
            queue_with_eta("never", None),
            queue_with_eta("slow", Some(30.0)),
            queue_with_eta("fast", Some(10.0)),
        ];

        sort_queues(&mut queues, &HashMap::new(), "eta", false);

        let keys = queues
            .iter()
            .map(|queue| queue.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["fast", "slow", "never"]);
    }

    #[test]
    fn eta_sort_descending_places_unknown_first() {
        let mut queues = vec![
            queue_with_eta("fast", Some(10.0)),
            queue_with_eta("never", None),
            queue_with_eta("slow", Some(30.0)),
        ];

        sort_queues(&mut queues, &HashMap::new(), "eta", true);

        let keys = queues
            .iter()
            .map(|queue| queue.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["never", "slow", "fast"]);
    }

    #[test]
    fn state_sort_uses_runtime_queue_config() {
        let mut queues = vec![
            queue_with_eta("active", None),
            queue_with_eta("paused", None),
            queue_with_eta("missing", None),
        ];
        let mut queue_configs = HashMap::new();
        queue_configs.insert(
            "active".to_string(),
            queue_config_view(oxana::QueueRuntimeConfig::new(1)),
        );
        queue_configs.insert(
            "paused".to_string(),
            queue_config_view(oxana::QueueRuntimeConfig {
                concurrency: Some(1),
                state: oxana::QueueState::Paused,
            }),
        );

        sort_queues(&mut queues, &queue_configs, "state", false);

        let keys = queues
            .iter()
            .map(|queue| queue.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["active", "missing", "paused"]);
    }

    fn worker_metrics(
        worker: &str,
        processed: u64,
        succeeded: u64,
        failed: u64,
        execution_ms: u64,
        successful_executions: u64,
    ) -> oxana::WorkerMetricsSummary {
        oxana::WorkerMetricsSummary {
            identity: oxana::MetricIdentity {
                worker: worker.to_string(),
            },
            totals: oxana::JobMetricsTotals {
                processed,
                succeeded,
                failed,
                execution_ms,
                successful_executions,
                ..oxana::JobMetricsTotals::default()
            },
            series: Vec::new(),
        }
    }

    #[test]
    fn worker_metrics_sort_defaults_to_descending_metric_values() {
        let workers = vec![
            worker_metrics("fast", 5, 5, 0, 100, 5),
            worker_metrics("busy", 20, 18, 2, 90, 10),
            worker_metrics("idle", 1, 1, 0, 500, 1),
        ];

        let sorted = sorted_worker_metrics(&workers, "processed", true);

        let names = sorted
            .iter()
            .map(|worker| worker.identity.worker.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["busy", "fast", "idle"]);
        assert_eq!(workers[0].identity.worker, "fast");
    }

    #[test]
    fn worker_metrics_sort_uses_average_execution_time() {
        let workers = vec![
            worker_metrics("quick", 5, 5, 0, 100, 5),
            worker_metrics("slow", 2, 2, 0, 600, 2),
            worker_metrics("medium", 3, 3, 0, 300, 3),
        ];

        let sorted = sorted_worker_metrics(&workers, "avg_time", true);

        let names = sorted
            .iter()
            .map(|worker| worker.identity.worker.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["slow", "medium", "quick"]);
    }

    #[test]
    fn cron_form_rejects_unknown_jobs() {
        let catalog = cron_catalog();

        let err = cron_envelope_from_form(
            &catalog,
            &CronEnqueueJobForm {
                name: "missing".to_string(),
            },
        )
        .expect_err("unknown cron jobs should not be accepted");

        assert!(matches!(err, oxana::OxanaError::GenericError(_)));
    }

    #[test]
    fn cron_form_builds_immediate_envelope() {
        let catalog = cron_catalog();
        let envelope = cron_envelope_from_form(
            &catalog,
            &CronEnqueueJobForm {
                name: CRON_WORKER_NAME.to_string(),
            },
        )
        .expect("valid cron form should build an envelope");

        assert_eq!(envelope.queue, "default");
        assert_eq!(envelope.job.name, CRON_WORKER_NAME);
        assert_eq!(envelope.job.args, serde_json::json!({}));
        assert!(!envelope.meta.unique);
        assert!(envelope.meta.on_conflict.is_none());
        assert!(!envelope.meta.resurrect);
        assert_eq!(envelope.meta.scheduled_at, envelope.meta.created_at);
    }

    #[test]
    fn cron_enqueue_redirects_to_cron_notice() {
        let redirect = cron_enqueued_redirect("/admin");
        let response = redirect.into_response();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/admin/cron?enqueued=1"
        );
    }

    #[test]
    fn on_demand_form_rejects_dynamic_or_unknown_queues() {
        let catalog = on_demand_catalog();

        let err = on_demand_envelope_from_form(
            &catalog,
            &OnDemandEnqueueJobForm {
                queue: "tenant#tenant=acme".to_string(),
                name: std::any::type_name::<OnDemandJob>().to_string(),
                args: serde_json::json!({
                    "id": 1,
                    "payload": "hello",
                })
                .to_string(),
            },
        )
        .expect_err("dynamic queues should not be accepted");

        assert!(matches!(err, oxana::OxanaError::GenericError(_)));
    }

    #[test]
    fn on_demand_queue_views_preselect_on_demand_queue() {
        let views = build_on_demand_queue_views(&[
            queue_info("alpha", false),
            queue_info("on_demand", false),
            queue_info("default", false),
            queue_info("main", false),
        ]);

        let selected_keys = views
            .iter()
            .filter(|queue| queue.selected)
            .map(|queue| queue.key.as_str())
            .collect::<Vec<_>>();

        assert_eq!(selected_keys, vec!["on_demand"]);
    }

    #[test]
    fn on_demand_queue_views_preselect_default_when_on_demand_is_missing() {
        let views = build_on_demand_queue_views(&[
            queue_info("alpha", false),
            queue_info("default", false),
            queue_info("main", false),
        ]);

        let selected_keys = views
            .iter()
            .filter(|queue| queue.selected)
            .map(|queue| queue.key.as_str())
            .collect::<Vec<_>>();

        assert_eq!(selected_keys, vec!["default"]);
    }

    #[test]
    fn on_demand_queue_views_preselect_main_when_default_is_missing() {
        let views = build_on_demand_queue_views(&[
            queue_info("default", true),
            queue_info("main", false),
            queue_info("worker", false),
        ]);

        let selected_keys = views
            .iter()
            .filter(|queue| queue.selected)
            .map(|queue| queue.key.as_str())
            .collect::<Vec<_>>();

        assert_eq!(selected_keys, vec!["main"]);
    }

    #[test]
    fn on_demand_queue_views_do_not_preselect_other_queues() {
        let views =
            build_on_demand_queue_views(&[queue_info("alpha", false), queue_info("worker", false)]);

        assert!(views.iter().all(|queue| !queue.selected));
    }

    #[test]
    fn on_demand_form_rejects_unknown_jobs() {
        let catalog = on_demand_catalog();

        let err = on_demand_envelope_from_form(
            &catalog,
            &OnDemandEnqueueJobForm {
                queue: "default".to_string(),
                name: "missing".to_string(),
                args: "{}".to_string(),
            },
        )
        .expect_err("unknown jobs should not be accepted");

        assert!(matches!(err, oxana::OxanaError::GenericError(_)));
    }

    #[tokio::test]
    async fn on_demand_enqueue_redirects_after_invalid_json() {
        let storage = oxana::Storage::builder()
            .build_from_redis_url("redis://127.0.0.1/0")
            .expect("test storage pool should build");
        let state = OxanaWebState::new(storage, on_demand_catalog(), "/admin".to_string());

        let redirect = enqueue_on_demand_job(
            Extension(state),
            Form(OnDemandEnqueueJobForm {
                queue: "default".to_string(),
                name: std::any::type_name::<OnDemandJob>().to_string(),
                args: "{".to_string(),
            }),
        )
        .await
        .expect("invalid JSON should redirect back to on-demand");

        let response = redirect.into_response();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/admin/on-demand?invalid_json=1"
        );
    }

    #[test]
    fn on_demand_form_builds_typed_envelope() {
        let catalog = on_demand_catalog();
        let envelope = on_demand_envelope_from_form(
            &catalog,
            &OnDemandEnqueueJobForm {
                queue: "default".to_string(),
                name: std::any::type_name::<OnDemandJob>().to_string(),
                args: serde_json::json!({
                    "id": 42,
                    "payload": "hello",
                })
                .to_string(),
            },
        )
        .expect("valid on-demand form should build an envelope");

        assert_eq!(envelope.queue, "default");
        assert_eq!(
            envelope.id,
            format!("{}/on_demand_42", std::any::type_name::<OnDemandJob>())
        );
        assert_eq!(
            envelope.job.args,
            serde_json::json!({
                "id": 42,
                "payload": "hello",
            })
        );
    }
}
