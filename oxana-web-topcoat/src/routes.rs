use crate::{
    OxanaWebState,
    handlers::{self, *},
};
use topcoat::{
    Result,
    context::{Cx, app_context},
    router::{Router, content::Form, error::SeeOther, page, path_param, route},
    view::{BoxView, View, ViewExt, view},
};

path_param!(queue_key);
path_param!(job_id);
path_param!(*detail_job_id);

pub(crate) fn router(state: OxanaWebState) -> Router {
    Router::builder()
        .app_context(state)
        .page(dashboard)
        .page(busy)
        .page(queues_list)
        .page(metrics)
        .page(metric_detail)
        .page(cron_jobs)
        .page(on_demand_jobs)
        .page(scheduled_jobs)
        .page(dead_jobs)
        .page(retry_jobs)
        .page(job_detail)
        .page(queue_detail)
        .route(enqueue_cron_job)
        .route(enqueue_on_demand_job)
        .route(revive_all_dead)
        .route(wipe_dead)
        .route(retry_all_now)
        .route(enqueue_job)
        .route(pause_queue)
        .route(unpause_queue)
        .route(set_queue_concurrency)
        .route(wipe_queue)
        .route(delete_job)
        .build()
}

#[page("/")]
async fn dashboard(cx: &Cx) -> Result<impl View + '_> {
    let data = handlers::dashboard(app_context::<OxanaWebState>(cx).clone()).await?;
    Ok(data.into_view(cx))
}

#[page("/busy")]
async fn busy(cx: &Cx) -> Result<impl View + '_> {
    let data = handlers::busy(app_context::<OxanaWebState>(cx).clone()).await?;
    Ok(data.into_view(cx))
}

#[page("/queues")]
async fn queues_list(cx: &Cx, Form(params): Form<QueuesParams>) -> Result<impl View + '_> {
    let data = handlers::queues_list(app_context::<OxanaWebState>(cx).clone(), params).await?;
    Ok(data.into_view(cx))
}

#[page("/metrics")]
async fn metrics(cx: &Cx, Form(params): Form<MetricsParams>) -> Result<impl View + '_> {
    let data = handlers::metrics(app_context::<OxanaWebState>(cx).clone(), params).await?;
    Ok(data.into_view(cx))
}

#[page("/metrics/job")]
async fn metric_detail(cx: &Cx, Form(params): Form<MetricDetailParams>) -> Result<impl View + '_> {
    let data = handlers::metric_detail(app_context::<OxanaWebState>(cx).clone(), params).await?;
    Ok(data.into_view(cx))
}

#[page("/cron")]
async fn cron_jobs(cx: &Cx, Form(params): Form<CronParams>) -> Result<impl View + '_> {
    let data = handlers::cron_jobs(app_context::<OxanaWebState>(cx).clone(), params).await;
    Ok(data.into_view(cx))
}

#[page("/on-demand")]
async fn on_demand_jobs(cx: &Cx, Form(params): Form<OnDemandParams>) -> Result<impl View + '_> {
    let data = handlers::on_demand_jobs(app_context::<OxanaWebState>(cx).clone(), params).await;
    Ok(data.into_view(cx))
}

#[page("/scheduled")]
async fn scheduled_jobs(cx: &Cx, Form(params): Form<PaginationParams>) -> Result<impl View + '_> {
    let data = handlers::scheduled_jobs(app_context::<OxanaWebState>(cx).clone(), params).await?;
    Ok(data.into_view(cx))
}

#[page("/dead")]
async fn dead_jobs(cx: &Cx, Form(params): Form<PaginationParams>) -> Result<impl View + '_> {
    let data = handlers::dead_jobs(app_context::<OxanaWebState>(cx).clone(), params).await?;
    Ok(data.into_view(cx))
}

#[page("/retries")]
async fn retry_jobs(cx: &Cx, Form(params): Form<PaginationParams>) -> Result<impl View + '_> {
    let data = handlers::retry_jobs(app_context::<OxanaWebState>(cx).clone(), params).await?;
    Ok(data.into_view(cx))
}

#[page("/jobs/{*detail_job_id}")]
async fn job_detail(cx: &Cx) -> Result<impl View + '_> {
    let data = handlers::job_detail(
        app_context::<OxanaWebState>(cx).clone(),
        path_param::<DetailJobId>(cx).collect::<Vec<_>>().join("/"),
    )
    .await?;
    Ok(data.into_view(cx))
}

#[page("/queues/{queue_key}")]
async fn queue_detail(cx: &Cx, Form(params): Form<PaginationParams>) -> Result<BoxView<'_>> {
    let data = handlers::queue_detail(
        app_context::<OxanaWebState>(cx).clone(),
        path_param::<QueueKey>(cx).to_string(),
        params,
    )
    .await?;
    match data {
        Some(data) => Ok(data.into_view(cx).boxed()),
        None => {
            let location = axum::http::HeaderValue::from_str(&format!(
                "{}/queues",
                app_context::<OxanaWebState>(cx).base_path,
            ))?;
            Ok(view! {
                (axum::http::StatusCode::SEE_OTHER)
                ((axum::http::header::LOCATION, location))
            }
            .boxed())
        }
    }
}

#[route(POST "/cron/enqueue")]
async fn enqueue_cron_job(cx: &Cx, Form(form): Form<CronEnqueueJobForm>) -> Result<SeeOther> {
    handlers::enqueue_cron_job(app_context::<OxanaWebState>(cx).clone(), form).await
}

#[route(POST "/on-demand/enqueue")]
async fn enqueue_on_demand_job(
    cx: &Cx,
    Form(form): Form<OnDemandEnqueueJobForm>,
) -> Result<SeeOther> {
    handlers::enqueue_on_demand_job(app_context::<OxanaWebState>(cx).clone(), form).await
}

#[route(POST "/dead/revive_all")]
async fn revive_all_dead(cx: &Cx) -> Result<SeeOther> {
    handlers::revive_all_dead(app_context::<OxanaWebState>(cx).clone()).await
}

#[route(POST "/dead/wipe")]
async fn wipe_dead(cx: &Cx) -> Result<SeeOther> {
    handlers::wipe_dead(app_context::<OxanaWebState>(cx).clone()).await
}

#[route(POST "/retries/retry_all_now")]
async fn retry_all_now(cx: &Cx) -> Result<SeeOther> {
    handlers::retry_all_now(app_context::<OxanaWebState>(cx).clone()).await
}

#[route(POST "/enqueue")]
async fn enqueue_job(cx: &Cx, Form(form): Form<EnqueueJobForm>) -> Result<SeeOther> {
    handlers::enqueue_job(app_context::<OxanaWebState>(cx).clone(), form).await
}

#[route(POST "/queues/{queue_key}/pause")]
async fn pause_queue(cx: &Cx) -> Result<SeeOther> {
    handlers::pause_queue(
        app_context::<OxanaWebState>(cx).clone(),
        path_param::<QueueKey>(cx).to_string(),
    )
    .await
}

#[route(POST "/queues/{queue_key}/unpause")]
async fn unpause_queue(cx: &Cx) -> Result<SeeOther> {
    handlers::unpause_queue(
        app_context::<OxanaWebState>(cx).clone(),
        path_param::<QueueKey>(cx).to_string(),
    )
    .await
}

#[route(POST "/queues/{queue_key}/concurrency")]
async fn set_queue_concurrency(
    cx: &Cx,
    Form(form): Form<QueueConcurrencyForm>,
) -> Result<SeeOther> {
    handlers::set_queue_concurrency(
        app_context::<OxanaWebState>(cx).clone(),
        path_param::<QueueKey>(cx).to_string(),
        form,
    )
    .await
}

#[route(POST "/queues/{queue_key}/wipe")]
async fn wipe_queue(cx: &Cx) -> Result<SeeOther> {
    handlers::wipe_queue(
        app_context::<OxanaWebState>(cx).clone(),
        path_param::<QueueKey>(cx).to_string(),
    )
    .await
}

#[route(POST "/queues/{queue_key}/jobs/{job_id}/delete")]
async fn delete_job(cx: &Cx) -> Result<SeeOther> {
    handlers::delete_job(
        app_context::<OxanaWebState>(cx).clone(),
        path_param::<QueueKey>(cx).to_string(),
        path_param::<JobId>(cx).to_string(),
    )
    .await
}
