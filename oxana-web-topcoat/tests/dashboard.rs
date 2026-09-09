use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    response::Response,
};
use oxana_web_topcoat::{OxanaWebState, router, topcoat_router};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

#[derive(Serialize)]
struct DefaultQueue;

impl oxana::Queue for DefaultQueue {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("default").dynamic_concurrency(2)
    }
}

#[derive(Serialize)]
struct TenantQueue;

impl oxana::Queue for TenantQueue {
    fn key(&self) -> String {
        "tenant#acme/space & more".to_string()
    }

    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_dynamic("tenant").dynamic_concurrency(1)
    }
}

#[derive(Debug, Serialize, Deserialize, oxana::Job)]
#[oxana(on_demand)]
#[oxana(unique_id = "item/{id}")]
struct DemoJob {
    id: u64,
    payload: String,
}

struct DemoWorker;

impl oxana::FromContext<()> for DemoWorker {
    fn from_context(_: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<DemoJob> for DemoWorker {
    type Error = std::io::Error;

    async fn run_batch(&self, _: Vec<oxana::BatchItem<DemoJob>>) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn state(base_path: &str) -> OxanaWebState {
    let storage = oxana::Storage::builder()
        .namespace(format!("oxana-topcoat-test-{}", uuid::Uuid::new_v4()))
        .build_from_redis_url(
            std::env::var("REDIS_URL").expect("REDIS_URL must point to a test Redis"),
        )
        .unwrap();
    let mut catalog = storage
        .runtime(())
        .queue::<DefaultQueue>()
        .queue::<TenantQueue>()
        .worker::<DemoWorker, DemoJob>()
        .catalog();
    catalog.cron_workers.push(oxana::CronWorkerInfo {
        name: "demo::CronWorker".to_string(),
        schedule: "*/5 * * * * *".parse().unwrap(),
        queue_key: "default".to_string(),
        resurrect: false,
    });
    OxanaWebState::new(storage, catalog, base_path.to_string())
}

async fn get(app: &Router, uri: &str) -> Response {
    app.clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn post(app: &Router, uri: &str, fields: &[(&str, &str)]) -> Response {
    let body = fields
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn html(response: Response) -> String {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "text/html; charset=utf-8"
    );
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

fn assert_redirect(response: &Response, location: &str) {
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()["location"], location);
}

#[tokio::test]
async fn every_page_renders_when_mounted_and_links_keep_the_prefix() {
    let state = state("/admin/oxana");
    let app = Router::new().nest("/admin/oxana", router(state));
    for path in [
        "",
        "/busy",
        "/queues?sort=key&dir=asc&minutes=120",
        "/queues/default",
        "/metrics?minutes=240",
        "/metrics/job?worker=demo%3A%3AWorker&minutes=1440",
        "/cron",
        "/on-demand",
        "/scheduled?page=2",
        "/retries",
        "/dead",
        "/jobs/missing%2Fjob",
    ] {
        let body = html(get(&app, &format!("/admin/oxana{path}")).await).await;
        assert!(body.contains("Oxana Dashboard"), "{path}");
        assert!(body.contains("href=\"/admin/oxana/queues\""), "{path}");
        assert!(!body.contains("href=\"/queues"), "{path}");
        assert!(!body.contains("{{"), "unconverted template on {path}");
    }
    assert_eq!(
        get(&app, "/admin/oxana/unknown").await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, "/admin/oxana/dead/wipe").await.status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    let redirect = get(&app, "/admin/oxana/queues/tenant").await;
    assert_eq!(redirect.status(), StatusCode::SEE_OTHER);
    assert_eq!(redirect.headers()["location"], "/admin/oxana/queues");
}

#[tokio::test]
async fn native_router_and_axum_root_mount_render_without_empty_home_links() {
    let state = state("");
    let native = topcoat_router(state.clone());
    let response = native
        .handle(
            Request::builder()
                .uri("/cron")
                .body(topcoat::router::Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = html(get(&router(state), "/on-demand").await).await;
    assert!(body.contains("href=\"/\""));
    assert!(body.contains("action=\"/on-demand/enqueue\""));
}

#[tokio::test]
async fn queue_controls_and_encoded_job_links_operate_on_the_selected_queue() {
    let state = state("/admin");
    let storage = state.storage.clone();
    let app = Router::new().nest("/admin", router(state));
    let queue = "/admin/queues/tenant%23acme%2Fspace%20%26%20more";
    let job_id = storage
        .enqueue(
            TenantQueue,
            DemoJob {
                id: 1,
                payload: "<script>alert('job')</script>".to_string(),
            },
        )
        .await
        .unwrap();
    let body = html(get(&app, queue).await).await;
    assert!(body.contains("&lt;script&gt;"));
    let job_link = format!("/admin/jobs/{}", urlencoding::encode(&job_id));
    assert!(body.contains(&format!("href=\"{job_link}\"")));
    let detail = html(get(&app, &job_link).await).await;
    assert!(detail.contains(&job_id));
    assert!(detail.contains("&lt;script&gt;"));

    assert_redirect(&post(&app, &format!("{queue}/pause"), &[]).await, queue);
    assert_eq!(
        storage
            .queue_config(TenantQueue)
            .await
            .unwrap()
            .unwrap()
            .state,
        oxana::QueueState::Paused
    );
    assert_redirect(&post(&app, &format!("{queue}/unpause"), &[]).await, queue);
    assert_eq!(
        storage
            .queue_config(TenantQueue)
            .await
            .unwrap()
            .unwrap()
            .state,
        oxana::QueueState::Active
    );
    assert_redirect(
        &post(
            &app,
            &format!("{queue}/concurrency"),
            &[("concurrency", "4")],
        )
        .await,
        queue,
    );
    assert_eq!(
        storage
            .queue_config(TenantQueue)
            .await
            .unwrap()
            .unwrap()
            .concurrency,
        Some(4)
    );
    let invalid = post(
        &app,
        &format!("{queue}/concurrency"),
        &[("concurrency", "0")],
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        storage
            .queue_config(TenantQueue)
            .await
            .unwrap()
            .unwrap()
            .concurrency,
        Some(4)
    );

    assert_redirect(
        &post(
            &app,
            &format!("{queue}/jobs/{}/delete", urlencoding::encode(&job_id)),
            &[],
        )
        .await,
        queue,
    );
    assert_eq!(storage.enqueued_count(TenantQueue).await.unwrap(), 0);
    storage
        .enqueue(
            TenantQueue,
            DemoJob {
                id: 2,
                payload: "wipe me".to_string(),
            },
        )
        .await
        .unwrap();
    assert_redirect(&post(&app, &format!("{queue}/wipe"), &[]).await, queue);
    assert_eq!(storage.enqueued_count(TenantQueue).await.unwrap(), 0);
}

#[tokio::test]
async fn enqueue_forms_preserve_payloads_and_redirect_to_their_pages() {
    let state = state("/admin");
    let storage = state.storage.clone();
    let app = Router::new().nest("/admin", router(state));
    assert_redirect(
        &post(&app, "/admin/cron/enqueue", &[("name", "demo::CronWorker")]).await,
        "/admin/cron?enqueued=1",
    );
    assert_redirect(
        &post(
            &app,
            "/admin/on-demand/enqueue",
            &[
                ("name", std::any::type_name::<DemoJob>()),
                ("queue", "default"),
                ("args", r#"{"id":42,"payload":"<hello> & friends"}"#),
            ],
        )
        .await,
        "/admin/on-demand?scheduled=1",
    );
    assert_eq!(storage.enqueued_count(DefaultQueue).await.unwrap(), 2);
    assert_redirect(
        &post(
            &app,
            "/admin/on-demand/enqueue",
            &[
                ("name", std::any::type_name::<DemoJob>()),
                ("queue", "default"),
                ("args", "{"),
            ],
        )
        .await,
        "/admin/on-demand?invalid_json=1",
    );
    assert_eq!(storage.enqueued_count(DefaultQueue).await.unwrap(), 2);

    assert_redirect(
        &post(
            &app,
            "/admin/enqueue",
            &[
                ("queue", "default"),
                ("name", "demo::Revived"),
                ("args", r#"{"id":7}"#),
                ("state", r#"{"cursor":1,"total":10}"#),
                ("redirect", "/dead"),
            ],
        )
        .await,
        "/admin/dead",
    );
    let jobs = storage
        .list_queue_jobs(
            DefaultQueue,
            &oxana::QueueListOpts {
                count: 10,
                offset: 0,
            },
        )
        .await
        .unwrap();
    let revived = jobs
        .iter()
        .find(|job| job.job.name == "demo::Revived")
        .unwrap();
    assert_eq!(revived.job.args, serde_json::json!({"id":7}));
    assert_eq!(
        revived.meta.state,
        Some(serde_json::json!({"cursor":1,"total":10}))
    );
    let on_demand = jobs
        .iter()
        .find(|job| job.job.name == std::any::type_name::<DemoJob>())
        .unwrap();
    assert_eq!(on_demand.job.args["payload"], "<hello> & friends");

    for (action, location) in [
        ("dead/revive_all", "/admin/dead"),
        ("dead/wipe", "/admin/dead"),
        ("retries/retry_all_now", "/admin/retries"),
    ] {
        assert_redirect(
            &post(&app, &format!("/admin/{action}"), &[]).await,
            location,
        );
    }
    storage.wipe_queue(DefaultQueue).await.unwrap();
}
