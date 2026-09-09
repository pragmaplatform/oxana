//! The Oxana dashboard rendered and routed with Topcoat.
//!
//! [`router`] mounts in an Axum application in the same way as `oxana-web`.
//! [`topcoat_router`] exposes the native Topcoat router for other hosts.

mod filters;
mod handlers;
mod models;
mod pagination;
mod routes;
mod views;

const JOBS_PER_PAGE: usize = 50;

/// Storage, registered components, and the dashboard's public URL prefix.
#[derive(Clone)]
pub struct OxanaWebState {
    pub storage: oxana::Storage,
    pub catalog: oxana::Catalog,
    pub base_path: String,
}

impl OxanaWebState {
    pub fn new(storage: oxana::Storage, catalog: oxana::Catalog, base_path: String) -> Self {
        Self {
            storage,
            catalog,
            base_path: base_path.trim_end_matches('/').to_string(),
        }
    }
}

/// Creates the dashboard for mounting with `axum::Router::nest`.
///
/// Set `state.base_path` to the mount path, or to an empty string at the root.
/// Topcoat handles all dashboard requests; Axum provides the hosting adapter.
pub fn router(state: OxanaWebState) -> axum::Router {
    axum::Router::new().fallback_service(topcoat::router::tower::TowerService::new(topcoat_router(
        state,
    )))
}

/// Creates the native Topcoat router with paths relative to the dashboard root.
///
/// When hosting at the root, use an empty `base_path`. Hosts mounting it below
/// a prefix must strip that prefix from incoming paths and set `base_path` to
/// the public mount path. Views and redirects include that public prefix.
pub fn topcoat_router(state: OxanaWebState) -> topcoat::router::Router {
    routes::router(state)
}
