# Oxana Web with Topcoat

`oxana-web-topcoat` implements the Oxana dashboard with Topcoat routing and Rust
views. It is an alternative to `oxana-web` and preserves its pages, forms, charts,
and queue controls. It requires Rust 1.98 and Topcoat 0.8.0.

Mount it in an Axum app using the same API as `oxana-web`:

```rust
use oxana_web_topcoat::{OxanaWebState, router};

let catalog = runtime.catalog();
let dashboard = router(OxanaWebState::new(
    storage.clone(),
    catalog,
    "/oxana".to_string(),
));
let app = axum::Router::new().nest("/oxana", dashboard);
```

For a dashboard at `/`, use an empty `base_path` and serve `router(state)`
directly. `topcoat_router(state)` also exposes the native Topcoat router, with
routes relative to the dashboard root. An embedding host must strip the mount
prefix from requests; `base_path` supplies that prefix for links and redirects.

Run the example with Redis:

```sh
REDIS_URL=redis://localhost:6379 cargo run -p oxana-web-topcoat --example web
```

Open <http://localhost:3000/oxana>. The example starts workers and seeds sample
jobs, including dynamic queues and progress reporting. Like `oxana-web`, the UI
loads Tailwind CSS and uPlot from their existing CDN URLs. Other scripts and CSS
are embedded in the crate; no frontend build step is needed.

Run the tests and format views:

```sh
REDIS_URL=redis://localhost:6379 cargo test -p oxana-web-topcoat --all-targets
topcoat fmt 'oxana-web-topcoat/src/views/*.rs'
cargo fmt --all
```

Topcoat's formatter handles HTML inside `view!`; `cargo fmt` formats the
surrounding Rust. Topcoat is experimental, so the dependency is pinned to 0.8.0.
