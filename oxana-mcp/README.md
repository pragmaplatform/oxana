# Oxana MCP

Read-only production monitoring for MCP clients such as Codex, in a separate
crate from the worker runtime and web dashboard. Uses the official Rust MCP SDK
and Streamable HTTP at `/mcp`.

## Run the example

Requires Rust 1.89 or newer and a running Redis instance containing Oxana data.
Use the same Redis URLs and namespace as your workers.

```sh
export REDIS_URL=redis://127.0.0.1:6379
export OXANA_MCP_TOKEN="$(openssl rand -hex 32)"
cargo run -p oxana-mcp --example server
```

The example listens on `127.0.0.1:8081`. Optional environment variables:

| Variable | Purpose |
| --- | --- |
| `REDIS_STATS_URL` | Separate statistics Redis, if your workers use one |
| `OXANA_NAMESPACE` | Worker namespace; defaults to Oxana's default namespace |
| `OXANA_MCP_BIND` | Listening address, e.g. `0.0.0.0:8081` inside a container |
| `OXANA_MCP_HOST` | Accepted public hostname, e.g. `oxana.example.com`; defaults to SDK loopback hosts |

For remote access, serve behind an HTTPS reverse proxy, preserve the configured
Host header, and forward Authorization. The example rejects browser Origin
headers; native MCP clients do not need one. Set an explicit origin allowlist
when embedding the router for browser clients.

## Connect Codex

Make the same `OXANA_MCP_TOKEN` available in the environment of the Codex process.
Add to `~/.codex/config.toml`:

```toml
[mcp_servers.oxana]
url = "http://127.0.0.1:8081/mcp"
bearer_token_env_var = "OXANA_MCP_TOKEN"
```

Use your HTTPS URL for production. Restart the client after changing its
configuration. This uses a preconfigured bearer token, not OAuth: no
`codex mcp login` step is needed. See the [Codex MCP documentation](https://learn.chatgpt.com/docs/extend/mcp?surface=cli).

## Embed in an Axum app

```rust,no_run
use oxana_mcp::{router, StreamableHttpServerConfig};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let storage = oxana::Storage::from_env()?;
let transport = StreamableHttpServerConfig::default()
    .with_allowed_hosts(["oxana.example.com"])
    .enforce_origin_validation();
let mcp = router(storage, std::env::var("OXANA_MCP_TOKEN")?, transport)?;
let app = axum::Router::new().merge(mcp);
# Ok(())
# }
```

`router` validates the token at construction and checks Authorization on every
request. Invalid or missing credentials receive HTTP 401 with a Bearer
challenge. The token permits all monitoring tools for the supplied storage
namespace; there are no mutation tools or per-user permissions. To rotate it,
replace the configured secret and restart the server and clients. Never commit
the secret or put it in a URL. The transport's cancellation token can be used
for server shutdown.

## Tools

| Tool | Inputs | Result |
| --- | --- | --- |
| `get_overview` | None | Global counts, queues, active processes, processing jobs |
| `list_queues` | None | Queue and dynamic-queue statistics |
| `list_processes` | None | Active processes and heartbeats |
| `get_job` | `id` | Job metadata, or null when absent |
| `list_jobs` | `list`, optional `queue`, `limit`, `offset` | Paginated job metadata |
| `get_job_metrics` | Optional `minutes` | Worker execution metrics |
| `get_queue_metrics` | Optional `minutes` | Queue length history |

`list_jobs.list` is `queue`, `dead`, `retries`, or `scheduled`. `queue` is required
only for the queue list and must be an exact queue key. The default limit is 20,
with a maximum of 100; offset defaults to zero. Live lists may change between
pages. Metrics use Oxana's lookback defaults: omitted or zero minutes means 60,
and values above 1440 are capped to the 24-hour retention window.

Successful results contain `namespace`, `observed_at` (RFC 3339), and `data`,
both as structured MCP output and JSON text. Job timestamps are Unix
microseconds; process timestamps are Unix seconds. Job arguments and resumable
state are always omitted. Job IDs, queue/worker names and error messages remain
visible and may contain application-specific information. Overview and metrics
are aggregate queries; only job lists are paginated.

## Tests

```sh
REDIS_URL=redis://127.0.0.1:6379 cargo test -p oxana-mcp
```

The storage integration test uses a unique namespace and deletes its jobs.
Authentication, protocol discovery, and input-validation tests do not need Redis.
