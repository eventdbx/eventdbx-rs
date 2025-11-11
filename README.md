# EventDBX Client

Async Rust client for EventDBX. The library wraps the Cap'n Proto
wire protocol exposed by EventDBX so applications can programmatically list aggregates
and events, append or patch data, select subsets of fields, toggle archive status, and
verify Merkle roots.

## Features

- Tokio-based TCP client with automatic hello handshake and per-request timeouts.
- High-level APIs for list/get/select aggregate queries, list events, append/create/patch
  events, and archive toggling.
- Strongly typed request builders backed by `serde_json::Value` to make working with JSON
  payloads ergonomic.
- Reusable `ClientConfig` so multiple clients can share the same runtime settings.

## Requirements

- Rust 1.75+ (edition 2024) with Cargo.
- Access to an EventDBX control server endpoint and credentials (token + tenant).
- The Cap'n Proto schema lives in `proto/control.capnp`; the build script compiles it
  automatically, so no manual `capnp compile` invocation is needed.

## Building

```bash
cargo build
```

Running `cargo check` is usually faster for edit/compile cycles:

```bash
cargo check
```

## Example Usage

```rust
use eventdbx_client::{
    AppendEventRequest, ClientConfig, EventDbxClient, ListAggregatesOptions, PatchEventRequest,
};
use serde_json::json;

#[tokio::main]
async fn main() -> eventdbx_client::Result<()> {
    let config = ClientConfig::new("127.0.0.1", "<token>")
        .with_tenant("tenant-123") // custom tenant
        .with_port(7000);          // custom port, 6363
    let client = EventDbxClient::connect(config).await?;

    // list aggregates
    let aggregates = client
        .list_aggregates(ListAggregatesOptions::default())
        .await?;
    println!("Aggregates: {}", aggregates.aggregates);

    // append a new event
    let append = AppendEventRequest::new(
        "person",
        "p-110",
        "person_status_updated",
        json!({ "status": "active" }),
    );
    client.append_event(append).await?;

    // patch an existing event with JSON Patch semantics
    let patch = PatchEventRequest::new(
        "person",
        "p-110",
        "person_status_updated",
        json!([{ "op": "replace", "path": "/status", "value": "inactive" }]),
    );
    client.patch_event(patch).await?;

    Ok(())
}
```

See `src/main.rs` for `dbxtest-cli`, a small binary that can exercise individual API calls.
Configure it via environment variables (or pass `--host/--port/--token/--tenant`):

- `EVENTDBX_HOST` (required)
- `EVENTDBX_PORT` (optional, defaults to `6363`)
- `EVENTDBX_TOKEN` (required)
- `EVENTDBX_TENANT` (optional, defaults to `"default"`)

Available commands mirror the client surface area: `list`, `select`, `get`, `events`, `append`,
`patch`, `create`, `archive`, and `verify`. Any argument you omit is filled with synthetic data
generated via the `fake` crate, which makes it easy to sanity-check serialization. Examples:

```bash
cargo run list --take 5
cargo run append --aggregate-type person --aggregate-id p-1 \
  --event-type person_created --payload '{"status":"active"}'
cargo run verify --aggregate-type person --aggregate-id p-1
```

## Updating the Schema

1. Modify `proto/control.capnp`.
2. Run `cargo build` (or `cargo check`). The build script scans `proto/` and regenerates
   `control_capnp.rs` inside `OUT_DIR`.
3. Re-run tests or your application.

## Testing

```bash
cargo test
```

No integration tests are checked in yet, but the command validates the build and runs any
future unit tests. Combine with `cargo fmt --check` in CI to ensure formatting.

## Contributing

1. Run `cargo fmt` & `cargo clippy --all-targets` before submitting patches.
2. Ensure any schema changes are reflected in `proto/control.capnp` and the corresponding
   client/type definitions.
3. Document new APIs in this README or Rustdoc comments where appropriate.
