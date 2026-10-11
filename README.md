# Scout

Scout crawls allowlisted technical documentation and serves keyword, semantic, or hybrid search
results through an HTTP API. Crawls produce immutable snapshots; search indexes are built from a
snapshot and activated separately.

## Quick start

Requires Rust/Cargo. Crawling needs network access. Semantic and hybrid indexing downloads and
caches the BGE Small embedding model on first use.

1. Build Scout:

   ```bash
   cargo build --release
   ```

2. Create a `sources.toml` file that lists the sites Scout may crawl. Each source needs seeds,
   allowed origins, and path prefixes. See [`data/configs/sources-500.toml`](data/configs/sources-500.toml)
   for an example. Scout respects `robots.txt` and stays within the configured URL boundaries.

3. Crawl, build an index, activate it, and start the API:

   ```bash
   BIN=target/release/scout
   DATA=./data

   $BIN crawl --config sources.toml --data-dir "$DATA"
   # Set snapshot_id in data/config/index.toml to the ID printed by crawl.
   # Use the profile example in data/configs/archive/ as a starting point.
   $BIN index build --corpus <snapshot-id> --data-dir "$DATA" --mode hybrid
   $BIN index activate --generation <generation-id> --data-dir "$DATA"
   $BIN serve --data-dir "$DATA" --bind 127.0.0.1:8080
   ```

The index profile is required and pins the snapshot, extraction settings, and model. Builds default
to `keyword-only`; choose `--mode hybrid` to enable semantic search too.

## Search API

```bash
curl -X POST http://127.0.0.1:8080/v1/search \
  -H 'content-type: application/json' \
  -d '{"query":"ownership and borrowing","mode":"hybrid","limit":10}'
```

Modes are `keyword`, `semantic`, and `hybrid`. Results group matching passages by page and include
the page URL, title, heading, and snippet. Other endpoints: `GET /v1/healthz`, `GET /v1/readyz`,
and `GET /v1/generation`.

## Distributed crawling

For a local menu-driven workflow, run `scripts/distributed-local-ui.sh`. For direct local setup and
control, see `scripts/distributed-local.sh`. Distributed crawling requires PostgreSQL; bind the
coordinator to a trusted private interface and restrict worker access.

## Development

Run the test suite with `cargo test --offline`.

The HTTP server has no TLS or authentication. Bind it to a trusted interface or put it behind a
trusted proxy.
