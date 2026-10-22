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

   Set request budgets in the same TOML. Top-level `origin_concurrency` and
   `min_start_spacing_ms` apply to every allowed origin unless `[[origins]]` overrides them:

   ```toml
   global_concurrency = 8
   origin_concurrency = 1
   min_start_spacing_ms = 500

   [[origins]]
   origin = "https://doc.rust-lang.org"
   origin_concurrency = 2
   min_start_spacing_ms = 1000

   [[origins]]
   origin = "https://docs.python.org"
   min_start_spacing_ms = 750
   ```

   Omitted top-level spacing defaults to 500 ms (at most two starts per second per
   origin); set `min_start_spacing_ms = 0` explicitly to disable spacing.
   Concurrency caps simultaneous requests independently of start spacing. Each
   `origin` must match an allowed scheme, host, and port. Omitted fields inherit
   top-level values. `global_concurrency` caps all requests. Budgets cover page,
   `robots.txt`, sitemap, redirect, and retry requests. Sources sharing a
   `politeness_group` use the strictest member spacing for group starts. Local
   crawls also cap combined requests in that group at top-level
   `origin_concurrency`; distributed crawls cap concurrency per origin.

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

## Local demo

[`demo/`](demo/) runs the full flow — crawl, snapshot, keyword index build, activate and verify,
serve, search — behind a localhost UI. See [`demo/README.md`](demo/README.md) for detail.

```bash
python3 demo/server.py --data-dir data/real-model-10k-fast-v1 --bind 127.0.0.1:8765
```

Then open `http://127.0.0.1:8765`. The demo uses the Python 3 standard library only, adds no
dependencies, and binds to loopback. Keyword search mode must be sent explicitly.

Smoke test: `python3 demo/smoke_test.py`

## Distributed crawling

For a local menu-driven workflow, run `scripts/distributed-local-ui.sh`. For direct local setup and
control, see `scripts/distributed-local.sh`. Distributed crawling requires PostgreSQL; bind the
coordinator to a trusted private interface and restrict worker access.
The same sources TOML drives `scout crawl` locally and `scout coordinator` with `scout worker`
for distributed crawls. The `distributed-local` scripts start coordinator and workers on one
machine.

## Development

Run the test suite with `cargo test --offline`.

The HTTP server has no TLS or authentication. Bind it to a trusted interface or put it behind a
trusted proxy.
