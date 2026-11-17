# Container image + Compose stack (issue #5)

Adds one image for every role, a Compose stack that crawls a fixture site and serves it, CI that
runs that stack, and a GHCR publish workflow.

## What was added

| File | Purpose |
| --- | --- |
| `Dockerfile`, `.dockerignore` | cargo-chef dependency layer, slim non-root runtime. One image; the subcommand picks the role. The context is an allowlist (`Cargo.*`, `build.rs`, `proto/`, `migrations/`, `src/`). |
| `compose.yaml`, `.env.example` | Core stack: `postgres:17`, `fixture`, `coordinator`, `worker` (`--scale`), one-shot `index`, `serve`. Healthchecks and `depends_on` conditions. Only `POSTGRES_PASSWORD` is required, from `.env`. `obs` and `data` profiles are noted in comments, not built. |
| `docker/` | Static fixture site (11 crawlable pages, one `robots.txt`-disallowed page, one sitemap-only page), crawl config, index profile template, `stack.sh` glue. |
| `scripts/ci/compose-smoke.sh` | Brings the stack up, asserts readiness, search results, robots/sitemap behaviour and a memory budget, always tears down. |
| `.github/workflows/ci.yml` | New `compose-smoke` job. Existing jobs are unchanged. |
| `.github/workflows/image.yml` | Publishes `ghcr.io/lifrocszh/scout` on `main` (`:<sha>`, `:latest`) and on `v*` tags. `packages: write` is set on that one job only. |
| `README.md` | "Run with Docker". |

No Rust code changed. `scripts/distributed-local.sh` is untouched.

## Design notes

- **Flow.** `coordinator` + `worker`s crawl; `stack.sh coordinator` stops the coordinator on the
  `crawl.completed` event, so the capture spool is closed before `scout crawl --materialize-spool`
  runs (the same order as `distributed-local.sh stop` then `materialize`). `index` then builds a
  keyword-only Generation, activates and verifies it. `serve` starts last. `/v1/readyz` flips from
  not ready to ready the moment `CURRENT` points at the Generation.
- **Coordinator bind.** The coordinator refuses non-private bind addresses, so `0.0.0.0` does not
  work. `stack.sh` binds the container's own address (`hostname -i`). The gRPC port is never
  published; it has no authentication.
- **No model in the image.** `fastembed` links ONNX Runtime at build time (the `cook` layer downloads
  it), but the BGE Small model is fetched on first semantic use into `<data-dir>/models`. Compose
  mounts that path as its own volume (`scout-models`), so it survives removing `scout-data`.
- **Restarts.** `docker compose up` is repeatable. A completed crawl makes the coordinator and
  workers exit 0 immediately and `index` a no-op. Workers are not gated on a healthy coordinator,
  because on a restart the coordinator exits 0 before it ever turns healthy.

## Verified locally (Docker 29.9, Compose 5.6, 4 CPUs, no cache)

| Measure | Result |
| --- | --- |
| Image size | 192 MB (`debian:trixie-slim` 120 MB + 53 MB stripped binary) |
| Cold image build | about 7 min: cargo-chef install 51 s, dependency `cook` 298 s, final compile 63 s |
| `up --scale worker=3` to ready | 7 to 10 s (Postgres, crawl of 11 pages, index, serve) |
| Work split across 3 workers | 5 / 5 / 3 tasks (13 requests: robots, sitemap, 11 pages) |
| Per-container peak memory | postgres 56 MiB, coordinator 5, workers 3 each, serve 3, fixture 1; sum 74 MiB |
| Generation | 11 pages, 32 passages; keyword search works, `hybrid` returns a clear `not_ready` error |

`compose-smoke.sh` passed 5 consecutive runs after the fixes below. Failure paths were exercised too
(memory budget too small, broken sources config): exit 1, logs dumped, no containers or volumes left.

Restart behaviour checked by hand: fresh `up`, a second `up` on the running stack, and
`down` followed by `up` with volumes kept all exit 0 and keep the same Generation.

### Bugs found by this verification

1. **Restart failed.** After a completed crawl, `up` exited 1 ("dependency failed to start:
   coordinator exited (0)") and left `serve` uncreated. Fixed with a `worker` role in `stack.sh`
   and `service_started` instead of `service_healthy`.
2. **Vacuous robots check.** The disallowed page was also too short for the crawler's 200-character
   minimum (`useful_content_below_minimum`), so the test passed even with robots handling removed.
   The page is now long enough; with the `Disallow` line removed the crawl indexes 12 pages and
   finds it, and with it 11 pages and no hit.
3. **Worker-count race.** With a 1-second crawl, a slow third worker could find the crawl finished.
   The fixture now uses 200 ms start spacing, and the smoke test requires every worker to exit 0
   and at least 2 to join.
4. **Memory sampler measured nothing.** `mawk` buffers piped input, so the first sampler passed at
   0 MiB. It is now a bash loop, and the test fails if it records nothing.

## Not verified here

- **Compose's first-run build.** This sandbox needs a proxy and a bake entitlement to build, so only
  `docker build` was run, not `docker compose up` from an empty image cache. CI also builds with
  buildx and hands Compose the finished image, so it does not cover this path either.
- **Hybrid mode and the model download.** `huggingface.co` is blocked by this sandbox's egress policy.
  The `/data/models` location was confirmed from the code (`build` and `serve` both read
  `<data-dir>/models`), not by running a download.
- **GHCR publish.** It runs only on `main` and `v*` tags, so the first real run is after merge.
- **Memory of short-lived containers.** Sampling is about once a second, so the `index` step
  (about 1 s) is not in the table. Its writer memory is capped by the profile at 16 MB.
- The 74 MiB figure is for the 11-page fixture. A real crawl grows with the corpus; the 2 GB budget
  is enforced only by the smoke test's measurement, not by container limits.

## CI results

Filled in after the first run on the pull request.

## Deferred

- arm64 images (would need QEMU or a native runner; not needed for the stated goals).
- SBOM and provenance attestations (`provenance: false`, `sbom: false` for now).
- Container memory limits, if a hard 2 GB cap is wanted.
- A GHCR package visibility check: a first publish may need the package set to public once.
- `obs` (#7) and `data` (#13, #14) profiles.
