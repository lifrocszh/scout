# Scout demo

A single-page localhost demo for the Scout search engine: pick a crawl config, watch a crawl
run, choose a snapshot, build a keyword index, activate + verify it, start `scout serve`, run
searches, and inspect the active Generation — all against a prebuilt corpus that ships with the
repo.

## Architecture

- `demo/server.py` is a small Python standard-library HTTP server that serves `demo/static/` and
  exposes a JSON API under `/api/*`.
- Every action is performed by the demo server as a subprocess call to the real `scout` binary.
  The browser never talks to `scout` directly.
- Read-only proxying: `/api/generation`, `/api/readyz`, and `/api/search` forward to the running
  `scout serve` and return its response body verbatim.
- Prebuilt data (`paths.prebuilt_data_dir`, typically `data/real-model-10k-fast-v1`) is never
  written by the demo: no corpus, index, or Generation artifact is ever created or modified there.
  The single exception is outside the data model: `scout` itself appends its own operational event
  logs under `<prebuilt>/logs/`, and `scout serve` started with root `prebuilt` does the same. That
  is harmless (plain logs, no artifact) but it is not literally untouched.
- Crawls and index builds write into a separate work dir (`paths.work_dir`); corpora, Generations,
  `CURRENT`, and job state all live there.
- The UI polls `GET /api/state` (1.5 s while a job runs, 5 s when idle) and renders from it, so a
  page reload resumes the current job view without losing anything.

## Prerequisites

- `cargo build --release --offline` has been run; the binary must exist at `target/release/scout`.
- Python 3.11+ (the demo server uses `tomllib` from the standard library).
- macOS or Linux.

## Run

```sh
python3 demo/server.py --data-dir data/real-model-10k-fast-v1 --bind 127.0.0.1:8765
```

Then open <http://127.0.0.1:8765>.

## Demo path A — fast (~30 s), prebuilt corpus

1. In **Serve**, choose root `prebuilt`, port `8788`, click **start serve**. Wait for the badge to
   read `running` and `ready: yes`.
2. The **Generation** panel shows the active Generation: 10,002 pages, 149,150 passages, badge
   `keyword only`.
3. In **Search**, type `ownership and borrowing`, leave `mode` on **keyword**, submit.
   Results render with rank, title link, source, score, heading path, and snippet — plus the
   `generation_id` and `mode` that served them.

This path exercises the prebuilt data only; it starts no crawl and no index build.

## Demo path B — live pipeline (slow)

1. In **Configs**, click **start crawl** on `sources-2k.toml`.
2. Watch the **Crawl job** card: state badge, elapsed clock, `captures / admitted / rejected /
   failed` counters, and the bounded log tail.
3. Stop the crawl with **stop crawl**, or let it finish. On completion the `snapshot_id` appears and
   the snapshot shows up in the **Snapshots** table with root `work`.
4. Select that snapshot row, click **build keyword index**.
5. Watch the **Index build** card: phase chain
   `verify_corpus → catalog → keyword → semantic → manifest → validation → validated`, page/passage
   counts, log tail.
6. When the build is `completed` with a `generation_id`, an **activate + verify** button appears.
   Click it; the result box reports `activated` and `verify` from `activate.verify.ok`. It never
   claims success unless `verify.ok` is true.
7. In **Serve**, switch root to `work`, click **start serve**, then search again.

## Warnings

- **Search always sends an explicit `mode`.** This UI does. A bare request that omits `mode`
  defaults to `hybrid` and returns `503 not_ready` against a keyword-only Generation.
- **Only the two approved configs are offered by the server.** Others are rejected with
  `invalid_config`; the UI disables the button for any config the server marks invalid.
- **The prebuilt tree is never written with demo artifacts.** Prebuilt snapshot rows show a
  disabled `read-only` button; selecting them or building from them is refused (`400 read_only`).
  `scout` still appends its own operational logs under `<prebuilt>/logs/`.
- **A 10k crawl is not part of the short demo.** Use `sources-2k.toml`; the 10k runs take far
  longer than the acceptance window.
- **Crawl stop kills only the managed child process group.** If you started anything by hand
  outside the demo server, stopping from the UI will not touch it.
- **A demo-initiated stop reports `stopped`, not `failed`.** A crawl or build the demo killed shows
  state `stopped` with no error. A negative exit code only becomes
  `failed: terminated by signal <n>` when the process died on a signal the demo did not send.

## Smoke test

```sh
python3 demo/smoke_test.py
```

Runs the endpoint checks server-side (no browser needed).

## Troubleshooting

- **Port already in use** — the serve port (default `8788`) is taken. Stop the other process or
  pick another port in the Serve card. The demo server's own bind port is set with `--bind`.
- **`scout serve` binary missing** — build it: `cargo build --release --offline`. The serve card
  reports `failed` with the error text.
- **`not_ready` right after activation** — serve is probably still pointed at the other root.
  Stop serve, set root to `work` (or `prebuilt`), start serve again, retry the search.
- **Serve shows `stale` (adopted across a restart)** — the previous demo server died (crash or
  `kill -9`) but its `scout serve` child kept running and kept the port. The state is kept so you can
  still stop it, but it is not usable. Click **stop serve** (the Stop button stays enabled), wait for
  the port to free, then **start serve** again. Starting while it is stale is refused with
  `409 conflict` on purpose — stop the adopted serve first.
- **`overloaded` from a search** — `scout serve` handles one request at a time, so a concurrent
  request is turned away. The UI retries once automatically after ~400 ms; retrying by hand works too.
- **If activation fails** — the demo refuses to activate any Generation that is not the output of a
  **completed** build in this demo, so a failed or half-finished build can never be activated. An
  `upstream_error` means `scout index activate` itself failed: its captured stderr is in the error
  message, and the build output is still in `generations/.staging/<run-id>/` under the work dir.
  `activated: true` always comes with `verify` — never trust the activation unless `verify.ok` is
  true (that is the `scout index verify` exit code).
- **Job shows `stale`** — the demo server restarted and the recorded process is gone. The job
  identity is kept for reference only; start a new crawl or build.
- **"server unreachable" banner** — the demo server stopped or the browser cannot reach it. The UI
  retries on its own every few seconds.
