# CI pipeline (issue #4)

Adds `.github/workflows/ci.yml`: on every pull request and on pushes to `main`.

| Job | What it runs |
| --- | --- |
| `lint` | `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` |
| `test` | `cargo test` (61 non-ignored tests) |
| `test-postgres` | `postgres:17` service + `scripts/ci/postgres-tests.sh` (the `#[ignore]`d DB tests) |
| `python` | `tests/local_monitor_test.py`, `scripts/databricks/test_transport_state.py` (unittest) |
| `features` | `cargo check --features openvino-gpu` (Ubuntu) |
| `features-macos` | `cargo check --features apple-coreml`; manual runs only (Actions → CI → Run workflow) |

`features-macos` is manual because macOS runners can queue for a long time. On 2026-11-17 one
attempt waited 15 minutes without a runner and was cancelled. Run it before merging changes to
`ort`/`fastembed` versions or to the `apple-coreml` code in `src/generation/`.

The Rust toolchain is pinned to 1.97 in the workflow. Builds are cached with `Swatinem/rust-cache`.

## Running the Postgres tests locally

```sh
PGHOST=localhost PGUSER=postgres PGPASSWORD=postgres scripts/ci/postgres-tests.sh            # all
PGHOST=localhost PGUSER=postgres PGPASSWORD=postgres scripts/ci/postgres-tests.sh lib        # one binary
```

Each test binary gets a fresh database. The tests are picky about the DSN:

- `analytics_recovery` needs a database named `scout_task15_*`, as a URL.
- `recrawl_policy_verification` needs a database named exactly `scout_ticket10_20261014`, as a URL.
- `distributed_rpc_*` (ticket 8 helpers) need a URL, because they add `application_name` as a query pair.
- `distributed_recovery` needs the keyword form (`host=... dbname=...`), because it appends ` application_name=...`.

## Local verification (2026-11-17)

I ran this in a cloud container on PostgreSQL 16.15. Postgres 17 could not be installed there; CI uses 17.

- clippy: clean after the fixes below. `cargo test`: 61 passed.
- Postgres suite: all 51 passed. Without `recrawl_policy_verification` it took about 2m17s, after a
  warm build.

## Code changes needed for green

- Clippy: applied the mechanical fixes (collapsible `if`, redundant `.ok()`, `RangeInclusive::contains`,
  needless deref). Boxed the large `ReadyPermit::Pending` variant. Removed an unreachable `"pending"`
  match pattern. Marked two unused items with `#[expect(dead_code)]` (`ScheduleAdjustment`,
  `discard_unstarted_run`). Allowed `too_many_arguments` and `type_complexity` crate-wide in `Cargo.toml`.
- `frontier::tests::postgres_shared_request_policy_contract` used 100–300 ms permit, claim and
  spacing windows. Its "still live" checks failed whenever a few DB round-trips on a busy runner took
  longer than that, and in CI it failed at a different spot on each run.
  - The limits, attempt-cap and round-robin sections test concurrency and fairness, not expiry. They
    now use a 5 s request timeout.
  - The spacing-restart section uses a 1 s spacing window, measured from the first grant, and asserts
    at least 900 ms.
  - The worker-death section does test expiry. It uses a 1 s lifetime and sleeps until a deadline
    measured from each grant or claim, instead of using fixed sleeps.
  - Checked under load: with 8 CPU-burning processes running, the old version failed 5 of 5 runs and
    the new version passed 8 of 8. The full Postgres suite also passes under the same load.
- `distributed_recovery::all_request_classes_obey_shared_policy_and_terminal_failures_continue`: this
  test asserted that the run stays `active`. That assertion predates `complete_run_if_quiescent`
  (de86d6b), which now completes a frontier-exhausted run. The test now waits for
  `completed`/`frontier_exhausted`.
- `distributed_recovery::durable_events_follow_commit_race_crash_replay_and_snapshot_publication`
  failed on the first CI run. Right after the snapshot ack became visible, it asserted that the
  consumer had already exited. The consumer exits only after it commits that ack, so this is a race.
  The test now waits for the exit (bounded by the 40 s `wait` helper), then still asserts success.
- `recrawl_policy_verification` read its fixture from
  `docs/runs/ticket09-history-2026-10-14/`. That path is gitignored, so the file only existed on the
  author's machine and the test could not run from a clean checkout. It now reads a committed
  synthetic fixture, `tests/fixtures/recrawl/completed-history-recommendation.json`. The fixture has
  the Ticket 09 shape: completed history, 3 snapshots, 24 of 48 pages changed, and 24h→18h at the 25%
  cap. Its scope and extraction hashes are computed from the test's own Source config. With a corrupted
  scope hash, the test fails (`out_of_scope`).

## Follow-ups

- Ruff is not added yet. Even the default rules report 275 findings, mostly F821 from Databricks
  globals such as `spark`.
- Turn on branch protection for `main` and require these checks.
