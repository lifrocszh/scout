# Recrawl recommendations

Run `recrawl_recommendations.sql` after snapshot history and live crawl-event
ingestion. It reads complete validated snapshot history and accepted event
batches. It writes no policy changes and does not call Scout's scheduler.

## Freeze one recommendation generation

The SQL creates `scout_recrawl_recommendation_inputs`. Insert one row for each
`(generation_id, source_id)` and keep that row unchanged. Use a new generation
ID when policy settings or evidence references change. Timestamps use the
second-precision RFC 3339 UTC form `YYYY-MM-DDTHH:mm:ssZ`;
`expected_policy_version` is an opaque, nonempty string matched exactly by
later policy validation.

Intervals are hours. Minimum/current/maximum must be positive, ordered, and
rounded to 0.001 hour. `max_adjustment_fraction` is 0–0.5, rounded to six
decimals. Supply 3–5 exact completed `snapshot_id` values in
`completed_snapshot_ids_json`, or `[]` for a provisional-only suggestion.
Snapshot references must belong to the Source and name complete ingestion
batches. The view only treats the set as supported when all selected snapshots
have the same Source scope and extraction contract, `frontier_exhausted`, and
zero fetch failures and rejections. It counts Page changes only when both the
current and previous snapshot are in the supplied set.

For an in-flight suggestion, optionally supply `provisional_run_id` and a
JSON array of 1–1000 exact `capture_committed` event IDs in
`provisional_event_ids_json`. They must resolve to accepted or duplicate rows
from complete event batches, for that run and Source. The run must have no
`crawl_completed` or `corpus_snapshot_published` event at `generated_at_utc`,
and its latest accepted event must be within
`maximum_provisional_event_age_seconds`. These captures cannot prove Page
admission or content change: the suggestion holds the current interval and is
marked `provisional` for explicit review. Set
`completed_snapshot_ids_json` to `[]` for provisional input; the two evidence
modes cannot be mixed in one row.

The exact selected IDs make a generation repeatable. The views use UTC session
time. The validation view intentionally checks expiry against the current
time, so an otherwise unchanged recommendation becomes stale when it expires.

## Heuristic

Completed history needs the supplied minimum of 3–5 complete, compatible
snapshots and the supplied minimum number of comparable Page observations.
The default fixture uses 20 observations. Change fraction is
`changed_page_observations / comparable_page_observations`:

- At 25% or more, raw target is half the current interval.
- At 2% or less, raw target is 1.5 times the current interval.
- Between the thresholds, raw target stays current.

Then cap the adjustment to `max_adjustment_fraction` from current and clamp it
to the supplied minimum/maximum. Quantize proposals to 0.001 hour toward the
current interval, so quantization cannot exceed the adjustment cap. Adjustment
fraction is signed and rounded to six decimals. Confidence is `high` only with at least four selected snapshots and
100 comparable observations; otherwise supported history is `medium`.
Provisional output is `provisional`; unsupported evidence is `low` and holds
the current interval. No model, embedding, training, or paid API is used.

## Recommendation JSON and identity

`gold_recrawl_recommendations.recommendation_json` uses schema
`scout.recrawl-recommendation.v1` with these fields:

- `schema_version`, fixed as `scout.recrawl-recommendation.v1`;
  `recommendation_id` and `idempotency_key`, identical
  `sha256:<64 lowercase hex>` strings; and `idempotency_basis`, a string.
- `generation_id`, `source_id`, `generated_at_utc`, `expires_at_utc`, and
  opaque `expected_policy_version`.
- `adjustment_type` (`increase`, `decrease`, `hold`) and `interval_hours`,
  containing numeric `minimum`, `current`, `maximum`, `proposed`, signed
  `adjustment` in hours, and signed `adjustment_fraction`.
- `evidence`, with `status` (`completed_history`, `provisional`, or
  `unsupported`), sorted `snapshot_refs` (each has `snapshot_id`,
  `source_snapshot_manifest_sha256`, `crawl_manifest_sha256`,
  `observed_at_utc`), nullable `provisional_run_id`, sorted
  `provisional_event_ids`, nullable `scope_sha256` and
  `extraction_contract_sha256`, `latest_observed_at_utc`,
  `latest_capture_event_at_utc`, `latest_run_event_at_utc`, comparable and
  changed Page counts, and nullable `change_fraction`. For provisional-only
  evidence, scope/extraction hashes and `latest_observed_at_utc` are null;
  `latest_capture_event_at_utc` and `latest_run_event_at_utc` are populated.
  Event timestamps are null for completed-history evidence. Unsupported
  evidence may have null or partial freshness fields.
- `confidence` (`high`, `medium`, `provisional`, or `low`) and ordered string
  array `reasons`.

`idempotency_basis` is the UTF-8 byte string returned by Spark
`TO_JSON(NAMED_STRUCT(...))`; SHA-256 is
`SHA2(idempotency_basis, 256)`. Its canonical keys and order are:

1. Top level: `schema_version`, `generation_id`, `source_id`,
   `generated_at_utc`, `expires_at_utc`, `expected_policy_version`,
   `interval_hours`, `recommendation_settings`, `evidence`.
2. `interval_hours`: `minimum`, `current`, `maximum`, `proposed`,
   `adjustment`, `adjustment_fraction`.
3. `recommendation_settings`: `max_adjustment_fraction`,
   `minimum_completed_snapshots`, `minimum_comparable_page_observations`,
   `maximum_evidence_age_days`, `maximum_provisional_event_age_seconds`,
   `change_fraction_decrease_threshold`, `change_fraction_increase_threshold`,
   `decrease_target_multiplier`, `increase_target_multiplier`,
   `high_confidence_minimum_snapshots`,
   `high_confidence_minimum_comparable_page_observations`.
4. `evidence`: `status`, `snapshot_ids`, `snapshot_refs`,
   `provisional_run_id`, `provisional_event_ids`, `scope_sha256`,
   `extraction_contract_sha256`,
   `latest_observed_at_utc`, `latest_capture_event_at_utc`,
   `latest_run_event_at_utc`, `comparable_page_observations`,
   `changed_page_observations`, `change_fraction`.

Every scalar in the basis is a JSON string. Interval values have exactly three
decimals; fractions and adjustment thresholds have six decimals; counts, bounds,
and multipliers are base-10 strings without padding. Missing scalar evidence is
the empty string. `snapshot_refs` is a JSON array of objects, each ordered as
`snapshot_id`, `source_snapshot_manifest_sha256`, `crawl_manifest_sha256`,
`observed_at_utc`, sorted by `snapshot_id`; an empty set is `[]`. In the basis,
`snapshot_ids` and `provisional_event_ids` preserve every supplied reference,
sorted and comma-joined into strings. `snapshot_refs` adds the resolved
manifest hashes and observation times when the selected snapshots are known.
SQL emits
compact JSON without whitespace. Input `generated_at_utc` and `expires_at_utc`
use second precision and `Z`. Snapshot ref `observed_at_utc` preserves the
stored RFC 3339 UTC string, including any fractional seconds. Normalized
`latest_*_at_utc` values use three fractional digits and `Z`. The full snapshot
tuples, event IDs, timestamps, hashes, counts, fractions,
intervals, adjustment thresholds, multipliers, and confidence thresholds are
in the basis. Hash the exact UTF-8 bytes in this field; do not parse and
re-serialize it. An importer can parse the basis afterward and compare its
values with the other envelope fields.

`scope_sha256` follows the Source scope canonicalization in
[Historical Page comparisons](snapshot-history.md). `extraction_contract_sha256`
uses the extraction-policy fields documented there.

## Validate before use

```sql
SELECT recommendation_id, source_id, expected_policy_version,
       evidence_status, validation_status, validation_reason,
       proposed_interval_hours
FROM workspace.default.gold_recrawl_recommendation_validation
WHERE generation_id = 'generation-replace-me'
ORDER BY source_id;
```

The report returns:

- `valid`: supported, fresh completed history.
- `provisional_review_required`: accepted active-run captures; must be reviewed
  after final Corpus reconciliation.
- `invalid`: malformed timestamps, policy bounds, ID lists, or duplicate
  generation/Source input.
- `stale`: expired recommendation or completed evidence older than its bound.
- `out_of_scope`: Source is unknown to the supplied evidence, or a referenced
  snapshot/event belongs to another Source or run.
- `unsupported`: sparse or incomparable history, incomplete crawls, too few
  comparable Pages, terminal/missing provisional run, or a same-run event
  other than `capture_committed`. A capture ref from another Source or run is
  `out_of_scope`.

Only `valid` is completed-history evidence. Ticket 10 owns policy acceptance,
approval, and scheduling integration. This SQL never changes recrawl
scheduling.

## Scout policy operator workflow

The local policy store is PostgreSQL-backed and starts disabled. Each Source
must have an actor-recorded policy before scheduled mode can select it. Use a
new policy version when changing its interval, bounds, or allowlist scope:

```sh
scout recrawl configure --config ./sources.toml --source-id docs \
  --policy-version recrawl-policy.v1 --current-hours 168 \
  --minimum-hours 24 --maximum-hours 720 \
  --max-interval-decrease-fraction 0.2 --enable-source \
  --actor operator@example.org --reason 'Approve initial Docs schedule'
scout recrawl enable --enabled true --actor operator@example.org
```

Import one exported recommendation JSON file. Valid routine changes apply
automatically inside that Source's bounds and decrease cap; larger frequency
increases and provisional recommendations enter the recorded review queue.
Scope mismatches fail closed. Reconfigure a changed Source scope with an
actor, reason, and new policy version before importing recommendations for it.

```sh
scout recrawl import --config ./sources.toml --recommendation recommendation.json
scout recrawl pending
scout recrawl review --config ./sources.toml \
  --recommendation-id sha256:<recommendation-digest> --approve \
  --actor operator@example.org --reason 'Reviewed published Corpus evidence'
```

Provisional review is accepted only after the referenced Run is completed and
its Corpus snapshot publication is persisted. Scheduled coordination is opt-in
per invocation:

```sh
scout coordinator --config ./sources.toml --data-dir ./data --scheduled
```

It reserves only due Sources with a matching approved scope. If none are due,
it reports `no_sources_due` without leaving an active Run. The configured
page target still applies. At completion, only selected Sources with no
unfinished tasks and at least `max(minimum_page_quota, 1)` unique content
groups advance to their next due time; unfinished Sources remain due. The
scheduled run's durable completion event and subsequent spool materialization
include only Sources that met that completion rule. Ordinary coordinator
invocations retain manual all-Source behavior. The recrawl policy does not
change robots rules, politeness, request budgets, or crawl limits.

## Deterministic fixture

[`fixtures/recrawl-recommendations-demo.json`](fixtures/recrawl-recommendations-demo.json)
contains a bounded completed-history recommendation, an active provisional
hold, and operator-readable invalid, stale, out-of-scope, and unsupported
examples. IDs are fixture values, not ingestable Scout artifacts. SQL execution
and live Databricks workspace behavior remain unverified here.
