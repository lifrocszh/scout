//! Process-wide Prometheus metrics.
//!
//! Every metric is a `static` that call sites update directly
//! (`metrics::TASK_CLAIMS.inc(&[])`), so no registry is threaded through function
//! signatures. [`render`] writes one role's families in the Prometheus text
//! exposition format (version 0.0.4).
//!
//! Label values must come from a small, bounded set (an origin from the configured
//! Sources is fine; a URL or a task ID is not). As a backstop each family keeps at
//! most [`MAX_SERIES`] series; further label combinations fold into one `overflow`
//! series instead of growing without bound.
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

const MAX_SERIES: usize = 512;
const OVERFLOW: &str = "overflow";

type Series<T> = Mutex<BTreeMap<Vec<String>, T>>;

/// A metric family that can write itself in the text exposition format.
pub(crate) trait Family: Sync {
    fn render(&self, out: &mut String);
}

/// Which process is rendering: each role exposes only its own families.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    Coordinator,
    Worker,
    Serve,
    KafkaRelay,
    KafkaReducer,
}

impl Role {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Role::Coordinator => "coordinator",
            Role::Worker => "worker",
            Role::Serve => "serve",
            Role::KafkaRelay => "kafka-relay",
            Role::KafkaReducer => "kafka-reducer",
        }
    }

    fn families(self) -> &'static [&'static dyn Family] {
        match self {
            Role::Coordinator => COORDINATOR_FAMILIES,
            Role::Worker => WORKER_FAMILIES,
            Role::Serve => SERVE_FAMILIES,
            Role::KafkaRelay => KAFKA_RELAY_FAMILIES,
            Role::KafkaReducer => KAFKA_REDUCER_FAMILIES,
        }
    }
}

static COORDINATOR_FAMILIES: &[&dyn Family] = &[
    &BUILD_INFO,
    &TASK_CLAIMS,
    &TASK_COMPLETIONS,
    &TASK_FAILURES,
    &LEASE_RENEWALS,
    &LEASE_EXPIRATIONS,
    &STALE_RESULTS,
    &ORIGIN_REQUESTS,
    &ORIGIN_THROTTLE_RESPONSES,
    &ORIGIN_BACKOFF_MS,
    &FRONTIER_TASKS,
    &FRONTIER_EXPIRED_LEASES,
    &FRONTIER_DEAD_LETTER_REQUEUES,
    &DB_POOL_WAIT,
    &DB_POOL_CONNECTIONS,
    &DB_POOL_IN_USE,
];
static WORKER_FAMILIES: &[&dyn Family] = &[&BUILD_INFO, &PAGES_FETCHED];
static SERVE_FAMILIES: &[&dyn Family] = &[
    &BUILD_INFO,
    &HTTP_REQUESTS,
    &SEARCH_REQUESTS,
    &SEARCH_DURATION,
    &CORPUS_PAGES,
    &CORPUS_NEAR_DUPLICATE_PAGES,
    &CORPUS_EXACT_DUPLICATE_ALIASES,
];

static KAFKA_RELAY_FAMILIES: &[&dyn Family] = &[
    &BUILD_INFO,
    &KAFKA_RELAY_PENDING_EVENTS,
    &KAFKA_RELAYED_RECORDS,
    &KAFKA_RELAY_FAILURES,
];
static KAFKA_REDUCER_FAMILIES: &[&dyn Family] = &[
    &BUILD_INFO,
    &KAFKA_CONSUMER_LAG,
    &KAFKA_CONSUMED_RECORDS,
    &KAFKA_SNAPSHOT_PARITY,
];

/// Coordinator RPCs that validate a lease, so each can report a fencing rejection.
const FENCED_RPCS: &[&str] = &[
    "renew_lease",
    "robots_decision",
    "commit_robots_refresh",
    "fail_robots_refresh",
    "claim_sitemap_discovery",
    "commit_sitemap_discovery",
    "acquire_request_permit_lease_check",
    "complete_task",
    "fail_task",
];
const SEARCH_MODES: &[&str] = &["keyword", "semantic", "hybrid"];

/// Create the series whose label values are a known, closed set, at zero, so that the first
/// event is visible to `rate()` and dashboards show a flat line rather than no data.
pub(crate) fn seed(role: Role) {
    match role {
        Role::Coordinator => {
            for outcome in ["accepted", "duplicate", "stale"] {
                TASK_COMPLETIONS.seed(&[outcome]);
            }
            for outcome in ["retry", "failed"] {
                TASK_FAILURES.seed(&[outcome]);
            }
            for outcome in ["renewed", "rejected"] {
                LEASE_RENEWALS.seed(&[outcome]);
            }
            for rpc in FENCED_RPCS {
                STALE_RESULTS.seed(&[rpc]);
            }
        }
        Role::Worker => {
            for outcome in ["success", "http_error", "transport_error", "body_error"] {
                PAGES_FETCHED.seed(&[outcome]);
            }
        }
        Role::KafkaRelay => KAFKA_RELAY_FAILURES.seed(&[]),
        // The consumed-records series carry the topic name, which only the reducer knows.
        Role::KafkaReducer => {}
        Role::Serve => {
            for mode in SEARCH_MODES {
                SEARCH_DURATION.seed(&[mode]);
                for outcome in ["ok", "error"] {
                    SEARCH_REQUESTS.seed(&[mode, outcome]);
                }
            }
        }
    }
}

/// The text exposition of every family `role` owns.
pub(crate) fn render(role: Role) -> String {
    let mut out = String::new();
    for family in role.families() {
        family.render(&mut out);
    }
    out
}

const POOL_WAIT_BUCKETS: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const SEARCH_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

// ---- All roles ------------------------------------------------------------------------------

pub(crate) static BUILD_INFO: Gauge = Gauge::new(
    "scout_build_info",
    "Always 1; labels carry the process role and crate version.",
    &["role", "version"],
);

// ---- Coordinator: task lifecycle --------------------------------------------------------------

pub(crate) static TASK_CLAIMS: Counter = Counter::new(
    "scout_task_claims_total",
    "Crawl tasks leased to workers.",
    &[],
);
pub(crate) static TASK_COMPLETIONS: Counter = Counter::new(
    "scout_task_completions_total",
    "Worker results by outcome: accepted, duplicate (idempotent replay) or stale (fenced).",
    &["outcome"],
);
pub(crate) static TASK_FAILURES: Counter = Counter::new(
    "scout_task_failures_total",
    "Worker-reported task failures by outcome: retry (requeued) or failed (attempts exhausted). Fenced reports count in scout_stale_results_total.",
    &["outcome"],
);
pub(crate) static LEASE_RENEWALS: Counter = Counter::new(
    "scout_lease_renewals_total",
    "Lease renewal attempts by outcome: renewed or rejected.",
    &["outcome"],
);
pub(crate) static LEASE_EXPIRATIONS: Counter = Counter::new(
    "scout_lease_expirations_total",
    "Expired leases reclaimed by a later claim (the previous holder lost the task).",
    &[],
);
pub(crate) static STALE_RESULTS: Counter = Counter::new(
    "scout_stale_results_total",
    "Calls rejected by lease fencing (stale or unknown lease or epoch), by coordinator RPC.",
    &["rpc"],
);
pub(crate) static ORIGIN_REQUESTS: Counter = Counter::new(
    "scout_origin_requests_total",
    "HTTP request permits granted to workers, by Source origin and request kind.",
    &["origin", "kind"],
);

pub(crate) static ORIGIN_THROTTLE_RESPONSES: Counter = Counter::new(
    "scout_origin_throttle_responses_total",
    "Responses that slowed an origin down (HTTP 429 or 503), by Source origin.",
    &["origin"],
);
pub(crate) static ORIGIN_BACKOFF_MS: Gauge = Gauge::new(
    "scout_origin_backoff_milliseconds",
    "Adaptive extra start spacing per origin: doubles on 429/503, decays on healthy responses.",
    &["origin"],
);

// ---- Coordinator: frontier and database -------------------------------------------------------

pub(crate) static FRONTIER_TASKS: Gauge = Gauge::new(
    "scout_frontier_tasks",
    "Crawl tasks in the frontier by status, sampled from PostgreSQL.",
    &["status"],
);
pub(crate) static FRONTIER_EXPIRED_LEASES: Gauge = Gauge::new(
    "scout_frontier_expired_leases",
    "Leased tasks whose deadline has passed and that no claim has reclaimed yet.",
    &[],
);
pub(crate) static FRONTIER_DEAD_LETTER_REQUEUES: Gauge = Gauge::new(
    "scout_frontier_dead_letter_requeues",
    "Dead-letter tasks moved back to pending with `scout dlq requeue`, summed over the run. The dead-letter queue itself is scout_frontier_tasks{status=\"failed\"}.",
    &[],
);
pub(crate) static DB_POOL_WAIT: Histogram = Histogram::new(
    "scout_db_pool_wait_seconds",
    "Time a coordinator RPC waited for a free PostgreSQL connection.",
    &[],
    POOL_WAIT_BUCKETS,
);
pub(crate) static DB_POOL_CONNECTIONS: Gauge = Gauge::new(
    "scout_db_pool_connections",
    "PostgreSQL connections in the coordinator's general pool.",
    &[],
);
pub(crate) static DB_POOL_IN_USE: Gauge = Gauge::new(
    "scout_db_pool_connections_in_use",
    "Pool connections currently checked out.",
    &[],
);

// ---- Worker -----------------------------------------------------------------------------------

pub(crate) static PAGES_FETCHED: Counter = Counter::new(
    "scout_pages_fetched_total",
    "Page HTTP fetches by outcome: success, http_error, transport_error or body_error.",
    &["outcome"],
);

// ---- Serve ------------------------------------------------------------------------------------

pub(crate) static HTTP_REQUESTS: Counter = Counter::new(
    "scout_http_requests_total",
    "Search API HTTP responses by route and status code.",
    &["route", "status"],
);
pub(crate) static SEARCH_REQUESTS: Counter = Counter::new(
    "scout_search_requests_total",
    "Search requests by mode and outcome (ok or error).",
    &["mode", "outcome"],
);
pub(crate) static SEARCH_DURATION: Histogram = Histogram::new(
    "scout_search_duration_seconds",
    "Time to run a search against the active Generation, by mode.",
    &["mode"],
    SEARCH_BUCKETS,
);

// ---- Kafka ingestion log (optional) ---------------------------------------------------------

pub(crate) static KAFKA_RELAY_PENDING_EVENTS: Gauge = Gauge::new(
    "scout_kafka_relay_pending_events",
    "Outbox events of the run that the relay has not yet written to Kafka: the relay's lag.",
    &[],
);
pub(crate) static KAFKA_RELAYED_RECORDS: Counter = Counter::new(
    "scout_kafka_relayed_records_total",
    "Records the relay wrote to Kafka, by topic. Re-sent records after a crash count again.",
    &["topic"],
);
pub(crate) static KAFKA_RELAY_FAILURES: Counter = Counter::new(
    "scout_kafka_relay_failures_total",
    "Relay attempts that failed and will be retried (broker unavailable, missing body).",
    &[],
);
pub(crate) static KAFKA_CONSUMER_LAG: Gauge = Gauge::new(
    "scout_kafka_consumer_lag_records",
    "Records between the reducer's next offset and the partition's high watermark.",
    &["topic", "partition"],
);
pub(crate) static KAFKA_CONSUMED_RECORDS: Counter = Counter::new(
    "scout_kafka_consumed_records_total",
    "Records the reducer read, by topic and outcome: applied, duplicate (already applied) or other_run.",
    &["topic", "outcome"],
);
pub(crate) static KAFKA_SNAPSHOT_PARITY: Gauge = Gauge::new(
    "scout_kafka_snapshot_parity",
    "1 when the snapshot rebuilt from the log equals the live snapshot, 0 when it differs. Absent until both exist.",
    &[],
);

// ---- Primitives -------------------------------------------------------------------------------

fn lock<T>(series: &Series<T>) -> MutexGuard<'_, BTreeMap<Vec<String>, T>> {
    // A panic elsewhere must never take the metrics down with it.
    series.lock().unwrap_or_else(|error| error.into_inner())
}

/// The series key for `values`, folding into the overflow series once the family is full.
fn series_key<T>(
    map: &BTreeMap<Vec<String>, T>,
    label_count: usize,
    values: &[&str],
) -> Vec<String> {
    debug_assert_eq!(values.len(), label_count, "label arity mismatch");
    let key: Vec<String> = values.iter().map(|value| (*value).to_owned()).collect();
    if map.len() >= MAX_SERIES && !map.contains_key(&key) {
        vec![OVERFLOW.to_owned(); label_count]
    } else {
        key
    }
}

fn header(out: &mut String, name: &str, help: &str, kind: &str) {
    let help = help.replace('\\', "\\\\").replace('\n', "\\n");
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// `{a="x",b="y"}` plus an optional extra pair (the histogram `le`), or nothing.
fn label_set(names: &[&str], values: &[String], extra: Option<(&str, &str)>) -> String {
    let mut pairs: Vec<String> = names
        .iter()
        .zip(values)
        .map(|(name, value)| format!("{name}=\"{}\"", escape(value)))
        .collect();
    if let Some((name, value)) = extra {
        pairs.push(format!("{name}=\"{value}\""));
    }
    if pairs.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", pairs.join(","))
    }
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub(crate) struct Counter {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
    series: Series<u64>,
}

impl Counter {
    pub(crate) const fn new(
        name: &'static str,
        help: &'static str,
        labels: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            help,
            labels,
            series: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn inc(&self, values: &[&str]) {
        self.add(values, 1);
    }

    pub(crate) fn add(&self, values: &[&str], amount: u64) {
        let mut map = lock(&self.series);
        let key = series_key(&map, self.labels.len(), values);
        let value = map.entry(key).or_insert(0);
        *value = value.saturating_add(amount);
    }

    /// Create the series at zero if it does not exist yet. A counter that first appears with a
    /// nonzero value hides that first increase from `rate()`, so rare events are seeded.
    fn seed(&self, values: &[&str]) {
        let mut map = lock(&self.series);
        let key = series_key(&map, self.labels.len(), values);
        map.entry(key).or_insert(0);
    }

    #[cfg(test)]
    pub(crate) fn get(&self, values: &[&str]) -> u64 {
        let key: Vec<String> = values.iter().map(|value| (*value).to_owned()).collect();
        lock(&self.series).get(&key).copied().unwrap_or(0)
    }
}

pub(crate) static CORPUS_PAGES: Gauge = Gauge::new(
    "scout_corpus_pages",
    "Pages in the active Generation's Corpus snapshot, by origin (from its near-duplicate report).",
    &["origin"],
);
pub(crate) static CORPUS_NEAR_DUPLICATE_PAGES: Gauge = Gauge::new(
    "scout_corpus_near_duplicate_pages",
    "Pages of the active Corpus whose SimHash is near an earlier Page of the same origin.",
    &["origin"],
);
pub(crate) static CORPUS_EXACT_DUPLICATE_ALIASES: Gauge = Gauge::new(
    "scout_corpus_exact_duplicate_aliases",
    "URLs of the active Corpus folded into a Page with identical extracted content, by origin.",
    &["origin"],
);

impl Family for Counter {
    fn render(&self, out: &mut String) {
        header(out, self.name, self.help, "counter");
        let map = lock(&self.series);
        if map.is_empty() && self.labels.is_empty() {
            let _ = writeln!(out, "{} 0", self.name);
        }
        for (values, count) in map.iter() {
            let labels = label_set(self.labels, values, None);
            let _ = writeln!(out, "{}{labels} {count}", self.name);
        }
    }
}

pub(crate) struct Gauge {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
    series: Series<f64>,
}

impl Gauge {
    pub(crate) const fn new(
        name: &'static str,
        help: &'static str,
        labels: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            help,
            labels,
            series: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn set(&self, values: &[&str], value: f64) {
        let mut map = lock(&self.series);
        let key = series_key(&map, self.labels.len(), values);
        map.insert(key, value);
    }

    pub(crate) fn add(&self, values: &[&str], delta: f64) {
        let mut map = lock(&self.series);
        let key = series_key(&map, self.labels.len(), values);
        *map.entry(key).or_insert(0.0) += delta;
    }

    #[cfg(test)]
    pub(crate) fn get(&self, values: &[&str]) -> f64 {
        let key: Vec<String> = values.iter().map(|value| (*value).to_owned()).collect();
        lock(&self.series).get(&key).copied().unwrap_or(0.0)
    }

    /// Drop every series, for gauges that describe one replaceable thing (the active Corpus).
    pub(crate) fn clear(&self) {
        lock(&self.series).clear();
    }
}

impl Family for Gauge {
    fn render(&self, out: &mut String) {
        header(out, self.name, self.help, "gauge");
        let map = lock(&self.series);
        if map.is_empty() && self.labels.is_empty() {
            let _ = writeln!(out, "{} 0", self.name);
        }
        for (values, value) in map.iter() {
            let labels = label_set(self.labels, values, None);
            let _ = writeln!(out, "{}{labels} {value}", self.name);
        }
    }
}

#[derive(Default)]
struct HistogramState {
    /// Observations per bucket, not cumulative; the last slot is the overflow (`+Inf`) bucket.
    buckets: Vec<u64>,
    sum: f64,
    count: u64,
}

pub(crate) struct Histogram {
    name: &'static str,
    help: &'static str,
    labels: &'static [&'static str],
    bounds: &'static [f64],
    series: Series<HistogramState>,
}

impl Histogram {
    pub(crate) const fn new(
        name: &'static str,
        help: &'static str,
        labels: &'static [&'static str],
        bounds: &'static [f64],
    ) -> Self {
        Self {
            name,
            help,
            labels,
            bounds,
            series: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn observe(&self, values: &[&str], elapsed: Duration) {
        self.observe_seconds(values, elapsed.as_secs_f64());
    }

    pub(crate) fn observe_seconds(&self, values: &[&str], seconds: f64) {
        let mut map = lock(&self.series);
        let key = series_key(&map, self.labels.len(), values);
        let state = map.entry(key).or_default();
        if state.buckets.is_empty() {
            state.buckets = vec![0; self.bounds.len() + 1];
        }
        let slot = self
            .bounds
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(self.bounds.len());
        state.buckets[slot] += 1;
        state.sum += seconds;
        state.count += 1;
    }

    fn seed(&self, values: &[&str]) {
        let mut map = lock(&self.series);
        let key = series_key(&map, self.labels.len(), values);
        let bounds = self.bounds.len() + 1;
        map.entry(key).or_insert_with(|| HistogramState {
            buckets: vec![0; bounds],
            ..HistogramState::default()
        });
    }

    #[cfg(test)]
    pub(crate) fn count(&self, values: &[&str]) -> u64 {
        let key: Vec<String> = values.iter().map(|value| (*value).to_owned()).collect();
        lock(&self.series).get(&key).map_or(0, |state| state.count)
    }
}

impl Family for Histogram {
    fn render(&self, out: &mut String) {
        header(out, self.name, self.help, "histogram");
        let map = lock(&self.series);
        for (values, state) in map.iter() {
            let mut cumulative = 0;
            for (slot, bound) in self.bounds.iter().enumerate() {
                cumulative += state.buckets[slot];
                let labels = label_set(self.labels, values, Some(("le", &bound.to_string())));
                let _ = writeln!(out, "{}_bucket{labels} {cumulative}", self.name);
            }
            let labels = label_set(self.labels, values, Some(("le", "+Inf")));
            let _ = writeln!(out, "{}_bucket{labels} {}", self.name, state.count);
            let labels = label_set(self.labels, values, None);
            let _ = writeln!(out, "{}_sum{labels} {}", self.name, state.sum);
            let _ = writeln!(out, "{}_count{labels} {}", self.name, state.count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(family: &dyn Family) -> String {
        let mut out = String::new();
        family.render(&mut out);
        out
    }

    #[test]
    fn counter_renders_labelled_series_in_sorted_order() {
        let counter = Counter::new("t_total", "Test counter.", &["origin", "kind"]);
        counter.inc(&["https://b.example", "page"]);
        counter.add(&["https://a.example", "page"], 2);
        counter.inc(&["https://a.example", "page"]);
        assert_eq!(
            rendered(&counter),
            "# HELP t_total Test counter.\n\
             # TYPE t_total counter\n\
             t_total{origin=\"https://a.example\",kind=\"page\"} 3\n\
             t_total{origin=\"https://b.example\",kind=\"page\"} 1\n"
        );
    }

    #[test]
    fn unlabelled_families_render_zero_before_first_use() {
        let counter = Counter::new("t_total", "Test.", &[]);
        let gauge = Gauge::new("t_gauge", "Test.", &[]);
        assert!(rendered(&counter).ends_with("t_total 0\n"));
        assert!(rendered(&gauge).ends_with("t_gauge 0\n"));
        counter.inc(&[]);
        assert!(rendered(&counter).ends_with("t_total 1\n"));
    }

    #[test]
    fn labelled_families_without_series_render_only_their_header() {
        let counter = Counter::new("t_total", "Test.", &["outcome"]);
        assert_eq!(
            rendered(&counter),
            "# HELP t_total Test.\n# TYPE t_total counter\n"
        );
    }

    #[test]
    fn gauge_sets_and_adjusts() {
        let gauge = Gauge::new("t_gauge", "Test.", &["status"]);
        gauge.set(&["pending"], 7.0);
        gauge.add(&["pending"], -2.0);
        gauge.set(&["leased"], 1.5);
        assert_eq!(gauge.get(&["pending"]), 5.0);
        assert_eq!(
            rendered(&gauge),
            "# HELP t_gauge Test.\n# TYPE t_gauge gauge\n\
             t_gauge{status=\"leased\"} 1.5\n\
             t_gauge{status=\"pending\"} 5\n"
        );
    }

    #[test]
    fn histogram_buckets_are_cumulative_with_sum_and_count() {
        static BOUNDS: &[f64] = &[0.1, 1.0];
        let histogram = Histogram::new("t_seconds", "Test.", &["mode"], BOUNDS);
        histogram.observe_seconds(&["keyword"], 0.0625);
        histogram.observe_seconds(&["keyword"], 0.5);
        histogram.observe(&["keyword"], Duration::from_secs(3));
        assert_eq!(histogram.count(&["keyword"]), 3);
        assert_eq!(
            rendered(&histogram),
            "# HELP t_seconds Test.\n# TYPE t_seconds histogram\n\
             t_seconds_bucket{mode=\"keyword\",le=\"0.1\"} 1\n\
             t_seconds_bucket{mode=\"keyword\",le=\"1\"} 2\n\
             t_seconds_bucket{mode=\"keyword\",le=\"+Inf\"} 3\n\
             t_seconds_sum{mode=\"keyword\"} 3.5625\n\
             t_seconds_count{mode=\"keyword\"} 3\n"
        );
    }

    #[test]
    fn unlabelled_histogram_uses_bare_le_labels() {
        static BOUNDS: &[f64] = &[1.0];
        let histogram = Histogram::new("t_seconds", "Test.", &[], BOUNDS);
        histogram.observe_seconds(&[], 0.5);
        let text = rendered(&histogram);
        assert!(text.contains("t_seconds_bucket{le=\"1\"} 1\n"), "{text}");
        assert!(text.contains("t_seconds_sum 0.5\n"), "{text}");
        assert!(text.contains("t_seconds_count 1\n"), "{text}");
    }

    #[test]
    fn label_values_are_escaped() {
        let counter = Counter::new("t_total", "Line one\nline two.", &["v"]);
        counter.inc(&["a\"b\\c\nd"]);
        let text = rendered(&counter);
        assert!(
            text.contains("# HELP t_total Line one\\nline two.\n"),
            "{text}"
        );
        assert!(
            text.contains("t_total{v=\"a\\\"b\\\\c\\nd\"} 1\n"),
            "{text}"
        );
    }

    #[test]
    fn series_beyond_the_cap_fold_into_one_overflow_series() {
        let counter = Counter::new("t_total", "Test.", &["origin"]);
        for index in 0..MAX_SERIES + 40 {
            counter.inc(&[&format!("origin-{index}")]);
        }
        assert_eq!(lock(&counter.series).len(), MAX_SERIES + 1);
        assert_eq!(counter.get(&[OVERFLOW]), 40);
        // An existing series keeps counting once the family is full.
        counter.inc(&["origin-0"]);
        assert_eq!(counter.get(&["origin-0"]), 2);
    }

    #[test]
    fn seeding_creates_zero_series_without_disturbing_counts() {
        let counter = Counter::new("t_total", "Test.", &["outcome"]);
        counter.inc(&["ok"]);
        counter.seed(&["ok"]);
        counter.seed(&["error"]);
        assert_eq!(counter.get(&["ok"]), 1);
        assert_eq!(counter.get(&["error"]), 0);
        assert!(rendered(&counter).contains("t_total{outcome=\"error\"} 0\n"));
        static BOUNDS: &[f64] = &[1.0];
        let histogram = Histogram::new("t_seconds", "Test.", &["mode"], BOUNDS);
        histogram.seed(&["keyword"]);
        let text = rendered(&histogram);
        assert!(
            text.contains("t_seconds_bucket{mode=\"keyword\",le=\"+Inf\"} 0\n"),
            "{text}"
        );
        assert!(
            text.contains("t_seconds_count{mode=\"keyword\"} 0\n"),
            "{text}"
        );
    }

    #[test]
    fn seeded_roles_expose_their_closed_label_sets() {
        for role in [Role::Coordinator, Role::Worker, Role::Serve] {
            seed(role);
        }
        let coordinator = render(Role::Coordinator);
        assert!(
            coordinator.contains("scout_stale_results_total{rpc=\"fail_task\"} "),
            "{coordinator}"
        );
        assert!(coordinator.contains("scout_lease_renewals_total{outcome=\"rejected\"} "));
        assert!(render(Role::Worker).contains("scout_pages_fetched_total{outcome=\"success\"} "));
        let serve = render(Role::Serve);
        assert!(
            serve.contains("scout_search_duration_seconds_count{mode=\"hybrid\"} "),
            "{serve}"
        );
        assert!(serve.contains("scout_search_requests_total{mode=\"keyword\",outcome=\"error\"} "));
    }

    #[test]
    fn every_role_renders_a_well_formed_exposition() {
        for role in [Role::Coordinator, Role::Worker, Role::Serve] {
            let text = render(role);
            assert!(text.contains("# TYPE scout_build_info gauge"), "{text}");
            for line in text.lines() {
                assert!(
                    line.starts_with('#') || line.starts_with("scout_"),
                    "unexpected line {line:?} for {role:?}"
                );
            }
        }
    }

    #[test]
    fn roles_expose_only_their_own_families() {
        let coordinator = render(Role::Coordinator);
        let worker = render(Role::Worker);
        let serve = render(Role::Serve);
        for name in [
            "scout_task_claims_total",
            "scout_lease_expirations_total",
            "scout_stale_results_total",
            "scout_frontier_tasks",
            "scout_db_pool_wait_seconds",
            "scout_origin_requests_total",
        ] {
            assert!(coordinator.contains(&format!("# TYPE {name} ")), "{name}");
            assert!(!worker.contains(name) && !serve.contains(name), "{name}");
        }
        assert!(worker.contains("# TYPE scout_pages_fetched_total counter"));
        assert!(serve.contains("# TYPE scout_search_duration_seconds histogram"));
        assert!(!coordinator.contains("scout_search_duration_seconds"));
    }

    #[test]
    fn family_names_are_unique_and_prometheus_valid() {
        let mut seen = std::collections::BTreeSet::new();
        for role in [Role::Coordinator, Role::Worker, Role::Serve] {
            for line in render(role)
                .lines()
                .filter(|line| line.starts_with("# TYPE "))
            {
                let name = line.split_whitespace().nth(2).expect("metric name");
                assert!(
                    name.starts_with("scout_")
                        && name.bytes().all(|byte| byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || byte == b'_'),
                    "invalid metric name {name}"
                );
                if name != "scout_build_info" {
                    assert!(
                        seen.insert(name.to_owned()),
                        "{name} is exposed by two roles"
                    );
                }
            }
        }
    }
}
