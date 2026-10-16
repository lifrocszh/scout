//! Ticket 10 operator workflow verification. Run only against a fresh
//! `scout_ticket10_20261014` PostgreSQL database via SCOUT_TEST_DATABASE_URL.

use chrono::{Duration, Utc};
use postgres::{Client, NoTls};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

const SOURCE_ID: &str = "ticket07-09-local";
const FIXTURE: &str = "docs/runs/ticket09-history-2026-10-14/completed-history-recommendation.json";

#[derive(Deserialize, Serialize)]
struct Basis {
    schema_version: String,
    generation_id: String,
    source_id: String,
    generated_at_utc: String,
    expires_at_utc: String,
    expected_policy_version: String,
    interval_hours: BasisInterval,
    recommendation_settings: RecommendationSettings,
    evidence: BasisEvidence,
}

#[derive(Deserialize, Serialize)]
struct BasisInterval {
    minimum: String,
    current: String,
    maximum: String,
    proposed: String,
    adjustment: String,
    adjustment_fraction: String,
}

#[derive(Deserialize, Serialize)]
struct RecommendationSettings {
    max_adjustment_fraction: String,
    minimum_completed_snapshots: String,
    minimum_comparable_page_observations: String,
    maximum_evidence_age_days: String,
    maximum_provisional_event_age_seconds: String,
    change_fraction_decrease_threshold: String,
    change_fraction_increase_threshold: String,
    decrease_target_multiplier: String,
    increase_target_multiplier: String,
    high_confidence_minimum_snapshots: String,
    high_confidence_minimum_comparable_page_observations: String,
}

#[derive(Deserialize, Serialize)]
struct BasisEvidence {
    status: String,
    snapshot_ids: String,
    snapshot_refs: Vec<BasisSnapshotReference>,
    provisional_run_id: String,
    provisional_event_ids: String,
    scope_sha256: String,
    extraction_contract_sha256: String,
    latest_observed_at_utc: String,
    latest_capture_event_at_utc: String,
    latest_run_event_at_utc: String,
    comparable_page_observations: String,
    changed_page_observations: String,
    change_fraction: String,
}

#[derive(Deserialize, Serialize)]
struct BasisSnapshotReference {
    snapshot_id: String,
    source_snapshot_manifest_sha256: String,
    crawl_manifest_sha256: String,
    observed_at_utc: String,
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("scout-ticket10-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).expect("create temporary Ticket 10 fixture directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
#[ignore = "requires a fresh disposable scout_ticket10_20261014 PostgreSQL database"]
fn ticket10_operator_workflow_is_disabled_audited_and_restart_safe() {
    let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
        .expect("SCOUT_TEST_DATABASE_URL must point to the disposable Ticket 10 database");
    let database_name = database_url
        .split('?')
        .next()
        .unwrap_or(&database_url)
        .rsplit('/')
        .next()
        .unwrap_or_default();
    assert_eq!(
        database_name, "scout_ticket10_20261014",
        "refusing to use any database other than the isolated Ticket 10 database"
    );

    let fixture_dir = TempDir::new();
    let config_path = fixture_dir.path().join("sources.toml");
    let config = ticket09_source_config();
    fs::write(&config_path, &config).expect("write scope-matched Ticket 09 Source config");
    let config_path = config_path.to_str().unwrap();
    let mut database = Client::connect(&database_url, NoTls)
        .expect("connect to fresh isolated Ticket 10 PostgreSQL database");

    // First public call applies migrations; no policy has been enabled or configured.
    let pending = scout(&database_url, &["recrawl", "pending"]);
    assert_success(&pending, "initialize and list the empty review queue");
    let default_queue: Value = serde_json::from_slice(&pending.stdout).expect("pending JSON");
    assert_eq!(default_queue, json!([]));
    assert!(
        !global_enabled(&mut database),
        "global policy defaults disabled"
    );
    assert_eq!(source_policy_count(&mut database), 0);

    let malformed = fixture_dir.path().join("malformed-recommendation.json");
    fs::write(&malformed, b"{").expect("write malformed recommendation payload");
    let invalid = import(&database_url, config_path, &malformed);
    assert_eq!(invalid["validation_status"], "invalid");
    assert_eq!(invalid["outcome"], "invalid");

    configure(&database_url, config_path, "recrawl-policy.v3", 24.0, 0.25);
    assert!(
        !global_enabled(&mut database),
        "Source configuration must not enable global policy"
    );
    assert!(source_enabled(&mut database));

    let disabled = write_recommendation(
        fixture_dir.path(),
        "disabled-default",
        "recrawl-policy.v3",
        false,
    );
    let disabled_result = import(&database_url, config_path, &disabled);
    assert_eq!(disabled_result["validation_status"], "valid");
    assert_eq!(disabled_result["outcome"], "disabled");
    assert_eq!(
        policy_state(&mut database),
        ("recrawl-policy.v3".into(), 24.0)
    );
    assert_eq!(policy_audit_count(&mut database, "automatic"), 0);

    assert_success(
        &scout(
            &database_url,
            &[
                "recrawl",
                "enable",
                "--enabled",
                "true",
                "--actor",
                "ticket10-test",
            ],
        ),
        "explicitly enable recrawl policy in the isolated test database",
    );
    assert!(global_enabled(&mut database));

    // The real T09 24h→18h recommendation is exactly at the approved 25% cap.
    let routine = write_recommendation(
        fixture_dir.path(),
        "routine-automation",
        "recrawl-policy.v3",
        false,
    );
    let routine_result = import(&database_url, config_path, &routine);
    assert_eq!(routine_result["validation_status"], "valid");
    assert_eq!(routine_result["outcome"], "scheduled");
    assert_eq!(policy_state(&mut database).1, 18.0);
    assert_eq!(policy_audit_count(&mut database, "automatic"), 1);

    // A 10% cap makes the same 25% decrease review-only. Change the version
    // while resetting the disposable Source policy to the fixture's 24h baseline.
    configure(&database_url, config_path, "recrawl-policy.v4", 24.0, 0.10);
    let stale = write_recommendation(
        fixture_dir.path(),
        "stale-policy",
        "recrawl-policy.v3",
        false,
    );
    let stale_result = import(&database_url, config_path, &stale);
    assert_eq!(stale_result["validation_status"], "stale");
    assert_eq!(stale_result["outcome"], "stale");
    assert!(
        import_reasons(&mut database, &stale_result["recommendation_id"])
            .contains("recommendation policy version or current interval is stale")
    );

    let expired = write_recommendation(
        fixture_dir.path(),
        "expired-recommendation",
        "recrawl-policy.v4",
        true,
    );
    let expired_result = import(&database_url, config_path, &expired);
    assert_eq!(expired_result["validation_status"], "stale");
    assert_eq!(expired_result["outcome"], "stale");
    assert!(
        import_reasons(&mut database, &expired_result["recommendation_id"])
            .contains("recommendation is expired, future-dated, or too old")
    );

    let denied = write_recommendation(
        fixture_dir.path(),
        "operator-denies",
        "recrawl-policy.v4",
        false,
    );
    let accepted = write_recommendation(
        fixture_dir.path(),
        "operator-accepts",
        "recrawl-policy.v4",
        false,
    );
    let denied_import = import(&database_url, config_path, &denied);
    let accepted_import = import(&database_url, config_path, &accepted);
    for imported in [&denied_import, &accepted_import] {
        assert_eq!(imported["validation_status"], "valid");
        assert_eq!(imported["outcome"], "pending_review");
    }
    let pending = scout(&database_url, &["recrawl", "pending"]);
    assert_success(
        &pending,
        "reopen persisted review queue in a fresh CLI process",
    );
    let pending_items: Value = serde_json::from_slice(&pending.stdout).expect("pending JSON");
    let pending_ids: Vec<&str> = pending_items
        .as_array()
        .expect("review queue is an array")
        .iter()
        .map(|item| {
            item["recommendation_id"]
                .as_str()
                .expect("recommendation ID")
        })
        .collect();
    assert_eq!(pending_ids.len(), 2);
    assert!(pending_ids.contains(&denied_import["recommendation_id"].as_str().unwrap()));
    assert!(pending_ids.contains(&accepted_import["recommendation_id"].as_str().unwrap()));

    let denied_decision = review(
        &database_url,
        config_path,
        denied_import["recommendation_id"].as_str().unwrap(),
        false,
    );
    assert_eq!(denied_decision["outcome"], "rejected");
    assert!(denied_decision["applied_policy_version"].is_null());
    assert_eq!(
        policy_state(&mut database),
        ("recrawl-policy.v4".into(), 24.0)
    );

    // Review queue and policy survive multiple CLI process restarts.
    let pending_after_denial = scout(&database_url, &["recrawl", "pending"]);
    assert_success(&pending_after_denial, "reopen review queue after denial");
    let pending_after_denial: Value =
        serde_json::from_slice(&pending_after_denial.stdout).expect("pending JSON");
    assert_eq!(pending_after_denial.as_array().unwrap().len(), 1);
    assert_eq!(
        pending_after_denial[0]["recommendation_id"],
        accepted_import["recommendation_id"]
    );

    let accepted_decision = review(
        &database_url,
        config_path,
        accepted_import["recommendation_id"].as_str().unwrap(),
        true,
    );
    assert_eq!(accepted_decision["outcome"], "scheduled");
    let applied_version = accepted_decision["applied_policy_version"]
        .as_str()
        .expect("approved adjustment returns applied version");
    let (persisted_version, interval) = policy_state(&mut database);
    assert_eq!(persisted_version, applied_version);
    assert_eq!(interval, 18.0);
    assert_eq!(policy_audit_count(&mut database, "approved"), 1);
    assert_eq!(policy_audit_count(&mut database, "denied"), 1);

    let final_queue = scout(&database_url, &["recrawl", "pending"]);
    assert_success(
        &final_queue,
        "verify decisions persist after another CLI restart",
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&final_queue.stdout).expect("pending JSON"),
        json!([])
    );

    // No crawl was scheduled or run: recrawl administration did not touch crawl budgets.
    let crawl_runs: i64 = database
        .query_one("SELECT count(*) FROM scout_crawl_runs", &[])
        .expect("read crawl run count")
        .get(0);
    let scheduled_sources: i64 = database
        .query_one("SELECT count(*) FROM scout_scheduled_run_sources", &[])
        .expect("read scheduled Source count")
        .get(0);
    assert_eq!(crawl_runs, 0);
    assert_eq!(scheduled_sources, 0);
    assert_eq!(fs::read_to_string(config_path).unwrap(), config);
}

fn ticket09_source_config() -> String {
    "schema_version = 'scout.sources.v1'\n\
contact_url = 'http://127.0.0.1:18753/contact'\n\
user_agent = 'Scout-Ticket09-History/1.0'\n\
embedding_model = 'BAAI/bge-small-en-v1.5'\n\
page_target = 24\n\
min_content_chars = 100\n\
max_decoded_body_bytes = 65536\n\
max_frontier = 128\n\
request_timeout_seconds = 30\n\
global_concurrency = 1\n\
origin_concurrency = 1\n\
min_start_spacing_ms = 250\n\
max_attempts = 1\n\
retry_backoff_ms = 1000\n\
retry_max_delay_ms = 1000\n\
max_total_attempts = 100\n\
max_duration_seconds = 1200\n\
robots_cache_seconds = 60\n\
max_extraction_bytes = 65536\n\
max_extraction_millis = 5000\n\
max_passage_tokens = 384\n\
\n[[sources]]\n\
source_id = 'ticket07-09-local'\n\
name = 'Ticket 07/09 localhost history fixture'\n\
seeds = ['http://127.0.0.1:18753/index']\n\
allowed_origins = ['http://127.0.0.1:18753']\n\
path_prefixes = ['/']\n\
deny_path_prefixes = []\n\
minimum_page_quota = 0\n"
        .to_owned()
}

fn configure(database_url: &str, config: &str, version: &str, current: f64, cap: f64) {
    let current = current.to_string();
    let cap = cap.to_string();
    assert_success(
        &scout(
            database_url,
            &[
                "recrawl",
                "configure",
                "--config",
                config,
                "--source-id",
                SOURCE_ID,
                "--policy-version",
                version,
                "--current-hours",
                &current,
                "--minimum-hours",
                "6",
                "--maximum-hours",
                "168",
                "--max-interval-decrease-fraction",
                &cap,
                "--actor",
                "ticket10-test",
                "--reason",
                "Ticket 10 isolated operator workflow verification",
                "--enable-source",
            ],
        ),
        "configure isolated Source policy",
    );
}

fn import(database_url: &str, config: &str, recommendation: &Path) -> Value {
    let recommendation = recommendation.to_str().unwrap();
    let output = scout(
        database_url,
        &[
            "recrawl",
            "import",
            "--config",
            config,
            "--recommendation",
            recommendation,
        ],
    );
    json_success(&output, "import recommendation")
}

fn review(database_url: &str, config: &str, recommendation_id: &str, approve: bool) -> Value {
    let decision = if approve { "--approve" } else { "--deny" };
    let output = scout(
        database_url,
        &[
            "recrawl",
            "review",
            "--config",
            config,
            "--recommendation-id",
            recommendation_id,
            decision,
            "--actor",
            "ticket10-operator",
            "--reason",
            "Ticket 10 isolated approval decision",
        ],
    );
    json_success(&output, "record operator review decision")
}

fn write_recommendation(
    directory: &Path,
    generation_id: &str,
    policy_version: &str,
    expired: bool,
) -> PathBuf {
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let mut envelope: Value = serde_json::from_slice(
        &fs::read(fixture_path).expect("read completed-history recommendation fixture"),
    )
    .expect("parse completed-history recommendation fixture");
    let mut basis: Basis = serde_json::from_str(
        envelope["idempotency_basis"]
            .as_str()
            .expect("basis string"),
    )
    .expect("parse canonical Ticket 09 basis");

    let now = Utc::now();
    let generated = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let observed = (now - Duration::seconds(60))
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    let expires = if expired {
        now - Duration::seconds(5)
    } else {
        now + Duration::days(1)
    }
    .format("%Y-%m-%dT%H:%M:%SZ")
    .to_string();
    basis.generation_id = generation_id.to_owned();
    basis.generated_at_utc = generated.clone();
    basis.expires_at_utc = expires.clone();
    basis.expected_policy_version = policy_version.to_owned();
    basis.evidence.latest_observed_at_utc = observed.clone();
    for reference in &mut basis.evidence.snapshot_refs {
        reference.observed_at_utc = observed.clone();
    }
    let basis = serde_json::to_string(&basis).expect("serialize canonical recommendation basis");

    envelope["generation_id"] = json!(generation_id);
    envelope["generated_at_utc"] = json!(generated);
    envelope["expires_at_utc"] = json!(expires);
    envelope["expected_policy_version"] = json!(policy_version);
    envelope["evidence"]["latest_observed_at_utc"] = json!(observed);
    for reference in envelope["evidence"]["snapshot_refs"]
        .as_array_mut()
        .expect("snapshot references")
    {
        reference["observed_at_utc"] = json!(observed);
    }
    envelope["idempotency_basis"] = json!(basis);
    let digest = format!("{:x}", Sha256::digest(basis.as_bytes()));
    let recommendation_id = format!("sha256:{digest}");
    envelope["recommendation_id"] = json!(recommendation_id);
    envelope["idempotency_key"] = json!(recommendation_id);

    let path = directory.join(format!("{generation_id}.json"));
    fs::write(&path, serde_json::to_vec_pretty(&envelope).unwrap())
        .expect("write rehashed recommendation fixture");
    path
}

fn scout(database_url: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scout"))
        .args(args)
        .env("SCOUT_DATABASE_URL", database_url)
        .output()
        .expect("run public Scout operator CLI")
}

fn assert_success(output: &Output, purpose: &str) {
    assert!(
        output.status.success(),
        "{purpose} failed; stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn json_success(output: &Output, purpose: &str) -> Value {
    assert_success(output, purpose);
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{purpose} returned invalid JSON ({error}): {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn global_enabled(database: &mut Client) -> bool {
    database
        .query_one(
            "SELECT enabled FROM scout_recrawl_control WHERE singleton=TRUE",
            &[],
        )
        .expect("read global recrawl switch")
        .get(0)
}

fn source_enabled(database: &mut Client) -> bool {
    database
        .query_one(
            "SELECT enabled FROM scout_source_recrawl_policy WHERE source_id=$1",
            &[&SOURCE_ID],
        )
        .expect("read Source recrawl switch")
        .get(0)
}

fn source_policy_count(database: &mut Client) -> i64 {
    database
        .query_one("SELECT count(*) FROM scout_source_recrawl_policy", &[])
        .expect("count Source policy rows")
        .get(0)
}

fn policy_state(database: &mut Client) -> (String, f64) {
    let row = database
        .query_one(
            "SELECT policy_version,current_interval_hours::float8 \
             FROM scout_source_recrawl_policy WHERE source_id=$1",
            &[&SOURCE_ID],
        )
        .expect("read persisted Source policy after CLI restart");
    (row.get(0), row.get(1))
}

fn policy_audit_count(database: &mut Client, decision: &str) -> i64 {
    database
        .query_one(
            "SELECT count(*) FROM scout_recrawl_policy_audit WHERE decision=$1",
            &[&decision],
        )
        .expect("count recrawl policy audit decisions")
        .get(0)
}

fn import_reasons(database: &mut Client, recommendation_id: &Value) -> String {
    database
        .query_one(
            "SELECT validation_reasons::text FROM scout_recrawl_recommendation_imports \
             WHERE recommendation_id=$1",
            &[&recommendation_id.as_str().expect("recommendation ID")],
        )
        .expect("read persisted recommendation validation reasons")
        .get(0)
}
