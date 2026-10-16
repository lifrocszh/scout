//! Focused CLI/PostgreSQL checks. Run ignored tests only with a disposable
//! `scout_task15_*` database in SCOUT_TEST_DATABASE_URL.

use postgres::{Client, NoTls};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
static TEST_DB_LOCK: Mutex<()> = Mutex::new(());
const RECEIPT_SCHEMA: &str = "scout.databricks-crawl-event-ingestion-receipt.v1";

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn temp_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let count = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "scout-{label}-{}-{nonce}-{count}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("create isolated analytics fixture directory");
    dir
}

struct Fixture {
    _test_lock: MutexGuard<'static, ()>,
    url: String,
    data_dir: PathBuf,
    database: Client,
    run_id: String,
    consumer_id: String,
}

impl Fixture {
    fn new() -> Self {
        let test_lock = TEST_DB_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable scout_task15_* database");
        let database_name = url
            .split('?')
            .next()
            .unwrap_or(&url)
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default();
        assert!(
            database_name.starts_with("scout_task15_"),
            "refusing non-disposable analytics test database name"
        );
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let run_id = format!(
            "run-{}",
            sha256(format!("analytics-{nonce}-{unique}").as_bytes())
        );
        let consumer_id = format!("task15-{}-{unique}", std::process::id());
        let data_dir = temp_dir("analytics-recovery");
        let database =
            Client::connect(&url, NoTls).expect("connect to disposable analytics database");
        let fixture = Self {
            _test_lock: test_lock,
            url,
            data_dir,
            database,
            run_id,
            consumer_id,
        };
        let registered = fixture.scout(&[
            "events",
            "consumer-register",
            "--consumer-id",
            &fixture.consumer_id,
            "--required",
            "--replay-days",
            "3650",
            "--data-dir",
            fixture.data_dir.to_str().unwrap(),
        ]);
        assert_success(&registered, "register required test consumer");
        fixture
    }

    fn scout(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_scout"))
            .args(args)
            .env("SCOUT_DATABASE_URL", &self.url)
            .output()
            .expect("run Scout CLI against disposable test database")
    }

    fn seed_run(&mut self) {
        self.database
            .execute(
                "INSERT INTO scout_crawl_runs(crawl_run_id, config_digest, status)
             VALUES ($1, $2, 'active')",
                &[&self.run_id, &sha256(self.run_id.as_bytes())],
            )
            .expect("insert isolated Crawl run and trigger retained run history");
    }

    fn seed_completed_event(&mut self) -> String {
        self.seed_run();
        let event_id = format!("crawl-completed.v1:{}", self.run_id);
        let payload = json!({
            "event_type": "crawl_completed",
            "data": {
                "event_id": event_id,
                "schema_version": 1,
                "crawl_run_id": self.run_id,
                "completed_at_utc": "2026-10-13T00:00:00Z",
                "stop_reason": "frontier_exhausted",
                "config_sha256": sha256(self.run_id.as_bytes()),
            }
        })
        .to_string();
        self.insert_event(&event_id, "crawl_completed", "run", &payload);
        event_id
    }

    fn seed_aged_completed_event(&mut self, run_id: &str, created_at: &str) -> String {
        let config_digest = sha256(run_id.as_bytes());
        self.database
            .execute(
                "INSERT INTO scout_crawl_runs(
                     crawl_run_id,config_digest,status,completed_at,stop_reason
                 ) VALUES ($1,$2,'completed',clock_timestamp(),'fixture')",
                &[&run_id, &config_digest],
            )
            .expect("insert completed fixture run for prune eligibility");
        let event_id = format!("crawl-completed.v1:{run_id}");
        let payload = json!({
            "event_type": "crawl_completed",
            "data": {
                "event_id": event_id,
                "schema_version": 1,
                "crawl_run_id": run_id,
                "completed_at_utc": created_at,
                "stop_reason": "frontier_exhausted",
                "config_sha256": config_digest,
            }
        })
        .to_string();
        let payload_sha = sha256(payload.as_bytes());
        self.database
            .execute(
                "INSERT INTO scout_crawl_events(
                     event_id,crawl_run_id,event_type,schema_version,dedupe_key,
                     payload_json,payload_sha256,created_at
                 ) VALUES ($1,$2,'crawl_completed',1,'run',$3,$4,$5::text::timestamptz)",
                &[&event_id, &run_id, &payload, &payload_sha, &created_at],
            )
            .expect("insert old event with retained history fingerprint");
        event_id
    }

    fn seed_capture_and_baseline(&mut self) -> String {
        self.seed_run();
        let task_key = sha256(format!("{}:task", self.run_id).as_bytes());
        let task_id: i64 = self
            .database
            .query_one(
                "INSERT INTO scout_crawl_tasks(
                 crawl_run_id, source_id, origin, normalized_url, status,
                 task_attempt_count, max_task_attempts, idempotency_key, schedule_key
             ) VALUES ($1,'fixture','https://example.invalid','https://example.invalid/page',
                       'succeeded',1,1,$2,'https://example.invalid')
             RETURNING task_id",
                &[&self.run_id, &task_key],
            )
            .expect("insert original crawl task baseline")
            .get(0);
        let result_sha = sha256(format!("{}:result", self.run_id).as_bytes());
        let content_sha = sha256(b"capture content");
        let body_sha = sha256(b"body");
        let accepted_at = "2026-10-13T00:00:00Z";
        self.database.execute(
            "INSERT INTO scout_crawl_task_results(
                 task_id,crawl_run_id,source_id,content_hash,body_hash,body_size,
                 capture_metadata,idempotency_key,lease_owner,lease_epoch,result_sha256,accepted_at
             ) VALUES ($1,$2,'fixture',$3,$4,4,'{}',$5,'fixture-worker',1,$6,$7::text::timestamptz)",
            &[&task_id, &self.run_id, &content_sha, &body_sha, &task_key, &result_sha, &accepted_at],
        ).expect("insert original accepted-capture task result");
        let event_id = format!("capture-committed.v1:{}:{task_id}", self.run_id);
        let payload = json!({
            "event_type": "capture_committed",
            "data": {
                "event_id": event_id,
                "schema_version": 1,
                "crawl_run_id": self.run_id,
                "source_id": "fixture",
                "task_id": task_id,
                "committed_at_utc": accepted_at,
                "result_sha256": result_sha,
                "content_sha256": content_sha,
                "body_artifact": {
                    "uri": format!("scout-artifact://crawl/{}/bodies/{body_sha}", self.run_id),
                    "sha256": body_sha,
                    "byte_size": 4,
                }
            }
        })
        .to_string();
        self.insert_event(
            &event_id,
            "capture_committed",
            &task_id.to_string(),
            &payload,
        );
        event_id
    }

    fn insert_event(&mut self, event_id: &str, event_type: &str, dedupe_key: &str, payload: &str) {
        let payload_sha = sha256(payload.as_bytes());
        self.database.execute(
            "INSERT INTO scout_crawl_events(
                 event_id,crawl_run_id,event_type,schema_version,dedupe_key,payload_json,payload_sha256
             ) VALUES ($1,$2,$3,1,$4,$5,$6)",
            &[&event_id, &self.run_id, &event_type, &dedupe_key, &payload, &payload_sha],
        ).expect("insert durable Crawl event and trigger independent history ledger");
    }

    fn export(&self, extra: &[&str]) -> Output {
        let mut args = vec![
            "events",
            "export",
            "--run-id",
            &self.run_id,
            "--consumer-id",
            &self.consumer_id,
            "--data-dir",
            self.data_dir.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        self.scout(&args)
    }

    fn preflight(&self) -> Value {
        let output = self.scout(&[
            "events",
            "preflight",
            "--run-id",
            &self.run_id,
            "--consumer-id",
            &self.consumer_id,
            "--data-dir",
            self.data_dir.to_str().unwrap(),
        ]);
        assert_success(&output, "run retained-history preflight");
        serde_json::from_slice(&output.stdout).expect("parse preflight JSON")
    }

    fn register_required_consumer(&self, consumer_id: &str) {
        let output = self.scout(&[
            "events",
            "consumer-register",
            "--consumer-id",
            consumer_id,
            "--required",
            "--replay-days",
            "3650",
            "--data-dir",
            self.data_dir.to_str().unwrap(),
        ]);
        assert_success(&output, "register second required test consumer");
    }

    fn acknowledge(&self, receipt: &Value, filename: &str) -> Output {
        let path = self.data_dir.join(filename);
        fs::write(&path, serde_json::to_vec(receipt).unwrap()).expect("write local test receipt");
        let path = path.to_str().unwrap();
        self.scout(&[
            "events",
            "acknowledge",
            "--receipt-file",
            path,
            "--data-dir",
            self.data_dir.to_str().unwrap(),
        ])
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.data_dir);
    }
}

fn assert_success(output: &Output, label: &str) {
    assert!(
        output.status.success(),
        "{label} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn json_output(output: &Output, label: &str) -> Value {
    assert_success(output, label);
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{label} emitted invalid JSON: {error}"))
}

fn published_batch(output: &Output) -> (String, PathBuf) {
    let value = json_output(output, "publish Crawl event batch");
    (
        value["batch_id"].as_str().unwrap().to_owned(),
        PathBuf::from(value["path"].as_str().unwrap()),
    )
}

fn archive_and_acknowledge_active_required_batches(
    fixture: &mut Fixture,
    run_id: &str,
    event_id: &str,
) -> Vec<(String, String, PathBuf)> {
    let consumers = fixture
        .database
        .query(
            "SELECT c.consumer_id FROM scout_analytics_consumers c
             WHERE c.ever_required AND c.is_required AND c.retired_at IS NULL
               AND EXISTS (SELECT 1 FROM scout_crawl_event_history h
                           WHERE h.event_id=$1 AND h.crawl_run_id=$2)
             ORDER BY c.consumer_id",
            &[&event_id, &run_id],
        )
        .expect("read active required consumers for event")
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();

    let mut copies = Vec::new();
    for (index, consumer_id) in consumers.iter().enumerate() {
        let export = fixture.scout(&[
            "events",
            "export",
            "--run-id",
            run_id,
            "--consumer-id",
            consumer_id,
            "--data-dir",
            fixture.data_dir.to_str().unwrap(),
        ]);
        let (batch_id, batch_path) = published_batch(&export);
        let manifest_bytes = fs::read(batch_path.join("batch-manifest.json")).unwrap();
        let manifest: Value = serde_json::from_slice(&manifest_bytes).unwrap();
        let coverage_sha: String = fixture
            .database
            .query_one(
                "SELECT coverage_sha256 FROM scout_crawl_event_batches WHERE batch_id=$1",
                &[&batch_id],
            )
            .expect("read batch event coverage")
            .get(0);
        let receipt = json!({
            "schema_version": RECEIPT_SCHEMA,
            "batch_id": batch_id,
            "consumer_id": consumer_id,
            "manifest_sha256": sha256(&manifest_bytes),
            "records_sha256": manifest["records_sha256"],
            "coverage_sha256": coverage_sha,
            "input_record_count": manifest["record_count"],
            "ingested_record_count": manifest["record_count"],
            "quarantine_count": 0,
            "conflict_count": 0,
            "status": "complete",
            "completed_at_utc": "2026-10-13T00:01:00Z",
        });
        assert_success(
            &fixture.acknowledge(&receipt, &format!("prune-receipt-{index}.json")),
            "record downstream receipt for prune coverage",
        );
        let archived = fixture.scout(&[
            "events",
            "archive",
            "--batch-id",
            &batch_id,
            "--data-dir",
            fixture.data_dir.to_str().unwrap(),
        ]);
        assert_success(&archived, "archive required-consumer batch copy");
        copies.push((consumer_id.clone(), batch_id, batch_path));
    }
    copies
}

fn fixture_http_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind isolated Crawl fixture server");
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept Crawl fixture request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let request = read_http_request(&mut stream);
            let (content_type, body): (&str, &[u8]) = if request.starts_with("GET /robots.txt ") {
                ("text/plain", b"User-agent: *\nAllow: /\n")
            } else if request.starts_with("GET / ") {
                ("text/html; charset=utf-8", FIXTURE_HTML)
            } else {
                panic!("unexpected Crawl fixture request: {request}");
            };
            write!(stream,
                   "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                   body.len()).unwrap();
            stream.write_all(body).unwrap();
        }
    });
    (origin, server)
}

const FIXTURE_HTML: &[u8] = r#"<!doctype html><html><head><title>Recovery fixture</title></head><body><nav>Ignore navigation</nav><main><h1>Recovery fixture</h1><p>Scout captures stable, useful documentation from a local explicitly allowlisted source. This representative paragraph is long enough to pass page admission and remains unchanged during the export replay. Additional prose keeps the local fixture useful for checking content hashes, event provenance, snapshot counts, and reconciliation inputs. Unicode checksum coverage: café 東京.</p></main></body></html>"#.as_bytes();

fn read_http_request(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let count = stream.read(&mut chunk).expect("read Crawl fixture request");
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).expect("Crawl fixture request is UTF-8")
}

fn only_child_dir(root: &std::path::Path) -> PathBuf {
    let dirs = fs::read_dir(root)
        .expect("read generated artifact root")
        .map(|entry| entry.expect("read artifact entry").path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    assert_eq!(
        dirs.len(),
        1,
        "expected one generated export directory in {root:?}"
    );
    dirs.into_iter().next().unwrap()
}

fn export_snapshot(
    data_dir: &std::path::Path,
    snapshot_id: &str,
    output_root: &std::path::Path,
) -> Output {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts/databricks/snapshot_batch.py");
    Command::new("python3")
        .args([
            script.to_str().unwrap(),
            "export",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--snapshot-id",
            snapshot_id,
            "--output-root",
            output_root.to_str().unwrap(),
        ])
        .output()
        .expect("run real local snapshot exporter")
}

#[test]
#[ignore = "requires disposable SCOUT_TEST_DATABASE_URL scout_task15_* database"]
fn pending_retry_reuses_identical_published_batch_and_receipt_is_strict() {
    let mut fixture = Fixture::new();
    let event_id = fixture.seed_completed_event();
    fixture
        .database
        .execute(
            "INSERT INTO scout_crawl_event_checkpoints(consumer_id,event_id,next_attempt_at)
         VALUES ($1,$2,clock_timestamp()+interval '1 hour')",
            &[&fixture.consumer_id, &event_id],
        )
        .expect("schedule retained event retry in the future");

    let waiting = json_output(&fixture.export(&[]), "report pending event retry");
    assert_eq!(waiting["status"], "pending_backoff");
    assert!(
        waiting["next_attempt_at_utc"]
            .as_str()
            .unwrap()
            .contains('T')
    );
    assert!(
        !fixture
            .data_dir
            .join("crawl-event-batches")
            .join(&fixture.run_id)
            .exists()
    );

    fixture.database.execute(
        "UPDATE scout_crawl_event_checkpoints SET next_attempt_at=clock_timestamp()-interval '1 second'
         WHERE consumer_id=$1 AND event_id=$2",
        &[&fixture.consumer_id, &event_id],
    ).expect("make retained event retry due");
    let (batch_id, batch_dir) = published_batch(&fixture.export(&[]));
    let original = ["batch-manifest.json", "records.jsonl", "_PUBLISHED"]
        .map(|name| fs::read(batch_dir.join(name)).unwrap());
    assert_eq!(fixture.database.query_one(
        "SELECT acknowledged_at IS NOT NULL FROM scout_crawl_event_checkpoints WHERE consumer_id=$1 AND event_id=$2",
        &[&fixture.consumer_id, &event_id],
    ).unwrap().get::<_, bool>(0), true);

    // Recreate the crash window: immutable files and batch registration survived,
    // while the local-export checkpoint did not.
    fixture.database.execute(
        "UPDATE scout_crawl_event_checkpoints SET acknowledged_at=NULL,next_attempt_at=clock_timestamp(),last_error=NULL
         WHERE consumer_id=$1 AND event_id=$2",
        &[&fixture.consumer_id, &event_id],
    ).expect("simulate interruption after publication but before checkpoint commit");
    let (replayed_id, replayed_dir) = published_batch(&fixture.export(&[]));
    assert_eq!(replayed_id, batch_id);
    let replayed = ["batch-manifest.json", "records.jsonl", "_PUBLISHED"]
        .map(|name| fs::read(replayed_dir.join(name)).unwrap());
    assert_eq!(replayed, original);
    let caught_up = json_output(&fixture.export(&[]), "report local exporter checkpoint");
    assert_eq!(caught_up["status"], "no_pending_records");
    assert_eq!(caught_up["checkpoint_scope"], "local-export-only");

    let manifest: Value = serde_json::from_slice(&original[0]).unwrap();
    let receipt = json!({
        "schema_version": RECEIPT_SCHEMA,
        "batch_id": batch_id,
        "consumer_id": fixture.consumer_id,
        "manifest_sha256": sha256(&original[0]),
        "records_sha256": manifest["records_sha256"],
        "coverage_sha256": fixture.database.query_one(
            "SELECT coverage_sha256 FROM scout_crawl_event_batches WHERE batch_id=$1", &[&batch_id]
        ).unwrap().get::<_, String>(0),
        "input_record_count": manifest["record_count"],
        "ingested_record_count": manifest["record_count"],
        "quarantine_count": 0,
        "conflict_count": 0,
        "status": "complete",
        "completed_at_utc": "2026-10-13T00:01:00Z",
    });
    let other_consumer = format!("{}-other", fixture.consumer_id);
    fixture.register_required_consumer(&other_consumer);
    assert!(
        fixture
            .database
            .query_one(
                "SELECT is_required FROM scout_analytics_consumers WHERE consumer_id=$1",
                &[&fixture.consumer_id]
            )
            .unwrap()
            .get::<_, bool>(0)
    );

    for (filename, invalid) in [
        ("bad-consumer.json", json!({"consumer_id": other_consumer})),
        (
            "bad-manifest.json",
            json!({"manifest_sha256": "0".repeat(64)}),
        ),
        ("bad-count.json", json!({"ingested_record_count": 9})),
        ("bad-status.json", json!({"status": "failed"})),
        ("bad-quarantine.json", json!({"quarantine_count": 1})),
        ("bad-conflict.json", json!({"conflict_count": 1})),
    ] {
        let mut invalid_receipt = receipt.clone();
        for (key, value) in invalid.as_object().unwrap() {
            invalid_receipt[key] = value.clone();
        }
        assert!(
            !fixture
                .acknowledge(&invalid_receipt, filename)
                .status
                .success(),
            "invalid receipt {filename} must not be recorded"
        );
    }
    assert_success(
        &fixture.acknowledge(&receipt, "receipt.json"),
        "verify complete downstream receipt",
    );
    assert_success(
        &fixture.acknowledge(&receipt, "receipt-replay.json"),
        "accept identical receipt retry",
    );
    let mut conflicting = receipt.clone();
    conflicting["completed_at_utc"] = json!("2026-10-13T00:02:00Z");
    assert_success(
        &fixture.acknowledge(&conflicting, "receipt-time-variant.json"),
        "accept receipt replay with a different non-authoritative completion timestamp",
    );
    assert_eq!(
        fixture
            .database
            .query_one(
                "SELECT count(*) FROM scout_crawl_event_ingestion_receipts WHERE batch_id=$1",
                &[&batch_id]
            )
            .unwrap()
            .get::<_, i64>(0),
        1
    );
}

#[test]
#[ignore = "requires disposable SCOUT_TEST_DATABASE_URL scout_task15_* database"]
fn capacity_rejection_does_not_publish_batch_or_acknowledge_event() {
    let mut fixture = Fixture::new();
    let event_id = fixture.seed_completed_event();
    let queue = fixture
        .data_dir
        .join("crawl-event-batches")
        .join("retained-fill");
    fs::create_dir_all(&queue).unwrap();
    let max_local = 4096_u64 + 1024 * 1024;
    fs::write(queue.join("fill.bin"), vec![b'x'; max_local as usize]).unwrap();

    let output = fixture.export(&["--max-bytes", "4096", "--max-local-bytes", "1052672"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("batch not acknowledged"));
    assert!(
        !fixture
            .data_dir
            .join("crawl-event-batches")
            .join(&fixture.run_id)
            .exists()
    );
    let row = fixture
        .database
        .query_one(
            "SELECT acknowledged_at IS NULL, last_error FROM scout_crawl_event_checkpoints
         WHERE consumer_id=$1 AND event_id=$2",
            &[&fixture.consumer_id, &event_id],
        )
        .expect("failed publish should leave a retry checkpoint");
    assert!(row.get::<_, bool>(0));
    assert!(
        row.get::<_, String>(1)
            .contains("local Crawl event queue full")
    );
    assert_eq!(
        fixture
            .database
            .query_one(
                "SELECT count(*) FROM scout_crawl_event_batches WHERE crawl_run_id=$1",
                &[&fixture.run_id]
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

#[test]
#[ignore = "requires disposable SCOUT_TEST_DATABASE_URL scout_task15_* database"]
fn deleting_capture_and_run_baselines_or_history_cannot_report_caught_up() {
    let mut fixture = Fixture::new();
    let event_id = fixture.seed_capture_and_baseline();
    fixture
        .database
        .execute(
            "DELETE FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&fixture.run_id],
        )
        .expect("simulate deletion of live run and cascaded task result/event");
    let deleted: (bool, i64, i64, i64, i64) = fixture
        .database
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM scout_crawl_runs WHERE crawl_run_id=$1),
                (SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id=$1),
                (SELECT count(*) FROM scout_crawl_task_results WHERE crawl_run_id=$1),
                (SELECT count(*) FROM scout_crawl_events WHERE crawl_run_id=$1),
                (SELECT count(*) FROM scout_crawl_event_history WHERE crawl_run_id=$1)",
            &[&fixture.run_id],
        )
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
        .unwrap();
    assert_eq!(deleted, (false, 0, 0, 0, 1));

    let export = fixture.export(&[]);
    assert!(!export.status.success());
    assert!(String::from_utf8_lossy(&export.stderr).contains("retained-history"));
    let preflight = fixture.preflight();
    assert_eq!(preflight["status"], "unrecoverable");
    assert!(
        preflight["unrecoverable_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap["event_id"] == event_id)
    );

    // A same-count mutation proves the independent fingerprint catches edits;
    // deleting the row then proves the independent count catches lost history.
    fixture.database.execute(
        "UPDATE scout_crawl_event_history SET dedupe_key=dedupe_key || '-tampered' WHERE event_id=$1",
        &[&event_id],
    ).expect("corrupt retained history metadata without changing count");
    let same_count = fixture.preflight();
    assert_eq!(same_count["status"], "unrecoverable");
    assert!(
        same_count["unrecoverable_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap["reason"]
                .as_str()
                .unwrap()
                .contains("expected 1 event identities, found 1"))
    );

    fixture
        .database
        .execute(
            "DELETE FROM scout_crawl_event_history WHERE event_id=$1",
            &[&event_id],
        )
        .expect("simulate deletion of the independently retained event history row");
    let deleted_history = fixture.preflight();
    assert_eq!(deleted_history["status"], "unrecoverable");
    assert!(
        deleted_history["unrecoverable_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap["reason"]
                .as_str()
                .unwrap()
                .contains("expected 1 event identities, found 0"))
    );
    let final_export = fixture.export(&[]);
    assert!(!final_export.status.success());
    assert!(
        String::from_utf8_lossy(&final_export.stderr)
            .contains("expected 1 event identities, found 0")
    );
}

#[test]
#[ignore = "requires disposable SCOUT_TEST_DATABASE_URL scout_task15_* database"]
fn disconnected_crawl_snapshot_reexport_preserves_reconciliation_inputs() {
    let fixture = Fixture::new();
    let (origin, server) = fixture_http_server();
    let config = fixture.data_dir.join("sources.toml");
    fs::write(
        &config,
        format!(
            "schema_version = \"scout.sources.v1\"\ncontact_url = \"https://example.invalid/scout\"\nuser_agent = \"Task15Fixture/1.0\"\npage_target = 1\n\n[[sources]]\nsource_id = \"fixture\"\nseeds = [\"{origin}/\"]\nallowed_origins = [\"{origin}\"]\npath_prefixes = [\"/\"]\n"
        ),
    )
    .expect("write isolated local Crawl config");
    let crawl = fixture.scout(&[
        "crawl",
        "--config",
        config.to_str().unwrap(),
        "--data-dir",
        fixture.data_dir.to_str().unwrap(),
    ]);
    server.join().expect("finish local Crawl fixture server");
    assert_success(&crawl, "crawl allowlisted local fixture");

    let crawl_dir = only_child_dir(&fixture.data_dir.join("crawls"));
    let crawl_manifest_bytes = fs::read(crawl_dir.join("crawl-manifest.json")).unwrap();
    let crawl_manifest: Value = serde_json::from_slice(&crawl_manifest_bytes).unwrap();
    assert_eq!(crawl_manifest["status"], "complete");
    let run_id = crawl_manifest["run_id"].as_str().unwrap();
    let snapshot_id = crawl_manifest["snapshot_id"].as_str().unwrap();
    let snapshot_dir = fixture.data_dir.join("corpora").join(snapshot_id);
    let snapshot_manifest_bytes = fs::read(snapshot_dir.join("snapshot-manifest.json")).unwrap();
    let snapshot_manifest: Value = serde_json::from_slice(&snapshot_manifest_bytes).unwrap();
    assert_eq!(snapshot_manifest["status"], "complete");
    assert_eq!(snapshot_manifest["run_id"], run_id);
    assert_eq!(
        snapshot_manifest["crawl_manifest_sha256"],
        crawl_manifest["manifest_sha256"]
    );

    // Local single-node Crawl does not register a durable distributed run.
    // Check the provenance and counts that the live reconciliation consumes.
    assert!(run_id.starts_with("run-"));
    let page_count = fs::read_to_string(snapshot_dir.join("pages.jsonl"))
        .unwrap()
        .lines()
        .count() as u64;
    let passage_count = fs::read_to_string(snapshot_dir.join("passages.jsonl"))
        .unwrap()
        .lines()
        .count() as u64;
    let alias_count = fs::read_to_string(snapshot_dir.join("aliases.jsonl"))
        .unwrap()
        .lines()
        .count() as u64;
    assert_eq!(
        page_count,
        snapshot_manifest["page_count"].as_u64().unwrap()
    );
    assert_eq!(
        passage_count,
        snapshot_manifest["passage_count"].as_u64().unwrap()
    );
    assert_eq!(
        alias_count,
        snapshot_manifest["alias_count"].as_u64().unwrap()
    );
    assert_eq!(
        page_count,
        crawl_manifest["summary"]["page_count"].as_u64().unwrap()
    );
    assert_eq!(
        page_count,
        crawl_manifest["summary"]["captured_page_count"]
            .as_u64()
            .unwrap()
    );

    // The production snapshot exporter must produce the same immutable logical rows on replay.
    let first_root = fixture.data_dir.join("snapshot-export-first");
    let second_root = fixture.data_dir.join("snapshot-export-replay");
    assert_success(
        &export_snapshot(&fixture.data_dir, snapshot_id, &first_root),
        "export local snapshot",
    );
    assert_success(
        &export_snapshot(&fixture.data_dir, snapshot_id, &second_root),
        "replay local snapshot export",
    );
    let first = only_child_dir(&first_root);
    let second = only_child_dir(&second_root);
    let first_names = fs::read_dir(&first)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<std::collections::BTreeSet<_>>();
    let second_names = fs::read_dir(&second)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(first_names, second_names);
    for name in first_names {
        assert_eq!(
            fs::read(first.join(&name)).unwrap(),
            fs::read(second.join(&name)).unwrap(),
            "snapshot export replay changed immutable file {name:?}"
        );
    }
    let batch_manifest: Value =
        serde_json::from_slice(&fs::read(first.join("batch-manifest.json")).unwrap()).unwrap();
    assert_eq!(
        batch_manifest["crawl_manifest_sha256"],
        crawl_manifest["manifest_sha256"]
    );
    let crawl_file_sha = sha256(&crawl_manifest_bytes);
    let mut batch_identity = format!(
        "{}\0{}\0{}\0{}",
        batch_manifest["schema_version"].as_str().unwrap(),
        batch_manifest["snapshot_id"].as_str().unwrap(),
        batch_manifest["source_snapshot_manifest_sha256"]
            .as_str()
            .unwrap(),
        crawl_file_sha,
    );
    if let Some(history_sha) = batch_manifest["history_evidence_sha256"].as_str() {
        batch_identity.push('\0');
        batch_identity.push_str(history_sha);
    }
    assert_eq!(
        batch_manifest["batch_id"],
        format!("batch-{}", sha256(batch_identity.as_bytes())),
        "raw file digest must separate this immutable batch from legacy identity"
    );
    assert_eq!(batch_manifest["crawl_manifest_file_sha256"], crawl_file_sha);
    assert_eq!(
        batch_manifest["crawl_manifest_file_bytes"],
        crawl_manifest_bytes.len() as u64
    );
    assert_eq!(
        batch_manifest["source_artifacts"]["crawl-manifest.json"],
        crawl_file_sha
    );
    assert_ne!(crawl_file_sha, crawl_manifest["manifest_sha256"]);
    assert_eq!(
        batch_manifest["page_count"],
        snapshot_manifest["page_count"]
    );
    assert_eq!(
        batch_manifest["alias_count"],
        snapshot_manifest["alias_count"]
    );
    assert_eq!(
        batch_manifest["passage_count"],
        snapshot_manifest["passage_count"]
    );
}

#[test]
#[ignore = "requires disposable SCOUT_TEST_DATABASE_URL scout_task15_* database"]
fn lagging_required_consumer_blocks_event_pruning() {
    let mut fixture = Fixture::new();
    let run_id = fixture.run_id.clone();
    let event_id = fixture.seed_aged_completed_event(&run_id, "2010-01-01T00:00:00Z");
    let (batch_id, batch_path) = published_batch(&fixture.export(&[]));
    let archived = fixture.scout(&[
        "events",
        "archive",
        "--batch-id",
        &batch_id,
        "--data-dir",
        fixture.data_dir.to_str().unwrap(),
    ]);
    assert_success(&archived, "archive event batch before prune preflight");

    let prune = fixture.scout(&[
        "events",
        "prune",
        "--run-id",
        &fixture.run_id,
        "--execute",
        "--data-dir",
        fixture.data_dir.to_str().unwrap(),
    ]);
    let report = json_output(&prune, "execute lagging-consumer prune check");
    assert_eq!(report["status"], "blocked");
    assert!(report["execute"].as_bool().unwrap());
    assert!(
        report["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| {
                blocker
                    .as_str()
                    .unwrap()
                    .contains("lacks a verified downstream receipt")
            })
    );
    assert_eq!(
        fixture
            .database
            .query_one(
                "SELECT count(*) FROM scout_crawl_events WHERE event_id=$1",
                &[&event_id],
            )
            .unwrap()
            .get::<_, i64>(0),
        1,
        "blocked pruning must retain the event"
    );
    assert!(
        batch_path.exists(),
        "blocked pruning must retain the local batch"
    );
}

#[test]
#[ignore = "requires disposable SCOUT_TEST_DATABASE_URL scout_task15_* database"]
fn retired_consumer_applies_only_to_events_at_or_before_retirement() {
    let mut fixture = Fixture::new();
    let retired = fixture.scout(&[
        "events",
        "consumer-retire",
        "--consumer-id",
        &fixture.consumer_id,
        "--data-dir",
        fixture.data_dir.to_str().unwrap(),
    ]);
    assert_success(&retired, "retire test analytics consumer");
    fixture
        .database
        .execute(
            "UPDATE scout_analytics_consumers
             SET retired_at='2000-06-01T00:00:00Z'::timestamptz
             WHERE consumer_id=$1",
            &[&fixture.consumer_id],
        )
        .expect("backdate retirement for deterministic cutoff coverage");

    let before_run = format!(
        "run-{}",
        sha256(format!("{}:before-retirement", fixture.run_id).as_bytes())
    );
    let before_event = fixture.seed_aged_completed_event(&before_run, "2000-01-01T00:00:00Z");
    archive_and_acknowledge_active_required_batches(&mut fixture, &before_run, &before_event);
    let before_prune = fixture.scout(&[
        "events",
        "prune",
        "--run-id",
        &before_run,
        "--execute",
        "--data-dir",
        fixture.data_dir.to_str().unwrap(),
    ]);
    let before_report = json_output(&before_prune, "prune pre-retirement event");
    assert_eq!(before_report["status"], "blocked");
    assert!(
        before_report["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| { blocker.as_str().unwrap().contains(&fixture.consumer_id) })
    );
    assert_eq!(
        fixture
            .database
            .query_one(
                "SELECT count(*) FROM scout_crawl_events WHERE event_id=$1",
                &[&before_event],
            )
            .unwrap()
            .get::<_, i64>(0),
        1,
        "event created before retirement remains obligated"
    );

    let after_run = format!(
        "run-{}",
        sha256(format!("{}:after-retirement", fixture.run_id).as_bytes())
    );
    let after_event = fixture.seed_aged_completed_event(&after_run, "2001-01-01T00:00:00Z");
    archive_and_acknowledge_active_required_batches(&mut fixture, &after_run, &after_event);
    let after_prune = fixture.scout(&[
        "events",
        "prune",
        "--run-id",
        &after_run,
        "--execute",
        "--data-dir",
        fixture.data_dir.to_str().unwrap(),
    ]);
    let after_report = json_output(&after_prune, "prune post-retirement event");
    assert_eq!(after_report["status"], "pruned");
    assert!(
        !after_report["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|blocker| { blocker.as_str().unwrap().contains(&fixture.consumer_id) })
    );
    assert_eq!(
        fixture
            .database
            .query_one(
                "SELECT count(*) FROM scout_crawl_events WHERE event_id=$1",
                &[&after_event],
            )
            .unwrap()
            .get::<_, i64>(0),
        0,
        "event created after retirement can prune once active consumers acknowledge"
    );
}

#[test]
#[ignore = "requires disposable SCOUT_TEST_DATABASE_URL scout_task15_* database"]
fn batch_target_resumes_partial_unlink_without_deleting_sibling_copy() {
    let mut fixture = Fixture::new();
    let run_id = fixture.run_id.clone();
    let event_id = fixture.seed_aged_completed_event(&run_id, "2010-01-01T00:00:00Z");
    let consumers = fixture
        .database
        .query(
            "SELECT c.consumer_id FROM scout_analytics_consumers c
             WHERE c.ever_required AND EXISTS (
                 SELECT 1 FROM scout_crawl_event_history h
                 WHERE h.event_id=$1 AND h.crawl_run_id=$2
                   AND (c.retired_at IS NULL OR h.created_at <= c.retired_at)
             ) ORDER BY c.consumer_id",
            &[&event_id, &run_id],
        )
        .expect("read all historically required consumers")
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert!(
        consumers.len() >= 2,
        "default and fixture required consumers exist"
    );

    let mut copies = Vec::new();
    for (index, consumer_id) in consumers.iter().enumerate() {
        let export = fixture.scout(&[
            "events",
            "export",
            "--run-id",
            &run_id,
            "--consumer-id",
            consumer_id,
            "--data-dir",
            fixture.data_dir.to_str().unwrap(),
        ]);
        let (batch_id, batch_path) = published_batch(&export);
        let manifest_bytes = fs::read(batch_path.join("batch-manifest.json")).unwrap();
        let manifest: Value = serde_json::from_slice(&manifest_bytes).unwrap();
        let coverage_sha: String = fixture
            .database
            .query_one(
                "SELECT coverage_sha256 FROM scout_crawl_event_batches WHERE batch_id=$1",
                &[&batch_id],
            )
            .expect("read batch event coverage")
            .get(0);
        let receipt = json!({
            "schema_version": RECEIPT_SCHEMA,
            "batch_id": batch_id,
            "consumer_id": consumer_id,
            "manifest_sha256": sha256(&manifest_bytes),
            "records_sha256": manifest["records_sha256"],
            "coverage_sha256": coverage_sha,
            "input_record_count": manifest["record_count"],
            "ingested_record_count": manifest["record_count"],
            "quarantine_count": 0,
            "conflict_count": 0,
            "status": "complete",
            "completed_at_utc": "2026-10-13T00:01:00Z",
        });
        assert_success(
            &fixture.acknowledge(&receipt, &format!("prune-receipt-{index}.json")),
            "record downstream receipt for prune coverage",
        );
        let archived = fixture.scout(&[
            "events",
            "archive",
            "--batch-id",
            &batch_id,
            "--data-dir",
            fixture.data_dir.to_str().unwrap(),
        ]);
        assert_success(&archived, "archive required-consumer batch copy");
        copies.push((consumer_id.clone(), batch_id, batch_path));
    }

    let (_, target_batch, target_path) = copies
        .iter()
        .find(|(consumer_id, _, _)| consumer_id == &fixture.consumer_id)
        .expect("fixture required consumer batch exists");
    let (_, sibling_batch, sibling_path) = copies
        .iter()
        .find(|(_, batch_id, _)| batch_id != target_batch)
        .expect("another required consumer has a sibling batch");
    assert_ne!(target_batch, sibling_batch);
    let batch_identity = fixture
        .database
        .query_one(
            "SELECT manifest_sha256,records_bytes FROM scout_crawl_event_batches WHERE batch_id=$1",
            &[target_batch],
        )
        .expect("read target batch immutable identity");
    let manifest_sha: String = batch_identity.get(0);
    let records_bytes: i64 = batch_identity.get(1);
    let target_local_path = target_path
        .canonicalize()
        .expect("resolve registered target batch path")
        .to_string_lossy()
        .into_owned();
    fixture
        .database
        .execute(
            "INSERT INTO scout_analytics_prune_items(
                 item_type,item_id,crawl_run_id,local_path,expected_sha256,expected_bytes
             ) VALUES ('batch_copy',$1,$2,$3,$4,$5)",
            &[
                target_batch,
                &fixture.run_id,
                &target_local_path,
                &manifest_sha,
                &records_bytes,
            ],
        )
        .expect("persist batch-copy intent before simulated interrupted unlink");
    fs::remove_file(target_path.join("records.jsonl"))
        .expect("simulate process interruption after unlinking one batch file");

    let prune = fixture.scout(&[
        "events",
        "prune",
        "--batch-id",
        target_batch,
        "--execute",
        "--data-dir",
        fixture.data_dir.to_str().unwrap(),
    ]);
    let report = json_output(&prune, "resume batch-target prune after partial unlink");
    assert_eq!(report["status"], "pruned", "prune report: {report}");
    assert_eq!(report["event_count"], 1);
    assert_eq!(report["batch_copy_count"], 1);
    assert_eq!(
        fixture
            .database
            .query_one(
                "SELECT count(*) FROM scout_crawl_events WHERE event_id=$1",
                &[&event_id],
            )
            .unwrap()
            .get::<_, i64>(0),
        0,
        "successful resume removes the selected event after every consumer receipt"
    );
    assert!(
        !target_path.exists(),
        "target batch copy is removed after resume"
    );
    assert!(
        sibling_path.exists(),
        "batch-target prune preserves sibling batch copy"
    );
}
