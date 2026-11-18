//! The optional Kafka ingestion log end to end (ADR 0003), against a real Postgres and a real
//! Kafka broker: crawl a local site with a coordinator and two workers, seal the live snapshot,
//! relay the outbox into Kafka, and rebuild the snapshot from Kafka alone, with the site switched
//! off. The relay and the reducer are each crashed once in the window between their effect and
//! their acknowledgement; the final replica must still equal the live snapshot exactly, with no
//! capture lost or doubled.
//!
//! Run: SCOUT_TEST_DATABASE_URL=postgres://... SCOUT_TEST_KAFKA_BOOTSTRAP=localhost:9092 \
//!      cargo test --test kafka_log -- --ignored
//! (scripts/ci/postgres-tests.sh kafka_log sets the database URL; CI runs it in `test-kafka`.)
use serde_json::Value;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SCOUT: &str = env!("CARGO_BIN_EXE_scout");

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos()
}

/// Poll `condition` until it holds; fail with `what` after `limit`.
fn wait_until(limit: Duration, what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

/// A small documentation site: an index linking five pages, two of them byte-identical (so the
/// snapshot has an Alias), plus robots.txt. Serves until dropped.
struct Site {
    origin: String,
    requests: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Site {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture site");
        let origin = format!("http://{}", listener.local_addr().expect("site address"));
        listener.set_nonblocking(true).expect("nonblocking site");
        let requests = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (served, stopping) = (requests.clone(), stop.clone());
        let thread = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        served.fetch_add(1, Ordering::Relaxed);
                        thread::spawn(move || answer(stream));
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept fixture request: {error}"),
                }
            }
        });
        Self {
            origin,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    fn shut_down(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join fixture site");
        }
    }
}

impl Drop for Site {
    fn drop(&mut self) {
        self.shut_down();
    }
}

fn page(title: &str, body: &str, links: &str) -> String {
    format!(
        "<!doctype html><html><head><title>{title}</title></head><body><main><h1>{title}</h1>\
         <p>{body} This page is part of the Kafka replay fixture and carries enough text to be \
         admitted as a Page.</p><h2>Details</h2><p>Second section about {title}, so the page \
         splits into more than one Passage.</p>{links}</main></body></html>"
    )
}

fn answer(mut stream: TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("read timeout");
    let mut head = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(count) => head.extend_from_slice(&buffer[..count]),
        }
    }
    let head = String::from_utf8_lossy(&head);
    let path = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    let duplicate = page(
        "Partitions",
        "Records with the same key land on one partition.",
        "",
    );
    let (status, content_type, body) = match path {
        "/robots.txt" => (
            "200 OK",
            "text/plain",
            "User-agent: *\nAllow: /\n".to_owned(),
        ),
        "/" => (
            "200 OK",
            "text/html",
            page(
                "Index",
                "The fixture index links every page.",
                "<ul><li><a href=\"/topics\">Topics</a></li><li><a href=\"/offsets\">Offsets</a></li>\
                 <li><a href=\"/lag\">Lag</a></li><li><a href=\"/partitions\">Partitions</a></li>\
                 <li><a href=\"/partitions-copy\">Partitions again</a></li></ul>",
            ),
        ),
        "/topics" => (
            "200 OK",
            "text/html",
            page(
                "Topics",
                "A topic is an append-only log split into partitions.",
                "",
            ),
        ),
        "/offsets" => (
            "200 OK",
            "text/html",
            page(
                "Offsets",
                "A consumer stores the next offset it will read.",
                "",
            ),
        ),
        "/lag" => (
            "200 OK",
            "text/html",
            page(
                "Lag",
                "Lag is the high watermark minus the next offset.",
                "",
            ),
        ),
        "/partitions" | "/partitions-copy" => ("200 OK", "text/html", duplicate),
        _ => ("404 Not Found", "text/plain", "missing".to_owned()),
    };
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

struct Harness {
    root: PathBuf,
    database_url: String,
    bootstrap: String,
    run_id: String,
    topic_prefix: String,
    config: PathBuf,
}

impl Harness {
    fn live(&self) -> PathBuf {
        self.root.join("live")
    }

    fn scout(&self, args: &[&str]) -> Command {
        let mut command = Command::new(SCOUT);
        command
            .args(args)
            .env("SCOUT_DATABASE_URL", &self.database_url)
            .env_remove("SCOUT_FAULT_KAFKA_ABORT")
            .env_remove("SCOUT_METRICS_BIND");
        command
    }

    fn relay(&self, follow: bool, failpoint: Option<&str>) -> Output {
        let live = self.live();
        let mut args = vec![
            "kafka",
            "relay",
            "--run-id",
            &self.run_id,
            "--data-dir",
            live.to_str().expect("path"),
            "--bootstrap",
            &self.bootstrap,
            "--topic-prefix",
            &self.topic_prefix,
            "--poll-interval-ms",
            "50",
        ];
        if follow {
            args.push("--follow");
        }
        let mut command = self.scout(&args);
        if let Some(failpoint) = failpoint {
            command.env("SCOUT_FAULT_KAFKA_ABORT", failpoint);
        }
        command.output().expect("run kafka relay")
    }

    fn reduce(&self, replica: &Path, follow: bool, failpoint: Option<&str>) -> Output {
        let mut args = vec![
            "kafka",
            "reduce",
            "--run-id",
            &self.run_id,
            "--config",
            self.config.to_str().expect("path"),
            "--data-dir",
            replica.to_str().expect("path"),
            "--bootstrap",
            &self.bootstrap,
            "--topic-prefix",
            &self.topic_prefix,
            "--poll-interval-ms",
            "50",
        ];
        if follow {
            args.push("--follow");
        }
        let mut command = self.scout(&args);
        if let Some(failpoint) = failpoint {
            command.env("SCOUT_FAULT_KAFKA_ABORT", failpoint);
        }
        command.output().expect("run kafka reduce")
    }

    fn crawl(&self) {
        let live = self.live();
        let address = {
            let listener = TcpListener::bind("127.0.0.1:0").expect("reserve coordinator port");
            listener.local_addr().expect("address")
        };
        let mut coordinator = self
            .scout(&[
                "coordinator",
                "--config",
                self.config.to_str().expect("path"),
                "--data-dir",
                live.to_str().expect("path"),
                "--bind",
                &address.to_string(),
                "--run-id",
                &self.run_id,
            ])
            .stdout(Stdio::null())
            .stderr(fs::File::create(self.root.join("coordinator.stderr")).expect("log"))
            .spawn()
            .expect("start coordinator");
        wait_until(Duration::from_secs(30), "the coordinator to listen", || {
            assert!(
                coordinator.try_wait().expect("check coordinator").is_none(),
                "coordinator exited early: {}",
                self.log("coordinator.stderr")
            );
            TcpStream::connect(address).is_ok()
        });
        let mut workers = ["kafka-worker-a", "kafka-worker-b"]
            .into_iter()
            .map(|worker_id| {
                self.scout(&[
                    "worker",
                    "--coordinator",
                    &format!("http://{address}"),
                    "--data-dir",
                    self.root.join(worker_id).to_str().expect("path"),
                    "--worker-id",
                    worker_id,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("start worker")
            })
            .collect::<Vec<_>>();
        // The coordinator logs crawl.completed once the frontier is exhausted.
        let event_log = live.join("logs").join(format!("{}.jsonl", self.run_id));
        wait_until(Duration::from_secs(90), "crawl.completed", || {
            fs::read_to_string(&event_log)
                .is_ok_and(|log| log.contains("\"event\":\"crawl.completed\""))
        });
        for worker in &mut workers {
            wait_until(Duration::from_secs(30), "a worker to exit", || {
                worker.try_wait().expect("check worker").is_some()
            });
        }
        interrupt(&mut coordinator);
        let status = coordinator.wait().expect("wait for coordinator");
        assert!(
            status.success(),
            "coordinator: {}",
            self.log("coordinator.stderr")
        );
    }

    fn materialize_live(&self) -> String {
        let live = self.live();
        let output = self
            .scout(&[
                "crawl",
                "--config",
                self.config.to_str().expect("path"),
                "--data-dir",
                live.to_str().expect("path"),
                "--materialize-spool",
                &self.run_id,
            ])
            .output()
            .expect("materialize live snapshot");
        assert!(
            output.status.success(),
            "materialize: {}",
            text(&output.stderr)
        );
        let stdout = text(&output.stdout);
        stdout
            .split_whitespace()
            .find_map(|word| word.strip_prefix("snapshot_id="))
            .unwrap_or_else(|| panic!("no snapshot_id in {stdout}"))
            .to_owned()
    }

    fn log(&self, name: &str) -> String {
        fs::read_to_string(self.root.join(name)).unwrap_or_default()
    }
}

fn interrupt(child: &mut Child) {
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("send SIGINT");
    assert!(status.success(), "SIGINT the coordinator");
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn summary(output: &Output, what: &str) -> Value {
    assert!(
        output.status.success(),
        "{what} failed ({}):\n{}",
        output.status,
        text(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{what} summary is not JSON ({error}): {}",
            text(&output.stdout)
        )
    })
}

fn assert_crashed(output: &Output, what: &str) {
    assert!(
        !output.status.success() && text(&output.stderr).contains("failpoint"),
        "{what} should have aborted at its failpoint ({}):\n{}",
        output.status,
        text(&output.stderr)
    );
}

fn snapshot_files(data_dir: &Path, snapshot_id: &str) -> Vec<(String, Vec<u8>)> {
    ["pages.jsonl", "aliases.jsonl", "passages.jsonl"]
        .into_iter()
        .map(|name| {
            let path = data_dir.join("corpora").join(snapshot_id).join(name);
            (
                name.to_owned(),
                fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
            )
        })
        .collect()
}

#[test]
#[ignore = "requires PostgreSQL in SCOUT_TEST_DATABASE_URL and Kafka in SCOUT_TEST_KAFKA_BOOTSTRAP"]
fn replica_from_kafka_matches_live_snapshot_across_relay_and_reducer_crashes() {
    let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
        .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
    let bootstrap = std::env::var("SCOUT_TEST_KAFKA_BOOTSTRAP")
        .expect("set SCOUT_TEST_KAFKA_BOOTSTRAP to a Kafka broker, e.g. localhost:9092");
    let nonce = nonce();
    let root = std::env::temp_dir().join(format!("scout-kafka-log-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&root).expect("create test root");
    let mut site = Site::start();
    let config = root.join("sources.toml");
    fs::write(
        &config,
        format!(
            "schema_version = \"scout.sources.v1\"\ncontact_url = \"https://example.invalid/scout\"\n\
             user_agent = \"Scout-Test/1 (+https://example.invalid/scout)\"\npage_target = 50\n\
             min_content_chars = 20\nglobal_concurrency = 2\norigin_concurrency = 2\n\
             min_start_spacing_ms = 0\nrequest_timeout_seconds = 5\nmax_attempts = 1\n\
             retry_backoff_ms = 1\nretry_max_delay_ms = 1\nmax_total_attempts = 40\n\
             max_duration_seconds = 60\nrobots_cache_seconds = 60\nmax_frontier = 50\n\n\
             [[sources]]\nsource_id = \"fixture\"\nseeds = [\"{origin}/\"]\n\
             allowed_origins = [\"{origin}\"]\npath_prefixes = [\"/\"]\n",
            origin = site.origin
        ),
    )
    .expect("write sources config");
    let harness = Harness {
        root: root.clone(),
        database_url: database_url.clone(),
        bootstrap,
        run_id: format!("run-{nonce:064x}"),
        topic_prefix: format!("scout-test-{nonce:x}"),
        config,
    };

    // The live path: crawl, then seal and publish the snapshot (its outbox event is the run's last).
    harness.crawl();
    let live_snapshot = harness.materialize_live();
    let mut database = postgres::Client::connect(&database_url, postgres::NoTls).expect("connect");
    let captures: i64 = database
        .query_one(
            "SELECT count(*) FROM scout_crawl_task_results
             WHERE crawl_run_id = $1 AND capture_metadata IS NOT NULL",
            &[&harness.run_id],
        )
        .expect("count captures")
        .get(0);
    let outbox_events: i64 = database
        .query_one(
            "SELECT count(*) FROM scout_crawl_events WHERE crawl_run_id = $1",
            &[&harness.run_id],
        )
        .expect("count outbox events")
        .get(0);
    assert_eq!(captures, 6, "index, four pages and the duplicate");
    assert_eq!(
        outbox_events,
        captures + 2,
        "captures, completion, snapshot"
    );

    // Everything below reads Kafka only: the site is gone.
    site.shut_down();
    let requests_after_crawl = site.requests.load(Ordering::Relaxed);

    // Relay crash: the second event is in Kafka but not acknowledged in the outbox.
    assert_crashed(
        &harness.relay(false, Some("relay-after-produce:2")),
        "relay",
    );
    let relayed = summary(&harness.relay(true, None), "relay --follow");
    assert_eq!(relayed["pending_events"], 0);
    assert_eq!(
        relayed["relayed_events"].as_i64(),
        Some(outbox_events - 1),
        "only the acknowledged event is skipped; the unacknowledged one is sent again"
    );
    // Relaying again finds nothing to do.
    let again = summary(&harness.relay(false, None), "relay again");
    assert_eq!(again["relayed_events"], 0);

    // Reducer crash: the third capture is in the replica spool but its offset is not recorded.
    let replica = root.join("replica");
    assert_crashed(
        &harness.reduce(&replica, false, Some("reduce-after-apply:3")),
        "reducer",
    );
    let reduced = summary(&harness.reduce(&replica, true, None), "reduce --follow");
    assert_eq!(reduced["parity"], true, "{reduced}");
    assert_eq!(reduced["replica_snapshot_id"], live_snapshot.as_str());
    assert_eq!(reduced["live_snapshot_id"], live_snapshot.as_str());
    assert_eq!(
        reduced["results_applied"].as_i64(),
        Some(captures),
        "{reduced}"
    );
    assert_eq!(
        reduced["spool_captures"].as_i64(),
        Some(captures),
        "a replayed record must not add a spool row: {reduced}"
    );

    // A fresh replica from offset 0 meets the relay's duplicate and still matches.
    let fresh = root.join("fresh-replica");
    let rebuilt = summary(&harness.reduce(&fresh, false, None), "fresh reduce");
    assert_eq!(rebuilt["parity"], true, "{rebuilt}");
    assert_eq!(rebuilt["replica_snapshot_id"], live_snapshot.as_str());
    assert_eq!(rebuilt["results_applied"].as_i64(), Some(captures));
    assert!(
        rebuilt["duplicate_results_skipped"].as_u64() >= Some(1),
        "the relay crash left one result twice in the log: {rebuilt}"
    );
    assert!(rebuilt["duplicate_events_skipped"].as_u64() >= Some(1));

    let live_files = snapshot_files(&harness.live(), &live_snapshot);
    assert_eq!(live_files, snapshot_files(&replica, &live_snapshot));
    assert_eq!(live_files, snapshot_files(&fresh, &live_snapshot));
    assert_eq!(
        site.requests.load(Ordering::Relaxed),
        requests_after_crawl,
        "replay must not fetch"
    );

    let _ = database.execute(
        "DELETE FROM scout_crawl_runs WHERE crawl_run_id = $1",
        &[&harness.run_id],
    );
    let _ = fs::remove_dir_all(&root);
}
