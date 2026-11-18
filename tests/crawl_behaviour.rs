//! Fixture crawl for issue #12: near-duplicate counts, dead letters for broken URLs, and an
//! origin that slows down after a 429. One coordinator and two workers crawl two local
//! origins; the Corpus is then materialized and the dead-letter CLI is exercised.
//!
//! Run with a disposable SCOUT_TEST_DATABASE_URL and --ignored --test-threads=1.
//! Timing is asserted only as lower bounds that the coordinator enforces (a throttled origin
//! waits at least its one-second backoff), never as upper bounds a slow host could miss.
use serde_json::Value;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A 400-word documentation-like text; every seed gives different text on one vocabulary.
fn page_text(seed: u64) -> String {
    const WORDS: [&str; 20] = [
        "pool", "server", "request", "handler", "timeout", "index", "segment", "merge", "query",
        "token", "field", "schema", "replica", "shard", "lease", "worker", "origin", "crawl",
        "page", "passage",
    ];
    let mut state = seed;
    (0..400)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            WORDS[(state >> 33) as usize % WORDS.len()]
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn html(body: &str) -> String {
    format!(
        "<!doctype html><html><head><title>Fixture guide</title></head><body><main><h1>Fixture guide</h1><p>{body}</p></main></body></html>"
    )
}

type Requests = Arc<Mutex<Vec<(Instant, String)>>>;

struct Origin {
    url: String,
    requests: Requests,
}

/// A local origin answering every request through `respond(path, nth request for that path)`.
fn origin(
    stop: &Arc<AtomicBool>,
    respond: fn(&str, usize) -> (u16, Vec<(&'static str, String)>, String),
) -> Origin {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let requests: Requests = Arc::default();
    let (stop, log) = (stop.clone(), requests.clone());
    thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            let Ok((mut stream, _)) = listener.accept() else {
                thread::sleep(Duration::from_millis(5));
                continue;
            };
            let path = read_path(&mut stream);
            let nth = {
                let mut log = log.lock().unwrap();
                log.push((Instant::now(), path.clone()));
                log.iter().filter(|(_, seen)| *seen == path).count()
            };
            let (status, headers, body) = respond(&path, nth);
            let reason = match status {
                200 => "OK",
                404 => "Not Found",
                429 => "Too Many Requests",
                _ => "Internal Server Error",
            };
            let content_type = if path == "/robots.txt" {
                "text/plain"
            } else {
                "text/html; charset=utf-8"
            };
            let mut head = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            for (name, value) in headers {
                head.push_str(&format!("{name}: {value}\r\n"));
            }
            head.push_str("\r\n");
            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(body.as_bytes());
        }
    });
    Origin { url, requests }
}

fn read_path(stream: &mut TcpStream) -> String {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(count) => bytes.extend_from_slice(&buffer[..count]),
        }
    }
    String::from_utf8_lossy(&bytes)
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .to_owned()
}

/// Docs origin: `/two` is `/one` with one sentence added, `/four` repeats `/three` exactly,
/// and `/broken` always fails.
fn docs(path: &str, _nth: usize) -> (u16, Vec<(&'static str, String)>, String) {
    match path {
        "/robots.txt" => (200, vec![], "User-agent: *\nAllow: /\n".into()),
        "/one" => (200, vec![], html(&page_text(1))),
        "/two" => (
            200,
            vec![],
            html(&format!("{} Updated in version 2.", page_text(1))),
        ),
        "/three" | "/four" => (200, vec![], html(&page_text(3))),
        "/broken" => (500, vec![], "fixture failure".into()),
        _ => (404, vec![], "missing".into()),
    }
}

/// Throttling origin: the first request for `/a` is refused with 429 and `Retry-After: 1`.
fn throttled(path: &str, nth: usize) -> (u16, Vec<(&'static str, String)>, String) {
    match (path, nth) {
        ("/robots.txt", _) => (200, vec![], "User-agent: *\nAllow: /\n".into()),
        ("/a", 1) => (429, vec![("Retry-After", "1".into())], "slow down".into()),
        ("/a", _) => (200, vec![], html(&page_text(5))),
        ("/b", _) => (200, vec![], html(&page_text(6))),
        ("/c", _) => (200, vec![], html(&page_text(7))),
        _ => (404, vec![], "missing".into()),
    }
}

fn free_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn scout(args: &[&str], database_url: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scout"))
        .args(args)
        .env("SCOUT_DATABASE_URL", database_url)
        .output()
        .unwrap()
}

fn wait_for(mut done: impl FnMut() -> bool, what: &str, limit: Duration) {
    let deadline = Instant::now() + limit;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(50));
    }
}

fn interrupt(child: &mut Child) {
    let _ = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn run_id_from_logs(dir: &Path) -> String {
    fs::read_dir(dir.join("logs"))
        .unwrap()
        .filter_map(Result::ok)
        .flat_map(|entry| {
            fs::read_to_string(entry.path())
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .find_map(|line| {
            let event: Value = serde_json::from_str(&line).ok()?;
            (event["event"] == "coordinator.started")
                .then(|| event["data"]["crawl_run_id"].as_str().map(str::to_owned))?
        })
        .expect("coordinator.started event with crawl_run_id")
}

struct Cleanup {
    database_url: String,
    run_id: Option<String>,
    dir: PathBuf,
    stop: Arc<AtomicBool>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let (Some(run_id), Ok(mut client)) = (
            &self.run_id,
            postgres::Client::connect(&self.database_url, postgres::NoTls),
        ) {
            let _ = client.execute(
                "DELETE FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[run_id],
            );
        }
        if !std::thread::panicking() {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

#[test]
#[ignore = "requires a disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn fixture_crawl_reports_duplicates_dead_letters_and_throttling() {
    let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
        .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("scout-behaviour-{}-{stamp}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let mut cleanup = Cleanup {
        database_url: database_url.clone(),
        run_id: None,
        dir: dir.clone(),
        stop: stop.clone(),
    };
    let docs = origin(&stop, docs);
    let slow = origin(&stop, throttled);
    let config = dir.join("sources.toml");
    fs::write(
        &config,
        format!(
            "schema_version = \"scout.sources.v1\"\ncontact_url = \"https://example.invalid/scout\"\nuser_agent = \"Scout-Test/1 (+https://example.invalid/scout)\"\npage_target = 20\nmin_content_chars = 20\nglobal_concurrency = 2\norigin_concurrency = 1\nmin_start_spacing_ms = 10\nrequest_timeout_seconds = 5\nmax_attempts = 2\nretry_backoff_ms = 10\nretry_max_delay_ms = 10\nmax_total_attempts = 100\nmax_duration_seconds = 90\nrobots_cache_seconds = 60\nmax_frontier = 50\n\n[[sources]]\nsource_id = \"docs\"\nseeds = [\"{d}/one\", \"{d}/two\", \"{d}/three\", \"{d}/four\", \"{d}/broken\"]\nallowed_origins = [\"{d}\"]\npath_prefixes = [\"/\"]\n\n[[sources]]\nsource_id = \"slow\"\nseeds = [\"{s}/a\", \"{s}/b\", \"{s}/c\"]\nallowed_origins = [\"{s}\"]\npath_prefixes = [\"/\"]\n",
            d = docs.url,
            s = slow.url,
        ),
    )
    .unwrap();

    let coordinator_addr = free_address();
    let dir_arg = dir.to_str().unwrap();
    let mut coordinator = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "coordinator",
            "--config",
            config.to_str().unwrap(),
            "--data-dir",
            dir_arg,
            "--bind",
            &coordinator_addr.to_string(),
        ])
        .env("SCOUT_DATABASE_URL", &database_url)
        .stdout(Stdio::from(
            fs::File::create(dir.join("coordinator.stdout")).unwrap(),
        ))
        .stderr(Stdio::from(
            fs::File::create(dir.join("coordinator.stderr")).unwrap(),
        ))
        .spawn()
        .unwrap();
    wait_for(
        || TcpStream::connect(coordinator_addr).is_ok(),
        "coordinator to listen",
        Duration::from_secs(20),
    );
    let run_id = run_id_from_logs(&dir);
    cleanup.run_id = Some(run_id.clone());
    let mut workers = ["behaviour-worker-a", "behaviour-worker-b"]
        .into_iter()
        .map(|worker_id| {
            Command::new(env!("CARGO_BIN_EXE_scout"))
                .args([
                    "worker",
                    "--coordinator",
                    &format!("http://{coordinator_addr}"),
                    "--data-dir",
                    dir_arg,
                    "--worker-id",
                    worker_id,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::from(
                    fs::File::create(dir.join(format!("{worker_id}.stderr"))).unwrap(),
                ))
                .spawn()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut database = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
    wait_for(
        || {
            database
                .query_one(
                    "SELECT status FROM scout_crawl_runs WHERE crawl_run_id = $1",
                    &[&run_id],
                )
                .map(|row| row.get::<_, String>(0) == "completed")
                .unwrap_or(false)
        },
        "the crawl run to complete",
        Duration::from_secs(90),
    );
    for worker in &mut workers {
        wait_for(
            || worker.try_wait().unwrap().is_some(),
            "workers to exit",
            Duration::from_secs(30),
        );
    }
    interrupt(&mut coordinator);

    // Dead letters: the broken URL, after two task attempts of two HTTP attempts each.
    let listed = scout(&["dlq", "list", "--run-id", &run_id], &database_url);
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let listing: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listing["run_status"], "completed");
    assert_eq!(listing["total"], 1, "{listing}");
    let entry = &listing["entries"][0];
    assert_eq!(entry["normalized_url"], format!("{}/broken", docs.url));
    assert_eq!(entry["task_attempt_count"], 2);
    assert!(
        entry["last_error"].as_str().unwrap().contains("HTTP 500"),
        "{entry}"
    );
    let broken_requests = docs
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, path)| path == "/broken")
        .count();
    assert_eq!(broken_requests, 4);
    // The run's Corpus is sealed, so its dead letters can be read but not requeued.
    let refused = scout(
        &["dlq", "requeue", "--run-id", &run_id, "--all", "--json"],
        &database_url,
    );
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("only an active run"));

    // Throttling: after the 429, the origin's next request waits out the backoff and the
    // one-second Retry-After, whichever worker sends it; the docs origin is not held.
    let slow_requests = slow.requests.lock().unwrap().clone();
    let (throttled_at, _) = slow_requests
        .iter()
        .find(|(_, path)| path == "/a")
        .cloned()
        .unwrap();
    let later = slow_requests
        .iter()
        .filter(|(at, _)| *at > throttled_at)
        .collect::<Vec<_>>();
    assert!(!later.is_empty(), "{slow_requests:?}");
    for (at, path) in later {
        assert!(
            at.duration_since(throttled_at) >= Duration::from_millis(950),
            "{path} started {:?} after the 429",
            at.duration_since(throttled_at)
        );
    }
    let slow_paths = slow_requests
        .iter()
        .filter(|(_, path)| path != "/robots.txt")
        .count();
    assert_eq!(slow_paths, 4, "a, its retry, b and c: {slow_requests:?}");
    let row = database
        .query_one(
            "SELECT throttle_responses, backoff_ms FROM scout_crawl_origin_throttle
             WHERE crawl_run_id = $1 AND origin = $2",
            &[&run_id, &slow.url],
        )
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    // Three healthy pages after the 429 have started to undo the one-second backoff.
    let backoff: i64 = row.get(1);
    assert!((1..1_000).contains(&backoff), "{backoff}");
    let docs_throttled: i64 = database
        .query_one(
            "SELECT count(*) FROM scout_crawl_origin_throttle
             WHERE crawl_run_id = $1 AND origin = $2",
            &[&run_id, &docs.url],
        )
        .unwrap()
        .get(0);
    assert_eq!(docs_throttled, 0);

    // Duplicates: materialize the Corpus and read the reduce step's report.
    let materialized = scout(
        &[
            "crawl",
            "--config",
            config.to_str().unwrap(),
            "--data-dir",
            dir_arg,
            "--materialize-spool",
            &run_id,
            "--json",
        ],
        &database_url,
    );
    assert!(
        materialized.status.success(),
        "{}",
        String::from_utf8_lossy(&materialized.stderr)
    );
    let report: Value = serde_json::from_slice(
        &fs::read(
            dir.join("crawls")
                .join(&run_id)
                .join("near-duplicates.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let origins = report["origins"].as_array().unwrap();
    let docs_report = origins
        .iter()
        .find(|origin| origin["origin"] == docs.url)
        .unwrap();
    // one, two and three are Pages; four is an exact-duplicate alias of three.
    assert_eq!(docs_report["page_count"], 3, "{report}");
    assert_eq!(docs_report["near_duplicate_page_count"], 1, "{report}");
    assert_eq!(docs_report["exact_duplicate_alias_count"], 1, "{report}");
    let slow_report = origins
        .iter()
        .find(|origin| origin["origin"] == slow.url)
        .unwrap();
    assert_eq!(slow_report["page_count"], 3);
    assert_eq!(slow_report["near_duplicate_page_count"], 0);
    assert_eq!(report["pairs"][0]["page_url"], format!("{}/two", docs.url));
    assert_eq!(
        report["pairs"][0]["near_duplicate_of"],
        format!("{}/one", docs.url)
    );
}
