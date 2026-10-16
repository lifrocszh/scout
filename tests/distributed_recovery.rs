//! Run with a disposable SCOUT_TEST_DATABASE_URL and --ignored --test-threads=1.
use postgres::{Client, NoTls};
use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Default)]
struct Metrics {
    requests: Vec<(Instant, String, String)>,
    active: usize,
    peak: usize,
    origins: BTreeMap<String, usize>,
    origin_peak: BTreeMap<String, usize>,
    counts: BTreeMap<String, usize>,
}
struct Fixture {
    origins: Vec<String>,
    metrics: Arc<Mutex<Metrics>>,
    stop: Arc<AtomicBool>,
}
impl Fixture {
    fn new() -> Self {
        Self::with_sitemaps("normal")
    }
    fn with_sitemaps(mode: &'static str) -> Self {
        let metrics = Arc::new(Mutex::new(Metrics::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let mut origins = Vec::new();
        for _ in 0..2 {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            origins.push(origin.clone());
            listener.set_nonblocking(true).unwrap();
            let metrics = metrics.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let Ok((mut stream, _)) = listener.accept() else {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    };
                    let metrics = metrics.clone();
                    let origin = origin.clone();
                    thread::spawn(move || {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        let mut bytes = [0; 8192];
                        let Ok(size) = stream.read(&mut bytes) else {
                            return;
                        };
                        let request = String::from_utf8_lossy(&bytes[..size]);
                        let path = request.split_whitespace().nth(1).unwrap_or("/").to_owned();
                        {
                            let mut m = metrics.lock().unwrap();
                            m.requests
                                .push((Instant::now(), origin.clone(), path.clone()));
                            m.active += 1;
                            m.peak = m.peak.max(m.active);
                            let active = m.origins.entry(origin.clone()).or_default();
                            *active += 1;
                            let active = *active;
                            let peak = m.origin_peak.entry(origin.clone()).or_default();
                            *peak = (*peak).max(active);
                            *m.counts.entry(path.clone()).or_default() += 1;
                        }
                        thread::sleep(Duration::from_millis(
                            if path == "/slow" || (mode == "tree-kill" && path == "/child.xml") {
                                800
                            } else {
                                180
                            },
                        ));
                        let (status, kind, extra, body) = match path.as_str() {
                            "/robots.txt" if mode == "caps" => (
                                "200 OK",
                                "text/plain",
                                "",
                                format!(
                                    "User-agent: *\nDisallow: /blocked\nDisallow: /denied-map.xml\nSitemap: {origin}/sitemap.xml\n"
                                ),
                            ),
                            "/sitemap.xml" if mode != "normal" => (
                                "200 OK",
                                "application/xml",
                                "",
                                format!(
                                    "<sitemapindex><sitemap><loc>{origin}/child.xml</loc></sitemap><sitemap><loc>{origin}/child.xml</loc></sitemap><sitemap><loc>{origin}/sitemap.xml</loc></sitemap>{}</sitemapindex>",
                                    if mode == "caps" {
                                        format!(
                                            "<sitemap><loc>{origin}/retry-map.xml</loc></sitemap><sitemap><loc>{origin}/denied-map.xml</loc></sitemap><sitemap><loc>{origin}/large-map.xml</loc></sitemap>"
                                        )
                                    } else {
                                        String::new()
                                    }
                                ),
                            ),
                            "/child.xml" => (
                                "200 OK",
                                "application/xml",
                                "",
                                format!(
                                    "<urlset><url><loc>{origin}/map</loc></url><url><loc>http://example.invalid/outside</loc></url></urlset>"
                                ),
                            ),
                            "/large-map.xml" => ("200 OK", "application/xml", "", "x".repeat(8192)),
                            "/retry-map.xml"
                                if metrics
                                    .lock()
                                    .unwrap()
                                    .counts
                                    .get(&path)
                                    .copied()
                                    .unwrap_or(0)
                                    <= 2 =>
                            {
                                (
                                    "503 Service Unavailable",
                                    "text/plain",
                                    "",
                                    "temporary sitemap failure".into(),
                                )
                            }
                            "/retry-map.xml" => (
                                "200 OK",
                                "application/xml",
                                "",
                                format!(
                                    "<urlset><url><loc>{origin}/recovered-map</loc></url></urlset>"
                                ),
                            ),
                            "/burst" => (
                                "200 OK",
                                "text/html",
                                "",
                                format!(
                                    "<html><title>Burst frontier fixture</title><body><main><p>Deterministic frontier fixture contains sufficient documentation prose.</p>{}</main></body></html>",
                                    (0..30)
                                        .map(|index| format!(
                                            "<a href='/frontier/{index}'>Link</a>"
                                        ))
                                        .collect::<String>()
                                ),
                            ),
                            "/robots.txt" => (
                                "200 OK",
                                "text/plain",
                                "",
                                format!(
                                    "User-agent: *\nDisallow: /blocked\nSitemap: {origin}/sitemap.xml\n"
                                ),
                            ),
                            "/sitemap.xml" => (
                                "200 OK",
                                "application/xml",
                                "",
                                format!("<urlset><url><loc>{origin}/map</loc></url></urlset>"),
                            ),
                            "/redirect" => (
                                "302 Found",
                                "text/plain",
                                "Location: /destination\r\n",
                                String::new(),
                            ),
                            "/retry" => (
                                "503 Service Unavailable",
                                "text/plain",
                                "",
                                "temporary failure".into(),
                            ),
                            "/large" => ("200 OK", "text/html", "", "x".repeat(8192)),
                            "/missing" => ("404 Not Found", "text/plain", "", "missing".into()),
                            _ => (
                                "200 OK",
                                "text/html",
                                "",
                                format!(
                                    "<html><title>Fixture {path}</title><body><main><h1>Fixture {path}</h1><p>Unique documentation {path}. This deterministic page supplies substantial useful prose for independent process recovery and request policy validation.</p>{}</main></body></html>",
                                    if path == "/seed" {
                                        "<a href='/slow'>Slow</a><a href='/child'>Child</a>"
                                    } else {
                                        "<a href='/blocked'>Blocked</a><a href='/deny/x'>Denied</a><a href='http://example.invalid/outside'>Outside</a>"
                                    }
                                ),
                            ),
                        };
                        let response = format!(
                            "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let mut m = metrics.lock().unwrap();
                        m.active -= 1;
                        *m.origins.get_mut(&origin).unwrap() -= 1;
                    });
                }
            });
        }
        Self {
            origins,
            metrics,
            stop,
        }
    }
    fn wait_path(&self, path: &str) {
        wait(
            || {
                self.metrics
                    .lock()
                    .unwrap()
                    .counts
                    .get(path)
                    .copied()
                    .unwrap_or(0)
                    > 0
            },
            "fixture request",
        );
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn wait(mut condition: impl FnMut() -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(40);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        thread::sleep(Duration::from_millis(20));
    }
}
struct Run {
    dir: PathBuf,
    id: String,
    database: Client,
    database_url: String,
    address: SocketAddr,
}
impl Run {
    fn new(
        fixture: &Fixture,
        seeds: &[(&str, &[&str])],
        target: usize,
        quota: usize,
        spacing: usize,
        frontier: usize,
    ) -> Self {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("disposable PostgreSQL database required");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let id = format!(
            "run-{:x}",
            Sha256::digest(format!("ticket6-{}-{nonce}", std::process::id()).as_bytes())
        );
        let dir = std::env::temp_dir().join(&id);
        fs::create_dir_all(&dir).unwrap();
        let mut config = format!(
            "schema_version = 'scout.sources.v1'\ncontact_url = 'https://example.invalid/scout'\nuser_agent = 'Scout-Test/1 (+https://example.invalid/scout)'\npage_target = {target}\nmin_content_chars = 20\nmax_decoded_body_bytes = 2048\nmax_frontier = {frontier}\nglobal_concurrency = 2\norigin_concurrency = 1\nmin_start_spacing_ms = {spacing}\nrequest_timeout_seconds = 1\nmax_attempts = 2\nretry_backoff_ms = 10\nretry_max_delay_ms = 10\nmax_total_attempts = 100\nmax_duration_seconds = 40\nrobots_cache_seconds = 60\n"
        );
        for (index, (source, paths)) in seeds.iter().enumerate() {
            let origin = &fixture.origins[index];
            let urls = paths
                .iter()
                .map(|path| format!("'{origin}{path}'"))
                .collect::<Vec<_>>()
                .join(",");
            config.push_str(&format!("\n[[sources]]\nsource_id = '{source}'\nseeds = [{urls}]\nallowed_origins = ['{origin}']\npath_prefixes = ['/']\ndeny_path_prefixes = ['/deny']\nminimum_page_quota = {quota}\n"));
        }
        fs::write(dir.join("sources.toml"), config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        Self {
            dir,
            id,
            database: Client::connect(&database_url, NoTls).unwrap(),
            database_url,
            address,
        }
    }
    fn coordinator(&self, label: &str) -> Process {
        let child = Command::new(env!("CARGO_BIN_EXE_scout"))
            .args([
                "coordinator",
                "--config",
                self.dir.join("sources.toml").to_str().unwrap(),
                "--data-dir",
                self.dir.to_str().unwrap(),
                "--bind",
                &self.address.to_string(),
                "--run-id",
                &self.id,
            ])
            .env("SCOUT_DATABASE_URL", &self.database_url)
            .stdout(Stdio::from(
                fs::File::create(self.dir.join(format!("{label}.stdout"))).unwrap(),
            ))
            .stderr(Stdio::from(
                fs::File::create(self.dir.join(format!("{label}.stderr"))).unwrap(),
            ))
            .spawn()
            .unwrap();
        let mut process = Process(child);
        wait(
            || {
                assert!(
                    process.0.try_wait().unwrap().is_none(),
                    "coordinator exited: {}",
                    fs::read_to_string(self.dir.join(format!("{label}.stderr"))).unwrap()
                );
                TcpStream::connect(self.address).is_ok()
            },
            "coordinator listening",
        );
        process
    }
    fn worker(&self, name: &str, tasks: Option<usize>) -> Process {
        let mut command = Command::new(env!("CARGO_BIN_EXE_scout"));
        command.args([
            "worker",
            "--coordinator",
            &format!("http://{}", self.address),
            "--data-dir",
            self.dir.to_str().unwrap(),
            "--worker-id",
            name,
        ]);
        if let Some(tasks) = tasks {
            command.args(["--max-tasks", &tasks.to_string()]);
        }
        Process(
            command
                .stdout(Stdio::from(
                    fs::File::create(self.dir.join(format!("{name}.stdout"))).unwrap(),
                ))
                .stderr(Stdio::from(
                    fs::File::create(self.dir.join(format!("{name}.stderr"))).unwrap(),
                ))
                .spawn()
                .unwrap(),
        )
    }
    fn count(&mut self, table: &str, predicate: &str) -> i64 {
        self.database
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE crawl_run_id=$1 AND {predicate}"),
                &[&self.id],
            )
            .unwrap()
            .get(0)
    }
    fn finish_workers(&self, workers: &mut [Process]) {
        for worker in workers {
            wait(
                || worker.0.try_wait().unwrap().is_some(),
                "worker completion",
            );
            assert!(
                worker.0.wait().unwrap().success(),
                "worker failed; logs {}",
                self.dir.display()
            );
        }
    }
    fn events(&self) -> Vec<Value> {
        let mut events: Vec<Value> = fs::read_dir(self.dir.join("logs"))
            .unwrap()
            .filter_map(Result::ok)
            .flat_map(|file| {
                fs::read_to_string(file.path())
                    .unwrap()
                    .lines()
                    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                    .collect::<Vec<_>>()
            })
            .collect();
        events.sort_by_key(|event| event["timestamp"].to_string());
        events
    }
    fn evidence(&self, name: &str, value: Value) {
        if let Ok(output) = std::env::var("SCOUT_TEST_EVIDENCE_DIR") {
            let output = PathBuf::from(output).join(name);
            if output.exists() {
                fs::remove_dir_all(&output).unwrap();
            }
            fs::create_dir_all(&output).unwrap();
            fs::write(
                output.join("assertions.json"),
                serde_json::to_vec_pretty(&value).unwrap(),
            )
            .unwrap();
            fs::copy(self.dir.join("sources.toml"), output.join("sources.toml")).unwrap();
            for entry in fs::read_dir(&self.dir).unwrap().filter_map(Result::ok) {
                if matches!(
                    entry
                        .path()
                        .extension()
                        .and_then(|extension| extension.to_str()),
                    Some("stdout" | "stderr")
                ) {
                    fs::copy(entry.path(), output.join(entry.file_name())).unwrap();
                }
            }
            for entry in fs::read_dir(self.dir.join("logs"))
                .unwrap()
                .filter_map(Result::ok)
            {
                fs::copy(entry.path(), output.join(entry.file_name())).unwrap();
            }
        }
        println!("{name}: {value}");
    }
}
impl Drop for Run {
    fn drop(&mut self) {
        // Preserve only fixture config/logs on failure, while still removing all crawl data.
        if thread::panicking() {
            if let Ok(output) = std::env::var("SCOUT_TEST_EVIDENCE_DIR") {
                let output = PathBuf::from(output).join(format!("failure-{}", self.id));
                let _ = fs::create_dir_all(&output);
                let _ = fs::copy(self.dir.join("sources.toml"), output.join("sources.toml"));
                for root in [&self.dir, &self.dir.join("logs")] {
                    if let Ok(entries) = fs::read_dir(root) {
                        for entry in entries.filter_map(Result::ok) {
                            if entry.path().is_file()
                                && (root.ends_with("logs")
                                    || matches!(
                                        entry
                                            .path()
                                            .extension()
                                            .and_then(|extension| extension.to_str()),
                                        Some("stdout" | "stderr")
                                    ))
                            {
                                let _ = fs::copy(entry.path(), output.join(entry.file_name()));
                            }
                        }
                    }
                }
            }
        }
        let _ = self.database.execute(
            "DELETE FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&self.id],
        );
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn worker_kill_and_coordinator_restart_preserve_accepted_work() {
    let fixture = Fixture::new();
    let mut run = Run::new(&fixture, &[("docs", &["/seed"])], 10, 0, 250, 20);
    let coordinator = run.coordinator("before");
    let worker = run.worker("killed-worker", None);
    fixture.wait_path("/slow");
    let accepted_before = run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL");
    assert!(accepted_before >= 1);
    let old = run.database.query_one("SELECT task_id,lease_epoch,lease_deadline FROM scout_crawl_tasks WHERE crawl_run_id=$1 AND normalized_url LIKE '%/slow'", &[&run.id]).unwrap();
    let task_id: i64 = old.get(0);
    let epoch: i64 = old.get(1);
    let spacing_before: String = run
        .database
        .query_one(
            "SELECT next_allowed_at::text FROM scout_crawl_request_spacing WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    drop(worker);
    drop(coordinator);
    let _coordinator = run.coordinator("after");
    let spacing_after: String = run
        .database
        .query_one(
            "SELECT next_allowed_at::text FROM scout_crawl_request_spacing WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(spacing_before, spacing_after);
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        accepted_before
    );
    assert!(run.count("scout_crawl_tasks", "status IN ('pending','leased')") >= 1);
    let mut workers = vec![
        run.worker("replacement-a", None),
        run.worker("replacement-b", None),
    ];
    run.finish_workers(&mut workers);
    let recovered = run
        .database
        .query_one(
            "SELECT lease_epoch,status FROM scout_crawl_tasks WHERE task_id=$1",
            &[&task_id],
        )
        .unwrap();
    assert_eq!(recovered.get::<_, i64>(0), epoch + 1);
    assert_eq!(recovered.get::<_, String>(1), "succeeded");
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        4
    );
    assert_eq!(
        run.count("scout_crawl_request_permits", "status='expired'"),
        1
    );
    let too_early: i64 = run.database.query_one("SELECT count(*) FROM scout_crawl_request_permits old JOIN scout_crawl_request_permits new ON old.crawl_run_id=new.crawl_run_id AND new.permit_id>old.permit_id WHERE old.crawl_run_id=$1 AND old.status='expired' AND new.acquired_at<old.expires_at", &[&run.id]).unwrap().get(0);
    assert_eq!(too_early, 0);
    let permit_lifetime_ms: i64 = run.database.query_one("SELECT (extract(epoch FROM (expires_at-acquired_at))*1000)::BIGINT FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND status='expired'", &[&run.id]).unwrap().get(0);
    assert!(permit_lifetime_ms >= 2000);

    assert_eq!(
        run.events()
            .iter()
            .filter(|event| event["event"] == "coordinator.resumed")
            .count(),
        1
    );
    run.evidence("recovery", serde_json::json!({"run_id": run.id,"accepted_before_restart":accepted_before,"accepted_after":4,"reclaimed_epoch":epoch+1,"abandoned_permits":1,"premature_permit_recovery":0,"permit_timeout_plus_margin_ms":permit_lifetime_ms,"persisted_spacing":spacing_after}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn all_request_classes_obey_shared_policy_and_terminal_failures_continue() {
    let fixture = Fixture::new();
    let mut run = Run::new(
        &fixture,
        &[
            (
                "alpha",
                &[
                    "/redirect",
                    "/retry",
                    "/large",
                    "/missing",
                    "/blocked",
                    "/burst",
                ],
            ),
            ("beta", &["/good", "/other"]),
        ],
        50,
        0,
        100,
        12,
    );
    let _coordinator = run.coordinator("policy");
    let mut workers = (0..3)
        .map(|index| run.worker(&format!("policy-{index}"), None))
        .collect::<Vec<_>>();
    run.finish_workers(&mut workers);
    let m = fixture.metrics.lock().unwrap();
    assert!(m.peak <= 2);
    assert!(m.origin_peak.values().all(|peak| *peak <= 1));
    assert!(!m.counts.contains_key("/blocked"));
    assert!(!m.counts.contains_key("/deny/x"));
    assert_eq!(m.counts.get("/retry"), Some(&4));
    for origin in &fixture.origins {
        let starts = m
            .requests
            .iter()
            .filter(|(_, value, _)| value == origin)
            .map(|(start, _, _)| *start)
            .collect::<Vec<_>>();
        for pair in starts.windows(2) {
            assert!(pair[1].duration_since(pair[0]) >= Duration::from_millis(80));
        }
    }
    assert!(m.counts.contains_key("/sitemap.xml"));
    assert!(m.counts.contains_key("/destination"));
    drop(m);
    let spacing_violations: i64 = run.database.query_one("SELECT count(*) FROM (SELECT extract(epoch FROM (acquired_at-lag(acquired_at) OVER (PARTITION BY schedule_key ORDER BY acquired_at)))*1000 AS gap_ms FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND acquired_at IS NOT NULL) gaps WHERE gap_ms < 100", &[&run.id]).unwrap().get(0);
    assert_eq!(spacing_violations, 0);
    let min_permit_gap_ms: f64 = run.database.query_one("SELECT min(gap_ms)::DOUBLE PRECISION FROM (SELECT extract(epoch FROM (acquired_at-lag(acquired_at) OVER (PARTITION BY schedule_key ORDER BY acquired_at)))*1000 AS gap_ms FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND acquired_at IS NOT NULL) gaps", &[&run.id]).unwrap().get(0);
    let metric = fixture.metrics.lock().unwrap();
    let min_http_gap_ms = fixture
        .origins
        .iter()
        .filter_map(|origin| {
            let starts = metric
                .requests
                .iter()
                .filter(|(_, value, _)| value == origin)
                .map(|(start, _, _)| *start)
                .collect::<Vec<_>>();
            starts
                .windows(2)
                .map(|pair| pair[1].duration_since(pair[0]).as_secs_f64() * 1000.0)
                .min_by(f64::total_cmp)
        })
        .min_by(f64::total_cmp)
        .unwrap();
    drop(metric);
    let retry = run.database.query_one("SELECT status,task_attempt_count FROM scout_crawl_tasks WHERE crawl_run_id=$1 AND normalized_url LIKE '%/retry'", &[&run.id]).unwrap();
    assert_eq!(retry.get::<_, String>(0), "failed");
    assert_eq!(retry.get::<_, i32>(1), 2);
    for (path, reason) in [
        ("%/large", "body_limit_exceeded"),
        ("%/blocked", "robots_denied"),
    ] {
        let rejections = run.database.query("SELECT r.rejection_reason FROM scout_crawl_task_results r JOIN scout_crawl_tasks t USING(task_id) WHERE r.crawl_run_id=$1 AND t.normalized_url LIKE $2", &[&run.id, &path]).unwrap();
        assert!(!rejections.is_empty());
        for row in rejections {
            assert_eq!(row.get::<_, String>(0), reason);
        }
    }
    assert_eq!(run.count("scout_crawl_tasks", "status='failed'"), 2);
    assert!(run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL") >= 4);
    assert_eq!(
        run.count("scout_crawl_tasks", "status IN ('pending','leased')"),
        0
    );
    let wrong_owner: i64 = run.database.query_one("SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id=$1 AND ((source_id='alpha' AND origin<>$2) OR (source_id='beta' AND origin<>$3))", &[&run.id, &fixture.origins[0], &fixture.origins[1]]).unwrap().get(0);
    assert_eq!(wrong_owner, 0);
    let burst = run.database.query_one("SELECT r.accepted_discovered_count,r.rejected_discovered_count FROM scout_crawl_task_results r JOIN scout_crawl_tasks t USING(task_id) WHERE r.crawl_run_id=$1 AND t.normalized_url LIKE '%/burst'", &[&run.id]).unwrap();
    let burst_accepted: i64 = burst.get(0);
    let burst_rejected: i64 = burst.get(1);
    assert!(burst_accepted <= 12 && burst_rejected > 0);
    let status: String = run
        .database
        .query_one(
            "SELECT status FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(status, "active");
    let requests = fixture.metrics.lock().unwrap();
    run.evidence("boundaries", serde_json::json!({"run_id":run.id,"global_peak":requests.peak,"origin_peak":requests.origin_peak,"requests":requests.counts,"failed_tasks":2,"wrong_owner":0,"configured_spacing_ms":100,"fixture_spacing_tolerance_ms":80,"durable_spacing_violations":spacing_violations,"minimum_permit_gap_ms":min_permit_gap_ms,"minimum_http_gap_ms":min_http_gap_ms,"frontier_limit":12,"burst_accepted":burst_accepted,"burst_rejected":burst_rejected,"body_limit":2048}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn target_and_source_minima_stop_claims_with_bounded_overshoot() {
    let fixture = Fixture::new();
    let mut run = Run::new(
        &fixture,
        &[
            ("alpha", &["/a", "/b", "/c"]),
            ("beta", &["/d", "/e", "/f"]),
        ],
        2,
        1,
        50,
        20,
    );
    let _coordinator = run.coordinator("target");
    let mut workers = (0..3)
        .map(|index| run.worker(&format!("target-{index}"), None))
        .collect::<Vec<_>>();
    run.finish_workers(&mut workers);
    let events = run.events();
    let coordinator_events = events
        .iter()
        .filter(|event| {
            matches!(
                event["event"].as_str(),
                Some("task.claimed" | "capture.accepted")
            )
        })
        .collect::<Vec<_>>();
    let first_target = coordinator_events
        .iter()
        .position(|event| {
            event["event"] == "capture.accepted" && event["data"]["target_reached"] == true
        })
        .expect("target reached");
    assert!(
        !coordinator_events[first_target + 1..]
            .iter()
            .any(|event| event["event"] == "task.claimed")
    );
    let outstanding = coordinator_events[..=first_target]
        .iter()
        .filter(|event| event["event"] == "task.claimed")
        .count()
        - coordinator_events[..=first_target]
            .iter()
            .filter(|event| event["event"] == "capture.accepted")
            .count();
    let total = run.count("scout_crawl_content_groups", "true");
    assert!(total >= 2 && total <= 2 + outstanding as i64);
    assert_eq!(run.database.query_one("SELECT count(DISTINCT source_id) FROM scout_crawl_content_groups WHERE crawl_run_id=$1", &[&run.id]).unwrap().get::<_,i64>(0), 2);
    run.evidence("targets", serde_json::json!({"run_id":run.id,"page_target":2,"per_source_minimum":1,"successful_groups":total,"overshoot":total-2,"active_work_at_target":outstanding,"claims_after_target":0}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn bounded_target_with_remaining_frontier_is_page_target_reached() {
    let fixture = Fixture::new();
    let mut run = Run::new(
        &fixture,
        &[("docs", &["/one", "/two", "/three"])],
        1,
        0,
        50,
        20,
    );
    let _coordinator = run.coordinator("bounded-target");
    let mut workers = vec![run.worker("bounded-target-worker", None)];
    run.finish_workers(&mut workers);

    assert!(run.count("scout_crawl_tasks", "status='pending'") >= 2);
    let stop_reason: String = run
        .database
        .query_one(
            "SELECT stop_reason FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(stop_reason, "page_target_reached");
    assert_eq!(
        run.count("scout_crawl_events", "event_type='crawl_completed'"),
        1
    );
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn completion_waits_for_accepted_capture_spool_projection() {
    let fixture = Fixture::new();
    let mut run = Run::new(&fixture, &[("docs", &["/good"])], 1, 0, 50, 1);
    let _coordinator = run.coordinator("projection");
    let spool_path = run
        .dir
        .join("crawls")
        .join(&run.id)
        .join("capture-spool.sqlite3");
    let projection_lock = Connection::open(spool_path).unwrap();
    projection_lock.execute_batch("BEGIN IMMEDIATE").unwrap();

    let mut workers = vec![run.worker("projection-worker", None)];
    wait(
        || run.count("scout_crawl_events", "event_type='capture_committed'") == 1,
        "durable capture acceptance",
    );
    let projected_while_locked: i64 = projection_lock
        .query_row("SELECT count(*) FROM captures", [], |row| row.get(0))
        .unwrap();
    assert_eq!(projected_while_locked, 0);
    let mut trigger = run.worker("projection-trigger", Some(1));
    wait(
        || {
            run.events().iter().any(|event| {
                event["event"] == "worker.registered"
                    && event["data"]["worker_id"] == "projection-trigger"
            })
        },
        "projection trigger registration",
    );
    wait(
        || trigger.0.try_wait().unwrap().is_some(),
        "projection trigger response",
    );
    assert!(trigger.0.wait().unwrap().success());
    let status_while_projection_blocked: String = run
        .database
        .query_one(
            "SELECT status FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(status_while_projection_blocked, "active");
    assert_eq!(
        run.count("scout_crawl_events", "event_type='crawl_completed'"),
        0
    );

    projection_lock.execute_batch("ROLLBACK").unwrap();
    drop(projection_lock);
    run.finish_workers(&mut workers);
    let stop_reason: String = run
        .database
        .query_one(
            "SELECT stop_reason FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(stop_reason, "frontier_exhausted");
    assert_eq!(
        run.count("scout_crawl_events", "event_type='crawl_completed'"),
        1
    );
    let spool = Connection::open(
        run.dir
            .join("crawls")
            .join(&run.id)
            .join("capture-spool.sqlite3"),
    )
    .unwrap();
    let projected_after_release: i64 = spool
        .query_row("SELECT count(*) FROM captures", [], |row| row.get(0))
        .unwrap();
    assert_eq!(projected_after_release, 1);
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn exact_target_with_drained_frontier_is_frontier_exhausted() {
    let fixture = Fixture::new();
    let mut run = Run::new(&fixture, &[("docs", &["/good"])], 1, 0, 50, 1);
    let _coordinator = run.coordinator("exact-target");
    let mut workers = vec![run.worker("exact-target-worker", None)];
    run.finish_workers(&mut workers);

    assert_eq!(
        run.count("scout_crawl_tasks", "status IN ('pending','leased')"),
        0
    );
    let stop_reason: String = run
        .database
        .query_one(
            "SELECT stop_reason FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(stop_reason, "frontier_exhausted");
    assert_eq!(
        run.count("scout_crawl_events", "event_type='crawl_completed'"),
        1
    );
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn storage_integrity_failure_fails_coordinator_and_run() {
    let fixture = Fixture::new();
    let mut run = Run::new(&fixture, &[("docs", &["/good"])], 1, 0, 50, 20);
    let mut coordinator = run.coordinator("integrity");
    let bodies = run.dir.join("crawls").join(&run.id).join("bodies");
    fs::remove_dir(&bodies).unwrap();
    fs::write(&bodies, "blocked body store").unwrap();
    let mut workers = vec![run.worker("integrity-worker", None)];
    wait(
        || coordinator.0.try_wait().unwrap().is_some(),
        "coordinator integrity exit",
    );
    assert!(!coordinator.0.wait().unwrap().success());
    wait(
        || workers[0].0.try_wait().unwrap().is_some(),
        "worker integrity exit",
    );
    assert!(!workers[0].0.wait().unwrap().success());
    let status: String = run
        .database
        .query_one(
            "SELECT status FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(status, "failed");
    let resume = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "coordinator",
            "--config",
            run.dir.join("sources.toml").to_str().unwrap(),
            "--data-dir",
            run.dir.to_str().unwrap(),
            "--bind",
            &run.address.to_string(),
            "--run-id",
            &run.id,
        ])
        .env("SCOUT_DATABASE_URL", &run.database_url)
        .output()
        .unwrap();
    assert!(!resume.status.success());
    assert!(String::from_utf8_lossy(&resume.stderr).contains("failed crawl run cannot resume"));
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        0
    );
    run.evidence("integrity", serde_json::json!({"run_id":run.id,"coordinator_exit_success":false,"run_status":status,"published_captures":0}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn exhausted_source_minimum_does_not_poll_unclaimable_frontier() {
    let fixture = Fixture::new();
    let mut run = Run::new(
        &fixture,
        &[("alpha", &["/good", "/other"]), ("beta", &["/blocked"])],
        1,
        1,
        50,
        20,
    );
    let config_path = run.dir.join("sources.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("max_duration_seconds = 40", "max_duration_seconds = 4"),
    )
    .unwrap();
    let _coordinator = run.coordinator("quota-exhausted");
    let mut workers = vec![run.worker("quota-worker", None)];
    let started = Instant::now();
    wait(
        || run.count("scout_crawl_content_groups", "source_id='alpha'") >= 1,
        "alpha quota capture",
    );
    assert!(run.count("scout_crawl_tasks", "status='pending'") >= 1);
    wait(
        || run.count("scout_crawl_tasks", "status='leased'") == 0,
        "in-flight quota task completion",
    );
    assert!(run.count("scout_crawl_tasks", "status='pending'") >= 1);
    let drained_status: String = run
        .database
        .query_one(
            "SELECT status FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(drained_status, "active");
    assert!(workers[0].0.try_wait().unwrap().is_none());
    assert_eq!(
        run.count("scout_crawl_events", "event_type='crawl_completed'"),
        0
    );
    assert_eq!(
        run.count("scout_crawl_content_groups", "source_id='beta'"),
        0
    );
    run.finish_workers(&mut workers);
    let stop_reason: String = run
        .database
        .query_one(
            "SELECT stop_reason FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(stop_reason, "duration_reached");
    assert!(started.elapsed() >= Duration::from_secs(3));
    assert!(run.count("scout_crawl_tasks", "status='pending'") >= 1);
    run.evidence("exhausted-quota", serde_json::json!({"run_id":run.id,"unmet_source":"beta","active_with_quota_blocked_pending":true,"stop_reason":stop_reason,"duration_ms":started.elapsed().as_millis()}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn corrupt_accepted_body_fails_resume_and_marks_run_failed() {
    let fixture = Fixture::new();
    let mut run = Run::new(&fixture, &[("docs", &["/good"])], 1, 0, 50, 20);
    let coordinator = run.coordinator("accepted");
    let mut workers = vec![run.worker("accepted-worker", Some(1))];
    run.finish_workers(&mut workers);
    drop(coordinator);
    let body_hash: String = run.database.query_one("SELECT body_hash FROM scout_crawl_task_results WHERE crawl_run_id=$1 AND capture_metadata IS NOT NULL", &[&run.id]).unwrap().get(0);
    fs::write(
        run.dir
            .join("crawls")
            .join(&run.id)
            .join("bodies")
            .join(body_hash),
        "corrupt",
    )
    .unwrap();
    let resumed = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "coordinator",
            "--config",
            run.dir.join("sources.toml").to_str().unwrap(),
            "--data-dir",
            run.dir.to_str().unwrap(),
            "--bind",
            &run.address.to_string(),
            "--run-id",
            &run.id,
        ])
        .env("SCOUT_DATABASE_URL", &run.database_url)
        .output()
        .unwrap();
    assert!(!resumed.status.success());
    let status: String = run
        .database
        .query_one(
            "SELECT status FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(status, "failed");
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        1
    );
    run.evidence("corrupt-resume", serde_json::json!({"run_id":run.id,"resume_exit_success":false,"run_status":status,"durable_captures_preserved":1,"stderr":String::from_utf8_lossy(&resumed.stderr)}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn sitemap_tree_is_shared_across_workers_and_links_survive_page_failure() {
    let fixture = Fixture::with_sitemaps("tree");
    let mut run = Run::new(
        &fixture,
        &[("docs", &["/missing", "/one", "/two", "/three"])],
        20,
        0,
        50,
        20,
    );
    let _coordinator = run.coordinator("sitemap-shared");
    let mut workers = vec![run.worker("sitemap-shared-0", None)];
    fixture.wait_path("/child.xml");
    workers.push(run.worker("sitemap-shared-1", None));
    workers.push(run.worker("sitemap-shared-2", None));
    run.finish_workers(&mut workers);
    let m = fixture.metrics.lock().unwrap();
    assert_eq!(m.counts.get("/sitemap.xml"), Some(&1));
    assert_eq!(m.counts.get("/child.xml"), Some(&1));
    assert_eq!(m.counts.get("/map"), Some(&1));
    assert_eq!(m.counts.get("/missing"), Some(&2));
    assert!(m.peak <= 1);
    drop(m);
    let scan = run.database.query_one("SELECT state,attempt_count,accepted_discovered_count,rejected_discovered_count FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1", &[&run.id]).unwrap();
    let scanner_url: String = run.database.query_one("SELECT task.normalized_url FROM scout_crawl_sitemap_discovery scan JOIN scout_crawl_tasks task ON task.task_id=scan.owner_task_id WHERE scan.crawl_run_id=$1", &[&run.id]).unwrap().get(0);
    assert!(scanner_url.ends_with("/missing"));
    assert_eq!(scan.get::<_, String>(0), "completed");
    assert_eq!(scan.get::<_, i32>(1), 1);
    assert_eq!(scan.get::<_, i64>(2), 1);
    assert!(scan.get::<_, i64>(3) >= 1);
    assert_eq!(run.count("scout_crawl_tasks", "status='failed'"), 1);
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        4
    );
    let events = run.events();
    let scan_index = events
        .iter()
        .position(|event| event["event"] == "sitemap.discovery_committed")
        .unwrap();
    let missing_index = events.iter().position(|event| {
        event["event"] == "task.failed"
            && event["data"]["url"]
                .as_str()
                .is_some_and(|url| url.ends_with("/missing"))
    });
    if let Some(index) = missing_index {
        assert!(scan_index < index);
    }
    run.evidence("sitemap-shared",serde_json::json!({"run_id":run.id,"root_requests":1,"child_requests":1,"recursion_cycle_skipped":true,"workers":3,"scan_attempts":1,"discovered_pages_durable_before_page_failure":true,"failed_page_tasks":1,"successful_captures":4,"outside_discovery_rejected":true}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn sitemap_worker_kill_and_coordinator_restart_reclaim_scan() {
    let fixture = Fixture::with_sitemaps("tree-kill");
    let mut run = Run::new(&fixture, &[("docs", &["/one", "/two"])], 20, 0, 50, 20);
    let coordinator = run.coordinator("sitemap-before");
    let worker = run.worker("sitemap-killed", None);
    fixture.wait_path("/child.xml");
    let before = run.database.query_one("SELECT state,scan_epoch,attempt_count FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1", &[&run.id]).unwrap();
    assert_eq!(before.get::<_, String>(0), "running");
    assert_eq!(before.get::<_, i64>(1), 1);
    drop(worker);
    drop(coordinator);
    let _coordinator = run.coordinator("sitemap-after");
    let mut workers = vec![
        run.worker("sitemap-replacement-a", None),
        run.worker("sitemap-replacement-b", None),
    ];
    run.finish_workers(&mut workers);
    let after = run.database.query_one("SELECT state,scan_epoch,attempt_count FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1", &[&run.id]).unwrap();
    assert_eq!(after.get::<_, String>(0), "completed");
    assert_eq!(after.get::<_, i64>(1), 2);
    assert_eq!(after.get::<_, i32>(2), 2);
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        3
    );
    let m = fixture.metrics.lock().unwrap();
    assert_eq!(m.counts.get("/sitemap.xml"), Some(&2));
    assert_eq!(m.counts.get("/child.xml"), Some(&2));
    assert_eq!(m.counts.get("/map"), Some(&1));
    drop(m);
    assert!(run.count("scout_crawl_request_permits", "status='expired'") >= 1);
    run.evidence("sitemap-takeover",serde_json::json!({"run_id":run.id,"killed_during":"/child.xml","scan_epoch_after":2,"scan_attempts":2,"root_requests":2,"child_requests":2,"accepted_captures":3,"map_pages_requested":1}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn sitemap_completed_discovery_survives_restart_before_page_result() {
    let fixture = Fixture::with_sitemaps("tree");
    let mut run = Run::new(&fixture, &[("docs", &["/slow", "/two"])], 20, 0, 50, 20);
    let coordinator = run.coordinator("sitemap-committed-before");
    let worker = run.worker("sitemap-committed-worker", None);
    fixture.wait_path("/slow");
    let state: String = run
        .database
        .query_one(
            "SELECT state FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(state, "completed");
    assert_eq!(
        run.count("scout_crawl_tasks", "normalized_url LIKE '%/map'"),
        1
    );
    drop(worker);
    drop(coordinator);
    let _coordinator = run.coordinator("sitemap-committed-after");
    let mut workers = vec![run.worker("sitemap-committed-replacement", None)];
    run.finish_workers(&mut workers);
    let m = fixture.metrics.lock().unwrap();
    assert_eq!(m.counts.get("/sitemap.xml"), Some(&1));
    assert_eq!(m.counts.get("/child.xml"), Some(&1));
    assert_eq!(m.counts.get("/map"), Some(&1));
    drop(m);
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        3
    );
    run.evidence("sitemap-committed-restart",serde_json::json!({"run_id":run.id,"durable_map_task_before_restart":1,"root_requests":1,"child_requests":1,"scan_repeated_after_restart":false,"accepted_captures":3}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn sitemap_optional_caps_and_mixed_child_retries_preserve_pages() {
    let fixture = Fixture::with_sitemaps("caps");
    let mut run = Run::new(&fixture, &[("docs", &["/one"])], 20, 0, 50, 20);
    let _coordinator = run.coordinator("sitemap-caps");
    let mut workers = vec![run.worker("sitemap-caps-worker", None)];
    run.finish_workers(&mut workers);
    let m = fixture.metrics.lock().unwrap();
    assert_eq!(m.counts.get("/sitemap.xml"), Some(&2));
    assert_eq!(m.counts.get("/child.xml"), Some(&2));
    assert_eq!(m.counts.get("/retry-map.xml"), Some(&3));
    assert_eq!(m.counts.get("/large-map.xml"), Some(&2));
    assert!(!m.counts.contains_key("/denied-map.xml"));
    assert_eq!(m.counts.get("/recovered-map"), Some(&1));
    drop(m);
    let scan=run.database.query_one("SELECT state,attempt_count,error_reason FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1", &[&run.id]).unwrap();
    assert_eq!(scan.get::<_, String>(0), "failed");
    assert_eq!(scan.get::<_, i32>(1), 2);
    assert!(!scan.get::<_, String>(2).is_empty());
    assert_eq!(run.count("scout_crawl_tasks", "status='failed'"), 0);
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        3
    );
    run.evidence("sitemap-caps",serde_json::json!({"run_id":run.id,"terminal_scan":"failed","scan_attempts":2,"root_requests":2,"transient_child_requests":3,"oversized_child_requests":2,"robots_denied_child_requests":0,"successful_pages":3,"ordinary_page_failures":0,"partial_discoveries_retained":true,"mixed_sibling_retry_preserved":true}));
}

#[test]
#[ignore = "requires disposable PostgreSQL database"]
fn sitemap_last_attempt_worker_death_records_terminal_discovery_failure() {
    let fixture = Fixture::with_sitemaps("tree-kill");
    let mut run = Run::new(&fixture, &[("docs", &["/one"])], 20, 0, 50, 20);
    let config = run.dir.join("sources.toml");
    fs::write(
        &config,
        fs::read_to_string(&config)
            .unwrap()
            .replace("max_attempts = 2", "max_attempts = 1"),
    )
    .unwrap();
    let _coordinator = run.coordinator("sitemap-last-attempt");
    let mut worker = run.worker("sitemap-last-killed", None);
    wait(
        || {
            assert!(
                worker.0.try_wait().unwrap().is_none(),
                "scanner exited before child request: {}",
                fs::read_to_string(run.dir.join("sitemap-last-killed.stderr")).unwrap()
            );
            fixture
                .metrics
                .lock()
                .unwrap()
                .counts
                .get("/child.xml")
                .copied()
                .unwrap_or_default()
                > 0
        },
        "child fixture request",
    );
    drop(worker);
    let mut workers = vec![run.worker("sitemap-last-replacement", None)];
    run.finish_workers(&mut workers);
    let scan=run.database.query_one("SELECT state,attempt_count,error_reason FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1", &[&run.id]).unwrap();
    assert_eq!(scan.get::<_, String>(0), "failed");
    assert_eq!(scan.get::<_, i32>(1), 1);
    assert_eq!(
        scan.get::<_, String>(2),
        "frontier/run stopped before discovery committed"
    );
    assert_eq!(run.count("scout_crawl_tasks", "status='failed'"), 1);
    assert_eq!(
        run.count("scout_crawl_task_results", "capture_metadata IS NOT NULL"),
        0
    );
    run.evidence("sitemap-exhausted-death",serde_json::json!({"run_id":run.id,"scan_attempts":1,"scan_state":"failed","owner_task_state":"failed","accepted_captures":0,"terminal_reason":"frontier/run stopped before discovery committed"}));
}
