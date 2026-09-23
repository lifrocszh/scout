use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let sequence = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "scout-{label}-{}-{nonce}-{sequence}",
        std::process::id()
    ));
    fs::create_dir_all(&path).expect("create temporary data directory");
    path
}

fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set request timeout");
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let count = stream.read(&mut chunk).expect("read request");
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).expect("request is UTF-8")
}

fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("write response headers");
    stream.write_all(body).expect("write response body");
}

fn run_scout(data_dir: &Path, args: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scout"));
    command.args(args);
    command.arg("--data-dir").arg(data_dir);
    command.output().expect("run scout")
}

fn write_crawl_config(data_dir: &Path, origin: &str, extra: &str) -> PathBuf {
    let path = data_dir.join("sources.toml");
    fs::write(
        &path,
        format!(
            r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
user_agent = "ScoutContract/1.0"
page_target = 1
{extra}

[[sources]]
source_id = "fixture"
seeds = ["{origin}/"]
allowed_origins = ["{origin}"]
path_prefixes = ["/"]
"#
        ),
    )
    .expect("write crawl config");
    path
}

fn manifest(data_dir: &Path, file: &str) -> Value {
    let crawl_dirs = fs::read_dir(data_dir.join("crawls"))
        .expect("read crawl directories")
        .map(|entry| entry.expect("read crawl directory entry").path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    assert_eq!(crawl_dirs.len(), 1, "expected one crawl directory");
    serde_json::from_slice(&fs::read(crawl_dirs[0].join(file)).expect("read crawl manifest"))
        .expect("parse crawl manifest")
}

fn valid_corpus() -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind corpus fixture");
    let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
    let handle = thread::spawn(move || {
        let page = br#"<!doctype html><html><head><title>Fixture page</title></head><body><main><h1>Fixture page</h1><p>Scout captures stable server rendered documentation content from a local contract fixture. This paragraph is longer than the admission threshold and supplies enough deterministic prose for building a Corpus snapshot and testing build profile validation at the CLI boundary.</p></main></body></html>"#;
        let mut requests = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept corpus request");
            let request = read_request(&mut stream);
            if request.starts_with("GET /robots.txt ") {
                respond(
                    &mut stream,
                    "200 OK",
                    "text/plain",
                    b"User-agent: *\nAllow: /\n",
                );
            } else {
                respond(&mut stream, "200 OK", "text/html; charset=utf-8", page);
            }
            requests.push(request);
        }
        requests
    });
    (origin, handle)
}

#[test]
fn crawl_attempt_safety_cap_writes_incomplete_manifest_without_corpus() {
    let data_dir = temp_dir("ticket-15-attempt-cap");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind safety fixture");
    let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept robots request");
        let request = read_request(&mut stream);
        assert!(request.starts_with("GET /robots.txt "));
        respond(
            &mut stream,
            "200 OK",
            "text/plain",
            b"User-agent: *\nAllow: /\n",
        );
    });
    let config = write_crawl_config(
        &data_dir,
        &origin,
        "max_attempts = 1\nmax_total_attempts = 1\nretry_backoff_ms = 0",
    );

    let output = run_scout(
        &data_dir,
        &[
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--json",
        ],
    );
    server.join().expect("join safety fixture");

    assert_eq!(output.status.code(), Some(1));
    let incomplete = manifest(&data_dir, "incomplete-manifest.json");
    assert_eq!(incomplete["status"], "incomplete");
    assert_eq!(incomplete["error"]["code"], "safety_cap");
    assert!(!data_dir.join("corpora").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("\"code\":\"safety_cap\""));

    fs::remove_dir_all(data_dir).expect("cleanup safety fixture");
}

#[test]
fn robots_outage_is_item_denial_and_does_not_abort_crawl() {
    let data_dir = temp_dir("ticket-15-robots-outage");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind robots fixture");
    let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept robots request");
        let request = read_request(&mut stream);
        assert!(request.starts_with("GET /robots.txt "));
        respond(
            &mut stream,
            "503 Service Unavailable",
            "text/plain",
            b"temporary outage",
        );
        vec![request]
    });
    let config = write_crawl_config(&data_dir, &origin, "max_attempts = 1\nretry_backoff_ms = 0");

    let output = run_scout(
        &data_dir,
        &[
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--json",
        ],
    );
    let requests = server.join().expect("join robots fixture");

    assert!(
        output.status.success(),
        "uncached robots outage must deny item, not fail run (status {:?})",
        output.status.code()
    );
    assert_eq!(requests.len(), 1, "robots denial must not fetch page");
    let complete = manifest(&data_dir, "crawl-manifest.json");
    assert_eq!(complete["status"], "complete");
    assert_eq!(complete["summary"]["rejected_count"], 1);
    assert_eq!(complete["summary"]["page_count"], 0);

    fs::remove_dir_all(data_dir).expect("cleanup robots fixture");
}

#[test]
fn sitemap_discovery_is_bounded_allowlisted_and_terminal_page_failures_continue() {
    let data_dir = temp_dir("ticket-26-sitemap");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind sitemap fixture");
    let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
    let config = data_dir.join("sources.toml");
    fs::write(
        &config,
        format!(
            r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
user_agent = "ScoutContract/1.0"
page_target = 3
global_concurrency = 1
max_attempts = 1
max_total_attempts = 20
retry_backoff_ms = 0

[[sources]]
source_id = "fixture"
seeds = ["{origin}/"]
allowed_origins = ["{origin}"]
path_prefixes = ["/"]
"#
        ),
    )
    .expect("write sitemap crawl config");

    let server = thread::spawn(move || {
        let page = |label: &str| {
            format!(
                "<!doctype html><html><body><main><h1>{label}</h1><p>{label} contains enough stable server rendered documentation content to satisfy the Page admission quality gate. This deterministic fixture exercises sitemap discovery, URL normalization, allowlist filtering, and terminal item failures without relying on live documentation.</p></main></body></html>"
            )
        };
        let sitemap_index = format!(
            "<?xml version=\"1.0\"?><sitemapindex><sitemap><loc>{origin}/broken.xml</loc></sitemap><sitemap><loc>{origin}/missing-sitemap.xml</loc></sitemap><sitemap><loc>{origin}/sitemap-pages.xml</loc></sitemap><sitemap><loc>{origin}/sitemap-pages.xml</loc></sitemap></sitemapindex>"
        );
        let sitemap_pages = format!(
            "<?xml version=\"1.0\"?><urlset><url><loc>{origin}/page-a</loc></url><url><loc>{origin}/page-a</loc></url><url><loc>{origin}/page-b?x=1&amp;y=2</loc></url><url><loc>https://outside.invalid/not-allowlisted</loc></url><url><loc>{origin}/missing</loc></url></urlset>"
        );
        let page_a = page("Sitemap page A");
        let page_b = page("Sitemap page B");
        let mut requests = Vec::new();
        for _ in 0..9 {
            let (mut stream, _) = listener.accept().expect("accept sitemap request");
            let request = read_request(&mut stream);
            if request.starts_with("GET /robots.txt ") {
                respond(
                    &mut stream,
                    "200 OK",
                    "text/plain",
                    format!("User-agent: *\nAllow: /\nSitemap: {origin}/sitemap-index.xml\n")
                        .as_bytes(),
                );
            } else if request.starts_with("GET /sitemap-index.xml ") {
                respond(
                    &mut stream,
                    "200 OK",
                    "application/xml",
                    sitemap_index.as_bytes(),
                );
            } else if request.starts_with("GET /broken.xml ") {
                respond(
                    &mut stream,
                    "200 OK",
                    "application/xml",
                    b"<urlset><url><loc>",
                );
            } else if request.starts_with("GET /missing-sitemap.xml ") {
                respond(
                    &mut stream,
                    "404 Not Found",
                    "text/plain",
                    b"missing sitemap",
                );
            } else if request.starts_with("GET /sitemap-pages.xml ") {
                respond(
                    &mut stream,
                    "200 OK",
                    "application/xml",
                    sitemap_pages.as_bytes(),
                );
            } else if request.starts_with("GET /page-a ") {
                respond(
                    &mut stream,
                    "200 OK",
                    "text/html; charset=utf-8",
                    page_a.as_bytes(),
                );
            } else if request.starts_with("GET /page-b?x=1&y=2 ") {
                respond(
                    &mut stream,
                    "200 OK",
                    "text/html; charset=utf-8",
                    page_b.as_bytes(),
                );
            } else if request.starts_with("GET / HTTP/1.1") || request.starts_with("GET /missing ")
            {
                respond(&mut stream, "404 Not Found", "text/plain", b"missing page");
            } else {
                panic!("unexpected sitemap request: {request}");
            }
            requests.push(request);
        }
        requests
    });

    let output = run_scout(
        &data_dir,
        &[
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--json",
        ],
    );
    let requests = server.join().expect("join sitemap fixture");

    assert!(output.status.success(), "crawl failed: {:?}", output.stderr);
    assert!(
        requests
            .iter()
            .all(|request| !request.contains("outside.invalid")),
        "outside-allowlist sitemap URL must not be fetched"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("\"event\":\"sitemap.discovered\""));
    assert!(stderr.contains("\"event\":\"sitemap.unavailable\""));
    let complete = manifest(&data_dir, "crawl-manifest.json");
    assert_eq!(complete["status"], "complete");
    assert_eq!(complete["summary"]["page_count"], 2);
    assert_eq!(complete["summary"]["failed_count"], 2);
    assert_eq!(complete["summary"]["stop_reason"], "frontier_exhausted");

    fs::remove_dir_all(data_dir).expect("cleanup sitemap fixture");
}

#[test]
fn index_build_rejects_profile_without_explicit_retrieval_inputs() {
    let data_dir = temp_dir("ticket-17-profile");
    let (origin, server) = valid_corpus();
    let config = write_crawl_config(&data_dir, &origin, "");
    let crawl = run_scout(
        &data_dir,
        &["crawl", "--config", config.to_str().expect("config path")],
    );
    server.join().expect("join corpus fixture");
    assert!(
        crawl.status.success(),
        "fixture crawl failed (status {:?})",
        crawl.status.code()
    );

    let snapshot_id = manifest(&data_dir, "crawl-manifest.json")["snapshot_id"]
        .as_str()
        .expect("snapshot id")
        .to_owned();
    let config_dir = data_dir.join("config");
    fs::create_dir_all(&config_dir).expect("create build config directory");
    fs::write(
        config_dir.join("index.toml"),
        format!("schema_version = \"scout.index.v1\"\nsnapshot_id = \"{snapshot_id}\"\n"),
    )
    .expect("write incomplete build profile");

    let output = run_scout(
        &data_dir,
        &["index", "build", "--corpus", &snapshot_id, "--json"],
    );

    assert_eq!(
        output.status.code(),
        Some(2),
        "incomplete build profile must be rejected"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("\"code\":\"invalid_configuration\""),
        "missing explicit profile error"
    );
    assert!(
        !data_dir.join("generations").join(".staging").exists(),
        "invalid profile must not create staging artifacts"
    );

    fs::remove_dir_all(data_dir).expect("cleanup profile fixture");
}
