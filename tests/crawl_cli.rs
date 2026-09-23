use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use sha2::{Digest, Sha256};

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
    fs::create_dir_all(&path).expect("create temp directory");
    path
}

fn write_config(data_dir: &Path, origin: &str, extra: &str) -> PathBuf {
    let config = format!(
        r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
user_agent = "FixtureBot/9.1"
page_target = 1
{extra}

[[sources]]
source_id = "fixture"
seeds = ["{origin}/"]
allowed_origins = ["{origin}"]
path_prefixes = ["/"]
"#
    );
    let path = data_dir.join("sources.toml");
    fs::write(&path, config).expect("write config");
    path
}

fn write_index_profile(
    data_dir: &Path,
    snapshot_id: &str,
    snippet_limit: usize,
    max_passage_tokens: usize,
    fusion_candidate_depth: usize,
) -> PathBuf {
    let config_dir = data_dir.join("config");
    fs::create_dir_all(&config_dir).expect("create index config directory");
    let path = config_dir.join("index.toml");
    fs::write(
        &path,
        format!(
            r#"schema_version = "scout.index.v1"
snapshot_id = "{snapshot_id}"
extraction_schema = "scout.extraction.v1"
passage_schema = "scout.passage.v1"
max_passage_tokens = {max_passage_tokens}
overlap_tokens = 0
no_overlap = true
analyzer_id = "scout.unicode-token-v1"
title_boost = 2.0
heading_boost = 1.5
body_boost = 1.0
candidate_depth = 5
writer_memory_bytes = 16000000
worker_count = 1
model_name = "BAAI/bge-small-en-v1.5"
model_source = "local"
model_revision = "fixture-revision"
model_license = "Apache-2.0"
onnx_sha256 = "fixture-onnx-sha256"
tokenizer_sha256 = "fixture-tokenizer-sha256"
model_config_sha256 = "fixture-config-sha256"
embedding_backend = "deterministic"
snippet_limit = {snippet_limit}
model_max_tokens = 512
pooling = "cls"
normalization = "l2"
query_instruction = "Represent this sentence for searching relevant passages: "
document_representation = "heading-path-and-passage-body"
dimensions = 384
embedding_batch_size = 4
embedding_workers = 1
embedding_intra_op_threads = 1
quantization = "none"
metric = "cosine"
scalar_kind = "f32"
connectivity = 16
construction_expansion = 64
search_expansion = 32
key_map = "one-based-catalog-order-page-id-passage-id"
fusion_candidate_depth = {fusion_candidate_depth}
fusion_algorithm = "rrf"
rrf_k = 60
page_aggregation = "max"
snippet_window = 240
"#
        ),
    )
    .expect("write index profile");
    path
}

fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set read timeout");
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
    String::from_utf8(bytes).expect("request is utf8")
}

fn response(stream: &mut TcpStream, content_type: &str, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("write response headers");
    stream.write_all(body).expect("write response body");
}

fn status_response(stream: &mut TcpStream, status: &str, headers: &str, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("write response headers");
    stream.write_all(body).expect("write response body");
}

fn fixture_server() -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fixture server");
    let origin = format!("http://{}", listener.local_addr().expect("server address"));
    let handle = thread::spawn(move || {
        let page = br#"<!doctype html><html><head><title>Fixture page</title></head><body><nav>Skip me</nav><main><h1>Fixture page</h1><p>Scout captures useful server rendered documentation content from an explicitly allowlisted fixture source. This paragraph is long enough to satisfy the page admission quality gate and remains stable across repeated crawls. It also gives the bounded crawler enough representative prose to verify deterministic capture, hashing, and snapshot publication without relying on live documentation.</p></main></body></html>"#;
        let mut requests = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().expect("accept fixture request");
            let request = read_request(&mut stream);
            if request.starts_with("GET /robots.txt ") {
                response(&mut stream, "text/plain", b"User-agent: *\nAllow: /\n");
            } else if request.starts_with("GET / ") {
                response(&mut stream, "text/html; charset=utf-8", page);
            } else {
                response(&mut stream, "text/plain", b"unexpected path");
            }
            requests.push(request);
        }
        requests
    });
    (origin, handle)
}

fn retrying_fixture_server() -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind retry fixture server");
    let origin = format!("http://{}", listener.local_addr().expect("server address"));
    let handle = thread::spawn(move || {
        let page = br#"<!doctype html><html><head><title>Retry page</title></head><body><main><h1>Retry page</h1><p>Scout retries bounded transient fixture failures before admitting stable server rendered documentation content. This body exceeds the admission threshold and remains deterministic. Extra fixture prose makes the admission contract explicit while keeping retry behavior fast, local, and reproducible for every test run.</p></main></body></html>"#;
        let mut requests = Vec::new();
        for attempt in 0..3 {
            let (mut stream, _) = listener.accept().expect("accept retry request");
            let request = read_request(&mut stream);
            if request.starts_with("GET /robots.txt ") {
                response(&mut stream, "text/plain", b"User-agent: *\nAllow: /\n");
            } else if attempt == 1 {
                status_response(
                    &mut stream,
                    "503 Service Unavailable",
                    "Retry-After: 0\r\n",
                    b"retry",
                );
            } else if request.starts_with("GET / ") {
                response(&mut stream, "text/html; charset=utf-8", page);
            } else {
                status_response(
                    &mut stream,
                    "404 Not Found",
                    "Content-Type: text/plain\r\n",
                    b"missing",
                );
            }
            requests.push(request);
        }
        requests
    });
    (origin, handle)
}

fn multi_source_fixture_servers() -> (
    String,
    String,
    JoinHandle<Vec<String>>,
    JoinHandle<Vec<String>>,
) {
    let first_listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind first source");
    let second_listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind second source");
    let first_origin = format!(
        "http://{}",
        first_listener.local_addr().expect("first address")
    );
    let second_origin = format!(
        "http://{}",
        second_listener.local_addr().expect("second address")
    );
    let first_target = second_origin.clone();
    let first_handle = thread::spawn(move || {
        let page = format!(
            "<!doctype html><html><body><main><h1>First Source</h1><p>First Source provides enough stable server rendered documentation content for the bounded multi-Source fixture. It links only to the explicitly allowlisted second Source and proves deterministic ownership traversal. Additional prose keeps this fixture representative of useful documentation while testing queue limits, quotas, and reproducible artifact creation.</p><a href=\"{first_target}/\">Second Source</a></main></body></html>"
        );
        let mut requests = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = first_listener.accept().expect("accept first request");
            let request = read_request(&mut stream);
            if request.starts_with("GET /robots.txt ") {
                response(&mut stream, "text/plain", b"User-agent: *\nAllow: /\n");
            } else {
                response(&mut stream, "text/html; charset=utf-8", page.as_bytes());
            }
            requests.push(request);
        }
        requests
    });
    let second_handle = thread::spawn(move || {
        let page = br#"<!doctype html><html><body><main><h1>Second Source</h1><p>Second Source supplies independent documentation content with enough useful prose to satisfy page admission. Its separate allowlist proves one Source can reach another only when ownership is explicit. Additional stable text makes this a complete fixture Page and exercises deterministic multi-Source completion without live network dependencies.</p></main></body></html>"#;
        let mut requests = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = second_listener.accept().expect("accept second request");
            let request = read_request(&mut stream);
            if request.starts_with("GET /robots.txt ") {
                response(&mut stream, "text/plain", b"User-agent: *\nAllow: /\n");
            } else {
                response(&mut stream, "text/html; charset=utf-8", page);
            }
            requests.push(request);
        }
        requests
    });
    (first_origin, second_origin, first_handle, second_handle)
}

type ConcurrentFixtureServers = (
    String,
    String,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<Instant>>>,
    Arc<Mutex<Vec<Instant>>>,
    JoinHandle<Vec<String>>,
    JoinHandle<Vec<String>>,
);

fn concurrent_fixture_servers() -> ConcurrentFixtureServers {
    let first_listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind first concurrent source");
    let second_listener =
        TcpListener::bind(("127.0.0.1", 0)).expect("bind second concurrent source");
    let first_origin = format!(
        "http://{}",
        first_listener
            .local_addr()
            .expect("first concurrent address")
    );
    let second_origin = format!(
        "http://{}",
        second_listener
            .local_addr()
            .expect("second concurrent address")
    );
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let first_starts = Arc::new(Mutex::new(Vec::new()));
    let second_starts = Arc::new(Mutex::new(Vec::new()));
    let spawn_server = |listener: TcpListener,
                        label: &'static str,
                        active: Arc<AtomicUsize>,
                        max_active: Arc<AtomicUsize>,
                        starts: Arc<Mutex<Vec<Instant>>>| {
        thread::spawn(move || {
            let page = format!(
                "<!doctype html><html><body><main><h1>{label}</h1><p>{label} provides enough stable server rendered documentation content for a concurrent crawl fixture. This paragraph exceeds the admission threshold and remains deterministic while delayed responses prove that independent Origins overlap without exceeding their per-Origin budgets.</p></main></body></html>"
            );
            let mut requests = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept concurrent request");
                let request = read_request(&mut stream);
                starts
                    .lock()
                    .expect("concurrent start lock")
                    .push(std::time::Instant::now());
                if request.starts_with("GET /robots.txt ") {
                    response(&mut stream, "text/plain", b"User-agent: *\nAllow: /\n");
                } else {
                    assert!(request.starts_with("GET / "), "unexpected concurrent path");
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(100));
                    response(&mut stream, "text/html; charset=utf-8", page.as_bytes());
                    active.fetch_sub(1, Ordering::SeqCst);
                }
                requests.push(request);
            }
            requests
        })
    };
    let first_handle = spawn_server(
        first_listener,
        "First concurrent source",
        active.clone(),
        max_active.clone(),
        first_starts.clone(),
    );
    let max_active_result = max_active.clone();
    let second_handle = spawn_server(
        second_listener,
        "Second concurrent source",
        active,
        max_active,
        second_starts.clone(),
    );
    (
        first_origin,
        second_origin,
        max_active_result,
        first_starts,
        second_starts,
        first_handle,
        second_handle,
    )
}

#[test]
fn bounded_crawl_overlaps_origins_and_honors_request_limits() {
    let data_dir = temp_dir("concurrent");
    let (
        first_origin,
        second_origin,
        max_active,
        first_starts,
        second_starts,
        first_server,
        second_server,
    ) = concurrent_fixture_servers();
    let config = data_dir.join("sources.toml");
    fs::write(
        &config,
        format!(
            r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
user_agent = "ConcurrentFixture/1.0"
page_target = 2
global_concurrency = 2
origin_concurrency = 1
min_start_spacing_ms = 40
max_attempts = 1
max_total_attempts = 8
max_duration_seconds = 30

[[sources]]
source_id = "first"
seeds = ["{first_origin}/"]
allowed_origins = ["{first_origin}"]
path_prefixes = ["/"]

[[sources]]
source_id = "second"
seeds = ["{second_origin}/"]
allowed_origins = ["{second_origin}"]
path_prefixes = ["/"]
"#
        ),
    )
    .expect("write concurrent crawl config");

    let output = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run concurrent crawl");
    first_server.join().expect("join first concurrent server");
    second_server.join().expect("join second concurrent server");

    assert!(output.status.success(), "stderr: {:?}", output.stderr);
    assert!(
        max_active.load(Ordering::SeqCst) >= 2,
        "independent Origins must overlap"
    );
    for starts in [first_starts, second_starts] {
        let starts = starts.lock().expect("read request starts");
        assert_eq!(starts.len(), 2);
        let spacing = starts[1].duration_since(starts[0]);
        assert!(
            spacing >= Duration::from_millis(30),
            "same-Origin requests started too close together: {spacing:?}"
        );
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let concurrency_line = stderr
        .lines()
        .find(|line| line.contains("\"event\":\"crawl.concurrency\""))
        .expect("crawl concurrency event");
    let concurrency: Value =
        serde_json::from_str(concurrency_line).expect("parse concurrency event");
    assert_eq!(concurrency["data"]["max_active_global"], 2);
    assert!(
        concurrency["data"]["max_active_by_origin"]
            .as_object()
            .expect("per-Origin concurrency")
            .values()
            .all(|value| value.as_u64().unwrap_or(0) <= 1)
    );

    fs::remove_dir_all(data_dir).expect("cleanup");
}

fn find_single_child(data_dir: &Path, name: &str) -> PathBuf {
    let entries = fs::read_dir(data_dir.join(name))
        .expect("read artifact directory")
        .map(|entry| entry.expect("read artifact entry").path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 1, "expected one {name} artifact");
    entries.into_iter().next().expect("artifact directory")
}

#[test]
fn valid_fixture_crawl_persists_reproducible_snapshot_and_events() {
    let data_dir = temp_dir("valid");
    let (origin, server) = fixture_server();
    let config = write_config(&data_dir, &origin, "");

    let output = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run scout");
    let requests = server.join().expect("join fixture server");

    assert!(output.status.success(), "stderr: {:?}", output.stderr);
    let crawl_dir = find_single_child(&data_dir, "crawls");
    let manifest: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json")).expect("read crawl manifest"),
    )
    .expect("parse crawl manifest");
    let run_id = manifest["run_id"].as_str().expect("run id");
    let body_hash = manifest["pages"][0]["body_sha256"]
        .as_str()
        .expect("body hash");
    let body_path = crawl_dir.join("bodies").join(body_hash);
    let body = fs::read(&body_path).expect("captured body");
    let actual_hash = format!("{:x}", Sha256::digest(&body));

    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["user_agent"], "FixtureBot/9.1");
    assert_eq!(manifest["pages"][0]["source_id"], "fixture");
    assert_eq!(body_hash, actual_hash);
    assert_eq!(
        manifest["pages"][0]["body_file"],
        format!("bodies/{body_hash}")
    );
    assert_eq!(manifest["sources"][0]["allowed_origins"][0], origin);
    assert!(requests.iter().all(|request| {
        request
            .to_ascii_lowercase()
            .contains("user-agent: fixturebot/9.1")
    }));
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("GET /robots.txt "))
    );
    assert!(requests.iter().any(|request| request.starts_with("GET / ")));

    let snapshot_id = manifest["snapshot_id"].as_str().expect("snapshot id");
    let snapshot_dir = data_dir.join("corpora").join(snapshot_id);
    let snapshot: Value = serde_json::from_slice(
        &fs::read(snapshot_dir.join("snapshot-manifest.json")).expect("read snapshot manifest"),
    )
    .expect("parse snapshot manifest");
    assert_eq!(snapshot["status"], "complete");
    assert_eq!(snapshot["page_count"], 1);
    assert_eq!(
        snapshot["crawl_manifest_sha256"],
        manifest["manifest_sha256"]
    );
    assert_eq!(
        fs::read_to_string(snapshot_dir.join("pages.jsonl"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert!(
        fs::read_to_string(snapshot_dir.join("aliases.jsonl"))
            .expect("aliases file")
            .is_empty()
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains(snapshot_id));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("\"event\":\"crawl.started\""));
    assert!(stderr.contains("\"event\":\"fetch.completed\""));
    assert!(stderr.contains("\"event\":\"crawl.completed\""));
    assert!(stderr.contains(&format!("\"run_id\":\"{run_id}\"")));

    fs::remove_dir_all(data_dir).expect("cleanup");
}

#[test]
fn index_build_persists_reopened_semantic_fixture_artifact() {
    let data_dir = temp_dir("semantic");
    let (origin, server) = fixture_server();
    let config = write_config(&data_dir, &origin, "max_passage_tokens = 8");

    let crawl = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run crawl");
    server.join().expect("join fixture server");
    assert!(crawl.status.success(), "crawl stderr: {:?}", crawl.stderr);

    let crawl_dir = find_single_child(&data_dir, "crawls");
    let crawl_manifest: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json")).expect("read crawl manifest"),
    )
    .expect("parse crawl manifest");
    let snapshot_id = crawl_manifest["snapshot_id"].as_str().expect("snapshot id");
    write_index_profile(&data_dir, snapshot_id, 240, 8, 1);

    let build = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "index",
            "build",
            "--corpus",
            snapshot_id,
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run index build");

    assert!(build.status.success(), "build stderr: {:?}", build.stderr);
    let staging = find_single_child(&data_dir, "generations/.staging");
    let manifest: Value = serde_json::from_slice(
        &fs::read(staging.join("generation-manifest.json")).expect("read generation manifest"),
    )
    .expect("parse generation manifest");
    let semantic = &manifest["semantic"];
    assert_eq!(manifest["status"], "validated");
    assert_eq!(semantic["backend"], "deterministic-usearch");
    assert_eq!(semantic["metric"], "cosine");
    assert_eq!(semantic["scalar_kind"], "f32");
    assert_eq!(semantic["dimensions"], 384);
    assert_eq!(semantic["catalog_count"], manifest["passage_count"]);
    assert!(
        semantic["catalog_count"]
            .as_u64()
            .is_some_and(|count| count > 1)
    );
    assert_eq!(semantic["vector_count"], manifest["passage_count"]);
    assert_eq!(semantic["reopened"], true);
    assert!(semantic["artifact_bytes"].as_u64().unwrap_or(0) > 0);
    assert!(
        !semantic["returned_identities"]
            .as_array()
            .expect("semantic identities")
            .is_empty()
    );
    assert!(staging.join("semantic.usearch").is_file());
    assert!(staging.join("semantic-vectors.f32").is_file());
    assert!(staging.join("checksums.sha256").is_file());
    let checksums = fs::read_to_string(staging.join("checksums.sha256")).expect("read checksums");
    assert!(checksums.contains("semantic.usearch"));
    let stderr = String::from_utf8_lossy(&build.stderr);
    assert!(stderr.contains("\"event\":\"semantic.completed\""));
    assert!(stderr.contains("\"artifact_bytes\":"));

    // A duplicate Page identity is rejected before a second build can claim
    // success; the valid staged Generation remains the only artifact made by
    // the successful build and no active pointer is changed.
    let snapshot_dir = data_dir.join("corpora").join(snapshot_id);
    let pages_path = snapshot_dir.join("pages.jsonl");
    let original_pages = fs::read(&pages_path).expect("read pages for corruption fixture");
    let first_page = original_pages
        .split(|byte| *byte == b'\n')
        .find(|line| !line.is_empty())
        .expect("one Page row");
    let mut duplicate_pages = original_pages.clone();
    duplicate_pages.extend_from_slice(first_page);
    duplicate_pages.push(b'\n');
    fs::write(&pages_path, &duplicate_pages).expect("write duplicate Page fixture");
    let snapshot_manifest_path = snapshot_dir.join("snapshot-manifest.json");
    let mut snapshot: Value =
        serde_json::from_slice(&fs::read(&snapshot_manifest_path).expect("read snapshot manifest"))
            .expect("parse snapshot manifest");
    snapshot["page_count"] = Value::from(2);
    snapshot["pages_jsonl_sha256"] =
        Value::String(format!("{:x}", Sha256::digest(&duplicate_pages)));
    fs::write(
        &snapshot_manifest_path,
        serde_json::to_vec_pretty(&snapshot).expect("serialize corrupt snapshot"),
    )
    .expect("write corrupt snapshot manifest");
    let invalid_build = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "index",
            "build",
            "--corpus",
            snapshot_id,
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run invalid index build");
    assert!(!invalid_build.status.success());
    assert!(String::from_utf8_lossy(&invalid_build.stderr).contains("Page identity"));
    assert!(!data_dir.join("CURRENT").exists());

    fs::remove_dir_all(data_dir).expect("cleanup");
}

#[test]
fn index_activation_recovery_and_pinned_retention_fixture() {
    let data_dir = temp_dir("activation");
    let (origin, server) = fixture_server();
    let config = write_config(&data_dir, &origin, "");
    let crawl = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run crawl");
    server.join().expect("join fixture server");
    assert!(crawl.status.success(), "crawl stderr: {:?}", crawl.stderr);

    let crawl_dir = find_single_child(&data_dir, "crawls");
    let crawl_manifest: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json")).expect("read crawl manifest"),
    )
    .expect("parse crawl manifest");
    let snapshot_id = crawl_manifest["snapshot_id"]
        .as_str()
        .expect("snapshot id")
        .to_string();
    let run_index = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_scout"))
            .args(args)
            .args(["--data-dir", data_dir.to_str().expect("data path")])
            .output()
            .expect("run index command")
    };
    let read_pointer = |name: &str| -> Value {
        serde_json::from_slice(&fs::read(data_dir.join(name)).expect("read Generation pointer"))
            .expect("parse Generation pointer")
    };
    let build_generation = |snippet_limit: usize| -> (String, PathBuf) {
        write_index_profile(&data_dir, &snapshot_id, snippet_limit, 384, 5);
        let output = run_index(&["index", "build", "--corpus", &snapshot_id]);
        assert!(output.status.success(), "build stderr: {:?}", output.stderr);
        let staging = find_single_child(&data_dir, "generations/.staging");
        let manifest: Value = serde_json::from_slice(
            &fs::read(staging.join("generation-manifest.json"))
                .expect("read staged Generation manifest"),
        )
        .expect("parse staged Generation manifest");
        let generation_id = manifest["generation_id"]
            .as_str()
            .expect("generation id")
            .to_string();
        (generation_id, staging)
    };
    let activate = |generation_id: &str| {
        let output = run_index(&["index", "activate", "--generation", generation_id]);
        assert!(
            output.status.success(),
            "activate stderr: {:?}",
            output.stderr
        );
    };

    let (first_id, first_staging) = build_generation(240);
    activate(&first_id);
    let first_dir = data_dir.join("generations").join(&first_id);
    assert!(!first_staging.exists());
    let first_manifest: Value = serde_json::from_slice(
        &fs::read(first_dir.join("generation-manifest.json")).expect("read sealed manifest"),
    )
    .expect("parse sealed manifest");
    assert_eq!(first_manifest["status"], "sealed");
    assert_eq!(read_pointer("CURRENT")["generation_id"], first_id);
    assert!(!data_dir.join("PREVIOUS").exists());

    let (second_id, _) = build_generation(200);
    assert_ne!(first_id, second_id);
    activate(&second_id);
    assert_eq!(read_pointer("CURRENT")["generation_id"], second_id);
    assert_eq!(read_pointer("PREVIOUS")["generation_id"], first_id);

    activate(&first_id);
    assert_eq!(read_pointer("CURRENT")["generation_id"], first_id);
    assert_eq!(read_pointer("PREVIOUS")["generation_id"], second_id);

    let (third_id, _) = build_generation(180);
    activate(&third_id);
    assert_eq!(read_pointer("CURRENT")["generation_id"], third_id);
    assert_eq!(read_pointer("PREVIOUS")["generation_id"], first_id);

    let second_dir = data_dir.join("generations").join(&second_id);
    fs::write(second_dir.join(".pinned"), b"fixture reader pin").expect("pin Generation");
    let retained = run_index(&["index", "prune", "--retain", &second_id]);
    assert!(
        retained.status.success(),
        "prune stderr: {:?}",
        retained.stderr
    );
    assert!(
        second_dir.exists(),
        "explicitly retained Generation removed"
    );

    let pinned = run_index(&["index", "prune"]);
    assert!(pinned.status.success(), "prune stderr: {:?}", pinned.stderr);
    assert!(second_dir.exists(), "pinned Generation removed");
    fs::remove_file(second_dir.join(".pinned")).expect("unpin Generation");
    let removed = run_index(&["index", "prune"]);
    assert!(
        removed.status.success(),
        "prune stderr: {:?}",
        removed.stderr
    );
    assert!(!second_dir.exists(), "unprotected old Generation retained");

    let third_dir = data_dir.join("generations").join(&third_id);
    fs::write(third_dir.join("catalog.jsonl"), b"corrupt\n").expect("corrupt active catalog");
    let recovered = run_index(&["index", "recover"]);
    assert!(
        recovered.status.success(),
        "recover stderr: {:?}",
        recovered.stderr
    );
    assert_eq!(read_pointer("CURRENT")["generation_id"], first_id);
    assert!(
        String::from_utf8_lossy(&recovered.stderr)
            .contains("\"event\":\"index.recover.completed\"")
    );

    fs::remove_dir_all(data_dir).expect("cleanup");
}

#[test]
fn invalid_source_configuration_returns_exit_two_and_structured_failure() {
    let data_dir = temp_dir("invalid");
    let config = data_dir.join("sources.toml");
    fs::write(
        &config,
        r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
page_target = 0

[[sources]]
source_id = "fixture"
seeds = ["http://127.0.0.1:1/"]
allowed_origins = ["http://127.0.0.1:1"]
path_prefixes = ["/"]
"#,
    )
    .expect("write invalid config");

    let output = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            data_dir.to_str().expect("data path"),
            "--json",
        ])
        .output()
        .expect("run scout");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("\"event\":\"crawl.failed\""));
    assert!(stderr.contains("\"code\":\"invalid_configuration\""));
    assert!(!data_dir.join("corpora").exists());
    fs::remove_dir_all(data_dir).expect("cleanup");
}

#[test]
fn fixture_fetch_failure_is_terminal_item_failure_and_publishes_complete_manifest() {
    let data_dir = temp_dir("failure");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind failing fixture server");
    let origin = format!("http://{}", listener.local_addr().expect("server address"));
    let server = thread::spawn(move || {
        let (mut robots, _) = listener.accept().expect("accept robots request");
        let request = read_request(&mut robots);
        assert!(request.starts_with("GET /robots.txt "));
        response(&mut robots, "text/plain", b"User-agent: *\nAllow: /\n");
        let (stream, _) = listener.accept().expect("accept failing request");
        stream
            .shutdown(Shutdown::Both)
            .expect("close failing request");
    });
    let config = write_config(&data_dir, &origin, "");

    let output = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run scout");
    server.join().expect("join failing fixture server");

    assert_eq!(output.status.code(), Some(0));
    let crawl_dir = find_single_child(&data_dir, "crawls");
    let complete: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json")).expect("read complete manifest"),
    )
    .expect("parse complete manifest");
    assert_eq!(complete["status"], "complete");
    assert_eq!(complete["summary"]["failed_count"], 1);
    assert_eq!(complete["summary"]["stop_reason"], "frontier_exhausted");
    assert!(!crawl_dir.join("incomplete-manifest.json").exists());
    assert!(data_dir.join("corpora").exists());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("\"event\":\"fetch.failed\""));
    assert!(stderr.contains("\"code\":\"fetch_failed\""));

    fs::remove_dir_all(data_dir).expect("cleanup");
}

#[test]
fn transient_fixture_failure_retries_within_attempt_cap() {
    let data_dir = temp_dir("retry");
    let (origin, server) = retrying_fixture_server();
    let config = data_dir.join("sources.toml");
    fs::write(
        &config,
        format!(
            r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
user_agent = "FixtureBot/9.1"
page_target = 1
max_attempts = 2
retry_backoff_ms = 0

[[sources]]
source_id = "fixture"
seeds = ["{origin}/"]
allowed_origins = ["{origin}"]
path_prefixes = ["/"]
"#
        ),
    )
    .expect("write retry config");

    let output = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run scout");
    let requests = server.join().expect("join retry fixture server");

    assert!(output.status.success(), "stderr: {:?}", output.stderr);
    assert_eq!(requests.len(), 3);
    assert!(requests[1].starts_with("GET / "));
    assert!(requests[2].starts_with("GET / "));
    assert!(String::from_utf8_lossy(&output.stderr).contains("\"event\":\"fetch.retry\""));
    fs::remove_dir_all(data_dir).expect("cleanup");
}

#[test]
fn explicitly_owned_sources_share_bounded_frontier_and_quotas() {
    let data_dir = temp_dir("multi-source");
    let (first_origin, second_origin, first_server, second_server) = multi_source_fixture_servers();
    let config = data_dir.join("sources.toml");
    fs::write(
        &config,
        format!(
            r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
user_agent = "FixtureBot/9.1"
page_target = 2
max_frontier = 4
max_attempts = 2
retry_backoff_ms = 0

[[sources]]
source_id = "first"
seeds = ["{first_origin}/"]
allowed_origins = ["{first_origin}"]
path_prefixes = ["/"]
minimum_page_quota = 1

[[sources]]
source_id = "second"
seeds = ["{second_origin}/"]
allowed_origins = ["{second_origin}"]
path_prefixes = ["/"]
minimum_page_quota = 1
"#
        ),
    )
    .expect("write multi-source config");

    let output = Command::new(env!("CARGO_BIN_EXE_scout"))
        .args([
            "crawl",
            "--config",
            config.to_str().expect("config path"),
            "--data-dir",
            data_dir.to_str().expect("data path"),
        ])
        .output()
        .expect("run scout");
    let first_requests = first_server.join().expect("join first server");
    let second_requests = second_server.join().expect("join second server");

    assert!(output.status.success(), "stderr: {:?}", output.stderr);
    assert_eq!(first_requests.len(), 2);
    assert_eq!(second_requests.len(), 2);
    let crawl_dir = find_single_child(&data_dir, "crawls");
    let manifest: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json")).expect("read crawl manifest"),
    )
    .expect("parse crawl manifest");
    assert_eq!(manifest["summary"]["page_count"], 2);
    assert_eq!(manifest["sources"].as_array().expect("sources").len(), 2);
    assert_eq!(manifest["policy"]["max_frontier"], 4);
    fs::remove_dir_all(data_dir).expect("cleanup");
}
