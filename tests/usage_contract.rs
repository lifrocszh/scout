use std::fs;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
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

fn run_scout<I, S>(data_dir: &Path, args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    Command::new(env!("CARGO_BIN_EXE_scout"))
        .args(args)
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run scout")
}

fn one_directory(path: &Path) -> PathBuf {
    let directories = fs::read_dir(path)
        .expect("read artifact directory")
        .map(|entry| entry.expect("read artifact entry").path())
        .filter(|entry| entry.is_dir())
        .collect::<Vec<_>>();
    assert_eq!(
        directories.len(),
        1,
        "expected one directory under {}",
        path.display()
    );
    directories.into_iter().next().expect("artifact directory")
}

fn write_source_config(data_dir: &Path, origin: &str) -> PathBuf {
    let path = data_dir.join("sources.toml");
    fs::write(
        &path,
        format!(
            r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout"
user_agent = "ScoutUsageContract/1.0"
page_target = 3
global_concurrency = 1
origin_concurrency = 1
max_attempts = 1
retry_backoff_ms = 0
max_total_attempts = 20
max_duration_seconds = 30

[[sources]]
source_id = "alpha"
seeds = ["{origin}/alpha/"]
allowed_origins = ["{origin}"]
path_prefixes = ["/alpha"]
minimum_page_quota = 1

[[sources]]
source_id = "beta"
seeds = ["{origin}/beta/"]
allowed_origins = ["{origin}"]
path_prefixes = ["/beta"]
minimum_page_quota = 1
"#
        ),
    )
    .expect("write source config");
    path
}

fn write_index_profile(data_dir: &Path, snapshot_id: &str) {
    let config_dir = data_dir.join("config");
    fs::create_dir_all(&config_dir).expect("create index config directory");
    fs::write(
        config_dir.join("index.toml"),
        format!(
            r#"schema_version = "scout.index.v1"
snapshot_id = "{snapshot_id}"
extraction_schema = "scout.extraction.v1"
passage_schema = "scout.passage.v1"
max_passage_tokens = 384
overlap_tokens = 0
no_overlap = true
analyzer_id = "scout.unicode-token-v1"
title_boost = 2.0
heading_boost = 1.5
body_boost = 1.0
candidate_depth = 5
writer_memory_bytes = 16000000
worker_count = 1
model_name = "fixture-deterministic"
model_source = "none"
model_revision = "fixture-revision"
model_license = "Apache-2.0"
onnx_sha256 = "fixture-onnx-sha256"
tokenizer_sha256 = "fixture-tokenizer-sha256"
model_config_sha256 = "fixture-config-sha256"
embedding_backend = "deterministic"
snippet_limit = 240
model_max_tokens = 512
pooling = "cls"
normalization = "l2"
query_instruction = ""
document_representation = "passage"
dimensions = 16
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
fusion_candidate_depth = 5
fusion_algorithm = "rrf"
rrf_k = 60
page_aggregation = "max"
snippet_window = 240
"#
        ),
    )
    .expect("write index profile");
}

fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set fixture read timeout");
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let count = stream.read(&mut chunk).expect("read fixture request");
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).expect("fixture request is UTF-8")
}

fn fixture_response(stream: &mut TcpStream, status: &str, content_type: &str, body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("write fixture response headers");
    stream.write_all(body).expect("write fixture response body");
}

fn fixture_server() -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind fixture server");
    listener
        .set_nonblocking(true)
        .expect("set fixture listener nonblocking");
    let origin = format!("http://{}", listener.local_addr().expect("fixture address"));
    let alpha_page = format!(
        r#"<!doctype html><html lang="en"><head><title>Alpha Retrieval</title><meta name="description" content="Alpha fixture documentation"></head><body><nav>ignored navigation</nav><main><h1>Alpha Retrieval</h1><p>Alpha source documents retrieval behavior with stable server rendered documentation content. This paragraph gives keyword search, semantic search, and hybrid search useful evidence while remaining deterministic for every local usage test.</p><p>Alpha documentation explains reproducible indexing, source filtering, page evidence, and passage snippets without any live network dependency.</p><a href="{origin}/alpha/alias">duplicate alias</a><a href="{origin}/alpha/missing">terminal missing page</a><a href="{origin}/outside">outside allowlist</a></main></body></html>"#
    );
    let beta_page = r#"<!doctype html><html lang="en"><head><title>Beta Retrieval</title><meta name="description" content="Beta fixture documentation"></head><body><main><h1>Beta Retrieval</h1><p>Beta source documents retrieval behavior with stable server rendered documentation content. This paragraph gives keyword search, semantic search, and hybrid search useful evidence while remaining deterministic for every local usage test.</p><p>Beta documentation explains reproducible indexing, source filtering, page evidence, and passage snippets without any live network dependency.</p></main></body></html>"#;
    let robots = format!("User-agent: *\nAllow: /\nSitemap: {origin}/sitemap.xml\n");
    let sitemap = format!(
        r#"<?xml version="1.0"?><urlset><url><loc>{origin}/alpha/</loc></url><url><loc>{origin}/alpha/alias</loc></url><url><loc>{origin}/alpha/missing</loc></url><url><loc>{origin}/beta/</loc></url><url><loc>{origin}/outside</loc></url></urlset>"#
    );
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        while requests.len() < 6 && Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let request = read_request(&mut stream);
                    if request.starts_with("GET /robots.txt ") {
                        fixture_response(&mut stream, "200 OK", "text/plain", robots.as_bytes());
                    } else if request.starts_with("GET /sitemap.xml ") {
                        fixture_response(
                            &mut stream,
                            "200 OK",
                            "application/xml",
                            sitemap.as_bytes(),
                        );
                    } else if request.starts_with("GET /alpha/alias ")
                        || request.starts_with("GET /alpha/ HTTP")
                    {
                        fixture_response(
                            &mut stream,
                            "200 OK",
                            "text/html; charset=utf-8",
                            alpha_page.as_bytes(),
                        );
                    } else if request.starts_with("GET /beta/ HTTP") {
                        fixture_response(
                            &mut stream,
                            "200 OK",
                            "text/html; charset=utf-8",
                            beta_page.as_bytes(),
                        );
                    } else if request.starts_with("GET /alpha/missing ") {
                        fixture_response(&mut stream, "404 Not Found", "text/plain", b"missing");
                    } else {
                        fixture_response(
                            &mut stream,
                            "500 Internal Server Error",
                            "text/plain",
                            b"unexpected fixture request",
                        );
                    }
                    requests.push(request);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept fixture request: {error}"),
            }
        }
        assert_eq!(requests.len(), 6, "fixture request count: {requests:?}");
        requests
    });
    (origin, handle)
}

fn http_json(address: &str, method: &str, path: &str, body: &[u8]) -> Result<(u16, Value), String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| error.to_string())?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .map_err(|error| error.to_string())?;
    stream.write_all(body).map_err(|error| error.to_string())?;
    stream
        .shutdown(Shutdown::Write)
        .map_err(|error| error.to_string())?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| error.to_string())?;
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .ok_or_else(|| "HTTP response missing header terminator".to_string())?;
    let header = String::from_utf8(response[..header_end].to_vec())
        .map_err(|error| format!("HTTP response headers are not UTF-8: {error}"))?;
    let status = header
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| "HTTP response missing status".to_string())?
        .parse::<u16>()
        .map_err(|error| format!("invalid HTTP response status: {error}"))?;
    let content_length = header
        .lines()
        .find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
        })
        .ok_or_else(|| "HTTP response missing content length".to_string())?;
    let response_body = response
        .get(header_end..header_end + content_length)
        .ok_or_else(|| "HTTP response body truncated".to_string())?;
    let value = serde_json::from_slice(response_body)
        .map_err(|error| format!("HTTP response body is not JSON: {error}"))?;
    Ok((status, value))
}

struct ServeProcess {
    child: Child,
}

impl ServeProcess {
    fn start(data_dir: &Path, address: &str) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_scout"))
            .args(["serve", "--bind", address])
            .arg("--data-dir")
            .arg(data_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start scout serve");
        Self { child }
    }
}

impl Drop for ServeProcess {
    fn drop(&mut self) {
        if self.child.try_wait().expect("poll scout serve").is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn free_address() -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve serve port");
    format!(
        "127.0.0.1:{}",
        listener.local_addr().expect("serve address").port()
    )
}

fn wait_for_health(address: &str) -> (u16, Value) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(response) = http_json(address, "GET", "/v1/healthz", b"")
            && response.0 == 200
        {
            return response;
        }
        assert!(
            Instant::now() < deadline,
            "scout serve did not become ready"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn assert_error(response: &Value, code: &str, message: &str) {
    assert_eq!(response["error"]["code"], code);
    assert_eq!(response["error"]["message"], message);
    assert!(
        response["error"]["request_id"]
            .as_str()
            .is_some_and(|request_id| !request_id.is_empty())
    );
}

#[test]
fn usage_contract_covers_crawl_snapshot_index_activation_verification_and_search() {
    let data_dir = TempDir::new("usage-contract");
    let (origin, fixture) = fixture_server();
    let source_config = write_source_config(data_dir.path(), &origin);

    let crawl = run_scout(
        data_dir.path(),
        [
            "crawl".to_owned(),
            "--config".to_owned(),
            source_config.display().to_string(),
        ],
    );
    assert!(
        crawl.status.success(),
        "crawl failed: {}",
        String::from_utf8_lossy(&crawl.stderr)
    );
    let requests = fixture.join().expect("join fixture server");
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("GET /robots.txt "))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("GET /sitemap.xml "))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("GET /alpha/missing "))
    );
    assert!(
        requests
            .iter()
            .all(|request| !request.starts_with("GET /outside ")),
        "outside-allowlist link must not be fetched"
    );

    let crawl_dir = one_directory(&data_dir.path().join("crawls"));
    let crawl_manifest: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json")).expect("read crawl manifest"),
    )
    .expect("parse crawl manifest");
    assert_eq!(crawl_manifest["status"], "complete");
    assert_eq!(crawl_manifest["summary"]["captured_page_count"], 3);
    assert_eq!(crawl_manifest["summary"]["page_count"], 2);
    assert_eq!(crawl_manifest["summary"]["alias_count"], 1);
    assert_eq!(crawl_manifest["summary"]["passage_count"], 2);
    assert_eq!(crawl_manifest["summary"]["failed_count"], 1);
    assert_eq!(crawl_manifest["summary"]["rejected_count"], 0);
    assert_eq!(
        crawl_manifest["summary"]["stop_reason"],
        "frontier_exhausted"
    );
    assert_eq!(crawl_manifest["sources"].as_array().map(Vec::len), Some(2));
    for page in crawl_manifest["pages"].as_array().expect("crawl pages") {
        let body_file = page["body_file"].as_str().expect("body file");
        let body = fs::read(crawl_dir.join(body_file)).expect("read captured body");
        assert_eq!(
            page["body_sha256"],
            format!("{:x}", Sha256::digest(&body)),
            "body hash must match captured body"
        );
    }

    let snapshot_id = crawl_manifest["snapshot_id"]
        .as_str()
        .expect("snapshot ID")
        .to_owned();
    let snapshot_dir = data_dir.path().join("corpora").join(&snapshot_id);
    let snapshot: Value = serde_json::from_slice(
        &fs::read(snapshot_dir.join("snapshot-manifest.json")).expect("read snapshot manifest"),
    )
    .expect("parse snapshot manifest");
    assert_eq!(snapshot["status"], "complete");
    assert_eq!(snapshot["snapshot_id"], snapshot_id);
    assert_eq!(snapshot["page_count"], 2);
    assert_eq!(snapshot["alias_count"], 1);
    assert_eq!(snapshot["passage_count"], 2);
    assert_eq!(
        snapshot["crawl_manifest_sha256"],
        crawl_manifest["manifest_sha256"]
    );
    assert_eq!(
        fs::read_to_string(snapshot_dir.join("pages.jsonl"))
            .expect("read pages")
            .lines()
            .count(),
        2
    );
    let alias: Value = serde_json::from_str(
        fs::read_to_string(snapshot_dir.join("aliases.jsonl"))
            .expect("read aliases")
            .lines()
            .next()
            .expect("one alias"),
    )
    .expect("parse alias");
    assert_eq!(alias["alias_kind"], "exact-alias");
    assert_ne!(alias["alias_page_id"], alias["representative_page_id"]);
    assert_eq!(
        fs::read_to_string(snapshot_dir.join("passages.jsonl"))
            .expect("read passages")
            .lines()
            .count(),
        2
    );

    write_index_profile(data_dir.path(), &snapshot_id);
    let build = run_scout(
        data_dir.path(),
        [
            "index".to_owned(),
            "build".to_owned(),
            "--corpus".to_owned(),
            snapshot_id.clone(),
        ],
    );
    assert!(
        build.status.success(),
        "index build failed: {}",
        String::from_utf8_lossy(&build.stderr)
    );
    assert!(
        !data_dir.path().join("models").exists(),
        "deterministic embedding must not create a model cache"
    );
    let staging = one_directory(&data_dir.path().join("generations/.staging"));
    let staged_manifest: Value = serde_json::from_slice(
        &fs::read(staging.join("generation-manifest.json")).expect("read staged manifest"),
    )
    .expect("parse staged manifest");
    let generation_id = staged_manifest["generation_id"]
        .as_str()
        .expect("generation ID")
        .to_owned();
    assert_eq!(staged_manifest["status"], "validated");
    assert_eq!(staged_manifest["corpus_snapshot_id"], snapshot_id);
    assert_eq!(staged_manifest["page_count"], 2);
    assert_eq!(staged_manifest["passage_count"], 2);
    assert_eq!(staged_manifest["keyword_count"], 2);
    assert_eq!(staged_manifest["semantic_count"], 2);
    assert_eq!(
        staged_manifest["semantic"]["backend"],
        "deterministic-usearch"
    );
    assert_eq!(
        staged_manifest["semantic"]["embedding_backend"],
        "deterministic"
    );
    assert_eq!(staged_manifest["semantic"]["reopened"], true);
    assert!(staging.join("catalog.jsonl").is_file());
    assert!(staging.join("keyword").is_dir());
    assert!(staging.join("semantic.usearch").is_file());
    assert!(staging.join("checksums.sha256").is_file());

    let activate = run_scout(
        data_dir.path(),
        [
            "index".to_owned(),
            "activate".to_owned(),
            "--generation".to_owned(),
            generation_id.clone(),
        ],
    );
    assert!(
        activate.status.success(),
        "index activation failed: {}",
        String::from_utf8_lossy(&activate.stderr)
    );
    assert!(
        !staging.exists(),
        "activated staging directory must be sealed"
    );
    let sealed_manifest: Value = serde_json::from_slice(
        &fs::read(
            data_dir
                .path()
                .join("generations")
                .join(&generation_id)
                .join("generation-manifest.json"),
        )
        .expect("read sealed manifest"),
    )
    .expect("parse sealed manifest");
    assert_eq!(sealed_manifest["status"], "sealed");
    assert_eq!(sealed_manifest["generation_id"], generation_id);
    let current: Value = serde_json::from_slice(
        &fs::read(data_dir.path().join("CURRENT")).expect("read current pointer"),
    )
    .expect("parse current pointer");
    assert_eq!(current["generation_id"], generation_id);
    assert_eq!(
        current["manifest_sha256"],
        sealed_manifest["manifest_sha256"]
    );

    let verify = run_scout(data_dir.path(), ["index", "verify"]);
    assert!(
        verify.status.success(),
        "index verify failed: {}",
        String::from_utf8_lossy(&verify.stderr)
    );
    assert!(
        String::from_utf8_lossy(&verify.stdout)
            .contains(&format!("generation verified: {generation_id}"))
    );

    let address = free_address();
    let _serve = ServeProcess::start(data_dir.path(), &address);
    let (status, health) = wait_for_health(&address);
    assert_eq!(status, 200);
    assert_eq!(health, json!({"status": "ok"}));

    let (status, ready) = http_json(&address, "GET", "/v1/readyz", b"").expect("ready response");
    assert_eq!(status, 200);
    assert_eq!(ready["status"], "ready");
    assert_eq!(ready["generation_id"], generation_id);

    let (status, generation) =
        http_json(&address, "GET", "/v1/generation", b"").expect("generation response");
    assert_eq!(status, 200);
    assert_eq!(generation["status"], "ready");
    assert_eq!(generation["generation_id"], generation_id);
    assert_eq!(
        generation["manifest_sha256"],
        sealed_manifest["manifest_sha256"]
    );
    assert_eq!(generation["corpus_snapshot_id"], snapshot_id);
    assert_eq!(generation["page_count"], 2);
    assert_eq!(generation["passage_count"], 2);
    assert_eq!(generation["keyword_count"], 2);
    assert_eq!(generation["semantic_count"], 2);

    for mode in ["keyword", "semantic", "hybrid"] {
        let request = serde_json::to_vec(&json!({
            "query": "retrieval",
            "mode": mode,
            "limit": 10,
        }))
        .expect("serialize search request");
        let (status, response) =
            http_json(&address, "POST", "/v1/search", &request).expect("search response");
        assert_eq!(status, 200, "{mode} search status");
        assert_eq!(response["schema_version"], "scout.search.v1");
        assert_eq!(response["generation_id"], generation_id);
        assert_eq!(response["generation"]["id"], generation_id);
        assert_eq!(response["mode"], mode);
        let results = response["results"].as_array().expect("search results");
        assert!(!results.is_empty(), "{mode} search must return evidence");
        assert!(results.iter().any(|result| result["source_id"] == "alpha"));
        assert!(results.iter().any(|result| result["source_id"] == "beta"));
        let first = &results[0];
        assert_eq!(first["rank"], 1);
        for field in [
            "page_id",
            "source_id",
            "url",
            "title",
            "winning_passage_id",
            "snippet",
        ] {
            assert!(
                first[field].as_str().is_some_and(|value| !value.is_empty()),
                "missing {field}"
            );
        }
        assert!(first["score"].is_number());
        assert!(first["heading_path"].is_array());
    }

    let filtered_request = serde_json::to_vec(&json!({
        "query": "retrieval",
        "mode": "hybrid",
        "source": "alpha",
        "limit": 10,
    }))
    .expect("serialize filtered search request");
    let (status, filtered) =
        http_json(&address, "POST", "/v1/search", &filtered_request).expect("filtered response");
    assert_eq!(status, 200);
    let filtered_results = filtered["results"].as_array().expect("filtered results");
    assert!(!filtered_results.is_empty());
    assert!(
        filtered_results
            .iter()
            .all(|result| result["source_id"] == "alpha")
    );

    let (status, invalid_json) =
        http_json(&address, "POST", "/v1/search", b"{").expect("invalid JSON response");
    assert_eq!(status, 400);
    assert_error(
        &invalid_json,
        "invalid_request",
        "request body must be valid JSON",
    );

    let invalid_mode_request = br#"{"query":"retrieval","mode":"bogus"}"#;
    let (status, invalid_mode) = http_json(&address, "POST", "/v1/search", invalid_mode_request)
        .expect("invalid mode response");
    assert_eq!(status, 400);
    assert_error(
        &invalid_mode,
        "invalid_request",
        "mode must be keyword, semantic, or hybrid",
    );

    let (status, empty_query) = http_json(&address, "POST", "/v1/search", br#"{"query":"   "}"#)
        .expect("empty query response");
    assert_eq!(status, 400);
    assert_error(
        &empty_query,
        "invalid_request",
        "query must be nonempty and at most 10000 characters",
    );

    let (status, not_found) =
        http_json(&address, "GET", "/v1/unknown", b"").expect("not found response");
    assert_eq!(status, 404);
    assert_error(&not_found, "not_found", "route not found");
}
