use crate::app::run;
use crate::corpus::{SNAPSHOT_SCHEMA, StoredSnapshotManifest};
use crate::error::Error;
use crate::generation::BuildMode;
use crate::generation::{
    GenerationManifest, IndexGeneration, collect_files, directory_digest, validate_generation,
};
use crate::http::read_http_request;
use crate::storage::{sha256_hex, write_if_absent};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::io::{self, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::thread;
use std::time::Duration;

pub(super) fn validate_benchmark_snapshot(
    data_dir: &Path,
    snapshot_id: &str,
) -> Result<(PathBuf, StoredSnapshotManifest), Error> {
    let snapshot_dir = data_dir.join("corpora").join(snapshot_id);
    let manifest_path = snapshot_dir.join("snapshot-manifest.json");
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|e| Error::invalid(format!("read benchmark Corpus snapshot: {e}")))?;
    let manifest: StoredSnapshotManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| Error::invalid(format!("parse benchmark Corpus snapshot: {e}")))?;
    if manifest.schema_version != SNAPSHOT_SCHEMA
        || manifest.status != "complete"
        || manifest.snapshot_id != snapshot_id
    {
        return Err(Error::invalid("benchmark Corpus snapshot is not complete"));
    }
    let files = [
        (
            "pages.jsonl",
            manifest.pages_jsonl_sha256.as_str(),
            manifest.page_count,
        ),
        (
            "aliases.jsonl",
            manifest.aliases_jsonl_sha256.as_str(),
            manifest.alias_count,
        ),
        (
            "passages.jsonl",
            manifest.passages_jsonl_sha256.as_str(),
            manifest.passage_count,
        ),
    ];
    for (name, expected_digest, expected_count) in files {
        let bytes = fs::read(snapshot_dir.join(name))
            .map_err(|e| Error::invalid(format!("read benchmark {name}: {e}")))?;
        if sha256_hex(&bytes) != expected_digest {
            return Err(Error::invalid(format!(
                "benchmark snapshot checksum mismatch: {name}"
            )));
        }
        let count = bytes.iter().filter(|byte| **byte == b'\n').count();
        if count != expected_count {
            return Err(Error::invalid(format!(
                "benchmark snapshot count mismatch: {name}"
            )));
        }
    }
    Ok((snapshot_dir, manifest))
}

fn copy_directory(source: &Path, destination: &Path) -> Result<(), Error> {
    if !source.is_dir() {
        return Err(Error::invalid(format!(
            "benchmark staging source is not a directory: {}",
            source.display()
        )));
    }
    let mut files = Vec::new();
    collect_files(source, source, &mut files)?;
    for (relative, bytes) in files {
        write_if_absent(&destination.join(relative), &bytes)?;
    }
    Ok(())
}

fn safe_replay_path(value: &str) -> Option<&Path> {
    let path = Path::new(value);
    (path.is_relative()
        && !path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir)))
    .then_some(path)
}

pub(super) fn inspect_replay_crawl(replay: &Path, expected_snapshot_id: &str) -> Value {
    let mut evidence = json!({
        "configured": true,
        "path": replay,
        "present": replay.is_dir(),
        "valid": false,
        "snapshot_id": Value::Null,
        "page_count": 0,
        "body_count": 0,
    });
    let Ok(manifest_bytes) = fs::read(replay.join("crawl-manifest.json")) else {
        return evidence;
    };
    let Ok(manifest) = serde_json::from_slice::<Value>(&manifest_bytes) else {
        return evidence;
    };
    let snapshot_id = manifest
        .get("snapshot_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let pages = manifest
        .get("pages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut bodies_valid = true;
    let mut body_count = 0_usize;
    for page in &pages {
        let Some(body_file) = page.get("body_file").and_then(Value::as_str) else {
            bodies_valid = false;
            continue;
        };
        let Some(relative) = safe_replay_path(body_file) else {
            bodies_valid = false;
            continue;
        };
        let Ok(body) = fs::read(replay.join(relative)) else {
            bodies_valid = false;
            continue;
        };
        let expected = page
            .get("body_sha256")
            .and_then(Value::as_str)
            .unwrap_or_default();
        bodies_valid &= !expected.is_empty() && sha256_hex(&body) == expected;
        body_count += 1;
    }
    let digest = directory_digest(replay).ok();
    evidence["digest"] = digest.clone().map_or(Value::Null, Value::String);
    evidence["crawl_manifest_sha256"] = Value::String(sha256_hex(&manifest_bytes));
    evidence["snapshot_id"] = Value::String(snapshot_id.into());
    evidence["page_count"] = json!(pages.len());
    evidence["body_count"] = json!(body_count);
    evidence["valid"] = json!(
        manifest.get("status") == Some(&Value::String("complete".into()))
            && snapshot_id == expected_snapshot_id
            && bodies_valid
            && digest.is_some()
    );
    evidence
}

fn load_replay_page_bodies(replay: &Path) -> Result<Vec<Vec<u8>>, Error> {
    let manifest_path = replay.join("crawl-manifest.json");
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|e| Error::invalid(format!("read replay Crawl Manifest: {e}")))?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| Error::invalid(format!("parse replay Crawl Manifest: {e}")))?;
    if manifest.get("status") != Some(&Value::String("complete".into())) {
        return Err(Error::invalid("replay Crawl Manifest is not complete"));
    }
    let pages = manifest
        .get("pages")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("replay Crawl Manifest has no Pages"))?;
    let mut bodies = Vec::new();
    for page in pages {
        if page.get("alias_of").is_some_and(|alias| !alias.is_null()) {
            continue;
        }
        let body_file = page
            .get("body_file")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid("replay Page has no body file"))?;
        let relative = safe_replay_path(body_file)
            .ok_or_else(|| Error::invalid("replay body path escapes replay directory"))?;
        let body = fs::read(replay.join(relative))
            .map_err(|e| Error::invalid(format!("read replay Page body: {e}")))?;
        let expected = page
            .get("body_sha256")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if expected.is_empty() || sha256_hex(&body) != expected {
            return Err(Error::invalid("replay Page body checksum mismatch"));
        }
        bodies.push(body);
    }
    if bodies.is_empty() {
        return Err(Error::invalid("replay Crawl has no representative Pages"));
    }
    Ok(bodies)
}

struct ReplayServer {
    origin: String,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ReplayServer {
    fn start(bodies: Vec<Vec<u8>>) -> Result<Self, Error> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .map_err(|e| Error::storage(format!("bind benchmark replay server: {e}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| Error::storage(format!("configure benchmark replay server: {e}")))?;
        let origin = format!(
            "http://{}",
            listener
                .local_addr()
                .map_err(|e| Error::storage(format!("read replay server address: {e}")))?
        );
        let routes = bodies
            .into_iter()
            .enumerate()
            .map(|(index, body)| (format!("/__scout_replay/{index}"), body))
            .collect::<HashMap<_, _>>();
        let expected_requests = routes.len() + 1;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let handle = thread::spawn(move || {
            let mut routes = routes;
            let mut served = 0_usize;
            while !thread_stop.load(AtomicOrdering::Relaxed) && served < expected_requests {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(_) => break,
                };
                let Ok((method, target, _)) = read_http_request(&mut stream) else {
                    break;
                };
                if method != "GET" {
                    let _ = write_replay_response(&mut stream, 405, "text/plain", b"method");
                    served += 1;
                    continue;
                }
                let path = target.split('?').next().unwrap_or_default();
                if path == "/robots.txt" {
                    let _ = write_replay_response(
                        &mut stream,
                        200,
                        "text/plain",
                        b"User-agent: *\nAllow: /\n",
                    );
                } else if let Some(body) = routes.remove(path) {
                    let _ = write_replay_response(&mut stream, 200, "text/html", &body);
                } else {
                    let _ = write_replay_response(&mut stream, 404, "text/plain", b"missing");
                }
                served += 1;
            }
        });
        Ok(Self {
            origin,
            stop,
            handle: Some(handle),
        })
    }

    fn stop(&mut self) {
        self.stop.store(true, AtomicOrdering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ReplayServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn write_replay_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

pub(super) struct FreshCrawl {
    pub(super) data_dir: PathBuf,
    pub(super) snapshot_id: String,
}

pub(super) fn benchmark_fresh_crawl(
    data_dir: &Path,
    output_dir: &Path,
    replay: &Path,
    run_number: usize,
) -> Result<FreshCrawl, Error> {
    let bodies = load_replay_page_bodies(replay)?;
    let mut server = ReplayServer::start(bodies.clone())?;
    let stage = output_dir
        .join("staging")
        .join(format!("crawl-run-{run_number:02}"));
    fs::create_dir_all(stage.join("config"))
        .map_err(|e| Error::storage(format!("create fresh crawl staging: {e}")))?;
    let profile = fs::read(data_dir.join("config").join("index.toml"))
        .map_err(|e| Error::invalid(format!("read benchmark build profile: {e}")))?;
    write_if_absent(&stage.join("config").join("index.toml"), &profile)?;
    let seeds = (0..bodies.len())
        .map(|index| format!("\"{}/__scout_replay/{index}\"", server.origin))
        .collect::<Vec<_>>()
        .join(",\n");
    let source_config = format!(
        r#"schema_version = "scout.sources.v1"
contact_url = "https://example.invalid/scout-benchmark"
user_agent = "Scout-Benchmark-Replay/0.1"
page_target = {}
max_frontier = {}
global_concurrency = 16
origin_concurrency = 16
max_attempts = 1
max_total_attempts = {}
max_duration_seconds = 86400
max_extraction_millis = 5000

[[sources]]
source_id = "benchmark-replay"
seeds = [{}]
allowed_origins = ["{}"]
path_prefixes = ["/"]
"#,
        bodies.len(),
        bodies.len().saturating_add(1),
        bodies.len().saturating_mul(2).saturating_add(1),
        seeds,
        server.origin,
    );
    let source_config_path = stage.join("sources.toml");
    fs::write(&source_config_path, source_config)
        .map_err(|e| Error::storage(format!("write fresh crawl config: {e}")))?;
    let status = run([
        OsString::from("scout"),
        OsString::from("crawl"),
        OsString::from("--config"),
        source_config_path.as_os_str().to_os_string(),
        OsString::from("--data-dir"),
        stage.as_os_str().to_os_string(),
    ]);
    server.stop();
    if status != 0 {
        return Err(Error::fetch(format!(
            "benchmark replay Crawl failed with exit code {status}"
        )));
    }
    let crawl_root = stage.join("crawls");
    let crawl_dirs = fs::read_dir(&crawl_root)
        .map_err(|e| Error::storage(format!("read fresh Crawl artifacts: {e}")))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    if crawl_dirs.len() != 1 {
        return Err(Error::storage(
            "fresh Crawl did not produce one Crawl artifact",
        ));
    }
    let crawl_dir = crawl_dirs
        .into_iter()
        .next()
        .expect("fresh Crawl artifact exists");
    let manifest: Value = serde_json::from_slice(
        &fs::read(crawl_dir.join("crawl-manifest.json"))
            .map_err(|e| Error::storage(format!("read fresh Crawl Manifest: {e}")))?,
    )
    .map_err(|e| Error::storage(format!("parse fresh Crawl Manifest: {e}")))?;
    let snapshot_id = manifest
        .get("snapshot_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::storage("fresh Crawl did not publish a Corpus snapshot"))?
        .to_owned();
    Ok(FreshCrawl {
        data_dir: stage,
        snapshot_id,
    })
}

pub(super) fn benchmark_fresh_index(
    data_dir: &Path,
    output_dir: &Path,
    snapshot_id: &str,
    run: usize,
    worker_count: usize,
) -> Result<(PathBuf, GenerationManifest), Error> {
    let workspace = output_dir
        .join("staging")
        .join(format!("index-run-{run:02}-workers-{worker_count}"));
    let (corpus_dir, _) = validate_benchmark_snapshot(data_dir, snapshot_id)?;
    copy_directory(&corpus_dir, &workspace.join("corpora").join(snapshot_id))?;
    let profile_path = data_dir.join("config").join("index.toml");
    let profile_text = fs::read_to_string(&profile_path)
        .map_err(|e| Error::invalid(format!("read benchmark build profile: {e}")))?;
    let mut profile: toml::Value = toml::from_str(&profile_text)
        .map_err(|e| Error::invalid(format!("parse benchmark build profile: {e}")))?;
    profile
        .as_table_mut()
        .ok_or_else(|| Error::invalid("benchmark build profile must be a TOML table"))?
        .insert(
            "worker_count".into(),
            toml::Value::Integer(worker_count as i64),
        );
    let profile_text = toml::to_string_pretty(&profile)
        .map_err(|e| Error::storage(format!("serialize benchmark build profile: {e}")))?;
    let profile_path = workspace.join("config").join("index.toml");
    write_if_absent(&profile_path, profile_text.as_bytes())?;
    let summary = IndexGeneration::at(&workspace).build(
        snapshot_id,
        &format!("benchmark-index-run-{run}-workers-{worker_count}"),
        BuildMode::Hybrid,
    )?;
    let staging = PathBuf::from(
        summary["staging_path"]
            .as_str()
            .ok_or_else(|| Error::storage("benchmark index did not return staging path"))?,
    );
    let manifest = validate_generation(&staging, "validated")?;
    if manifest.corpus_snapshot_id != snapshot_id {
        return Err(Error::invalid("fresh staging snapshot mismatch"));
    }
    Ok((staging, manifest))
}
