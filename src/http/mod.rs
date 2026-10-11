use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct SearchRequest {
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) mode: Option<String>,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    #[serde(default)]
    pub(crate) source: Option<String>,
}

use super::app::report_error;
use super::generation::{
    ActiveGeneration, IndexGeneration, LifecycleAction, PageQuery, SearchMode, read_pointer,
};
use super::observability::Events;
use super::storage::{new_run_id, sha256_hex};
use crate::cli::{Cli, Operation};
use crate::error::Error;
use chrono::Utc;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{
    Arc, Mutex,
    mpsc::{self, TrySendError},
};
use std::thread;
use std::time::Duration;

const SERVE_ACTIVE_WORKERS: usize = 1;
const SERVE_CONNECTION_QUEUE_CAPACITY: usize = 1;
const SERVE_OVERLOAD_REQUEST_READ_TIMEOUT: Duration = Duration::from_millis(250);

pub(crate) fn run_serve(cli: &Cli) -> i32 {
    let Operation::Serve { bind, access_log } = &cli.operation else {
        unreachable!("serve operation required")
    };
    let address = match bind.parse::<SocketAddr>() {
        Ok(address) => address,
        Err(error) => {
            let error = Error::invalid(format!("invalid bind address {bind:?}: {error}"));
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    let listener = match TcpListener::bind(address) {
        Ok(listener) => listener,
        Err(error) => {
            let error = Error::storage(format!("bind {bind}: {error}"));
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    let access = match access_log {
        Some(path) => match OpenOptions::new().create(true).append(true).open(path) {
            Ok(file) => Some(Arc::new(Mutex::new(Some(file)))),
            Err(error) => {
                let error = Error::storage(format!("open access log {}: {error}", path.display()));
                report_error(cli, &error);
                return error.exit_code;
            }
        },
        None => None,
    };
    let run_id = new_run_id();
    let mut events = match Events::open(&cli.data_dir, &run_id, "serve") {
        Ok(events) => events,
        Err(error) => {
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    let ready = IndexGeneration::at(&cli.data_dir)
        .lifecycle(LifecycleAction::Recover)
        .is_ok();
    let state = Arc::new(ServingState {
        data_dir: cli.data_dir.clone(),
        active_generation: Mutex::new(None),
    });
    events.emit(
        "serve.started",
        "success",
        json!({"bind": bind, "ready": ready, "data_dir": cli.data_dir}),
    );
    let bound = listener
        .local_addr()
        .map(|address| address.to_string())
        .unwrap_or_else(|_| bind.clone());
    println!("scout serve listening on http://{bound}");
    let worker_state = state.clone();
    let worker_access = access.clone();
    let (connection_tx, connection_rx) =
        mpsc::sync_channel::<TcpStream>(SERVE_CONNECTION_QUEUE_CAPACITY);
    if let Err(error) = thread::Builder::new()
        .name("scout-http-worker-0".into())
        .spawn(move || {
            for stream in connection_rx {
                handle_http_connection(stream, &worker_state, worker_access.clone());
            }
        })
    {
        let error = Error::storage(format!("start HTTP worker: {error}"));
        report_error(cli, &error);
        return error.exit_code;
    }
    for incoming in listener.incoming() {
        let Ok(stream) = incoming else { continue };
        match connection_tx.try_send(stream) {
            Ok(()) => {}
            Err(TrySendError::Full(stream) | TrySendError::Disconnected(stream)) => {
                reject_overloaded(stream, &state.data_dir, access.clone());
            }
        }
    }
    0
}

fn reject_overloaded(
    mut stream: TcpStream,
    data_dir: &std::path::Path,
    access_log: Option<Arc<Mutex<Option<File>>>>,
) {
    let peer = stream.peer_addr().ok();
    // Consume a complete, promptly delivered request before closing the socket.
    // This keeps TCP from resetting a client that is still sending its small body.
    let _ = read_http_request_with_timeout(&mut stream, SERVE_OVERLOAD_REQUEST_READ_TIMEOUT);
    let request_id = new_run_id();
    let error = Error {
        code: "overloaded".into(),
        message: "serving capacity reached; retry later".into(),
        exit_code: 1,
    };
    let body =
        serde_json::to_vec(&error_envelope(&error, &request_id)).unwrap_or_else(|_| b"{}".to_vec());
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));
    let _ = write_http_response(&mut stream, 503, &body);
    write_access_log(access_log, peer, "UNKNOWN", "-", 503, body.len());
    if let Ok(mut events) = Events::open(data_dir, &request_id, "serve") {
        events.emit(
            "serve.request_rejected",
            "failure",
            json!({
                "request_id": request_id,
                "reason": "capacity_reached",
                "status": 503,
                "active_workers": SERVE_ACTIVE_WORKERS,
                "queued_connections": SERVE_CONNECTION_QUEUE_CAPACITY,
            }),
        );
    }
}

struct ServingState {
    data_dir: std::path::PathBuf,
    active_generation: Mutex<Option<CachedGeneration>>,
}

struct CachedGeneration {
    generation_id: String,
    manifest_sha256: String,
    generation: Arc<ActiveGeneration>,
}

impl ServingState {
    fn active_generation(&self) -> Result<Arc<ActiveGeneration>, Error> {
        let pointer = read_pointer(&self.data_dir, "CURRENT").ok().flatten();
        let mut cached = self
            .active_generation
            .lock()
            .map_err(|_| Error::storage("serving Generation lock poisoned"))?;
        if let (Some(pointer), Some(cached)) = (pointer.as_ref(), cached.as_ref())
            && pointer.generation_id == cached.generation_id
            && pointer.manifest_sha256 == cached.manifest_sha256
        {
            return Ok(cached.generation.clone());
        }

        let generation = Arc::new(IndexGeneration::at(&self.data_dir).open_active()?);
        let info = generation.info();
        *cached = Some(CachedGeneration {
            generation_id: info.generation_id,
            manifest_sha256: info.manifest_sha256,
            generation: generation.clone(),
        });
        Ok(generation)
    }
}

fn handle_http_connection(
    mut stream: TcpStream,
    state: &ServingState,
    access_log: Option<Arc<Mutex<Option<File>>>>,
) {
    let peer = stream.peer_addr().ok();
    let (method, target, body) = match read_http_request(&mut stream) {
        Ok(request) => request,
        Err(_) => return,
    };
    let path = target.split('?').next().unwrap_or(&target).to_string();
    let request_id = new_run_id();
    let result = match (method.as_str(), path.as_str()) {
        ("GET", "/v1/healthz") => Ok(json!({"status": "ok"})),
        ("GET", "/v1/readyz") => ready_response(state),
        ("GET", "/v1/generation") => generation_response(state),
        ("POST", "/v1/search") => search_response(state, &body),
        _ => Err(Error {
            code: "not_found".into(),
            message: "route not found".into(),
            exit_code: 1,
        }),
    };
    let (status, response_body) = match result {
        Ok(value) => (200, value),
        Err(error) => (
            http_status(&error.code),
            error_envelope(&error, &request_id),
        ),
    };
    let body_bytes = serde_json::to_vec(&response_body).unwrap_or_else(|_| b"{}".to_vec());
    let _ = write_http_response(&mut stream, status, &body_bytes);
    write_access_log(access_log, peer, &method, &target, status, body_bytes.len());
    if let Ok(mut events) = Events::open(&state.data_dir, &request_id, "serve") {
        let event = if path == "/v1/search" {
            "search.completed"
        } else {
            "http.completed"
        };
        events.emit(
            event,
            if status < 400 { "success" } else { "failure" },
            json!({
                "request_id": request_id,
                "method": method,
                "path": path,
                "status": status,
                "query_hash": if path == "/v1/search" { search_query_hash(&body) } else { Value::Null },
                "query_length": if path == "/v1/search" { search_query_length(&body) } else { 0 },
            }),
        );
    }
}

fn ready_response(state: &ServingState) -> Result<Value, Error> {
    let generation = state.active_generation()?;
    let info = generation.info();
    Ok(json!({"status": "ready", "generation_id": info.generation_id}))
}

fn generation_response(state: &ServingState) -> Result<Value, Error> {
    let generation = state.active_generation()?;
    let info = generation.info();
    Ok(json!({
        "status": "ready",
        "generation_id": info.generation_id,
        "manifest_sha256": info.manifest_sha256,
        "corpus_snapshot_id": info.corpus_snapshot_id,
        "page_count": info.page_count,
        "passage_count": info.passage_count,
        "keyword_count": info.keyword_count,
        "semantic_count": info.semantic_count,
    }))
}

fn search_response(state: &ServingState, body: &[u8]) -> Result<Value, Error> {
    let request: SearchRequest = serde_json::from_slice(body).map_err(|_| Error {
        code: "invalid_request".into(),
        message: "request body must be valid JSON".into(),
        exit_code: 2,
    })?;
    let query = request.query.trim();
    if query.is_empty() || query.chars().count() > 10_000 {
        return Err(Error {
            code: "invalid_request".into(),
            message: "query must be nonempty and at most 10000 characters".into(),
            exit_code: 2,
        });
    }
    let mode = request.mode.as_deref().unwrap_or("hybrid");
    let mode = SearchMode::parse(mode).map_err(|_| Error {
        code: "invalid_request".into(),
        message: "mode must be keyword, semantic, or hybrid".into(),
        exit_code: 2,
    })?;
    let limit = request.limit.unwrap_or(10);
    if !(1..=100).contains(&limit) {
        return Err(Error {
            code: "invalid_request".into(),
            message: "limit must be between 1 and 100".into(),
            exit_code: 2,
        });
    }
    let generation = state.active_generation()?;
    let results = generation.search(PageQuery {
        query,
        mode,
        source: request.source.as_deref(),
        limit: Some(limit),
    })?;
    let info = results.generation;
    Ok(json!({
        "schema_version": "scout.search.v1",
        "generation": {
            "id": info.generation_id,
            "corpus_snapshot_id": info.corpus_snapshot_id,
            "manifest_sha256": info.manifest_sha256,
        },
        "generation_id": info.generation_id,
        "mode": results.mode.as_str(),
        "results": results.pages,
    }))
}

fn error_envelope(error: &Error, request_id: &str) -> Value {
    json!({"error": {"code": error.code, "message": error.message, "request_id": request_id}})
}

fn http_status(code: &str) -> u16 {
    match code {
        "invalid_request" => 400,
        "not_found" => 404,
        "not_ready" => 503,
        "overloaded" => 503,
        "conflict" => 409,
        "integrity_error" => 503,
        _ => 500,
    }
}

pub(crate) fn read_http_request(
    stream: &mut TcpStream,
) -> Result<(String, String, Vec<u8>), Error> {
    read_http_request_with_timeout(stream, Duration::from_secs(5))
}

fn read_http_request_with_timeout(
    stream: &mut TcpStream,
    timeout: Duration,
) -> Result<(String, String, Vec<u8>), Error> {
    let deadline = std::time::Instant::now() + timeout;
    let mut bytes = Vec::new();
    let mut header_end = None;
    let mut chunk = [0_u8; 8192];
    while header_end.is_none() && bytes.len() < 1_048_576 {
        let count = read_request_chunk(stream, &mut chunk, deadline)
            .map_err(|e| Error::fetch(e.to_string()))?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        header_end = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4);
    }
    let header_end = header_end.ok_or_else(|| Error::invalid("malformed HTTP request"))?;
    let header = String::from_utf8(bytes[..header_end].to_vec())
        .map_err(|_| Error::invalid("HTTP headers must be UTF-8"))?;
    let mut lines = header.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| Error::invalid("missing HTTP request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let content_length = lines
        .find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
        })
        .unwrap_or(0);
    if content_length > 1_048_576 {
        return Err(Error::invalid("request body too large"));
    }
    while bytes.len() < header_end + content_length {
        let count = read_request_chunk(stream, &mut chunk, deadline)
            .map_err(|e| Error::fetch(e.to_string()))?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    if bytes.len() < header_end + content_length {
        return Err(Error::invalid("truncated HTTP request"));
    }
    Ok((
        method,
        target,
        bytes[header_end..header_end + content_length].to_vec(),
    ))
}

fn read_request_chunk(
    stream: &mut TcpStream,
    chunk: &mut [u8],
    deadline: std::time::Instant,
) -> io::Result<usize> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "HTTP request deadline exceeded",
        ));
    }
    stream.set_read_timeout(Some(remaining))?;
    stream.read(chunk)
}

fn write_http_response(stream: &mut TcpStream, status: u16, body: &[u8]) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

fn write_access_log(
    access_log: Option<Arc<Mutex<Option<File>>>>,
    peer: Option<SocketAddr>,
    method: &str,
    target: &str,
    status: u16,
    bytes: usize,
) {
    let Some(access_log) = access_log else {
        return;
    };
    let Ok(mut guard) = access_log.lock() else {
        return;
    };
    let Some(file) = guard.as_mut() else {
        return;
    };
    let timestamp = Utc::now().format("%d/%b/%Y:%H:%M:%S %z");
    let peer = peer
        .map(|peer| peer.ip().to_string())
        .unwrap_or_else(|| "-".into());
    let _ = writeln!(
        file,
        "{peer} - - [{timestamp}] \"{method} {target} HTTP/1.1\" {status} {bytes} \"-\" \"-\""
    );
    let _ = file.flush();
}

fn search_query_hash(body: &[u8]) -> Value {
    serde_json::from_slice::<SearchRequest>(body)
        .ok()
        .map(|request| Value::String(sha256_hex(request.query.as_bytes())))
        .unwrap_or(Value::Null)
}

fn search_query_length(body: &[u8]) -> usize {
    serde_json::from_slice::<SearchRequest>(body)
        .map(|request| request.query.chars().count())
        .unwrap_or(0)
}
