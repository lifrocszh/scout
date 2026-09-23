use super::app::report_error;
use super::generation::recover_generation;
use super::observability::Events;
use super::search::{hybrid_hits, keyword_hits, load_active_generation, semantic_hits_for_query};
use super::storage::{new_run_id, sha256_hex, trim_snippet};
use super::{
    Arc, BTreeMap, CatalogRecord, Cli, Duration, Error, File, Mutex, OpenOptions, Operation,
    Ordering, PageSearchResult, Path, Read, SearchRequest, SocketAddr, TcpListener, TcpStream, Utc,
    Value, Write, io, json, thread,
};

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
    let ready = recover_generation(&cli.data_dir).is_ok();
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
    for incoming in listener.incoming() {
        let Ok(stream) = incoming else { continue };
        let data_dir = cli.data_dir.clone();
        let access = access.clone();
        thread::spawn(move || handle_http_connection(stream, &data_dir, access));
    }
    0
}

fn handle_http_connection(
    mut stream: TcpStream,
    data_dir: &Path,
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
        ("GET", "/v1/readyz") => ready_response(data_dir),
        ("GET", "/v1/generation") => generation_response(data_dir),
        ("POST", "/v1/search") => search_response(data_dir, &body),
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
    if let Ok(mut events) = Events::open(data_dir, &request_id, "serve") {
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

fn ready_response(data_dir: &Path) -> Result<Value, Error> {
    let generation = load_active_generation(data_dir)?;
    Ok(json!({"status": "ready", "generation_id": generation.manifest.generation_id}))
}

fn generation_response(data_dir: &Path) -> Result<Value, Error> {
    let generation = load_active_generation(data_dir)?;
    Ok(json!({
        "status": "ready",
        "generation_id": generation.manifest.generation_id,
        "manifest_sha256": generation.manifest.manifest_sha256,
        "corpus_snapshot_id": generation.manifest.corpus_snapshot_id,
        "page_count": generation.manifest.page_count,
        "passage_count": generation.manifest.passage_count,
        "keyword_count": generation.manifest.keyword_count,
        "semantic_count": generation.manifest.semantic_count,
    }))
}

fn search_response(data_dir: &Path, body: &[u8]) -> Result<Value, Error> {
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
    if !matches!(mode, "keyword" | "semantic" | "hybrid") {
        return Err(Error {
            code: "invalid_request".into(),
            message: "mode must be keyword, semantic, or hybrid".into(),
            exit_code: 2,
        });
    }
    let limit = request.limit.unwrap_or(10);
    if !(1..=100).contains(&limit) {
        return Err(Error {
            code: "invalid_request".into(),
            message: "limit must be between 1 and 100".into(),
            exit_code: 2,
        });
    }
    let generation = load_active_generation(data_dir)?;
    let snippet_window = generation
        .profile
        .snippet_window
        .ok_or_else(|| Error::not_ready("snippet window missing from Generation profile"))?;
    let mut hits = match mode {
        "keyword" => keyword_hits(&generation, query, request.source.as_deref())?,
        "semantic" => semantic_hits_for_query(&generation, query, request.source.as_deref())?,
        "hybrid" => hybrid_hits(&generation, query, request.source.as_deref())?,
        _ => unreachable!("validated search mode"),
    };
    let mut pages = BTreeMap::<String, (f32, &CatalogRecord)>::new();
    for (score, record) in hits.drain(..) {
        let entry = pages
            .entry(record.page_id.clone())
            .or_insert((score, record));
        if score > entry.0 || (score == entry.0 && record.passage_id < entry.1.passage_id) {
            *entry = (score, record);
        }
    }
    let mut results = pages
        .into_iter()
        .map(|(page_id, (score, record))| PageSearchResult {
            rank: 0,
            page_id,
            source_id: record.source_id.clone(),
            url: record.normalized_final_url.clone(),
            title: record.title.clone(),
            score,
            winning_passage_id: record.passage_id.clone(),
            heading_path: record.heading_path.clone(),
            snippet: trim_snippet(&record.text, snippet_window),
        })
        .collect::<Vec<_>>();
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.winning_passage_id.cmp(&b.winning_passage_id))
            .then_with(|| a.page_id.cmp(&b.page_id))
    });
    results.truncate(limit);
    for (index, result) in results.iter_mut().enumerate() {
        result.rank = index + 1;
    }
    Ok(json!({
        "schema_version": "scout.search.v1",
        "generation": {
            "id": generation.manifest.generation_id,
            "corpus_snapshot_id": generation.manifest.corpus_snapshot_id,
            "manifest_sha256": generation.manifest.manifest_sha256,
        },
        "generation_id": generation.manifest.generation_id,
        "mode": mode,
        "results": results,
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
        "conflict" => 409,
        "integrity_error" => 503,
        _ => 500,
    }
}

pub(crate) fn read_http_request(
    stream: &mut TcpStream,
) -> Result<(String, String, Vec<u8>), Error> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| Error::fetch(e.to_string()))?;
    let mut bytes = Vec::new();
    let mut header_end = None;
    let mut chunk = [0_u8; 8192];
    while header_end.is_none() && bytes.len() < 1_048_576 {
        let count = stream
            .read(&mut chunk)
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
        let count = stream
            .read(&mut chunk)
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
