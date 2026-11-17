//! The `/metrics` HTTP endpoint. Off unless a bind address is given, by
//! `--metrics-bind <addr>` or the `SCOUT_METRICS_BIND` environment variable.
//!
//! It serves plain-text metrics to a Prometheus scraper, one connection at a time,
//! with no authentication or TLS: bind it to a private interface and never publish
//! the port (the Compose stack keeps it on the internal network).
use super::metrics::{self, Role};
use crate::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

const BIND_ENV: &str = "SCOUT_METRICS_BIND";
const LINGER_ENV: &str = "SCOUT_METRICS_LINGER_SECS";
const MAX_LINGER_SECS: u64 = 300;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const MAX_REQUEST_HEAD_BYTES: usize = 8 * 1024;
const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// A running `/metrics` endpoint. Dropping it waits out `SCOUT_METRICS_LINGER_SECS`
/// (default 0) with the endpoint still serving, so a short-lived process (a crawl
/// that has just finished) gets one last scrape with its final counters.
#[derive(Debug)]
pub(crate) struct Exporter {
    address: SocketAddr,
    linger: Duration,
}

impl Exporter {
    #[cfg(test)]
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for Exporter {
    fn drop(&mut self) {
        if !self.linger.is_zero() {
            eprintln!(
                "scout metrics: serving {} for {}s more so the final counters can be scraped",
                self.address,
                self.linger.as_secs()
            );
            thread::sleep(self.linger);
        }
    }
}

/// Start the endpoint for `role` if a bind address is configured; `Ok(None)` when it is not.
/// An explicit `bind` (the CLI flag) wins over `SCOUT_METRICS_BIND`; an empty value means off.
pub(crate) fn start_exporter(bind: Option<&str>, role: Role) -> Result<Option<Exporter>, Error> {
    let from_env = std::env::var(BIND_ENV).ok();
    let Some(bind) = bind.or(from_env.as_deref()).filter(|bind| !bind.is_empty()) else {
        return Ok(None);
    };
    let linger = linger_from_env()?;
    let address: SocketAddr = bind.parse().map_err(|error| {
        Error::invalid(format!("invalid metrics bind address {bind:?}: {error}"))
    })?;
    let listener = TcpListener::bind(address)
        .map_err(|error| Error::storage(format!("bind metrics endpoint {bind}: {error}")))?;
    let address = listener
        .local_addr()
        .map_err(|error| Error::storage(format!("read metrics endpoint address: {error}")))?;
    metrics::BUILD_INFO.set(&[role.name(), env!("CARGO_PKG_VERSION")], 1.0);
    metrics::seed(role);
    thread::Builder::new()
        .name("scout-metrics".into())
        .spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => answer(stream, role),
                    // A persistent accept error (out of descriptors) must not spin the thread.
                    Err(_) => thread::sleep(ACCEPT_ERROR_BACKOFF),
                }
            }
        })
        .map_err(|error| Error::storage(format!("start metrics endpoint: {error}")))?;
    eprintln!(
        "scout metrics: {} listening on http://{address}/metrics",
        role.name()
    );
    Ok(Some(Exporter { address, linger }))
}

fn linger_from_env() -> Result<Duration, Error> {
    let Some(value) = std::env::var(LINGER_ENV)
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(Duration::ZERO);
    };
    match value.parse::<u64>() {
        Ok(seconds) if seconds <= MAX_LINGER_SECS => Ok(Duration::from_secs(seconds)),
        _ => Err(Error::invalid(format!(
            "{LINGER_ENV} must be a whole number of seconds from 0 to {MAX_LINGER_SECS}"
        ))),
    }
}

fn answer(mut stream: TcpStream, role: Role) {
    let _ = stream.set_read_timeout(Some(CONNECTION_TIMEOUT));
    let _ = stream.set_write_timeout(Some(CONNECTION_TIMEOUT));
    let Some(request_line) = read_request_line(&mut stream) else {
        return;
    };
    let mut parts = request_line.split_whitespace();
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let path = target.split('?').next().unwrap_or("");
    let (status, content_type, body) = match (method, path) {
        ("GET", "/metrics") => ("200 OK", CONTENT_TYPE, metrics::render(role)),
        ("GET", _) => (
            "404 Not Found",
            "text/plain",
            "not found; try /metrics\n".to_owned(),
        ),
        _ => (
            "405 Method Not Allowed",
            "text/plain",
            "only GET /metrics is served\n".to_owned(),
        ),
    };
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body.as_bytes());
}

/// The request line, once the whole header block has arrived (or the limits are hit).
fn read_request_line(stream: &mut TcpStream) -> Option<String> {
    let deadline = Instant::now() + CONNECTION_TIMEOUT;
    let mut head = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        // The per-read timeout alone would let a client that dribbles bytes hold the thread.
        if head.len() >= MAX_REQUEST_HEAD_BYTES || Instant::now() >= deadline {
            return None;
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => head.extend_from_slice(&chunk[..read]),
        }
    }
    let line = head.split(|byte| *byte == b'\r').next()?;
    String::from_utf8(line.to_vec())
        .ok()
        .filter(|line| !line.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(address: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(address).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(request.as_bytes()).expect("send");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read");
        response
    }

    #[test]
    fn serves_metrics_over_http_and_rejects_everything_else() {
        let exporter = start_exporter(Some("127.0.0.1:0"), Role::Coordinator)
            .expect("start")
            .expect("enabled");
        metrics::TASK_CLAIMS.inc(&[]);

        let response = get(
            exporter.address(),
            "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
        );
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(
            response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"),
            "{response}"
        );
        assert!(
            response.contains("\r\n\r\n# HELP scout_build_info "),
            "{response}"
        );
        assert!(
            response.contains("scout_build_info{role=\"coordinator\",version=\""),
            "{response}"
        );
        assert!(
            response.contains("# TYPE scout_task_claims_total counter"),
            "{response}"
        );
        let claims = response
            .lines()
            .find(|line| line.starts_with("scout_task_claims_total "))
            .expect("claims sample");
        assert!(
            claims.split(' ').nth(1).unwrap().parse::<u64>().unwrap() >= 1,
            "{claims}"
        );

        let with_query = get(exporter.address(), "GET /metrics?x=1 HTTP/1.1\r\n\r\n");
        assert!(
            with_query.starts_with("HTTP/1.1 200 OK\r\n"),
            "{with_query}"
        );
        let missing = get(exporter.address(), "GET /other HTTP/1.1\r\n\r\n");
        assert!(
            missing.starts_with("HTTP/1.1 404 Not Found\r\n"),
            "{missing}"
        );
        let post = get(exporter.address(), "POST /metrics HTTP/1.1\r\n\r\n");
        assert!(
            post.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
            "{post}"
        );
    }

    #[test]
    fn a_stalled_client_does_not_block_the_next_scrape() {
        let exporter = start_exporter(Some("127.0.0.1:0"), Role::Worker)
            .expect("start")
            .expect("enabled");
        // Connects and sends nothing; the endpoint gives up after CONNECTION_TIMEOUT.
        let _stalled = TcpStream::connect(exporter.address()).expect("connect");
        let response = get(exporter.address(), "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(
            response.contains("# TYPE scout_pages_fetched_total counter"),
            "{response}"
        );
    }

    #[test]
    fn disabled_without_a_bind_address_and_rejects_a_bad_one() {
        // Not asserting on SCOUT_METRICS_BIND here: it is process-wide and tests run in parallel.
        assert!(
            start_exporter(Some(""), Role::Serve)
                .expect("empty is off")
                .is_none()
        );
        let error =
            start_exporter(Some("not-an-address"), Role::Serve).expect_err("invalid address");
        assert!(
            error.message.contains("invalid metrics bind address"),
            "{}",
            error.message
        );
    }
}
