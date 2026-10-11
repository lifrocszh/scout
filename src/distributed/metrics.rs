//! Opt-in benchmark observations. Timings include observer overhead; no telemetry in normal runs.
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("SCOUT_DISTRIBUTED_METRICS").is_some())
}

fn emit(data: serde_json::Value) {
    if enabled() {
        eprintln!("{data}");
    }
}

pub(super) fn rpc(name: &str, queued: u128, lock_wait: u128, service: u128, total: u128) {
    emit(serde_json::json!({
        "metric": "rpc", "operation": name, "end_unix_ns": unix_ns(),
        "blocking_queue_ns": queued as u64,
        "core_lock_wait_ns": lock_wait as u64,
        "service_ns": service as u64,
        "server_total_ns": total as u64,
    }));
}

/// Measures BEGIN through function exit (COMMIT/ROLLBACK and local result decoding included).
pub(super) struct DatabaseScope(&'static str, Option<Instant>);

impl DatabaseScope {
    pub(super) fn new(operation: &'static str) -> Self {
        Self(operation, enabled().then(Instant::now))
    }
}

pub(super) fn completion_phase(phase: &'static str, started: Instant) {
    emit(serde_json::json!({
        "metric": "completion_phase",
        "phase": phase,
        "elapsed_ns": started.elapsed().as_nanos() as u64,
    }));
}

pub(super) fn completion_queue(jobs: usize, bytes: usize, lanes: usize) {
    emit(serde_json::json!({
        "metric": "completion_queue",
        "in_flight_jobs": jobs,
        "in_flight_bytes": bytes,
        "lanes": lanes,
    }));
}

impl Drop for DatabaseScope {
    fn drop(&mut self) {
        if let Some(started) = self.1 {
            emit(serde_json::json!({
                "metric": "database_scope", "operation": self.0,
                "elapsed_ns": started.elapsed().as_nanos() as u64,
            }));
        }
    }
}

pub(super) struct HttpAttempt {
    data: Option<serde_json::Value>,
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

impl HttpAttempt {
    pub(super) fn new(run_id: &str, worker: &str, url: &str, kind: &str, permit_id: i64) -> Self {
        Self {
            data: enabled().then(|| {
                serde_json::json!({
                    "metric": "http_attempt", "run_id": run_id, "worker_id": worker,
                    "url": super::protocol::safe_event_url(url), "kind": kind,
                    "permit_id": permit_id, "start_unix_ns": unix_ns(),
                    "decoded_body_bytes": 0, "outcome": "interrupted_or_body_error",
                })
            }),
        }
    }

    pub(super) fn finish(mut self, outcome: &str, bytes: usize) {
        if let Some(data) = self.data.as_mut() {
            data["outcome"] = outcome.into();
            data["decoded_body_bytes"] = bytes.into();
        }
    }
}

impl Drop for HttpAttempt {
    fn drop(&mut self) {
        if let Some(mut data) = self.data.take() {
            data["end_unix_ns"] = unix_ns().into();
            emit(data);
        }
    }
}

/// Worker wall time for one complete gRPC call, including loopback transport.
pub(super) struct RpcScope(&'static str, Option<Instant>);

impl RpcScope {
    pub(super) fn new(operation: &'static str) -> Self {
        Self(operation, enabled().then(Instant::now))
    }
}

impl Drop for RpcScope {
    fn drop(&mut self) {
        if let Some(started) = self.1 {
            emit(serde_json::json!({
                "metric": "worker_rpc", "operation": self.0,
                "elapsed_ns": started.elapsed().as_nanos() as u64,
            }));
        }
    }
}
