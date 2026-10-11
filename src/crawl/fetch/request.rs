use super::super::Config;
use super::super::robots::RobotsPolicy;
use crate::error::Error;
use crate::observability::Events;
use crate::storage::read_bounded;
use reqwest::header::RETRY_AFTER;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub(super) struct CachedRobots {
    pub(super) rules: RobotsPolicy,
    pub(super) fetched_at: Instant,
    pub(super) body_sha256: String,
    pub(super) sitemap_page_count: usize,
}

#[derive(Default)]
struct RequestState {
    next_start_by_key: HashMap<String, Instant>,
    total_attempts: usize,
    active_requests: usize,
    active_by_key: HashMap<String, usize>,
    active_by_origin: HashMap<String, usize>,
    max_active_requests: usize,
    max_active_by_origin: HashMap<String, usize>,
}

#[derive(Clone, Copy)]
pub(super) struct RequestConcurrency {
    pub(super) active_global: usize,
    pub(super) active_origin: usize,
    pub(super) peak_global: usize,
    pub(super) peak_origin: usize,
}

pub(super) struct RequestCoordinator {
    state: Mutex<RequestState>,
    wake: Condvar,
    robots: Mutex<HashMap<String, CachedRobots>>,
    robots_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    global_concurrency: usize,
    origin_concurrency: usize,
    min_start_spacing: Duration,
    max_total_attempts: usize,
    deadline: Instant,
}

impl RequestCoordinator {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            state: Mutex::new(RequestState::default()),
            wake: Condvar::new(),
            robots: Mutex::new(HashMap::new()),
            robots_locks: Mutex::new(HashMap::new()),
            global_concurrency: config.global_concurrency,
            origin_concurrency: config.origin_concurrency,
            min_start_spacing: Duration::from_millis(config.min_start_spacing_ms),
            max_total_attempts: config.max_total_attempts,
            deadline: Instant::now() + Duration::from_secs(config.max_duration_seconds),
        }
    }

    pub(super) fn fresh_robots_policy(
        &self,
        origin: &str,
        path: &str,
        max_age: Duration,
    ) -> Option<(String, bool)> {
        self.robots
            .lock()
            .expect("robots cache lock")
            .get(origin)
            .filter(|cached| cached.fetched_at.elapsed() <= max_age)
            .map(|cached| (cached.body_sha256.clone(), cached.rules.allows(path)))
    }

    pub(super) fn cache_robots(&self, origin: String, cached: CachedRobots) {
        self.robots
            .lock()
            .expect("robots cache lock")
            .insert(origin, cached);
    }

    pub(super) fn sitemap_page_counts(&self) -> Vec<(String, usize)> {
        self.robots
            .lock()
            .expect("robots cache lock")
            .iter()
            .map(|(origin, cached)| (origin.clone(), cached.sitemap_page_count))
            .collect()
    }

    pub(super) fn robots_lock(&self, origin: &str) -> Arc<Mutex<()>> {
        self.robots_locks
            .lock()
            .expect("robots lock map")
            .entry(origin.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub(super) fn start_request(
        self: &Arc<Self>,
        key: &str,
        origin: &str,
    ) -> Result<RequestPermit, Error> {
        let mut state = self.state.lock().expect("request coordinator lock");
        loop {
            if state.total_attempts >= self.max_total_attempts {
                return Err(Error::safety("request attempt limit exceeded"));
            }
            let now = Instant::now();
            if now >= self.deadline {
                return Err(Error::safety("crawl duration limit exceeded"));
            }
            let next_start = state.next_start_by_key.get(key).copied().unwrap_or(now);
            let active_for_key = state.active_by_key.get(key).copied().unwrap_or(0);
            if state.active_requests < self.global_concurrency
                && active_for_key < self.origin_concurrency
                && next_start <= now
            {
                state.total_attempts += 1;
                state.active_requests += 1;
                *state.active_by_key.entry(key.to_owned()).or_default() += 1;
                *state.active_by_origin.entry(origin.to_owned()).or_default() += 1;
                state.max_active_requests = state.max_active_requests.max(state.active_requests);
                let active_for_origin = state.active_by_origin.get(origin).copied().unwrap_or(0);
                state
                    .max_active_by_origin
                    .entry(origin.to_owned())
                    .and_modify(|maximum| *maximum = (*maximum).max(active_for_origin))
                    .or_insert(active_for_origin);
                state
                    .next_start_by_key
                    .insert(key.to_owned(), now + self.min_start_spacing);
                let active_origin = state.active_by_origin.get(origin).copied().unwrap_or(0);
                let peak_origin = state
                    .max_active_by_origin
                    .get(origin)
                    .copied()
                    .unwrap_or(active_origin);
                return Ok(RequestPermit {
                    coordinator: self.clone(),
                    key: key.to_owned(),
                    origin: origin.to_owned(),
                    concurrency: RequestConcurrency {
                        active_global: state.active_requests,
                        active_origin,
                        peak_global: state.max_active_requests,
                        peak_origin,
                    },
                });
            }
            let wait_for_spacing = next_start.saturating_duration_since(now);
            let wait_for = if wait_for_spacing.is_zero() {
                Duration::from_millis(10)
            } else {
                wait_for_spacing.min(Duration::from_millis(50))
            };
            state = self
                .wake
                .wait_timeout(state, wait_for)
                .expect("request coordinator wait")
                .0;
        }
    }

    fn release(&self, key: &str, origin: &str) {
        let mut state = self.state.lock().expect("request coordinator lock");
        state.active_requests = state.active_requests.saturating_sub(1);
        let remove_key = state
            .active_by_key
            .get_mut(key)
            .map(|active| {
                *active = active.saturating_sub(1);
                *active == 0
            })
            .unwrap_or(false);
        if remove_key {
            state.active_by_key.remove(key);
        }
        let remove_origin = state
            .active_by_origin
            .get_mut(origin)
            .map(|active| {
                *active = active.saturating_sub(1);
                *active == 0
            })
            .unwrap_or(false);
        if remove_origin {
            state.active_by_origin.remove(origin);
        }
        drop(state);
        self.wake.notify_all();
    }

    fn mark_started(&self, key: &str) {
        let mut state = self.state.lock().expect("request coordinator lock");
        state
            .next_start_by_key
            .insert(key.to_owned(), Instant::now() + self.min_start_spacing);
    }

    pub(super) fn stats(&self) -> (usize, HashMap<String, usize>) {
        let state = self.state.lock().expect("request coordinator lock");
        (
            state.max_active_requests,
            state.max_active_by_origin.clone(),
        )
    }
}

pub(super) struct RequestPermit {
    coordinator: Arc<RequestCoordinator>,
    key: String,
    origin: String,
    pub(super) concurrency: RequestConcurrency,
}

impl RequestPermit {
    pub(super) fn mark_started(&self) {
        self.coordinator.mark_started(&self.key);
    }
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.coordinator.release(&self.key, &self.origin);
    }
}

pub(super) struct RequestAttempt {
    pub(super) url: String,
    pub(super) origin: String,
    pub(super) request_kind: &'static str,
    pub(super) attempt: usize,
    pub(super) started_at: Instant,
    pub(super) concurrency: RequestConcurrency,
}

impl RequestAttempt {
    pub(super) fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    pub(super) fn emit(
        &self,
        events: &Arc<Mutex<Events>>,
        request_elapsed_ms: u64,
        status_code: Option<u16>,
        transport_error: Option<&str>,
        response_body_error: Option<&str>,
        timed_out: bool,
    ) {
        let outcome = if transport_error.is_some()
            || response_body_error.is_some()
            || status_code.is_some_and(|status| status >= 400)
        {
            "failure"
        } else {
            "success"
        };
        events.lock().expect("crawl event log lock").emit(
            "fetch.attempt",
            outcome,
            json!({
                "schema_version": "scout.crawl-attempt.v1",
                "request_kind": self.request_kind,
                "url": self.url,
                "origin": self.origin,
                "attempt": self.attempt,
                "request_elapsed_ms": request_elapsed_ms,
                "request_elapsed_scope": "client_send_start_to_response_drop_or_transport_error",
                "active_requests_global_at_start": self.concurrency.active_global,
                "active_requests_origin_at_start": self.concurrency.active_origin,
                "peak_active_requests_global_at_start": self.concurrency.peak_global,
                "peak_active_requests_origin_at_start": self.concurrency.peak_origin,
                "active_request_scope": "request_permit_through_response_drop_including_body_read",
                "status_code": status_code,
                "retryable_status": status_code.is_some_and(retryable_status),
                "transport_error": transport_error,
                "response_body_error": response_body_error,
                "timed_out": timed_out,
            }),
        );
    }
}

// Permit remains live until response body is consumed and response is dropped.
pub(super) struct RequestResponse {
    pub(super) response: Option<reqwest::blocking::Response>,
    pub(super) _permit: Option<RequestPermit>,
    pub(super) attempt: RequestAttempt,
    pub(super) events: Arc<Mutex<Events>>,
    pub(super) status_code: u16,
    pub(super) response_body_error: Option<String>,
    pub(super) timed_out: bool,
}

impl RequestResponse {
    pub(super) fn read_bounded(&mut self, limit: usize) -> Result<Option<Vec<u8>>, Error> {
        let response = self.response.take().expect("response body read once");
        let result = read_bounded(response, limit);
        if let Err(error) = &result {
            let message = error.message.to_ascii_lowercase();
            self.timed_out = message.contains("timed out") || message.contains("timeout");
            self.response_body_error = Some(error.message.clone());
        }
        result
    }
}

impl std::ops::Deref for RequestResponse {
    type Target = reqwest::blocking::Response;

    fn deref(&self) -> &Self::Target {
        self.response.as_ref().expect("response body already read")
    }
}

impl Drop for RequestResponse {
    fn drop(&mut self) {
        drop(self.response.take());
        let request_elapsed_ms = self.attempt.elapsed_ms();
        drop(self._permit.take());
        self.attempt.emit(
            &self.events,
            request_elapsed_ms,
            Some(self.status_code),
            None,
            self.response_body_error.as_deref(),
            self.timed_out,
        );
    }
}

pub(super) fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

pub(super) fn retry_delay(
    response: &reqwest::blocking::Response,
    attempt: usize,
    config: &Config,
) -> u64 {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1000))
        .unwrap_or_else(|| retry_delay_without_response(attempt, config))
}

pub(super) fn retry_delay_without_response(attempt: usize, config: &Config) -> u64 {
    let multiplier = 1_u64 << attempt.saturating_sub(1).min(20);
    config
        .retry_backoff_ms
        .saturating_mul(multiplier)
        .min(config.retry_max_delay_ms)
}
