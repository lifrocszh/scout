mod discovery;

use super::super::MAX_REDIRECTS;
use super::super::extract::extract;
use super::super::spool::CaptureSpool;
use super::helpers::{owner_for, schedule_key};
use super::request::{
    RequestAttempt, RequestCoordinator, RequestResponse, retry_delay, retry_delay_without_response,
    retryable_status,
};
use super::{Config, FetchOutcome};
use crate::error::Error;
use crate::observability::Events;
use crate::url::{normalize_url, origin_key};
use ::url::Url;
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub(super) struct PageResult {
    pub(super) url: String,
    pub(super) outcome: Result<FetchOutcome, Error>,
}

pub(super) struct CrawlWorker<'a> {
    client: Client,
    config: &'a Config,
    events: Arc<Mutex<Events>>,
    coordinator: Arc<RequestCoordinator>,
    spool: CaptureSpool,
}

impl<'a> CrawlWorker<'a> {
    pub(super) fn new(
        client: Client,
        config: &'a Config,
        events: Arc<Mutex<Events>>,
        coordinator: Arc<RequestCoordinator>,
        spool: CaptureSpool,
    ) -> Self {
        Self {
            client,
            config,
            events,
            coordinator,
            spool,
        }
    }

    pub(super) fn process(&self, url: String) -> PageResult {
        let outcome = match self.ensure_robots(&url) {
            Ok(true) => self.fetch(&url),
            Ok(false) => Ok(FetchOutcome::Rejected("robots_denied".into())),
            Err(error) => Err(error),
        };
        PageResult { url, outcome }
    }

    fn emit(&self, event: &str, outcome: &'static str, data: Value) {
        self.events
            .lock()
            .expect("crawl event log lock")
            .emit(event, outcome, data);
    }

    fn fetch(&self, requested_url: &str) -> Result<FetchOutcome, Error> {
        let mut current =
            Url::parse(requested_url).map_err(|e| Error::storage(format!("parse URL: {e}")))?;
        let mut source_id = owner_for(self.config, requested_url)
            .ok_or_else(|| Error::invalid("requested URL outside Source allowlist"))?
            .source_id
            .clone();
        let mut redirects = Vec::new();
        for _ in 0..=MAX_REDIRECTS {
            let mut response = self.request(&current, "page")?;
            if response.status().is_redirection() {
                let Some(location) = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                else {
                    return Ok(FetchOutcome::Rejected("redirect_missing_location".into()));
                };
                let next = current
                    .join(location)
                    .map_err(|_| Error::fetch("invalid redirect location"))?;
                let next = match normalize_url(next) {
                    Ok(url) => url,
                    Err(reason) => return Ok(FetchOutcome::Rejected(reason)),
                };
                let Some(next_source_id) =
                    owner_for(self.config, &next).map(|source| source.source_id.clone())
                else {
                    return Ok(FetchOutcome::Rejected(
                        "redirect_outside_source_allowlist".into(),
                    ));
                };
                if next == requested_url || redirects.contains(&next) {
                    return Ok(FetchOutcome::Rejected("redirect_loop".into()));
                }
                drop(response);
                if !self.ensure_robots(&next)? {
                    return Ok(FetchOutcome::Rejected("redirect_robots_denied".into()));
                }
                source_id = next_source_id;
                redirects.push(next.clone());
                current = Url::parse(&next).expect("normalized redirect parses");
                continue;
            }
            if !response.status().is_success() {
                return Err(Error::fetch(format!(
                    "GET {current} returned HTTP {}",
                    response.status()
                )));
            }
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            let content_type_lower = content_type.to_ascii_lowercase();
            if !content_type.is_empty()
                && !content_type_lower.starts_with("text/html")
                && !content_type_lower.starts_with("application/xhtml+xml")
            {
                return Ok(FetchOutcome::Rejected("non_html_content_type".into()));
            }
            if response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|length| length > self.config.max_body_bytes)
            {
                return Ok(FetchOutcome::Rejected("body_limit_exceeded".into()));
            }
            let status_code = response.status().as_u16();
            let Some(body) = response.read_bounded(
                self.config
                    .max_body_bytes
                    .min(self.config.max_extraction_bytes),
            )?
            else {
                return Ok(FetchOutcome::Rejected("body_limit_exceeded".into()));
            };
            drop(response);
            if body.len() > self.config.max_body_bytes
                || body.len() > self.config.max_extraction_bytes
            {
                return Ok(FetchOutcome::Rejected("body_limit_exceeded".into()));
            }
            let html = match String::from_utf8(body.clone()) {
                Ok(html) => html.trim_start_matches('\u{feff}').to_string(),
                Err(_) => return Ok(FetchOutcome::Rejected("body_decode_failed".into())),
            };
            let extraction_started = Instant::now();
            let extracted = match extract(&html, &current, self.config.min_content_chars) {
                Ok(page) => page,
                Err(reason) => return Ok(FetchOutcome::Rejected(reason)),
            };
            if extraction_started.elapsed()
                > Duration::from_millis(self.config.max_extraction_millis)
            {
                return Ok(FetchOutcome::Rejected(
                    "extraction_cpu_budget_exceeded".into(),
                ));
            }
            return Ok(FetchOutcome::Admitted {
                source_id,
                final_url: current.to_string(),
                redirects,
                status_code,
                content_type,
                body,
                extracted: Box::new(extracted),
            });
        }
        Ok(FetchOutcome::Rejected("redirect_limit_exceeded".into()))
    }

    fn request(&self, url: &Url, request_kind: &'static str) -> Result<RequestResponse, Error> {
        for attempt in 1..=self.config.max_attempts {
            let schedule_key = schedule_key(self.config, url);
            let origin = origin_key(url);
            let permit = self.coordinator.start_request(&schedule_key, &origin)?;
            permit.mark_started();
            let request_attempt = RequestAttempt {
                url: url.to_string(),
                origin: origin.clone(),
                request_kind,
                attempt,
                started_at: Instant::now(),
                concurrency: permit.concurrency,
            };
            let result = self.client.get(url.clone()).send();
            match result {
                Ok(response) => {
                    let status_code = response.status().as_u16();
                    let response = RequestResponse {
                        response: Some(response),
                        _permit: Some(permit),
                        attempt: request_attempt,
                        events: self.events.clone(),
                        status_code,
                        response_body_error: None,
                        timed_out: false,
                    };
                    if retryable_status(status_code) && attempt < self.config.max_attempts {
                        let delay = retry_delay(&response, attempt, self.config);
                        let request_elapsed_ms = response.attempt.elapsed_ms();
                        drop(response);
                        self.emit(
                            "fetch.retry",
                            "success",
                            json!({
                                "url": url.as_str(),
                                "origin": origin,
                                "request_kind": request_kind,
                                "attempt": attempt,
                                "next_attempt": attempt + 1,
                                "status_code": status_code,
                                "request_elapsed_ms": request_elapsed_ms,
                                "delay_ms": delay,
                            }),
                        );
                        if delay > 0 {
                            thread::sleep(Duration::from_millis(delay));
                        }
                    } else if retryable_status(status_code) {
                        return Err(Error::fetch(format!(
                            "GET {url} exhausted {} attempts with HTTP {status_code}",
                            self.config.max_attempts,
                        )));
                    } else {
                        return Ok(response);
                    }
                }
                Err(error) if attempt < self.config.max_attempts => {
                    let delay = retry_delay_without_response(attempt, self.config);
                    let timed_out = error.is_timeout();
                    let message = error.to_string();
                    let request_elapsed_ms = request_attempt.elapsed_ms();
                    drop(permit);
                    request_attempt.emit(
                        &self.events,
                        request_elapsed_ms,
                        None,
                        Some(&message),
                        None,
                        timed_out,
                    );
                    self.emit(
                        "fetch.retry",
                        "success",
                        json!({
                            "url": url.as_str(),
                            "origin": origin,
                            "request_kind": request_kind,
                            "attempt": attempt,
                            "next_attempt": attempt + 1,
                            "reason": "transport_error",
                            "error": message,
                            "timed_out": timed_out,
                            "request_elapsed_ms": request_elapsed_ms,
                            "delay_ms": delay,
                        }),
                    );
                    if delay > 0 {
                        thread::sleep(Duration::from_millis(delay));
                    }
                }
                Err(error) => {
                    let timed_out = error.is_timeout();
                    let message = error.to_string();
                    let request_elapsed_ms = request_attempt.elapsed_ms();
                    drop(permit);
                    request_attempt.emit(
                        &self.events,
                        request_elapsed_ms,
                        None,
                        Some(&message),
                        None,
                        timed_out,
                    );
                    return Err(Error::fetch(format!(
                        "GET {url} exhausted {} attempts: {message}",
                        self.config.max_attempts
                    )));
                }
            }
        }
        Err(Error::safety("request attempt loop exhausted"))
    }
}
