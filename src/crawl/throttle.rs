//! Adaptive per-origin pacing. An origin that answers 429 or 503 gets extra start spacing
//! that doubles on every throttling response and shrinks by an eighth on every healthy
//! one, so a crawl slows down quickly and speeds back up slowly. A `Retry-After` header
//! additionally holds the origin until the time it names.
//!
//! The rules are pure functions so the in-process crawler and the PostgreSQL permit
//! store apply exactly the same policy.

use std::time::{Duration, SystemTime};

/// The first throttling response adds at least this much spacing.
pub(crate) const MIN_BACKOFF_MS: u64 = 1_000;
/// Extra spacing never grows beyond this.
pub(crate) const MAX_BACKOFF_MS: u64 = 60_000;
/// A `Retry-After` hold is capped so one header cannot stall an origin for a whole run.
pub(crate) const MAX_RETRY_AFTER_MS: u64 = 300_000;
/// Extra spacing below this is treated as fully recovered.
const RECOVERED_BELOW_MS: u64 = 100;

/// What one HTTP response says about the origin's capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Signal {
    /// 429 Too Many Requests or 503 Service Unavailable.
    Throttled,
    /// Any other response below 500: the origin is serving normally.
    Healthy,
    /// Other server errors and transport failures say nothing about pacing.
    Neutral,
}

pub(crate) fn classify(status: u16) -> Signal {
    match status {
        429 | 503 => Signal::Throttled,
        status if status < 500 => Signal::Healthy,
        _ => Signal::Neutral,
    }
}

/// Classify a worker's reported request outcome (`success`, `http_429`, `transport_error`, ...).
pub(crate) fn classify_outcome(outcome: &str) -> Signal {
    if outcome == "success" {
        return Signal::Healthy;
    }
    outcome
        .strip_prefix("http_")
        .and_then(|status| status.parse::<u16>().ok())
        .map_or(Signal::Neutral, classify)
}

/// Extra spacing after a throttling response.
pub(crate) fn backoff_after_throttle(extra_ms: u64) -> u64 {
    extra_ms
        .saturating_mul(2)
        .clamp(MIN_BACKOFF_MS, MAX_BACKOFF_MS)
}

/// Extra spacing after a healthy response.
pub(crate) fn backoff_after_healthy(extra_ms: u64) -> u64 {
    let next = extra_ms - extra_ms / 8;
    if next < RECOVERED_BELOW_MS { 0 } else { next }
}

/// Apply one signal to the current extra spacing.
pub(crate) fn next_backoff(extra_ms: u64, signal: Signal) -> u64 {
    match signal {
        Signal::Throttled => backoff_after_throttle(extra_ms),
        Signal::Healthy => backoff_after_healthy(extra_ms),
        Signal::Neutral => extra_ms,
    }
}

/// How long the origin must wait before its next request start after a throttling
/// response: the new extra spacing, or the capped `Retry-After`, whichever is longer.
pub(crate) fn hold_after_throttle(extra_ms: u64, retry_after_ms: Option<u64>) -> u64 {
    extra_ms.max(retry_after_ms.unwrap_or(0).min(MAX_RETRY_AFTER_MS))
}

/// Parse `Retry-After` as delay-seconds or an HTTP-date (RFC 9110 §10.2.3), in milliseconds.
/// A date in the past means no wait.
pub(crate) fn parse_retry_after(value: &str, now: SystemTime) -> Option<u64> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds.saturating_mul(1000));
    }
    let date = httpdate::parse_http_date(value).ok()?;
    Some(
        date.duration_since(now)
            .unwrap_or(Duration::ZERO)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttling_doubles_from_a_floor_up_to_a_cap() {
        let mut extra = 0;
        let mut seen = Vec::new();
        for _ in 0..8 {
            extra = next_backoff(extra, Signal::Throttled);
            seen.push(extra);
        }
        assert_eq!(
            seen,
            [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000]
        );
    }

    #[test]
    fn recovery_is_slower_than_backoff_and_reaches_zero() {
        let mut extra = MAX_BACKOFF_MS;
        let mut healthy_responses = 0;
        while extra > 0 {
            extra = next_backoff(extra, Signal::Healthy);
            healthy_responses += 1;
        }
        // Seven throttling responses reach the cap; dozens of healthy ones are needed to undo it.
        assert!(
            (40..=60).contains(&healthy_responses),
            "{healthy_responses}"
        );
        assert_eq!(next_backoff(0, Signal::Healthy), 0);
    }

    #[test]
    fn neutral_outcomes_leave_spacing_alone() {
        assert_eq!(next_backoff(4_000, Signal::Neutral), 4_000);
        assert_eq!(classify(500), Signal::Neutral);
        assert_eq!(classify(502), Signal::Neutral);
        assert_eq!(classify(404), Signal::Healthy);
        assert_eq!(classify(200), Signal::Healthy);
        assert_eq!(classify(429), Signal::Throttled);
        assert_eq!(classify(503), Signal::Throttled);
    }

    #[test]
    fn worker_outcomes_map_to_signals() {
        assert_eq!(classify_outcome("success"), Signal::Healthy);
        assert_eq!(classify_outcome("http_404"), Signal::Healthy);
        assert_eq!(classify_outcome("http_429"), Signal::Throttled);
        assert_eq!(classify_outcome("http_503"), Signal::Throttled);
        assert_eq!(classify_outcome("http_500"), Signal::Neutral);
        assert_eq!(classify_outcome("transport_error"), Signal::Neutral);
        assert_eq!(classify_outcome("body_limit"), Signal::Neutral);
    }

    #[test]
    fn retry_after_accepts_seconds_and_http_dates_and_is_capped_in_holds() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_445_412_480);
        assert_eq!(parse_retry_after(" 120 ", now), Some(120_000));
        // Wed, 21 Oct 2015 07:28:00 GMT is exactly `now`; 30 s later is a 30 s wait.
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:28:30 GMT", now),
            Some(30_000)
        );
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:00:00 GMT", now),
            Some(0)
        );
        assert_eq!(parse_retry_after("soon", now), None);
        assert_eq!(hold_after_throttle(2_000, None), 2_000);
        assert_eq!(hold_after_throttle(2_000, Some(30_000)), 30_000);
        assert_eq!(
            hold_after_throttle(2_000, Some(u64::MAX)),
            MAX_RETRY_AFTER_MS
        );
    }
}
