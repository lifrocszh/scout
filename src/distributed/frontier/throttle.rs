//! Shared adaptive pacing state for the request permit store. The policy itself lives in
//! `crate::crawl::throttle`; this module only persists it per run and origin. Callers hold
//! the run's `scout_crawl_request_policy` row lock, which serializes every update.
use crate::crawl::throttle::{self, Signal};
use crate::error::Error;
use crate::observability::metrics;

/// Extra start spacing currently applied to `origin`, in milliseconds.
pub(super) fn origin_backoff_ms(
    tx: &mut postgres::Transaction<'_>,
    run_id: &str,
    origin: &str,
) -> Result<i64, Error> {
    tx.query_opt(
        "SELECT backoff_ms FROM scout_crawl_origin_throttle
         WHERE crawl_run_id = $1 AND origin = $2",
        &[&run_id, &origin],
    )
    .map(|row| row.map_or(0, |row| row.get(0)))
    .map_err(|error| Error::storage(format!("read origin backoff: {error}")))
}

/// Apply one reported request outcome to the origin's backoff. A throttling response also
/// pushes the origin's next allowed start out by the backoff or the capped `Retry-After`.
pub(super) fn record_outcome(
    tx: &mut postgres::Transaction<'_>,
    run_id: &str,
    origin: &str,
    outcome: &str,
    retry_after_ms: Option<u64>,
) -> Result<(), Error> {
    let signal = throttle::classify_outcome(outcome);
    if signal == Signal::Neutral {
        return Ok(());
    }
    let current = origin_backoff_ms(tx, run_id, origin)?;
    if current == 0 && signal == Signal::Healthy {
        return Ok(());
    }
    let next = throttle::next_backoff(u64::try_from(current).unwrap_or(0), signal);
    let next_ms = i64::try_from(next).map_err(|_| Error::storage("origin backoff overflow"))?;
    let throttled = signal == Signal::Throttled;
    tx.execute(
        "INSERT INTO scout_crawl_origin_throttle
             (crawl_run_id, origin, backoff_ms, throttle_responses, last_throttled_at)
         VALUES ($1, $2, $3, CASE WHEN $4 THEN 1 ELSE 0 END,
                 CASE WHEN $4 THEN clock_timestamp() END)
         ON CONFLICT (crawl_run_id, origin) DO UPDATE
         SET backoff_ms = EXCLUDED.backoff_ms,
             throttle_responses = scout_crawl_origin_throttle.throttle_responses
                 + EXCLUDED.throttle_responses,
             last_throttled_at = COALESCE(EXCLUDED.last_throttled_at,
                                          scout_crawl_origin_throttle.last_throttled_at)",
        &[&run_id, &origin, &next_ms, &throttled],
    )
    .map_err(|error| Error::storage(format!("persist origin backoff: {error}")))?;
    if throttled {
        let hold_ms = i64::try_from(throttle::hold_after_throttle(next, retry_after_ms))
            .map_err(|_| Error::storage("origin hold overflow"))?;
        tx.execute(
            "INSERT INTO scout_crawl_request_spacing(crawl_run_id, schedule_key, next_allowed_at)
             VALUES ($1, $2, clock_timestamp() + ($3::BIGINT * interval '1 millisecond'))
             ON CONFLICT (crawl_run_id, schedule_key) DO UPDATE
             SET next_allowed_at = GREATEST(scout_crawl_request_spacing.next_allowed_at,
                                            EXCLUDED.next_allowed_at)",
            &[&run_id, &origin, &hold_ms],
        )
        .map_err(|error| Error::storage(format!("hold throttled origin: {error}")))?;
        metrics::ORIGIN_THROTTLE_RESPONSES.inc(&[origin]);
    }
    metrics::ORIGIN_BACKOFF_MS.set(&[origin], next as f64);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct OriginThrottle {
    pub(crate) backoff_ms: i64,
    pub(crate) throttle_responses: i64,
}

#[cfg(test)]
impl super::FrontierStore {
    /// The persisted pacing state of one origin, if it was ever throttled.
    pub(crate) fn origin_throttle(
        &mut self,
        run_id: &str,
        origin: &str,
    ) -> Result<Option<OriginThrottle>, Error> {
        self.client
            .query_opt(
                "SELECT backoff_ms, throttle_responses FROM scout_crawl_origin_throttle
                 WHERE crawl_run_id = $1 AND origin = $2",
                &[&run_id, &origin],
            )
            .map(|row| {
                row.map(|row| OriginThrottle {
                    backoff_ms: row.get(0),
                    throttle_responses: row.get(1),
                })
            })
            .map_err(|error| Error::storage(format!("read origin throttle: {error}")))
    }
}
