//! Samples frontier depth from PostgreSQL into gauges, off the RPC path.
//!
//! A scrape must never wait on a database query, so a dedicated thread with its own
//! connection (not one of the pool's, which would distort the pool-wait histogram)
//! refreshes the gauges every few seconds. It only runs when `/metrics` is enabled.
use super::FrontierStore;
use crate::error::Error;
use crate::observability::metrics;
use std::thread;
use std::time::Duration;

const INTERVAL_ENV: &str = "SCOUT_METRICS_SAMPLE_SECS";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);
const MAX_INTERVAL_SECS: u64 = 300;
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Start the sampler for `run_id`. The thread runs until the process exits.
pub(super) fn spawn(database_url: &str, run_id: &str) -> Result<(), Error> {
    let interval = interval_from_env()?;
    let (database_url, run_id) = (database_url.to_owned(), run_id.to_owned());
    thread::Builder::new()
        .name("scout-frontier-gauges".into())
        .spawn(move || sample_forever(&database_url, &run_id, interval))
        .map(|_| ())
        .map_err(|error| Error::storage(format!("start frontier gauge sampler: {error}")))
}

fn interval_from_env() -> Result<Duration, Error> {
    let Some(value) = std::env::var(INTERVAL_ENV)
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(DEFAULT_INTERVAL);
    };
    match value.parse::<u64>() {
        Ok(seconds) if (1..=MAX_INTERVAL_SECS).contains(&seconds) => {
            Ok(Duration::from_secs(seconds))
        }
        _ => Err(Error::invalid(format!(
            "{INTERVAL_ENV} must be a whole number of seconds from 1 to {MAX_INTERVAL_SECS}"
        ))),
    }
}

fn sample_forever(database_url: &str, run_id: &str, interval: Duration) {
    let mut store = None;
    loop {
        if store.is_none() {
            match FrontierStore::connect(database_url) {
                Ok(connected) => store = Some(connected),
                Err(error) => {
                    eprintln!(
                        "scout metrics: frontier sampler cannot connect: {}",
                        error.message
                    );
                    thread::sleep(RECONNECT_DELAY);
                    continue;
                }
            }
        }
        let connected = store.as_mut().expect("connected above");
        match connected.task_status_counts(run_id) {
            Ok(counts) => {
                metrics::FRONTIER_TASKS.set(&["pending"], counts.pending as f64);
                metrics::FRONTIER_TASKS.set(&["leased"], counts.leased as f64);
                metrics::FRONTIER_TASKS.set(&["succeeded"], counts.succeeded as f64);
                metrics::FRONTIER_TASKS.set(&["failed"], counts.failed as f64);
                metrics::FRONTIER_EXPIRED_LEASES.set(&[], counts.expired_leases as f64);
                match connected.dead_letter_requeue_count(run_id) {
                    Ok(requeues) => {
                        metrics::FRONTIER_DEAD_LETTER_REQUEUES.set(&[], requeues as f64)
                    }
                    Err(error) => {
                        eprintln!(
                            "scout metrics: dead-letter sampler query failed: {}",
                            error.message
                        );
                        store = None;
                    }
                }
            }
            Err(error) => {
                // Reconnect next round: the connection may be gone, and stale gauges are worse
                // than a gap, so leave the last values and say so.
                eprintln!(
                    "scout metrics: frontier sampler query failed: {}",
                    error.message
                );
                store = None;
            }
        }
        thread::sleep(interval);
    }
}
