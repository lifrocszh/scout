//! Optional Kafka ingestion log ([ADR 0003](../../docs/adr/0003-kafka-ingestion-log.md)).
//!
//! Postgres stays the authority: the coordinator accepts results and writes their outbox events
//! as before, whether or not Kafka runs. `scout kafka relay` copies one run's outbox into two
//! topics, and `scout kafka reduce` rebuilds the run's snapshot from those topics alone and checks
//! it against the live one. Both are at-least-once with idempotent effects.
mod log;
mod record;
mod reducer;
mod reducer_state;
mod relay;

use crate::cli::{Cli, KafkaCommand};
use crate::error::Error;
use crate::observability::metrics::Role;
use crate::observability::start_exporter;
use std::time::Duration;

const BOOTSTRAP_ENV: &str = "SCOUT_KAFKA_BOOTSTRAP";
const FAILPOINT_ENV: &str = "SCOUT_FAULT_KAFKA_ABORT";

/// The two topic names under one prefix.
pub(crate) struct Topics {
    pub(crate) results: String,
    pub(crate) events: String,
}

impl Topics {
    pub(crate) fn new(prefix: &str) -> Result<Self, Error> {
        // Kafka topic names: [A-Za-z0-9._-], at most 249 characters.
        if prefix.is_empty()
            || prefix.len() > 200
            || !prefix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(Error::invalid(
                "--topic-prefix must be 1-200 characters of letters, digits, '.', '_' or '-'",
            ));
        }
        Ok(Self {
            results: format!("{prefix}.crawl-results"),
            events: format!("{prefix}.crawl-events"),
        })
    }
}

/// Run `scout kafka relay|reduce`; print a JSON summary on stdout and return the exit code.
pub(crate) fn run(cli: &Cli, command: &KafkaCommand) -> i32 {
    let role = match command {
        KafkaCommand::Relay { .. } => Role::KafkaRelay,
        KafkaCommand::Reduce { .. } => Role::KafkaReducer,
    };
    let result = (|| -> Result<(serde_json::Value, bool), Error> {
        // Held to the end: SCOUT_METRICS_LINGER_SECS keeps it serving for a last scrape.
        let _metrics = start_exporter(cli.metrics_bind.as_deref(), role)?;
        match command {
            KafkaCommand::Relay {
                run_id,
                consumer_id,
                bootstrap,
                topic_prefix,
                partitions,
                follow,
                poll_interval_ms,
            } => {
                let database_url = std::env::var("SCOUT_DATABASE_URL")
                    .ok()
                    .filter(|url| !url.trim().is_empty())
                    .ok_or_else(|| {
                        Error::invalid("SCOUT_DATABASE_URL must be set for kafka relay")
                    })?;
                let bootstrap = bootstrap_servers(bootstrap.as_deref())?;
                let topics = Topics::new(topic_prefix)?;
                let summary = relay::relay(&relay::RelayOptions {
                    database_url: &database_url,
                    bootstrap: &bootstrap,
                    run_id,
                    consumer_id,
                    data_dir: &cli.data_dir,
                    topics: &topics,
                    partitions: *partitions,
                    follow: *follow,
                    poll_interval: Duration::from_millis(*poll_interval_ms),
                })?;
                Ok((json(&summary)?, true))
            }
            KafkaCommand::Reduce {
                run_id,
                config,
                bootstrap,
                topic_prefix,
                follow,
                poll_interval_ms,
                record_delay_ms,
            } => {
                let bootstrap = bootstrap_servers(bootstrap.as_deref())?;
                let topics = Topics::new(topic_prefix)?;
                let summary = reducer::reduce(&reducer::ReduceOptions {
                    bootstrap: &bootstrap,
                    run_id,
                    config_path: config,
                    data_dir: &cli.data_dir,
                    topics: &topics,
                    follow: *follow,
                    poll_interval: Duration::from_millis(*poll_interval_ms),
                    record_delay: Duration::from_millis(*record_delay_ms),
                })?;
                let parity_ok = summary.parity != Some(false);
                Ok((json(&summary)?, parity_ok))
            }
        }
    })();
    match result {
        Ok((summary, ok)) => {
            println!("{summary}");
            if ok {
                0
            } else {
                eprintln!("scout: the snapshot rebuilt from Kafka differs from the live snapshot");
                1
            }
        }
        Err(error) => {
            crate::app::report_error(cli, &error);
            error.exit_code
        }
    }
}

fn bootstrap_servers(flag: Option<&str>) -> Result<String, Error> {
    flag.map(str::to_owned)
        .or_else(|| std::env::var(BOOTSTRAP_ENV).ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::invalid(format!("pass --bootstrap or set {BOOTSTRAP_ENV}")))
}

fn json(value: &impl serde::Serialize) -> Result<serde_json::Value, Error> {
    serde_json::to_value(value)
        .map_err(|error| Error::storage(format!("serialize summary: {error}")))
}

/// Crash injection for the replay tests: with `SCOUT_FAULT_KAFKA_ABORT=<name>:<n>`, the n-th time
/// the process reaches `name` it aborts on the spot (no destructors, no flushes, like SIGKILL).
/// Unset in every normal run.
fn failpoint(name: &str, hit: u64) {
    let Ok(spec) = std::env::var(FAILPOINT_ENV) else {
        return;
    };
    if spec.split_once(':') == Some((name, hit.to_string().as_str())) {
        eprintln!("scout kafka: failpoint {name}:{hit} reached; aborting");
        std::process::abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_prefix_is_validated() {
        let topics = Topics::new("scout").expect("valid");
        assert_eq!(topics.results, "scout.crawl-results");
        assert_eq!(topics.events, "scout.crawl-events");
        for bad in ["", "has space", "slash/", &"x".repeat(201)] {
            assert!(Topics::new(bad).is_err(), "{bad:?}");
        }
    }
}
