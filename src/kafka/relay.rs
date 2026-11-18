//! `scout kafka relay`: copy one run's outbox into Kafka, in outbox order, at least once.
//!
//! For each unacknowledged outbox event, oldest first: a `capture_committed` event first gets its
//! result record (body plus accepted metadata) on the results topic, then every event is written
//! to the events topic, then the outbox checkpoint is acknowledged. A failure retries the same
//! event (head-of-line), so a run's `crawl_completed` never overtakes one of its captures. A crash
//! between writing and acknowledging writes the event again on restart: consumers deduplicate.
use super::log::Log;
use super::record::{EventRecord, ResultRecord};
use super::{Topics, failpoint};
use crate::distributed::events::CrawlEvent;
use crate::distributed::frontier::FrontierStore;
use crate::distributed::frontier::outbox_relay::OutboxEvent;
use crate::error::Error;
use crate::observability::metrics::{
    KAFKA_RELAY_FAILURES, KAFKA_RELAY_PENDING_EVENTS, KAFKA_RELAYED_RECORDS,
};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

const BATCH: usize = 64;
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

pub(super) struct RelayOptions<'a> {
    pub(super) database_url: &'a str,
    pub(super) bootstrap: &'a str,
    pub(super) run_id: &'a str,
    pub(super) consumer_id: &'a str,
    pub(super) data_dir: &'a Path,
    pub(super) topics: &'a Topics,
    pub(super) partitions: i32,
    pub(super) follow: bool,
    pub(super) poll_interval: Duration,
}

#[derive(Debug, Default, Serialize)]
pub(super) struct RelaySummary {
    crawl_run_id: String,
    relayed_events: u64,
    relayed_results: u64,
    retried_attempts: u64,
    pending_events: i64,
}

pub(super) fn relay(options: &RelayOptions<'_>) -> Result<RelaySummary, Error> {
    let mut outbox = FrontierStore::connect(options.database_url)?;
    outbox.apply_migrations()?;
    // A second connection: the outbox batch is read on the first while bodies are looked up here.
    let mut captures = FrontierStore::connect(options.database_url)?;
    let mut log = Log::connect(options.bootstrap)?;
    let result_partitions = log.ensure_topic(&options.topics.results, options.partitions)?;
    let event_partitions = log.ensure_topic(&options.topics.events, options.partitions)?;
    crate::observability::set_ready(true);
    let crawl_dir = options.data_dir.join("crawls").join(options.run_id);
    let mut summary = RelaySummary {
        crawl_run_id: options.run_id.to_owned(),
        ..RelaySummary::default()
    };
    let mut backoff = options.poll_interval;
    'poll: loop {
        let pending = outbox.unrelayed_event_count(options.run_id, options.consumer_id)?;
        KAFKA_RELAY_PENDING_EVENTS.set(&[], pending as f64);
        let batch = outbox.unrelayed_events(options.run_id, options.consumer_id, BATCH)?;
        if batch.is_empty() {
            // Following ends once the run's last event, its published snapshot, is in Kafka.
            if !options.follow || outbox.snapshot_published(options.run_id)? {
                break;
            }
            thread::sleep(options.poll_interval);
            continue;
        }
        for item in batch {
            let event_id = item.event.event_id().to_owned();
            let written = write_event(
                &mut log,
                &mut captures,
                &crawl_dir,
                options.topics,
                (result_partitions, event_partitions),
                &item,
            );
            match written {
                Ok(results) => {
                    summary.relayed_events += 1;
                    summary.relayed_results += results;
                    failpoint("relay-after-produce", summary.relayed_events);
                    outbox.acknowledge_relayed_event(options.consumer_id, &event_id)?;
                    backoff = options.poll_interval;
                }
                Err(error) => {
                    KAFKA_RELAY_FAILURES.inc(&[]);
                    summary.retried_attempts += 1;
                    outbox.record_relay_failure(options.consumer_id, &event_id, &error.message)?;
                    if !options.follow {
                        return Err(error);
                    }
                    eprintln!(
                        "scout kafka relay: {event_id} not relayed, retrying in {}ms: {}",
                        backoff.as_millis(),
                        error.message
                    );
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
                    continue 'poll;
                }
            }
        }
    }
    summary.pending_events = outbox.unrelayed_event_count(options.run_id, options.consumer_id)?;
    KAFKA_RELAY_PENDING_EVENTS.set(&[], summary.pending_events as f64);
    Ok(summary)
}

/// Write one outbox event (and, for a capture, its result first); return the result count.
fn write_event(
    log: &mut Log,
    captures: &mut FrontierStore,
    crawl_dir: &Path,
    topics: &Topics,
    (result_partitions, event_partitions): (i32, i32),
    item: &OutboxEvent,
) -> Result<u64, Error> {
    let mut results = 0;
    if let CrawlEvent::CaptureCommitted(capture) = &item.event {
        let metadata =
            captures.accepted_capture_metadata(&capture.crawl_run_id, capture.task_id)?;
        let body = read_body(crawl_dir, &capture.body_artifact.sha256)?;
        let record = ResultRecord::new(
            &capture.event_id,
            &capture.crawl_run_id,
            capture.task_id,
            metadata,
            body,
        )?;
        if record.capture.body_hash != capture.body_artifact.sha256 {
            return Err(Error::storage(format!(
                "{} references a different body than its accepted capture",
                capture.event_id
            )));
        }
        log.produce(&topics.results, result_partitions, record.encode())?;
        KAFKA_RELAYED_RECORDS.inc(&[&topics.results]);
        results = 1;
    }
    let record = EventRecord {
        event: item.event.clone(),
        payload_json: item.payload_json.clone(),
    };
    log.produce(
        &topics.events,
        event_partitions,
        record.encode(&item.payload_sha256),
    )?;
    KAFKA_RELAYED_RECORDS.inc(&[&topics.events]);
    Ok(results)
}

fn read_body(crawl_dir: &Path, sha256: &str) -> Result<Vec<u8>, Error> {
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::storage("body artifact digest is invalid"));
    }
    let path: PathBuf = crawl_dir.join("bodies").join(sha256);
    fs::read(&path)
        .map_err(|error| Error::storage(format!("read body {}: {error}", path.display())))
}
