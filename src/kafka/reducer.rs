//! `scout kafka reduce`: rebuild one run's capture spool and Corpus snapshot from the log alone,
//! then check it against the live snapshot.
//!
//! Each result record is applied as: write the body (content-addressed, verified), insert the
//! capture into the replica spool (idempotent), then record "applied + next offset" in one SQLite
//! transaction. A crash anywhere replays at most the records after the last recorded offset, and
//! replaying a record changes nothing. Duplicates the relay wrote are skipped by `event_id`.
//!
//! The run is complete in the log once its `crawl_completed` event has been read and every
//! `capture_committed` event before it (same partition, so already read) has its result applied.
//! The reducer then seals a snapshot with the same materializer as the live path. Snapshot IDs
//! digest the config and every Page, Alias and Passage record, so equal IDs mean equal content.
use super::log::{Fetched, Log};
use super::record::{EventRecord, ResultRecord};
use super::reducer_state::{Position, ReducerState};
use super::{Topics, failpoint};
use crate::corpus::{SpoolMaterialization, capture_selection_key, materialize_spooled};
use crate::crawl::spool::CaptureSpool;
use crate::crawl::{Config, CrawlResult, load_config};
use crate::distributed::events::CrawlEvent;
use crate::error::Error;
use crate::observability::metrics::{
    KAFKA_CONSUMED_RECORDS, KAFKA_CONSUMER_LAG, KAFKA_SNAPSHOT_PARITY,
};
use crate::storage::{new_run_id, sha256_hex};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

/// How long one partition fetch waits for new records before the loop moves on.
const FETCH_WAIT: Duration = Duration::from_millis(100);
/// How often every partition's lag gauge is refreshed while records are being applied.
const LAG_REFRESH: Duration = Duration::from_secs(1);

pub(super) struct ReduceOptions<'a> {
    pub(super) bootstrap: &'a str,
    pub(super) run_id: &'a str,
    pub(super) config_path: &'a Path,
    pub(super) data_dir: &'a Path,
    pub(super) topics: &'a Topics,
    pub(super) follow: bool,
    pub(super) poll_interval: Duration,
    /// Extra time per applied result: a deliberately slow consumer, for the backpressure demo.
    pub(super) record_delay: Duration,
}

#[derive(Debug, Default, Serialize)]
pub(super) struct ReduceSummary {
    pub(super) crawl_run_id: String,
    /// Distinct results applied to the replica, over every reducer run on this data directory.
    pub(super) results_applied: i64,
    /// Rows in the replica capture spool. Equal to `results_applied` when no effect doubled.
    pub(super) spool_captures: usize,
    pub(super) duplicate_results_skipped: u64,
    pub(super) duplicate_events_skipped: u64,
    pub(super) other_run_records: u64,
    pub(super) replica_snapshot_id: Option<String>,
    pub(super) live_snapshot_id: Option<String>,
    /// `Some(true)` when the replica equals the live snapshot; `None` until both exist.
    pub(super) parity: Option<bool>,
}

pub(super) fn reduce(options: &ReduceOptions<'_>) -> Result<ReduceSummary, Error> {
    let config = load_config(options.config_path)?;
    let crawl_dir = options.data_dir.join("crawls").join(options.run_id);
    let spool = CaptureSpool::open(&crawl_dir, config.max_frontier)?;
    let mut state = ReducerState::open(options.data_dir)?;
    let mut log = Log::connect(options.bootstrap)?;
    let partitions = wait_for_topics(&mut log, options)?;
    crate::observability::set_ready(true);
    // Without --follow, read what the log holds now and stop there.
    let mut stop_at = BTreeMap::new();
    if !options.follow {
        for (topic, partition) in &partitions {
            stop_at.insert(
                (topic.clone(), *partition),
                log.high_watermark(topic, *partition)?,
            );
        }
    }
    let mut summary = ReduceSummary {
        crawl_run_id: options.run_id.to_owned(),
        ..ReduceSummary::default()
    };
    let mut applied_here = 0;
    refresh_lag(&mut log, &state, &partitions)?;
    let mut last_lag_refresh = Instant::now();
    loop {
        let mut read_any = false;
        for (topic, partition) in &partitions {
            let next = state.next_offset(topic, *partition)?;
            if stop_at
                .get(&(topic.clone(), *partition))
                .is_some_and(|end| next >= *end)
            {
                continue;
            }
            let (records, high_watermark) = log.fetch(topic, *partition, next, FETCH_WAIT)?;
            let partition_label = partition.to_string();
            let lag = |next: i64| (high_watermark - next).max(0) as f64;
            KAFKA_CONSUMER_LAG.set(&[topic, &partition_label], lag(next));
            for fetched in records {
                // While one partition's batch is applied (slowly, in the backpressure demo), the
                // others' gauges would keep the value from their last fetch.
                if last_lag_refresh.elapsed() >= LAG_REFRESH {
                    refresh_lag(&mut log, &state, &partitions)?;
                    last_lag_refresh = Instant::now();
                }
                let after = fetched.offset + 1;
                read_any = true;
                let at = Position {
                    topic,
                    partition: *partition,
                    offset: fetched.offset,
                };
                if topic == &options.topics.results {
                    apply_result(
                        &mut state,
                        &spool,
                        &crawl_dir,
                        options,
                        at,
                        fetched,
                        &mut summary,
                        &mut applied_here,
                    )?;
                } else {
                    apply_event(&mut state, options, at, fetched, &mut summary)?;
                }
                KAFKA_CONSUMER_LAG.set(&[topic, &partition_label], lag(after));
            }
        }
        if summary.replica_snapshot_id.is_none() {
            summary.replica_snapshot_id =
                seal_when_complete(&mut state, &spool, &config, &crawl_dir, options)?;
        }
        if summary.replica_snapshot_id.is_some() && summary.parity.is_none() {
            compare_with_live(&state, options.run_id, &mut summary)?;
        }
        let done = if options.follow {
            summary.parity.is_some()
        } else {
            let mut drained = true;
            for ((topic, partition), end) in &stop_at {
                drained &= state.next_offset(topic, *partition)? >= *end;
            }
            drained
        };
        if done {
            break;
        }
        if !read_any {
            thread::sleep(options.poll_interval.saturating_sub(FETCH_WAIT));
        }
    }
    summary.results_applied = state.applied_results(options.run_id)?;
    summary.spool_captures = spool.capture_count()?;
    Ok(summary)
}

/// Set every partition's lag gauge from a fresh high watermark.
fn refresh_lag(
    log: &mut Log,
    state: &ReducerState,
    partitions: &[(String, i32)],
) -> Result<(), Error> {
    for (topic, partition) in partitions {
        let lag = log.high_watermark(topic, *partition)? - state.next_offset(topic, *partition)?;
        KAFKA_CONSUMER_LAG.set(&[topic, &partition.to_string()], lag.max(0) as f64);
    }
    Ok(())
}

fn wait_for_topics(
    log: &mut Log,
    options: &ReduceOptions<'_>,
) -> Result<Vec<(String, i32)>, Error> {
    let mut told = false;
    loop {
        let results = log.partition_count(&options.topics.results)?;
        let events = log.partition_count(&options.topics.events)?;
        if let (Some(results), Some(events)) = (results, events) {
            let mut partitions = Vec::new();
            for (topic, count) in [
                (&options.topics.results, results),
                (&options.topics.events, events),
            ] {
                partitions.extend((0..count).map(|partition| (topic.clone(), partition)));
            }
            return Ok(partitions);
        }
        if !options.follow {
            return Err(Error::not_ready(format!(
                "Kafka topics {} and {} do not exist yet; start the relay first",
                options.topics.results, options.topics.events
            )));
        }
        if !told {
            eprintln!("scout kafka reduce: waiting for the relay to create the topics");
            told = true;
        }
        thread::sleep(options.poll_interval);
    }
}

/// Apply one result record unless it belongs to another run or was applied before.
fn apply_result(
    state: &mut ReducerState,
    spool: &CaptureSpool,
    crawl_dir: &Path,
    options: &ReduceOptions<'_>,
    at: Position<'_>,
    fetched: Fetched,
    summary: &mut ReduceSummary,
    applied_here: &mut u64,
) -> Result<(), Error> {
    let record = ResultRecord::decode(fetched.record).map_err(|error| {
        Error::safety(format!(
            "{} partition {} offset {}: {}",
            at.topic, at.partition, at.offset, error.message
        ))
    })?;
    if record.crawl_run_id != options.run_id {
        KAFKA_CONSUMED_RECORDS.inc(&[at.topic, "other_run"]);
        summary.other_run_records += 1;
        return state.skip(at);
    }
    if state.is_applied(&record.event_id)? {
        KAFKA_CONSUMED_RECORDS.inc(&[at.topic, "duplicate"]);
        summary.duplicate_results_skipped += 1;
        return state.skip(at);
    }
    write_body(crawl_dir, &record.capture.body_hash, &record.body)?;
    spool.insert_capture(&record.capture, &capture_selection_key(&record.capture))?;
    *applied_here += 1;
    // The effect is done and the offset not yet recorded: the window a crash replays.
    failpoint("reduce-after-apply", *applied_here);
    state.record_result(at, &record.event_id, &record.crawl_run_id, record.task_id)?;
    KAFKA_CONSUMED_RECORDS.inc(&[at.topic, "applied"]);
    if !options.record_delay.is_zero() {
        thread::sleep(options.record_delay);
    }
    Ok(())
}

fn apply_event(
    state: &mut ReducerState,
    options: &ReduceOptions<'_>,
    at: Position<'_>,
    fetched: Fetched,
    summary: &mut ReduceSummary,
) -> Result<(), Error> {
    let record = EventRecord::decode(fetched.record).map_err(|error| {
        Error::safety(format!(
            "{} partition {} offset {}: {}",
            at.topic, at.partition, at.offset, error.message
        ))
    })?;
    if record.event.crawl_run_id() != options.run_id {
        KAFKA_CONSUMED_RECORDS.inc(&[at.topic, "other_run"]);
        summary.other_run_records += 1;
        return state.skip(at);
    }
    let inserted = state.record_event(
        at,
        record.event.event_id(),
        record.event.crawl_run_id(),
        record.event.event_type(),
        &record.payload_json,
    )?;
    if inserted {
        KAFKA_CONSUMED_RECORDS.inc(&[at.topic, "applied"]);
    } else {
        KAFKA_CONSUMED_RECORDS.inc(&[at.topic, "duplicate"]);
        summary.duplicate_events_skipped += 1;
    }
    Ok(())
}

/// Seal the replica snapshot once the log holds the whole run; its ID, or `None` if not yet.
fn seal_when_complete(
    state: &mut ReducerState,
    spool: &CaptureSpool,
    config: &Config,
    crawl_dir: &Path,
    options: &ReduceOptions<'_>,
) -> Result<Option<String>, Error> {
    let run_id = options.run_id;
    if let Some((snapshot_id, _, _)) = state.snapshot(run_id)? {
        return Ok(Some(snapshot_id));
    }
    let Some(completed) = state
        .run_events(run_id, "crawl_completed")?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    if state.missing_results(run_id)? > 0 {
        return Ok(None);
    }
    let CrawlEvent::CrawlCompleted(completed) = decode(&completed)? else {
        return Err(Error::storage(
            "stored crawl_completed event has another type",
        ));
    };
    if completed.config_sha256 != config.digest {
        return Err(Error::invalid(format!(
            "the Sources config digest {} differs from the run's {}; pass the run's config",
            config.digest, completed.config_sha256
        )));
    }
    // An earlier reducer may have sealed the snapshot and stopped before recording it.
    if let Some((snapshot_id, pages, aliases)) = sealed_manifest(crawl_dir, options.data_dir)? {
        state.set_snapshot(run_id, &snapshot_id, pages, aliases)?;
        return Ok(Some(snapshot_id));
    }
    let mut started = completed.completed_at_utc.clone();
    for payload in state.run_events(run_id, "capture_committed")? {
        if let CrawlEvent::CaptureCommitted(capture) = decode(&payload)? {
            started = started.min(capture.committed_at_utc);
        }
    }
    spool.validate_source_scope(&config.sources)?;
    let snapshot = materialize_spooled(
        SpoolMaterialization {
            data_dir: options.data_dir,
            crawl_dir,
            run_id,
            config,
            started: &started,
            completed: &completed.completed_at_utc,
            spool,
        },
        // The log carries accepted captures only; rejected and failed counts stay in Postgres.
        CrawlResult {
            captures: Vec::new(),
            rejected_count: 0,
            failed_count: 0,
            stop_reason: completed.stop_reason.clone(),
        },
    )?;
    state.set_snapshot(run_id, &snapshot.id, snapshot.pages, snapshot.aliases)?;
    eprintln!(
        "scout kafka reduce: sealed replica snapshot {} ({} pages, {} aliases)",
        snapshot.id, snapshot.pages, snapshot.aliases
    );
    Ok(Some(snapshot.id))
}

fn compare_with_live(
    state: &ReducerState,
    run_id: &str,
    summary: &mut ReduceSummary,
) -> Result<(), Error> {
    let Some((replica_id, pages, aliases)) = state.snapshot(run_id)? else {
        return Ok(());
    };
    let mut live = Vec::new();
    for payload in state.run_events(run_id, "corpus_snapshot_published")? {
        if let CrawlEvent::CorpusSnapshotPublished(published) = decode(&payload)? {
            live.push(published);
        }
    }
    let Some(first) = live.first() else {
        return Ok(());
    };
    let matching = live.iter().find(|published| {
        published.snapshot_id == replica_id
            && published.page_count == pages as u64
            && published.alias_count == aliases as u64
    });
    summary.live_snapshot_id = Some(matching.unwrap_or(first).snapshot_id.clone());
    summary.parity = Some(matching.is_some());
    KAFKA_SNAPSHOT_PARITY.set(&[], if matching.is_some() { 1.0 } else { 0.0 });
    Ok(())
}

fn decode(payload: &str) -> Result<CrawlEvent, Error> {
    serde_json::from_str(payload)
        .map_err(|error| Error::storage(format!("decode stored run event: {error}")))
}

/// The snapshot a sealed replica manifest names, if the snapshot directory exists.
fn sealed_manifest(
    crawl_dir: &Path,
    data_dir: &Path,
) -> Result<Option<(String, usize, usize)>, Error> {
    let path = crawl_dir.join("crawl-manifest.json");
    let Ok(bytes) = fs::read(&path) else {
        return Ok(None);
    };
    let manifest: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| Error::storage(format!("parse replica crawl manifest: {error}")))?;
    let Some(snapshot_id) = manifest.get("snapshot_id").and_then(|value| value.as_str()) else {
        return Ok(None);
    };
    if !data_dir.join("corpora").join(snapshot_id).is_dir() {
        return Ok(None);
    }
    let count = |name: &str| {
        manifest
            .pointer(&format!("/summary/{name}"))
            .and_then(|value| value.as_u64())
            .unwrap_or_default() as usize
    };
    Ok(Some((
        snapshot_id.to_owned(),
        count("page_count"),
        count("alias_count"),
    )))
}

/// Write a body to `<crawl>/bodies/<sha256>` atomically; an existing file must match.
fn write_body(crawl_dir: &Path, sha256: &str, body: &[u8]) -> Result<(), Error> {
    let bodies = crawl_dir.join("bodies");
    let destination = bodies.join(sha256);
    if destination.exists() {
        let existing = fs::read(&destination)
            .map_err(|error| Error::storage(format!("read replica body: {error}")))?;
        if sha256_hex(&existing) != sha256 {
            return Err(Error::storage(format!(
                "replica body {} is corrupt",
                destination.display()
            )));
        }
        return Ok(());
    }
    fs::create_dir_all(&bodies)
        .map_err(|error| Error::storage(format!("create replica body directory: {error}")))?;
    let temporary = bodies.join(format!(".{sha256}.{}", new_run_id()));
    let written = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .and_then(|mut file| file.write_all(body).and_then(|_| file.sync_all()))
        .and_then(|_| fs::rename(&temporary, &destination));
    if let Err(error) = written {
        let _ = fs::remove_file(&temporary);
        return Err(Error::storage(format!("write replica body: {error}")));
    }
    Ok(())
}
