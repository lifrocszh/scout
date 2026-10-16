//! Immutable local publication for Databricks crawl-event batches.

use crate::distributed::events::{CRAWL_EVENT_SCHEMA_VERSION, CrawlEvent};
use crate::error::Error;
use crate::storage::sha256_hex;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(crate) const BATCH_SCHEMA: &str = "scout.databricks-crawl-event-batch.v1";
pub(crate) const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct PendingEvent {
    pub(crate) event_id: String,
    pub(crate) event_type: String,
    pub(crate) schema_version: i32,
    pub(crate) payload_json: String,
    pub(crate) payload_sha256: Option<String>,
    pub(crate) created_at_utc: String,
    pub(crate) force_quarantine: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct EventEnvelope {
    pub(crate) schema_version: String,
    pub(crate) batch_id: String,
    pub(crate) crawl_run_id: String,
    pub(crate) record_type: String,
    pub(crate) record_id: String,
    pub(crate) source_id: Option<String>,
    pub(crate) event_type: String,
    pub(crate) event_schema_version: i32,
    pub(crate) event_created_at_utc: String,
    pub(crate) payload_sha256: Option<String>,
    pub(crate) record_json: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BatchManifest {
    pub(crate) schema_version: String,
    pub(crate) batch_id: String,
    pub(crate) crawl_run_id: String,
    pub(crate) consumer_id: String,
    pub(crate) records_file: String,
    pub(crate) records_sha256: String,
    pub(crate) records_bytes: usize,
    pub(crate) record_count: usize,
    pub(crate) quarantine_count: usize,
    pub(crate) max_records: usize,
    pub(crate) max_batch_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct PublishedBatch {
    pub(crate) batch_id: String,
    pub(crate) crawl_run_id: String,
    pub(crate) directory: PathBuf,
    pub(crate) event_ids: Vec<String>,
    pub(crate) event_coverage: Vec<EventCoverage>,
    pub(crate) snapshot_published: bool,
    pub(crate) record_count: usize,
    pub(crate) quarantine_count: usize,
    pub(crate) records_bytes: usize,
    pub(crate) manifest_sha256: String,
    pub(crate) records_sha256: String,
    pub(crate) coverage_sha256: String,
}

#[derive(Debug, Clone)]
pub(crate) struct EventCoverage {
    pub(crate) event_id: String,
    pub(crate) event_type: String,
    pub(crate) payload_sha256: Option<String>,
}

impl PendingEvent {
    fn envelope(&self, run_id: &str, batch_id: &str) -> Result<EventEnvelope, Error> {
        let parsed = serde_json::from_str::<CrawlEvent>(&self.payload_json).ok();
        let valid = !self.force_quarantine
            && parsed.as_ref().is_some_and(|event| {
                event.event_id() == self.event_id
                    && event.crawl_run_id() == run_id
                    && event.event_type() == self.event_type
                    && event.schema_version() == CRAWL_EVENT_SCHEMA_VERSION
                    && i32::try_from(event.schema_version()).ok() == Some(self.schema_version)
                    && self
                        .payload_sha256
                        .as_deref()
                        .is_none_or(|expected| sha256_hex(self.payload_json.as_bytes()) == expected)
            });
        let (record_type, source_id, record_json) = if valid {
            let event = parsed.expect("validated event exists");
            let source_id = match &event {
                CrawlEvent::CaptureCommitted(capture) => Some(capture.source_id.clone()),
                CrawlEvent::CrawlCompleted(_) | CrawlEvent::CorpusSnapshotPublished(_) => None,
            };
            (
                "crawl_event".to_owned(),
                source_id,
                self.payload_json.clone(),
            )
        } else {
            let reason = match parsed {
                Some(_) => "event envelope, run, schema, or payload checksum mismatch",
                None => "unsupported or invalid crawl event payload",
            };
            let excerpt: String = self.payload_json.chars().take(2048).collect();
            let quarantine = serde_json::json!({
                "reason": reason,
                "raw_payload_sha256": sha256_hex(self.payload_json.as_bytes()),
                "raw_payload_bytes": self.payload_json.len(),
                "raw_payload_excerpt": excerpt,
                "full_payload_retained_in_postgres": true,
            });
            let record_json = serde_json::to_string(&quarantine).map_err(|error| {
                Error::storage(format!("serialize quarantined crawl event: {error}"))
            })?;
            ("quarantine".to_owned(), None, record_json)
        };
        Ok(EventEnvelope {
            schema_version: BATCH_SCHEMA.to_owned(),
            batch_id: batch_id.to_owned(),
            crawl_run_id: run_id.to_owned(),
            record_type,
            record_id: self.event_id.clone(),
            source_id,
            event_type: self.event_type.clone(),
            event_schema_version: self.schema_version,
            event_created_at_utc: self.created_at_utc.clone(),
            payload_sha256: self.payload_sha256.clone(),
            record_json,
        })
    }

    pub(crate) fn is_snapshot_published(&self, run_id: &str) -> bool {
        !self.force_quarantine
            && serde_json::from_str::<CrawlEvent>(&self.payload_json)
                .ok()
                .is_some_and(|event| {
                    event.event_id() == self.event_id
                        && event.crawl_run_id() == run_id
                        && event.event_type() == self.event_type
                        && event.schema_version() == CRAWL_EVENT_SCHEMA_VERSION
                        && i32::try_from(event.schema_version()).ok() == Some(self.schema_version)
                        && self.payload_sha256.as_deref().is_none_or(|expected| {
                            sha256_hex(self.payload_json.as_bytes()) == expected
                        })
                        && matches!(event, CrawlEvent::CorpusSnapshotPublished(_))
                })
    }
}

pub(crate) fn encoded_record_size(event: &PendingEvent, run_id: &str) -> Result<usize, Error> {
    let envelope = event.envelope(run_id, &format!("batch-{}", "0".repeat(64)))?;
    serde_json::to_vec(&envelope)
        .map(|bytes| bytes.len() + 1)
        .map_err(|error| Error::storage(format!("serialize crawl-event envelope: {error}")))
}

pub(crate) fn publish_batch(
    queue_root: &Path,
    batch_root: &Path,
    run_id: &str,
    consumer_id: &str,
    events: &[PendingEvent],
    max_records: usize,
    max_batch_bytes: usize,
    max_local_bytes: u64,
) -> Result<PublishedBatch, Error> {
    if events.is_empty() || events.len() > max_records || max_batch_bytes > MAX_BATCH_BYTES {
        return Err(Error::invalid(
            "crawl-event batch exceeds its configured bounds",
        ));
    }
    let identity = format!(
        "{BATCH_SCHEMA}\0{run_id}\0{consumer_id}\0{max_records}\0{max_batch_bytes}\0{}",
        events
            .iter()
            .map(|event| event.event_id.as_str())
            .collect::<Vec<_>>()
            .join("\0")
    );
    let batch_id = format!("batch-{}", sha256_hex(identity.as_bytes()));
    let envelopes = events
        .iter()
        .map(|event| event.envelope(run_id, &batch_id))
        .collect::<Result<Vec<_>, _>>()?;
    let mut records = Vec::new();
    for envelope in &envelopes {
        records
            .extend(serde_json::to_vec(envelope).map_err(|error| {
                Error::storage(format!("serialize crawl-event batch: {error}"))
            })?);
        records.push(b'\n');
    }
    if records.len() > max_batch_bytes {
        return Err(Error::invalid("crawl-event batch exceeds its byte limit"));
    }
    let quarantine_count = envelopes
        .iter()
        .filter(|envelope| envelope.record_type == "quarantine")
        .count();
    let manifest = BatchManifest {
        schema_version: BATCH_SCHEMA.to_owned(),
        batch_id: batch_id.clone(),
        crawl_run_id: run_id.to_owned(),
        consumer_id: consumer_id.to_owned(),
        records_file: "records.jsonl".to_owned(),
        records_sha256: sha256_hex(&records),
        records_bytes: records.len(),
        record_count: envelopes.len(),
        quarantine_count,
        max_records,
        max_batch_bytes,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|error| {
        Error::storage(format!("serialize crawl-event batch manifest: {error}"))
    })?;
    let manifest_sha256 = sha256_hex(&manifest_bytes);
    let event_ids = events
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<Vec<_>>();
    let coverage_sha256 = sha256_hex(event_ids.join("\0").as_bytes());
    let destination = batch_root.join(&batch_id);
    if destination.exists() {
        verify_existing(&destination, &manifest_bytes, &manifest_sha256, &records)?;
    } else {
        let current_bytes = directory_bytes(queue_root)?;
        let added_bytes =
            records.len() as u64 + manifest_bytes.len() as u64 + (manifest_sha256.len() + 1) as u64;
        if current_bytes.saturating_add(added_bytes) > max_local_bytes {
            return Err(Error::safety(format!(
                "local Crawl event queue full: {current_bytes} + {added_bytes} bytes exceeds --max-local-bytes {max_local_bytes}; batch not acknowledged; pause export and free or archive retained batches"
            )));
        }
        fs::create_dir_all(batch_root)
            .map_err(|error| Error::storage(format!("create crawl-event batch root: {error}")))?;
        sync_directory_chain(batch_root)?;
        let staging = batch_root.join(format!(".{batch_id}.tmp-{}", crate::storage::new_run_id()));
        fs::create_dir(&staging).map_err(|error| {
            Error::storage(format!("create crawl-event batch staging: {error}"))
        })?;
        write_synced(&staging.join("records.jsonl"), &records)?;
        write_synced(&staging.join("batch-manifest.json"), &manifest_bytes)?;
        write_synced(
            &staging.join("_PUBLISHED"),
            format!("{manifest_sha256}\n").as_bytes(),
        )?;
        sync_directory(&staging)?;
        fs::rename(&staging, &destination)
            .map_err(|error| Error::storage(format!("publish crawl-event batch: {error}")))?;
        sync_directory_chain(batch_root)?;
    }
    Ok(PublishedBatch {
        batch_id,
        crawl_run_id: run_id.to_owned(),
        directory: destination,
        event_ids,
        event_coverage: events
            .iter()
            .map(|event| EventCoverage {
                event_id: event.event_id.clone(),
                event_type: event.event_type.clone(),
                payload_sha256: event.payload_sha256.clone(),
            })
            .collect(),
        snapshot_published: events
            .iter()
            .any(|event| event.is_snapshot_published(run_id)),
        record_count: envelopes.len(),
        quarantine_count,
        records_bytes: records.len(),
        manifest_sha256,
        records_sha256: manifest.records_sha256,
        coverage_sha256,
    })
}

fn directory_bytes(root: &Path) -> Result<u64, Error> {
    let metadata = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(Error::storage(format!(
                "inspect Crawl event queue: {error}"
            )));
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(Error::safety("Crawl event queue contains a symlink"));
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Err(Error::safety("Crawl event queue contains a non-file entry"));
    }
    let mut bytes = 0u64;
    for entry in fs::read_dir(root)
        .map_err(|error| Error::storage(format!("read Crawl event queue: {error}")))?
    {
        let path = entry
            .map_err(|error| Error::storage(format!("read Crawl event queue entry: {error}")))?
            .path();
        bytes = bytes.saturating_add(directory_bytes(&path)?);
    }
    Ok(bytes)
}

fn verify_existing(
    directory: &Path,
    manifest: &[u8],
    manifest_sha256: &str,
    records: &[u8],
) -> Result<(), Error> {
    if !directory.is_dir() {
        return Err(Error::storage(
            "crawl-event batch identity conflicts with a file",
        ));
    }
    let existing_manifest = fs::read(directory.join("batch-manifest.json"))
        .map_err(|error| Error::storage(format!("read published crawl-event manifest: {error}")))?;
    let existing_records = fs::read(directory.join("records.jsonl"))
        .map_err(|error| Error::storage(format!("read published crawl-event records: {error}")))?;
    let published = fs::read_to_string(directory.join("_PUBLISHED"))
        .map_err(|error| Error::storage(format!("read crawl-event publication marker: {error}")))?;
    if existing_manifest != manifest
        || existing_records != records
        || published != format!("{manifest_sha256}\n")
    {
        return Err(Error::storage(
            "crawl-event batch identity was reused with conflicting content",
        ));
    }
    for name in ["batch-manifest.json", "records.jsonl", "_PUBLISHED"] {
        OpenOptions::new()
            .read(true)
            .open(directory.join(name))
            .and_then(|file| file.sync_all())
            .map_err(|error| Error::storage(format!("sync published crawl-event file: {error}")))?;
    }
    sync_directory_chain(directory)?;
    Ok(())
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| Error::storage(format!("create durable batch file: {error}")))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::storage(format!("write durable batch file: {error}")))
}

fn sync_directory(path: &Path) -> Result<(), Error> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Error::storage(format!("sync crawl-event batch directory: {error}")))
}

fn sync_directory_chain(path: &Path) -> Result<(), Error> {
    let mut current = Some(path);
    while let Some(directory) = current {
        if directory.as_os_str().is_empty() {
            break;
        }
        sync_directory(directory)?;
        let parent = directory.parent();
        if parent == Some(directory) {
            break;
        }
        current = parent;
    }
    Ok(())
}
