use super::proto;
use super::{AcceptedCapture, AcceptedCaptureRecord, ClaimedTask, DiscoveredTask, FrontierStore};
use super::{MAX_BODY_TRANSFER_BYTES, MAX_URL_BYTES, owner_for, schedule_key, to_status};
use crate::corpus::capture_selection_key;
use crate::crawl::spool::CaptureSpool;
use crate::crawl::{Capture, Config};
use crate::error::Error;
use crate::storage::{new_run_id, sha256_hex};
use crate::url::normalize_url;
use ::url::Url;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tonic::Status;

pub(super) fn validate_capture(
    config: &Config,
    task: &ClaimedTask,
    capture: &proto::CaptureData,
) -> Result<(), Status> {
    if capture.requested_url != task.normalized_url
        || capture.body.len() > config.max_body_bytes
        || capture.body.len() > config.max_extraction_bytes
        || capture.body.len() > MAX_BODY_TRANSFER_BYTES
        || capture.content_bytes != capture.body.len() as u64
        || capture.body_hash != sha256_hex(&capture.body)
        || capture.content_hash != sha256_hex(capture.identity.as_bytes())
        || !matches!(
            capture.extraction_method.as_str(),
            "semantic_main" | "readability" | "cleaned_body"
        )
    {
        return Err(Status::invalid_argument(
            "capture metadata or body digest is invalid",
        ));
    }
    let final_url = normalize_capture_url(&capture.final_url)?;
    let final_owner = owner_for(config, &final_url)
        .ok_or_else(|| Status::permission_denied("capture final URL is outside Sources"))?;
    if final_owner.source_id != capture.source_id {
        return Err(Status::permission_denied(
            "capture Source does not own final URL",
        ));
    }
    for redirect in &capture.redirects {
        let redirect = normalize_capture_url(redirect)?;
        if owner_for(config, &redirect).is_none() {
            return Err(Status::permission_denied("redirect URL is outside Sources"));
        }
    }
    if capture.status_code < 200 || capture.status_code >= 300 {
        return Err(Status::invalid_argument(
            "capture status code must be successful",
        ));
    }
    Ok(())
}

fn normalize_capture_url(value: &str) -> Result<String, Status> {
    if value.len() > MAX_URL_BYTES {
        return Err(Status::invalid_argument("capture URL exceeds size limit"));
    }
    let parsed =
        Url::parse(value).map_err(|_| Status::invalid_argument("capture URL is invalid"))?;
    let normalized = normalize_url(parsed)
        .map_err(|_| Status::invalid_argument("capture URL is not HTTP(S)"))?;
    if normalized != value {
        return Err(Status::invalid_argument("capture URL must be normalized"));
    }
    Ok(normalized)
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct StoredCapture {
    requested_url: String,
    final_url: String,
    redirects: Vec<String>,
    source_id: String,
    title: String,
    description: Option<String>,
    language: Option<String>,
    canonical_url: Option<String>,
    extraction_method: String,
    content: String,
    identity: String,
    content_hash: String,
    pub(super) content_bytes: u64,
    pub(super) body_hash: String,
    content_type: String,
    status_code: u32,
}

impl From<&proto::CaptureData> for StoredCapture {
    fn from(capture: &proto::CaptureData) -> Self {
        Self {
            requested_url: capture.requested_url.clone(),
            final_url: capture.final_url.clone(),
            redirects: capture.redirects.clone(),
            source_id: capture.source_id.clone(),
            title: capture.title.clone(),
            description: capture.description.clone(),
            language: capture.language.clone(),
            canonical_url: capture.canonical_url.clone(),
            extraction_method: capture.extraction_method.clone(),
            content: capture.content.clone(),
            identity: capture.identity.clone(),
            content_hash: capture.content_hash.clone(),
            content_bytes: capture.content_bytes,
            body_hash: capture.body_hash.clone(),
            content_type: capture.content_type.clone(),
            status_code: capture.status_code,
        }
    }
}

impl StoredCapture {
    pub(super) fn to_frontier_capture(&self) -> Result<AcceptedCapture, Status> {
        Ok(AcceptedCapture {
            source_id: self.source_id.clone(),
            content_hash: self.content_hash.clone(),
            identity: self.identity.clone(),
            body_hash: self.body_hash.clone(),
            body_size: i64::try_from(self.content_bytes)
                .map_err(|_| Status::invalid_argument("capture body size is too large"))?,
            metadata_json: serde_json::to_string(self).map_err(|error| {
                Status::internal(format!("serialize capture metadata: {error}"))
            })?,
        })
    }

    pub(super) fn to_crawl_capture(&self) -> Result<Capture, Error> {
        let extraction_method = match self.extraction_method.as_str() {
            "semantic_main" => "semantic_main",
            "readability" => "readability",
            "cleaned_body" => "cleaned_body",
            _ => return Err(Error::invalid("stored extraction method is unsupported")),
        };
        Ok(Capture {
            requested_url: self.requested_url.clone(),
            final_url: self.final_url.clone(),
            redirects: self.redirects.clone(),
            source_id: self.source_id.clone(),
            title: self.title.clone(),
            description: self.description.clone(),
            language: self.language.clone(),
            canonical_url: self.canonical_url.clone(),
            extraction_method,
            content: self.content.clone(),
            identity: self.identity.clone(),
            content_hash: self.content_hash.clone(),
            blocks: Vec::new(),
            content_bytes: usize::try_from(self.content_bytes)
                .map_err(|_| Error::invalid("stored body size does not fit memory"))?,
            body_hash: self.body_hash.clone(),
            body_file: format!("bodies/{}", self.body_hash),
            content_type: self.content_type.clone(),
            status_code: u16::try_from(self.status_code)
                .map_err(|_| Error::invalid("stored HTTP status code is invalid"))?,
        })
    }
}

#[derive(Serialize)]
struct CompletionIdentity<'a> {
    capture: Option<&'a StoredCapture>,
    discovered_urls: Vec<&'a str>,
    rejection_reason: &'a str,
}

pub(super) fn completion_result_sha256(
    capture: Option<&StoredCapture>,
    discovered_urls: &[String],
    rejection_reason: &str,
) -> Result<String, Status> {
    let mut discovered_urls = discovered_urls
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    discovered_urls.sort_unstable();
    discovered_urls.dedup();
    let identity = CompletionIdentity {
        capture,
        discovered_urls,
        rejection_reason,
    };
    let bytes = serde_json::to_vec(&identity)
        .map_err(|error| Status::internal(format!("serialize task result identity: {error}")))?;
    Ok(sha256_hex(&bytes))
}

pub(super) fn eligible_discoveries(
    config: &Config,
    values: &[String],
    max_frontier: usize,
) -> (Vec<DiscoveredTask>, usize) {
    let mut discovered = BTreeSet::new();
    let mut accepted = Vec::new();
    let mut rejected = 0;
    for value in values {
        if value.len() > MAX_URL_BYTES || discovered.len() >= max_frontier {
            rejected += 1;
            continue;
        }
        let Ok(parsed) = Url::parse(value) else {
            rejected += 1;
            continue;
        };
        let Ok(normalized) = normalize_url(parsed.clone()) else {
            rejected += 1;
            continue;
        };
        let Some(owner) = owner_for(config, &normalized) else {
            rejected += 1;
            continue;
        };
        if normalized != *value {
            rejected += 1;
            continue;
        }
        if discovered.insert(normalized.clone()) {
            accepted.push(DiscoveredTask {
                source_id: owner.source_id.clone(),
                normalized_url: normalized,
                schedule_key: schedule_key(config, &parsed),
            });
        }
    }
    (accepted, rejected)
}

pub(super) fn write_body_file(
    crawl_dir: &Path,
    capture: &proto::CaptureData,
) -> Result<(), Status> {
    let expected_size = usize::try_from(capture.content_bytes)
        .map_err(|_| Status::invalid_argument("capture body size is too large"))?;
    if expected_size != capture.body.len() || sha256_hex(&capture.body) != capture.body_hash {
        return Err(Status::invalid_argument(
            "capture body size or digest is invalid",
        ));
    }
    let relative = PathBuf::from("bodies").join(&capture.body_hash);
    let destination = crawl_dir.join(&relative);
    let parent = destination
        .parent()
        .ok_or_else(|| Status::internal("body path has no parent directory"))?;
    if destination.exists() {
        verify_body_file(&destination, expected_size, &capture.body_hash).map_err(to_status)?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| Status::internal(format!("sync body directory: {error}")))?;
        return Ok(());
    }
    fs::create_dir_all(parent)
        .map_err(|error| Status::internal(format!("create body directory: {error}")))?;
    let temporary = parent.join(format!(".{}.{}", capture.body_hash, new_run_id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| Status::internal(format!("create temporary body file: {error}")))?;
    if let Err(error) = file.write_all(&capture.body).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(&temporary);
        return Err(Status::internal(format!(
            "write content-addressed body: {error}"
        )));
    }
    drop(file);
    if let Err(error) = fs::rename(&temporary, &destination) {
        let _ = fs::remove_file(&temporary);
        return Err(Status::internal(format!(
            "publish content-addressed body: {error}"
        )));
    }
    verify_body_file(&destination, expected_size, &capture.body_hash).map_err(to_status)?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| Status::internal(format!("sync body directory: {error}")))
}

fn verify_body_file(path: &Path, expected_size: usize, expected_hash: &str) -> Result<(), Error> {
    let metadata = fs::metadata(path)
        .map_err(|error| Error::storage(format!("stat content-addressed body: {error}")))?;
    if metadata.len() != expected_size as u64 {
        return Err(Error::storage("content-addressed body size mismatch"));
    }
    let bytes = fs::read(path)
        .map_err(|error| Error::storage(format!("read content-addressed body: {error}")))?;
    if sha256_hex(&bytes) != expected_hash {
        return Err(Error::storage("content-addressed body digest mismatch"));
    }
    Ok(())
}

fn project_capture(
    crawl_dir: &Path,
    spool: &CaptureSpool,
    stored: &StoredCapture,
) -> Result<(), Error> {
    let capture = stored.to_crawl_capture()?;
    verify_body_file(
        &crawl_dir.join(&capture.body_file),
        capture.content_bytes,
        &capture.body_hash,
    )?;
    spool.insert_capture(&capture, &capture_selection_key(&capture))?;
    Ok(())
}

pub(super) fn replay_accepted_captures(
    frontier: &mut FrontierStore,
    run_id: &str,
    crawl_dir: &Path,
    spool: &CaptureSpool,
) -> Result<usize, Error> {
    const BATCH_SIZE: i64 = 64;
    let mut after_task_id = 0;
    let mut projected = 0;
    loop {
        let batch = frontier.accepted_capture_batch(run_id, after_task_id, BATCH_SIZE)?;
        if batch.is_empty() {
            break;
        }
        for AcceptedCaptureRecord {
            task_id,
            metadata_json,
        } in &batch
        {
            let stored: StoredCapture = serde_json::from_str(metadata_json).map_err(|error| {
                Error::storage(format!("decode accepted capture metadata: {error}"))
            })?;
            project_capture(crawl_dir, spool, &stored)?;
            after_task_id = *task_id;
            projected += 1;
        }
        if batch.len() < BATCH_SIZE as usize {
            break;
        }
    }
    Ok(projected)
}
