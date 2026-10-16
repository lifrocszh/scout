//! Transport-independent durable crawl event contracts.

use serde::{Deserialize, Serialize};

pub(crate) const CRAWL_EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data", rename_all = "snake_case")]
pub(crate) enum CrawlEvent {
    CaptureCommitted(CaptureCommitted),
    CrawlCompleted(CrawlCompleted),
    CorpusSnapshotPublished(CorpusSnapshotPublished),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtifactReference {
    pub(crate) uri: String,
    pub(crate) sha256: String,
    pub(crate) byte_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CaptureCommitted {
    pub(crate) event_id: String,
    pub(crate) schema_version: u32,
    pub(crate) crawl_run_id: String,
    pub(crate) source_id: String,
    pub(crate) task_id: i64,
    pub(crate) committed_at_utc: String,
    pub(crate) result_sha256: String,
    pub(crate) content_sha256: String,
    pub(crate) body_artifact: ArtifactReference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CrawlCompleted {
    pub(crate) event_id: String,
    pub(crate) schema_version: u32,
    pub(crate) crawl_run_id: String,
    pub(crate) completed_at_utc: String,
    pub(crate) stop_reason: String,
    pub(crate) config_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CorpusSnapshotPublished {
    pub(crate) event_id: String,
    pub(crate) schema_version: u32,
    pub(crate) crawl_run_id: String,
    pub(crate) snapshot_id: String,
    pub(crate) published_at_utc: String,
    pub(crate) snapshot_manifest: ArtifactReference,
    pub(crate) crawl_manifest: ArtifactReference,
    pub(crate) page_count: u64,
    pub(crate) alias_count: u64,
    pub(crate) passage_count: u64,
}

impl CrawlEvent {
    pub(crate) fn event_id(&self) -> &str {
        match self {
            Self::CaptureCommitted(event) => &event.event_id,
            Self::CrawlCompleted(event) => &event.event_id,
            Self::CorpusSnapshotPublished(event) => &event.event_id,
        }
    }

    pub(crate) fn crawl_run_id(&self) -> &str {
        match self {
            Self::CaptureCommitted(event) => &event.crawl_run_id,
            Self::CrawlCompleted(event) => &event.crawl_run_id,
            Self::CorpusSnapshotPublished(event) => &event.crawl_run_id,
        }
    }

    pub(crate) fn event_type(&self) -> &'static str {
        match self {
            Self::CaptureCommitted(_) => "capture_committed",
            Self::CrawlCompleted(_) => "crawl_completed",
            Self::CorpusSnapshotPublished(_) => "corpus_snapshot_published",
        }
    }

    pub(crate) fn schema_version(&self) -> u32 {
        match self {
            Self::CaptureCommitted(event) => event.schema_version,
            Self::CrawlCompleted(event) => event.schema_version,
            Self::CorpusSnapshotPublished(event) => event.schema_version,
        }
    }
}
