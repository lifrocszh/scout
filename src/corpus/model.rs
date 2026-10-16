use crate::crawl::{CrawlPolicy, Source};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ManifestPage {
    pub(crate) requested_url: String,
    pub(crate) final_url: String,
    pub(crate) redirect_chain: Vec<String>,
    pub(crate) source_id: String,
    pub(crate) page_id: String,
    pub(crate) admission_status: String,
    pub(crate) capture_id: String,
    pub(crate) selection_key: String,
    pub(crate) selected_capture_id: String,
    pub(crate) selection_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) alias_of: Option<String>,
    pub(crate) title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) canonical_url: Option<String>,
    pub(crate) extraction_method: String,
    pub(crate) content_type: String,
    pub(crate) content_bytes: usize,
    pub(crate) body_file: String,
    pub(crate) body_sha256: String,
    pub(crate) content_sha256: String,
    pub(crate) status_code: u16,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PageRecord {
    pub(crate) page_id: String,
    pub(crate) source_id: String,
    pub(crate) normalized_final_url: String,
    pub(crate) title: String,
    pub(crate) description: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) canonical_url: Option<String>,
    pub(crate) extraction_method: String,
    pub(crate) content: String,
    pub(crate) content_identity: String,
    pub(crate) content_sha256: String,
    pub(crate) body_sha256: String,
    pub(crate) body_file: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PassageRecord {
    pub(crate) page_id: String,
    pub(crate) passage_id: String,
    pub(crate) source_id: String,
    pub(crate) normalized_final_url: String,
    pub(crate) title: String,
    pub(crate) heading_path: Vec<String>,
    pub(crate) ordinal: usize,
    pub(crate) content: String,
    pub(crate) content_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AliasRecord {
    pub(crate) source_id: String,
    pub(crate) alias_url: String,
    pub(crate) alias_page_id: String,
    pub(crate) representative_page_id: String,
    pub(crate) alias_kind: &'static str,
    pub(crate) requested_url: String,
    pub(crate) redirect_chain: Vec<String>,
    pub(crate) canonical_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Summary {
    pub(crate) captured_page_count: usize,
    pub(crate) page_count: usize,
    pub(crate) alias_count: usize,
    pub(crate) passage_count: usize,
    pub(crate) rejected_count: usize,
    pub(crate) failed_count: usize,
    pub(crate) target_page_count: usize,
    pub(crate) stop_reason: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct Manifest {
    pub(crate) schema_version: &'static str,
    pub(crate) run_id: String,
    pub(crate) status: &'static str,
    pub(crate) started_at_utc: String,
    pub(crate) completed_at_utc: String,
    pub(crate) source_config_sha256: String,
    pub(crate) manifest_sha256: String,
    pub(crate) snapshot_id: Option<String>,
    pub(crate) capture_selection_policy: &'static str,
    pub(crate) user_agent: String,
    pub(crate) robots_user_agent: &'static str,
    pub(crate) policy: CrawlPolicy,
    pub(crate) sources: Vec<Source>,
    pub(crate) pages: Vec<ManifestPage>,
    pub(crate) summary: Summary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<ManifestError>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ManifestError {
    pub(crate) code: String,
    pub(crate) message: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct SnapshotManifest {
    pub(crate) schema_version: &'static str,
    pub(crate) snapshot_id: String,
    pub(crate) status: &'static str,
    pub(crate) run_id: String,
    pub(crate) crawl_manifest_sha256: String,
    pub(crate) source_config_sha256: String,
    pub(crate) embedding_model: String,
    pub(crate) created_at_utc: String,
    pub(crate) page_count: usize,
    pub(crate) alias_count: usize,
    pub(crate) pages_jsonl_sha256: String,
    pub(crate) aliases_jsonl_sha256: String,
    pub(crate) passage_count: usize,
    pub(crate) passages_jsonl_sha256: String,
}

// These records mirror the on-disk schema. Some fields are intentionally only
// read by external tools or future migration code.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct StoredSnapshotManifest {
    pub(crate) schema_version: String,
    pub(crate) snapshot_id: String,
    pub(crate) status: String,
    pub(crate) run_id: String,
    pub(crate) crawl_manifest_sha256: String,
    pub(crate) source_config_sha256: String,
    #[serde(default)]
    pub(crate) embedding_model: Option<String>,
    pub(crate) created_at_utc: String,
    pub(crate) page_count: usize,
    pub(crate) alias_count: usize,
    pub(crate) pages_jsonl_sha256: String,
    pub(crate) aliases_jsonl_sha256: String,
    pub(crate) passage_count: usize,
    pub(crate) passages_jsonl_sha256: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct StoredPage {
    pub(crate) page_id: String,
    pub(crate) source_id: String,
    pub(crate) normalized_final_url: String,
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) description: Option<String>,
    #[serde(default)]
    pub(crate) language: Option<String>,
    #[serde(default)]
    pub(crate) canonical_url: Option<String>,
    pub(crate) extraction_method: String,
    pub(crate) content: String,
    pub(crate) content_identity: String,
    pub(crate) content_sha256: String,
    pub(crate) body_sha256: String,
    pub(crate) body_file: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct StoredPassage {
    pub(crate) page_id: String,
    pub(crate) passage_id: String,
    pub(crate) source_id: String,
    pub(crate) normalized_final_url: String,
    pub(crate) title: String,
    pub(crate) heading_path: Vec<String>,
    pub(crate) ordinal: usize,
    pub(crate) content: String,
    pub(crate) content_sha256: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct StoredAlias {
    pub(crate) source_id: String,
    pub(crate) alias_url: String,
    pub(crate) alias_page_id: String,
    pub(crate) representative_page_id: String,
    pub(crate) alias_kind: String,
    #[serde(default)]
    pub(crate) requested_url: String,
    #[serde(default)]
    pub(crate) redirect_chain: Vec<String>,
    #[serde(default)]
    pub(crate) canonical_url: Option<String>,
}

#[derive(Debug)]
pub(crate) struct SnapshotResult {
    pub(crate) id: String,
    pub(crate) pages: usize,
    pub(crate) aliases: usize,
}
