use super::{EXTRACTION_SCHEMA, PASSAGE_SCHEMA};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Deserialize)]
pub(crate) struct RawConfig {
    pub(crate) schema_version: Option<String>,
    pub(crate) contact_url: Option<String>,
    pub(crate) user_agent: Option<String>,
    pub(crate) embedding_model: Option<String>,
    pub(crate) page_target: Option<usize>,
    pub(crate) min_content_chars: Option<usize>,
    pub(crate) max_decoded_body_bytes: Option<usize>,
    pub(crate) max_frontier: Option<usize>,
    pub(crate) request_timeout_seconds: Option<u64>,
    pub(crate) global_concurrency: Option<usize>,
    pub(crate) origin_concurrency: Option<usize>,
    pub(crate) min_start_spacing_ms: Option<u64>,
    #[serde(default)]
    pub(crate) origins: Vec<RawOriginPolicy>,
    pub(crate) max_attempts: Option<usize>,
    pub(crate) retry_backoff_ms: Option<u64>,
    pub(crate) retry_max_delay_ms: Option<u64>,
    pub(crate) max_total_attempts: Option<usize>,
    pub(crate) max_duration_seconds: Option<u64>,
    pub(crate) robots_cache_seconds: Option<u64>,
    pub(crate) max_extraction_bytes: Option<usize>,
    pub(crate) max_extraction_millis: Option<u64>,
    #[serde(alias = "passage_max_tokens", alias = "max_model_tokens")]
    pub(crate) max_passage_tokens: Option<usize>,
    #[serde(default)]
    pub(crate) sources: Vec<RawSource>,
    pub(crate) crawl: Option<RawProfile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawOriginPolicy {
    pub(crate) origin: String,
    pub(crate) origin_concurrency: Option<usize>,
    pub(crate) min_start_spacing_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OriginRequestPolicy {
    pub(crate) origin: String,
    pub(crate) origin_concurrency: usize,
    pub(crate) min_start_spacing_ms: u64,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RawProfile {
    pub(crate) user_agent: Option<String>,
    pub(crate) page_target: Option<usize>,
    pub(crate) min_content_chars: Option<usize>,
    pub(crate) max_decoded_body_bytes: Option<usize>,
    pub(crate) max_frontier: Option<usize>,
    pub(crate) request_timeout_seconds: Option<u64>,
    pub(crate) global_concurrency: Option<usize>,
    pub(crate) origin_concurrency: Option<usize>,
    pub(crate) min_start_spacing_ms: Option<u64>,
    pub(crate) max_attempts: Option<usize>,
    pub(crate) retry_backoff_ms: Option<u64>,
    pub(crate) retry_max_delay_ms: Option<u64>,
    pub(crate) max_total_attempts: Option<usize>,
    pub(crate) max_duration_seconds: Option<u64>,
    pub(crate) robots_cache_seconds: Option<u64>,
    pub(crate) max_extraction_bytes: Option<usize>,
    pub(crate) max_extraction_millis: Option<u64>,
    #[serde(alias = "passage_max_tokens", alias = "max_model_tokens")]
    pub(crate) max_passage_tokens: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawSource {
    pub(crate) source_id: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) seeds: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) allowed_urls: Option<Vec<String>>,
    #[serde(alias = "origins")]
    pub(crate) allowed_origins: Option<Vec<String>>,
    #[serde(alias = "allowed_path_prefixes", alias = "paths")]
    pub(crate) path_prefixes: Option<Vec<String>>,
    #[serde(default, alias = "deny_paths")]
    pub(crate) deny_path_prefixes: Vec<String>,
    #[serde(default)]
    pub(crate) sitemap_url_prefixes: Vec<String>,
    pub(crate) politeness_group: Option<String>,
    pub(crate) license_urls: Option<Vec<String>>,
    #[serde(alias = "min_pages", alias = "minimum_pages")]
    pub(crate) minimum_page_quota: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Source {
    pub(crate) source_id: String,
    pub(crate) name: Option<String>,
    pub(crate) seeds: Vec<String>,
    // When set, only these normalized page URLs belong to this Source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) allowed_urls: Option<BTreeSet<String>>,
    pub(crate) allowed_origins: Vec<String>,
    pub(crate) path_prefixes: Vec<String>,
    pub(crate) deny_path_prefixes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) sitemap_url_prefixes: Vec<String>,
    pub(crate) politeness_group: Option<String>,
    pub(crate) license_urls: Vec<String>,
    pub(crate) minimum_page_quota: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) schema_version: String,
    pub(crate) contact_url: String,
    pub(crate) user_agent: String,
    pub(crate) embedding_model: String,
    pub(crate) page_target: usize,
    pub(crate) min_content_chars: usize,
    pub(crate) max_body_bytes: usize,
    pub(crate) max_frontier: usize,
    pub(crate) timeout_seconds: u64,
    pub(crate) global_concurrency: usize,
    pub(crate) origin_concurrency: usize,
    pub(crate) min_start_spacing_ms: u64,
    pub(crate) origin_policies: Vec<OriginRequestPolicy>,
    pub(crate) max_attempts: usize,
    pub(crate) retry_backoff_ms: u64,
    pub(crate) retry_max_delay_ms: u64,
    pub(crate) max_total_attempts: usize,
    pub(crate) max_duration_seconds: u64,
    pub(crate) robots_cache_seconds: u64,
    pub(crate) max_extraction_bytes: usize,
    pub(crate) max_extraction_millis: u64,
    pub(crate) max_passage_tokens: usize,
    pub(crate) sources: Vec<Source>,
    pub(crate) digest: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CrawlPolicy {
    pub(crate) schema_version: String,
    pub(crate) contact_url: String,
    pub(crate) embedding_model: String,
    pub(crate) global_concurrency: usize,
    pub(crate) origin_concurrency: usize,
    pub(crate) min_start_spacing_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) origin_policies: Vec<OriginRequestPolicy>,
    pub(crate) max_frontier: usize,
    pub(crate) max_decoded_body_bytes: usize,
    pub(crate) max_extraction_bytes: usize,
    pub(crate) max_extraction_millis: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) max_attempts: usize,
    pub(crate) retry_backoff_ms: u64,
    pub(crate) retry_max_delay_ms: u64,
    pub(crate) max_total_attempts: usize,
    pub(crate) max_duration_seconds: u64,
    pub(crate) robots_cache_seconds: u64,
    pub(crate) extraction_schema: &'static str,
    pub(crate) passage_schema: &'static str,
    pub(crate) max_passage_tokens: usize,
}

impl Config {
    pub(crate) fn origin_policy(&self, origin: &str) -> OriginRequestPolicy {
        self.origin_policies
            .iter()
            .find(|policy| policy.origin == origin)
            .cloned()
            .unwrap_or_else(|| OriginRequestPolicy {
                origin: origin.to_owned(),
                origin_concurrency: self.origin_concurrency,
                min_start_spacing_ms: self.min_start_spacing_ms,
            })
    }

    pub(crate) fn policy(&self) -> CrawlPolicy {
        CrawlPolicy {
            schema_version: self.schema_version.clone(),
            contact_url: self.contact_url.clone(),
            embedding_model: self.embedding_model.clone(),
            global_concurrency: self.global_concurrency,
            origin_concurrency: self.origin_concurrency,
            min_start_spacing_ms: self.min_start_spacing_ms,
            origin_policies: self.origin_policies.clone(),
            max_frontier: self.max_frontier,
            max_decoded_body_bytes: self.max_body_bytes,
            max_extraction_bytes: self.max_extraction_bytes,
            max_extraction_millis: self.max_extraction_millis,
            request_timeout_seconds: self.timeout_seconds,
            max_attempts: self.max_attempts,
            retry_backoff_ms: self.retry_backoff_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            max_total_attempts: self.max_total_attempts,
            max_duration_seconds: self.max_duration_seconds,
            robots_cache_seconds: self.robots_cache_seconds,
            extraction_schema: EXTRACTION_SCHEMA,
            passage_schema: PASSAGE_SCHEMA,
            max_passage_tokens: self.max_passage_tokens,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct CanonicalConfig<'a> {
    pub(crate) schema_version: &'a str,
    pub(crate) contact_url: &'a str,
    pub(crate) user_agent: &'a str,
    pub(crate) embedding_model: &'a str,
    pub(crate) page_target: usize,
    pub(crate) min_content_chars: usize,
    pub(crate) max_body_bytes: usize,
    pub(crate) max_frontier: usize,
    pub(crate) timeout_seconds: u64,
    pub(crate) global_concurrency: usize,
    pub(crate) origin_concurrency: usize,
    pub(crate) min_start_spacing_ms: u64,
    pub(crate) max_attempts: usize,
    pub(crate) retry_backoff_ms: u64,
    pub(crate) retry_max_delay_ms: u64,
    pub(crate) max_total_attempts: usize,
    pub(crate) max_duration_seconds: u64,
    pub(crate) robots_cache_seconds: u64,
    pub(crate) max_extraction_bytes: usize,
    pub(crate) max_extraction_millis: u64,
    pub(crate) max_passage_tokens: usize,
    pub(crate) sources: &'a [Source],
    #[serde(skip_serializing_if = "<[OriginRequestPolicy]>::is_empty")]
    pub(crate) origin_policies: &'a [OriginRequestPolicy],
}

#[derive(Debug, Clone)]
pub(crate) struct Extracted {
    pub(crate) title: String,
    pub(crate) description: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) canonical_url: Option<String>,
    pub(crate) extraction_method: &'static str,
    pub(crate) content: String,
    pub(crate) identity: String,
    pub(crate) content_hash: String,
    pub(crate) blocks: Vec<ContentBlock>,
    pub(crate) links: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum ContentBlock {
    Heading { level: usize, text: String },
    Prose(String),
    Code(String),
}

#[derive(Debug, Clone)]
pub(crate) struct Capture {
    pub(crate) requested_url: String,
    pub(crate) final_url: String,
    pub(crate) redirects: Vec<String>,
    pub(crate) source_id: String,
    pub(crate) title: String,
    pub(crate) description: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) canonical_url: Option<String>,
    pub(crate) extraction_method: &'static str,
    pub(crate) content: String,
    pub(crate) identity: String,
    pub(crate) content_hash: String,
    pub(crate) blocks: Vec<ContentBlock>,
    pub(crate) content_bytes: usize,
    pub(crate) body_hash: String,
    pub(crate) body_file: String,
    pub(crate) content_type: String,
    pub(crate) status_code: u16,
}

#[derive(Debug)]
pub(crate) enum FetchOutcome {
    Admitted {
        source_id: String,
        final_url: String,
        redirects: Vec<String>,
        status_code: u16,
        content_type: String,
        body: Vec<u8>,
        extracted: Box<Extracted>,
    },
    Rejected(String),
}

#[derive(Debug)]
pub(crate) struct CrawlResult {
    pub(crate) captures: Vec<Capture>,
    pub(crate) rejected_count: usize,
    pub(crate) failed_count: usize,
    pub(crate) stop_reason: String,
}
