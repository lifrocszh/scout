use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ::url::Url;
use chrono::{SecondsFormat, Utc};
use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION, RETRY_AFTER};
use reqwest::redirect::Policy;
use scraper::{ElementRef, Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{
    IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions,
    Value as TantivyValue,
};
use tantivy::{Index, TantivyDocument};
use usearch::{Index as UsearchIndex, IndexOptions as UsearchIndexOptions, MetricKind, ScalarKind};

const CRAWL_SCHEMA: &str = "scout.crawl-manifest.v1";
const SNAPSHOT_SCHEMA: &str = "scout.corpus-snapshot.v1";
const EVENT_SCHEMA: &str = "scout.operational-event.v1";
const EXTRACTION_SCHEMA: &str = "scout.extraction.v1";
const PASSAGE_SCHEMA: &str = "scout.passage.v1";
const DEFAULT_BODY_LIMIT: usize = 1024 * 1024;
const DEFAULT_CONTENT_MINIMUM: usize = 200;
const DEFAULT_FRONTIER_LIMIT: usize = 10_000;
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_PASSAGE_MAX_TOKENS: usize = 384;
const MAX_MODEL_PASSAGE_TOKENS: usize = 512;
const MAX_REDIRECTS: usize = 10;
const MAX_ROBOTS_BYTES: usize = 512 * 1024;
const SEMANTIC_ARTIFACT: &str = "semantic.usearch";
const SEMANTIC_VECTOR_ARTIFACT: &str = "semantic-vectors.f32";
const SEMANTIC_MAGIC: &[u8] = b"SCOUT-SEMANTIC-F32-COSINE-V1\0";
const EVALUATION_SCHEMA: &str = "scout.evaluation.v1";
const EVALUATION_QUERY_COUNT: usize = 64;
const EVALUATION_DEVELOPMENT_QUERY_COUNT: usize = 16;
const EVALUATION_HELD_OUT_QUERY_COUNT: usize = 48;
const EVALUATION_BOOTSTRAP_ITERATIONS: usize = 10_000;
const EVALUATION_BOOTSTRAP_SEED: u64 = 0x5343_4f55_545f_4556;
const EVALUATION_SOURCES: [&str; 4] = [
    "rust-docs",
    "python-docs",
    "kubernetes-docs",
    "postgres-docs",
];
const EVALUATION_INTENTS: [&str; 4] = ["exact-api", "conceptual", "how-to", "semantic-paraphrase"];
const BENCHMARK_SCHEMA: &str = "scout.benchmark.v1";
const BENCHMARK_DEFAULT_REPETITIONS: usize = 3;
const BENCHMARK_DEFAULT_COLD_OPEN_RUNS: usize = 5;
const BENCHMARK_DEFAULT_WARMUP_PASSES: usize = 2;
const BENCHMARK_DEFAULT_MEASURED_PASSES: usize = 10;
const BENCHMARK_DEFAULT_CONCURRENCY: [usize; 4] = [1, 4, 8, 16];
const BENCHMARK_DEFAULT_INDEX_WORKERS: [usize; 4] = [1, 4, 8, 18];
const BENCHMARK_INSTABILITY_LIMIT: f64 = 0.15;

#[derive(Debug)]
struct Cli {
    operation: Operation,
    data_dir: PathBuf,
    json_errors: bool,
}

#[derive(Debug)]
enum Operation {
    Crawl {
        config: PathBuf,
    },
    IndexBuild {
        corpus: String,
    },
    IndexActivate {
        generation: String,
    },
    IndexRecover,
    IndexVerify,
    IndexPrune {
        retain: Vec<String>,
    },
    Serve {
        bind: String,
        access_log: Option<PathBuf>,
    },
    Evaluate {
        package: PathBuf,
    },
    Benchmark {
        config: PathBuf,
    },
}

#[derive(Debug, Clone)]
struct Error {
    code: String,
    message: String,
    exit_code: i32,
}

impl Error {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_configuration".into(),
            message: message.into(),
            exit_code: 2,
        }
    }

    fn fetch(message: impl Into<String>) -> Self {
        Self {
            code: "fetch_failed".into(),
            message: message.into(),
            exit_code: 1,
        }
    }

    fn storage(message: impl Into<String>) -> Self {
        Self {
            code: "storage_failed".into(),
            message: message.into(),
            exit_code: 1,
        }
    }

    fn safety(message: impl Into<String>) -> Self {
        Self {
            code: "safety_cap".into(),
            message: message.into(),
            exit_code: 1,
        }
    }

    fn not_ready(message: impl Into<String>) -> Self {
        Self {
            code: "not_ready".into(),
            message: message.into(),
            exit_code: 3,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    schema_version: Option<String>,
    contact_url: Option<String>,
    user_agent: Option<String>,
    page_target: Option<usize>,
    min_content_chars: Option<usize>,
    max_decoded_body_bytes: Option<usize>,
    max_frontier: Option<usize>,
    request_timeout_seconds: Option<u64>,
    global_concurrency: Option<usize>,
    origin_concurrency: Option<usize>,
    min_start_spacing_ms: Option<u64>,
    max_attempts: Option<usize>,
    retry_backoff_ms: Option<u64>,
    retry_max_delay_ms: Option<u64>,
    max_total_attempts: Option<usize>,
    max_duration_seconds: Option<u64>,
    robots_cache_seconds: Option<u64>,
    max_extraction_bytes: Option<usize>,
    max_extraction_millis: Option<u64>,
    #[serde(alias = "passage_max_tokens", alias = "max_model_tokens")]
    max_passage_tokens: Option<usize>,
    #[serde(default)]
    sources: Vec<RawSource>,
    crawl: Option<RawProfile>,
}

#[derive(Debug, Default, Deserialize)]
struct RawProfile {
    user_agent: Option<String>,
    page_target: Option<usize>,
    min_content_chars: Option<usize>,
    max_decoded_body_bytes: Option<usize>,
    max_frontier: Option<usize>,
    request_timeout_seconds: Option<u64>,
    global_concurrency: Option<usize>,
    origin_concurrency: Option<usize>,
    min_start_spacing_ms: Option<u64>,
    max_attempts: Option<usize>,
    retry_backoff_ms: Option<u64>,
    retry_max_delay_ms: Option<u64>,
    max_total_attempts: Option<usize>,
    max_duration_seconds: Option<u64>,
    robots_cache_seconds: Option<u64>,
    max_extraction_bytes: Option<usize>,
    max_extraction_millis: Option<u64>,
    #[serde(alias = "passage_max_tokens", alias = "max_model_tokens")]
    max_passage_tokens: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RawSource {
    source_id: Option<String>,
    name: Option<String>,
    seeds: Option<Vec<String>>,
    #[serde(alias = "origins")]
    allowed_origins: Option<Vec<String>>,
    #[serde(alias = "allowed_path_prefixes", alias = "paths")]
    path_prefixes: Option<Vec<String>>,
    #[serde(default, alias = "deny_paths")]
    deny_path_prefixes: Vec<String>,
    politeness_group: Option<String>,
    license_urls: Option<Vec<String>>,
    #[serde(alias = "min_pages", alias = "minimum_pages")]
    minimum_page_quota: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct Source {
    source_id: String,
    name: Option<String>,
    seeds: Vec<String>,
    allowed_origins: Vec<String>,
    path_prefixes: Vec<String>,
    deny_path_prefixes: Vec<String>,
    politeness_group: Option<String>,
    license_urls: Vec<String>,
    minimum_page_quota: usize,
}

#[derive(Debug, Clone)]
struct Config {
    schema_version: String,
    contact_url: String,
    user_agent: String,
    page_target: usize,
    min_content_chars: usize,
    max_body_bytes: usize,
    max_frontier: usize,
    timeout_seconds: u64,
    global_concurrency: usize,
    origin_concurrency: usize,
    min_start_spacing_ms: u64,
    max_attempts: usize,
    retry_backoff_ms: u64,
    retry_max_delay_ms: u64,
    max_total_attempts: usize,
    max_duration_seconds: u64,
    robots_cache_seconds: u64,
    max_extraction_bytes: usize,
    max_extraction_millis: u64,
    max_passage_tokens: usize,
    sources: Vec<Source>,
    digest: String,
}

#[derive(Debug, Serialize)]
struct CrawlPolicy {
    schema_version: String,
    contact_url: String,
    global_concurrency: usize,
    origin_concurrency: usize,
    min_start_spacing_ms: u64,
    max_frontier: usize,
    max_decoded_body_bytes: usize,
    max_extraction_bytes: usize,
    max_extraction_millis: u64,
    request_timeout_seconds: u64,
    max_attempts: usize,
    retry_backoff_ms: u64,
    retry_max_delay_ms: u64,
    max_total_attempts: usize,
    max_duration_seconds: u64,
    robots_cache_seconds: u64,
    extraction_schema: &'static str,
    passage_schema: &'static str,
    max_passage_tokens: usize,
}

impl Config {
    fn policy(&self) -> CrawlPolicy {
        CrawlPolicy {
            schema_version: self.schema_version.clone(),
            contact_url: self.contact_url.clone(),
            global_concurrency: self.global_concurrency,
            origin_concurrency: self.origin_concurrency,
            min_start_spacing_ms: self.min_start_spacing_ms,
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
struct CanonicalConfig<'a> {
    schema_version: &'a str,
    contact_url: &'a str,
    user_agent: &'a str,
    page_target: usize,
    min_content_chars: usize,
    max_body_bytes: usize,
    max_frontier: usize,
    timeout_seconds: u64,
    global_concurrency: usize,
    origin_concurrency: usize,
    min_start_spacing_ms: u64,
    max_attempts: usize,
    retry_backoff_ms: u64,
    retry_max_delay_ms: u64,
    max_total_attempts: usize,
    max_duration_seconds: u64,
    robots_cache_seconds: u64,
    max_extraction_bytes: usize,
    max_extraction_millis: u64,
    max_passage_tokens: usize,
    sources: &'a [Source],
}

#[derive(Debug, Clone)]
struct Extracted {
    title: String,
    description: Option<String>,
    language: Option<String>,
    canonical_url: Option<String>,
    extraction_method: &'static str,
    content: String,
    identity: String,
    content_hash: String,
    blocks: Vec<ContentBlock>,
    links: Vec<String>,
}

#[derive(Debug, Clone)]
enum ContentBlock {
    Heading { level: usize, text: String },
    Prose(String),
    Code(String),
}

#[derive(Debug, Clone)]
struct Capture {
    requested_url: String,
    final_url: String,
    redirects: Vec<String>,
    source_id: String,
    title: String,
    description: Option<String>,
    language: Option<String>,
    canonical_url: Option<String>,
    extraction_method: &'static str,
    content: String,
    identity: String,
    content_hash: String,
    blocks: Vec<ContentBlock>,
    content_bytes: usize,
    body_hash: String,
    body_file: String,
    content_type: String,
    status_code: u16,
}

#[derive(Debug)]
enum FetchOutcome {
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

#[derive(Debug, Clone, Serialize)]
struct ManifestPage {
    requested_url: String,
    final_url: String,
    redirect_chain: Vec<String>,
    source_id: String,
    page_id: String,
    admission_status: String,
    capture_id: String,
    selection_key: String,
    selected_capture_id: String,
    selection_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    alias_of: Option<String>,
    title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    canonical_url: Option<String>,
    extraction_method: &'static str,
    content_type: String,
    content_bytes: usize,
    body_file: String,
    body_sha256: String,
    content_sha256: String,
    status_code: u16,
}

#[derive(Debug, Clone, Serialize)]
struct PageRecord {
    page_id: String,
    source_id: String,
    normalized_final_url: String,
    title: String,
    description: Option<String>,
    language: Option<String>,
    canonical_url: Option<String>,
    extraction_method: &'static str,
    content: String,
    content_identity: String,
    content_sha256: String,
    body_sha256: String,
    body_file: String,
}

#[derive(Debug, Clone, Serialize)]
struct PassageRecord {
    page_id: String,
    passage_id: String,
    source_id: String,
    normalized_final_url: String,
    title: String,
    heading_path: Vec<String>,
    ordinal: usize,
    content: String,
    content_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct AliasRecord {
    source_id: String,
    alias_url: String,
    alias_page_id: String,
    representative_page_id: String,
    alias_kind: &'static str,
    requested_url: String,
    redirect_chain: Vec<String>,
    canonical_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct Summary {
    captured_page_count: usize,
    page_count: usize,
    alias_count: usize,
    passage_count: usize,
    rejected_count: usize,
    failed_count: usize,
    target_page_count: usize,
    stop_reason: String,
}

#[derive(Debug, Serialize)]
struct Manifest {
    schema_version: &'static str,
    run_id: String,
    status: &'static str,
    started_at_utc: String,
    completed_at_utc: String,
    source_config_sha256: String,
    manifest_sha256: String,
    snapshot_id: Option<String>,
    capture_selection_policy: &'static str,
    user_agent: String,
    robots_user_agent: &'static str,
    policy: CrawlPolicy,
    sources: Vec<Source>,
    pages: Vec<ManifestPage>,
    summary: Summary,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ManifestError>,
}

#[derive(Debug, Serialize)]
struct ManifestError {
    code: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct SnapshotManifest {
    schema_version: &'static str,
    snapshot_id: String,
    status: &'static str,
    run_id: String,
    crawl_manifest_sha256: String,
    source_config_sha256: String,
    created_at_utc: String,
    page_count: usize,
    alias_count: usize,
    pages_jsonl_sha256: String,
    aliases_jsonl_sha256: String,
    passage_count: usize,
    passages_jsonl_sha256: String,
}

// These records mirror the on-disk schema. Some fields are intentionally only
// read by external tools or future migration code.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct StoredSnapshotManifest {
    schema_version: String,
    snapshot_id: String,
    status: String,
    run_id: String,
    crawl_manifest_sha256: String,
    source_config_sha256: String,
    page_count: usize,
    alias_count: usize,
    pages_jsonl_sha256: String,
    aliases_jsonl_sha256: String,
    passage_count: usize,
    passages_jsonl_sha256: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct StoredPage {
    page_id: String,
    source_id: String,
    normalized_final_url: String,
    title: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    canonical_url: Option<String>,
    extraction_method: String,
    content: String,
    content_identity: String,
    content_sha256: String,
    body_sha256: String,
    body_file: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct StoredPassage {
    page_id: String,
    passage_id: String,
    source_id: String,
    normalized_final_url: String,
    title: String,
    heading_path: Vec<String>,
    ordinal: usize,
    content: String,
    content_sha256: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct StoredAlias {
    source_id: String,
    alias_url: String,
    alias_page_id: String,
    representative_page_id: String,
    alias_kind: String,
    #[serde(default)]
    requested_url: String,
    #[serde(default)]
    redirect_chain: Vec<String>,
    #[serde(default)]
    canonical_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawBuildProfile {
    schema_version: Option<String>,
    #[serde(alias = "corpus_snapshot_id")]
    snapshot_id: Option<String>,
    #[serde(alias = "extraction_schema_id")]
    extraction_schema: Option<String>,
    #[serde(alias = "passage_schema_id")]
    passage_schema: Option<String>,
    #[serde(alias = "passage_max_tokens", alias = "max_model_tokens")]
    max_passage_tokens: Option<usize>,
    #[serde(alias = "passage_overlap_tokens")]
    overlap_tokens: Option<usize>,
    no_overlap: Option<bool>,
    snippet_limit: Option<usize>,
    analyzer_id: Option<String>,
    title_boost: Option<f64>,
    heading_boost: Option<f64>,
    body_boost: Option<f64>,
    #[serde(alias = "keyword_candidate_depth")]
    candidate_depth: Option<usize>,
    #[serde(alias = "writer_memory_budget_bytes")]
    writer_memory_bytes: Option<usize>,
    #[serde(alias = "workers")]
    worker_count: Option<usize>,
    model_name: Option<String>,
    model_source: Option<String>,
    model_revision: Option<String>,
    model_license: Option<String>,
    onnx_sha256: Option<String>,
    tokenizer_sha256: Option<String>,
    model_config_sha256: Option<String>,
    embedding_backend: Option<String>,
    model_max_tokens: Option<usize>,
    pooling: Option<String>,
    normalization: Option<String>,
    quantization: Option<String>,
    query_instruction: Option<String>,
    document_representation: Option<String>,
    dimensions: Option<usize>,
    embedding_batch_size: Option<usize>,
    embedding_workers: Option<usize>,
    embedding_intra_op_threads: Option<usize>,
    metric: Option<String>,
    scalar_kind: Option<String>,
    connectivity: Option<usize>,
    construction_expansion: Option<usize>,
    search_expansion: Option<usize>,
    key_map: Option<String>,
    build_limit: Option<usize>,
    fusion_candidate_depth: Option<usize>,
    fusion_algorithm: Option<String>,
    rrf_k: Option<usize>,
    page_aggregation: Option<String>,
    snippet_window: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BuildProfile {
    schema_version: String,
    snapshot_id: String,
    extraction_schema: String,
    passage_schema: String,
    max_passage_tokens: usize,
    overlap_tokens: usize,
    no_overlap: bool,
    snippet_limit: usize,
    analyzer_id: String,
    title_boost: f64,
    heading_boost: f64,
    body_boost: f64,
    candidate_depth: usize,
    writer_memory_bytes: usize,
    worker_count: usize,
    model_name: Option<String>,
    model_source: Option<String>,
    model_revision: Option<String>,
    model_license: Option<String>,
    onnx_sha256: Option<String>,
    tokenizer_sha256: Option<String>,
    model_config_sha256: Option<String>,
    embedding_backend: String,
    model_max_tokens: Option<usize>,
    pooling: Option<String>,
    normalization: Option<String>,
    quantization: Option<String>,
    query_instruction: Option<String>,
    document_representation: Option<String>,
    dimensions: Option<usize>,
    embedding_batch_size: Option<usize>,
    embedding_workers: Option<usize>,
    embedding_intra_op_threads: Option<usize>,
    metric: Option<String>,
    scalar_kind: Option<String>,
    connectivity: Option<usize>,
    construction_expansion: Option<usize>,
    search_expansion: Option<usize>,
    key_map: Option<String>,
    build_limit: Option<usize>,
    fusion_candidate_depth: Option<usize>,
    fusion_algorithm: Option<String>,
    rrf_k: Option<usize>,
    page_aggregation: Option<String>,
    snippet_window: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CatalogRecord {
    page_id: String,
    passage_id: String,
    source_id: String,
    normalized_final_url: String,
    title: String,
    heading_path: Vec<String>,
    passage_ordinal: usize,
    text: String,
    display_text: String,
    snippet: String,
    alias_page_ids: Vec<String>,
    vector_key: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GenerationManifest {
    schema_version: String,
    generation_id: String,
    status: String,
    build_run_id: String,
    corpus_snapshot_id: String,
    corpus_snapshot_sha256: String,
    crawl_manifest_sha256: String,
    source_config_sha256: String,
    extraction_schema: String,
    passage_schema: String,
    build_config_sha256: String,
    catalog_sha256: String,
    catalog_file: String,
    keyword_artifact: String,
    keyword_sha256: String,
    keyword_count: usize,
    semantic_artifact: String,
    semantic_sha256: String,
    semantic_count: usize,
    semantic: SemanticEvidence,
    checksums_file: String,
    page_count: usize,
    passage_count: usize,
    alias_count: usize,
    vector_key_count: usize,
    vector_key_map: String,
    created_at_utc: String,
    manifest_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GenerationPointer {
    generation_id: String,
    manifest_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SemanticHit {
    vector_key: u64,
    page_id: String,
    passage_id: String,
    score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SemanticEvidence {
    schema_version: String,
    backend: String,
    artifact: String,
    reopen_mode: String,
    model_name: Option<String>,
    model_source: Option<String>,
    model_revision: Option<String>,
    model_license: Option<String>,
    onnx_sha256: Option<String>,
    tokenizer_sha256: Option<String>,
    model_config_sha256: Option<String>,
    embedding_backend: String,
    model_max_tokens: Option<usize>,
    pooling: Option<String>,
    normalization: Option<String>,
    query_instruction: Option<String>,
    document_representation: Option<String>,
    dimensions: usize,
    metric: Option<String>,
    scalar_kind: Option<String>,
    embedding_batch_size: Option<usize>,
    embedding_workers: Option<usize>,
    embedding_intra_op_threads: Option<usize>,
    connectivity: Option<usize>,
    construction_expansion: Option<usize>,
    search_expansion: Option<usize>,
    key_map: Option<String>,
    catalog_count: usize,
    vector_count: usize,
    candidate_depth: usize,
    query: String,
    query_hit_count: usize,
    returned_identities: Vec<SemanticHit>,
    artifact_bytes: u64,
    artifact_sha256: String,
    build_duration_ms: u128,
    embedding_passages_per_second: f64,
    memory_observation: String,
    reopened: bool,
}

#[derive(Debug)]
struct StoredSemanticIndex {
    dimensions: usize,
    vectors: Vec<(u64, Vec<f32>)>,
}

#[derive(Debug)]
struct CrawlResult {
    captures: Vec<Capture>,
    rejected_count: usize,
    failed_count: usize,
    stop_reason: String,
}

#[derive(Debug)]
struct SnapshotResult {
    id: String,
    pages: usize,
    aliases: usize,
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    query: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EvaluationQuery {
    query_id: String,
    query: String,
    source: String,
    intent: String,
    split: String,
    #[serde(default)]
    expected_page_candidates: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct EvaluationJudgment {
    query_id: String,
    page_id: String,
    #[serde(default)]
    grade: Option<u8>,
    #[serde(default)]
    judge_id: Option<String>,
    #[serde(default)]
    adjudicated_grade: Option<u8>,
    #[serde(default)]
    judge_grades: BTreeMap<String, u8>,
}

#[derive(Debug, Deserialize)]
struct EvaluationRanking {
    query_id: String,
    keyword: Vec<String>,
    semantic: Vec<String>,
    hybrid: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
struct QueryMetrics {
    ndcg_at_10: f64,
    mrr_at_10: f64,
    recall_at_100: f64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
struct BootstrapInterval {
    mean: f64,
    lower: f64,
    upper: f64,
    iterations: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BenchmarkReferenceProfile {
    #[serde(default)]
    hardware: Option<String>,
    #[serde(default)]
    cpu_model: Option<String>,
    #[serde(default, alias = "cpu_cache_sizes")]
    cpu_cache: Option<String>,
    #[serde(default)]
    logical_cpus: Option<usize>,
    #[serde(default)]
    ram_bytes: Option<u64>,
    #[serde(default)]
    swap_bytes: Option<u64>,
    #[serde(default)]
    os: Option<String>,
    #[serde(default)]
    kernel: Option<String>,
    #[serde(default)]
    rust_toolchain: Option<String>,
    #[serde(default)]
    lockfile_sha256: Option<String>,
    #[serde(default)]
    filesystem: Option<String>,
    #[serde(default)]
    free_disk_bytes: Option<u64>,
    #[serde(default)]
    background_load: Option<String>,
    #[serde(default)]
    source_revision: Option<String>,
    #[serde(default)]
    model_name: Option<String>,
    #[serde(default)]
    model_revision: Option<String>,
    #[serde(default)]
    tokenizer_sha256: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawBenchmarkConfig {
    schema_version: Option<String>,
    benchmark_id: Option<String>,
    reference_profile: Option<String>,
    #[serde(default)]
    reference: Option<BenchmarkReferenceProfile>,
    corpus_snapshot_id: Option<String>,
    generation_id: Option<String>,
    build_id: Option<String>,
    evaluation_id: Option<String>,
    evaluation_package: Option<PathBuf>,
    seed: Option<u64>,
    query_order: Option<String>,
    cache_state: Option<String>,
    repetitions: Option<usize>,
    fresh_staging_runs: Option<usize>,
    cold_open_runs: Option<usize>,
    warmup_passes: Option<usize>,
    measured_passes: Option<usize>,
    #[serde(alias = "search_concurrency")]
    concurrency: Option<Vec<usize>>,
    index_workers: Option<Vec<usize>>,
    #[serde(alias = "replay_crawl")]
    replay_crawl_dir: Option<PathBuf>,
    runlens_metadata: Option<PathBuf>,
    #[serde(alias = "sampling_interval_ms")]
    resource_sampling_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkConfig {
    schema_version: String,
    benchmark_id: String,
    reference_profile: String,
    reference: BenchmarkReferenceProfile,
    corpus_snapshot_id: String,
    generation_id: String,
    build_id: String,
    evaluation_id: String,
    evaluation_package: PathBuf,
    seed: u64,
    query_order: String,
    cache_state: String,
    repetitions: usize,
    fresh_staging_runs: usize,
    cold_open_runs: usize,
    warmup_passes: usize,
    measured_passes: usize,
    concurrency: Vec<usize>,
    index_workers: Vec<usize>,
    replay_crawl_dir: Option<PathBuf>,
    runlens_metadata: Option<PathBuf>,
    resource_sampling_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkPlan {
    schema_version: &'static str,
    status: &'static str,
    run_id: String,
    benchmark_id: String,
    config_sha256: String,
    reference_profile: String,
    reference: BenchmarkReferenceProfile,
    corpus_snapshot_id: String,
    corpus_snapshot_sha256: String,
    generation_id: String,
    generation_manifest_sha256: String,
    build_id: String,
    build_profile_sha256: String,
    evaluation_id: String,
    evaluation_package: String,
    evaluation_package_sha256: String,
    evaluation_metadata_sha256: Option<String>,
    evaluation_queries_sha256: String,
    evaluation_judgments_sha256: Option<String>,
    query_count: usize,
    query_ids: Vec<String>,
    seed: u64,
    query_order: String,
    cache_state: String,
    repetitions: usize,
    fresh_staging_runs: usize,
    cold_open_runs: usize,
    warmup_passes: usize,
    measured_passes: usize,
    concurrency: Vec<usize>,
    index_workers: Vec<usize>,
    replay_crawl: Value,
    runlens_metadata: Option<String>,
    resource_sampling_interval_ms: u64,
    performance_thresholds: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkDistribution {
    sample_count: usize,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    median_spread: f64,
    unstable: bool,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkSample {
    phase: String,
    run: usize,
    pass: Option<usize>,
    mode: Option<String>,
    concurrency: Option<usize>,
    worker_count: Option<usize>,
    cache_state: String,
    duration_ms: f64,
    query_count: usize,
    qps: Option<f64>,
    latency_ms: Option<BenchmarkDistribution>,
    integrity_verified: bool,
    artifact_path: Option<String>,
    status: String,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkSystemEvidence {
    benchmark_run_id: String,
    process_id: u32,
    swap_in_before: Option<u64>,
    swap_in_after: Option<u64>,
    swap_out_before: Option<u64>,
    swap_out_after: Option<u64>,
    swap_activity_detected: bool,
    oom_observed: Option<bool>,
    partial_artifacts: bool,
    integrity_verified: bool,
    page_cache_state: String,
    runlens_metadata_path: Option<String>,
    runlens_metadata_sha256: Option<String>,
    runlens_metadata: Value,
    resource_samples_path: String,
    resource_boundaries_path: String,
    resource_summary_path: String,
    resource_sampler: Value,
}

struct LoadedGeneration {
    manifest: GenerationManifest,
    profile: BuildProfile,
    catalog: Vec<CatalogRecord>,
    semantic: StoredSemanticIndex,
    keyword_index: Index,
    semantic_index: UsearchIndex,
    model_cache_dir: PathBuf,
}

#[derive(Debug, Serialize)]
struct PageSearchResult {
    rank: usize,
    page_id: String,
    source_id: String,
    url: String,
    title: String,
    score: f32,
    winning_passage_id: String,
    heading_path: Vec<String>,
    snippet: String,
}

mod app;
mod benchmark;
mod cli;
mod config;
mod corpus;
mod crawl;
mod evaluation;
mod extract;
mod generation;
mod http;
mod observability;
mod search;
mod storage;
mod url;

pub use app::run;

#[cfg(test)]
mod tests {
    use super::benchmark::{benchmark_distribution, benchmark_query_order};
    use super::crawl::page_id;
    use super::evaluation::{
        compute_query_metrics, normalize_evaluation_judgments, paired_bootstrap_ci,
        validate_evaluation_queries,
    };
    use super::*;

    #[test]
    fn page_id_has_stable_domain_separator() {
        assert_eq!(
            page_id("https://example.invalid/docs"),
            "cba82a3ed648ebc4de37b61ae6cdbe866591385b3ed945405fe81c0e365804ff"
        );
    }

    #[test]
    fn source_allowlist_preserves_path_case() {
        let source = Source {
            source_id: "fixture".into(),
            name: None,
            seeds: vec![],
            allowed_origins: vec!["https://example.invalid".into()],
            path_prefixes: vec!["/Docs/".into()],
            deny_path_prefixes: vec!["/Docs/private/".into()],
            politeness_group: None,
            license_urls: vec![],
            minimum_page_quota: 0,
        };
        assert!(source.owns("https://example.invalid/Docs/Guide"));
        assert!(!source.owns("https://example.invalid/docs/Guide"));
        assert!(!source.owns("https://example.invalid/Docs/private/Guide"));
    }

    #[test]
    fn evaluation_metrics_use_page_grades_at_fixed_cutoffs() {
        let ranking = ["p1", "p2", "p3", "p4"];
        let judgments = BTreeMap::from([
            ("p1".into(), 3_u8),
            ("p2".into(), 0_u8),
            ("p3".into(), 2_u8),
            ("p4".into(), 1_u8),
        ]);

        let metrics = compute_query_metrics(&ranking, &judgments).expect("metrics");

        assert!((metrics.ndcg_at_10 - 0.9508013338940989).abs() < 1e-12);
        assert_eq!(metrics.mrr_at_10, 1.0);
        assert_eq!(metrics.recall_at_100, 1.0);
    }

    #[test]
    fn evaluation_query_validation_requires_frozen_64_shape() {
        let queries = evaluation_test_queries();
        validate_evaluation_queries(&queries).expect("valid frozen package");

        let mut incomplete = queries;
        incomplete.pop();
        let error = validate_evaluation_queries(&incomplete).expect_err("missing query");
        assert!(error.message.contains("exactly 64"));
    }

    #[test]
    fn paired_bootstrap_is_deterministic() {
        let deltas = [0.0, 0.1, 0.2, -0.1, 0.3];
        let first = paired_bootstrap_ci(&deltas, 7, 2_000);
        let second = paired_bootstrap_ci(&deltas, 7, 2_000);

        assert_eq!(first, second);
        assert!(first.lower <= first.mean && first.mean <= first.upper);
        assert_eq!(first.iterations, 2_000);
    }

    #[test]
    fn independent_page_judgments_require_adjudication() {
        let query = EvaluationQuery {
            query_id: "q1".into(),
            query: "query".into(),
            source: "rust-docs".into(),
            intent: "exact-api".into(),
            split: "held-out".into(),
            expected_page_candidates: vec!["starter-hint".into()],
        };
        let metadata = json!({
            "evaluation_id": "eval",
            "corpus_snapshot_id": "snapshot",
            "build_id": "build",
            "model_digest": "model",
            "judges": ["judge-a", "judge-b"],
            "adjudication": "independent-then-adjudicated",
        });
        let rows = vec![
            EvaluationJudgment {
                query_id: "q1".into(),
                page_id: "page".into(),
                grade: Some(2),
                judge_id: Some("judge-a".into()),
                adjudicated_grade: None,
                judge_grades: BTreeMap::new(),
            },
            EvaluationJudgment {
                query_id: "q1".into(),
                page_id: "page".into(),
                grade: Some(3),
                judge_id: Some("judge-b".into()),
                adjudicated_grade: None,
                judge_grades: BTreeMap::new(),
            },
            EvaluationJudgment {
                query_id: "q1".into(),
                page_id: "page".into(),
                grade: None,
                judge_id: None,
                adjudicated_grade: Some(3),
                judge_grades: BTreeMap::new(),
            },
        ];
        let (judgments, protocol) =
            normalize_evaluation_judgments(rows, &[query], &metadata).expect("judgments");
        assert_eq!(judgments["q1"]["page"], 3);
        assert_eq!(protocol["format"], "independent-plus-adjudicated");
        assert_eq!(protocol["independent_judgment_rows"], 2);
    }

    #[test]
    fn benchmark_distribution_records_percentiles_and_instability() {
        let distribution = benchmark_distribution(&[1.0, 2.0, 3.0, 4.0, 5.0]);

        assert_eq!(distribution.sample_count, 5);
        assert_eq!(distribution.min_ms, 1.0);
        assert_eq!(distribution.p50_ms, 3.0);
        assert_eq!(distribution.p95_ms, 5.0);
        assert_eq!(distribution.p99_ms, 5.0);
        assert_eq!(distribution.max_ms, 5.0);
        assert!((distribution.median_spread - (4.0 / 3.0)).abs() < 1e-12);
        assert!(distribution.unstable);
    }

    #[test]
    fn benchmark_seeded_query_order_repeats_exactly() {
        let queries = evaluation_test_queries();
        let first = benchmark_query_order(queries.clone(), "seeded", 42).expect("order");
        let second = benchmark_query_order(queries, "seeded", 42).expect("order");

        assert_eq!(first, second);
        assert_ne!(first, evaluation_test_queries());
    }

    fn evaluation_test_queries() -> Vec<EvaluationQuery> {
        EVALUATION_SOURCES
            .iter()
            .flat_map(|source| {
                (0..16).map(move |index| EvaluationQuery {
                    query_id: format!("{}-{index:02}", source),
                    query: format!("query {source} {index}"),
                    source: (*source).into(),
                    intent: EVALUATION_INTENTS[index % EVALUATION_INTENTS.len()].into(),
                    split: if index < 4 {
                        "development".into()
                    } else {
                        "held-out".into()
                    },
                    expected_page_candidates: vec!["starter-hint-only".into()],
                })
            })
            .collect()
    }
}
