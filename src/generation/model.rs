use super::QueryEmbedder;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tantivy::Index;
use usearch::Index as UsearchIndex;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum BuildMode {
    Hybrid,
    SemanticOnly,
    #[default]
    KeywordOnly,
}

impl BuildMode {
    pub(crate) fn has_keyword(self) -> bool {
        !matches!(self, Self::SemanticOnly)
    }

    pub(crate) fn has_semantic(self) -> bool {
        !matches!(self, Self::KeywordOnly)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Hybrid => "hybrid",
            Self::SemanticOnly => "semantic-only",
            Self::KeywordOnly => "keyword-only",
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawBuildProfile {
    pub(crate) schema_version: Option<String>,
    #[serde(alias = "corpus_snapshot_id")]
    pub(crate) snapshot_id: Option<String>,
    #[serde(alias = "extraction_schema_id")]
    pub(crate) extraction_schema: Option<String>,
    #[serde(alias = "passage_schema_id")]
    pub(crate) passage_schema: Option<String>,
    #[serde(alias = "passage_max_tokens", alias = "max_model_tokens")]
    pub(crate) max_passage_tokens: Option<usize>,
    #[serde(alias = "passage_overlap_tokens")]
    pub(crate) overlap_tokens: Option<usize>,
    pub(crate) no_overlap: Option<bool>,
    pub(crate) snippet_limit: Option<usize>,
    pub(crate) analyzer_id: Option<String>,
    pub(crate) title_boost: Option<f64>,
    pub(crate) heading_boost: Option<f64>,
    pub(crate) body_boost: Option<f64>,
    #[serde(alias = "keyword_candidate_depth")]
    pub(crate) candidate_depth: Option<usize>,
    #[serde(alias = "writer_memory_budget_bytes")]
    pub(crate) writer_memory_bytes: Option<usize>,
    #[serde(alias = "workers")]
    pub(crate) worker_count: Option<usize>,
    pub(crate) model_name: Option<String>,
    pub(crate) model_source: Option<String>,
    pub(crate) model_revision: Option<String>,
    pub(crate) model_license: Option<String>,
    pub(crate) onnx_sha256: Option<String>,
    pub(crate) tokenizer_sha256: Option<String>,
    pub(crate) model_config_sha256: Option<String>,
    pub(crate) embedding_backend: Option<String>,
    pub(crate) model_max_tokens: Option<usize>,
    pub(crate) pooling: Option<String>,
    pub(crate) normalization: Option<String>,
    pub(crate) quantization: Option<String>,
    pub(crate) query_instruction: Option<String>,
    pub(crate) document_representation: Option<String>,
    pub(crate) dimensions: Option<usize>,
    pub(crate) embedding_batch_size: Option<usize>,
    pub(crate) embedding_workers: Option<usize>,
    pub(crate) embedding_intra_op_threads: Option<usize>,
    pub(crate) embedding_execution_provider: Option<String>,
    pub(crate) metric: Option<String>,
    pub(crate) scalar_kind: Option<String>,
    pub(crate) connectivity: Option<usize>,
    pub(crate) construction_expansion: Option<usize>,
    pub(crate) search_expansion: Option<usize>,
    pub(crate) key_map: Option<String>,
    pub(crate) build_limit: Option<usize>,
    pub(crate) fusion_candidate_depth: Option<usize>,
    pub(crate) fusion_algorithm: Option<String>,
    pub(crate) rrf_k: Option<usize>,
    pub(crate) page_aggregation: Option<String>,
    pub(crate) snippet_window: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BuildProfile {
    pub(crate) schema_version: String,
    pub(crate) snapshot_id: String,
    #[serde(default)]
    pub(crate) build_mode: BuildMode,
    pub(crate) extraction_schema: String,
    pub(crate) passage_schema: String,
    pub(crate) max_passage_tokens: usize,
    pub(crate) overlap_tokens: usize,
    pub(crate) no_overlap: bool,
    pub(crate) snippet_limit: usize,
    pub(crate) analyzer_id: String,
    pub(crate) title_boost: f64,
    pub(crate) heading_boost: f64,
    pub(crate) body_boost: f64,
    pub(crate) candidate_depth: usize,
    pub(crate) writer_memory_bytes: usize,
    pub(crate) worker_count: usize,
    pub(crate) model_name: Option<String>,
    pub(crate) model_source: Option<String>,
    pub(crate) model_revision: Option<String>,
    pub(crate) model_license: Option<String>,
    pub(crate) onnx_sha256: Option<String>,
    pub(crate) tokenizer_sha256: Option<String>,
    pub(crate) model_config_sha256: Option<String>,
    pub(crate) embedding_backend: String,
    pub(crate) model_max_tokens: Option<usize>,
    pub(crate) pooling: Option<String>,
    pub(crate) normalization: Option<String>,
    pub(crate) quantization: Option<String>,
    pub(crate) query_instruction: Option<String>,
    pub(crate) document_representation: Option<String>,
    pub(crate) dimensions: Option<usize>,
    pub(crate) embedding_batch_size: Option<usize>,
    pub(crate) embedding_workers: Option<usize>,
    pub(crate) embedding_intra_op_threads: Option<usize>,
    pub(crate) embedding_execution_provider: Option<String>,
    pub(crate) metric: Option<String>,
    pub(crate) scalar_kind: Option<String>,
    pub(crate) connectivity: Option<usize>,
    pub(crate) construction_expansion: Option<usize>,
    pub(crate) search_expansion: Option<usize>,
    pub(crate) key_map: Option<String>,
    pub(crate) build_limit: Option<usize>,
    pub(crate) fusion_candidate_depth: Option<usize>,
    pub(crate) fusion_algorithm: Option<String>,
    pub(crate) rrf_k: Option<usize>,
    pub(crate) page_aggregation: Option<String>,
    pub(crate) snippet_window: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CatalogRecord {
    pub(crate) page_id: String,
    pub(crate) passage_id: String,
    pub(crate) source_id: String,
    pub(crate) normalized_final_url: String,
    pub(crate) title: String,
    pub(crate) heading_path: Vec<String>,
    pub(crate) passage_ordinal: usize,
    pub(crate) text: String,
    pub(crate) display_text: String,
    pub(crate) snippet: String,
    pub(crate) alias_page_ids: Vec<String>,
    pub(crate) vector_key: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GenerationComponents {
    pub(crate) keyword: bool,
    pub(crate) semantic: bool,
}

impl GenerationComponents {
    pub(crate) fn for_mode(mode: BuildMode) -> Self {
        Self {
            keyword: mode.has_keyword(),
            semantic: mode.has_semantic(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GenerationManifest {
    pub(crate) schema_version: String,
    pub(crate) generation_id: String,
    pub(crate) status: String,
    pub(crate) build_run_id: String,
    pub(crate) corpus_snapshot_id: String,
    pub(crate) corpus_snapshot_sha256: String,
    pub(crate) crawl_manifest_sha256: String,
    pub(crate) source_config_sha256: String,
    pub(crate) extraction_schema: String,
    pub(crate) passage_schema: String,
    pub(crate) build_config_sha256: String,
    pub(crate) catalog_sha256: String,
    pub(crate) catalog_file: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) keyword_artifact: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) keyword_sha256: Option<String>,
    pub(crate) keyword_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) semantic_artifact: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) semantic_sha256: Option<String>,
    pub(crate) semantic_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) semantic: Option<SemanticEvidence>,
    pub(crate) checksums_file: String,
    pub(crate) page_count: usize,
    pub(crate) passage_count: usize,
    pub(crate) alias_count: usize,
    pub(crate) vector_key_count: usize,
    pub(crate) vector_key_map: String,
    pub(crate) created_at_utc: String,
    pub(crate) manifest_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) components: Option<GenerationComponents>,
}

impl GenerationManifest {
    pub(crate) fn components(&self) -> GenerationComponents {
        self.components.clone().unwrap_or(GenerationComponents {
            keyword: true,
            semantic: true,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GenerationPointer {
    pub(crate) generation_id: String,
    pub(crate) manifest_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SemanticHit {
    pub(crate) vector_key: u64,
    pub(crate) page_id: String,
    pub(crate) passage_id: String,
    pub(crate) score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SemanticStageTimings {
    pub(crate) model_initialization_ms: u128,
    pub(crate) passage_embedding_ms: u128,
    pub(crate) passage_embedding_calls: usize,
    pub(crate) query_embedding_ms: u128,
    pub(crate) vector_write_ms: u128,
    pub(crate) vector_index_insertion_ms: u128,
    pub(crate) vector_flush_ms: u128,
    pub(crate) vector_validation_ms: u128,
    pub(crate) index_persistence_ms: u128,
    pub(crate) index_reopen_validation_ms: u128,
    pub(crate) smoke_search_ms: u128,
    pub(crate) other_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SemanticEvidence {
    pub(crate) schema_version: String,
    pub(crate) backend: String,
    pub(crate) artifact: String,
    pub(crate) reopen_mode: String,
    pub(crate) model_name: Option<String>,
    pub(crate) model_source: Option<String>,
    pub(crate) model_revision: Option<String>,
    pub(crate) model_license: Option<String>,
    pub(crate) onnx_sha256: Option<String>,
    pub(crate) tokenizer_sha256: Option<String>,
    pub(crate) model_config_sha256: Option<String>,
    pub(crate) embedding_backend: String,
    pub(crate) model_max_tokens: Option<usize>,
    pub(crate) pooling: Option<String>,
    pub(crate) normalization: Option<String>,
    pub(crate) query_instruction: Option<String>,
    pub(crate) document_representation: Option<String>,
    pub(crate) dimensions: usize,
    pub(crate) metric: Option<String>,
    pub(crate) scalar_kind: Option<String>,
    pub(crate) embedding_batch_size: Option<usize>,
    pub(crate) embedding_workers: Option<usize>,
    pub(crate) embedding_intra_op_threads: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) embedding_execution_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) embedding_fingerprint: Option<String>,
    pub(crate) connectivity: Option<usize>,
    pub(crate) construction_expansion: Option<usize>,
    pub(crate) search_expansion: Option<usize>,
    pub(crate) key_map: Option<String>,
    pub(crate) catalog_count: usize,
    pub(crate) vector_count: usize,
    pub(crate) candidate_depth: usize,
    pub(crate) query: String,
    pub(crate) query_hit_count: usize,
    pub(crate) returned_identities: Vec<SemanticHit>,
    pub(crate) artifact_bytes: u64,
    pub(crate) artifact_sha256: String,
    pub(crate) build_duration_ms: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) stage_timings: Option<SemanticStageTimings>,
    pub(crate) embedding_passages_per_second: f64,
    pub(crate) memory_observation: String,
    pub(crate) reopened: bool,
}

#[derive(Debug)]
pub(crate) struct StoredSemanticIndex {
    pub(crate) dimensions: usize,
    pub(crate) vectors: Vec<(u64, Vec<f32>)>,
}

pub(crate) struct LoadedGeneration {
    pub(crate) manifest: GenerationManifest,
    pub(crate) profile: BuildProfile,
    pub(crate) catalog: Vec<CatalogRecord>,
    pub(crate) semantic_dimensions: Option<usize>,
    pub(crate) keyword_index: Option<Index>,
    pub(crate) semantic_index: Option<UsearchIndex>,
    pub(crate) model_cache_dir: PathBuf,
    pub(crate) query_embedder: QueryEmbedder,
}
