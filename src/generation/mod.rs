mod build;
mod checkpoint;
mod checksums;
mod embedding_cache;
mod embeddings;
mod keyword;
mod lifecycle;
mod model;
mod profile;
mod reader;
mod search;
mod semantic;
mod semantic_storage;
mod validation;

pub(crate) const MAX_MODEL_PASSAGE_TOKENS: usize = 512;
pub(crate) const DEFAULT_EMBEDDING_MODEL: &str = "BAAI/bge-small-en-v1.5";
pub(crate) const MINILM_EMBEDDING_MODEL: &str = "sentence-transformers/all-MiniLM-L6-v2";
const SEMANTIC_ARTIFACT: &str = "semantic.usearch";
const SEMANTIC_VECTOR_ARTIFACT: &str = "semantic-vectors.f32";
const SEMANTIC_MAGIC: &[u8] = b"SCOUT-SEMANTIC-F32-COSINE-V1\0";

pub(crate) use model::{BuildMode, GenerationManifest};
use model::{
    BuildProfile, CatalogRecord, GenerationComponents, GenerationPointer, LoadedGeneration,
    RawBuildProfile, SemanticEvidence, SemanticHit, SemanticStageTimings, StoredSemanticIndex,
};

use build::build_generation;
pub(crate) use checksums::{collect_files, directory_bytes, directory_digest};
use embeddings::QueryEmbedder;
use keyword::register_keyword_analyzer;
pub(crate) use lifecycle::read_pointer;
use lifecycle::{activate_generation, prune_generations, recover_generation, validate_pointer};
pub(crate) use reader::{
    ActiveGeneration, IndexGeneration, LifecycleAction, PageQuery, SearchMode,
};
use semantic::open_semantic_index;
pub(crate) use validation::validate_generation;
