mod build;
mod checkpoint;
mod checksums;
mod embedding_cache;
mod embeddings;
mod keyword;
mod lifecycle;
mod profile;
mod reader;
mod semantic;
mod semantic_storage;
mod validation;

pub(crate) use build::build_generation;
pub(crate) use checksums::{collect_files, directory_bytes, directory_digest};
pub(crate) use embeddings::QueryEmbedder;
pub(crate) use keyword::register_keyword_analyzer;
pub(crate) use lifecycle::{
    activate_generation, prune_generations, read_pointer, recover_generation, validate_pointer,
};
pub(crate) use reader::{
    ActiveGeneration, IndexGeneration, LifecycleAction, PageQuery, SearchMode,
};
pub(crate) use semantic::open_semantic_index;
pub(crate) use validation::validate_generation;
