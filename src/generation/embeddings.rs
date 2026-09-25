use crate::{BuildProfile, Error, MAX_MODEL_PASSAGE_TOKENS};
use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

const EMBEDDING_CHUNK_MULTIPLIER: usize = 32;

pub(super) struct EmbeddingSession {
    backend: EmbeddingBackend,
    dimensions: usize,
    batch_size: usize,
}

enum EmbeddingBackend {
    Deterministic,
    FastEmbed(TextEmbedding),
}

pub(super) fn model_cache_dir_for_staging(staging: &Path) -> PathBuf {
    staging
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .join("models")
}

pub(super) fn embedding_chunk_size(profile: &BuildProfile) -> usize {
    profile
        .embedding_batch_size
        .unwrap_or(1)
        .saturating_mul(EMBEDDING_CHUNK_MULTIPLIER)
        .max(1)
}

impl EmbeddingSession {
    pub(super) fn new(
        profile: &BuildProfile,
        cache_dir: &Path,
        dimensions: usize,
    ) -> Result<Self, Error> {
        let batch_size = profile.embedding_batch_size.unwrap_or(1);
        let backend = match profile.embedding_backend.as_str() {
            "deterministic" => EmbeddingBackend::Deterministic,
            "fastembed" => {
                if profile.model_name.as_deref() != Some("BAAI/bge-small-en-v1.5") {
                    return Err(Error::invalid(
                        "fastembed backend requires BAAI/bge-small-en-v1.5",
                    ));
                }
                fs::create_dir_all(cache_dir)
                    .map_err(|e| Error::storage(format!("create model cache: {e}")))?;
                let max_length = profile.model_max_tokens.unwrap_or(MAX_MODEL_PASSAGE_TOKENS);
                let threads = profile.embedding_intra_op_threads.unwrap_or(1);
                let model = TextEmbedding::try_new(
                    TextInitOptions::new(EmbeddingModel::BGESmallENV15)
                        .with_cache_dir(cache_dir.to_path_buf())
                        .with_max_length(max_length)
                        .with_intra_threads(threads)
                        .with_show_download_progress(false),
                )
                .map_err(|error| Error::storage(format!("initialize FastEmbed model: {error}")))?;
                EmbeddingBackend::FastEmbed(model)
            }
            backend => {
                return Err(Error::invalid(format!(
                    "unsupported embedding backend {backend:?}"
                )));
            }
        };
        Ok(Self {
            backend,
            dimensions,
            batch_size,
        })
    }

    pub(super) fn embed(&mut self, inputs: Vec<String>) -> Result<Vec<Vec<f32>>, Error> {
        let vectors = match &mut self.backend {
            EmbeddingBackend::Deterministic => inputs
                .iter()
                .map(|input| deterministic_embedding(input, self.dimensions))
                .collect(),
            EmbeddingBackend::FastEmbed(model) => model
                .embed(inputs, Some(self.batch_size))
                .map_err(|error| {
                    Error::storage(format!("embed passages with FastEmbed: {error}"))
                })?,
        };
        if vectors.iter().any(|vector| vector.len() != self.dimensions) {
            return Err(Error::invalid(
                "embedding dimensions do not match declared build profile",
            ));
        }
        Ok(vectors)
    }
}

pub(crate) fn embed_query(
    query: &str,
    profile: &BuildProfile,
    cache_dir: &Path,
    dimensions: usize,
) -> Result<Vec<f32>, Error> {
    let instruction = profile.query_instruction.as_deref().unwrap_or_default();
    let input = format!("{instruction}{query}");
    let mut embedder = EmbeddingSession::new(profile, cache_dir, dimensions)?;
    embedder
        .embed(vec![input])?
        .into_iter()
        .next()
        .ok_or_else(|| Error::invalid("semantic query embedding was empty"))
}

fn deterministic_embedding(text: &str, dimensions: usize) -> Vec<f32> {
    if dimensions == 0 {
        return Vec::new();
    }
    let mut vector = vec![0.0_f32; dimensions];
    for token in text.split_whitespace() {
        let normalized = token.to_ascii_lowercase();
        let digest = Sha256::digest(format!("scout.semantic.hash.v1\0{normalized}").as_bytes());
        let first = u64::from_le_bytes(digest[..8].try_into().expect("digest width"));
        let second = u64::from_le_bytes(digest[8..16].try_into().expect("digest width"));
        let first_index = (first % dimensions as u64) as usize;
        let second_index = (second % dimensions as u64) as usize;
        let magnitude = 1.0 + f32::from(digest[16]) / 255.0;
        let sign = if digest[17] & 1 == 0 { 1.0 } else { -1.0 };
        vector[first_index] += sign * magnitude;
        vector[second_index] -= sign * 0.5;
    }
    let norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt() as f32;
    if norm == 0.0 {
        vector[0] = 1.0;
    } else {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}
