use super::BuildProfile;
use super::MAX_MODEL_PASSAGE_TOKENS;
use crate::error::Error;
use crate::storage::sha256_hex;
use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const EMBEDDING_CHUNK_MULTIPLIER: usize = 32;

pub(super) struct EmbeddingSession {
    backend: EmbeddingBackend,
    dimensions: usize,
    batch_size: usize,
}

pub(crate) struct QueryEmbedder {
    session: Mutex<Option<EmbeddingSession>>,
    #[cfg(test)]
    session_initializations: std::sync::atomic::AtomicUsize,
}

enum EmbeddingBackend {
    Deterministic,
    FastEmbed(Box<TextEmbedding>),
}

pub(super) struct FastEmbedModelSpec {
    pub(super) model: EmbeddingModel,
    pub(super) source: &'static str,
    pub(super) license: &'static str,
    pub(super) dimensions: usize,
    pub(super) pooling: &'static str,
    pub(super) max_tokens: usize,
    pub(super) query_instruction: &'static str,
}

pub(super) fn fastembed_model_spec(name: &str) -> Result<FastEmbedModelSpec, Error> {
    match name {
        super::DEFAULT_EMBEDDING_MODEL => Ok(FastEmbedModelSpec {
            model: EmbeddingModel::BGESmallENV15,
            source: "hf://Xenova/bge-small-en-v1.5",
            license: "MIT",
            dimensions: 384,
            pooling: "cls",
            max_tokens: 512,
            query_instruction: "Represent this sentence for searching relevant passages: ",
        }),
        super::MINILM_EMBEDDING_MODEL => Ok(FastEmbedModelSpec {
            model: EmbeddingModel::AllMiniLML6V2,
            source: "hf://Qdrant/all-MiniLM-L6-v2-onnx",
            license: "Apache-2.0",
            dimensions: 384,
            pooling: "mean",
            max_tokens: 512,
            query_instruction: "",
        }),
        _ => Err(Error::invalid(format!(
            "unsupported FastEmbed model {name:?}"
        ))),
    }
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

pub(super) fn document_embedding_fingerprint(
    profile: &BuildProfile,
    model_cache: &Path,
) -> Result<String, Error> {
    let model_assets_sha256 = if profile.embedding_backend == "fastembed" {
        let source = profile.model_source.as_deref().unwrap_or_default();
        let repository = source
            .strip_prefix("hf://")
            .ok_or_else(|| Error::invalid("embedding model source cannot be verified locally"))?;
        let model_root = model_cache.join(format!("models--{}", repository.replace('/', "--")));
        let revision = profile.model_revision.as_deref().unwrap_or_default();
        let cached_revision = fs::read_to_string(model_root.join("refs/main"))
            .map_err(|error| Error::invalid(format!("read pinned model revision: {error}")))?;
        if cached_revision.trim() != revision {
            return Err(Error::invalid(
                "cached embedding model revision does not match the build profile",
            ));
        }
        let snapshot = model_root.join("snapshots").join(revision);
        verify_model_asset(
            &snapshot.join("onnx/model.onnx"),
            profile.onnx_sha256.as_deref(),
            "ONNX model",
        )?;
        verify_model_asset(
            &snapshot.join("tokenizer.json"),
            profile.tokenizer_sha256.as_deref(),
            "tokenizer",
        )?;
        verify_model_asset(
            &snapshot.join("config.json"),
            profile.model_config_sha256.as_deref(),
            "model config",
        )?;
        Some(super::checksums::directory_digest(&snapshot)?)
    } else {
        None
    };
    let runtime_sha256 = if profile.embedding_backend == "fastembed" {
        Some(embedding_runtime_fingerprint(
            profile.embedding_execution_provider.as_deref(),
        )?)
    } else {
        None
    };
    let compatible_settings = json!({
        "schema": "scout.document-embedding-input.v1",
        "implementation": "fastembed-7.0.1-ort-2.0.0-rc.13",
        "model_name": profile.model_name.as_deref(),
        "model_source": profile.model_source.as_deref(),
        "model_revision": profile.model_revision.as_deref(),
        "model_license": profile.model_license.as_deref(),
        "model_assets_sha256": model_assets_sha256,
        "runtime_sha256": runtime_sha256,
        "embedding_backend": profile.embedding_backend.as_str(),
        "execution_provider": profile.embedding_execution_provider.as_deref(),
        "provider_device": if profile.embedding_execution_provider.as_deref() == Some("openvino_gpu") || profile.embedding_execution_provider.as_deref() == Some("apple_coreml_gpu") { "GPU" } else { "default" },
        "model_max_tokens": profile.model_max_tokens,
        "pooling": profile.pooling.as_deref(),
        "normalization": profile.normalization.as_deref(),
        "quantization": profile.quantization.as_deref(),
        "dimensions": profile.dimensions,
        "embedding_batch_size": profile.embedding_batch_size,
        "embedding_workers": profile.embedding_workers,
        "embedding_intra_op_threads": profile.embedding_intra_op_threads,
        "document_representation": profile.document_representation.as_deref(),
        "input_construction": "normalized-heading-path-newline-passage.v1",
        "output_format": "little-endian-f32.v1",
        "architecture": env::consts::ARCH,
    });
    let bytes = serde_json::to_vec(&compatible_settings)
        .map_err(|error| Error::storage(format!("serialize embedding fingerprint: {error}")))?;
    Ok(sha256_hex(&bytes))
}

fn verify_model_asset(path: &Path, expected: Option<&str>, label: &str) -> Result<(), Error> {
    let expected = expected.ok_or_else(|| Error::invalid(format!("{label} hash missing")))?;
    let bytes = fs::read(path)
        .map_err(|error| Error::invalid(format!("read verified {label} asset: {error}")))?;
    if sha256_hex(&bytes) != expected {
        return Err(Error::invalid(format!(
            "verified {label} asset hash mismatch"
        )));
    }
    Ok(())
}

fn embedding_runtime_fingerprint(provider: Option<&str>) -> Result<String, Error> {
    let dynamic_library = env::var_os("ORT_DYLIB_PATH")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
        .or_else(|| {
            env::split_paths(&env::var_os("LD_LIBRARY_PATH").unwrap_or_default())
                .map(|directory| directory.join("libonnxruntime.so"))
                .find(|path| path.is_file())
        });
    let Some(dynamic_library) = dynamic_library else {
        let executable = env::current_exe().map_err(|error| {
            Error::storage(format!("locate embedding runtime executable: {error}"))
        })?;
        let bytes = fs::read(executable).map_err(|error| {
            Error::storage(format!("read embedding runtime executable: {error}"))
        })?;
        return Ok(sha256_hex(&bytes));
    };
    let runtime_dir = dynamic_library
        .parent()
        .ok_or_else(|| Error::invalid("ONNX Runtime library has no parent directory"))?;
    let mut libraries = fs::read_dir(runtime_dir)
        .map_err(|error| Error::storage(format!("read embedding runtime libraries: {error}")))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        (name.starts_with("libonnxruntime")
                            || name.starts_with("libopenvino")
                            || name.starts_with("libtbb"))
                            && name.contains(".so")
                    })
        })
        .collect::<Vec<_>>();
    libraries.sort();
    if libraries.is_empty() {
        return Err(Error::invalid(
            "ONNX Runtime provider libraries cannot be fingerprinted",
        ));
    }
    if provider == Some("openvino_gpu")
        && !libraries.iter().any(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "libonnxruntime_providers_openvino.so")
        })
    {
        return Err(Error::invalid(
            "OpenVINO provider library is missing from the verified runtime directory",
        ));
    }
    let mut identities = Vec::with_capacity(libraries.len());
    for path in libraries {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let bytes = fs::read(&path)
            .map_err(|error| Error::storage(format!("read runtime library {name}: {error}")))?;
        identities.push((name.to_string(), sha256_hex(&bytes)));
    }
    let bytes = serde_json::to_vec(&identities)
        .map_err(|error| Error::storage(format!("serialize runtime identity: {error}")))?;
    Ok(sha256_hex(&bytes))
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
                let model =
                    fastembed_model_spec(profile.model_name.as_deref().unwrap_or_default())?;
                fs::create_dir_all(cache_dir)
                    .map_err(|e| Error::storage(format!("create model cache: {e}")))?;
                let max_length = profile.model_max_tokens.unwrap_or(MAX_MODEL_PASSAGE_TOKENS);
                let threads = profile.embedding_intra_op_threads.unwrap_or(1);
                let options =
                    fastembed_options(profile, cache_dir, max_length, threads, model.model)?;
                let model = TextEmbedding::try_new(options).map_err(|error| {
                    Error::storage(format!("initialize FastEmbed model: {error}"))
                })?;
                EmbeddingBackend::FastEmbed(Box::new(model))
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

impl Default for QueryEmbedder {
    fn default() -> Self {
        Self {
            session: Mutex::new(None),
            #[cfg(test)]
            session_initializations: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl QueryEmbedder {
    pub(crate) fn embed(
        &self,
        query: &str,
        profile: &BuildProfile,
        cache_dir: &Path,
        dimensions: usize,
    ) -> Result<Vec<f32>, Error> {
        let mut session = self
            .session
            .lock()
            .map_err(|_| Error::storage("query embedding session lock poisoned"))?;
        if session.is_none() {
            *session = Some(EmbeddingSession::new(profile, cache_dir, dimensions)?);
            #[cfg(test)]
            self.session_initializations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let instruction = profile.query_instruction.as_deref().unwrap_or_default();
        session
            .as_mut()
            .expect("query embedding session initialized")
            .embed(vec![format!("{instruction}{query}")])?
            .into_iter()
            .next()
            .ok_or_else(|| Error::invalid("semantic query embedding was empty"))
    }

    #[cfg(test)]
    pub(crate) fn session_initializations(&self) -> usize {
        self.session_initializations
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn fastembed_options(
    profile: &BuildProfile,
    cache_dir: &Path,
    max_length: usize,
    threads: usize,
    model: EmbeddingModel,
) -> Result<TextInitOptions, Error> {
    let options = TextInitOptions::new(model)
        .with_cache_dir(cache_dir.to_path_buf())
        .with_max_length(max_length)
        .with_intra_threads(threads)
        .with_show_download_progress(false);

    match profile
        .embedding_execution_provider
        .as_deref()
        .unwrap_or("cpu")
    {
        "cpu" => Ok(options),
        "openvino_gpu" => {
            #[cfg(feature = "openvino-gpu")]
            {
                Ok(options
                    .with_execution_providers(vec![
                        ort::ep::OpenVINO::default()
                            .with_device_type("GPU")
                            .build()
                            .error_on_failure(),
                    ])
                    .with_session_config("session.disable_cpu_ep_fallback", "1"))
            }
            #[cfg(not(feature = "openvino-gpu"))]
            {
                Err(Error::invalid(
                    "openvino_gpu embedding requires the openvino-gpu Cargo feature and an OpenVINO-enabled ONNX Runtime",
                ))
            }
        }
        "apple_coreml_gpu" => {
            #[cfg(all(feature = "apple-coreml", target_os = "macos"))]
            {
                Ok(options.with_execution_providers(vec![
                    ort::ep::CoreML::default()
                        .with_compute_units(ort::ep::coreml::ComputeUnits::CPUAndGPU)
                        .with_model_cache_dir(cache_dir.join("coreml").to_string_lossy())
                        .build()
                        .error_on_failure(),
                ]))
            }
            #[cfg(not(all(feature = "apple-coreml", target_os = "macos")))]
            {
                Err(Error::invalid(
                    "apple_coreml_gpu embedding requires the apple-coreml Cargo feature on macOS",
                ))
            }
        }
        provider => Err(Error::invalid(format!(
            "unsupported embedding_execution_provider {provider:?}"
        ))),
    }
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

#[cfg(test)]
mod tests {
    use super::BuildProfile;
    use super::QueryEmbedder;
    use serde_json::json;
    use std::sync::Arc;

    fn deterministic_profile() -> BuildProfile {
        serde_json::from_value(json!({
            "schema_version": "scout.build.v1",
            "snapshot_id": "snapshot-test",
            "extraction_schema": "scout.extraction.v1",
            "passage_schema": "scout.passage.v1",
            "max_passage_tokens": 384,
            "overlap_tokens": 0,
            "no_overlap": true,
            "snippet_limit": 5,
            "analyzer_id": "standard",
            "title_boost": 3.0,
            "heading_boost": 2.0,
            "body_boost": 1.0,
            "candidate_depth": 10,
            "writer_memory_bytes": 1024,
            "worker_count": 1,
            "embedding_backend": "deterministic",
            "dimensions": 8,
            "query_instruction": "find: "
        }))
        .expect("valid deterministic profile")
    }

    #[test]
    fn query_embedder_reuses_one_session_across_concurrent_queries() {
        let embedder = Arc::new(QueryEmbedder::default());
        let profile = Arc::new(deterministic_profile());
        let cache_dir = std::env::temp_dir();
        let workers = (0..4)
            .map(|_| {
                let embedder = embedder.clone();
                let profile = profile.clone();
                let cache_dir = cache_dir.clone();
                std::thread::spawn(move || {
                    embedder
                        .embed("how does ownership work", &profile, &cache_dir, 8)
                        .expect("embed query")
                })
            })
            .collect::<Vec<_>>();
        let vectors = workers
            .into_iter()
            .map(|worker| worker.join().expect("query worker"))
            .collect::<Vec<_>>();

        assert!(vectors.iter().all(|vector| vector == &vectors[0]));
        assert_eq!(embedder.session_initializations(), 1);
    }
}
