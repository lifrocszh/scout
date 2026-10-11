use super::embeddings::fastembed_model_spec;
use super::{BuildMode, BuildProfile, MAX_MODEL_PASSAGE_TOKENS, RawBuildProfile};
use crate::error::Error;

pub(super) fn build_profile(
    raw: RawBuildProfile,
    corpus_id: &str,
    corpus_model: &str,
    build_mode: BuildMode,
) -> Result<BuildProfile, Error> {
    macro_rules! required {
        ($field:ident) => {
            raw.$field.ok_or_else(|| {
                Error::invalid(format!(
                    "build profile field `{}` is required",
                    stringify!($field)
                ))
            })?
        };
    }

    let schema_version = required!(schema_version);
    if !schema_version.ends_with(".v1") {
        return Err(Error::invalid(format!(
            "unsupported build schema_version {schema_version:?}"
        )));
    }
    let snapshot_id = required!(snapshot_id);
    if snapshot_id != corpus_id {
        return Err(Error::invalid(
            "build profile snapshot_id does not match --corpus",
        ));
    }
    let extraction_schema = required!(extraction_schema);
    let passage_schema = required!(passage_schema);
    let max_passage_tokens = required!(max_passage_tokens);
    if max_passage_tokens == 0 || max_passage_tokens > MAX_MODEL_PASSAGE_TOKENS {
        return Err(Error::invalid(
            "max_passage_tokens must be between 1 and 512",
        ));
    }
    let overlap_tokens = required!(overlap_tokens);
    let no_overlap = required!(no_overlap);
    if overlap_tokens != 0 || !no_overlap {
        return Err(Error::invalid("Passage overlap must be zero"));
    }
    let snippet_limit = required!(snippet_limit);
    let analyzer_id = required!(analyzer_id);
    let title_boost = required!(title_boost);
    let heading_boost = required!(heading_boost);
    let body_boost = required!(body_boost);
    let candidate_depth = required!(candidate_depth);
    let writer_memory_bytes = required!(writer_memory_bytes);
    let worker_count = required!(worker_count);
    if raw.model_name.is_some() {
        return Err(Error::invalid(
            "model_name must be selected in the crawl config before crawling, not in the index profile",
        ));
    }
    let model_name = corpus_model.to_string();
    let model_source = required!(model_source);
    let model_revision = required!(model_revision);
    let model_license = required!(model_license);
    let onnx_sha256 = required!(onnx_sha256);
    let tokenizer_sha256 = required!(tokenizer_sha256);
    let model_config_sha256 = required!(model_config_sha256);
    let embedding_backend = required!(embedding_backend);
    let model_max_tokens = required!(model_max_tokens);
    let pooling = required!(pooling);
    let normalization = required!(normalization);
    let quantization = required!(quantization);
    let query_instruction = required!(query_instruction);
    let document_representation = required!(document_representation);
    let dimensions = required!(dimensions);
    let embedding_batch_size = required!(embedding_batch_size);
    let embedding_workers = required!(embedding_workers);
    let embedding_intra_op_threads = required!(embedding_intra_op_threads);
    let embedding_execution_provider = if build_mode == BuildMode::KeywordOnly {
        "cpu".into()
    } else {
        raw.embedding_execution_provider
            .unwrap_or_else(|| "cpu".into())
    };
    let metric = required!(metric);
    let scalar_kind = required!(scalar_kind);
    let connectivity = required!(connectivity);
    let construction_expansion = required!(construction_expansion);
    let search_expansion = required!(search_expansion);
    let key_map = required!(key_map);
    let fusion_candidate_depth = required!(fusion_candidate_depth);
    let fusion_algorithm = required!(fusion_algorithm);
    let rrf_k = required!(rrf_k);
    let page_aggregation = required!(page_aggregation);
    let snippet_window = required!(snippet_window);
    if snippet_limit == 0
        || !title_boost.is_finite()
        || !heading_boost.is_finite()
        || !body_boost.is_finite()
        || title_boost <= 0.0
        || heading_boost <= 0.0
        || body_boost <= 0.0
        || candidate_depth == 0
        || writer_memory_bytes == 0
        || worker_count == 0
        || model_max_tokens == 0
        || model_max_tokens > MAX_MODEL_PASSAGE_TOKENS
        || dimensions == 0
        || dimensions > 4096
        || embedding_batch_size == 0
        || embedding_workers == 0
        || embedding_intra_op_threads == 0
        || connectivity == 0
        || construction_expansion == 0
        || search_expansion == 0
        || fusion_candidate_depth == 0
        || rrf_k == 0
        || snippet_window == 0
    {
        return Err(Error::invalid(
            "build profile numeric limits and boosts must be positive",
        ));
    }
    if !matches!(embedding_backend.as_str(), "fastembed" | "deterministic") {
        return Err(Error::invalid(
            "embedding_backend must be fastembed or deterministic",
        ));
    }
    if !matches!(
        embedding_execution_provider.as_str(),
        "cpu" | "openvino_gpu" | "apple_coreml_gpu"
    ) {
        return Err(Error::invalid(
            "embedding_execution_provider must be cpu, openvino_gpu, or apple_coreml_gpu",
        ));
    }
    if embedding_backend == "deterministic" && embedding_execution_provider != "cpu" {
        return Err(Error::invalid(
            "deterministic embedding_backend only supports embedding_execution_provider=cpu",
        ));
    }
    #[cfg(not(feature = "openvino-gpu"))]
    if embedding_execution_provider == "openvino_gpu" {
        return Err(Error::invalid(
            "embedding_execution_provider=openvino_gpu requires the openvino-gpu Cargo feature and an OpenVINO-enabled ONNX Runtime",
        ));
    }
    #[cfg(not(feature = "apple-coreml"))]
    if embedding_execution_provider == "apple_coreml_gpu" {
        return Err(Error::invalid(
            "embedding_execution_provider=apple_coreml_gpu requires the apple-coreml Cargo feature and Core ML",
        ));
    }
    #[cfg(not(target_os = "macos"))]
    if embedding_execution_provider == "apple_coreml_gpu" {
        return Err(Error::invalid(
            "embedding_execution_provider=apple_coreml_gpu is only supported on macOS",
        ));
    }
    if !matches!(pooling.as_str(), "cls" | "mean")
        || normalization != "l2"
        || quantization != "none"
        || metric != "cosine"
        || scalar_kind != "f32"
        || fusion_algorithm != "rrf"
        || page_aggregation != "max"
    {
        return Err(Error::invalid(
            "build profile must use model-supported pooling/l2/none/cosine/f32/rrf/max MVP settings",
        ));
    }
    let profile = BuildProfile {
        schema_version,
        snapshot_id: corpus_id.into(),
        build_mode,
        extraction_schema,
        passage_schema,
        max_passage_tokens,
        overlap_tokens,
        no_overlap,
        snippet_limit,
        analyzer_id,
        title_boost,
        heading_boost,
        body_boost,
        candidate_depth,
        writer_memory_bytes,
        worker_count,
        model_name: Some(model_name),
        model_source: Some(model_source),
        model_revision: Some(model_revision),
        model_license: Some(model_license),
        onnx_sha256: Some(onnx_sha256),
        tokenizer_sha256: Some(tokenizer_sha256),
        model_config_sha256: Some(model_config_sha256),
        embedding_backend,
        model_max_tokens: Some(model_max_tokens),
        pooling: Some(pooling),
        normalization: Some(normalization),
        quantization: Some(quantization),
        query_instruction: Some(query_instruction),
        document_representation: Some(document_representation),
        dimensions: Some(dimensions),
        embedding_batch_size: Some(embedding_batch_size),
        embedding_workers: Some(embedding_workers),
        embedding_intra_op_threads: Some(embedding_intra_op_threads),
        embedding_execution_provider: Some(embedding_execution_provider),
        metric: Some(metric),
        scalar_kind: Some(scalar_kind),
        connectivity: Some(connectivity),
        construction_expansion: Some(construction_expansion),
        search_expansion: Some(search_expansion),
        key_map: Some(key_map),
        build_limit: raw.build_limit,
        fusion_candidate_depth: Some(fusion_candidate_depth),
        fusion_algorithm: Some(fusion_algorithm),
        rrf_k: Some(rrf_k),
        page_aggregation: Some(page_aggregation),
        snippet_window: Some(snippet_window),
    };
    validate_model_metadata(&profile)?;
    Ok(profile)
}

pub(super) fn validate_model_metadata(profile: &BuildProfile) -> Result<(), Error> {
    if profile.embedding_backend != "fastembed" {
        return Ok(());
    }
    let spec = fastembed_model_spec(profile.model_name.as_deref().unwrap_or_default())?;
    let sha256_fields = [
        profile.onnx_sha256.as_deref(),
        profile.tokenizer_sha256.as_deref(),
        profile.model_config_sha256.as_deref(),
    ];
    if profile.model_source.as_deref() != Some(spec.source)
        || profile.model_license.as_deref() != Some(spec.license)
        || profile.pooling.as_deref() != Some(spec.pooling)
        || profile.dimensions != Some(spec.dimensions)
        || profile
            .model_max_tokens
            .is_none_or(|limit| limit > spec.max_tokens)
        || profile.query_instruction.as_deref() != Some(spec.query_instruction)
        || profile.model_revision.as_deref().is_none_or(str::is_empty)
        || sha256_fields.into_iter().any(|value| {
            value.is_none_or(|digest| {
                digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        })
    {
        return Err(Error::invalid(
            "FastEmbed model metadata does not match the selected model",
        ));
    }
    Ok(())
}
