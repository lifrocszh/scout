use super::embeddings::{EmbeddingSession, embedding_chunk_size, model_cache_dir_for_staging};
use super::semantic_storage::{read_semantic_index, semantic_hits, validate_semantic_index};
use crate::storage::write_json;
use crate::{
    BuildProfile, CatalogRecord, Error, MetricKind, SEMANTIC_ARTIFACT, SEMANTIC_MAGIC,
    SEMANTIC_VECTOR_ARTIFACT, SemanticEvidence, SemanticHit,
};
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Instant;
use usearch::{Index as UsearchIndex, IndexOptions as UsearchIndexOptions, ScalarKind};

pub(super) fn build_semantic_index(
    staging: &Path,
    catalog: &[CatalogRecord],
    profile: &BuildProfile,
) -> Result<SemanticEvidence, Error> {
    let dimensions = profile
        .dimensions
        .ok_or_else(|| Error::invalid("semantic dimensions are required"))?;
    let candidate_depth = profile.fusion_candidate_depth.unwrap_or(20);
    let started = Instant::now();
    if catalog.is_empty() {
        return Err(Error::invalid(
            "semantic artifact requires at least one Passage",
        ));
    }

    let cache_dir = model_cache_dir_for_staging(staging);
    let mut embedder = EmbeddingSession::new(profile, &cache_dir, dimensions)?;
    let vector_artifact_path = staging.join(SEMANTIC_VECTOR_ARTIFACT);
    let artifact_path = staging.join(SEMANTIC_ARTIFACT);
    let options = semantic_index_options(profile, dimensions)?;
    let index = UsearchIndex::new(&options)
        .map_err(|error| Error::storage(format!("create USearch index: {error}")))?;
    index
        .reserve(catalog.len())
        .map_err(|error| Error::storage(format!("reserve USearch index: {error}")))?;
    let mut vector_file = File::create(&vector_artifact_path)
        .map_err(|e| Error::storage(format!("create semantic vector artifact: {e}")))?;
    vector_file
        .write_all(SEMANTIC_MAGIC)
        .and_then(|_| vector_file.write_all(&(dimensions as u32).to_le_bytes()))
        .and_then(|_| vector_file.write_all(&(catalog.len() as u64).to_le_bytes()))
        .map_err(|e| Error::storage(format!("write semantic vector header: {e}")))?;
    for chunk in catalog.chunks(embedding_chunk_size(profile)) {
        let inputs = chunk
            .iter()
            .map(|record| format!("{}\n{}", record.heading_path.join(" "), record.text))
            .collect::<Vec<_>>();
        let vectors = embedder.embed(inputs)?;
        if vectors.len() != chunk.len() {
            return Err(Error::invalid(
                "embedding count does not match catalog chunk",
            ));
        }
        for (record, vector) in chunk.iter().zip(vectors) {
            vector_file
                .write_all(&record.vector_key.to_le_bytes())
                .and_then(|_| {
                    vector
                        .iter()
                        .try_for_each(|value| vector_file.write_all(&value.to_le_bytes()))
                })
                .map_err(|e| Error::storage(format!("write semantic vector: {e}")))?;
            index
                .add(record.vector_key, &vector)
                .map_err(|error| Error::storage(format!("add USearch vector: {error}")))?;
        }
    }
    vector_file
        .flush()
        .map_err(|e| Error::storage(format!("flush semantic vector artifact: {e}")))?;
    drop(vector_file);

    let first = read_semantic_index(&vector_artifact_path)?;
    validate_semantic_index(&first, catalog, dimensions)?;
    let query = catalog
        .iter()
        .find_map(|record| {
            record
                .text
                .split_whitespace()
                .find(|token| token.chars().count() > 3)
        })
        .unwrap_or("passage")
        .to_string();
    let instruction = profile.query_instruction.as_deref().unwrap_or_default();
    let query_vector = embedder
        .embed(vec![format!("{instruction}{query}")])?
        .into_iter()
        .next()
        .ok_or_else(|| Error::invalid("semantic query embedding was empty"))?;
    let first_hits = semantic_hits(&first, catalog, &query_vector, candidate_depth)?;
    index
        .save(&artifact_path.to_string_lossy())
        .map_err(|error| Error::storage(format!("save USearch index: {error}")))?;
    let artifact_bytes = fs::metadata(&artifact_path)
        .map_err(|e| Error::storage(format!("stat semantic artifact: {e}")))?
        .len();
    let artifact_sha256 = super::checksums::directory_or_file_digest(&artifact_path)?;
    let reopened = UsearchIndex::restore_view(&artifact_path.to_string_lossy())
        .map_err(|error| Error::storage(format!("reopen USearch index: {error}")))?;
    validate_usearch_index(&reopened, catalog, dimensions, profile)?;
    let reopened_matches = reopened
        .search(&query_vector, candidate_depth)
        .map_err(|error| Error::storage(format!("search reopened USearch index: {error}")))?;
    let reopened_hits = usearch_hits(&reopened_matches, catalog)?;
    let repeated_matches = reopened
        .search(&query_vector, candidate_depth)
        .map_err(|error| Error::storage(format!("repeat USearch smoke search: {error}")))?;
    let repeated_hits = usearch_hits(&repeated_matches, catalog)?;
    let reopened_keys = reopened_hits
        .iter()
        .map(|hit| hit.vector_key)
        .collect::<Vec<_>>();
    let repeated_keys = repeated_hits
        .iter()
        .map(|hit| hit.vector_key)
        .collect::<Vec<_>>();
    if reopened_keys != repeated_keys || (first_hits.is_empty() && reopened_hits.is_empty()) {
        return Err(Error::invalid("semantic candidates changed after reopen"));
    }

    let build_duration_ms = started.elapsed().as_millis();
    let embedding_passages_per_second =
        catalog.len() as f64 / started.elapsed().as_secs_f64().max(f64::EPSILON);
    let evidence = SemanticEvidence {
        schema_version: "scout.semantic.usearch.v1".into(),
        backend: format!("{}-usearch", profile.embedding_backend),
        artifact: SEMANTIC_ARTIFACT.into(),
        reopen_mode: "restore_view-mmap".into(),
        model_name: profile.model_name.clone(),
        model_source: profile.model_source.clone(),
        model_revision: profile.model_revision.clone(),
        model_license: profile.model_license.clone(),
        onnx_sha256: profile.onnx_sha256.clone(),
        tokenizer_sha256: profile.tokenizer_sha256.clone(),
        model_config_sha256: profile.model_config_sha256.clone(),
        embedding_backend: profile.embedding_backend.clone(),
        model_max_tokens: profile.model_max_tokens,
        pooling: profile.pooling.clone(),
        normalization: profile.normalization.clone(),
        query_instruction: profile.query_instruction.clone(),
        document_representation: profile.document_representation.clone(),
        dimensions,
        metric: profile.metric.clone(),
        scalar_kind: profile.scalar_kind.clone(),
        embedding_batch_size: profile.embedding_batch_size,
        embedding_workers: profile.embedding_workers,
        embedding_intra_op_threads: profile.embedding_intra_op_threads,
        connectivity: profile.connectivity,
        construction_expansion: profile.construction_expansion,
        search_expansion: profile.search_expansion,
        key_map: profile.key_map.clone(),
        catalog_count: catalog.len(),
        // A smoke search returns only candidate_depth hits; report the count
        // observed in the reopened persisted index instead.
        vector_count: reopened.size(),
        candidate_depth,
        query,
        query_hit_count: reopened_hits.len(),
        returned_identities: reopened_hits,
        artifact_bytes,
        artifact_sha256,
        build_duration_ms,
        embedding_passages_per_second,
        memory_observation: format!("usearch_memory_bytes={}", reopened.memory_usage()),
        reopened: true,
    };
    write_json(&staging.join("semantic-manifest.json"), &evidence)?;
    Ok(evidence)
}

fn semantic_index_options(
    profile: &BuildProfile,
    dimensions: usize,
) -> Result<UsearchIndexOptions, Error> {
    Ok(UsearchIndexOptions {
        dimensions,
        metric: MetricKind::Cos,
        quantization: ScalarKind::F32,
        connectivity: profile.connectivity.unwrap_or(16),
        expansion_add: profile.construction_expansion.unwrap_or(128),
        expansion_search: profile.search_expansion.unwrap_or(64),
        multi: false,
    })
}

pub(crate) fn open_semantic_index(
    path: &Path,
    profile: &BuildProfile,
) -> Result<UsearchIndex, Error> {
    let index = UsearchIndex::restore_view(&path.to_string_lossy())
        .map_err(|error| Error::invalid(format!("reopen USearch artifact: {error}")))?;
    let dimensions = profile
        .dimensions
        .ok_or_else(|| Error::invalid("semantic dimensions missing from profile"))?;
    if index.dimensions() != dimensions
        || !matches!(index.metric_kind(), MetricKind::Cos)
        || !matches!(index.scalar_kind(), ScalarKind::F32)
    {
        return Err(Error::invalid("USearch profile compatibility mismatch"));
    }
    Ok(index)
}

pub(super) fn validate_usearch_index(
    index: &UsearchIndex,
    catalog: &[CatalogRecord],
    dimensions: usize,
    profile: &BuildProfile,
) -> Result<(), Error> {
    if index.dimensions() != dimensions
        || !matches!(index.metric_kind(), MetricKind::Cos)
        || !matches!(index.scalar_kind(), ScalarKind::F32)
        || index.size() != catalog.len()
    {
        return Err(Error::invalid("USearch dimension/metric/count mismatch"));
    }
    if index.connectivity() != profile.connectivity.unwrap_or(16) {
        return Err(Error::invalid("USearch connectivity mismatch"));
    }
    for record in catalog {
        if !index.contains(record.vector_key) {
            return Err(Error::invalid("USearch vector key missing from catalog"));
        }
    }
    Ok(())
}

fn usearch_hits(
    matches: &usearch::ffi::Matches,
    catalog: &[CatalogRecord],
) -> Result<Vec<SemanticHit>, Error> {
    let by_key = catalog
        .iter()
        .map(|record| (record.vector_key, record))
        .collect::<std::collections::HashMap<_, _>>();
    matches
        .keys
        .iter()
        .zip(matches.distances.iter())
        .map(|(vector_key, distance)| {
            let record = by_key
                .get(vector_key)
                .ok_or_else(|| Error::invalid("USearch candidate missing from catalog"))?;
            Ok(SemanticHit {
                vector_key: *vector_key,
                page_id: record.page_id.clone(),
                passage_id: record.passage_id.clone(),
                score: 1.0 - distance,
            })
        })
        .collect()
}
