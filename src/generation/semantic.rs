use super::checkpoint::{BuildCheckpointIdentity, BuildCheckpointStore, CHECKPOINT_SCHEMA};
use super::embedding_cache::EmbeddingReuseCache;
use super::embeddings::{
    EmbeddingSession, document_embedding_fingerprint, embedding_chunk_size,
    model_cache_dir_for_staging,
};
use super::semantic_storage::{read_semantic_index, semantic_hits, validate_semantic_index};
use super::{
    BuildProfile, CatalogRecord, SEMANTIC_ARTIFACT, SEMANTIC_MAGIC, SEMANTIC_VECTOR_ARTIFACT,
    SemanticEvidence, SemanticHit, SemanticStageTimings,
};
use crate::error::Error;
use crate::storage::write_json;
use serde_json::{Value, json};
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use usearch::MetricKind;
use usearch::{Index as UsearchIndex, IndexOptions as UsearchIndexOptions, ScalarKind};

pub(super) struct SemanticCheckpointStats {
    pub(super) schema: &'static str,
    pub(super) identity_sha256: String,
    pub(super) chunks_reused: usize,
    pub(super) chunks_created: usize,
    pub(super) embedding_fingerprint: String,
    pub(super) cache_vectors_reused: usize,
    pub(super) cache_vectors_created: usize,
    pub(super) checkpoint_vectors_reused: usize,
    pub(super) local_duplicate_vectors_reused: usize,
}

pub(super) struct SemanticBuildInput<'a> {
    pub(super) staging: &'a Path,
    pub(super) checkpoint_root: &'a Path,
    pub(super) embedding_cache_root: &'a Path,
    pub(super) build_run_id: &'a str,
    pub(super) corpus_snapshot_sha256: &'a str,
    pub(super) build_profile_sha256: &'a str,
    pub(super) catalog_sha256: &'a str,
    pub(super) resume: bool,
    pub(super) catalog: &'a [CatalogRecord],
    pub(super) profile: &'a BuildProfile,
}

pub(super) fn build_semantic_index(
    input: SemanticBuildInput<'_>,
) -> Result<(SemanticEvidence, SemanticCheckpointStats), Error> {
    let SemanticBuildInput {
        staging,
        checkpoint_root,
        embedding_cache_root,
        build_run_id,
        corpus_snapshot_sha256,
        build_profile_sha256,
        catalog_sha256,
        resume,
        catalog,
        profile,
    } = input;
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

    let progress_path = staging.join("semantic-build-progress.jsonl");
    let mut progress = BufWriter::new(
        File::create(&progress_path)
            .map_err(|e| Error::storage(format!("create semantic progress file: {e}")))?,
    );
    write_progress(
        &mut progress,
        json!({"event":"semantic_build_started", "passage_count":catalog.len()}),
    )?;

    let cache_dir = model_cache_dir_for_staging(staging);
    let model_started = Instant::now();
    let mut embedder = EmbeddingSession::new(profile, &cache_dir, dimensions)?;
    let model_initialization = model_started.elapsed();
    let chunk_size = embedding_chunk_size(profile);
    let embedding_fingerprint = document_embedding_fingerprint(profile, &cache_dir)?;
    let checkpoint_identity = BuildCheckpointIdentity::new(
        build_run_id,
        corpus_snapshot_sha256,
        build_profile_sha256,
        catalog_sha256,
        &embedding_fingerprint,
        dimensions,
        chunk_size,
    );
    let checkpoints = BuildCheckpointStore::open(checkpoint_root, &checkpoint_identity, resume)?;
    let mut embedding_cache =
        EmbeddingReuseCache::open(embedding_cache_root, &embedding_fingerprint, dimensions)?;
    write_progress(
        &mut progress,
        json!({"event":"model_initialization_completed", "duration_ms":model_initialization.as_millis()}),
    )?;

    let vector_artifact_path = staging.join(SEMANTIC_VECTOR_ARTIFACT);
    let artifact_path = staging.join(SEMANTIC_ARTIFACT);
    let options = semantic_index_options(profile, dimensions)?;
    let index = UsearchIndex::new(&options)
        .map_err(|error| Error::storage(format!("create USearch index: {error}")))?;
    index
        .reserve(catalog.len())
        .map_err(|error| Error::storage(format!("reserve USearch index: {error}")))?;
    let mut vector_write = Duration::ZERO;
    let vector_file_started = Instant::now();
    let mut vector_file = File::create(&vector_artifact_path)
        .map_err(|e| Error::storage(format!("create semantic vector artifact: {e}")))?;
    vector_file
        .write_all(SEMANTIC_MAGIC)
        .and_then(|_| vector_file.write_all(&(dimensions as u32).to_le_bytes()))
        .and_then(|_| vector_file.write_all(&(catalog.len() as u64).to_le_bytes()))
        .map_err(|e| Error::storage(format!("write semantic vector header: {e}")))?;
    vector_write += vector_file_started.elapsed();

    let mut passage_embedding = Duration::ZERO;
    let mut passage_embedding_calls = 0;
    let mut vector_index_insertion = Duration::ZERO;
    let mut vector_count = 0;
    let mut checkpoint_chunks_reused = 0;
    let mut checkpoint_chunks_created = 0;
    let mut cache_vectors_reused = 0;
    let mut cache_vectors_created = 0;
    let mut checkpoint_vectors_reused = 0;
    let mut local_duplicate_vectors_reused = 0;
    for (chunk_number, chunk) in catalog.chunks(chunk_size).enumerate() {
        let expected_keys = chunk
            .iter()
            .map(|record| record.vector_key)
            .collect::<Vec<_>>();
        let vectors = if let Some(vectors) = checkpoints.load_chunk(chunk_number, &expected_keys)? {
            checkpoint_chunks_reused += 1;
            checkpoint_vectors_reused += vectors.len();
            vectors
        } else {
            let inputs = chunk
                .iter()
                .map(|record| format!("{}\n{}", record.heading_path.join(" "), record.text))
                .collect::<Vec<_>>();
            let mut vectors = (0..chunk.len()).map(|_| None).collect::<Vec<_>>();
            let mut missing_positions = Vec::new();
            let mut missing_keys = Vec::new();
            let mut missing_inputs = Vec::new();
            let mut first_missing_by_key = std::collections::HashMap::new();
            let mut duplicate_positions = Vec::new();
            for (position, input) in inputs.iter().enumerate() {
                let cache_key = embedding_cache.key_for_input(input);
                if let Some(vector) = embedding_cache.load(&cache_key)? {
                    cache_vectors_reused += 1;
                    vectors[position] = Some(vector);
                } else if let Some(first_position) = first_missing_by_key.get(&cache_key).copied() {
                    duplicate_positions.push((position, first_position));
                } else {
                    first_missing_by_key.insert(cache_key, position);
                    missing_positions.push(position);
                    missing_keys.push(cache_key);
                    missing_inputs.push(input.clone());
                }
            }
            let mut cache_entries = Vec::with_capacity(missing_inputs.len());
            if !missing_inputs.is_empty() {
                let embedding_started = Instant::now();
                let embedded = embedder.embed(missing_inputs)?;
                passage_embedding += embedding_started.elapsed();
                passage_embedding_calls += 1;
                if embedded.len() != missing_positions.len() {
                    return Err(Error::invalid(
                        "embedding count does not match catalog chunk misses",
                    ));
                }
                for ((position, cache_key), vector) in missing_positions
                    .into_iter()
                    .zip(missing_keys)
                    .zip(embedded)
                {
                    cache_entries.push((cache_key, vector.clone()));
                    vectors[position] = Some(vector);
                }
                cache_vectors_created += cache_entries.len();
                embedding_cache.store(&cache_entries)?;
            }
            for (position, first_position) in duplicate_positions {
                vectors[position] = vectors[first_position].clone();
                local_duplicate_vectors_reused += 1;
            }
            let vectors = chunk
                .iter()
                .zip(vectors)
                .map(|(record, vector)| {
                    vector
                        .map(|vector| (record.vector_key, vector))
                        .ok_or_else(|| Error::invalid("embedding reuse left a Passage empty"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            checkpoints.save_chunk(chunk_number, &vectors)?;
            checkpoint_chunks_created += 1;
            vectors
        };
        for (record, (key, vector)) in chunk.iter().zip(vectors) {
            if record.vector_key != key {
                return Err(Error::invalid("embedding checkpoint Passage key mismatch"));
            }
            let write_started = Instant::now();
            vector_file
                .write_all(&key.to_le_bytes())
                .and_then(|_| {
                    vector
                        .iter()
                        .try_for_each(|value| vector_file.write_all(&value.to_le_bytes()))
                })
                .map_err(|e| Error::storage(format!("write semantic vector: {e}")))?;
            vector_write += write_started.elapsed();
            let insert_started = Instant::now();
            index
                .add(key, &vector)
                .map_err(|error| Error::storage(format!("add USearch vector: {error}")))?;
            vector_index_insertion += insert_started.elapsed();
            vector_count += 1;
        }
        write_progress(
            &mut progress,
            json!({
                "event":"passage_chunk_completed",
                "chunk":chunk_number + 1,
                "passages_completed":vector_count,
                "checkpoint_chunks_reused":checkpoint_chunks_reused,
                "checkpoint_chunks_created":checkpoint_chunks_created,
                "embedding_cache_vectors_reused":cache_vectors_reused,
                "embedding_cache_vectors_created":cache_vectors_created,
                "local_duplicate_vectors_reused":local_duplicate_vectors_reused,
                "build_checkpoint_vectors_reused":checkpoint_vectors_reused,
                "passage_embedding_ms":passage_embedding.as_millis(),
                "vector_write_ms":vector_write.as_millis(),
                "vector_index_insertion_ms":vector_index_insertion.as_millis()
            }),
        )?;
    }
    write_progress(
        &mut progress,
        json!({"event":"passage_embedding_completed", "passage_count":vector_count, "call_count":passage_embedding_calls}),
    )?;

    let vector_flush_started = Instant::now();
    vector_file
        .flush()
        .map_err(|e| Error::storage(format!("flush semantic vector artifact: {e}")))?;
    let vector_flush = vector_flush_started.elapsed();
    drop(vector_file);

    let vector_validation_started = Instant::now();
    let first = read_semantic_index(&vector_artifact_path)?;
    validate_semantic_index(&first, catalog, dimensions)?;
    let vector_validation = vector_validation_started.elapsed();
    write_progress(
        &mut progress,
        json!({"event":"vector_validation_completed", "duration_ms":vector_validation.as_millis(), "vector_count":first.vectors.len()}),
    )?;

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
    let query_embedding_started = Instant::now();
    let query_vector = embedder
        .embed(vec![format!("{instruction}{query}")])?
        .into_iter()
        .next()
        .ok_or_else(|| Error::invalid("semantic query embedding was empty"))?;
    let query_embedding = query_embedding_started.elapsed();

    let mut smoke_search = Duration::ZERO;
    let smoke_search_started = Instant::now();
    let first_hits = semantic_hits(&first, catalog, &query_vector, candidate_depth)?;
    smoke_search += smoke_search_started.elapsed();

    let index_persistence_started = Instant::now();
    index
        .save(&artifact_path.to_string_lossy())
        .map_err(|error| Error::storage(format!("save USearch index: {error}")))?;
    let artifact_bytes = fs::metadata(&artifact_path)
        .map_err(|e| Error::storage(format!("stat semantic artifact: {e}")))?
        .len();
    let artifact_sha256 = super::checksums::directory_or_file_digest(&artifact_path)?;
    let index_persistence = index_persistence_started.elapsed();

    let index_reopen_validation_started = Instant::now();
    let reopened = UsearchIndex::restore_view(&artifact_path.to_string_lossy())
        .map_err(|error| Error::storage(format!("reopen USearch index: {error}")))?;
    validate_usearch_index(&reopened, catalog, dimensions, profile)?;
    let index_reopen_validation = index_reopen_validation_started.elapsed();

    let smoke_search_started = Instant::now();
    let reopened_matches = reopened
        .search(&query_vector, candidate_depth)
        .map_err(|error| Error::storage(format!("search reopened USearch index: {error}")))?;
    let reopened_hits = usearch_hits(&reopened_matches, catalog)?;
    let repeated_matches = reopened
        .search(&query_vector, candidate_depth)
        .map_err(|error| Error::storage(format!("repeat USearch smoke search: {error}")))?;
    let repeated_hits = usearch_hits(&repeated_matches, catalog)?;
    smoke_search += smoke_search_started.elapsed();

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
    let stage_timings = SemanticStageTimings {
        model_initialization_ms: model_initialization.as_millis(),
        passage_embedding_ms: passage_embedding.as_millis(),
        passage_embedding_calls,
        query_embedding_ms: query_embedding.as_millis(),
        vector_write_ms: vector_write.as_millis(),
        vector_index_insertion_ms: vector_index_insertion.as_millis(),
        vector_flush_ms: vector_flush.as_millis(),
        vector_validation_ms: vector_validation.as_millis(),
        index_persistence_ms: index_persistence.as_millis(),
        index_reopen_validation_ms: index_reopen_validation.as_millis(),
        smoke_search_ms: smoke_search.as_millis(),
        other_ms: build_duration_ms.saturating_sub(
            model_initialization.as_millis()
                + passage_embedding.as_millis()
                + query_embedding.as_millis()
                + vector_write.as_millis()
                + vector_index_insertion.as_millis()
                + vector_flush.as_millis()
                + vector_validation.as_millis()
                + index_persistence.as_millis()
                + index_reopen_validation.as_millis()
                + smoke_search.as_millis(),
        ),
    };
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
        embedding_execution_provider: profile.embedding_execution_provider.clone(),
        embedding_fingerprint: Some(embedding_fingerprint.clone()),
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
        stage_timings: Some(stage_timings),
        embedding_passages_per_second,
        memory_observation: format!("usearch_memory_bytes={}", reopened.memory_usage()),
        reopened: true,
    };
    write_json(&staging.join("semantic-manifest.json"), &evidence)?;
    write_progress(
        &mut progress,
        json!({"event":"semantic_build_completed", "duration_ms":build_duration_ms, "vector_count":reopened.size()}),
    )?;
    drop(progress);
    fs::remove_file(&progress_path)
        .map_err(|e| Error::storage(format!("remove semantic progress file: {e}")))?;
    Ok((
        evidence,
        SemanticCheckpointStats {
            schema: CHECKPOINT_SCHEMA,
            identity_sha256: checkpoints.identity_sha256().into(),
            chunks_reused: checkpoint_chunks_reused,
            chunks_created: checkpoint_chunks_created,
            embedding_fingerprint,
            cache_vectors_reused,
            cache_vectors_created,
            checkpoint_vectors_reused,
            local_duplicate_vectors_reused,
        },
    ))
}

fn write_progress(progress: &mut BufWriter<File>, mut event: Value) -> Result<(), Error> {
    event["timestamp"] = json!(crate::storage::now());
    serde_json::to_writer(&mut *progress, &event)
        .map_err(|e| Error::storage(format!("serialize semantic progress: {e}")))?;
    progress
        .write_all(b"\n")
        .and_then(|_| progress.flush())
        .map_err(|e| Error::storage(format!("write semantic progress: {e}")))
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
