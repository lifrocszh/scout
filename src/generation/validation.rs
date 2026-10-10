use super::checksums::{directory_digest, directory_or_file_digest};
use super::keyword::register_keyword_analyzer;
use super::profile::validate_model_metadata;
use super::semantic::{open_semantic_index, validate_usearch_index};
use super::semantic_storage::validate_semantic_file;
use crate::storage::parse_jsonl;
use crate::storage::sha256_hex;
use crate::{
    BTreeMap, BTreeSet, BuildProfile, CatalogRecord, Error, GenerationManifest, Path,
    SEMANTIC_ARTIFACT, SEMANTIC_VECTOR_ARTIFACT, SemanticEvidence, fs,
};
use tantivy::Index;

pub(super) fn load_generation_manifest(generation: &Path) -> Result<GenerationManifest, Error> {
    let path = generation.join("generation-manifest.json");
    let bytes = fs::read(&path)
        .map_err(|e| Error::invalid(format!("read Generation manifest {}: {e}", path.display())))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| Error::invalid(format!("parse Generation manifest: {e}")))
}

pub(super) fn manifest_digest(manifest: &GenerationManifest) -> Result<String, Error> {
    let mut unsigned = manifest.clone();
    unsigned.manifest_sha256.clear();
    let bytes = serde_json::to_vec_pretty(&unsigned)
        .map_err(|e| Error::storage(format!("serialize Generation manifest: {e}")))?;
    // Normalize floating-point evidence through the same serde round-trip used by
    // validation so the digest is stable after a manifest is reopened.
    let canonical: GenerationManifest = serde_json::from_slice(&bytes)
        .map_err(|e| Error::storage(format!("normalize Generation manifest: {e}")))?;
    let canonical_bytes = serde_json::to_vec_pretty(&canonical)
        .map_err(|e| Error::storage(format!("serialize normalized Generation manifest: {e}")))?;
    Ok(sha256_hex(&canonical_bytes))
}

pub(super) fn write_generation_manifest(
    path: &Path,
    manifest: &mut GenerationManifest,
) -> Result<(), Error> {
    manifest.manifest_sha256 = manifest_digest(manifest)?;
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| Error::storage(format!("serialize Generation manifest: {e}")))?;
    fs::write(path, bytes)
        .map_err(|e| Error::storage(format!("write Generation manifest {}: {e}", path.display())))
}

pub(crate) fn validate_generation(
    generation: &Path,
    expected_status: &str,
) -> Result<GenerationManifest, Error> {
    if !generation.is_dir() {
        return Err(Error::invalid(format!(
            "Generation directory {} is missing",
            generation.display()
        )));
    }
    let manifest = load_generation_manifest(generation)?;
    if manifest.status != expected_status {
        return Err(Error::invalid(format!(
            "Generation status {:?} is not {expected_status:?}",
            manifest.status
        )));
    }
    if manifest.manifest_sha256 != manifest_digest(&manifest)? {
        return Err(Error::invalid("Generation manifest checksum mismatch"));
    }
    if manifest.catalog_file != "catalog.jsonl" || manifest.checksums_file != "checksums.sha256" {
        return Err(Error::invalid("Generation artifact layout mismatch"));
    }

    let profile_bytes = fs::read(generation.join("build-profile.json"))
        .map_err(|e| Error::invalid(format!("read Generation build profile: {e}")))?;
    let profile: BuildProfile = serde_json::from_slice(&profile_bytes)
        .map_err(|e| Error::invalid(format!("parse Generation build profile: {e}")))?;
    match manifest.schema_version.as_str() {
        "scout.generation-manifest.v1"
            if manifest.components.is_none() && profile.build_mode == crate::BuildMode::Hybrid => {}
        "scout.generation-manifest.v2" if manifest.components.is_some() => {}
        "scout.generation-manifest.v1" => {
            return Err(Error::invalid(
                "legacy Generation manifest must describe a hybrid Generation",
            ));
        }
        "scout.generation-manifest.v2" => {
            return Err(Error::invalid(
                "Generation manifest component declaration is missing",
            ));
        }
        _ => return Err(Error::invalid("unsupported Generation manifest schema")),
    }
    if profile.snapshot_id != manifest.corpus_snapshot_id
        || profile.key_map.as_deref() != Some(manifest.vector_key_map.as_str())
    {
        return Err(Error::invalid("Generation profile compatibility mismatch"));
    }
    let components = manifest.components();
    if components != crate::GenerationComponents::for_mode(profile.build_mode) {
        return Err(Error::invalid(
            "Generation component declaration does not match build profile",
        ));
    }
    validate_model_metadata(&profile)?;

    let catalog_path = generation.join(&manifest.catalog_file);
    let catalog_bytes = fs::read(&catalog_path)
        .map_err(|e| Error::invalid(format!("read Generation catalog: {e}")))?;
    if sha256_hex(&catalog_bytes) != manifest.catalog_sha256 {
        return Err(Error::invalid("Generation catalog checksum mismatch"));
    }
    let catalog = parse_jsonl::<CatalogRecord>(&catalog_bytes, "catalog.jsonl")?;
    drop(catalog_bytes);
    if catalog.len() != manifest.passage_count || catalog.len() != manifest.vector_key_count {
        return Err(Error::invalid("Generation catalog count mismatch"));
    }
    let mut passage_ids = BTreeSet::new();
    let mut vector_keys = BTreeSet::new();
    let mut page_ids = BTreeSet::new();
    let mut page_shapes = BTreeMap::<String, (String, String)>::new();
    let mut passage_ordinals = BTreeSet::new();
    let mut alias_ids = BTreeSet::new();
    for record in &catalog {
        if record.page_id.is_empty()
            || record.passage_id.is_empty()
            || record.source_id.is_empty()
            || record.normalized_final_url.is_empty()
            || !passage_ids.insert(record.passage_id.clone())
            || !vector_keys.insert(record.vector_key)
            || !passage_ordinals.insert((record.page_id.clone(), record.passage_ordinal))
            || record.alias_page_ids.windows(2).any(|ids| ids[0] >= ids[1])
        {
            return Err(Error::invalid("Generation catalog identity mismatch"));
        }
        if let Some(shape) = page_shapes.get(&record.page_id)
            && (shape.0 != record.source_id || shape.1 != record.normalized_final_url)
        {
            return Err(Error::invalid("Generation catalog Page reference mismatch"));
        }
        page_shapes.insert(
            record.page_id.clone(),
            (
                record.source_id.clone(),
                record.normalized_final_url.clone(),
            ),
        );
        page_ids.insert(record.page_id.clone());
        alias_ids.extend(record.alias_page_ids.iter().cloned());
    }
    if alias_ids
        .iter()
        .any(|alias_id| alias_id.is_empty() || page_ids.contains(alias_id))
    {
        return Err(Error::invalid("Generation catalog alias identity mismatch"));
    }
    let expected_key_count = catalog.len();
    // Unique keys with count N and endpoints 1/N prove the set is exactly 1..=N.
    let vector_keys_are_contiguous = vector_keys.len() == expected_key_count
        && (expected_key_count == 0
            || (vector_keys.first().copied() == Some(1)
                && vector_keys.last().copied() == Some(expected_key_count as u64)));
    if !vector_keys_are_contiguous
        || page_ids.len() != manifest.page_count
        || alias_ids.len() != manifest.alias_count
    {
        return Err(Error::invalid(
            "Generation catalog key, Page, or alias count mismatch",
        ));
    }
    drop((
        passage_ids,
        page_ids,
        page_shapes,
        passage_ordinals,
        alias_ids,
    ));

    if components.keyword {
        let artifact = manifest
            .keyword_artifact
            .as_deref()
            .filter(|artifact| *artifact == "keyword")
            .ok_or_else(|| Error::invalid("keyword artifact declaration is missing"))?;
        let expected_digest = manifest
            .keyword_sha256
            .as_deref()
            .ok_or_else(|| Error::invalid("keyword artifact checksum is missing"))?;
        let keyword_path = generation.join(artifact);
        if directory_digest(&keyword_path)? != expected_digest {
            return Err(Error::invalid("keyword artifact checksum mismatch"));
        }
        let mut keyword_index = Index::open_in_dir(&keyword_path)
            .map_err(|e| Error::invalid(format!("reopen keyword artifact: {e}")))?;
        register_keyword_analyzer(&mut keyword_index, &profile)?;
        let keyword_reader = keyword_index
            .reader()
            .map_err(|e| Error::invalid(format!("read keyword artifact: {e}")))?;
        let keyword_count = keyword_reader.searcher().num_docs() as usize;
        if keyword_count != manifest.keyword_count || keyword_count != catalog.len() {
            return Err(Error::invalid("keyword/catalog count mismatch"));
        }
    } else if manifest.keyword_artifact.is_some()
        || manifest.keyword_sha256.is_some()
        || manifest.keyword_count != 0
        || generation.join("keyword").exists()
    {
        return Err(Error::invalid(
            "keyword artifacts exist for an unavailable Generation component",
        ));
    }

    if components.semantic {
        let artifact = manifest
            .semantic_artifact
            .as_deref()
            .filter(|artifact| *artifact == SEMANTIC_ARTIFACT)
            .ok_or_else(|| Error::invalid("semantic artifact declaration is missing"))?;
        let expected_digest = manifest
            .semantic_sha256
            .as_deref()
            .ok_or_else(|| Error::invalid("semantic artifact checksum is missing"))?;
        let semantic_path = generation.join(artifact);
        if directory_or_file_digest(&semantic_path)? != expected_digest {
            return Err(Error::invalid("semantic artifact checksum mismatch"));
        }
        let dimensions = profile
            .dimensions
            .ok_or_else(|| Error::invalid("semantic dimensions missing from profile"))?;
        let vector_count = validate_semantic_file(
            &generation.join(SEMANTIC_VECTOR_ARTIFACT),
            &mut vector_keys,
            dimensions,
        )?;
        let usearch_index = open_semantic_index(&semantic_path, &profile)?;
        validate_usearch_index(&usearch_index, &catalog, dimensions, &profile)?;
        let semantic_manifest_bytes = fs::read(generation.join("semantic-manifest.json"))
            .map_err(|e| Error::invalid(format!("read semantic manifest: {e}")))?;
        let semantic_evidence: SemanticEvidence = serde_json::from_slice(&semantic_manifest_bytes)
            .map_err(|e| Error::invalid(format!("parse semantic manifest: {e}")))?;
        let manifest_evidence = manifest
            .semantic
            .as_ref()
            .ok_or_else(|| Error::invalid("semantic evidence is missing from Generation"))?;
        if semantic_evidence.artifact != artifact
            || semantic_evidence.artifact_sha256 != expected_digest
            || semantic_evidence.catalog_count != catalog.len()
            || semantic_evidence.vector_count != vector_count
            || semantic_evidence.dimensions != dimensions
            || semantic_evidence.embedding_fingerprint != manifest_evidence.embedding_fingerprint
            || !model_metadata_matches(&semantic_evidence, &profile)
            || !model_metadata_matches(manifest_evidence, &profile)
            || !semantic_evidence.reopened
        {
            return Err(Error::invalid(
                "semantic evidence does not match persisted artifacts",
            ));
        }
        if semantic_evidence
            .embedding_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| {
                fingerprint.len() != 64
                    || !fingerprint
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            })
        {
            return Err(Error::invalid("semantic embedding fingerprint is invalid"));
        }
        if manifest.semantic_count != catalog.len()
            || manifest_evidence.vector_count != catalog.len()
            || manifest_evidence.catalog_count != catalog.len()
            || manifest_evidence.dimensions != dimensions
            || manifest_evidence.artifact_sha256 != expected_digest
        {
            return Err(Error::invalid("semantic/catalog compatibility mismatch"));
        }
    } else if manifest.semantic_artifact.is_some()
        || manifest.semantic_sha256.is_some()
        || manifest.semantic.is_some()
        || manifest.semantic_count != 0
        || generation.join(SEMANTIC_ARTIFACT).exists()
        || generation.join(SEMANTIC_VECTOR_ARTIFACT).exists()
        || generation.join("semantic-manifest.json").exists()
        || generation.join("semantic-build-progress.jsonl").exists()
    {
        return Err(Error::invalid(
            "semantic artifacts exist for an unavailable Generation component",
        ));
    }
    verify_checksums(generation)?;
    Ok(manifest)
}

fn model_metadata_matches(evidence: &SemanticEvidence, profile: &BuildProfile) -> bool {
    evidence.model_name == profile.model_name
        && evidence.model_source == profile.model_source
        && evidence.model_revision == profile.model_revision
        && evidence.model_license == profile.model_license
        && evidence.onnx_sha256 == profile.onnx_sha256
        && evidence.tokenizer_sha256 == profile.tokenizer_sha256
        && evidence.model_config_sha256 == profile.model_config_sha256
        && evidence.embedding_backend == profile.embedding_backend
        && evidence.model_max_tokens == profile.model_max_tokens
        && evidence.pooling == profile.pooling
        && evidence.normalization == profile.normalization
        && evidence.query_instruction == profile.query_instruction
        && evidence.document_representation == profile.document_representation
        && profile.dimensions == Some(evidence.dimensions)
        && evidence.embedding_execution_provider == profile.embedding_execution_provider
}

fn verify_checksums(generation: &Path) -> Result<(), Error> {
    let path = generation.join("checksums.sha256");
    let text = fs::read_to_string(&path)
        .map_err(|e| Error::invalid(format!("read Generation checksums: {e}")))?;
    let mut listed = BTreeSet::new();
    for line in text.lines() {
        let Some((expected, relative)) = line.split_once("  ") else {
            return Err(Error::invalid("invalid Generation checksum line"));
        };
        let relative_path = Path::new(relative);
        if relative_path.is_absolute()
            || relative_path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(Error::invalid("invalid Generation checksum path"));
        }
        let actual = super::checksums::file_digest(&generation.join(relative_path))
            .map_err(|e| Error::invalid(format!("read checksummed artifact {relative}: {e}")))?;
        if actual != expected || !listed.insert(relative.to_string()) {
            return Err(Error::invalid(format!(
                "Generation checksum mismatch for {relative}"
            )));
        }
    }
    let mut actual = BTreeSet::new();
    collect_file_paths(generation, generation, &mut actual)?;
    actual.remove("checksums.sha256");
    if listed != actual {
        return Err(Error::invalid("Generation checksum coverage mismatch"));
    }
    Ok(())
}

fn collect_file_paths(
    root: &Path,
    current: &Path,
    files: &mut BTreeSet<String>,
) -> Result<(), Error> {
    for entry in
        fs::read_dir(current).map_err(|e| Error::storage(format!("read index directory: {e}")))?
    {
        let path = entry
            .map_err(|e| Error::storage(format!("read index entry: {e}")))?
            .path();
        if path.is_dir() {
            collect_file_paths(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| Error::storage(format!("relative index path: {e}")))?
                .to_string_lossy()
                .replace('\\', "/");
            files.insert(relative);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::SemanticEvidence;

    #[test]
    fn absent_embedding_provider_is_omitted_for_legacy_manifest_digest() {
        let evidence: SemanticEvidence = serde_json::from_str(
            r#"{
                "schema_version": "scout.semantic.usearch.v1",
                "backend": "fastembed-usearch",
                "artifact": "semantic.usearch",
                "reopen_mode": "restore_view-mmap",
                "embedding_backend": "fastembed",
                "dimensions": 384,
                "catalog_count": 1,
                "vector_count": 1,
                "candidate_depth": 1,
                "query": "test",
                "query_hit_count": 0,
                "returned_identities": [],
                "artifact_bytes": 0,
                "artifact_sha256": "",
                "build_duration_ms": 0,
                "embedding_passages_per_second": 0.0,
                "memory_observation": "",
                "reopened": true
            }"#,
        )
        .expect("parse legacy semantic evidence");

        let serialized = serde_json::to_value(evidence).expect("serialize semantic evidence");
        assert!(serialized.get("embedding_execution_provider").is_none());
    }
}
