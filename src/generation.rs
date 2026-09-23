use super::crawl::page_id;
use super::search::register_keyword_analyzer;
use super::storage::{
    jsonl, new_run_id, normalize_text, now, parse_jsonl, sha256_hex, trim_snippet, write_if_absent,
    write_json,
};
use super::{
    BTreeMap, BTreeSet, BuildProfile, CatalogRecord, Digest, EmbeddingModel, Error, File,
    GenerationManifest, GenerationPointer, HashMap, HashSet, Index, IndexRecordOption, Instant,
    MAX_MODEL_PASSAGE_TOKENS, MetricKind, OpenOptions, Ordering, OsStr, Path, PathBuf,
    RawBuildProfile, SEMANTIC_ARTIFACT, SEMANTIC_MAGIC, SEMANTIC_VECTOR_ARTIFACT, SNAPSHOT_SCHEMA,
    STORED, STRING, ScalarKind, Schema, SemanticEvidence, SemanticHit, Sha256, StoredAlias,
    StoredPage, StoredPassage, StoredSemanticIndex, StoredSnapshotManifest, TantivyDocument,
    TextEmbedding, TextFieldIndexing, TextInitOptions, TextOptions, UsearchIndex,
    UsearchIndexOptions, Value, fs, io, json,
};

pub(crate) fn activate_generation(data_dir: &Path, generation_id: &str) -> Result<Value, Error> {
    let _lock = GenerationLock::acquire(data_dir)?;
    let candidate = find_generation_path(data_dir, generation_id)?;
    let sealed = if candidate
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == OsStr::new(".staging"))
    {
        seal_generation(data_dir, &candidate, generation_id)?
    } else {
        candidate
    };
    let manifest = validate_generation(&sealed, "sealed")?;
    let pointer = GenerationPointer {
        generation_id: manifest.generation_id.clone(),
        manifest_sha256: manifest.manifest_sha256.clone(),
    };
    let current = read_pointer(data_dir, "CURRENT").ok().flatten();
    if current
        .as_ref()
        .is_some_and(|current| current.generation_id == pointer.generation_id)
    {
        return Ok(json!({
            "generation_id": pointer.generation_id,
            "manifest_sha256": pointer.manifest_sha256,
            "changed": false,
            "sealed_path": sealed,
        }));
    }
    if let Some(current) = current.filter(|current| validate_pointer(data_dir, current).is_ok()) {
        write_pointer_atomic(data_dir, "PREVIOUS", &current)?;
    }
    write_pointer_atomic(data_dir, "CURRENT", &pointer)?;
    Ok(json!({
        "generation_id": pointer.generation_id,
        "manifest_sha256": pointer.manifest_sha256,
        "changed": true,
        "sealed_path": sealed,
    }))
}

pub(crate) fn recover_generation(data_dir: &Path) -> Result<Value, Error> {
    let current = read_pointer(data_dir, "CURRENT").ok().flatten();
    if let Some(pointer) = current.as_ref()
        && validate_pointer(data_dir, pointer).is_ok()
    {
        return Ok(json!({
            "generation_id": pointer.generation_id,
            "manifest_sha256": pointer.manifest_sha256,
            "fallback": false,
            "ready": true,
        }));
    }

    let previous = read_pointer(data_dir, "PREVIOUS").ok().flatten();
    if let Some(pointer) = previous.as_ref()
        && validate_pointer(data_dir, pointer).is_ok()
    {
        write_pointer_atomic(data_dir, "CURRENT", pointer)?;
        return Ok(json!({
            "generation_id": pointer.generation_id,
            "manifest_sha256": pointer.manifest_sha256,
            "fallback": true,
            "ready": true,
        }));
    }

    let generations = data_dir.join("generations");
    let mut paths = fs::read_dir(&generations)
        .map_err(|e| Error::storage(format!("read Generation directory: {e}")))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .is_some_and(|name| name != OsStr::new(".staging"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        let Ok(manifest) = validate_generation(&path, "sealed") else {
            continue;
        };
        let pointer = GenerationPointer {
            generation_id: manifest.generation_id,
            manifest_sha256: manifest.manifest_sha256,
        };
        write_pointer_atomic(data_dir, "CURRENT", &pointer)?;
        return Ok(json!({
            "generation_id": pointer.generation_id,
            "manifest_sha256": pointer.manifest_sha256,
            "fallback": true,
            "ready": true,
        }));
    }
    Err(Error::invalid("no valid sealed Generation is available"))
}

pub(crate) fn prune_generations(
    data_dir: &Path,
    explicitly_retained: &[String],
) -> Result<Value, Error> {
    let _lock = GenerationLock::acquire(data_dir)?;
    let current = read_pointer(data_dir, "CURRENT").ok().flatten();
    let previous = read_pointer(data_dir, "PREVIOUS").ok().flatten();
    let mut protected = explicitly_retained.iter().cloned().collect::<HashSet<_>>();
    if let Some(pointer) = current.as_ref() {
        protected.insert(pointer.generation_id.clone());
    }
    if let Some(pointer) = previous.as_ref() {
        protected.insert(pointer.generation_id.clone());
    }
    let generations = data_dir.join("generations");
    let entries = fs::read_dir(&generations)
        .map_err(|e| Error::storage(format!("read Generation directory: {e}")))?;
    let mut removed = Vec::new();
    let mut kept = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|e| Error::storage(format!("read Generation entry: {e}")))?
            .path();
        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        if !path.is_dir() || name == ".staging" {
            continue;
        }
        if protected.contains(name)
            || path.join(".pinned").exists()
            || path.join(".in-use").exists()
            || validate_generation(&path, "sealed").is_err()
        {
            kept.push(name.to_string());
            continue;
        }
        fs::remove_dir_all(&path)
            .map_err(|e| Error::storage(format!("remove Generation {}: {e}", path.display())))?;
        removed.push(name.to_string());
    }
    removed.sort();
    kept.sort();
    Ok(json!({"removed": removed, "kept": kept}))
}

struct GenerationLock {
    path: PathBuf,
}

impl GenerationLock {
    fn acquire(data_dir: &Path) -> Result<Self, Error> {
        let generations = data_dir.join("generations");
        fs::create_dir_all(&generations)
            .map_err(|e| Error::storage(format!("create Generation directory: {e}")))?;
        let path = generations.join(".generation.lock");
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => Ok(Self { path }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Err(Error::storage(
                "another Generation operation is in progress",
            )),
            Err(error) => Err(Error::storage(format!("create Generation lock: {error}"))),
        }
    }
}

impl Drop for GenerationLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn find_generation_path(data_dir: &Path, generation_id: &str) -> Result<PathBuf, Error> {
    if generation_id.is_empty() || generation_id.contains('/') || generation_id.contains('\\') {
        return Err(Error::invalid("generation ID is not a safe path component"));
    }
    let generations = data_dir.join("generations");
    let sealed = generations.join(generation_id);
    if sealed.is_dir() {
        return Ok(sealed);
    }
    let staging = generations.join(".staging");
    let entries = fs::read_dir(&staging)
        .map_err(|e| Error::invalid(format!("read Generation staging directory: {e}")))?;
    let mut matches = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|e| Error::storage(format!("read Generation staging entry: {e}")))?
            .path();
        if !path.is_dir() {
            continue;
        }
        let Ok(manifest) = load_generation_manifest(&path) else {
            continue;
        };
        if manifest.generation_id == generation_id {
            matches.push(path);
        }
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(Error::invalid(format!(
            "Generation {generation_id:?} not found"
        ))),
        _ => Err(Error::invalid(format!(
            "Generation {generation_id:?} is ambiguous"
        ))),
    }
}

fn seal_generation(data_dir: &Path, staging: &Path, generation_id: &str) -> Result<PathBuf, Error> {
    let mut manifest = validate_generation(staging, "validated")?;
    if manifest.generation_id != generation_id {
        return Err(Error::invalid(
            "staging Generation ID does not match activation request",
        ));
    }
    manifest.status = "sealed".into();
    write_generation_manifest(&staging.join("generation-manifest.json"), &mut manifest)?;
    refresh_checksums(staging)?;
    let final_path = data_dir.join("generations").join(generation_id);
    fs::rename(staging, &final_path)
        .map_err(|e| Error::storage(format!("seal Generation {}: {e}", final_path.display())))?;
    validate_generation(&final_path, "sealed")?;
    Ok(final_path)
}

fn load_generation_manifest(generation: &Path) -> Result<GenerationManifest, Error> {
    let path = generation.join("generation-manifest.json");
    let bytes = fs::read(&path)
        .map_err(|e| Error::invalid(format!("read Generation manifest {}: {e}", path.display())))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| Error::invalid(format!("parse Generation manifest: {e}")))
}

fn manifest_digest(manifest: &GenerationManifest) -> Result<String, Error> {
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

fn write_generation_manifest(path: &Path, manifest: &mut GenerationManifest) -> Result<(), Error> {
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
    if manifest.catalog_file != "catalog.jsonl"
        || manifest.keyword_artifact != "keyword"
        || manifest.semantic_artifact != SEMANTIC_ARTIFACT
        || manifest.checksums_file != "checksums.sha256"
    {
        return Err(Error::invalid("Generation artifact layout mismatch"));
    }

    let profile_bytes = fs::read(generation.join("build-profile.json"))
        .map_err(|e| Error::invalid(format!("read Generation build profile: {e}")))?;
    let profile: BuildProfile = serde_json::from_slice(&profile_bytes)
        .map_err(|e| Error::invalid(format!("parse Generation build profile: {e}")))?;
    if profile.snapshot_id != manifest.corpus_snapshot_id
        || profile.key_map.as_deref() != Some(manifest.vector_key_map.as_str())
    {
        return Err(Error::invalid("Generation profile compatibility mismatch"));
    }

    let catalog_path = generation.join(&manifest.catalog_file);
    let catalog_bytes = fs::read(&catalog_path)
        .map_err(|e| Error::invalid(format!("read Generation catalog: {e}")))?;
    if sha256_hex(&catalog_bytes) != manifest.catalog_sha256 {
        return Err(Error::invalid("Generation catalog checksum mismatch"));
    }
    let catalog = parse_jsonl::<CatalogRecord>(&catalog_bytes, "catalog.jsonl")?;
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
    let expected_keys = (1..=catalog.len() as u64).collect::<BTreeSet<_>>();
    if vector_keys != expected_keys
        || page_ids.len() != manifest.page_count
        || alias_ids.len() != manifest.alias_count
    {
        return Err(Error::invalid(
            "Generation catalog key, Page, or alias count mismatch",
        ));
    }

    let keyword_path = generation.join(&manifest.keyword_artifact);
    if directory_digest(&keyword_path)? != manifest.keyword_sha256 {
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

    let semantic_path = generation.join(&manifest.semantic_artifact);
    if directory_or_file_digest(&semantic_path)? != manifest.semantic_sha256 {
        return Err(Error::invalid("semantic artifact checksum mismatch"));
    }
    let vector_bytes = fs::read(generation.join(SEMANTIC_VECTOR_ARTIFACT))
        .map_err(|e| Error::invalid(format!("read semantic vector artifact: {e}")))?;
    let semantic_index = read_semantic_bytes(&vector_bytes)?;
    let dimensions = profile
        .dimensions
        .ok_or_else(|| Error::invalid("semantic dimensions missing from profile"))?;
    validate_semantic_index(&semantic_index, &catalog, dimensions)?;
    let usearch_index = open_semantic_index(&semantic_path, &profile)?;
    validate_usearch_index(&usearch_index, &catalog, dimensions, &profile)?;
    let semantic_manifest_bytes = fs::read(generation.join("semantic-manifest.json"))
        .map_err(|e| Error::invalid(format!("read semantic manifest: {e}")))?;
    let semantic_evidence: SemanticEvidence = serde_json::from_slice(&semantic_manifest_bytes)
        .map_err(|e| Error::invalid(format!("parse semantic manifest: {e}")))?;
    if semantic_evidence.artifact != manifest.semantic_artifact
        || semantic_evidence.artifact_sha256 != manifest.semantic_sha256
        || semantic_evidence.catalog_count != catalog.len()
        || semantic_evidence.vector_count != semantic_index.vectors.len()
        || semantic_evidence.dimensions != dimensions
        || !semantic_evidence.reopened
    {
        return Err(Error::invalid(
            "semantic evidence does not match persisted artifacts",
        ));
    }
    if manifest.semantic_count != catalog.len()
        || manifest.semantic.vector_count != catalog.len()
        || manifest.semantic.catalog_count != catalog.len()
        || manifest.semantic.dimensions != dimensions
        || manifest.semantic.artifact_sha256 != manifest.semantic_sha256
    {
        return Err(Error::invalid("semantic/catalog compatibility mismatch"));
    }
    verify_checksums(generation)?;
    Ok(manifest)
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
        let bytes = fs::read(generation.join(relative_path))
            .map_err(|e| Error::invalid(format!("read checksummed artifact {relative}: {e}")))?;
        if sha256_hex(&bytes) != expected || !listed.insert(relative.to_string()) {
            return Err(Error::invalid(format!(
                "Generation checksum mismatch for {relative}"
            )));
        }
    }
    let mut files = Vec::new();
    collect_files(generation, generation, &mut files)?;
    let actual = files
        .into_iter()
        .map(|(relative, _)| relative)
        .filter(|relative| relative != "checksums.sha256")
        .collect::<BTreeSet<_>>();
    if listed != actual {
        return Err(Error::invalid("Generation checksum coverage mismatch"));
    }
    Ok(())
}

pub(crate) fn read_pointer(
    data_dir: &Path,
    name: &str,
) -> Result<Option<GenerationPointer>, Error> {
    let path = data_dir.join(name);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::storage(format!("read {name} pointer: {error}"))),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| Error::invalid(format!("parse {name} pointer: {e}")))
}

pub(crate) fn validate_pointer(data_dir: &Path, pointer: &GenerationPointer) -> Result<(), Error> {
    let path = data_dir.join("generations").join(&pointer.generation_id);
    let manifest = validate_generation(&path, "sealed")?;
    if manifest.manifest_sha256 != pointer.manifest_sha256 {
        return Err(Error::invalid("Generation pointer digest mismatch"));
    }
    Ok(())
}

fn write_pointer_atomic(
    data_dir: &Path,
    name: &str,
    pointer: &GenerationPointer,
) -> Result<(), Error> {
    let bytes = serde_json::to_vec_pretty(pointer)
        .map_err(|e| Error::storage(format!("serialize {name} pointer: {e}")))?;
    fs::create_dir_all(data_dir)
        .map_err(|e| Error::storage(format!("create data directory: {e}")))?;
    let temporary = data_dir.join(format!(".{name}.tmp-{}", new_run_id()));
    fs::write(&temporary, bytes)
        .map_err(|e| Error::storage(format!("write temporary {name} pointer: {e}")))?;
    fs::rename(&temporary, data_dir.join(name))
        .map_err(|e| Error::storage(format!("publish {name} pointer: {e}")))?;
    if let Ok(directory) = File::open(data_dir) {
        let _ = directory.sync_all();
    }
    Ok(())
}

pub(crate) fn build_generation(
    data_dir: &Path,
    corpus_id: &str,
    run_id: &str,
) -> Result<Value, Error> {
    let corpus_dir = data_dir.join("corpora").join(corpus_id);
    let manifest_path = corpus_dir.join("snapshot-manifest.json");
    let manifest_bytes = fs::read(&manifest_path).map_err(|e| {
        Error::invalid(format!(
            "read Corpus snapshot {}: {e}",
            manifest_path.display()
        ))
    })?;
    let snapshot: StoredSnapshotManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| Error::invalid(format!("parse Corpus snapshot manifest: {e}")))?;
    if snapshot.schema_version != SNAPSHOT_SCHEMA
        || snapshot.status != "complete"
        || snapshot.snapshot_id != corpus_id
    {
        return Err(Error::invalid(
            "Corpus snapshot is not complete or ID does not match",
        ));
    }

    let pages_bytes = fs::read(corpus_dir.join("pages.jsonl"))
        .map_err(|e| Error::invalid(format!("read pages.jsonl: {e}")))?;
    let aliases_bytes = fs::read(corpus_dir.join("aliases.jsonl"))
        .map_err(|e| Error::invalid(format!("read aliases.jsonl: {e}")))?;
    let passages_bytes = fs::read(corpus_dir.join("passages.jsonl"))
        .map_err(|e| Error::invalid(format!("read passages.jsonl: {e}")))?;
    if sha256_hex(&pages_bytes) != snapshot.pages_jsonl_sha256
        || sha256_hex(&aliases_bytes) != snapshot.aliases_jsonl_sha256
        || sha256_hex(&passages_bytes) != snapshot.passages_jsonl_sha256
    {
        return Err(Error::invalid("Corpus snapshot checksum mismatch"));
    }
    let pages = parse_jsonl::<StoredPage>(&pages_bytes, "pages.jsonl")?;
    let aliases = parse_jsonl::<StoredAlias>(&aliases_bytes, "aliases.jsonl")?;
    let mut passages = parse_jsonl::<StoredPassage>(&passages_bytes, "passages.jsonl")?;
    if pages.len() != snapshot.page_count
        || aliases.len() != snapshot.alias_count
        || passages.len() != snapshot.passage_count
    {
        return Err(Error::invalid("Corpus snapshot count mismatch"));
    }
    validate_stored_snapshot(&pages, &aliases, &passages)?;

    let profile_path = data_dir.join("config").join("index.toml");
    let profile_text = fs::read_to_string(&profile_path).map_err(|e| {
        Error::invalid(format!(
            "read build profile {}: {e}",
            profile_path.display()
        ))
    })?;
    let raw: RawBuildProfile = toml::from_str(&profile_text)
        .map_err(|e| Error::invalid(format!("parse build profile: {e}")))?;
    let profile = build_profile(raw, corpus_id)?;
    let profile_bytes = serde_json::to_vec_pretty(&profile)
        .map_err(|e| Error::storage(format!("serialize build profile: {e}")))?;
    let profile_digest = sha256_hex(&profile_bytes);

    let mut aliases_by_page = BTreeMap::<String, Vec<String>>::new();
    for alias in aliases {
        aliases_by_page
            .entry(alias.representative_page_id)
            .or_default()
            .push(alias.alias_page_id);
    }
    for ids in aliases_by_page.values_mut() {
        ids.sort();
        ids.dedup();
    }
    passages.sort_by(|a, b| {
        a.page_id
            .cmp(&b.page_id)
            .then_with(|| a.passage_id.cmp(&b.passage_id))
    });
    let page_ids = pages
        .iter()
        .map(|page| page.page_id.as_str())
        .collect::<BTreeSet<_>>();
    if passages
        .iter()
        .any(|passage| !page_ids.contains(passage.page_id.as_str()))
    {
        return Err(Error::invalid("Passage references unknown Page"));
    }
    let mut catalog = Vec::with_capacity(passages.len());
    for (index, passage) in passages.iter().enumerate() {
        let text = normalize_text(&passage.content);
        catalog.push(CatalogRecord {
            page_id: passage.page_id.clone(),
            passage_id: passage.passage_id.clone(),
            source_id: passage.source_id.clone(),
            normalized_final_url: passage.normalized_final_url.clone(),
            title: passage.title.clone(),
            heading_path: passage.heading_path.clone(),
            passage_ordinal: passage.ordinal,
            display_text: text.clone(),
            snippet: trim_snippet(&text, profile.snippet_limit),
            text,
            alias_page_ids: aliases_by_page
                .get(&passage.page_id)
                .cloned()
                .unwrap_or_default(),
            vector_key: (index + 1) as u64,
        });
    }
    let catalog_bytes = jsonl(&catalog)?;
    let catalog_digest = sha256_hex(&catalog_bytes);
    let snapshot_digest = sha256_hex(&manifest_bytes);
    let generation_id = format!(
        "generation-{}",
        sha256_hex(
            &[
                corpus_id.as_bytes(),
                b"\0",
                snapshot_digest.as_bytes(),
                b"\0",
                profile_digest.as_bytes(),
                b"\0",
                catalog_digest.as_bytes(),
            ]
            .concat(),
        )
    );
    let staging_root = data_dir.join("generations").join(".staging");
    let staging = staging_root.join(run_id);
    fs::create_dir_all(&staging)
        .map_err(|e| Error::storage(format!("create Generation staging directory: {e}")))?;
    write_if_absent(&staging.join("build-profile.json"), &profile_bytes)?;
    write_if_absent(&staging.join("catalog.jsonl"), &catalog_bytes)?;
    let (keyword_digest, keyword_count, keyword_duration_ms, keyword_artifact_bytes) =
        build_keyword_index(&staging, &catalog, &profile)?;
    let semantic = build_semantic_index(&staging, &catalog, &profile)?;
    let mut generation = GenerationManifest {
        schema_version: "scout.generation-manifest.v1".into(),
        generation_id: generation_id.clone(),
        status: "validated".into(),
        build_run_id: run_id.into(),
        corpus_snapshot_id: corpus_id.into(),
        corpus_snapshot_sha256: snapshot_digest,
        crawl_manifest_sha256: snapshot.crawl_manifest_sha256,
        source_config_sha256: snapshot.source_config_sha256,
        extraction_schema: profile.extraction_schema.clone(),
        passage_schema: profile.passage_schema.clone(),
        build_config_sha256: profile_digest,
        catalog_sha256: catalog_digest,
        catalog_file: "catalog.jsonl".into(),
        keyword_artifact: "keyword".into(),
        keyword_sha256: keyword_digest,
        keyword_count,
        semantic_artifact: SEMANTIC_ARTIFACT.into(),
        semantic_sha256: semantic.artifact_sha256.clone(),
        semantic_count: semantic.vector_count,
        semantic: semantic.clone(),
        checksums_file: "checksums.sha256".into(),
        page_count: pages.len(),
        passage_count: catalog.len(),
        alias_count: snapshot.alias_count,
        vector_key_count: catalog.len(),
        vector_key_map: "one-based-catalog-order-page-id-passage-id".into(),
        created_at_utc: now(),
        manifest_sha256: String::new(),
    };
    generation.manifest_sha256 = manifest_digest(&generation)?;
    write_json(&staging.join("generation-manifest.json"), &generation)?;
    let checksums_sha256 = write_checksums(&staging)?;
    // Build success is meaningful only after reopening every persisted artifact
    // through the same full validator activation uses. This also keeps a bad
    // staging directory from being reported as a usable Generation.
    let validated = validate_generation(&staging, "validated")?;
    Ok(json!({
        "generation_id": validated.generation_id,
        "staging_path": staging,
        "page_count": validated.page_count,
        "passage_count": validated.passage_count,
        "catalog_sha256": validated.catalog_sha256,
        "keyword": {
            "artifact": generation.keyword_artifact.clone(),
            "artifact_bytes": keyword_artifact_bytes,
            "build_duration_ms": keyword_duration_ms,
            "count": keyword_count,
        },
        "semantic": validated.semantic,
        "checksums_sha256": checksums_sha256,
    }))
}

fn validate_stored_snapshot(
    pages: &[StoredPage],
    aliases: &[StoredAlias],
    passages: &[StoredPassage],
) -> Result<(), Error> {
    let mut pages_by_id = BTreeMap::new();
    let mut page_urls = BTreeSet::new();
    for page in pages {
        if page.page_id.is_empty()
            || page.source_id.is_empty()
            || page.normalized_final_url.is_empty()
            || page.content_identity.is_empty()
            || page.page_id != page_id(&page.normalized_final_url)
            || page.content_sha256 != sha256_hex(page.content_identity.as_bytes())
            || pages_by_id.insert(page.page_id.as_str(), page).is_some()
            || !page_urls.insert((page.source_id.as_str(), page.normalized_final_url.as_str()))
        {
            return Err(Error::invalid("Corpus Page identity or content mismatch"));
        }
    }

    let mut alias_ids = BTreeSet::new();
    let mut alias_urls = BTreeSet::new();
    for alias in aliases {
        let Some(representative) = pages_by_id.get(alias.representative_page_id.as_str()) else {
            return Err(Error::invalid(
                "Corpus alias references unknown representative Page",
            ));
        };
        if alias.alias_url.is_empty()
            || alias.alias_page_id.is_empty()
            || !matches!(alias.alias_kind.as_str(), "exact-alias" | "redirect-alias")
            || alias.alias_page_id == alias.representative_page_id
            || alias.alias_page_id != page_id(&alias.alias_url)
            || alias.source_id != representative.source_id
            || alias.alias_url == representative.normalized_final_url
            || pages_by_id.contains_key(alias.alias_page_id.as_str())
            || !alias_ids.insert(alias.alias_page_id.as_str())
            || !alias_urls.insert((alias.source_id.as_str(), alias.alias_url.as_str()))
        {
            return Err(Error::invalid(
                "Corpus alias identity or reference mismatch",
            ));
        }
    }

    let mut passage_ids = BTreeSet::new();
    let mut passage_ordinals = BTreeSet::new();
    for passage in passages {
        let Some(page) = pages_by_id.get(passage.page_id.as_str()) else {
            return Err(Error::invalid("Corpus Passage references unknown Page"));
        };
        let expected_passage_id = sha256_hex(
            format!(
                "scout.passage.v1\0{}{}{}",
                passage.page_id, page.content_identity, passage.ordinal
            )
            .as_bytes(),
        );
        if passage.passage_id.is_empty()
            || passage.passage_id != expected_passage_id
            || !passage_ids.insert(passage.passage_id.as_str())
            || passage.source_id != page.source_id
            || passage.normalized_final_url != page.normalized_final_url
            || passage.content.is_empty()
            || passage.content_sha256 != sha256_hex(passage.content.as_bytes())
            || !passage_ordinals.insert((passage.page_id.as_str(), passage.ordinal))
        {
            return Err(Error::invalid(
                "Corpus Passage identity or reference mismatch",
            ));
        }
    }
    Ok(())
}

fn build_keyword_index(
    staging: &Path,
    catalog: &[CatalogRecord],
    profile: &BuildProfile,
) -> Result<(String, usize, u128, u64), Error> {
    let started = Instant::now();
    let directory = staging.join("keyword");
    fs::create_dir_all(&directory)
        .map_err(|e| Error::storage(format!("create Tantivy directory: {e}")))?;
    let mut builder = Schema::builder();
    let text_options = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(&profile.analyzer_id)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
        .set_stored();
    let title = builder.add_text_field("title", text_options.clone());
    let heading = builder.add_text_field("heading", text_options.clone());
    let body = builder.add_text_field("body", text_options);
    let page_id = builder.add_text_field("page_id", STRING | STORED);
    let passage_id = builder.add_text_field("passage_id", STRING | STORED);
    let source_id = builder.add_text_field("source_id", STRING | STORED);
    let url = builder.add_text_field("url", STRING | STORED);
    let snippet = builder.add_text_field("snippet", STORED);
    let schema = builder.build();
    let mut index = Index::create_in_dir(&directory, schema.clone())
        .map_err(|e| Error::storage(format!("create Tantivy index: {e}")))?;
    register_keyword_analyzer(&mut index, profile)?;
    let mut writer = index
        .writer_with_num_threads(profile.worker_count.max(1), profile.writer_memory_bytes)
        .map_err(|e| Error::storage(format!("open Tantivy writer: {e}")))?;
    for record in catalog {
        let mut document = TantivyDocument::default();
        document.add_text(title, &record.title);
        document.add_text(heading, record.heading_path.join(" "));
        document.add_text(body, &record.text);
        document.add_text(page_id, &record.page_id);
        document.add_text(passage_id, &record.passage_id);
        document.add_text(source_id, &record.source_id);
        document.add_text(url, &record.normalized_final_url);
        document.add_text(snippet, &record.snippet);
        writer
            .add_document(document)
            .map_err(|e| Error::storage(format!("add Tantivy document: {e}")))?;
    }
    writer
        .commit()
        .map_err(|e| Error::storage(format!("commit Tantivy index: {e}")))?;
    writer
        .wait_merging_threads()
        .map_err(|e| Error::storage(format!("merge Tantivy index: {e}")))?;
    let reader = index
        .reader()
        .map_err(|e| Error::storage(format!("open Tantivy reader: {e}")))?;
    let count = reader.searcher().num_docs() as usize;
    if count != catalog.len() {
        return Err(Error::invalid(format!(
            "Tantivy document count {count} does not match catalog {}",
            catalog.len()
        )));
    }
    write_json(
        &directory.join("build.json"),
        &json!({
            "analyzer_id": profile.analyzer_id,
            "title_boost": profile.title_boost,
            "heading_boost": profile.heading_boost,
            "body_boost": profile.body_boost,
            "candidate_depth": profile.candidate_depth,
            "document_count": count,
            "bm25": "tantivy-fixed-defaults"
        }),
    )?;
    let artifact_bytes = directory_bytes(&directory)?;
    Ok((
        directory_digest(&directory)?,
        count,
        started.elapsed().as_millis(),
        artifact_bytes,
    ))
}

fn build_semantic_index(
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

    let vectors = embed_catalog(staging, catalog, profile, dimensions)?;
    let mut bytes = Vec::with_capacity(
        SEMANTIC_MAGIC.len()
            + 12
            + vectors
                .len()
                .saturating_mul(8 + dimensions.saturating_mul(std::mem::size_of::<f32>())),
    );
    bytes.extend_from_slice(SEMANTIC_MAGIC);
    bytes.extend_from_slice(&(dimensions as u32).to_le_bytes());
    bytes.extend_from_slice(&(vectors.len() as u64).to_le_bytes());
    for (key, vector) in &vectors {
        bytes.extend_from_slice(&key.to_le_bytes());
        for value in vector {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }

    let vector_artifact_path = staging.join(SEMANTIC_VECTOR_ARTIFACT);
    write_if_absent(&vector_artifact_path, &bytes)?;

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
    let query_vector = embed_query(
        &query,
        profile,
        &model_cache_dir_for_staging(staging),
        dimensions,
    )?;
    let first_hits = semantic_hits(&first, catalog, &query_vector, candidate_depth)?;

    let artifact_path = staging.join(SEMANTIC_ARTIFACT);
    let options = semantic_index_options(profile, dimensions)?;
    let index = UsearchIndex::new(&options)
        .map_err(|error| Error::storage(format!("create USearch index: {error}")))?;
    index
        .reserve(catalog.len())
        .map_err(|error| Error::storage(format!("reserve USearch index: {error}")))?;
    for (key, vector) in &vectors {
        index
            .add(*key, vector)
            .map_err(|error| Error::storage(format!("add USearch vector: {error}")))?;
    }
    index
        .save(&artifact_path.to_string_lossy())
        .map_err(|error| Error::storage(format!("save USearch index: {error}")))?;
    let artifact_bytes = fs::metadata(&artifact_path)
        .map_err(|e| Error::storage(format!("stat semantic artifact: {e}")))?
        .len();
    let artifact_sha256 = directory_or_file_digest(&artifact_path)?;
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

fn model_cache_dir_for_staging(staging: &Path) -> PathBuf {
    staging
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .join("models")
}

fn embed_catalog(
    staging: &Path,
    catalog: &[CatalogRecord],
    profile: &BuildProfile,
    dimensions: usize,
) -> Result<Vec<(u64, Vec<f32>)>, Error> {
    let inputs = catalog
        .iter()
        .map(|record| format!("{}\n{}", record.heading_path.join(" "), record.text))
        .collect::<Vec<_>>();
    let embeddings = embed_texts(
        inputs,
        profile,
        &model_cache_dir_for_staging(staging),
        dimensions,
    )?;
    Ok(catalog
        .iter()
        .zip(embeddings)
        .map(|(record, vector)| (record.vector_key, vector))
        .collect())
}

pub(crate) fn embed_query(
    query: &str,
    profile: &BuildProfile,
    cache_dir: &Path,
    dimensions: usize,
) -> Result<Vec<f32>, Error> {
    let instruction = profile.query_instruction.as_deref().unwrap_or_default();
    let input = format!("{instruction}{query}");
    embed_texts(vec![input], profile, cache_dir, dimensions)?
        .into_iter()
        .next()
        .ok_or_else(|| Error::invalid("semantic query embedding was empty"))
}

fn embed_texts(
    inputs: Vec<String>,
    profile: &BuildProfile,
    cache_dir: &Path,
    dimensions: usize,
) -> Result<Vec<Vec<f32>>, Error> {
    match profile.embedding_backend.as_str() {
        "deterministic" => Ok(inputs
            .iter()
            .map(|input| deterministic_embedding(input, dimensions))
            .collect()),
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
            let batch = profile.embedding_batch_size.unwrap_or(1);
            let mut model = TextEmbedding::try_new(
                TextInitOptions::new(EmbeddingModel::BGESmallENV15)
                    .with_cache_dir(cache_dir.to_path_buf())
                    .with_max_length(max_length)
                    .with_intra_threads(threads)
                    .with_show_download_progress(false),
            )
            .map_err(|error| Error::storage(format!("initialize FastEmbed model: {error}")))?;
            let vectors = model.embed(inputs, Some(batch)).map_err(|error| {
                Error::storage(format!("embed passages with FastEmbed: {error}"))
            })?;
            if vectors.iter().any(|vector| vector.len() != dimensions) {
                return Err(Error::invalid(
                    "FastEmbed dimensions do not match declared build profile",
                ));
            }
            Ok(vectors)
        }
        backend => Err(Error::invalid(format!(
            "unsupported embedding backend {backend:?}"
        ))),
    }
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

fn validate_usearch_index(
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
        .collect::<HashMap<_, _>>();
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

pub(crate) fn read_semantic_index(path: &Path) -> Result<StoredSemanticIndex, Error> {
    let bytes = fs::read(path)
        .map_err(|e| Error::storage(format!("read semantic artifact {}: {e}", path.display())))?;
    read_semantic_bytes(&bytes)
}

fn read_semantic_bytes(bytes: &[u8]) -> Result<StoredSemanticIndex, Error> {
    if !bytes.starts_with(SEMANTIC_MAGIC) {
        return Err(Error::invalid("semantic artifact magic mismatch"));
    }
    let mut cursor = SEMANTIC_MAGIC.len();
    let dimensions = usize::try_from(read_semantic_u32(bytes, &mut cursor)?)
        .map_err(|_| Error::invalid("semantic dimensions overflow"))?;
    let count = usize::try_from(read_semantic_u64(bytes, &mut cursor)?)
        .map_err(|_| Error::invalid("semantic vector count overflow"))?;
    if dimensions == 0 || dimensions > 4096 {
        return Err(Error::invalid(
            "semantic dimensions outside supported bound",
        ));
    }
    let record_size = 8_usize
        .checked_add(
            dimensions
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| Error::invalid("semantic record size overflow"))?,
        )
        .ok_or_else(|| Error::invalid("semantic record size overflow"))?;
    let expected_len = cursor
        .checked_add(
            count
                .checked_mul(record_size)
                .ok_or_else(|| Error::invalid("semantic artifact size overflow"))?,
        )
        .ok_or_else(|| Error::invalid("semantic artifact size overflow"))?;
    if expected_len != bytes.len() {
        return Err(Error::invalid("semantic artifact length mismatch"));
    }
    let mut vectors = Vec::with_capacity(count);
    for _ in 0..count {
        let key = read_semantic_u64(bytes, &mut cursor)?;
        let mut vector = Vec::with_capacity(dimensions);
        for _ in 0..dimensions {
            let raw = read_semantic_slice(bytes, &mut cursor, std::mem::size_of::<f32>())?;
            vector.push(f32::from_le_bytes(raw.try_into().expect("float width")));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(Error::invalid(
                "semantic artifact contains non-finite vector",
            ));
        }
        vectors.push((key, vector));
    }
    Ok(StoredSemanticIndex {
        dimensions,
        vectors,
    })
}

fn read_semantic_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, Error> {
    Ok(u32::from_le_bytes(
        read_semantic_slice(bytes, cursor, std::mem::size_of::<u32>())?
            .try_into()
            .expect("integer width"),
    ))
}

fn read_semantic_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, Error> {
    Ok(u64::from_le_bytes(
        read_semantic_slice(bytes, cursor, std::mem::size_of::<u64>())?
            .try_into()
            .expect("integer width"),
    ))
}

fn read_semantic_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], Error> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| Error::invalid("semantic cursor overflow"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::invalid("truncated semantic artifact"))?;
    *cursor = end;
    Ok(slice)
}

fn validate_semantic_index(
    index: &StoredSemanticIndex,
    catalog: &[CatalogRecord],
    dimensions: usize,
) -> Result<(), Error> {
    if index.dimensions != dimensions || index.vectors.len() != catalog.len() {
        return Err(Error::invalid("semantic dimensions/count mismatch"));
    }
    let expected = catalog
        .iter()
        .map(|record| record.vector_key)
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for (key, vector) in &index.vectors {
        if !actual.insert(*key) {
            return Err(Error::invalid("duplicate semantic vector key"));
        }
        if vector.len() != dimensions || !expected.contains(key) {
            return Err(Error::invalid(
                "semantic vector key does not map to catalog",
            ));
        }
    }
    if actual != expected {
        return Err(Error::invalid(
            "semantic vector keys do not equal catalog keys",
        ));
    }
    Ok(())
}

fn semantic_hits(
    index: &StoredSemanticIndex,
    catalog: &[CatalogRecord],
    query: &[f32],
    candidate_depth: usize,
) -> Result<Vec<SemanticHit>, Error> {
    let catalog_by_key = catalog
        .iter()
        .map(|record| (record.vector_key, record))
        .collect::<BTreeMap<_, _>>();
    let mut scored = index
        .vectors
        .iter()
        .map(|(key, vector)| {
            let score = vector
                .iter()
                .zip(query)
                .map(|(left, right)| left * right)
                .sum::<f32>();
            (*key, score)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored
        .into_iter()
        .take(candidate_depth)
        .map(|(vector_key, score)| {
            let record = catalog_by_key
                .get(&vector_key)
                .ok_or_else(|| Error::invalid("semantic candidate missing from catalog"))?;
            Ok(SemanticHit {
                vector_key,
                page_id: record.page_id.clone(),
                passage_id: record.passage_id.clone(),
                score,
            })
        })
        .collect()
}

fn write_checksums(staging: &Path) -> Result<String, Error> {
    let bytes = checksum_payload(staging)?;
    write_if_absent(&staging.join("checksums.sha256"), &bytes)?;
    Ok(sha256_hex(&bytes))
}

fn refresh_checksums(staging: &Path) -> Result<String, Error> {
    let bytes = checksum_payload(staging)?;
    fs::write(staging.join("checksums.sha256"), &bytes)
        .map_err(|e| Error::storage(format!("refresh Generation checksums: {e}")))?;
    Ok(sha256_hex(&bytes))
}

fn checksum_payload(staging: &Path) -> Result<Vec<u8>, Error> {
    let mut files = Vec::new();
    collect_files(staging, staging, &mut files)?;
    files.retain(|(relative, _)| relative != "checksums.sha256");
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let mut bytes = Vec::new();
    for (relative, content) in files {
        bytes.extend_from_slice(sha256_hex(&content).as_bytes());
        bytes.extend_from_slice(b"  ");
        bytes.extend_from_slice(relative.as_bytes());
        bytes.push(b'\n');
    }
    Ok(bytes)
}

pub(crate) fn directory_digest(directory: &Path) -> Result<String, Error> {
    let mut files = Vec::new();
    collect_files(directory, directory, &mut files)?;
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let mut digest_input = Vec::new();
    for (relative, bytes) in files {
        digest_input.extend(relative.as_bytes());
        digest_input.push(0);
        digest_input.extend(bytes);
        digest_input.push(0);
    }
    Ok(sha256_hex(&digest_input))
}

fn directory_or_file_digest(path: &Path) -> Result<String, Error> {
    if path.is_file() {
        return fs::read(path)
            .map(|bytes| sha256_hex(&bytes))
            .map_err(|e| Error::storage(format!("read artifact {}: {e}", path.display())));
    }
    directory_digest(path)
}

pub(crate) fn directory_bytes(directory: &Path) -> Result<u64, Error> {
    let mut files = Vec::new();
    collect_files(directory, directory, &mut files)?;
    Ok(files.iter().map(|(_, bytes)| bytes.len() as u64).sum())
}

pub(crate) fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), Error> {
    for entry in
        fs::read_dir(current).map_err(|e| Error::storage(format!("read index directory: {e}")))?
    {
        let path = entry
            .map_err(|e| Error::storage(format!("read index entry: {e}")))?
            .path();
        if path.is_dir() {
            collect_files(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| Error::storage(format!("relative index path: {e}")))?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((
                relative,
                fs::read(&path).map_err(|e| Error::storage(format!("read index artifact: {e}")))?,
            ));
        }
    }
    Ok(())
}

fn build_profile(raw: RawBuildProfile, corpus_id: &str) -> Result<BuildProfile, Error> {
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
    let model_name = required!(model_name);
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
    if pooling != "cls"
        || normalization != "l2"
        || quantization != "none"
        || metric != "cosine"
        || scalar_kind != "f32"
        || fusion_algorithm != "rrf"
        || page_aggregation != "max"
    {
        return Err(Error::invalid(
            "build profile must use cls/l2/none/cosine/f32/rrf/max MVP settings",
        ));
    }
    Ok(BuildProfile {
        schema_version,
        snapshot_id: corpus_id.into(),
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
    })
}
