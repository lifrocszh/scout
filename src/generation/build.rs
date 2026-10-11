use super::checkpoint::remove_checkpoint_store;
use super::checksums::write_checksums;
use super::keyword::build_keyword_index;
use super::profile::build_profile;
use super::semantic::{SemanticBuildInput, build_semantic_index};
use super::validation::{manifest_digest, validate_generation};
use super::{CatalogRecord, GenerationManifest, RawBuildProfile, SEMANTIC_ARTIFACT};
use crate::corpus::{
    SNAPSHOT_SCHEMA, StoredAlias, StoredPage, StoredPassage, StoredSnapshotManifest,
};
use crate::crawl::page_id;
use crate::error::Error;
use crate::observability::Events;
use crate::storage::{
    jsonl, normalize_text, now, parse_jsonl, sha256_hex, trim_snippet, write_if_absent, write_json,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

pub(crate) fn build_generation(
    data_dir: &Path,
    corpus_id: &str,
    run_id: &str,
    resume: bool,
    mode: super::BuildMode,
) -> Result<Value, Error> {
    if !valid_build_run_id(run_id) {
        return Err(Error::invalid(
            "build run ID must be run- followed by 64 hexadecimal characters",
        ));
    }
    // Progress is operational evidence outside the immutable Generation artifacts.
    // Losing telemetry must not make an otherwise valid build fail.
    let mut progress =
        Events::open_or_append(data_dir, &format!("{run_id}.build-progress"), "index")
            .ok()
            .map(|(events, _)| events);
    emit_build_phase(&mut progress, "verify_corpus", None);
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
    drop((pages_bytes, aliases_bytes, passages_bytes));
    let page_count = pages.len();
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
    let corpus_model = snapshot
        .embedding_model
        .as_deref()
        .unwrap_or(super::DEFAULT_EMBEDDING_MODEL);
    let profile = build_profile(raw, corpus_id, corpus_model, mode)?;
    let profile_bytes = serde_json::to_vec_pretty(&profile)
        .map_err(|e| Error::storage(format!("serialize build profile: {e}")))?;
    let profile_digest = sha256_hex(&profile_bytes);

    emit_build_phase(&mut progress, "catalog", Some(passages.len()));

    let mut aliases_by_page = BTreeMap::<String, BTreeSet<String>>::new();
    for alias in &aliases {
        if let Some(identity_url) = alias_identity_url(alias) {
            aliases_by_page
                .entry(alias.representative_page_id.clone())
                .or_default()
                .insert(page_id(identity_url));
        }
    }
    let alias_count = aliases_by_page.values().map(BTreeSet::len).sum();
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
    drop(page_ids);
    drop((pages, aliases));
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
                .map(|ids| ids.iter().cloned().collect())
                .unwrap_or_default(),
            vector_key: (index + 1) as u64,
        });
    }
    drop(passages);
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
    let checkpoint_root = data_dir.join("generation-build-checkpoints").join(run_id);
    let embedding_cache_root = data_dir.join("generation-embedding-cache");
    fs::create_dir_all(&staging_root)
        .map_err(|e| Error::storage(format!("create Generation staging directory: {e}")))?;
    if resume {
        if !staging.is_dir() {
            return Err(Error::invalid(format!(
                "staging build {} does not exist; omit --resume to start a new build",
                staging.display()
            )));
        }
        verify_resume_input(&staging, "build-profile.json", &profile_bytes)?;
        verify_resume_input(&staging, "catalog.jsonl", &catalog_bytes)?;
        drop(catalog_bytes);
        drop(catalog);
        if let Ok(validated) = validate_generation(&staging, "validated") {
            remove_checkpoint_store(&checkpoint_root)?;
            emit_build_phase(&mut progress, "validated", Some(validated.passage_count));
            return completed_build_summary(&staging, &validated);
        }
        remove_incomplete_artifacts(&staging)?;
        let stored_catalog_bytes = fs::read(staging.join("catalog.jsonl"))
            .map_err(|error| Error::storage(format!("read resumed catalog: {error}")))?;
        catalog = parse_jsonl(&stored_catalog_bytes, "catalog.jsonl")?;
        drop(stored_catalog_bytes);
    } else {
        if staging.exists() || checkpoint_root.exists() {
            return Err(Error::storage(format!(
                "build staging {} already exists; use --resume with this build run ID",
                staging.display()
            )));
        }
        fs::create_dir(&staging)
            .map_err(|e| Error::storage(format!("create Generation staging directory: {e}")))?;
        write_if_absent(&staging.join("build-profile.json"), &profile_bytes)?;
        write_if_absent(&staging.join("catalog.jsonl"), &catalog_bytes)?;
        drop(catalog_bytes);
    }
    let keyword = if mode.has_keyword() {
        emit_build_phase(&mut progress, "keyword", Some(catalog.len()));
        Some(build_keyword_index(
            &staging,
            &catalog,
            &profile,
            &mut progress,
        )?)
    } else {
        None
    };
    let semantic = if mode.has_semantic() {
        emit_build_phase(&mut progress, "semantic", Some(catalog.len()));
        Some(build_semantic_index(SemanticBuildInput {
            staging: &staging,
            checkpoint_root: &checkpoint_root,
            embedding_cache_root: &embedding_cache_root,
            build_run_id: run_id,
            corpus_snapshot_sha256: &snapshot_digest,
            build_profile_sha256: &profile_digest,
            catalog_sha256: &catalog_digest,
            resume,
            catalog: &catalog,
            profile: &profile,
        })?)
    } else {
        None
    };
    let keyword_digest = keyword.as_ref().map(|result| result.0.clone());
    let keyword_count = keyword.as_ref().map_or(0, |result| result.1);
    let keyword_duration_ms = keyword.as_ref().map(|result| result.2);
    let keyword_artifact_bytes = keyword.as_ref().map(|result| result.3);
    let semantic_evidence = semantic.as_ref().map(|result| result.0.clone());
    let checkpoint_stats = semantic.as_ref().map(|result| &result.1);
    emit_build_phase(&mut progress, "manifest", Some(catalog.len()));
    let mut generation = GenerationManifest {
        schema_version: "scout.generation-manifest.v2".into(),
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
        keyword_artifact: mode.has_keyword().then(|| "keyword".into()),
        keyword_sha256: keyword_digest,
        keyword_count,
        semantic_artifact: mode.has_semantic().then(|| SEMANTIC_ARTIFACT.into()),
        semantic_sha256: semantic_evidence
            .as_ref()
            .map(|evidence| evidence.artifact_sha256.clone()),
        semantic_count: semantic_evidence
            .as_ref()
            .map_or(0, |evidence| evidence.vector_count),
        semantic: semantic_evidence,
        components: Some(super::GenerationComponents::for_mode(mode)),
        checksums_file: "checksums.sha256".into(),
        page_count,
        passage_count: catalog.len(),
        alias_count,
        vector_key_count: catalog.len(),
        vector_key_map: "one-based-catalog-order-page-id-passage-id".into(),
        created_at_utc: now(),
        manifest_sha256: String::new(),
    };
    generation.manifest_sha256 = manifest_digest(&generation)?;
    write_json(&staging.join("generation-manifest.json"), &generation)?;
    let checksums_sha256 = write_checksums(&staging)?;
    drop(catalog);
    // Build success is meaningful only after reopening every persisted artifact
    // through the same full validator activation uses. This also keeps a bad
    // staging directory from being reported as a usable Generation.
    emit_build_phase(&mut progress, "validation", None);
    let validated = validate_generation(&staging, "validated")?;
    remove_checkpoint_store(&checkpoint_root)?;
    emit_build_phase(&mut progress, "validated", Some(validated.passage_count));
    Ok(json!({
        "generation_id": validated.generation_id,
        "staging_path": staging,
        "page_count": validated.page_count,
        "passage_count": validated.passage_count,
        "catalog_sha256": validated.catalog_sha256,
        "keyword": keyword_duration_ms.map(|duration| json!({
            "artifact": "keyword",
            "artifact_bytes": keyword_artifact_bytes.unwrap_or_default(),
            "build_duration_ms": duration,
            "count": keyword_count,
        })),
        "semantic": validated.semantic,
        "build_checkpoints": checkpoint_stats.map(|stats| json!({
            "schema": stats.schema,
            "identity_sha256": stats.identity_sha256,
            "chunks_reused": stats.chunks_reused,
            "chunks_created": stats.chunks_created,
        })),
        "embedding_reuse": checkpoint_stats.map(|stats| json!({
            "schema": "scout.embedding-reuse.v1",
            "embedding_fingerprint": stats.embedding_fingerprint,
            "cache_vectors_reused": stats.cache_vectors_reused,
            "cache_vectors_created": stats.cache_vectors_created,
            "build_checkpoint_vectors_reused": stats.checkpoint_vectors_reused,
            "local_duplicate_vectors_reused": stats.local_duplicate_vectors_reused,
        })),
        "checksums_sha256": checksums_sha256,
    }))
}

fn emit_build_phase(progress: &mut Option<Events>, phase: &str, passage_count: Option<usize>) {
    if let Some(events) = progress.as_mut() {
        events.emit(
            "index.phase",
            "success",
            json!({"phase": phase, "passage_count": passage_count}),
        );
    }
}

fn valid_build_run_id(run_id: &str) -> bool {
    run_id.strip_prefix("run-").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn verify_resume_input(staging: &Path, name: &str, expected: &[u8]) -> Result<(), Error> {
    let path = staging.join(name);
    let existing = fs::read(&path).map_err(|error| {
        Error::invalid(format!("read resume input {}: {error}", path.display()))
    })?;
    if existing != expected {
        return Err(Error::invalid(format!(
            "resume input {name} does not match the original Corpus and build profile"
        )));
    }
    Ok(())
}

fn remove_incomplete_artifacts(staging: &Path) -> Result<(), Error> {
    for name in [
        "keyword",
        SEMANTIC_ARTIFACT,
        "semantic-vectors.f32",
        "semantic-manifest.json",
        "semantic-build-progress.jsonl",
        "generation-manifest.json",
        "checksums.sha256",
    ] {
        let path = staging.join(name);
        if path.is_dir() {
            fs::remove_dir_all(&path).map_err(|error| {
                Error::storage(format!(
                    "remove incomplete artifact {}: {error}",
                    path.display()
                ))
            })?;
        } else if path.exists() {
            fs::remove_file(&path).map_err(|error| {
                Error::storage(format!(
                    "remove incomplete artifact {}: {error}",
                    path.display()
                ))
            })?;
        }
    }
    Ok(())
}

fn completed_build_summary(staging: &Path, manifest: &GenerationManifest) -> Result<Value, Error> {
    let checksums = fs::read(staging.join("checksums.sha256"))
        .map_err(|error| Error::storage(format!("read completed Generation checksums: {error}")))?;
    let keyword = manifest
        .keyword_artifact
        .as_ref()
        .map(|artifact| {
            Ok::<_, Error>(json!({
                "artifact": artifact,
                "artifact_bytes": super::checksums::directory_bytes(&staging.join(artifact))?,
                "count": manifest.keyword_count,
            }))
        })
        .transpose()?;
    Ok(json!({
        "generation_id": manifest.generation_id,
        "staging_path": staging,
        "page_count": manifest.page_count,
        "passage_count": manifest.passage_count,
        "catalog_sha256": manifest.catalog_sha256,
        "keyword": keyword,
        "semantic": manifest.semantic,
        "checksums_sha256": sha256_hex(&checksums),
        "resumed_complete": true,
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
            || !matches!(alias.alias_kind.as_str(), "exact-alias" | "redirect-alias")
            || alias.source_id != representative.source_id
        {
            return Err(Error::invalid(
                "Corpus alias identity or reference mismatch",
            ));
        }
        let Some(identity_url) = alias_identity_url(alias) else {
            // Legacy no-op redirects point at their representative final URL.
            // They are provenance, not distinct searchable aliases.
            continue;
        };
        let derived_alias_id = page_id(identity_url);
        let inserted_id = alias_ids.insert(derived_alias_id.clone());
        let inserted_url = alias_urls.insert((alias.source_id.as_str(), identity_url));
        if inserted_id != inserted_url {
            return Err(Error::invalid(
                "Corpus alias identity or reference mismatch",
            ));
        }
        if !inserted_id {
            // Duplicate raw redirect captures share one requested URL identity.
            continue;
        }
        if (alias.alias_kind == "exact-alias" && alias.alias_page_id != derived_alias_id)
            || derived_alias_id == alias.representative_page_id
            || pages_by_id.contains_key(derived_alias_id.as_str())
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

fn alias_identity_url(alias: &StoredAlias) -> Option<&str> {
    match alias.alias_kind.as_str() {
        "exact-alias" => Some(alias.alias_url.as_str()),
        "redirect-alias" => {
            let requested_url = if alias.requested_url.is_empty() {
                alias.alias_url.as_str()
            } else {
                alias.requested_url.as_str()
            };
            (requested_url != alias.alias_url).then_some(requested_url)
        }
        _ => None,
    }
}
