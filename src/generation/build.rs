use super::checksums::write_checksums;
use super::keyword::build_keyword_index;
use super::profile::build_profile;
use super::semantic::build_semantic_index;
use super::validation::{manifest_digest, validate_generation};
use crate::crawl::page_id;
use crate::storage::{
    jsonl, normalize_text, now, parse_jsonl, sha256_hex, trim_snippet, write_if_absent, write_json,
};
use crate::{
    CatalogRecord, Error, GenerationManifest, RawBuildProfile, SEMANTIC_ARTIFACT, SNAPSHOT_SCHEMA,
    StoredAlias, StoredPage, StoredPassage, StoredSnapshotManifest,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

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
        alias_count,
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
