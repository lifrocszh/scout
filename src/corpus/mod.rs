mod model;

pub(crate) const SNAPSHOT_SCHEMA: &str = "scout.corpus-snapshot.v1";
const CRAWL_SCHEMA: &str = "scout.crawl-manifest.v1";

use model::{
    AliasRecord, Manifest, ManifestError, ManifestPage, PageRecord, PassageRecord,
    SnapshotManifest, Summary,
};
pub(crate) use model::{
    SnapshotResult, StoredAlias, StoredPage, StoredPassage, StoredSnapshotManifest,
};

use crate::crawl::page_id;
use crate::crawl::spool::CaptureSpool;
use crate::crawl::{Capture, Config, ContentBlock, CrawlResult};
use crate::error::Error;
use crate::observability::{Events, emit_shared};
use crate::storage::{now, sha256_hex, write_json};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

mod bounded;
pub(crate) use bounded::{
    materialize_spooled, seal_existing_materialized_spool, write_incomplete_spooled,
};

struct DigestWriter<W> {
    writer: W,
    digest: Sha256,
}

impl<W> DigestWriter<W> {
    fn new(writer: W) -> Self {
        Self {
            writer,
            digest: Sha256::new(),
        }
    }

    fn finish(self) -> String {
        format!("{:x}", self.digest.finalize())
    }
}

impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

fn serialized_sha256<T: Serialize>(value: &T, name: &str) -> Result<String, Error> {
    let mut writer = DigestWriter::new(io::sink());
    serde_json::to_writer_pretty(&mut writer, value)
        .map_err(|e| Error::storage(format!("serialize {name}: {e}")))?;
    writer
        .flush()
        .map_err(|e| Error::storage(format!("flush serialized {name}: {e}")))?;
    Ok(writer.finish())
}

fn write_pretty_json<T: Serialize>(path: &Path, value: &T) -> Result<(), Error> {
    let file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            serde_json::to_writer_pretty(io::sink(), value)
                .map_err(|e| Error::storage(format!("serialize {}: {e}", path.display())))?;
            return Ok(());
        }
        Err(error) => {
            return Err(Error::storage(format!(
                "create {}: {error}",
                path.display()
            )));
        }
    };
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .map_err(|e| Error::storage(format!("serialize {}: {e}", path.display())))?;
    writer
        .flush()
        .map_err(|e| Error::storage(format!("write {}: {e}", path.display())))
}

#[cfg(test)]
fn write_jsonl<T: Serialize>(path: &Path, values: &[T]) -> Result<String, Error> {
    let output: Box<dyn Write> = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => Box::new(BufWriter::new(file)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Box::new(io::sink()),
        Err(error) => {
            return Err(Error::storage(format!(
                "create {}: {error}",
                path.display()
            )));
        }
    };
    let mut writer = DigestWriter::new(output);
    for value in values {
        serde_json::to_writer(&mut writer, value)
            .map_err(|e| Error::storage(format!("serialize {} JSONL: {e}", path.display())))?;
        writer
            .write_all(b"\n")
            .map_err(|e| Error::storage(format!("write {}: {e}", path.display())))?;
    }
    writer
        .flush()
        .map_err(|e| Error::storage(format!("flush {}: {e}", path.display())))?;
    Ok(writer.finish())
}

fn build_passages(
    page_id: &str,
    source_id: &str,
    final_url: &str,
    title: &str,
    identity: &str,
    blocks: &[ContentBlock],
    max_tokens: usize,
) -> Vec<PassageRecord> {
    let mut sections = Vec::<(Vec<String>, Vec<ContentBlock>)>::new();
    let mut headings = Vec::<(usize, String)>::new();
    let mut heading_path = Vec::new();
    let mut content = Vec::new();
    for block in blocks {
        match block {
            ContentBlock::Heading { level, text } => {
                if !content.is_empty() {
                    sections.push((heading_path.clone(), std::mem::take(&mut content)));
                }
                while headings
                    .last()
                    .is_some_and(|(current_level, _)| *current_level >= *level)
                {
                    headings.pop();
                }
                headings.push((*level, text.clone()));
                heading_path = headings.iter().map(|(_, text)| text.clone()).collect();
            }
            ContentBlock::Prose(_) | ContentBlock::Code(_) => content.push(block.clone()),
        }
    }
    if !content.is_empty() {
        sections.push((heading_path, content));
    }

    let mut passages = Vec::new();
    let mut ordinal = 0;
    for (heading_path, blocks) in sections {
        for text in split_blocks(&blocks, max_tokens) {
            let passage_id = passage_id(page_id, identity, ordinal);
            passages.push(PassageRecord {
                page_id: page_id.into(),
                passage_id,
                source_id: source_id.into(),
                normalized_final_url: final_url.into(),
                title: title.into(),
                heading_path: heading_path.clone(),
                ordinal,
                content_sha256: sha256_hex(text.as_bytes()),
                content: text,
            });
            ordinal += 1;
        }
    }
    passages
}

fn split_blocks(blocks: &[ContentBlock], max_tokens: usize) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = Vec::new();
    let mut current_tokens = 0;
    for block in blocks {
        let text = match block {
            ContentBlock::Prose(text) | ContentBlock::Code(text) => text,
            ContentBlock::Heading { .. } => continue,
        };
        for piece in split_text(text, max_tokens) {
            let tokens = token_count(&piece);
            if current_tokens > 0 && current_tokens + tokens > max_tokens {
                result.push(current.join("\n"));
                current.clear();
                current_tokens = 0;
            }
            current.push(piece);
            current_tokens += tokens;
        }
    }
    if !current.is_empty() {
        result.push(current.join("\n"));
    }
    result
}

fn split_text(text: &str, max_tokens: usize) -> Vec<String> {
    if token_count(text) <= max_tokens {
        return vec![text.into()];
    }
    text.split('\n')
        .flat_map(|line| {
            let words = line.split_whitespace().collect::<Vec<_>>();
            if words.is_empty() {
                Vec::new()
            } else {
                words
                    .chunks(max_tokens)
                    .map(|chunk| chunk.join(" "))
                    .collect::<Vec<_>>()
            }
        })
        .collect()
}

fn token_count(text: &str) -> usize {
    text.split_whitespace().count()
}

fn passage_id(page_id: &str, identity: &str, ordinal: usize) -> String {
    sha256_hex(format!("scout.passage.v1\0{page_id}{identity}{ordinal}").as_bytes())
}

/// Deterministic conflict policy for captures sharing `(source_id, final_url)`.
/// Lexicographically smallest content hash wins, followed by extracted identity,
/// body hash, request provenance, and response metadata. The tuple is recorded
/// in the Crawl Manifest so a superseded response remains explainable.
pub(crate) fn capture_selection_key(capture: &Capture) -> String {
    format!(
        "{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
        capture.content_hash,
        capture.identity,
        capture.body_hash,
        capture.requested_url,
        capture.redirects.join("\0"),
        capture.status_code,
        capture.content_type,
        capture.body_file,
        sha256_hex(
            format!(
                "{}\0{:?}\0{:?}\0{:?}\0{}\0{}",
                capture.title,
                capture.description,
                capture.language,
                capture.canonical_url,
                capture.extraction_method,
                capture.content,
            )
            .as_bytes(),
        ),
    )
}

const CAPTURE_SELECTION_POLICY: &str = "lexicographic(content_sha256, extracted_identity, body_sha256, requested_url, redirect_chain, response_metadata)";

fn capture_id(capture: &Capture) -> String {
    sha256_hex(
        format!(
            "scout.capture.v1\0{}\0{}\0{}",
            capture.source_id,
            capture.final_url,
            capture_selection_key(capture),
        )
        .as_bytes(),
    )
}

#[cfg(test)]
pub(crate) fn materialize(
    data_dir: &Path,
    crawl_dir: &Path,
    run_id: &str,
    config: &Config,
    started: &str,
    completed: &str,
    result: CrawlResult,
) -> Result<SnapshotResult, Error> {
    let mut captures = result.captures;
    let captured_page_count = captures.len();
    // Conflicting responses for one final URL are selected by this stable tuple,
    // never by worker completion order. The content hash is first so a changed
    // response cannot win merely because it arrived earlier.
    captures.sort_by_key(capture_selection_key);

    // First select one capture for each Source-scoped final URL. This is the
    // integrity boundary that prevents one URL with changing content from
    // becoming duplicate Page records.
    let mut captures_by_url: BTreeMap<(String, String), Vec<&Capture>> = BTreeMap::new();
    for capture in &captures {
        captures_by_url
            .entry((capture.source_id.clone(), capture.final_url.clone()))
            .or_default()
            .push(capture);
    }
    let mut selected_by_url = BTreeMap::<(String, String), &Capture>::new();
    for (key, captures) in &mut captures_by_url {
        captures.sort_by_key(|capture| capture_selection_key(capture));
        selected_by_url.insert(key.clone(), captures[0]);
    }

    // Exact extracted identities still collapse into aliases, but only after
    // URL conflicts have been resolved. This keeps aliases and Passages tied to
    // the selected Page content.
    let mut identity_groups: BTreeMap<(String, String), Vec<&Capture>> = BTreeMap::new();
    for capture in selected_by_url.values() {
        identity_groups
            .entry((capture.source_id.clone(), capture.identity.clone()))
            .or_default()
            .push(*capture);
    }
    for group in identity_groups.values_mut() {
        group.sort_by(|a, b| {
            a.final_url
                .cmp(&b.final_url)
                .then_with(|| capture_selection_key(a).cmp(&capture_selection_key(b)))
        });
    }

    let mut pages = Vec::new();
    let mut aliases = Vec::new();
    let mut manifest_pages = Vec::new();
    let mut passages = Vec::new();
    let mut selected_capture_ids = HashSet::new();
    let mut page_for_capture = HashMap::new();
    let mut alias_for_capture = HashMap::new();
    for group in identity_groups.values() {
        let representative = group[0];
        let representative_id = page_id(&representative.final_url);
        pages.push(PageRecord {
            page_id: representative_id.clone(),
            source_id: representative.source_id.clone(),
            normalized_final_url: representative.final_url.clone(),
            title: representative.title.clone(),
            description: representative.description.clone(),
            language: representative.language.clone(),
            canonical_url: representative.canonical_url.clone(),
            extraction_method: representative.extraction_method.into(),
            content: representative.content.clone(),
            content_identity: representative.identity.clone(),
            content_sha256: representative.content_hash.clone(),
            body_sha256: representative.body_hash.clone(),
            body_file: representative.body_file.clone(),
        });
        passages.extend(build_passages(
            &representative_id,
            &representative.source_id,
            &representative.final_url,
            &representative.title,
            &representative.identity,
            &representative.blocks,
            config.max_passage_tokens,
        ));
        for (index, capture) in group.iter().enumerate() {
            let capture_key = capture_id(capture);
            selected_capture_ids.insert(capture_key.clone());
            page_for_capture.insert(capture_key.clone(), representative_id.clone());
            let representative_page = index == 0;
            let id = page_id(&capture.final_url);
            if !representative_page {
                alias_for_capture.insert(capture_key, representative_id.clone());
                aliases.push(AliasRecord {
                    source_id: capture.source_id.clone(),
                    alias_url: capture.final_url.clone(),
                    alias_page_id: id,
                    representative_page_id: representative_id.clone(),
                    alias_kind: "exact-alias",
                    requested_url: capture.requested_url.clone(),
                    redirect_chain: capture.redirects.clone(),
                    canonical_url: capture.canonical_url.clone(),
                });
            }
        }
    }

    for capture in &captures {
        let key = (capture.source_id.clone(), capture.final_url.clone());
        let selected = selected_by_url
            .get(&key)
            .expect("every Capture has a selected final URL representative");
        let capture_key = capture_id(capture);
        let selected_id = capture_id(selected);
        let selected_page_id = page_for_capture
            .get(&selected_id)
            .cloned()
            .expect("selected Capture has a Page mapping");
        let is_selected = selected_capture_ids.contains(&capture_key);
        let exact_alias = is_selected && alias_for_capture.contains_key(&capture_key);
        let selection_reason = if !is_selected {
            if capture.content_hash == selected.content_hash {
                "superseded-duplicate-capture"
            } else {
                "superseded-conflicting-content"
            }
        } else if exact_alias {
            "exact-content-alias"
        } else {
            "deterministic-minimum-selection"
        };
        let admission_status = if !is_selected {
            "superseded-capture"
        } else if exact_alias {
            "exact-alias"
        } else {
            "admitted"
        };
        let page_id_value = page_id(&capture.final_url);
        manifest_pages.push(ManifestPage {
            requested_url: capture.requested_url.clone(),
            final_url: capture.final_url.clone(),
            redirect_chain: capture.redirects.clone(),
            source_id: capture.source_id.clone(),
            page_id: page_id_value,
            admission_status: admission_status.into(),
            capture_id: capture_key.clone(),
            selection_key: capture_selection_key(capture),
            selected_capture_id: selected_id,
            selection_reason: selection_reason.into(),
            alias_of: if is_selected && exact_alias {
                alias_for_capture.get(&capture_key).cloned()
            } else if !is_selected && capture.final_url != selected.final_url {
                Some(selected_page_id)
            } else {
                None
            },
            title: capture.title.clone(),
            description: capture.description.clone(),
            language: capture.language.clone(),
            canonical_url: capture.canonical_url.clone(),
            extraction_method: capture.extraction_method.into(),
            content_type: capture.content_type.clone(),
            content_bytes: capture.content_bytes,
            body_file: capture.body_file.clone(),
            body_sha256: capture.body_hash.clone(),
            content_sha256: capture.content_hash.clone(),
            status_code: capture.status_code,
        });
    }
    drop(alias_for_capture);
    drop(page_for_capture);
    drop(selected_capture_ids);
    drop(identity_groups);
    drop(selected_by_url);
    drop(captures_by_url);
    drop(captures);

    pages.sort_by(|a, b| {
        a.source_id
            .cmp(&b.source_id)
            .then_with(|| a.normalized_final_url.cmp(&b.normalized_final_url))
    });
    aliases.sort_by(|a, b| {
        a.source_id
            .cmp(&b.source_id)
            .then_with(|| a.alias_url.cmp(&b.alias_url))
    });
    manifest_pages.sort_by(|a, b| {
        a.source_id
            .cmp(&b.source_id)
            .then_with(|| a.final_url.cmp(&b.final_url))
    });
    passages.sort_by(|a, b| {
        a.source_id
            .cmp(&b.source_id)
            .then_with(|| a.normalized_final_url.cmp(&b.normalized_final_url))
            .then_with(|| a.ordinal.cmp(&b.ordinal))
    });
    validate_snapshot_records(&pages, &aliases, &passages)?;
    let mut snapshot_digest = DigestWriter::new(io::sink());
    snapshot_digest
        .write_all(config.digest.as_bytes())
        .map_err(|e| Error::storage(format!("hash snapshot identity: {e}")))?;
    snapshot_digest
        .write_all(b"\0")
        .map_err(|e| Error::storage(format!("hash snapshot identity: {e}")))?;
    serde_json::to_writer(&mut snapshot_digest, &pages)
        .map_err(|e| Error::storage(format!("serialize pages for snapshot identity: {e}")))?;
    snapshot_digest
        .write_all(b"\0")
        .map_err(|e| Error::storage(format!("hash snapshot identity: {e}")))?;
    serde_json::to_writer(&mut snapshot_digest, &passages)
        .map_err(|e| Error::storage(format!("serialize passages for snapshot identity: {e}")))?;
    snapshot_digest
        .flush()
        .map_err(|e| Error::storage(format!("flush snapshot identity hash: {e}")))?;
    let snapshot_id = format!("snapshot-{}", snapshot_digest.finish());
    let page_count = pages.len();
    let alias_count = aliases.len();
    let passage_count = passages.len();
    let mut manifest = Manifest {
        schema_version: CRAWL_SCHEMA,
        run_id: run_id.into(),
        status: "complete",
        started_at_utc: started.into(),
        completed_at_utc: completed.into(),
        source_config_sha256: config.digest.clone(),
        manifest_sha256: String::new(),
        snapshot_id: Some(snapshot_id.clone()),
        capture_selection_policy: CAPTURE_SELECTION_POLICY,
        user_agent: config.user_agent.clone(),
        robots_user_agent: "Scout",
        policy: config.policy(),
        sources: config.sources.clone(),
        pages: manifest_pages,
        summary: Summary {
            captured_page_count,
            page_count,
            alias_count,
            passage_count,
            rejected_count: result.rejected_count,
            failed_count: result.failed_count,
            target_page_count: config.page_target,
            stop_reason: result.stop_reason,
        },
        error: None,
    };
    manifest.manifest_sha256 = serialized_sha256(&manifest, "manifest")?;
    write_pretty_json(&crawl_dir.join("crawl-manifest.json"), &manifest)?;
    let crawl_manifest_sha256 = manifest.manifest_sha256.clone();
    drop(manifest);
    let corpora = data_dir.join("corpora");
    fs::create_dir_all(&corpora)
        .map_err(|e| Error::storage(format!("create corpora directory: {e}")))?;
    let staging = corpora.join(format!(".staging-{run_id}"));
    fs::create_dir_all(&staging)
        .map_err(|e| Error::storage(format!("create snapshot staging: {e}")))?;
    let pages_jsonl_sha256 = write_jsonl(&staging.join("pages.jsonl"), &pages)?;
    let aliases_jsonl_sha256 = write_jsonl(&staging.join("aliases.jsonl"), &aliases)?;
    let passages_jsonl_sha256 = write_jsonl(&staging.join("passages.jsonl"), &passages)?;
    drop(pages);
    drop(aliases);
    drop(passages);
    let snapshot_manifest = SnapshotManifest {
        schema_version: SNAPSHOT_SCHEMA,
        snapshot_id: snapshot_id.clone(),
        status: "complete",
        run_id: run_id.into(),
        crawl_manifest_sha256,
        source_config_sha256: config.digest.clone(),
        embedding_model: config.embedding_model.clone(),
        created_at_utc: completed.into(),
        page_count,
        alias_count,
        pages_jsonl_sha256,
        aliases_jsonl_sha256,
        passage_count,
        passages_jsonl_sha256,
    };
    write_json(&staging.join("snapshot-manifest.json"), &snapshot_manifest)?;
    let final_dir = corpora.join(&snapshot_id);
    if final_dir.exists() {
        return Err(Error::storage(format!(
            "snapshot already exists: {}",
            final_dir.display()
        )));
    }
    fs::rename(&staging, &final_dir).map_err(|e| Error::storage(format!("seal snapshot: {e}")))?;
    Ok(SnapshotResult {
        id: snapshot_id,
        pages: page_count,
        aliases: alias_count,
    })
}

/// Validate the immutable records before a snapshot can be sealed. Keep this
/// stricter than the count checks in the manifest: JSONL references are the
/// trust boundary consumed by index builds and search.
#[cfg(test)]
pub(crate) fn validate_snapshot_records(
    pages: &[PageRecord],
    aliases: &[AliasRecord],
    passages: &[PassageRecord],
) -> Result<(), Error> {
    let mut pages_by_id = BTreeMap::new();
    let mut page_urls = HashSet::new();
    for page in pages {
        if page.page_id.is_empty()
            || page.source_id.is_empty()
            || page.normalized_final_url.is_empty()
            || page.content_identity.is_empty()
            || page.page_id != page_id(&page.normalized_final_url)
            || page.content_sha256 != sha256_hex(page.content_identity.as_bytes())
            || pages_by_id.insert(page.page_id.as_str(), page).is_some()
            || !page_urls.insert((&page.source_id, &page.normalized_final_url))
        {
            return Err(Error::invalid("Corpus Page identity or content mismatch"));
        }
    }

    let mut alias_ids = HashSet::new();
    let mut alias_urls = HashSet::new();
    for alias in aliases {
        let Some(representative) = pages_by_id.get(alias.representative_page_id.as_str()) else {
            return Err(Error::invalid(
                "Corpus alias references unknown representative Page",
            ));
        };
        if alias.alias_url.is_empty()
            || !matches!(alias.alias_kind, "exact-alias" | "redirect-alias")
            || alias.source_id != representative.source_id
        {
            return Err(Error::invalid(
                "Corpus alias identity or reference mismatch",
            ));
        }
        let Some(identity_url) = alias_identity_url(alias) else {
            continue;
        };
        let derived_alias_id = page_id(identity_url);
        let inserted_id = alias_ids.insert(derived_alias_id.clone());
        let inserted_url = alias_urls.insert((&alias.source_id, identity_url));
        if inserted_id != inserted_url {
            return Err(Error::invalid(
                "Corpus alias identity or reference mismatch",
            ));
        }
        if !inserted_id {
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

    let mut passage_ids = HashSet::new();
    let mut ordinals = HashSet::new();
    for passage in passages {
        let Some(page) = pages_by_id.get(passage.page_id.as_str()) else {
            return Err(Error::invalid("Corpus Passage references unknown Page"));
        };
        if passage.passage_id.is_empty()
            || passage.passage_id
                != passage_id(&passage.page_id, &page.content_identity, passage.ordinal)
            || !passage_ids.insert(passage.passage_id.as_str())
            || passage.source_id != page.source_id
            || passage.normalized_final_url != page.normalized_final_url
            || passage.content.is_empty()
            || passage.content_sha256 != sha256_hex(passage.content.as_bytes())
            || !ordinals.insert((passage.page_id.as_str(), passage.ordinal))
        {
            return Err(Error::invalid(
                "Corpus Passage identity or reference mismatch",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
fn alias_identity_url(alias: &AliasRecord) -> Option<&str> {
    match alias.alias_kind {
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

pub(crate) struct IncompleteCrawl<'a> {
    pub(crate) captures: &'a [Capture],
    pub(crate) rejected: usize,
    pub(crate) failed: usize,
}

pub(crate) fn write_incomplete(
    crawl_dir: &Path,
    run_id: &str,
    config: &Config,
    started: &str,
    incomplete: IncompleteCrawl<'_>,
    error: &Error,
) {
    let pages = incomplete
        .captures
        .iter()
        .map(|capture| ManifestPage {
            requested_url: capture.requested_url.clone(),
            final_url: capture.final_url.clone(),
            redirect_chain: capture.redirects.clone(),
            source_id: capture.source_id.clone(),
            page_id: page_id(&capture.final_url),
            admission_status: "admitted_but_incomplete".into(),
            capture_id: capture_id(capture),
            selection_key: capture_selection_key(capture),
            selected_capture_id: capture_id(capture),
            selection_reason: "incomplete-crawl".into(),
            alias_of: None,
            title: capture.title.clone(),
            description: capture.description.clone(),
            language: capture.language.clone(),
            canonical_url: capture.canonical_url.clone(),
            extraction_method: capture.extraction_method.into(),
            content_type: capture.content_type.clone(),
            content_bytes: capture.content_bytes,
            body_file: capture.body_file.clone(),
            body_sha256: capture.body_hash.clone(),
            content_sha256: capture.content_hash.clone(),
            status_code: capture.status_code,
        })
        .collect();
    let manifest = Manifest {
        schema_version: CRAWL_SCHEMA,
        run_id: run_id.into(),
        status: "incomplete",
        started_at_utc: started.into(),
        completed_at_utc: now(),
        source_config_sha256: config.digest.clone(),
        manifest_sha256: String::new(),
        snapshot_id: None,
        capture_selection_policy: CAPTURE_SELECTION_POLICY,
        user_agent: config.user_agent.clone(),
        robots_user_agent: "Scout",
        policy: config.policy(),
        sources: config.sources.clone(),
        pages,
        summary: Summary {
            captured_page_count: incomplete.captures.len(),
            page_count: incomplete.captures.len(),
            alias_count: 0,
            passage_count: 0,
            rejected_count: incomplete.rejected,
            failed_count: incomplete.failed,
            target_page_count: config.page_target,
            stop_reason: "operational_failure".into(),
        },
        error: Some(ManifestError {
            code: error.code.clone(),
            message: error.message.clone(),
        }),
    };
    let _ = write_pretty_json(&crawl_dir.join("incomplete-manifest.json"), &manifest);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crawl::Source;
    use serde_json::Value;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn config() -> Config {
        Config {
            schema_version: CRAWL_SCHEMA.into(),
            contact_url: "https://example.invalid/contact".into(),
            user_agent: "Scout/test".into(),
            embedding_model: crate::generation::DEFAULT_EMBEDDING_MODEL.into(),
            page_target: 1,
            min_content_chars: 1,
            max_body_bytes: 1_000_000,
            max_frontier: 100,
            timeout_seconds: 1,
            global_concurrency: 1,
            origin_concurrency: 1,
            min_start_spacing_ms: 0,
            max_attempts: 1,
            retry_backoff_ms: 1,
            retry_max_delay_ms: 1,
            max_total_attempts: 10,
            max_duration_seconds: 10,
            robots_cache_seconds: 10,
            max_extraction_bytes: 1_000_000,
            max_extraction_millis: 1_000,
            max_passage_tokens: 64,
            sources: vec![Source {
                source_id: "fixture".into(),
                name: None,
                seeds: vec!["https://example.invalid/docs".into()],
                allowed_urls: None,
                allowed_origins: vec!["https://example.invalid".into()],
                path_prefixes: vec!["/".into()],
                deny_path_prefixes: Vec::new(),
                sitemap_url_prefixes: Vec::new(),
                politeness_group: None,
                license_urls: Vec::new(),
                minimum_page_quota: 0,
            }],
            digest: "config-digest".into(),
        }
    }

    fn capture(content: &str, body: &str, requested_url: &str) -> Capture {
        let identity = format!("Prose\0{content}");
        Capture {
            requested_url: requested_url.into(),
            final_url: "https://example.invalid/docs/docker".into(),
            redirects: Vec::new(),
            source_id: "fixture".into(),
            title: "Docker".into(),
            description: None,
            language: Some("en".into()),
            canonical_url: None,
            extraction_method: "test",
            content: content.into(),
            identity: identity.clone(),
            content_hash: sha256_hex(identity.as_bytes()),
            blocks: vec![ContentBlock::Prose(content.into())],
            content_bytes: content.len(),
            body_hash: sha256_hex(body.as_bytes()),
            body_file: format!("bodies/{}", sha256_hex(body.as_bytes())),
            content_type: "text/html".into(),
            status_code: 200,
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("scout-corpus-{label}-{stamp}"))
    }

    #[test]
    fn conflicting_capture_selection_is_order_independent_and_explainable() {
        let first = capture("z-content", "body-z", "https://example.invalid/seed-z");
        let second = capture("a-content", "body-a", "https://example.invalid/seed-a");
        let mut forward = [first.clone(), second.clone()];
        let mut reverse = [second, first];
        forward.sort_by_key(capture_selection_key);
        reverse.sort_by_key(capture_selection_key);
        assert_eq!(capture_id(&forward[0]), capture_id(&reverse[0]));
        assert_eq!(forward[0].content, reverse[0].content);
        assert!(capture_selection_key(&forward[0]) < capture_selection_key(&forward[1]));
    }

    #[test]
    fn materialize_conflicts_publishes_one_page_and_selected_passages() {
        let config = config();
        let first_root = temp_root("forward");
        let second_root = temp_root("reverse");
        for (root, captures) in [
            (
                &first_root,
                vec![
                    capture("z-content", "body-z", "https://example.invalid/seed-z"),
                    capture("a-content", "body-a", "https://example.invalid/seed-a"),
                ],
            ),
            (
                &second_root,
                vec![
                    capture("a-content", "body-a", "https://example.invalid/seed-a"),
                    capture("z-content", "body-z", "https://example.invalid/seed-z"),
                ],
            ),
        ] {
            let crawl_dir = root.join("crawl");
            fs::create_dir_all(crawl_dir.join("bodies")).expect("create crawl dir");
            let result = CrawlResult {
                captures,
                rejected_count: 0,
                failed_count: 0,
                stop_reason: "frontier_exhausted".into(),
            };
            materialize(
                root,
                &crawl_dir,
                "run-fixed",
                &config,
                "2025-01-01T00:00:00.000Z",
                "2025-01-01T00:00:01.000Z",
                result,
            )
            .expect("materialize conflict fixture");
        }
        let first_snapshot = fs::read_dir(first_root.join("corpora"))
            .expect("first corpus")
            .next()
            .expect("first snapshot")
            .expect("first entry")
            .path();
        let second_snapshot = fs::read_dir(second_root.join("corpora"))
            .expect("second corpus")
            .next()
            .expect("second snapshot")
            .expect("second entry")
            .path();
        assert_eq!(
            fs::read(first_snapshot.join("pages.jsonl")).expect("first pages"),
            fs::read(second_snapshot.join("pages.jsonl")).expect("second pages")
        );
        assert_eq!(
            fs::read(first_snapshot.join("passages.jsonl")).expect("first passages"),
            fs::read(second_snapshot.join("passages.jsonl")).expect("second passages")
        );
        assert_eq!(
            fs::read_to_string(first_snapshot.join("pages.jsonl"))
                .expect("pages")
                .lines()
                .count(),
            1
        );
        let manifest: Value = serde_json::from_slice(
            &fs::read(first_root.join("crawl/crawl-manifest.json")).expect("crawl manifest"),
        )
        .expect("parse crawl manifest");
        assert_eq!(manifest["summary"]["captured_page_count"], 2);
        assert_eq!(manifest["summary"]["page_count"], 1);
        assert!(
            manifest["pages"]
                .as_array()
                .expect("manifest pages")
                .iter()
                .any(|page| page["admission_status"] == "superseded-capture")
        );
        let _ = fs::remove_dir_all(first_root);
        let _ = fs::remove_dir_all(second_root);
    }

    #[test]
    fn snapshot_validation_rejects_duplicate_page_ids() {
        let page = PageRecord {
            page_id: page_id("https://example.invalid/docs"),
            source_id: "fixture".into(),
            normalized_final_url: "https://example.invalid/docs".into(),
            title: "Docs".into(),
            description: None,
            language: None,
            canonical_url: None,
            extraction_method: "test".into(),
            content: "content".into(),
            content_identity: "Prose\0content".into(),
            content_sha256: sha256_hex(b"Prose\0content"),
            body_sha256: "body".into(),
            body_file: "bodies/body".into(),
        };
        assert!(validate_snapshot_records(&[page.clone(), page], &[], &[]).is_err());
    }
}

pub(crate) fn materialize_existing_spool(
    data_dir: &std::path::Path,
    source_run_id: &str,
    config_path: &std::path::Path,
    config: &Config,
    events: &Arc<Mutex<Events>>,
) -> Result<SnapshotResult, Error> {
    let valid_run_id = source_run_id.strip_prefix("run-").is_some_and(|suffix| {
        suffix.len() == 64 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    });
    if !valid_run_id {
        return Err(Error::invalid("invalid source Crawl run ID"));
    }
    let crawl_dir = data_dir.join("crawls").join(source_run_id);
    let spool_path = crawl_dir.join("capture-spool.sqlite3");
    if !spool_path.is_file() || crawl_dir.join("crawl-manifest.json").exists() {
        return Err(Error::invalid(
            "source Crawl needs an unsealed capture spool and no complete manifest",
        ));
    }
    let (started_at, completed_at, rejected_count, failed_count) =
        read_source_run_summary(data_dir, source_run_id, config_path, config)?;
    let spool = CaptureSpool::open(&crawl_dir, config.max_frontier)?;
    spool.validate_source_scope(&config.sources)?;
    let groups = spool.unique_groups_by_source()?;
    let total_groups: usize = groups.values().sum();
    let quotas_met = config.sources.iter().all(|source| {
        source.minimum_page_quota == 0
            || groups.get(&source.source_id).copied().unwrap_or_default()
                >= source.minimum_page_quota
    });
    if total_groups < config.page_target || !quotas_met {
        return Err(Error::invalid(format!(
            "capture spool has {total_groups} unique groups; target {} and source quotas are not met",
            config.page_target
        )));
    }

    let staging = data_dir
        .join("corpora")
        .join(format!(".staging-{source_run_id}"));
    let preserved = data_dir
        .join("corpora")
        .join(format!(".interrupted-{source_run_id}"));
    if preserved.exists() {
        if !["pages.jsonl", "aliases.jsonl", "passages.jsonl"]
            .iter()
            .all(|name| preserved.join(name).is_file())
        {
            return Err(Error::invalid(
                "preserved materialization is missing a complete JSONL file set",
            ));
        }
        let snapshot = seal_existing_materialized_spool(
            data_dir,
            &crawl_dir,
            source_run_id,
            config,
            &started_at,
            &completed_at,
            rejected_count,
            failed_count,
            &spool,
            &preserved,
        )?;
        emit_shared(
            events,
            "crawl.materialization.snapshot_sealed",
            "success",
            json!({"source_run_id": source_run_id, "snapshot_id": snapshot.id, "unique_groups": total_groups, "reused_preserved_jsonl": true}),
        );
        return Ok(snapshot);
    }
    if staging.exists() {
        fs::rename(&staging, &preserved).map_err(|error| {
            Error::storage(format!("preserve interrupted materialization: {error}"))
        })?;
    }
    let result = CrawlResult {
        captures: Vec::new(),
        rejected_count,
        failed_count,
        stop_reason: "page_target_reached".into(),
    };
    let snapshot = materialize_spooled(
        data_dir,
        &crawl_dir,
        source_run_id,
        config,
        &started_at,
        &completed_at,
        result,
        &spool,
    )?;
    emit_shared(
        events,
        "crawl.materialization.snapshot_sealed",
        "success",
        json!({"source_run_id": source_run_id, "snapshot_id": snapshot.id, "unique_groups": total_groups}),
    );
    Ok(snapshot)
}

fn read_source_run_summary(
    data_dir: &std::path::Path,
    source_run_id: &str,
    config_path: &std::path::Path,
    config: &Config,
) -> Result<(String, String, usize, usize), Error> {
    let path = data_dir.join("logs").join(format!("{source_run_id}.jsonl"));
    let file = File::open(&path)
        .map_err(|error| Error::storage(format!("open source Crawl event log: {error}")))?;
    let mut first_timestamp = None;
    let mut first_event = None;
    let mut recorded_config_path = None;
    let mut last_timestamp = None;
    let mut last_event = None;
    let mut policy_matches = false;
    let mut recorded_config_digest = None;
    let mut rejected_count = 0;
    let mut failed_count = 0;
    let expected_policy = serde_json::to_value(config.policy())
        .map_err(|error| Error::invalid(format!("serialize Crawl policy: {error}")))?;
    for line in BufReader::new(file).lines() {
        let line =
            line.map_err(|error| Error::storage(format!("read source Crawl event log: {error}")))?;
        let event: Value = serde_json::from_str(&line)
            .map_err(|error| Error::invalid(format!("invalid source Crawl event log: {error}")))?;
        let timestamp = event
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid("source Crawl event has no timestamp"))?
            .to_owned();
        if event.get("run_id").and_then(Value::as_str) != Some(source_run_id) {
            return Err(Error::invalid("source Crawl event has the wrong run ID"));
        }
        first_timestamp.get_or_insert_with(|| timestamp.clone());
        last_timestamp = Some(timestamp);
        let event_name = event
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or_default();
        first_event.get_or_insert_with(|| event_name.to_owned());
        last_event = Some(event_name.to_owned());
        if event_name == "crawl.started" {
            recorded_config_path = event
                .get("data")
                .and_then(|data| data.get("config_path"))
                .and_then(Value::as_str)
                .map(std::path::PathBuf::from);
        }
        if event_name == "crawl.policy" {
            policy_matches = event.get("data") == Some(&expected_policy);
        }
        if event_name == "crawl.config" {
            let digest = event
                .get("data")
                .and_then(|data| data.get("source_config_sha256"))
                .and_then(Value::as_str)
                .ok_or_else(|| Error::invalid("source Crawl config event has no digest"))?;
            if recorded_config_digest.is_some_and(|previous| previous != digest) {
                return Err(Error::invalid("source Crawl config digest is inconsistent"));
            }
            recorded_config_digest = Some(digest.to_owned());
        }
        rejected_count += usize::from(event_name == "page.rejected");
        failed_count += usize::from(event_name == "fetch.failed");
    }
    if last_event.as_deref() != Some("crawl.concurrency") {
        return Err(Error::invalid(
            "source Crawl did not finish capture and concurrency reporting",
        ));
    }
    if first_event.as_deref() != Some("crawl.started") || !policy_matches {
        return Err(Error::invalid(
            "source Crawl config or policy does not match the supplied config",
        ));
    }
    let current_config_path = fs::canonicalize(config_path)
        .map_err(|error| Error::invalid(format!("resolve supplied Crawl config: {error}")))?;
    let original_config_path = recorded_config_path
        .ok_or_else(|| Error::invalid("source Crawl log has no config path"))?;
    let original_config_path = fs::canonicalize(original_config_path)
        .map_err(|error| Error::invalid(format!("resolve source Crawl config: {error}")))?;
    if original_config_path != current_config_path {
        return Err(Error::invalid(
            "supplied Crawl config path differs from the source Crawl config path",
        ));
    }
    if recorded_config_digest.is_some_and(|digest| digest != config.digest) {
        return Err(Error::invalid(
            "supplied Crawl config digest differs from the source Crawl config digest",
        ));
    }
    Ok((
        first_timestamp.ok_or_else(|| Error::invalid("source Crawl event log is empty"))?,
        last_timestamp.ok_or_else(|| Error::invalid("source Crawl event log is empty"))?,
        rejected_count,
        failed_count,
    ))
}
