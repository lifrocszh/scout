use super::checksums::{directory_bytes, directory_digest};
use crate::storage::write_json;
use crate::{BuildProfile, CatalogRecord, Error};
use std::fs;
use std::time::Instant;
use tantivy::schema::{IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions};
use tantivy::{Index, TantivyDocument};

pub(crate) fn register_keyword_analyzer(
    index: &mut Index,
    profile: &BuildProfile,
) -> Result<(), Error> {
    let analyzer = index
        .tokenizers()
        .get("default")
        .ok_or_else(|| Error::not_ready("Tantivy default analyzer unavailable"))?;
    index.tokenizers().register(&profile.analyzer_id, analyzer);
    Ok(())
}

pub(super) fn build_keyword_index(
    staging: &std::path::Path,
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
        &serde_json::json!({
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
