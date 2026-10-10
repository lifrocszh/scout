use super::*;
use crate::extract::extract;
use crate::spool::{CaptureSpool, CaptureStub};
use rusqlite::{Connection, params};
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use ::url::Url;
use serde_json::Value;

const SQLITE_CACHE_KIB: i64 = -8192;

pub(crate) fn materialize_spooled(
    data_dir: &Path,
    crawl_dir: &Path,
    run_id: &str,
    config: &Config,
    started: &str,
    completed: &str,
    result: CrawlResult,
    spool: &CaptureSpool,
) -> Result<SnapshotResult, Error> {
    let connection = Connection::open(spool.path())
        .map_err(|error| Error::storage(format!("open capture spool for materialization: {error}")))?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .and_then(|_| connection.pragma_update(None, "temp_store", "FILE"))
        .and_then(|_| connection.pragma_update(None, "cache_size", SQLITE_CACHE_KIB))
        .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
        .map_err(|error| Error::storage(format!("configure materialization database: {error}")))?;
    connection
        .execute_batch(
            "CREATE TEMP TABLE url_winners AS
             SELECT row_id, source_id, final_url, content_hash, selection_key, payload_json
             FROM (
                 SELECT c.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY source_id, final_url
                            ORDER BY selection_key, row_id
                        ) AS winner_rank
                 FROM captures AS c
             )
             WHERE winner_rank = 1;
             CREATE INDEX url_winners_identity
                 ON url_winners(source_id, content_hash, final_url, selection_key, row_id);
             CREATE INDEX url_winners_url
                 ON url_winners(source_id, final_url, content_hash, row_id);
             CREATE TEMP TABLE identity_representatives AS
             SELECT row_id, source_id, final_url, content_hash, selection_key, payload_json
             FROM (
                 SELECT w.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY source_id, content_hash
                            ORDER BY final_url, selection_key, row_id
                        ) AS representative_rank
                 FROM url_winners AS w
             )
             WHERE representative_rank = 1;
             CREATE INDEX identity_representatives_order
                 ON identity_representatives(source_id, final_url);
             CREATE INDEX identity_representatives_identity
                 ON identity_representatives(source_id, content_hash);
             CREATE TEMP TABLE output_pages (
                 page_id TEXT PRIMARY KEY,
                 source_id TEXT NOT NULL,
                 final_url TEXT NOT NULL,
                 content_identity TEXT NOT NULL,
                 payload_json TEXT NOT NULL,
                 UNIQUE(source_id, final_url)
             ) WITHOUT ROWID;
             CREATE TEMP TABLE output_aliases (
                 alias_page_id TEXT PRIMARY KEY,
                 source_id TEXT NOT NULL,
                 alias_url TEXT NOT NULL,
                 representative_page_id TEXT NOT NULL,
                 payload_json TEXT NOT NULL,
                 UNIQUE(source_id, alias_url),
                 FOREIGN KEY(representative_page_id) REFERENCES output_pages(page_id)
             ) WITHOUT ROWID;
             CREATE TEMP TABLE output_passages (
                 passage_id TEXT PRIMARY KEY,
                 page_id TEXT NOT NULL,
                 source_id TEXT NOT NULL,
                 final_url TEXT NOT NULL,
                 ordinal INTEGER NOT NULL,
                 payload_json TEXT NOT NULL,
                 UNIQUE(page_id, ordinal),
                 FOREIGN KEY(page_id) REFERENCES output_pages(page_id)
             ) WITHOUT ROWID;
             CREATE INDEX output_passages_order
                 ON output_passages(source_id, final_url, ordinal);",
        )
        .map_err(|error| Error::storage(format!("prepare bounded materialization: {error}")))?;

    let captured_page_count = count(&connection, "SELECT COUNT(*) FROM captures")?;
    let winner_count = count(&connection, "SELECT COUNT(*) FROM url_winners")?;
    let expected_page_count = count(&connection, "SELECT COUNT(*) FROM identity_representatives")?;
    let expected_alias_count = winner_count.saturating_sub(expected_page_count);
    if !result.captures.is_empty() {
        return Err(Error::invalid(
            "bounded materialization received in-memory captures",
        ));
    }

    let corpora = data_dir.join("corpora");
    fs::create_dir_all(&corpora)
        .map_err(|error| Error::storage(format!("create corpora directory: {error}")))?;
    let staging = corpora.join(format!(".staging-{run_id}"));
    fs::create_dir_all(&staging)
        .map_err(|error| Error::storage(format!("create snapshot staging: {error}")))?;
    let pages_path = staging.join("pages.jsonl");
    let aliases_path = staging.join("aliases.jsonl");
    let passages_path = staging.join("passages.jsonl");
    let mut pages_writer = new_jsonl_writer(&pages_path)?;
    let mut snapshot_identity = DigestWriter::new(io::sink());
    snapshot_identity
        .write_all(config.digest.as_bytes())
        .and_then(|_| snapshot_identity.write_all(b"\0["))
        .map_err(|error| Error::storage(format!("start snapshot identity: {error}")))?;

    let mut first_page = true;
    let mut page_count = 0usize;
    let mut representatives = connection
        .prepare(
            "SELECT payload_json, selection_key
             FROM identity_representatives
             ORDER BY source_id, final_url",
        )
        .map_err(|error| Error::storage(format!("query representative Pages: {error}")))?;
    let mut representative_rows = representatives
        .query([])
        .map_err(|error| Error::storage(format!("read representative Pages: {error}")))?;
    while let Some(row) = representative_rows
        .next()
        .map_err(|error| Error::storage(format!("read representative Page: {error}")))?
    {
        let payload_json: String = row
            .get(0)
            .map_err(|error| Error::storage(format!("read representative metadata: {error}")))?;
        let selection_key: String = row
            .get(1)
            .map_err(|error| Error::storage(format!("read representative selection: {error}")))?;
        let stub = decode_stub(&payload_json)?;
        let capture = restore_capture(crawl_dir, config, stub)?;
        if capture_selection_key(&capture) != selection_key {
            return Err(Error::invalid(
                "spooled capture selection key does not match its body",
            ));
        }
        let record = PageRecord {
            page_id: page_id(&capture.final_url),
            source_id: capture.source_id.clone(),
            normalized_final_url: capture.final_url.clone(),
            title: capture.title.clone(),
            description: capture.description.clone(),
            language: capture.language.clone(),
            canonical_url: capture.canonical_url.clone(),
            extraction_method: capture.extraction_method.into(),
            content: capture.content.clone(),
            content_identity: capture.identity.clone(),
            content_sha256: capture.content_hash.clone(),
            body_sha256: capture.body_hash.clone(),
            body_file: capture.body_file.clone(),
        };
        validate_page(&record)?;
        let record_json = encode_record(&record, "Page")?;
        connection
            .execute(
                "INSERT INTO output_pages(page_id, source_id, final_url, content_identity, payload_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    record.page_id,
                    record.source_id,
                    record.normalized_final_url,
                    record.content_identity,
                    record_json
                ],
            )
            .map_err(|error| Error::invalid(format!("duplicate or invalid Corpus Page: {error}")))?;
        if !first_page {
            snapshot_identity
                .write_all(b",")
                .map_err(|error| Error::storage(format!("hash snapshot Pages: {error}")))?;
        }
        first_page = false;
        snapshot_identity
            .write_all(record_json.as_bytes())
            .map_err(|error| Error::storage(format!("hash snapshot Page: {error}")))?;
        write_jsonl_payload(&mut pages_writer, &record_json, "pages.jsonl")?;
        page_count += 1;

        let passages = build_passages(
            &record.page_id,
            &record.source_id,
            &record.normalized_final_url,
            &record.title,
            &record.content_identity,
            &capture.blocks,
            config.max_passage_tokens,
        );
        for passage in passages {
            validate_passage(&record, &passage)?;
            let passage_json = encode_record(&passage, "Passage")?;
            connection
                .execute(
                    "INSERT INTO output_passages(
                         passage_id, page_id, source_id, final_url, ordinal, payload_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        passage.passage_id,
                        passage.page_id,
                        passage.source_id,
                        passage.normalized_final_url,
                        passage.ordinal as i64,
                        passage_json
                    ],
                )
                .map_err(|error| {
                    Error::invalid(format!("duplicate or invalid Corpus Passage: {error}"))
                })?;
        }
    }
    drop(representative_rows);
    drop(representatives);
    pages_writer
        .flush()
        .map_err(|error| Error::storage(format!("flush pages.jsonl: {error}")))?;
    let pages_jsonl_sha256 = pages_writer.finish();
    if page_count != expected_page_count {
        return Err(Error::invalid("bounded Page count does not match selection index"));
    }
    snapshot_identity
        .write_all(b"]\0[")
        .map_err(|error| Error::storage(format!("separate snapshot record arrays: {error}")))?;

    let aliases_jsonl_sha256 = write_aliases(&connection, &aliases_path)?;
    let alias_count = count(&connection, "SELECT COUNT(*) FROM output_aliases")?;
    if alias_count != expected_alias_count {
        return Err(Error::invalid("bounded Alias count does not match selection index"));
    }

    let mut passages_writer = new_jsonl_writer(&passages_path)?;
    let mut first_passage = true;
    let mut passages = connection
        .prepare(
            "SELECT payload_json FROM output_passages
             ORDER BY source_id, final_url, ordinal",
        )
        .map_err(|error| Error::storage(format!("query bounded Passages: {error}")))?;
    let mut passage_rows = passages
        .query([])
        .map_err(|error| Error::storage(format!("read bounded Passages: {error}")))?;
    while let Some(row) = passage_rows
        .next()
        .map_err(|error| Error::storage(format!("read bounded Passage: {error}")))?
    {
        let payload_json: String = row
            .get(0)
            .map_err(|error| Error::storage(format!("read Passage record: {error}")))?;
        if !first_passage {
            snapshot_identity
                .write_all(b",")
                .map_err(|error| Error::storage(format!("hash snapshot Passages: {error}")))?;
        }
        first_passage = false;
        snapshot_identity
            .write_all(payload_json.as_bytes())
            .map_err(|error| Error::storage(format!("hash snapshot Passage: {error}")))?;
        write_jsonl_payload(&mut passages_writer, &payload_json, "passages.jsonl")?;
    }
    drop(passage_rows);
    drop(passages);
    passages_writer
        .flush()
        .map_err(|error| Error::storage(format!("flush passages.jsonl: {error}")))?;
    let passages_jsonl_sha256 = passages_writer.finish();
    let passage_count = count(&connection, "SELECT COUNT(*) FROM output_passages")?;
    snapshot_identity
        .write_all(b"]")
        .and_then(|_| snapshot_identity.flush())
        .map_err(|error| Error::storage(format!("finish snapshot identity: {error}")))?;
    let snapshot_id = format!("snapshot-{}", snapshot_identity.finish());

    let mut manifest = bounded_manifest(
        &connection,
        run_id,
        config,
        started,
        completed,
        &snapshot_id,
        captured_page_count,
        page_count,
        alias_count,
        passage_count,
        result.rejected_count,
        result.failed_count,
        result.stop_reason,
    );
    manifest.manifest_sha256 = serialized_sha256(&manifest, "manifest")?;
    write_pretty_json(&crawl_dir.join("crawl-manifest.json"), &manifest)?;
    let crawl_manifest_sha256 = manifest.manifest_sha256.clone();
    drop(manifest);

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
    fs::rename(&staging, &final_dir)
        .map_err(|error| Error::storage(format!("seal snapshot: {error}")))?;
    Ok(SnapshotResult {
        id: snapshot_id,
        pages: page_count,
        aliases: alias_count,
    })
}

pub(crate) fn seal_existing_materialized_spool(
    data_dir: &Path,
    crawl_dir: &Path,
    run_id: &str,
    config: &Config,
    started: &str,
    completed: &str,
    rejected_count: usize,
    failed_count: usize,
    spool: &CaptureSpool,
    corpus_dir: &Path,
) -> Result<SnapshotResult, Error> {
    let connection = Connection::open(spool.path())
        .map_err(|error| Error::storage(format!("open capture spool for sealing: {error}")))?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .and_then(|_| connection.pragma_update(None, "temp_store", "FILE"))
        .and_then(|_| connection.pragma_update(None, "cache_size", SQLITE_CACHE_KIB))
        .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
        .map_err(|error| Error::storage(format!("configure sealing database: {error}")))?;
    connection
        .execute_batch(
            "CREATE TEMP TABLE url_winners AS
             SELECT row_id, source_id, final_url, content_hash, selection_key, payload_json
             FROM (
                 SELECT c.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY source_id, final_url
                            ORDER BY selection_key, row_id
                        ) AS winner_rank
                 FROM captures AS c
             )
             WHERE winner_rank = 1;
             CREATE INDEX url_winners_url
                 ON url_winners(source_id, final_url, content_hash, row_id);
             CREATE TEMP TABLE identity_representatives AS
             SELECT row_id, source_id, final_url, content_hash, selection_key, payload_json
             FROM (
                 SELECT w.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY source_id, content_hash
                            ORDER BY final_url, selection_key, row_id
                        ) AS representative_rank
                 FROM url_winners AS w
             )
             WHERE representative_rank = 1;
             CREATE INDEX identity_representatives_identity
                 ON identity_representatives(source_id, content_hash);
             CREATE TEMP TABLE preserved_page_ids (
                 page_id TEXT PRIMARY KEY
             ) WITHOUT ROWID;",
        )
        .map_err(|error| Error::storage(format!("prepare spool manifest indexes: {error}")))?;

    if corpus_dir.join("snapshot-manifest.json").exists()
        || crawl_dir.join("crawl-manifest.json").exists()
    {
        return Err(Error::invalid("source spool or Corpus is already sealed"));
    }
    let captured_page_count = count(&connection, "SELECT COUNT(*) FROM captures")?;
    let expected_pages = count(&connection, "SELECT COUNT(*) FROM identity_representatives")?;
    let expected_aliases = count(&connection, "SELECT COUNT(*) FROM url_winners")? - expected_pages;
    validate_existing_materialized_records(
        &connection,
        crawl_dir,
        config,
        corpus_dir,
        expected_pages,
        expected_aliases,
    )?;

    let mut snapshot_identity = DigestWriter::new(io::sink());
    snapshot_identity
        .write_all(config.digest.as_bytes())
        .and_then(|_| snapshot_identity.write_all(b"\0["))
        .map_err(|error| Error::storage(format!("start recovered snapshot identity: {error}")))?;
    let (pages_jsonl_sha256, page_count) =
        hash_existing_jsonl(&corpus_dir.join("pages.jsonl"), Some(&mut snapshot_identity))?;
    snapshot_identity
        .write_all(b"]\0[")
        .map_err(|error| Error::storage(format!("separate recovered snapshot arrays: {error}")))?;
    let (aliases_jsonl_sha256, alias_count) =
        hash_existing_jsonl(&corpus_dir.join("aliases.jsonl"), None)?;
    let (passages_jsonl_sha256, passage_count) = hash_existing_jsonl(
        &corpus_dir.join("passages.jsonl"),
        Some(&mut snapshot_identity),
    )?;
    snapshot_identity
        .write_all(b"]")
        .and_then(|_| snapshot_identity.flush())
        .map_err(|error| Error::storage(format!("finish recovered snapshot identity: {error}")))?;
    if page_count != expected_pages || alias_count != expected_aliases || passage_count == 0 {
        return Err(Error::invalid(format!(
            "preserved materialized records do not match the capture spool: Pages {page_count}/{expected_pages}, Aliases {alias_count}/{expected_aliases}, Passages {passage_count}"
        )));
    }
    let snapshot_id = format!("snapshot-{}", snapshot_identity.finish());
    let final_dir = data_dir.join("corpora").join(&snapshot_id);
    if final_dir.exists() {
        return Err(Error::storage(format!(
            "snapshot already exists: {}",
            final_dir.display()
        )));
    }

    let mut manifest = bounded_manifest(
        &connection,
        run_id,
        config,
        started,
        completed,
        &snapshot_id,
        captured_page_count,
        page_count,
        alias_count,
        passage_count,
        rejected_count,
        failed_count,
        "page_target_reached".into(),
    );
    manifest.manifest_sha256 = serialized_sha256(&manifest, "recovered Crawl manifest")?;
    write_pretty_json(&crawl_dir.join("crawl-manifest.json"), &manifest)?;
    let crawl_manifest_sha256 = manifest.manifest_sha256.clone();
    drop(manifest);

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
    write_json(&corpus_dir.join("snapshot-manifest.json"), &snapshot_manifest)?;
    fs::rename(corpus_dir, &final_dir)
        .map_err(|error| Error::storage(format!("seal recovered snapshot: {error}")))?;
    Ok(SnapshotResult {
        id: snapshot_id,
        pages: page_count,
        aliases: alias_count,
    })
}

fn validate_existing_materialized_records(
    connection: &Connection,
    crawl_dir: &Path,
    config: &Config,
    corpus_dir: &Path,
    expected_pages: usize,
    expected_aliases: usize,
) -> Result<(), Error> {
    let pages_path = corpus_dir.join("pages.jsonl");
    let aliases_path = corpus_dir.join("aliases.jsonl");
    let passages_path = corpus_dir.join("passages.jsonl");
    let mut pages = BufReader::new(
        File::open(&pages_path)
            .map_err(|error| Error::storage(format!("open preserved Pages: {error}")))?,
    );
    let mut aliases = BufReader::new(
        File::open(&aliases_path)
            .map_err(|error| Error::storage(format!("open preserved Aliases: {error}")))?,
    );
    let mut passages = BufReader::new(
        File::open(&passages_path)
            .map_err(|error| Error::storage(format!("open preserved Passages: {error}")))?,
    );
    let mut insert_page_id = connection
        .prepare_cached("INSERT OR IGNORE INTO preserved_page_ids(page_id) VALUES (?1)")
        .map_err(|error| {
            Error::storage(format!("prepare preserved Page identity check: {error}"))
        })?;
    let mut page_count = 0;
    let mut passage_count = 0;
    let mut representatives = connection
        .prepare(
            "SELECT payload_json, selection_key
             FROM identity_representatives
             ORDER BY source_id, final_url",
        )
        .map_err(|error| Error::storage(format!("query preserved Page selections: {error}")))?;
    let mut rows = representatives
        .query([])
        .map_err(|error| Error::storage(format!("read preserved Page selections: {error}")))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| Error::storage(format!("read preserved Page selection: {error}")))?
    {
        let payload_json: String = row
            .get(0)
            .map_err(|error| Error::storage(format!("read preserved Page metadata: {error}")))?;
        let selection_key: String = row
            .get(1)
            .map_err(|error| Error::storage(format!("read preserved Page selection: {error}")))?;
        let capture = restore_capture(crawl_dir, config, decode_stub(&payload_json)?)?;
        if capture_selection_key(&capture) != selection_key {
            return Err(Error::invalid(
                "spooled Page selection key does not match its body",
            ));
        }
        let page = PageRecord {
            page_id: page_id(&capture.final_url),
            source_id: capture.source_id.clone(),
            normalized_final_url: capture.final_url.clone(),
            title: capture.title.clone(),
            description: capture.description.clone(),
            language: capture.language.clone(),
            canonical_url: capture.canonical_url.clone(),
            extraction_method: capture.extraction_method.into(),
            content: capture.content.clone(),
            content_identity: capture.identity.clone(),
            content_sha256: capture.content_hash.clone(),
            body_sha256: capture.body_hash.clone(),
            body_file: capture.body_file.clone(),
        };
        validate_page(&page)?;
        if insert_page_id
            .execute([page.page_id.as_str()])
            .map_err(|error| Error::storage(format!("record preserved Page identity: {error}")))?
            == 0
        {
            return Err(Error::invalid("preserved Pages contain duplicate identities"));
        }
        verify_jsonl_record(&mut pages, &page, "pages.jsonl", page_count)?;
        for passage in build_passages(
            &page.page_id,
            &page.source_id,
            &page.normalized_final_url,
            &page.title,
            &page.content_identity,
            &capture.blocks,
            config.max_passage_tokens,
        ) {
            validate_passage(&page, &passage)?;
            verify_jsonl_record(&mut passages, &passage, "passages.jsonl", passage_count)?;
            passage_count += 1;
        }
        page_count += 1;
    }
    drop(rows);
    drop(representatives);
    verify_jsonl_end(&mut pages, "pages.jsonl", page_count)?;
    verify_jsonl_end(&mut passages, "passages.jsonl", passage_count)?;
    if page_count != expected_pages || passage_count == 0 {
        return Err(Error::invalid(format!(
            "preserved Pages/Passages do not match the capture spool: Pages {page_count}/{expected_pages}, Passages {passage_count}"
        )));
    }

    let mut statement = connection
        .prepare(
            "SELECT w.payload_json, w.selection_key, r.payload_json
             FROM url_winners AS w
             JOIN identity_representatives AS r
               ON r.source_id = w.source_id AND r.content_hash = w.content_hash
             WHERE w.row_id <> r.row_id
             ORDER BY w.source_id, w.final_url",
        )
        .map_err(|error| Error::storage(format!("query preserved Alias selections: {error}")))?;
    let mut check_page_id = connection
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM preserved_page_ids WHERE page_id = ?1)")
        .map_err(|error| {
            Error::storage(format!("prepare preserved Alias reference check: {error}"))
        })?;
    let mut rows = statement
        .query([])
        .map_err(|error| Error::storage(format!("read preserved Alias selections: {error}")))?;
    let mut alias_count = 0;
    while let Some(row) = rows
        .next()
        .map_err(|error| Error::storage(format!("read preserved Alias selection: {error}")))?
    {
        let alias_json: String = row
            .get(0)
            .map_err(|error| Error::storage(format!("read preserved Alias metadata: {error}")))?;
        let selection_key: String = row
            .get(1)
            .map_err(|error| Error::storage(format!("read preserved Alias selection: {error}")))?;
        let representative_json: String = row
            .get(2)
            .map_err(|error| Error::storage(format!("read preserved representative: {error}")))?;
        let capture = restore_capture(crawl_dir, config, decode_stub(&alias_json)?)?;
        if capture_selection_key(&capture) != selection_key {
            return Err(Error::invalid(
                "spooled Alias selection key does not match its body",
            ));
        }
        let representative = decode_stub(&representative_json)?;
        let alias_page_id = page_id(&capture.final_url);
        let representative_page_id = page_id(&representative.final_url);
        let alias_is_page: bool = check_page_id
            .query_row([alias_page_id.as_str()], |row| row.get(0))
            .map_err(|error| Error::storage(format!("check preserved Alias identity: {error}")))?;
        let representative_exists: bool = check_page_id
            .query_row([representative_page_id.as_str()], |row| row.get(0))
            .map_err(|error| {
                Error::storage(format!("check preserved representative Page: {error}"))
            })?;
        if capture.source_id != representative.source_id
            || alias_is_page
            || alias_page_id == representative_page_id
            || !representative_exists
        {
            return Err(Error::invalid(
                "preserved Alias has an invalid Page identity or reference",
            ));
        }
        let alias = AliasRecord {
            source_id: capture.source_id.clone(),
            alias_url: capture.final_url.clone(),
            alias_page_id,
            representative_page_id,
            alias_kind: "exact-alias",
            requested_url: capture.requested_url,
            redirect_chain: capture.redirects,
            canonical_url: capture.canonical_url,
        };
        verify_jsonl_record(&mut aliases, &alias, "aliases.jsonl", alias_count)?;
        alias_count += 1;
    }
    drop(rows);
    drop(statement);
    verify_jsonl_end(&mut aliases, "aliases.jsonl", alias_count)?;
    if alias_count != expected_aliases {
        return Err(Error::invalid(format!(
            "preserved Aliases do not match the capture spool: Aliases {alias_count}/{expected_aliases}"
        )));
    }
    Ok(())
}

fn verify_jsonl_record<R: BufRead, T: Serialize>(
    reader: &mut R,
    expected: &T,
    file_name: &str,
    record_index: usize,
) -> Result<(), Error> {
    let expected = serde_json::to_vec(expected)
        .map_err(|error| Error::storage(format!("serialize expected {file_name} row: {error}")))?;
    let mut actual = vec![0; expected.len()];
    if let Err(error) = reader.read_exact(&mut actual) {
        return Err(Error::invalid(format!(
            "preserved {file_name} is missing or truncates expected row {record_index}: {error}"
        )));
    }
    let mut newline = [0; 1];
    if reader.read_exact(&mut newline).is_err() || newline[0] != b'\n' || actual != expected {
        return Err(Error::invalid(format!(
            "preserved {file_name} row {record_index} does not match the capture spool"
        )));
    }
    Ok(())
}

fn verify_jsonl_end<R: BufRead>(
    reader: &mut R,
    file_name: &str,
    expected_count: usize,
) -> Result<(), Error> {
    let mut extra = Vec::new();
    if reader
        .read_until(b'\n', &mut extra)
        .map_err(|error| Error::storage(format!("read preserved {file_name}: {error}")))?
        != 0
    {
        return Err(Error::invalid(format!(
            "preserved {file_name} has extra rows after expected row count {expected_count}"
        )));
    }
    Ok(())
}

fn hash_existing_jsonl(
    path: &Path,
    mut snapshot_identity: Option<&mut DigestWriter<io::Sink>>,
) -> Result<(String, usize), Error> {
    let file = File::open(path)
        .map_err(|error| Error::storage(format!("open preserved {}: {error}", path.display())))?;
    let mut source = BufReader::new(file);
    let mut file_digest = Sha256::new();
    let mut line = Vec::new();
    let mut count = 0;
    let mut first_identity = true;
    loop {
        line.clear();
        let bytes_read = source
            .read_until(b'\n', &mut line)
            .map_err(|error| Error::storage(format!("read preserved {}: {error}", path.display())))?;
        if bytes_read == 0 {
            break;
        }
        file_digest.update(&line);
        let payload = line.strip_suffix(b"\n").unwrap_or(&line);
        let payload = payload.strip_suffix(b"\r").unwrap_or(payload);
        if payload.is_empty() {
            continue;
        }
        serde_json::from_slice::<Value>(payload)
            .map_err(|error| Error::invalid(format!("invalid preserved {} record: {error}", path.display())))?;
        if let Some(identity) = snapshot_identity.as_deref_mut() {
            if !first_identity {
                identity
                    .write_all(b",")
                    .map_err(|error| Error::storage(format!("separate recovered snapshot records: {error}")))?;
            }
            first_identity = false;
            identity
                .write_all(payload)
                .map_err(|error| Error::storage(format!("hash preserved {} record: {error}", path.display())))?;
        }
        count += 1;
    }
    Ok((format!("{:x}", file_digest.finalize()), count))
}

fn write_aliases(connection: &Connection, path: &Path) -> Result<String, Error> {
    let mut writer = new_jsonl_writer(path)?;
    let mut statement = connection
        .prepare(
            "SELECT w.payload_json, r.payload_json
             FROM url_winners AS w
             JOIN identity_representatives AS r
               ON r.source_id = w.source_id AND r.content_hash = w.content_hash
             WHERE w.row_id <> r.row_id
             ORDER BY w.source_id, w.final_url",
        )
        .map_err(|error| Error::storage(format!("query bounded Aliases: {error}")))?;
    let mut rows = statement
        .query([])
        .map_err(|error| Error::storage(format!("read bounded Aliases: {error}")))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| Error::storage(format!("read bounded Alias: {error}")))?
    {
        let alias_json: String = row
            .get(0)
            .map_err(|error| Error::storage(format!("read Alias capture: {error}")))?;
        let representative_json: String = row
            .get(1)
            .map_err(|error| Error::storage(format!("read Alias representative: {error}")))?;
        let capture = decode_stub(&alias_json)?;
        let representative = decode_stub(&representative_json)?;
        if capture.source_id != representative.source_id {
            return Err(Error::invalid("Corpus Alias crosses Source boundary"));
        }
        let alias_page_id = page_id(&capture.final_url);
        let representative_page_id = page_id(&representative.final_url);
        let page_exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM output_pages WHERE page_id = ?1)",
                [&alias_page_id],
                |row| row.get(0),
            )
            .map_err(|error| Error::storage(format!("validate Alias Page identity: {error}")))?;
        if page_exists || alias_page_id == representative_page_id {
            return Err(Error::invalid("Corpus Alias identity collides with a Page"));
        }
        let alias = AliasRecord {
            source_id: capture.source_id.clone(),
            alias_url: capture.final_url.clone(),
            alias_page_id: alias_page_id.clone(),
            representative_page_id: representative_page_id.clone(),
            alias_kind: "exact-alias",
            requested_url: capture.requested_url,
            redirect_chain: capture.redirects,
            canonical_url: capture.canonical_url,
        };
        let payload_json = encode_record(&alias, "Alias")?;
        connection
            .execute(
                "INSERT INTO output_aliases(
                     alias_page_id, source_id, alias_url, representative_page_id, payload_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    alias.alias_page_id,
                    alias.source_id,
                    alias.alias_url,
                    alias.representative_page_id,
                    payload_json
                ],
            )
            .map_err(|error| Error::invalid(format!("duplicate or invalid Corpus Alias: {error}")))?;
        write_jsonl_payload(&mut writer, &payload_json, "aliases.jsonl")?;
    }
    drop(rows);
    drop(statement);
    writer
        .flush()
        .map_err(|error| Error::storage(format!("flush aliases.jsonl: {error}")))?;
    Ok(writer.finish())
}

fn bounded_manifest<'a>(
    connection: &'a Connection,
    run_id: &str,
    config: &Config,
    started: &str,
    completed: &str,
    snapshot_id: &str,
    captured_page_count: usize,
    page_count: usize,
    alias_count: usize,
    passage_count: usize,
    rejected_count: usize,
    failed_count: usize,
    stop_reason: String,
) -> BoundedManifest<'a> {
    BoundedManifest {
        schema_version: CRAWL_SCHEMA,
        run_id: run_id.into(),
        status: "complete",
        started_at_utc: started.into(),
        completed_at_utc: completed.into(),
        source_config_sha256: config.digest.clone(),
        manifest_sha256: String::new(),
        snapshot_id: Some(snapshot_id.into()),
        capture_selection_policy: CAPTURE_SELECTION_POLICY,
        user_agent: config.user_agent.clone(),
        robots_user_agent: "Scout",
        policy: config.policy(),
        sources: config.sources.clone(),
        pages: ManifestPageStream { connection },
        summary: Summary {
            captured_page_count,
            page_count,
            alias_count,
            passage_count,
            rejected_count,
            failed_count,
            target_page_count: config.page_target,
            stop_reason,
        },
        error: None,
    }
}

#[derive(Serialize)]
struct BoundedManifest<'a> {
    schema_version: &'static str,
    run_id: String,
    status: &'static str,
    started_at_utc: String,
    completed_at_utc: String,
    source_config_sha256: String,
    manifest_sha256: String,
    snapshot_id: Option<String>,
    capture_selection_policy: &'static str,
    user_agent: String,
    robots_user_agent: &'static str,
    policy: super::super::CrawlPolicy,
    sources: Vec<super::super::Source>,
    pages: ManifestPageStream<'a>,
    summary: Summary,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ManifestError>,
}

struct ManifestPageStream<'a> {
    connection: &'a Connection,
}

impl Serialize for ManifestPageStream<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(None)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT c.row_id, c.payload_json, c.selection_key,
                        s.row_id, s.payload_json, s.selection_key,
                        r.row_id, r.payload_json
                 FROM captures AS c
                 JOIN url_winners AS s
                   ON s.source_id = c.source_id AND s.final_url = c.final_url
                 JOIN identity_representatives AS r
                   ON r.source_id = s.source_id AND r.content_hash = s.content_hash
                 ORDER BY c.source_id, c.final_url, c.row_id",
            )
            .map_err(serde::ser::Error::custom)?;
        let mut rows = statement.query([]).map_err(serde::ser::Error::custom)?;
        while let Some(row) = rows.next().map_err(serde::ser::Error::custom)? {
            let capture_row_id: i64 = row.get(0).map_err(serde::ser::Error::custom)?;
            let capture_json: String = row.get(1).map_err(serde::ser::Error::custom)?;
            let selection_key: String = row.get(2).map_err(serde::ser::Error::custom)?;
            let selected_row_id: i64 = row.get(3).map_err(serde::ser::Error::custom)?;
            let selected_json: String = row.get(4).map_err(serde::ser::Error::custom)?;
            let selected_key: String = row.get(5).map_err(serde::ser::Error::custom)?;
            let representative_row_id: i64 = row.get(6).map_err(serde::ser::Error::custom)?;
            let representative_json: String = row.get(7).map_err(serde::ser::Error::custom)?;
            let capture = serde_json::from_str::<CaptureStub>(&capture_json)
                .map_err(serde::ser::Error::custom)?;
            let selected = serde_json::from_str::<CaptureStub>(&selected_json)
                .map_err(serde::ser::Error::custom)?;
            let representative = serde_json::from_str::<CaptureStub>(&representative_json)
                .map_err(serde::ser::Error::custom)?;
            let value = make_manifest_page(
                capture_row_id,
                capture,
                selection_key,
                selected_row_id,
                selected,
                selected_key,
                representative_row_id,
                representative,
            );
            sequence.serialize_element(&value)?;
        }
        sequence.end()
    }
}

fn make_manifest_page(
    capture_row_id: i64,
    capture: CaptureStub,
    selection_key: String,
    selected_row_id: i64,
    selected: CaptureStub,
    selected_key: String,
    representative_row_id: i64,
    representative: CaptureStub,
) -> ManifestPage {
    let capture_id = capture_id_parts(&capture.source_id, &capture.final_url, &selection_key);
    let selected_capture_id =
        capture_id_parts(&selected.source_id, &selected.final_url, &selected_key);
    let is_selected = capture_row_id == selected_row_id;
    let is_alias = is_selected && selected_row_id != representative_row_id;
    let selection_reason = if !is_selected {
        if capture.content_hash == selected.content_hash {
            "superseded-duplicate-capture"
        } else {
            "superseded-conflicting-content"
        }
    } else if is_alias {
        "exact-content-alias"
    } else {
        "deterministic-minimum-selection"
    };
    let admission_status = if !is_selected {
        "superseded-capture"
    } else if is_alias {
        "exact-alias"
    } else {
        "admitted"
    };
    ManifestPage {
        requested_url: capture.requested_url,
        final_url: capture.final_url.clone(),
        redirect_chain: capture.redirects,
        source_id: capture.source_id,
        page_id: page_id(&capture.final_url),
        admission_status: admission_status.into(),
        capture_id,
        selection_key,
        selected_capture_id,
        selection_reason: selection_reason.into(),
        alias_of: if is_alias {
            Some(page_id(&representative.final_url))
        } else {
            None
        },
        title: capture.title,
        description: capture.description,
        language: capture.language,
        canonical_url: capture.canonical_url,
        extraction_method: capture.extraction_method,
        content_type: capture.content_type,
        content_bytes: capture.content_bytes,
        body_file: capture.body_file,
        body_sha256: capture.body_hash,
        content_sha256: capture.content_hash,
        status_code: capture.status_code,
    }
}

fn capture_id_parts(source_id: &str, final_url: &str, selection_key: &str) -> String {
    sha256_hex(
        format!("scout.capture.v1\0{source_id}\0{final_url}\0{selection_key}").as_bytes(),
    )
}

fn restore_capture(crawl_dir: &Path, config: &Config, stub: CaptureStub) -> Result<Capture, Error> {
    let body_path = crawl_dir.join(&stub.body_file);
    let body = fs::read(&body_path)
        .map_err(|error| Error::storage(format!("read capture body {}: {error}", body_path.display())))?;
    if body.len() != stub.content_bytes || sha256_hex(&body) != stub.body_hash {
        return Err(Error::invalid("spooled capture body hash or length mismatch"));
    }
    let html = String::from_utf8(body)
        .map_err(|_| Error::invalid("spooled capture body is not UTF-8"))?;
    let html = html.trim_start_matches('\u{feff}');
    let page_url = Url::parse(&stub.final_url)
        .map_err(|error| Error::invalid(format!("invalid spooled final URL: {error}")))?;
    let extracted = extract(html, &page_url, config.min_content_chars)
        .map_err(|reason| Error::invalid(format!("spooled capture no longer extracts: {reason}")))?;
    if extracted.content_hash != stub.content_hash
        || extracted.title != stub.title
        || extracted.description != stub.description
        || extracted.language != stub.language
        || extracted.canonical_url != stub.canonical_url
        || extracted.extraction_method != stub.extraction_method
    {
        return Err(Error::invalid("spooled capture metadata does not match its body"));
    }
    Ok(Capture {
        requested_url: stub.requested_url,
        final_url: stub.final_url,
        redirects: stub.redirects,
        source_id: stub.source_id,
        title: extracted.title,
        description: extracted.description,
        language: extracted.language,
        canonical_url: extracted.canonical_url,
        extraction_method: extracted.extraction_method,
        content: extracted.content,
        identity: extracted.identity,
        content_hash: extracted.content_hash,
        blocks: extracted.blocks,
        content_bytes: stub.content_bytes,
        body_hash: stub.body_hash,
        body_file: stub.body_file,
        content_type: stub.content_type,
        status_code: stub.status_code,
    })
}

fn decode_stub(payload: &str) -> Result<CaptureStub, Error> {
    serde_json::from_str(payload)
        .map_err(|error| Error::storage(format!("decode capture spool metadata: {error}")))
}

fn encode_record<T: Serialize>(record: &T, name: &str) -> Result<String, Error> {
    serde_json::to_string(record)
        .map_err(|error| Error::storage(format!("serialize Corpus {name}: {error}")))
}

fn validate_page(page: &PageRecord) -> Result<(), Error> {
    if page.page_id.is_empty()
        || page.source_id.is_empty()
        || page.normalized_final_url.is_empty()
        || page.content_identity.is_empty()
        || page.page_id != page_id(&page.normalized_final_url)
        || page.content_sha256 != sha256_hex(page.content_identity.as_bytes())
    {
        return Err(Error::invalid("Corpus Page identity or content mismatch"));
    }
    Ok(())
}

fn validate_passage(page: &PageRecord, passage: &PassageRecord) -> Result<(), Error> {
    if passage.passage_id
        != passage_id(&passage.page_id, &page.content_identity, passage.ordinal)
        || passage.page_id != page.page_id
        || passage.source_id != page.source_id
        || passage.normalized_final_url != page.normalized_final_url
        || passage.content.is_empty()
        || passage.content_sha256 != sha256_hex(passage.content.as_bytes())
    {
        return Err(Error::invalid("Corpus Passage identity or reference mismatch"));
    }
    Ok(())
}

fn count(connection: &Connection, query: &str) -> Result<usize, Error> {
    let count: i64 = connection
        .query_row(query, [], |row| row.get(0))
        .map_err(|error| Error::storage(format!("count bounded materialization rows: {error}")))?;
    usize::try_from(count)
        .map_err(|_| Error::invalid("bounded materialization count is out of range"))
}

fn new_jsonl_writer(path: &Path) -> Result<DigestWriter<BufWriter<File>>, Error> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| Error::storage(format!("create {}: {error}", path.display())))?;
    Ok(DigestWriter::new(BufWriter::new(file)))
}

fn write_jsonl_payload<W: Write>(
    writer: &mut DigestWriter<W>,
    payload: &str,
    file_name: &str,
) -> Result<(), Error> {
    writer
        .write_all(payload.as_bytes())
        .and_then(|_| writer.write_all(b"\n"))
        .map_err(|error| Error::storage(format!("write {file_name}: {error}")))
}

pub(crate) fn write_incomplete_spooled(
    crawl_dir: &Path,
    run_id: &str,
    config: &Config,
    started: &str,
    spool: &CaptureSpool,
    rejected: usize,
    failed: usize,
    error: &Error,
) {
    let Ok(connection) = Connection::open(spool.path()) else {
        return;
    };
    let count = spool.capture_count().unwrap_or_default();
    let manifest = IncompleteSpoolManifest {
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
        pages: IncompleteManifestPageStream { connection: &connection },
        summary: Summary {
            captured_page_count: count,
            page_count: count,
            alias_count: 0,
            passage_count: 0,
            rejected_count: rejected,
            failed_count: failed,
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

#[derive(Serialize)]
struct IncompleteSpoolManifest<'a> {
    schema_version: &'static str,
    run_id: String,
    status: &'static str,
    started_at_utc: String,
    completed_at_utc: String,
    source_config_sha256: String,
    manifest_sha256: String,
    snapshot_id: Option<String>,
    capture_selection_policy: &'static str,
    user_agent: String,
    robots_user_agent: &'static str,
    policy: super::super::CrawlPolicy,
    sources: Vec<super::super::Source>,
    pages: IncompleteManifestPageStream<'a>,
    summary: Summary,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ManifestError>,
}

struct IncompleteManifestPageStream<'a> {
    connection: &'a Connection,
}

impl Serialize for IncompleteManifestPageStream<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(None)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT payload_json, selection_key FROM captures ORDER BY row_id",
            )
            .map_err(serde::ser::Error::custom)?;
        let mut rows = statement.query([]).map_err(serde::ser::Error::custom)?;
        while let Some(row) = rows.next().map_err(serde::ser::Error::custom)? {
            let payload: String = row.get(0).map_err(serde::ser::Error::custom)?;
            let selection_key: String = row.get(1).map_err(serde::ser::Error::custom)?;
            let capture = serde_json::from_str::<CaptureStub>(&payload)
                .map_err(serde::ser::Error::custom)?;
            let capture_id = capture_id_parts(&capture.source_id, &capture.final_url, &selection_key);
            sequence.serialize_element(&ManifestPage {
                requested_url: capture.requested_url,
                final_url: capture.final_url.clone(),
                redirect_chain: capture.redirects,
                source_id: capture.source_id,
                page_id: page_id(&capture.final_url),
                admission_status: "admitted_but_incomplete".into(),
                capture_id: capture_id.clone(),
                selection_key,
                selected_capture_id: capture_id,
                selection_reason: "incomplete-crawl".into(),
                alias_of: None,
                title: capture.title,
                description: capture.description,
                language: capture.language,
                canonical_url: capture.canonical_url,
                extraction_method: capture.extraction_method,
                content_type: capture.content_type,
                content_bytes: capture.content_bytes,
                body_file: capture.body_file,
                body_sha256: capture.body_hash,
                content_sha256: capture.content_hash,
                status_code: capture.status_code,
            })?;
        }
        sequence.end()
    }
}
