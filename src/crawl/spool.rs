use crate::crawl::{Capture, Source};
use crate::error::Error;
use rusqlite::Connection as SqlConnection;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct CaptureSpool {
    path: PathBuf,
    connection: Arc<Mutex<SqlConnection>>,
    max_frontier: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CaptureStub {
    pub(crate) requested_url: String,
    pub(crate) final_url: String,
    pub(crate) redirects: Vec<String>,
    pub(crate) source_id: String,
    pub(crate) title: String,
    pub(crate) description: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) canonical_url: Option<String>,
    pub(crate) extraction_method: String,
    pub(crate) content_hash: String,
    pub(crate) content_bytes: usize,
    pub(crate) body_hash: String,
    pub(crate) body_file: String,
    pub(crate) content_type: String,
    pub(crate) status_code: u16,
}

impl From<&Capture> for CaptureStub {
    fn from(capture: &Capture) -> Self {
        Self {
            requested_url: capture.requested_url.clone(),
            final_url: capture.final_url.clone(),
            redirects: capture.redirects.clone(),
            source_id: capture.source_id.clone(),
            title: capture.title.clone(),
            description: capture.description.clone(),
            language: capture.language.clone(),
            canonical_url: capture.canonical_url.clone(),
            extraction_method: capture.extraction_method.into(),
            content_hash: capture.content_hash.clone(),
            content_bytes: capture.content_bytes,
            body_hash: capture.body_hash.clone(),
            body_file: capture.body_file.clone(),
            content_type: capture.content_type.clone(),
            status_code: capture.status_code,
        }
    }
}

impl CaptureSpool {
    pub(crate) fn open(crawl_dir: &Path, max_frontier: usize) -> Result<Self, Error> {
        fs::create_dir_all(crawl_dir)
            .map_err(|error| Error::storage(format!("create capture spool directory: {error}")))?;
        let path = crawl_dir.join("capture-spool.sqlite3");
        let connection = SqlConnection::open(&path)
            .map_err(|error| Error::storage(format!("open capture spool: {error}")))?;
        connection
            .busy_timeout(std::time::Duration::from_secs(30))
            .map_err(|error| Error::storage(format!("configure capture spool: {error}")))?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .and_then(|_| connection.pragma_update(None, "synchronous", "NORMAL"))
            .and_then(|_| connection.pragma_update(None, "temp_store", "FILE"))
            .and_then(|_| connection.pragma_update(None, "cache_size", -8192_i64))
            .and_then(|_| connection.pragma_update(None, "mmap_size", 0_i64))
            .and_then(|_| connection.pragma_update(None, "wal_autocheckpoint", 1000_i64))
            .map_err(|error| Error::storage(format!("configure capture spool: {error}")))?;
        connection
            .execute_batch(
                "PRAGMA foreign_keys = ON;
                 CREATE TABLE IF NOT EXISTS captures (
                     row_id INTEGER PRIMARY KEY,
                     source_id TEXT NOT NULL,
                     final_url TEXT NOT NULL,
                     content_hash TEXT NOT NULL,
                     selection_key TEXT NOT NULL,
                     payload_json TEXT NOT NULL
                 );
                 CREATE INDEX IF NOT EXISTS captures_url_selection
                     ON captures(source_id, final_url, selection_key, row_id);
                 CREATE INDEX IF NOT EXISTS captures_identity
                     ON captures(source_id, content_hash, final_url, selection_key, row_id);
                 CREATE TABLE IF NOT EXISTS unique_content_groups (
                     source_id TEXT NOT NULL,
                     content_hash TEXT NOT NULL,
                     identity TEXT NOT NULL,
                     PRIMARY KEY(source_id, content_hash)
                 ) WITHOUT ROWID;
                 CREATE TABLE IF NOT EXISTS visited_urls (
                     url TEXT PRIMARY KEY
                 ) WITHOUT ROWID;
                 CREATE TABLE IF NOT EXISTS sitemap_pages (
                     origin TEXT NOT NULL,
                     url TEXT NOT NULL,
                     PRIMARY KEY(origin, url)
                 ) WITHOUT ROWID;
                 CREATE TABLE IF NOT EXISTS sitemap_origins (
                     origin TEXT PRIMARY KEY,
                     page_count INTEGER NOT NULL
                 ) WITHOUT ROWID;",
            )
            .map_err(|error| Error::storage(format!("initialize capture spool: {error}")))?;
        Ok(Self {
            path,
            connection: Arc::new(Mutex::new(connection)),
            max_frontier,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn insert_capture(
        &self,
        capture: &Capture,
        selection_key: &str,
    ) -> Result<bool, Error> {
        let mut connection = self.connection.lock().expect("capture spool lock");
        let transaction = connection
            .transaction()
            .map_err(|error| Error::storage(format!("begin capture spool insert: {error}")))?;
        let inserted_group = transaction
            .execute(
                "INSERT OR IGNORE INTO unique_content_groups(source_id, content_hash, identity)
                 VALUES (?1, ?2, ?3)",
                params![capture.source_id, capture.content_hash, capture.identity],
            )
            .map_err(|error| Error::storage(format!("record unique content group: {error}")))?
            > 0;
        if !inserted_group {
            let existing_identity: String = transaction
                .query_row(
                    "SELECT identity FROM unique_content_groups
                     WHERE source_id = ?1 AND content_hash = ?2",
                    params![capture.source_id, capture.content_hash],
                    |row| row.get(0),
                )
                .map_err(|error| Error::storage(format!("read content identity: {error}")))?;
            if existing_identity != capture.identity {
                return Err(Error::invalid(
                    "extracted content SHA-256 collision within a source",
                ));
            }
        }
        let payload = serde_json::to_string(&CaptureStub::from(capture))
            .map_err(|error| Error::storage(format!("serialize capture metadata: {error}")))?;
        transaction
            .execute(
                "INSERT INTO captures(source_id, final_url, content_hash, selection_key, payload_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    capture.source_id,
                    capture.final_url,
                    capture.content_hash,
                    selection_key,
                    payload
                ],
            )
            .map_err(|error| Error::storage(format!("write capture metadata: {error}")))?;
        transaction
            .commit()
            .map_err(|error| Error::storage(format!("commit capture spool insert: {error}")))?;
        Ok(inserted_group)
    }

    pub(crate) fn mark_visited(&self, url: &str) -> Result<bool, Error> {
        let connection = self.connection.lock().expect("capture spool lock");
        connection
            .execute("INSERT OR IGNORE INTO visited_urls(url) VALUES (?1)", [url])
            .map(|count| count > 0)
            .map_err(|error| Error::storage(format!("record visited URL: {error}")))
    }

    pub(crate) fn is_visited(&self, url: &str) -> Result<bool, Error> {
        let connection = self.connection.lock().expect("capture spool lock");
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM visited_urls WHERE url = ?1)",
                [url],
                |row| row.get(0),
            )
            .map_err(|error| Error::storage(format!("check visited URL: {error}")))
    }

    pub(crate) fn capture_count(&self) -> Result<usize, Error> {
        let connection = self.connection.lock().expect("capture spool lock");
        connection
            .query_row("SELECT COUNT(*) FROM captures", [], |row| row.get(0))
            .map_err(|error| Error::storage(format!("count spooled captures: {error}")))
    }

    pub(crate) fn unique_groups_by_source(&self) -> Result<HashMap<String, usize>, Error> {
        let connection = self.connection.lock().expect("capture spool lock");
        let mut statement = connection
            .prepare("SELECT source_id, COUNT(*) FROM unique_content_groups GROUP BY source_id")
            .map_err(|error| Error::storage(format!("prepare source group counts: {error}")))?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|error| Error::storage(format!("query source group counts: {error}")))?;
        let mut counts = HashMap::new();
        for row in rows {
            let (source_id, count) =
                row.map_err(|error| Error::storage(format!("read source group count: {error}")))?;
            counts.insert(source_id, count);
        }
        Ok(counts)
    }

    pub(crate) fn validate_source_scope(&self, sources: &[Source]) -> Result<(), Error> {
        let connection = self.connection.lock().expect("capture spool lock");
        connection
            .pragma_update(None, "temp_store", "FILE")
            .map_err(|error| {
                Error::storage(format!("configure capture scope validation: {error}"))
            })?;
        let mut statement = connection
            .prepare(
                "SELECT c.source_id, c.final_url, c.content_hash, c.payload_json,
                        u.content_hash
                 FROM captures AS c
                 LEFT JOIN unique_content_groups AS u
                   ON u.source_id = c.source_id AND u.content_hash = c.content_hash",
            )
            .map_err(|error| Error::storage(format!("prepare capture scope check: {error}")))?;
        let mut rows = statement
            .query([])
            .map_err(|error| Error::storage(format!("query capture scope: {error}")))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| Error::storage(format!("read capture scope: {error}")))?
        {
            let source_id: String = row
                .get(0)
                .map_err(|error| Error::storage(format!("read capture Source: {error}")))?;
            let final_url: String = row
                .get(1)
                .map_err(|error| Error::storage(format!("read capture URL: {error}")))?;
            let content_hash: String = row
                .get(2)
                .map_err(|error| Error::storage(format!("read capture content hash: {error}")))?;
            let payload_json: String = row
                .get(3)
                .map_err(|error| Error::storage(format!("read capture metadata: {error}")))?;
            let group_hash: Option<String> = row
                .get(4)
                .map_err(|error| Error::storage(format!("read content group: {error}")))?;
            if group_hash.as_deref() != Some(content_hash.as_str()) {
                return Err(Error::invalid(
                    "capture spool content groups do not match captured rows",
                ));
            }
            let source = sources
                .iter()
                .find(|source| source.source_id == source_id)
                .ok_or_else(|| {
                    Error::invalid(format!(
                        "capture spool contains unknown Source {source_id:?}"
                    ))
                })?;
            if !source.owns(&final_url) {
                return Err(Error::invalid(format!(
                    "capture URL is outside Source {source_id:?} scope: {final_url}"
                )));
            }
            let stub: CaptureStub = serde_json::from_str(&payload_json)
                .map_err(|error| Error::invalid(format!("invalid capture metadata: {error}")))?;
            if stub.source_id != source_id
                || stub.final_url != final_url
                || stub.content_hash != content_hash
            {
                return Err(Error::invalid(
                    "capture spool indexed fields do not match its metadata",
                ));
            }
            if !std::iter::once(&stub.requested_url)
                .chain(stub.redirects.iter())
                .all(|url| sources.iter().any(|source| source.owns(url)))
            {
                return Err(Error::invalid(
                    "capture request or redirect is outside configured Source scopes",
                ));
            }
        }
        drop(rows);
        drop(statement);

        let capture_group_count: usize = connection
            .query_row(
                "SELECT COUNT(*) FROM (
                     SELECT source_id, content_hash
                     FROM captures
                     GROUP BY source_id, content_hash
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(|error| Error::storage(format!("count captured content groups: {error}")))?;
        let group_count: usize = connection
            .query_row("SELECT COUNT(*) FROM unique_content_groups", [], |row| {
                row.get(0)
            })
            .map_err(|error| Error::storage(format!("count unique content groups: {error}")))?;
        if group_count != capture_group_count {
            return Err(Error::invalid(
                "capture spool unique content groups do not match captured rows",
            ));
        }
        let mut statement = connection
            .prepare("SELECT DISTINCT source_id FROM unique_content_groups")
            .map_err(|error| {
                Error::storage(format!("prepare content group scope check: {error}"))
            })?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| Error::storage(format!("query content group scope: {error}")))?;
        for row in rows {
            let source_id =
                row.map_err(|error| Error::storage(format!("read content group Source: {error}")))?;
            if !sources.iter().any(|source| source.source_id == source_id) {
                return Err(Error::invalid(format!(
                    "capture spool contains unknown Source {source_id:?}"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn insert_sitemap_pages(
        &self,
        origin: &str,
        urls: &[String],
    ) -> Result<usize, Error> {
        let mut connection = self.connection.lock().expect("capture spool lock");
        let transaction = connection
            .transaction()
            .map_err(|error| Error::storage(format!("begin sitemap Page insert: {error}")))?;
        let current_count: usize = transaction
            .query_row(
                "SELECT page_count FROM sitemap_origins WHERE origin = ?1",
                [origin],
                |row| row.get(0),
            )
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(0),
                other => Err(other),
            })
            .map_err(|error| Error::storage(format!("count sitemap Pages: {error}")))?;
        let mut count = current_count;
        {
            let mut statement = transaction
                .prepare_cached("INSERT OR IGNORE INTO sitemap_pages(origin, url) VALUES (?1, ?2)")
                .map_err(|error| Error::storage(format!("prepare sitemap Page insert: {error}")))?;
            for url in urls {
                if statement
                    .execute(params![origin, url])
                    .map_err(|error| Error::storage(format!("record sitemap Page: {error}")))?
                    > 0
                {
                    count += 1;
                    if count > self.max_frontier {
                        return Err(Error::safety(format!(
                            "sitemap Page candidate cap exceeded {}",
                            self.max_frontier
                        )));
                    }
                }
            }
        }
        if count != current_count {
            transaction
                .execute(
                    "INSERT INTO sitemap_origins(origin, page_count) VALUES (?1, ?2)
                     ON CONFLICT(origin) DO UPDATE SET page_count = excluded.page_count",
                    params![origin, count as i64],
                )
                .map_err(|error| Error::storage(format!("save sitemap Page count: {error}")))?;
        }
        transaction
            .commit()
            .map_err(|error| Error::storage(format!("commit sitemap Pages: {error}")))?;
        Ok(count)
    }

    pub(crate) fn sitemap_page_count(&self, origin: &str) -> Result<usize, Error> {
        let connection = self.connection.lock().expect("capture spool lock");
        connection
            .query_row(
                "SELECT page_count FROM sitemap_origins WHERE origin = ?1",
                [origin],
                |row| row.get(0),
            )
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(0),
                other => Err(other),
            })
            .map_err(|error| Error::storage(format!("count sitemap Pages: {error}")))
    }

    pub(crate) fn sitemap_pages(&self, origin: &str) -> Result<Vec<String>, Error> {
        let connection = self.connection.lock().expect("capture spool lock");
        let mut statement = connection
            .prepare("SELECT url FROM sitemap_pages WHERE origin = ?1 ORDER BY url")
            .map_err(|error| Error::storage(format!("query sitemap Pages: {error}")))?;
        let rows = statement
            .query_map([origin], |row| row.get(0))
            .map_err(|error| Error::storage(format!("read sitemap Pages: {error}")))?;
        rows.collect::<Result<Vec<String>, _>>()
            .map_err(|error| Error::storage(format!("read sitemap Page: {error}")))
    }
}
