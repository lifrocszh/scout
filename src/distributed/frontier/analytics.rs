use super::{FrontierStore, validate_digest, validate_request_identity};
use crate::distributed::event_batch::{EventCoverage, PublishedBatch};
use crate::error::Error;
use crate::storage::sha256_hex;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const RECEIPT_SCHEMA: &str = "scout.databricks-crawl-event-ingestion-receipt.v1";

#[derive(Debug, Deserialize)]
pub(crate) struct IngestionReceipt {
    pub(crate) schema_version: String,
    pub(crate) batch_id: String,
    pub(crate) consumer_id: String,
    pub(crate) manifest_sha256: String,
    pub(crate) records_sha256: String,
    pub(crate) coverage_sha256: String,
    pub(crate) input_record_count: i32,
    pub(crate) ingested_record_count: i32,
    pub(crate) quarantine_count: i32,
    pub(crate) conflict_count: i32,
    pub(crate) status: String,
    pub(crate) completed_at_utc: String,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct ReplayPreflight {
    pub(crate) run_id: String,
    pub(crate) consumer_id: String,
    pub(crate) history_event_count: usize,
    pub(crate) replayable_gaps: Vec<ReplayableGap>,
    pub(crate) unrecoverable_gaps: Vec<ReplayGap>,
    pub(crate) status: &'static str,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct ReplayableGap {
    pub(crate) event_id: String,
    pub(crate) batch_ids: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct ReplayGap {
    pub(crate) event_id: String,
    pub(crate) reason: String,
}

#[derive(Debug)]
pub(super) struct BatchCopy {
    pub(super) batch_id: String,
    pub(super) manifest_sha256: String,
    pub(super) records_sha256: String,
    pub(super) coverage_sha256: String,
    pub(super) record_count: i32,
    pub(super) quarantine_count: i32,
    pub(super) records_bytes: i64,
}

impl FrontierStore {
    pub(crate) fn validate_export_history(&mut self, run_id: &str) -> Result<(), Error> {
        if let Some(gap) = self.run_history_integrity_gap(run_id)? {
            return Err(Error::safety(gap));
        }
        let mismatch = self.client.query_opt(
            "SELECT h.event_id
             FROM scout_crawl_event_history h
             LEFT JOIN scout_crawl_events e ON e.event_id = h.event_id
             WHERE h.crawl_run_id = $1 AND (
                 e.event_id IS NULL OR e.crawl_run_id IS DISTINCT FROM h.crawl_run_id
                 OR e.event_type IS DISTINCT FROM h.event_type
                 OR (h.payload_sha256 IS NOT NULL AND (
                     e.payload_sha256 IS DISTINCT FROM h.payload_sha256
                     OR encode(sha256(convert_to(e.payload_json, 'UTF8')), 'hex') <> h.payload_sha256
                 ))
             )
             ORDER BY h.event_id LIMIT 1",
            &[&run_id],
        ).map_err(|error| Error::storage(format!("compare live Crawl events with retained history: {error}")))?;
        if let Some(row) = mismatch {
            return Err(Error::safety(format!(
                "retained-history identity or payload checksum differs for event {:?}; export withheld",
                row.get::<_, String>(0)
            )));
        }
        Ok(())
    }

    fn run_history_integrity_gap(&mut self, run_id: &str) -> Result<Option<String>, Error> {
        let row = self.client.query_opt(
            "SELECT run.expected_event_count, run.expected_event_coverage::TEXT,
                    (SELECT COUNT(*) FROM scout_crawl_event_history h WHERE h.crawl_run_id=run.crawl_run_id),
                    COALESCE((
                        SELECT bit_xor(scout_event_history_fingerprint(
                            h.event_id,h.event_type,h.schema_version,h.dedupe_key,h.payload_sha256,h.created_at
                        ))::TEXT
                        FROM scout_crawl_event_history h WHERE h.crawl_run_id=run.crawl_run_id
                    ), repeat('0',256))
             FROM scout_crawl_run_history run WHERE run.crawl_run_id=$1",
            &[&run_id],
        ).map_err(|error| Error::storage(format!("verify retained Crawl event history coverage: {error}")))?;
        let Some(row) = row else {
            return Ok(Some(format!("unknown Crawl run {run_id:?}")));
        };
        let expected_count: i64 = row.get(0);
        let expected_coverage: String = row.get(1);
        let retained_count: i64 = row.get(2);
        let retained_coverage: String = row.get(3);
        if expected_count != retained_count || expected_coverage != retained_coverage {
            return Ok(Some(format!(
                "retained Crawl event history ledger is incomplete or changed for {run_id}: expected {expected_count} event identities, found {retained_count}; replay preflight required"
            )));
        }
        Ok(None)
    }

    pub(crate) fn ensure_analytics_consumer(&mut self, consumer_id: &str) -> Result<(), Error> {
        validate_request_identity(consumer_id, "consumer_id")?;
        self.client
            .execute(
                "INSERT INTO scout_analytics_consumers(consumer_id, is_required)
             VALUES ($1, FALSE) ON CONFLICT (consumer_id) DO NOTHING",
                &[&consumer_id],
            )
            .map_err(|error| {
                Error::storage(format!("register optional analytics consumer: {error}"))
            })?;
        let retired: bool = self.client.query_one(
            "SELECT retired_at IS NOT NULL FROM scout_analytics_consumers WHERE consumer_id = $1",
            &[&consumer_id],
        ).map_err(|error| Error::storage(format!("check analytics consumer status: {error}")))?.get(0);
        if retired {
            return Err(Error::invalid(format!(
                "analytics consumer {consumer_id:?} is retired; explicitly register it again to resume"
            )));
        }
        Ok(())
    }

    pub(crate) fn register_analytics_consumer(
        &mut self,
        consumer_id: &str,
        required: bool,
        replay_days: u32,
    ) -> Result<(), Error> {
        validate_request_identity(consumer_id, "consumer_id")?;
        if !(1..=3650).contains(&replay_days) {
            return Err(Error::invalid(
                "replay window must be between 1 and 3650 days",
            ));
        }
        self.client.execute(
            "INSERT INTO scout_analytics_consumers(consumer_id, is_required, replay_window, retired_at, ever_required)
             VALUES ($1, $2, make_interval(days => $3), NULL, $2)
             ON CONFLICT (consumer_id) DO UPDATE SET is_required = EXCLUDED.is_required,
                 ever_required = scout_analytics_consumers.ever_required OR EXCLUDED.ever_required,
                 replay_window = EXCLUDED.replay_window, retired_at = NULL",
            &[&consumer_id, &required, &(replay_days as i32)],
        ).map_err(|error| Error::storage(format!("register analytics consumer: {error}")))?;
        Ok(())
    }

    pub(crate) fn retire_analytics_consumer(&mut self, consumer_id: &str) -> Result<(), Error> {
        validate_request_identity(consumer_id, "consumer_id")?;
        let changed = self.client.execute(
            "UPDATE scout_analytics_consumers SET retired_at = COALESCE(retired_at, clock_timestamp())
             WHERE consumer_id = $1",
            &[&consumer_id],
        ).map_err(|error| Error::storage(format!("retire analytics consumer: {error}")))?;
        if changed == 0 {
            return Err(Error::invalid(format!(
                "analytics consumer {consumer_id:?} is not registered"
            )));
        }
        Ok(())
    }

    pub(crate) fn acknowledge_event_batch(
        &mut self,
        receipt: IngestionReceipt,
    ) -> Result<(), Error> {
        let completed_at_is_utc = chrono::DateTime::parse_from_rfc3339(&receipt.completed_at_utc)
            .is_ok_and(|timestamp| timestamp.offset().local_minus_utc() == 0);
        if receipt.schema_version != RECEIPT_SCHEMA
            || receipt.status != "complete"
            || !completed_at_is_utc
            || receipt.quarantine_count != 0
            || receipt.conflict_count != 0
            || receipt.input_record_count <= 0
            || receipt.input_record_count != receipt.ingested_record_count
        {
            return Err(Error::safety(
                "Databricks receipt is incomplete, quarantined, conflicted, or has an incorrect record count",
            ));
        }
        validate_digest(&receipt.manifest_sha256, "receipt manifest SHA-256")?;
        validate_digest(&receipt.records_sha256, "receipt records SHA-256")?;
        validate_digest(&receipt.coverage_sha256, "receipt event coverage SHA-256")?;
        let row = self
            .client
            .query_opt(
                "SELECT manifest_sha256, records_sha256, coverage_sha256, record_count, consumer_id,
                    COALESCE(quarantine_count, -1)
             FROM scout_crawl_event_batches WHERE batch_id = $1",
                &[&receipt.batch_id],
            )
            .map_err(|error| {
                Error::storage(format!(
                    "read exported batch before ingestion receipt: {error}"
                ))
            })?
            .ok_or_else(|| {
                Error::invalid(format!("unknown exported batch {:?}", receipt.batch_id))
            })?;
        let expected = (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(2),
            row.get::<_, i32>(3),
            row.get::<_, String>(4),
            row.get::<_, i32>(5),
        );
        if expected
            != (
                receipt.manifest_sha256.clone(),
                receipt.records_sha256.clone(),
                receipt.coverage_sha256.clone(),
                receipt.input_record_count,
                receipt.consumer_id.clone(),
                0,
            )
        {
            return Err(Error::safety(
                "Databricks receipt does not match the registered immutable batch identity and event coverage, or the batch contains quarantined records",
            ));
        }
        let active: bool = self
            .client
            .query_one(
                "SELECT retired_at IS NULL FROM scout_analytics_consumers WHERE consumer_id = $1",
                &[&receipt.consumer_id],
            )
            .map_err(|error| {
                Error::storage(format!("check analytics consumer before receipt: {error}"))
            })?
            .get(0);
        if !active {
            return Err(Error::invalid(
                "cannot record a downstream receipt for a retired analytics consumer",
            ));
        }
        self.client
            .execute(
                "INSERT INTO scout_crawl_event_ingestion_receipts(
                 consumer_id, batch_id, manifest_sha256, records_sha256, coverage_sha256,
                 ingested_record_count, downstream_status, downstream_completed_at
             ) VALUES ($1, $2, $3, $4, $5, $6, 'complete', $7)
             ON CONFLICT (consumer_id, batch_id) DO NOTHING",
                &[
                    &receipt.consumer_id,
                    &receipt.batch_id,
                    &receipt.manifest_sha256,
                    &receipt.records_sha256,
                    &receipt.coverage_sha256,
                    &receipt.ingested_record_count,
                    &receipt.completed_at_utc,
                ],
            )
            .map_err(|error| {
                Error::storage(format!("record downstream ingestion receipt: {error}"))
            })?;
        let saved = self
            .client
            .query_one(
                "SELECT manifest_sha256, records_sha256, coverage_sha256, ingested_record_count,
                    downstream_status, downstream_completed_at
             FROM scout_crawl_event_ingestion_receipts WHERE consumer_id = $1 AND batch_id = $2",
                &[&receipt.consumer_id, &receipt.batch_id],
            )
            .map_err(|error| {
                Error::storage(format!("verify downstream ingestion receipt: {error}"))
            })?;
        if saved.get::<_, String>(0) != receipt.manifest_sha256
            || saved.get::<_, String>(1) != receipt.records_sha256
            || saved.get::<_, String>(2) != receipt.coverage_sha256
            || saved.get::<_, i32>(3) != receipt.ingested_record_count
            || saved.get::<_, String>(4) != "complete"
        {
            return Err(Error::safety(
                "conflicting ingestion receipt already exists for this batch",
            ));
        }
        Ok(())
    }

    pub(crate) fn register_exported_event_batch(
        &mut self,
        consumer_id: &str,
        batch: &PublishedBatch,
    ) -> Result<(), Error> {
        self.ensure_analytics_consumer(consumer_id)?;
        let batch_path = batch
            .directory
            .canonicalize()
            .map_err(|error| Error::storage(format!("resolve published batch path: {error}")))?;
        let local_path = batch_path.to_string_lossy().into_owned();
        let count = i32::try_from(batch.record_count)
            .map_err(|_| Error::invalid("batch record count is too large"))?;
        let bytes = i64::try_from(batch.records_bytes)
            .map_err(|_| Error::invalid("batch byte count is too large"))?;
        let quarantine_count = i32::try_from(batch.quarantine_count)
            .map_err(|_| Error::invalid("batch quarantine count is too large"))?;
        if batch.event_coverage.len() != batch.record_count {
            return Err(Error::safety(
                "exported batch coverage count does not match its records",
            ));
        }
        let coverage = batch
            .event_coverage
            .iter()
            .map(|event| event.event_id.as_str())
            .collect::<Vec<_>>()
            .join("\0");
        if sha256_hex(coverage.as_bytes()) != batch.coverage_sha256 {
            return Err(Error::safety(
                "exported batch coverage checksum does not match its event IDs",
            ));
        }
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin immutable event batch registration: {error}"))
        })?;
        tx.execute(
            "INSERT INTO scout_crawl_event_batches(
                 batch_id, consumer_id, crawl_run_id, manifest_sha256, records_sha256,
                 coverage_sha256, record_count, records_bytes, quarantine_count
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (batch_id) DO NOTHING",
            &[
                &batch.batch_id,
                &consumer_id,
                &batch.crawl_run_id,
                &batch.manifest_sha256,
                &batch.records_sha256,
                &batch.coverage_sha256,
                &count,
                &bytes,
                &quarantine_count,
            ],
        )
        .map_err(|error| Error::storage(format!("register immutable exported batch: {error}")))?;
        let existing = tx.query_one(
            "SELECT consumer_id, manifest_sha256, records_sha256, coverage_sha256, record_count, records_bytes, quarantine_count
             FROM scout_crawl_event_batches WHERE batch_id = $1 FOR UPDATE",
            &[&batch.batch_id],
        ).map_err(|error| Error::storage(format!("verify immutable exported batch registration: {error}")))?;
        if existing.get::<_, Option<i32>>(6).is_none() {
            let prior = BatchCopy {
                batch_id: batch.batch_id.clone(),
                manifest_sha256: batch.manifest_sha256.clone(),
                records_sha256: batch.records_sha256.clone(),
                coverage_sha256: batch.coverage_sha256.clone(),
                record_count: count,
                quarantine_count,
                records_bytes: bytes,
            };
            verify_batch_files(&batch.directory, &prior)?;
            tx.execute(
                "UPDATE scout_crawl_event_batches SET quarantine_count=$2
                 WHERE batch_id=$1 AND quarantine_count IS NULL",
                &[&batch.batch_id, &quarantine_count],
            )
            .map_err(|error| {
                Error::storage(format!("recover immutable batch quarantine count: {error}"))
            })?;
        }
        if existing.get::<_, String>(0) != consumer_id
            || existing.get::<_, String>(1) != batch.manifest_sha256
            || existing.get::<_, String>(2) != batch.records_sha256
            || existing.get::<_, String>(3) != batch.coverage_sha256
            || existing.get::<_, i32>(4) != count
            || existing.get::<_, i64>(5) != bytes
            || existing
                .get::<_, Option<i32>>(6)
                .is_some_and(|saved| saved != quarantine_count)
        {
            return Err(Error::safety(
                "batch ID was registered with conflicting checksums or event coverage",
            ));
        }
        tx.execute(
            "INSERT INTO scout_crawl_event_batch_locations(batch_id, local_path)
             VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[&batch.batch_id, &local_path],
        )
        .map_err(|error| Error::storage(format!("register local batch copy: {error}")))?;
        for EventCoverage {
            event_id,
            event_type,
            payload_sha256,
        } in &batch.event_coverage
        {
            let payload_sha256 = payload_sha256.as_deref();
            tx.execute(
                "INSERT INTO scout_crawl_event_batch_events(batch_id, event_id, event_type, payload_sha256)
                 VALUES ($1,$2,$3,$4) ON CONFLICT (batch_id, event_id) DO NOTHING",
                &[&batch.batch_id, event_id, event_type, &payload_sha256],
            ).map_err(|error| Error::storage(format!("register exported event coverage: {error}")))?;
            let saved = tx
                .query_one(
                    "SELECT event_type, payload_sha256 FROM scout_crawl_event_batch_events
                 WHERE batch_id=$1 AND event_id=$2",
                    &[&batch.batch_id, event_id],
                )
                .map_err(|error| {
                    Error::storage(format!("verify exported event coverage: {error}"))
                })?;
            if saved.get::<_, String>(0) != *event_type
                || saved.get::<_, Option<String>>(1).as_deref() != payload_sha256
            {
                return Err(Error::safety(
                    "batch event coverage conflicts with an existing immutable registration",
                ));
            }
        }
        tx.commit().map_err(|error| {
            Error::storage(format!(
                "commit immutable event batch registration: {error}"
            ))
        })
    }

    pub(crate) fn archive_event_batch(
        &mut self,
        batch_id: &str,
        data_dir: &Path,
    ) -> Result<(), Error> {
        validate_digest(
            batch_id.strip_prefix("batch-").unwrap_or_default(),
            "batch ID suffix",
        )?;
        fs::create_dir_all(data_dir).map_err(|error| {
            Error::storage(format!(
                "create data directory for analytics archive: {error}"
            ))
        })?;
        let data_root = data_dir.canonicalize().map_err(|error| {
            Error::storage(format!(
                "resolve data directory for analytics archive: {error}"
            ))
        })?;
        let archive_root = data_root.join("analytics-archive");
        create_managed_dir(&data_root, &archive_root)?;
        let archive_root = archive_root
            .canonicalize()
            .map_err(|error| Error::storage(format!("resolve analytics archive root: {error}")))?;
        if !archive_root.starts_with(&data_root) {
            return Err(Error::safety(
                "analytics archive must remain under --data-dir",
            ));
        }
        let row = self.client.query_opt(
            "SELECT manifest_sha256, records_sha256, coverage_sha256, record_count, records_bytes,
                    COALESCE(quarantine_count, -1)
             FROM scout_crawl_event_batches WHERE batch_id=$1",
            &[&batch_id],
        ).map_err(|error| Error::storage(format!("read batch before archive: {error}")))?
            .ok_or_else(|| Error::invalid(format!("unknown exported batch {batch_id:?}")))?;
        let mut batch = BatchCopy {
            batch_id: batch_id.to_owned(),
            manifest_sha256: row.get(0),
            records_sha256: row.get(1),
            coverage_sha256: row.get(2),
            record_count: row.get(3),
            records_bytes: row.get(4),
            quarantine_count: row.get(5),
        };
        let sources = self.client.query(
            "SELECT local_path FROM scout_crawl_event_batch_locations WHERE batch_id=$1 ORDER BY registered_at",
            &[&batch_id],
        ).map_err(|error| Error::storage(format!("find local event batch copies: {error}")))?;
        let source = sources.iter().map(|row| PathBuf::from(row.get::<_, String>(0)))
            .find(|path| managed_path_is_safe(&data_root, path).is_ok() && verify_batch_files(path, &batch).is_ok())
            .ok_or_else(|| Error::safety("no registered local event batch copy passes checksum and event coverage verification"))?;
        if batch.quarantine_count < 0 {
            batch.quarantine_count = batch_quarantine_count(&source, &batch)?;
            self.client
                .execute(
                    "UPDATE scout_crawl_event_batches SET quarantine_count=$2
                 WHERE batch_id=$1 AND quarantine_count IS NULL",
                    &[&batch_id, &batch.quarantine_count],
                )
                .map_err(|error| {
                    Error::storage(format!("recover immutable batch quarantine count: {error}"))
                })?;
            let saved: Option<i32> = self
                .client
                .query_one(
                    "SELECT quarantine_count FROM scout_crawl_event_batches WHERE batch_id=$1",
                    &[&batch_id],
                )
                .map_err(|error| {
                    Error::storage(format!("verify recovered batch quarantine count: {error}"))
                })?
                .get(0);
            if saved != Some(batch.quarantine_count) {
                return Err(Error::safety(
                    "registered batch quarantine count conflicts with its verified immutable manifest",
                ));
            }
        }
        let batch_dest = archive_root.join("batches").join(batch_id);
        create_managed_dir(&archive_root, &archive_root.join("batches"))?;
        copy_batch(&archive_root, &source, &batch_dest, &batch)?;

        let artifact_rows = self.client.query(
            "SELECT DISTINCT artifact_uri, sha256, byte_size FROM scout_crawl_event_artifacts
             WHERE event_id IN (SELECT event_id FROM scout_crawl_event_batch_events WHERE batch_id=$1)
             ORDER BY artifact_uri",
            &[&batch_id],
        ).map_err(|error| Error::storage(format!("read event replay artifact dependencies: {error}")))?;
        let mut archive_records = vec![(
            "batch".to_owned(),
            batch_id.to_owned(),
            format!("batches/{batch_id}"),
            batch.manifest_sha256.clone(),
            batch.records_bytes,
        )];
        for row in artifact_rows {
            let uri: String = row.get(0);
            let digest: String = row.get(1);
            let size: i64 = row.get(2);
            let local = artifact_path(&data_root, &uri)?;
            managed_path_is_safe(&data_root, &local)?;
            let bytes = read_verified(&local, &digest, size as u64)?;
            let relative = format!("artifacts/{digest}");
            let destination = archive_root.join(&relative);
            create_managed_dir(&archive_root, &archive_root.join("artifacts"))?;
            write_verified_copy(&destination, &bytes, &digest)?;
            archive_records.push(("artifact".to_owned(), uri, relative, digest, size));
        }
        let batch_relative = format!("batches/{batch_id}");
        verify_batch_files(&archive_root.join(&batch_relative), &batch)?;
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin verified archive registration: {error}"))
        })?;
        for (kind, id, relative, digest, size) in archive_records {
            tx.execute(
                "INSERT INTO scout_crawl_event_archive_items(item_type,item_id,archive_path,sha256,byte_size,verified_at)
                 VALUES ($1,$2,$3,$4,$5,clock_timestamp()) ON CONFLICT (item_type,item_id) DO NOTHING",
                &[&kind, &id, &relative, &digest, &size],
            ).map_err(|error| Error::storage(format!("register verified replay archive: {error}")))?;
            let saved = tx.query_one(
                "SELECT archive_path,sha256,byte_size FROM scout_crawl_event_archive_items WHERE item_type=$1 AND item_id=$2",
                &[&kind, &id],
            ).map_err(|error| Error::storage(format!("verify replay archive registration: {error}")))?;
            if saved.get::<_, String>(0) != relative
                || saved.get::<_, String>(1) != digest
                || saved.get::<_, i64>(2) != size
            {
                return Err(Error::safety(
                    "archive identity conflicts with a previous verification",
                ));
            }
        }
        tx.commit().map_err(|error| {
            Error::storage(format!("commit verified archive registration: {error}"))
        })
    }

    pub(crate) fn replay_preflight(
        &mut self,
        run_id: &str,
        consumer_id: &str,
        data_dir: &Path,
    ) -> Result<ReplayPreflight, Error> {
        self.ensure_analytics_consumer(consumer_id)?;
        let known_run: bool = self
            .client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM scout_crawl_runs WHERE crawl_run_id=$1)
                 OR EXISTS (SELECT 1 FROM scout_crawl_run_history WHERE crawl_run_id=$1)
                 OR EXISTS (SELECT 1 FROM scout_crawl_event_history WHERE crawl_run_id=$1)",
                &[&run_id],
            )
            .map_err(|error| {
                Error::storage(format!("check run identity for replay preflight: {error}"))
            })?
            .get(0);
        if !known_run {
            return Ok(ReplayPreflight {
                run_id: run_id.into(),
                consumer_id: consumer_id.into(),
                history_event_count: 0,
                replayable_gaps: Vec::new(),
                unrecoverable_gaps: vec![ReplayGap {
                    event_id: String::new(),
                    reason:
                        "run has no retained history or live metadata; caught-up status is unsafe"
                            .into(),
                }],
                status: "unrecoverable",
            });
        }
        let integrity_gap = self.run_history_integrity_gap(run_id)?;
        let history = self
            .client
            .query(
                "SELECT h.event_id, h.payload_sha256, (e.event_id IS NOT NULL), e.payload_sha256,
                    CASE WHEN e.event_id IS NULL THEN NULL
                         ELSE encode(sha256(convert_to(e.payload_json,'UTF8')),'hex') END,
                    h.event_type, e.event_type
             FROM scout_crawl_event_history h
             LEFT JOIN scout_crawl_events e ON e.event_id=h.event_id
             WHERE h.crawl_run_id=$1 ORDER BY h.created_at,h.event_id",
                &[&run_id],
            )
            .map_err(|error| {
                Error::storage(format!(
                    "read retained Crawl event history for preflight: {error}"
                ))
            })?;
        let mut event_batches: HashMap<
            String,
            Vec<(String, String, String, String, i32, i64, i32)>,
        > = HashMap::new();
        for row in self.client.query(
            "SELECT b.batch_id,b.manifest_sha256,b.records_sha256,b.coverage_sha256,b.record_count,b.records_bytes,
                    COALESCE(b.quarantine_count, -1),e.event_id
             FROM scout_crawl_event_batches b JOIN scout_crawl_event_batch_events e USING(batch_id)
             WHERE b.consumer_id=$1 AND b.crawl_run_id=$2",
            &[&consumer_id, &run_id],
        ).map_err(|error| Error::storage(format!("read registered batch event coverage: {error}")))? {
            let event_id: String = row.get(7);
            event_batches.entry(event_id).or_default().push((row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6)));
        }
        let mut batch_validity = HashMap::<String, bool>::new();
        let mut replayable_gaps = Vec::new();
        let mut unrecoverable_gaps = Vec::new();
        if let Some(reason) = integrity_gap {
            unrecoverable_gaps.push(ReplayGap {
                event_id: String::new(),
                reason,
            });
        }
        let artifact_root = data_dir
            .canonicalize()
            .unwrap_or_else(|_| data_dir.to_path_buf());
        for row in &history {
            let event_id: String = row.get(0);
            let expected_sha: Option<String> = row.get(1);
            let event_exists: bool = row.get(2);
            let event_sha: Option<String> = row.get(3);
            let payload_sha: Option<String> = row.get(4);
            let expected_type: String = row.get(5);
            let live_type: Option<String> = row.get(6);
            let mut event_ok = event_exists
                && expected_type == live_type.as_deref().unwrap_or_default()
                && (expected_sha.is_none()
                    || (expected_sha == event_sha && expected_sha == payload_sha));
            if !event_exists || !event_ok {
                let mut valid_batch_ids = Vec::new();
                if let Some(batches) = event_batches.get(&event_id) {
                    for (id, manifest, records, coverage, count, bytes, quarantine_count) in batches
                    {
                        let valid = if let Some(valid) = batch_validity.get(id) {
                            *valid
                        } else {
                            let batch = BatchCopy {
                                batch_id: id.clone(),
                                manifest_sha256: manifest.clone(),
                                records_sha256: records.clone(),
                                coverage_sha256: coverage.clone(),
                                record_count: *count,
                                quarantine_count: *quarantine_count,
                                records_bytes: *bytes,
                            };
                            let local = self.client.query(
                                "SELECT local_path FROM scout_crawl_event_batch_locations WHERE batch_id=$1",
                                &[id],
            ).ok().is_some_and(|paths| paths.iter().any(|path| {
                let path = PathBuf::from(path.get::<_,String>(0));
                managed_path_is_safe(&artifact_root, &path).is_ok() && verify_batch_files(&path, &batch).is_ok()
            }));
                            let archived = self
                                .archive_batch_valid(&artifact_root, id, &batch)
                                .unwrap_or(false);
                            let valid = *quarantine_count == 0 && (local || archived);
                            batch_validity.insert(id.clone(), valid);
                            valid
                        };
                        if valid {
                            valid_batch_ids.push(id.clone());
                        }
                    }
                }
                event_ok = !valid_batch_ids.is_empty();
                if event_ok {
                    replayable_gaps.push(ReplayableGap {
                        event_id: event_id.clone(),
                        batch_ids: valid_batch_ids,
                    });
                } else {
                    unrecoverable_gaps.push(ReplayGap { event_id: event_id.clone(), reason: "event is absent or conflicts with history, and no verified consumer batch is retained".into() });
                }
            }
            let artifacts = self.client.query(
                "SELECT artifact_uri,sha256,byte_size FROM scout_crawl_event_artifacts WHERE event_id=$1",
                &[&event_id],
            ).map_err(|error| Error::storage(format!("read replay dependencies for event: {error}")))?;
            for artifact in artifacts {
                let uri: String = artifact.get(0);
                let sha: String = artifact.get(1);
                let size: i64 = artifact.get(2);
                let local_ok = artifact_path(&artifact_root, &uri)
                    .ok()
                    .is_some_and(|path| {
                        managed_path_is_safe(&artifact_root, &path).is_ok()
                            && read_verified(&path, &sha, size as u64).is_ok()
                    });
                if !local_ok && !self.archive_artifact_valid(&artifact_root, &uri, &sha, size)? {
                    unrecoverable_gaps.push(ReplayGap {
                        event_id: event_id.clone(),
                        reason: format!(
                            "referenced artifact is missing or fails checksum verification: {uri}"
                        ),
                    });
                }
            }
        }
        replayable_gaps.sort_by(|a, b| a.event_id.cmp(&b.event_id));
        unrecoverable_gaps
            .sort_by(|a, b| a.event_id.cmp(&b.event_id).then(a.reason.cmp(&b.reason)));
        unrecoverable_gaps.dedup_by(|a, b| a.event_id == b.event_id && a.reason == b.reason);
        let status = if !unrecoverable_gaps.is_empty() {
            "unrecoverable"
        } else if !replayable_gaps.is_empty() {
            "replayable"
        } else {
            "complete"
        };
        Ok(ReplayPreflight {
            run_id: run_id.to_owned(),
            consumer_id: consumer_id.to_owned(),
            history_event_count: history.len(),
            replayable_gaps,
            unrecoverable_gaps,
            status,
        })
    }

    fn archive_batch_valid(
        &mut self,
        root: &Path,
        id: &str,
        batch: &BatchCopy,
    ) -> Result<bool, Error> {
        let row = self.client.query_opt(
            "SELECT archive_path FROM scout_crawl_event_archive_items WHERE item_type='batch' AND item_id=$1",
            &[&id],
        ).map_err(|error| Error::storage(format!("read verified archive location: {error}")))?;
        let Some(row) = row else {
            return Ok(false);
        };
        let archive_root = root.join("analytics-archive");
        managed_path_is_safe(root, &archive_root)?;
        let path = safe_relative_path(&archive_root, &row.get::<_, String>(0))?;
        Ok(verify_batch_files(&path, batch).is_ok())
    }

    fn archive_artifact_valid(
        &mut self,
        root: &Path,
        uri: &str,
        sha: &str,
        size: i64,
    ) -> Result<bool, Error> {
        let row = self
            .client
            .query_opt(
                "SELECT archive_path,sha256,byte_size FROM scout_crawl_event_archive_items
             WHERE item_type='artifact' AND item_id=$1",
                &[&uri],
            )
            .map_err(|error| Error::storage(format!("read archived artifact location: {error}")))?;
        let Some(row) = row else {
            return Ok(false);
        };
        let archive_root = root.join("analytics-archive");
        managed_path_is_safe(root, &archive_root)?;
        let path = safe_relative_path(&archive_root, &row.get::<_, String>(0))?;
        let saved_sha: String = row.get(1);
        let saved_size: i64 = row.get(2);
        if saved_sha != sha || saved_size != size {
            return Ok(false);
        }
        Ok(read_verified(&path, sha, size as u64).is_ok())
    }
}

pub(super) fn register_exported_event_batch(
    store: &mut FrontierStore,
    consumer_id: &str,
    batch: &PublishedBatch,
) -> Result<(), Error> {
    store.register_exported_event_batch(consumer_id, batch)
}

pub(super) fn verify_batch_files(path: &Path, batch: &BatchCopy) -> Result<(), Error> {
    let directory = fs::symlink_metadata(path)
        .map_err(|error| Error::storage(format!("inspect event batch directory: {error}")))?;
    if directory.file_type().is_symlink() || !directory.is_dir() {
        return Err(Error::safety(
            "event batch directory is not a regular directory",
        ));
    }
    let names = fs::read_dir(path)
        .map_err(|error| Error::storage(format!("read event batch copy: {error}")))?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|error| Error::storage(format!("read event batch files: {error}")))?;
    if names
        != HashSet::from([
            "batch-manifest.json".into(),
            "records.jsonl".into(),
            "_PUBLISHED".into(),
        ])
    {
        return Err(Error::safety("event batch copy has unexpected files"));
    }
    for name in ["batch-manifest.json", "records.jsonl", "_PUBLISHED"] {
        let metadata = fs::symlink_metadata(path.join(name))
            .map_err(|error| Error::storage(format!("inspect event batch file {name}: {error}")))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::safety(format!(
                "event batch file {name} is not a regular file"
            )));
        }
    }
    let manifest = fs::read(path.join("batch-manifest.json"))
        .map_err(|error| Error::storage(format!("read event batch manifest: {error}")))?;
    let records = fs::read(path.join("records.jsonl"))
        .map_err(|error| Error::storage(format!("read event batch records: {error}")))?;
    let published = fs::read_to_string(path.join("_PUBLISHED"))
        .map_err(|error| Error::storage(format!("read event batch marker: {error}")))?;
    if sha256_hex(&manifest) != batch.manifest_sha256
        || sha256_hex(&records) != batch.records_sha256
        || published != format!("{}\n", batch.manifest_sha256)
        || records.len() as i64 != batch.records_bytes
    {
        return Err(Error::safety(
            "event batch copy checksum or size differs from its registered identity",
        ));
    }
    let manifest_json: serde_json::Value = serde_json::from_slice(&manifest)
        .map_err(|error| Error::safety(format!("invalid event batch manifest: {error}")))?;
    if manifest_json.get("batch_id").and_then(|v| v.as_str()) != Some(batch.batch_id.as_str())
        || manifest_json.get("record_count").and_then(|v| v.as_u64())
            != Some(batch.record_count as u64)
        || manifest_json
            .get("quarantine_count")
            .and_then(|v| v.as_u64())
            .is_none_or(|count| {
                batch.quarantine_count >= 0 && count != batch.quarantine_count as u64
            })
    {
        return Err(Error::safety(
            "event batch manifest identity or record count differs",
        ));
    }
    let mut ids = Vec::new();
    for line in records
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let envelope: serde_json::Value = serde_json::from_slice(line)
            .map_err(|error| Error::safety(format!("invalid event batch record: {error}")))?;
        ids.push(
            envelope
                .get("record_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
        );
    }
    if ids.len() != batch.record_count as usize
        || sha256_hex(ids.join("\0").as_bytes()) != batch.coverage_sha256
    {
        return Err(Error::safety(
            "event batch event coverage differs from its registered identity",
        ));
    }
    Ok(())
}

pub(super) fn batch_quarantine_count(path: &Path, batch: &BatchCopy) -> Result<i32, Error> {
    verify_batch_files(path, batch)?;
    let manifest = fs::read(path.join("batch-manifest.json"))
        .map_err(|error| Error::storage(format!("read event batch quarantine count: {error}")))?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest)
        .map_err(|error| Error::safety(format!("parse event batch quarantine count: {error}")))?;
    let count = manifest
        .get("quarantine_count")
        .and_then(|value| value.as_i64())
        .filter(|count| *count >= 0 && *count <= i64::from(batch.record_count))
        .ok_or_else(|| Error::safety("event batch manifest has an invalid quarantine count"))?;
    i32::try_from(count).map_err(|_| Error::safety("event batch quarantine count is too large"))
}

fn copy_batch(
    root: &Path,
    source: &Path,
    destination: &Path,
    batch: &BatchCopy,
) -> Result<(), Error> {
    verify_batch_files(source, batch)?;
    create_managed_dir(root, destination)?;
    for name in ["batch-manifest.json", "records.jsonl", "_PUBLISHED"] {
        let source = source.join(name);
        let bytes = fs::read(&source)
            .map_err(|error| Error::storage(format!("read archived batch source: {error}")))?;
        let digest = sha256_hex(&bytes);
        write_verified_copy(&destination.join(name), &bytes, &digest)?;
    }
    File::open(destination)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| Error::storage(format!("sync archived batch directory: {error}")))?;
    verify_batch_files(destination, batch)
}

pub(super) fn artifact_path(data_root: &Path, uri: &str) -> Result<PathBuf, Error> {
    let (area, id, file) = if let Some(value) = uri.strip_prefix("scout-artifact://crawl/") {
        let mut parts = value.split('/');
        ("crawls", parts.next(), parts.collect::<Vec<_>>())
    } else if let Some(value) = uri.strip_prefix("scout-artifact://corpora/") {
        let mut parts = value.split('/');
        ("corpora", parts.next(), parts.collect::<Vec<_>>())
    } else {
        return Err(Error::safety(format!(
            "unsupported replay artifact URI: {uri}"
        )));
    };
    let id = id.ok_or_else(|| Error::safety("replay artifact URI has no identity"))?;
    let expected_prefix = if area == "crawls" {
        "run-"
    } else {
        "snapshot-"
    };
    let suffix = id.strip_prefix(expected_prefix).unwrap_or_default();
    if suffix.len() != 64
        || !suffix
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::safety("replay artifact URI has malformed identity"));
    }
    let expected = if area == "crawls" {
        "crawl-manifest.json"
    } else {
        "snapshot-manifest.json"
    };
    let path = if area == "crawls" && file.len() == 2 && file[0] == "bodies" {
        let body = file
            .get(1)
            .ok_or_else(|| Error::safety("body artifact URI has no hash"))?;
        if file.len() != 2
            || body.len() != 64
            || !body
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(Error::safety("body artifact URI has malformed hash"));
        }
        data_root.join("crawls").join(id).join("bodies").join(body)
    } else if file.len() == 1 && file[0] == expected {
        data_root.join(area).join(id).join(expected)
    } else {
        return Err(Error::safety(
            "replay artifact URI is outside supported artifact paths",
        ));
    };
    Ok(path)
}

pub(super) fn read_verified(
    path: &Path,
    digest: &str,
    expected_bytes: u64,
) -> Result<Vec<u8>, Error> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        Error::storage(format!(
            "inspect replay artifact {}: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != expected_bytes
    {
        return Err(Error::safety(format!(
            "replay artifact {} has wrong type or size",
            path.display()
        )));
    }
    let bytes = fs::read(path).map_err(|error| {
        Error::storage(format!("read replay artifact {}: {error}", path.display()))
    })?;
    if sha256_hex(&bytes) != digest {
        return Err(Error::safety(format!(
            "replay artifact {} checksum mismatch",
            path.display()
        )));
    }
    Ok(bytes)
}

fn write_verified_copy(destination: &Path, bytes: &[u8], digest: &str) -> Result<(), Error> {
    if let Some(parent) = destination.parent() {
        let metadata = fs::symlink_metadata(parent)
            .map_err(|error| Error::storage(format!("inspect archive directory: {error}")))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::safety(
                "analytics archive parent is not a regular directory",
            ));
        }
    }
    match fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(Error::safety(
                "existing analytics archive item is not a regular file",
            ));
        }
        Ok(_) => {
            let existing = fs::read(destination)
                .map_err(|error| Error::storage(format!("read existing archive copy: {error}")))?;
            if sha256_hex(&existing) != digest || existing != bytes {
                return Err(Error::safety(
                    "existing analytics archive copy fails checksum verification",
                ));
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(Error::storage(format!(
                "inspect existing archive copy: {error}"
            )));
        }
    }
    let staging = destination.with_extension(format!("tmp-{}", crate::storage::new_run_id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .map_err(|error| Error::storage(format!("create staged archive copy: {error}")))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| Error::storage(format!("write staged archive copy: {error}")))?;
    fs::rename(&staging, destination)
        .map_err(|error| Error::storage(format!("publish archive copy: {error}")))?;
    if sha256_hex(
        &fs::read(destination)
            .map_err(|error| Error::storage(format!("verify archive copy: {error}")))?,
    ) != digest
    {
        return Err(Error::safety(
            "archive copy failed post-write checksum verification",
        ));
    }
    if let Some(parent) = destination.parent() {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                Error::storage(format!("sync analytics archive directory: {error}"))
            })?;
    }
    Ok(())
}

fn create_managed_dir(root: &Path, path: &Path) -> Result<(), Error> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| Error::safety("analytics archive path escapes its managed root"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(Error::safety(
                "analytics archive path contains an unsafe component",
            ));
        }
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(Error::safety(
                    "analytics archive path contains a symlink or non-directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {
                        if let Some(parent) = current.parent() {
                            File::open(parent)
                                .and_then(|directory| directory.sync_all())
                                .map_err(|error| {
                                    Error::storage(format!(
                                        "sync analytics archive parent: {error}"
                                    ))
                                })?;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(Error::storage(format!(
                            "create analytics archive directory: {error}"
                        )));
                    }
                }
                let metadata = fs::symlink_metadata(&current).map_err(|error| {
                    Error::storage(format!("inspect analytics archive directory: {error}"))
                })?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(Error::safety(
                        "analytics archive path contains a symlink or non-directory",
                    ));
                }
            }
            Err(error) => {
                return Err(Error::storage(format!(
                    "inspect analytics archive directory: {error}"
                )));
            }
        }
    }
    let resolved = current
        .canonicalize()
        .map_err(|error| Error::storage(format!("resolve analytics archive directory: {error}")))?;
    if !resolved.starts_with(root) {
        return Err(Error::safety(
            "analytics archive path resolves outside its managed root",
        ));
    }
    Ok(())
}

pub(super) fn managed_path_is_safe(root: &Path, path: &Path) -> Result<(), Error> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| Error::safety("managed analytics path escapes --data-dir"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(Error::safety(
                "managed analytics path contains an unsafe component",
            ));
        }
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Error::safety("managed analytics path contains a symlink"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(Error::storage(format!(
                    "inspect managed analytics path: {error}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn safe_relative_path(root: &Path, relative: &str) -> Result<PathBuf, Error> {
    let relative = Path::new(relative);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(Error::safety(
            "registered analytics archive path is not a safe relative path",
        ));
    }
    let path = root.join(relative);
    managed_path_is_safe(root, &path)?;
    Ok(path)
}
