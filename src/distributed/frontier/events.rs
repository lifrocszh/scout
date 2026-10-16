use super::{FrontierStore, validate_digest};
use crate::distributed::event_batch::{self, MAX_BATCH_BYTES, PendingEvent, PublishedBatch};
use crate::distributed::events::{
    ArtifactReference, CRAWL_EVENT_SCHEMA_VERSION, CaptureCommitted, CorpusSnapshotPublished,
    CrawlCompleted, CrawlEvent,
};
use crate::error::Error;
use crate::storage::sha256_hex;
use postgres::Transaction;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

pub(crate) struct SealedSnapshot {
    pub(crate) crawl_run_id: String,
    pub(crate) snapshot_id: String,
    pub(crate) published_at_utc: String,
    pub(crate) snapshot_manifest_sha256: String,
    pub(crate) snapshot_manifest_bytes: u64,
    pub(crate) crawl_manifest_sha256: String,
    pub(crate) crawl_manifest_bytes: u64,
    pub(crate) page_count: u64,
    pub(crate) alias_count: u64,
    pub(crate) passage_count: u64,
}

pub(crate) enum EventExportOutcome {
    Published(PublishedBatch),
    PendingBackoff { next_attempt_at_utc: String },
    NoPendingRecords,
}

impl FrontierStore {
    /// Publish and register a local batch before advancing its export checkpoint.
    pub(crate) fn export_local_event_batch(
        &mut self,
        run_id: &str,
        consumer_id: &str,
        queue_root: &Path,
        output_root: &Path,
        max_records: usize,
        max_batch_bytes: usize,
        max_local_bytes: u64,
        max_elapsed: Duration,
        poll_interval: Duration,
        follow: bool,
    ) -> Result<EventExportOutcome, Error> {
        self.ensure_analytics_consumer(consumer_id)?;
        self.validate_export_history(run_id)?;
        let run_suffix = run_id.strip_prefix("run-").unwrap_or_default();
        if run_suffix.len() != 64
            || !run_suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::invalid(
                "crawl_run_id must be run- followed by 64 lowercase hex characters",
            ));
        }
        if !(1..=128).contains(&max_records) {
            return Err(Error::invalid("max_records must be between 1 and 128"));
        }
        if !(4096..=MAX_BATCH_BYTES).contains(&max_batch_bytes) {
            return Err(Error::invalid(
                "max_batch_bytes must be between 4096 and 64 MiB",
            ));
        }
        if max_local_bytes < max_batch_bytes as u64 + 1024 * 1024 {
            return Err(Error::invalid(
                "max_local_bytes must fit one batch plus its manifest reserve",
            ));
        }
        if max_elapsed.is_zero() || poll_interval.is_zero() {
            return Err(Error::invalid(
                "batch elapsed and poll intervals must be positive",
            ));
        }

        let started = Instant::now();
        let mut events = Vec::<PendingEvent>::new();
        let mut records_bytes = 0usize;
        let mut stop = false;
        while events.len() < max_records && !stop {
            let selected_event_ids = events
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<Vec<_>>();
            let limit = i64::try_from(max_records - events.len())
                .map_err(|_| Error::invalid("event batch limit is too large"))?;
            let rows = self
                .client
                .query(
                    "SELECT event.event_id, event.event_type, event.schema_version,
                            event.payload_json, event.payload_sha256,
                            to_char(event.created_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
                     FROM scout_crawl_events AS event
                     LEFT JOIN scout_crawl_event_checkpoints AS checkpoint
                       ON checkpoint.consumer_id = $2 AND checkpoint.event_id = event.event_id
                     WHERE event.crawl_run_id = $1
                       AND NOT (event.event_id = ANY($3::TEXT[]))
                       AND (checkpoint.event_id IS NULL OR (
                           checkpoint.acknowledged_at IS NULL
                           AND checkpoint.next_attempt_at <= clock_timestamp()
                       ))
                     ORDER BY event.created_at, event.event_id
                     LIMIT $4",
                    &[&run_id, &consumer_id, &selected_event_ids, &limit],
                )
                .map_err(|error| Error::storage(format!("read pending Crawl events: {error}")))?;
            for row in rows {
                if follow && !events.is_empty() && started.elapsed() >= max_elapsed {
                    stop = true;
                    break;
                }
                let mut event = PendingEvent {
                    event_id: row.get(0),
                    event_type: row.get(1),
                    schema_version: row.get(2),
                    payload_json: row.get(3),
                    payload_sha256: row.get(4),
                    created_at_utc: row.get(5),
                    force_quarantine: false,
                };
                let mut size = event_batch::encoded_record_size(&event, run_id)?;
                if size > max_batch_bytes {
                    event.force_quarantine = true;
                    size = event_batch::encoded_record_size(&event, run_id)?;
                }
                if size > max_batch_bytes {
                    let error = Error::safety(
                        "crawl event cannot fit the configured batch limit, even when quarantined",
                    );
                    self.record_event_failure(consumer_id, &event.event_id, &error.message)?;
                    return Err(error);
                }
                if records_bytes.saturating_add(size) > max_batch_bytes {
                    stop = true;
                    break;
                }
                records_bytes += size;
                stop = event.is_snapshot_published(run_id);
                events.push(event);
                if stop {
                    break;
                }
            }
            if stop || events.len() >= max_records || !follow {
                break;
            }
            let remaining = max_elapsed.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            // Continue collecting until the count, byte, terminal-event, or elapsed bound.
            thread::sleep(poll_interval.min(remaining));
            if started.elapsed() >= max_elapsed {
                break;
            }
        }
        if events.is_empty() {
            self.reject_missing_required_events(run_id)?;
            let retry = self
                .client
                .query_one(
                    "SELECT to_char(MIN(checkpoint.next_attempt_at) AT TIME ZONE 'UTC',
                                'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
                 FROM scout_crawl_events event
                 JOIN scout_crawl_event_checkpoints checkpoint
                   ON checkpoint.consumer_id=$2 AND checkpoint.event_id=event.event_id
                 WHERE event.crawl_run_id=$1 AND checkpoint.acknowledged_at IS NULL",
                    &[&run_id, &consumer_id],
                )
                .map_err(|error| {
                    Error::storage(format!("check Crawl event retry backoff: {error}"))
                })?;
            let next_attempt_at_utc: Option<String> = retry.get(0);
            return Ok(match next_attempt_at_utc {
                Some(next_attempt_at_utc) => EventExportOutcome::PendingBackoff {
                    next_attempt_at_utc,
                },
                None => EventExportOutcome::NoPendingRecords,
            });
        }
        events.sort_by(|left, right| {
            left.created_at_utc
                .cmp(&right.created_at_utc)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });

        let published = match event_batch::publish_batch(
            queue_root,
            output_root,
            run_id,
            consumer_id,
            &events,
            max_records,
            max_batch_bytes,
            max_local_bytes,
        ) {
            Ok(published) => published,
            Err(error) => {
                for event in &events {
                    self.record_event_failure(consumer_id, &event.event_id, &error.message)?;
                }
                return Err(error);
            }
        };
        if published.snapshot_published {
            self.reject_missing_required_events(run_id)?;
        }
        super::analytics::register_exported_event_batch(self, consumer_id, &published)?;
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin Crawl event batch acknowledgement: {error}"))
        })?;
        for event_id in &published.event_ids {
            tx.execute(
                "INSERT INTO scout_crawl_event_checkpoints(
                     consumer_id, event_id, attempt_count, next_attempt_at,
                     last_error, acknowledged_at
                 ) VALUES ($1, $2, 0, clock_timestamp(), NULL, clock_timestamp())
                 ON CONFLICT (consumer_id, event_id) DO UPDATE
                 SET next_attempt_at = clock_timestamp(), last_error = NULL,
                     acknowledged_at = clock_timestamp()",
                &[&consumer_id, event_id],
            )
            .map_err(|error| {
                Error::storage(format!("acknowledge published Crawl event batch: {error}"))
            })?;
        }
        tx.commit().map_err(|error| {
            Error::storage(format!("commit Crawl event batch acknowledgement: {error}"))
        })?;
        Ok(EventExportOutcome::Published(published))
    }

    fn reject_missing_required_events(&mut self, run_id: &str) -> Result<(), Error> {
        let row = self
            .client
            .query_one(
                "WITH expected AS (
                 SELECT history.crawl_run_id, history.event_type, history.event_id
                 FROM scout_crawl_event_history AS history
                 WHERE history.crawl_run_id = $1
             ), missing AS (
                 SELECT expected.event_type, expected.event_id
                 FROM expected
                 WHERE NOT EXISTS (
                     SELECT 1
                     FROM scout_crawl_events AS event
                     WHERE event.crawl_run_id = expected.crawl_run_id
                       AND event.event_id = expected.event_id
                       AND event.event_type = expected.event_type
                 )
             ), ranked AS (
                 SELECT event_type, event_id,
                        row_number() OVER (ORDER BY event_id) AS ordinal
                 FROM missing
             )
             SELECT EXISTS (
                        SELECT 1 FROM scout_crawl_runs WHERE crawl_run_id = $1
                    ) OR EXISTS (
                        SELECT 1 FROM scout_crawl_event_history WHERE crawl_run_id = $1
                    ),
                    COUNT(*)::BIGINT,
                    COALESCE(
                        array_agg(event_type || ':' || event_id ORDER BY event_id)
                            FILTER (WHERE ordinal <= 20),
                        ARRAY[]::TEXT[]
                    )
             FROM ranked",
                &[&run_id],
            )
            .map_err(|error| {
                Error::storage(format!("check retained Crawl event history: {error}"))
            })?;
        let run_exists: bool = row.get(0);
        if !run_exists {
            return Err(Error::invalid(format!("unknown Crawl run: {run_id}")));
        }
        let missing_count: i64 = row.get(1);
        if missing_count == 0 {
            return Ok(());
        }
        let missing_sample: Vec<String> = row.get(2);
        let omitted = if missing_count as usize > missing_sample.len() {
            format!(
                "; sample omits {} more",
                missing_count as usize - missing_sample.len()
            )
        } else {
            String::new()
        };
        Err(Error::safety(format!(
            "retained-history gap for {run_id}: {missing_count} required event(s) are missing; sample=[{}]{omitted}. No-pending status withheld. Replay retained immutable event batch(es); if no retained copy exists, record an unrecoverable gap. Do not infer missing captures or mark the run caught up",
            missing_sample.join(",")
        )))
    }

    /// Run a bounded at-least-once local delivery batch for one independent consumer.
    /// Handlers must be idempotent because a crash after processing and before ack replays.
    pub(crate) fn consume_local_events(
        &mut self,
        run_id: &str,
        consumer_id: &str,
        limit: usize,
        mut process: impl FnMut(&CrawlEvent) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        self.ensure_analytics_consumer(consumer_id)?;
        if run_id.trim().is_empty() {
            return Err(Error::invalid("crawl_run_id must not be empty"));
        }
        if !(1..=128).contains(&limit) {
            return Err(Error::invalid(
                "event batch limit must be between 1 and 128",
            ));
        }
        let limit = i64::try_from(limit).map_err(|_| Error::invalid("event batch is too large"))?;
        let rows = self
            .client
            .query(
                "SELECT event.event_id, event.event_type, event.schema_version,
                        event.payload_json, event.payload_sha256
                 FROM scout_crawl_events AS event
                 LEFT JOIN scout_crawl_event_checkpoints AS checkpoint
                   ON checkpoint.consumer_id = $2 AND checkpoint.event_id = event.event_id
                 WHERE event.crawl_run_id = $1
                   AND (checkpoint.event_id IS NULL OR (
                       checkpoint.acknowledged_at IS NULL
                       AND checkpoint.next_attempt_at <= clock_timestamp()
                   ))
                 ORDER BY event.created_at, event.event_id
                 LIMIT $3",
                &[&run_id, &consumer_id, &limit],
            )
            .map_err(|error| Error::storage(format!("read pending crawl events: {error}")))?;
        let mut processed = 0;
        for row in rows {
            let event_id: String = row.get(0);
            let event_type: String = row.get(1);
            let schema_version: i32 = row.get(2);
            let payload: String = row.get(3);
            let payload_sha256: Option<String> = row.get(4);
            let event = match serde_json::from_str::<CrawlEvent>(&payload) {
                Ok(event)
                    if event.event_id() == event_id
                        && event.crawl_run_id() == run_id
                        && event.event_type() == event_type
                        && i32::try_from(event.schema_version()).ok() == Some(schema_version)
                        && payload_sha256
                            .as_deref()
                            .is_none_or(|expected| sha256_hex(payload.as_bytes()) == expected) =>
                {
                    event
                }
                Ok(_) => {
                    let error = Error::storage(format!(
                        "crawl event {event_id:?} does not match its durable envelope"
                    ));
                    self.record_event_failure(consumer_id, &event_id, &error.message)?;
                    return Err(error);
                }
                Err(error) => {
                    let error = Error::storage(format!("decode crawl event {event_id:?}: {error}"));
                    self.record_event_failure(consumer_id, &event_id, &error.message)?;
                    return Err(error);
                }
            };
            if let Err(error) = process(&event) {
                self.record_event_failure(consumer_id, &event_id, &error.message)?;
                return Err(error);
            }
            self.client
                .execute(
                    "INSERT INTO scout_crawl_event_checkpoints(
                         consumer_id, event_id, attempt_count, next_attempt_at,
                         last_error, acknowledged_at
                     ) VALUES ($1, $2, 0, clock_timestamp(), NULL, clock_timestamp())
                     ON CONFLICT (consumer_id, event_id) DO UPDATE
                     SET next_attempt_at = clock_timestamp(), last_error = NULL,
                         acknowledged_at = clock_timestamp()",
                    &[&consumer_id, &event_id],
                )
                .map_err(|error| Error::storage(format!("acknowledge crawl event: {error}")))?;
            processed += 1;
            if matches!(event, CrawlEvent::CorpusSnapshotPublished(_)) {
                break;
            }
        }
        Ok(processed)
    }

    /// Publish validated, immutably sealed artifacts for a registered completed Crawl.
    /// Return `false` when no durable run is registered; this is normal for local crawls,
    /// which share the `run-*` ID format. Repeating a completed publication is safe.
    pub(crate) fn publish_snapshot_if_registered(
        &mut self,
        snapshot: SealedSnapshot,
    ) -> Result<bool, Error> {
        if snapshot.crawl_run_id.trim().is_empty()
            || snapshot.snapshot_id.trim().is_empty()
            || snapshot.published_at_utc.trim().is_empty()
        {
            return Err(Error::invalid(
                "sealed snapshot identity and timestamp are required",
            ));
        }
        validate_digest(
            &snapshot.snapshot_manifest_sha256,
            "snapshot manifest SHA-256",
        )?;
        validate_digest(&snapshot.crawl_manifest_sha256, "Crawl manifest SHA-256")?;
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin snapshot publication: {error}")))?;
        let Some(run) = tx
            .query_opt(
                "SELECT status FROM scout_crawl_runs WHERE crawl_run_id = $1 FOR UPDATE",
                &[&snapshot.crawl_run_id],
            )
            .map_err(|error| Error::storage(format!("read snapshot Crawl state: {error}")))?
        else {
            tx.commit()
                .map_err(|error| Error::storage(format!("finish snapshot lookup: {error}")))?;
            return Ok(false);
        };
        let status = run.get::<_, String>(0);
        if status != "completed" {
            return Err(Error::invalid(
                "Corpus snapshot publication requires a durably completed Crawl",
            ));
        }
        let event_id = format!(
            "corpus-snapshot-published.v1:{}:{}",
            snapshot.crawl_run_id, snapshot.snapshot_id
        );
        let event = CrawlEvent::CorpusSnapshotPublished(CorpusSnapshotPublished {
            event_id,
            schema_version: CRAWL_EVENT_SCHEMA_VERSION,
            crawl_run_id: snapshot.crawl_run_id.clone(),
            snapshot_id: snapshot.snapshot_id.clone(),
            published_at_utc: snapshot.published_at_utc,
            snapshot_manifest: ArtifactReference {
                uri: format!(
                    "scout-artifact://corpora/{}/snapshot-manifest.json",
                    snapshot.snapshot_id
                ),
                sha256: snapshot.snapshot_manifest_sha256,
                byte_size: snapshot.snapshot_manifest_bytes,
            },
            crawl_manifest: ArtifactReference {
                uri: format!(
                    "scout-artifact://crawl/{}/crawl-manifest.json",
                    snapshot.crawl_run_id
                ),
                sha256: snapshot.crawl_manifest_sha256,
                byte_size: snapshot.crawl_manifest_bytes,
            },
            page_count: snapshot.page_count,
            alias_count: snapshot.alias_count,
            passage_count: snapshot.passage_count,
        });
        insert_event(&mut tx, &event, &snapshot.snapshot_id)?;
        tx.commit()
            .map_err(|error| Error::storage(format!("commit snapshot publication: {error}")))?;
        Ok(true)
    }

    /// Mark the run complete after claims stop and accepted result projections drain.
    pub(crate) fn complete_run_if_quiescent(
        &mut self,
        run_id: &str,
        stop_reason: &str,
        completed_source_ids: &[String],
        result_jobs: usize,
    ) -> Result<bool, Error> {
        if !matches!(
            stop_reason,
            "page_target_reached" | "duration_reached" | "frontier_exhausted"
        ) {
            return Err(Error::invalid("unknown Crawl stop reason"));
        }
        if result_jobs > 0 {
            return Ok(false);
        }
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin Crawl completion: {error}")))?;
        tx.query_opt(
            "SELECT crawl_run_id FROM scout_crawl_task_scheduler
             WHERE crawl_run_id = $1 FOR UPDATE",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("lock Crawl scheduler for completion: {error}")))?
        .ok_or_else(|| Error::storage("crawl task scheduler is missing"))?;
        let run = tx
            .query_opt(
                "SELECT config_digest, status FROM scout_crawl_runs
                 WHERE crawl_run_id = $1 FOR UPDATE",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("lock Crawl run for completion: {error}")))?;
        let Some(run) = run else {
            return Err(Error::storage(format!(
                "crawl run {run_id:?} does not exist"
            )));
        };
        let config_sha256: String = run.get(0);
        let status: String = run.get(1);
        if status != "active" {
            return Ok(false);
        }
        let live_leases: i64 = tx
            .query_one(
                "SELECT count(*) FROM scout_crawl_tasks
                 WHERE crawl_run_id = $1 AND status = 'leased'
                   AND lease_deadline > clock_timestamp()",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("check live Crawl leases: {error}")))?
            .get(0);
        if live_leases > 0 {
            return Ok(false);
        }
        if stop_reason == "frontier_exhausted" {
            let unfinished: i64 = tx
                .query_one(
                    "SELECT count(*) FROM scout_crawl_tasks
                     WHERE crawl_run_id=$1 AND status IN ('pending','leased')",
                    &[&run_id],
                )
                .map_err(|error| Error::storage(format!("check global Crawl frontier: {error}")))?
                .get(0);
            if unfinished > 0 {
                return Ok(false);
            }
        }
        let completed_memberships: i64 = tx
            .query_one(
                "SELECT count(*) FROM scout_scheduled_run_sources
                 WHERE crawl_run_id=$1 AND source_id=ANY($2::TEXT[])",
                &[&run_id, &completed_source_ids],
            )
            .map_err(|error| {
                Error::storage(format!("validate completed scheduled Sources: {error}"))
            })?
            .get(0);
        if completed_memberships != completed_source_ids.len() as i64 {
            return Err(Error::invalid(
                "completed Source IDs are outside the persisted scheduled run scope",
            ));
        }
        tx.execute(
            "UPDATE scout_scheduled_run_sources
             SET work_completed=(source_id=ANY($2::TEXT[])), completion_stop_reason=$3
             WHERE crawl_run_id=$1",
            &[&run_id, &completed_source_ids, &stop_reason],
        )
        .map_err(|error| Error::storage(format!("persist scheduled Source completion: {error}")))?;
        let completed_at: String = tx
            .query_one(
                "UPDATE scout_crawl_runs
                 SET status = 'completed', completed_at = clock_timestamp(), stop_reason = $2
                 WHERE crawl_run_id = $1 AND status = 'active'
                 RETURNING to_char(completed_at AT TIME ZONE 'UTC',
                                   'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
                &[&run_id, &stop_reason],
            )
            .map_err(|error| Error::storage(format!("persist Crawl completion: {error}")))?
            .get(0);
        let event = CrawlEvent::CrawlCompleted(CrawlCompleted {
            event_id: format!("crawl-completed.v1:{run_id}"),
            schema_version: CRAWL_EVENT_SCHEMA_VERSION,
            crawl_run_id: run_id.to_owned(),
            completed_at_utc: completed_at,
            stop_reason: stop_reason.to_owned(),
            config_sha256,
        });
        insert_event(&mut tx, &event, "run")?;
        tx.commit()
            .map_err(|error| Error::storage(format!("commit Crawl completion: {error}")))?;
        Ok(true)
    }

    fn record_event_failure(
        &mut self,
        consumer_id: &str,
        event_id: &str,
        message: &str,
    ) -> Result<(), Error> {
        let message = message.chars().take(2048).collect::<String>();
        self.client
            .execute(
                "INSERT INTO scout_crawl_event_checkpoints(
                     consumer_id, event_id, attempt_count, next_attempt_at, last_error
                 ) VALUES ($1, $2, 1, clock_timestamp() + interval '2 seconds', $3)
                 ON CONFLICT (consumer_id, event_id) DO UPDATE
                 SET attempt_count = scout_crawl_event_checkpoints.attempt_count + 1,
                     next_attempt_at = clock_timestamp() +
                         LEAST(60, POWER(2, LEAST(
                             scout_crawl_event_checkpoints.attempt_count + 1, 6
                         )))::DOUBLE PRECISION * interval '1 second',
                     last_error = EXCLUDED.last_error,
                     acknowledged_at = NULL",
                &[&consumer_id, &event_id, &message],
            )
            .map_err(|error| Error::storage(format!("record Crawl event retry: {error}")))?;
        Ok(())
    }
}

pub(super) fn persist_capture_committed(
    tx: &mut Transaction<'_>,
    run_id: &str,
    source_id: &str,
    task_id: i64,
    committed_at_utc: &str,
    result_sha256: &str,
    content_sha256: &str,
    body_sha256: &str,
    body_size: u64,
) -> Result<(), Error> {
    let event = CrawlEvent::CaptureCommitted(CaptureCommitted {
        event_id: format!("capture-committed.v1:{run_id}:{task_id}"),
        schema_version: CRAWL_EVENT_SCHEMA_VERSION,
        crawl_run_id: run_id.to_owned(),
        source_id: source_id.to_owned(),
        task_id,
        committed_at_utc: committed_at_utc.to_owned(),
        result_sha256: result_sha256.to_owned(),
        content_sha256: content_sha256.to_owned(),
        body_artifact: ArtifactReference {
            uri: format!("scout-artifact://crawl/{run_id}/bodies/{body_sha256}"),
            sha256: body_sha256.to_owned(),
            byte_size: body_size,
        },
    });
    insert_event(tx, &event, &task_id.to_string())
}

fn insert_event(
    tx: &mut Transaction<'_>,
    event: &CrawlEvent,
    dedupe_key: &str,
) -> Result<(), Error> {
    let payload = serde_json::to_string(event)
        .map_err(|error| Error::storage(format!("serialize Crawl event: {error}")))?;
    let payload_sha256 = sha256_hex(payload.as_bytes());
    let inserted = tx
        .execute(
            "INSERT INTO scout_crawl_events(
                 event_id, crawl_run_id, event_type, schema_version, dedupe_key,
                 payload_json, payload_sha256
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (crawl_run_id, event_type, dedupe_key) DO NOTHING",
            &[
                &event.event_id(),
                &event.crawl_run_id(),
                &event.event_type(),
                &i32::try_from(event.schema_version())
                    .map_err(|_| Error::invalid("Crawl event schema version is too large"))?,
                &dedupe_key,
                &payload,
                &payload_sha256,
            ],
        )
        .map_err(|error| Error::storage(format!("persist Crawl event: {error}")))?;
    if inserted == 0 {
        let existing = tx
            .query_one(
                "SELECT event_id, payload_sha256 FROM scout_crawl_events
                 WHERE crawl_run_id = $1 AND event_type = $2 AND dedupe_key = $3",
                &[&event.crawl_run_id(), &event.event_type(), &dedupe_key],
            )
            .map_err(|error| Error::storage(format!("read existing Crawl event: {error}")))?;
        let existing_id: String = existing.get(0);
        let existing_sha256: Option<String> = existing.get(1);
        if existing_id != event.event_id()
            || existing_sha256
                .as_deref()
                .is_some_and(|stored| stored != payload_sha256)
        {
            return Err(Error::storage(format!(
                "Crawl event identity {:?} was reused with conflicting content",
                event.event_id()
            )));
        }
    }
    Ok(())
}
