use super::{
    AcceptedCapture, AcceptedCaptureRecord, ClaimedTask, CompletionAcceptance, DiscoveredTask,
    FrontierStore, validate_digest, validate_schedule_key,
};
use crate::error::Error;
use crate::storage::sha256_hex;
use crate::url::{normalize_url, origin_key};
use ::url::Url;

impl FrontierStore {
    /// Commit one accepted worker result, its eligible discoveries, and task completion atomically.
    pub(crate) fn accept_task_result(
        &mut self,
        task: &ClaimedTask,
        result_sha256: &str,
        capture: Option<&AcceptedCapture>,
        rejection_reason: &str,
        discovered: &[DiscoveredTask],
        initially_rejected_discoveries: usize,
        max_task_attempts: i32,
        max_frontier: usize,
    ) -> Result<CompletionAcceptance, Error> {
        validate_digest(result_sha256, "result SHA-256")?;
        if max_task_attempts <= 0 {
            return Err(Error::invalid("max_task_attempts must be positive"));
        }
        match capture {
            Some(capture) => {
                validate_digest(&capture.content_hash, "content SHA-256")?;
                validate_digest(&capture.body_hash, "body SHA-256")?;
                if capture.source_id.trim().is_empty()
                    || capture.identity.is_empty()
                    || sha256_hex(capture.identity.as_bytes()) != capture.content_hash
                    || capture.body_size < 0
                    || !rejection_reason.is_empty()
                    || capture.metadata_json.is_empty()
                {
                    return Err(Error::invalid("accepted capture metadata is inconsistent"));
                }
            }
            None if rejection_reason.trim().is_empty() => {
                return Err(Error::invalid(
                    "completion requires a capture or rejection reason",
                ));
            }
            None => {}
        }
        let max_frontier =
            i64::try_from(max_frontier).map_err(|_| Error::invalid("max_frontier is too large"))?;
        let initial_rejected = i64::try_from(initially_rejected_discoveries)
            .map_err(|_| Error::invalid("rejected discovery count is too large"))?;
        let _measurement = crate::distributed::metrics::DatabaseScope::new("accept_task_result");
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin capture acceptance: {error}")))?;

        // Only discoveries consume frontier capacity. Page-only completions avoid the fair
        // scheduler cursor lock; discovery admission still serializes with claims/sitemaps.
        if !discovered.is_empty() {
            let started = std::time::Instant::now();
            tx.query_opt(
                "SELECT crawl_run_id FROM scout_crawl_task_scheduler
                 WHERE crawl_run_id = $1 FOR UPDATE",
                &[&task.crawl_run_id],
            )
            .map_err(|error| Error::storage(format!("lock capture frontier: {error}")))?
            .ok_or_else(|| Error::storage("crawl task scheduler is missing"))?;
            crate::distributed::metrics::completion_phase("postgres_scheduler_lock_wait", started);
        }

        let started = std::time::Instant::now();
        let row = tx
            .query_opt(
                "SELECT crawl_run_id, source_id, origin, normalized_url, schedule_key,
                        idempotency_key, status, lease_owner, lease_epoch
                 FROM scout_crawl_tasks WHERE task_id = $1 FOR UPDATE",
                &[&task.task_id],
            )
            .map_err(|error| Error::storage(format!("lock capture task: {error}")))?;
        crate::distributed::metrics::completion_phase("postgres_task_lock_wait", started);
        let Some(row) = row else {
            return Ok(CompletionAcceptance::Stale);
        };
        let identity_matches = row.get::<_, String>(0) == task.crawl_run_id
            && row.get::<_, String>(1) == task.source_id
            && row.get::<_, String>(2) == task.origin
            && row.get::<_, String>(3) == task.normalized_url
            && row.get::<_, String>(4) == task.schedule_key
            && row.get::<_, String>(5) == task.idempotency_key;
        if !identity_matches {
            return Ok(CompletionAcceptance::Stale);
        }
        let prior = tx
            .query_opt(
                "SELECT idempotency_key, lease_owner, lease_epoch, result_sha256,
                        accepted_discovered_count, rejected_discovered_count
                 FROM scout_crawl_task_results WHERE task_id = $1",
                &[&task.task_id],
            )
            .map_err(|error| Error::storage(format!("read accepted task result: {error}")))?;
        if let Some(prior) = prior {
            if prior.get::<_, i64>(2) != task.lease_epoch
                || prior.get::<_, String>(1) != task.lease_owner
            {
                return Ok(CompletionAcceptance::Stale);
            }
            if prior.get::<_, String>(0) != task.idempotency_key
                || prior.get::<_, String>(3) != result_sha256
            {
                return Err(Error::invalid(
                    "task idempotency key was reused with a conflicting result payload",
                ));
            }
            let accepted_discovered_count = usize::try_from(prior.get::<_, i64>(4))
                .map_err(|_| Error::storage("accepted discovery count is invalid"))?;
            let rejected_discovered_count = usize::try_from(prior.get::<_, i64>(5))
                .map_err(|_| Error::storage("rejected discovery count is invalid"))?;
            tx.commit().map_err(|error| {
                Error::storage(format!("finish duplicate capture acceptance: {error}"))
            })?;
            return Ok(CompletionAcceptance::Duplicate {
                accepted_discovered_count,
                rejected_discovered_count,
            });
        }

        if row.get::<_, &str>(6) != "leased"
            || row.get::<_, Option<String>>(7).as_deref() != Some(task.lease_owner.as_str())
            || row.get::<_, i64>(8) != task.lease_epoch
            || !super::task_lease_is_live_locked(&mut tx, task)?
        {
            return Ok(CompletionAcceptance::Stale);
        }

        if let Some(capture) = capture {
            let started = std::time::Instant::now();
            let group = tx
                .query_opt(
                    "SELECT identity FROM scout_crawl_content_groups
                     WHERE crawl_run_id = $1 AND source_id = $2 AND content_hash = $3 FOR UPDATE",
                    &[
                        &task.crawl_run_id,
                        &capture.source_id,
                        &capture.content_hash,
                    ],
                )
                .map_err(|error| Error::storage(format!("read unique content group: {error}")))?;
            crate::distributed::metrics::completion_phase(
                "postgres_content_group_lock_wait",
                started,
            );
            if group.is_some_and(|row| row.get::<_, Vec<u8>>(0) != capture.identity.as_bytes()) {
                return Err(Error::storage(
                    "extracted content SHA-256 collision within a Source",
                ));
            }
            let identity_bytes = capture.identity.as_bytes().to_vec();
            tx.execute(
                "INSERT INTO scout_crawl_content_groups(
                     crawl_run_id, source_id, content_hash, identity
                 ) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                &[
                    &task.crawl_run_id,
                    &capture.source_id,
                    &capture.content_hash,
                    &identity_bytes,
                ],
            )
            .map_err(|error| Error::storage(format!("record unique content group: {error}")))?;
        }

        let (accepted_discovered_count, rejected_discovered_count, persisted_discoveries) =
            enqueue_bounded_discoveries(
                &mut tx,
                &task.crawl_run_id,
                discovered,
                initial_rejected,
                max_task_attempts,
                max_frontier,
            )?;

        let (source_id, content_hash, body_hash, body_size, metadata_json) = capture
            .map(|capture| {
                (
                    Some(capture.source_id.as_str()),
                    Some(capture.content_hash.as_str()),
                    Some(capture.body_hash.as_str()),
                    Some(capture.body_size),
                    Some(capture.metadata_json.as_str()),
                )
            })
            .unwrap_or((None, None, None, None, None));
        let accepted_at_utc: String = tx
            .query_one(
                "INSERT INTO scout_crawl_task_results(
                 task_id, crawl_run_id, source_id, content_hash, body_hash, body_size,
                 capture_metadata, idempotency_key, lease_owner, lease_epoch, result_sha256,
                 discovered_urls, accepted_discovered_count, rejected_discovered_count,
                 rejection_reason
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
             RETURNING to_char(accepted_at AT TIME ZONE 'UTC',
                               'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
                &[
                    &task.task_id,
                    &task.crawl_run_id,
                    &source_id,
                    &content_hash,
                    &body_hash,
                    &body_size,
                    &metadata_json,
                    &task.idempotency_key,
                    &task.lease_owner,
                    &task.lease_epoch,
                    &result_sha256,
                    &persisted_discoveries,
                    &accepted_discovered_count,
                    &rejected_discovered_count,
                    &rejection_reason,
                ],
            )
            .map_err(|error| Error::storage(format!("persist accepted task result: {error}")))?
            .get(0);
        if let Some(capture) = capture {
            let body_size = u64::try_from(capture.body_size)
                .map_err(|_| Error::storage("accepted capture body size is invalid"))?;
            super::events::persist_capture_committed(
                &mut tx,
                &task.crawl_run_id,
                &capture.source_id,
                task.task_id,
                &accepted_at_utc,
                result_sha256,
                &capture.content_hash,
                &capture.body_hash,
                body_size,
            )?;
        }
        let completed = tx
            .execute(
                "UPDATE scout_crawl_tasks
                 SET status = 'succeeded', lease_owner = NULL, lease_deadline = NULL,
                     updated_at = clock_timestamp()
                 WHERE task_id = $1 AND status = 'leased'
                   AND lease_owner = $2 AND lease_epoch = $3
                   AND lease_deadline > clock_timestamp()",
                &[&task.task_id, &task.lease_owner, &task.lease_epoch],
            )
            .map_err(|error| Error::storage(format!("complete accepted task: {error}")))?;
        if completed != 1 {
            return Ok(CompletionAcceptance::Stale);
        }
        tx.commit()
            .map_err(|error| Error::storage(format!("commit accepted task result: {error}")))?;
        Ok(CompletionAcceptance::Accepted {
            accepted_discovered_count: usize::try_from(accepted_discovered_count)
                .map_err(|_| Error::storage("accepted discovery count is invalid"))?,
            rejected_discovered_count: usize::try_from(rejected_discovered_count)
                .map_err(|_| Error::storage("rejected discovery count is invalid"))?,
        })
    }

    /// Durable unique `(Source, extracted identity)` counts drive quotas and page targets.
    pub(crate) fn content_group_counts_by_source(
        &mut self,
        run_id: &str,
    ) -> Result<std::collections::HashMap<String, usize>, Error> {
        let rows = self
            .client
            .query(
                "SELECT source_id, count(*) FROM scout_crawl_content_groups
                 WHERE crawl_run_id = $1 GROUP BY source_id",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("count accepted content groups: {error}")))?;
        rows.into_iter()
            .map(|row| {
                let count = usize::try_from(row.get::<_, i64>(1))
                    .map_err(|_| Error::storage("content group count does not fit memory"))?;
                Ok((row.get(0), count))
            })
            .collect()
    }

    /// Read bounded batches so a coordinator restart can rebuild its local SQLite projection.
    pub(crate) fn accepted_capture_batch(
        &mut self,
        run_id: &str,
        after_task_id: i64,
        limit: i64,
    ) -> Result<Vec<AcceptedCaptureRecord>, Error> {
        let rows = self
            .client
            .query(
                "SELECT task_id, capture_metadata FROM scout_crawl_task_results
                 WHERE crawl_run_id = $1 AND capture_metadata IS NOT NULL AND task_id > $2
                 ORDER BY task_id LIMIT $3",
                &[&run_id, &after_task_id, &limit],
            )
            .map_err(|error| Error::storage(format!("read accepted capture batch: {error}")))?;
        rows.into_iter()
            .map(|row| {
                Ok(AcceptedCaptureRecord {
                    task_id: row.get(0),
                    metadata_json: row.get(1),
                })
            })
            .collect()
    }
}

fn enqueue_discovered_in(
    tx: &mut postgres::Transaction<'_>,
    insert_statement: &mut Option<postgres::Statement>,
    run_id: &str,
    discovery: &DiscoveredTask,
    max_task_attempts: i32,
) -> Result<bool, Error> {
    if discovery.source_id.trim().is_empty() || max_task_attempts <= 0 {
        return Err(Error::invalid(
            "discovered Source and max_task_attempts must be positive",
        ));
    }
    let parsed = Url::parse(&discovery.normalized_url)
        .map_err(|error| Error::invalid(format!("invalid discovered URL: {error}")))?;
    let normalized = normalize_url(parsed.clone()).map_err(Error::invalid)?;
    if normalized != discovery.normalized_url {
        return Err(Error::invalid("discovered URL must already be normalized"));
    }
    let origin = origin_key(&parsed);
    validate_schedule_key(&discovery.schedule_key)?;
    let idempotency_key = sha256_hex(
        format!(
            "scout.crawl-task.v1\0{run_id}\0{}\0{}",
            discovery.source_id, discovery.normalized_url
        )
        .as_bytes(),
    );
    let statement = match insert_statement {
        Some(statement) => statement,
        None => insert_statement.insert(
            tx.prepare(
                "INSERT INTO scout_crawl_tasks(
                     crawl_run_id, source_id, origin, normalized_url, schedule_key,
                     max_task_attempts, idempotency_key
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (crawl_run_id, source_id, normalized_url) DO NOTHING",
            )
            .map_err(|error| Error::storage(format!("enqueue discovered crawl URL: {error}")))?,
        ),
    };
    tx.execute(
        statement,
        &[
            &run_id,
            &discovery.source_id,
            &origin,
            &discovery.normalized_url,
            &discovery.schedule_key,
            &max_task_attempts,
            &idempotency_key,
        ],
    )
    .map(|count| count > 0)
    .map_err(|error| Error::storage(format!("enqueue discovered crawl URL: {error}")))
}

/// Caller holds the run scheduler lock; shared by capture and sitemap acceptance.
pub(super) fn enqueue_bounded_discoveries(
    tx: &mut postgres::Transaction<'_>,
    run_id: &str,
    discovered: &[DiscoveredTask],
    initially_rejected: i64,
    max_task_attempts: i32,
    max_frontier: i64,
) -> Result<(i64, i64, Vec<String>), Error> {
    if discovered.is_empty() {
        return Ok((0, initially_rejected, Vec::new()));
    }
    let mut unfinished: i64 = tx
        .query_one(
            "SELECT count(*) FROM scout_crawl_tasks
         WHERE crawl_run_id = $1 AND status IN ('pending', 'leased')",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("count capture frontier: {error}")))?
        .get(0);
    let mut accepted_discovered_count = 0_i64;
    let mut rejected_discovered_count = initially_rejected;
    let mut persisted_discoveries = Vec::new();
    let mut insert_statement = None;
    let mut seen = std::collections::HashSet::new();
    for discovery in discovered {
        if !seen.insert((
            discovery.source_id.as_str(),
            discovery.normalized_url.as_str(),
        )) {
            continue;
        }
        if unfinished >= max_frontier {
            rejected_discovered_count += 1;
            continue;
        }
        if enqueue_discovered_in(
            tx,
            &mut insert_statement,
            run_id,
            discovery,
            max_task_attempts,
        )? {
            accepted_discovered_count += 1;
            unfinished += 1;
            persisted_discoveries.push(discovery.normalized_url.clone());
        }
    }

    Ok((
        accepted_discovered_count,
        rejected_discovered_count,
        persisted_discoveries,
    ))
}
