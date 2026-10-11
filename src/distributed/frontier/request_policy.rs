use super::{
    FrontierStore, PermitDecision, RequestPermitGrant, RequestPolicyLimits, duration_millis,
    validate_request_identity, validate_schedule_key,
};
use crate::error::Error;
use crate::url::normalize_origin;
use std::time::Duration;

impl FrontierStore {
    /// Persist run identity and immutable shared policy limits before workers start.
    pub(crate) fn configure_run(
        &mut self,
        run_id: &str,
        config_digest: &str,
        limits: RequestPolicyLimits,
    ) -> Result<bool, Error> {
        let persisted = request_policy_values(limits)?;
        let inserted = self.ensure_run(run_id, config_digest)?;
        self.client
            .execute(
                "INSERT INTO scout_crawl_request_policy(
                     crawl_run_id, global_concurrency, origin_concurrency,
                     min_start_spacing_ms, request_timeout_ms,
                     permit_safety_margin_ms, robots_cache_ms,
                     max_total_attempts, max_pending_requests
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (crawl_run_id) DO NOTHING",
                &[
                    &run_id,
                    &persisted.global_concurrency,
                    &persisted.origin_concurrency,
                    &persisted.min_start_spacing_ms,
                    &persisted.request_timeout_ms,
                    &persisted.permit_safety_margin_ms,
                    &persisted.robots_cache_ms,
                    &persisted.max_total_attempts,
                    &persisted.max_pending_requests,
                ],
            )
            .map_err(|error| Error::storage(format!("persist shared request policy: {error}")))?;
        let row = self
            .client
            .query_one(
                "SELECT global_concurrency, origin_concurrency,
                        min_start_spacing_ms, request_timeout_ms,
                        permit_safety_margin_ms, robots_cache_ms,
                        max_total_attempts, max_pending_requests
                 FROM scout_crawl_request_policy WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("read shared request policy: {error}")))?;
        let stored = PersistedRequestPolicy {
            global_concurrency: row.get(0),
            origin_concurrency: row.get(1),
            min_start_spacing_ms: row.get(2),
            request_timeout_ms: row.get(3),
            permit_safety_margin_ms: row.get(4),
            robots_cache_ms: row.get(5),
            max_total_attempts: row.get(6),
            max_pending_requests: row.get(7),
        };
        if stored != persisted {
            return Err(Error::invalid(format!(
                "crawl run {run_id:?} already has different shared request policy limits"
            )));
        }
        Ok(inserted)
    }

    /// Queue one outbound request and grant only when DB-wide limits and fairness permit it.
    /// Acquired marks the shared start time; caller must dispatch HTTP immediately.
    pub(crate) fn try_acquire_request_permit(
        &mut self,
        run_id: &str,
        worker_id: &str,
        request_id: &str,
        schedule_key: &str,
        origin: &str,
        request_kind: &str,
    ) -> Result<PermitDecision, Error> {
        // ponytail: per-run row lock keeps permit counts atomic; shard only if this becomes a throughput limit.
        validate_request_identity(worker_id, "worker_id")?;
        validate_request_identity(request_id, "request_id")?;
        validate_schedule_key(schedule_key)?;
        validate_request_identity(request_kind, "request_kind")?;
        let normalized_origin = normalize_origin(origin)?;
        if normalized_origin != origin {
            return Err(Error::invalid("request origin must already be normalized"));
        }
        let _measurement =
            crate::distributed::metrics::DatabaseScope::new("acquire_request_permit");
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin request permit transaction: {error}"))
        })?;
        let limits = tx
            .query_opt(
                "SELECT global_concurrency, origin_concurrency,
                        min_start_spacing_ms, request_timeout_ms,
                        permit_safety_margin_ms, max_total_attempts,
                        max_pending_requests, total_attempts, last_schedule_key
                 FROM scout_crawl_request_policy
                 WHERE crawl_run_id = $1 FOR UPDATE",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("lock shared request policy: {error}")))?
            .ok_or_else(|| Error::invalid(format!("crawl run {run_id:?} has no request policy")))?;
        let global_limit: i32 = limits.get(0);
        let origin_limit: i32 = limits.get(1);
        let global_limit_db = i64::from(global_limit);
        let origin_limit_db = i64::from(origin_limit);
        let spacing_ms: i64 = limits.get(2);
        let timeout_ms: i64 = limits.get(3);
        let safety_ms: i64 = limits.get(4);
        let max_attempts: i64 = limits.get(5);
        let max_pending: i32 = limits.get(6);
        let total_attempts: i64 = limits.get(7);
        let last_schedule_key: Option<String> = limits.get(8);
        let permit_lifetime_ms = timeout_ms
            .checked_add(safety_ms)
            .ok_or_else(|| Error::invalid("request permit lifetime is too large"))?;

        tx.execute(
            "UPDATE scout_crawl_request_permits
             SET status = 'expired', finished_at = clock_timestamp(), outcome = 'permit_timeout'
             WHERE crawl_run_id = $1 AND status = 'active'
               AND expires_at <= clock_timestamp()",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("reclaim expired request permits: {error}")))?;
        tx.execute(
            "DELETE FROM scout_crawl_request_permits
             WHERE crawl_run_id = $1 AND status = 'pending'
               AND pending_deadline <= clock_timestamp()",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("expire pending request permits: {error}")))?;

        let prior = tx
            .query_opt(
                "SELECT permit_id, worker_id, schedule_key, origin, request_kind, status,
                        (extract(epoch FROM expires_at) * 1000)::BIGINT AS expires_at_unix_ms
                 FROM scout_crawl_request_permits
                 WHERE crawl_run_id = $1 AND request_id = $2",
                &[&run_id, &request_id],
            )
            .map_err(|error| Error::storage(format!("read request permit: {error}")))?;
        if let Some(row) = prior {
            let metadata_matches = row.get::<_, String>(1) == worker_id
                && row.get::<_, String>(2) == schedule_key
                && row.get::<_, String>(3) == origin
                && row.get::<_, String>(4) == request_kind;
            if !metadata_matches {
                return Err(Error::invalid(
                    "request_id was reused with different permit metadata",
                ));
            }
            let permit_id: i64 = row.get(0);
            let status: String = row.get(5);
            match status.as_str() {
                "active" => {
                    let grant = request_permit_grant(
                        &mut tx,
                        run_id,
                        permit_id,
                        request_id,
                        origin,
                        row.get(6),
                    )?;
                    tx.commit().map_err(|error| {
                        Error::storage(format!("commit existing request permit: {error}"))
                    })?;
                    return Ok(PermitDecision::Acquired(grant));
                }
                "completed" => {
                    tx.commit().map_err(|error| {
                        Error::storage(format!("commit completed request lookup: {error}"))
                    })?;
                    return Ok(PermitDecision::AlreadyCompleted);
                }
                "expired" => {
                    return Err(Error::safety(
                        "request permit expired; do not resume HTTP request",
                    ));
                }
                "pending" => {
                    tx.execute(
                        "UPDATE scout_crawl_request_permits
                         SET pending_deadline = clock_timestamp() + ($3::BIGINT * interval '1 millisecond')
                         WHERE crawl_run_id = $1 AND request_id = $2 AND status = 'pending'",
                        &[&run_id, &request_id, &permit_lifetime_ms],
                    )
                    .map_err(|error| {
                        Error::storage(format!("renew pending request permit: {error}"))
                    })?;
                }
                "cancelled" => return Err(Error::safety("request permit was cancelled")),
                other => {
                    return Err(Error::storage(format!(
                        "unknown request permit status from PostgreSQL: {other}"
                    )));
                }
            }
        } else {
            let pending: i64 = tx
                .query_one(
                    "SELECT count(*) FROM scout_crawl_request_permits
                     WHERE crawl_run_id = $1 AND status = 'pending'",
                    &[&run_id],
                )
                .map_err(|error| Error::storage(format!("count pending request permits: {error}")))?
                .get(0);
            if pending >= i64::from(max_pending) {
                return Err(Error::safety("pending request limit exceeded"));
            }
            tx.execute(
                "INSERT INTO scout_crawl_request_permits(
                     crawl_run_id, request_id, worker_id, schedule_key, origin,
                     request_kind, status, pending_deadline
                 ) VALUES ($1, $2, $3, $4, $5, $6, 'pending',
                           clock_timestamp() + ($7::BIGINT * interval '1 millisecond'))",
                &[
                    &run_id,
                    &request_id,
                    &worker_id,
                    &schedule_key,
                    &origin,
                    &request_kind,
                    &permit_lifetime_ms,
                ],
            )
            .map_err(|error| Error::storage(format!("queue request permit: {error}")))?;
        }

        if total_attempts >= max_attempts {
            tx.execute(
                "DELETE FROM scout_crawl_request_permits
                 WHERE crawl_run_id = $1 AND request_id = $2 AND status = 'pending'",
                &[&run_id, &request_id],
            )
            .map_err(|error| {
                Error::storage(format!("remove request beyond attempt cap: {error}"))
            })?;
            tx.commit().map_err(|error| {
                Error::storage(format!("commit request attempt limit cleanup: {error}"))
            })?;
            return Err(Error::safety("request attempt limit exceeded"));
        }

        let candidate = tx
            .query_opt(
                "SELECT request.request_id, request.schedule_key, request.origin
                 FROM scout_crawl_request_permits AS request
                 WHERE request.crawl_run_id = $1 AND request.status = 'pending'
                   AND request.pending_deadline > clock_timestamp()
                   AND (SELECT count(*) FROM scout_crawl_request_permits AS active
                        WHERE active.crawl_run_id = $1 AND active.status = 'active') < $2::BIGINT
                   AND (SELECT count(*) FROM scout_crawl_request_permits AS active
                        WHERE active.crawl_run_id = $1 AND active.status = 'active'
                          AND active.origin = request.origin) < $3::BIGINT
                   AND NOT EXISTS (
                       SELECT 1 FROM scout_crawl_request_spacing AS spacing
                       WHERE spacing.crawl_run_id = $1
                         AND spacing.schedule_key = request.schedule_key
                         AND spacing.next_allowed_at > clock_timestamp()
                   )
                 ORDER BY CASE
                     WHEN $4::TEXT IS NULL OR request.schedule_key > $4 THEN 0 ELSE 1
                 END, request.schedule_key, request.queued_at, request.request_id
                 LIMIT 1",
                &[
                    &run_id,
                    &global_limit_db,
                    &origin_limit_db,
                    &last_schedule_key,
                ],
            )
            .map_err(|error| Error::storage(format!("select fair request permit: {error}")))?;
        let Some(candidate) = candidate else {
            tx.commit().map_err(|error| {
                Error::storage(format!("commit queued request permit: {error}"))
            })?;
            return Ok(PermitDecision::Waiting(Duration::from_millis(25)));
        };
        let selected_id: String = candidate.get(0);
        if selected_id != request_id {
            tx.commit().map_err(|error| {
                Error::storage(format!("commit waiting request permit: {error}"))
            })?;
            return Ok(PermitDecision::Waiting(Duration::from_millis(25)));
        }
        let selected_key: String = candidate.get(1);
        let selected_origin: String = candidate.get(2);
        let spacing_ms = spacing_ms.max(0);
        let active_origin: i64 = tx
            .query_one(
                "SELECT count(*) FROM scout_crawl_request_permits
                 WHERE crawl_run_id = $1 AND status = 'active' AND origin = $2",
                &[&run_id, &selected_origin],
            )
            .map_err(|error| Error::storage(format!("count active origin requests: {error}")))?
            .get(0);
        tx.execute(
            "UPDATE scout_crawl_request_permits
             SET status = 'active', pending_deadline = NULL, acquired_at = clock_timestamp(),
                 expires_at = clock_timestamp() + ($3::BIGINT * interval '1 millisecond')
             WHERE crawl_run_id = $1 AND request_id = $2 AND status = 'pending'",
            &[&run_id, &request_id, &permit_lifetime_ms],
        )
        .map_err(|error| Error::storage(format!("activate request permit: {error}")))?;
        tx.execute(
            "UPDATE scout_crawl_request_policy
             SET total_attempts = total_attempts + 1, last_schedule_key = $2
             WHERE crawl_run_id = $1",
            &[&run_id, &selected_key],
        )
        .map_err(|error| Error::storage(format!("advance request scheduler: {error}")))?;
        tx.execute(
            "INSERT INTO scout_crawl_request_spacing(crawl_run_id, schedule_key, next_allowed_at)
             VALUES ($1, $2, clock_timestamp() + ($3::BIGINT * interval '1 millisecond'))
             ON CONFLICT (crawl_run_id, schedule_key) DO UPDATE
             SET next_allowed_at = EXCLUDED.next_allowed_at",
            &[&run_id, &selected_key, &spacing_ms],
        )
        .map_err(|error| Error::storage(format!("persist request start spacing: {error}")))?;
        let permit_id: i64 = tx
            .query_one(
                "SELECT permit_id FROM scout_crawl_request_permits
                 WHERE crawl_run_id = $1 AND request_id = $2",
                &[&run_id, &request_id],
            )
            .map_err(|error| Error::storage(format!("read activated request permit: {error}")))?
            .get(0);
        let row = tx
            .query_one(
                "SELECT (extract(epoch FROM expires_at) * 1000)::BIGINT
                 FROM scout_crawl_request_permits WHERE permit_id = $1",
                &[&permit_id],
            )
            .map_err(|error| Error::storage(format!("read request permit expiry: {error}")))?;
        let grant = request_permit_grant(
            &mut tx,
            run_id,
            permit_id,
            request_id,
            &selected_origin,
            Some(row.get(0)),
        )?;
        debug_assert!(active_origin < i64::from(origin_limit));
        tx.commit()
            .map_err(|error| Error::storage(format!("commit request permit: {error}")))?;
        Ok(PermitDecision::Acquired(grant))
    }

    /// RPC waiters vanish on coordinator restart. Never clear active HTTP permits here.
    pub(crate) fn clear_pending_request_waits(&mut self, run_id: &str) -> Result<(), Error> {
        self.client.execute("DELETE FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND status='pending'", &[&run_id])
            .map_err(|error| Error::storage(format!("clear disconnected request waits: {error}")))?;
        Ok(())
    }

    /// Queue lifetime follows the renewable task lease, rather than requiring request polling.
    pub(crate) fn bind_request_waits_to_lease(
        &mut self,
        task: &super::ClaimedTask,
        request_ids: &[String],
    ) -> Result<(), Error> {
        if request_ids.is_empty() {
            return Ok(());
        }
        self.client.execute(
            "UPDATE scout_crawl_request_permits AS request SET pending_deadline=task.lease_deadline
             FROM scout_crawl_tasks AS task WHERE request.crawl_run_id=$1 AND request.request_id=ANY($2::TEXT[])
               AND request.status='pending' AND task.task_id=$3 AND task.crawl_run_id=$1
               AND task.status='leased' AND task.lease_owner=$4 AND task.lease_epoch=$5",
            &[&task.crawl_run_id, &request_ids, &task.task_id, &task.lease_owner, &task.lease_epoch])
            .map_err(|error| Error::storage(format!("bind queued requests to task lease: {error}")))?;
        Ok(())
    }

    pub(crate) fn expired_request_waits(
        &mut self,
        run_id: &str,
        request_ids: &[String],
    ) -> Result<Vec<String>, Error> {
        self.client.query("SELECT ids.id FROM unnest($2::TEXT[]) AS ids(id) LEFT JOIN scout_crawl_request_permits AS request ON request.crawl_run_id=$1 AND request.request_id=ids.id WHERE request.request_id IS NULL OR (request.status='pending' AND request.pending_deadline<=clock_timestamp())", &[&run_id, &request_ids])
            .map(|rows| rows.into_iter().map(|row| row.get(0)).collect())
            .map_err(|error| Error::storage(format!("read expired request waits: {error}")))
    }

    /// One eligible request, using exactly the durable round-robin ordering of permit grants.
    pub(crate) fn ready_request_id(&mut self, run_id: &str) -> Result<Option<String>, Error> {
        self.client.query_opt(
            "SELECT request.request_id FROM scout_crawl_request_permits AS request
             JOIN scout_crawl_request_policy AS policy USING(crawl_run_id)
             WHERE request.crawl_run_id=$1 AND request.status='pending' AND request.pending_deadline>clock_timestamp()
               AND (policy.total_attempts>=policy.max_total_attempts OR (
                 (SELECT count(*) FROM scout_crawl_request_permits AS active WHERE active.crawl_run_id=$1 AND active.status='active' AND active.expires_at>clock_timestamp())<policy.global_concurrency
                 AND (SELECT count(*) FROM scout_crawl_request_permits AS active WHERE active.crawl_run_id=$1 AND active.status='active' AND active.origin=request.origin AND active.expires_at>clock_timestamp())<policy.origin_concurrency
                 AND NOT EXISTS (SELECT 1 FROM scout_crawl_request_spacing AS spacing WHERE spacing.crawl_run_id=$1 AND spacing.schedule_key=request.schedule_key AND spacing.next_allowed_at>clock_timestamp())))
             ORDER BY CASE WHEN policy.last_schedule_key IS NULL OR request.schedule_key>policy.last_schedule_key THEN 0 ELSE 1 END,
                      request.schedule_key, request.queued_at, request.request_id LIMIT 1",
            &[&run_id])
            .map(|row| row.map(|row| row.get(0)))
            .map_err(|error| Error::storage(format!("select queued request notification: {error}")))
    }

    pub(crate) fn dispatch_clock_ms(&mut self) -> Result<i64, Error> {
        self.client
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::BIGINT",
                &[],
            )
            .map(|row| row.get(0))
            .map_err(|error| Error::storage(format!("read dispatcher clock: {error}")))
    }

    /// Deadline-driven dispatcher wakeup. Events handle earlier completions and new work.
    pub(crate) fn next_dispatch_delay(
        &mut self,
        run_id: &str,
        claims: bool,
        permits: bool,
        excluded_sources: &[String],
        max_duration_seconds: u64,
        reference_unix_ms: i64,
    ) -> Result<Option<Duration>, Error> {
        let duration = i64::try_from(max_duration_seconds)
            .map_err(|_| Error::invalid("crawl duration too large"))?;
        self.client.query_one(
            "SELECT ceil(extract(epoch FROM (min(due)-clock_timestamp()))*1000)::BIGINT FROM (
               SELECT retry_at AS due FROM scout_crawl_tasks WHERE $2 AND crawl_run_id=$1 AND status='pending' AND retry_at>to_timestamp($6::BIGINT/1000.0) AND NOT(source_id=ANY($4::TEXT[]))
               UNION ALL SELECT lease_deadline FROM scout_crawl_tasks WHERE $2 AND crawl_run_id=$1 AND status='leased' AND lease_deadline>to_timestamp($6::BIGINT/1000.0)
               UNION ALL SELECT expires_at FROM scout_crawl_request_permits WHERE $3 AND crawl_run_id=$1 AND status='active' AND expires_at>to_timestamp($6::BIGINT/1000.0)
               UNION ALL SELECT pending_deadline FROM scout_crawl_request_permits WHERE $3 AND crawl_run_id=$1 AND status='pending'
               UNION ALL SELECT spacing.next_allowed_at FROM scout_crawl_request_spacing AS spacing WHERE $3 AND spacing.crawl_run_id=$1 AND spacing.next_allowed_at>to_timestamp($6::BIGINT/1000.0) AND EXISTS(SELECT 1 FROM scout_crawl_request_permits AS request WHERE request.crawl_run_id=$1 AND request.status='pending' AND request.schedule_key=spacing.schedule_key)
               UNION ALL SELECT created_at+($5::BIGINT*interval '1 second') FROM scout_crawl_runs WHERE $2 AND crawl_run_id=$1
             ) deadlines", &[&run_id, &claims, &permits, &excluded_sources, &duration, &reference_unix_ms])
            .map(|row| row.get::<_, Option<i64>>(0).map(|millis| Duration::from_millis(millis.max(1) as u64)))
            .map_err(|error| Error::storage(format!("read scheduling deadline: {error}")))
    }

    /// Remove a queued request after worker cancellation; active permits require report/recovery.
    pub(crate) fn cancel_request_wait(
        &mut self,
        run_id: &str,
        worker_id: &str,
        request_id: &str,
    ) -> Result<bool, Error> {
        let _measurement = crate::distributed::metrics::DatabaseScope::new("cancel_request_wait");
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin pending request cancellation: {error}"))
        })?;
        tx.query_opt(
            "SELECT crawl_run_id FROM scout_crawl_request_policy
             WHERE crawl_run_id = $1 FOR UPDATE",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("lock shared request policy: {error}")))?
        .ok_or_else(|| Error::invalid(format!("crawl run {run_id:?} has no request policy")))?;
        let changed = tx
            .execute(
                "DELETE FROM scout_crawl_request_permits
                 WHERE crawl_run_id = $1 AND worker_id = $2 AND request_id = $3
                   AND status = 'pending'",
                &[&run_id, &worker_id, &request_id],
            )
            .map_err(|error| Error::storage(format!("cancel queued request permit: {error}")))?;
        tx.commit().map_err(|error| {
            Error::storage(format!("commit pending request cancellation: {error}"))
        })?;
        Ok(changed == 1)
    }

    /// Report request result after response body handling; permits remain active until this call.
    pub(crate) fn release_request_permit(
        &mut self,
        run_id: &str,
        worker_id: &str,
        permit_id: i64,
        outcome: &str,
    ) -> Result<bool, Error> {
        validate_request_identity(outcome, "request outcome")?;
        let _measurement =
            crate::distributed::metrics::DatabaseScope::new("release_request_permit");
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin request release transaction: {error}"))
        })?;
        tx.query_opt(
            "SELECT crawl_run_id FROM scout_crawl_request_policy
             WHERE crawl_run_id = $1 FOR UPDATE",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("lock shared request policy: {error}")))?
        .ok_or_else(|| Error::invalid(format!("crawl run {run_id:?} has no request policy")))?;
        tx.execute(
            "UPDATE scout_crawl_request_permits
             SET status = 'expired', finished_at = clock_timestamp(), outcome = 'permit_timeout'
             WHERE crawl_run_id = $1 AND status = 'active'
               AND expires_at <= clock_timestamp()",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("reclaim expired request permits: {error}")))?;
        let changed = tx
            .execute(
                "UPDATE scout_crawl_request_permits
                 SET status = 'completed', finished_at = clock_timestamp(), outcome = $4
                 WHERE crawl_run_id = $1 AND worker_id = $2 AND permit_id = $3
                   AND status = 'active' AND expires_at > clock_timestamp()",
                &[&run_id, &worker_id, &permit_id, &outcome],
            )
            .map_err(|error| Error::storage(format!("release request permit: {error}")))?;
        if changed == 1 {
            tx.commit()
                .map_err(|error| Error::storage(format!("commit request release: {error}")))?;
            return Ok(true);
        }
        let already_reported = tx
            .query_opt(
                "SELECT outcome FROM scout_crawl_request_permits
                 WHERE crawl_run_id = $1 AND worker_id = $2 AND permit_id = $3
                   AND status = 'completed'",
                &[&run_id, &worker_id, &permit_id],
            )
            .map_err(|error| Error::storage(format!("read released request permit: {error}")))?
            .is_some_and(|row| row.get::<_, Option<String>>(0).as_deref() == Some(outcome));
        tx.commit().map_err(|error| {
            Error::storage(format!("commit duplicate request release: {error}"))
        })?;
        Ok(already_reported)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct PersistedRequestPolicy {
    global_concurrency: i32,
    origin_concurrency: i32,
    min_start_spacing_ms: i64,
    request_timeout_ms: i64,
    permit_safety_margin_ms: i64,
    robots_cache_ms: i64,
    max_total_attempts: i64,
    max_pending_requests: i32,
}

fn request_policy_values(limits: RequestPolicyLimits) -> Result<PersistedRequestPolicy, Error> {
    let spacing_ms = duration_millis_allow_zero(limits.min_start_spacing)?;
    let timeout_ms = duration_millis(limits.request_timeout)?;
    let safety_ms = duration_millis(limits.permit_safety_margin)?;
    let cache_ms = duration_millis(limits.robots_cache_ttl)?;
    let max_attempts = i64::try_from(limits.max_total_attempts)
        .map_err(|_| Error::invalid("max_total_attempts is too large"))?;
    let max_pending = i32::try_from(limits.max_pending_requests)
        .map_err(|_| Error::invalid("max_pending_requests is too large"))?;
    if limits.global_concurrency == 0
        || limits.origin_concurrency == 0
        || max_attempts <= 0
        || max_pending <= 0
    {
        return Err(Error::invalid("shared request limits must be positive"));
    }
    Ok(PersistedRequestPolicy {
        global_concurrency: i32::try_from(limits.global_concurrency)
            .map_err(|_| Error::invalid("global_concurrency is too large"))?,
        origin_concurrency: i32::try_from(limits.origin_concurrency)
            .map_err(|_| Error::invalid("origin_concurrency is too large"))?,
        min_start_spacing_ms: spacing_ms,
        request_timeout_ms: timeout_ms,
        permit_safety_margin_ms: safety_ms,
        robots_cache_ms: cache_ms,
        max_total_attempts: max_attempts,
        max_pending_requests: max_pending,
    })
}

fn duration_millis_allow_zero(duration: Duration) -> Result<i64, Error> {
    let millis = duration.as_nanos().div_ceil(1_000_000);
    i64::try_from(millis)
        .map_err(|_| Error::invalid("duration is too large for PostgreSQL policy state"))
}

fn request_permit_grant(
    tx: &mut postgres::Transaction<'_>,
    run_id: &str,
    permit_id: i64,
    request_id: &str,
    origin: &str,
    expires_at_unix_ms: Option<i64>,
) -> Result<RequestPermitGrant, Error> {
    let expires_at_unix_ms =
        expires_at_unix_ms.ok_or_else(|| Error::storage("active request permit has no expiry"))?;
    let active_global: i64 = tx
        .query_one(
            "SELECT count(*) FROM scout_crawl_request_permits
             WHERE crawl_run_id = $1 AND status = 'active'",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("count active requests: {error}")))?
        .get(0);
    let active_origin: i64 = tx
        .query_one(
            "SELECT count(*) FROM scout_crawl_request_permits
             WHERE crawl_run_id = $1 AND status = 'active' AND origin = $2",
            &[&run_id, &origin],
        )
        .map_err(|error| Error::storage(format!("count active origin requests: {error}")))?
        .get(0);
    Ok(RequestPermitGrant {
        permit_id,
        request_id: request_id.into(),
        expires_at_unix_ms,
        active_global: usize::try_from(active_global)
            .map_err(|_| Error::storage("active request count does not fit memory"))?,
        active_origin: usize::try_from(active_origin)
            .map_err(|_| Error::storage("active origin request count does not fit memory"))?,
    })
}
