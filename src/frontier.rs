use super::storage::sha256_hex;
use super::url::{normalize_origin, normalize_url, origin_key};
use super::{Error, Url};
use postgres::{Client, NoTls};
use std::time::Duration;

const MIGRATION_V1: &str = include_str!("../migrations/0001_durable_crawl_frontier.sql");
const MIGRATION_V2: &str = include_str!("../migrations/0002_shared_request_policy.sql");
const MIGRATION_V3: &str = include_str!("../migrations/0003_fair_frontier_schedule.sql");
const MIGRATION_LOCK: i64 = 0x5343_4f55_545f_4652;
const REQUEST_MIGRATION: [(i32, &str, &str); 3] = [
    (1, "durable-crawl-frontier", MIGRATION_V1),
    (2, "shared-request-policy", MIGRATION_V2),
    (3, "fair-frontier-schedule", MIGRATION_V3),
];

const CLAIM_ONE_TASK: &str = r#"
WITH scheduler AS MATERIALIZED (
    SELECT crawl_run_id, last_schedule_key
    FROM scout_crawl_task_scheduler
    WHERE crawl_run_id = $1
    FOR UPDATE
), exhausted AS (
    UPDATE scout_crawl_tasks
    SET status = 'failed',
        lease_owner = NULL,
        lease_deadline = NULL,
        last_error = COALESCE(last_error, 'lease expired after final task attempt'),
        updated_at = clock_timestamp()
    WHERE crawl_run_id = $1
      AND status = 'leased'
      AND lease_deadline <= clock_timestamp()
      AND task_attempt_count >= max_task_attempts
    RETURNING task_id
), candidate AS (
    SELECT task.task_id, task.schedule_key
    FROM scout_crawl_tasks AS task
    CROSS JOIN scheduler
    WHERE task.crawl_run_id = $1
      AND task.task_attempt_count < task.max_task_attempts
      AND (
          (task.status = 'pending' AND task.retry_at <= clock_timestamp())
          OR (task.status = 'leased' AND task.lease_deadline <= clock_timestamp())
      )
    ORDER BY CASE
                 WHEN scheduler.last_schedule_key IS NULL
                   OR task.schedule_key > scheduler.last_schedule_key THEN 0 ELSE 1
             END,
             task.schedule_key, task.retry_at, task.task_id
    LIMIT 1
    FOR UPDATE OF task SKIP LOCKED
), claimed AS (
    UPDATE scout_crawl_tasks AS task
    SET status = 'leased',
        task_attempt_count = task.task_attempt_count + 1,
        lease_owner = $2,
        lease_deadline = clock_timestamp() + ($3::BIGINT * interval '1 millisecond'),
        lease_epoch = task.lease_epoch + 1,
        updated_at = clock_timestamp()
    FROM candidate
    WHERE task.task_id = candidate.task_id
    RETURNING task.task_id, task.crawl_run_id, task.source_id, task.origin,
              task.normalized_url, task.schedule_key, task.task_attempt_count,
              task.lease_owner, task.lease_epoch, task.lease_deadline, task.idempotency_key
), advanced AS (
    UPDATE scout_crawl_task_scheduler AS cursor
    SET last_schedule_key = claimed.schedule_key
    FROM claimed
    WHERE cursor.crawl_run_id = claimed.crawl_run_id
    RETURNING cursor.crawl_run_id
)
SELECT claimed.task_id, claimed.crawl_run_id, claimed.source_id, claimed.origin,
       claimed.normalized_url, claimed.schedule_key, claimed.task_attempt_count,
       claimed.lease_owner, claimed.lease_epoch,
       (extract(epoch FROM claimed.lease_deadline) * 1000)::BIGINT AS lease_deadline_unix_ms,
       claimed.idempotency_key
FROM claimed
JOIN advanced USING (crawl_run_id)
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskStatus {
    Pending,
    Leased,
    Succeeded,
    Failed,
}

impl TaskStatus {
    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            other => Err(Error::storage(format!(
                "unknown crawl task status from PostgreSQL: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaimedTask {
    pub(crate) task_id: i64,
    pub(crate) crawl_run_id: String,
    pub(crate) source_id: String,
    pub(crate) origin: String,
    pub(crate) normalized_url: String,
    pub(crate) schedule_key: String,
    pub(crate) task_attempt_count: i32,
    pub(crate) lease_owner: String,
    pub(crate) lease_epoch: i64,
    pub(crate) lease_deadline_unix_ms: i64,
    pub(crate) idempotency_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaseTiming {
    pub(crate) lease_for: Duration,
    pub(crate) renew_after: Duration,
}

impl LeaseTiming {
    pub(crate) fn new(
        request_timeout: Duration,
        lease_for: Duration,
        renew_after: Duration,
    ) -> Result<Self, Error> {
        if request_timeout.is_zero()
            || lease_for <= request_timeout
            || renew_after.is_zero()
            || renew_after >= lease_for
        {
            return Err(Error::invalid(
                "task lease must exceed request timeout and renewal interval must be shorter than the lease",
            ));
        }
        duration_millis(lease_for)?;
        duration_millis(renew_after)?;
        Ok(Self {
            lease_for,
            renew_after,
        })
    }

    pub(crate) fn for_request_timeout(request_timeout: Duration) -> Result<Self, Error> {
        let lease_for = request_timeout
            .checked_mul(2)
            .ok_or_else(|| Error::invalid("request timeout is too large for a task lease"))?;
        Self::new(request_timeout, lease_for, lease_for / 3)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestPolicyLimits {
    pub(crate) global_concurrency: usize,
    pub(crate) origin_concurrency: usize,
    pub(crate) min_start_spacing: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) permit_safety_margin: Duration,
    pub(crate) robots_cache_ttl: Duration,
    pub(crate) max_total_attempts: usize,
    pub(crate) max_pending_requests: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RobotsPolicy {
    pub(crate) groups: Vec<RobotsGroup>,
    pub(crate) sitemaps: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RobotsGroup {
    pub(crate) agents: Vec<String>,
    pub(crate) rules: Vec<RobotsRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RobotsRule {
    pub(crate) allow: bool,
    pub(crate) path: String,
}

impl RobotsPolicy {
    pub(crate) fn allows(&self, path: &str) -> bool {
        let has_specific_group = self
            .groups
            .iter()
            .any(|group| group.agents.iter().any(|agent| agent == "scout"));
        let mut rules = self
            .groups
            .iter()
            .filter(|group| {
                group.agents.iter().any(|agent| {
                    if has_specific_group {
                        agent == "scout"
                    } else {
                        agent == "*"
                    }
                })
            })
            .flat_map(|group| group.rules.iter())
            .filter(|rule| !rule.path.is_empty() && path.starts_with(&rule.path))
            .collect::<Vec<_>>();
        rules.sort_by(|left, right| {
            right
                .path
                .len()
                .cmp(&left.path.len())
                .then_with(|| right.allow.cmp(&left.allow))
        });
        rules.first().map(|rule| rule.allow).unwrap_or(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestPermitGrant {
    pub(crate) permit_id: i64,
    pub(crate) request_id: String,
    pub(crate) expires_at_unix_ms: i64,
    pub(crate) active_global: usize,
    pub(crate) active_origin: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PermitDecision {
    Acquired(RequestPermitGrant),
    Waiting(Duration),
    AlreadyCompleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RobotsDecision {
    Cached {
        allowed: bool,
        body_sha256: Option<String>,
        sitemaps: Vec<String>,
    },
    Refresh {
        epoch: i64,
    },
    Waiting(Duration),
}

pub(crate) struct FrontierStore {
    client: Client,
}

impl FrontierStore {
    pub(crate) fn connect(database_url: &str) -> Result<Self, Error> {
        let client = Client::connect(database_url, NoTls)
            .map_err(|error| Error::storage(format!("connect coordinator PostgreSQL: {error}")))?;
        Ok(Self { client })
    }

    pub(crate) fn apply_migrations(&mut self) -> Result<(), Error> {
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin frontier migration: {error}")))?;
        tx.query_one("SELECT pg_advisory_xact_lock($1)", &[&MIGRATION_LOCK])
            .map_err(|error| Error::storage(format!("lock frontier migrations: {error}")))?;
        tx.batch_execute(
            "CREATE TABLE IF NOT EXISTS scout_schema_migrations (
                 version INTEGER PRIMARY KEY,
                 name TEXT NOT NULL,
                 checksum TEXT NOT NULL,
                 applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
             )",
        )
        .map_err(|error| Error::storage(format!("create migration ledger: {error}")))?;

        let applied = tx
            .query("SELECT version, checksum FROM scout_schema_migrations", &[])
            .map_err(|error| Error::storage(format!("read migration ledger: {error}")))?;
        for row in &applied {
            let version: i32 = row.get(0);
            if version > REQUEST_MIGRATION.last().expect("migration list nonempty").0 {
                return Err(Error::storage(format!(
                    "database frontier schema version {version} is newer than this binary"
                )));
            }
        }
        for (version, name, migration) in REQUEST_MIGRATION {
            let checksum = sha256_hex(migration.as_bytes());
            if let Some(row) = applied.iter().find(|row| row.get::<_, i32>(0) == version) {
                if row.get::<_, String>(1) != checksum {
                    return Err(Error::storage(format!(
                        "frontier migration {version} checksum differs from the applied migration"
                    )));
                }
                continue;
            }
            tx.batch_execute(migration).map_err(|error| {
                Error::storage(format!("apply frontier migration {version}: {error}"))
            })?;
            tx.execute(
                "INSERT INTO scout_schema_migrations(version, name, checksum)
                 VALUES ($1, $2, $3)",
                &[&version, &name, &checksum],
            )
            .map_err(|error| {
                Error::storage(format!("record frontier migration {version}: {error}"))
            })?;
        }
        tx.commit()
            .map_err(|error| Error::storage(format!("commit frontier migration: {error}")))
    }

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

    /// Remove a queued request after worker cancellation; active permits require report/recovery.
    pub(crate) fn cancel_request_wait(
        &mut self,
        run_id: &str,
        worker_id: &str,
        request_id: &str,
    ) -> Result<bool, Error> {
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

    /// Return a fresh shared decision or claim the one allowed robots refresh for this origin.
    pub(crate) fn robots_decision(
        &mut self,
        run_id: &str,
        worker_id: &str,
        refresh_epoch: Option<i64>,
        origin: &str,
        path: &str,
    ) -> Result<RobotsDecision, Error> {
        validate_request_identity(worker_id, "worker_id")?;
        let normalized_origin = normalize_origin(origin)?;
        if normalized_origin != origin || !path.starts_with('/') {
            return Err(Error::invalid("robots origin or path is not normalized"));
        }
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin robots policy transaction: {error}")))?;
        let settings = tx
            .query_opt(
                "SELECT request_timeout_ms, permit_safety_margin_ms
                 FROM scout_crawl_request_policy WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("read robots policy settings: {error}")))?
            .ok_or_else(|| Error::invalid(format!("crawl run {run_id:?} has no request policy")))?;
        let lease_ms: i64 = settings
            .get::<_, i64>(0)
            .checked_add(settings.get::<_, i64>(1))
            .ok_or_else(|| Error::invalid("robots refresh lease is too large"))?;
        let inserted = tx
            .execute(
                "INSERT INTO scout_crawl_robots_policy(
                 crawl_run_id, origin, state, policy, refresh_owner, refresh_epoch, refresh_deadline
             ) VALUES ($1, $2, 'refreshing', '{\"groups\":[],\"sitemaps\":[]}', $3, 1,
                       clock_timestamp() + ($4::BIGINT * interval '1 millisecond'))
             ON CONFLICT (crawl_run_id, origin) DO NOTHING",
                &[&run_id, &origin, &worker_id, &lease_ms],
            )
            .map_err(|error| Error::storage(format!("create robots refresh state: {error}")))?
            == 1;
        if inserted {
            tx.commit()
                .map_err(|error| Error::storage(format!("commit robots refresh claim: {error}")))?;
            return Ok(RobotsDecision::Refresh { epoch: 1 });
        }
        let row = tx
            .query_one(
                "SELECT state, policy, body_sha256, refresh_owner, refresh_epoch,
                        (refresh_deadline > clock_timestamp()) AS refresh_live,
                        (expires_at > clock_timestamp()) AS cache_fresh
                 FROM scout_crawl_robots_policy
                 WHERE crawl_run_id = $1 AND origin = $2 FOR UPDATE",
                &[&run_id, &origin],
            )
            .map_err(|error| Error::storage(format!("lock robots policy state: {error}")))?;
        let state: String = row.get(0);
        match state.as_str() {
            "ready" | "denied" if row.get::<_, bool>(6) => {
                let allowed = if state == "denied" {
                    false
                } else {
                    let serialized: String = row.get(1);
                    let policy: RobotsPolicy =
                        serde_json::from_str(&serialized).map_err(|error| {
                            Error::storage(format!("decode shared robots policy: {error}"))
                        })?;
                    policy.allows(path)
                };
                let sitemaps = if state == "ready" {
                    let serialized: String = row.get(1);
                    let policy: RobotsPolicy =
                        serde_json::from_str(&serialized).map_err(|error| {
                            Error::storage(format!("decode shared robots policy: {error}"))
                        })?;
                    policy.sitemaps
                } else {
                    Vec::new()
                };
                let decision = RobotsDecision::Cached {
                    allowed,
                    body_sha256: row.get(2),
                    sitemaps,
                };
                tx.commit()
                    .map_err(|error| Error::storage(format!("commit robots decision: {error}")))?;
                Ok(decision)
            }
            "refreshing" if row.get::<_, bool>(5) => {
                let owner: String = row.get(3);
                let epoch: i64 = row.get(4);
                if owner == worker_id && refresh_epoch == Some(epoch) {
                    tx.execute(
                        "UPDATE scout_crawl_robots_policy
                         SET refresh_deadline = clock_timestamp() + ($4::BIGINT * interval '1 millisecond')
                         WHERE crawl_run_id = $1 AND origin = $2 AND refresh_epoch = $3",
                        &[&run_id, &origin, &epoch, &lease_ms],
                    )
                    .map_err(|error| Error::storage(format!("renew robots refresh claim: {error}")))?;
                    tx.commit().map_err(|error| {
                        Error::storage(format!("commit robots refresh claim: {error}"))
                    })?;
                    Ok(RobotsDecision::Refresh { epoch })
                } else {
                    tx.commit().map_err(|error| {
                        Error::storage(format!("commit robots refresh wait: {error}"))
                    })?;
                    Ok(RobotsDecision::Waiting(Duration::from_millis(25)))
                }
            }
            "ready" | "denied" | "refreshing" => {
                let epoch: i64 = row.get(4);
                let next_epoch = epoch
                    .checked_add(1)
                    .ok_or_else(|| Error::storage("robots refresh epoch exhausted"))?;
                tx.execute(
                    "UPDATE scout_crawl_robots_policy
                     SET state = 'refreshing', refresh_owner = $3, refresh_epoch = $4,
                         refresh_deadline = clock_timestamp() + ($5::BIGINT * interval '1 millisecond'),
                         fetched_at = NULL, expires_at = NULL
                     WHERE crawl_run_id = $1 AND origin = $2",
                    &[&run_id, &origin, &worker_id, &next_epoch, &lease_ms],
                )
                .map_err(|error| Error::storage(format!("claim robots refresh: {error}")))?;
                tx.commit().map_err(|error| {
                    Error::storage(format!("commit robots refresh claim: {error}"))
                })?;
                Ok(RobotsDecision::Refresh { epoch: next_epoch })
            }
            other => Err(Error::storage(format!(
                "unknown robots policy state from PostgreSQL: {other}"
            ))),
        }
    }

    pub(crate) fn commit_robots_refresh(
        &mut self,
        run_id: &str,
        worker_id: &str,
        origin: &str,
        epoch: i64,
        policy: &RobotsPolicy,
        body_sha256: &str,
    ) -> Result<bool, Error> {
        let normalized_origin = normalize_origin(origin)?;
        if normalized_origin != origin || epoch <= 0 {
            return Err(Error::invalid("robots refresh identity is invalid"));
        }
        validate_digest(body_sha256, "robots body digest")?;
        let mut policy = policy.clone();
        policy.sitemaps = policy
            .sitemaps
            .into_iter()
            .map(|url| {
                let parsed = Url::parse(&url)
                    .map_err(|error| Error::invalid(format!("invalid sitemap URL: {error}")))?;
                normalize_url(parsed).map_err(Error::invalid)
            })
            .collect::<Result<Vec<_>, _>>()?;
        policy.sitemaps.sort();
        policy.sitemaps.dedup();
        let serialized = serde_json::to_string(&policy)
            .map_err(|error| Error::storage(format!("encode shared robots policy: {error}")))?;
        let changed = self
            .client
            .execute(
                "UPDATE scout_crawl_robots_policy AS robots
                 SET state = 'ready', policy = $5, body_sha256 = $6,
                     fetched_at = clock_timestamp(),
                     expires_at = clock_timestamp() + (settings.robots_cache_ms * interval '1 millisecond'),
                     refresh_owner = NULL, refresh_deadline = NULL
                 FROM scout_crawl_request_policy AS settings
                 WHERE robots.crawl_run_id = $1 AND robots.origin = $2
                   AND robots.refresh_owner = $3 AND robots.refresh_epoch = $4
                   AND robots.state = 'refreshing' AND robots.refresh_deadline > clock_timestamp()
                   AND settings.crawl_run_id = robots.crawl_run_id",
                &[&run_id, &origin, &worker_id, &epoch, &serialized, &body_sha256],
            )
            .map_err(|error| Error::storage(format!("commit shared robots policy: {error}")))?;
        Ok(changed == 1)
    }

    pub(crate) fn fail_robots_refresh(
        &mut self,
        run_id: &str,
        worker_id: &str,
        origin: &str,
        epoch: i64,
    ) -> Result<bool, Error> {
        let normalized_origin = normalize_origin(origin)?;
        if normalized_origin != origin || epoch <= 0 {
            return Err(Error::invalid("robots refresh identity is invalid"));
        }
        let changed = self
            .client
            .execute(
                "UPDATE scout_crawl_robots_policy AS robots
                 SET state = 'denied', policy = '{\"groups\":[],\"sitemaps\":[]}',
                     body_sha256 = NULL, fetched_at = clock_timestamp(),
                     expires_at = clock_timestamp() + (settings.robots_cache_ms * interval '1 millisecond'),
                     refresh_owner = NULL, refresh_deadline = NULL
                 FROM scout_crawl_request_policy AS settings
                 WHERE robots.crawl_run_id = $1 AND robots.origin = $2
                   AND robots.refresh_owner = $3 AND robots.refresh_epoch = $4
                   AND robots.state = 'refreshing' AND robots.refresh_deadline > clock_timestamp()
                   AND settings.crawl_run_id = robots.crawl_run_id",
                &[&run_id, &origin, &worker_id, &epoch],
            )
            .map_err(|error| Error::storage(format!("fail shared robots refresh: {error}")))?;
        Ok(changed == 1)
    }

    /// Insert a run once; an existing ID is resumable only with the original digest.
    pub(crate) fn ensure_run(&mut self, run_id: &str, config_digest: &str) -> Result<bool, Error> {
        validate_digest(config_digest, "crawl config digest")?;
        if run_id.trim().is_empty() {
            return Err(Error::invalid("crawl_run_id must not be empty"));
        }
        let inserted = self
            .client
            .execute(
                "INSERT INTO scout_crawl_runs(crawl_run_id, config_digest)
                 VALUES ($1, $2)
                 ON CONFLICT (crawl_run_id) DO NOTHING",
                &[&run_id, &config_digest],
            )
            .map_err(|error| Error::storage(format!("persist crawl run identity: {error}")))?
            > 0;
        let stored_digest: String = self
            .client
            .query_one(
                "SELECT config_digest FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("read crawl run identity: {error}")))?
            .get(0);
        if stored_digest != config_digest {
            return Err(Error::invalid(format!(
                "crawl run {run_id:?} has config digest {stored_digest}; refusing resume with {config_digest}"
            )));
        }
        self.client
            .execute(
                "INSERT INTO scout_crawl_task_scheduler(crawl_run_id)
                 VALUES ($1) ON CONFLICT (crawl_run_id) DO NOTHING",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("initialize task schedule cursor: {error}")))?;
        Ok(inserted)
    }

    /// Keep the configured crawl duration anchored to the persisted run creation time,
    /// including when a coordinator resumes after a restart.
    pub(crate) fn run_duration_exceeded(
        &mut self,
        run_id: &str,
        max_duration: Duration,
    ) -> Result<bool, Error> {
        let max_seconds = i64::try_from(max_duration.as_secs())
            .map_err(|_| Error::invalid("max crawl duration is too large"))?;
        self.client
            .query_opt(
                "SELECT created_at + ($2::BIGINT * interval '1 second') <= clock_timestamp()
                 FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[&run_id, &max_seconds],
            )
            .map_err(|error| Error::storage(format!("check crawl duration: {error}")))?
            .map(|row| row.get(0))
            .ok_or_else(|| Error::storage(format!("crawl run {run_id:?} does not exist")))
    }

    /// Add one logical URL task. Re-enqueueing the same run/source/URL is harmless.
    pub(crate) fn enqueue_url(
        &mut self,
        run_id: &str,
        source_id: &str,
        normalized_url: &str,
        max_task_attempts: i32,
    ) -> Result<bool, Error> {
        self.enqueue_url_with_schedule(run_id, source_id, normalized_url, max_task_attempts, None)
    }

    pub(crate) fn enqueue_scheduled_url(
        &mut self,
        run_id: &str,
        source_id: &str,
        normalized_url: &str,
        max_task_attempts: i32,
        schedule_key: &str,
    ) -> Result<bool, Error> {
        self.enqueue_url_with_schedule(
            run_id,
            source_id,
            normalized_url,
            max_task_attempts,
            Some(schedule_key),
        )
    }

    fn enqueue_url_with_schedule(
        &mut self,
        run_id: &str,
        source_id: &str,
        normalized_url: &str,
        max_task_attempts: i32,
        schedule_key: Option<&str>,
    ) -> Result<bool, Error> {
        if source_id.trim().is_empty() || max_task_attempts <= 0 {
            return Err(Error::invalid(
                "source_id and max_task_attempts must be positive",
            ));
        }
        let parsed = Url::parse(normalized_url)
            .map_err(|error| Error::invalid(format!("invalid frontier URL: {error}")))?;
        let normalized = normalize_url(parsed.clone()).map_err(Error::invalid)?;
        if normalized != normalized_url {
            return Err(Error::invalid("frontier URL must already be normalized"));
        }
        let origin = origin_key(&parsed);
        let schedule_key = schedule_key.unwrap_or(&origin);
        validate_schedule_key(schedule_key)?;
        let idempotency_key = sha256_hex(
            format!("scout.crawl-task.v1\0{run_id}\0{source_id}\0{normalized_url}").as_bytes(),
        );
        self.client
            .execute(
                "INSERT INTO scout_crawl_tasks(
                     crawl_run_id, source_id, origin, normalized_url, schedule_key,
                     max_task_attempts, idempotency_key
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (crawl_run_id, source_id, normalized_url) DO NOTHING",
                &[
                    &run_id,
                    &source_id,
                    &origin,
                    &normalized_url,
                    &schedule_key,
                    &max_task_attempts,
                    &idempotency_key,
                ],
            )
            .map(|count| count > 0)
            .map_err(|error| Error::storage(format!("enqueue crawl URL: {error}")))
    }

    /// Claim exactly one due task; row locking coordinates independent PostgreSQL clients.
    pub(crate) fn claim_one(
        &mut self,
        run_id: &str,
        worker_id: &str,
        timing: LeaseTiming,
    ) -> Result<Option<ClaimedTask>, Error> {
        if worker_id.trim().is_empty() {
            return Err(Error::invalid("worker_id must not be empty"));
        }
        let lease_ms = duration_millis(timing.lease_for)?;
        self.client
            .query_opt(CLAIM_ONE_TASK, &[&run_id, &worker_id, &lease_ms])
            .map(|row| row.map(claimed_task))
            .map_err(|error| Error::storage(format!("claim crawl task: {error}")))
    }

    pub(crate) fn renew_lease(
        &mut self,
        task: &ClaimedTask,
        timing: LeaseTiming,
    ) -> Result<bool, Error> {
        let lease_ms = duration_millis(timing.lease_for)?;
        self.client
            .execute(
                "UPDATE scout_crawl_tasks
                 SET lease_deadline = clock_timestamp() + ($4::BIGINT * interval '1 millisecond'),
                     updated_at = clock_timestamp()
                 WHERE task_id = $1 AND status = 'leased'
                   AND lease_owner = $2 AND lease_epoch = $3
                   AND lease_deadline > clock_timestamp()
                   AND crawl_run_id = $5 AND source_id = $6 AND origin = $7
                   AND normalized_url = $8 AND schedule_key = $9 AND idempotency_key = $10",
                &[
                    &task.task_id,
                    &task.lease_owner,
                    &task.lease_epoch,
                    &lease_ms,
                    &task.crawl_run_id,
                    &task.source_id,
                    &task.origin,
                    &task.normalized_url,
                    &task.schedule_key,
                    &task.idempotency_key,
                ],
            )
            .map(|count| count == 1)
            .map_err(|error| Error::storage(format!("renew crawl task lease: {error}")))
    }

    pub(crate) fn complete_task(&mut self, task: &ClaimedTask) -> Result<bool, Error> {
        self.client
            .execute(
                "UPDATE scout_crawl_tasks
                 SET status = 'succeeded', lease_owner = NULL, lease_deadline = NULL,
                     updated_at = clock_timestamp()
                 WHERE task_id = $1 AND status = 'leased'
                   AND lease_owner = $2 AND lease_epoch = $3
                   AND lease_deadline > clock_timestamp()
                   AND crawl_run_id = $4 AND source_id = $5 AND origin = $6
                   AND normalized_url = $7 AND schedule_key = $8 AND idempotency_key = $9",
                &[
                    &task.task_id,
                    &task.lease_owner,
                    &task.lease_epoch,
                    &task.crawl_run_id,
                    &task.source_id,
                    &task.origin,
                    &task.normalized_url,
                    &task.schedule_key,
                    &task.idempotency_key,
                ],
            )
            .map(|count| count == 1)
            .map_err(|error| Error::storage(format!("complete crawl task: {error}")))
    }

    /// Persist a bounded task retry or terminal failure. HTTP retry counts stay separate.
    pub(crate) fn fail_task(
        &mut self,
        task: &ClaimedTask,
        error_message: &str,
        retry_delay: Duration,
    ) -> Result<Option<TaskStatus>, Error> {
        let delay_ms = duration_millis(retry_delay)?;
        let row = self
            .client
            .query_opt(
                "UPDATE scout_crawl_tasks
                 SET status = CASE
                         WHEN task_attempt_count >= max_task_attempts THEN 'failed'
                         ELSE 'pending'
                     END,
                     retry_at = CASE
                         WHEN task_attempt_count >= max_task_attempts THEN retry_at
                         ELSE clock_timestamp() + ($4::BIGINT * interval '1 millisecond')
                     END,
                     lease_owner = NULL,
                     lease_deadline = NULL,
                     last_error = $5,
                     updated_at = clock_timestamp()
                 WHERE task_id = $1 AND status = 'leased'
                   AND lease_owner = $2 AND lease_epoch = $3
                   AND lease_deadline > clock_timestamp()
                   AND crawl_run_id = $6 AND source_id = $7 AND origin = $8
                   AND normalized_url = $9 AND schedule_key = $10 AND idempotency_key = $11
                 RETURNING status",
                &[
                    &task.task_id,
                    &task.lease_owner,
                    &task.lease_epoch,
                    &delay_ms,
                    &error_message,
                    &task.crawl_run_id,
                    &task.source_id,
                    &task.origin,
                    &task.normalized_url,
                    &task.schedule_key,
                    &task.idempotency_key,
                ],
            )
            .map_err(|error| Error::storage(format!("record crawl task failure: {error}")))?;
        row.map(|row| TaskStatus::parse(row.get::<_, &str>(0)))
            .transpose()
    }

    pub(crate) fn lease_is_live(&mut self, task: &ClaimedTask) -> Result<bool, Error> {
        self.client
            .query_opt(
                "SELECT task_id FROM scout_crawl_tasks
                 WHERE task_id = $1 AND crawl_run_id = $2 AND source_id = $3 AND origin = $4
                   AND normalized_url = $5 AND schedule_key = $6 AND idempotency_key = $7
                   AND status = 'leased' AND lease_owner = $8 AND lease_epoch = $9
                   AND lease_deadline > clock_timestamp()",
                &[
                    &task.task_id,
                    &task.crawl_run_id,
                    &task.source_id,
                    &task.origin,
                    &task.normalized_url,
                    &task.schedule_key,
                    &task.idempotency_key,
                    &task.lease_owner,
                    &task.lease_epoch,
                ],
            )
            .map(|row| row.is_some())
            .map_err(|error| Error::storage(format!("validate crawl task lease: {error}")))
    }

    pub(crate) fn unfinished_task_count(&mut self, run_id: &str) -> Result<usize, Error> {
        let count: i64 = self
            .client
            .query_one(
                "SELECT count(*) FROM scout_crawl_tasks
                 WHERE crawl_run_id = $1 AND status IN ('pending', 'leased')",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("count unfinished crawl tasks: {error}")))?
            .get(0);
        usize::try_from(count)
            .map_err(|_| Error::storage("unfinished crawl task count does not fit memory"))
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

fn validate_request_identity(value: &str, label: &str) -> Result<(), Error> {
    if value.trim().is_empty()
        || value.len() > 256
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Error::invalid(format!(
            "{label} must be nonempty ASCII text of at most 256 bytes"
        )));
    }
    Ok(())
}

fn validate_schedule_key(value: &str) -> Result<(), Error> {
    if value.trim().is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
        return Err(Error::invalid(
            "schedule_key must be nonempty text of at most 1024 bytes",
        ));
    }
    Ok(())
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

fn claimed_task(row: postgres::Row) -> ClaimedTask {
    ClaimedTask {
        task_id: row.get("task_id"),
        crawl_run_id: row.get("crawl_run_id"),
        source_id: row.get("source_id"),
        origin: row.get("origin"),
        normalized_url: row.get("normalized_url"),
        schedule_key: row.get("schedule_key"),
        task_attempt_count: row.get("task_attempt_count"),
        lease_owner: row.get("lease_owner"),
        lease_epoch: row.get("lease_epoch"),
        lease_deadline_unix_ms: row.get("lease_deadline_unix_ms"),
        idempotency_key: row.get("idempotency_key"),
    }
}

fn validate_digest(value: &str, name: &str) -> Result<(), Error> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(Error::invalid(format!(
            "{name} must be a lowercase SHA-256 digest"
        )));
    }
    Ok(())
}

fn duration_millis(duration: Duration) -> Result<i64, Error> {
    let millis = duration.as_nanos().div_ceil(1_000_000);
    if millis == 0 {
        return Err(Error::invalid(
            "task lease and retry durations must be at least 1 ms",
        ));
    }
    i64::try_from(millis).map_err(|_| Error::invalid("task lease or retry duration is too large"))
}

#[cfg(test)]
mod tests {
    use super::{
        FrontierStore, LeaseTiming, PermitDecision, RequestPermitGrant, RequestPolicyLimits,
        RobotsDecision, RobotsGroup, RobotsPolicy, RobotsRule, TaskStatus,
    };
    use crate::storage::sha256_hex;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    struct RunCleanup {
        database_url: String,
        run_id: String,
    }

    impl Drop for RunCleanup {
        fn drop(&mut self) {
            if let Ok(mut client) = postgres::Client::connect(&self.database_url, postgres::NoTls) {
                let _ = client.execute(
                    "DELETE FROM scout_crawl_runs WHERE crawl_run_id = $1",
                    &[&self.run_id],
                );
            }
        }
    }

    fn policy_limits(
        global_concurrency: usize,
        origin_concurrency: usize,
        min_start_spacing: Duration,
        request_timeout: Duration,
        permit_safety_margin: Duration,
    ) -> RequestPolicyLimits {
        RequestPolicyLimits {
            global_concurrency,
            origin_concurrency,
            min_start_spacing,
            request_timeout,
            permit_safety_margin,
            robots_cache_ttl: Duration::from_secs(2),
            max_total_attempts: 64,
            max_pending_requests: 32,
        }
    }

    fn setup_policy_run(
        database_url: &str,
        limits: RequestPolicyLimits,
        suffix: &str,
    ) -> (String, RunCleanup, FrontierStore) {
        let run_id = format!(
            "policy-test-{suffix}-{}",
            sha256_hex(format!("{suffix}-{:?}", Instant::now()).as_bytes())
        );
        let cleanup = RunCleanup {
            database_url: database_url.into(),
            run_id: run_id.clone(),
        };
        let mut store = FrontierStore::connect(database_url).unwrap();
        store.apply_migrations().unwrap();
        let digest = sha256_hex(run_id.as_bytes());
        assert!(store.configure_run(&run_id, &digest, limits).unwrap());
        assert!(!store.configure_run(&run_id, &digest, limits).unwrap());
        (run_id, cleanup, store)
    }

    fn acquired(decision: PermitDecision) -> RequestPermitGrant {
        match decision {
            PermitDecision::Acquired(grant) => grant,
            other => panic!("expected request permit, got {other:?}"),
        }
    }

    fn wait_for_permit(
        store: &mut FrontierStore,
        run_id: &str,
        worker_id: &str,
        request_id: &str,
        schedule_key: &str,
        origin: &str,
        request_kind: &str,
    ) -> RequestPermitGrant {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match store
                .try_acquire_request_permit(
                    run_id,
                    worker_id,
                    request_id,
                    schedule_key,
                    origin,
                    request_kind,
                )
                .unwrap()
            {
                PermitDecision::Acquired(grant) => return grant,
                PermitDecision::Waiting(delay) if Instant::now() < deadline => thread::sleep(delay),
                other => panic!("request permit did not become available: {other:?}"),
            }
        }
    }

    #[test]
    fn default_lease_exceeds_the_http_timeout_and_renews_before_expiry() {
        let timing = LeaseTiming::for_request_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(timing.lease_for, Duration::from_secs(60));
        assert_eq!(timing.renew_after, Duration::from_secs(20));
        assert!(
            LeaseTiming::new(
                Duration::from_secs(30),
                Duration::from_secs(30),
                Duration::from_secs(10)
            )
            .is_err()
        );
        assert!(
            LeaseTiming::new(
                Duration::from_secs(30),
                Duration::from_secs(60),
                Duration::from_secs(60)
            )
            .is_err()
        );
    }

    // Run with SCOUT_TEST_DATABASE_URL set and `cargo test --lib frontier::tests::postgres_frontier_contract -- --ignored`.
    #[test]
    #[ignore = "requires a coordinator-only PostgreSQL test database"]
    fn postgres_frontier_contract() {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
        let run_id = format!(
            "frontier-test-{}",
            sha256_hex(format!("{:?}", std::time::SystemTime::now()).as_bytes())
        );
        let config_digest = sha256_hex(run_id.as_bytes());
        let default_timing = LeaseTiming::for_request_timeout(Duration::from_secs(30)).unwrap();
        let mut store = FrontierStore::connect(&database_url).unwrap();
        store.apply_migrations().unwrap();
        assert!(store.ensure_run(&run_id, &config_digest).unwrap());
        assert!(!store.ensure_run(&run_id, &config_digest).unwrap());
        assert!(store.ensure_run(&run_id, &sha256_hex(b"changed")).is_err());
        assert!(
            !store
                .run_duration_exceeded(&run_id, Duration::from_secs(60))
                .unwrap()
        );
        store
            .client
            .execute(
                "UPDATE scout_crawl_runs SET created_at = clock_timestamp() - interval '2 seconds'
                 WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .unwrap();
        assert!(
            store
                .run_duration_exceeded(&run_id, Duration::from_secs(1))
                .unwrap()
        );

        assert!(
            store
                .enqueue_url(&run_id, "docs", "https://example.com/one", 2)
                .unwrap()
        );
        assert!(
            !store
                .enqueue_url(&run_id, "docs", "https://example.com/one", 2)
                .unwrap()
        );
        let initial = store
            .claim_one(&run_id, "setup-worker", default_timing)
            .unwrap()
            .unwrap();
        assert!(store.complete_task(&initial).unwrap());
        assert_eq!(initial.schedule_key, "https://example.com");
        assert!(
            store
                .enqueue_url(&run_id, "docs", "https://example.com/concurrent", 3)
                .unwrap()
        );

        let barrier = Arc::new(Barrier::new(3));
        let handles = (0..2)
            .map(|index| {
                let database_url = database_url.clone();
                let run_id = run_id.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    let mut store = FrontierStore::connect(&database_url).unwrap();
                    store.apply_migrations().unwrap();
                    barrier.wait();
                    store
                        .claim_one(&run_id, &format!("claimer-{index}"), default_timing)
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let claims = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let concurrent_claims = claims
            .into_iter()
            .flatten()
            .filter(|claim| claim.normalized_url.ends_with("/concurrent"))
            .collect::<Vec<_>>();
        assert_eq!(concurrent_claims.len(), 1);
        let current = concurrent_claims.into_iter().next().unwrap();
        assert!(store.renew_lease(&current, default_timing).unwrap());
        assert!(store.complete_task(&current).unwrap());
        assert!(!store.complete_task(&current).unwrap());

        assert!(
            store
                .enqueue_url(&run_id, "docs", "https://example.com/expiry", 3)
                .unwrap()
        );
        let short_timing = LeaseTiming::new(
            Duration::from_millis(10),
            Duration::from_millis(120),
            Duration::from_millis(40),
        )
        .unwrap();
        let expired = store
            .claim_one(&run_id, "worker-old", short_timing)
            .unwrap()
            .unwrap();
        thread::sleep(Duration::from_millis(180));
        assert!(!store.renew_lease(&expired, short_timing).unwrap());
        assert!(!store.complete_task(&expired).unwrap());
        let reclaimed = store
            .claim_one(&run_id, "worker-new", default_timing)
            .unwrap()
            .unwrap();
        assert_eq!(reclaimed.lease_epoch, expired.lease_epoch + 1);
        assert!(!store.complete_task(&expired).unwrap());
        assert!(store.complete_task(&reclaimed).unwrap());

        assert!(
            store
                .enqueue_url(&run_id, "docs", "https://example.com/retry", 2)
                .unwrap()
        );
        let first = store
            .claim_one(&run_id, "retry-worker", default_timing)
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .fail_task(&first, "first failure", Duration::from_millis(1))
                .unwrap(),
            Some(TaskStatus::Pending)
        );
        thread::sleep(Duration::from_millis(10));
        let second = store
            .claim_one(&run_id, "retry-worker", default_timing)
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .fail_task(&second, "final failure", Duration::from_millis(1))
                .unwrap(),
            Some(TaskStatus::Failed)
        );
        thread::sleep(Duration::from_millis(10));
        assert!(
            store
                .claim_one(&run_id, "retry-worker", default_timing)
                .unwrap()
                .is_none()
        );

        store
            .client
            .execute(
                "DELETE FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .unwrap();
    }

    #[test]
    fn robots_policy_prefers_longest_matching_path_and_allow_on_tie() {
        let policy = RobotsPolicy {
            groups: vec![RobotsGroup {
                agents: vec!["*".into()],
                rules: vec![
                    RobotsRule {
                        allow: false,
                        path: "/private".into(),
                    },
                    RobotsRule {
                        allow: true,
                        path: "/private/open".into(),
                    },
                    RobotsRule {
                        allow: false,
                        path: "/same".into(),
                    },
                    RobotsRule {
                        allow: true,
                        path: "/same".into(),
                    },
                ],
            }],
            sitemaps: Vec::new(),
        };
        assert!(!policy.allows("/private/page"));
        assert!(policy.allows("/private/open/page"));
        assert!(policy.allows("/same/page"));
    }

    // Run with SCOUT_TEST_DATABASE_URL set and `cargo test --lib frontier::tests::postgres_shared_request_policy_contract -- --ignored`.
    #[test]
    #[ignore = "requires a coordinator-only PostgreSQL test database"]
    fn postgres_shared_request_policy_contract() {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
        let limits = policy_limits(
            2,
            1,
            Duration::from_millis(100),
            Duration::from_millis(120),
            Duration::from_millis(150),
        );
        let (run_id, _cleanup, mut store) = setup_policy_run(&database_url, limits, "limits");
        let changed_limits = RequestPolicyLimits {
            min_start_spacing: Duration::from_millis(101),
            ..limits
        };
        assert!(
            store
                .configure_run(&run_id, &sha256_hex(run_id.as_bytes()), changed_limits)
                .is_err()
        );

        let first = acquired(
            store
                .try_acquire_request_permit(
                    &run_id,
                    "worker-a",
                    "page-a",
                    "group:a",
                    "https://a.example",
                    "page",
                )
                .unwrap(),
        );
        assert_eq!(first.active_global, 1);
        assert_eq!(first.active_origin, 1);
        let second = acquired(
            store
                .try_acquire_request_permit(
                    &run_id,
                    "worker-b",
                    "page-b",
                    "group:b",
                    "https://b.example",
                    "page",
                )
                .unwrap(),
        );
        assert_eq!(second.active_global, 2);
        assert_eq!(
            store
                .try_acquire_request_permit(
                    &run_id,
                    "worker-c",
                    "same-origin",
                    "group:c",
                    "https://a.example",
                    "redirect",
                )
                .unwrap(),
            PermitDecision::Waiting(Duration::from_millis(25))
        );
        assert!(matches!(
            store
                .try_acquire_request_permit(
                    &run_id,
                    "worker-d",
                    "global-limit",
                    "group:d",
                    "https://d.example",
                    "retry",
                )
                .unwrap(),
            PermitDecision::Waiting(_)
        ));
        assert!(
            store
                .release_request_permit(&run_id, "worker-a", first.permit_id, "success")
                .unwrap()
        );
        assert!(
            store
                .release_request_permit(&run_id, "worker-a", first.permit_id, "success")
                .unwrap()
        );
        assert!(
            !store
                .release_request_permit(&run_id, "worker-a", first.permit_id, "failure")
                .unwrap()
        );
        let same_origin = wait_for_permit(
            &mut store,
            &run_id,
            "worker-c",
            "same-origin",
            "group:c",
            "https://a.example",
            "redirect",
        );
        assert!(
            store
                .release_request_permit(&run_id, "worker-b", second.permit_id, "success")
                .unwrap()
        );
        let global_slot = wait_for_permit(
            &mut store,
            &run_id,
            "worker-d",
            "global-limit",
            "group:d",
            "https://d.example",
            "retry",
        );
        for (grant, worker) in [(&same_origin, "worker-c"), (&global_slot, "worker-d")] {
            assert!(
                store
                    .release_request_permit(&run_id, worker, grant.permit_id, "success")
                    .unwrap()
            );
        }

        for (index, kind) in ["robots", "sitemap", "page", "redirect", "retry"]
            .into_iter()
            .enumerate()
        {
            let request_id = format!("kind-{kind}-{index}");
            let origin = format!("https://kind-{index}.example");
            let grant = acquired(
                store
                    .try_acquire_request_permit(
                        &run_id,
                        "kind-worker",
                        &request_id,
                        &format!("group:kind-{index}"),
                        &origin,
                        kind,
                    )
                    .unwrap(),
            );
            assert!(
                store
                    .release_request_permit(&run_id, "kind-worker", grant.permit_id, "success")
                    .unwrap()
            );
        }

        let barrier = Arc::new(Barrier::new(9));
        let handles = (0..8)
            .map(|index| {
                let database_url = database_url.clone();
                let run_id = run_id.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    let mut independent = FrontierStore::connect(&database_url).unwrap();
                    barrier.wait();
                    let worker = format!("racer-{index}");
                    let request = format!("raced-{index}");
                    let key = format!("group:racer-{index}");
                    let origin = format!("https://racer-{index}.example");
                    let decision = independent
                        .try_acquire_request_permit(
                            &run_id, &worker, &request, &key, &origin, "page",
                        )
                        .unwrap();
                    (index, worker, request, decision)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let raced = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            raced
                .iter()
                .filter(|(_, _, _, decision)| matches!(decision, PermitDecision::Acquired(_)))
                .count(),
            2
        );
        for (_, worker, request, decision) in raced {
            match decision {
                PermitDecision::Acquired(grant) => assert!(
                    store
                        .release_request_permit(&run_id, &worker, grant.permit_id, "success")
                        .unwrap()
                ),
                PermitDecision::Waiting(_) => assert!(
                    store
                        .cancel_request_wait(&run_id, &worker, &request)
                        .unwrap()
                ),
                other => panic!("unexpected concurrent permit result: {other:?}"),
            }
        }

        let cap_limits = RequestPolicyLimits {
            max_total_attempts: 1,
            ..policy_limits(
                1,
                1,
                Duration::ZERO,
                Duration::from_millis(200),
                Duration::from_millis(100),
            )
        };
        let (cap_run, _cap_cleanup, mut cap_store) =
            setup_policy_run(&database_url, cap_limits, "attempt-cap");
        let only_attempt = acquired(
            cap_store
                .try_acquire_request_permit(
                    &cap_run,
                    "cap-worker",
                    "only-attempt",
                    "cap-key",
                    "https://cap.example",
                    "page",
                )
                .unwrap(),
        );
        assert!(
            cap_store
                .release_request_permit(&cap_run, "cap-worker", only_attempt.permit_id, "success")
                .unwrap()
        );
        assert!(
            cap_store
                .try_acquire_request_permit(
                    &cap_run,
                    "cap-worker",
                    "over-cap",
                    "cap-key-2",
                    "https://cap-2.example",
                    "retry",
                )
                .is_err()
        );
        let pending_after_cap: i64 = cap_store
            .client
            .query_one(
                "SELECT count(*) FROM scout_crawl_request_permits
                 WHERE crawl_run_id = $1 AND status = 'pending'",
                &[&cap_run],
            )
            .unwrap()
            .get(0);
        assert_eq!(pending_after_cap, 0);

        let rr_limits = policy_limits(
            1,
            1,
            Duration::ZERO,
            Duration::from_millis(200),
            Duration::from_millis(100),
        );
        let (rr_run, _rr_cleanup, mut rr_store) =
            setup_policy_run(&database_url, rr_limits, "round-robin");
        let blocker = acquired(
            rr_store
                .try_acquire_request_permit(
                    &rr_run,
                    "rr-worker",
                    "blocker",
                    "a",
                    "https://blocker.example",
                    "page",
                )
                .unwrap(),
        );
        assert!(matches!(
            rr_store
                .try_acquire_request_permit(
                    &rr_run,
                    "rr-worker-a",
                    "queued-a",
                    "a",
                    "https://queued-a.example",
                    "page",
                )
                .unwrap(),
            PermitDecision::Waiting(_)
        ));
        assert!(matches!(
            rr_store
                .try_acquire_request_permit(
                    &rr_run,
                    "rr-worker-b",
                    "queued-b",
                    "b",
                    "https://queued-b.example",
                    "page",
                )
                .unwrap(),
            PermitDecision::Waiting(_)
        ));
        assert!(
            rr_store
                .release_request_permit(&rr_run, "rr-worker", blocker.permit_id, "success")
                .unwrap()
        );
        let fair_first = acquired(
            rr_store
                .try_acquire_request_permit(
                    &rr_run,
                    "rr-worker-b",
                    "queued-b",
                    "b",
                    "https://queued-b.example",
                    "page",
                )
                .unwrap(),
        );
        assert!(matches!(
            rr_store
                .try_acquire_request_permit(
                    &rr_run,
                    "rr-worker-a",
                    "queued-a",
                    "a",
                    "https://queued-a.example",
                    "page",
                )
                .unwrap(),
            PermitDecision::Waiting(_)
        ));
        assert!(
            rr_store
                .release_request_permit(&rr_run, "rr-worker-b", fair_first.permit_id, "success")
                .unwrap()
        );
        let fair_second = acquired(
            rr_store
                .try_acquire_request_permit(
                    &rr_run,
                    "rr-worker-a",
                    "queued-a",
                    "a",
                    "https://queued-a.example",
                    "page",
                )
                .unwrap(),
        );
        assert!(
            rr_store
                .release_request_permit(&rr_run, "rr-worker-a", fair_second.permit_id, "success")
                .unwrap()
        );

        for (url, key) in [
            ("https://tasks.example/a-first", "a"),
            ("https://tasks.example/a-second", "a"),
            ("https://tasks.example/b-first", "b"),
        ] {
            assert!(
                rr_store
                    .enqueue_scheduled_url(&rr_run, "docs", url, 2, key)
                    .unwrap()
            );
        }
        let task_timing = LeaseTiming::new(
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_millis(250),
        )
        .unwrap();
        let task_a_first = rr_store
            .claim_one(&rr_run, "task-worker-a", task_timing)
            .unwrap()
            .unwrap();
        assert!(task_a_first.normalized_url.ends_with("/a-first"));
        assert_eq!(task_a_first.schedule_key, "a");
        assert!(rr_store.complete_task(&task_a_first).unwrap());
        let task_b_first = rr_store
            .claim_one(&rr_run, "task-worker-b", task_timing)
            .unwrap()
            .unwrap();
        assert!(task_b_first.normalized_url.ends_with("/b-first"));
        assert_eq!(task_b_first.schedule_key, "b");
        assert!(rr_store.complete_task(&task_b_first).unwrap());
        let task_a_second = rr_store
            .claim_one(&rr_run, "task-worker-a", task_timing)
            .unwrap()
            .unwrap();
        assert!(task_a_second.normalized_url.ends_with("/a-second"));
        assert!(rr_store.complete_task(&task_a_second).unwrap());

        let spacing_limits = policy_limits(
            1,
            1,
            Duration::from_millis(160),
            Duration::from_millis(500),
            Duration::from_millis(100),
        );
        let (spacing_run, _spacing_cleanup, mut spacing_store) =
            setup_policy_run(&database_url, spacing_limits, "spacing-restart");
        let spaced_first = acquired(
            spacing_store
                .try_acquire_request_permit(
                    &spacing_run,
                    "spacing-worker",
                    "spacing-first",
                    "shared-key",
                    "https://spacing-a.example",
                    "page",
                )
                .unwrap(),
        );
        assert!(
            spacing_store
                .release_request_permit(
                    &spacing_run,
                    "spacing-worker",
                    spaced_first.permit_id,
                    "success"
                )
                .unwrap()
        );
        drop(spacing_store);
        thread::sleep(Duration::from_millis(20));
        let mut restarted = FrontierStore::connect(&database_url).unwrap();
        let spacing_started = Instant::now();
        assert!(matches!(
            restarted
                .try_acquire_request_permit(
                    &spacing_run,
                    "spacing-worker-2",
                    "spacing-second",
                    "shared-key",
                    "https://spacing-b.example",
                    "redirect",
                )
                .unwrap(),
            PermitDecision::Waiting(_)
        ));
        let spaced_second = wait_for_permit(
            &mut restarted,
            &spacing_run,
            "spacing-worker-2",
            "spacing-second",
            "shared-key",
            "https://spacing-b.example",
            "redirect",
        );
        assert!(spacing_started.elapsed() >= Duration::from_millis(120));
        assert!(
            restarted
                .release_request_permit(
                    &spacing_run,
                    "spacing-worker-2",
                    spaced_second.permit_id,
                    "success"
                )
                .unwrap()
        );

        let robots_limits = policy_limits(
            2,
            1,
            Duration::ZERO,
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let (robots_run, _robots_cleanup, mut robots_store) =
            setup_policy_run(&database_url, robots_limits, "robots-restart");
        let robots_origin = "https://robots.example";
        let refresh_epoch = match robots_store
            .robots_decision(
                &robots_run,
                "robots-worker-a",
                None,
                robots_origin,
                "/private",
            )
            .unwrap()
        {
            RobotsDecision::Refresh { epoch } => epoch,
            other => panic!("expected robots refresh claim, got {other:?}"),
        };
        assert!(matches!(
            robots_store
                .robots_decision(
                    &robots_run,
                    "robots-worker-b",
                    None,
                    robots_origin,
                    "/private"
                )
                .unwrap(),
            RobotsDecision::Waiting(_)
        ));
        let robots_policy = RobotsPolicy {
            groups: vec![RobotsGroup {
                agents: vec!["*".into()],
                rules: vec![RobotsRule {
                    allow: false,
                    path: "/private".into(),
                }],
            }],
            sitemaps: vec!["https://robots.example/sitemap.xml".into()],
        };
        assert!(
            robots_store
                .commit_robots_refresh(
                    &robots_run,
                    "robots-worker-a",
                    robots_origin,
                    refresh_epoch,
                    &robots_policy,
                    &sha256_hex(b"robots"),
                )
                .unwrap()
        );
        drop(robots_store);
        let mut robots_restarted = FrontierStore::connect(&database_url).unwrap();
        assert_eq!(
            robots_restarted
                .robots_decision(
                    &robots_run,
                    "robots-worker-b",
                    None,
                    robots_origin,
                    "/private/page",
                )
                .unwrap(),
            RobotsDecision::Cached {
                allowed: false,
                body_sha256: Some(sha256_hex(b"robots")),
                sitemaps: vec!["https://robots.example/sitemap.xml".into()],
            }
        );
        assert!(matches!(
            robots_restarted
                .robots_decision(
                    &robots_run,
                    "robots-worker-c",
                    None,
                    robots_origin,
                    "/public",
                )
                .unwrap(),
            RobotsDecision::Cached { allowed: true, .. }
        ));
        let fail_epoch = match robots_restarted
            .robots_decision(
                &robots_run,
                "robots-worker-fail",
                None,
                "https://robots-fail.example",
                "/",
            )
            .unwrap()
        {
            RobotsDecision::Refresh { epoch } => epoch,
            other => panic!("expected robots failure claim, got {other:?}"),
        };
        assert!(
            robots_restarted
                .fail_robots_refresh(
                    &robots_run,
                    "robots-worker-fail",
                    "https://robots-fail.example",
                    fail_epoch,
                )
                .unwrap()
        );
        assert!(matches!(
            robots_restarted
                .robots_decision(
                    &robots_run,
                    "robots-worker-c",
                    None,
                    "https://robots-fail.example",
                    "/allowed-looking-path",
                )
                .unwrap(),
            RobotsDecision::Cached { allowed: false, .. }
        ));

        let death_limits = policy_limits(
            1,
            1,
            Duration::ZERO,
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let (death_run, _death_cleanup, mut dying_store) =
            setup_policy_run(&database_url, death_limits, "worker-death");
        let abandoned = acquired(
            dying_store
                .try_acquire_request_permit(
                    &death_run,
                    "dead-worker",
                    "abandoned-request",
                    "death-key",
                    "https://death.example",
                    "page",
                )
                .unwrap(),
        );
        drop(dying_store);
        thread::sleep(Duration::from_millis(80));
        let mut death_restarted = FrontierStore::connect(&database_url).unwrap();
        assert!(matches!(
            death_restarted
                .try_acquire_request_permit(
                    &death_run,
                    "replacement-worker",
                    "replacement-request",
                    "death-key",
                    "https://death.example",
                    "retry",
                )
                .unwrap(),
            PermitDecision::Waiting(_)
        ));
        drop(death_restarted);
        thread::sleep(Duration::from_millis(150));
        let mut death_restarted = FrontierStore::connect(&database_url).unwrap();
        let replacement = acquired(
            death_restarted
                .try_acquire_request_permit(
                    &death_run,
                    "replacement-worker",
                    "replacement-request",
                    "death-key",
                    "https://death.example",
                    "retry",
                )
                .unwrap(),
        );
        assert!(
            !death_restarted
                .release_request_permit(&death_run, "dead-worker", abandoned.permit_id, "success")
                .unwrap()
        );
        assert!(
            death_restarted
                .release_request_permit(
                    &death_run,
                    "replacement-worker",
                    replacement.permit_id,
                    "success"
                )
                .unwrap()
        );

        let dead_robots_epoch = match death_restarted
            .robots_decision(
                &death_run,
                "dead-robots-worker",
                None,
                "https://dead-robots.example",
                "/",
            )
            .unwrap()
        {
            RobotsDecision::Refresh { epoch } => epoch,
            other => panic!("expected robots refresh claim, got {other:?}"),
        };
        drop(death_restarted);
        thread::sleep(Duration::from_millis(230));
        let mut replacement_store = FrontierStore::connect(&database_url).unwrap();
        let replacement_epoch = match replacement_store
            .robots_decision(
                &death_run,
                "replacement-robots-worker",
                None,
                "https://dead-robots.example",
                "/",
            )
            .unwrap()
        {
            RobotsDecision::Refresh { epoch } => epoch,
            other => panic!("expected reclaimed robots refresh, got {other:?}"),
        };
        assert_eq!(replacement_epoch, dead_robots_epoch + 1);
        assert!(
            !replacement_store
                .commit_robots_refresh(
                    &death_run,
                    "dead-robots-worker",
                    "https://dead-robots.example",
                    dead_robots_epoch,
                    &RobotsPolicy::default(),
                    &sha256_hex(b"stale robots"),
                )
                .unwrap()
        );
        assert!(
            replacement_store
                .commit_robots_refresh(
                    &death_run,
                    "replacement-robots-worker",
                    "https://dead-robots.example",
                    replacement_epoch,
                    &RobotsPolicy::default(),
                    &sha256_hex(b"fresh robots"),
                )
                .unwrap()
        );
    }
}
