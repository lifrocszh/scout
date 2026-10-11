use crate::error::Error;
use crate::storage::sha256_hex;
use postgres::{Client, NoTls};
use std::time::Duration;

const MIGRATION_V1: &str = include_str!("../../migrations/0001_durable_crawl_frontier.sql");
const MIGRATION_V2: &str = include_str!("../../migrations/0002_shared_request_policy.sql");
const MIGRATION_V3: &str = include_str!("../../migrations/0003_fair_frontier_schedule.sql");
const MIGRATION_V4: &str = include_str!("../../migrations/0004_durable_crawl_results.sql");
const MIGRATION_V5: &str = include_str!("../../migrations/0005_shared_sitemap_discovery.sql");
const MIGRATION_V6: &str = include_str!("../../migrations/0006_origin_request_policy.sql");
const MIGRATION_LOCK: i64 = 0x5343_4f55_545f_4652;
const REQUEST_MIGRATION: [(i32, &str, &str); 6] = [
    (1, "durable-crawl-frontier", MIGRATION_V1),
    (2, "shared-request-policy", MIGRATION_V2),
    (3, "fair-frontier-schedule", MIGRATION_V3),
    (4, "durable-crawl-results", MIGRATION_V4),
    (5, "shared-sitemap-discovery", MIGRATION_V5),
    (6, "origin-request-policy", MIGRATION_V6),
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
      AND NOT (task.source_id = ANY($4::TEXT[]))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredTask {
    pub(crate) source_id: String,
    pub(crate) normalized_url: String,
    pub(crate) schedule_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptedCapture {
    pub(crate) source_id: String,
    pub(crate) content_hash: String,
    pub(crate) identity: String,
    pub(crate) body_hash: String,
    pub(crate) body_size: i64,
    pub(crate) metadata_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptedCaptureRecord {
    pub(crate) task_id: i64,
    pub(crate) metadata_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionAcceptance {
    Accepted {
        accepted_discovered_count: usize,
        rejected_discovered_count: usize,
    },
    Duplicate {
        accepted_discovered_count: usize,
        rejected_discovered_count: usize,
    },
    Stale,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OriginPolicyLimit {
    pub(crate) origin: String,
    pub(crate) schedule_key: String,
    pub(crate) origin_concurrency: usize,
    pub(crate) min_start_spacing_ms: u64,
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

mod request_policy;
mod results;
mod robots;
#[cfg(test)]
mod sitemap_tests;
mod sitemaps;
mod tasks;

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
        AcceptedCapture, FrontierStore, LeaseTiming, OriginPolicyLimit, PermitDecision,
        RequestPermitGrant, RequestPolicyLimits, RobotsDecision, TaskStatus,
    };
    use crate::crawl::{RobotsGroup, RobotsPolicy, RobotsRule};
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
        assert!(store.configure_run(&run_id, &digest, limits, &[]).unwrap());
        assert!(!store.configure_run(&run_id, &digest, limits, &[]).unwrap());
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

    #[test]
    #[ignore = "requires disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
    fn page_completion_does_not_wait_for_fair_scheduler_cursor() {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
        let (run_id, _cleanup, mut frontier) = setup_policy_run(
            &database_url,
            policy_limits(
                4,
                2,
                Duration::ZERO,
                Duration::from_secs(2),
                Duration::from_secs(1),
            ),
            "completion-scheduler-lock",
        );
        let url = "https://example.invalid/docs/page";
        frontier.enqueue_url(&run_id, "docs", url, 3).unwrap();
        let task = frontier
            .claim_one(
                &run_id,
                "worker-a",
                LeaseTiming::for_request_timeout(Duration::from_secs(2)).unwrap(),
            )
            .unwrap()
            .unwrap();
        let mut blocker = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
        let mut transaction = blocker.transaction().unwrap();
        transaction
            .query_one(
                "SELECT crawl_run_id FROM scout_crawl_task_scheduler WHERE crawl_run_id=$1 FOR UPDATE",
                &[&run_id],
            )
            .unwrap();
        let identity = "unique completion identity";
        let capture = AcceptedCapture {
            source_id: "docs".into(),
            content_hash: sha256_hex(identity.as_bytes()),
            identity: identity.into(),
            body_hash: sha256_hex(b"body"),
            body_size: 4,
            metadata_json: "{}".into(),
        };
        let result_hash = sha256_hex(b"page-only completion");
        let (sent, received) = std::sync::mpsc::channel();
        let mut result_frontier = FrontierStore::connect(&database_url).unwrap();
        let result_run_id = run_id.clone();
        let handle = thread::spawn(move || {
            let result = result_frontier.accept_task_result(
                &task,
                &result_hash,
                Some(&capture),
                "",
                &[],
                0,
                3,
                100,
            );
            sent.send(result).unwrap();
        });
        let completed_while_cursor_locked = received.recv_timeout(Duration::from_secs(1));
        transaction.commit().unwrap();
        handle.join().unwrap();
        assert!(
            matches!(
                completed_while_cursor_locked,
                Ok(Ok(super::CompletionAcceptance::Accepted { .. }))
            ),
            "page-only result was blocked by scheduler cursor for run {result_run_id}"
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
        // Quota-satisfied Sources still have live leases to drain. Their expiry must wake
        // queued claims even though new work from those Sources is excluded.
        let reference = store.dispatch_clock_ms().unwrap();
        let wake = store
            .next_dispatch_delay(&run_id, true, false, &["docs".into()], 60, reference)
            .unwrap()
            .expect("leased excluded Source has a deadline");
        assert!(
            wake <= short_timing.lease_for,
            "must wake on lease expiry, not run duration: {wake:?}"
        );
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

    // Run with SCOUT_TEST_DATABASE_URL set and `cargo test --lib frontier::tests::postgres_origin_limits_and_group_spacing_survive_restart -- --ignored`.
    #[test]
    #[ignore = "requires a coordinator-only PostgreSQL test database"]
    fn postgres_origin_limits_and_group_spacing_survive_restart() {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
        let run_id = format!(
            "origin-policy-test-{}",
            sha256_hex(format!("{:?}", Instant::now()).as_bytes())
        );
        let _cleanup = RunCleanup {
            database_url: database_url.clone(),
            run_id: run_id.clone(),
        };
        let digest = sha256_hex(run_id.as_bytes());
        let limits = policy_limits(
            4,
            1,
            Duration::ZERO,
            Duration::from_secs(2),
            Duration::from_secs(1),
        );
        let origins = vec![
            OriginPolicyLimit {
                origin: "https://a.example".into(),
                schedule_key: "https://a.example".into(),
                origin_concurrency: 2,
                min_start_spacing_ms: 0,
            },
            OriginPolicyLimit {
                origin: "https://b.example".into(),
                schedule_key: "group:shared".into(),
                origin_concurrency: 1,
                min_start_spacing_ms: 600,
            },
            OriginPolicyLimit {
                origin: "https://c.example".into(),
                schedule_key: "group:shared".into(),
                origin_concurrency: 1,
                min_start_spacing_ms: 0,
            },
        ];
        let mut store = FrontierStore::connect(&database_url).unwrap();
        store.apply_migrations().unwrap();
        assert!(
            store
                .configure_run(&run_id, &digest, limits, &origins)
                .unwrap()
        );
        let acquire = |store: &mut FrontierStore, id, key, origin| {
            store
                .try_acquire_request_permit(&run_id, "worker", id, key, origin, "page")
                .unwrap()
        };
        let a1 = acquired(acquire(
            &mut store,
            "a1",
            "https://a.example",
            "https://a.example",
        ));
        let a2 = acquired(acquire(
            &mut store,
            "a2",
            "https://a.example",
            "https://a.example",
        ));
        assert_eq!(a2.active_origin, 2);
        assert!(matches!(
            acquire(&mut store, "a3", "https://a.example", "https://a.example"),
            PermitDecision::Waiting(_)
        ));
        let c1 = acquired(acquire(
            &mut store,
            "c1",
            "group:shared",
            "https://c.example",
        ));
        assert!(matches!(
            acquire(&mut store, "b1", "group:shared", "https://b.example"),
            PermitDecision::Waiting(_)
        ));
        assert_eq!(store.ready_request_id(&run_id).unwrap(), None);
        for permit in [a1, a2, c1] {
            assert!(
                store
                    .release_request_permit(&run_id, "worker", permit.permit_id, "success")
                    .unwrap()
            );
        }
        drop(store);

        let mut restarted = FrontierStore::connect(&database_url).unwrap();
        restarted.apply_migrations().unwrap();
        assert!(
            !restarted
                .configure_run(&run_id, &digest, limits, &origins)
                .unwrap()
        );
        let mut changed = origins.clone();
        changed[1].min_start_spacing_ms = 0;
        assert!(
            restarted
                .configure_run(&run_id, &digest, limits, &changed)
                .is_err()
        );
        let mut added = origins.clone();
        added.push(OriginPolicyLimit {
            origin: "https://d.example".into(),
            schedule_key: "https://d.example".into(),
            origin_concurrency: 1,
            min_start_spacing_ms: 0,
        });
        assert!(
            restarted
                .configure_run(&run_id, &digest, limits, &added)
                .is_err()
        );
        assert!(
            restarted
                .configure_run(&run_id, &digest, limits, &[])
                .is_err()
        );
        assert!(
            !restarted
                .configure_run(&run_id, &digest, limits, &origins)
                .unwrap()
        );
        restarted.clear_pending_request_waits(&run_id).unwrap();
        assert!(matches!(
            acquire(&mut restarted, "b2", "group:shared", "https://b.example"),
            PermitDecision::Waiting(_)
        ));
        thread::sleep(Duration::from_millis(650));
        assert_eq!(
            restarted.ready_request_id(&run_id).unwrap(),
            Some("b2".into())
        );
        let _b2 = acquired(acquire(
            &mut restarted,
            "b2",
            "group:shared",
            "https://b.example",
        ));
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
                .configure_run(&run_id, &sha256_hex(run_id.as_bytes()), changed_limits, &[])
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

    // Run with SCOUT_TEST_DATABASE_URL set and `cargo test --lib frontier::tests::postgres_ticket4_acceptance_contract -- --ignored`.
    #[test]
    #[ignore = "requires a coordinator-only PostgreSQL test database"]
    fn postgres_ticket4_acceptance_contract() {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
        let suffix = sha256_hex(format!("ticket4-{:?}", Instant::now()).as_bytes());
        let run_id = format!("ticket4-acceptance-{suffix}");
        let _cleanup = RunCleanup {
            database_url: database_url.clone(),
            run_id: run_id.clone(),
        };
        let mut store = FrontierStore::connect(&database_url).unwrap();
        store.apply_migrations().unwrap();
        store
            .ensure_run(&run_id, &sha256_hex(run_id.as_bytes()))
            .unwrap();
        store
            .enqueue_url(&run_id, "docs", "https://example.com/seed", 3)
            .unwrap();
        let timing = LeaseTiming::for_request_timeout(Duration::from_secs(1)).unwrap();
        let task = store
            .claim_one(&run_id, "worker-a", timing)
            .unwrap()
            .unwrap();
        let accepted = |value: &str| {
            let identity = format!("Prose\0{value}");
            super::AcceptedCapture {
                source_id: "docs".into(),
                content_hash: sha256_hex(identity.as_bytes()),
                identity,
                body_hash: sha256_hex(format!("body-{value}").as_bytes()),
                body_size: value.len() as i64,
                metadata_json: format!("{{\"capture\":\"{value}\"}}"),
            }
        };
        let capture = accepted("first");
        let discovered = vec![
            super::DiscoveredTask {
                source_id: "docs".into(),
                normalized_url: "https://example.com/discovered".into(),
                schedule_key: "https://example.com".into(),
            },
            super::DiscoveredTask {
                source_id: "docs".into(),
                normalized_url: "https://example.com/discovered".into(),
                schedule_key: "https://example.com".into(),
            },
        ];
        let result_hash = sha256_hex(b"ticket4-first-result");
        assert_eq!(
            store
                .accept_task_result(
                    &task,
                    &result_hash,
                    Some(&capture),
                    "",
                    &discovered,
                    1,
                    3,
                    20
                )
                .unwrap(),
            super::CompletionAcceptance::Accepted {
                accepted_discovered_count: 1,
                rejected_discovered_count: 1,
            }
        );
        assert_eq!(
            store
                .accept_task_result(
                    &task,
                    &result_hash,
                    Some(&capture),
                    "",
                    &discovered,
                    1,
                    3,
                    20
                )
                .unwrap(),
            super::CompletionAcceptance::Duplicate {
                accepted_discovered_count: 1,
                rejected_discovered_count: 1,
            }
        );
        assert!(
            store
                .accept_task_result(
                    &task,
                    &sha256_hex(b"conflicting-payload"),
                    Some(&capture),
                    "",
                    &discovered,
                    1,
                    3,
                    20,
                )
                .is_err()
        );
        assert_eq!(
            store
                .content_group_counts_by_source(&run_id)
                .unwrap()
                .get("docs"),
            Some(&1)
        );
        assert!(
            store
                .claim_one_excluding(&run_id, "quota-worker", timing, &["docs".into()])
                .unwrap()
                .is_none(),
            "quota-satisfied Source should stop receiving claims"
        );
        assert_eq!(
            store.accepted_capture_batch(&run_id, 0, 100).unwrap().len(),
            1
        );
        assert_eq!(
            store
                .client
                .query_one(
                    "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id = $1",
                    &[&run_id],
                )
                .unwrap()
                .get::<_, i64>(0),
            2,
            "duplicate discoveries should enqueue one URL"
        );

        // This error occurs after the group insert point. A failed acceptance must roll back
        // the group, capture result, discovery, and task completion as one transaction.
        let invalid_discovery = super::DiscoveredTask {
            source_id: "docs".into(),
            normalized_url: "not a URL".into(),
            schedule_key: "https://example.com".into(),
        };
        let rollback_task = store
            .claim_one(&run_id, "worker-a", timing)
            .unwrap()
            .unwrap();
        let rollback_capture = accepted("rollback");
        assert!(
            store
                .accept_task_result(
                    &rollback_task,
                    &sha256_hex(b"rollback-result"),
                    Some(&rollback_capture),
                    "",
                    &[invalid_discovery],
                    0,
                    3,
                    20,
                )
                .is_err()
        );
        assert_eq!(
            store
                .content_group_counts_by_source(&run_id)
                .unwrap()
                .get("docs"),
            Some(&1)
        );
        assert_eq!(
            store.accepted_capture_batch(&run_id, 0, 100).unwrap().len(),
            1
        );
        assert!(store.lease_is_live(&rollback_task).unwrap());

        let stale_run_id = format!("ticket4-stale-{suffix}");
        let _stale_cleanup = RunCleanup {
            database_url: database_url.clone(),
            run_id: stale_run_id.clone(),
        };
        let mut stale_store = FrontierStore::connect(&database_url).unwrap();
        stale_store.apply_migrations().unwrap();
        stale_store
            .ensure_run(&stale_run_id, &sha256_hex(stale_run_id.as_bytes()))
            .unwrap();
        stale_store
            .enqueue_url(&stale_run_id, "docs", "https://example.com/late", 3)
            .unwrap();
        let short_timing = LeaseTiming::new(
            Duration::from_millis(1),
            Duration::from_millis(25),
            Duration::from_millis(5),
        )
        .unwrap();
        let expired = stale_store
            .claim_one(&stale_run_id, "worker-old", short_timing)
            .unwrap()
            .unwrap();
        thread::sleep(Duration::from_millis(75));
        let current = stale_store
            .claim_one(&stale_run_id, "worker-new", short_timing)
            .unwrap()
            .unwrap();
        assert_eq!(current.lease_epoch, expired.lease_epoch + 1);
        let late_capture = accepted("late");
        assert_eq!(
            stale_store
                .accept_task_result(
                    &expired,
                    &sha256_hex(b"late-result"),
                    Some(&late_capture),
                    "",
                    &[],
                    0,
                    3,
                    20,
                )
                .unwrap(),
            super::CompletionAcceptance::Stale
        );
        assert!(
            stale_store
                .content_group_counts_by_source(&stale_run_id)
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            stale_store
                .accept_task_result(
                    &current,
                    &sha256_hex(b"late-result"),
                    Some(&late_capture),
                    "",
                    &[],
                    0,
                    3,
                    20,
                )
                .unwrap(),
            super::CompletionAcceptance::Accepted { .. }
        ));
        assert_eq!(
            stale_store
                .content_group_counts_by_source(&stale_run_id)
                .unwrap()
                .get("docs"),
            Some(&1)
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SitemapCompletion {
    pub(crate) terminal: bool,
    pub(crate) accepted_discovered_count: usize,
    pub(crate) rejected_discovered_count: usize,
}
