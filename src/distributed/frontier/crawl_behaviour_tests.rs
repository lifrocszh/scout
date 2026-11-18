//! PostgreSQL contract for the dead-letter queue and adaptive origin pacing.
//! Each test owns and deletes its run. Pacing assertions read persisted state, and the only
//! timing they rely on is a hold of a second or more, so a slow CI host cannot flip them.
use super::dead_letters::DeadLetterSelection;
use super::*;
use crate::crawl::throttle::{MIN_BACKOFF_MS, backoff_after_healthy};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct TestRun {
    database_url: String,
    run_id: String,
}

impl TestRun {
    fn new(label: &str) -> (Self, FrontierStore) {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let run = Self {
            database_url,
            run_id: format!("behaviour-{label}-{}-{stamp}", std::process::id()),
        };
        let mut store = FrontierStore::connect(&run.database_url).unwrap();
        store.apply_migrations().unwrap();
        (run, store)
    }

    fn configure(&self, store: &mut FrontierStore, spacing: Duration) {
        let limits = RequestPolicyLimits {
            global_concurrency: 4,
            origin_concurrency: 1,
            min_start_spacing: spacing,
            request_timeout: Duration::from_secs(30),
            permit_safety_margin: Duration::from_secs(5),
            robots_cache_ttl: Duration::from_secs(60),
            max_total_attempts: 100,
            max_pending_requests: 32,
        };
        let digest = sha256_hex(self.run_id.as_bytes());
        store
            .configure_run(&self.run_id, &digest, limits, &[])
            .unwrap();
    }

    fn acquire(&self, store: &mut FrontierStore, request_id: &str, origin: &str) -> PermitDecision {
        store
            .try_acquire_request_permit(&self.run_id, "w", request_id, origin, origin, "page")
            .unwrap()
    }

    fn release(
        &self,
        store: &mut FrontierStore,
        decision: PermitDecision,
        outcome: &str,
        retry_after_ms: Option<u64>,
    ) {
        let PermitDecision::Acquired(grant) = decision else {
            panic!("expected a request permit, got {decision:?}");
        };
        assert!(
            store
                .report_request_outcome(&self.run_id, "w", grant.permit_id, outcome, retry_after_ms)
                .unwrap()
        );
    }

    fn hold_ms(&self, store: &mut FrontierStore, origin: &str) -> i64 {
        store
            .client
            .query_one(
                "SELECT COALESCE(ceil(extract(epoch FROM max(next_allowed_at) - clock_timestamp())
                                 * 1000), 0)::BIGINT
                 FROM scout_crawl_request_spacing WHERE crawl_run_id = $1 AND schedule_key = $2",
                &[&self.run_id, &origin],
            )
            .unwrap()
            .get(0)
    }

    /// Let the next request start without waiting out a spacing hold.
    fn clear_hold(&self, store: &mut FrontierStore) {
        store
            .client
            .execute(
                "UPDATE scout_crawl_request_spacing SET next_allowed_at = clock_timestamp()
                 WHERE crawl_run_id = $1",
                &[&self.run_id],
            )
            .unwrap();
    }
}

impl Drop for TestRun {
    fn drop(&mut self) {
        if let Ok(mut client) = Client::connect(&self.database_url, NoTls) {
            let _ = client.execute(
                "DELETE FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[&self.run_id],
            );
        }
    }
}

fn lease() -> LeaseTiming {
    LeaseTiming::for_request_timeout(Duration::from_secs(30)).unwrap()
}

#[test]
#[ignore = "requires a disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn postgres_retry_exhausted_tasks_dead_letter_and_requeue_with_a_fresh_budget() {
    let (run, mut store) = TestRun::new("dlq");
    store
        .ensure_run(&run.run_id, &sha256_hex(run.run_id.as_bytes()))
        .unwrap();
    let broken = "https://broken.example/missing";
    let healthy = "https://broken.example/ok";
    store.enqueue_url(&run.run_id, "docs", healthy, 2).unwrap();
    let task = store.claim_one(&run.run_id, "w", lease()).unwrap().unwrap();
    assert_eq!(task.normalized_url, healthy);
    assert!(store.complete_task(&task).unwrap());
    store.enqueue_url(&run.run_id, "docs", broken, 2).unwrap();

    // Two failed attempts exhaust the broken URL; the first is only a retry.
    let mut dead_task_id = None;
    for expected in [TaskStatus::Pending, TaskStatus::Failed] {
        store
            .client
            .execute(
                "UPDATE scout_crawl_tasks SET retry_at = clock_timestamp()
                 WHERE crawl_run_id = $1 AND status = 'pending'",
                &[&run.run_id],
            )
            .unwrap();
        let task = store.claim_one(&run.run_id, "w", lease()).unwrap().unwrap();
        assert_eq!(task.normalized_url, broken);
        let status = store
            .fail_task(&task, "HTTP 500 from fixture", Duration::from_millis(1))
            .unwrap();
        assert_eq!(status, Some(expected));
        dead_task_id = Some(task.task_id);
    }
    let dead_task_id = dead_task_id.unwrap();

    let listing = store.list_dead_letters(&run.run_id, 10).unwrap();
    assert_eq!(listing.run_status, "active");
    assert_eq!(listing.total, 1);
    let entry = &listing.entries[0];
    assert_eq!(entry.task_id, dead_task_id);
    assert_eq!(entry.normalized_url, broken);
    assert_eq!(entry.origin, "https://broken.example");
    assert_eq!((entry.task_attempt_count, entry.max_task_attempts), (2, 2));
    assert_eq!(entry.last_error.as_deref(), Some("HTTP 500 from fixture"));
    assert!(entry.dead_lettered_at_unix_ms > 0);
    assert_eq!(entry.dead_letter_requeues, 0);
    assert_eq!(store.task_status_counts(&run.run_id).unwrap().failed, 1);

    // Unknown IDs are reported back, never silently ignored.
    let outcome = store
        .requeue_dead_letters(
            &run.run_id,
            &DeadLetterSelection::Tasks(vec![dead_task_id, 999_999_999]),
        )
        .unwrap();
    assert_eq!(outcome.requeued_task_ids, vec![dead_task_id]);
    assert_eq!(outcome.skipped_task_ids, vec![999_999_999]);
    assert_eq!(store.list_dead_letters(&run.run_id, 10).unwrap().total, 0);
    assert_eq!(store.dead_letter_requeue_count(&run.run_id).unwrap(), 1);

    // The requeued task is due now and has its whole attempt budget back.
    let again = store
        .claim_one(&run.run_id, "w2", lease())
        .unwrap()
        .unwrap();
    assert_eq!(again.task_id, dead_task_id);
    assert_eq!(again.task_attempt_count, 1);
    assert!(store.complete_task(&again).unwrap());

    // Requeueing again finds nothing, and a run that is no longer active refuses.
    let outcome = store
        .requeue_dead_letters(&run.run_id, &DeadLetterSelection::All)
        .unwrap();
    assert!(outcome.requeued_task_ids.is_empty());
    store
        .client
        .execute(
            "UPDATE scout_crawl_runs SET status = 'completed' WHERE crawl_run_id = $1",
            &[&run.run_id],
        )
        .unwrap();
    let error = store
        .requeue_dead_letters(&run.run_id, &DeadLetterSelection::All)
        .unwrap_err();
    assert!(
        error.message.contains("only an active run"),
        "{}",
        error.message
    );
    assert!(store.list_dead_letters("no-such-run", 10).is_err());
}

#[test]
#[ignore = "requires a disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn postgres_lease_expiry_on_the_final_attempt_dead_letters_the_task() {
    let (run, mut store) = TestRun::new("dlq-lease");
    store
        .ensure_run(&run.run_id, &sha256_hex(run.run_id.as_bytes()))
        .unwrap();
    let url = "https://stall.example/page";
    store.enqueue_url(&run.run_id, "docs", url, 1).unwrap();
    let task = store.claim_one(&run.run_id, "w", lease()).unwrap().unwrap();
    // Expire the lease in the database rather than by waiting for it.
    store
        .client
        .execute(
            "UPDATE scout_crawl_tasks SET lease_deadline = clock_timestamp() - interval '1 second'
             WHERE task_id = $1",
            &[&task.task_id],
        )
        .unwrap();
    assert!(
        store
            .claim_one(&run.run_id, "w2", lease())
            .unwrap()
            .is_none()
    );
    let listing = store.list_dead_letters(&run.run_id, 10).unwrap();
    assert_eq!(listing.total, 1);
    assert_eq!(listing.entries[0].task_id, task.task_id);
    assert_eq!(
        listing.entries[0].last_error.as_deref(),
        Some("lease expired after final task attempt")
    );
}

#[test]
#[ignore = "requires a disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn postgres_throttled_origin_backs_off_and_recovers_while_others_keep_going() {
    let (run, mut store) = TestRun::new("throttle");
    run.configure(&mut store, Duration::ZERO);
    let slow = "https://slow.example";
    let fast = "https://fast.example";

    // Healthy responses on a fresh origin write no pacing state at all.
    let decision = run.acquire(&mut store, "fast-0", fast);
    run.release(&mut store, decision, "success", None);
    assert_eq!(store.origin_throttle(&run.run_id, fast).unwrap(), None);

    // A 429 sets the floor backoff and holds the origin; other origins are unaffected.
    let decision = run.acquire(&mut store, "slow-0", slow);
    run.release(&mut store, decision, "http_429", None);
    let throttle = store.origin_throttle(&run.run_id, slow).unwrap().unwrap();
    assert_eq!(throttle.backoff_ms, MIN_BACKOFF_MS as i64);
    assert_eq!(throttle.throttle_responses, 1);
    assert!(run.hold_ms(&mut store, slow) > 0);
    assert!(matches!(
        run.acquire(&mut store, "slow-1", slow),
        PermitDecision::Waiting(_)
    ));
    let decision = run.acquire(&mut store, "fast-1", fast);
    run.release(&mut store, decision, "success", None);
    // The queued slow request must not block a later fast one either.
    assert!(
        store
            .cancel_request_wait(&run.run_id, "w", "slow-1")
            .unwrap()
    );

    // Retry-After wins when it is longer than the backoff, and 503 counts like 429.
    run.clear_hold(&mut store);
    let decision = run.acquire(&mut store, "slow-2", slow);
    run.release(&mut store, decision, "http_503", Some(60_000));
    let throttle = store.origin_throttle(&run.run_id, slow).unwrap().unwrap();
    assert_eq!(throttle.backoff_ms, 2 * MIN_BACKOFF_MS as i64);
    assert_eq!(throttle.throttle_responses, 2);
    assert!(run.hold_ms(&mut store, slow) > 30_000);

    // A granted request spaces the next start by the backoff even without a new 429.
    run.clear_hold(&mut store);
    let decision = run.acquire(&mut store, "slow-3", slow);
    assert!(run.hold_ms(&mut store, slow) > 1_000);
    // Healthy responses decay the backoff; other server errors leave it alone.
    run.release(&mut store, decision, "http_500", None);
    assert_eq!(
        store
            .origin_throttle(&run.run_id, slow)
            .unwrap()
            .unwrap()
            .backoff_ms,
        2 * MIN_BACKOFF_MS as i64
    );
    run.clear_hold(&mut store);
    let decision = run.acquire(&mut store, "slow-4", slow);
    run.release(&mut store, decision, "success", None);
    assert_eq!(
        store
            .origin_throttle(&run.run_id, slow)
            .unwrap()
            .unwrap()
            .backoff_ms,
        backoff_after_healthy(2 * MIN_BACKOFF_MS) as i64
    );

    // A duplicate release of the same permit does not apply the outcome twice.
    run.clear_hold(&mut store);
    let PermitDecision::Acquired(grant) = run.acquire(&mut store, "slow-5", slow) else {
        panic!("expected a permit");
    };
    for _ in 0..2 {
        assert!(
            store
                .report_request_outcome(&run.run_id, "w", grant.permit_id, "http_429", None)
                .unwrap()
        );
    }
    assert_eq!(
        store
            .origin_throttle(&run.run_id, slow)
            .unwrap()
            .unwrap()
            .throttle_responses,
        3
    );
}
