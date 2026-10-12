#[path = "common/ticket8_rpc.rs"]
mod support;

use postgres::Transaction;
use std::thread;
use std::time::{Duration, Instant};
use support::proto;
use support::proto::coordinator_client::CoordinatorClient;
use support::{
    Gate, RpcProcess, Run, SourceSpec, VERSION, connect_named, local_origin, parse_lease,
    parse_result, url,
};
use tonic::transport::Channel;

fn one_source(origin: &str, paths: &[&str]) -> Vec<SourceSpec> {
    vec![SourceSpec {
        id: "docs".into(),
        seeds: paths.iter().map(|path| url(origin, path)).collect(),
        group: None,
    }]
}

fn wait_for_lock(db_url: &str, app_name: &str, fragment: &str) {
    let mut observer = connect_named(db_url, "scout-ticket8-test-observer");
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let found: bool = observer
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
             WHERE application_name=$1 AND wait_event_type='Lock' AND query ILIKE $2)",
                &[&app_name, &format!("%{fragment}%")],
            )
            .unwrap()
            .get(0);
        if found {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no coordinator backend waited on {fragment}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn rpc_waiting_past_task_deadline(
    run: &Run,
    lease: &proto::Lease,
    action: &str,
    worker: &str,
    values: &[(&str, &str)],
) -> serde_json::Value {
    let mut fault = connect_named(&run.database_url, "scout-ticket8-test-fault");
    fault
        .execute(
            "UPDATE scout_crawl_tasks
             SET lease_deadline=clock_timestamp()+interval '2 seconds'
             WHERE task_id=$1",
            &[&lease.task_id],
        )
        .unwrap();
    let mut blocker = connect_named(&run.database_url, "scout-ticket8-test-blocker");
    let mut tx = blocker.transaction().unwrap();
    tx.query_one(
        "SELECT task_id FROM scout_crawl_tasks WHERE task_id=$1 FOR UPDATE",
        &[&lease.task_id],
    )
    .unwrap();
    let mut rpc = RpcProcess::spawn(run, action, worker, Some(lease), None, values);
    wait_for_lock(
        &run.database_url,
        &run.coordinator_app_name,
        "scout_crawl_tasks",
    );
    thread::sleep(Duration::from_millis(2100));
    tx.commit().unwrap();
    parse_result(&rpc.wait(Duration::from_secs(5)))
}

fn coordinator_backend_counts(run: &mut Run) -> (i64, i64, i64) {
    let rows = run.database.query(
        "SELECT application_name, count(*) FROM pg_stat_activity
         WHERE datname=current_database() AND application_name IN ($1, 'scout-coordinator-general', 'scout-coordinator-completion')
         GROUP BY application_name",
        &[&run.coordinator_app_name],
    ).unwrap();
    let mut total = 0;
    let mut general = 0;
    let mut completion = 0;
    for row in rows {
        let app: String = row.get(0);
        let count: i64 = row.get(1);
        total += count;
        if app == "scout-coordinator-general" {
            general += count;
        }
        if app == "scout-coordinator-completion" {
            completion += count;
        }
    }
    (total, general, completion)
}

fn assert_backend_bound(run: &mut Run) {
    let (total, general, completion) = coordinator_backend_counts(run);
    assert!(general <= 4, "general pool opened {general} backends");
    assert!(
        completion <= 4,
        "completion pool opened {completion} backends"
    );
    assert!(
        total <= 8,
        "coordinator opened {total} backends; expected general 4 + completion 4"
    );
}

fn active_permits(run: &mut Run) -> Vec<(i64, String, String)> {
    run.database
        .query(
            "SELECT permit_id, worker_id, request_id FROM scout_crawl_request_permits
         WHERE crawl_run_id=$1 AND status='active' ORDER BY permit_id",
            &[&run.id],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

fn release_active(
    run: &mut Run,
    runtime: &tokio::runtime::Runtime,
    client: &mut CoordinatorClient<Channel>,
) {
    for (permit_id, worker, _) in active_permits(run) {
        let response = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(4),
                client.release_request_permit(proto::ReleaseRequestPermitRequest {
                    protocol_version: VERSION,
                    crawl_run_id: run.id.clone(),
                    worker_id: worker,
                    permit_id,
                    outcome: "success".into(),
                }),
            )
            .await
            .unwrap()
            .unwrap()
            .into_inner()
        });
        assert!(response.success);
    }
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn multi_process_permits_preserve_global_origin_and_group_spacing() {
    let group_a = local_origin();
    let group_b = local_origin();
    let solo = local_origin();
    let specs = vec![
        SourceSpec {
            id: "a".into(),
            seeds: vec![url(&group_a, "/a")],
            group: Some("shared-docs".into()),
        },
        SourceSpec {
            id: "b".into(),
            seeds: vec![url(&group_b, "/b")],
            group: Some("shared-docs".into()),
        },
        SourceSpec {
            id: "solo".into(),
            seeds: vec![url(&solo, "/one"), url(&solo, "/two")],
            group: None,
        },
    ];
    let overrides = vec![
        (group_a.clone(), 1, 0),
        (group_b.clone(), 1, 300),
        (solo.clone(), 1, 0),
    ];
    let mut run = Run::new_with_origin_overrides("group-permits", &specs, 2, 1, 0, 2, &overrides);
    let (runtime, mut client) = run.client();
    let leases = (0..4)
        .map(|index| {
            run.claim(&runtime, &mut client, &format!("permit-owner-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let gate = Gate::new(&run, "group-permits");
    let mut workers = leases
        .iter()
        .enumerate()
        .map(|(index, lease)| {
            let id = format!("permit-{index}");
            let request_id = format!("request-{index}");
            RpcProcess::spawn(
                &run,
                "permit",
                &id,
                Some(lease),
                Some(&gate),
                &[("SCOUT_RPC_REQUEST_ID", &request_id)],
            )
        })
        .collect::<Vec<_>>();
    gate.wait_ready(&["permit-0", "permit-1", "permit-2", "permit-3"]);
    gate.release();

    let deadline = Instant::now() + Duration::from_secs(8);
    let mut results = Vec::new();
    while results.len() < workers.len() {
        for worker in &mut workers {
            if let Some(output) = worker.try_wait() {
                let result = parse_result(&output);
                assert_eq!(
                    result["decision"],
                    proto::acquire_request_permit_response::Decision::Acquired as i32
                );
                results.push(result);
            }
        }
        let active: i64 = run.database.query_one("SELECT count(*) FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND status='active'", &[&run.id]).unwrap().get(0);
        assert!(active <= 2, "global permit cap exceeded: {active}");
        if active >= 2 {
            release_active(&mut run, &runtime, &mut client);
        }
        assert!(
            Instant::now() < deadline,
            "permit clients did not all receive grants"
        );
        thread::sleep(Duration::from_millis(20));
    }
    release_active(&mut run, &runtime, &mut client);
    // `try_wait` consumed outputs, so permit correctness is checked from the durable grant rows.
    let active_origin_peak: i64 = run.database.query_one(
        "WITH events AS (
           SELECT origin, acquired_at AS at, 1 AS delta FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND acquired_at IS NOT NULL
           UNION ALL SELECT origin, COALESCE(finished_at, expires_at) AS at, -1 AS delta FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND acquired_at IS NOT NULL
         ), levels AS (SELECT sum(delta) OVER (PARTITION BY origin ORDER BY at, delta DESC ROWS UNBOUNDED PRECEDING) AS active FROM events)
         SELECT COALESCE(max(active),0) FROM levels",
        &[&run.id],
    ).unwrap().get(0);
    assert!(
        active_origin_peak <= 1,
        "origin concurrency exceeded: {active_origin_peak}"
    );
    let global_peak: i64 = run.database.query_one(
        "WITH events AS (
           SELECT acquired_at AS at, 1 AS delta FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND acquired_at IS NOT NULL
           UNION ALL SELECT COALESCE(finished_at, expires_at) AS at, -1 AS delta FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND acquired_at IS NOT NULL
         ), levels AS (SELECT sum(delta) OVER (ORDER BY at, delta DESC ROWS UNBOUNDED PRECEDING) AS active FROM events)
         SELECT COALESCE(max(active),0) FROM levels",
        &[&run.id],
    ).unwrap().get(0);
    assert_eq!(
        global_peak, 2,
        "global limit should be exercised and never exceeded"
    );
    let group_gap: f64 = run.database.query_one(
        "SELECT min(gap_ms)::DOUBLE PRECISION FROM (
           SELECT extract(epoch FROM (acquired_at-lag(acquired_at) OVER (PARTITION BY schedule_key ORDER BY acquired_at)))*1000 AS gap_ms
           FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND schedule_key='group:shared-docs'
         ) gaps WHERE gap_ms IS NOT NULL",
        &[&run.id],
    ).unwrap().get(0);
    assert!(
        group_gap >= 299.0,
        "shared schedule key spacing fell below 300ms: {group_gap}ms"
    );
    let granted: i64 = run.database.query_one("SELECT count(*) FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND acquired_at IS NOT NULL", &[&run.id]).unwrap().get(0);
    assert_eq!(granted, 4);
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn initializing_permit_accepts_renew_cancel_and_duplicate_rpc_without_stalling() {
    let origin = local_origin();
    let mut run = Run::new(
        "initializing-permit-churn",
        &one_source(&origin, &["/holder", "/waiter"]),
        1,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let holder_lease = run.claim(&runtime, &mut client, "holder").unwrap();
    let waiter_lease = run.claim(&runtime, &mut client, "waiter").unwrap();
    let mut held = RpcProcess::spawn(
        &run,
        "permit",
        "holder-client",
        Some(&holder_lease),
        None,
        &[("SCOUT_RPC_REQUEST_ID", "held-permit")],
    );
    let held_result = parse_result(&held.wait(Duration::from_secs(5)));
    assert_eq!(
        held_result["decision"],
        proto::acquire_request_permit_response::Decision::Acquired as i32
    );

    let mut blocker = connect_named(&run.database_url, "scout-ticket8-test-blocker");
    let mut tx = blocker.transaction().unwrap();
    tx.query_one(
        "SELECT crawl_run_id FROM scout_crawl_request_policy WHERE crawl_run_id=$1 FOR UPDATE",
        &[&run.id],
    )
    .unwrap();
    let request_id = "initializing-wait";
    let request_args = [("SCOUT_RPC_REQUEST_ID", request_id)];
    let mut initial = RpcProcess::spawn(
        &run,
        "permit",
        "waiter",
        Some(&waiter_lease),
        None,
        &request_args,
    );
    wait_for_lock(
        &run.database_url,
        &run.coordinator_app_name,
        "scout_crawl_request_policy",
    );

    let mut renewal = RpcProcess::spawn(&run, "renew", "waiter", Some(&waiter_lease), None, &[]);
    assert_eq!(
        parse_result(&renewal.wait(Duration::from_secs(5)))["renewed"],
        true,
        "task renewal should complete while permit initialization is held"
    );

    let duplicate_gate = Gate::new(&run, "initializing-permit-duplicate");
    let mut duplicate = RpcProcess::spawn(
        &run,
        "permit",
        "waiter",
        Some(&waiter_lease),
        Some(&duplicate_gate),
        &request_args,
    );
    duplicate_gate.wait_ready(&["waiter"]);
    duplicate_gate.release();
    thread::sleep(Duration::from_millis(100));

    let cancel_gate = Gate::new(&run, "initializing-permit-cancel");
    let mut cancel = RpcProcess::spawn(
        &run,
        "cancel",
        "waiter",
        Some(&waiter_lease),
        Some(&cancel_gate),
        &request_args,
    );
    cancel_gate.wait_ready(&["waiter"]);
    cancel_gate.release();
    thread::sleep(Duration::from_millis(100));
    tx.commit().unwrap();

    assert_eq!(
        parse_result(&initial.wait(Duration::from_secs(5)))["error"],
        "Cancelled"
    );
    assert_eq!(
        parse_result(&duplicate.wait(Duration::from_secs(5)))["error"],
        "Cancelled"
    );
    assert_eq!(
        parse_result(&cancel.wait(Duration::from_secs(5)))["success"],
        true
    );
    let remaining: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_request_permits
             WHERE crawl_run_id=$1 AND request_id=$2",
            &[&run.id, &request_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        remaining, 0,
        "cancelled initialization must leave no permit row"
    );

    release_active(&mut run, &runtime, &mut client);
    let mut after_cancel = RpcProcess::spawn(
        &run,
        "permit",
        "waiter",
        Some(&waiter_lease),
        None,
        &[("SCOUT_RPC_REQUEST_ID", "after-initial-cancel")],
    );
    assert_eq!(
        parse_result(&after_cancel.wait(Duration::from_secs(5)))["decision"],
        proto::acquire_request_permit_response::Decision::Acquired as i32,
        "cancellation during initialization must not stall a later permit grant"
    );
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn renewed_pending_wait_survives_expiry_while_its_row_is_locked() {
    let origin = local_origin();
    let mut run = Run::new(
        "renew-pending-prune-race",
        &one_source(&origin, &["/holder", "/waiter"]),
        1,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let holder_lease = run.claim(&runtime, &mut client, "holder").unwrap();
    let waiter_lease = run.claim(&runtime, &mut client, "waiter").unwrap();
    let mut holder = RpcProcess::spawn(
        &run,
        "permit",
        "holder-permit",
        Some(&holder_lease),
        None,
        &[("SCOUT_RPC_REQUEST_ID", "renew-race-holder")],
    );
    assert_eq!(
        parse_result(&holder.wait(Duration::from_secs(5)))["decision"],
        proto::acquire_request_permit_response::Decision::Acquired as i32
    );

    let request_id = "renew-race-waiter";
    let request = [("SCOUT_RPC_REQUEST_ID", request_id)];
    let mut waiting = RpcProcess::spawn(
        &run,
        "permit",
        "waiting-permit",
        Some(&waiter_lease),
        None,
        &request,
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let pending: bool = run
            .database
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM scout_crawl_request_permits
                 WHERE crawl_run_id=$1 AND request_id=$2 AND status='pending')",
                &[&run.id, &request_id],
            )
            .unwrap()
            .get(0);
        if pending {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "waiter did not persist its pending permit"
        );
        thread::sleep(Duration::from_millis(10));
    }

    run.database
        .execute(
            "UPDATE scout_crawl_request_permits
             SET pending_deadline=clock_timestamp()+interval '1200 milliseconds'
             WHERE crawl_run_id=$1 AND request_id=$2 AND status='pending'",
            &[&run.id, &request_id],
        )
        .unwrap();
    let mut blocker = connect_named(&run.database_url, "scout-ticket8-test-blocker");
    let mut tx = blocker.transaction().unwrap();
    tx.query_one(
        "SELECT request_id FROM scout_crawl_request_permits
         WHERE crawl_run_id=$1 AND request_id=$2 FOR UPDATE",
        &[&run.id, &request_id],
    )
    .unwrap();

    let mut renewal = RpcProcess::spawn(
        &run,
        "renew",
        "renew-waiter",
        Some(&waiter_lease),
        None,
        &[],
    );
    wait_for_lock(
        &run.database_url,
        &run.coordinator_app_name,
        "UPDATE scout_crawl_request_permits",
    );
    thread::sleep(Duration::from_millis(1300));

    let duplicate_gate = Gate::new(&run, "renew-race-duplicate");
    let mut duplicate = RpcProcess::spawn(
        &run,
        "permit",
        "duplicate-waiter",
        Some(&waiter_lease),
        Some(&duplicate_gate),
        &request,
    );
    duplicate_gate.wait_ready(&["duplicate-waiter"]);
    duplicate_gate.release();
    wait_for_lock(
        &run.database_url,
        &run.coordinator_app_name,
        "SELECT request_id FROM scout_crawl_request_permits",
    );
    tx.commit().unwrap();

    assert_eq!(
        parse_result(&renewal.wait(Duration::from_secs(5)))["renewed"],
        true
    );
    let live_pending: bool = run
        .database
        .query_one(
            "SELECT pending_deadline > clock_timestamp() FROM scout_crawl_request_permits
             WHERE crawl_run_id=$1 AND request_id=$2 AND status='pending'",
            &[&run.id, &request_id],
        )
        .unwrap()
        .get(0);
    assert!(
        live_pending,
        "renewal/prune race expired a live task's pending wait"
    );

    release_active(&mut run, &runtime, &mut client);
    for process in [&mut waiting, &mut duplicate] {
        assert_eq!(
            parse_result(&process.wait(Duration::from_secs(5)))["decision"],
            proto::acquire_request_permit_response::Decision::Acquired as i32
        );
    }
    let row: (i64, String) = run
        .database
        .query_one(
            "SELECT count(*), min(status) FROM scout_crawl_request_permits
             WHERE crawl_run_id=$1 AND request_id=$2 AND acquired_at IS NOT NULL",
            &[&run.id, &request_id],
        )
        .map(|row| (row.get(0), row.get(1)))
        .unwrap();
    assert_eq!(row, (1, "active".into()));
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn independent_database_rpcs_progress_while_a_lease_row_is_locked_with_bounded_pool() {
    let origin = local_origin();
    let mut run = Run::new(
        "db-pool-progress",
        &one_source(&origin, &["/locked", "/free"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let blocked_lease = run.claim(&runtime, &mut client, "blocked-owner").unwrap();
    let mut blocker = connect_named(&run.database_url, "scout-ticket8-test-blocker");
    let mut tx = blocker.transaction().unwrap();
    tx.query_one(
        "SELECT task_id FROM scout_crawl_tasks WHERE task_id=$1 FOR UPDATE",
        &[&blocked_lease.task_id],
    )
    .unwrap();
    let mut blocked = RpcProcess::spawn(
        &run,
        "renew",
        "blocked-renew",
        Some(&blocked_lease),
        None,
        &[],
    );
    wait_for_lock(
        &run.database_url,
        &run.coordinator_app_name,
        "scout_crawl_tasks",
    );
    let mut independent = RpcProcess::spawn(
        &run,
        "claim",
        "independent-claim",
        None,
        None,
        &[("SCOUT_RPC_TIMEOUT_MS", "3000")],
    );
    let independent_result = parse_result(&independent.wait(Duration::from_secs(4)));
    let other_lease = parse_lease(&independent_result);
    assert_ne!(other_lease.task_id, blocked_lease.task_id);
    assert_backend_bound(&mut run);
    tx.commit().unwrap();
    assert_eq!(
        parse_result(&blocked.wait(Duration::from_secs(5)))["renewed"],
        true
    );
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn task_fence_rechecks_lease_deadline_after_waiting_for_the_task_row() {
    let origin = local_origin();
    let mut run = Run::new(
        "lease-expiry-during-row-lock",
        &one_source(&origin, &["/lease"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let lease = run.claim(&runtime, &mut client, "deadline-owner").unwrap();
    let result = rpc_waiting_past_task_deadline(
        &run,
        &lease,
        "permit",
        "expiry-during-lock",
        &[("SCOUT_RPC_REQUEST_ID", "must-not-start-after-expiry")],
    );
    assert_eq!(
        result["error"], "FailedPrecondition",
        "task fence must check lease deadline after obtaining its row lock"
    );
    let active: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_request_permits
         WHERE crawl_run_id=$1 AND request_id='must-not-start-after-expiry' AND status='active'",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(active, 0, "expired lease must not receive a permit");
    let attempts: i64 = run
        .database
        .query_one(
            "SELECT total_attempts FROM scout_crawl_request_policy WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(attempts, 0, "expired lease must not consume request budget");
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn renewal_rechecks_lease_deadline_after_waiting_for_the_task_row() {
    let origin = local_origin();
    let mut run = Run::new(
        "renew-expiry-during-row-lock",
        &one_source(&origin, &["/lease"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let lease = run
        .claim(&runtime, &mut client, "renew-deadline-owner")
        .unwrap();
    let result =
        rpc_waiting_past_task_deadline(&run, &lease, "renew", "renew-expiry-during-lock", &[]);
    assert_eq!(
        result["renewed"], false,
        "renewal must return false for a lease expired during its row-lock wait"
    );
    let live: bool = run
        .database
        .query_one(
            "SELECT lease_deadline > clock_timestamp() FROM scout_crawl_tasks WHERE task_id=$1",
            &[&lease.task_id],
        )
        .unwrap()
        .get(0);
    assert!(!live, "stale renewal must not extend the task lease");
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn sitemap_commit_rechecks_lease_deadline_after_waiting_for_scan_row() {
    let origin = local_origin();
    let mut run = Run::new(
        "sitemap-expiry-during-row-lock",
        &one_source(&origin, &["/sitemap-root"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let lease = run
        .claim(&runtime, &mut client, "sitemap-deadline-owner")
        .unwrap();
    let mut claim = RpcProcess::spawn(
        &run,
        "sitemap_claim",
        "sitemap-claim",
        Some(&lease),
        None,
        &[("SCOUT_RPC_ORIGIN", &origin)],
    );
    let claimed = parse_result(&claim.wait(Duration::from_secs(5)));
    assert_eq!(claimed["claimed"], true);
    let epoch = claimed["epoch"].as_i64().unwrap().to_string();

    let mut fault = connect_named(&run.database_url, "scout-ticket8-test-fault");
    fault
        .execute(
            "UPDATE scout_crawl_tasks SET lease_deadline=clock_timestamp()+interval '2 seconds'
             WHERE task_id=$1",
            &[&lease.task_id],
        )
        .unwrap();
    let mut blocker = connect_named(&run.database_url, "scout-ticket8-test-blocker");
    let mut tx = blocker.transaction().unwrap();
    tx.query_one(
        "SELECT crawl_run_id FROM scout_crawl_sitemap_discovery
         WHERE crawl_run_id=$1 AND origin=$2 FOR UPDATE",
        &[&run.id, &origin],
    )
    .unwrap();
    let candidate = url(&origin, "/must-not-be-discovered");
    let mut commit = RpcProcess::spawn(
        &run,
        "sitemap_commit",
        "sitemap-commit",
        Some(&lease),
        None,
        &[
            ("SCOUT_RPC_ORIGIN", &origin),
            ("SCOUT_RPC_EPOCH", &epoch),
            ("SCOUT_RPC_DISCOVERED_URLS", &candidate),
        ],
    );
    wait_for_lock(
        &run.database_url,
        &run.coordinator_app_name,
        "scout_crawl_sitemap_discovery",
    );
    thread::sleep(Duration::from_millis(2100));
    tx.commit().unwrap();

    let result = parse_result(&commit.wait(Duration::from_secs(5)));
    assert_eq!(
        result["committed"], false,
        "expired lease cannot commit a sitemap scan after waiting on its row"
    );
    let row = run
        .database
        .query_one(
            "SELECT state, result_sha256 FROM scout_crawl_sitemap_discovery
             WHERE crawl_run_id=$1 AND origin=$2",
            &[&run.id, &origin],
        )
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "running");
    assert!(row.get::<_, Option<String>>(1).is_none());
    let discovered: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id=$1 AND normalized_url=$2",
            &[&run.id, &candidate],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        discovered, 0,
        "stale sitemap result must not enqueue discoveries"
    );
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn completion_acceptance_rechecks_lease_deadline_after_waiting_for_the_task_row() {
    let origin = local_origin();
    let mut run = Run::new(
        "complete-expiry-during-row-lock",
        &one_source(&origin, &["/lease"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let lease = run
        .claim(&runtime, &mut client, "complete-deadline-owner")
        .unwrap();
    let result = rpc_waiting_past_task_deadline(
        &run,
        &lease,
        "complete",
        "complete-expiry-during-lock",
        &[("SCOUT_RPC_REJECTION", "must not accept after expiry")],
    );
    assert!(
        result["completed"] == false || result["error"] == "FailedPrecondition",
        "completion must be rejected after lease expiry during its row-lock wait: {result}"
    );
    let rows: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_task_results WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 0, "expired completion must not persist a task result");

    let replay_lease = run
        .claim(&runtime, &mut client, "fresh-completion-owner")
        .unwrap();
    assert_eq!(replay_lease.task_id, lease.task_id);
    let payload = [("SCOUT_RPC_REJECTION", "stable accepted result")];
    let mut accepted = RpcProcess::spawn(
        &run,
        "complete",
        "accepted-after-expiry",
        Some(&replay_lease),
        None,
        &payload,
    );
    assert_eq!(
        parse_result(&accepted.wait(Duration::from_secs(5)))["completed"],
        true
    );
    let mut replay = RpcProcess::spawn(
        &run,
        "complete",
        "duplicate-after-completion",
        Some(&replay_lease),
        None,
        &payload,
    );
    assert_eq!(
        parse_result(&replay.wait(Duration::from_secs(5)))["completed"],
        true,
        "an identical result remains replayable after completion"
    );
    let rows: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_task_results WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        rows, 1,
        "idempotent replay must not duplicate the result row"
    );
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn coordinator_shutdown_joins_an_initial_permit_job_waiting_on_postgres() {
    let origin = local_origin();
    let mut run = Run::new(
        "shutdown-with-blocked-permit-job",
        &one_source(&origin, &["/permit"]),
        1,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let lease = run.claim(&runtime, &mut client, "shutdown-owner").unwrap();
    let mut blocker = connect_named(&run.database_url, "scout-ticket8-test-blocker");
    let mut tx = blocker.transaction().unwrap();
    tx.query_one(
        "SELECT crawl_run_id FROM scout_crawl_request_policy WHERE crawl_run_id=$1 FOR UPDATE",
        &[&run.id],
    )
    .unwrap();
    let mut permit = RpcProcess::spawn(
        &run,
        "permit",
        "shutdown-permit",
        Some(&lease),
        None,
        &[("SCOUT_RPC_REQUEST_ID", "shutdown-blocked-initial")],
    );
    wait_for_lock(
        &run.database_url,
        &run.coordinator_app_name,
        "scout_crawl_request_policy",
    );

    let signal = std::process::Command::new("kill")
        .args(["-INT", &run.child.id().to_string()])
        .status()
        .unwrap();
    assert!(signal.success(), "could not send SIGINT to coordinator");
    thread::sleep(Duration::from_millis(100));
    assert!(
        run.child.try_wait().unwrap().is_none(),
        "shutdown exited while its PostgreSQL job was still blocked"
    );
    tx.commit().unwrap();

    let permit_output = permit.wait(Duration::from_secs(6));
    assert!(
        permit_output.status.success(),
        "blocked permit child did not exit cleanly: {}",
        String::from_utf8_lossy(&permit_output.stderr)
    );
    let _ = parse_result(&permit_output);
    let deadline = Instant::now() + Duration::from_secs(6);
    let coordinator_status = loop {
        if let Some(status) = run.child.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "coordinator shutdown did not finish"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(
        coordinator_status.success(),
        "coordinator exited unsuccessfully"
    );
    let stderr = std::fs::read_to_string(run.dir.join("coordinator.stderr")).unwrap();
    assert!(
        !stderr.contains("Cannot drop a runtime in a context where blocking is not allowed")
            && !stderr.contains("panicked at"),
        "coordinator panicked during shutdown: {stderr}"
    );
    let backends: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname=current_database() AND application_name=$1",
            &[&run.coordinator_app_name],
        )
        .unwrap()
        .get(0);
    assert_eq!(backends, 0, "coordinator leaked tagged PostgreSQL backends");
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn blocked_permit_wait_is_refenced_when_the_lease_is_reclaimed() {
    let origin = local_origin();
    let mut run = Run::new(
        "stale-grant-race",
        &one_source(&origin, &["/lease"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let old = run.claim(&runtime, &mut client, "old-owner").unwrap();
    let mut blocker = connect_named(&run.database_url, "scout-ticket8-test-blocker");
    let mut tx: Transaction<'_> = blocker.transaction().unwrap();
    tx.query_one(
        "SELECT crawl_run_id FROM scout_crawl_request_policy WHERE crawl_run_id=$1 FOR UPDATE",
        &[&run.id],
    )
    .unwrap();
    let mut stale_permit = RpcProcess::spawn(
        &run,
        "permit",
        "stale-permit",
        Some(&old),
        None,
        &[("SCOUT_RPC_REQUEST_ID", "must-not-start")],
    );
    wait_for_lock(
        &run.database_url,
        &run.coordinator_app_name,
        "scout_crawl_request_policy",
    );

    let mut fault = connect_named(&run.database_url, "scout-ticket8-test-fault");
    fault.execute("UPDATE scout_crawl_tasks SET lease_deadline=clock_timestamp()-interval '1 second' WHERE task_id=$1", &[&old.task_id]).unwrap();
    let mut reclaim = RpcProcess::spawn(
        &run,
        "claim",
        "new-owner",
        None,
        None,
        &[("SCOUT_RPC_TIMEOUT_MS", "3000")],
    );
    let new_lease = parse_lease(&parse_result(&reclaim.wait(Duration::from_secs(4))));
    assert_eq!(new_lease.task_id, old.task_id);
    assert_eq!(new_lease.lease_epoch, old.lease_epoch + 1);
    assert_backend_bound(&mut run);

    tx.commit().unwrap();
    let result = parse_result(&stale_permit.wait(Duration::from_secs(5)));
    assert_eq!(
        result["error"], "FailedPrecondition",
        "stale waiter must fail lease fencing before permit grant"
    );
    let active: i64 = run.database.query_one("SELECT count(*) FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND request_id='must-not-start' AND status='active'", &[&run.id]).unwrap().get(0);
    assert_eq!(active, 0, "stale request must not consume an HTTP permit");
    let attempts: i64 = run
        .database
        .query_one(
            "SELECT total_attempts FROM scout_crawl_request_policy WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        attempts, 0,
        "stale request must not consume the shared request-attempt budget"
    );
}

#[test]
fn rpc_client_process() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(support::child_rpc());
}
