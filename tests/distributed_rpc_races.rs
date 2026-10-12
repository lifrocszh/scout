#[path = "common/ticket8_rpc.rs"]
mod support;

use postgres::Transaction;
use std::thread;
use std::time::{Duration, Instant};
use support::proto;
use support::{
    Gate, RpcProcess, Run, SourceSpec, connect_named, local_origin, parse_lease, parse_result, url,
};

fn one_source(origin: &str, paths: &[&str]) -> Vec<SourceSpec> {
    vec![SourceSpec {
        id: "docs".into(),
        seeds: paths.iter().map(|path| url(origin, path)).collect(),
        group: None,
    }]
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn process_claims_are_unique_and_fair_across_schedule_keys() {
    let origin = local_origin();
    let mut run = Run::new(
        "duplicate-claim",
        &one_source(&origin, &["/only"]),
        2,
        1,
        0,
        2,
    );
    let gate = Gate::new(&run, "duplicate-claim");
    let mut a = RpcProcess::spawn(
        &run,
        "claim",
        "claim-a",
        None,
        Some(&gate),
        &[("SCOUT_RPC_TIMEOUT_MS", "1200")],
    );
    let mut b = RpcProcess::spawn(
        &run,
        "claim",
        "claim-b",
        None,
        Some(&gate),
        &[("SCOUT_RPC_TIMEOUT_MS", "1200")],
    );
    gate.wait_ready(&["claim-a", "claim-b"]);
    gate.release();
    let first = parse_result(&a.wait(Duration::from_secs(3)));
    let second = parse_result(&b.wait(Duration::from_secs(3)));
    assert_ne!(
        first["lease"].is_null(),
        second["lease"].is_null(),
        "one queued task must produce exactly one lease"
    );
    let leased: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id=$1 AND status='leased'",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(leased, 1);
    drop(run);

    let source_a = local_origin();
    let source_b = local_origin();
    let specs = vec![
        SourceSpec {
            id: "alpha".into(),
            seeds: vec![url(&source_a, "/a")],
            group: None,
        },
        SourceSpec {
            id: "beta".into(),
            seeds: vec![url(&source_b, "/b")],
            group: None,
        },
    ];
    let mut run = Run::new("fair-claims", &specs, 2, 1, 0, 2);
    let gate = Gate::new(&run, "fair-claims");
    let mut a = RpcProcess::spawn(&run, "claim", "fair-a", None, Some(&gate), &[]);
    let mut b = RpcProcess::spawn(&run, "claim", "fair-b", None, Some(&gate), &[]);
    gate.wait_ready(&["fair-a", "fair-b"]);
    gate.release();
    let first = parse_lease(&parse_result(&a.wait(Duration::from_secs(3))));
    let second = parse_lease(&parse_result(&b.wait(Duration::from_secs(3))));
    assert_ne!(first.task_id, second.task_id);
    assert_ne!(
        first.schedule_key, second.schedule_key,
        "simultaneous claimers must advance to a distinct eligible schedule key"
    );
    let leased: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id=$1 AND status='leased'",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(leased, 2);
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn completion_races_are_idempotent_and_conflicting_payloads_are_rejected() {
    let origin = local_origin();
    let mut run = Run::new(
        "completion-race",
        &one_source(&origin, &["/complete"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let lease = run
        .claim(&runtime, &mut client, "completion-owner")
        .unwrap();
    let gate = Gate::new(&run, "completion-race");
    let mut a = RpcProcess::spawn(
        &run,
        "complete",
        "complete-a",
        Some(&lease),
        Some(&gate),
        &[("SCOUT_RPC_REJECTION", "same result")],
    );
    let mut b = RpcProcess::spawn(
        &run,
        "complete",
        "complete-b",
        Some(&lease),
        Some(&gate),
        &[("SCOUT_RPC_REJECTION", "same result")],
    );
    gate.wait_ready(&["complete-a", "complete-b"]);
    gate.release();
    assert_eq!(
        parse_result(&a.wait(Duration::from_secs(5)))["completed"],
        true
    );
    assert_eq!(
        parse_result(&b.wait(Duration::from_secs(5)))["completed"],
        true
    );
    let rows: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_task_results WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 1, "duplicate reports must persist one result row");

    let mut conflict = RpcProcess::spawn(
        &run,
        "complete",
        "complete-conflict",
        Some(&lease),
        None,
        &[("SCOUT_RPC_REJECTION", "different result")],
    );
    let result = parse_result(&conflict.wait(Duration::from_secs(5)));
    assert_eq!(
        result["error"], "InvalidArgument",
        "same idempotency key cannot accept a different result"
    );
    let rows: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_task_results WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 1);
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn retry_limits_fence_the_old_lease_at_the_rpc_boundary() {
    let origin = local_origin();
    let mut run = Run::new("retry-limit", &one_source(&origin, &["/retry"]), 2, 1, 0, 2);
    let (runtime, mut client) = run.client();
    let first = run.claim(&runtime, &mut client, "retry-owner-a").unwrap();
    let mut fail_first = RpcProcess::spawn(&run, "fail", "fail-first", Some(&first), None, &[]);
    let first_failure = parse_result(&fail_first.wait(Duration::from_secs(5)));
    assert_eq!(first_failure["status"], "pending", "{first_failure}");
    let second = run.claim(&runtime, &mut client, "retry-owner-b").unwrap();
    assert_eq!(second.task_id, first.task_id);
    assert_eq!(second.lease_epoch, first.lease_epoch + 1);
    assert_eq!(second.task_attempt_count, 2);
    let mut stale_completion = RpcProcess::spawn(
        &run,
        "complete",
        "stale-completion",
        Some(&first),
        None,
        &[("SCOUT_RPC_REJECTION", "stale result")],
    );
    let stale_completion_result = parse_result(&stale_completion.wait(Duration::from_secs(5)));
    assert_eq!(
        stale_completion_result["completed"], false,
        "stale completion must not persist a result: {stale_completion_result}"
    );
    let accepted: i64 = run
        .database
        .query_one(
            "SELECT count(*) FROM scout_crawl_task_results WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap()
        .get(0);
    assert_eq!(accepted, 0, "stale completion must not persist a result");
    let mut stale = RpcProcess::spawn(&run, "renew", "stale-renew", Some(&first), None, &[]);
    assert_eq!(
        parse_result(&stale.wait(Duration::from_secs(5)))["error"],
        "FailedPrecondition",
        "stale renewal must be rejected at the RPC lease fence"
    );
    let mut fail_second = RpcProcess::spawn(&run, "fail", "fail-second", Some(&second), None, &[]);
    assert_eq!(
        parse_result(&fail_second.wait(Duration::from_secs(5)))["status"],
        "failed"
    );
    let mut exhausted = RpcProcess::spawn(
        &run,
        "claim",
        "after-exhaustion",
        None,
        None,
        &[("SCOUT_RPC_TIMEOUT_MS", "400")],
    );
    assert_eq!(
        parse_result(&exhausted.wait(Duration::from_secs(3)))["lease"],
        serde_json::Value::Null
    );
    let row = run
        .database
        .query_one(
            "SELECT status,task_attempt_count FROM scout_crawl_tasks WHERE crawl_run_id=$1",
            &[&run.id],
        )
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, i32>(1), 2);
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn robots_refresh_has_one_rpc_owner_and_fences_nonowner_commits() {
    let origin = local_origin();
    let mut run = Run::new(
        "robots-owner",
        &one_source(&origin, &["/a", "/b"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let lease_a = run.claim(&runtime, &mut client, "robots-owner-a").unwrap();
    let lease_b = run.claim(&runtime, &mut client, "robots-owner-b").unwrap();
    let gate = Gate::new(&run, "robots-owner");
    let origin_arg = [("SCOUT_RPC_ORIGIN", origin.as_str())];
    let mut a = RpcProcess::spawn(
        &run,
        "robots",
        "robots-a",
        Some(&lease_a),
        Some(&gate),
        &origin_arg,
    );
    let mut b = RpcProcess::spawn(
        &run,
        "robots",
        "robots-b",
        Some(&lease_b),
        Some(&gate),
        &origin_arg,
    );
    gate.wait_ready(&["robots-a", "robots-b"]);
    gate.release();
    let a = parse_result(&a.wait(Duration::from_secs(5)));
    let b = parse_result(&b.wait(Duration::from_secs(5)));
    let refresh = proto::robots_decision_response::Decision::Refresh as i32;
    let waiting = proto::robots_decision_response::Decision::Waiting as i32;
    let (owner, other, epoch) = if a["decision"] == refresh {
        (&lease_a, &lease_b, a["epoch"].as_i64().unwrap())
    } else {
        assert_eq!(a["decision"], waiting, "first robots response: {a}");
        assert_eq!(b["decision"], refresh, "second robots response: {b}");
        (&lease_b, &lease_a, b["epoch"].as_i64().unwrap())
    };
    let epoch_text = epoch.to_string();
    let mut wrong = RpcProcess::spawn(
        &run,
        "commit_robots",
        "robots-nonowner",
        Some(other),
        None,
        &[
            ("SCOUT_RPC_ORIGIN", origin.as_str()),
            ("SCOUT_RPC_EPOCH", &epoch_text),
        ],
    );
    assert_eq!(
        parse_result(&wrong.wait(Duration::from_secs(5)))["success"],
        false
    );
    let mut commit = RpcProcess::spawn(
        &run,
        "commit_robots",
        "robots-actual-owner",
        Some(owner),
        None,
        &[
            ("SCOUT_RPC_ORIGIN", origin.as_str()),
            ("SCOUT_RPC_EPOCH", &epoch_text),
        ],
    );
    assert_eq!(
        parse_result(&commit.wait(Duration::from_secs(5)))["success"],
        true
    );
    let state: String = run
        .database
        .query_one(
            "SELECT state FROM scout_crawl_robots_policy WHERE crawl_run_id=$1 AND origin=$2",
            &[&run.id, &origin],
        )
        .unwrap()
        .get(0);
    assert_eq!(state, "ready");
}

#[test]
#[ignore = "requires disposable Ticket 8 test database in SCOUT_TEST_DATABASE_URL"]
fn stale_robots_commit_is_fenced_after_the_task_lease_is_reclaimed() {
    let origin = local_origin();
    let mut run = Run::new(
        "stale-robots-commit",
        &one_source(&origin, &["/refresh"]),
        2,
        1,
        0,
        2,
    );
    let (runtime, mut client) = run.client();
    let old_lease = run
        .claim(&runtime, &mut client, "robots-old-owner")
        .unwrap();
    let mut refresh = RpcProcess::spawn(
        &run,
        "robots",
        "robots-old-owner",
        Some(&old_lease),
        None,
        &[("SCOUT_RPC_ORIGIN", origin.as_str())],
    );
    let refresh_result = parse_result(&refresh.wait(Duration::from_secs(5)));
    assert_eq!(
        refresh_result["decision"],
        proto::robots_decision_response::Decision::Refresh as i32
    );
    let epoch = refresh_result["epoch"].as_i64().unwrap();
    let epoch_text = epoch.to_string();

    let mut blocker = connect_named(&run.database_url, "scout-ticket8-test-blocker");
    let mut tx: Transaction<'_> = blocker.transaction().unwrap();
    tx.query_one(
        "SELECT state FROM scout_crawl_robots_policy WHERE crawl_run_id=$1 AND origin=$2 FOR UPDATE",
        &[&run.id, &origin],
    )
    .unwrap();
    let mut stale_commit = RpcProcess::spawn(
        &run,
        "commit_robots",
        "robots-old-owner",
        Some(&old_lease),
        None,
        &[
            ("SCOUT_RPC_ORIGIN", origin.as_str()),
            ("SCOUT_RPC_EPOCH", &epoch_text),
        ],
    );
    let mut observer = connect_named(&run.database_url, "scout-ticket8-test-observer");
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let waiting: bool = observer
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                 WHERE application_name=$1 AND wait_event_type='Lock'
                   AND query ILIKE '%scout_crawl_robots_policy%')",
                &[&run.coordinator_app_name],
            )
            .unwrap()
            .get(0);
        if waiting {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "robots commit RPC did not block on its policy row"
        );
        thread::sleep(Duration::from_millis(10));
    }

    let mut fault = connect_named(&run.database_url, "scout-ticket8-test-fault");
    fault.execute(
        "UPDATE scout_crawl_tasks SET lease_deadline=clock_timestamp()-interval '1 second' WHERE task_id=$1",
        &[&old_lease.task_id],
    ).unwrap();
    let mut reclaim = RpcProcess::spawn(
        &run,
        "claim",
        "robots-new-owner",
        None,
        None,
        &[("SCOUT_RPC_TIMEOUT_MS", "3000")],
    );
    let new_lease = parse_lease(&parse_result(&reclaim.wait(Duration::from_secs(4))));
    assert_eq!(new_lease.task_id, old_lease.task_id);
    assert_eq!(new_lease.lease_epoch, old_lease.lease_epoch + 1);

    tx.commit().unwrap();
    let stale_result = parse_result(&stale_commit.wait(Duration::from_secs(5)));
    assert_eq!(
        stale_result["error"], "FailedPrecondition",
        "reclaimed-task robots mutation must fail lease fencing"
    );
    let policy_row = run
        .database
        .query_one(
            "SELECT state, refresh_owner, refresh_epoch, body_sha256
             FROM scout_crawl_robots_policy WHERE crawl_run_id=$1 AND origin=$2",
            &[&run.id, &origin],
        )
        .unwrap();
    assert_eq!(policy_row.get::<_, String>(0), "refreshing");
    assert_eq!(policy_row.get::<_, String>(1), "robots-old-owner");
    assert_eq!(policy_row.get::<_, i64>(2), epoch);
    assert_eq!(
        policy_row.get::<_, Option<String>>(3),
        None,
        "stale commit must not populate cached policy"
    );
    run.database
        .execute(
            "UPDATE scout_crawl_robots_policy
             SET refresh_deadline=clock_timestamp()-interval '1 second'
             WHERE crawl_run_id=$1 AND origin=$2",
            &[&run.id, &origin],
        )
        .unwrap();

    let mut new_refresh = RpcProcess::spawn(
        &run,
        "robots",
        "robots-new-owner",
        Some(&new_lease),
        None,
        &[("SCOUT_RPC_ORIGIN", origin.as_str())],
    );
    let new_decision = parse_result(&new_refresh.wait(Duration::from_secs(5)));
    assert_eq!(
        new_decision["decision"],
        proto::robots_decision_response::Decision::Refresh as i32,
        "new owner robots response: {new_decision}; coordinator stderr: {}",
        std::fs::read_to_string(run.dir.join("coordinator.stderr")).unwrap_or_default()
    );
    assert_eq!(new_decision["epoch"], epoch + 1);
}

#[test]
fn rpc_client_process() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(support::child_rpc());
}
