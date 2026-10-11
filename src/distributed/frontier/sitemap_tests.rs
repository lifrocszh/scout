//! Direct durability/fencing contract; each check owns and cleans its PostgreSQL run.
use super::*;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct SitemapRun {
    database_url: String,
    run_id: String,
}

impl SitemapRun {
    fn new(label: &str) -> (Self, FrontierStore) {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let run_id = format!("sitemap-{label}-{}-{stamp}", std::process::id());
        let run = Self {
            database_url,
            run_id,
        };
        let mut store = run.connect();
        store.apply_migrations().unwrap();
        store
            .ensure_run(&run.run_id, &sha256_hex(run.run_id.as_bytes()))
            .unwrap();
        (run, store)
    }

    fn connect(&self) -> FrontierStore {
        FrontierStore::connect(&self.database_url).unwrap()
    }

    fn task(&self, store: &mut FrontierStore, url: &str, worker: &str) -> ClaimedTask {
        // Each subcase claims its intended seed while previously discovered pages stay durable.
        store.client.execute(
            "UPDATE scout_crawl_tasks SET retry_at=clock_timestamp()+interval '1 hour' WHERE crawl_run_id=$1 AND status='pending' AND normalized_url<>$2",
            &[&self.run_id, &url],
        ).unwrap();
        store.enqueue_url(&self.run_id, "docs", url, 4).unwrap();
        let task = store
            .claim_one(&self.run_id, worker, long_lease())
            .unwrap()
            .unwrap();
        assert_eq!(task.normalized_url, url);
        task
    }

    fn count_url(&self, store: &mut FrontierStore, url: &str) -> i64 {
        store.client.query_one(
            "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id=$1 AND normalized_url=$2",
            &[&self.run_id, &url],
        ).unwrap().get(0)
    }
}

impl Drop for SitemapRun {
    fn drop(&mut self) {
        if let Ok(mut client) = Client::connect(&self.database_url, NoTls) {
            let _ = client.execute(
                "DELETE FROM scout_crawl_runs WHERE crawl_run_id=$1",
                &[&self.run_id],
            );
        }
    }
}

fn long_lease() -> LeaseTiming {
    LeaseTiming::for_request_timeout(Duration::from_secs(30)).unwrap()
}

fn discovery(url: &str) -> DiscoveredTask {
    DiscoveredTask {
        source_id: "docs".into(),
        normalized_url: url.into(),
        schedule_key: "https://example.invalid".into(),
    }
}

#[test]
#[ignore = "requires disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn postgres_sitemap_competing_claims_and_atomic_idempotent_commit_survive_restart() {
    let (run, mut store) = SitemapRun::new("atomic");
    let tasks = [
        run.task(&mut store, "https://example.invalid/one", "scanner-one"),
        run.task(&mut store, "https://example.invalid/two", "scanner-two"),
    ];
    let barrier = Arc::new(Barrier::new(3));
    let handles = tasks
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, task)| {
            let database_url = run.database_url.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let mut store = FrontierStore::connect(&database_url).unwrap();
                barrier.wait();
                (index, store.claim_sitemap_discovery(&task, 2).unwrap())
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let claims = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    let winners = claims
        .iter()
        .filter(|(_, epoch)| epoch.is_some())
        .collect::<Vec<_>>();
    assert_eq!(
        winners.len(),
        1,
        "one origin scan across independent PostgreSQL clients"
    );
    let (index, epoch) = *winners[0];
    let task = &tasks[index];
    let epoch = epoch.unwrap();
    assert_eq!(store.claim_sitemap_discovery(task, 2).unwrap(), Some(epoch));

    let committed_url = "https://example.invalid/sitemap-page";
    let mut invalid = discovery("not a URL");
    invalid.source_id = "docs".into();
    assert!(
        store
            .commit_sitemap_discovery(
                task,
                &task.origin,
                epoch,
                &sha256_hex(b"rollback"),
                &[discovery(committed_url), invalid],
                0,
                4,
                20,
                "",
                false,
            )
            .is_err()
    );
    assert_eq!(
        run.count_url(&mut store, committed_url),
        0,
        "failed transaction cannot enqueue a prefix"
    );
    assert_eq!(
        store.claim_sitemap_discovery(task, 2).unwrap(),
        Some(epoch),
        "failed transaction cannot mark scan complete"
    );

    let hash = sha256_hex(b"fixed-sitemap-completion");
    let completion = store
        .commit_sitemap_discovery(
            task,
            &task.origin,
            epoch,
            &hash,
            &[discovery(committed_url), discovery(committed_url)],
            2,
            4,
            20,
            "",
            false,
        )
        .unwrap()
        .unwrap();
    assert!(completion.terminal);
    assert_eq!(completion.accepted_discovered_count, 1);
    assert_eq!(completion.rejected_discovered_count, 2);
    assert_eq!(run.count_url(&mut store, committed_url), 1);
    drop(store);
    let mut store = run.connect();
    let repeated = store
        .commit_sitemap_discovery(
            task,
            &task.origin,
            epoch,
            &hash,
            &[discovery(committed_url), discovery(committed_url)],
            2,
            4,
            20,
            "",
            false,
        )
        .unwrap()
        .unwrap();
    assert_eq!(repeated.terminal, completion.terminal);
    assert_eq!(
        repeated.accepted_discovered_count,
        completion.accepted_discovered_count
    );
    assert_eq!(
        repeated.rejected_discovered_count,
        completion.rejected_discovered_count
    );
    assert!(
        store
            .commit_sitemap_discovery(
                task,
                &task.origin,
                epoch,
                &sha256_hex(b"conflicting-completion"),
                &[discovery(committed_url)],
                0,
                4,
                20,
                "",
                false,
            )
            .is_err(),
        "a discovery epoch cannot accept conflicting payload identities"
    );
    assert_eq!(
        store.claim_sitemap_discovery(&tasks[1 - index], 2).unwrap(),
        None
    );
    // Sitemap discoveries must survive the page's subsequent failure.
    assert_eq!(
        store
            .fail_task(
                task,
                "page request failed after scan",
                Duration::from_millis(1)
            )
            .unwrap(),
        Some(TaskStatus::Pending)
    );
    assert_eq!(run.count_url(&mut store, committed_url), 1);
    assert_eq!(
        store.claim_sitemap_discovery(&tasks[1 - index], 2).unwrap(),
        None
    );
    let repeated_after_page_failure = store
        .commit_sitemap_discovery(
            task,
            &task.origin,
            epoch,
            &hash,
            &[discovery(committed_url), discovery(committed_url)],
            2,
            4,
            20,
            "",
            false,
        )
        .unwrap()
        .unwrap();
    assert_eq!(repeated_after_page_failure.accepted_discovered_count, 1);
    assert!(repeated_after_page_failure.terminal);
}

#[test]
#[ignore = "requires disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn postgres_sitemap_lease_expiry_during_enqueue_rolls_back_discoveries() {
    let (run, mut store) = SitemapRun::new("commit-expiry");
    let task = run.task(&mut store, "https://example.invalid/seed", "slow-committer");
    let epoch = store.claim_sitemap_discovery(&task, 2).unwrap().unwrap();
    assert_eq!(
        store
            .finalize_abandoned_sitemap_discovery(&run.run_id)
            .unwrap(),
        0
    );
    let state: String = store
        .client
        .query_one(
            "SELECT state FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1 AND origin=$2",
            &[&run.run_id, &task.origin],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        state, "running",
        "stop cleanup must preserve live in-flight discovery"
    );
    let prefix_url = "https://example.invalid/rollback-prefix";
    let blocked_url = "https://example.invalid/blocked-insert";
    let mut blocking_client = Client::connect(&run.database_url, NoTls).unwrap();
    let mut blocking_transaction = blocking_client.transaction().unwrap();
    blocking_transaction.execute(
        "INSERT INTO scout_crawl_tasks(crawl_run_id,source_id,origin,normalized_url,schedule_key,max_task_attempts,idempotency_key) VALUES($1,'docs','https://example.invalid',$2,'https://example.invalid',4,$3)",
        &[&run.run_id, &blocked_url, &sha256_hex(format!("blocking-insert-{}", run.run_id).as_bytes())],
    ).unwrap();
    store.client.execute(
        "UPDATE scout_crawl_tasks SET lease_deadline=clock_timestamp()+interval '1 second' WHERE task_id=$1",
        &[&task.task_id],
    ).unwrap();
    let application_name = format!(
        "sitemap-expiry-{}",
        &sha256_hex(run.run_id.as_bytes())[..16]
    );
    store
        .client
        .query_one(
            "SELECT set_config('application_name',$1,FALSE)",
            &[&application_name],
        )
        .unwrap();
    let handle = thread::spawn(move || {
        store.commit_sitemap_discovery(
            &task,
            &task.origin,
            epoch,
            &sha256_hex(b"expiry-during-enqueue"),
            &[discovery(prefix_url), discovery(blocked_url)],
            0,
            4,
            20,
            "",
            false,
        )
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        // PostgreSQL caches statistics snapshots even inside READ COMMITTED transactions.
        blocking_transaction
            .query_one("SELECT pg_stat_clear_snapshot()", &[])
            .unwrap();
        let waiting: bool = blocking_transaction.query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')",
            &[&application_name],
        ).unwrap().get(0);
        if waiting {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "discovery commit did not reach blocked enqueue"
        );
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(1100));
    blocking_transaction.commit().unwrap();
    assert!(
        handle.join().unwrap().unwrap().is_none(),
        "expired task cannot commit a new discovery result"
    );
    let mut store = run.connect();
    assert_eq!(
        run.count_url(&mut store, prefix_url),
        0,
        "enqueue prefix must roll back with stale commit"
    );
    assert_eq!(
        run.count_url(&mut store, blocked_url),
        1,
        "only independently committed blocking row survives"
    );
    assert_eq!(
        store
            .finalize_abandoned_sitemap_discovery(&run.run_id)
            .unwrap(),
        1
    );
    let finalized = store
        .client
        .query_one(
            "SELECT state,error_reason FROM scout_crawl_sitemap_discovery WHERE crawl_run_id=$1",
            &[&run.run_id],
        )
        .unwrap();
    assert_eq!(finalized.get::<_, String>(0), "failed");
    assert_eq!(
        finalized.get::<_, String>(1),
        "frontier/run stopped before discovery committed"
    );
    assert_eq!(
        store
            .finalize_abandoned_sitemap_discovery(&run.run_id)
            .unwrap(),
        0
    );
}

#[test]
#[ignore = "requires disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn postgres_sitemap_expired_owner_takeover_fences_task_and_discovery_epochs() {
    let (run, mut store) = SitemapRun::new("takeover");
    let old_task = run.task(
        &mut store,
        "https://example.invalid/only-seed",
        "dead-scanner",
    );
    let old_epoch = store
        .claim_sitemap_discovery(&old_task, 3)
        .unwrap()
        .unwrap();
    // Expire the durable task lease to simulate a killed worker without waiting a minute.
    store.client.execute(
        "UPDATE scout_crawl_tasks SET lease_deadline=clock_timestamp()-interval '1 second' WHERE task_id=$1",
        &[&old_task.task_id],
    ).unwrap();
    drop(store);
    let mut store = run.connect();
    let new_task = store
        .claim_one(&run.run_id, "replacement-scanner", long_lease())
        .unwrap()
        .unwrap();
    assert_eq!(new_task.task_id, old_task.task_id);
    assert_eq!(new_task.lease_epoch, old_task.lease_epoch + 1);
    let new_epoch = store
        .claim_sitemap_discovery(&new_task, 3)
        .unwrap()
        .unwrap();
    assert!(new_epoch > old_epoch);
    let stale_url = "https://example.invalid/stale-discovery";
    assert!(
        store
            .commit_sitemap_discovery(
                &old_task,
                &old_task.origin,
                old_epoch,
                &sha256_hex(b"old-page-lease"),
                &[discovery(stale_url)],
                0,
                4,
                20,
                "",
                false,
            )
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .commit_sitemap_discovery(
                &new_task,
                &new_task.origin,
                old_epoch,
                &sha256_hex(b"old-discovery-epoch"),
                &[discovery(stale_url)],
                0,
                4,
                20,
                "",
                false,
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(run.count_url(&mut store, stale_url), 0);
    let current_url = "https://example.invalid/current-discovery";
    let completion = store
        .commit_sitemap_discovery(
            &new_task,
            &new_task.origin,
            new_epoch,
            &sha256_hex(b"current-discovery"),
            &[discovery(current_url)],
            0,
            4,
            20,
            "",
            false,
        )
        .unwrap()
        .unwrap();
    assert!(completion.terminal);
    assert_eq!(completion.accepted_discovered_count, 1);
    assert_eq!(run.count_url(&mut store, current_url), 1);
    assert_eq!(store.claim_sitemap_discovery(&old_task, 3).unwrap(), None);
}

#[test]
#[ignore = "requires disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn postgres_sitemap_retry_exhaustion_permanent_failures_and_frontier_limits_are_durable() {
    let (run, mut store) = SitemapRun::new("retry");
    let task = run.task(
        &mut store,
        "https://example.invalid/retry-seed",
        "retry-scanner",
    );
    let first_epoch = store.claim_sitemap_discovery(&task, 2).unwrap().unwrap();
    let first = store
        .commit_sitemap_discovery(
            &task,
            &task.origin,
            first_epoch,
            &sha256_hex(b"first-partial-error"),
            &[discovery("https://example.invalid/partial")],
            0,
            4,
            20,
            "temporary sitemap fetch failure",
            true,
        )
        .unwrap()
        .unwrap();
    assert!(!first.terminal);
    assert_eq!(first.accepted_discovered_count, 1);
    assert_eq!(
        run.count_url(&mut store, "https://example.invalid/partial"),
        1
    );
    drop(store);
    let mut store = run.connect();
    let second_epoch = store.claim_sitemap_discovery(&task, 2).unwrap().unwrap();
    assert!(second_epoch > first_epoch);
    let second = store
        .commit_sitemap_discovery(
            &task,
            &task.origin,
            second_epoch,
            &sha256_hex(b"second-transient-error"),
            &[],
            0,
            4,
            20,
            "temporary sitemap fetch failure",
            true,
        )
        .unwrap()
        .unwrap();
    assert!(
        second.terminal,
        "discovery attempts must be bounded independently of page attempts"
    );
    assert_eq!(store.claim_sitemap_discovery(&task, 2).unwrap(), None);
    assert_eq!(
        run.count_url(&mut store, "https://example.invalid/partial"),
        1
    );

    let permanent_task = run.task(
        &mut store,
        "https://other.invalid/permanent-seed",
        "permanent-scanner",
    );
    let permanent_epoch = store
        .claim_sitemap_discovery(&permanent_task, 3)
        .unwrap()
        .unwrap();
    let permanent = store
        .commit_sitemap_discovery(
            &permanent_task,
            &permanent_task.origin,
            permanent_epoch,
            &sha256_hex(b"permanent-error"),
            &[],
            0,
            4,
            20,
            "sitemap denied by robots",
            false,
        )
        .unwrap()
        .unwrap();
    assert!(permanent.terminal);
    assert_eq!(
        store.claim_sitemap_discovery(&permanent_task, 3).unwrap(),
        None
    );

    let limited_task = run.task(&mut store, "https://limit.invalid/seed", "bounded-scanner");
    let limited_epoch = store
        .claim_sitemap_discovery(&limited_task, 2)
        .unwrap()
        .unwrap();
    let active: i64 = store.client.query_one(
        "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id=$1 AND status IN ('pending','leased')",
        &[&run.run_id],
    ).unwrap().get(0);
    let limited = store
        .commit_sitemap_discovery(
            &limited_task,
            &limited_task.origin,
            limited_epoch,
            &sha256_hex(b"frontier-bounded"),
            &[
                discovery("https://example.invalid/capped-a"),
                discovery("https://example.invalid/capped-b"),
            ],
            0,
            4,
            active as usize + 1,
            "",
            false,
        )
        .unwrap()
        .unwrap();
    assert!(limited.terminal);
    assert_eq!(limited.accepted_discovered_count, 1);
    assert_eq!(limited.rejected_discovered_count, 1);
    assert_eq!(
        run.count_url(&mut store, "https://example.invalid/capped-a"),
        1
    );
    assert_eq!(
        run.count_url(&mut store, "https://example.invalid/capped-b"),
        0
    );
}
