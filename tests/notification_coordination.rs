//! Black-box contract for coordinator-owned waiting. Run only against a disposable DB.
use postgres::{Client, NoTls};
use sha2::{Digest, Sha256};
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
use tonic::transport::Channel;

mod proto {
    tonic::include_proto!("scout.distributed.v1");
}
use proto::coordinator_client::CoordinatorClient;

const VERSION: u32 = 3;
const ORIGIN: &str = "http://127.0.0.1:1";

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn stop_gracefully(child: &mut Child) {
    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "graceful shutdown failed");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "held RPC blocked graceful shutdown"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

struct Run {
    child: Child,
    database: Client,
    dir: PathBuf,
    id: String,
    address: SocketAddr,
}
impl Run {
    fn new() -> Self {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to disposable PostgreSQL database");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let id = format!(
            "run-{:x}",
            Sha256::digest(format!("notifications-{}-{nonce}", std::process::id()).as_bytes())
        );
        let dir = std::env::temp_dir().join(&id);
        fs::create_dir_all(&dir).unwrap();
        let config = format!(
            "schema_version = 'scout.sources.v1'\ncontact_url = 'https://example.invalid/scout'\nuser_agent = 'Scout-Test/1 (+https://example.invalid/scout)'\npage_target = 2\nmin_content_chars = 20\nmax_frontier = 8\nglobal_concurrency = 1\norigin_concurrency = 1\nmin_start_spacing_ms = 350\nrequest_timeout_seconds = 1\nmax_attempts = 2\nretry_backoff_ms = 10\nretry_max_delay_ms = 10\nmax_total_attempts = 100\nmax_duration_seconds = 30\nrobots_cache_seconds = 60\n\n[[sources]]\nsource_id = 'fixture'\nseeds = ['{ORIGIN}/one', '{ORIGIN}/two']\nallowed_origins = ['{ORIGIN}']\npath_prefixes = ['/']\n"
        );
        fs::write(dir.join("sources.toml"), config).unwrap();
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_scout"))
            .args([
                "coordinator",
                "--config",
                dir.join("sources.toml").to_str().unwrap(),
                "--data-dir",
                dir.to_str().unwrap(),
                "--bind",
                &address.to_string(),
                "--run-id",
                &id,
            ])
            .env("SCOUT_DATABASE_URL", &database_url)
            .stdout(Stdio::from(
                fs::File::create(dir.join("coordinator.stdout")).unwrap(),
            ))
            .stderr(Stdio::from(
                fs::File::create(dir.join("coordinator.stderr")).unwrap(),
            ))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(address).is_err() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "coordinator exited: {}",
                fs::read_to_string(dir.join("coordinator.stderr")).unwrap()
            );
            assert!(Instant::now() < deadline, "coordinator startup timed out");
            thread::sleep(Duration::from_millis(10));
        }
        Self {
            child,
            database: Client::connect(&database_url, NoTls).unwrap(),
            dir,
            id,
            address,
        }
    }
    fn wait_pending_count(&mut self, request_id: &str, expected: i64) {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let count: i64 = self.database.query_one("SELECT count(*) FROM scout_crawl_request_permits WHERE crawl_run_id=$1 AND request_id=$2 AND status='pending'", &[&self.id, &request_id]).unwrap().get(0);
            if count == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "pending request count never reached {expected}: {request_id}"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for Run {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = self.database.execute(
            "DELETE FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&self.id],
        );
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn permit(lease: &proto::Lease, id: &str) -> proto::AcquireRequestPermitRequest {
    proto::AcquireRequestPermitRequest {
        protocol_version: VERSION,
        lease: Some(lease.clone()),
        request_id: id.into(),
        url: lease.normalized_url.clone(),
        request_kind: "page".into(),
    }
}
fn keep_alive(
    runtime: &Runtime,
    mut client: CoordinatorClient<Channel>,
    lease: proto::Lease,
) -> tokio::task::JoinHandle<()> {
    runtime.spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let response = client
                .renew_lease(proto::LeaseRequest {
                    protocol_version: VERSION,
                    lease: Some(lease.clone()),
                })
                .await;
            if !response.is_ok_and(|response| response.into_inner().renewed) {
                return;
            }
        }
    })
}
fn held_permit(
    runtime: &Runtime,
    mut client: CoordinatorClient<Channel>,
    lease: &proto::Lease,
    id: &str,
) -> tokio::task::JoinHandle<
    Result<tonic::Response<proto::AcquireRequestPermitResponse>, tonic::Status>,
> {
    let request = permit(lease, id);
    runtime.spawn(async move { client.acquire_request_permit(request).await })
}
fn grant(
    runtime: &Runtime,
    pending: tokio::task::JoinHandle<
        Result<tonic::Response<proto::AcquireRequestPermitResponse>, tonic::Status>,
    >,
) -> proto::AcquireRequestPermitResponse {
    let response = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner()
    });
    assert_eq!(
        response.decision,
        proto::acquire_request_permit_response::Decision::Acquired as i32
    );
    response
}
fn release(
    runtime: &Runtime,
    client: &mut CoordinatorClient<Channel>,
    run: &Run,
    lease: &proto::Lease,
    id: i64,
) {
    let response = runtime
        .block_on(
            client.release_request_permit(proto::ReleaseRequestPermitRequest {
                protocol_version: VERSION,
                crawl_run_id: run.id.clone(),
                worker_id: lease.lease_owner.clone(),
                permit_id: id,
                outcome: "success".into(),
            }),
        )
        .unwrap()
        .into_inner();
    assert!(response.success);
}

#[test]
#[ignore = "requires disposable PostgreSQL database in SCOUT_TEST_DATABASE_URL"]
fn queued_calls_preserve_lease_policy_cancellation_and_shutdown() {
    let mut run = Run::new();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut client = runtime
        .block_on(CoordinatorClient::connect(format!(
            "http://{}",
            run.address
        )))
        .unwrap();
    for worker in ["owner-a", "owner-b", "idle", "extra"] {
        let response = runtime
            .block_on(client.register_worker(proto::RegisterWorkerRequest {
                worker_id: worker.into(),
                supported_protocol_versions: vec![VERSION],
            }))
            .unwrap()
            .into_inner();
        assert_eq!(response.protocol_version, VERSION);
    }
    let first = runtime
        .block_on(client.claim_one(proto::ClaimOneRequest {
            protocol_version: VERSION,
            worker_id: "owner-a".into(),
        }))
        .unwrap()
        .into_inner()
        .lease
        .unwrap();
    let second = runtime
        .block_on(client.claim_one(proto::ClaimOneRequest {
            protocol_version: VERSION,
            worker_id: "owner-b".into(),
        }))
        .unwrap()
        .into_inner()
        .lease
        .unwrap();
    assert_ne!(first.task_id, second.task_id);
    let renew_first = keep_alive(&runtime, client.clone(), first.clone());
    let renew_second = keep_alive(&runtime, client.clone(), second.clone());
    // A real worker awaiting its capacity assignment must still honor SIGINT.
    let worker_dir = run.dir.join("idle-worker");
    let mut worker = Process(
        Command::new(env!("CARGO_BIN_EXE_scout"))
            .args([
                "worker",
                "--coordinator",
                &format!("http://{}", run.address),
                "--data-dir",
                worker_dir.to_str().unwrap(),
                "--worker-id",
                "idle-process",
            ])
            .env_remove("SCOUT_BENCHMARK_WORKER_CONCURRENCY")
            .env_remove("SCOUT_DATABASE_URL")
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                fs::File::create(run.dir.join("idle-worker.stderr")).unwrap(),
            ))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if runtime
            .block_on(client.heartbeat(proto::WorkerRequest {
                protocol_version: VERSION,
                worker_id: "idle-process".into(),
            }))
            .is_ok()
        {
            break;
        }
        assert!(
            worker.0.try_wait().unwrap().is_none(),
            "idle worker exited before registration"
        );
        assert!(Instant::now() < deadline, "idle worker did not register");
        thread::sleep(Duration::from_millis(10));
    }
    thread::sleep(Duration::from_millis(100));
    stop_gracefully(&mut worker.0);

    let mut idle_client = client.clone();
    let idle = runtime.spawn(async move {
        idle_client
            .claim_one(proto::ClaimOneRequest {
                protocol_version: VERSION,
                worker_id: "idle".into(),
            })
            .await
    });
    thread::sleep(Duration::from_millis(100));
    assert!(
        !idle.is_finished(),
        "idle claim must wait for delegated work"
    );
    runtime
        .block_on(client.fail_task(proto::FailTaskRequest {
            protocol_version: VERSION,
            lease: Some(first.clone()),
            error_code: "test_retry".into(),
            error_message: "test retry".into(),
            retry_delay_ms: 10,
        }))
        .unwrap();
    renew_first.abort();
    let reassigned = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), idle)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner()
            .lease
            .unwrap()
    });
    assert_eq!(reassigned.task_id, first.task_id);
    assert_eq!(reassigned.lease_epoch, first.lease_epoch + 1);
    let renew_reassigned = keep_alive(&runtime, client.clone(), reassigned.clone());
    assert_eq!(
        runtime
            .block_on(client.acquire_request_permit(permit(&first, "stale")))
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );

    let blocker = grant(
        &runtime,
        held_permit(&runtime, client.clone(), &second, "blocker"),
    );
    let cancelled = held_permit(&runtime, client.clone(), &reassigned, "cancelled");
    run.wait_pending_count("cancelled", 1);
    assert!(
        !cancelled.is_finished(),
        "permit call must stay queued, never return Waiting"
    );
    let mut conflict = permit(&reassigned, "cancelled");
    conflict.request_kind = "robots".into();
    assert!(
        runtime
            .block_on(client.acquire_request_permit(conflict))
            .is_err(),
        "conflicting request metadata was accepted"
    );
    let response = runtime
        .block_on(client.cancel_request_wait(proto::CancelRequestWaitRequest {
            protocol_version: VERSION,
            crawl_run_id: run.id.clone(),
            worker_id: reassigned.lease_owner.clone(),
            request_id: "cancelled".into(),
        }))
        .unwrap()
        .into_inner();
    assert!(response.success);
    assert!(
        runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), cancelled)
                    .await
                    .unwrap()
                    .unwrap()
            })
            .is_err()
    );
    let disconnected = held_permit(&runtime, client.clone(), &reassigned, "disconnected");
    run.wait_pending_count("disconnected", 1);
    disconnected.abort();
    runtime.block_on(async {
        assert!(disconnected.await.unwrap_err().is_cancelled());
    });
    run.wait_pending_count("disconnected", 0);
    let replacement = held_permit(&runtime, client.clone(), &reassigned, "replacement");
    run.wait_pending_count("replacement", 1);
    release(&runtime, &mut client, &run, &second, blocker.permit_id);
    let replacement = grant(&runtime, replacement);
    release(
        &runtime,
        &mut client,
        &run,
        &reassigned,
        replacement.permit_id,
    );

    // With no active request or new work, only the durable spacing timer can grant.
    let spaced = grant(
        &runtime,
        held_permit(&runtime, client.clone(), &second, "spaced"),
    );
    let spacing_ok: bool = run.database.query_one("SELECT new.acquired_at >= old.acquired_at + interval '350 milliseconds' FROM scout_crawl_request_permits old JOIN scout_crawl_request_permits new ON old.crawl_run_id=new.crawl_run_id WHERE old.crawl_run_id=$1 AND old.request_id='replacement' AND new.request_id='spaced'", &[&run.id]).unwrap().get(0);
    assert!(spacing_ok, "timer weakened durable start spacing");

    // Simulate an unreported request. Lease renewal keeps the waiting task valid;
    // the coordinator must reclaim the permit only after timeout plus safety.
    let expired_replacement = held_permit(&runtime, client.clone(), &reassigned, "after-expiry");
    run.wait_pending_count("after-expiry", 1);
    let expired_replacement = grant(&runtime, expired_replacement);
    let expiry_ok: bool = run.database.query_one("SELECT old.status='expired' AND new.acquired_at >= old.expires_at FROM scout_crawl_request_permits old JOIN scout_crawl_request_permits new ON old.crawl_run_id=new.crawl_run_id WHERE old.crawl_run_id=$1 AND old.permit_id=$2 AND new.permit_id=$3", &[&run.id, &spaced.permit_id, &expired_replacement.permit_id]).unwrap().get(0);
    assert!(expiry_ok, "replacement overlapped uncertain active permit");
    release(
        &runtime,
        &mut client,
        &run,
        &reassigned,
        expired_replacement.permit_id,
    );
    let duplicate = runtime
        .block_on(client.acquire_request_permit(permit(&reassigned, "after-expiry")))
        .unwrap()
        .into_inner();
    assert_eq!(
        duplicate.decision,
        proto::acquire_request_permit_response::Decision::AlreadyCompleted as i32
    );

    // Every suspended claim consumes a bounded queue credit, including claims
    // that have not yet created any durable permit row.
    let mut held_claims = Vec::new();
    for index in 0..8 {
        let worker_id = format!("idle-{index}");
        runtime
            .block_on(client.register_worker(proto::RegisterWorkerRequest {
                worker_id: worker_id.clone(),
                supported_protocol_versions: vec![VERSION],
            }))
            .unwrap();
        let mut idle_client = client.clone();
        held_claims.push(runtime.spawn(async move {
            idle_client
                .claim_one(proto::ClaimOneRequest {
                    protocol_version: VERSION,
                    worker_id,
                })
                .await
        }));
    }
    thread::sleep(Duration::from_millis(100));
    assert!(held_claims.iter().all(|call| !call.is_finished()));
    let overflow = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(2),
            client.claim_one(proto::ClaimOneRequest {
                protocol_version: VERSION,
                worker_id: "extra".into(),
            }),
        )
        .await
        .unwrap()
    });
    assert_eq!(overflow.unwrap_err().code(), tonic::Code::ResourceExhausted);
    stop_gracefully(&mut run.child);
    for held_claim in held_claims {
        assert!(
            runtime
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(2), held_claim)
                        .await
                        .unwrap()
                        .unwrap()
                })
                .is_err()
        );
    }
    renew_second.abort();
    renew_reassigned.abort();
}
