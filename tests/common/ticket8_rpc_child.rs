use super::{CoordinatorClient, RESULT_PREFIX, VERSION, lease_json, proto};
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

pub async fn child_rpc() {
    let Ok(action) = std::env::var("SCOUT_RPC_ACTION") else {
        return;
    };
    let endpoint = std::env::var("SCOUT_RPC_ENDPOINT").unwrap();
    let worker = std::env::var("SCOUT_RPC_WORKER").unwrap();
    let timeout = Duration::from_millis(
        std::env::var("SCOUT_RPC_TIMEOUT_MS")
            .unwrap()
            .parse()
            .unwrap(),
    );
    let mut client =
        tokio::time::timeout(Duration::from_secs(4), CoordinatorClient::connect(endpoint))
            .await
            .unwrap()
            .unwrap();
    let registration = tokio::time::timeout(
        Duration::from_secs(4),
        client.register_worker(proto::RegisterWorkerRequest {
            worker_id: worker.clone(),
            supported_protocol_versions: vec![VERSION],
        }),
    )
    .await
    .unwrap()
    .unwrap()
    .into_inner();
    assert_eq!(registration.protocol_version, VERSION);
    if let (Ok(ready), Ok(go)) = (
        std::env::var("SCOUT_RPC_READY"),
        std::env::var("SCOUT_RPC_GO"),
    ) {
        fs::write(ready, "ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        while !PathBuf::from(&go).exists() {
            assert!(Instant::now() < deadline, "start gate timed out");
            thread::sleep(Duration::from_millis(5));
        }
    }
    let lease: Option<proto::Lease> = std::env::var("SCOUT_RPC_LEASE").ok().map(|value| {
        let v: serde_json::Value = serde_json::from_str(&value).unwrap();
        proto::Lease {
            task_id: v["task_id"].as_i64().unwrap(),
            crawl_run_id: v["crawl_run_id"].as_str().unwrap().into(),
            source_id: v["source_id"].as_str().unwrap().into(),
            origin: v["origin"].as_str().unwrap().into(),
            normalized_url: v["normalized_url"].as_str().unwrap().into(),
            schedule_key: v["schedule_key"].as_str().unwrap().into(),
            task_attempt_count: v["task_attempt_count"].as_i64().unwrap() as i32,
            lease_owner: v["lease_owner"].as_str().unwrap().into(),
            lease_epoch: v["lease_epoch"].as_i64().unwrap(),
            lease_deadline_unix_ms: v["lease_deadline_unix_ms"].as_i64().unwrap(),
            idempotency_key: v["idempotency_key"].as_str().unwrap().into(),
        }
    });
    let result = match action.as_str() {
        "claim" => match tokio::time::timeout(
            timeout,
            client.claim_one(proto::ClaimOneRequest {
                protocol_version: VERSION,
                worker_id: worker,
            }),
        )
        .await
        {
            Ok(Ok(response)) => {
                let lease = response.into_inner().lease;
                serde_json::json!({"lease": lease.as_ref().map(lease_json)})
            }
            Ok(Err(error)) => serde_json::json!({"error": format!("{:?}", error.code())}),
            Err(_) => serde_json::json!({"lease": null, "timeout": true}),
        },
        "renew" => match tokio::time::timeout(
            timeout,
            client.renew_lease(proto::LeaseRequest {
                protocol_version: VERSION,
                lease,
            }),
        )
        .await
        {
            Ok(Ok(response)) => serde_json::json!({"renewed": response.into_inner().renewed}),
            Ok(Err(error)) => serde_json::json!({"error": format!("{:?}", error.code())}),
            Err(_) => serde_json::json!({"timeout": true}),
        },
        "complete" => {
            let rejection_reason =
                std::env::var("SCOUT_RPC_REJECTION").unwrap_or_else(|_| "fixture rejection".into());
            let request = proto::CompleteTaskRequest {
                protocol_version: VERSION,
                lease,
                capture: None,
                discovered_urls: Vec::new(),
                rejection_reason,
            };
            match tokio::time::timeout(timeout, client.complete_task(request)).await {
                Ok(Ok(response)) => {
                    let r = response.into_inner();
                    serde_json::json!({"completed":r.completed,"accepted":r.accepted_discovered_count,"rejected":r.rejected_discovered_count})
                }
                Ok(Err(error)) => {
                    serde_json::json!({"error": format!("{:?}", error.code()), "message": error.message()})
                }
                Err(_) => serde_json::json!({"timeout": true}),
            }
        }
        "fail" => {
            let request = proto::FailTaskRequest {
                protocol_version: VERSION,
                lease,
                error_code: "fixture_retry".into(),
                error_message: "test retry limit".into(),
                retry_delay_ms: 1,
            };
            match tokio::time::timeout(timeout, client.fail_task(request)).await {
                Ok(Ok(response)) => serde_json::json!({"status": response.into_inner().status}),
                Ok(Err(error)) => serde_json::json!({
                    "error": format!("{:?}", error.code()),
                    "message": error.message()
                }),
                Err(_) => serde_json::json!({"timeout": true}),
            }
        }
        "robots" => {
            let origin = std::env::var("SCOUT_RPC_ORIGIN").unwrap();
            let path = std::env::var("SCOUT_RPC_PATH").unwrap_or_else(|_| "/".into());
            let request = proto::RobotsDecisionRequest {
                protocol_version: VERSION,
                lease,
                origin,
                path,
            };
            match tokio::time::timeout(timeout, client.robots_decision(request)).await {
                Ok(Ok(response)) => {
                    let r = response.into_inner();
                    serde_json::json!({"decision":r.decision,"epoch":r.refresh_epoch,"allowed":r.allowed})
                }
                Ok(Err(error)) => serde_json::json!({"error": format!("{:?}", error.code())}),
                Err(_) => serde_json::json!({"timeout": true}),
            }
        }
        "commit_robots" => {
            let request = proto::CommitRobotsRefreshRequest {
                protocol_version: VERSION,
                lease,
                origin: std::env::var("SCOUT_RPC_ORIGIN").unwrap(),
                refresh_epoch: std::env::var("SCOUT_RPC_EPOCH").unwrap().parse().unwrap(),
                policy: Some(proto::RobotsPolicy {
                    groups: Vec::new(),
                    sitemaps: Vec::new(),
                }),
                body_sha256: "ab".repeat(32),
            };
            match tokio::time::timeout(timeout, client.commit_robots_refresh(request)).await {
                Ok(Ok(response)) => serde_json::json!({"success":response.into_inner().success}),
                Ok(Err(error)) => serde_json::json!({"error": format!("{:?}", error.code())}),
                Err(_) => serde_json::json!({"timeout": true}),
            }
        }
        "sitemap_claim" => {
            match tokio::time::timeout(
                timeout,
                client.claim_sitemap_discovery(proto::SitemapDiscoveryRequest {
                    protocol_version: VERSION,
                    lease,
                    origin: std::env::var("SCOUT_RPC_ORIGIN").unwrap(),
                }),
            )
            .await
            {
                Ok(Ok(response)) => {
                    let response = response.into_inner();
                    serde_json::json!({"claimed": response.claimed, "epoch": response.scan_epoch})
                }
                Ok(Err(error)) => serde_json::json!({"error": format!("{:?}", error.code())}),
                Err(_) => serde_json::json!({"timeout": true}),
            }
        }
        "sitemap_commit" => {
            let discovered_urls = std::env::var("SCOUT_RPC_DISCOVERED_URLS")
                .unwrap_or_default()
                .split('|')
                .filter(|url| !url.is_empty())
                .map(str::to_owned)
                .collect();
            match tokio::time::timeout(
                timeout,
                client.commit_sitemap_discovery(proto::CommitSitemapDiscoveryRequest {
                    protocol_version: VERSION,
                    lease,
                    origin: std::env::var("SCOUT_RPC_ORIGIN").unwrap(),
                    scan_epoch: std::env::var("SCOUT_RPC_EPOCH").unwrap().parse().unwrap(),
                    discovered_urls,
                    error_reason: String::new(),
                    retryable: false,
                }),
            )
            .await
            {
                Ok(Ok(response)) => {
                    let response = response.into_inner();
                    serde_json::json!({"committed": response.committed, "terminal": response.terminal})
                }
                Ok(Err(error)) => serde_json::json!({"error": format!("{:?}", error.code())}),
                Err(_) => serde_json::json!({"timeout": true}),
            }
        }
        "permit" => {
            let lease = lease.unwrap();
            let request = proto::AcquireRequestPermitRequest {
                protocol_version: VERSION,
                url: lease.normalized_url.clone(),
                lease: Some(lease),
                request_id: std::env::var("SCOUT_RPC_REQUEST_ID").unwrap(),
                request_kind: "page".into(),
            };
            match tokio::time::timeout(timeout, client.acquire_request_permit(request)).await {
                Ok(Ok(response)) => {
                    let r = response.into_inner();
                    serde_json::json!({"decision":r.decision,"permit_id":r.permit_id})
                }
                Ok(Err(error)) => {
                    serde_json::json!({"error":format!("{:?}", error.code()),"message":error.message()})
                }
                Err(_) => serde_json::json!({"timeout": true}),
            }
        }
        "cancel" => {
            let lease = lease.unwrap();
            let request = proto::CancelRequestWaitRequest {
                protocol_version: VERSION,
                crawl_run_id: lease.crawl_run_id,
                worker_id: worker,
                request_id: std::env::var("SCOUT_RPC_REQUEST_ID").unwrap(),
            };
            match tokio::time::timeout(timeout, client.cancel_request_wait(request)).await {
                Ok(Ok(response)) => serde_json::json!({"success":response.into_inner().success}),
                Ok(Err(error)) => serde_json::json!({"error":format!("{:?}", error.code())}),
                Err(_) => serde_json::json!({"timeout": true}),
            }
        }
        _ => panic!("unsupported child RPC action {action}"),
    };
    println!("{RESULT_PREFIX}{result}");
}
