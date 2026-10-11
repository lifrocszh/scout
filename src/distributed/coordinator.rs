use super::frontier::{
    AcceptedCapture, AcceptedCaptureRecord, ClaimedTask, DiscoveredTask, FrontierStore,
    LeaseTiming, OriginPolicyLimit, PermitDecision, RequestPolicyLimits, RobotsDecision,
    TaskStatus,
};
use super::proto;
use super::protocol::{check_protocol, safe_event_url, validate_identity};
use super::{MAX_BODY_TRANSFER_BYTES, MAX_RPC_MESSAGE_BYTES, MAX_URL_BYTES, PROTOCOL_VERSION};
use crate::app::report_error;
use crate::cli::{Cli, Operation};
use crate::crawl::spool::CaptureSpool;
use crate::crawl::{
    Config, RobotsGroup, RobotsPolicy, RobotsRule, Source, load_config, schedule_key,
    sitemap_url_allowed,
};
use crate::error::Error;
use crate::observability::{Events, emit_shared};
use crate::storage::new_run_id;
use crate::url::{normalize_origin, normalize_url, origin_key};
use ::url::Url;
use proto::coordinator_server::{Coordinator, CoordinatorServer};
use std::collections::HashSet;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status, transport::Server};

mod capture;
mod results;
mod rpc;
mod scheduling;
mod sitemaps;
use capture::{
    StoredCapture, completion_result_sha256, eligible_discoveries, replay_accepted_captures,
    validate_capture, write_body_file,
};

struct CoordinatorState {
    core: Mutex<CoordinatorCore>,
    shutdown: tokio::sync::Notify,
    schedule_changed: Arc<tokio::sync::Notify>,
    results: Arc<results::ResultPool>,
}

struct CoordinatorCore {
    frontier: FrontierStore,
    spool: CaptureSpool,
    crawl_dir: PathBuf,
    config: Config,
    run_id: String,
    lease_timing: LeaseTiming,
    registered_workers: HashSet<String>,
    fatal_error: Option<String>,
    events: Arc<Mutex<Events>>,
    waits: scheduling::Waits,
}

impl CoordinatorCore {
    fn target_reached(&mut self) -> Result<bool, Status> {
        let counts = self
            .frontier
            .content_group_counts_by_source(&self.run_id)
            .map_err(to_status)?;
        let total: usize = counts.values().sum();
        let quotas_met = self.config.sources.iter().all(|source| {
            source.minimum_page_quota == 0
                || counts.get(&source.source_id).copied().unwrap_or_default()
                    >= source.minimum_page_quota
        });
        if total >= self.config.page_target && quotas_met {
            return Ok(true);
        }
        self.frontier
            .run_duration_exceeded(
                &self.run_id,
                Duration::from_secs(self.config.max_duration_seconds),
            )
            .map_err(to_status)
    }

    fn quota_satisfied_source_ids(&mut self) -> Result<Vec<String>, Status> {
        let counts = self
            .frontier
            .content_group_counts_by_source(&self.run_id)
            .map_err(to_status)?;
        let total: usize = counts.values().sum();
        let quotas_met = self.config.sources.iter().all(|source| {
            source.minimum_page_quota == 0
                || counts.get(&source.source_id).copied().unwrap_or_default()
                    >= source.minimum_page_quota
        });
        if quotas_met && total < self.config.page_target {
            return Ok(Vec::new());
        }
        Ok(self
            .config
            .sources
            .iter()
            .filter(|source| {
                source.minimum_page_quota > 0
                    && counts.get(&source.source_id).copied().unwrap_or_default()
                        >= source.minimum_page_quota
            })
            .map(|source| source.source_id.clone())
            .collect())
    }

    fn validate_lease(&mut self, lease: &proto::Lease) -> Result<ClaimedTask, Status> {
        let task = claimed_task(lease)?;
        self.validate_task_identity(&task)?;
        if !self.frontier.lease_is_live(&task).map_err(to_status)? {
            return Err(Status::failed_precondition(
                "task lease is stale or unknown",
            ));
        }
        Ok(task)
    }

    fn validate_task_identity(&self, task: &ClaimedTask) -> Result<(), Status> {
        if task.crawl_run_id != self.run_id || !self.registered_workers.contains(&task.lease_owner)
        {
            return Err(Status::failed_precondition(
                "task lease is stale or unknown",
            ));
        }
        let owner = owner_for(&self.config, &task.normalized_url)
            .ok_or_else(|| Status::permission_denied("leased URL is outside configured Sources"))?;
        if owner.source_id != task.source_id
            || origin_key(
                &Url::parse(&task.normalized_url)
                    .map_err(|_| Status::invalid_argument("leased URL is invalid"))?,
            ) != task.origin
        {
            return Err(Status::permission_denied(
                "leased URL ownership does not match Source",
            ));
        }
        Ok(())
    }

    fn validate_request_url(&self, value: &str, kind: &str) -> Result<(String, Url), Status> {
        if value.len() > MAX_URL_BYTES {
            return Err(Status::invalid_argument("request URL exceeds size limit"));
        }
        let parsed =
            Url::parse(value).map_err(|_| Status::invalid_argument("request URL is invalid"))?;
        let normalized = normalize_url(parsed.clone())
            .map_err(|_| Status::invalid_argument("request URL is not HTTP(S)"))?;
        if normalized != value {
            return Err(Status::invalid_argument("request URL must be normalized"));
        }
        let origin = origin_key(&parsed);
        let allowed_origin = self.config.sources.iter().any(|source| {
            source
                .allowed_origins
                .iter()
                .any(|allowed| allowed == &origin)
        });
        if !allowed_origin {
            return Err(Status::permission_denied(
                "request origin is outside configured Sources",
            ));
        }
        let allowed = match kind {
            "page" => owner_for(&self.config, &normalized).is_some(),
            "robots" => true,
            "sitemap" => sitemap_url_allowed(&self.config, &origin, &normalized),
            _ => return Err(Status::invalid_argument("unknown request kind")),
        };
        if !allowed {
            return Err(Status::permission_denied(
                "request URL is outside configured policy",
            ));
        }
        Ok((origin, parsed))
    }
}

#[derive(Clone)]
struct CoordinatorRpc {
    state: Arc<CoordinatorState>,
}

pub(crate) fn run_coordinator(cli: &Cli) -> i32 {
    let Operation::Coordinator {
        config: config_path,
        bind,
        run_id,
    } = &cli.operation
    else {
        unreachable!("coordinator operation required")
    };
    match coordinator_main(cli, config_path, bind, run_id.as_deref()) {
        Ok(()) => 0,
        Err(error) => {
            report_error(cli, &error);
            error.exit_code
        }
    }
}

fn coordinator_main(
    cli: &Cli,
    config_path: &std::path::Path,
    bind: &str,
    requested_run_id: Option<&str>,
) -> Result<(), Error> {
    let address = bind
        .parse::<SocketAddr>()
        .map_err(|error| Error::invalid(format!("invalid coordinator bind address: {error}")))?;
    if !private_ip(address.ip()) {
        return Err(Error::invalid(
            "coordinator bind must use loopback, private, link-local, or Tailscale IP",
        ));
    }
    let config = load_config(config_path)?;
    if config.max_body_bytes > MAX_BODY_TRANSFER_BYTES
        || config.max_extraction_bytes > MAX_BODY_TRANSFER_BYTES
    {
        return Err(Error::invalid(format!(
            "distributed body and extraction limits must not exceed {MAX_BODY_TRANSFER_BYTES} bytes"
        )));
    }
    let database_url = std::env::var("SCOUT_DATABASE_URL")
        .map_err(|_| Error::invalid("SCOUT_DATABASE_URL must be set on coordinator"))?;
    let run_id = requested_run_id
        .map(str::to_owned)
        .unwrap_or_else(new_run_id);
    validate_identity(&run_id, "crawl run ID")
        .map_err(|status| Error::invalid(status.message().to_owned()))?;
    if !run_id.strip_prefix("run-").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        return Err(Error::invalid(
            "crawl run ID must be run- followed by 64 lowercase hexadecimal characters",
        ));
    }
    let mut frontier = FrontierStore::connect(&database_url)?;
    frontier.apply_migrations()?;
    let timeout = Duration::from_secs(config.timeout_seconds);
    let lease_timing = LeaseTiming::for_request_timeout(timeout)?;
    let policy_limits = RequestPolicyLimits {
        global_concurrency: config.global_concurrency,
        origin_concurrency: config.origin_concurrency,
        min_start_spacing: Duration::from_millis(config.min_start_spacing_ms),
        request_timeout: timeout,
        permit_safety_margin: Duration::from_secs((config.timeout_seconds / 3).max(1)),
        robots_cache_ttl: Duration::from_secs(config.robots_cache_seconds),
        max_total_attempts: config.max_total_attempts,
        max_pending_requests: config.max_frontier,
    };
    let mut seen_origins = HashSet::new();
    let mut origin_policies = Vec::new();
    for source in &config.sources {
        for origin in &source.allowed_origins {
            if seen_origins.insert(origin.clone()) {
                let policy = config.origin_policy(origin);
                origin_policies.push(OriginPolicyLimit {
                    origin: origin.clone(),
                    schedule_key: source
                        .politeness_group
                        .as_ref()
                        .map(|group| format!("group:{group}"))
                        .unwrap_or_else(|| origin.clone()),
                    origin_concurrency: policy.origin_concurrency,
                    min_start_spacing_ms: policy.min_start_spacing_ms,
                });
            }
        }
    }
    frontier.configure_run(&run_id, &config.digest, policy_limits, &origin_policies)?;
    // Pending RPC connections do not survive restart; active HTTP permits still do.
    frontier.clear_pending_request_waits(&run_id)?;
    let crawl_dir = cli.data_dir.join("crawls").join(&run_id);
    let recovered = (|| {
        fs::create_dir_all(crawl_dir.join("bodies")).map_err(|error| {
            Error::storage(format!("create distributed crawl directory: {error}"))
        })?;
        let spool = CaptureSpool::open(&crawl_dir, config.max_frontier)?;
        replay_accepted_captures(&mut frontier, &run_id, &crawl_dir, &spool)?;
        Ok::<_, Error>(spool)
    })();
    let spool = match recovered {
        Ok(spool) => spool,
        Err(error) => {
            let _ = frontier.mark_run_failed(&run_id);
            return Err(Error::storage(error.message));
        }
    };
    let completion_lanes = results::ResultPool::lane_count()?;
    let (results, result_workers) =
        results::ResultPool::new(&database_url, completion_lanes, config.max_body_bytes)?;
    let results = Arc::new(results);
    let (run_events, first_start) = Events::open_or_append(&cli.data_dir, &run_id, "coordinator")?;
    let events = Arc::new(Mutex::new(run_events));
    if first_start {
        emit_shared(
            &events,
            "crawl.started",
            "success",
            serde_json::json!({"config_path": config_path, "data_dir": cli.data_dir}),
        );
        emit_shared(
            &events,
            "crawl.policy",
            "success",
            serde_json::json!(config.policy()),
        );
        emit_shared(
            &events,
            "crawl.config",
            "success",
            serde_json::json!({
                "source_config_sha256": config.digest,
                "sources": &config.sources
            }),
        );
    } else {
        emit_shared(
            &events,
            "coordinator.resumed",
            "success",
            serde_json::json!({"crawl_run_id": run_id}),
        );
    }
    for source in &config.sources {
        for seed in &source.seeds {
            let url = Url::parse(seed).expect("validated seed URL");
            frontier.enqueue_scheduled_url(
                &run_id,
                &source.source_id,
                seed,
                config.max_attempts as i32,
                &schedule_key(&config, &url),
            )?;
        }
    }
    emit_shared(
        &events,
        "coordinator.started",
        "success",
        serde_json::json!({
            "bind": address,
            "protocol_version": PROTOCOL_VERSION,
            "crawl_run_id": run_id,
            "page_target": config.page_target,
            "database_credentials": "coordinator_only",
            "auth": "tailnet_acl",
            "completion_lanes": completion_lanes,
        }),
    );
    println!("scout coordinator listening on {address}");
    let state = Arc::new(CoordinatorState {
        core: Mutex::new(CoordinatorCore {
            frontier,
            spool,
            crawl_dir,
            config,
            run_id,
            lease_timing,
            registered_workers: HashSet::new(),
            fatal_error: None,
            events,
            waits: scheduling::Waits::default(),
        }),
        shutdown: tokio::sync::Notify::new(),
        schedule_changed: results.notify(),
        results: results.clone(),
    });
    let service = CoordinatorRpc {
        state: state.clone(),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::storage(format!("start coordinator runtime: {error}")))?;
    let shutdown_state = state.clone();
    let runtime_state = state.clone();
    let result = runtime.block_on(async move {
        let (config, run_id, spool, events) = {
            let core = runtime_state.core.lock().expect("coordinator state lock");
            (
                core.config.clone(),
                core.run_id.clone(),
                core.spool.clone(),
                core.events.clone(),
            )
        };
        let result_handles = runtime_state.results.start(
            result_workers,
            Arc::downgrade(&runtime_state),
            config,
            run_id,
            spool,
            events,
        );
        let dispatcher = tokio::spawn(scheduling::run(runtime_state.clone()));
        let server_result = Server::builder()
            .add_service(
                CoordinatorServer::new(service)
                    .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES),
            )
            .serve_with_shutdown(address, async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = shutdown_state.shutdown.notified() => {},
                }
                scheduling::close(&shutdown_state);
            })
            .await
            .map_err(|error| Error::storage(format!("coordinator gRPC server: {error}")));
        scheduling::close(&runtime_state);
        let _ = dispatcher.await;
        (server_result, result_handles)
    });
    let (server_result, result_handles) = result;
    let (run_events, global_concurrency, origin_concurrency, fatal_error) = {
        let core = state.core.lock().expect("coordinator state lock");
        (
            core.events.clone(),
            core.config.global_concurrency,
            core.config.origin_concurrency,
            core.fatal_error.clone(),
        )
    };
    emit_shared(
        &run_events,
        "crawl.concurrency",
        "success",
        serde_json::json!({
            "max_active_global": 0,
            "max_active_by_origin": {},
            "configured_global": global_concurrency,
            "configured_origin": origin_concurrency,
        }),
    );
    // Drop the synchronous PostgreSQL client outside Tokio; its destructor drives its own runtime.
    drop(state);
    drop(results);
    runtime.block_on(async move {
        for handle in result_handles {
            let _ = handle.await;
        }
    });
    server_result?;
    match fatal_error {
        Some(message) => Err(Error::storage(message)),
        None => Ok(()),
    }
}

fn claimed_task(lease: &proto::Lease) -> Result<ClaimedTask, Status> {
    validate_identity(&lease.lease_owner, "lease owner")?;
    if lease.task_id <= 0 || lease.lease_epoch <= 0 {
        return Err(Status::invalid_argument("invalid task lease identity"));
    }
    Ok(ClaimedTask {
        task_id: lease.task_id,
        crawl_run_id: lease.crawl_run_id.clone(),
        source_id: lease.source_id.clone(),
        origin: lease.origin.clone(),
        normalized_url: lease.normalized_url.clone(),
        schedule_key: lease.schedule_key.clone(),
        task_attempt_count: lease.task_attempt_count,
        lease_owner: lease.lease_owner.clone(),
        lease_epoch: lease.lease_epoch,
        lease_deadline_unix_ms: lease.lease_deadline_unix_ms,
        idempotency_key: lease.idempotency_key.clone(),
    })
}

fn owner_for<'a>(config: &'a Config, value: &str) -> Option<&'a Source> {
    config.sources.iter().find(|source| source.owns(value))
}

fn validate_allowed_origin(config: &Config, value: &str) -> Result<(), Status> {
    let origin = normalize_origin(value).map_err(|_| Status::invalid_argument("invalid origin"))?;
    if origin != value
        || !config.sources.iter().any(|source| {
            source
                .allowed_origins
                .iter()
                .any(|allowed| allowed == &origin)
        })
    {
        return Err(Status::permission_denied(
            "origin is outside configured Sources",
        ));
    }
    Ok(())
}

fn to_status(error: Error) -> Status {
    if error.code == "safety_cap" {
        Status::resource_exhausted(error.message)
    } else if error.exit_code == 2 {
        Status::invalid_argument(error.message)
    } else {
        Status::internal(error.message)
    }
}

fn task_status_name(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::Leased => "leased",
        TaskStatus::Succeeded => "succeeded",
        TaskStatus::Failed => "failed",
    }
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1])
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip
                    .to_ipv4_mapped()
                    .is_some_and(|mapped| private_ip(IpAddr::V4(mapped)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PROTOCOL_VERSION, StoredCapture, check_protocol, completion_result_sha256,
        eligible_discoveries, private_ip, proto, replay_accepted_captures, validate_capture,
        write_body_file,
    };
    use crate::crawl::spool::CaptureSpool;
    use crate::crawl::{Config, Source};
    use crate::distributed::frontier::{FrontierStore, LeaseTiming};
    use crate::storage::sha256_hex;
    use crate::url::{normalize_url, origin_key};
    use ::url::Url;
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::time::{SystemTime, UNIX_EPOCH};

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

    fn config() -> Config {
        Config {
            schema_version: "scout.crawl.v1".into(),
            contact_url: "https://example.invalid/contact".into(),
            user_agent: "Scout/test".into(),
            embedding_model: "test-model".into(),
            page_target: 2,
            min_content_chars: 1,
            max_body_bytes: 1_000_000,
            max_frontier: 10,
            timeout_seconds: 1,
            global_concurrency: 1,
            origin_concurrency: 1,
            min_start_spacing_ms: 0,
            origin_policies: Vec::new(),
            max_attempts: 2,
            retry_backoff_ms: 1,
            retry_max_delay_ms: 1,
            max_total_attempts: 10,
            max_duration_seconds: 10,
            robots_cache_seconds: 10,
            max_extraction_bytes: 1_000_000,
            max_extraction_millis: 1_000,
            max_passage_tokens: 64,
            sources: vec![Source {
                source_id: "docs".into(),
                name: None,
                seeds: vec!["https://example.invalid/docs".into()],
                allowed_urls: None,
                allowed_origins: vec!["https://example.invalid".into()],
                path_prefixes: vec!["/docs".into()],
                deny_path_prefixes: vec!["/docs/private".into()],
                sitemap_url_prefixes: Vec::new(),
                politeness_group: None,
                license_urls: Vec::new(),
                minimum_page_quota: 1,
            }],
            digest: "digest".into(),
        }
    }

    fn temp_dir() -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("scout-coordinator-test-{stamp}"))
    }

    fn capture_data(url: &str, body: &[u8]) -> proto::CaptureData {
        let identity = "Prose\0fixture".to_owned();
        proto::CaptureData {
            requested_url: url.into(),
            final_url: url.into(),
            redirects: Vec::new(),
            source_id: "docs".into(),
            title: "Fixture".into(),
            description: None,
            language: None,
            canonical_url: None,
            extraction_method: "cleaned_body".into(),
            content: "fixture".into(),
            identity: identity.clone(),
            content_hash: sha256_hex(identity.as_bytes()),
            blocks: Vec::new(),
            content_bytes: body.len() as u64,
            body_hash: sha256_hex(body),
            content_type: "text/html".into(),
            status_code: 200,
            body: body.to_vec(),
        }
    }

    fn lease(url: &str) -> super::ClaimedTask {
        let parsed = Url::parse(url).expect("fixture URL");
        super::ClaimedTask {
            task_id: 1,
            crawl_run_id: "run-test".into(),
            source_id: "docs".into(),
            origin: origin_key(&parsed),
            normalized_url: normalize_url(parsed.clone()).expect("normalized URL"),
            schedule_key: origin_key(&parsed),
            task_attempt_count: 1,
            lease_owner: "worker-a".into(),
            lease_epoch: 1,
            lease_deadline_unix_ms: 0,
            idempotency_key: "ab".repeat(32),
        }
    }

    #[test]
    fn protocol_messages_round_trip_and_versions_are_explicit() {
        let lease = proto::Lease {
            task_id: 17,
            crawl_run_id: "run-test".into(),
            source_id: "docs".into(),
            origin: "https://docs.example".into(),
            normalized_url: "https://docs.example/guide/".into(),
            schedule_key: "group:docs".into(),
            task_attempt_count: 1,
            lease_owner: "worker-a".into(),
            lease_epoch: 4,
            lease_deadline_unix_ms: 123456,
            idempotency_key: "ab".repeat(32),
        };
        let request = proto::LeaseRequest {
            protocol_version: PROTOCOL_VERSION,
            lease: Some(lease.clone()),
        };
        let encoded = prost::Message::encode_to_vec(&request);
        let decoded = <proto::LeaseRequest as prost::Message>::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded, request);
        assert!(check_protocol(PROTOCOL_VERSION).is_ok());
        assert!(check_protocol(PROTOCOL_VERSION + 1).is_err());
    }

    #[test]
    fn coordinator_bind_accepts_private_interfaces_only() {
        assert!(private_ip(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(private_ip("100.100.10.2".parse().unwrap()));
        assert!(private_ip("fd00::1".parse().unwrap()));
        assert!(!private_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED)));
        assert!(!private_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
        assert!(!private_ip("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn body_file_is_verified_and_an_orphan_blob_can_be_reused() {
        let root = temp_dir();
        let url = "https://example.invalid/docs/page";
        let body = b"orphan body";
        let capture = capture_data(url, body);
        write_body_file(&root, &capture).expect("write body");
        write_body_file(&root, &capture).expect("reuse body after failed DB acceptance");
        assert_eq!(
            fs::read(root.join("bodies").join(&capture.body_hash)).expect("stored body"),
            body
        );
        let mut wrong_size = capture.clone();
        wrong_size.content_bytes += 1;
        assert!(write_body_file(&root, &wrong_size).is_err());
        let mut wrong_hash = capture;
        wrong_hash.body_hash = "00".repeat(32);
        assert!(write_body_file(&root, &wrong_hash).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn capture_validation_rejects_body_size_and_digest_mismatches() {
        let url = "https://example.invalid/docs/page";
        let task = lease(url);
        let capture = capture_data(url, b"body");
        validate_capture(&config(), &task, &capture).expect("valid capture");
        let mut wrong_size = capture.clone();
        wrong_size.content_bytes += 1;
        assert!(validate_capture(&config(), &task, &wrong_size).is_err());
        let mut wrong_hash = capture;
        wrong_hash.body_hash = "00".repeat(32);
        assert!(validate_capture(&config(), &task, &wrong_hash).is_err());
    }

    #[test]
    fn discovered_urls_are_deduped_and_source_allowlisted() {
        let urls = vec![
            "https://example.invalid/docs/one".into(),
            "https://example.invalid/docs/one".into(),
            "https://example.invalid/docs/private/page".into(),
            "https://outside.invalid/docs/page".into(),
            "not a URL".into(),
        ];
        let (discovered, rejected) = eligible_discoveries(&config(), &urls, 10);
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].source_id, "docs");
        assert_eq!(
            discovered[0].normalized_url,
            "https://example.invalid/docs/one"
        );
        assert_eq!(rejected, 3);
    }

    #[test]
    fn completion_identity_is_order_independent_and_changes_with_payload() {
        let capture = capture_data("https://example.invalid/docs/page", b"body");
        let capture = StoredCapture::from(&capture);
        let forward = vec![
            "https://example.invalid/docs/b".into(),
            "https://example.invalid/docs/a".into(),
        ];
        let reverse = vec![
            "https://example.invalid/docs/a".into(),
            "https://example.invalid/docs/b".into(),
        ];
        assert_eq!(
            completion_result_sha256(Some(&capture), &forward, "").unwrap(),
            completion_result_sha256(Some(&capture), &reverse, "").unwrap()
        );
        assert_ne!(
            completion_result_sha256(Some(&capture), &forward, "").unwrap(),
            completion_result_sha256(None, &forward, "rejected").unwrap()
        );
    }

    // Run with SCOUT_TEST_DATABASE_URL set and `cargo test --lib distributed::coordinator::tests::postgres_ticket4_spool_replay_contract -- --ignored`.
    #[test]
    #[ignore = "requires a coordinator-only PostgreSQL test database"]
    fn postgres_ticket4_spool_replay_contract() {
        let database_url = std::env::var("SCOUT_TEST_DATABASE_URL")
            .expect("set SCOUT_TEST_DATABASE_URL to a disposable PostgreSQL database");
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let run_id = format!("coordinator-spool-test-{stamp}");
        let _cleanup = RunCleanup {
            database_url: database_url.clone(),
            run_id: run_id.clone(),
        };
        let mut frontier = FrontierStore::connect(&database_url).unwrap();
        frontier.apply_migrations().unwrap();
        frontier
            .ensure_run(&run_id, &sha256_hex(run_id.as_bytes()))
            .unwrap();
        let url = "https://example.invalid/docs/page";
        frontier.enqueue_url(&run_id, "docs", url, 3).unwrap();
        let task = frontier
            .claim_one(
                &run_id,
                "worker-a",
                LeaseTiming::for_request_timeout(std::time::Duration::from_secs(1)).unwrap(),
            )
            .unwrap()
            .unwrap();
        let proto_capture = capture_data(url, b"durable worker body");
        let stored = StoredCapture::from(&proto_capture);
        let accepted = stored.to_frontier_capture().unwrap();
        let result_hash = completion_result_sha256(Some(&stored), &[], "").unwrap();
        let root = temp_dir();
        let crawl_dir = root.join("crawls").join(&run_id);
        write_body_file(&crawl_dir, &proto_capture).unwrap();
        assert!(matches!(
            frontier
                .accept_task_result(&task, &result_hash, Some(&accepted), "", &[], 0, 3, 100,)
                .unwrap(),
            crate::distributed::frontier::CompletionAcceptance::Accepted { .. }
        ));
        let spool = CaptureSpool::open(&crawl_dir, 100).unwrap();
        assert_eq!(
            replay_accepted_captures(&mut frontier, &run_id, &crawl_dir, &spool).unwrap(),
            1
        );
        assert_eq!(spool.capture_count().unwrap(), 1);
        // Replaying the database projection after a coordinator restart is harmless.
        assert_eq!(
            replay_accepted_captures(&mut frontier, &run_id, &crawl_dir, &spool).unwrap(),
            1
        );
        assert_eq!(spool.capture_count().unwrap(), 1);
        let _ = fs::remove_dir_all(root);
    }
}
