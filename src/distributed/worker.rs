use super::proto;
use super::protocol::{check_protocol, safe_event_url, validate_identity};
use super::{MAX_BODY_TRANSFER_BYTES, MAX_RPC_MESSAGE_BYTES, PROTOCOL_VERSION};
use crate::app::report_error;
use crate::cli::{Cli, Operation};
use crate::crawl::{
    RobotsPolicy, extract_sitemap_locations, is_sitemap_index, normalize_sitemap_url, parse_robots,
};
use crate::error::Error;
use crate::observability::metrics::{self, Role};
use crate::observability::{Events, emit_shared, set_ready, start_exporter};
use crate::storage::{new_run_id, sha256_hex};
use crate::url::{normalize_url, origin_key};
use ::url::Url;
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

/// Seconds an in-flight task gets to finish after a stop request before its lease is released.
/// Keep it below the pod's `terminationGracePeriodSeconds`.
const DRAIN_ENV: &str = "SCOUT_WORKER_DRAIN_SECS";
const DEFAULT_DRAIN_SECS: u64 = 20;
const MAX_DRAIN_SECS: u64 = 3600;
/// When set (`1`/`true`), a worker that finds the crawl finished idles until it is stopped instead
/// of exiting 0, so a Deployment does not restart it in a loop.
const IDLE_ENV: &str = "SCOUT_WORKER_IDLE_WHEN_DONE";
const HEARTBEAT_EVERY: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn run_worker(cli: &Cli) -> i32 {
    let Operation::Worker {
        coordinator,
        worker_id,
        max_tasks,
    } = &cli.operation
    else {
        unreachable!("worker operation required")
    };
    match worker_main(cli, coordinator, worker_id.as_deref(), *max_tasks) {
        Ok(()) => 0,
        Err(error) => {
            report_error(cli, &error);
            error.exit_code
        }
    }
}

mod sitemaps;

struct WorkerConfig {
    run_id: String,
    timeout: Duration,
    renew_after: Duration,
    max_body_bytes: usize,
    max_extraction_bytes: usize,
    max_extraction_millis: u64,
    max_attempts: usize,
    retry_backoff_ms: u64,
    retry_max_delay_ms: u64,
    max_frontier: usize,
    min_content_chars: usize,
    user_agent: String,
    sources: Vec<proto::SourcePolicy>,
}

#[derive(Clone)]
struct WorkerContext {
    rpc: proto::coordinator_client::CoordinatorClient<tonic::transport::Channel>,
    http: reqwest::Client,
    worker_id: String,
    config: Arc<WorkerConfig>,
    events: Arc<Mutex<Events>>,
    drain: Duration,
    /// Set once the coordinator reports the crawl target reached and has no more work.
    crawl_finished: Arc<AtomicBool>,
}

struct TaskReport {
    capture: Option<proto::CaptureData>,
    rejection_reason: String,
    discovered_urls: Vec<String>,
}

struct HttpResponse {
    status: u16,
    content_type: String,
    location: Option<String>,
    body: Vec<u8>,
}

struct RobotsResult {
    allowed: bool,
    sitemaps: Vec<String>,
}

fn worker_main(
    cli: &Cli,
    address: &str,
    worker_id: Option<&str>,
    max_tasks: Option<usize>,
) -> Result<(), Error> {
    if !address.starts_with("http://") {
        return Err(Error::invalid(
            "worker coordinator address must use http:// on the private network",
        ));
    }
    // Held until the worker returns; SCOUT_METRICS_LINGER_SECS keeps it serving after the last task.
    let _metrics = start_exporter(cli.metrics_bind.as_deref(), Role::Worker)?;
    let worker_id = worker_id.map(str::to_owned).unwrap_or_else(new_run_id);
    validate_identity(&worker_id, "worker_id")
        .map_err(|status| Error::invalid(status.message().to_owned()))?;
    let concurrency = benchmark_concurrency()?;
    let drain = drain_timeout()?;
    let idle_when_done = env_flag(IDLE_ENV);
    if concurrency > 1 && max_tasks.is_some() {
        return Err(Error::invalid(
            "benchmark concurrency cannot be combined with --max-tasks",
        ));
    }
    let run_id = new_run_id();
    let events = Arc::new(Mutex::new(Events::open(&cli.data_dir, &run_id, "worker")?));
    let endpoint = tonic::transport::Endpoint::from_shared(address.to_owned())
        .map_err(|error| Error::invalid(format!("invalid coordinator address: {error}")))?
        .connect_timeout(Duration::from_secs(5));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::storage(format!("start worker runtime: {error}")))?;
    runtime.block_on(async move {
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| Error::storage(format!("connect to coordinator: {error}")))?;
        let mut rpc = proto::coordinator_client::CoordinatorClient::new(channel)
            .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES);
        let registered = rpc
            .register_worker(proto::RegisterWorkerRequest {
                worker_id: worker_id.clone(),
                supported_protocol_versions: vec![PROTOCOL_VERSION],
            })
            .await
            .map_err(|error| Error::invalid(format!("coordinator protocol negotiation failed: {error}")))?
            .into_inner();
        if registered.protocol_version != PROTOCOL_VERSION {
            return Err(Error::invalid(format!(
                "coordinator negotiated unsupported protocol version {}",
                registered.protocol_version
            )));
        }
        if registered.max_body_bytes as usize > MAX_BODY_TRANSFER_BYTES
            || registered.max_extraction_bytes as usize > MAX_BODY_TRANSFER_BYTES
        {
            return Err(Error::invalid("coordinator body limit exceeds worker transfer bound"));
        }
        let worker_config = Arc::new(WorkerConfig {
            run_id: registered.crawl_run_id,
            timeout: Duration::from_millis(registered.request_timeout_ms),
            renew_after: Duration::from_millis(registered.renew_after_ms),
            max_body_bytes: registered.max_body_bytes as usize,
            max_extraction_bytes: registered.max_extraction_bytes as usize,
            max_extraction_millis: registered.max_extraction_millis,
            max_attempts: registered.max_attempts as usize,
            retry_backoff_ms: registered.retry_backoff_ms,
            retry_max_delay_ms: registered.retry_max_delay_ms,
            max_frontier: registered.max_frontier as usize,
            min_content_chars: registered.min_content_chars as usize,
            user_agent: registered.user_agent,
            sources: registered.sources,
        });
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(worker_config.timeout)
            .user_agent(&worker_config.user_agent)
            .build()
            .map_err(|error| Error::storage(format!("build worker HTTP client: {error}")))?;
        let ctx = WorkerContext {
            rpc,
            http,
            worker_id: worker_id.clone(),
            config: worker_config,
            events: events.clone(),
            drain,
            crawl_finished: Arc::new(AtomicBool::new(false)),
        };
        emit_shared(
            &events,
            "worker.started",
            "success",
            serde_json::json!({"worker_id": worker_id, "coordinator": address, "protocol_version": PROTOCOL_VERSION, "concurrency": concurrency, "prefetch": 0}),
        );
        let (shutdown, _) = watch::channel(false);
        let signal = shutdown.clone();
        tokio::spawn(async move {
            crate::shutdown::requested().await;
            // Unready first, so the orchestrator stops counting this worker while it drains.
            set_ready(false);
            signal.send_replace(true);
        });
        tokio::spawn(heartbeat_loop(ctx.clone(), shutdown.subscribe()));
        let mut lanes = tokio::task::JoinSet::new();
        for _ in 0..concurrency {
            lanes.spawn(worker_loop(ctx.clone(), shutdown.subscribe(), max_tasks));
        }
        let mut completed = 0;
        let mut failure = None;
        while let Some(result) = lanes.join_next().await {
            match result.unwrap_or_else(|error| Err(Error::storage(format!("worker lane failed: {error}")))) {
                Ok(count) => completed += count,
                Err(error) => {
                    shutdown.send_replace(true);
                    failure.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        if idle_when_done && ctx.crawl_finished.load(Ordering::Relaxed) {
            emit_shared(
                &ctx.events,
                "worker.idle",
                "success",
                serde_json::json!({"worker_id": ctx.worker_id, "reason": "crawl_finished"}),
            );
            idle_until_stopped(shutdown.subscribe()).await;
        }
        emit_shared(
            &ctx.events,
            "worker.stopped",
            "success",
            serde_json::json!({"worker_id": ctx.worker_id, "completed_tasks": completed}),
        );
        Ok(())
    })
}

// Benchmark-only load knob. Normal workers keep their one-task behavior.
fn benchmark_concurrency() -> Result<usize, Error> {
    let Some(value) = std::env::var_os("SCOUT_BENCHMARK_WORKER_CONCURRENCY") else {
        return Ok(1);
    };
    if std::env::var_os("SCOUT_DISTRIBUTED_METRICS").is_none() {
        return Err(Error::invalid(
            "benchmark concurrency requires SCOUT_DISTRIBUTED_METRICS",
        ));
    }
    parse_benchmark_concurrency(&value.to_string_lossy())
}

fn parse_benchmark_concurrency(value: &str) -> Result<usize, Error> {
    value
        .parse::<usize>()
        .ok()
        .filter(|count| (1..=32).contains(count))
        .ok_or_else(|| Error::invalid("benchmark worker concurrency must be between 1 and 32"))
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| matches!(value.as_str(), "1" | "true"))
}

fn drain_timeout() -> Result<Duration, Error> {
    let Some(value) = std::env::var(DRAIN_ENV)
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(Duration::from_secs(DEFAULT_DRAIN_SECS));
    };
    match value.parse::<u64>() {
        Ok(seconds) if seconds <= MAX_DRAIN_SECS => Ok(Duration::from_secs(seconds)),
        _ => Err(Error::invalid(format!(
            "{DRAIN_ENV} must be a whole number of seconds from 0 to {MAX_DRAIN_SECS}"
        ))),
    }
}

/// Keeps `/readyz` honest: the worker is ready while the coordinator answers a heartbeat.
/// Returns once a stop is requested.
async fn heartbeat_loop(ctx: WorkerContext, mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        let beat = tokio::time::timeout(
            HEARTBEAT_TIMEOUT,
            ctx.rpc.clone().heartbeat(proto::WorkerRequest {
                protocol_version: PROTOCOL_VERSION,
                worker_id: ctx.worker_id.clone(),
            }),
        )
        .await;
        set_ready(matches!(beat, Ok(Ok(_))));
        tokio::select! {
            _ = tokio::time::sleep(HEARTBEAT_EVERY) => {}
            _ = shutdown.changed() => {}
        }
    }
    set_ready(false);
}

/// Wait for a stop request. The coordinator of a finished crawl has stopped, so the worker is
/// unready here, and it stays up: the orchestrator replaces it when a new crawl is deployed.
async fn idle_until_stopped(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

/// Pending until a stop is requested, then resolves once `drain` has passed: the time an
/// in-flight task has to finish before it is given up.
async fn drain_expired(shutdown: &mut watch::Receiver<bool>, drain: Duration) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return std::future::pending().await;
        }
    }
    tokio::time::sleep(drain).await;
}

/// Hand the lease of an unfinished task back so another worker can claim it at once instead of
/// waiting for the lease to expire. The attempt it used still counts. Best effort: if this fails,
/// lease expiry fences the task anyway.
async fn release_lease(ctx: &WorkerContext, lease: &proto::Lease) {
    let released = ctx
        .rpc
        .clone()
        .fail_task(proto::FailTaskRequest {
            protocol_version: PROTOCOL_VERSION,
            lease: Some(lease.clone()),
            error_code: "worker_shutdown".into(),
            error_message: "worker stopped before the task finished".into(),
            // The coordinator rejects 0; 1 ms makes the task claimable at once.
            retry_delay_ms: 1,
        })
        .await;
    let (outcome, detail) = match released {
        Ok(response) => ("success", response.into_inner().status),
        Err(status) => ("failure", status.message().to_owned()),
    };
    emit_shared(
        &ctx.events,
        "worker.task_released",
        outcome,
        serde_json::json!({"worker_id": ctx.worker_id, "task_id": lease.task_id, "lease_epoch": lease.lease_epoch, "detail": detail}),
    );
}

async fn worker_loop(
    ctx: WorkerContext,
    mut shutdown: watch::Receiver<bool>,
    max_tasks: Option<usize>,
) -> Result<usize, Error> {
    let mut completed = 0_usize;
    'work: loop {
        if *shutdown.borrow() || max_tasks.is_some_and(|limit| completed >= limit) {
            break;
        }
        let rpc_measurement = super::metrics::RpcScope::new("claim_one");
        let mut rpc = ctx.rpc.clone();
        let response = tokio::select! {
            response = rpc.claim_one(proto::ClaimOneRequest {
                protocol_version: PROTOCOL_VERSION,
                worker_id: ctx.worker_id.clone(),
            }) => response.map_err(|error| Error::storage(format!("claim task: {error}")))?.into_inner(),
            _ = shutdown.changed() => break,
        };
        drop(rpc_measurement);
        check_protocol(response.protocol_version)
            .map_err(|error| Error::invalid(error.message().to_owned()))?;
        let Some(lease) = response.lease else {
            if response.target_reached {
                ctx.crawl_finished.store(true, Ordering::Relaxed);
                break;
            }
            return Err(Error::storage(
                "coordinator returned an empty capacity assignment",
            ));
        };
        emit_shared(
            &ctx.events,
            "worker.task_started",
            "success",
            serde_json::json!({"worker_id": ctx.worker_id, "task_id": lease.task_id, "source_id": lease.source_id, "url": safe_event_url(&lease.normalized_url), "lease_epoch": lease.lease_epoch}),
        );
        let report = tokio::select! {
            report = run_with_lease_renewal(&ctx, lease.clone()) => report,
            // Dropping the task future aborts its HTTP work (see run_with_lease_renewal).
            _ = drain_expired(&mut shutdown, ctx.drain) => {
                release_lease(&ctx, &lease).await;
                break 'work;
            }
        };
        match report {
            Ok(report) => {
                let mut lease = lease;
                let mut completion = proto::CompleteTaskRequest {
                    protocol_version: PROTOCOL_VERSION,
                    lease: Some(lease.clone()),
                    capture: report.capture,
                    discovered_urls: report.discovered_urls,
                    rejection_reason: report.rejection_reason,
                };
                let mut retry_delay = Duration::from_millis(25);
                let mut renew_at = tokio::time::Instant::now() + ctx.config.renew_after;
                let response = loop {
                    let rpc_measurement = super::metrics::RpcScope::new("complete_task");
                    let result = ctx.rpc.clone().complete_task(completion.clone()).await;
                    drop(rpc_measurement);
                    match result {
                        Ok(response) => break response.into_inner(),
                        Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                            if tokio::time::Instant::now() >= renew_at {
                                let renewed = ctx.rpc.clone().renew_lease(proto::LeaseRequest {
                                    protocol_version: PROTOCOL_VERSION,
                                    lease: Some(lease.clone()),
                                }).await.map_err(|error| Error::storage(format!("renew task lease while waiting for result capacity: {error}")))?.into_inner();
                                if !renewed.renewed {
                                    emit_shared(
                                        &ctx.events,
                                        "worker.task_stale",
                                        "failure",
                                        serde_json::json!({"worker_id": ctx.worker_id, "task_id": lease.task_id, "lease_epoch": lease.lease_epoch}),
                                    );
                                    completed += 1;
                                    continue 'work;
                                }
                                lease.lease_deadline_unix_ms = renewed.lease_deadline_unix_ms;
                                completion.lease = Some(lease.clone());
                                renew_at = tokio::time::Instant::now() + ctx.config.renew_after;
                            }
                            tokio::select! {
                                _ = tokio::time::sleep(retry_delay) => {},
                                _ = shutdown.changed() => break 'work,
                            }
                            retry_delay = (retry_delay * 2).min(Duration::from_millis(500));
                        }
                        Err(status) => {
                            return Err(Error::storage(format!(
                                "report task completion: {status}"
                            )));
                        }
                    }
                };
                if response.protocol_version != PROTOCOL_VERSION {
                    return Err(Error::invalid("coordinator protocol changed during run"));
                }
                if !response.completed {
                    emit_shared(
                        &ctx.events,
                        "worker.task_stale",
                        "failure",
                        serde_json::json!({"worker_id": ctx.worker_id, "task_id": lease.task_id, "lease_epoch": lease.lease_epoch}),
                    );
                    completed += 1;
                    continue;
                }
                completed += 1;
                emit_shared(
                    &ctx.events,
                    "worker.task_completed",
                    "success",
                    serde_json::json!({"worker_id": ctx.worker_id, "task_id": lease.task_id, "capture_received": response.capture_received, "accepted_discovered_count": response.accepted_discovered_count, "rejected_discovered_count": response.rejected_discovered_count, "target_reached": response.target_reached}),
                );
            }
            Err(error) => {
                let response = ctx
                    .rpc
                    .clone()
                    .fail_task(proto::FailTaskRequest {
                        protocol_version: PROTOCOL_VERSION,
                        lease: Some(lease.clone()),
                        error_code: error.code.clone(),
                        error_message: error.message.chars().take(2048).collect(),
                        retry_delay_ms: retry_delay_ms(1, &ctx.config),
                    })
                    .await;
                let response = match response {
                    Ok(response) => response.into_inner(),
                    Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                        emit_shared(
                            &ctx.events,
                            "worker.task_stale",
                            "failure",
                            serde_json::json!({"worker_id": ctx.worker_id, "task_id": lease.task_id, "lease_epoch": lease.lease_epoch}),
                        );
                        completed += 1;
                        continue;
                    }
                    Err(status) => {
                        return Err(Error::storage(format!("report task failure: {status}")));
                    }
                };
                if response.protocol_version != PROTOCOL_VERSION {
                    return Err(Error::invalid("coordinator protocol changed during run"));
                }
                emit_shared(
                    &ctx.events,
                    "worker.task_failed",
                    "failure",
                    serde_json::json!({"worker_id": ctx.worker_id, "task_id": lease.task_id, "code": error.code, "status": response.status}),
                );
                completed += 1;
            }
        }
    }
    Ok(completed)
}

async fn run_with_lease_renewal(
    ctx: &WorkerContext,
    lease: proto::Lease,
) -> Result<TaskReport, Error> {
    let task_ctx = ctx.clone();
    let task_lease = lease.clone();
    let mut task = tokio::spawn(async move { process_task(&task_ctx, &task_lease).await });
    // A dropped/rejected lease must stop its HTTP task rather than detach it.
    struct AbortOnDrop(tokio::task::AbortHandle);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _abort = AbortOnDrop(task.abort_handle());
    let mut renew = tokio::time::interval(ctx.config.renew_after);
    renew.tick().await;
    loop {
        tokio::select! {
            result = &mut task => {
                return match result {
                    Ok(result) => result,
                    Err(error) => Err(Error::storage(format!("crawl task worker failed: {error}"))),
                };
            }
            _ = renew.tick() => {
                let rpc_measurement = super::metrics::RpcScope::new("renew_lease");
                let response = ctx.rpc.clone().renew_lease(proto::LeaseRequest {
                    protocol_version: PROTOCOL_VERSION,
                    lease: Some(lease.clone()),
                }).await.map_err(|error| Error::storage(format!("renew task lease: {error}")))?.into_inner();
                drop(rpc_measurement);
                if response.protocol_version != PROTOCOL_VERSION || !response.renewed {
                    return Err(Error::safety("task lease renewal was rejected"));
                }
            }
        }
    }
}

async fn process_task(ctx: &WorkerContext, lease: &proto::Lease) -> Result<TaskReport, Error> {
    let requested = lease.normalized_url.as_str();
    let requested_url = Url::parse(requested).map_err(|error| Error::invalid(error.to_string()))?;
    let requested_origin = origin_key(&requested_url);
    let robots = ensure_robots(ctx, lease, requested).await?;
    if !robots.allowed {
        return Ok(TaskReport {
            capture: None,
            rejection_reason: "robots_denied".into(),
            discovered_urls: Vec::new(),
        });
    }
    sitemaps::discover_origin(ctx, lease, &requested_origin, &robots.sitemaps).await?;
    let mut discovered = BTreeSet::new();
    let mut current = requested_url.clone();
    let mut redirects = Vec::new();
    let mut source_id = lease.source_id.clone();
    let response = loop {
        if redirects.len() > 10 {
            return Ok(TaskReport {
                capture: None,
                rejection_reason: "redirect_limit_exceeded".into(),
                discovered_urls: discovered.into_iter().collect(),
            });
        }
        let normalized = normalize_url(current.clone()).map_err(Error::invalid)?;
        if !ctx_page_url_allowed(ctx, lease, &normalized) {
            return Ok(TaskReport {
                capture: None,
                rejection_reason: "redirect_outside_source_allowlist".into(),
                discovered_urls: discovered.into_iter().collect(),
            });
        }
        if !ensure_robots(ctx, lease, &normalized).await?.allowed {
            return Ok(TaskReport {
                capture: None,
                rejection_reason: "redirect_robots_denied".into(),
                discovered_urls: discovered.into_iter().collect(),
            });
        }
        let response = match request(
            ctx,
            lease,
            &normalized,
            "page",
            ctx.config
                .max_body_bytes
                .min(ctx.config.max_extraction_bytes),
        )
        .await
        {
            Ok(response) => response,
            Err(error) if error.code == "safety_cap" => {
                return Ok(TaskReport {
                    capture: None,
                    rejection_reason: "body_limit_exceeded".into(),
                    discovered_urls: discovered.into_iter().collect(),
                });
            }
            Err(error) => return Err(error),
        };
        if (300..400).contains(&response.status) {
            let Some(location) = response.location.as_deref() else {
                return Ok(TaskReport {
                    capture: None,
                    rejection_reason: "redirect_missing_location".into(),
                    discovered_urls: discovered.into_iter().collect(),
                });
            };
            let next = current
                .join(location)
                .map_err(|_| Error::fetch("invalid redirect location"))?;
            let normalized = match normalize_url(next.clone()) {
                Ok(url) => url,
                Err(reason) => {
                    return Ok(TaskReport {
                        capture: None,
                        rejection_reason: reason,
                        discovered_urls: discovered.into_iter().collect(),
                    });
                }
            };
            if normalized == requested || redirects.contains(&normalized) {
                return Ok(TaskReport {
                    capture: None,
                    rejection_reason: "redirect_loop".into(),
                    discovered_urls: discovered.into_iter().collect(),
                });
            }
            if !ctx_page_url_allowed(ctx, lease, &normalized) {
                return Ok(TaskReport {
                    capture: None,
                    rejection_reason: "redirect_outside_source_allowlist".into(),
                    discovered_urls: discovered.into_iter().collect(),
                });
            }
            source_id = owner_for_worker(ctx, &normalized)
                .map(|owner| owner.source_id.clone())
                .unwrap_or_else(|| lease.source_id.clone());
            redirects.push(normalized.clone());
            current = Url::parse(&normalized).expect("normalized URL parses");
            continue;
        }
        break response;
    };
    if response.status < 200 || response.status >= 300 {
        return Err(Error::fetch(format!(
            "GET {current} returned HTTP {}",
            response.status
        )));
    }
    if !response.content_type.is_empty()
        && !response
            .content_type
            .to_ascii_lowercase()
            .starts_with("text/html")
        && !response
            .content_type
            .to_ascii_lowercase()
            .starts_with("application/xhtml+xml")
    {
        return Ok(TaskReport {
            capture: None,
            rejection_reason: "non_html_content_type".into(),
            discovered_urls: discovered.into_iter().collect(),
        });
    }
    let html = match String::from_utf8(response.body.clone()) {
        Ok(html) => html.trim_start_matches('\u{feff}').to_owned(),
        Err(_) => {
            return Ok(TaskReport {
                capture: None,
                rejection_reason: "body_decode_failed".into(),
                discovered_urls: discovered.into_iter().collect(),
            });
        }
    };
    let started = std::time::Instant::now();
    let extraction_url = current.clone();
    let min_content_chars = ctx.config.min_content_chars;
    let extraction_result = tokio::task::spawn_blocking(move || {
        crate::crawl::extract::extract(&html, &extraction_url, min_content_chars)
    })
    .await
    .map_err(|error| Error::storage(format!("extract page content in worker: {error}")))?;
    let extracted = match extraction_result {
        Ok(extracted) => extracted,
        Err(reason) => {
            return Ok(TaskReport {
                capture: None,
                rejection_reason: reason,
                discovered_urls: discovered.into_iter().collect(),
            });
        }
    };
    if started.elapsed() > Duration::from_millis(ctx.config.max_extraction_millis) {
        return Ok(TaskReport {
            capture: None,
            rejection_reason: "extraction_cpu_budget_exceeded".into(),
            discovered_urls: discovered.into_iter().collect(),
        });
    }
    discovered.extend(extracted.links.iter().cloned());
    let body_hash = sha256_hex(&response.body);
    let capture = proto::CaptureData {
        requested_url: requested.to_owned(),
        final_url: current.to_string(),
        redirects,
        source_id,
        title: extracted.title,
        description: extracted.description,
        language: extracted.language,
        canonical_url: extracted.canonical_url,
        extraction_method: extracted.extraction_method.to_owned(),
        content: extracted.content,
        identity: extracted.identity,
        content_hash: extracted.content_hash,
        blocks: extracted
            .blocks
            .into_iter()
            .map(|block| match block {
                crate::crawl::ContentBlock::Heading { level, text } => proto::ContentBlock {
                    block: Some(proto::content_block::Block::Heading(proto::Heading {
                        level: level as u32,
                        text,
                    })),
                },
                crate::crawl::ContentBlock::Prose(text) => proto::ContentBlock {
                    block: Some(proto::content_block::Block::Prose(text)),
                },
                crate::crawl::ContentBlock::Code(text) => proto::ContentBlock {
                    block: Some(proto::content_block::Block::Code(text)),
                },
            })
            .collect(),
        content_bytes: response.body.len() as u64,
        body_hash,
        content_type: response.content_type,
        status_code: response.status as u32,
        body: response.body,
    };
    Ok(TaskReport {
        capture: Some(capture),
        rejection_reason: String::new(),
        discovered_urls: discovered.into_iter().collect(),
    })
}

fn ctx_page_url_allowed(ctx: &WorkerContext, _lease: &proto::Lease, normalized: &str) -> bool {
    owner_for_worker(ctx, normalized).is_some()
}

fn owner_for_worker<'a>(ctx: &'a WorkerContext, value: &str) -> Option<&'a proto::SourcePolicy> {
    let url = Url::parse(value).ok()?;
    let normalized = normalize_url(url.clone()).ok()?;
    ctx.config.sources.iter().find(|source| {
        source
            .allowed_origins
            .iter()
            .any(|origin| origin == &origin_key(&url))
            && source
                .path_prefixes
                .iter()
                .any(|prefix| url.path().starts_with(prefix))
            && !source
                .deny_path_prefixes
                .iter()
                .any(|prefix| url.path().starts_with(prefix))
            && (!source.explicit_allowed_urls || source.allowed_urls.contains(&normalized))
    })
}

fn sitemap_url_allowed_worker(ctx: &WorkerContext, value: &str) -> bool {
    let Ok(url) = Url::parse(value) else {
        return false;
    };
    let origin = origin_key(&url);
    ctx.config.sources.iter().any(|source| {
        source
            .allowed_origins
            .iter()
            .any(|allowed| allowed == &origin)
            && (source.sitemap_url_prefixes.is_empty()
                || source
                    .sitemap_url_prefixes
                    .iter()
                    .any(|prefix| value.starts_with(prefix)))
    })
}

async fn request(
    ctx: &WorkerContext,
    lease: &proto::Lease,
    value: &str,
    request_kind: &str,
    body_limit: usize,
) -> Result<HttpResponse, Error> {
    for attempt in 1..=ctx.config.max_attempts {
        let request_id = new_run_id();
        let permit_id = {
            let rpc_measurement = super::metrics::RpcScope::new("acquire_request_permit");
            let response = ctx
                .rpc
                .clone()
                .acquire_request_permit(proto::AcquireRequestPermitRequest {
                    protocol_version: PROTOCOL_VERSION,
                    lease: Some(lease.clone()),
                    request_id: request_id.clone(),
                    url: value.to_owned(),
                    request_kind: request_kind.to_owned(),
                })
                .await
                .map_err(|error| Error::storage(format!("acquire request permit: {error}")))?
                .into_inner();
            drop(rpc_measurement);
            if response.protocol_version != PROTOCOL_VERSION {
                return Err(Error::invalid(
                    "coordinator protocol changed during request",
                ));
            }
            match proto::acquire_request_permit_response::Decision::try_from(response.decision) {
                Ok(proto::acquire_request_permit_response::Decision::Acquired) => {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|_| Error::fetch("worker clock is before Unix epoch"))?
                        .as_millis();
                    let minimum = ctx.config.timeout.as_millis().saturating_add(100);
                    if response.expires_at_unix_ms <= 0
                        || (response.expires_at_unix_ms as u128).saturating_sub(now_ms) < minimum
                    {
                        // Clocks must be synchronized across hosts. Never start HTTP on a delayed grant.
                        let _ = release_permit(ctx, response.permit_id, "late_grant").await;
                        return Err(Error::fetch(
                            "request permit arrived too late to safely start HTTP",
                        ));
                    }
                    response.permit_id
                }
                Ok(proto::acquire_request_permit_response::Decision::Waiting) => {
                    return Err(Error::storage(
                        "coordinator returned a polling permit response",
                    ));
                }
                Ok(proto::acquire_request_permit_response::Decision::AlreadyCompleted) => {
                    return Err(Error::safety(
                        "request permit already completed; do not repeat HTTP",
                    ));
                }
                Err(_) => {
                    return Err(Error::storage(
                        "coordinator returned unknown permit decision",
                    ));
                }
            }
        };
        let measurement = super::metrics::HttpAttempt::new(
            &ctx.config.run_id,
            &ctx.worker_id,
            value,
            request_kind,
            permit_id,
        );
        // Only page fetches count towards throughput; robots and sitemap requests are
        // visible per origin in the coordinator's permit counter.
        let count_page = |outcome: &'static str| {
            if request_kind == "page" {
                metrics::PAGES_FETCHED.inc(&[outcome]);
            }
        };
        let sent = ctx.http.get(value).send().await;
        let response = match sent {
            Ok(response) => response,
            Err(error) => {
                count_page("transport_error");
                measurement.finish("transport_error", 0);
                release_permit(ctx, permit_id, "transport_error").await?;
                if attempt == ctx.config.max_attempts {
                    return Err(Error::fetch(format!(
                        "GET {value} exhausted retries: {error}"
                    )));
                }
                tokio::time::sleep(Duration::from_millis(retry_delay_ms(attempt, &ctx.config)))
                    .await;
                continue;
            }
        };
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if response
            .content_length()
            .is_some_and(|length| length > body_limit as u64)
        {
            count_page("body_error");
            measurement.finish("body_limit", 0);
            release_permit(ctx, permit_id, "body_limit").await?;
            return Err(Error::safety("response body exceeded configured limit"));
        }
        let body = match read_limited(response, body_limit).await {
            Ok(body) => body,
            Err(error) => {
                count_page("body_error");
                measurement.finish("body_read_error", 0);
                release_permit(ctx, permit_id, "body_read_error").await?;
                return Err(error);
            }
        };
        let outcome = if status < 400 {
            "success".to_owned()
        } else {
            format!("http_{status}")
        };
        count_page(if status < 400 {
            "success"
        } else {
            "http_error"
        });
        measurement.finish(&outcome, body.len());
        release_permit(ctx, permit_id, &outcome).await?;
        if retryable_status_code(status) && attempt < ctx.config.max_attempts {
            let delay = retry_after
                .as_deref()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(|seconds| seconds.saturating_mul(1000))
                .unwrap_or_else(|| retry_delay_ms(attempt, &ctx.config));
            tokio::time::sleep(Duration::from_millis(delay)).await;
            continue;
        }
        if retryable_status_code(status) {
            return Err(Error::fetch(format!(
                "GET {value} exhausted {} attempts with HTTP {status}",
                ctx.config.max_attempts
            )));
        }
        return Ok(HttpResponse {
            status,
            content_type,
            location,
            body,
        });
    }
    Err(Error::safety("request attempt loop exhausted"))
}

async fn read_limited(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| Error::fetch(format!("read response body: {error}")))?
    {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(Error::safety("response body exceeded configured limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn release_permit(ctx: &WorkerContext, permit_id: i64, outcome: &str) -> Result<(), Error> {
    let response = ctx
        .rpc
        .clone()
        .release_request_permit(proto::ReleaseRequestPermitRequest {
            protocol_version: PROTOCOL_VERSION,
            crawl_run_id: ctx.config.run_id.clone(),
            worker_id: ctx.worker_id.clone(),
            permit_id,
            outcome: outcome.to_owned(),
        })
        .await
        .map_err(|error| Error::storage(format!("release request permit: {error}")))?
        .into_inner();
    if response.protocol_version != PROTOCOL_VERSION || !response.success {
        return Err(Error::safety(
            "request permit expired or release was rejected",
        ));
    }
    Ok(())
}

async fn ensure_robots(
    ctx: &WorkerContext,
    lease: &proto::Lease,
    page_url: &str,
) -> Result<RobotsResult, Error> {
    let parsed = Url::parse(page_url).map_err(|error| Error::invalid(error.to_string()))?;
    let origin = origin_key(&parsed);
    loop {
        let response = ctx
            .rpc
            .clone()
            .robots_decision(proto::RobotsDecisionRequest {
                protocol_version: PROTOCOL_VERSION,
                lease: Some(lease.clone()),
                origin: origin.clone(),
                path: parsed.path().to_owned(),
            })
            .await
            .map_err(|error| Error::storage(format!("read shared robots decision: {error}")))?
            .into_inner();
        if response.protocol_version != PROTOCOL_VERSION {
            return Err(Error::invalid(
                "coordinator protocol changed during robots check",
            ));
        }
        match proto::robots_decision_response::Decision::try_from(response.decision) {
            Ok(proto::robots_decision_response::Decision::Cached) => {
                return Ok(RobotsResult {
                    allowed: response.allowed,
                    sitemaps: response.sitemaps,
                });
            }
            Ok(proto::robots_decision_response::Decision::Waiting) => {
                tokio::time::sleep(Duration::from_millis(response.retry_after_ms.max(1))).await;
            }
            Ok(proto::robots_decision_response::Decision::Refresh) => {
                let epoch = response.refresh_epoch;
                let robots_url = format!("{origin}/robots.txt");
                let mut current = robots_url.clone();
                let mut result = None;
                for _ in 0..=5 {
                    let response = request(ctx, lease, &current, "robots", 512 * 1024).await;
                    let response = match response {
                        Ok(response) => response,
                        Err(_) => break,
                    };
                    if (400..500).contains(&response.status) {
                        result = Some((RobotsPolicy::default(), sha256_hex(&[])));
                        break;
                    }
                    if (300..400).contains(&response.status) {
                        let Some(location) = response.location else {
                            break;
                        };
                        let Ok(next) = Url::parse(&current).and_then(|url| url.join(&location))
                        else {
                            break;
                        };
                        let Ok(normalized) = normalize_url(next.clone()) else {
                            break;
                        };
                        if origin_key(&next) != origin {
                            break;
                        }
                        current = normalized;
                        continue;
                    }
                    if response.status < 200 || response.status >= 300 {
                        break;
                    }
                    let body = response.body;
                    let parsed = tokio::task::spawn_blocking(move || {
                        let hash = sha256_hex(&body);
                        (parse_robots(&body), hash)
                    })
                    .await
                    .map_err(|error| {
                        Error::storage(format!("parse robots response in worker: {error}"))
                    })?;
                    result = Some(parsed);
                    break;
                }
                if let Some((policy, body_sha256)) = result {
                    let mut client = ctx.rpc.clone();
                    let committed = client
                        .commit_robots_refresh(proto::CommitRobotsRefreshRequest {
                            protocol_version: PROTOCOL_VERSION,
                            lease: Some(lease.clone()),
                            origin: origin.clone(),
                            refresh_epoch: epoch,
                            policy: Some(policy_to_proto(&policy)),
                            body_sha256,
                        })
                        .await
                        .map_err(|error| {
                            Error::storage(format!("commit shared robots policy: {error}"))
                        })?
                        .into_inner();
                    if !committed.success {
                        return Err(Error::safety("robots refresh lease expired"));
                    }
                } else {
                    let mut client = ctx.rpc.clone();
                    let _ = client
                        .fail_robots_refresh(proto::FailRobotsRefreshRequest {
                            protocol_version: PROTOCOL_VERSION,
                            lease: Some(lease.clone()),
                            origin: origin.clone(),
                            refresh_epoch: epoch,
                        })
                        .await;
                    return Ok(RobotsResult {
                        allowed: false,
                        sitemaps: Vec::new(),
                    });
                }
            }
            Err(_) => {
                return Err(Error::storage(
                    "coordinator returned unknown robots decision",
                ));
            }
        }
    }
}

fn policy_to_proto(policy: &RobotsPolicy) -> proto::RobotsPolicy {
    proto::RobotsPolicy {
        groups: policy
            .groups
            .iter()
            .map(|group| proto::RobotsGroup {
                agents: group.agents.clone(),
                rules: group
                    .rules
                    .iter()
                    .map(|rule| proto::RobotsRule {
                        allow: rule.allow,
                        path: rule.path.clone(),
                    })
                    .collect(),
            })
            .collect(),
        sitemaps: policy.sitemaps.clone(),
    }
}

fn retryable_status_code(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn retry_delay_ms(attempt: usize, config: &WorkerConfig) -> u64 {
    let multiplier = 1_u64 << attempt.saturating_sub(1).min(20);
    config
        .retry_backoff_ms
        .saturating_mul(multiplier)
        .min(config.retry_max_delay_ms)
}

#[cfg(test)]
mod benchmark_tests {
    #[test]
    fn benchmark_concurrency_is_bounded() {
        for count in [1, 4, 8, 16, 32] {
            assert_eq!(
                super::parse_benchmark_concurrency(&count.to_string()).unwrap(),
                count
            );
        }
        for invalid in ["0", "33", "-1", "threads", ""] {
            assert!(super::parse_benchmark_concurrency(invalid).is_err());
        }
    }
}
