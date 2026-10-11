use super::proto;
use super::protocol::{check_protocol, safe_event_url, validate_identity};
use super::{EMPTY_POLL_MS, MAX_BODY_TRANSFER_BYTES, MAX_RPC_MESSAGE_BYTES, PROTOCOL_VERSION};
use crate::app::report_error;
use crate::cli::{Cli, Operation};
use crate::crawl::{
    RobotsPolicy, extract_sitemap_locations, is_sitemap_index, normalize_sitemap_url, parse_robots,
};
use crate::error::Error;
use crate::observability::{Events, emit_shared};
use crate::storage::{new_run_id, sha256_hex};
use crate::url::{normalize_url, origin_key};
use ::url::Url;
use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    let worker_id = worker_id.map(str::to_owned).unwrap_or_else(new_run_id);
    validate_identity(&worker_id, "worker_id")
        .map_err(|status| Error::invalid(status.message().to_owned()))?;
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
        };
        emit_shared(
            &events,
            "worker.started",
            "success",
            serde_json::json!({"worker_id": worker_id, "coordinator": address, "protocol_version": PROTOCOL_VERSION, "concurrency": 1, "prefetch": 0}),
        );
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            signal.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let mut completed = 0_usize;
        loop {
            if shutdown.load(std::sync::atomic::Ordering::SeqCst)
                || max_tasks.is_some_and(|limit| completed >= limit)
            {
                break;
            }
            let rpc_measurement = super::metrics::RpcScope::new("claim_one");
            let response = ctx
                .rpc
                .clone()
                .claim_one(proto::ClaimOneRequest {
                    protocol_version: PROTOCOL_VERSION,
                    worker_id: ctx.worker_id.clone(),
                })
                .await
                .map_err(|error| Error::storage(format!("claim task: {error}")))?
                .into_inner();
            drop(rpc_measurement);
            check_protocol(response.protocol_version)
                .map_err(|error| Error::invalid(error.message().to_owned()))?;
            let Some(lease) = response.lease else {
                if response.target_reached {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(response.retry_after_ms.max(EMPTY_POLL_MS)))
                    .await;
                continue;
            };
            emit_shared(
                &ctx.events,
                "worker.task_started",
                "success",
                serde_json::json!({"worker_id": ctx.worker_id, "task_id": lease.task_id, "source_id": lease.source_id, "url": safe_event_url(&lease.normalized_url), "lease_epoch": lease.lease_epoch}),
            );
            let report = run_with_lease_renewal(&ctx, lease.clone()).await;
            match report {
                Ok(report) => {
                    let rpc_measurement = super::metrics::RpcScope::new("complete_task");
                    let response = ctx
                        .rpc
                        .clone()
                        .complete_task(proto::CompleteTaskRequest {
                            protocol_version: PROTOCOL_VERSION,
                            lease: Some(lease.clone()),
                            capture: report.capture,
                            discovered_urls: report.discovered_urls,
                            rejection_reason: report.rejection_reason,
                        })
                        .await
                        .map_err(|error| Error::storage(format!("report task completion: {error}")))?
                        .into_inner();
                    drop(rpc_measurement);
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
                            return Err(Error::storage(format!(
                                "report task failure: {status}"
                            )));
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
        emit_shared(
            &ctx.events,
            "worker.stopped",
            "success",
            serde_json::json!({"worker_id": ctx.worker_id, "completed_tasks": completed}),
        );
        Ok(())
    })
}

async fn run_with_lease_renewal(
    ctx: &WorkerContext,
    lease: proto::Lease,
) -> Result<TaskReport, Error> {
    let task_ctx = ctx.clone();
    let task_lease = lease.clone();
    let mut task = tokio::spawn(async move { process_task(&task_ctx, &task_lease).await });
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
        let permit_id = loop {
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
                    break response.permit_id;
                }
                Ok(proto::acquire_request_permit_response::Decision::Waiting) => {
                    tokio::time::sleep(Duration::from_millis(response.retry_after_ms.max(1))).await;
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
        let sent = ctx.http.get(value).send().await;
        let response = match sent {
            Ok(response) => response,
            Err(error) => {
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
            measurement.finish("body_limit", 0);
            release_permit(ctx, permit_id, "body_limit").await?;
            return Err(Error::safety("response body exceeded configured limit"));
        }
        let body = match read_limited(response, body_limit).await {
            Ok(body) => body,
            Err(error) => {
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
