use super::frontier::{
    ClaimedTask, FrontierStore, LeaseTiming, PermitDecision, RequestPolicyLimits, RobotsDecision,
    TaskStatus,
};
use super::proto;
use super::protocol::{check_protocol, safe_event_url, validate_identity};
use super::{
    EMPTY_POLL_MS, MAX_BODY_TRANSFER_BYTES, MAX_RPC_MESSAGE_BYTES, MAX_URL_BYTES, PROTOCOL_VERSION,
};
use crate::app::report_error;
use crate::cli::{Cli, Operation};
use crate::crawl::{
    Config, RobotsGroup, RobotsPolicy, RobotsRule, Source, load_config, schedule_key,
    sitemap_url_allowed,
};
use crate::error::Error;
use crate::observability::{Events, emit_shared};
use crate::storage::{new_run_id, sha256_hex};
use crate::url::{normalize_origin, normalize_url, origin_key};
use ::url::Url;
use proto::coordinator_server::{Coordinator, CoordinatorServer};
use std::collections::{BTreeSet, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status, transport::Server};

struct CoordinatorState {
    core: Mutex<CoordinatorCore>,
}

struct CoordinatorCore {
    frontier: FrontierStore,
    config: Config,
    run_id: String,
    lease_timing: LeaseTiming,
    registered_workers: HashSet<String>,
    unique_captures: HashSet<(String, String)>,
    captures: usize,
    rejected: usize,
    events: Arc<Mutex<Events>>,
}

impl CoordinatorCore {
    fn target_reached(&mut self) -> Result<bool, Status> {
        if self.captures >= self.config.page_target {
            return Ok(true);
        }
        self.frontier
            .run_duration_exceeded(
                &self.run_id,
                Duration::from_secs(self.config.max_duration_seconds),
            )
            .map_err(to_status)
    }

    fn validate_lease(&mut self, lease: &proto::Lease) -> Result<ClaimedTask, Status> {
        let task = claimed_task(lease)?;
        if task.crawl_run_id != self.run_id
            || !self.registered_workers.contains(&task.lease_owner)
            || !self.frontier.lease_is_live(&task).map_err(to_status)?
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
        Ok(task)
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

async fn with_coordinator_core<T, F>(
    state: Arc<CoordinatorState>,
    operation: F,
) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce(&mut CoordinatorCore) -> Result<T, Status> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut core = state.core.lock().expect("coordinator state lock");
        operation(&mut core)
    })
    .await
    .map_err(|error| Status::internal(format!("coordinator request task failed: {error}")))?
}

#[tonic::async_trait]
impl Coordinator for CoordinatorRpc {
    async fn register_worker(
        &self,
        request: Request<proto::RegisterWorkerRequest>,
    ) -> Result<Response<proto::RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        validate_identity(&request.worker_id, "worker_id")?;
        if !request
            .supported_protocol_versions
            .contains(&PROTOCOL_VERSION)
        {
            return Err(Status::failed_precondition(
                "no mutually supported protocol version",
            ));
        }
        let response = with_coordinator_core(self.state.clone(), move |core| {
            core.registered_workers.insert(request.worker_id.clone());
            emit_shared(
                &core.events,
                "worker.registered",
                "success",
                serde_json::json!({"worker_id": request.worker_id, "protocol_version": PROTOCOL_VERSION}),
            );
            Ok(proto::RegisterWorkerResponse {
                protocol_version: PROTOCOL_VERSION,
                crawl_run_id: core.run_id.clone(),
                request_timeout_ms: core.config.timeout_seconds.saturating_mul(1000),
                lease_for_ms: core.lease_timing.lease_for.as_millis() as u64,
                renew_after_ms: core.lease_timing.renew_after.as_millis() as u64,
                max_body_bytes: core.config.max_body_bytes as u64,
                max_extraction_bytes: core.config.max_extraction_bytes as u64,
                max_extraction_millis: core.config.max_extraction_millis,
                max_attempts: core.config.max_attempts as u64,
                retry_backoff_ms: core.config.retry_backoff_ms,
                retry_max_delay_ms: core.config.retry_max_delay_ms,
                max_frontier: core.config.max_frontier as u64,
                user_agent: core.config.user_agent.clone(),
                min_content_chars: core.config.min_content_chars as u64,
                sources: core
                    .config
                    .sources
                    .iter()
                    .map(|source| proto::SourcePolicy {
                        source_id: source.source_id.clone(),
                        allowed_origins: source.allowed_origins.clone(),
                        path_prefixes: source.path_prefixes.clone(),
                        deny_path_prefixes: source.deny_path_prefixes.clone(),
                        sitemap_url_prefixes: source.sitemap_url_prefixes.clone(),
                        allowed_urls: source
                            .allowed_urls
                            .as_ref()
                            .map(|urls| urls.iter().cloned().collect())
                            .unwrap_or_default(),
                        explicit_allowed_urls: source.allowed_urls.is_some(),
                    })
                    .collect(),
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<proto::WorkerRequest>,
    ) -> Result<Response<proto::WorkerResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            require_worker(core, &request.worker_id)?;
            Ok(proto::WorkerResponse {
                protocol_version: PROTOCOL_VERSION,
                crawl_run_id: core.run_id.clone(),
                server_time_unix_ms: unix_ms().max(0) as u64,
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn claim_one(
        &self,
        request: Request<proto::ClaimOneRequest>,
    ) -> Result<Response<proto::ClaimOneResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            require_worker(core, &request.worker_id)?;
            let run_id = core.run_id.clone();
            if core.target_reached()?
                || core
                    .frontier
                    .unfinished_task_count(&run_id)
                    .map_err(to_status)?
                    == 0
            {
                return Ok(proto::ClaimOneResponse {
                    protocol_version: PROTOCOL_VERSION,
                    lease: None,
                    target_reached: true,
                    retry_after_ms: 0,
                });
            }
            let timing = core.lease_timing;
            let task = core
                .frontier
                .claim_one(&run_id, &request.worker_id, timing)
                .map_err(to_status)?;
            if let Some(task) = task {
            let safe_url = safe_event_url(&task.normalized_url);
                emit_shared(
                    &core.events,
                    "task.claimed",
                    "success",
                serde_json::json!({"worker_id": request.worker_id, "task_id": task.task_id, "source_id": task.source_id, "url": safe_url, "lease_epoch": task.lease_epoch, "task_attempt": task.task_attempt_count}),
                );
                Ok(proto::ClaimOneResponse {
                    protocol_version: PROTOCOL_VERSION,
                    lease: Some(task_to_proto(&task)),
                    target_reached: false,
                    retry_after_ms: 0,
                })
            } else {
                Ok(proto::ClaimOneResponse {
                    protocol_version: PROTOCOL_VERSION,
                    lease: None,
                    target_reached: false,
                    retry_after_ms: EMPTY_POLL_MS,
                })
            }
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn renew_lease(
        &self,
        request: Request<proto::LeaseRequest>,
    ) -> Result<Response<proto::LeaseResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let lease = request
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            let task = core.validate_lease(&lease)?;
            let timing = core.lease_timing;
            let renewed = core
                .frontier
                .renew_lease(&task, timing)
                .map_err(to_status)?;
            if renewed {
                emit_shared(
                    &core.events,
                    "task.lease_renewed",
                    "success",
                    serde_json::json!({"worker_id": task.lease_owner, "task_id": task.task_id, "lease_epoch": task.lease_epoch}),
                );
            }
            Ok(proto::LeaseResponse {
                protocol_version: PROTOCOL_VERSION,
                renewed,
                lease_deadline_unix_ms: unix_ms()
                    .saturating_add(core.lease_timing.lease_for.as_millis() as i64),
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn acquire_request_permit(
        &self,
        request: Request<proto::AcquireRequestPermitRequest>,
    ) -> Result<Response<proto::AcquireRequestPermitResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let lease = request
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            let task = core.validate_lease(&lease)?;
            let (origin, url) = core.validate_request_url(&request.url, &request.request_kind)?;
            let key = schedule_key(&core.config, &url);
            let run_id = core.run_id.clone();
            let decision = core
                .frontier
                .try_acquire_request_permit(
                    &run_id,
                    &task.lease_owner,
                    &request.request_id,
                    &key,
                    &origin,
                    &request.request_kind,
                )
                .map_err(to_status)?;
            Ok(match decision {
                PermitDecision::Acquired(grant) => proto::AcquireRequestPermitResponse {
                    protocol_version: PROTOCOL_VERSION,
                    decision: proto::acquire_request_permit_response::Decision::Acquired as i32,
                    permit_id: grant.permit_id,
                    expires_at_unix_ms: grant.expires_at_unix_ms,
                    retry_after_ms: 0,
                },
                PermitDecision::Waiting(wait) => proto::AcquireRequestPermitResponse {
                    protocol_version: PROTOCOL_VERSION,
                    decision: proto::acquire_request_permit_response::Decision::Waiting as i32,
                    permit_id: 0,
                    expires_at_unix_ms: 0,
                    retry_after_ms: wait.as_millis().max(1) as u64,
                },
                PermitDecision::AlreadyCompleted => proto::AcquireRequestPermitResponse {
                    protocol_version: PROTOCOL_VERSION,
                    decision: proto::acquire_request_permit_response::Decision::AlreadyCompleted
                        as i32,
                    permit_id: 0,
                    expires_at_unix_ms: 0,
                    retry_after_ms: 0,
                },
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn cancel_request_wait(
        &self,
        request: Request<proto::CancelRequestWaitRequest>,
    ) -> Result<Response<proto::BoolResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            require_worker(core, &request.worker_id)?;
            let success = core
                .frontier
                .cancel_request_wait(
                    &request.crawl_run_id,
                    &request.worker_id,
                    &request.request_id,
                )
                .map_err(to_status)?;
            Ok(proto::BoolResponse {
                protocol_version: PROTOCOL_VERSION,
                success,
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn release_request_permit(
        &self,
        request: Request<proto::ReleaseRequestPermitRequest>,
    ) -> Result<Response<proto::BoolResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            require_worker(core, &request.worker_id)?;
            if request.crawl_run_id != core.run_id {
                return Err(Status::failed_precondition("crawl run ID mismatch"));
            }
            let success = core
                .frontier
                .release_request_permit(
                    &request.crawl_run_id,
                    &request.worker_id,
                    request.permit_id,
                    &request.outcome,
                )
                .map_err(to_status)?;
            Ok(proto::BoolResponse {
                protocol_version: PROTOCOL_VERSION,
                success,
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn robots_decision(
        &self,
        request: Request<proto::RobotsDecisionRequest>,
    ) -> Result<Response<proto::RobotsDecisionResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let lease = request
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            let task = core.validate_lease(&lease)?;
            let origin = normalize_origin(&request.origin)
                .map_err(|_| Status::invalid_argument("robots origin is invalid"))?;
            if origin != request.origin || !request.path.starts_with('/') {
                return Err(Status::invalid_argument(
                    "robots origin or path is not normalized",
                ));
            }
            if !core.config.sources.iter().any(|source| {
                source
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed == &origin)
            }) {
                return Err(Status::permission_denied(
                    "robots origin is outside configured Sources",
                ));
            }
            let run_id = core.run_id.clone();
            let decision = core
                .frontier
                .robots_decision(&run_id, &task.lease_owner, None, &origin, &request.path)
                .map_err(to_status)?;
            Ok(match decision {
                RobotsDecision::Cached {
                    allowed,
                    body_sha256,
                    sitemaps,
                } => proto::RobotsDecisionResponse {
                    protocol_version: PROTOCOL_VERSION,
                    decision: proto::robots_decision_response::Decision::Cached as i32,
                    allowed,
                    refresh_epoch: 0,
                    retry_after_ms: 0,
                    body_sha256: body_sha256.unwrap_or_default(),
                    sitemaps,
                    groups: Vec::new(),
                },
                RobotsDecision::Refresh { epoch } => proto::RobotsDecisionResponse {
                    protocol_version: PROTOCOL_VERSION,
                    decision: proto::robots_decision_response::Decision::Refresh as i32,
                    allowed: false,
                    refresh_epoch: epoch,
                    retry_after_ms: 0,
                    body_sha256: String::new(),
                    sitemaps: Vec::new(),
                    groups: Vec::new(),
                },
                RobotsDecision::Waiting(wait) => proto::RobotsDecisionResponse {
                    protocol_version: PROTOCOL_VERSION,
                    decision: proto::robots_decision_response::Decision::Waiting as i32,
                    allowed: false,
                    refresh_epoch: 0,
                    retry_after_ms: wait.as_millis().max(1) as u64,
                    body_sha256: String::new(),
                    sitemaps: Vec::new(),
                    groups: Vec::new(),
                },
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn commit_robots_refresh(
        &self,
        request: Request<proto::CommitRobotsRefreshRequest>,
    ) -> Result<Response<proto::BoolResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let lease = request
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        let policy = request
            .policy
            .ok_or_else(|| Status::invalid_argument("robots policy is required"))?;
        let policy = RobotsPolicy {
            groups: policy
                .groups
                .into_iter()
                .map(|group| RobotsGroup {
                    agents: group.agents,
                    rules: group
                        .rules
                        .into_iter()
                        .map(|rule| RobotsRule {
                            allow: rule.allow,
                            path: rule.path,
                        })
                        .collect(),
                })
                .collect(),
            sitemaps: policy.sitemaps,
        };
        let response = with_coordinator_core(self.state.clone(), move |core| {
            let task = core.validate_lease(&lease)?;
            validate_allowed_origin(&core.config, &request.origin)?;
            let run_id = core.run_id.clone();
            let success = core
                .frontier
                .commit_robots_refresh(
                    &run_id,
                    &task.lease_owner,
                    &request.origin,
                    request.refresh_epoch,
                    &policy,
                    &request.body_sha256,
                )
                .map_err(to_status)?;
            Ok(proto::BoolResponse {
                protocol_version: PROTOCOL_VERSION,
                success,
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn fail_robots_refresh(
        &self,
        request: Request<proto::FailRobotsRefreshRequest>,
    ) -> Result<Response<proto::BoolResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let lease = request
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            let task = core.validate_lease(&lease)?;
            validate_allowed_origin(&core.config, &request.origin)?;
            let run_id = core.run_id.clone();
            let success = core
                .frontier
                .fail_robots_refresh(
                    &run_id,
                    &task.lease_owner,
                    &request.origin,
                    request.refresh_epoch,
                )
                .map_err(to_status)?;
            Ok(proto::BoolResponse {
                protocol_version: PROTOCOL_VERSION,
                success,
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn complete_task(
        &self,
        request: Request<proto::CompleteTaskRequest>,
    ) -> Result<Response<proto::CompleteTaskResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let lease = request
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        let response = with_coordinator_core(self.state.clone(), move |core| {
            let task = core.validate_lease(&lease)?;
            let capture_received = request.capture.is_some();
            if let Some(capture) = request.capture.as_ref() {
                validate_capture(&core.config, &task, capture)?;
            } else if request.rejection_reason.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "completion requires capture or rejection reason",
                ));
            }
            let mut discovered = BTreeSet::new();
            let mut rejected_discovered_count = 0_u64;
            let mut accepted_discovered_count = 0_u64;
            let run_id = core.run_id.clone();
            let max_task_attempts = core.config.max_attempts as i32;
            let max_frontier = core.config.max_frontier;
            let mut unfinished = core
                .frontier
                .unfinished_task_count(&run_id)
                .map_err(to_status)?;
            for value in request.discovered_urls {
                if value.len() > MAX_URL_BYTES || discovered.len() >= max_frontier {
                    rejected_discovered_count += 1;
                    continue;
                }
                let Ok(parsed) = Url::parse(&value) else {
                    rejected_discovered_count += 1;
                    continue;
                };
                let Ok(normalized) = normalize_url(parsed.clone()) else {
                    rejected_discovered_count += 1;
                    continue;
                };
                let Some(owner) = owner_for(&core.config, &normalized) else {
                    rejected_discovered_count += 1;
                    continue;
                };
                let source_id = owner.source_id.clone();
                if normalized != value || unfinished >= max_frontier {
                    rejected_discovered_count += 1;
                    continue;
                }
                let key = schedule_key(&core.config, &parsed);
                if core
                    .frontier
                    .enqueue_scheduled_url(
                        &run_id,
                        &source_id,
                        &normalized,
                        max_task_attempts,
                        &key,
                    )
                    .map_err(to_status)?
                {
                    accepted_discovered_count += 1;
                    unfinished += 1;
                }
                discovered.insert(normalized);
            }
            if let Some(capture) = request.capture.as_ref() {
                let key = (capture.source_id.clone(), capture.content_hash.clone());
                core.unique_captures.insert(key);
                core.captures = core.unique_captures.len();
            } else {
                core.rejected += 1;
            }
            let completed = core.frontier.complete_task(&task).map_err(to_status)?;
            let target_reached = core.target_reached()?;
            if completed {
                let safe_url = safe_event_url(&task.normalized_url);
                emit_shared(
                    &core.events,
                    if capture_received {
                        "capture.ready_for_ticket_4"
                    } else {
                        "page.rejected"
                    },
                    "success",
                    serde_json::json!({
                        "task_id": task.task_id,
                        "source_id": task.source_id,
                        "url": safe_url,
                        "capture_received": capture_received,
                        "body_bytes": request.capture.as_ref().map(|capture| capture.body.len()).unwrap_or(0),
                        "body_sha256": request.capture.as_ref().map(|capture| capture.body_hash.as_str()),
                        "rejection_reason": request.rejection_reason,
                        "accepted_discovered_count": accepted_discovered_count,
                        "rejected_discovered_count": rejected_discovered_count,
                        "capture_persistence": "ticket_4_boundary",
                        "target_reached": target_reached,
                    }),
                );
            }
            Ok(proto::CompleteTaskResponse {
                protocol_version: PROTOCOL_VERSION,
                completed,
                accepted_discovered_count,
                rejected_discovered_count,
                capture_received,
                target_reached,
            })
        })
        .await?;
        Ok(Response::new(response))
    }

    async fn fail_task(
        &self,
        request: Request<proto::FailTaskRequest>,
    ) -> Result<Response<proto::FailTaskResponse>, Status> {
        let request = request.into_inner();
        check_protocol(request.protocol_version)?;
        let lease = request
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        if request.error_message.len() > 2048 || request.error_code.len() > 128 {
            return Err(Status::invalid_argument(
                "task failure detail exceeds size limit",
            ));
        }
        let response = with_coordinator_core(self.state.clone(), move |core| {
            let task = core.validate_lease(&lease)?;
            let state = core
                .frontier
                .fail_task(
                    &task,
                    &request.error_message,
                    Duration::from_millis(request.retry_delay_ms),
                )
                .map_err(to_status)?;
            let status = state.map(task_status_name).unwrap_or("stale");
            emit_shared(
                &core.events,
                "task.failed",
                if status == "failed" {
                    "failure"
                } else {
                    "success"
                },
                serde_json::json!({"worker_id": task.lease_owner, "task_id": task.task_id, "task_status": status, "error_code": request.error_code}),
            );
            Ok(proto::FailTaskResponse {
                protocol_version: PROTOCOL_VERSION,
                status: status.to_owned(),
            })
        })
        .await?;
        Ok(Response::new(response))
    }
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
    let event_id = new_run_id();
    let events = Arc::new(Mutex::new(Events::open(
        &cli.data_dir,
        &event_id,
        "coordinator",
    )?));
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
    frontier.configure_run(&run_id, &config.digest, policy_limits)?;
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
        }),
    );
    println!("scout coordinator listening on {address}");
    let state = Arc::new(CoordinatorState {
        core: Mutex::new(CoordinatorCore {
            frontier,
            config,
            run_id,
            lease_timing,
            registered_workers: HashSet::new(),
            unique_captures: HashSet::new(),
            captures: 0,
            rejected: 0,
            events,
        }),
    });
    let service = CoordinatorRpc {
        state: state.clone(),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::storage(format!("start coordinator runtime: {error}")))?;
    let result = runtime.block_on(async move {
        Server::builder()
            .add_service(
                CoordinatorServer::new(service)
                    .max_decoding_message_size(MAX_RPC_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_RPC_MESSAGE_BYTES),
            )
            .serve_with_shutdown(address, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await
            .map_err(|error| Error::storage(format!("coordinator gRPC server: {error}")))
    });
    // Drop the synchronous PostgreSQL client outside Tokio; its destructor drives its own runtime.
    drop(state);
    result
}

fn require_worker(core: &CoordinatorCore, worker_id: &str) -> Result<(), Status> {
    validate_identity(worker_id, "worker_id")?;
    if core.registered_workers.contains(worker_id) {
        Ok(())
    } else {
        Err(Status::failed_precondition(
            "worker must negotiate protocol first",
        ))
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

fn task_to_proto(task: &ClaimedTask) -> proto::Lease {
    proto::Lease {
        task_id: task.task_id,
        crawl_run_id: task.crawl_run_id.clone(),
        source_id: task.source_id.clone(),
        origin: task.origin.clone(),
        normalized_url: task.normalized_url.clone(),
        schedule_key: task.schedule_key.clone(),
        task_attempt_count: task.task_attempt_count,
        lease_owner: task.lease_owner.clone(),
        lease_epoch: task.lease_epoch,
        lease_deadline_unix_ms: task.lease_deadline_unix_ms,
        idempotency_key: task.idempotency_key.clone(),
    }
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

fn validate_capture(
    config: &Config,
    task: &ClaimedTask,
    capture: &proto::CaptureData,
) -> Result<(), Status> {
    if capture.requested_url != task.normalized_url
        || capture.body.len() > config.max_body_bytes
        || capture.body.len() > config.max_extraction_bytes
        || capture.body.len() > MAX_BODY_TRANSFER_BYTES
        || capture.content_bytes as usize != capture.body.len()
        || capture.body_hash != sha256_hex(&capture.body)
        || capture.content_hash != sha256_hex(capture.identity.as_bytes())
    {
        return Err(Status::invalid_argument(
            "capture metadata or body digest is invalid",
        ));
    }
    let final_url = normalize_capture_url(&capture.final_url)?;
    let final_owner = owner_for(config, &final_url)
        .ok_or_else(|| Status::permission_denied("capture final URL is outside Sources"))?;
    if final_owner.source_id != capture.source_id {
        return Err(Status::permission_denied(
            "capture Source does not own final URL",
        ));
    }
    for redirect in &capture.redirects {
        let redirect = normalize_capture_url(redirect)?;
        if owner_for(config, &redirect).is_none() {
            return Err(Status::permission_denied("redirect URL is outside Sources"));
        }
    }
    if capture.status_code < 200 || capture.status_code >= 300 {
        return Err(Status::invalid_argument(
            "capture status code must be successful",
        ));
    }
    Ok(())
}

fn normalize_capture_url(value: &str) -> Result<String, Status> {
    if value.len() > MAX_URL_BYTES {
        return Err(Status::invalid_argument("capture URL exceeds size limit"));
    }
    let parsed =
        Url::parse(value).map_err(|_| Status::invalid_argument("capture URL is invalid"))?;
    let normalized = normalize_url(parsed)
        .map_err(|_| Status::invalid_argument("capture URL is not HTTP(S)"))?;
    if normalized != value {
        return Err(Status::invalid_argument("capture URL must be normalized"));
    }
    Ok(normalized)
}

fn to_status(error: Error) -> Status {
    if error.exit_code == 2 {
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
    use super::{PROTOCOL_VERSION, check_protocol, private_ip, proto};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

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
        assert!(check_protocol(1).is_ok());
        assert!(check_protocol(2).is_err());
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
}
