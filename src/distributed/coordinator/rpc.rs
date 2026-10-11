use super::*;

pub(super) async fn with_coordinator_core<T, F>(
    state: Arc<CoordinatorState>,
    operation_name: &'static str,
    operation: F,
) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce(&mut CoordinatorCore) -> Result<T, Status> + Send + 'static,
{
    let started = std::time::Instant::now();
    tokio::task::spawn_blocking(move || {
        let queued = started.elapsed();
        let locking = std::time::Instant::now();
        let mut core = state.core.lock().expect("coordinator state lock");
        let lock_wait = locking.elapsed();
        let servicing = std::time::Instant::now();
        if let Some(message) = core.fatal_error.as_ref() {
            return Err(Status::internal(message.clone()));
        }
        let result = operation(&mut core);
        if let Err(error) = &result {
            if error.code() == tonic::Code::Internal {
                let run_id = core.run_id.clone();
                let _ = core.frontier.mark_run_failed(&run_id);
                emit_shared(
                    &core.events,
                    "coordinator.failed",
                    "failure",
                    serde_json::json!({"crawl_run_id": run_id, "error": error.message()}),
                );
                core.fatal_error = Some(error.message().to_owned());
                state.shutdown.notify_one();
            }
        }
        super::super::metrics::rpc(
            operation_name,
            queued.as_nanos(),
            lock_wait.as_nanos(),
            servicing.elapsed().as_nanos(),
            started.elapsed().as_nanos(),
        );
        result
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
        let response = with_coordinator_core(self.state.clone(), "register_worker", move |core| {
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
        let response = with_coordinator_core(self.state.clone(), "heartbeat", move |core| {
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
        let response = with_coordinator_core(self.state.clone(), "claim_one", move |core| {
            require_worker(core, &request.worker_id)?;
            let run_id = core.run_id.clone();
            let quota_satisfied = core.quota_satisfied_source_ids()?;
            if core.target_reached()?
                || core
                    .frontier
                    .unfinished_task_count_excluding(&run_id, &quota_satisfied)
                    .map_err(to_status)?
                    == 0
            {
                let abandoned = core.frontier.finalize_abandoned_sitemap_discovery(&run_id).map_err(to_status)?;
                if abandoned > 0 {
                    emit_shared(&core.events, "sitemap.discovery_abandoned", "failure",
                        serde_json::json!({"count":abandoned,"reason":"frontier/run stopped before discovery committed"}));
                }
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
                .claim_one_excluding(&run_id, &request.worker_id, timing, &quota_satisfied)
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
        let response = with_coordinator_core(self.state.clone(), "renew_lease", move |core| {
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
        let response =
            with_coordinator_core(self.state.clone(), "acquire_request_permit", move |core| {
                let task = core.validate_lease(&lease)?;
                let (origin, url) =
                    core.validate_request_url(&request.url, &request.request_kind)?;
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
        let response =
            with_coordinator_core(self.state.clone(), "cancel_request_wait", move |core| {
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
        let response =
            with_coordinator_core(self.state.clone(), "release_request_permit", move |core| {
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
        let response = with_coordinator_core(self.state.clone(), "robots_decision", move |core| {
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
        let response =
            with_coordinator_core(self.state.clone(), "commit_robots_refresh", move |core| {
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
        let response =
            with_coordinator_core(self.state.clone(), "fail_robots_refresh", move |core| {
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

    async fn claim_sitemap_discovery(
        &self,
        request: Request<proto::SitemapDiscoveryRequest>,
    ) -> Result<Response<proto::SitemapDiscoveryResponse>, Status> {
        super::sitemaps::claim(self.state.clone(), request).await
    }

    async fn commit_sitemap_discovery(
        &self,
        request: Request<proto::CommitSitemapDiscoveryRequest>,
    ) -> Result<Response<proto::CommitSitemapDiscoveryResponse>, Status> {
        super::sitemaps::commit(self.state.clone(), request).await
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
        let response = with_coordinator_core(self.state.clone(), "complete_task", move |core| {
            let task = claimed_task(&lease)?;
            core.validate_task_identity(&task)?;
            let capture_received = request.capture.is_some();
            if let Some(capture) = request.capture.as_ref() {
                validate_capture(&core.config, &task, capture)?;
            } else if request.rejection_reason.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "completion requires capture or rejection reason",
                ));
            }
            let stored_capture = request.capture.as_ref().map(StoredCapture::from);
            if let Some(capture) = request.capture.as_ref() {
                write_body_file(&core.crawl_dir, capture)?;
            }
            let result_sha256 = completion_result_sha256(
                stored_capture.as_ref(),
                &request.discovered_urls,
                &request.rejection_reason,
            )?;
            let (discovered, initially_rejected) =
                eligible_discoveries(&core.config, &request.discovered_urls, core.config.max_frontier);
            let accepted_capture = stored_capture
                .as_ref()
                .map(StoredCapture::to_frontier_capture)
                .transpose()?;
            let acceptance = core
                .frontier
                .accept_task_result(
                    &task,
                    &result_sha256,
                    accepted_capture.as_ref(),
                    &request.rejection_reason,
                    &discovered,
                    initially_rejected,
                    core.config.max_attempts as i32,
                    core.config.max_frontier,
                )
                .map_err(to_status)?;
            let (completed, accepted_discovered_count, rejected_discovered_count) = match acceptance {
                CompletionAcceptance::Accepted {
                    accepted_discovered_count,
                    rejected_discovered_count,
                }
                | CompletionAcceptance::Duplicate {
                    accepted_discovered_count,
                    rejected_discovered_count,
                } => {
                    if let Some(capture) = stored_capture.as_ref() {
                        let capture = capture
                            .to_crawl_capture()
                            .map_err(to_status)?;
                        core.spool
                            .insert_capture(&capture, &capture_selection_key(&capture))
                            .map_err(|error| to_status(Error::storage(error.message)))?;
                    }
                    (true, accepted_discovered_count as u64, rejected_discovered_count as u64)
                }
                CompletionAcceptance::Stale => (false, 0, 0),
            };
            let target_reached = core.target_reached()?;
            if completed && matches!(acceptance, CompletionAcceptance::Accepted { .. }) {
                let safe_url = safe_event_url(&task.normalized_url);
                emit_shared(
                    &core.events,
                    if capture_received {
                        "capture.accepted"
                    } else {
                        "page.rejected"
                    },
                    "success",
                    serde_json::json!({
                        "task_id": task.task_id,
                        "source_id": task.source_id,
                        "url": safe_url,
                        "capture_received": capture_received,
                        "body_bytes": stored_capture.as_ref().map(|capture| capture.content_bytes),
                        "body_sha256": stored_capture.as_ref().map(|capture| capture.body_hash.as_str()),
                        "rejection_reason": request.rejection_reason,
                        "accepted_discovered_count": accepted_discovered_count,
                        "rejected_discovered_count": rejected_discovered_count,
                        "capture_persistence": "postgresql_transaction",
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
        let response = with_coordinator_core(self.state.clone(), "fail_task", move |core| {
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
                if status == "failed" {
                    "fetch.failed"
                } else {
                    "task.failed"
                },
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
