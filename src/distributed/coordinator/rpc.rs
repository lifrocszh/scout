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
        if core.waits.has_pending()
            && matches!(
                operation_name,
                "claim_one"
                    | "acquire_request_permit"
                    | "cancel_request_wait"
                    | "release_request_permit"
                    | "renew_lease"
                    | "commit_robots_refresh"
                    | "fail_robots_refresh"
                    | "commit_sitemap_discovery"
                    | "complete_task"
                    | "fail_task"
            )
        {
            state.schedule_changed.notify_one();
        }
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
        let response = scheduling::claim(self.state.clone(), request).await?;
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
                scheduling::renew_pending(core, &task)?;
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
        let response = scheduling::permit(self.state.clone(), request).await?;
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
                if request.crawl_run_id != core.run_id {
                    return Err(Status::failed_precondition("crawl run ID mismatch"));
                }
                let success = core
                    .frontier
                    .cancel_request_wait(
                        &request.crawl_run_id,
                        &request.worker_id,
                        &request.request_id,
                    )
                    .map_err(to_status)?;
                if success {
                    scheduling::cancel_pending(core, &request.worker_id, &request.request_id);
                }
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
        let size = prost::Message::encoded_len(&request).max(1);
        let lease = request
            .lease
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        let reservation = self.state.results.reserve(size)?;
        let state = self.state.clone();
        let rpc_started = std::time::Instant::now();
        let blocking_started = std::time::Instant::now();
        let blocking_queue_ns = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let core_lock_ns = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let intake_service_ns = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let queue_metric = blocking_queue_ns.clone();
        let lock_metric = core_lock_ns.clone();
        let service_metric = intake_service_ns.clone();
        let failure_state = self.state.clone();
        let (reply, receive) = tokio::sync::oneshot::channel();
        let intake = tokio::task::spawn_blocking(move || {
            queue_metric.store(
                blocking_started.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            let lock_started = std::time::Instant::now();
            let (config, crawl_dir) = {
                let core = state.core.lock().expect("coordinator state lock");
                let core_service_started = std::time::Instant::now();
                lock_metric.store(
                    lock_started.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                if let Some(message) = core.fatal_error.as_ref() {
                    return Err(Status::internal(message.clone()));
                }
                let task = claimed_task(&lease)?;
                core.validate_task_identity(&task)?;
                let snapshot = (core.config.clone(), core.crawl_dir.clone());
                service_metric.store(
                    core_service_started.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                snapshot
            };
            let task = claimed_task(&lease)?;
            let validation_started = std::time::Instant::now();
            if let Some(capture) = request.capture.as_ref() {
                validate_capture(&config, &task, capture)?;
            } else if request.rejection_reason.trim().is_empty() {
                return Err(Status::invalid_argument(
                    "completion requires capture or rejection reason",
                ));
            }
            let stored_capture = request.capture.as_ref().map(StoredCapture::from);
            let result_sha256 = completion_result_sha256(
                stored_capture.as_ref(),
                &request.discovered_urls,
                &request.rejection_reason,
            )?;
            let (discovered, initially_rejected) =
                eligible_discoveries(&config, &request.discovered_urls, config.max_frontier);
            let accepted_capture = stored_capture
                .as_ref()
                .map(StoredCapture::to_frontier_capture)
                .transpose()?;
            crate::distributed::metrics::completion_phase("validation_hashing", validation_started);
            if let Some(capture) = request.capture.as_ref() {
                let started = std::time::Instant::now();
                write_body_file(&crawl_dir, capture)?;
                crate::distributed::metrics::completion_phase("body_write_fsync", started);
            }
            let job = super::results::Job::new(
                task,
                result_sha256,
                stored_capture,
                accepted_capture,
                request.rejection_reason,
                discovered,
                initially_rejected,
                config.max_attempts as i32,
                config.max_frontier,
                reply,
            );
            state.results.submit(job, reservation);
            Ok(())
        })
        .await
        .map_err(|error| Status::internal(format!("completion intake task failed: {error}")))?;
        if let Err(error) = &intake {
            if error.code() == tonic::Code::Internal {
                let message = error.message().to_owned();
                let _ = tokio::task::spawn_blocking(move || {
                    let mut core = failure_state.core.lock().expect("coordinator state lock");
                    if core.fatal_error.is_none() {
                        let run_id = core.run_id.clone();
                        let _ = core.frontier.mark_run_failed(&run_id);
                        emit_shared(
                            &core.events,
                            "coordinator.failed",
                            "failure",
                            serde_json::json!({"crawl_run_id": run_id, "error": message}),
                        );
                        core.fatal_error = Some(message);
                        failure_state.shutdown.notify_one();
                    }
                })
                .await;
            }
        }
        intake?;
        let response = receive
            .await
            .map_err(|_| Status::unavailable("completion processor stopped"))?;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                let total = rpc_started.elapsed().as_nanos() as u64;
                crate::distributed::metrics::rpc(
                    "complete_task",
                    blocking_queue_ns.load(std::sync::atomic::Ordering::Relaxed) as u128,
                    core_lock_ns.load(std::sync::atomic::Ordering::Relaxed) as u128,
                    intake_service_ns.load(std::sync::atomic::Ordering::Relaxed) as u128,
                    total as u128,
                );
                return Err(error);
            }
        };
        let total = rpc_started.elapsed().as_nanos() as u64;
        crate::distributed::metrics::rpc(
            "complete_task",
            blocking_queue_ns.load(std::sync::atomic::Ordering::Relaxed) as u128,
            core_lock_ns.load(std::sync::atomic::Ordering::Relaxed) as u128,
            intake_service_ns.load(std::sync::atomic::Ordering::Relaxed) as u128,
            total as u128,
        );
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

pub(super) fn require_worker(core: &CoordinatorCore, worker_id: &str) -> Result<(), Status> {
    validate_identity(worker_id, "worker_id")?;
    if core.registered_workers.contains(worker_id) {
        Ok(())
    } else {
        Err(Status::failed_precondition(
            "worker must negotiate protocol first",
        ))
    }
}

pub(super) fn task_to_proto(task: &ClaimedTask) -> proto::Lease {
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
