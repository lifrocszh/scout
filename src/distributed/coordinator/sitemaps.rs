//! Thin RPC boundary for durable optional discovery. Page completion remains independent.
use super::rpc::with_coordinator_db;
use super::*;

pub(super) async fn claim(
    state: Arc<CoordinatorState>,
    request: Request<proto::SitemapDiscoveryRequest>,
) -> Result<Response<proto::SitemapDiscoveryResponse>, Status> {
    let request = request.into_inner();
    check_protocol(request.protocol_version)?;
    let lease = request
        .lease
        .ok_or_else(|| Status::invalid_argument("lease is required"))?;
    let origin = request.origin;
    let (response, claimed_task) = with_coordinator_db(
        state.clone(),
        "claim_sitemap_discovery",
        move |context, snapshot, frontier| {
            let task = context.validate_lease(&lease, snapshot, frontier)?;
            if origin != task.origin {
                return Err(Status::permission_denied(
                    "sitemap origin must match task origin",
                ));
            }
            let epoch = frontier
                .claim_sitemap_discovery(&task, context.config.max_attempts as i32)
                .map_err(to_status)?;
            Ok((
                proto::SitemapDiscoveryResponse {
                    protocol_version: PROTOCOL_VERSION,
                    claimed: epoch.is_some(),
                    scan_epoch: epoch.unwrap_or_default(),
                },
                epoch.map(|epoch| (task, epoch)),
            ))
        },
    )
    .await?;
    if let Some((task, epoch)) = claimed_task {
        emit_shared(
            &state.context.events,
            "sitemap.discovery_claimed",
            "success",
            serde_json::json!({"origin":task.origin,"task_id":task.task_id,"worker_id":task.lease_owner,"scan_epoch":epoch}),
        );
    }
    state.schedule_changed.notify_one();
    Ok(Response::new(response))
}

pub(super) async fn commit(
    state: Arc<CoordinatorState>,
    request: Request<proto::CommitSitemapDiscoveryRequest>,
) -> Result<Response<proto::CommitSitemapDiscoveryResponse>, Status> {
    let request = request.into_inner();
    check_protocol(request.protocol_version)?;
    let lease = request
        .lease
        .ok_or_else(|| Status::invalid_argument("lease is required"))?;
    let (discovered, rejected) = eligible_discoveries(
        &state.context.config,
        &request.discovered_urls,
        state.context.config.max_frontier,
    );
    let digest = crate::storage::sha256_hex(
        &serde_json::to_vec(&(
            &request.discovered_urls,
            &request.error_reason,
            request.retryable,
        ))
        .map_err(|error| Status::internal(format!("encode sitemap completion: {error}")))?,
    );
    let origin = request.origin;
    let scan_epoch = request.scan_epoch;
    let error_reason = request.error_reason;
    let event_error_reason = error_reason.clone();
    let retryable = request.retryable;
    let (response, committed_task) = with_coordinator_db(
        state.clone(),
        "commit_sitemap_discovery",
        move |context, snapshot, frontier| {
            let task = claimed_task(&lease)?;
            context.validate_task_identity(&task, snapshot)?;
            if origin != task.origin {
                return Err(Status::permission_denied(
                    "sitemap origin must match task origin",
                ));
            }
            let result = frontier
                .commit_sitemap_discovery(
                    &task,
                    &origin,
                    scan_epoch,
                    &digest,
                    &discovered,
                    rejected,
                    context.config.max_attempts as i32,
                    context.config.max_frontier,
                    &error_reason,
                    retryable,
                )
                .map_err(to_status)?;
            let response = proto::CommitSitemapDiscoveryResponse {
                protocol_version: PROTOCOL_VERSION,
                committed: result.is_some(),
                terminal: result.as_ref().is_some_and(|result| result.terminal),
                accepted_discovered_count: result
                    .as_ref()
                    .map_or(0, |result| result.accepted_discovered_count as u64),
                rejected_discovered_count: result
                    .as_ref()
                    .map_or(0, |result| result.rejected_discovered_count as u64),
            };
            Ok((response, result.map(|result| (task, result))))
        },
    )
    .await?;
    if let Some((task, result)) = committed_task {
        emit_shared(
            &state.context.events,
            "sitemap.discovery_committed",
            if event_error_reason.is_empty() {
                "success"
            } else {
                "failure"
            },
            serde_json::json!({"origin":task.origin,"task_id":task.task_id,"worker_id":task.lease_owner,"scan_epoch":scan_epoch,"error_reason":event_error_reason,"accepted_discovered_count":result.accepted_discovered_count,"rejected_discovered_count":result.rejected_discovered_count}),
        );
    }
    state.schedule_changed.notify_one();
    Ok(Response::new(response))
}
