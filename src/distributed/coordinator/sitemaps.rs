//! Thin RPC boundary for durable optional discovery. Page completion remains independent.
use super::rpc::with_coordinator_core;
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
    let response = with_coordinator_core(state,"claim_sitemap_discovery", move |core| {
        let task = core.validate_lease(&lease)?;
        if request.origin != task.origin { return Err(Status::permission_denied("sitemap origin must match task origin")); }
        let epoch = core.frontier.claim_sitemap_discovery(&task, core.config.max_attempts as i32).map_err(to_status)?;
        if let Some(epoch) = epoch {
            emit_shared(&core.events,"sitemap.discovery_claimed","success",serde_json::json!({"origin":request.origin,"task_id":task.task_id,"worker_id":task.lease_owner,"scan_epoch":epoch}));
        }
        Ok(proto::SitemapDiscoveryResponse {protocol_version:PROTOCOL_VERSION,claimed:epoch.is_some(),scan_epoch:epoch.unwrap_or_default()})
    }).await?;
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
    let response = with_coordinator_core(state,"commit_sitemap_discovery", move |core| {
        let task = claimed_task(&lease)?; core.validate_task_identity(&task)?;
        if request.origin != task.origin { return Err(Status::permission_denied("sitemap origin must match task origin")); }
        let (discovered,rejected) = eligible_discoveries(&core.config,&request.discovered_urls,core.config.max_frontier);
        let digest = crate::storage::sha256_hex(&serde_json::to_vec(&(&request.discovered_urls,&request.error_reason,request.retryable)).map_err(|error| Status::internal(format!("encode sitemap completion: {error}")))?);
        let result = core.frontier.commit_sitemap_discovery(&task,&request.origin,request.scan_epoch,&digest,&discovered,rejected,core.config.max_attempts as i32,core.config.max_frontier,&request.error_reason,request.retryable).map_err(to_status)?;
        if let Some(result) = result.as_ref() {
            emit_shared(&core.events,"sitemap.discovery_committed",if request.error_reason.is_empty() {"success"} else {"failure"},serde_json::json!({"origin":request.origin,"task_id":task.task_id,"worker_id":task.lease_owner,"scan_epoch":request.scan_epoch,"terminal":result.terminal,"error_reason":request.error_reason,"accepted_discovered_count":result.accepted_discovered_count,"rejected_discovered_count":result.rejected_discovered_count}));
        }
        Ok(proto::CommitSitemapDiscoveryResponse {protocol_version:PROTOCOL_VERSION,committed:result.is_some(),terminal:result.as_ref().is_some_and(|result| result.terminal),accepted_discovered_count:result.as_ref().map_or(0,|result|result.accepted_discovered_count as u64),rejected_discovered_count:result.as_ref().map_or(0,|result|result.rejected_discovered_count as u64)})
    }).await?;
    Ok(Response::new(response))
}
