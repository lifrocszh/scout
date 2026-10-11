//! Queued unary RPCs are worker capacity credits. One dispatcher sends ready replies.
//! Durable frontier policy remains authoritative; disconnected pending calls never grant HTTP.
use super::rpc::{require_worker, task_to_proto};
use super::*;
use std::collections::{HashMap, VecDeque};
use tokio::sync::oneshot;

type Reply<T> = oneshot::Sender<Result<T, Status>>;

#[derive(Default)]
pub(super) struct Waits {
    claims: VecDeque<(String, Reply<proto::ClaimOneResponse>)>,
    permits: HashMap<String, PendingPermit>,
    closed: bool,
}

impl Waits {
    pub(super) fn has_pending(&self) -> bool {
        !self.claims.is_empty() || !self.permits.is_empty()
    }
}

struct PendingPermit {
    request: proto::AcquireRequestPermitRequest,
    replies: Vec<Reply<proto::AcquireRequestPermitResponse>>,
}

// Dropping an RPC only wakes cleanup. An already granted permit stays active until report/expiry.
struct WakeOnDrop(Arc<CoordinatorState>);
impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.schedule_changed.notify_one();
    }
}

fn check_capacity(core: &CoordinatorCore) -> Result<(), Status> {
    let count = core.waits.claims.len()
        + core
            .waits
            .permits
            .values()
            .map(|wait| wait.replies.len())
            .sum::<usize>();
    if core.waits.closed {
        return Err(Status::unavailable("coordinator stopping"));
    }
    if count >= core.config.max_frontier {
        return Err(Status::resource_exhausted(
            "coordinator pending capacity limit reached",
        ));
    }
    Ok(())
}

pub(super) async fn claim(
    state: Arc<CoordinatorState>,
    request: proto::ClaimOneRequest,
) -> Result<proto::ClaimOneResponse, Status> {
    let (reply, receive) = oneshot::channel();
    let _cleanup = WakeOnDrop(state.clone());
    rpc::with_coordinator_core(state, "claim_one", move |core| {
        require_worker(core, &request.worker_id)?;
        check_capacity(core)?;
        core.waits.claims.push_back((request.worker_id, reply));
        Ok(())
    })
    .await?;
    receive
        .await
        .map_err(|_| Status::unavailable("coordinator dispatcher stopped"))?
}

pub(super) async fn permit(
    state: Arc<CoordinatorState>,
    request: proto::AcquireRequestPermitRequest,
) -> Result<proto::AcquireRequestPermitResponse, Status> {
    let (reply, receive) = oneshot::channel();
    let _cleanup = WakeOnDrop(state.clone());
    rpc::with_coordinator_core(state, "acquire_request_permit", move |core| {
        check_capacity(core)?;
        let lease = request
            .lease
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("lease is required"))?;
        let task = core.validate_lease(lease)?;
        let (origin, url) = core.validate_request_url(&request.url, &request.request_kind)?;
        if let Some(wait) = core.waits.permits.get_mut(&request.request_id) {
            if wait.request != request {
                return Err(Status::invalid_argument(
                    "request_id was reused with different queued request metadata",
                ));
            }
            wait.replies.push(reply);
            return Ok(());
        }
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
        match decision {
            PermitDecision::Waiting(_) => {
                core.frontier
                    .bind_request_waits_to_lease(&task, &[request.request_id.clone()])
                    .map_err(to_status)?;
                core.waits.permits.insert(
                    request.request_id.clone(),
                    PendingPermit {
                        request,
                        replies: vec![reply],
                    },
                );
            }
            decision => {
                let _ = reply.send(Ok(permit_response(decision)?));
            }
        }
        Ok(())
    })
    .await?;
    receive
        .await
        .map_err(|_| Status::unavailable("coordinator dispatcher stopped"))?
}

fn permit_response(
    decision: PermitDecision,
) -> Result<proto::AcquireRequestPermitResponse, Status> {
    let (decision, permit_id, expiry) = match decision {
        PermitDecision::Acquired(grant) => (
            proto::acquire_request_permit_response::Decision::Acquired,
            grant.permit_id,
            grant.expires_at_unix_ms,
        ),
        PermitDecision::AlreadyCompleted => (
            proto::acquire_request_permit_response::Decision::AlreadyCompleted,
            0,
            0,
        ),
        PermitDecision::Waiting(_) => {
            return Err(Status::internal(
                "dispatcher selected an ineligible request",
            ));
        }
    };
    Ok(proto::AcquireRequestPermitResponse {
        protocol_version: PROTOCOL_VERSION,
        decision: decision as i32,
        permit_id,
        expires_at_unix_ms: expiry,
        retry_after_ms: 0,
    })
}

pub(super) fn renew_pending(core: &mut CoordinatorCore, task: &ClaimedTask) -> Result<(), Status> {
    let ids: Vec<_> = core
        .waits
        .permits
        .values()
        .filter(|wait| {
            wait.request.lease.as_ref().is_some_and(|lease| {
                lease.task_id == task.task_id
                    && lease.lease_epoch == task.lease_epoch
                    && lease.lease_owner == task.lease_owner
            })
        })
        .map(|wait| wait.request.request_id.clone())
        .collect();
    core.frontier
        .bind_request_waits_to_lease(task, &ids)
        .map_err(to_status)
}

pub(super) fn cancel_pending(core: &mut CoordinatorCore, worker: &str, id: &str) {
    if core.waits.permits.get(id).is_some_and(|wait| {
        wait.request
            .lease
            .as_ref()
            .is_some_and(|lease| lease.lease_owner == worker)
    }) {
        let wait = core.waits.permits.remove(id).expect("pending wait");
        for reply in wait.replies {
            let _ = reply.send(Err(Status::cancelled("request wait cancelled")));
        }
    }
}

pub(super) fn close(state: &CoordinatorState) {
    let mut core = state.core.lock().expect("coordinator state lock");
    core.waits.closed = true;
    for (_, reply) in core.waits.claims.drain(..) {
        let _ = reply.send(Err(Status::unavailable("coordinator stopping")));
    }
    for wait in core.waits.permits.values_mut() {
        for reply in wait.replies.drain(..) {
            let _ = reply.send(Err(Status::unavailable("coordinator stopping")));
        }
    }
    state.schedule_changed.notify_one();
}

pub(super) async fn run(state: Arc<CoordinatorState>) {
    loop {
        // Notify stores a wakeup if mutation happens during the blocking dispatcher pass.
        let notified = state.schedule_changed.notified();
        let can_assign = state.results.ready_for_assignments();
        let result_jobs = state.results.queued_jobs();
        let max_outstanding = state.results.max_outstanding_tasks();
        let result = rpc::with_coordinator_core(state.clone(), "dispatch", move |core| {
            dispatch(core, can_assign, result_jobs, max_outstanding)
        })
        .await;
        match result {
            Ok((true, _)) => break,
            Err(_) => {
                close(&state);
                break;
            }
            Ok((false, delay)) => {
                if let Some(delay) = delay {
                    tokio::select! { _ = notified => {}, _ = tokio::time::sleep(delay) => {} }
                } else {
                    notified.await;
                }
            }
        }
    }
}

fn prune_permit_waits(core: &mut CoordinatorCore) -> Result<(), Status> {
    let ids: Vec<_> = core.waits.permits.keys().cloned().collect();
    let expired = if ids.is_empty() {
        Vec::new()
    } else {
        core.frontier
            .expired_request_waits(&core.run_id, &ids)
            .map_err(to_status)?
    };
    let cancelled: Vec<_> = core
        .waits
        .permits
        .iter_mut()
        .filter_map(|(id, wait)| {
            wait.replies.retain(|reply| !reply.is_closed());
            (wait.replies.is_empty() || expired.contains(id)).then(|| id.clone())
        })
        .collect();
    for id in cancelled {
        let wait = core.waits.permits.remove(&id).expect("pending wait");
        let worker = &wait
            .request
            .lease
            .as_ref()
            .expect("validated lease")
            .lease_owner;
        core.frontier
            .cancel_request_wait(&core.run_id, worker, &id)
            .map_err(to_status)?;
        for reply in wait.replies {
            let _ = reply.send(Err(Status::failed_precondition(
                "queued request lease expired or wait cancelled",
            )));
        }
    }
    Ok(())
}

fn stop_claims(core: &mut CoordinatorCore) -> Result<(), Status> {
    let run_id = core.run_id.clone();
    let abandoned = core
        .frontier
        .finalize_abandoned_sitemap_discovery(&run_id)
        .map_err(to_status)?;
    if abandoned > 0 {
        emit_shared(
            &core.events,
            "sitemap.discovery_abandoned",
            "failure",
            serde_json::json!({"count":abandoned,"reason":"frontier/run stopped before discovery committed"}),
        );
    }
    for (_, reply) in core.waits.claims.drain(..) {
        let _ = reply.send(Ok(proto::ClaimOneResponse {
            protocol_version: PROTOCOL_VERSION,
            lease: None,
            target_reached: true,
            retry_after_ms: 0,
        }));
    }
    Ok(())
}

fn dispatch(
    core: &mut CoordinatorCore,
    can_assign: bool,
    result_jobs: usize,
    max_outstanding: usize,
) -> Result<(bool, Option<Duration>), Status> {
    core.waits.claims.retain(|(_, reply)| !reply.is_closed());
    let run_id = core.run_id.clone();
    let reference = if core.waits.claims.is_empty() && core.waits.permits.is_empty() {
        0
    } else {
        core.frontier.dispatch_clock_ms().map_err(to_status)?
    };
    prune_permit_waits(core)?;
    if core.waits.closed {
        return Ok((true, None));
    }
    let mut excluded = Vec::new();
    if !core.waits.claims.is_empty() {
        excluded = core.quota_satisfied_source_ids()?;
        let stopped = core.target_reached()?
            || core
                .frontier
                .unfinished_task_count_excluding(&run_id, &excluded)
                .map_err(to_status)?
                == 0;
        if stopped {
            stop_claims(core)?;
        } else {
            let live_leases = core
                .frontier
                .live_leased_task_count_excluding(&run_id, &excluded)
                .map_err(to_status)?;
            let mut assignment_budget =
                max_outstanding.saturating_sub(live_leases.saturating_add(result_jobs));
            if can_assign {
                while assignment_budget > 0 {
                    let Some((worker, reply)) = core.waits.claims.pop_front() else {
                        break;
                    };
                    if reply.is_closed() {
                        continue;
                    }
                    let task = core
                        .frontier
                        .claim_one_excluding(&run_id, &worker, core.lease_timing, &excluded)
                        .map_err(to_status)?;
                    let Some(task) = task else {
                        core.waits.claims.push_front((worker, reply));
                        break;
                    };
                    emit_shared(
                        &core.events,
                        "task.claimed",
                        "success",
                        serde_json::json!({"worker_id":worker,"task_id":task.task_id,"source_id":task.source_id,"url":safe_event_url(&task.normalized_url),"lease_epoch":task.lease_epoch,"task_attempt":task.task_attempt_count}),
                    );
                    // If the RPC disappears here, its unacknowledged task remains fenced until lease expiry.
                    let _ = reply.send(Ok(proto::ClaimOneResponse {
                        protocol_version: PROTOCOL_VERSION,
                        lease: Some(task_to_proto(&task)),
                        target_reached: false,
                        retry_after_ms: 0,
                    }));
                    assignment_budget -= 1;
                }
            }
        }
    }
    // Claiming also marks final expired attempts failed; stop remaining credits immediately.
    if !core.waits.claims.is_empty()
        && core
            .frontier
            .unfinished_task_count_excluding(&run_id, &excluded)
            .map_err(to_status)?
            == 0
    {
        stop_claims(core)?;
    }
    let mut attempted_grant = false;
    let mut retry_delay = None;
    while !core.waits.permits.is_empty() {
        let Some(id) = core.frontier.ready_request_id(&run_id).map_err(to_status)? else {
            break;
        };
        let Some(wait) = core.waits.permits.remove(&id) else {
            return Err(Status::internal(
                "durable queued request has no connected waiter",
            ));
        };
        let decision = (|| {
            let task =
                core.validate_lease(wait.request.lease.as_ref().expect("validated lease"))?;
            let (origin, url) =
                core.validate_request_url(&wait.request.url, &wait.request.request_kind)?;
            let key = schedule_key(&core.config, &url);
            attempted_grant = true;
            let decision = core
                .frontier
                .try_acquire_request_permit(
                    &run_id,
                    &task.lease_owner,
                    &id,
                    &key,
                    &origin,
                    &wait.request.request_kind,
                )
                .map_err(to_status)?;
            Ok(decision)
        })();
        if let Ok(PermitDecision::Waiting(delay)) = decision {
            // Readiness is advisory; the transaction rechecks the durable policy.
            // Keep the waiter queued if policy changed between those checks.
            core.waits.permits.insert(id, wait);
            retry_delay = Some(delay);
            break;
        }
        let result = decision.and_then(permit_response);
        if result.is_err() {
            let worker = &wait
                .request
                .lease
                .as_ref()
                .expect("validated lease")
                .lease_owner;
            core.frontier
                .cancel_request_wait(&run_id, worker, &id)
                .map_err(to_status)?;
        }
        for reply in wait.replies {
            let _ = reply.send(result.clone());
        }
        if result
            .as_ref()
            .is_err_and(|error| error.code() == tonic::Code::Internal)
        {
            return result.map(|_| (false, None));
        }
    }
    // A grant transaction may expire another queue row after the first cleanup pass.
    // Resolve those orphaned receivers before arming a timer from the remaining DB rows.
    if attempted_grant && !core.waits.permits.is_empty() {
        prune_permit_waits(core)?;
    }
    let scheduler_delay = if core.waits.claims.is_empty() && core.waits.permits.is_empty() {
        None
    } else {
        core.frontier
            .next_dispatch_delay(
                &run_id,
                !core.waits.claims.is_empty(),
                !core.waits.permits.is_empty(),
                &excluded,
                core.config.max_duration_seconds,
                reference,
            )
            .map_err(to_status)?
    };
    let delay = match (retry_delay, scheduler_delay) {
        (Some(retry), Some(scheduled)) => Some(retry.min(scheduled)),
        (Some(retry), None) => Some(retry),
        (None, scheduled) => scheduled,
    };
    Ok((false, delay))
}
