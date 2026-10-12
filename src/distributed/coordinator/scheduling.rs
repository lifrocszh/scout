//! One owner serializes scheduling policy while all PostgreSQL work uses bounded connections.
use super::rpc::{self, require_worker, task_to_proto};
use super::*;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};

type Reply<T> = oneshot::Sender<Result<T, Status>>;

pub(super) struct SchedulerHandle {
    sender: mpsc::Sender<Command>,
    credits: Arc<AtomicUsize>,
    max_credits: usize,
    closed: Arc<AtomicBool>,
}

pub(super) struct Credit(Arc<AtomicUsize>);

struct PendingClaim {
    worker_id: String,
    reply: Reply<proto::ClaimOneResponse>,
    _credit: Credit,
}

struct ClaimCompletion {
    reply: Reply<proto::ClaimOneResponse>,
    result: Result<proto::ClaimOneResponse, Status>,
    _credit: Credit,
}

#[derive(Default)]
struct Waits {
    claims: VecDeque<PendingClaim>,
    permits: HashMap<String, PendingPermit>,
    initializing_permits: HashMap<String, InitialPermit>,
    next_permit_generation: u64,
    closed: bool,
}

struct PendingReply<T> {
    reply: Reply<T>,
    _credit: Credit,
}

struct PendingPermit {
    request: proto::AcquireRequestPermitRequest,
    replies: Vec<PendingReply<proto::AcquireRequestPermitResponse>>,
    generation: u64,
}

enum ReadyPermit {
    Initializing,
    Pending(PendingPermit),
    Missing,
}

fn take_ready_permit(waits: &mut Waits, request_id: &str) -> ReadyPermit {
    if waits.initializing_permits.contains_key(request_id) {
        ReadyPermit::Initializing
    } else {
        waits
            .permits
            .remove(request_id)
            .map(ReadyPermit::Pending)
            .unwrap_or(ReadyPermit::Missing)
    }
}

struct InitialPermit {
    request: proto::AcquireRequestPermitRequest,
    replies: Vec<PendingReply<proto::AcquireRequestPermitResponse>>,
    cancel_replies: Vec<Reply<proto::BoolResponse>>,
}

pub(super) enum Command {
    Claim {
        worker_id: String,
        reply: Reply<proto::ClaimOneResponse>,
        credit: Credit,
    },
    BeginPermit {
        request: proto::AcquireRequestPermitRequest,
        reply: Reply<proto::AcquireRequestPermitResponse>,
        credit: Credit,
    },
    FinishPermit {
        request_id: String,
        result: Result<PermitDecision, Status>,
    },
    CancelPermit {
        worker_id: String,
        crawl_run_id: String,
        request_id: String,
        reply: Reply<proto::BoolResponse>,
    },
    FinishInitialCancel {
        request_id: String,
        result: Result<bool, Status>,
    },
    FinishWaitCancel {
        request_id: String,
        generation: Option<u64>,
        reply: Reply<proto::BoolResponse>,
        result: Result<bool, Status>,
    },
    RenewPending {
        task: ClaimedTask,
        reply: Reply<()>,
    },
}

impl SchedulerHandle {
    fn credit(&self) -> Result<Credit, Status> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Status::unavailable("coordinator stopping"));
        }
        self.credits
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < self.max_credits).then_some(count + 1)
            })
            .map(|_| Credit(self.credits.clone()))
            .map_err(|_| Status::resource_exhausted("coordinator pending capacity limit reached"))
    }

    fn try_send(&self, command: Command) -> Result<(), Status> {
        match self.sender.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(command)) => {
                drop(command);
                Err(Status::resource_exhausted(
                    "coordinator pending capacity limit reached",
                ))
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                drop(command);
                Err(Status::unavailable("coordinator stopping"))
            }
        }
    }
}

impl Drop for Credit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) fn channel(max_credits: usize) -> (SchedulerHandle, mpsc::Receiver<Command>) {
    let (sender, receiver) = mpsc::channel(max_credits.max(1));
    (
        SchedulerHandle {
            sender,
            credits: Arc::new(AtomicUsize::new(0)),
            max_credits,
            closed: Arc::new(AtomicBool::new(false)),
        },
        receiver,
    )
}

// Dropping a held RPC only wakes cleanup. An already granted permit stays active until report/expiry.
struct WakeOnDrop(Arc<CoordinatorState>);
impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.schedule_changed.notify_one();
    }
}

pub(super) async fn claim(
    state: Arc<CoordinatorState>,
    request: proto::ClaimOneRequest,
) -> Result<proto::ClaimOneResponse, Status> {
    let worker_id = request.worker_id.clone();
    rpc::with_coordinator_memory(state.clone(), "claim_one_admission", move |_, memory| {
        require_worker(&memory.registered_workers, &worker_id)
    })
    .await?;
    let credit = state.scheduler.credit()?;
    let (reply, receive) = oneshot::channel();
    let _cleanup = WakeOnDrop(state.clone());
    state.scheduler.try_send(Command::Claim {
        worker_id: request.worker_id,
        reply,
        credit,
    })?;
    state.schedule_changed.notify_one();
    receive
        .await
        .map_err(|_| Status::unavailable("coordinator dispatcher stopped"))?
}

pub(super) async fn permit(
    state: Arc<CoordinatorState>,
    request: proto::AcquireRequestPermitRequest,
) -> Result<proto::AcquireRequestPermitResponse, Status> {
    // Capacity is reserved before actor-side validation, matching the RPC's admission order.
    let credit = state.scheduler.credit()?;
    let (reply, receive) = oneshot::channel();
    let _cleanup = WakeOnDrop(state.clone());
    state.scheduler.try_send(Command::BeginPermit {
        request,
        reply,
        credit,
    })?;
    state.schedule_changed.notify_one();
    receive
        .await
        .map_err(|_| Status::unavailable("coordinator dispatcher stopped"))?
}

pub(super) async fn cancel_pending(
    state: Arc<CoordinatorState>,
    worker_id: String,
    crawl_run_id: String,
    request_id: String,
) -> Result<proto::BoolResponse, Status> {
    let (reply, receive) = oneshot::channel();
    state.scheduler.try_send(Command::CancelPermit {
        worker_id,
        crawl_run_id,
        request_id,
        reply,
    })?;
    state.schedule_changed.notify_one();
    receive
        .await
        .map_err(|_| Status::unavailable("coordinator dispatcher stopped"))?
}

pub(super) async fn renew_pending(
    state: Arc<CoordinatorState>,
    task: ClaimedTask,
) -> Result<(), Status> {
    let (reply, receive) = oneshot::channel();
    state
        .scheduler
        .try_send(Command::RenewPending { task, reply })?;
    state.schedule_changed.notify_one();
    receive
        .await
        .map_err(|_| Status::unavailable("coordinator dispatcher stopped"))?
}

pub(super) fn close(state: &CoordinatorState) {
    state.scheduler.closed.store(true, Ordering::Relaxed);
    state.schedule_changed.notify_one();
}

pub(super) async fn run(state: Arc<CoordinatorState>, mut commands: mpsc::Receiver<Command>) {
    let mut waits = Waits::default();
    let mut jobs = tokio::task::JoinSet::new();
    loop {
        if state.scheduler.closed.load(Ordering::Relaxed) {
            close_waits(&mut waits);
            break;
        }
        while jobs.try_join_next().is_some() {}
        match commands.try_recv() {
            Ok(command) => {
                process_command(&state, &mut waits, command, &mut jobs).await;
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                close_waits(&mut waits);
                break;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        let notified = state.schedule_changed.notified();
        let can_assign = state.results.ready_for_assignments();
        let result_jobs = state.results.queued_jobs();
        let max_outstanding = state.results.max_outstanding_tasks();
        let dispatch_started = std::time::Instant::now();
        let waits_for_dispatch = std::mem::take(&mut waits);
        let state_for_dispatch = state.clone();
        let dispatched = tokio::task::spawn_blocking(move || {
            let mut waits = waits_for_dispatch;
            let queued = dispatch_started.elapsed();
            let started = std::time::Instant::now();
            let mut events = Vec::new();
            let mut claim_completions = Vec::new();
            let mut lock_wait = 0;
            let mut lock_hold = 0;
            let (result, pool_wait) = state_for_dispatch.frontier.with_store(|frontier| {
                let (snapshot, waited, held) = rpc::memory_snapshot(&state_for_dispatch);
                lock_wait = waited;
                lock_hold = held;
                if let Some(message) = snapshot.fatal_error.as_ref() {
                    return Err(Status::internal(message.clone()));
                }
                dispatch(
                    &state_for_dispatch.context,
                    &snapshot,
                    frontier,
                    &mut waits,
                    can_assign,
                    result_jobs,
                    max_outstanding,
                    &mut events,
                    &mut claim_completions,
                )
            });
            emit_events(&state_for_dispatch.context.events, events);
            for completion in claim_completions {
                let _ = completion.reply.send(completion.result);
            }
            crate::distributed::metrics::rpc(
                "dispatch",
                queued.as_nanos(),
                lock_wait,
                lock_hold,
                pool_wait,
                started.elapsed().as_nanos().saturating_sub(pool_wait),
                dispatch_started.elapsed().as_nanos(),
            );
            if let Err(error) = &result {
                if error.code() == tonic::Code::Internal {
                    rpc::fail_coordinator(&state_for_dispatch, error.message());
                }
            }
            (waits, result)
        })
        .await;
        let (next_waits, result) = match dispatched {
            Ok(result) => result,
            Err(_) => {
                close_waits(&mut waits);
                break;
            }
        };
        waits = next_waits;
        match result {
            Ok((true, _)) => {
                close_waits(&mut waits);
                break;
            }
            Err(_) => {
                close_waits(&mut waits);
                break;
            }
            Ok((false, delay)) => {
                tokio::select! {
                    _ = notified => {},
                    command = commands.recv() => match command {
                        Some(command) => process_command(&state, &mut waits, command, &mut jobs).await,
                        None => { close_waits(&mut waits); break; }
                    },
                    _ = jobs.join_next(), if !jobs.is_empty() => {},
                    _ = async {
                        if let Some(delay) = delay { tokio::time::sleep(delay).await; }
                        else { std::future::pending::<()>().await; }
                    } => {},
                }
            }
        }
    }
    // Jobs can be blocked sending completion commands. Drop receiver first, then join all
    // jobs while this dispatcher still owns State; its synchronous PostgreSQL clients must
    // be dropped after runtime.block_on returns, not from a child Tokio task.
    drop(commands);
    while jobs.join_next().await.is_some() {}
}

async fn process_command(
    state: &Arc<CoordinatorState>,
    waits: &mut Waits,
    command: Command,
    jobs: &mut tokio::task::JoinSet<()>,
) {
    match command {
        Command::Claim {
            worker_id,
            reply,
            credit,
        } => {
            if state.scheduler.closed.load(Ordering::Relaxed) || waits.closed {
                let _ = reply.send(Err(Status::unavailable("coordinator stopping")));
                return;
            }
            waits.claims.push_back(PendingClaim {
                worker_id,
                reply,
                _credit: credit,
            });
        }
        Command::BeginPermit {
            request,
            reply,
            credit,
        } => begin_permit(state, waits, request, reply, credit, jobs),
        Command::FinishPermit { request_id, result } => {
            finish_permit(state, waits, request_id, result, jobs);
        }
        Command::CancelPermit {
            worker_id,
            crawl_run_id,
            request_id,
            reply,
        } => {
            let snapshot = rpc::memory_snapshot(state).0;
            if let Err(error) = require_worker(&snapshot.registered_workers, &worker_id) {
                let _ = reply.send(Err(error));
                return;
            }
            if crawl_run_id != state.context.run_id {
                let _ = reply.send(Err(Status::failed_precondition("crawl run ID mismatch")));
                return;
            }
            if let Some(initial) = waits.initializing_permits.get_mut(&request_id) {
                if initial
                    .request
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.lease_owner == worker_id)
                {
                    // The initial transaction has not completed. Defer cancellation until its
                    // outcome is known so a cancel cannot lose a race and leave a new row behind.
                    initial.cancel_replies.push(reply);
                } else {
                    start_untracked_cancel(state, request_id, worker_id, crawl_run_id, reply, jobs);
                }
            } else if waits.permits.contains_key(&request_id) {
                let generation = waits
                    .permits
                    .get(&request_id)
                    .map(|wait| wait.generation)
                    .expect("wait checked above");
                start_wait_cancel(
                    state,
                    request_id,
                    generation,
                    worker_id,
                    crawl_run_id,
                    reply,
                    jobs,
                );
            } else {
                start_untracked_cancel(state, request_id, worker_id, crawl_run_id, reply, jobs);
            }
        }
        Command::FinishInitialCancel { request_id, result } => {
            finish_initial_cancel(waits, request_id, result);
        }
        Command::FinishWaitCancel {
            request_id,
            generation,
            reply,
            result,
        } => match result {
            Ok(success) => {
                if success
                    && generation.is_some_and(|generation| {
                        waits
                            .permits
                            .get(&request_id)
                            .is_some_and(|wait| wait.generation == generation)
                    })
                {
                    if let Some(wait) = waits.permits.remove(&request_id) {
                        for pending in wait.replies {
                            let _ = pending
                                .reply
                                .send(Err(Status::cancelled("request wait cancelled")));
                        }
                    }
                }
                let _ = reply.send(Ok(proto::BoolResponse {
                    protocol_version: PROTOCOL_VERSION,
                    success,
                }));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        },
        Command::RenewPending { task, reply } => {
            let mut request_ids = Vec::new();
            for (id, wait) in &waits.permits {
                if wait.request.lease.as_ref().is_some_and(|lease| {
                    lease.task_id == task.task_id
                        && lease.lease_epoch == task.lease_epoch
                        && lease.lease_owner == task.lease_owner
                }) {
                    request_ids.push(id.clone());
                }
            }
            for (id, initial) in &waits.initializing_permits {
                if initial.request.lease.as_ref().is_some_and(|lease| {
                    lease.task_id == task.task_id
                        && lease.lease_epoch == task.lease_epoch
                        && lease.lease_owner == task.lease_owner
                }) {
                    request_ids.push(id.clone());
                }
            }
            let state = state.clone();
            jobs.spawn(async move {
                let result = rpc::with_coordinator_db(
                    state.clone(),
                    "renew_pending_request_waits",
                    move |_, _, frontier| {
                        frontier
                            .bind_request_waits_to_lease(&task, &request_ids)
                            .map_err(to_status)
                    },
                )
                .await;
                let _ = reply.send(result);
            });
        }
    }
}

fn begin_permit(
    state: &Arc<CoordinatorState>,
    waits: &mut Waits,
    request: proto::AcquireRequestPermitRequest,
    reply: Reply<proto::AcquireRequestPermitResponse>,
    credit: Credit,
    jobs: &mut tokio::task::JoinSet<()>,
) {
    if state.scheduler.closed.load(Ordering::Relaxed) || waits.closed {
        let _ = reply.send(Err(Status::unavailable("coordinator stopping")));
        return;
    }
    if let Some(initial) = waits.initializing_permits.get_mut(&request.request_id) {
        if initial.request != request {
            let _ = reply.send(Err(Status::invalid_argument(
                "request_id was reused with different queued request metadata",
            )));
        } else {
            initial.replies.push(PendingReply {
                reply,
                _credit: credit,
            });
        }
        return;
    }
    if let Some(wait) = waits.permits.get_mut(&request.request_id) {
        if wait.request != request {
            let _ = reply.send(Err(Status::invalid_argument(
                "request_id was reused with different queued request metadata",
            )));
        } else {
            wait.replies.push(PendingReply {
                reply,
                _credit: credit,
            });
        }
        return;
    }
    let request_id = request.request_id.clone();
    let pending = InitialPermit {
        request: request.clone(),
        replies: vec![PendingReply {
            reply,
            _credit: credit,
        }],
        cancel_replies: Vec::new(),
    };
    waits
        .initializing_permits
        .insert(request_id.clone(), pending);
    start_initial_permit(state, request, jobs);
}

fn start_initial_permit(
    state: &Arc<CoordinatorState>,
    request: proto::AcquireRequestPermitRequest,
    jobs: &mut tokio::task::JoinSet<()>,
) {
    let state = state.clone();
    jobs.spawn(async move {
        let result = initial_permit(state.clone(), request.clone()).await;
        let command = Command::FinishPermit {
            request_id: request.request_id,
            result,
        };
        let _ = state.scheduler.sender.send(command).await;
    });
}

async fn initial_permit(
    state: Arc<CoordinatorState>,
    request: proto::AcquireRequestPermitRequest,
) -> Result<PermitDecision, Status> {
    let lease = request
        .lease
        .clone()
        .ok_or_else(|| Status::invalid_argument("lease is required"))?;
    let task = rpc::with_coordinator_db(
        state.clone(),
        "acquire_request_permit_lease_check",
        move |context, snapshot, frontier| context.validate_lease(&lease, snapshot, frontier),
    )
    .await?;
    let (origin, url) = state
        .context
        .validate_request_url(&request.url, &request.request_kind)?;
    let schedule_key = schedule_key(&state.context.config, &url);
    let worker_id = task.lease_owner.clone();
    let request_id = request.request_id.clone();
    let request_kind = request.request_kind.clone();
    rpc::with_coordinator_db(state, "acquire_request_permit", move |_, _, frontier| {
        frontier
            .try_acquire_request_permit_for_task(
                &task,
                &worker_id,
                &request_id,
                &schedule_key,
                &origin,
                &request_kind,
            )
            .map_err(to_status)
    })
    .await
}

fn finish_permit(
    state: &Arc<CoordinatorState>,
    waits: &mut Waits,
    request_id: String,
    result: Result<PermitDecision, Status>,
    jobs: &mut tokio::task::JoinSet<()>,
) {
    let Some(initial) = waits.initializing_permits.remove(&request_id) else {
        return;
    };
    match result {
        Ok(PermitDecision::Waiting(_)) if !initial.cancel_replies.is_empty() => {
            let cancel_request = initial.request.clone();
            waits
                .initializing_permits
                .insert(request_id.clone(), initial);
            start_initial_cancel(state, request_id, cancel_request, jobs);
        }
        Ok(PermitDecision::Waiting(_)) => {
            waits.permits.insert(
                request_id,
                PendingPermit {
                    request: initial.request,
                    replies: initial.replies,
                    generation: {
                        let generation = waits.next_permit_generation;
                        waits.next_permit_generation = generation.wrapping_add(1);
                        generation
                    },
                },
            );
            state.schedule_changed.notify_one();
        }
        Ok(decision) => {
            let response = permit_response(decision);
            for pending in initial.replies {
                let _ = pending.reply.send(response.clone());
            }
            for reply in initial.cancel_replies {
                let _ = reply.send(Ok(proto::BoolResponse {
                    protocol_version: PROTOCOL_VERSION,
                    success: false,
                }));
            }
        }
        Err(error) => {
            for pending in initial.replies {
                let _ = pending.reply.send(Err(error.clone()));
            }
            for reply in initial.cancel_replies {
                let _ = reply.send(Ok(proto::BoolResponse {
                    protocol_version: PROTOCOL_VERSION,
                    success: false,
                }));
            }
        }
    }
}

fn start_initial_cancel(
    state: &Arc<CoordinatorState>,
    request_id: String,
    request: proto::AcquireRequestPermitRequest,
    jobs: &mut tokio::task::JoinSet<()>,
) {
    let state = state.clone();
    jobs.spawn(async move {
        let result = cancel_request_wait(&state, &request_id, &request).await;
        let _ = state
            .scheduler
            .sender
            .send(Command::FinishInitialCancel { request_id, result })
            .await;
    });
}

async fn cancel_request_wait(
    state: &Arc<CoordinatorState>,
    request_id: &str,
    request: &proto::AcquireRequestPermitRequest,
) -> Result<bool, Status> {
    let lease = request
        .lease
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("lease is required"))?;
    let worker_id = lease.lease_owner.clone();
    let run_id = state.context.run_id.clone();
    let request_id = request_id.to_owned();
    rpc::with_coordinator_db(
        state.clone(),
        "cancel_request_wait",
        move |context, snapshot, frontier| {
            require_worker(&snapshot.registered_workers, &worker_id)?;
            if run_id != context.run_id {
                return Err(Status::failed_precondition("crawl run ID mismatch"));
            }
            frontier
                .cancel_request_wait(&run_id, &worker_id, &request_id)
                .map_err(to_status)
        },
    )
    .await
}

fn start_wait_cancel(
    state: &Arc<CoordinatorState>,
    request_id: String,
    generation: u64,
    worker_id: String,
    run_id: String,
    reply: Reply<proto::BoolResponse>,
    jobs: &mut tokio::task::JoinSet<()>,
) {
    start_cancel(
        state,
        request_id,
        Some(generation),
        worker_id,
        run_id,
        reply,
        jobs,
    );
}

fn start_untracked_cancel(
    state: &Arc<CoordinatorState>,
    request_id: String,
    worker_id: String,
    run_id: String,
    reply: Reply<proto::BoolResponse>,
    jobs: &mut tokio::task::JoinSet<()>,
) {
    start_cancel(state, request_id, None, worker_id, run_id, reply, jobs);
}

fn start_cancel(
    state: &Arc<CoordinatorState>,
    request_id: String,
    generation: Option<u64>,
    worker_id: String,
    run_id: String,
    reply: Reply<proto::BoolResponse>,
    jobs: &mut tokio::task::JoinSet<()>,
) {
    let state = state.clone();
    jobs.spawn(async move {
        let id = request_id.clone();
        let result = rpc::with_coordinator_db(
            state.clone(),
            "cancel_request_wait",
            move |context, snapshot, frontier| {
                require_worker(&snapshot.registered_workers, &worker_id)?;
                if run_id != context.run_id {
                    return Err(Status::failed_precondition("crawl run ID mismatch"));
                }
                frontier
                    .cancel_request_wait(&run_id, &worker_id, &id)
                    .map_err(to_status)
            },
        )
        .await;
        let _ = state
            .scheduler
            .sender
            .send(Command::FinishWaitCancel {
                request_id,
                generation,
                reply,
                result,
            })
            .await;
    });
}

fn finish_initial_cancel(waits: &mut Waits, request_id: String, result: Result<bool, Status>) {
    let Some(initial) = waits.initializing_permits.remove(&request_id) else {
        return;
    };
    match result {
        Ok(true) => {
            for pending in initial.replies {
                let _ = pending
                    .reply
                    .send(Err(Status::cancelled("request wait cancelled")));
            }
            for reply in initial.cancel_replies {
                let _ = reply.send(Ok(proto::BoolResponse {
                    protocol_version: PROTOCOL_VERSION,
                    success: true,
                }));
            }
        }
        Ok(false) => {
            // The row can only disappear through expiry while this initial attempt is in flight.
            for pending in initial.replies {
                let _ = pending.reply.send(Err(Status::failed_precondition(
                    "queued request lease expired or wait cancelled",
                )));
            }
            for reply in initial.cancel_replies {
                let _ = reply.send(Ok(proto::BoolResponse {
                    protocol_version: PROTOCOL_VERSION,
                    success: false,
                }));
            }
        }
        Err(error) => {
            for pending in initial.replies {
                let _ = pending.reply.send(Err(error.clone()));
            }
            for reply in initial.cancel_replies {
                let _ = reply.send(Err(error.clone()));
            }
        }
    }
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

fn close_waits(waits: &mut Waits) {
    waits.closed = true;
    for claim in waits.claims.drain(..) {
        let _ = claim
            .reply
            .send(Err(Status::unavailable("coordinator stopping")));
    }
    for wait in waits.permits.values_mut() {
        for pending in wait.replies.drain(..) {
            let _ = pending
                .reply
                .send(Err(Status::unavailable("coordinator stopping")));
        }
    }
    waits.permits.clear();
    for initial in waits
        .initializing_permits
        .drain()
        .map(|(_, initial)| initial)
    {
        for pending in initial.replies {
            let _ = pending
                .reply
                .send(Err(Status::unavailable("coordinator stopping")));
        }
        for reply in initial.cancel_replies {
            let _ = reply.send(Err(Status::unavailable("coordinator stopping")));
        }
    }
}

struct Event {
    name: &'static str,
    level: &'static str,
    data: serde_json::Value,
}

fn emit_events(events: &Arc<Mutex<Events>>, pending: Vec<Event>) {
    for event in pending {
        emit_shared(events, event.name, event.level, event.data);
    }
}

fn prune_permit_waits(
    context: &CoordinatorContext,
    frontier: &mut FrontierStore,
    waits: &mut Waits,
) -> Result<(), Status> {
    let ids: Vec<_> = waits.permits.keys().cloned().collect();
    let initially_expired = if ids.is_empty() {
        Vec::new()
    } else {
        frontier
            .expired_request_waits(&context.run_id, &ids)
            .map_err(to_status)?
    };
    for id in &initially_expired {
        let Some(wait) = waits.permits.get(id) else {
            continue;
        };
        if wait.replies.iter().all(|reply| reply.reply.is_closed()) {
            continue;
        }
        let lease = wait.request.lease.as_ref().expect("validated lease");
        let task = super::claimed_task(lease)?;
        frontier
            .refresh_pending_request_wait_for_task(&task, id)
            .map_err(to_status)?;
    }
    let expired = if initially_expired.is_empty() {
        initially_expired
    } else {
        frontier
            .expired_request_waits(&context.run_id, &ids)
            .map_err(to_status)?
    };
    let cancelled: Vec<_> = waits
        .permits
        .iter_mut()
        .filter_map(|(id, wait)| {
            wait.replies.retain(|reply| !reply.reply.is_closed());
            (wait.replies.is_empty() || expired.contains(id)).then(|| id.clone())
        })
        .collect();
    for id in cancelled {
        let wait = waits.permits.remove(&id).expect("pending wait");
        let worker = &wait
            .request
            .lease
            .as_ref()
            .expect("validated lease")
            .lease_owner;
        frontier
            .cancel_request_wait(&context.run_id, worker, &id)
            .map_err(to_status)?;
        for pending in wait.replies {
            let _ = pending.reply.send(Err(Status::failed_precondition(
                "queued request lease expired or wait cancelled",
            )));
        }
    }
    Ok(())
}

fn stop_claims(
    context: &CoordinatorContext,
    frontier: &mut FrontierStore,
    waits: &mut Waits,
    events: &mut Vec<Event>,
    claim_completions: &mut Vec<ClaimCompletion>,
) -> Result<(), Status> {
    let abandoned = frontier
        .finalize_abandoned_sitemap_discovery(&context.run_id)
        .map_err(to_status)?;
    if abandoned > 0 {
        events.push(Event {
            name: "sitemap.discovery_abandoned",
            level: "failure",
            data: serde_json::json!({"count":abandoned,"reason":"frontier/run stopped before discovery committed"}),
        });
    }
    for claim in waits.claims.drain(..) {
        claim_completions.push(ClaimCompletion {
            reply: claim.reply,
            result: Ok(proto::ClaimOneResponse {
                protocol_version: PROTOCOL_VERSION,
                lease: None,
                target_reached: true,
                retry_after_ms: 0,
            }),
            _credit: claim._credit,
        });
    }
    Ok(())
}

fn dispatch(
    context: &CoordinatorContext,
    snapshot: &CoordinatorSnapshot,
    frontier: &mut FrontierStore,
    waits: &mut Waits,
    can_assign: bool,
    result_jobs: usize,
    max_outstanding: usize,
    events: &mut Vec<Event>,
    claim_completions: &mut Vec<ClaimCompletion>,
) -> Result<(bool, Option<Duration>), Status> {
    waits.claims.retain(|claim| !claim.reply.is_closed());
    let run_id = context.run_id.clone();
    let reference = if waits.claims.is_empty() && waits.permits.is_empty() {
        0
    } else {
        frontier.dispatch_clock_ms().map_err(to_status)?
    };
    prune_permit_waits(context, frontier, waits)?;
    if waits.closed {
        return Ok((true, None));
    }
    let mut excluded = Vec::new();
    if !waits.claims.is_empty() {
        excluded = context.quota_satisfied_source_ids(frontier)?;
        let stopped = context.target_reached(frontier)?
            || frontier
                .unfinished_task_count_excluding(&run_id, &excluded)
                .map_err(to_status)?
                == 0;
        if stopped {
            stop_claims(context, frontier, waits, events, claim_completions)?;
        } else {
            let live_leases = frontier
                .live_leased_task_count_excluding(&run_id, &excluded)
                .map_err(to_status)?;
            let mut assignment_budget =
                max_outstanding.saturating_sub(live_leases.saturating_add(result_jobs));
            if can_assign {
                while assignment_budget > 0 {
                    let Some(claim) = waits.claims.pop_front() else {
                        break;
                    };
                    if claim.reply.is_closed() {
                        continue;
                    }
                    let task = frontier
                        .claim_one_excluding(
                            &run_id,
                            &claim.worker_id,
                            context.lease_timing,
                            &excluded,
                        )
                        .map_err(to_status)?;
                    let Some(task) = task else {
                        waits.claims.push_front(claim);
                        break;
                    };
                    events.push(Event {
                        name: "task.claimed",
                        level: "success",
                        data: serde_json::json!({"worker_id":claim.worker_id,"task_id":task.task_id,"source_id":task.source_id,"url":safe_event_url(&task.normalized_url),"lease_epoch":task.lease_epoch,"task_attempt":task.task_attempt_count}),
                    });
                    // A disconnected claimant cannot release its durable lease; expiry fences it.
                    claim_completions.push(ClaimCompletion {
                        reply: claim.reply,
                        result: Ok(proto::ClaimOneResponse {
                            protocol_version: PROTOCOL_VERSION,
                            lease: Some(task_to_proto(&task)),
                            target_reached: false,
                            retry_after_ms: 0,
                        }),
                        _credit: claim._credit,
                    });
                    assignment_budget -= 1;
                }
            }
        }
    }
    if !waits.claims.is_empty()
        && frontier
            .unfinished_task_count_excluding(&run_id, &excluded)
            .map_err(to_status)?
            == 0
    {
        stop_claims(context, frontier, waits, events, claim_completions)?;
    }
    let mut attempted_grant = false;
    let mut retry_delay = None;
    while !waits.permits.is_empty() {
        let Some(id) = frontier.ready_request_id(&run_id).map_err(to_status)? else {
            break;
        };
        // Initial attempts reserve an ID in memory before the transaction commits. Keep the
        // durable round-robin head in place until its completion command installs the waiter.
        let wait = match take_ready_permit(waits, &id) {
            ReadyPermit::Initializing => break,
            ReadyPermit::Pending(wait) => wait,
            ReadyPermit::Missing => {
                return Err(Status::internal(
                    "durable queued request has no connected waiter",
                ));
            }
        };
        let lease = wait
            .request
            .lease
            .as_ref()
            .expect("validated lease")
            .clone();
        let task_result = (|| {
            let task = context.validate_lease(&lease, snapshot, frontier)?;
            let (origin, url) =
                context.validate_request_url(&wait.request.url, &wait.request.request_kind)?;
            let key = schedule_key(&context.config, &url);
            attempted_grant = true;
            let decision = frontier
                .grant_pending_request_permit_for_task(
                    &task,
                    &task.lease_owner,
                    &id,
                    &key,
                    &origin,
                    &wait.request.request_kind,
                )
                .map_err(to_status)?;
            Ok((task, decision))
        })();
        let result = match task_result {
            Ok((_task, Some(PermitDecision::Waiting(delay)))) => {
                waits.permits.insert(id, wait);
                retry_delay = Some(delay);
                break;
            }
            Ok((_task, None)) => {
                // Keep the in-memory waiter until the cancellation/expiry owner removes it.
                waits.permits.insert(id, wait);
                retry_delay = Some(Duration::from_millis(25));
                break;
            }
            Ok((_task, Some(decision))) => permit_response(decision),
            Err(error) => {
                frontier
                    .cancel_request_wait(
                        &run_id,
                        &wait
                            .request
                            .lease
                            .as_ref()
                            .expect("validated lease")
                            .lease_owner,
                        &id,
                    )
                    .map_err(to_status)?;
                Err(error)
            }
        };
        if result
            .as_ref()
            .is_err_and(|error| error.code() == tonic::Code::Internal)
        {
            for pending in wait.replies {
                let _ = pending.reply.send(result.clone());
            }
            return result.map(|_| (false, None));
        }
        for pending in wait.replies {
            let _ = pending.reply.send(result.clone());
        }
    }
    if attempted_grant && !waits.permits.is_empty() {
        prune_permit_waits(context, frontier, waits)?;
    }
    let scheduler_delay = if waits.claims.is_empty() && waits.permits.is_empty() {
        None
    } else {
        frontier
            .next_dispatch_delay(
                &run_id,
                !waits.claims.is_empty(),
                !waits.permits.is_empty(),
                &excluded,
                context.config.max_duration_seconds,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_ready_head_stays_queued_until_initial_attempt_finishes() {
        let request_id = "initializing-request".to_owned();
        let mut waits = Waits::default();
        waits.initializing_permits.insert(
            request_id.clone(),
            InitialPermit {
                request: proto::AcquireRequestPermitRequest::default(),
                replies: Vec::new(),
                cancel_replies: Vec::new(),
            },
        );

        assert!(matches!(
            take_ready_permit(&mut waits, &request_id),
            ReadyPermit::Initializing
        ));
        assert!(waits.initializing_permits.contains_key(&request_id));
    }
}
