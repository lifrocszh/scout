//! Bounded result intake. Each lane owns one PostgreSQL connection; projection follows commit.
use super::super::frontier::{
    AcceptedCapture, ClaimedTask, CompletionAcceptance, DiscoveredTask, FrontierStore,
};
use super::capture::StoredCapture;
use super::proto;
use super::{MAX_RPC_MESSAGE_BYTES, PROTOCOL_VERSION, to_status};
use crate::corpus::capture_selection_key;
use crate::crawl::Config;
use crate::crawl::spool::CaptureSpool;
use crate::error::Error;
use crate::observability::Events;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

const DEFAULT_LANES: usize = 4;
const MAX_LANES: usize = 8;
const QUEUE_PER_LANE: usize = 7;

pub(super) struct ResultPool {
    senders: Vec<mpsc::Sender<Job>>,
    next: AtomicUsize,
    max_bytes: usize,
    bytes: Arc<AtomicUsize>,
    jobs: Arc<AtomicUsize>,
    notify: Arc<tokio::sync::Notify>,
}

pub(super) struct Job {
    task: ClaimedTask,
    result_sha256: String,
    capture: Option<StoredCapture>,
    accepted_capture: Option<AcceptedCapture>,
    rejection_reason: String,
    discovered: Vec<DiscoveredTask>,
    initially_rejected: usize,
    max_task_attempts: i32,
    max_frontier: usize,
    reply: Option<oneshot::Sender<Result<proto::CompleteTaskResponse, Status>>>,
    _accounting: Option<Accounting>,
}

struct Accounting {
    bytes: Arc<AtomicUsize>,
    jobs: Arc<AtomicUsize>,
    size: usize,
    lanes: usize,
    notify: Arc<tokio::sync::Notify>,
}

pub(super) struct Reservation {
    permit: Option<mpsc::OwnedPermit<Job>>,
    accounting: Option<Accounting>,
}

impl Drop for Accounting {
    fn drop(&mut self) {
        let bytes = self.bytes.fetch_sub(self.size, Ordering::Relaxed) - self.size;
        let jobs = self.jobs.fetch_sub(1, Ordering::Relaxed) - 1;
        crate::distributed::metrics::completion_queue(jobs, bytes, self.lanes);
        self.notify.notify_one();
    }
}

impl ResultPool {
    pub(super) fn lane_count() -> Result<usize, Error> {
        let lanes = std::env::var("SCOUT_COMPLETION_LANES")
            .ok()
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| Error::invalid("SCOUT_COMPLETION_LANES must be an integer"))?
            .unwrap_or(DEFAULT_LANES);
        if !(2..=MAX_LANES).contains(&lanes) {
            return Err(Error::invalid(format!(
                "SCOUT_COMPLETION_LANES must be between 2 and {MAX_LANES}"
            )));
        }
        Ok(lanes)
    }

    pub(super) fn new(
        database_url: &str,
        lanes: usize,
        max_body_bytes: usize,
    ) -> Result<(Self, Vec<(mpsc::Receiver<Job>, FrontierStore)>), Error> {
        let mut senders = Vec::with_capacity(lanes);
        let mut workers = Vec::with_capacity(lanes);
        for _ in 0..lanes {
            let (sender, receiver) = mpsc::channel(QUEUE_PER_LANE);
            senders.push(sender);
            workers.push((receiver, FrontierStore::connect(database_url)?));
        }
        Ok((
            Self {
                senders,
                next: AtomicUsize::new(0),
                max_bytes: max_body_bytes
                    .saturating_mul((lanes * 2).min(8))
                    .max(MAX_RPC_MESSAGE_BYTES),
                bytes: Arc::new(AtomicUsize::new(0)),
                jobs: Arc::new(AtomicUsize::new(0)),
                notify: Arc::new(tokio::sync::Notify::new()),
            },
            workers,
        ))
    }

    pub(super) fn notify(&self) -> Arc<tokio::sync::Notify> {
        self.notify.clone()
    }

    pub(super) fn ready_for_assignments(&self) -> bool {
        self.jobs.load(Ordering::Relaxed) < self.senders.len() * (QUEUE_PER_LANE + 1)
            && self.bytes.load(Ordering::Relaxed) < self.max_bytes * 3 / 4
            && self.senders.iter().any(|sender| sender.capacity() > 0)
    }

    pub(super) fn queued_jobs(&self) -> usize {
        self.jobs.load(Ordering::Relaxed)
    }

    pub(super) fn max_outstanding_tasks(&self) -> usize {
        self.senders.len() * (QUEUE_PER_LANE + 1)
    }

    pub(super) fn reserve(&self, size: usize) -> Result<Reservation, Status> {
        let mut permit = None;
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        for offset in 0..self.senders.len() {
            if let Ok(reserved) = self.senders[(start + offset) % self.senders.len()]
                .clone()
                .try_reserve_owned()
            {
                permit = Some(reserved);
                break;
            }
        }
        let permit =
            permit.ok_or_else(|| Status::resource_exhausted("coordinator result queue is full"))?;
        let mut current = self.bytes.load(Ordering::Relaxed);
        loop {
            if current.saturating_add(size) > self.max_bytes {
                return Err(Status::resource_exhausted(
                    "coordinator result byte capacity reached",
                ));
            }
            match self.bytes.compare_exchange_weak(
                current,
                current + size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        self.jobs.fetch_add(1, Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let jobs = self.jobs.load(Ordering::Relaxed);
        crate::distributed::metrics::completion_queue(jobs, bytes, self.senders.len());
        Ok(Reservation {
            permit: Some(permit),
            accounting: Some(Accounting {
                bytes: self.bytes.clone(),
                jobs: self.jobs.clone(),
                size,
                lanes: self.senders.len(),
                notify: self.notify.clone(),
            }),
        })
    }

    pub(super) fn submit(&self, mut job: Job, mut reservation: Reservation) {
        job._accounting = reservation.accounting.take();
        reservation
            .permit
            .take()
            .expect("reserved queue slot")
            .send(job);
    }

    pub(super) fn start(
        self: &Arc<Self>,
        workers: Vec<(mpsc::Receiver<Job>, FrontierStore)>,
        state: Weak<super::CoordinatorState>,
        config: Config,
        run_id: String,
        spool: CaptureSpool,
        events: Arc<std::sync::Mutex<Events>>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        workers
            .into_iter()
            .map(|(mut receiver, mut frontier)| {
                let config = config.clone();
                let run_id = run_id.clone();
                let spool = spool.clone();
                let events = events.clone();
                let state = state.clone();
                tokio::task::spawn_blocking(move || {
                    while let Some(job) = receiver.blocking_recv() {
                        let mut job = job;
                        let reply = job.reply.take().expect("completion reply");
                        let result = process(&mut frontier, &config, &run_id, &spool, &events, job);
                        if let Err(message) = &result {
                            if message.code() == tonic::Code::Internal {
                                if let Some(state) = state.upgrade() {
                                    super::rpc::fail_coordinator(&state, message.message());
                                }
                            }
                        }
                        let _ = reply.send(result);
                        // A disconnected caller does not undo a committed result.
                    }
                })
            })
            .collect()
    }
}

impl Job {
    pub(super) fn new(
        task: ClaimedTask,
        result_sha256: String,
        capture: Option<StoredCapture>,
        accepted_capture: Option<AcceptedCapture>,
        rejection_reason: String,
        discovered: Vec<DiscoveredTask>,
        initially_rejected: usize,
        max_task_attempts: i32,
        max_frontier: usize,
        reply: oneshot::Sender<Result<proto::CompleteTaskResponse, Status>>,
    ) -> Self {
        Self {
            task,
            result_sha256,
            capture,
            accepted_capture,
            rejection_reason,
            discovered,
            initially_rejected,
            max_task_attempts,
            max_frontier,
            reply: Some(reply),
            // Replaced with byte accounting only after queue admission.
            _accounting: None,
        }
    }
}

fn process(
    frontier: &mut FrontierStore,
    config: &Config,
    run_id: &str,
    spool: &CaptureSpool,
    events: &Arc<std::sync::Mutex<Events>>,
    job: Job,
) -> Result<proto::CompleteTaskResponse, Status> {
    let Job {
        task,
        result_sha256,
        capture,
        accepted_capture,
        rejection_reason,
        discovered,
        initially_rejected,
        max_task_attempts,
        max_frontier,
        reply: _,
        _accounting: _,
    } = job;
    let started = Instant::now();
    let acceptance = frontier
        .accept_task_result(
            &task,
            &result_sha256,
            accepted_capture.as_ref(),
            &rejection_reason,
            &discovered,
            initially_rejected,
            max_task_attempts,
            max_frontier,
        )
        .map_err(to_status)?;
    crate::distributed::metrics::completion_phase("postgres_transaction", started);
    let (completed, accepted_count, rejected_count) = match acceptance {
        CompletionAcceptance::Accepted {
            accepted_discovered_count,
            rejected_discovered_count,
        }
        | CompletionAcceptance::Duplicate {
            accepted_discovered_count,
            rejected_discovered_count,
        } => {
            if let Some(capture) = capture.as_ref() {
                let started = Instant::now();
                let crawl_capture = capture.to_crawl_capture().map_err(to_status)?;
                spool
                    .insert_capture(&crawl_capture, &capture_selection_key(&crawl_capture))
                    .map_err(|error| Status::internal(error.message))?;
                crate::distributed::metrics::completion_phase("sqlite_projection", started);
            }
            (
                true,
                accepted_discovered_count as u64,
                rejected_discovered_count as u64,
            )
        }
        CompletionAcceptance::Stale => (false, 0, 0),
    };
    let started = Instant::now();
    let counts = frontier
        .content_group_counts_by_source(run_id)
        .map_err(to_status)?;
    let total: usize = counts.values().sum();
    let quotas_met = config.sources.iter().all(|source| {
        source.minimum_page_quota == 0
            || counts.get(&source.source_id).copied().unwrap_or_default()
                >= source.minimum_page_quota
    });
    let target_reached = (total >= config.page_target && quotas_met)
        || frontier
            .run_duration_exceeded(
                run_id,
                std::time::Duration::from_secs(config.max_duration_seconds),
            )
            .map_err(to_status)?;
    crate::distributed::metrics::completion_phase("quota_bookkeeping", started);
    if completed && matches!(acceptance, CompletionAcceptance::Accepted { .. }) {
        crate::observability::emit_shared(
            events,
            if capture.is_some() {
                "capture.accepted"
            } else {
                "page.rejected"
            },
            "success",
            serde_json::json!({
                "task_id": task.task_id,
                "source_id": task.source_id,
                "url": super::safe_event_url(&task.normalized_url),
                "capture_received": capture.is_some(),
                "body_bytes": capture.as_ref().map(|capture| capture.content_bytes),
                "body_sha256": capture.as_ref().map(|capture| capture.body_hash.as_str()),
                "rejection_reason": rejection_reason,
                "accepted_discovered_count": accepted_count,
                "rejected_discovered_count": rejected_count,
                "capture_persistence": "postgresql_transaction",
                "target_reached": target_reached,
            }),
        );
    }
    let response = proto::CompleteTaskResponse {
        protocol_version: PROTOCOL_VERSION,
        completed,
        accepted_discovered_count: accepted_count,
        rejected_discovered_count: rejected_count,
        capture_received: capture.is_some(),
        target_reached,
    };
    Ok(response)
}
