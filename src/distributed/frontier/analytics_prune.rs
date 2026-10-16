use super::{FrontierStore, analytics};
use crate::error::Error;
use postgres::GenericClient;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

const MAX_PRUNE_EVENTS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub(crate) enum PruneTarget {
    Run(String),
    Batch(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct PruneReport {
    pub(crate) target: PruneTarget,
    pub(crate) run_id: String,
    pub(crate) execute: bool,
    pub(crate) status: &'static str,
    pub(crate) event_count: usize,
    pub(crate) batch_copy_count: usize,
    pub(crate) preserved_artifact_count: usize,
    pub(crate) blockers: Vec<String>,
    pub(crate) note: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EventPlan {
    event_id: String,
    event_type: String,
    payload_sha256: String,
    payload_bytes: i64,
    delete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BatchPlan {
    batch_id: String,
    consumer_id: String,
    run_id: String,
    manifest_sha256: String,
    records_sha256: String,
    coverage_sha256: String,
    record_count: i32,
    records_bytes: i64,
    quarantine_count: i32,
    event_ids: Vec<String>,
    event_coverage: Vec<BatchEventPlan>,
    locations: Vec<PathBuf>,
    archive_path: Option<String>,
    archive_valid: bool,
    pending_copy_intent: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BatchEventPlan {
    event_id: String,
    event_type: String,
    schema_version: i32,
    payload_sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ArtifactPlan {
    event_id: String,
    uri: String,
    kind: String,
    sha256: String,
    bytes: i64,
    archive_path: Option<String>,
    archive_valid: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BatchFilePlan {
    batch_id: String,
    path: PathBuf,
    manifest_sha256: String,
    records_bytes: i64,
    complete: bool,
    intent_preexisting: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Plan {
    report: PruneReport,
    data_root: PathBuf,
    archive_root: Option<PathBuf>,
    events: Vec<EventPlan>,
    batches: Vec<BatchPlan>,
    artifacts: Vec<ArtifactPlan>,
    batch_files: Vec<BatchFilePlan>,
    recovered_quarantine: BTreeMap<String, i32>,
    expected_event_count: i64,
    expected_event_coverage: String,
}

impl FrontierStore {
    /// Inspect or prune a bounded set of retained analytics events.
    ///
    /// A run target selects at most `max_events` oldest unpruned events. A
    /// batch target selects only that immutable batch's event coverage.
    pub(crate) fn prune_analytics(
        &mut self,
        target: PruneTarget,
        data_dir: &Path,
        execute: bool,
        max_events: usize,
    ) -> Result<PruneReport, Error> {
        if !(1..=MAX_PRUNE_EVENTS).contains(&max_events) {
            return Err(Error::invalid(format!(
                "analytics prune max_events must be between 1 and {MAX_PRUNE_EVENTS}"
            )));
        }
        let data_root = data_dir.canonicalize().map_err(|error| {
            Error::storage(format!("resolve analytics prune data directory: {error}"))
        })?;
        if !data_root.is_dir() {
            return Err(Error::invalid(
                "analytics prune data directory must be a directory",
            ));
        }

        let mut plan = inspect_plan(&mut self.client, &target, &data_root, max_events, true)?;
        if !execute || !plan.report.blockers.is_empty() {
            plan.report.execute = execute;
            plan.report.status = if plan.report.blockers.is_empty() {
                "ready"
            } else {
                "blocked"
            };
            return Ok(plan.report);
        }

        self.prepare_intents(&plan)?;
        // Check archives and exact local copies again after intent persistence.
        verify_plan_files(&plan)?;
        let deleted_events = self.delete_event_rows(&plan, max_events)?;
        let deleted_batch_copies = self.delete_batch_copies(&plan)?;

        let mut report = plan.report;
        report.execute = true;
        report.status = "pruned";
        report.event_count = deleted_events;
        report.batch_copy_count = deleted_batch_copies;
        Ok(report)
    }

    fn prepare_intents(&mut self, plan: &Plan) -> Result<(), Error> {
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin analytics prune intent transaction: {error}"))
        })?;
        lock_prune_inputs(&mut tx)?;
        verify_locked_run(&mut tx, plan)?;

        for (batch_id, quarantine_count) in &plan.recovered_quarantine {
            tx.execute(
                "UPDATE scout_crawl_event_batches SET quarantine_count=$2
                 WHERE batch_id=$1 AND quarantine_count IS NULL",
                &[batch_id, quarantine_count],
            )
            .map_err(|error| {
                Error::storage(format!("recover analytics batch quarantine count: {error}"))
            })?;
            let saved: Option<i32> = tx
                .query_one(
                    "SELECT quarantine_count FROM scout_crawl_event_batches WHERE batch_id=$1",
                    &[batch_id],
                )
                .map_err(|error| {
                    Error::storage(format!(
                        "verify recovered analytics quarantine count: {error}"
                    ))
                })?
                .get(0);
            if saved != Some(*quarantine_count) {
                return Err(Error::safety(
                    "verified batch quarantine count conflicts with its registered identity",
                ));
            }
        }

        for event in plan.events.iter().filter(|event| event.delete) {
            tx.execute(
                "INSERT INTO scout_analytics_prune_items(
                     item_type,item_id,crawl_run_id,local_path,expected_sha256,expected_bytes
                 ) VALUES ('event',$1,$2,'',$3,$4) ON CONFLICT DO NOTHING",
                &[
                    &event.event_id,
                    &plan.report.run_id,
                    &event.payload_sha256,
                    &event.payload_bytes,
                ],
            )
            .map_err(|error| Error::storage(format!("persist event prune intent: {error}")))?;
            verify_intent(
                &mut tx,
                "event",
                &event.event_id,
                "",
                &plan.report.run_id,
                &event.payload_sha256,
                event.payload_bytes,
            )?;
        }

        for file in plan.batch_files.iter().filter(|file| !file.complete) {
            let local_path = file.path.to_string_lossy().into_owned();
            tx.execute(
                "INSERT INTO scout_analytics_prune_items(
                     item_type,item_id,crawl_run_id,local_path,expected_sha256,expected_bytes
                 ) VALUES ('batch_copy',$1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                &[
                    &file.batch_id,
                    &plan.report.run_id,
                    &local_path,
                    &file.manifest_sha256,
                    &file.records_bytes,
                ],
            )
            .map_err(|error| Error::storage(format!("persist batch-copy prune intent: {error}")))?;
            verify_intent(
                &mut tx,
                "batch_copy",
                &file.batch_id,
                &local_path,
                &plan.report.run_id,
                &file.manifest_sha256,
                file.records_bytes,
            )?;
        }
        tx.commit()
            .map_err(|error| Error::storage(format!("commit analytics prune intents: {error}")))
    }

    fn delete_event_rows(&mut self, plan: &Plan, max_events: usize) -> Result<usize, Error> {
        let mut tx = self.client.transaction().map_err(|error| {
            Error::storage(format!("begin analytics event prune transaction: {error}"))
        })?;
        lock_prune_inputs(&mut tx)?;
        verify_locked_run(&mut tx, plan)?;
        let refreshed = inspect_plan(
            &mut tx,
            &plan.report.target,
            &plan.data_root,
            max_events,
            false,
        )?;
        if !refreshed.report.blockers.is_empty() || !same_plan_database(&refreshed, plan) {
            return Err(Error::safety(format!(
                "analytics prune preflight changed after intents were persisted: {}",
                refreshed.report.blockers.join("; ")
            )));
        }

        let mut deleted = 0usize;
        for event in plan.events.iter().filter(|event| event.delete) {
            let row = tx
                .query_opt(
                    "SELECT payload_json, payload_sha256
                     FROM scout_crawl_events WHERE event_id=$1 FOR UPDATE",
                    &[&event.event_id],
                )
                .map_err(|error| Error::storage(format!("lock event before prune: {error}")))?;
            let Some(row) = row else {
                return Err(Error::safety(format!(
                    "event {} disappeared before its prune intent completed",
                    event.event_id
                )));
            };
            let payload: String = row.get(0);
            let digest: Option<String> = row.get(1);
            if digest.as_deref() != Some(event.payload_sha256.as_str())
                || payload.len() as i64 != event.payload_bytes
                || crate::storage::sha256_hex(payload.as_bytes()) != event.payload_sha256
            {
                return Err(Error::safety(format!(
                    "event {} changed after prune preflight",
                    event.event_id
                )));
            }
            let affected = tx
                .execute(
                    "DELETE FROM scout_crawl_events WHERE event_id=$1",
                    &[&event.event_id],
                )
                .map_err(|error| Error::storage(format!("delete retained Crawl event: {error}")))?;
            if affected != 1 {
                return Err(Error::safety(format!(
                    "event {} was not deleted exactly once",
                    event.event_id
                )));
            }
            let completed = tx
                .execute(
                    "UPDATE scout_analytics_prune_items SET completed_at=clock_timestamp()
                     WHERE item_type='event' AND item_id=$1 AND local_path=''
                       AND crawl_run_id=$2 AND completed_at IS NULL",
                    &[&event.event_id, &plan.report.run_id],
                )
                .map_err(|error| Error::storage(format!("complete event prune intent: {error}")))?;
            if completed != 1 {
                return Err(Error::safety(format!(
                    "event {} prune intent was not completed exactly once",
                    event.event_id
                )));
            }
            deleted += 1;
        }
        tx.commit()
            .map_err(|error| Error::storage(format!("commit analytics event pruning: {error}")))?;
        Ok(deleted)
    }

    fn delete_batch_copies(&mut self, plan: &Plan) -> Result<usize, Error> {
        let mut deleted = 0usize;
        for file in plan.batch_files.iter().filter(|file| !file.complete) {
            let local_path = file.path.to_string_lossy().into_owned();
            match fs::symlink_metadata(&file.path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(Error::storage(format!(
                        "inspect batch copy during prune resume: {error}"
                    )));
                }
                Ok(_) => {
                    remove_verified_batch_dir(plan, file)?;
                    deleted += 1;
                }
            }
            let completed = self
                .client
                .execute(
                    "UPDATE scout_analytics_prune_items SET completed_at=clock_timestamp()
                     WHERE item_type='batch_copy' AND item_id=$1 AND local_path=$2
                       AND crawl_run_id=$3 AND completed_at IS NULL",
                    &[&file.batch_id, &local_path, &plan.report.run_id],
                )
                .map_err(|error| {
                    Error::storage(format!("complete batch-copy prune intent: {error}"))
                })?;
            if completed != 1 {
                return Err(Error::safety(format!(
                    "batch copy {} prune intent was not completed exactly once",
                    file.batch_id
                )));
            }
        }
        Ok(deleted)
    }
}

fn inspect_plan<C: GenericClient>(
    client: &mut C,
    target: &PruneTarget,
    data_root: &Path,
    max_events: usize,
    verify_files: bool,
) -> Result<Plan, Error> {
    let mut blockers = Vec::<String>::new();
    let (run_id, target_batch) = match target {
        PruneTarget::Run(run_id) => (run_id.clone(), None),
        PruneTarget::Batch(batch_id) => {
            let row = client
                .query_opt(
                    "SELECT crawl_run_id FROM scout_crawl_event_batches WHERE batch_id=$1",
                    &[batch_id],
                )
                .map_err(|error| Error::storage(format!("resolve prune batch target: {error}")))?;
            let Some(row) = row else {
                return Err(Error::invalid(format!(
                    "unknown analytics batch {batch_id:?}"
                )));
            };
            (row.get(0), Some(batch_id.clone()))
        }
    };
    let run_suffix = run_id.strip_prefix("run-").unwrap_or_default();
    if run_suffix.len() != 64
        || !run_suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        blockers.push(format!("invalid Crawl run identity {run_id:?}"));
    }
    let run = client
        .query_opt(
            "SELECT status FROM scout_crawl_runs WHERE crawl_run_id=$1",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("read prune run status: {error}")))?;
    match run {
        Some(row) if row.get::<_, String>(0) == "completed" => {}
        Some(row) => blockers.push(format!(
            "Crawl run {run_id} is {}, so its analytics cannot be pruned",
            row.get::<_, String>(0)
        )),
        None => blockers.push(format!("Crawl run {run_id} has no live completion record")),
    }

    let ledger = client
        .query_opt(
            "SELECT run.expected_event_count, run.expected_event_coverage::TEXT,
                    (SELECT COUNT(*) FROM scout_crawl_event_history h WHERE h.crawl_run_id=run.crawl_run_id),
                    COALESCE((
                        SELECT bit_xor(scout_event_history_fingerprint(
                            h.event_id,h.event_type,h.schema_version,h.dedupe_key,h.payload_sha256,h.created_at
                        ))::TEXT FROM scout_crawl_event_history h WHERE h.crawl_run_id=run.crawl_run_id
                    ), repeat('0',256))
             FROM scout_crawl_run_history run WHERE run.crawl_run_id=$1",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("verify prune event-history ledger: {error}")))?;
    let (expected_event_count, expected_event_coverage) = match ledger {
        Some(row) => {
            let expected_count: i64 = row.get(0);
            let expected_coverage: String = row.get(1);
            let retained_count: i64 = row.get(2);
            let retained_coverage: String = row.get(3);
            if expected_count != retained_count || expected_coverage != retained_coverage {
                blockers.push(format!(
                    "retained event-history gap for {run_id}: expected {expected_count} identities, found {retained_count}"
                ));
            }
            (expected_count, expected_coverage)
        }
        None => {
            blockers.push(format!(
                "Crawl run {run_id} has no retained run-history row"
            ));
            (0, String::new())
        }
    };

    let history_rows = client
        .query(
            "SELECT h.event_id,h.event_type,h.schema_version,h.dedupe_key,h.payload_sha256,
                    e.event_id IS NOT NULL,e.crawl_run_id,e.event_type,e.schema_version,e.dedupe_key,
                    e.payload_sha256,e.payload_json,
                    EXISTS(SELECT 1 FROM scout_analytics_prune_items p
                           WHERE p.item_type='event' AND p.item_id=h.event_id AND p.local_path=''
                             AND p.completed_at IS NOT NULL),
                    p.expected_sha256,p.expected_bytes,p.completed_at IS NOT NULL
             FROM scout_crawl_event_history h
             LEFT JOIN scout_crawl_events e ON e.event_id=h.event_id
             LEFT JOIN scout_analytics_prune_items p
               ON p.item_type='event' AND p.item_id=h.event_id AND p.local_path=''
             WHERE h.crawl_run_id=$1 ORDER BY h.created_at,h.event_id",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("compare live events with prune history: {error}")))?;

    let mut history_ids = BTreeSet::new();
    let mut completed_ids = BTreeSet::new();
    let mut pending_history = Vec::<(String, String, Option<String>, i64, bool)>::new();
    for row in history_rows {
        let event_id: String = row.get(0);
        let event_type: String = row.get(1);
        let schema_version: i32 = row.get(2);
        let dedupe_key: String = row.get(3);
        let history_sha: Option<String> = row.get(4);
        let live: bool = row.get(5);
        let complete: bool = row.get(12);
        let intent_sha: Option<String> = row.get(13);
        let intent_bytes: Option<i64> = row.get(14);
        let intent_complete: bool = row.get(15);
        history_ids.insert(event_id.clone());
        if complete != intent_complete {
            blockers.push(format!(
                "event {event_id} has conflicting prune intent rows"
            ));
        }
        if live {
            let live_run: String = row.get(6);
            let live_type: String = row.get(7);
            let live_version: i32 = row.get(8);
            let live_dedupe: String = row.get(9);
            let live_sha: Option<String> = row.get(10);
            let payload: String = row.get(11);
            let payload_bytes = payload.len() as i64;
            let actual_sha = crate::storage::sha256_hex(payload.as_bytes());
            if complete {
                blockers.push(format!(
                    "completed prune intent for event {event_id} has a live payload again"
                ));
            }
            if live_run != run_id
                || live_type != event_type
                || live_version != schema_version
                || live_dedupe != dedupe_key
                || live_sha != history_sha
                || history_sha.as_deref().is_some_and(|sha| sha != actual_sha)
            {
                blockers.push(format!(
                    "live event {event_id} conflicts with retained event history"
                ));
            }
            if intent_sha.is_some() && intent_sha != live_sha
                || intent_bytes.is_some_and(|bytes| bytes != payload_bytes)
            {
                blockers.push(format!(
                    "event {event_id} differs from its persisted prune intent"
                ));
            }
            pending_history.push((event_id, event_type, live_sha, payload_bytes, false));
        } else if complete {
            completed_ids.insert(event_id.clone());
        } else {
            blockers.push(format!(
                "retained-history gap for event {event_id}: payload is missing without a completed prune intent"
            ));
            pending_history.push((event_id, event_type, history_sha, 0, false));
        }
    }

    let mut candidate_ids = if let Some(batch_id) = &target_batch {
        let rows = client
            .query(
                "SELECT event_id FROM scout_crawl_event_batch_events WHERE batch_id=$1 ORDER BY event_id",
                &[batch_id],
            )
            .map_err(|error| Error::storage(format!("read target batch coverage: {error}")))?;
        let candidate_ids = rows.iter().map(|row| row.get(0)).collect::<Vec<String>>();
        if candidate_ids.is_empty() {
            blockers.push(format!(
                "target batch {batch_id} has no registered event coverage"
            ));
        }
        if candidate_ids.len() > max_events {
            blockers.push(format!(
                "target batch {batch_id} covers {} events, above max_events {max_events}",
                candidate_ids.len()
            ));
        }
        for event_id in &candidate_ids {
            if !history_ids.contains(event_id) {
                blockers.push(format!(
                    "batch {batch_id} references event {event_id} missing from retained history"
                ));
            }
        }
        candidate_ids
    } else {
        pending_history
            .iter()
            .map(|event| event.0.clone())
            .take(max_events)
            .collect::<Vec<_>>()
    };
    candidate_ids.sort();
    candidate_ids.dedup();
    let candidate_set = candidate_ids.iter().cloned().collect::<HashSet<_>>();
    let event_by_id = pending_history
        .into_iter()
        .map(|(id, kind, sha, bytes, _)| (id, (kind, sha, bytes)))
        .collect::<HashMap<_, _>>();
    let mut events = Vec::new();
    for event_id in &candidate_ids {
        if completed_ids.contains(event_id) {
            continue;
        }
        match event_by_id.get(event_id) {
            Some((event_type, Some(sha), bytes)) if *bytes > 0 => events.push(EventPlan {
                event_id: event_id.clone(),
                event_type: event_type.clone(),
                payload_sha256: sha.clone(),
                payload_bytes: *bytes,
                delete: true,
            }),
            Some((_event_type, None, _)) => blockers.push(format!(
                "event {event_id} has no payload checksum; preserve it until integrity evidence exists"
            )),
            Some((_event_type, Some(_), _)) => blockers.push(format!(
                "event {event_id} is missing or has an empty live payload"
            )),
            None => blockers.push(format!("event {event_id} is not present in retained history")),
        }
    }

    let archive_root = checked_archive_root(data_root, verify_files).ok();
    if archive_root.is_none() {
        blockers.push("analytics-archive directory is missing or unsafe under --data-dir".into());
    }

    let mut recovered_quarantine = BTreeMap::new();
    let batches = load_batches(
        client,
        &run_id,
        &candidate_set,
        target_batch.as_deref(),
        &candidate_set,
        data_root,
        archive_root.as_deref(),
        verify_files,
        &mut recovered_quarantine,
        &mut blockers,
    )?;

    for event in &events {
        check_consumer_receipts(client, &run_id, event, &batches, &mut blockers)?;
        if !batches.iter().any(|batch| {
            batch.quarantine_count == 0
                && batch.event_ids.contains(&event.event_id)
                && batch.archive_valid
        }) {
            blockers.push(format!(
                "event {} has no independently verified archived, non-quarantined batch",
                event.event_id
            ));
        }
    }

    let artifacts = load_artifacts(
        client,
        &events,
        archive_root.as_deref(),
        verify_files,
        &mut blockers,
    )?;
    validate_event_artifact_payloads(client, &events, &artifacts, &mut blockers)?;
    for artifact in &artifacts {
        if !artifact.archive_valid {
            blockers.push(format!(
                "artifact {} for event {} has no checksum-verified analytics archive copy",
                artifact.uri, artifact.event_id
            ));
        }
    }

    let delete_batch_ids = batches
        .iter()
        .filter(|batch| {
            batch.quarantine_count == 0
                && batch.archive_valid
                && batch
                    .event_ids
                    .iter()
                    .all(|id| completed_ids.contains(id) || candidate_set.contains(id))
                && (target_batch.is_none()
                    || target_batch.as_deref() == Some(batch.batch_id.as_str())
                    || batch.pending_copy_intent)
        })
        .map(|batch| batch.batch_id.clone())
        .collect::<HashSet<_>>();
    if let Some(batch_id) = &target_batch {
        if !delete_batch_ids.contains(batch_id) {
            blockers.push(format!(
                "target batch {batch_id} is not cleanly archived and cannot be pruned"
            ));
        }
    }
    let batch_files = load_batch_files(
        client,
        &batches,
        &delete_batch_ids,
        &run_id,
        data_root,
        verify_files,
        target_batch.as_deref(),
        &mut blockers,
    )?;

    let preserved_artifact_count = artifacts.len();
    let report = PruneReport {
        target: target.clone(),
        run_id,
        execute: false,
        status: if blockers.is_empty() {
            "ready"
        } else {
            "blocked"
        },
        event_count: events.iter().filter(|event| event.delete).count(),
        batch_copy_count: batch_files.iter().filter(|file| !file.complete).count(),
        preserved_artifact_count,
        blockers,
        note: "body blobs, crawl manifests, snapshots, and Generation inputs are retained as authoritative capture and corpus inputs",
    };
    Ok(Plan {
        report,
        data_root: data_root.to_path_buf(),
        archive_root,
        events,
        batches,
        artifacts,
        batch_files,
        recovered_quarantine,
        expected_event_count,
        expected_event_coverage,
    })
}

fn load_batches<C: GenericClient>(
    client: &mut C,
    run_id: &str,
    candidate_ids: &HashSet<String>,
    target_batch: Option<&str>,
    selected_ids: &HashSet<String>,
    data_root: &Path,
    archive_root: Option<&Path>,
    verify_files: bool,
    recovered_quarantine: &mut BTreeMap<String, i32>,
    blockers: &mut Vec<String>,
) -> Result<Vec<BatchPlan>, Error> {
    let mut rows = client
        .query(
            "SELECT b.batch_id,b.consumer_id,b.crawl_run_id,b.manifest_sha256,b.records_sha256,
                    b.coverage_sha256,b.record_count,b.records_bytes,COALESCE(b.quarantine_count,-1),
                    COALESCE((SELECT array_agg(e.event_id ORDER BY e.event_id)
                              FROM scout_crawl_event_batch_events e WHERE e.batch_id=b.batch_id), ARRAY[]::TEXT[]),
                    EXISTS(SELECT 1 FROM scout_analytics_prune_items p
                           WHERE p.item_type='batch_copy' AND p.item_id=b.batch_id
                             AND p.completed_at IS NULL)
             FROM scout_crawl_event_batches b
             WHERE b.crawl_run_id=$1 AND (
                 $2::TEXT IS NOT NULL AND b.batch_id=$2
                 OR EXISTS (SELECT 1 FROM scout_crawl_event_batch_events e
                            WHERE e.batch_id=b.batch_id AND e.event_id=ANY($3::TEXT[]))
                 OR EXISTS (SELECT 1 FROM scout_analytics_prune_items p
                            WHERE p.item_type='batch_copy' AND p.item_id=b.batch_id
                              AND p.completed_at IS NULL)
             ) ORDER BY b.batch_id",
            &[&run_id, &target_batch, &candidate_ids.iter().cloned().collect::<Vec<_>>()],
        )
        .map_err(|error| Error::storage(format!("read registered event batch identities: {error}")))?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let batch_id: String = row.get(0);
        let consumer_id: String = row.get(1);
        let batch_run: String = row.get(2);
        let manifest_sha256: String = row.get(3);
        let records_sha256: String = row.get(4);
        let coverage_sha256: String = row.get(5);
        let record_count: i32 = row.get(6);
        let records_bytes: i64 = row.get(7);
        let mut quarantine_count: i32 = row.get(8);
        let event_ids: Vec<String> = row.get(9);
        let pending_copy_intent: bool = row.get(10);
        if batch_run != run_id || event_ids.len() != record_count as usize {
            blockers.push(format!(
                "batch {batch_id} has inconsistent run or event coverage"
            ));
        }
        let mapped = client
            .query(
                "SELECT e.event_id,e.event_type,e.payload_sha256,h.event_type,h.payload_sha256,h.crawl_run_id,h.schema_version
                 FROM scout_crawl_event_batch_events e
                 LEFT JOIN scout_crawl_event_history h ON h.event_id=e.event_id
                 WHERE e.batch_id=$1 ORDER BY e.event_id",
                &[&batch_id],
            )
            .map_err(|error| Error::storage(format!("verify batch event-history coverage: {error}")))?;
        let mut event_coverage = Vec::with_capacity(mapped.len());
        for row in mapped {
            let event_id: String = row.get(0);
            let mapped_type: String = row.get(1);
            let mapped_sha: Option<String> = row.get(2);
            let history_type: Option<String> = row.get(3);
            let history_sha: Option<String> = row.get(4);
            let history_run: Option<String> = row.get(5);
            let schema_version: Option<i32> = row.get(6);
            if history_type.as_deref() != Some(mapped_type.as_str())
                || history_sha != mapped_sha
                || history_run.as_deref() != Some(run_id)
                || schema_version.is_none()
            {
                blockers.push(format!(
                    "batch {batch_id} coverage conflicts with event history at {event_id}"
                ));
            }
            event_coverage.push(BatchEventPlan {
                event_id,
                event_type: mapped_type,
                schema_version: schema_version.unwrap_or_default(),
                payload_sha256: mapped_sha,
            });
        }
        let locations = client
            .query(
                "SELECT local_path FROM scout_crawl_event_batch_locations
                 WHERE batch_id=$1 ORDER BY local_path",
                &[&batch_id],
            )
            .map_err(|error| Error::storage(format!("read local batch-copy paths: {error}")))?
            .into_iter()
            .map(|row| PathBuf::from(row.get::<_, String>(0)))
            .collect::<Vec<_>>();

        let archive_row = client
            .query_opt(
                "SELECT archive_path,sha256,byte_size FROM scout_crawl_event_archive_items
                 WHERE item_type='batch' AND item_id=$1",
                &[&batch_id],
            )
            .map_err(|error| Error::storage(format!("read archived batch manifest: {error}")))?;
        let archive_path = archive_row.as_ref().map(|row| row.get::<_, String>(0));
        let batch_copy = analytics::BatchCopy {
            batch_id: batch_id.clone(),
            manifest_sha256: manifest_sha256.clone(),
            records_sha256: records_sha256.clone(),
            coverage_sha256: coverage_sha256.clone(),
            record_count,
            quarantine_count,
            records_bytes,
        };
        let mut archive_valid = false;
        if let (Some(root), Some(row)) = (archive_root, archive_row.as_ref()) {
            let saved_sha: String = row.get(1);
            let saved_bytes: i64 = row.get(2);
            if saved_sha == manifest_sha256 && saved_bytes == records_bytes {
                if let Ok(path) = analytics::safe_relative_path(root, &row.get::<_, String>(0)) {
                    if verify_files {
                        if analytics::verify_batch_files(&path, &batch_copy).is_ok()
                            && verify_batch_coverage(
                                &path,
                                &batch_id,
                                &batch_run,
                                &consumer_id,
                                &event_coverage,
                                &coverage_sha256,
                            )
                            .is_ok()
                        {
                            archive_valid = true;
                        }
                    } else {
                        archive_valid = true;
                    }
                }
            }
        }

        if quarantine_count < 0 && verify_files {
            let mut recovered = None;
            for path in &locations {
                if expected_batch_path(data_root, &batch_run, &batch_id) != *path
                    || analytics::managed_path_is_safe(data_root, path).is_err()
                {
                    continue;
                }
                if analytics::verify_batch_files(path, &batch_copy).is_ok() {
                    recovered = analytics::batch_quarantine_count(path, &batch_copy).ok();
                    if recovered.is_some() {
                        break;
                    }
                }
            }
            if recovered.is_none() {
                if let (Some(root), Some(relative)) = (archive_root, archive_path.as_deref()) {
                    if let Ok(path) = analytics::safe_relative_path(root, relative) {
                        if analytics::verify_batch_files(&path, &batch_copy).is_ok() {
                            recovered = analytics::batch_quarantine_count(&path, &batch_copy).ok();
                        }
                    }
                }
            }
            match recovered {
                Some(count) => {
                    quarantine_count = count;
                    recovered_quarantine.insert(batch_id.clone(), count);
                }
                None => blockers.push(format!(
                    "batch {batch_id} has unknown quarantine count and no verified manifest to recover it"
                )),
            }
        }

        if target_batch.as_deref() == Some(batch_id.as_str()) && quarantine_count != 0 {
            blockers.push(format!(
                "target batch {batch_id} contains quarantined records"
            ));
        }
        if quarantine_count > 0 && event_ids.iter().any(|id| selected_ids.contains(id)) {
            blockers.push(format!(
                "batch {batch_id} contains quarantined raw payload; its covered event rows must remain"
            ));
        }
        result.push(BatchPlan {
            batch_id,
            consumer_id,
            run_id: batch_run,
            manifest_sha256,
            records_sha256,
            coverage_sha256,
            record_count,
            records_bytes,
            quarantine_count,
            event_ids,
            event_coverage,
            locations,
            archive_path,
            archive_valid,
            pending_copy_intent,
        });
    }
    Ok(result)
}

fn check_consumer_receipts<C: GenericClient>(
    client: &mut C,
    run_id: &str,
    event: &EventPlan,
    batches: &[BatchPlan],
    blockers: &mut Vec<String>,
) -> Result<(), Error> {
    let time_rows = client
        .query(
            "SELECT consumer_id FROM scout_analytics_consumers
             WHERE is_required AND retired_at IS NULL
               AND EXISTS (SELECT 1 FROM scout_crawl_event_history h
                           WHERE h.event_id=$1 AND h.crawl_run_id=$2
                             AND h.created_at + replay_window > clock_timestamp())
             ORDER BY consumer_id",
            &[&event.event_id, &run_id],
        )
        .map_err(|error| {
            Error::storage(format!("check required consumer replay windows: {error}"))
        })?;
    for row in time_rows {
        let consumer: String = row.get(0);
        blockers.push(format!(
            "active required consumer {consumer} replay window remains open for event {}",
            event.event_id
        ));
    }

    let consumers = client
        .query(
            "SELECT c.consumer_id FROM scout_analytics_consumers c
             WHERE c.ever_required AND EXISTS (
                 SELECT 1 FROM scout_crawl_event_history h
                 WHERE h.event_id=$1 AND h.crawl_run_id=$2
                   AND (c.retired_at IS NULL OR h.created_at <= c.retired_at)
             ) ORDER BY c.consumer_id",
            &[&event.event_id, &run_id],
        )
        .map_err(|error| {
            Error::storage(format!(
                "read historically required analytics consumers: {error}"
            ))
        })?;
    let required = consumers
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    for consumer in &required {
        let receipt = client
            .query_opt(
                "SELECT b.batch_id,b.quarantine_count
                 FROM scout_crawl_event_batch_events e
                 JOIN scout_crawl_event_batches b USING(batch_id)
                 JOIN scout_crawl_event_ingestion_receipts r
                   ON r.batch_id=b.batch_id AND r.consumer_id=b.consumer_id
                 WHERE e.event_id=$1 AND b.crawl_run_id=$2
                   AND b.consumer_id=$3
                   AND r.downstream_status='complete'
                   AND r.ingested_record_count=b.record_count
                   AND r.manifest_sha256=b.manifest_sha256
                   AND r.records_sha256=b.records_sha256
                   AND r.coverage_sha256=b.coverage_sha256
                 ORDER BY b.batch_id LIMIT 1",
                &[&event.event_id, &run_id, consumer],
            )
            .map_err(|error| {
                Error::storage(format!("verify downstream event receipts: {error}"))
            })?;
        let valid = receipt.is_some_and(|row| {
            let batch_id: String = row.get(0);
            let db_quarantine: Option<i32> = row.get(1);
            let recovered = batches
                .iter()
                .find(|batch| batch.batch_id == batch_id)
                .map(|batch| batch.quarantine_count);
            db_quarantine.or(recovered) == Some(0)
        });
        if !valid {
            blockers.push(format!(
                "historically required consumer {consumer} lacks a verified downstream receipt for event {}",
                event.event_id
            ));
        }
    }
    Ok(())
}

fn load_artifacts<C: GenericClient>(
    client: &mut C,
    events: &[EventPlan],
    archive_root: Option<&Path>,
    verify_files: bool,
    blockers: &mut Vec<String>,
) -> Result<Vec<ArtifactPlan>, Error> {
    let ids = events
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = client
        .query(
            "SELECT event_id,artifact_uri,artifact_kind,sha256,byte_size
             FROM scout_crawl_event_artifacts WHERE event_id=ANY($1::TEXT[])
             ORDER BY event_id,artifact_uri",
            &[&ids],
        )
        .map_err(|error| {
            Error::storage(format!("read analytics artifact dependencies: {error}"))
        })?;
    let mut artifacts = Vec::new();
    let mut identities = HashMap::<String, (String, i64)>::new();
    for row in rows {
        let event_id: String = row.get(0);
        let uri: String = row.get(1);
        let kind: String = row.get(2);
        let sha256: String = row.get(3);
        let bytes: i64 = row.get(4);
        if let Some((prior_sha, prior_bytes)) = identities.get(&uri) {
            if prior_sha != &sha256 || *prior_bytes != bytes {
                blockers.push(format!(
                    "shared artifact URI {uri} has conflicting retained checksums"
                ));
            }
        } else {
            identities.insert(uri.clone(), (sha256.clone(), bytes));
        }
        let archive = client
            .query_opt(
                "SELECT archive_path,sha256,byte_size FROM scout_crawl_event_archive_items
                 WHERE item_type='artifact' AND item_id=$1",
                &[&uri],
            )
            .map_err(|error| Error::storage(format!("read archived artifact identity: {error}")))?;
        let mut archive_valid = false;
        let mut archive_path = None;
        if let (Some(root), Some(row)) = (archive_root, archive) {
            let path_text: String = row.get(0);
            let saved_sha: String = row.get(1);
            let saved_bytes: i64 = row.get(2);
            if saved_sha == sha256 && saved_bytes == bytes && bytes >= 0 {
                if let Ok(path) = analytics::safe_relative_path(root, &path_text) {
                    archive_path = Some(path_text);
                    archive_valid = !verify_files
                        || analytics::read_verified(&path, &sha256, bytes as u64).is_ok();
                }
            }
        }
        artifacts.push(ArtifactPlan {
            event_id,
            uri,
            kind,
            sha256,
            bytes,
            archive_path,
            archive_valid,
        });
    }
    Ok(artifacts)
}

fn validate_event_artifact_payloads<C: GenericClient>(
    client: &mut C,
    events: &[EventPlan],
    artifacts: &[ArtifactPlan],
    blockers: &mut Vec<String>,
) -> Result<(), Error> {
    for event in events {
        let rows = client
            .query(
                "SELECT artifact_uri,artifact_kind,sha256,byte_size
                 FROM scout_crawl_event_artifacts WHERE event_id=$1 ORDER BY artifact_kind,artifact_uri",
                &[&event.event_id],
            )
            .map_err(|error| Error::storage(format!("verify event artifact identities: {error}")))?;
        let saved = rows
            .iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, String>(1),
                    row.get::<_, String>(2),
                    row.get::<_, i64>(3),
                )
            })
            .collect::<Vec<_>>();
        let payload_row = client
            .query_opt(
                "SELECT payload_json FROM scout_crawl_events WHERE event_id=$1",
                &[&event.event_id],
            )
            .map_err(|error| Error::storage(format!("read event artifact payload: {error}")))?;
        let Some(payload_row) = payload_row else {
            // Already-pruned rows are only represented in `events` when a caller
            // selected the same batch again; their completed intent is sufficient.
            continue;
        };
        let payload: String = payload_row.get(0);
        let payload: serde_json::Value = serde_json::from_str(&payload).map_err(|error| {
            Error::safety(format!(
                "parse event {} before prune: {error}",
                event.event_id
            ))
        })?;
        let expected_keys: &[(&str, &str)] = match event.event_type.as_str() {
            "capture_committed" => &[("body_artifact", "body")],
            "corpus_snapshot_published" => &[
                ("crawl_manifest", "crawl_manifest"),
                ("snapshot_manifest", "snapshot_manifest"),
            ],
            "crawl_completed" => &[],
            other => {
                blockers.push(format!(
                    "event {} has unsupported type {other:?}",
                    event.event_id
                ));
                continue;
            }
        };
        if saved.len() != expected_keys.len() {
            blockers.push(format!(
                "event {} has {} retained artifact references; expected {}",
                event.event_id,
                saved.len(),
                expected_keys.len()
            ));
        }
        for (key, kind) in expected_keys {
            let Some(value) = payload.pointer(&format!("/data/{key}")) else {
                blockers.push(format!(
                    "event {} payload lacks required {key}",
                    event.event_id
                ));
                continue;
            };
            let uri = value.get("uri").and_then(serde_json::Value::as_str);
            let sha = value.get("sha256").and_then(serde_json::Value::as_str);
            let bytes = value.get("byte_size").and_then(serde_json::Value::as_i64);
            let matches = saved
                .iter()
                .any(|(saved_uri, saved_kind, saved_sha, saved_bytes)| {
                    Some(saved_uri.as_str()) == uri
                        && saved_kind == kind
                        && Some(saved_sha.as_str()) == sha
                        && Some(*saved_bytes) == bytes
                });
            if !matches {
                blockers.push(format!(
                    "event {} payload artifact {key} differs from retained artifact history",
                    event.event_id
                ));
            }
            let has_archive = artifacts.iter().any(|artifact| {
                artifact.event_id == event.event_id
                    && Some(artifact.uri.as_str()) == uri
                    && Some(artifact.sha256.as_str()) == sha
                    && Some(artifact.bytes) == bytes
                    && artifact.archive_valid
            });
            if !has_archive {
                blockers.push(format!(
                    "event {} artifact {key} has no verified archive copy",
                    event.event_id
                ));
            }
        }
    }
    Ok(())
}

fn load_batch_files<C: GenericClient>(
    client: &mut C,
    batches: &[BatchPlan],
    selected_batch_ids: &HashSet<String>,
    run_id: &str,
    data_root: &Path,
    verify_files: bool,
    target_batch: Option<&str>,
    blockers: &mut Vec<String>,
) -> Result<Vec<BatchFilePlan>, Error> {
    let mut files = Vec::new();
    for batch in batches
        .iter()
        .filter(|batch| selected_batch_ids.contains(&batch.batch_id))
    {
        let expected = expected_batch_path(data_root, run_id, &batch.batch_id);
        let intent_rows = client
            .query(
                "SELECT local_path,expected_sha256,expected_bytes,completed_at IS NOT NULL
                 FROM scout_analytics_prune_items
                 WHERE item_type='batch_copy' AND item_id=$1 AND crawl_run_id=$2",
                &[&batch.batch_id, &run_id],
            )
            .map_err(|error| Error::storage(format!("read batch-copy prune intent: {error}")))?;
        let intents = intent_rows
            .iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    (
                        row.get::<_, Option<String>>(1),
                        row.get::<_, Option<i64>>(2),
                        row.get::<_, bool>(3),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        for path in &batch.locations {
            if *path != expected {
                let message = format!(
                    "registered batch path {} is outside its canonical local queue slot",
                    path.display()
                );
                if target_batch == Some(batch.batch_id.as_str()) {
                    blockers.push(message);
                }
                continue;
            }
            let path_string = path.to_string_lossy().into_owned();
            let intent = intents.get(&path_string);
            let mut complete = false;
            if let Some((sha, bytes, done)) = intent {
                if sha.as_deref() != Some(batch.manifest_sha256.as_str())
                    || *bytes != Some(batch.records_bytes)
                {
                    blockers.push(format!(
                        "batch {} differs from its persisted prune intent",
                        batch.batch_id
                    ));
                    continue;
                }
                complete = *done;
            }
            let exists = match fs::symlink_metadata(path) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => {
                    return Err(Error::storage(format!("inspect local batch copy: {error}")));
                }
            };
            if exists {
                if verify_files {
                    if analytics::managed_path_is_safe(data_root, path).is_err() {
                        blockers.push(format!(
                            "local batch {} contains an unsafe path",
                            batch.batch_id
                        ));
                        continue;
                    }
                    let valid = if intent.is_some() && !complete {
                        verify_batch_delete_resume(path, batch)
                    } else {
                        verify_batch_directory(path, batch)
                    };
                    if let Err(error) = valid {
                        blockers.push(format!(
                            "local batch copy {} is unsafe to prune: {}",
                            batch.batch_id, error.message
                        ));
                        continue;
                    }
                }
                if complete {
                    blockers.push(format!(
                        "completed batch-copy prune intent for {} has a live directory",
                        batch.batch_id
                    ));
                    continue;
                }
            } else if !complete && intent.is_none() {
                let message = format!(
                    "registered local batch copy {} is missing without a prune intent",
                    batch.batch_id
                );
                if target_batch == Some(batch.batch_id.as_str()) {
                    blockers.push(message);
                }
                continue;
            }
            if !complete {
                files.push(BatchFilePlan {
                    batch_id: batch.batch_id.clone(),
                    path: path.clone(),
                    manifest_sha256: batch.manifest_sha256.clone(),
                    records_bytes: batch.records_bytes,
                    complete,
                    intent_preexisting: intent.is_some(),
                });
            }
        }
    }
    Ok(files)
}

fn verify_batch_coverage(
    path: &Path,
    batch_id: &str,
    run_id: &str,
    consumer_id: &str,
    expected: &[BatchEventPlan],
    coverage_sha256: &str,
) -> Result<(), Error> {
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(path.join("batch-manifest.json"))
            .map_err(|error| Error::storage(format!("read archived batch manifest: {error}")))?,
    )
    .map_err(|error| Error::safety(format!("parse archived batch manifest: {error}")))?;
    if manifest.get("batch_id").and_then(serde_json::Value::as_str) != Some(batch_id)
        || manifest
            .get("crawl_run_id")
            .and_then(serde_json::Value::as_str)
            != Some(run_id)
        || manifest
            .get("consumer_id")
            .and_then(serde_json::Value::as_str)
            != Some(consumer_id)
    {
        return Err(Error::safety(
            "batch manifest owner or run identity differs",
        ));
    }
    let records = fs::read(path.join("records.jsonl"))
        .map_err(|error| Error::storage(format!("read batch coverage records: {error}")))?;
    let expected_by_id = expected
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect::<HashMap<_, _>>();
    let mut record_ids = Vec::new();
    let mut seen = HashSet::new();
    for line in records
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let value: serde_json::Value = serde_json::from_slice(line)
            .map_err(|error| Error::safety(format!("parse batch coverage record: {error}")))?;
        let id = value
            .get("record_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::safety("batch record has no record_id"))?;
        let event = expected_by_id
            .get(id)
            .ok_or_else(|| Error::safety("batch contains an unregistered event ID"))?;
        let payload = value
            .get("record_json")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::safety("batch event has no full payload"))?;
        if value.get("batch_id").and_then(serde_json::Value::as_str) != Some(batch_id)
            || value
                .get("crawl_run_id")
                .and_then(serde_json::Value::as_str)
                != Some(run_id)
            || value.get("record_type").and_then(serde_json::Value::as_str) != Some("crawl_event")
            || value.get("event_type").and_then(serde_json::Value::as_str)
                != Some(event.event_type.as_str())
            || value
                .get("event_schema_version")
                .and_then(serde_json::Value::as_i64)
                != Some(i64::from(event.schema_version))
            || value
                .get("payload_sha256")
                .and_then(serde_json::Value::as_str)
                != event.payload_sha256.as_deref()
            || !seen.insert(id.to_owned())
            || event
                .payload_sha256
                .as_deref()
                .is_some_and(|sha| crate::storage::sha256_hex(payload.as_bytes()) != sha)
        {
            return Err(Error::safety(
                "batch envelope or payload differs from registered event identity",
            ));
        }
        let payload: serde_json::Value = serde_json::from_str(payload)
            .map_err(|error| Error::safety(format!("parse batch event payload: {error}")))?;
        if payload
            .get("event_type")
            .and_then(serde_json::Value::as_str)
            != Some(event.event_type.as_str())
            || payload
                .pointer("/data/event_id")
                .and_then(serde_json::Value::as_str)
                != Some(event.event_id.as_str())
            || payload
                .pointer("/data/crawl_run_id")
                .and_then(serde_json::Value::as_str)
                != Some(run_id)
            || payload
                .pointer("/data/schema_version")
                .and_then(serde_json::Value::as_u64)
                != u64::try_from(event.schema_version).ok()
        {
            return Err(Error::safety(
                "batch event payload identity differs from its envelope",
            ));
        }
        record_ids.push(id.to_owned());
    }
    let actual_digest = crate::storage::sha256_hex(record_ids.join("\0").as_bytes());
    let mut actual_set = record_ids.clone();
    let mut expected_set = expected
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<Vec<_>>();
    actual_set.sort();
    expected_set.sort();
    if actual_digest != coverage_sha256
        || actual_set != expected_set
        || seen.len() != expected.len()
    {
        return Err(Error::safety(
            "batch record IDs differ from registered event coverage",
        ));
    }
    Ok(())
}

fn verify_batch_directory(path: &Path, batch: &BatchPlan) -> Result<(), Error> {
    analytics::verify_batch_files(path, &batch_copy(batch))?;
    verify_batch_coverage(
        path,
        &batch.batch_id,
        &batch.run_id,
        &batch.consumer_id,
        &batch.event_coverage,
        &batch.coverage_sha256,
    )
}

fn verify_batch_delete_resume(path: &Path, batch: &BatchPlan) -> Result<(), Error> {
    let directory = fs::symlink_metadata(path)
        .map_err(|error| Error::storage(format!("inspect interrupted batch deletion: {error}")))?;
    if directory.file_type().is_symlink() || !directory.is_dir() {
        return Err(Error::safety(
            "interrupted batch copy is not a regular directory",
        ));
    }
    let mut present = HashSet::new();
    for entry in fs::read_dir(path)
        .map_err(|error| Error::storage(format!("read interrupted batch deletion: {error}")))?
    {
        let entry = entry
            .map_err(|error| Error::storage(format!("read interrupted batch entry: {error}")))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !["batch-manifest.json", "records.jsonl", "_PUBLISHED"].contains(&name.as_str()) {
            return Err(Error::safety(
                "interrupted batch copy has an unexpected entry",
            ));
        }
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|error| Error::storage(format!("inspect interrupted batch file: {error}")))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::safety(
                "interrupted batch copy contains an unsafe file",
            ));
        }
        present.insert(name);
    }
    if present.contains("batch-manifest.json") {
        let bytes = fs::read(path.join("batch-manifest.json"))
            .map_err(|error| Error::storage(format!("read interrupted batch manifest: {error}")))?;
        if crate::storage::sha256_hex(&bytes) != batch.manifest_sha256 {
            return Err(Error::safety("interrupted batch manifest changed"));
        }
    }
    if present.contains("records.jsonl") {
        let bytes = fs::read(path.join("records.jsonl"))
            .map_err(|error| Error::storage(format!("read interrupted batch records: {error}")))?;
        if crate::storage::sha256_hex(&bytes) != batch.records_sha256
            || bytes.len() as i64 != batch.records_bytes
        {
            return Err(Error::safety("interrupted batch records changed"));
        }
    }
    if present.contains("_PUBLISHED") {
        let marker = fs::read_to_string(path.join("_PUBLISHED"))
            .map_err(|error| Error::storage(format!("read interrupted batch marker: {error}")))?;
        if marker != format!("{}\n", batch.manifest_sha256) {
            return Err(Error::safety("interrupted batch marker changed"));
        }
    }
    Ok(())
}

fn batch_copy(batch: &BatchPlan) -> analytics::BatchCopy {
    analytics::BatchCopy {
        batch_id: batch.batch_id.clone(),
        manifest_sha256: batch.manifest_sha256.clone(),
        records_sha256: batch.records_sha256.clone(),
        coverage_sha256: batch.coverage_sha256.clone(),
        record_count: batch.record_count,
        quarantine_count: batch.quarantine_count,
        records_bytes: batch.records_bytes,
    }
}

fn expected_batch_path(data_root: &Path, run_id: &str, batch_id: &str) -> PathBuf {
    data_root
        .join("crawl-event-batches")
        .join(run_id)
        .join(batch_id)
}

fn checked_archive_root(data_root: &Path, verify_files: bool) -> Result<PathBuf, Error> {
    let path = data_root.join("analytics-archive");
    if !verify_files {
        return Ok(path);
    }
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| Error::storage(format!("inspect analytics archive root: {error}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::safety(
            "analytics archive root must be a regular directory",
        ));
    }
    let resolved = path
        .canonicalize()
        .map_err(|error| Error::storage(format!("resolve analytics archive root: {error}")))?;
    if !resolved.starts_with(data_root) {
        return Err(Error::safety("analytics archive root escapes --data-dir"));
    }
    Ok(resolved)
}

fn remove_verified_batch_dir(plan: &Plan, file: &BatchFilePlan) -> Result<(), Error> {
    let expected = expected_batch_path(&plan.data_root, &plan.report.run_id, &file.batch_id);
    if file.path != expected {
        return Err(Error::safety(
            "refuse to remove a batch path outside the local queue",
        ));
    }
    analytics::managed_path_is_safe(&plan.data_root, &file.path)?;
    let batch = plan
        .batches
        .iter()
        .find(|batch| batch.batch_id == file.batch_id)
        .ok_or_else(|| Error::safety("batch-copy prune intent has no retained batch identity"))?;
    if file.intent_preexisting {
        verify_batch_delete_resume(&file.path, batch)?;
    } else {
        verify_batch_directory(&file.path, batch)?;
    }
    if let Some(root) = &plan.archive_root {
        let archive_path = batch
            .archive_path
            .as_deref()
            .ok_or_else(|| Error::safety("batch archive path disappeared before local deletion"))?;
        let archived = analytics::safe_relative_path(root, archive_path)?;
        verify_batch_directory(&archived, batch)?;
        for artifact in plan
            .artifacts
            .iter()
            .filter(|artifact| batch.event_ids.contains(&artifact.event_id))
        {
            let relative = artifact.archive_path.as_deref().ok_or_else(|| {
                Error::safety("artifact archive path disappeared before batch deletion")
            })?;
            let artifact_path = analytics::safe_relative_path(root, relative)?;
            analytics::read_verified(
                &artifact_path,
                &artifact.sha256,
                u64::try_from(artifact.bytes)
                    .map_err(|_| Error::safety("artifact size is invalid"))?,
            )?;
        }
    } else {
        return Err(Error::safety("verified analytics archive root disappeared"));
    }
    for name in ["batch-manifest.json", "records.jsonl", "_PUBLISHED"] {
        let path = file.path.join(name);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error)
                if file.intent_preexisting && error.kind() == std::io::ErrorKind::NotFound =>
            {
                continue;
            }
            Err(error) => {
                return Err(Error::storage(format!(
                    "inspect batch file before deletion: {error}"
                )));
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::safety("refuse to remove unexpected batch file type"));
        }
        fs::remove_file(&path).map_err(|error| {
            Error::storage(format!("remove archived local batch file: {error}"))
        })?;
    }
    fs::remove_dir(&file.path)
        .map_err(|error| Error::storage(format!("remove empty local batch directory: {error}")))
}

fn verify_plan_files(plan: &Plan) -> Result<(), Error> {
    for batch in &plan.batches {
        if batch.archive_valid {
            let root = plan
                .archive_root
                .as_deref()
                .ok_or_else(|| Error::safety("analytics archive root disappeared"))?;
            let relative = batch
                .archive_path
                .as_deref()
                .ok_or_else(|| Error::safety("registered archive batch path disappeared"))?;
            let path = analytics::safe_relative_path(root, relative)?;
            verify_batch_directory(&path, batch)?;
        }
    }
    for artifact in plan
        .artifacts
        .iter()
        .filter(|artifact| artifact.archive_valid)
    {
        let root = plan
            .archive_root
            .as_deref()
            .ok_or_else(|| Error::safety("analytics archive root disappeared"))?;
        let relative = artifact
            .archive_path
            .as_deref()
            .ok_or_else(|| Error::safety("registered artifact archive path disappeared"))?;
        let path = analytics::safe_relative_path(root, relative)?;
        analytics::read_verified(&path, &artifact.sha256, artifact.bytes as u64)?;
    }
    for file in plan.batch_files.iter().filter(|file| !file.complete) {
        match fs::symlink_metadata(&file.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(Error::storage(format!(
                    "inspect local batch before prune: {error}"
                )));
            }
            Ok(_) => {}
        }
        let batch = plan
            .batches
            .iter()
            .find(|batch| batch.batch_id == file.batch_id)
            .ok_or_else(|| Error::safety("local batch copy has no retained batch identity"))?;
        analytics::managed_path_is_safe(&plan.data_root, &file.path)?;
        if file.intent_preexisting {
            verify_batch_delete_resume(&file.path, batch)?;
        } else {
            verify_batch_directory(&file.path, batch)?;
        }
    }
    Ok(())
}

fn lock_prune_inputs<C: GenericClient>(client: &mut C) -> Result<(), Error> {
    client
        .batch_execute(
            "LOCK TABLE scout_analytics_consumers, scout_crawl_event_batches,
                 scout_crawl_event_batch_events, scout_crawl_event_ingestion_receipts,
                 scout_crawl_event_archive_items, scout_crawl_event_artifacts
             IN SHARE MODE",
        )
        .map_err(|error| Error::storage(format!("lock analytics prune evidence: {error}")))
}

fn verify_locked_run<C: GenericClient>(client: &mut C, plan: &Plan) -> Result<(), Error> {
    let row = client
        .query_opt(
            "SELECT status FROM scout_crawl_runs WHERE crawl_run_id=$1 FOR UPDATE",
            &[&plan.report.run_id],
        )
        .map_err(|error| Error::storage(format!("lock Crawl run before pruning: {error}")))?;
    if row.is_none_or(|row| row.get::<_, String>(0) != "completed") {
        return Err(Error::safety(
            "Crawl run changed or is not completed before pruning",
        ));
    }
    let row = client
        .query_one(
            "SELECT expected_event_count,expected_event_coverage::TEXT,
                    (SELECT COUNT(*) FROM scout_crawl_event_history h WHERE h.crawl_run_id=$1),
                    COALESCE((SELECT bit_xor(scout_event_history_fingerprint(
                        h.event_id,h.event_type,h.schema_version,h.dedupe_key,h.payload_sha256,h.created_at
                    ))::TEXT FROM scout_crawl_event_history h WHERE h.crawl_run_id=$1),repeat('0',256))
             FROM scout_crawl_run_history WHERE crawl_run_id=$1",
            &[&plan.report.run_id],
        )
        .map_err(|error| Error::storage(format!("recheck retained event-history coverage: {error}")))?;
    let expected: i64 = row.get(0);
    let expected_fp: String = row.get(1);
    let actual_count: i64 = row.get(2);
    let actual_fp: String = row.get(3);
    if expected != actual_count
        || expected_fp != actual_fp
        || expected != plan.expected_event_count
        || expected_fp != plan.expected_event_coverage
    {
        return Err(Error::safety(
            "retained event-history coverage changed after prune preflight",
        ));
    }
    Ok(())
}

fn verify_intent<C: GenericClient>(
    client: &mut C,
    item_type: &str,
    item_id: &str,
    local_path: &str,
    run_id: &str,
    expected_sha256: &str,
    expected_bytes: i64,
) -> Result<(), Error> {
    let row = client
        .query_opt(
            "SELECT crawl_run_id,expected_sha256,expected_bytes FROM scout_analytics_prune_items
             WHERE item_type=$1 AND item_id=$2 AND local_path=$3",
            &[&item_type, &item_id, &local_path],
        )
        .map_err(|error| Error::storage(format!("verify persisted prune intent: {error}")))?;
    if row.is_none_or(|row| {
        row.get::<_, String>(0) != run_id
            || row.get::<_, Option<String>>(1).as_deref() != Some(expected_sha256)
            || row.get::<_, Option<i64>>(2) != Some(expected_bytes)
    }) {
        return Err(Error::safety(format!(
            "conflicting {item_type} prune intent for {item_id}"
        )));
    }
    Ok(())
}

fn same_plan_database(current: &Plan, original: &Plan) -> bool {
    current.report.target == original.report.target
        && current.report.run_id == original.report.run_id
        && current.expected_event_count == original.expected_event_count
        && current.expected_event_coverage == original.expected_event_coverage
        && current.events == original.events
        && current.batches.len() == original.batches.len()
        && current
            .batches
            .iter()
            .zip(&original.batches)
            .all(|(current, original)| {
                let mut current = current.clone();
                current.pending_copy_intent = false;
                let mut original = original.clone();
                original.pending_copy_intent = false;
                current == original
            })
        && current.artifacts == original.artifacts
        && current.batch_files.len() == original.batch_files.len()
        && current
            .batch_files
            .iter()
            .zip(&original.batch_files)
            .all(|(current, original)| {
                current.batch_id == original.batch_id
                    && current.path == original.path
                    && current.manifest_sha256 == original.manifest_sha256
                    && current.records_bytes == original.records_bytes
                    && current.complete == original.complete
            })
        && current.report.blockers == original.report.blockers
}
