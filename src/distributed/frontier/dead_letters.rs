//! The dead-letter queue: tasks that exhausted `max_task_attempts`.
//!
//! A dead-lettered task is a `failed` row of `scout_crawl_tasks` with `dead_lettered_at` set;
//! there is no second copy of it. Listing reads those rows; requeueing moves them back to
//! `pending` with a fresh attempt budget, which the claim query then serves like any other
//! due task. Only an active run can be requeued: a completed run's Corpus is already sealed.
use super::FrontierStore;
use crate::error::Error;
use serde::Serialize;

/// The most entries `list_dead_letters` returns at once.
pub(crate) const MAX_LIST_LIMIT: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DeadLetter {
    pub(crate) task_id: i64,
    pub(crate) source_id: String,
    pub(crate) origin: String,
    pub(crate) normalized_url: String,
    pub(crate) task_attempt_count: i32,
    pub(crate) max_task_attempts: i32,
    pub(crate) last_error: Option<String>,
    pub(crate) dead_lettered_at_unix_ms: i64,
    /// How many times this task was requeued from the dead-letter queue before.
    pub(crate) dead_letter_requeues: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DeadLetterListing {
    pub(crate) crawl_run_id: String,
    pub(crate) run_status: String,
    pub(crate) total: i64,
    pub(crate) entries: Vec<DeadLetter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeadLetterSelection {
    All,
    Tasks(Vec<i64>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct RequeueOutcome {
    pub(crate) crawl_run_id: String,
    pub(crate) requeued_task_ids: Vec<i64>,
    /// Requested task IDs that are not dead letters of this run (unknown, or not `failed`).
    pub(crate) skipped_task_ids: Vec<i64>,
}

impl FrontierStore {
    /// Dead letters of one run, oldest first.
    pub(crate) fn list_dead_letters(
        &mut self,
        run_id: &str,
        limit: usize,
    ) -> Result<DeadLetterListing, Error> {
        let limit = i64::try_from(limit.clamp(1, MAX_LIST_LIMIT)).expect("bounded limit");
        let run_status: String = self
            .client
            .query_opt(
                "SELECT status FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("read crawl run status: {error}")))?
            .ok_or_else(|| Error::invalid(format!("crawl run {run_id:?} does not exist")))?
            .get(0);
        let total: i64 = self
            .client
            .query_one(
                "SELECT count(*) FROM scout_crawl_tasks
                 WHERE crawl_run_id = $1 AND status = 'failed'",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("count dead-letter tasks: {error}")))?
            .get(0);
        let entries = self
            .client
            .query(
                "SELECT task_id, source_id, origin, normalized_url, task_attempt_count,
                        max_task_attempts, last_error,
                        (extract(epoch FROM dead_lettered_at) * 1000)::BIGINT,
                        dead_letter_requeues
                 FROM scout_crawl_tasks
                 WHERE crawl_run_id = $1 AND status = 'failed'
                 ORDER BY dead_lettered_at, task_id
                 LIMIT $2",
                &[&run_id, &limit],
            )
            .map_err(|error| Error::storage(format!("list dead-letter tasks: {error}")))?
            .into_iter()
            .map(|row| DeadLetter {
                task_id: row.get(0),
                source_id: row.get(1),
                origin: row.get(2),
                normalized_url: row.get(3),
                task_attempt_count: row.get(4),
                max_task_attempts: row.get(5),
                last_error: row.get(6),
                dead_lettered_at_unix_ms: row.get(7),
                dead_letter_requeues: row.get(8),
            })
            .collect();
        Ok(DeadLetterListing {
            crawl_run_id: run_id.to_owned(),
            run_status,
            total,
            entries,
        })
    }

    /// Move dead letters back to `pending` with a fresh attempt budget, due immediately.
    pub(crate) fn requeue_dead_letters(
        &mut self,
        run_id: &str,
        selection: &DeadLetterSelection,
    ) -> Result<RequeueOutcome, Error> {
        let (all, requested) = match selection {
            DeadLetterSelection::All => (true, Vec::new()),
            DeadLetterSelection::Tasks(ids) if ids.is_empty() => {
                return Err(Error::invalid("select at least one dead-letter task"));
            }
            DeadLetterSelection::Tasks(ids) => (false, ids.clone()),
        };
        let mut tx = self
            .client
            .transaction()
            .map_err(|error| Error::storage(format!("begin dead-letter requeue: {error}")))?;
        // Same lock order as run completion (scheduler, then run), so a requeue either lands
        // before the run completes or sees it completed.
        tx.query_opt(
            "SELECT crawl_run_id FROM scout_crawl_task_scheduler
             WHERE crawl_run_id = $1 FOR UPDATE",
            &[&run_id],
        )
        .map_err(|error| Error::storage(format!("lock crawl scheduler for requeue: {error}")))?;
        let status: String = tx
            .query_opt(
                "SELECT status FROM scout_crawl_runs WHERE crawl_run_id = $1 FOR UPDATE",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("lock crawl run for requeue: {error}")))?
            .ok_or_else(|| Error::invalid(format!("crawl run {run_id:?} does not exist")))?
            .get(0);
        if status != "active" {
            return Err(Error::invalid(format!(
                "crawl run {run_id:?} is {status}; only an active run's dead letters can be requeued"
            )));
        }
        let mut requeued_task_ids = tx
            .query(
                "UPDATE scout_crawl_tasks
                 SET status = 'pending', task_attempt_count = 0, retry_at = clock_timestamp(),
                     dead_lettered_at = NULL, dead_letter_requeues = dead_letter_requeues + 1,
                     updated_at = clock_timestamp()
                 WHERE crawl_run_id = $1 AND status = 'failed'
                   AND ($2 OR task_id = ANY($3::BIGINT[]))
                 RETURNING task_id",
                &[&run_id, &all, &requested],
            )
            .map_err(|error| Error::storage(format!("requeue dead-letter tasks: {error}")))?
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        tx.commit()
            .map_err(|error| Error::storage(format!("commit dead-letter requeue: {error}")))?;
        requeued_task_ids.sort_unstable();
        let mut skipped_task_ids = requested
            .into_iter()
            .filter(|id| requeued_task_ids.binary_search(id).is_err())
            .collect::<Vec<_>>();
        skipped_task_ids.sort_unstable();
        skipped_task_ids.dedup();
        Ok(RequeueOutcome {
            crawl_run_id: run_id.to_owned(),
            requeued_task_ids,
            skipped_task_ids,
        })
    }

    /// Total requeues from the dead-letter queue in one run, for the frontier gauges.
    pub(crate) fn dead_letter_requeue_count(&mut self, run_id: &str) -> Result<i64, Error> {
        self.client
            .query_one(
                "SELECT COALESCE(sum(dead_letter_requeues), 0)::BIGINT
                 FROM scout_crawl_tasks WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .map(|row| row.get(0))
            .map_err(|error| Error::storage(format!("count dead-letter requeues: {error}")))
    }
}
