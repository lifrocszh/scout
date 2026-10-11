use super::{
    CLAIM_ONE_TASK, ClaimedTask, FrontierStore, LeaseTiming, TaskStatus, duration_millis,
    validate_digest, validate_schedule_key,
};
use crate::error::Error;
use crate::storage::sha256_hex;
use crate::url::{normalize_url, origin_key};
use ::url::Url;
use std::time::Duration;

impl FrontierStore {
    pub(crate) fn mark_run_failed(&mut self, run_id: &str) -> Result<(), Error> {
        self.client
            .execute(
                "UPDATE scout_crawl_runs SET status = 'failed' WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("mark crawl run failed: {error}")))?;
        Ok(())
    }

    /// Insert a run once; an existing ID is resumable only with the original digest.
    pub(crate) fn ensure_run(&mut self, run_id: &str, config_digest: &str) -> Result<bool, Error> {
        validate_digest(config_digest, "crawl config digest")?;
        if run_id.trim().is_empty() {
            return Err(Error::invalid("crawl_run_id must not be empty"));
        }
        let inserted = self
            .client
            .execute(
                "INSERT INTO scout_crawl_runs(crawl_run_id, config_digest)
                 VALUES ($1, $2)
                 ON CONFLICT (crawl_run_id) DO NOTHING",
                &[&run_id, &config_digest],
            )
            .map_err(|error| Error::storage(format!("persist crawl run identity: {error}")))?
            > 0;
        let stored = self
            .client
            .query_one(
                "SELECT config_digest,status FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("read crawl run identity: {error}")))?;
        let stored_digest: String = stored.get(0);
        if stored.get::<_, String>(1) == "failed" {
            return Err(Error::invalid(
                "failed crawl run cannot resume; start a new run after repairing storage",
            ));
        }
        if stored_digest != config_digest {
            return Err(Error::invalid(format!(
                "crawl run {run_id:?} has config digest {stored_digest}; refusing resume with {config_digest}"
            )));
        }
        self.client
            .execute(
                "INSERT INTO scout_crawl_task_scheduler(crawl_run_id)
                 VALUES ($1) ON CONFLICT (crawl_run_id) DO NOTHING",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("initialize task schedule cursor: {error}")))?;
        Ok(inserted)
    }

    /// Keep the configured crawl duration anchored to the persisted run creation time,
    /// including when a coordinator resumes after a restart.
    pub(crate) fn run_duration_exceeded(
        &mut self,
        run_id: &str,
        max_duration: Duration,
    ) -> Result<bool, Error> {
        let max_seconds = i64::try_from(max_duration.as_secs())
            .map_err(|_| Error::invalid("max crawl duration is too large"))?;
        self.client
            .query_opt(
                "SELECT created_at + ($2::BIGINT * interval '1 second') <= clock_timestamp()
                 FROM scout_crawl_runs WHERE crawl_run_id = $1",
                &[&run_id, &max_seconds],
            )
            .map_err(|error| Error::storage(format!("check crawl duration: {error}")))?
            .map(|row| row.get(0))
            .ok_or_else(|| Error::storage(format!("crawl run {run_id:?} does not exist")))
    }

    /// Add one logical URL task. Re-enqueueing the same run/source/URL is harmless.
    #[cfg(test)]
    pub(crate) fn enqueue_url(
        &mut self,
        run_id: &str,
        source_id: &str,
        normalized_url: &str,
        max_task_attempts: i32,
    ) -> Result<bool, Error> {
        self.enqueue_url_with_schedule(run_id, source_id, normalized_url, max_task_attempts, None)
    }

    pub(crate) fn enqueue_scheduled_url(
        &mut self,
        run_id: &str,
        source_id: &str,
        normalized_url: &str,
        max_task_attempts: i32,
        schedule_key: &str,
    ) -> Result<bool, Error> {
        self.enqueue_url_with_schedule(
            run_id,
            source_id,
            normalized_url,
            max_task_attempts,
            Some(schedule_key),
        )
    }

    fn enqueue_url_with_schedule(
        &mut self,
        run_id: &str,
        source_id: &str,
        normalized_url: &str,
        max_task_attempts: i32,
        schedule_key: Option<&str>,
    ) -> Result<bool, Error> {
        if source_id.trim().is_empty() || max_task_attempts <= 0 {
            return Err(Error::invalid(
                "source_id and max_task_attempts must be positive",
            ));
        }
        let parsed = Url::parse(normalized_url)
            .map_err(|error| Error::invalid(format!("invalid frontier URL: {error}")))?;
        let normalized = normalize_url(parsed.clone()).map_err(Error::invalid)?;
        if normalized != normalized_url {
            return Err(Error::invalid("frontier URL must already be normalized"));
        }
        let origin = origin_key(&parsed);
        let schedule_key = schedule_key.unwrap_or(&origin);
        validate_schedule_key(schedule_key)?;
        let idempotency_key = sha256_hex(
            format!("scout.crawl-task.v1\0{run_id}\0{source_id}\0{normalized_url}").as_bytes(),
        );
        self.client
            .execute(
                "INSERT INTO scout_crawl_tasks(
                     crawl_run_id, source_id, origin, normalized_url, schedule_key,
                     max_task_attempts, idempotency_key
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (crawl_run_id, source_id, normalized_url) DO NOTHING",
                &[
                    &run_id,
                    &source_id,
                    &origin,
                    &normalized_url,
                    &schedule_key,
                    &max_task_attempts,
                    &idempotency_key,
                ],
            )
            .map(|count| count > 0)
            .map_err(|error| Error::storage(format!("enqueue crawl URL: {error}")))
    }

    /// Claim exactly one due task; row locking coordinates independent PostgreSQL clients.
    #[cfg(test)]
    pub(crate) fn claim_one(
        &mut self,
        run_id: &str,
        worker_id: &str,
        timing: LeaseTiming,
    ) -> Result<Option<ClaimedTask>, Error> {
        self.claim_one_excluding(run_id, worker_id, timing, &[])
    }

    pub(crate) fn claim_one_excluding(
        &mut self,
        run_id: &str,
        worker_id: &str,
        timing: LeaseTiming,
        excluded_source_ids: &[String],
    ) -> Result<Option<ClaimedTask>, Error> {
        if worker_id.trim().is_empty() {
            return Err(Error::invalid("worker_id must not be empty"));
        }
        let lease_ms = duration_millis(timing.lease_for)?;
        let _measurement = crate::distributed::metrics::DatabaseScope::new("claim_one_autocommit");
        self.client
            .query_opt(
                CLAIM_ONE_TASK,
                &[&run_id, &worker_id, &lease_ms, &excluded_source_ids],
            )
            .map(|row| row.map(claimed_task))
            .map_err(|error| Error::storage(format!("claim crawl task: {error}")))
    }

    pub(crate) fn renew_lease(
        &mut self,
        task: &ClaimedTask,
        timing: LeaseTiming,
    ) -> Result<bool, Error> {
        let lease_ms = duration_millis(timing.lease_for)?;
        let _measurement =
            crate::distributed::metrics::DatabaseScope::new("renew_lease_autocommit");
        self.client
            .execute(
                "UPDATE scout_crawl_tasks
                 SET lease_deadline = clock_timestamp() + ($4::BIGINT * interval '1 millisecond'),
                     updated_at = clock_timestamp()
                 WHERE task_id = $1 AND status = 'leased'
                   AND lease_owner = $2 AND lease_epoch = $3
                   AND lease_deadline > clock_timestamp()
                   AND crawl_run_id = $5 AND source_id = $6 AND origin = $7
                   AND normalized_url = $8 AND schedule_key = $9 AND idempotency_key = $10",
                &[
                    &task.task_id,
                    &task.lease_owner,
                    &task.lease_epoch,
                    &lease_ms,
                    &task.crawl_run_id,
                    &task.source_id,
                    &task.origin,
                    &task.normalized_url,
                    &task.schedule_key,
                    &task.idempotency_key,
                ],
            )
            .map(|count| count == 1)
            .map_err(|error| Error::storage(format!("renew crawl task lease: {error}")))
    }

    #[cfg(test)]
    pub(crate) fn complete_task(&mut self, task: &ClaimedTask) -> Result<bool, Error> {
        self.client
            .execute(
                "UPDATE scout_crawl_tasks
                 SET status = 'succeeded', lease_owner = NULL, lease_deadline = NULL,
                     updated_at = clock_timestamp()
                 WHERE task_id = $1 AND status = 'leased'
                   AND lease_owner = $2 AND lease_epoch = $3
                   AND lease_deadline > clock_timestamp()
                   AND crawl_run_id = $4 AND source_id = $5 AND origin = $6
                   AND normalized_url = $7 AND schedule_key = $8 AND idempotency_key = $9",
                &[
                    &task.task_id,
                    &task.lease_owner,
                    &task.lease_epoch,
                    &task.crawl_run_id,
                    &task.source_id,
                    &task.origin,
                    &task.normalized_url,
                    &task.schedule_key,
                    &task.idempotency_key,
                ],
            )
            .map(|count| count == 1)
            .map_err(|error| Error::storage(format!("complete crawl task: {error}")))
    }

    /// Persist a bounded task retry or terminal failure. HTTP retry counts stay separate.
    pub(crate) fn fail_task(
        &mut self,
        task: &ClaimedTask,
        error_message: &str,
        retry_delay: Duration,
    ) -> Result<Option<TaskStatus>, Error> {
        let delay_ms = duration_millis(retry_delay)?;
        let row = self
            .client
            .query_opt(
                "UPDATE scout_crawl_tasks
                 SET status = CASE
                         WHEN task_attempt_count >= max_task_attempts THEN 'failed'
                         ELSE 'pending'
                     END,
                     retry_at = CASE
                         WHEN task_attempt_count >= max_task_attempts THEN retry_at
                         ELSE clock_timestamp() + ($4::BIGINT * interval '1 millisecond')
                     END,
                     lease_owner = NULL,
                     lease_deadline = NULL,
                     last_error = $5,
                     updated_at = clock_timestamp()
                 WHERE task_id = $1 AND status = 'leased'
                   AND lease_owner = $2 AND lease_epoch = $3
                   AND lease_deadline > clock_timestamp()
                   AND crawl_run_id = $6 AND source_id = $7 AND origin = $8
                   AND normalized_url = $9 AND schedule_key = $10 AND idempotency_key = $11
                 RETURNING status",
                &[
                    &task.task_id,
                    &task.lease_owner,
                    &task.lease_epoch,
                    &delay_ms,
                    &error_message,
                    &task.crawl_run_id,
                    &task.source_id,
                    &task.origin,
                    &task.normalized_url,
                    &task.schedule_key,
                    &task.idempotency_key,
                ],
            )
            .map_err(|error| Error::storage(format!("record crawl task failure: {error}")))?;
        row.map(|row| TaskStatus::parse(row.get::<_, &str>(0)))
            .transpose()
    }

    pub(crate) fn lease_is_live(&mut self, task: &ClaimedTask) -> Result<bool, Error> {
        self.client
            .query_opt(
                "SELECT task_id FROM scout_crawl_tasks
                 WHERE task_id = $1 AND crawl_run_id = $2 AND source_id = $3 AND origin = $4
                   AND normalized_url = $5 AND schedule_key = $6 AND idempotency_key = $7
                   AND status = 'leased' AND lease_owner = $8 AND lease_epoch = $9
                   AND lease_deadline > clock_timestamp()",
                &[
                    &task.task_id,
                    &task.crawl_run_id,
                    &task.source_id,
                    &task.origin,
                    &task.normalized_url,
                    &task.schedule_key,
                    &task.idempotency_key,
                    &task.lease_owner,
                    &task.lease_epoch,
                ],
            )
            .map(|row| row.is_some())
            .map_err(|error| Error::storage(format!("validate crawl task lease: {error}")))
    }

    pub(crate) fn unfinished_task_count_excluding(
        &mut self,
        run_id: &str,
        excluded: &[String],
    ) -> Result<usize, Error> {
        let count: i64 = self
            .client
            .query_one(
                "SELECT count(*) FROM scout_crawl_tasks
                 WHERE crawl_run_id = $1 AND (status = 'leased'
                    OR (status = 'pending' AND NOT (source_id = ANY($2))))",
                &[&run_id, &excluded],
            )
            .map_err(|error| Error::storage(format!("count unfinished crawl tasks: {error}")))?
            .get(0);
        usize::try_from(count)
            .map_err(|_| Error::storage("unfinished crawl task count does not fit memory"))
    }
}

fn claimed_task(row: postgres::Row) -> ClaimedTask {
    ClaimedTask {
        task_id: row.get("task_id"),
        crawl_run_id: row.get("crawl_run_id"),
        source_id: row.get("source_id"),
        origin: row.get("origin"),
        normalized_url: row.get("normalized_url"),
        schedule_key: row.get("schedule_key"),
        task_attempt_count: row.get("task_attempt_count"),
        lease_owner: row.get("lease_owner"),
        lease_epoch: row.get("lease_epoch"),
        lease_deadline_unix_ms: row.get("lease_deadline_unix_ms"),
        idempotency_key: row.get("idempotency_key"),
    }
}
