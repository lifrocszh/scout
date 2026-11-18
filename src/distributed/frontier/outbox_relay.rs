//! Outbox reads for an ordered relay (`scout kafka relay`).
//!
//! `consume_local_events` skips an event in retry backoff and moves on, which is right for an
//! independent consumer but would let a run's `crawl_completed` overtake a capture that failed to
//! publish. A relay feeding an ordered log needs head-of-line order instead: it always gets the
//! oldest unacknowledged events, backoff or not, and retries the head itself.
use super::FrontierStore;
use super::events::decode_durable_event;
use crate::distributed::events::CrawlEvent;
use crate::error::Error;

/// One outbox event, decoded and checked, with the exact bytes Postgres stored.
pub(crate) struct OutboxEvent {
    pub(crate) event: CrawlEvent,
    pub(crate) payload_json: String,
    pub(crate) payload_sha256: String,
}

impl FrontierStore {
    /// The oldest events of `run_id` that `consumer_id` has not acknowledged, in outbox order.
    pub(crate) fn unrelayed_events(
        &mut self,
        run_id: &str,
        consumer_id: &str,
        limit: usize,
    ) -> Result<Vec<OutboxEvent>, Error> {
        self.ensure_analytics_consumer(consumer_id)?;
        let limit = i64::try_from(limit.clamp(1, 128))
            .map_err(|_| Error::invalid("relay batch limit is too large"))?;
        let rows = self
            .client
            .query(
                "SELECT event.event_id, event.event_type, event.schema_version,
                        event.payload_json, event.payload_sha256
                 FROM scout_crawl_events AS event
                 LEFT JOIN scout_crawl_event_checkpoints AS checkpoint
                   ON checkpoint.consumer_id = $2 AND checkpoint.event_id = event.event_id
                 WHERE event.crawl_run_id = $1
                   AND checkpoint.acknowledged_at IS NULL
                 ORDER BY event.created_at, event.event_id
                 LIMIT $3",
                &[&run_id, &consumer_id, &limit],
            )
            .map_err(|error| Error::storage(format!("read unrelayed crawl events: {error}")))?;
        rows.into_iter()
            .map(|row| {
                let event_id: String = row.get(0);
                let event_type: String = row.get(1);
                let schema_version: i32 = row.get(2);
                let payload_json: String = row.get(3);
                let stored_sha256: Option<String> = row.get(4);
                let event = decode_durable_event(
                    run_id,
                    &event_id,
                    &event_type,
                    schema_version,
                    &payload_json,
                    stored_sha256.as_deref(),
                )?;
                let payload_sha256 = stored_sha256
                    .unwrap_or_else(|| crate::storage::sha256_hex(payload_json.as_bytes()));
                Ok(OutboxEvent {
                    event,
                    payload_json,
                    payload_sha256,
                })
            })
            .collect()
    }

    /// Acknowledge one relayed event.
    pub(crate) fn acknowledge_relayed_event(
        &mut self,
        consumer_id: &str,
        event_id: &str,
    ) -> Result<(), Error> {
        self.acknowledge_event(consumer_id, event_id)
    }

    /// Record a failed relay attempt, for `scout events preflight` and operators. The relay
    /// retries the same event itself; the stored backoff does not reorder it.
    pub(crate) fn record_relay_failure(
        &mut self,
        consumer_id: &str,
        event_id: &str,
        message: &str,
    ) -> Result<(), Error> {
        self.record_event_failure(consumer_id, event_id, message)
    }

    /// Events of `run_id` not yet acknowledged by `consumer_id`: the relay's lag.
    pub(crate) fn unrelayed_event_count(
        &mut self,
        run_id: &str,
        consumer_id: &str,
    ) -> Result<i64, Error> {
        Ok(self
            .client
            .query_one(
                "SELECT count(*) FROM scout_crawl_events AS event
                 LEFT JOIN scout_crawl_event_checkpoints AS checkpoint
                   ON checkpoint.consumer_id = $2 AND checkpoint.event_id = event.event_id
                 WHERE event.crawl_run_id = $1 AND checkpoint.acknowledged_at IS NULL",
                &[&run_id, &consumer_id],
            )
            .map_err(|error| Error::storage(format!("count unrelayed crawl events: {error}")))?
            .get(0))
    }

    /// Whether the run's snapshot has been published: its last outbox event.
    pub(crate) fn snapshot_published(&mut self, run_id: &str) -> Result<bool, Error> {
        Ok(self
            .client
            .query_one(
                "SELECT EXISTS (
                     SELECT 1 FROM scout_crawl_events
                     WHERE crawl_run_id = $1 AND event_type = 'corpus_snapshot_published'
                 )",
                &[&run_id],
            )
            .map_err(|error| Error::storage(format!("check snapshot publication: {error}")))?
            .get(0))
    }

    /// The accepted capture metadata of one task, as the coordinator stored it.
    pub(crate) fn accepted_capture_metadata(
        &mut self,
        run_id: &str,
        task_id: i64,
    ) -> Result<String, Error> {
        self.client
            .query_opt(
                "SELECT capture_metadata FROM scout_crawl_task_results
                 WHERE crawl_run_id = $1 AND task_id = $2 AND capture_metadata IS NOT NULL",
                &[&run_id, &task_id],
            )
            .map_err(|error| Error::storage(format!("read accepted capture: {error}")))?
            .map(|row| row.get(0))
            .ok_or_else(|| {
                Error::storage(format!(
                    "capture_committed event for task {task_id} has no accepted capture"
                ))
            })
    }
}
