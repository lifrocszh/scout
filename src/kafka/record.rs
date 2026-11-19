//! The two record kinds on the ingestion log, and their validation.
//!
//! * `<prefix>.crawl-results`: one record per accepted capture. Key: the task's normalized URL.
//!   Value: the raw page body. Headers: schema, outbox `event_id`, Crawl run, task, the accepted
//!   capture metadata exactly as Postgres stored it, and the body SHA-256.
//! * `<prefix>.crawl-events`: every outbox event, byte for byte. Key: the Crawl run ID, so one
//!   run's events share a partition and keep their order. Headers: schema, `event_id`, run, type
//!   and the payload SHA-256 the outbox recorded.
//!
//! Both may be delivered more than once; `event_id` is the deduplication key.
use super::log::LogRecord;
use crate::crawl::Capture;
use crate::distributed::capture_from_accepted_metadata;
use crate::distributed::events::CrawlEvent;
use crate::error::Error;
use crate::storage::sha256_hex;
use std::collections::BTreeMap;

pub(crate) const RESULT_SCHEMA: &str = "scout.kafka.crawl-result.v1";
pub(crate) const EVENT_SCHEMA: &str = "scout.kafka.crawl-event.v1";

const SCHEMA: &str = "scout-schema";
const EVENT_ID: &str = "scout-event-id";
const RUN_ID: &str = "scout-crawl-run-id";
const TASK_ID: &str = "scout-task-id";
const CAPTURE: &str = "scout-capture";
const BODY_SHA256: &str = "scout-body-sha256";
const EVENT_TYPE: &str = "scout-event-type";
const PAYLOAD_SHA256: &str = "scout-payload-sha256";

/// An accepted capture with its body.
#[derive(Debug, Clone)]
pub(crate) struct ResultRecord {
    pub(crate) event_id: String,
    pub(crate) crawl_run_id: String,
    pub(crate) task_id: i64,
    pub(crate) capture_metadata: String,
    pub(crate) capture: Capture,
    pub(crate) body: Vec<u8>,
}

impl ResultRecord {
    /// Build and validate a record from the outbox event, the stored metadata and the body.
    pub(crate) fn new(
        event_id: &str,
        crawl_run_id: &str,
        task_id: i64,
        capture_metadata: String,
        body: Vec<u8>,
    ) -> Result<Self, Error> {
        let capture = capture_from_accepted_metadata(&capture_metadata)?;
        if capture.content_bytes != body.len() || sha256_hex(&body) != capture.body_hash {
            return Err(Error::storage(format!(
                "body of {event_id:?} does not match its accepted capture"
            )));
        }
        Ok(Self {
            event_id: event_id.to_owned(),
            crawl_run_id: crawl_run_id.to_owned(),
            task_id,
            capture_metadata,
            capture,
            body,
        })
    }

    pub(crate) fn encode(&self) -> LogRecord {
        let headers = BTreeMap::from([
            (SCHEMA.to_owned(), RESULT_SCHEMA.as_bytes().to_vec()),
            (EVENT_ID.to_owned(), self.event_id.as_bytes().to_vec()),
            (RUN_ID.to_owned(), self.crawl_run_id.as_bytes().to_vec()),
            (TASK_ID.to_owned(), self.task_id.to_string().into_bytes()),
            (
                CAPTURE.to_owned(),
                self.capture_metadata.as_bytes().to_vec(),
            ),
            (
                BODY_SHA256.to_owned(),
                self.capture.body_hash.as_bytes().to_vec(),
            ),
        ]);
        LogRecord {
            key: self.capture.requested_url.as_bytes().to_vec(),
            value: self.body.clone(),
            headers,
        }
    }

    pub(crate) fn decode(record: LogRecord) -> Result<Self, Error> {
        require_schema(&record, RESULT_SCHEMA)?;
        let event_id = header(&record, EVENT_ID)?;
        let task_id = header(&record, TASK_ID)?
            .parse::<i64>()
            .map_err(|_| Error::storage(format!("result {event_id:?} has an invalid task ID")))?;
        let run_id = header(&record, RUN_ID)?;
        let metadata = header(&record, CAPTURE)?;
        let body_sha256 = header(&record, BODY_SHA256)?;
        let LogRecord { key, value, .. } = record;
        let decoded = Self::new(&event_id, &run_id, task_id, metadata, value)?;
        if body_sha256 != decoded.capture.body_hash
            || key != decoded.capture.requested_url.as_bytes()
        {
            return Err(Error::storage(format!(
                "result {event_id:?} key or body digest does not match its capture"
            )));
        }
        Ok(decoded)
    }
}

/// One outbox event as the log carries it.
#[derive(Debug, Clone)]
pub(crate) struct EventRecord {
    pub(crate) event: CrawlEvent,
    pub(crate) payload_json: String,
}

impl EventRecord {
    pub(crate) fn encode(&self, payload_sha256: &str) -> LogRecord {
        let headers = BTreeMap::from([
            (SCHEMA.to_owned(), EVENT_SCHEMA.as_bytes().to_vec()),
            (
                EVENT_ID.to_owned(),
                self.event.event_id().as_bytes().to_vec(),
            ),
            (
                RUN_ID.to_owned(),
                self.event.crawl_run_id().as_bytes().to_vec(),
            ),
            (
                EVENT_TYPE.to_owned(),
                self.event.event_type().as_bytes().to_vec(),
            ),
            (
                PAYLOAD_SHA256.to_owned(),
                payload_sha256.as_bytes().to_vec(),
            ),
        ]);
        LogRecord {
            key: self.event.crawl_run_id().as_bytes().to_vec(),
            value: self.payload_json.as_bytes().to_vec(),
            headers,
        }
    }

    pub(crate) fn decode(record: LogRecord) -> Result<Self, Error> {
        require_schema(&record, EVENT_SCHEMA)?;
        let event_id = header(&record, EVENT_ID)?;
        let payload_json = String::from_utf8(record.value.clone())
            .map_err(|_| Error::storage(format!("event {event_id:?} payload is not UTF-8")))?;
        let event: CrawlEvent = serde_json::from_str(&payload_json)
            .map_err(|error| Error::storage(format!("decode event {event_id:?}: {error}")))?;
        if event.event_id() != event_id
            || event.crawl_run_id() != header(&record, RUN_ID)?
            || event.event_type() != header(&record, EVENT_TYPE)?
            || sha256_hex(payload_json.as_bytes()) != header(&record, PAYLOAD_SHA256)?
            || record.key != event.crawl_run_id().as_bytes()
        {
            return Err(Error::storage(format!(
                "event {event_id:?} does not match its record headers"
            )));
        }
        Ok(Self {
            event,
            payload_json,
        })
    }
}

fn require_schema(record: &LogRecord, expected: &str) -> Result<(), Error> {
    let schema = header(record, SCHEMA)?;
    if schema != expected {
        return Err(Error::storage(format!(
            "unexpected Kafka record schema {schema:?}; expected {expected:?}"
        )));
    }
    Ok(())
}

fn header(record: &LogRecord, name: &str) -> Result<String, Error> {
    let value = record
        .headers
        .get(name)
        .ok_or_else(|| Error::storage(format!("Kafka record has no {name} header")))?;
    String::from_utf8(value.clone())
        .map_err(|_| Error::storage(format!("Kafka record header {name} is not UTF-8")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::events::{CRAWL_EVENT_SCHEMA_VERSION, CrawlCompleted};

    fn metadata(body: &[u8]) -> String {
        let identity = "Heading\nSome useful text";
        serde_json::json!({
            "requested_url": "http://fixture:8080/a",
            "final_url": "http://fixture:8080/a",
            "redirects": [],
            "source_id": "fixture",
            "title": "A",
            "description": null,
            "language": null,
            "canonical_url": null,
            "extraction_method": "semantic_main",
            "content": "Some useful text",
            "identity": identity,
            "content_hash": sha256_hex(identity.as_bytes()),
            "content_bytes": body.len(),
            "body_hash": sha256_hex(body),
            "content_type": "text/html",
            "status_code": 200
        })
        .to_string()
    }

    #[test]
    fn result_record_round_trips_and_rejects_a_changed_body() {
        let body = b"<html><body>a</body></html>".to_vec();
        let record = ResultRecord::new(
            "capture-committed.v1:run-x:7",
            "run-x",
            7,
            metadata(&body),
            body,
        )
        .expect("valid record");
        let encoded = record.encode();
        assert_eq!(encoded.key, b"http://fixture:8080/a");
        let decoded = ResultRecord::decode(encoded.clone()).expect("decode");
        assert_eq!(decoded.event_id, record.event_id);
        assert_eq!(decoded.task_id, 7);
        assert_eq!(decoded.body, record.body);

        let mut tampered = encoded.clone();
        tampered.value.push(b'!');
        assert!(ResultRecord::decode(tampered).is_err());

        let mut wrong_schema = encoded;
        wrong_schema
            .headers
            .insert(SCHEMA.into(), EVENT_SCHEMA.as_bytes().to_vec());
        assert!(ResultRecord::decode(wrong_schema).is_err());
    }

    #[test]
    fn event_record_round_trips_byte_for_byte() {
        let event = CrawlEvent::CrawlCompleted(CrawlCompleted {
            event_id: "crawl-completed.v1:run-x".into(),
            schema_version: CRAWL_EVENT_SCHEMA_VERSION,
            crawl_run_id: "run-x".into(),
            completed_at_utc: "2026-11-18T00:00:00.000000Z".into(),
            stop_reason: "frontier_exhausted".into(),
            config_sha256: "0".repeat(64),
        });
        let payload_json = serde_json::to_string(&event).expect("serialize");
        let record = EventRecord {
            event,
            payload_json: payload_json.clone(),
        };
        let encoded = record.encode(&sha256_hex(payload_json.as_bytes()));
        assert_eq!(encoded.key, b"run-x");
        let decoded = EventRecord::decode(encoded.clone()).expect("decode");
        assert_eq!(decoded.payload_json, payload_json);

        let mut tampered = encoded;
        tampered
            .headers
            .insert(PAYLOAD_SHA256.into(), "0".repeat(64).into_bytes());
        assert!(EventRecord::decode(tampered).is_err());
    }
}
