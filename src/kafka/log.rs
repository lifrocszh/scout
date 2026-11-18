//! A small synchronous view of one Kafka cluster: create topics, produce one keyed record, fetch
//! a partition from an offset, read high watermarks.
//!
//! The rest of Scout is synchronous (blocking Postgres, SQLite), so this owns a current-thread
//! Tokio runtime and blocks on the async `rskafka` client. Every call has a deadline: a broker
//! that is down surfaces as an error instead of a hang.
use crate::error::Error;
use rskafka::BackoffConfig;
use rskafka::client::partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Every broker call gives up after this long, retries included.
const CALL_DEADLINE: Duration = Duration::from_secs(30);
/// Large enough for one 16 MiB body plus its headers.
pub(crate) const MAX_MESSAGE_BYTES: usize = 20 * 1024 * 1024;

/// One record as Scout writes and reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) headers: BTreeMap<String, Vec<u8>>,
}

/// A fetched record and its offset.
#[derive(Debug)]
pub(crate) struct Fetched {
    pub(crate) offset: i64,
    pub(crate) record: LogRecord,
}

pub(crate) struct Log {
    runtime: Runtime,
    client: Client,
    partitions: HashMap<(String, i32), Arc<PartitionClient>>,
}

impl Log {
    /// Connect to the cluster through `bootstrap` (comma-separated `host:port` list).
    pub(crate) fn connect(bootstrap: &str) -> Result<Self, Error> {
        let brokers = bootstrap
            .split(',')
            .map(str::trim)
            .filter(|broker| !broker.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if brokers.is_empty() {
            return Err(Error::invalid("Kafka bootstrap servers must not be empty"));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| Error::storage(format!("start Kafka client runtime: {error}")))?;
        let builder = ClientBuilder::new(brokers)
            .client_id("scout")
            .max_message_size(MAX_MESSAGE_BYTES + 1024 * 1024)
            .backoff_config(BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(5),
                base: 2.0,
                deadline: Some(CALL_DEADLINE),
            });
        let client = block(&runtime, "connect to Kafka", builder.build())?;
        Ok(Self {
            runtime,
            client,
            partitions: HashMap::new(),
        })
    }

    /// The partition count of `topic`, or `None` when it does not exist.
    pub(crate) fn partition_count(&mut self, topic: &str) -> Result<Option<i32>, Error> {
        let topics = block(
            &self.runtime,
            "list Kafka topics",
            self.client.list_topics(),
        )?;
        Ok(topics
            .into_iter()
            .find(|candidate| candidate.name == topic)
            .map(|found| found.partitions.len() as i32))
    }

    /// Create `topic` with `partitions` partitions unless it exists; return its partition count.
    pub(crate) fn ensure_topic(&mut self, topic: &str, partitions: i32) -> Result<i32, Error> {
        if let Some(existing) = self.partition_count(topic)? {
            return Ok(existing);
        }
        let controller = self
            .client
            .controller_client()
            .map_err(|error| Error::storage(format!("reach the Kafka controller: {error}")))?;
        let created = block(
            &self.runtime,
            "create Kafka topic",
            controller.create_topic(topic, partitions, 1, 10_000),
        );
        // A concurrent creator (another relay) is not an error. A new topic can take a moment to
        // appear in the metadata the broker hands out, so look for it for a while.
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(count) = self.partition_count(topic)? {
                return Ok(count);
            }
            if std::time::Instant::now() >= deadline {
                return Err(created.err().unwrap_or_else(|| {
                    Error::storage(format!(
                        "Kafka topic {topic:?} did not appear after creation"
                    ))
                }));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Append `record` to the partition its key maps to; return (partition, offset).
    pub(crate) fn produce(
        &mut self,
        topic: &str,
        partitions: i32,
        record: LogRecord,
    ) -> Result<(i32, i64), Error> {
        let partition = partition_for(&record.key, partitions);
        let client = self.partition(topic, partition)?;
        let record = Record {
            key: Some(record.key),
            value: Some(record.value),
            headers: record.headers,
            timestamp: chrono::Utc::now(),
        };
        let offsets = block(
            &self.runtime,
            "produce to Kafka",
            client.produce(vec![record], Compression::NoCompression),
        )?;
        let offset = offsets
            .first()
            .copied()
            .ok_or_else(|| Error::storage("Kafka acknowledged a produce with no offset"))?;
        Ok((partition, offset))
    }

    /// Records of `topic`/`partition` from `offset` on (waiting up to `max_wait` when there are
    /// none yet), and the partition's high watermark.
    pub(crate) fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_wait: Duration,
    ) -> Result<(Vec<Fetched>, i64), Error> {
        let client = self.partition(topic, partition)?;
        let wait_ms = i32::try_from(max_wait.as_millis()).unwrap_or(i32::MAX);
        let max_bytes = i32::try_from(MAX_MESSAGE_BYTES + 1024 * 1024).unwrap_or(i32::MAX);
        let (records, high_watermark) = block(
            &self.runtime,
            "fetch from Kafka",
            client.fetch_records(offset, 1..max_bytes, wait_ms),
        )?;
        let fetched = records
            .into_iter()
            // A fetch returns the whole batch that holds `offset`; skip what precedes it.
            .filter(|fetched| fetched.offset >= offset)
            .map(|fetched| Fetched {
                offset: fetched.offset,
                record: LogRecord {
                    key: fetched.record.key.unwrap_or_default(),
                    value: fetched.record.value.unwrap_or_default(),
                    headers: fetched.record.headers,
                },
            })
            .collect();
        Ok((fetched, high_watermark))
    }

    /// The offset the next record of `topic`/`partition` will get.
    pub(crate) fn high_watermark(&mut self, topic: &str, partition: i32) -> Result<i64, Error> {
        let client = self.partition(topic, partition)?;
        block(
            &self.runtime,
            "read a Kafka high watermark",
            client.get_offset(OffsetAt::Latest),
        )
    }

    fn partition(&mut self, topic: &str, partition: i32) -> Result<Arc<PartitionClient>, Error> {
        let key = (topic.to_owned(), partition);
        if let Some(client) = self.partitions.get(&key) {
            return Ok(Arc::clone(client));
        }
        let client = Arc::new(block(
            &self.runtime,
            "open a Kafka partition",
            self.client
                .partition_client(topic, partition, UnknownTopicHandling::Retry),
        )?);
        self.partitions.insert(key, Arc::clone(&client));
        Ok(client)
    }
}

fn block<T, E: std::fmt::Display>(
    runtime: &Runtime,
    what: &str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, Error> {
    let limit = CALL_DEADLINE + Duration::from_secs(5);
    // The timer must be created inside the runtime.
    match runtime.block_on(async { tokio::time::timeout(limit, future).await }) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Error::storage(format!("{what}: {error}"))),
        Err(_) => Err(Error::storage(format!(
            "{what}: no answer within {}s",
            CALL_DEADLINE.as_secs() + 5
        ))),
    }
}

/// The partition Kafka's default partitioner picks for `key`: `toPositive(murmur2(key)) % n`, so
/// a Java or librdkafka producer (with the murmur2 partitioner) places the same key alike.
pub(crate) fn partition_for(key: &[u8], partitions: i32) -> i32 {
    debug_assert!(partitions > 0);
    ((murmur2(key) & 0x7fff_ffff) % partitions.max(1) as u32) as i32
}

/// MurmurHash2, 32-bit, seed 0x9747b28c: `org.apache.kafka.common.utils.Utils.murmur2`.
fn murmur2(data: &[u8]) -> u32 {
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let mut h: u32 = 0x9747_b28c ^ data.len() as u32;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let tail = chunks.remainder();
    if tail.len() >= 3 {
        h ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        h ^= u32::from(tail[0]);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur2_matches_kafka_reference_values() {
        // From Kafka's UtilsTest.testMurmur2 (signed Java ints).
        let cases: &[(&str, i32)] = &[
            ("21", -973_932_308),
            ("foobar", -790_332_482),
            ("a-little-bit-long-string", -985_981_536),
            ("a-little-bit-longer-string", -1_486_304_829),
            (
                "lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8",
                -58_897_971,
            ),
            ("abc", 479_470_107),
        ];
        for (input, expected) in cases {
            assert_eq!(
                murmur2(input.as_bytes()) as i32,
                *expected,
                "murmur2({input:?})"
            );
        }
    }

    #[test]
    fn partition_is_stable_and_in_range() {
        for partitions in [1, 3, 7] {
            for key in ["", "a", "http://fixture:8080/docs/", "run-0"] {
                let partition = partition_for(key.as_bytes(), partitions);
                assert!((0..partitions).contains(&partition));
                assert_eq!(partition, partition_for(key.as_bytes(), partitions));
            }
        }
    }
}
