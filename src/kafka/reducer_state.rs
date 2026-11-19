//! The reducer's own durable state, beside its capture spool: per-partition next offsets, the
//! result events already applied, the run events seen, and the snapshot it sealed.
//!
//! Offsets live here rather than in a Kafka consumer group so that "applied" and "offset
//! advanced" are recorded in one SQLite transaction. The effect itself (body file and spool row)
//! happens just before that transaction and is idempotent, so a crash in between only replays it.
use crate::error::Error;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::time::Duration;

pub(super) struct ReducerState {
    connection: Connection,
}

/// Where a record sits in the log.
#[derive(Clone, Copy)]
pub(super) struct Position<'a> {
    pub(super) topic: &'a str,
    pub(super) partition: i32,
    pub(super) offset: i64,
}

impl ReducerState {
    pub(super) fn open(data_dir: &Path) -> Result<Self, Error> {
        std::fs::create_dir_all(data_dir)
            .map_err(|error| Error::storage(format!("create reducer data directory: {error}")))?;
        let connection = Connection::open(data_dir.join("kafka-reducer.sqlite3"))
            .map_err(|error| Error::storage(format!("open reducer state: {error}")))?;
        connection
            .busy_timeout(Duration::from_secs(30))
            .map_err(|error| Error::storage(format!("configure reducer state: {error}")))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 CREATE TABLE IF NOT EXISTS offsets (
                     topic TEXT NOT NULL,
                     partition INTEGER NOT NULL,
                     next_offset INTEGER NOT NULL,
                     PRIMARY KEY (topic, partition)
                 ) WITHOUT ROWID;
                 CREATE TABLE IF NOT EXISTS applied_results (
                     event_id TEXT PRIMARY KEY,
                     crawl_run_id TEXT NOT NULL,
                     task_id INTEGER NOT NULL
                 ) WITHOUT ROWID;
                 CREATE TABLE IF NOT EXISTS run_events (
                     event_id TEXT PRIMARY KEY,
                     crawl_run_id TEXT NOT NULL,
                     event_type TEXT NOT NULL,
                     payload_json TEXT NOT NULL
                 ) WITHOUT ROWID;
                 CREATE INDEX IF NOT EXISTS run_events_by_type
                     ON run_events(crawl_run_id, event_type);
                 CREATE TABLE IF NOT EXISTS snapshots (
                     crawl_run_id TEXT PRIMARY KEY,
                     snapshot_id TEXT NOT NULL,
                     page_count INTEGER NOT NULL,
                     alias_count INTEGER NOT NULL
                 ) WITHOUT ROWID;",
            )
            .map_err(|error| Error::storage(format!("initialize reducer state: {error}")))?;
        Ok(Self { connection })
    }

    pub(super) fn next_offset(&self, topic: &str, partition: i32) -> Result<i64, Error> {
        self.connection
            .query_row(
                "SELECT next_offset FROM offsets WHERE topic = ?1 AND partition = ?2",
                params![topic, partition],
                |row| row.get(0),
            )
            .optional()
            .map(Option::unwrap_or_default)
            .map_err(|error| Error::storage(format!("read reducer offset: {error}")))
    }

    pub(super) fn is_applied(&self, event_id: &str) -> Result<bool, Error> {
        self.connection
            .query_row(
                "SELECT 1 FROM applied_results WHERE event_id = ?1",
                [event_id],
                |_| Ok(()),
            )
            .optional()
            .map(|found| found.is_some())
            .map_err(|error| Error::storage(format!("read applied results: {error}")))
    }

    /// Move past a record without recording anything else (another run's, or a duplicate).
    pub(super) fn skip(&mut self, at: Position<'_>) -> Result<(), Error> {
        let tx = self.transaction()?;
        advance(&tx, at)?;
        commit(tx)
    }

    /// Record an applied result and move past it, atomically.
    pub(super) fn record_result(
        &mut self,
        at: Position<'_>,
        event_id: &str,
        run_id: &str,
        task_id: i64,
    ) -> Result<(), Error> {
        let tx = self.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO applied_results(event_id, crawl_run_id, task_id)
             VALUES (?1, ?2, ?3)",
            params![event_id, run_id, task_id],
        )
        .map_err(|error| Error::storage(format!("record applied result: {error}")))?;
        advance(&tx, at)?;
        commit(tx)
    }

    /// Record a run event and move past it; `false` when the event was already recorded.
    pub(super) fn record_event(
        &mut self,
        at: Position<'_>,
        event_id: &str,
        run_id: &str,
        event_type: &str,
        payload_json: &str,
    ) -> Result<bool, Error> {
        let tx = self.transaction()?;
        let inserted = tx
            .execute(
                "INSERT OR IGNORE INTO run_events(event_id, crawl_run_id, event_type, payload_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![event_id, run_id, event_type, payload_json],
            )
            .map_err(|error| Error::storage(format!("record run event: {error}")))?
            > 0;
        if !inserted {
            let stored: String = tx
                .query_row(
                    "SELECT payload_json FROM run_events WHERE event_id = ?1",
                    [event_id],
                    |row| row.get(0),
                )
                .map_err(|error| Error::storage(format!("read run event: {error}")))?;
            if stored != payload_json {
                return Err(Error::storage(format!(
                    "event {event_id:?} was delivered twice with different payloads"
                )));
            }
        }
        advance(&tx, at)?;
        commit(tx)?;
        Ok(inserted)
    }

    /// Payloads of the run's events of one type, in event ID order.
    pub(super) fn run_events(&self, run_id: &str, event_type: &str) -> Result<Vec<String>, Error> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT payload_json FROM run_events
                 WHERE crawl_run_id = ?1 AND event_type = ?2 ORDER BY event_id",
            )
            .map_err(|error| Error::storage(format!("query run events: {error}")))?;
        let rows = statement
            .query_map(params![run_id, event_type], |row| row.get(0))
            .map_err(|error| Error::storage(format!("read run events: {error}")))?;
        rows.collect::<Result<Vec<String>, _>>()
            .map_err(|error| Error::storage(format!("read run event: {error}")))
    }

    /// `capture_committed` events of the run whose result has not been applied yet.
    pub(super) fn missing_results(&self, run_id: &str) -> Result<i64, Error> {
        self.count(
            "SELECT count(*) FROM run_events AS event
             WHERE event.crawl_run_id = ?1 AND event.event_type = 'capture_committed'
               AND NOT EXISTS (
                   SELECT 1 FROM applied_results AS result WHERE result.event_id = event.event_id
               )",
            run_id,
        )
    }

    pub(super) fn applied_results(&self, run_id: &str) -> Result<i64, Error> {
        self.count(
            "SELECT count(*) FROM applied_results WHERE crawl_run_id = ?1",
            run_id,
        )
    }

    pub(super) fn snapshot(&self, run_id: &str) -> Result<Option<(String, usize, usize)>, Error> {
        self.connection
            .query_row(
                "SELECT snapshot_id, page_count, alias_count FROM snapshots WHERE crawl_run_id = ?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| Error::storage(format!("read reducer snapshot: {error}")))
    }

    pub(super) fn set_snapshot(
        &mut self,
        run_id: &str,
        snapshot_id: &str,
        pages: usize,
        aliases: usize,
    ) -> Result<(), Error> {
        self.connection
            .execute(
                "INSERT INTO snapshots(crawl_run_id, snapshot_id, page_count, alias_count)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (crawl_run_id) DO UPDATE SET snapshot_id = excluded.snapshot_id,
                     page_count = excluded.page_count, alias_count = excluded.alias_count",
                params![run_id, snapshot_id, pages as i64, aliases as i64],
            )
            .map_err(|error| Error::storage(format!("record reducer snapshot: {error}")))?;
        Ok(())
    }

    fn count(&self, sql: &str, run_id: &str) -> Result<i64, Error> {
        self.connection
            .query_row(sql, [run_id], |row| row.get(0))
            .map_err(|error| Error::storage(format!("count reducer state: {error}")))
    }

    fn transaction(&mut self) -> Result<rusqlite::Transaction<'_>, Error> {
        self.connection
            .transaction()
            .map_err(|error| Error::storage(format!("begin reducer state update: {error}")))
    }
}

fn advance(tx: &rusqlite::Transaction<'_>, at: Position<'_>) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO offsets(topic, partition, next_offset) VALUES (?1, ?2, ?3)
         ON CONFLICT (topic, partition) DO UPDATE
         SET next_offset = max(next_offset, excluded.next_offset)",
        params![at.topic, at.partition, at.offset + 1],
    )
    .map_err(|error| Error::storage(format!("advance reducer offset: {error}")))?;
    Ok(())
}

fn commit(tx: rusqlite::Transaction<'_>) -> Result<(), Error> {
    tx.commit()
        .map_err(|error| Error::storage(format!("commit reducer state: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "scout-reducer-state-{name}-{}-{}",
            std::process::id(),
            crate::storage::new_run_id()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn offsets_only_move_forward_and_results_are_recorded_once() {
        let dir = temp_dir("offsets");
        let mut state = ReducerState::open(&dir).expect("open");
        let at = |offset| Position {
            topic: "t",
            partition: 1,
            offset,
        };
        assert_eq!(state.next_offset("t", 1).unwrap(), 0);
        state.record_result(at(4), "e1", "run-a", 1).unwrap();
        assert_eq!(state.next_offset("t", 1).unwrap(), 5);
        // A replay of an earlier offset never moves the offset back.
        state.record_result(at(2), "e1", "run-a", 1).unwrap();
        assert_eq!(state.next_offset("t", 1).unwrap(), 5);
        assert_eq!(state.applied_results("run-a").unwrap(), 1);
        assert!(state.is_applied("e1").unwrap());

        drop(state);
        let state = ReducerState::open(&dir).expect("reopen");
        assert_eq!(state.next_offset("t", 1).unwrap(), 5);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn run_events_deduplicate_and_track_missing_results() {
        let dir = temp_dir("events");
        let mut state = ReducerState::open(&dir).expect("open");
        let at = |offset| Position {
            topic: "events",
            partition: 0,
            offset,
        };
        assert!(
            state
                .record_event(at(0), "c1", "run-a", "capture_committed", "{}")
                .unwrap()
        );
        assert!(
            !state
                .record_event(at(1), "c1", "run-a", "capture_committed", "{}")
                .unwrap()
        );
        assert!(
            state
                .record_event(at(2), "c1", "run-a", "capture_committed", "{\"x\":1}")
                .is_err()
        );
        assert_eq!(state.missing_results("run-a").unwrap(), 1);
        state
            .record_result(
                Position {
                    topic: "results",
                    partition: 0,
                    offset: 0,
                },
                "c1",
                "run-a",
                9,
            )
            .unwrap();
        assert_eq!(state.missing_results("run-a").unwrap(), 0);
        let _ = std::fs::remove_dir_all(dir);
    }
}
