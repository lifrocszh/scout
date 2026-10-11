use super::{Config, Crawler};
use crate::corpus::{
    IncompleteCrawl, IncompleteSpoolRun, SnapshotResult, SpoolMaterialization, materialize_spooled,
    write_incomplete, write_incomplete_spooled,
};
use crate::error::Error;
use crate::observability::{Events, emit_shared};
use crate::storage::now;
use serde_json::json;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub(crate) fn execute(
    data_dir: &Path,
    run_id: &str,
    config: &Config,
    events: Arc<Mutex<Events>>,
) -> Result<SnapshotResult, Error> {
    let crawl_dir = data_dir.join("crawls").join(run_id);
    fs::create_dir_all(crawl_dir.join("bodies"))
        .map_err(|error| Error::storage(format!("create crawl directory: {error}")))?;
    let started_at = now();
    let mut crawler = match Crawler::new(config, &crawl_dir, events.clone()) {
        Ok(crawler) => crawler,
        Err(error) => {
            write_incomplete(
                &crawl_dir,
                run_id,
                config,
                &started_at,
                IncompleteCrawl {
                    captures: &[],
                    rejected: 0,
                    failed: 1,
                },
                &error,
            );
            return Err(error);
        }
    };
    let crawl_result = match crawler.run() {
        Ok(result) => result,
        Err(error) => {
            write_incomplete_spooled(IncompleteSpoolRun {
                crawl_dir: &crawl_dir,
                run_id,
                config,
                started: &started_at,
                spool: crawler.spool(),
                rejected: crawler.rejected_count,
                failed: crawler.failed_count,
                error: &error,
            });
            return Err(error);
        }
    };
    let (max_active_global, max_active_by_origin) = crawler.concurrency_stats();
    emit_shared(
        &events,
        "crawl.concurrency",
        "success",
        json!({
            "max_active_global": max_active_global,
            "max_active_by_origin": max_active_by_origin,
            "configured_global": config.global_concurrency,
            "configured_origin": config.origin_concurrency,
        }),
    );
    let completed_at = now();
    materialize_spooled(
        SpoolMaterialization {
            data_dir,
            crawl_dir: &crawl_dir,
            run_id,
            config,
            started: &started_at,
            completed: &completed_at,
            spool: crawler.spool(),
        },
        crawl_result,
    )
}
