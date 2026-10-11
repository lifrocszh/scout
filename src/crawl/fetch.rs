mod helpers;
mod request;
mod worker;

use super::spool::CaptureSpool;
use super::{Extracted, FetchOutcome};
use crate::corpus::capture_selection_key;
use crate::crawl::{Capture, Config, CrawlResult};
use crate::error::Error;
use crate::observability::Events;
use crate::storage::{sha256_hex, write_if_absent};
use ::url::Url;
use reqwest::blocking::Client;
use reqwest::redirect::Policy;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use self::helpers::owner_for;
pub(crate) use self::helpers::{
    extract_sitemap_locations, is_sitemap_index, normalize_sitemap_url, page_id, parse_robots,
    schedule_key, sitemap_url_allowed, sources_overlap,
};
use self::request::RequestCoordinator;
use self::worker::{CrawlWorker, PageResult};

const MAX_SITEMAP_DEPTH: usize = 5;
const MAX_SITEMAP_INDEX_LOCATIONS: usize = 100_000;

pub(crate) struct Crawler<'a> {
    client: Client,
    config: &'a Config,
    crawl_dir: &'a Path,
    events: Arc<Mutex<Events>>,
    coordinator: Arc<RequestCoordinator>,
    frontier: BTreeSet<String>,
    frontier_by_schedule: BTreeMap<String, VecDeque<String>>,
    last_schedule_key: Option<String>,
    sitemap_origins_seen: HashSet<String>,
    spool: CaptureSpool,
    unique_content_group_count: usize,
    unique_groups_by_source: HashMap<String, usize>,
    pub(crate) rejected_count: usize,
    pub(crate) failed_count: usize,
    started_at: Instant,
}

impl<'a> Crawler<'a> {
    pub(crate) fn new(
        config: &'a Config,
        crawl_dir: &'a Path,
        events: Arc<Mutex<Events>>,
    ) -> Result<Self, Error> {
        let spool = CaptureSpool::open(crawl_dir, config.max_frontier)?;
        let client = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(config.timeout_seconds))
            .user_agent(config.user_agent.clone())
            .build()
            .map_err(|e| Error::storage(format!("build HTTP client: {e}")))?;
        let mut crawler = Self {
            client,
            config,
            crawl_dir,
            events,
            coordinator: Arc::new(RequestCoordinator::new(config)),
            frontier: BTreeSet::new(),
            frontier_by_schedule: BTreeMap::new(),
            last_schedule_key: None,
            sitemap_origins_seen: HashSet::new(),
            spool,
            unique_content_group_count: 0,
            unique_groups_by_source: HashMap::new(),
            rejected_count: 0,
            failed_count: 0,
            started_at: Instant::now(),
        };
        for seed in config.sources.iter().flat_map(|source| source.seeds.iter()) {
            crawler.enqueue_frontier(seed.clone());
        }
        Ok(crawler)
    }

    pub(crate) fn run(&mut self) -> Result<CrawlResult, Error> {
        if self.frontier.len() > self.config.max_frontier {
            return Err(Error::safety(format!(
                "frontier exceeded {}",
                self.config.max_frontier
            )));
        }
        while !self.frontier.is_empty() && !self.completion_reached() {
            if self.started_at.elapsed() > Duration::from_secs(self.config.max_duration_seconds) {
                return Err(Error::safety("crawl duration limit exceeded"));
            }
            let worker_count = self
                .config
                .global_concurrency
                .min(self.frontier.len())
                .max(1);
            let mut jobs = Vec::with_capacity(worker_count);
            while jobs.len() < worker_count && !self.completion_reached() {
                let Some(url) = self.pop_frontier() else {
                    break;
                };
                if !self.spool.mark_visited(&url)? {
                    continue;
                }
                jobs.push(url);
            }
            if jobs.is_empty() {
                break;
            }
            let results = thread::scope(|scope| {
                let mut handles = Vec::with_capacity(jobs.len());
                for url in jobs {
                    let worker = self.worker();
                    handles.push(scope.spawn(move || worker.process(url)));
                }
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| Error::storage("crawl worker panicked"))
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            for result in results {
                self.apply_result(result)?;
            }
        }
        let stop_reason = if self.completion_reached() {
            "page_target_reached"
        } else {
            "frontier_exhausted"
        };
        self.frontier = BTreeSet::new();
        self.frontier_by_schedule = BTreeMap::new();
        self.last_schedule_key = None;
        self.sitemap_origins_seen = HashSet::new();
        self.unique_groups_by_source = HashMap::new();
        Ok(CrawlResult {
            captures: Vec::new(),
            rejected_count: self.rejected_count,
            failed_count: self.failed_count,
            stop_reason: stop_reason.into(),
        })
    }

    fn worker(&self) -> CrawlWorker<'a> {
        CrawlWorker::new(
            self.client.clone(),
            self.config,
            self.events.clone(),
            self.coordinator.clone(),
            self.spool.clone(),
        )
    }

    fn apply_result(&mut self, result: PageResult) -> Result<(), Error> {
        let PageResult { url, outcome } = result;
        self.enqueue_cached_sitemaps()?;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.failed_count += 1;
                self.emit(
                    "fetch.failed",
                    "failure",
                    json!({"url": url, "code": error.code, "message": error.message}),
                );
                if error.code != "fetch_failed" {
                    return Err(error);
                }
                return Ok(());
            }
        };
        match outcome {
            FetchOutcome::Rejected(reason) => {
                self.rejected_count += 1;
                self.emit(
                    "page.rejected",
                    "success",
                    json!({"url": url, "reason": reason}),
                );
            }
            FetchOutcome::Admitted {
                source_id,
                final_url,
                redirects,
                status_code,
                content_type,
                body,
                extracted,
            } => {
                let body_hash = sha256_hex(&body);
                let body_file = format!("bodies/{body_hash}");
                write_if_absent(&self.crawl_dir.join(&body_file), &body)?;
                let id = page_id(&final_url);
                let Extracted {
                    title,
                    description,
                    language,
                    canonical_url,
                    extraction_method,
                    content,
                    identity,
                    content_hash,
                    blocks,
                    links,
                } = *extracted;
                let capture = Capture {
                    requested_url: url,
                    final_url,
                    redirects,
                    source_id,
                    title,
                    description,
                    language,
                    canonical_url,
                    extraction_method,
                    content,
                    identity,
                    content_hash,
                    blocks,
                    content_bytes: body.len(),
                    body_hash,
                    body_file,
                    content_type,
                    status_code,
                };
                let selection_key = capture_selection_key(&capture);
                if self.spool.insert_capture(&capture, &selection_key)? {
                    self.unique_content_group_count += 1;
                    *self
                        .unique_groups_by_source
                        .entry(capture.source_id.clone())
                        .or_default() += 1;
                }
                self.emit("fetch.completed", "success", json!({"url": capture.requested_url, "final_url": capture.final_url, "status_code": capture.status_code, "content_bytes": capture.content_bytes, "body_sha256": capture.body_hash, "user_agent": self.config.user_agent}));
                self.emit("page.admitted", "success", json!({"page_id": id, "source_id": capture.source_id, "final_url": capture.final_url, "content_sha256": capture.content_hash}));
                for link in links {
                    if owner_for(self.config, &link).is_some() && !self.spool.is_visited(&link)? {
                        if !self.frontier.contains(&link)
                            && self.frontier.len() >= self.config.max_frontier
                        {
                            return Err(Error::safety(format!(
                                "frontier exceeded {}",
                                self.config.max_frontier
                            )));
                        }
                        self.enqueue_frontier(link);
                    }
                }
            }
        }
        Ok(())
    }

    fn completion_reached(&self) -> bool {
        if self.unique_content_group_count < self.config.page_target {
            return false;
        }
        self.config.sources.iter().all(|source| {
            source.minimum_page_quota == 0
                || self
                    .unique_groups_by_source
                    .get(&source.source_id)
                    .copied()
                    .unwrap_or_default()
                    >= source.minimum_page_quota
        })
    }

    fn pop_frontier(&mut self) -> Option<String> {
        let keys = self
            .frontier_by_schedule
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return None;
        }
        let start = self
            .last_schedule_key
            .as_ref()
            .and_then(|last| keys.iter().position(|key| key == last))
            .map(|index| (index + 1) % keys.len())
            .unwrap_or(0);
        let (schedule_key, url) = (0..keys.len()).find_map(|offset| {
            let index = (start + offset) % keys.len();
            let key = &keys[index];
            self.frontier_by_schedule
                .get_mut(key)
                .and_then(VecDeque::pop_front)
                .map(|url| (key.clone(), url))
        })?;
        self.last_schedule_key = Some(schedule_key.clone());
        if self
            .frontier_by_schedule
            .get(&schedule_key)
            .is_some_and(VecDeque::is_empty)
        {
            self.frontier_by_schedule.remove(&schedule_key);
        }
        let removed = self.frontier.remove(&url);
        debug_assert!(removed);
        Some(url)
    }

    fn enqueue_frontier(&mut self, url: String) {
        if !self.frontier.insert(url.clone()) {
            return;
        }
        let parsed = Url::parse(&url).expect("normalized frontier URL parses");
        let key = schedule_key(self.config, &parsed);
        self.frontier_by_schedule
            .entry(key)
            .or_default()
            .push_back(url);
    }

    fn enqueue_cached_sitemaps(&mut self) -> Result<(), Error> {
        let pending = self
            .coordinator
            .sitemap_page_counts()
            .into_iter()
            .filter(|(origin, _)| !self.sitemap_origins_seen.contains(origin))
            .collect::<Vec<_>>();
        for (origin, candidate_count) in pending {
            self.sitemap_origins_seen.insert(origin.clone());
            let mut enqueued_count = 0;
            for url in self.spool.sitemap_pages(&origin)? {
                if owner_for(self.config, &url).is_none()
                    || self.spool.is_visited(&url)?
                    || self.frontier.contains(&url)
                {
                    continue;
                }
                if self.frontier.len() >= self.config.max_frontier {
                    return Err(Error::safety(format!(
                        "frontier exceeded {}",
                        self.config.max_frontier
                    )));
                }
                self.enqueue_frontier(url);
                enqueued_count += 1;
            }
            if candidate_count > 0 {
                self.emit(
                    "sitemap.discovered",
                    "success",
                    json!({
                        "origin": origin,
                        "candidate_count": candidate_count,
                        "enqueued_count": enqueued_count,
                    }),
                );
            }
        }
        Ok(())
    }

    fn emit(&self, event: &str, outcome: &'static str, data: Value) {
        self.events
            .lock()
            .expect("crawl event log lock")
            .emit(event, outcome, data);
    }

    pub(crate) fn concurrency_stats(&self) -> (usize, HashMap<String, usize>) {
        self.coordinator.stats()
    }

    pub(crate) fn spool(&self) -> &CaptureSpool {
        &self.spool
    }
}
