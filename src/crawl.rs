use super::corpus::capture_selection_key;
use super::extract::extract;
use super::frontier::{RobotsGroup, RobotsPolicy, RobotsRule};
use super::observability::Events;
use super::spool::CaptureSpool;
use super::storage::{read_bounded, sha256_hex, write_if_absent};
use super::url::{normalize_url, origin_key};
use super::{
    Arc, BTreeMap, BTreeSet, CONTENT_LENGTH, CONTENT_TYPE, Capture, Client, Condvar, Config,
    CrawlResult, Duration, Error, Extracted, FetchOutcome, HashMap, HashSet, Instant, LOCATION,
    MAX_REDIRECTS, MAX_ROBOTS_BYTES, Mutex, Path, Policy, RETRY_AFTER, Source, Url, Value,
    VecDeque, json, thread,
};

const MAX_SITEMAP_DEPTH: usize = 5;
const MAX_SITEMAP_INDEX_LOCATIONS: usize = 100_000;

struct CachedRobots {
    rules: RobotsPolicy,
    fetched_at: Instant,
    body_sha256: String,
    sitemap_page_count: usize,
}

#[derive(Default)]
struct RequestState {
    next_start_by_key: HashMap<String, Instant>,
    total_attempts: usize,
    active_requests: usize,
    active_by_key: HashMap<String, usize>,
    active_by_origin: HashMap<String, usize>,
    max_active_requests: usize,
    max_active_by_origin: HashMap<String, usize>,
}

#[derive(Clone, Copy)]
struct RequestConcurrency {
    active_global: usize,
    active_origin: usize,
    peak_global: usize,
    peak_origin: usize,
}

struct RequestCoordinator {
    state: Mutex<RequestState>,
    wake: Condvar,
    robots: Mutex<HashMap<String, CachedRobots>>,
    robots_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    global_concurrency: usize,
    origin_concurrency: usize,
    min_start_spacing: Duration,
    max_total_attempts: usize,
    deadline: Instant,
}

impl RequestCoordinator {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            state: Mutex::new(RequestState::default()),
            wake: Condvar::new(),
            robots: Mutex::new(HashMap::new()),
            robots_locks: Mutex::new(HashMap::new()),
            global_concurrency: config.global_concurrency,
            origin_concurrency: config.origin_concurrency,
            min_start_spacing: Duration::from_millis(config.min_start_spacing_ms),
            max_total_attempts: config.max_total_attempts,
            deadline: Instant::now() + Duration::from_secs(config.max_duration_seconds),
        }
    }

    fn robots_lock(&self, origin: &str) -> Arc<Mutex<()>> {
        self.robots_locks
            .lock()
            .expect("robots lock map")
            .entry(origin.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn start_request(self: &Arc<Self>, key: &str, origin: &str) -> Result<RequestPermit, Error> {
        let mut state = self.state.lock().expect("request coordinator lock");
        loop {
            if state.total_attempts >= self.max_total_attempts {
                return Err(Error::safety("request attempt limit exceeded"));
            }
            let now = Instant::now();
            if now >= self.deadline {
                return Err(Error::safety("crawl duration limit exceeded"));
            }
            let next_start = state.next_start_by_key.get(key).copied().unwrap_or(now);
            let active_for_key = state.active_by_key.get(key).copied().unwrap_or(0);
            if state.active_requests < self.global_concurrency
                && active_for_key < self.origin_concurrency
                && next_start <= now
            {
                state.total_attempts += 1;
                state.active_requests += 1;
                *state.active_by_key.entry(key.to_owned()).or_default() += 1;
                *state.active_by_origin.entry(origin.to_owned()).or_default() += 1;
                state.max_active_requests = state.max_active_requests.max(state.active_requests);
                let active_for_origin = state.active_by_origin.get(origin).copied().unwrap_or(0);
                state
                    .max_active_by_origin
                    .entry(origin.to_owned())
                    .and_modify(|maximum| *maximum = (*maximum).max(active_for_origin))
                    .or_insert(active_for_origin);
                state
                    .next_start_by_key
                    .insert(key.to_owned(), now + self.min_start_spacing);
                let active_origin = state.active_by_origin.get(origin).copied().unwrap_or(0);
                let peak_origin = state
                    .max_active_by_origin
                    .get(origin)
                    .copied()
                    .unwrap_or(active_origin);
                return Ok(RequestPermit {
                    coordinator: self.clone(),
                    key: key.to_owned(),
                    origin: origin.to_owned(),
                    concurrency: RequestConcurrency {
                        active_global: state.active_requests,
                        active_origin,
                        peak_global: state.max_active_requests,
                        peak_origin,
                    },
                });
            }
            let wait_for_spacing = next_start.saturating_duration_since(now);
            let wait_for = if wait_for_spacing.is_zero() {
                Duration::from_millis(10)
            } else {
                wait_for_spacing.min(Duration::from_millis(50))
            };
            state = self
                .wake
                .wait_timeout(state, wait_for)
                .expect("request coordinator wait")
                .0;
        }
    }

    fn release(&self, key: &str, origin: &str) {
        let mut state = self.state.lock().expect("request coordinator lock");
        state.active_requests = state.active_requests.saturating_sub(1);
        let remove_key = state
            .active_by_key
            .get_mut(key)
            .map(|active| {
                *active = active.saturating_sub(1);
                *active == 0
            })
            .unwrap_or(false);
        if remove_key {
            state.active_by_key.remove(key);
        }
        let remove_origin = state
            .active_by_origin
            .get_mut(origin)
            .map(|active| {
                *active = active.saturating_sub(1);
                *active == 0
            })
            .unwrap_or(false);
        if remove_origin {
            state.active_by_origin.remove(origin);
        }
        drop(state);
        self.wake.notify_all();
    }

    fn mark_started(&self, key: &str) {
        let mut state = self.state.lock().expect("request coordinator lock");
        state
            .next_start_by_key
            .insert(key.to_owned(), Instant::now() + self.min_start_spacing);
    }

    fn stats(&self) -> (usize, HashMap<String, usize>) {
        let state = self.state.lock().expect("request coordinator lock");
        (
            state.max_active_requests,
            state.max_active_by_origin.clone(),
        )
    }
}

struct RequestPermit {
    coordinator: Arc<RequestCoordinator>,
    key: String,
    origin: String,
    concurrency: RequestConcurrency,
}

impl RequestPermit {
    fn mark_started(&self) {
        self.coordinator.mark_started(&self.key);
    }
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.coordinator.release(&self.key, &self.origin);
    }
}

struct RequestAttempt {
    url: String,
    origin: String,
    request_kind: &'static str,
    attempt: usize,
    started_at: Instant,
    concurrency: RequestConcurrency,
}

impl RequestAttempt {
    fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    fn emit(
        &self,
        events: &Arc<Mutex<Events>>,
        request_elapsed_ms: u64,
        status_code: Option<u16>,
        transport_error: Option<&str>,
        response_body_error: Option<&str>,
        timed_out: bool,
    ) {
        let outcome = if transport_error.is_some()
            || response_body_error.is_some()
            || status_code.is_some_and(|status| status >= 400)
        {
            "failure"
        } else {
            "success"
        };
        events.lock().expect("crawl event log lock").emit(
            "fetch.attempt",
            outcome,
            json!({
                "schema_version": "scout.crawl-attempt.v1",
                "request_kind": self.request_kind,
                "url": self.url,
                "origin": self.origin,
                "attempt": self.attempt,
                "request_elapsed_ms": request_elapsed_ms,
                "request_elapsed_scope": "client_send_start_to_response_drop_or_transport_error",
                "active_requests_global_at_start": self.concurrency.active_global,
                "active_requests_origin_at_start": self.concurrency.active_origin,
                "peak_active_requests_global_at_start": self.concurrency.peak_global,
                "peak_active_requests_origin_at_start": self.concurrency.peak_origin,
                "active_request_scope": "request_permit_through_response_drop_including_body_read",
                "status_code": status_code,
                "retryable_status": status_code.is_some_and(retryable_status),
                "transport_error": transport_error,
                "response_body_error": response_body_error,
                "timed_out": timed_out,
            }),
        );
    }
}

struct RequestResponse {
    response: Option<reqwest::blocking::Response>,
    _permit: Option<RequestPermit>,
    attempt: RequestAttempt,
    events: Arc<Mutex<Events>>,
    status_code: u16,
    response_body_error: Option<String>,
    timed_out: bool,
}

impl RequestResponse {
    fn read_bounded(&mut self, limit: usize) -> Result<Option<Vec<u8>>, Error> {
        let response = self.response.take().expect("response body read once");
        let result = read_bounded(response, limit);
        if let Err(error) = &result {
            let message = error.message.to_ascii_lowercase();
            self.timed_out = message.contains("timed out") || message.contains("timeout");
            self.response_body_error = Some(error.message.clone());
        }
        result
    }
}

impl std::ops::Deref for RequestResponse {
    type Target = reqwest::blocking::Response;

    fn deref(&self) -> &Self::Target {
        self.response.as_ref().expect("response body already read")
    }
}

impl Drop for RequestResponse {
    fn drop(&mut self) {
        drop(self.response.take());
        let request_elapsed_ms = self.attempt.elapsed_ms();
        drop(self._permit.take());
        self.attempt.emit(
            &self.events,
            request_elapsed_ms,
            Some(self.status_code),
            None,
            self.response_body_error.as_deref(),
            self.timed_out,
        );
    }
}

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
        CrawlWorker {
            client: self.client.clone(),
            config: self.config,
            events: self.events.clone(),
            coordinator: self.coordinator.clone(),
            spool: self.spool.clone(),
        }
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
        let pending = {
            let robots = self.coordinator.robots.lock().expect("robots cache lock");
            robots
                .iter()
                .filter(|(origin, _)| !self.sitemap_origins_seen.contains(*origin))
                .map(|(origin, cached)| (origin.clone(), cached.sitemap_page_count))
                .collect::<Vec<_>>()
        };
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

struct PageResult {
    url: String,
    outcome: Result<FetchOutcome, Error>,
}

struct CrawlWorker<'a> {
    client: Client,
    config: &'a Config,
    events: Arc<Mutex<Events>>,
    coordinator: Arc<RequestCoordinator>,
    spool: CaptureSpool,
}

impl<'a> CrawlWorker<'a> {
    fn process(&self, url: String) -> PageResult {
        let outcome = match self.ensure_robots(&url) {
            Ok(true) => self.fetch(&url),
            Ok(false) => Ok(FetchOutcome::Rejected("robots_denied".into())),
            Err(error) => Err(error),
        };
        PageResult { url, outcome }
    }

    fn emit(&self, event: &str, outcome: &'static str, data: Value) {
        self.events
            .lock()
            .expect("crawl event log lock")
            .emit(event, outcome, data);
    }

    fn ensure_robots(&self, page_url: &str) -> Result<bool, Error> {
        let parsed = Url::parse(page_url).map_err(|e| Error::storage(format!("parse URL: {e}")))?;
        let origin = origin_key(&parsed);
        if let Some(cached) = self
            .coordinator
            .robots
            .lock()
            .expect("robots cache lock")
            .get(&origin)
            && cached.fetched_at.elapsed() <= Duration::from_secs(self.config.robots_cache_seconds)
        {
            return Ok(cached.rules.allows(parsed.path()));
        }
        let robots_lock = self.coordinator.robots_lock(&origin);
        let _robots_guard = robots_lock.lock().expect("robots origin lock");
        if let Some(cached) = self
            .coordinator
            .robots
            .lock()
            .expect("robots cache lock")
            .get(&origin)
            && cached.fetched_at.elapsed() <= Duration::from_secs(self.config.robots_cache_seconds)
        {
            return Ok(cached.rules.allows(parsed.path()));
        }
        let mut current = Url::parse(&format!("{origin}/robots.txt")).expect("robots URL parses");
        for _ in 0..=5 {
            let mut response = match self.request(&current, "robots") {
                Ok(response) => response,
                Err(error) => {
                    if let Some(cached) = self
                        .coordinator
                        .robots
                        .lock()
                        .expect("robots cache lock")
                        .get(&origin)
                        && cached.fetched_at.elapsed()
                            <= Duration::from_secs(self.config.robots_cache_seconds)
                    {
                        self.emit("robots.cache_used", "success", json!({"origin": origin, "body_sha256": cached.body_sha256, "reason": error.code}));
                        return Ok(cached.rules.allows(parsed.path()));
                    }
                    if error.code == "safety_cap" {
                        return Err(error);
                    }
                    self.emit(
                        "robots.unavailable",
                        "failure",
                        json!({"origin": origin, "reason": error.message, "policy": "deny"}),
                    );
                    return Ok(false);
                }
            };
            let status = response.status().as_u16();
            if (400..500).contains(&status) {
                self.coordinator
                    .robots
                    .lock()
                    .expect("robots cache lock")
                    .insert(
                        origin.clone(),
                        CachedRobots {
                            rules: RobotsPolicy::default(),
                            fetched_at: Instant::now(),
                            body_sha256: sha256_hex(&[]),
                            sitemap_page_count: 0,
                        },
                    );
                return Ok(true);
            }
            if response.status().is_redirection() {
                let Some(location) = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                else {
                    self.emit(
                        "robots.unavailable",
                        "failure",
                        json!({"origin": origin, "reason": "redirect_missing_location", "policy": "deny"}),
                    );
                    return Ok(false);
                };
                current = current
                    .join(location)
                    .map_err(|_| Error::fetch("invalid robots redirect location"))?;
                continue;
            }
            if !response.status().is_success() {
                self.emit(
                    "robots.unavailable",
                    "failure",
                    json!({"origin": origin, "reason": format!("HTTP {status}"), "policy": "deny"}),
                );
                return Ok(false);
            }
            let Some(body) = (match response.read_bounded(MAX_ROBOTS_BYTES) {
                Ok(body) => body,
                Err(error) if error.code == "safety_cap" => return Err(error),
                Err(error) => {
                    self.emit(
                        "robots.unavailable",
                        "failure",
                        json!({"origin": origin, "reason": error.message, "policy": "deny"}),
                    );
                    return Ok(false);
                }
            }) else {
                self.emit(
                    "robots.unavailable",
                    "failure",
                    json!({"origin": origin, "reason": format!("body exceeds {MAX_ROBOTS_BYTES} bytes"), "policy": "deny"}),
                );
                return Ok(false);
            };
            drop(response);
            let body_sha256 = sha256_hex(&body);
            let rules = parse_robots(&body);
            let sitemap_page_count = self.expand_sitemaps(&origin, &rules.sitemaps)?;
            let allowed = rules.allows(parsed.path());
            self.coordinator
                .robots
                .lock()
                .expect("robots cache lock")
                .insert(
                    origin,
                    CachedRobots {
                        rules,
                        fetched_at: Instant::now(),
                        body_sha256,
                        sitemap_page_count,
                    },
                );
            return Ok(allowed);
        }
        self.emit(
            "robots.unavailable",
            "failure",
            json!({"origin": origin, "reason": "redirect_limit_exceeded", "policy": "deny"}),
        );
        Ok(false)
    }

    fn expand_sitemaps(&self, origin: &str, roots: &[String]) -> Result<usize, Error> {
        let mut pending = roots
            .iter()
            .filter_map(|url| {
                let parsed = Url::parse(url).ok()?;
                (origin_key(&parsed) == origin && sitemap_url_allowed(self.config, origin, url))
                    .then(|| (url.clone(), 0))
            })
            .collect::<VecDeque<_>>();
        let mut seen_sitemaps = HashSet::new();
        let mut page_count = self.spool.sitemap_page_count(origin)?;
        let mut index_locations_seen = pending.len();
        if index_locations_seen > MAX_SITEMAP_INDEX_LOCATIONS {
            return Err(Error::safety("sitemap index location cap exceeded"));
        }

        while let Some((sitemap_url, depth)) = pending.pop_front() {
            if !seen_sitemaps.insert(sitemap_url.clone()) {
                continue;
            }
            let sitemap = Url::parse(&sitemap_url).expect("normalized sitemap URL parses");
            let mut response = match self.request(&sitemap, "sitemap") {
                Ok(response) => response,
                Err(error) if error.code == "safety_cap" => return Err(error),
                Err(error) => {
                    self.emit(
                        "sitemap.unavailable",
                        "failure",
                        json!({"url": sitemap_url, "reason": error.message}),
                    );
                    continue;
                }
            };
            let status = response.status().as_u16();
            if !response.status().is_success() {
                self.emit(
                    "sitemap.unavailable",
                    "failure",
                    json!({"url": sitemap_url, "reason": format!("HTTP {status}")}),
                );
                continue;
            }
            let body = match response.read_bounded(self.config.max_body_bytes) {
                Ok(Some(body)) => body,
                Ok(None) => {
                    self.emit(
                        "sitemap.unavailable",
                        "failure",
                        json!({"url": sitemap_url, "reason": "body_limit_exceeded"}),
                    );
                    continue;
                }
                Err(error) if error.code == "safety_cap" => return Err(error),
                Err(error) => {
                    self.emit(
                        "sitemap.unavailable",
                        "failure",
                        json!({"url": sitemap_url, "reason": error.message}),
                    );
                    continue;
                }
            };
            drop(response);
            let xml = String::from_utf8_lossy(&body);
            let locations = extract_sitemap_locations(&xml);
            if locations.is_empty() {
                self.emit(
                    "sitemap.unavailable",
                    "failure",
                    json!({"url": sitemap_url, "reason": "no_locations"}),
                );
                continue;
            }
            if is_sitemap_index(&xml) {
                if depth >= MAX_SITEMAP_DEPTH {
                    self.emit(
                        "sitemap.unavailable",
                        "failure",
                        json!({"url": sitemap_url, "reason": "depth_limit_exceeded"}),
                    );
                    continue;
                }
                for location in locations {
                    let Some(normalized) = normalize_sitemap_url(&location) else {
                        continue;
                    };
                    let Ok(parsed) = Url::parse(&normalized) else {
                        continue;
                    };
                    if origin_key(&parsed) == origin
                        && sitemap_url_allowed(self.config, origin, &normalized)
                    {
                        index_locations_seen += 1;
                        if index_locations_seen > MAX_SITEMAP_INDEX_LOCATIONS {
                            return Err(Error::safety("sitemap index location cap exceeded"));
                        }
                        pending.push_back((normalized, depth + 1));
                    }
                }
            } else {
                let mut candidates = Vec::new();
                for location in locations {
                    let Some(normalized) = normalize_sitemap_url(&location) else {
                        continue;
                    };
                    if owner_for(self.config, &normalized).is_some() {
                        candidates.push(normalized);
                    }
                }
                page_count = self.spool.insert_sitemap_pages(origin, &candidates)?;
            }
        }

        Ok(page_count)
    }

    fn fetch(&self, requested_url: &str) -> Result<FetchOutcome, Error> {
        let mut current =
            Url::parse(requested_url).map_err(|e| Error::storage(format!("parse URL: {e}")))?;
        let mut source_id = owner_for(self.config, requested_url)
            .ok_or_else(|| Error::invalid("requested URL outside Source allowlist"))?
            .source_id
            .clone();
        let mut redirects = Vec::new();
        for _ in 0..=MAX_REDIRECTS {
            let mut response = self.request(&current, "page")?;
            if response.status().is_redirection() {
                let Some(location) = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                else {
                    return Ok(FetchOutcome::Rejected("redirect_missing_location".into()));
                };
                let next = current
                    .join(location)
                    .map_err(|_| Error::fetch("invalid redirect location"))?;
                let next = match normalize_url(next) {
                    Ok(url) => url,
                    Err(reason) => return Ok(FetchOutcome::Rejected(reason)),
                };
                let Some(next_source_id) =
                    owner_for(self.config, &next).map(|source| source.source_id.clone())
                else {
                    return Ok(FetchOutcome::Rejected(
                        "redirect_outside_source_allowlist".into(),
                    ));
                };
                if next == requested_url || redirects.contains(&next) {
                    return Ok(FetchOutcome::Rejected("redirect_loop".into()));
                }
                drop(response);
                if !self.ensure_robots(&next)? {
                    return Ok(FetchOutcome::Rejected("redirect_robots_denied".into()));
                }
                source_id = next_source_id;
                redirects.push(next.clone());
                current = Url::parse(&next).expect("normalized redirect parses");
                continue;
            }
            if !response.status().is_success() {
                return Err(Error::fetch(format!(
                    "GET {current} returned HTTP {}",
                    response.status()
                )));
            }
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            let content_type_lower = content_type.to_ascii_lowercase();
            if !content_type.is_empty()
                && !content_type_lower.starts_with("text/html")
                && !content_type_lower.starts_with("application/xhtml+xml")
            {
                return Ok(FetchOutcome::Rejected("non_html_content_type".into()));
            }
            if response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|length| length > self.config.max_body_bytes)
            {
                return Ok(FetchOutcome::Rejected("body_limit_exceeded".into()));
            }
            let status_code = response.status().as_u16();
            let Some(body) = response.read_bounded(
                self.config
                    .max_body_bytes
                    .min(self.config.max_extraction_bytes),
            )?
            else {
                return Ok(FetchOutcome::Rejected("body_limit_exceeded".into()));
            };
            drop(response);
            if body.len() > self.config.max_body_bytes
                || body.len() > self.config.max_extraction_bytes
            {
                return Ok(FetchOutcome::Rejected("body_limit_exceeded".into()));
            }
            let html = match String::from_utf8(body.clone()) {
                Ok(html) => html.trim_start_matches('\u{feff}').to_string(),
                Err(_) => return Ok(FetchOutcome::Rejected("body_decode_failed".into())),
            };
            let extraction_started = Instant::now();
            let extracted = match extract(&html, &current, self.config.min_content_chars) {
                Ok(page) => page,
                Err(reason) => return Ok(FetchOutcome::Rejected(reason)),
            };
            if extraction_started.elapsed()
                > Duration::from_millis(self.config.max_extraction_millis)
            {
                return Ok(FetchOutcome::Rejected(
                    "extraction_cpu_budget_exceeded".into(),
                ));
            }
            return Ok(FetchOutcome::Admitted {
                source_id,
                final_url: current.to_string(),
                redirects,
                status_code,
                content_type,
                body,
                extracted: Box::new(extracted),
            });
        }
        Ok(FetchOutcome::Rejected("redirect_limit_exceeded".into()))
    }

    fn request(&self, url: &Url, request_kind: &'static str) -> Result<RequestResponse, Error> {
        for attempt in 1..=self.config.max_attempts {
            let schedule_key = schedule_key(self.config, url);
            let origin = origin_key(url);
            let permit = self.coordinator.start_request(&schedule_key, &origin)?;
            permit.mark_started();
            let request_attempt = RequestAttempt {
                url: url.to_string(),
                origin: origin.clone(),
                request_kind,
                attempt,
                started_at: Instant::now(),
                concurrency: permit.concurrency,
            };
            let result = self.client.get(url.clone()).send();
            match result {
                Ok(response) => {
                    let status_code = response.status().as_u16();
                    let response = RequestResponse {
                        response: Some(response),
                        _permit: Some(permit),
                        attempt: request_attempt,
                        events: self.events.clone(),
                        status_code,
                        response_body_error: None,
                        timed_out: false,
                    };
                    if retryable_status(status_code) && attempt < self.config.max_attempts {
                        let delay = retry_delay(&response, attempt, self.config);
                        let request_elapsed_ms = response.attempt.elapsed_ms();
                        drop(response);
                        self.emit(
                            "fetch.retry",
                            "success",
                            json!({
                                "url": url.as_str(),
                                "origin": origin,
                                "request_kind": request_kind,
                                "attempt": attempt,
                                "next_attempt": attempt + 1,
                                "status_code": status_code,
                                "request_elapsed_ms": request_elapsed_ms,
                                "delay_ms": delay,
                            }),
                        );
                        if delay > 0 {
                            thread::sleep(Duration::from_millis(delay));
                        }
                    } else if retryable_status(status_code) {
                        return Err(Error::fetch(format!(
                            "GET {url} exhausted {} attempts with HTTP {status_code}",
                            self.config.max_attempts,
                        )));
                    } else {
                        return Ok(response);
                    }
                }
                Err(error) if attempt < self.config.max_attempts => {
                    let delay = retry_delay_without_response(attempt, self.config);
                    let timed_out = error.is_timeout();
                    let message = error.to_string();
                    let request_elapsed_ms = request_attempt.elapsed_ms();
                    drop(permit);
                    request_attempt.emit(
                        &self.events,
                        request_elapsed_ms,
                        None,
                        Some(&message),
                        None,
                        timed_out,
                    );
                    self.emit(
                        "fetch.retry",
                        "success",
                        json!({
                            "url": url.as_str(),
                            "origin": origin,
                            "request_kind": request_kind,
                            "attempt": attempt,
                            "next_attempt": attempt + 1,
                            "reason": "transport_error",
                            "error": message,
                            "timed_out": timed_out,
                            "request_elapsed_ms": request_elapsed_ms,
                            "delay_ms": delay,
                        }),
                    );
                    if delay > 0 {
                        thread::sleep(Duration::from_millis(delay));
                    }
                }
                Err(error) => {
                    let timed_out = error.is_timeout();
                    let message = error.to_string();
                    let request_elapsed_ms = request_attempt.elapsed_ms();
                    drop(permit);
                    request_attempt.emit(
                        &self.events,
                        request_elapsed_ms,
                        None,
                        Some(&message),
                        None,
                        timed_out,
                    );
                    return Err(Error::fetch(format!(
                        "GET {url} exhausted {} attempts: {message}",
                        self.config.max_attempts
                    )));
                }
            }
        }
        Err(Error::safety("request attempt loop exhausted"))
    }
}

fn owner_for<'a>(config: &'a Config, url: &str) -> Option<&'a Source> {
    config.sources.iter().find(|source| source.owns(url))
}

pub(crate) fn sitemap_url_allowed(config: &Config, origin: &str, url: &str) -> bool {
    config.sources.iter().any(|source| {
        source
            .allowed_origins
            .iter()
            .any(|allowed| allowed == origin)
            && (source.sitemap_url_prefixes.is_empty()
                || source
                    .sitemap_url_prefixes
                    .iter()
                    .any(|prefix| url.starts_with(prefix)))
    })
}

pub(crate) fn schedule_key(config: &Config, url: &Url) -> String {
    let origin = origin_key(url);
    config
        .sources
        .iter()
        .find(|source| source.allowed_origins.contains(&origin))
        .and_then(|source| source.politeness_group.as_ref())
        .map(|group| format!("group:{group}"))
        .unwrap_or(origin)
}

impl Source {
    pub(crate) fn owns(&self, value: &str) -> bool {
        let Ok(url) = Url::parse(value) else {
            return false;
        };
        let Ok(normalized) = normalize_url(url.clone()) else {
            return false;
        };
        self.allowed_origins.contains(&origin_key(&url))
            && self
                .path_prefixes
                .iter()
                .any(|prefix| url.path().starts_with(prefix))
            && !self
                .deny_path_prefixes
                .iter()
                .any(|prefix| url.path().starts_with(prefix))
            && self
                .allowed_urls
                .as_ref()
                .map_or(true, |allowed| allowed.contains(&normalized))
    }
}

pub(crate) fn sources_overlap(left: &Source, right: &Source) -> bool {
    left.allowed_origins
        .iter()
        .any(|origin| right.allowed_origins.contains(origin))
        && left.path_prefixes.iter().any(|left_path| {
            right.path_prefixes.iter().any(|right_path| {
                left_path.starts_with(right_path) || right_path.starts_with(left_path)
            })
        })
}

fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn retry_delay(response: &reqwest::blocking::Response, attempt: usize, config: &Config) -> u64 {
    response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1000))
        .unwrap_or_else(|| retry_delay_without_response(attempt, config))
}

fn retry_delay_without_response(attempt: usize, config: &Config) -> u64 {
    let multiplier = 1_u64 << attempt.saturating_sub(1).min(20);
    config
        .retry_backoff_ms
        .saturating_mul(multiplier)
        .min(config.retry_max_delay_ms)
}

pub(crate) fn parse_robots(body: &[u8]) -> RobotsPolicy {
    let mut groups = Vec::new();
    let mut current = RobotsGroup::default();
    let mut sitemaps = BTreeSet::new();
    for raw_line in String::from_utf8_lossy(body).lines() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            if !current.agents.is_empty() {
                groups.push(current);
                current = RobotsGroup::default();
            }
            continue;
        }
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        let field = field.trim().to_ascii_lowercase();
        let value = value.trim();
        match field.as_str() {
            "sitemap" => {
                if let Ok(url) = Url::parse(value)
                    && let Ok(normalized) = normalize_url(url)
                {
                    sitemaps.insert(normalized);
                }
            }
            "user-agent" => {
                if !current.rules.is_empty() {
                    groups.push(current);
                    current = RobotsGroup::default();
                }
                current.agents.push(value.to_ascii_lowercase());
            }
            "allow" | "disallow" if !current.agents.is_empty() => current.rules.push(RobotsRule {
                allow: field == "allow",
                path: value.into(),
            }),
            _ => {}
        }
    }
    if !current.agents.is_empty() {
        groups.push(current);
    }
    RobotsPolicy {
        groups,
        sitemaps: sitemaps.into_iter().collect(),
    }
}

pub(crate) fn extract_sitemap_locations(xml: &str) -> Vec<String> {
    let lowercase = xml.to_ascii_lowercase();
    let mut locations = BTreeSet::new();
    let mut offset = 0;
    while let Some(open_relative) = lowercase[offset..].find("<loc>") {
        let open = offset + open_relative;
        let content_start = open + "<loc>".len();
        let Some(close_relative) = lowercase[content_start..].find("</loc>") else {
            break;
        };
        let close = content_start + close_relative;
        let location = decode_xml_entities(xml[content_start..close].trim());
        if !location.is_empty() {
            locations.insert(location);
        }
        offset = close + "</loc>".len();
    }
    locations.into_iter().collect()
}

fn decode_xml_entities(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

pub(crate) fn normalize_sitemap_url(value: &str) -> Option<String> {
    Url::parse(value)
        .ok()
        .and_then(|url| normalize_url(url).ok())
}

pub(crate) fn is_sitemap_index(xml: &str) -> bool {
    xml.to_ascii_lowercase().contains("<sitemapindex")
}

pub(crate) fn page_id(url: &str) -> String {
    sha256_hex(format!("scout.page.v1\0{url}").as_bytes())
}
