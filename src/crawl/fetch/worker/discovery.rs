use super::super::helpers::{
    extract_sitemap_locations, is_sitemap_index, normalize_sitemap_url, owner_for, parse_robots,
    sitemap_url_allowed,
};
use super::super::request::CachedRobots;
use super::super::{MAX_SITEMAP_DEPTH, MAX_SITEMAP_INDEX_LOCATIONS};
use super::CrawlWorker;
use crate::crawl::{MAX_ROBOTS_BYTES, RobotsPolicy};
use crate::error::Error;
use crate::storage::sha256_hex;
use crate::url::origin_key;
use ::url::Url;
use reqwest::header::LOCATION;
use serde_json::json;
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

impl<'a> CrawlWorker<'a> {
    pub(super) fn ensure_robots(&self, page_url: &str) -> Result<bool, Error> {
        let parsed = Url::parse(page_url).map_err(|e| Error::storage(format!("parse URL: {e}")))?;
        let origin = origin_key(&parsed);
        let cache_ttl = Duration::from_secs(self.config.robots_cache_seconds);
        if let Some((_, allowed)) =
            self.coordinator
                .fresh_robots_policy(&origin, parsed.path(), cache_ttl)
        {
            return Ok(allowed);
        }
        let robots_lock = self.coordinator.robots_lock(&origin);
        let _robots_guard = robots_lock.lock().expect("robots origin lock");
        if let Some((_, allowed)) =
            self.coordinator
                .fresh_robots_policy(&origin, parsed.path(), cache_ttl)
        {
            return Ok(allowed);
        }
        let mut current = Url::parse(&format!("{origin}/robots.txt")).expect("robots URL parses");
        for _ in 0..=5 {
            let mut response = match self.request(&current, "robots") {
                Ok(response) => response,
                Err(error) => {
                    if let Some((body_sha256, allowed)) =
                        self.coordinator
                            .fresh_robots_policy(&origin, parsed.path(), cache_ttl)
                    {
                        self.emit("robots.cache_used", "success", json!({"origin": origin, "body_sha256": body_sha256, "reason": error.code}));
                        return Ok(allowed);
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
                self.coordinator.cache_robots(
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
            self.coordinator.cache_robots(
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
}
