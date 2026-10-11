mod config;
pub(crate) mod extract;
mod fetch;
mod model;
mod robots;
mod run;
pub(crate) mod spool;

pub(crate) const EXTRACTION_SCHEMA: &str = "scout.extraction.v1";
pub(crate) const PASSAGE_SCHEMA: &str = "scout.passage.v1";
pub(crate) const DEFAULT_BODY_LIMIT: usize = 1024 * 1024;
pub(crate) const DEFAULT_CONTENT_MINIMUM: usize = 200;
pub(crate) const DEFAULT_FRONTIER_LIMIT: usize = 10_000;
pub(crate) const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
pub(crate) const DEFAULT_PASSAGE_MAX_TOKENS: usize = 384;
const MAX_REDIRECTS: usize = 10;
const MAX_ROBOTS_BYTES: usize = 512 * 1024;

pub(crate) use config::load_config;
use fetch::Crawler;
pub(crate) use fetch::{
    extract_sitemap_locations, is_sitemap_index, normalize_sitemap_url, page_id, parse_robots,
    schedule_key, sitemap_url_allowed,
};
use model::{CanonicalConfig, Extracted, FetchOutcome, RawConfig};
pub(crate) use model::{
    Capture, Config, ContentBlock, CrawlPolicy, CrawlResult, OriginRequestPolicy, Source,
};
pub(crate) use robots::{RobotsGroup, RobotsPolicy, RobotsRule};
pub(crate) use run::execute;
