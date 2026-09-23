use super::{
    CanonicalConfig, Config, DEFAULT_BODY_LIMIT, DEFAULT_CONTENT_MINIMUM, DEFAULT_FRONTIER_LIMIT,
    DEFAULT_PASSAGE_MAX_TOKENS, DEFAULT_TIMEOUT_SECONDS, Error, HashSet, MAX_MODEL_PASSAGE_TOKENS,
    Path, RawConfig, Source, Url, fs,
};
use super::{
    crawl::sources_overlap,
    storage::sha256_hex,
    url::{normalize_origin, normalize_url},
};

pub(crate) fn load_config(path: &Path) -> Result<Config, Error> {
    let text = fs::read_to_string(path)
        .map_err(|e| Error::invalid(format!("read config {}: {e}", path.display())))?;
    let raw: RawConfig = toml::from_str(&text)
        .map_err(|e| Error::invalid(format!("parse config {}: {e}", path.display())))?;
    let profile = raw.crawl.unwrap_or_default();
    let schema_version = raw
        .schema_version
        .ok_or_else(|| Error::invalid("schema_version is required"))?;
    if !schema_version.ends_with(".v1") {
        return Err(Error::invalid(format!(
            "unsupported schema_version {schema_version:?}"
        )));
    }
    let contact_url = raw
        .contact_url
        .ok_or_else(|| Error::invalid("contact_url is required"))?;
    let contact = Url::parse(&contact_url)
        .map_err(|e| Error::invalid(format!("invalid contact_url: {e}")))?;
    if !matches!(contact.scheme(), "http" | "https") {
        return Err(Error::invalid("contact_url must use http or https"));
    }
    let user_agent = raw
        .user_agent
        .or(profile.user_agent)
        .unwrap_or_else(|| format!("Scout/{} (+{contact_url})", env!("CARGO_PKG_VERSION")));
    if user_agent.trim().is_empty() {
        return Err(Error::invalid("user_agent must not be empty"));
    }
    if raw.sources.is_empty() {
        return Err(Error::invalid("at least one Source is required"));
    }
    let mut sources = Vec::new();
    for raw_source in raw.sources {
        let source_id = raw_source
            .source_id
            .ok_or_else(|| Error::invalid("source_id is required"))?;
        if source_id.trim().is_empty() || !source_id.is_ascii() {
            return Err(Error::invalid("source_id must be nonempty ASCII text"));
        }
        let seeds = raw_source
            .seeds
            .ok_or_else(|| Error::invalid(format!("seeds are required for Source {source_id}")))?;
        if seeds.is_empty() {
            return Err(Error::invalid(format!(
                "seeds must not be empty for Source {source_id}"
            )));
        }
        let origins = raw_source.allowed_origins.ok_or_else(|| {
            Error::invalid(format!(
                "allowed_origins are required for Source {source_id}"
            ))
        })?;
        if origins.is_empty() {
            return Err(Error::invalid(format!(
                "allowed_origins must not be empty for Source {source_id}"
            )));
        }
        let allowed_origins = origins
            .into_iter()
            .map(|origin| normalize_origin(&origin))
            .collect::<Result<Vec<_>, _>>()?;
        let path_prefixes = raw_source.path_prefixes.ok_or_else(|| {
            Error::invalid(format!("path_prefixes are required for Source {source_id}"))
        })?;
        if path_prefixes.is_empty() || path_prefixes.iter().any(|path| !path.starts_with('/')) {
            return Err(Error::invalid(
                "path_prefixes must contain an absolute path",
            ));
        }
        if raw_source
            .deny_path_prefixes
            .iter()
            .any(|path| !path.starts_with('/'))
        {
            return Err(Error::invalid(
                "deny_path_prefixes must contain absolute paths",
            ));
        }
        let seeds = seeds
            .into_iter()
            .map(|seed| {
                Url::parse(&seed)
                    .map_err(|e| Error::invalid(format!("invalid seed {seed:?}: {e}")))
                    .and_then(|url| normalize_url(url).map_err(Error::invalid))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let source = Source {
            source_id,
            name: raw_source.name,
            seeds,
            allowed_origins,
            path_prefixes,
            deny_path_prefixes: raw_source.deny_path_prefixes,
            politeness_group: raw_source.politeness_group,
            license_urls: raw_source.license_urls.unwrap_or_default(),
            minimum_page_quota: raw_source.minimum_page_quota.unwrap_or(0),
        };
        if source.seeds.iter().any(|seed| !source.owns(seed)) {
            return Err(Error::invalid("every seed must belong to Source allowlist"));
        }
        sources.push(source);
    }
    if sources
        .iter()
        .map(|source| &source.source_id)
        .collect::<HashSet<_>>()
        .len()
        != sources.len()
    {
        return Err(Error::invalid("source_id values must be unique"));
    }
    for left in 0..sources.len() {
        for right in left + 1..sources.len() {
            if sources_overlap(&sources[left], &sources[right]) {
                return Err(Error::invalid("Source allowlists must not overlap"));
            }
        }
    }
    let page_target = raw.page_target.or(profile.page_target).unwrap_or(1);
    if page_target == 0 {
        return Err(Error::invalid("page_target must be positive"));
    }
    let min_content_chars = raw
        .min_content_chars
        .or(profile.min_content_chars)
        .unwrap_or(DEFAULT_CONTENT_MINIMUM);
    if min_content_chars == 0 {
        return Err(Error::invalid("min_content_chars must be positive"));
    }
    let max_body_bytes = raw
        .max_decoded_body_bytes
        .or(profile.max_decoded_body_bytes)
        .unwrap_or(DEFAULT_BODY_LIMIT);
    if max_body_bytes == 0 {
        return Err(Error::invalid("max_decoded_body_bytes must be positive"));
    }
    let max_frontier = raw
        .max_frontier
        .or(profile.max_frontier)
        .unwrap_or(DEFAULT_FRONTIER_LIMIT);
    if max_frontier == 0 {
        return Err(Error::invalid("max_frontier must be positive"));
    }
    let timeout_seconds = raw
        .request_timeout_seconds
        .or(profile.request_timeout_seconds)
        .unwrap_or(DEFAULT_TIMEOUT_SECONDS);
    if timeout_seconds == 0 {
        return Err(Error::invalid("request_timeout_seconds must be positive"));
    }
    let global_concurrency = raw
        .global_concurrency
        .or(profile.global_concurrency)
        .unwrap_or(1);
    let origin_concurrency = raw
        .origin_concurrency
        .or(profile.origin_concurrency)
        .unwrap_or(1);
    let min_start_spacing_ms = raw
        .min_start_spacing_ms
        .or(profile.min_start_spacing_ms)
        .unwrap_or(0);
    let max_attempts = raw.max_attempts.or(profile.max_attempts).unwrap_or(4);
    let retry_backoff_ms = raw
        .retry_backoff_ms
        .or(profile.retry_backoff_ms)
        .unwrap_or(100);
    let retry_max_delay_ms = raw
        .retry_max_delay_ms
        .or(profile.retry_max_delay_ms)
        .unwrap_or(30_000);
    let max_total_attempts = raw
        .max_total_attempts
        .or(profile.max_total_attempts)
        .unwrap_or(10_000);
    let max_duration_seconds = raw
        .max_duration_seconds
        .or(profile.max_duration_seconds)
        .unwrap_or(3600);
    let robots_cache_seconds = raw
        .robots_cache_seconds
        .or(profile.robots_cache_seconds)
        .unwrap_or(86_400);
    let max_extraction_bytes = raw
        .max_extraction_bytes
        .or(profile.max_extraction_bytes)
        .unwrap_or(max_body_bytes);
    let max_extraction_millis = raw
        .max_extraction_millis
        .or(profile.max_extraction_millis)
        .unwrap_or(5_000);
    let max_passage_tokens = raw
        .max_passage_tokens
        .or(profile.max_passage_tokens)
        .unwrap_or(DEFAULT_PASSAGE_MAX_TOKENS);
    if global_concurrency == 0
        || origin_concurrency == 0
        || max_attempts == 0
        || max_total_attempts == 0
        || max_duration_seconds == 0
        || robots_cache_seconds == 0
        || max_extraction_bytes == 0
        || max_extraction_millis == 0
        || max_passage_tokens == 0
    {
        return Err(Error::invalid("crawl limits must be positive"));
    }
    if max_attempts > 4 {
        return Err(Error::invalid("max_attempts must not exceed four"));
    }
    if max_passage_tokens > MAX_MODEL_PASSAGE_TOKENS {
        return Err(Error::invalid(format!(
            "max_passage_tokens must not exceed {MAX_MODEL_PASSAGE_TOKENS}"
        )));
    }
    if retry_max_delay_ms < retry_backoff_ms {
        return Err(Error::invalid(
            "retry_max_delay_ms must cover retry_backoff_ms",
        ));
    }
    let canonical = CanonicalConfig {
        schema_version: &schema_version,
        contact_url: &contact_url,
        user_agent: &user_agent,
        page_target,
        min_content_chars,
        max_body_bytes,
        max_frontier,
        timeout_seconds,
        global_concurrency,
        origin_concurrency,
        min_start_spacing_ms,
        max_attempts,
        retry_backoff_ms,
        retry_max_delay_ms,
        max_total_attempts,
        max_duration_seconds,
        robots_cache_seconds,
        max_extraction_bytes,
        max_extraction_millis,
        max_passage_tokens,
        sources: &sources,
    };
    let digest = sha256_hex(
        &serde_json::to_vec(&canonical)
            .map_err(|e| Error::invalid(format!("serialize config: {e}")))?,
    );
    Ok(Config {
        schema_version,
        contact_url,
        user_agent,
        page_target,
        min_content_chars,
        max_body_bytes,
        max_frontier,
        timeout_seconds,
        global_concurrency,
        origin_concurrency,
        min_start_spacing_ms,
        max_attempts,
        retry_backoff_ms,
        retry_max_delay_ms,
        max_total_attempts,
        max_duration_seconds,
        robots_cache_seconds,
        max_extraction_bytes,
        max_extraction_millis,
        max_passage_tokens,
        sources,
        digest,
    })
}
