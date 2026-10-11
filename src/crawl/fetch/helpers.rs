use super::super::robots::{RobotsGroup, RobotsPolicy, RobotsRule};
use super::super::{Config, Source};
use crate::storage::sha256_hex;
use crate::url::{normalize_url, origin_key};
use ::url::Url;
use std::collections::BTreeSet;

pub(super) fn owner_for<'a>(config: &'a Config, url: &str) -> Option<&'a Source> {
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
                .is_none_or(|allowed| allowed.contains(&normalized))
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
