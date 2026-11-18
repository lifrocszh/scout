//! Near-duplicate detection at the reduce step (Corpus materialization).
//!
//! Exact duplicates already collapse into one representative Page with aliases. Pages that
//! are only *nearly* the same (a changed footer, a version banner) stay distinct Pages, as
//! CONTEXT.md requires, so this module only measures them: each Page gets a 64-bit SimHash
//! of its word 3-shingles, and a Page whose fingerprint is within
//! [`MAX_HAMMING_DISTANCE`] bits of an earlier Page of the same origin counts as a near
//! duplicate. The report lands next to the crawl manifest and is not part of the snapshot
//! identity, so it cannot change what is indexed or how it ranks.
//!
//! Six bits is about a 2-3% edit of a 400-word Page; unrelated Pages sharing one site's
//! vocabulary sit near 30 bits apart, so false matches are rare. Candidate search uses
//! pigeonhole bands (Manku et al., WWW 2007): two fingerprints within six bits agree
//! exactly on at least one of seven bands, so only Pages sharing a band are compared.
use crate::error::Error;
use crate::observability::metrics;
use crate::storage::write_json;
use crate::url::origin_key;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

pub(crate) const SCHEMA: &str = "scout.near-duplicates.v1";
/// File name of the report inside `crawls/<run_id>/`.
pub(crate) const REPORT_FILE: &str = "near-duplicates.json";
pub(crate) const MAX_HAMMING_DISTANCE: u32 = 6;
const SHINGLE_WORDS: usize = 3;
/// One more band than the distance, so a match always shares a band (pigeonhole).
const BANDS: usize = MAX_HAMMING_DISTANCE as usize + 1;
/// Pairs listed in the report; counts are always complete.
const MAX_REPORTED_PAIRS: usize = 1_000;

/// SimHash of the text's lowercase word 3-shingles; `None` when it has fewer than three words.
pub(crate) fn simhash(text: &str) -> Option<u64> {
    let words = text
        .split(|character: char| !character.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_lowercase)
        .collect::<Vec<_>>();
    if words.len() < SHINGLE_WORDS {
        return None;
    }
    let mut weights = [0_i64; 64];
    for shingle in words.windows(SHINGLE_WORDS) {
        let hash = shingle_hash(shingle);
        for (bit, weight) in weights.iter_mut().enumerate() {
            *weight += if hash >> bit & 1 == 1 { 1 } else { -1 };
        }
    }
    Some(
        weights
            .iter()
            .enumerate()
            .filter(|(_, weight)| **weight > 0)
            .fold(0, |fingerprint, (bit, _)| fingerprint | 1 << bit),
    )
}

/// FNV-1a over the shingle, finished with the SplitMix64 mixer so every output bit is usable.
fn shingle_hash(words: &[String]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (index, word) in words.iter().enumerate() {
        if index > 0 {
            hash = (hash ^ u64::from(b' ')).wrapping_mul(0x0100_0000_01b3);
        }
        for byte in word.bytes() {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// Bits `[64 * index / BANDS, 64 * (index + 1) / BANDS)` of the fingerprint.
fn band(fingerprint: u64, index: usize) -> u16 {
    let start = 64 * index / BANDS;
    let width = 64 * (index + 1) / BANDS - start;
    ((fingerprint >> start) & ((1 << width) - 1)) as u16
}

#[derive(Default)]
struct OriginState {
    page_count: usize,
    near_duplicate_page_count: usize,
    exact_duplicate_alias_count: usize,
    fingerprints: Vec<(u64, String)>,
    bands: [HashMap<u16, Vec<usize>>; BANDS],
}

impl OriginState {
    /// The earliest earlier Page within the distance, with that distance.
    fn nearest_earlier(&self, fingerprint: u64) -> Option<(usize, u32)> {
        let mut best: Option<(usize, u32)> = None;
        for (index, bands) in self.bands.iter().enumerate() {
            for &candidate in bands.get(&band(fingerprint, index)).into_iter().flatten() {
                let distance = (self.fingerprints[candidate].0 ^ fingerprint).count_ones();
                if distance <= MAX_HAMMING_DISTANCE
                    && best.is_none_or(|(current, _)| candidate < current)
                {
                    best = Some((candidate, distance));
                }
            }
        }
        best
    }
}

/// Accumulates Pages in the snapshot's deterministic order and produces the report.
#[derive(Default)]
pub(crate) struct NearDuplicateDetector {
    origins: BTreeMap<String, OriginState>,
    pairs: Vec<NearDuplicatePair>,
    pairs_truncated: bool,
}

impl NearDuplicateDetector {
    pub(crate) fn observe_page(&mut self, page_url: &str, content: &str) -> Result<(), Error> {
        let state = self.origins.entry(url_origin(page_url)?).or_default();
        state.page_count += 1;
        let Some(fingerprint) = simhash(content) else {
            return Ok(());
        };
        if let Some((earlier, distance)) = state.nearest_earlier(fingerprint) {
            state.near_duplicate_page_count += 1;
            if self.pairs.len() < MAX_REPORTED_PAIRS {
                self.pairs.push(NearDuplicatePair {
                    page_url: page_url.to_owned(),
                    near_duplicate_of: state.fingerprints[earlier].1.clone(),
                    hamming_distance: distance,
                });
            } else {
                self.pairs_truncated = true;
            }
        }
        let index = state.fingerprints.len();
        state.fingerprints.push((fingerprint, page_url.to_owned()));
        for (band_index, bands) in state.bands.iter_mut().enumerate() {
            bands
                .entry(band(fingerprint, band_index))
                .or_default()
                .push(index);
        }
        Ok(())
    }

    /// Count one URL folded into an exact-duplicate representative.
    pub(crate) fn observe_alias(&mut self, alias_url: &str) -> Result<(), Error> {
        self.origins
            .entry(url_origin(alias_url)?)
            .or_default()
            .exact_duplicate_alias_count += 1;
        Ok(())
    }

    pub(crate) fn finish(self, run_id: &str, snapshot_id: &str) -> NearDuplicateReport {
        let origins = self
            .origins
            .into_iter()
            .map(|(origin, state)| OriginNearDuplicates {
                origin,
                page_count: state.page_count,
                near_duplicate_page_count: state.near_duplicate_page_count,
                near_duplicate_rate: rate(state.near_duplicate_page_count, state.page_count),
                exact_duplicate_alias_count: state.exact_duplicate_alias_count,
            })
            .collect::<Vec<_>>();
        let page_count = origins.iter().map(|origin| origin.page_count).sum();
        let near_duplicate_page_count = origins
            .iter()
            .map(|origin| origin.near_duplicate_page_count)
            .sum();
        NearDuplicateReport {
            schema_version: SCHEMA.into(),
            run_id: run_id.into(),
            snapshot_id: snapshot_id.into(),
            fingerprint: "simhash64-word3-shingles".into(),
            max_hamming_distance: MAX_HAMMING_DISTANCE,
            page_count,
            near_duplicate_page_count,
            near_duplicate_rate: rate(near_duplicate_page_count, page_count),
            exact_duplicate_alias_count: origins
                .iter()
                .map(|origin| origin.exact_duplicate_alias_count)
                .sum(),
            origins,
            pairs: self.pairs,
            pairs_truncated: self.pairs_truncated,
        }
    }
}

fn url_origin(value: &str) -> Result<String, Error> {
    url::Url::parse(value)
        .map(|url| origin_key(&url))
        .map_err(|error| Error::invalid(format!("Page URL {value:?} is invalid: {error}")))
}

fn rate(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct NearDuplicatePair {
    pub(crate) page_url: String,
    pub(crate) near_duplicate_of: String,
    pub(crate) hamming_distance: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct OriginNearDuplicates {
    pub(crate) origin: String,
    pub(crate) page_count: usize,
    pub(crate) near_duplicate_page_count: usize,
    pub(crate) near_duplicate_rate: f64,
    pub(crate) exact_duplicate_alias_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct NearDuplicateReport {
    pub(crate) schema_version: String,
    pub(crate) run_id: String,
    pub(crate) snapshot_id: String,
    pub(crate) fingerprint: String,
    pub(crate) max_hamming_distance: u32,
    pub(crate) page_count: usize,
    pub(crate) near_duplicate_page_count: usize,
    pub(crate) near_duplicate_rate: f64,
    pub(crate) exact_duplicate_alias_count: usize,
    pub(crate) origins: Vec<OriginNearDuplicates>,
    pub(crate) pairs: Vec<NearDuplicatePair>,
    pub(crate) pairs_truncated: bool,
}

impl NearDuplicateReport {
    pub(crate) fn write(&self, crawl_dir: &Path) -> Result<(), Error> {
        write_json(&crawl_dir.join(REPORT_FILE), self).map(|_| ())
    }

    pub(crate) fn read(crawl_dir: &Path) -> Result<Option<Self>, Error> {
        let path = crawl_dir.join(REPORT_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(Error::storage(format!("read {}: {error}", path.display())));
            }
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| Error::invalid(format!("parse {}: {error}", path.display())))
    }

    /// Publish the per-origin counts as the serving Corpus gauges.
    pub(crate) fn publish_gauges(&self) {
        metrics::CORPUS_PAGES.clear();
        metrics::CORPUS_NEAR_DUPLICATE_PAGES.clear();
        metrics::CORPUS_EXACT_DUPLICATE_ALIASES.clear();
        for origin in &self.origins {
            let labels = [origin.origin.as_str()];
            metrics::CORPUS_PAGES.set(&labels, origin.page_count as f64);
            metrics::CORPUS_NEAR_DUPLICATE_PAGES
                .set(&labels, origin.near_duplicate_page_count as f64);
            metrics::CORPUS_EXACT_DUPLICATE_ALIASES
                .set(&labels, origin.exact_duplicate_alias_count as f64);
        }
    }
}

/// Set the Corpus gauges from the report of the snapshot's crawl, when it has one.
pub(crate) fn publish_snapshot_gauges(data_dir: &Path, snapshot_id: &str) -> Result<(), Error> {
    #[derive(Deserialize)]
    struct SnapshotRun {
        run_id: String,
    }
    let manifest_path = data_dir
        .join("corpora")
        .join(snapshot_id)
        .join("snapshot-manifest.json");
    let bytes = std::fs::read(&manifest_path)
        .map_err(|error| Error::storage(format!("read {}: {error}", manifest_path.display())))?;
    let run: SnapshotRun = serde_json::from_slice(&bytes)
        .map_err(|error| Error::invalid(format!("parse {}: {error}", manifest_path.display())))?;
    match NearDuplicateReport::read(&data_dir.join("crawls").join(&run.run_id))? {
        Some(report) if report.snapshot_id == snapshot_id => report.publish_gauges(),
        _ => {
            metrics::CORPUS_PAGES.clear();
            metrics::CORPUS_NEAR_DUPLICATE_PAGES.clear();
            metrics::CORPUS_EXACT_DUPLICATE_ALIASES.clear();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 400-word documentation-like Page drawn from one site's vocabulary, so different
    /// seeds share words the way unrelated Pages of one origin do.
    fn page(seed: u64) -> String {
        const WORDS: [&str; 20] = [
            "pool", "server", "request", "handler", "timeout", "index", "segment", "merge",
            "query", "token", "field", "schema", "replica", "shard", "lease", "worker", "origin",
            "crawl", "page", "passage",
        ];
        let mut state = seed;
        (0..400)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                WORDS[(state >> 33) as usize % WORDS.len()]
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn bands_cover_every_bit_once() {
        let covered = (0..BANDS).fold(0_u64, |covered, index| {
            let start = 64 * index / BANDS;
            let width = 64 * (index + 1) / BANDS - start;
            assert_eq!(u64::from(band(u64::MAX, index)), (1 << width) - 1);
            assert_eq!(covered & (((1 << width) - 1) << start), 0);
            covered | ((1 << width) - 1) << start
        });
        assert_eq!(covered, u64::MAX);
    }

    #[test]
    fn identical_text_has_identical_fingerprints_and_short_text_has_none() {
        let base = page(1);
        assert_eq!(simhash(&base), simhash(&base.to_uppercase()));
        assert_eq!(simhash("two words"), None);
        assert!(simhash("three words here").is_some());
    }

    #[test]
    fn a_small_edit_stays_near_and_different_text_is_far() {
        let text = page(1);
        let base = simhash(&text).unwrap();
        for edit in [
            "Updated in version 2.",
            "Copyright 2026 Example Corp. All rights reserved. Privacy policy.",
        ] {
            let edited = simhash(&format!("{text} {edit}")).unwrap();
            assert!(
                (base ^ edited).count_ones() <= MAX_HAMMING_DISTANCE,
                "{edit}"
            );
        }
        for seed in 2..12 {
            let other = simhash(&page(seed)).unwrap();
            assert!(
                (base ^ other).count_ones() > 2 * MAX_HAMMING_DISTANCE,
                "{seed}"
            );
        }
    }

    #[test]
    fn report_counts_near_duplicates_per_origin_against_earlier_pages_only() {
        let mut detector = NearDuplicateDetector::default();
        let base = page(1);
        detector
            .observe_page("https://a.example/one", &base)
            .unwrap();
        detector
            .observe_page(
                "https://a.example/two",
                &format!("{base} Updated in version 2."),
            )
            .unwrap();
        detector
            .observe_page("https://a.example/three", &page(2))
            .unwrap();
        // The same text on another origin is not compared with origin a.
        detector
            .observe_page("https://b.example/one", &base)
            .unwrap();
        detector
            .observe_page("https://b.example/tiny", "hi")
            .unwrap();
        detector.observe_alias("https://b.example/copy").unwrap();
        let report = detector.finish("run-1", "snapshot-1");

        assert_eq!(report.page_count, 5);
        assert_eq!(report.near_duplicate_page_count, 1);
        assert_eq!(report.exact_duplicate_alias_count, 1);
        assert_eq!(
            report.origins,
            vec![
                OriginNearDuplicates {
                    origin: "https://a.example".into(),
                    page_count: 3,
                    near_duplicate_page_count: 1,
                    near_duplicate_rate: 1.0 / 3.0,
                    exact_duplicate_alias_count: 0,
                },
                OriginNearDuplicates {
                    origin: "https://b.example".into(),
                    page_count: 2,
                    near_duplicate_page_count: 0,
                    near_duplicate_rate: 0.0,
                    exact_duplicate_alias_count: 1,
                },
            ]
        );
        assert_eq!(report.pairs.len(), 1);
        assert_eq!(report.pairs[0].page_url, "https://a.example/two");
        assert_eq!(report.pairs[0].near_duplicate_of, "https://a.example/one");
        assert!(!report.pairs_truncated);
    }

    #[test]
    fn report_round_trips_and_drives_the_corpus_gauges() {
        let dir = std::env::temp_dir().join(format!(
            "scout-near-dup-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("crawls/run-g")).unwrap();
        std::fs::create_dir_all(dir.join("corpora/snapshot-g")).unwrap();
        std::fs::write(
            dir.join("corpora/snapshot-g/snapshot-manifest.json"),
            r#"{"run_id":"run-g","snapshot_id":"snapshot-g"}"#,
        )
        .unwrap();
        let mut detector = NearDuplicateDetector::default();
        let base = page(3);
        detector
            .observe_page("https://gauge.example/a", &base)
            .unwrap();
        detector
            .observe_page("https://gauge.example/b", &format!("{base} Edited."))
            .unwrap();
        let report = detector.finish("run-g", "snapshot-g");
        report.write(&dir.join("crawls/run-g")).unwrap();
        assert_eq!(
            NearDuplicateReport::read(&dir.join("crawls/run-g")).unwrap(),
            Some(report)
        );
        publish_snapshot_gauges(&dir, "snapshot-g").unwrap();
        assert_eq!(metrics::CORPUS_PAGES.get(&["https://gauge.example"]), 2.0);
        assert_eq!(
            metrics::CORPUS_NEAR_DUPLICATE_PAGES.get(&["https://gauge.example"]),
            1.0
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
