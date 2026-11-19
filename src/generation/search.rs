use super::{BuildProfile, CatalogRecord, LoadedGeneration};
use super::{
    open_semantic_index, read_pointer, register_keyword_analyzer, validate_generation,
    validate_pointer,
};
use crate::error::Error;
use crate::storage::parse_jsonl;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tantivy::collector::TopDocs;
use tantivy::query::{EmptyQuery, Query, QueryParser, QueryParserError};
use tantivy::schema::Value as TantivyValue;
use tantivy::{Index, TantivyDocument};

pub(crate) fn load_active_generation(data_dir: &Path) -> Result<LoadedGeneration, Error> {
    for pointer_name in ["CURRENT", "PREVIOUS"] {
        let Ok(Some(pointer)) = read_pointer(data_dir, pointer_name) else {
            continue;
        };
        if validate_pointer(data_dir, &pointer).is_err() {
            continue;
        }
        let path = data_dir.join("generations").join(&pointer.generation_id);
        let Ok(manifest) = validate_generation(&path, "sealed") else {
            continue;
        };
        let Ok(profile_bytes) = fs::read(path.join("build-profile.json")) else {
            continue;
        };
        let Ok(profile) = serde_json::from_slice::<BuildProfile>(&profile_bytes) else {
            continue;
        };
        let Ok(catalog_bytes) = fs::read(path.join(&manifest.catalog_file)) else {
            continue;
        };
        let Ok(catalog) = parse_jsonl::<CatalogRecord>(&catalog_bytes, "catalog.jsonl") else {
            continue;
        };
        drop(catalog_bytes);
        let components = manifest.components();
        let keyword_index = if components.keyword {
            let Some(artifact) = manifest.keyword_artifact.as_deref() else {
                continue;
            };
            let Ok(mut index) = Index::open_in_dir(path.join(artifact)) else {
                continue;
            };
            if register_keyword_analyzer(&mut index, &profile).is_err() {
                continue;
            }
            Some(index)
        } else {
            None
        };
        let semantic_index = if components.semantic {
            let Some(artifact) = manifest.semantic_artifact.as_deref() else {
                continue;
            };
            let Ok(index) = open_semantic_index(&path.join(artifact), &profile) else {
                continue;
            };
            Some(index)
        } else {
            None
        };
        let semantic_dimensions = manifest
            .semantic
            .as_ref()
            .map(|evidence| evidence.dimensions);
        return Ok(LoadedGeneration {
            manifest,
            profile,
            catalog,
            semantic_dimensions,
            keyword_index,
            semantic_index,
            model_cache_dir: data_dir.join("models"),
            query_embedder: super::QueryEmbedder::default(),
        });
    }
    Err(Error::not_ready(
        "no valid active or previous Generation is available",
    ))
}

pub(crate) fn keyword_hits<'a>(
    generation: &'a LoadedGeneration,
    query: &str,
    source: Option<&str>,
) -> Result<Vec<(f32, &'a CatalogRecord)>, Error> {
    let keyword_index = generation
        .keyword_index
        .as_ref()
        .ok_or_else(|| Error::not_ready("keyword search is unavailable in this Generation"))?;
    let schema = keyword_index.schema();
    let title = schema
        .get_field("title")
        .map_err(|_| Error::not_ready("keyword title field unavailable"))?;
    let heading = schema
        .get_field("heading")
        .map_err(|_| Error::not_ready("keyword heading field unavailable"))?;
    let body = schema
        .get_field("body")
        .map_err(|_| Error::not_ready("keyword body field unavailable"))?;
    let passage_id = schema
        .get_field("passage_id")
        .map_err(|_| Error::not_ready("keyword passage identity unavailable"))?;
    let source_id = schema
        .get_field("source_id")
        .map_err(|_| Error::not_ready("keyword source field unavailable"))?;
    let mut parser = QueryParser::for_index(keyword_index, vec![title, heading, body]);
    parser.set_field_boost(title, generation.profile.title_boost as f32);
    parser.set_field_boost(heading, generation.profile.heading_boost as f32);
    parser.set_field_boost(body, generation.profile.body_boost as f32);
    let parsed = keyword_query(&parser, query);
    let catalog_by_passage = generation
        .catalog
        .iter()
        .map(|record| (record.passage_id.as_str(), record))
        .collect::<HashMap<_, _>>();
    let searcher = keyword_index
        .reader()
        .map_err(|e| Error::not_ready(format!("open keyword reader: {e}")))?
        .searcher();
    let mut hits = Vec::new();
    let top_docs = searcher
        .search(
            &parsed,
            &TopDocs::with_limit(generation.profile.candidate_depth.max(1)).order_by_score(),
        )
        .map_err(|error| Error::not_ready(format!("keyword search failed: {error}")))?;
    for (score, address) in top_docs {
        let document: TantivyDocument = searcher
            .doc(address)
            .map_err(|error| Error::not_ready(format!("read keyword document: {error}")))?;
        let Some(passage) = document
            .get_first(passage_id)
            .and_then(|value| value.as_str())
        else {
            continue;
        };
        let document_source = document
            .get_first(source_id)
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        if source.is_some_and(|expected| expected != document_source) {
            continue;
        }
        if let Some(record) = catalog_by_passage.get(passage).copied() {
            hits.push((score, record));
        }
    }
    Ok(hits)
}

/// Builds the keyword query for user text. The text is plain text, never Tantivy's query language:
/// syntax characters, `field:` prefixes and the words `AND`, `OR`, `NOT` and `IN` are searched for
/// like any other word, so no input is a syntax error. Only double quotes keep a meaning, to mark
/// a phrase (see "Keyword queries" in the README).
fn keyword_query(parser: &QueryParser, text: &str) -> Box<dyn Query> {
    // A quoted literal cannot be a syntax error. The strict parser still rejects one input: text
    // whose literals all analyze to no terms (`+ :`) counts as "only excluding terms". The
    // lenient parse reports that too, and then returns a query that matches every document, so it
    // is replaced by one that matches none. Any other error leaves a best-effort query: user text
    // never becomes an error.
    let (query, errors) = parser.parse_query_lenient(&quote_literals(text));
    if errors.contains(&QueryParserError::AllButQueryForbidden) {
        return Box::new(EmptyQuery);
    }
    query
}

/// Rewrites `text` as a sequence of double-quoted literals for the Tantivy parser: one per
/// whitespace-separated word and one per phrase the user quoted.
///
/// Inside quotes only `"` and `\` can change the parse. The first ends the literal, so the
/// rewrite splits there, and the second is replaced with a space. The index's analyzer then splits
/// every literal into terms exactly as it did for the bare word, so a query without syntax builds
/// the same query, and ranks the same, as before. A literal with several terms (`std::fs`,
/// `rust-analyzer`) matches them as a phrase. An unterminated quote runs to the end of the text.
fn quote_literals(text: &str) -> String {
    fn flush(literal: &mut String, rewritten: &mut String) {
        let literal_text = literal.trim();
        if !literal_text.is_empty() {
            if !rewritten.is_empty() {
                rewritten.push(' ');
            }
            rewritten.push('"');
            rewritten.push_str(literal_text);
            rewritten.push('"');
        }
        literal.clear();
    }
    let mut rewritten = String::with_capacity(text.len() + 8);
    let mut literal = String::new();
    let mut in_phrase = false;
    for ch in text.chars() {
        match ch {
            '"' => {
                flush(&mut literal, &mut rewritten);
                in_phrase = !in_phrase;
            }
            '\\' => literal.push(' '),
            ch if ch.is_whitespace() && !in_phrase => flush(&mut literal, &mut rewritten),
            ch if ch.is_whitespace() => literal.push(' '),
            ch => literal.push(ch),
        }
    }
    flush(&mut literal, &mut rewritten);
    rewritten
}

pub(crate) fn semantic_hits_for_query<'a>(
    generation: &'a LoadedGeneration,
    query: &str,
    source: Option<&str>,
) -> Result<Vec<(f32, &'a CatalogRecord)>, Error> {
    let dimensions = generation
        .semantic_dimensions
        .ok_or_else(|| Error::not_ready("semantic search is unavailable in this Generation"))?;
    let semantic_index = generation
        .semantic_index
        .as_ref()
        .ok_or_else(|| Error::not_ready("semantic search is unavailable in this Generation"))?;
    let query_vector = generation.query_embedder.embed(
        query,
        &generation.profile,
        &generation.model_cache_dir,
        dimensions,
    )?;
    let by_key = generation
        .catalog
        .iter()
        .map(|record| (record.vector_key, record))
        .collect::<HashMap<_, _>>();
    let mut hits = Vec::new();
    let matches = semantic_index
        .search(&query_vector, generation.profile.candidate_depth.max(1))
        .map_err(|error| Error::not_ready(format!("semantic search failed: {error}")))?;
    for (key, distance) in matches.keys.iter().zip(matches.distances.iter()) {
        let Some(record) = by_key.get(key).copied() else {
            continue;
        };
        if source.is_some_and(|source| source != record.source_id) {
            continue;
        }
        let score = 1.0 - distance;
        hits.push((score, record));
    }
    hits.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.1.passage_id.cmp(&b.1.passage_id))
    });
    Ok(hits)
}

pub(crate) fn hybrid_hits<'a>(
    generation: &'a LoadedGeneration,
    query: &str,
    source: Option<&str>,
) -> Result<Vec<(f32, &'a CatalogRecord)>, Error> {
    let keyword = keyword_hits(generation, query, source)?;
    let semantic = semantic_hits_for_query(generation, query, source)?;
    let depth = generation
        .profile
        .fusion_candidate_depth
        .ok_or_else(|| Error::not_ready("fusion candidate depth missing from Generation profile"))?
        .clamp(1, 100);
    let rrf_k = generation
        .profile
        .rrf_k
        .ok_or_else(|| Error::not_ready("RRF k missing from Generation profile"))?
        as f32;
    if generation.profile.fusion_algorithm.as_deref() != Some("rrf") {
        return Err(Error::invalid("only configured rrf fusion is supported"));
    }
    let mut fused = HashMap::<String, (f32, &'a CatalogRecord)>::new();
    for (rank, (_, record)) in keyword.into_iter().take(depth).enumerate() {
        let score = 1.0 / (rrf_k + rank as f32 + 1.0);
        let entry = fused
            .entry(record.passage_id.clone())
            .or_insert((0.0, record));
        entry.0 += score;
    }
    for (rank, (_, record)) in semantic.into_iter().take(depth).enumerate() {
        let score = 1.0 / (rrf_k + rank as f32 + 1.0);
        let entry = fused
            .entry(record.passage_id.clone())
            .or_insert((0.0, record));
        entry.0 += score;
    }
    let mut hits = fused.into_values().collect::<Vec<_>>();
    hits.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.1.passage_id.cmp(&b.1.passage_id))
    });
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::doc;
    use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};

    const DOCS: &[(&str, &str, &str)] = &[
        (
            "Rust standard library",
            "std::fs",
            "Use std::fs::read_to_string to read a file. The --verbose flag prints more.",
        ),
        (
            "Command line flags",
            "Verbose output",
            "Pass --verbose to print more detail and --quiet to print less. foo:bar is a key value pair.",
        ),
        (
            "Kubernetes pods",
            "Pod lifecycle",
            "A pod moves through pending, running and succeeded. kubectl logs --previous shows the last container.",
        ),
        (
            "Rust analyzer",
            "rust-analyzer setup",
            "Install rust-analyzer for editor support. Vec::retain keeps the elements a closure accepts.",
        ),
        (
            "Boolean words",
            "AND OR NOT",
            "The words and, or, not and in are ordinary words in a sentence about rust.",
        ),
    ];

    /// A small index with the schema, analyzer and field boosts of a keyword Generation.
    struct Fixture {
        index: Index,
        parser: QueryParser,
        title: Field,
    }

    impl Fixture {
        fn new() -> Self {
            let options = TextOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_stored();
            let mut builder = Schema::builder();
            let title = builder.add_text_field("title", options.clone());
            let heading = builder.add_text_field("heading", options.clone());
            let body = builder.add_text_field("body", options);
            let index = Index::create_in_ram(builder.build());
            let mut writer = index.writer(15_000_000).expect("open index writer");
            for (doc_title, doc_heading, doc_body) in DOCS {
                writer
                    .add_document(
                        doc!(title => *doc_title, heading => *doc_heading, body => *doc_body),
                    )
                    .expect("add document");
            }
            writer.commit().expect("commit documents");
            let mut parser = QueryParser::for_index(&index, vec![title, heading, body]);
            parser.set_field_boost(title, 2.0);
            parser.set_field_boost(heading, 1.5);
            parser.set_field_boost(body, 1.0);
            Self {
                index,
                parser,
                title,
            }
        }

        /// What `keyword_hits` runs: ranked (score, title) pairs.
        fn search(&self, text: &str) -> Vec<(f32, String)> {
            self.ranked(keyword_query(&self.parser, text).as_ref())
        }

        /// The query parser as `keyword_hits` used it before user text was rewritten.
        fn legacy_search(&self, text: &str) -> Result<Vec<(f32, String)>, String> {
            let query = self.parser.parse_query(text).map_err(|e| e.to_string())?;
            Ok(self.ranked(query.as_ref()))
        }

        fn titles(&self, text: &str) -> Vec<String> {
            self.search(text)
                .into_iter()
                .map(|(_, title)| title)
                .collect()
        }

        fn ranked(&self, query: &dyn Query) -> Vec<(f32, String)> {
            let searcher = self.index.reader().expect("open reader").searcher();
            searcher
                .search(query, &TopDocs::with_limit(10).order_by_score())
                .expect("search the fixture index")
                .into_iter()
                .map(|(score, address)| {
                    let document: TantivyDocument = searcher.doc(address).expect("read document");
                    let title = document
                        .get_first(self.title)
                        .and_then(|value| value.as_str())
                        .expect("stored title");
                    (score, title.to_owned())
                })
                .collect()
        }
    }

    #[test]
    fn literals_are_quoted_words_and_phrases() {
        for (text, want) in [
            (
                "rebalance consumer group",
                r#""rebalance" "consumer" "group""#,
            ),
            ("  spaced \t out\n", r#""spaced" "out""#),
            ("std::fs", r#""std::fs""#),
            ("--verbose", r#""--verbose""#),
            ("foo:bar title:rust", r#""foo:bar" "title:rust""#),
            ("rust AND NOT python", r#""rust" "AND" "NOT" "python""#),
            (r#""exact phrase" other"#, r#""exact phrase" "other""#),
            ("say \"hello  world", r#""say" "hello  world""#),
            (r#"say"hi there"now"#, r#""say" "hi there" "now""#),
            (r"back\slash \", r#""back slash""#),
            ("\"\" \"   \"", ""),
            ("", ""),
            ("   ", ""),
        ] {
            assert_eq!(quote_literals(text), want, "rewrite of {text:?}");
        }
    }

    /// Each input used to be rejected by the parser (`legacy_search` is an error) and is now
    /// searched as text: the expected top hit, or no hit when the text matches nothing.
    #[test]
    fn text_that_used_to_be_a_syntax_error_returns_results() {
        let fixture = Fixture::new();
        for (text, want) in [
            ("std::fs", Some("Rust standard library")),
            ("--verbose", Some("Command line flags")),
            ("foo:bar", Some("Command line flags")),
            ("Vec::retain", Some("Rust analyzer")),
            ("kubectl logs --previous", Some("Kubernetes pods")),
            ("rust AND", Some("Rust analyzer")),
            ("OR rust", Some("Rust analyzer")),
            ("NOT", Some("Boolean words")),
            ("IN", Some("Boolean words")),
            ("\"unbalanced", None),
            ("pod \"lifecycle", Some("Kubernetes pods")),
            ("(", None),
            ("rust)", Some("Rust analyzer")),
            ("---", None),
            ("a^b", None),
            ("'", None),
            ("\\", None),
            ("/regex/", None),
        ] {
            assert!(
                fixture.legacy_search(text).is_err(),
                "{text:?} is no longer a syntax error: drop it from this table"
            );
            let titles = fixture.titles(text);
            assert_eq!(
                titles.first().map(String::as_str),
                want,
                "top hit for {text:?}, all hits: {titles:?}"
            );
        }
    }

    #[test]
    fn operator_syntax_is_searched_as_text() {
        let fixture = Fixture::new();
        // A field prefix, a wildcard and `-` are words, not a field filter, match-all or exclusion.
        assert!(fixture.titles("title:rust").is_empty());
        assert!(fixture.titles("*").is_empty());
        assert_eq!(fixture.titles("-verbose")[0], "Command line flags");
        // AND and NOT are words that match like any other: the result is a union, not an
        // intersection, and `NOT` excludes nothing.
        let titles = fixture.titles("kubectl AND NOT verbose");
        assert!(titles.contains(&"Kubernetes pods".to_owned()), "{titles:?}");
        assert!(
            titles.contains(&"Command line flags".to_owned()),
            "{titles:?}"
        );
        // Quotes are the one operator: a phrase must match as written, bare words may be apart.
        assert_eq!(fixture.titles("\"rust analyzer\""), ["Rust analyzer"]);
        assert!(fixture.titles("rust analyzer").len() > 1);
        assert!(fixture.titles("\"analyzer rust\"").is_empty());
    }

    /// Text with no syntax parses to the same query as before, so it ranks identically.
    #[test]
    fn queries_without_syntax_rank_as_before() {
        let fixture = Fixture::new();
        for text in [
            "rust",
            "Rust Analyzer",
            "rust-analyzer",
            "verbose output",
            "pod lifecycle pending",
            "kubectl logs",
            "READ A FILE",
            "pending running succeeded container",
            "keeps elements closure",
            "über naïve",
            "snake_case 2024 v1.2.3",
            "nothing matches this",
        ] {
            let before = fixture.legacy_search(text).expect("legacy parse");
            let after = fixture.search(text);
            assert_eq!(
                before.iter().map(|(_, title)| title).collect::<Vec<_>>(),
                after.iter().map(|(_, title)| title).collect::<Vec<_>>(),
                "ranking of {text:?}"
            );
            for ((old, _), (new, _)) in before.iter().zip(&after) {
                assert!(
                    (old - new).abs() < 1e-5,
                    "score of {text:?}: {old} vs {new}"
                );
            }
        }
    }

    /// No input, however hostile, may fail to parse. The one thing the rewrite leaves to the lenient
    /// fallback in `keyword_query` is text with no terms at all, which must match nothing (not
    /// every document).
    #[test]
    fn no_text_is_a_syntax_error() {
        let fixture = Fixture::new();
        const PIECES: &[&str] = &[
            "\"",
            "\\",
            "'",
            "(",
            ")",
            "[",
            "]",
            "{",
            "}",
            ":",
            "^",
            "~",
            "*",
            "/",
            "+",
            "-",
            "!",
            " ",
            "\t",
            "\n",
            "\u{0}",
            "AND",
            "OR",
            "NOT",
            "IN",
            "TO",
            "title:",
            "body:",
            "a",
            "rust",
            "é",
            "😀",
            "\"a b\"~2",
            "[1 TO 5]",
            "x^2",
            "-(",
            ">=",
            "&&",
            "||",
        ];
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..5_000 {
            let text = (0..=next() % 12)
                .map(|_| PIECES[(next() % PIECES.len() as u64) as usize])
                .collect::<String>();
            let rewritten = quote_literals(&text);
            let (_, errors) = fixture.parser.parse_query_lenient(&rewritten);
            assert!(
                errors.is_empty() || errors == [QueryParserError::AllButQueryForbidden],
                "{text:?} rewritten to {rewritten:?}: {errors:?}"
            );
            if !errors.is_empty() {
                assert!(fixture.search(&text).is_empty(), "{text:?} has no terms");
            }
            fixture.search(&text);
        }
    }
}
