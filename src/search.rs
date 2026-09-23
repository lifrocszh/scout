use super::generation::{
    embed_query, open_semantic_index, read_pointer, read_semantic_index, validate_generation,
    validate_pointer,
};
use super::storage::parse_jsonl;
use super::{
    BuildProfile, CatalogRecord, Error, HashMap, Index, LoadedGeneration, Ordering, Path,
    QueryParser, SEMANTIC_VECTOR_ARTIFACT, TantivyDocument, TantivyValue, TopDocs, fs,
};

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
        let Ok(semantic) = read_semantic_index(&path.join(SEMANTIC_VECTOR_ARTIFACT)) else {
            continue;
        };
        let Ok(mut keyword_index) = Index::open_in_dir(path.join(&manifest.keyword_artifact))
        else {
            continue;
        };
        if register_keyword_analyzer(&mut keyword_index, &profile).is_err() {
            continue;
        }
        let Ok(semantic_index) =
            open_semantic_index(&path.join(&manifest.semantic_artifact), &profile)
        else {
            continue;
        };
        return Ok(LoadedGeneration {
            manifest,
            profile,
            catalog,
            semantic,
            keyword_index,
            semantic_index,
            model_cache_dir: data_dir.join("models"),
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
    let schema = generation.keyword_index.schema();
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
    let mut parser = QueryParser::for_index(&generation.keyword_index, vec![title, heading, body]);
    parser.set_field_boost(title, generation.profile.title_boost as f32);
    parser.set_field_boost(heading, generation.profile.heading_boost as f32);
    parser.set_field_boost(body, generation.profile.body_boost as f32);
    let parsed = parser
        .parse_query(query)
        .map_err(|error| Error::invalid(format!("invalid keyword query: {error}")))?;
    let catalog_by_passage = generation
        .catalog
        .iter()
        .map(|record| (record.passage_id.as_str(), record))
        .collect::<HashMap<_, _>>();
    let searcher = generation
        .keyword_index
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

pub(crate) fn register_keyword_analyzer(
    index: &mut Index,
    profile: &BuildProfile,
) -> Result<(), Error> {
    let analyzer = index
        .tokenizers()
        .get("default")
        .ok_or_else(|| Error::not_ready("Tantivy default analyzer unavailable"))?;
    index.tokenizers().register(&profile.analyzer_id, analyzer);
    Ok(())
}

pub(crate) fn semantic_hits_for_query<'a>(
    generation: &'a LoadedGeneration,
    query: &str,
    source: Option<&str>,
) -> Result<Vec<(f32, &'a CatalogRecord)>, Error> {
    let dimensions = generation.semantic.dimensions;
    let query_vector = embed_query(
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
    let matches = generation
        .semantic_index
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
