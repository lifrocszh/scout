use super::super::search::load_active_generation;
use super::super::storage::trim_snippet;
use super::super::{BTreeMap, CatalogRecord, Error, LoadedGeneration, Ordering, Path, Value};
use super::{activate_generation, build_generation, prune_generations, recover_generation};

pub(crate) struct IndexGeneration {
    data_dir: std::path::PathBuf,
}

pub(crate) enum LifecycleAction<'a> {
    Activate { generation_id: &'a str },
    Recover,
    Prune { explicitly_retained: &'a [String] },
}

pub(crate) struct ActiveGeneration {
    inner: LoadedGeneration,
}

#[derive(Debug, Clone)]
pub(crate) struct GenerationInfo {
    pub(crate) generation_id: String,
    pub(crate) corpus_snapshot_id: String,
    pub(crate) manifest_sha256: String,
    pub(crate) page_count: usize,
    pub(crate) passage_count: usize,
    pub(crate) alias_count: usize,
    pub(crate) keyword_count: usize,
    pub(crate) semantic_count: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SearchMode {
    Keyword,
    Semantic,
    Hybrid,
}

impl SearchMode {
    pub(crate) fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "keyword" => Ok(Self::Keyword),
            "semantic" => Ok(Self::Semantic),
            "hybrid" => Ok(Self::Hybrid),
            _ => Err(Error::invalid("mode must be keyword, semantic, or hybrid")),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Keyword => "keyword",
            Self::Semantic => "semantic",
            Self::Hybrid => "hybrid",
        }
    }
}

pub(crate) struct PageQuery<'a> {
    pub(crate) query: &'a str,
    pub(crate) mode: SearchMode,
    pub(crate) source: Option<&'a str>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct PageSearchResult {
    pub(crate) rank: usize,
    pub(crate) page_id: String,
    pub(crate) source_id: String,
    pub(crate) url: String,
    pub(crate) title: String,
    pub(crate) score: f32,
    pub(crate) winning_passage_id: String,
    pub(crate) heading_path: Vec<String>,
    pub(crate) snippet: String,
}

pub(crate) struct PageResults {
    pub(crate) generation: GenerationInfo,
    pub(crate) mode: SearchMode,
    pub(crate) pages: Vec<PageSearchResult>,
}

impl IndexGeneration {
    pub(crate) fn at(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
        }
    }

    pub(crate) fn build(&self, corpus_id: &str, run_id: &str) -> Result<Value, Error> {
        build_generation(&self.data_dir, corpus_id, run_id, false)
    }

    pub(crate) fn resume_build(&self, corpus_id: &str, run_id: &str) -> Result<Value, Error> {
        build_generation(&self.data_dir, corpus_id, run_id, true)
    }

    pub(crate) fn lifecycle(&self, action: LifecycleAction<'_>) -> Result<Value, Error> {
        match action {
            LifecycleAction::Activate { generation_id } => {
                activate_generation(&self.data_dir, generation_id)
            }
            LifecycleAction::Recover => recover_generation(&self.data_dir),
            LifecycleAction::Prune {
                explicitly_retained,
            } => prune_generations(&self.data_dir, explicitly_retained),
        }
    }

    pub(crate) fn open_active(&self) -> Result<ActiveGeneration, Error> {
        load_active_generation(&self.data_dir).map(|inner| ActiveGeneration { inner })
    }
}

impl ActiveGeneration {
    pub(crate) fn info(&self) -> GenerationInfo {
        GenerationInfo {
            generation_id: self.inner.manifest.generation_id.clone(),
            corpus_snapshot_id: self.inner.manifest.corpus_snapshot_id.clone(),
            manifest_sha256: self.inner.manifest.manifest_sha256.clone(),
            page_count: self.inner.manifest.page_count,
            passage_count: self.inner.manifest.passage_count,
            alias_count: self.inner.manifest.alias_count,
            keyword_count: self.inner.manifest.keyword_count,
            semantic_count: self.inner.manifest.semantic_count,
        }
    }

    pub(crate) fn search(&self, request: PageQuery<'_>) -> Result<PageResults, Error> {
        let query = request.query.trim();
        if query.is_empty() || query.chars().count() > 10_000 {
            return Err(Error::invalid(
                "query must be nonempty and at most 10000 characters",
            ));
        }
        if request
            .limit
            .is_some_and(|limit| !(1..=100).contains(&limit))
        {
            return Err(Error::invalid("limit must be between 1 and 100"));
        }

        let hits = match request.mode {
            SearchMode::Keyword => {
                super::super::search::keyword_hits(&self.inner, query, request.source)?
            }
            SearchMode::Semantic => {
                super::super::search::semantic_hits_for_query(&self.inner, query, request.source)?
            }
            SearchMode::Hybrid => {
                super::super::search::hybrid_hits(&self.inner, query, request.source)?
            }
        };
        let snippet_window =
            self.inner.profile.snippet_window.ok_or_else(|| {
                Error::not_ready("snippet window missing from Generation profile")
            })?;
        let mut pages = BTreeMap::<String, (f32, &CatalogRecord)>::new();
        for (score, record) in hits {
            let entry = pages
                .entry(record.page_id.clone())
                .or_insert((score, record));
            if score > entry.0 || (score == entry.0 && record.passage_id < entry.1.passage_id) {
                *entry = (score, record);
            }
        }
        let mut results = pages
            .into_iter()
            .map(|(page_id, (score, record))| PageSearchResult {
                rank: 0,
                page_id,
                source_id: record.source_id.clone(),
                url: record.normalized_final_url.clone(),
                title: record.title.clone(),
                score,
                winning_passage_id: record.passage_id.clone(),
                heading_path: record.heading_path.clone(),
                snippet: trim_snippet(&record.text, snippet_window),
            })
            .collect::<Vec<_>>();
        results.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.winning_passage_id.cmp(&right.winning_passage_id))
                .then_with(|| left.page_id.cmp(&right.page_id))
        });
        if let Some(limit) = request.limit {
            results.truncate(limit);
        }
        for (index, result) in results.iter_mut().enumerate() {
            result.rank = index + 1;
        }
        Ok(PageResults {
            generation: self.info(),
            mode: request.mode,
            pages: results,
        })
    }

    pub(crate) fn ranked_page_ids(
        &self,
        mode: SearchMode,
        query: &str,
        source: Option<&str>,
    ) -> Result<Vec<String>, Error> {
        Ok(self
            .search(PageQuery {
                query,
                mode,
                source,
                limit: None,
            })?
            .pages
            .into_iter()
            .map(|page| page.page_id)
            .collect())
    }
}
