use super::app::report_error;
use super::observability::Events;
use super::search::{hybrid_hits, keyword_hits, load_active_generation, semantic_hits_for_query};
use super::storage::{new_run_id, parse_jsonl, sha256_hex, write_json};
use super::{
    BTreeMap, BTreeSet, BootstrapInterval, Cli, EVALUATION_BOOTSTRAP_ITERATIONS,
    EVALUATION_BOOTSTRAP_SEED, EVALUATION_DEVELOPMENT_QUERY_COUNT, EVALUATION_HELD_OUT_QUERY_COUNT,
    EVALUATION_INTENTS, EVALUATION_QUERY_COUNT, EVALUATION_SCHEMA, EVALUATION_SOURCES, Error,
    EvaluationJudgment, EvaluationQuery, EvaluationRanking, HashMap, HashSet, LoadedGeneration,
    Operation, Ordering, Path, QueryMetrics, Value, fs, json,
};

type PageGrades = BTreeMap<String, BTreeMap<String, u8>>;
type NormalizedJudgments = (PageGrades, Value);

pub(crate) fn run_evaluate(cli: &Cli) -> i32 {
    let Operation::Evaluate { package } = &cli.operation else {
        unreachable!("evaluation operation required")
    };
    let run_id = new_run_id();
    let mut events = match Events::open(&cli.data_dir, &run_id, "evaluation") {
        Ok(events) => events,
        Err(error) => {
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    let result = evaluate_package(package, &cli.data_dir, &run_id);
    match result {
        Ok(report) => {
            events.emit("evaluation.completed", "success", report.clone());
            println!(
                "evaluation complete: run_id={run_id} report={}",
                report["report_path"].as_str().unwrap_or_default()
            );
            0
        }
        Err(error) => {
            events.emit(
                "evaluation.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(cli, &error);
            error.exit_code
        }
    }
}

pub(crate) fn metadata_string(metadata: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        metadata
            .get(*key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|value| !value.trim().is_empty())
    })
}

pub(crate) fn load_evaluation_metadata(package: &Path) -> Result<Value, Error> {
    let path = package.join("metadata.json");
    let bytes = fs::read(&path)
        .map_err(|e| Error::invalid(format!("read evaluation metadata {}: {e}", path.display())))?;
    let metadata: Value = serde_json::from_slice(&bytes)
        .map_err(|e| Error::invalid(format!("parse evaluation metadata: {e}")))?;
    let Some(object) = metadata.as_object() else {
        return Err(Error::invalid("evaluation metadata must be a JSON object"));
    };
    for (name, keys) in [
        ("evaluation_id", ["evaluation_id"].as_slice()),
        (
            "corpus_snapshot_id",
            ["corpus_snapshot_id", "corpus_snapshot"].as_slice(),
        ),
        (
            "build_id",
            ["build_id", "build_profile_digest", "build_profile_sha256"].as_slice(),
        ),
    ] {
        if metadata_string(&metadata, keys).is_none() {
            return Err(Error::invalid(format!(
                "evaluation metadata field `{name}` is required"
            )));
        }
    }
    if metadata_string(
        &metadata,
        &[
            "model_digest",
            "model_profile_digest",
            "model_artifact_digest",
            "model_artifacts_sha256",
        ],
    )
    .is_none()
        && !object.get("model_artifacts").is_some_and(Value::is_object)
    {
        return Err(Error::invalid(
            "evaluation metadata must record model artifact provenance",
        ));
    }
    let judges = metadata
        .get("judges")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("evaluation metadata judges are required"))?;
    let mut judge_ids = HashSet::new();
    for judge in judges {
        let Some(judge) = judge.as_str() else {
            return Err(Error::invalid(
                "evaluation metadata judge IDs must be strings",
            ));
        };
        if judge.trim().is_empty() || !judge_ids.insert(judge) {
            return Err(Error::invalid(
                "evaluation metadata judge IDs must be nonempty and unique",
            ));
        }
    }
    if judges.len() < 2 {
        return Err(Error::invalid(
            "evaluation metadata must record at least two independent judges",
        ));
    }
    let adjudication = metadata_string(&metadata, &["adjudication", "adjudication_method"])
        .ok_or_else(|| Error::invalid("evaluation metadata adjudication is required"))?;
    if adjudication.to_ascii_lowercase().contains("starter") {
        return Err(Error::invalid(
            "starter candidates must not participate in adjudication",
        ));
    }
    if let Some(unit) = metadata_string(&metadata, &["judgment_unit"])
        && !unit.eq_ignore_ascii_case("page")
    {
        return Err(Error::invalid("evaluation judgments must be Page-level"));
    }
    if metadata
        .get("starter_candidates_are_labels")
        .and_then(Value::as_bool)
        .is_some_and(|value| value)
        || metadata
            .get("expected_page_candidates_used_as_labels")
            .and_then(Value::as_bool)
            .is_some_and(|value| value)
    {
        return Err(Error::invalid(
            "starter candidates must not be treated as labels",
        ));
    }
    Ok(metadata)
}

pub(crate) fn normalize_evaluation_judgments(
    rows: Vec<EvaluationJudgment>,
    queries: &[EvaluationQuery],
    metadata: &Value,
) -> Result<NormalizedJudgments, Error> {
    let query_ids = queries
        .iter()
        .map(|query| query.query_id.as_str())
        .collect::<HashSet<_>>();
    let judges = metadata
        .get("judges")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<HashSet<_>>();
    let mut final_grades = PageGrades::new();
    let mut independent = BTreeMap::<(String, String), BTreeSet<String>>::new();
    let mut independent_rows = 0_usize;
    let mut adjudicated_rows = 0_usize;

    for row in rows {
        if row.query_id.trim().is_empty() || row.page_id.trim().is_empty() {
            return Err(Error::invalid(
                "judgment query_id and page_id must be nonempty",
            ));
        }
        if !query_ids.contains(row.query_id.as_str()) {
            return Err(Error::invalid(format!(
                "judgment references unknown query {}",
                row.query_id
            )));
        }
        for grade in row.grade.into_iter().chain(row.adjudicated_grade) {
            if grade > 3 {
                return Err(Error::invalid("judgment grade must be between 0 and 3"));
            }
        }
        for (judge_id, grade) in &row.judge_grades {
            if !judges.contains(judge_id.as_str()) || *grade > 3 {
                return Err(Error::invalid(
                    "independent judgment has unknown judge or invalid grade",
                ));
            }
            let entry = independent
                .entry((row.query_id.clone(), row.page_id.clone()))
                .or_default();
            if !entry.insert(judge_id.clone()) {
                return Err(Error::invalid("duplicate independent Page judgment"));
            }
            independent_rows += 1;
        }
        if let Some(judge_id) = row.judge_id.as_deref() {
            if !judges.contains(judge_id) {
                return Err(Error::invalid(
                    "independent judgment references unknown judge",
                ));
            }
            if row.grade.is_none() {
                return Err(Error::invalid(
                    "independent judgment row must contain grade",
                ));
            }
            let entry = independent
                .entry((row.query_id.clone(), row.page_id.clone()))
                .or_default();
            if !entry.insert(judge_id.into()) {
                return Err(Error::invalid("duplicate independent Page judgment"));
            }
            independent_rows += 1;
            if row.adjudicated_grade.is_some() {
                adjudicated_rows += 1;
            }
        }
        let final_grade = if row.judge_id.is_none() && row.judge_grades.is_empty() {
            row.adjudicated_grade.or(row.grade)
        } else {
            row.adjudicated_grade
                .or_else(|| row.judge_id.is_none().then_some(row.grade).flatten())
        };
        if row.judge_id.is_none() && !row.judge_grades.is_empty() && final_grade.is_some() {
            adjudicated_rows += 1;
        }
        if let Some(grade) = final_grade
            && final_grades
                .entry(row.query_id)
                .or_default()
                .insert(row.page_id, grade)
                .is_some()
        {
            return Err(Error::invalid("duplicate adjudicated Page judgment"));
        }
    }

    for ((query_id, page_id), judged_by) in &independent {
        if judged_by.len() < judges.len() {
            return Err(Error::invalid(format!(
                "Page judgment {query_id}/{page_id} lacks independent judge coverage"
            )));
        }
        if !final_grades
            .get(query_id)
            .is_some_and(|pages| pages.contains_key(page_id))
        {
            return Err(Error::invalid(format!(
                "Page judgment {query_id}/{page_id} lacks adjudication"
            )));
        }
    }
    for query in queries {
        if !final_grades.contains_key(&query.query_id) {
            return Err(Error::invalid(format!(
                "missing judgments for {}",
                query.query_id
            )));
        }
    }
    let format = if independent_rows == 0 {
        "adjudicated-page"
    } else {
        "independent-plus-adjudicated"
    };
    Ok((
        final_grades,
        json!({
            "format": format,
            "judge_count": judges.len(),
            "judges": metadata.get("judges").cloned().unwrap_or(Value::Null),
            "adjudication": metadata_string(metadata, &["adjudication", "adjudication_method"]),
            "independent_judgment_rows": independent_rows,
            "adjudicated_page_judgments": adjudicated_rows,
            "page_level": true,
            "starter_candidates_used_as_labels": false,
        }),
    ))
}

fn evaluate_package(package: &Path, data_dir: &Path, run_id: &str) -> Result<Value, Error> {
    let metadata = load_evaluation_metadata(package)?;
    let metadata_bytes = fs::read(package.join("metadata.json"))
        .map_err(|e| Error::invalid(format!("read evaluation metadata: {e}")))?;
    let query_bytes = fs::read(package.join("queries.jsonl"))
        .map_err(|e| Error::invalid(format!("read evaluation queries: {e}")))?;
    let queries = parse_jsonl::<EvaluationQuery>(&query_bytes, "queries.jsonl")?;
    validate_evaluation_queries(&queries)?;
    let judgment_bytes = fs::read(package.join("judgments.jsonl"))
        .map_err(|e| Error::invalid(format!("read evaluation judgments: {e}")))?;
    let judgment_rows = parse_jsonl::<EvaluationJudgment>(&judgment_bytes, "judgments.jsonl")?;
    let (judgments, judgment_protocol) =
        normalize_evaluation_judgments(judgment_rows, &queries, &metadata)?;
    let ranking_path = package.join("rankings.jsonl");
    let ranking_bytes = if ranking_path.exists() {
        Some(
            fs::read(&ranking_path)
                .map_err(|e| Error::invalid(format!("read evaluation rankings: {e}")))?,
        )
    } else {
        None
    };
    let supplied = if let Some(ranking_bytes) = &ranking_bytes {
        parse_jsonl::<EvaluationRanking>(ranking_bytes, "rankings.jsonl")?
            .into_iter()
            .try_fold(HashMap::new(), |mut rankings, ranking| {
                if rankings.insert(ranking.query_id.clone(), ranking).is_some() {
                    return Err(Error::invalid(
                        "rankings.jsonl contains duplicate query IDs",
                    ));
                }
                Ok(rankings)
            })?
    } else {
        HashMap::new()
    };
    if ranking_bytes.is_some()
        && (supplied.len() != queries.len()
            || queries
                .iter()
                .any(|query| !supplied.contains_key(&query.query_id)))
    {
        return Err(Error::invalid(
            "rankings.jsonl must contain exactly one ranking row per query",
        ));
    }
    if ranking_bytes.is_some() && supplied.is_empty() {
        return Err(Error::invalid("rankings.jsonl must not be empty"));
    }
    if !supplied.is_empty()
        && (supplied.len() != queries.len()
            || queries
                .iter()
                .any(|query| !supplied.contains_key(&query.query_id)))
    {
        return Err(Error::invalid(
            "rankings.jsonl must contain exactly one ranking row per query",
        ));
    }
    let generation = if ranking_bytes.is_none() {
        Some(load_active_generation(data_dir)?)
    } else {
        None
    };
    let mut per_mode = BTreeMap::<String, Vec<QueryMetrics>>::new();
    for query in &queries {
        let query_judgments = judgments
            .get(&query.query_id)
            .ok_or_else(|| Error::invalid(format!("missing judgments for {}", query.query_id)))?;
        if !query_judgments.values().any(|grade| *grade >= 2) {
            return Err(Error::invalid(format!(
                "{} has no useful Page judgment",
                query.query_id
            )));
        }
        let ranking = if let Some(ranking) = supplied.get(&query.query_id) {
            vec![
                ranking.keyword.clone(),
                ranking.semantic.clone(),
                ranking.hybrid.clone(),
            ]
        } else {
            let generation = generation
                .as_ref()
                .expect("generated when no supplied rankings");
            vec![
                ranked_page_ids(generation, "keyword", &query.query, Some(&query.source))?,
                ranked_page_ids(generation, "semantic", &query.query, Some(&query.source))?,
                ranked_page_ids(generation, "hybrid", &query.query, Some(&query.source))?,
            ]
        };
        for (mode, ids) in ["keyword", "semantic", "hybrid"].into_iter().zip(ranking) {
            let refs = ids.iter().map(String::as_str).collect::<Vec<_>>();
            per_mode
                .entry(mode.into())
                .or_default()
                .push(compute_query_metrics(&refs, query_judgments)?);
        }
    }
    let aggregates = per_mode
        .iter()
        .map(|(mode, metrics)| (mode.clone(), aggregate_metrics(metrics)))
        .collect::<BTreeMap<_, _>>();
    let held_out = queries
        .iter()
        .enumerate()
        .filter(|(_, query)| query.split == "held-out")
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let keyword_metrics = per_mode
        .get("keyword")
        .ok_or_else(|| Error::invalid("keyword metrics missing"))?;
    let semantic_metrics = per_mode
        .get("semantic")
        .ok_or_else(|| Error::invalid("semantic metrics missing"))?;
    let hybrid_metrics = per_mode
        .get("hybrid")
        .ok_or_else(|| Error::invalid("hybrid metrics missing"))?;
    let keyword_held_out = held_out
        .iter()
        .map(|index| keyword_metrics[*index])
        .collect::<Vec<_>>();
    let semantic_held_out = held_out
        .iter()
        .map(|index| semantic_metrics[*index])
        .collect::<Vec<_>>();
    let hybrid_held_out = held_out
        .iter()
        .map(|index| hybrid_metrics[*index])
        .collect::<Vec<_>>();
    let keyword_aggregate = aggregate_metrics(&keyword_held_out);
    let semantic_aggregate = aggregate_metrics(&semantic_held_out);
    let hybrid_aggregate = aggregate_metrics(&hybrid_held_out);
    let keyword_deltas = held_out
        .iter()
        .map(|index| hybrid_metrics[*index].ndcg_at_10 - keyword_metrics[*index].ndcg_at_10)
        .collect::<Vec<_>>();
    let semantic_deltas = held_out
        .iter()
        .map(|index| hybrid_metrics[*index].ndcg_at_10 - semantic_metrics[*index].ndcg_at_10)
        .collect::<Vec<_>>();
    let keyword_bootstrap = paired_bootstrap_ci(
        &keyword_deltas,
        EVALUATION_BOOTSTRAP_SEED,
        EVALUATION_BOOTSTRAP_ITERATIONS,
    );
    let semantic_bootstrap = paired_bootstrap_ci(
        &semantic_deltas,
        EVALUATION_BOOTSTRAP_SEED.wrapping_add(1),
        EVALUATION_BOOTSTRAP_ITERATIONS,
    );
    let held_out_gate = hybrid_aggregate.ndcg_at_10 >= keyword_aggregate.ndcg_at_10 + 0.03
        && hybrid_aggregate.ndcg_at_10 >= semantic_aggregate.ndcg_at_10 + 0.03
        && keyword_bootstrap.lower > 0.0
        && semantic_bootstrap.lower > 0.0
        && hybrid_aggregate.mrr_at_10 + 0.02
            >= keyword_aggregate
                .mrr_at_10
                .max(semantic_aggregate.mrr_at_10)
        && hybrid_aggregate.recall_at_100 + 0.02
            >= keyword_aggregate
                .recall_at_100
                .max(semantic_aggregate.recall_at_100);
    let diagnostics = evaluation_diagnostics(&queries, &per_mode);
    let metrics_by_split = evaluation_split_metrics(&queries, &per_mode);
    let evaluation_id = metadata_string(&metadata, &["evaluation_id"]);
    let corpus_snapshot_id = metadata_string(&metadata, &["corpus_snapshot_id", "corpus_snapshot"]);
    let build_id = metadata_string(
        &metadata,
        &["build_id", "build_profile_digest", "build_profile_sha256"],
    );
    let build_profile_digest =
        metadata_string(&metadata, &["build_profile_digest", "build_profile_sha256"]);
    let model_profile_digest = metadata_string(
        &metadata,
        &[
            "model_profile_digest",
            "model_digest",
            "model_artifact_digest",
            "model_artifacts_sha256",
        ],
    );
    let model_digest = metadata_string(
        &metadata,
        &[
            "model_digest",
            "model_profile_digest",
            "model_artifact_digest",
            "model_artifacts_sha256",
        ],
    );
    let adjudication = metadata_string(&metadata, &["adjudication", "adjudication_method"]);
    let judges = metadata.get("judges").cloned().unwrap_or(Value::Null);
    let query_strata = queries.iter().fold(
        BTreeMap::<String, BTreeMap<String, usize>>::new(),
        |mut strata, query| {
            *strata
                .entry("source".into())
                .or_default()
                .entry(query.source.clone())
                .or_default() += 1;
            *strata
                .entry("intent".into())
                .or_default()
                .entry(query.intent.clone())
                .or_default() += 1;
            strata
        },
    );
    let output_dir = data_dir.join("evaluations").join(run_id);
    fs::create_dir_all(&output_dir)
        .map_err(|e| Error::storage(format!("create evaluation output: {e}")))?;
    let report = json!({
        "schema_version": EVALUATION_SCHEMA,
        "run_id": run_id,
        "query_count": queries.len(),
        "development_query_count": queries.iter().filter(|query| query.split == "development").count(),
        "held_out_query_count": held_out.len(),
        "query_strata": query_strata,
        "metrics": aggregates,
        "metrics_by_split": metrics_by_split,
        "judgment_protocol": judgment_protocol,
        "starter_candidates_used_as_labels": false,
        "provenance": {
            "package_path": package,
            "metadata": metadata,
            "metadata_sha256": sha256_hex(&metadata_bytes),
            "evaluation_id": evaluation_id,
            "corpus_snapshot_id": corpus_snapshot_id,
            "build_id": build_id,
            "build_profile_digest": build_profile_digest,
            "model_digest": model_digest,
            "model_profile_digest": model_profile_digest,
            "judges": judges,
            "adjudication": adjudication,
            "queries_sha256": sha256_hex(&query_bytes),
            "judgments_sha256": sha256_hex(&judgment_bytes),
            "rankings_sha256": ranking_bytes.as_ref().map(|bytes| sha256_hex(bytes)),
            "ranking_source": if ranking_bytes.is_none() { "active_generation" } else { "rankings.jsonl" },
            "judgment_relevance_threshold": 2,
            "bootstrap_iterations": EVALUATION_BOOTSTRAP_ITERATIONS,
        },
        "held_out_gate": {
            "passed": held_out_gate,
            "thresholds": {"ndcg_uplift": 0.03, "max_mrr_regression": 0.02, "max_recall_regression": 0.02, "bootstrap_ci_lower": 0.0},
            "keyword": keyword_aggregate,
            "semantic": semantic_aggregate,
            "hybrid": hybrid_aggregate,
            "hybrid_minus_keyword": keyword_bootstrap,
            "hybrid_minus_semantic": semantic_bootstrap,
        },
        "diagnostics": diagnostics,
    });
    let report_path = output_dir.join("report.json");
    write_json(&report_path, &report)?;
    Ok(
        json!({"report_path": report_path, "query_count": queries.len(), "held_out_gate_passed": held_out_gate}),
    )
}

pub(crate) fn ranked_page_ids(
    generation: &LoadedGeneration,
    mode: &str,
    query: &str,
    source: Option<&str>,
) -> Result<Vec<String>, Error> {
    let hits = match mode {
        "keyword" => keyword_hits(generation, query, source)?,
        "semantic" => semantic_hits_for_query(generation, query, source)?,
        "hybrid" => hybrid_hits(generation, query, source)?,
        _ => return Err(Error::invalid(format!("unknown search mode {mode:?}"))),
    };
    let mut pages = Vec::<(f32, String, String)>::new();
    let mut seen = HashSet::new();
    for (score, record) in hits {
        if seen.insert(record.page_id.clone()) {
            pages.push((score, record.passage_id.clone(), record.page_id.clone()));
        }
    }
    pages.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    Ok(pages.into_iter().map(|(_, _, page_id)| page_id).collect())
}

pub(crate) fn validate_evaluation_queries(queries: &[EvaluationQuery]) -> Result<(), Error> {
    if queries.len() != EVALUATION_QUERY_COUNT {
        return Err(Error::invalid(format!(
            "evaluation package must contain exactly {EVALUATION_QUERY_COUNT} queries"
        )));
    }
    let mut ids = HashSet::new();
    let mut development = 0;
    let mut held_out = 0;
    let mut source_counts = BTreeMap::<String, usize>::new();
    let mut intent_counts = BTreeMap::<String, usize>::new();
    let mut strata_counts = BTreeMap::<(String, String), usize>::new();
    for query in queries {
        if query.query_id.is_empty()
            || query.query.trim().is_empty()
            || !ids.insert(query.query_id.clone())
        {
            return Err(Error::invalid(
                "evaluation query IDs and text must be nonempty and unique",
            ));
        }
        if !EVALUATION_SOURCES.contains(&query.source.as_str())
            || !EVALUATION_INTENTS.contains(&query.intent.as_str())
        {
            return Err(Error::invalid(
                "evaluation query source or intent is outside frozen strata",
            ));
        }
        *source_counts.entry(query.source.clone()).or_default() += 1;
        *intent_counts.entry(query.intent.clone()).or_default() += 1;
        *strata_counts
            .entry((query.source.clone(), query.intent.clone()))
            .or_default() += 1;
        match query.split.as_str() {
            "development" => development += 1,
            "held-out" => held_out += 1,
            _ => {
                return Err(Error::invalid(
                    "evaluation query split must be development or held-out",
                ));
            }
        }
    }
    if development != EVALUATION_DEVELOPMENT_QUERY_COUNT
        || held_out != EVALUATION_HELD_OUT_QUERY_COUNT
        || EVALUATION_SOURCES
            .iter()
            .any(|source| source_counts.get(*source).copied().unwrap_or(0) != 16)
        || EVALUATION_INTENTS
            .iter()
            .any(|intent| intent_counts.get(*intent).copied().unwrap_or(0) != 16)
        || EVALUATION_SOURCES.iter().any(|source| {
            EVALUATION_INTENTS.iter().any(|intent| {
                strata_counts
                    .get(&(source.to_string(), intent.to_string()))
                    .copied()
                    .unwrap_or(0)
                    != 4
            })
        })
    {
        return Err(Error::invalid(
            "evaluation package must contain balanced 16-query sources/intents and 16 development plus 48 held-out queries",
        ));
    }
    Ok(())
}

pub(crate) fn validate_benchmark_queries(queries: &[EvaluationQuery]) -> Result<(), Error> {
    if queries.len() != EVALUATION_QUERY_COUNT {
        return Err(Error::invalid(format!(
            "benchmark workload must contain exactly {EVALUATION_QUERY_COUNT} queries"
        )));
    }
    let mut ids = HashSet::new();
    let mut development = 0;
    let mut held_out = 0;
    for query in queries {
        if query.query_id.trim().is_empty()
            || query.query.trim().is_empty()
            || query.source.trim().is_empty()
            || query.intent.trim().is_empty()
            || !ids.insert(query.query_id.clone())
        {
            return Err(Error::invalid(
                "benchmark query IDs, source, intent, and text must be nonempty and unique",
            ));
        }
        match query.split.as_str() {
            "development" => development += 1,
            "held-out" => held_out += 1,
            _ => return Err(Error::invalid("benchmark query split is invalid")),
        }
    }
    if development != EVALUATION_DEVELOPMENT_QUERY_COUNT
        || held_out != EVALUATION_HELD_OUT_QUERY_COUNT
    {
        return Err(Error::invalid(
            "benchmark workload must contain 16 development and 48 held-out queries",
        ));
    }
    Ok(())
}

pub(crate) fn compute_query_metrics(
    ranking: &[&str],
    judgments: &BTreeMap<String, u8>,
) -> Result<QueryMetrics, Error> {
    let relevant = judgments.values().filter(|grade| **grade >= 2).count();
    if relevant == 0 {
        return Err(Error::invalid("query has no relevant Page"));
    }
    let gains = ranking
        .iter()
        .take(10)
        .map(|page_id| judgments.get(*page_id).copied().unwrap_or(0))
        .collect::<Vec<_>>();
    let dcg = gains
        .iter()
        .enumerate()
        .map(|(index, grade)| (2_f64.powi(i32::from(*grade)) - 1.0) / ((index + 2) as f64).log2())
        .sum::<f64>();
    let mut ideal = judgments.values().copied().collect::<Vec<_>>();
    ideal.sort_by(|left, right| right.cmp(left));
    let idcg = ideal
        .iter()
        .take(10)
        .enumerate()
        .map(|(index, grade)| (2_f64.powi(i32::from(*grade)) - 1.0) / ((index + 2) as f64).log2())
        .sum::<f64>();
    let mrr = ranking
        .iter()
        .take(10)
        .position(|page_id| judgments.get(*page_id).is_some_and(|grade| *grade >= 2))
        .map(|index| 1.0 / (index + 1) as f64)
        .unwrap_or(0.0);
    let recalled = ranking
        .iter()
        .take(100)
        .filter(|page_id| judgments.get(**page_id).is_some_and(|grade| *grade >= 2))
        .count();
    Ok(QueryMetrics {
        ndcg_at_10: if idcg == 0.0 { 0.0 } else { dcg / idcg },
        mrr_at_10: mrr,
        recall_at_100: recalled as f64 / relevant as f64,
    })
}

fn aggregate_metrics(metrics: &[QueryMetrics]) -> QueryMetrics {
    if metrics.is_empty() {
        return QueryMetrics {
            ndcg_at_10: 0.0,
            mrr_at_10: 0.0,
            recall_at_100: 0.0,
        };
    }
    let count = metrics.len() as f64;
    QueryMetrics {
        ndcg_at_10: metrics.iter().map(|metric| metric.ndcg_at_10).sum::<f64>() / count,
        mrr_at_10: metrics.iter().map(|metric| metric.mrr_at_10).sum::<f64>() / count,
        recall_at_100: metrics
            .iter()
            .map(|metric| metric.recall_at_100)
            .sum::<f64>()
            / count,
    }
}

fn evaluation_split_metrics(
    queries: &[EvaluationQuery],
    per_mode: &BTreeMap<String, Vec<QueryMetrics>>,
) -> BTreeMap<String, BTreeMap<String, QueryMetrics>> {
    ["development", "held-out"]
        .into_iter()
        .map(|split| {
            let indexes = queries
                .iter()
                .enumerate()
                .filter(|(_, query)| query.split == split)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let modes = ["keyword", "semantic", "hybrid"]
                .into_iter()
                .filter_map(|mode| {
                    per_mode.get(mode).map(|metrics| {
                        (
                            mode.to_owned(),
                            aggregate_metrics(
                                &indexes
                                    .iter()
                                    .map(|index| metrics[*index])
                                    .collect::<Vec<_>>(),
                            ),
                        )
                    })
                })
                .collect();
            (split.to_owned(), modes)
        })
        .collect()
}

fn evaluation_diagnostics(
    queries: &[EvaluationQuery],
    per_mode: &BTreeMap<String, Vec<QueryMetrics>>,
) -> Value {
    let mut dimensions = serde_json::Map::new();
    for (name, values) in [
        (
            "source",
            queries
                .iter()
                .map(|query| query.source.clone())
                .collect::<BTreeSet<_>>(),
        ),
        (
            "intent",
            queries
                .iter()
                .map(|query| query.intent.clone())
                .collect::<BTreeSet<_>>(),
        ),
    ] {
        let mut slices = serde_json::Map::new();
        for value in values {
            let indexes = queries
                .iter()
                .enumerate()
                .filter(|(_, query)| {
                    if name == "source" {
                        query.source == value
                    } else {
                        query.intent == value
                    }
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let mut modes = serde_json::Map::new();
            for mode in ["keyword", "semantic", "hybrid"] {
                if let Some(metrics) = per_mode.get(mode) {
                    let slice = indexes
                        .iter()
                        .map(|index| metrics[*index])
                        .collect::<Vec<_>>();
                    modes.insert(mode.into(), json!(aggregate_metrics(&slice)));
                }
            }
            slices.insert(value, Value::Object(modes));
        }
        dimensions.insert(name.into(), Value::Object(slices));
    }
    Value::Object(dimensions)
}

pub(crate) fn paired_bootstrap_ci(
    deltas: &[f64],
    seed: u64,
    iterations: usize,
) -> BootstrapInterval {
    if deltas.is_empty() || iterations == 0 {
        return BootstrapInterval {
            mean: 0.0,
            lower: 0.0,
            upper: 0.0,
            iterations,
        };
    }
    let mean = deltas.iter().sum::<f64>() / deltas.len() as f64;
    let mut state = seed;
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let mut total = 0.0;
        for _ in deltas {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            total += deltas[(state as usize) % deltas.len()];
        }
        samples.push(total / deltas.len() as f64);
    }
    samples.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let lower = samples[((iterations - 1) * 25 / 1000).min(iterations - 1)];
    let upper = samples[((iterations - 1) * 975 / 1000).min(iterations - 1)];
    BootstrapInterval {
        mean,
        lower,
        upper,
        iterations,
    }
}
