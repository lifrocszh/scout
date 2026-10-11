pub(crate) mod error;

mod app;
mod benchmark;
mod cli;
mod corpus;
mod crawl;
mod distributed;
mod evaluation;
mod generation;
mod http;
mod observability;
mod storage;
mod url;

pub use app::run;

#[cfg(test)]
mod tests {
    use super::benchmark::{benchmark_distribution, benchmark_query_order};
    use super::crawl::{Source, page_id};
    use super::evaluation::{
        EVALUATION_INTENTS, EVALUATION_SOURCES, EvaluationJudgment, EvaluationQuery,
        compute_query_metrics, normalize_evaluation_judgments, paired_bootstrap_ci,
        validate_evaluation_queries,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn page_id_has_stable_domain_separator() {
        assert_eq!(
            page_id("https://example.invalid/docs"),
            "cba82a3ed648ebc4de37b61ae6cdbe866591385b3ed945405fe81c0e365804ff"
        );
    }

    #[test]
    fn source_allowlist_preserves_path_case() {
        let source = Source {
            source_id: "fixture".into(),
            name: None,
            seeds: vec![],
            allowed_urls: None,
            allowed_origins: vec!["https://example.invalid".into()],
            path_prefixes: vec!["/Docs/".into()],
            deny_path_prefixes: vec!["/Docs/private/".into()],
            sitemap_url_prefixes: vec![],
            politeness_group: None,
            license_urls: vec![],
            minimum_page_quota: 0,
        };
        assert!(source.owns("https://example.invalid/Docs/Guide"));
        assert!(!source.owns("https://example.invalid/docs/Guide"));
        assert!(!source.owns("https://example.invalid/Docs/private/Guide"));
    }

    #[test]
    fn evaluation_metrics_use_page_grades_at_fixed_cutoffs() {
        let ranking = ["p1", "p2", "p3", "p4"];
        let judgments = BTreeMap::from([
            ("p1".into(), 3_u8),
            ("p2".into(), 0_u8),
            ("p3".into(), 2_u8),
            ("p4".into(), 1_u8),
        ]);

        let metrics = compute_query_metrics(&ranking, &judgments).expect("metrics");

        assert!((metrics.ndcg_at_10 - 0.9508013338940989).abs() < 1e-12);
        assert_eq!(metrics.mrr_at_10, 1.0);
        assert_eq!(metrics.recall_at_100, 1.0);
    }

    #[test]
    fn evaluation_query_validation_requires_frozen_64_shape() {
        let queries = evaluation_test_queries();
        validate_evaluation_queries(&queries).expect("valid frozen package");

        let mut incomplete = queries;
        incomplete.pop();
        let error = validate_evaluation_queries(&incomplete).expect_err("missing query");
        assert!(error.message.contains("exactly 64"));
    }

    #[test]
    fn paired_bootstrap_is_deterministic() {
        let deltas = [0.0, 0.1, 0.2, -0.1, 0.3];
        let first = paired_bootstrap_ci(&deltas, 7, 2_000);
        let second = paired_bootstrap_ci(&deltas, 7, 2_000);

        assert_eq!(first, second);
        assert!(first.lower <= first.mean && first.mean <= first.upper);
        assert_eq!(first.iterations, 2_000);
    }

    #[test]
    fn independent_page_judgments_require_adjudication() {
        let query = EvaluationQuery {
            query_id: "q1".into(),
            query: "query".into(),
            source: "rust-docs".into(),
            intent: "exact-api".into(),
            split: "held-out".into(),
            expected_page_candidates: vec!["starter-hint".into()],
        };
        let metadata = json!({
            "evaluation_id": "eval",
            "corpus_snapshot_id": "snapshot",
            "build_id": "build",
            "model_digest": "model",
            "judges": ["judge-a", "judge-b"],
            "adjudication": "independent-then-adjudicated",
        });
        let rows = vec![
            EvaluationJudgment {
                query_id: "q1".into(),
                page_id: "page".into(),
                grade: Some(2),
                judge_id: Some("judge-a".into()),
                adjudicated_grade: None,
                judge_grades: BTreeMap::new(),
            },
            EvaluationJudgment {
                query_id: "q1".into(),
                page_id: "page".into(),
                grade: Some(3),
                judge_id: Some("judge-b".into()),
                adjudicated_grade: None,
                judge_grades: BTreeMap::new(),
            },
            EvaluationJudgment {
                query_id: "q1".into(),
                page_id: "page".into(),
                grade: None,
                judge_id: None,
                adjudicated_grade: Some(3),
                judge_grades: BTreeMap::new(),
            },
        ];
        let (judgments, protocol) =
            normalize_evaluation_judgments(rows, &[query], &metadata).expect("judgments");
        assert_eq!(judgments["q1"]["page"], 3);
        assert_eq!(protocol["format"], "independent-plus-adjudicated");
        assert_eq!(protocol["independent_judgment_rows"], 2);
    }

    #[test]
    fn benchmark_distribution_records_percentiles_and_instability() {
        let distribution = benchmark_distribution(&[1.0, 2.0, 3.0, 4.0, 5.0]);

        assert_eq!(distribution.sample_count, 5);
        assert_eq!(distribution.min_ms, 1.0);
        assert_eq!(distribution.p50_ms, 3.0);
        assert_eq!(distribution.p95_ms, 5.0);
        assert_eq!(distribution.p99_ms, 5.0);
        assert_eq!(distribution.max_ms, 5.0);
        assert!((distribution.median_spread - (4.0 / 3.0)).abs() < 1e-12);
        assert!(distribution.unstable);
    }

    #[test]
    fn benchmark_seeded_query_order_repeats_exactly() {
        let queries = evaluation_test_queries();
        let first = benchmark_query_order(queries.clone(), "seeded", 42).expect("order");
        let second = benchmark_query_order(queries, "seeded", 42).expect("order");

        assert_eq!(first, second);
        assert_ne!(first, evaluation_test_queries());
    }

    fn evaluation_test_queries() -> Vec<EvaluationQuery> {
        EVALUATION_SOURCES
            .iter()
            .flat_map(|source| {
                (0..16).map(move |index| EvaluationQuery {
                    query_id: format!("{}-{index:02}", source),
                    query: format!("query {source} {index}"),
                    source: (*source).into(),
                    intent: EVALUATION_INTENTS[index % EVALUATION_INTENTS.len()].into(),
                    split: if index < 4 {
                        "development".into()
                    } else {
                        "held-out".into()
                    },
                    expected_page_candidates: vec!["starter-hint-only".into()],
                })
            })
            .collect()
    }
}
