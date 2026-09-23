use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let sequence = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "scout-{label}-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temporary directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Copy)]
enum EvaluationCase {
    Baseline,
    GradeOneOnly,
    HybridDoesNotBeatSemantic,
}

fn write_jsonl(path: &Path, rows: &[Value]) {
    let mut content = rows
        .iter()
        .map(|row| serde_json::to_string(row).expect("serialize JSONL row"))
        .collect::<Vec<_>>()
        .join("\n");
    content.push('\n');
    fs::write(path, content).expect("write JSONL");
}

fn write_evaluation_package(root: &Path, case: EvaluationCase) -> PathBuf {
    let package = root.join("evaluation-package");
    fs::create_dir_all(&package).expect("create evaluation package");

    let sources = [
        "rust-docs",
        "python-docs",
        "kubernetes-docs",
        "postgres-docs",
    ];
    let intents = ["exact-api", "conceptual", "how-to", "semantic-paraphrase"];
    let mut queries = Vec::new();
    let mut judgments = Vec::new();
    let mut rankings = Vec::new();

    for source in sources {
        for index in 0..16 {
            let query_id = format!("{source}-{index:02}");
            let split = if index < 4 { "development" } else { "held-out" };
            queries.push(json!({
                "query_id": query_id,
                "query": format!("frozen query {source} {index}"),
                "source": source,
                "intent": intents[index % intents.len()],
                "split": split,
            }));

            match case {
                EvaluationCase::Baseline => {
                    let page_id = format!("answer-{query_id}");
                    judgments.push(json!({
                        "query_id": query_id,
                        "page_id": page_id,
                        "grade": 3,
                    }));
                    rankings.push(json!({
                        "query_id": query_id,
                        "keyword": [page_id.clone()],
                        "semantic": [page_id.clone()],
                        "hybrid": [page_id],
                    }));
                }
                EvaluationCase::GradeOneOnly => {
                    let page_id = format!("related-{query_id}");
                    judgments.push(json!({
                        "query_id": query_id,
                        "page_id": page_id,
                        "grade": 1,
                    }));
                    rankings.push(json!({
                        "query_id": query_id,
                        "keyword": [page_id.clone()],
                        "semantic": [page_id.clone()],
                        "hybrid": [page_id],
                    }));
                }
                EvaluationCase::HybridDoesNotBeatSemantic => {
                    let keyword_page = format!("keyword-{query_id}");
                    let semantic_page = format!("semantic-{query_id}");
                    judgments.push(json!({
                        "query_id": query_id,
                        "page_id": keyword_page,
                        "grade": 2,
                    }));
                    judgments.push(json!({
                        "query_id": query_id,
                        "page_id": semantic_page,
                        "grade": 3,
                    }));
                    rankings.push(json!({
                        "query_id": query_id,
                        "keyword": [keyword_page.clone()],
                        "semantic": [semantic_page],
                        "hybrid": [keyword_page],
                    }));
                }
            }
        }
    }

    write_jsonl(&package.join("queries.jsonl"), &queries);
    write_jsonl(&package.join("judgments.jsonl"), &judgments);
    write_jsonl(&package.join("rankings.jsonl"), &rankings);
    fs::write(
        package.join("metadata.json"),
        serde_json::to_vec_pretty(&json!({
            "evaluation_id": "eval-contract-v1",
            "corpus_snapshot_id": "snapshot-contract",
            "build_id": "build-contract",
            "model_digest": "sha256:model-contract",
            "judges": ["judge-a", "judge-b"],
            "adjudication": "independent-then-adjudicated",
        }))
        .expect("serialize evaluation metadata"),
    )
    .expect("write evaluation metadata");
    package
}

fn run_scout(data_dir: &Path, args: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_scout"))
        .args(args)
        .arg("--data-dir")
        .arg(data_dir)
        .output()
        .expect("run scout")
}

fn evaluation_report(data_dir: &Path) -> Value {
    let directories = fs::read_dir(data_dir.join("evaluations"))
        .expect("read evaluation runs")
        .map(|entry| entry.expect("read evaluation run").path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    assert_eq!(directories.len(), 1, "expected one evaluation run");
    serde_json::from_slice(
        &fs::read(directories[0].join("report.json")).expect("read evaluation report"),
    )
    .expect("parse evaluation report")
}

fn run_evaluation(case: EvaluationCase) -> (TempDir, Value) {
    let data_dir = TempDir::new("evaluation-contract");
    let package = write_evaluation_package(data_dir.path(), case);
    let output = run_scout(
        data_dir.path(),
        &[
            "evaluate".into(),
            "--package".into(),
            package.display().to_string(),
        ],
    );
    assert!(
        output.status.success(),
        "evaluation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = evaluation_report(data_dir.path());
    (data_dir, report)
}

#[test]
fn evaluation_report_contract_records_frozen_shape_and_all_modes() {
    let (_data_dir, report) = run_evaluation(EvaluationCase::Baseline);

    assert_eq!(report["schema_version"], "scout.evaluation.v1");
    assert_eq!(report["query_count"], 64);
    assert_eq!(report["development_query_count"], 16);
    assert_eq!(report["held_out_query_count"], 48);
    for mode in ["keyword", "semantic", "hybrid"] {
        assert!(report["metrics"][mode]["ndcg_at_10"].is_number());
        assert!(report["metrics"][mode]["mrr_at_10"].is_number());
        assert!(report["metrics"][mode]["recall_at_100"].is_number());
    }
    assert_eq!(
        report["held_out_gate"]["hybrid_minus_keyword"]["iterations"],
        10_000
    );
    assert_eq!(
        report["held_out_gate"]["hybrid_minus_semantic"]["iterations"],
        10_000
    );
}

#[test]
fn evaluation_gate_contract_compares_hybrid_with_both_single_channels() {
    let (_data_dir, report) = run_evaluation(EvaluationCase::HybridDoesNotBeatSemantic);

    assert_eq!(
        report["held_out_gate"]["passed"], false,
        "hybrid must fail when it does not beat semantic retrieval"
    );
}

#[test]
fn evaluation_contract_rejects_queries_without_a_grade_two_or_three_page() {
    let data_dir = TempDir::new("evaluation-contract-grade-one");
    let package = write_evaluation_package(data_dir.path(), EvaluationCase::GradeOneOnly);
    let output = run_scout(
        data_dir.path(),
        &[
            "evaluate".into(),
            "--package".into(),
            package.display().to_string(),
        ],
    );

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("has no useful Page judgment"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn evaluation_report_contract_keeps_provenance_and_slice_diagnostics() {
    let (_data_dir, report) = run_evaluation(EvaluationCase::Baseline);

    assert!(report["provenance"].is_object());
    assert_eq!(report["provenance"]["evaluation_id"], "eval-contract-v1");
    assert_eq!(
        report["provenance"]["corpus_snapshot_id"],
        "snapshot-contract"
    );
    assert_eq!(report["provenance"]["build_id"], "build-contract");
    assert_eq!(
        report["provenance"]["judges"].as_array().map(Vec::len),
        Some(2)
    );
    assert_eq!(
        report["judgment_protocol"]["adjudication"],
        "independent-then-adjudicated"
    );
    assert_eq!(report["starter_candidates_used_as_labels"], false);
    assert!(report["metrics_by_split"]["development"].is_object());
    assert!(report["metrics_by_split"]["held-out"].is_object());
    assert!(report["diagnostics"]["source"].is_object());
    assert!(report["diagnostics"]["intent"].is_object());
}

#[test]
fn evaluation_contract_requires_frozen_metadata() {
    let data_dir = TempDir::new("evaluation-contract-metadata");
    let package = write_evaluation_package(data_dir.path(), EvaluationCase::Baseline);
    fs::remove_file(package.join("metadata.json")).expect("remove metadata");
    let output = run_scout(
        data_dir.path(),
        &[
            "evaluate".into(),
            "--package".into(),
            package.display().to_string(),
        ],
    );

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("metadata.json"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
