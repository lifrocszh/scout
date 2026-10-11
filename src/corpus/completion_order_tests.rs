use super::*;

fn jsonl_records(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .expect("read Corpus records")
        .lines()
        .map(|line| serde_json::from_str(line).expect("valid JSONL record"))
        .collect()
}

fn validate_materialized_run(
    root: &Path,
    run_id: &str,
    config: &Config,
    snapshot: &SnapshotResult,
    captures: &[Capture],
) -> (Value, Vec<Vec<u8>>) {
    let snapshot_dir = root.join("corpora").join(&snapshot.id);
    let manifest_text =
        fs::read_to_string(root.join("crawl/crawl-manifest.json")).expect("read Crawl manifest");
    let manifest: Value = serde_json::from_str(&manifest_text).expect("parse Crawl manifest");
    let stored: StoredSnapshotManifest = serde_json::from_slice(
        &fs::read(snapshot_dir.join("snapshot-manifest.json")).expect("read snapshot manifest"),
    )
    .expect("validate snapshot manifest schema");
    assert_eq!(manifest["schema_version"], CRAWL_SCHEMA);
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["run_id"], run_id);
    assert_eq!(manifest["snapshot_id"], snapshot.id);
    assert_eq!(manifest["source_config_sha256"], config.digest);
    assert_eq!(stored.schema_version, SNAPSHOT_SCHEMA);
    assert_eq!(stored.status, "complete");
    assert_eq!(stored.run_id, run_id);
    assert_eq!(stored.snapshot_id, snapshot.id);
    assert_eq!(stored.source_config_sha256, config.digest);
    let digest = manifest["manifest_sha256"]
        .as_str()
        .expect("manifest digest");
    let unhashed = manifest_text.replacen(
        &format!("\"manifest_sha256\": \"{digest}\""),
        "\"manifest_sha256\": \"\"",
        1,
    );
    assert_eq!(sha256_hex(unhashed.as_bytes()), digest);
    assert_eq!(stored.crawl_manifest_sha256, digest);
    assert_eq!(manifest["summary"]["captured_page_count"], captures.len());
    assert_eq!(manifest["summary"]["page_count"], stored.page_count);
    assert_eq!(manifest["summary"]["alias_count"], stored.alias_count);
    assert_eq!(manifest["summary"]["passage_count"], stored.passage_count);
    for record in manifest["pages"]
        .as_array()
        .expect("manifest capture records")
    {
        let capture = captures
            .iter()
            .find(|capture| capture_id(capture) == record["capture_id"])
            .expect("manifest must reference a fixed accepted capture");
        let bytes = fs::read(root.join("crawl").join(&capture.body_file)).expect("capture body");
        assert_eq!(sha256_hex(&bytes), record["body_sha256"]);
        assert_eq!(bytes.len(), record["content_bytes"]);
    }
    let artifacts = [
        ("pages.jsonl", stored.pages_jsonl_sha256, stored.page_count),
        (
            "aliases.jsonl",
            stored.aliases_jsonl_sha256,
            stored.alias_count,
        ),
        (
            "passages.jsonl",
            stored.passages_jsonl_sha256,
            stored.passage_count,
        ),
    ]
    .map(|(file, hash, count)| {
        let path = snapshot_dir.join(file);
        let bytes = fs::read(&path).expect("read Corpus artifact");
        assert_eq!(sha256_hex(&bytes), hash, "{file} digest");
        assert_eq!(jsonl_records(&path).len(), count, "{file} record count");
        bytes
    });
    let pages = jsonl_records(&snapshot_dir.join("pages.jsonl"));
    let aliases = jsonl_records(&snapshot_dir.join("aliases.jsonl"));
    let passages = jsonl_records(&snapshot_dir.join("passages.jsonl"));
    assert_eq!(pages.len(), 2);
    assert_eq!(aliases.len(), 2);
    assert!(!passages.is_empty());
    let page_ids: HashSet<_> = pages.iter().map(|page| page["page_id"].clone()).collect();
    assert_eq!(page_ids.len(), pages.len());
    let alias_ids: HashSet<_> = aliases
        .iter()
        .map(|alias| alias["alias_page_id"].clone())
        .collect();
    assert_eq!(alias_ids.len(), aliases.len());
    assert!(page_ids.is_disjoint(&alias_ids));
    let passage_ids: HashSet<_> = passages
        .iter()
        .map(|passage| passage["passage_id"].clone())
        .collect();
    assert_eq!(passage_ids.len(), passages.len());
    for alias in aliases {
        assert!(page_ids.contains(&alias["representative_page_id"]));
        assert_eq!(
            alias["alias_page_id"],
            page_id(alias["alias_url"].as_str().unwrap())
        );
    }
    for passage in passages {
        assert!(page_ids.contains(&passage["page_id"]));
    }
    let winner = captures
        .iter()
        .filter(|capture| capture.final_url == "https://example.invalid/docs/a")
        .min_by_key(|capture| capture_selection_key(capture))
        .expect("conflicting URL captures");
    let selected = pages
        .iter()
        .find(|page| page["normalized_final_url"] == winner.final_url)
        .expect("lexicographically first URL represents its selected content");
    assert_eq!(selected["content_sha256"], winner.content_hash);
    assert_eq!(selected["page_id"], page_id(&winner.final_url));
    assert!(manifest["pages"].as_array().unwrap().iter().any(|capture| {
        capture["admission_status"] == "superseded-capture"
            && capture["selected_capture_id"] == capture_id(winner)
    }));
    (manifest, artifacts.to_vec())
}

#[test]
fn fixed_accepted_captures_have_identical_corpus_across_completion_and_replay_orders() {
    let config = config();
    let bodies = [
        "<html lang=\"en\"><head><title>Docker \"guide\"</title></head><body><main><h1>Docker</h1><p>Docker packages applications reliably across development and production.</p></main></body></html>",
        "<html lang=\"en\"><head><title>Container guide</title></head><body><main><h1>Containers</h1><p>Containers isolate services and help teams deploy software safely.</p></main></body></html>",
    ];
    let fixtures = [
        (
            0,
            "https://example.invalid/docs/a",
            "https://example.invalid/docs/a",
        ),
        (
            0,
            "https://example.invalid/docs/z",
            "https://example.invalid/docs/z",
        ),
        (
            1,
            "https://example.invalid/docs/refresh",
            "https://example.invalid/docs/a",
        ),
        (
            1,
            "https://example.invalid/docs/b",
            "https://example.invalid/docs/b",
        ),
        (
            1,
            "https://example.invalid/docs/y",
            "https://example.invalid/docs/y",
        ),
    ];
    let captures: Vec<_> = fixtures
        .iter()
        .map(|(body, requested, final_url)| extracted_capture(bodies[*body], requested, final_url))
        .collect();
    assert_ne!(captures[0].content_hash, captures[2].content_hash);
    let mut expected_snapshot = None;
    let mut expected_artifacts = None;
    let mut previous_manifest_digest = None;
    // Memory is the existing reference; durable runs vary completion order and provenance.
    for (index, order) in [
        [0, 1, 2, 3, 4],
        [4, 3, 2, 1, 0],
        [2, 0, 4, 1, 3],
        [1, 3, 0, 4, 2],
    ]
    .into_iter()
    .enumerate()
    {
        let root = temp_root(&format!("completion-order-{index}"));
        let crawl_dir = root.join("crawl");
        fs::create_dir_all(crawl_dir.join("bodies")).expect("create crawl bodies");
        for (capture, (body, _, _)) in captures.iter().zip(fixtures) {
            fs::write(crawl_dir.join(&capture.body_file), bodies[body]).expect("write fixed body");
        }
        let run_id = format!("run-completion-order-{index}");
        let completed = format!("2025-01-01T00:00:0{index}.000Z");
        let result = CrawlResult {
            captures: if index == 0 {
                captures.clone()
            } else {
                Vec::new()
            },
            rejected_count: index,
            failed_count: 1,
            stop_reason: "frontier_exhausted".into(),
        };
        let snapshot = if index == 0 {
            materialize(
                &root,
                &crawl_dir,
                &run_id,
                &config,
                "2025-01-01T00:00:00.000Z",
                &completed,
                result,
            )
            .expect("materialize reference Corpus")
        } else {
            {
                let spool =
                    CaptureSpool::open(&crawl_dir, config.max_frontier).expect("open spool");
                for capture in order[..2].iter().map(|index| &captures[*index]) {
                    spool
                        .insert_capture(capture, &capture_selection_key(capture))
                        .expect("accept first captures");
                }
            }
            let spool = CaptureSpool::open(&crawl_dir, config.max_frontier).expect("recover spool");
            for capture in order.iter().map(|index| &captures[*index]) {
                spool
                    .insert_capture(capture, &capture_selection_key(capture))
                    .expect("replay accepted capture");
            }
            assert_eq!(
                spool.capture_count().expect("replayed capture count"),
                captures.len()
            );
            materialize_spooled(
                SpoolMaterialization {
                    data_dir: &root,
                    crawl_dir: &crawl_dir,
                    run_id: &run_id,
                    config: &config,
                    started: "2025-01-01T00:00:00.000Z",
                    completed: &completed,
                    spool: &spool,
                },
                result,
            )
            .expect("materialize recovered Corpus")
        };
        let (manifest, artifacts) =
            validate_materialized_run(&root, &run_id, &config, &snapshot, &captures);
        if let Some(expected) = &expected_snapshot {
            assert_eq!(
                &snapshot.id, expected,
                "completion order changed snapshot identity"
            );
            assert_eq!(
                Some(&artifacts),
                expected_artifacts.as_ref(),
                "completion order changed Pages/Aliases/Passages IDs or bytes"
            );
            assert_ne!(
                previous_manifest_digest.as_ref(),
                Some(&manifest["manifest_sha256"])
            );
        } else {
            expected_snapshot = Some(snapshot.id.clone());
            expected_artifacts = Some(artifacts);
        }
        previous_manifest_digest = Some(manifest["manifest_sha256"].clone());
        fs::remove_dir_all(root).expect("remove completion-order fixture");
    }
}
