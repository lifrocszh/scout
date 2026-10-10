use super::{Arc, Cli, CrawlResult, Error, Mutex, Operation, OsString, Value, fs, json};
use super::{
    benchmark::run_benchmark,
    cli::parse_cli,
    config::load_config,
    corpus::{
        IncompleteCrawl, materialize_spooled, seal_existing_materialized_spool,
        write_incomplete, write_incomplete_spooled,
    },
    crawl::Crawler,
    evaluation::run_evaluate,
    generation::{IndexGeneration, LifecycleAction},
    http::run_serve,
    observability::{Events, emit_shared},
    spool::CaptureSpool,
    storage::{new_run_id, now},
};
use std::fs::File;
use std::io::{BufRead, BufReader};

pub fn run<I>(args: I) -> i32
where
    I: IntoIterator<Item = OsString>,
{
    let cli = match parse_cli(args) {
        Ok(cli) => cli,
        Err(message) => {
            if !message.is_empty() {
                eprintln!("scout: {message}");
            }
            return 2;
        }
    };
    match &cli.operation {
        Operation::Coordinator { .. } => return super::distributed::run_coordinator(&cli),
        Operation::Worker { .. } => return super::distributed::run_worker(&cli),
        Operation::IndexBuild { .. } => return run_index_build(&cli),
        Operation::IndexActivate { .. } => return run_index_activate(&cli),
        Operation::IndexRecover => return run_index_recover(&cli),
        Operation::IndexVerify => return run_index_verify(&cli),
        Operation::IndexPrune { .. } => return run_index_prune(&cli),
        Operation::Serve { .. } => return run_serve(&cli),
        Operation::Evaluate { .. } => return run_evaluate(&cli),
        Operation::Benchmark { .. } => return run_benchmark(&cli),
        Operation::Crawl { .. } => {}
    }
    let run_id = new_run_id();
    let events = match Events::open(&cli.data_dir, &run_id, "crawl") {
        Ok(events) => Arc::new(Mutex::new(events)),
        Err(error) => {
            report_error(&cli, &error);
            return error.exit_code;
        }
    };
    let config_path = match &cli.operation {
        Operation::Crawl { config, .. } => config,
        Operation::Coordinator { .. } | Operation::Worker { .. } => {
            unreachable!("distributed operation returned above")
        }
        Operation::IndexBuild { .. }
        | Operation::IndexActivate { .. }
        | Operation::IndexRecover
        | Operation::IndexVerify
        | Operation::IndexPrune { .. }
        | Operation::Serve { .. }
        | Operation::Evaluate { .. }
        | Operation::Benchmark { .. } => {
            unreachable!("index, serve, evaluation, or benchmark operation returned above")
        }
    };
    emit_shared(
        &events,
        "crawl.started",
        "success",
        json!({"config_path": config_path, "data_dir": cli.data_dir}),
    );
    let config = match load_config(config_path) {
        Ok(config) => config,
        Err(error) => {
            emit_shared(
                &events,
                "crawl.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(&cli, &error);
            return error.exit_code;
        }
    };
    emit_shared(&events, "crawl.policy", "success", json!(config.policy()));
    emit_shared(
        &events,
        "crawl.config",
        "success",
        json!({
            "source_config_sha256": config.digest.as_str(),
            "sources": &config.sources
        }),
    );
    if let Operation::Crawl {
        materialize_spool: Some(source_run_id),
        ..
    } = &cli.operation
    {
        emit_shared(
            &events,
            "crawl.materialization.started",
            "success",
            json!({"source_run_id": source_run_id}),
        );
        return match materialize_existing_spool(
            &cli.data_dir,
            source_run_id,
            config_path,
            &config,
            &events,
        ) {
            Ok(snapshot) => {
                emit_shared(
                    &events,
                    "crawl.materialization.completed",
                    "success",
                    json!({"source_run_id": source_run_id, "snapshot_id": snapshot.id, "page_count": snapshot.pages, "alias_count": snapshot.aliases}),
                );
                println!(
                    "crawl materialization complete: run_id={source_run_id} snapshot_id={} pages={} aliases={}",
                    snapshot.id, snapshot.pages, snapshot.aliases
                );
                0
            }
            Err(error) => {
                emit_shared(
                    &events,
                    "crawl.materialization.failed",
                    "failure",
                    json!({"source_run_id": source_run_id, "code": error.code, "message": error.message}),
                );
                report_error(&cli, &error);
                error.exit_code
            }
        };
    }
    let crawl_dir = cli.data_dir.join("crawls").join(&run_id);
    if let Err(error) = fs::create_dir_all(crawl_dir.join("bodies")) {
        let error = Error::storage(format!("create crawl directory: {error}"));
        emit_shared(
            &events,
            "crawl.failed",
            "failure",
            json!({"code": error.code, "message": error.message}),
        );
        report_error(&cli, &error);
        return error.exit_code;
    }
    let started_at = now();
    let mut crawler = match Crawler::new(&config, &crawl_dir, events.clone()) {
        Ok(crawler) => crawler,
        Err(error) => {
            write_incomplete(
                &crawl_dir,
                &run_id,
                &config,
                &started_at,
                IncompleteCrawl {
                    captures: &[],
                    rejected: 0,
                    failed: 1,
                },
                &error,
            );
            emit_shared(
                &events,
                "crawl.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(&cli, &error);
            return error.exit_code;
        }
    };
    let crawl_result = match crawler.run() {
        Ok(result) => result,
        Err(error) => {
            write_incomplete_spooled(
                &crawl_dir,
                &run_id,
                &config,
                &started_at,
                crawler.spool(),
                crawler.rejected_count,
                crawler.failed_count,
                &error,
            );
            emit_shared(
                &events,
                "crawl.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(&cli, &error);
            return error.exit_code;
        }
    };
    let (max_active_global, max_active_by_origin) = crawler.concurrency_stats();
    emit_shared(
        &events,
        "crawl.concurrency",
        "success",
        json!({
            "max_active_global": max_active_global,
            "max_active_by_origin": max_active_by_origin,
            "configured_global": config.global_concurrency,
            "configured_origin": config.origin_concurrency,
        }),
    );
    let completed_at = now();
    let snapshot = match materialize_spooled(
        &cli.data_dir,
        &crawl_dir,
        &run_id,
        &config,
        &started_at,
        &completed_at,
        crawl_result,
        crawler.spool(),
    ) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            emit_shared(
                &events,
                "crawl.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(&cli, &error);
            return error.exit_code;
        }
    };
    emit_shared(
        &events,
        "crawl.completed",
        "success",
        json!({"snapshot_id": snapshot.id, "page_count": snapshot.pages, "alias_count": snapshot.aliases, "user_agent": config.user_agent}),
    );
    println!(
        "crawl complete: run_id={run_id} snapshot_id={} pages={} aliases={}",
        snapshot.id, snapshot.pages, snapshot.aliases
    );
    0
}

fn materialize_existing_spool(
    data_dir: &std::path::Path,
    source_run_id: &str,
    config_path: &std::path::Path,
    config: &super::Config,
    events: &Arc<Mutex<Events>>,
) -> Result<super::SnapshotResult, Error> {
    let valid_run_id = source_run_id.strip_prefix("run-").is_some_and(|suffix| {
        suffix.len() == 64 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    });
    if !valid_run_id {
        return Err(Error::invalid("invalid source Crawl run ID"));
    }
    let crawl_dir = data_dir.join("crawls").join(source_run_id);
    let spool_path = crawl_dir.join("capture-spool.sqlite3");
    if !spool_path.is_file() || crawl_dir.join("crawl-manifest.json").exists() {
        return Err(Error::invalid(
            "source Crawl needs an unsealed capture spool and no complete manifest",
        ));
    }
    let (started_at, completed_at, rejected_count, failed_count) =
        read_source_run_summary(data_dir, source_run_id, config_path, config)?;
    let spool = CaptureSpool::open(&crawl_dir, config.max_frontier)?;
    spool.validate_source_scope(&config.sources)?;
    let groups = spool.unique_groups_by_source()?;
    let total_groups: usize = groups.values().sum();
    let quotas_met = config.sources.iter().all(|source| {
        source.minimum_page_quota == 0
            || groups.get(&source.source_id).copied().unwrap_or_default()
                >= source.minimum_page_quota
    });
    if total_groups < config.page_target || !quotas_met {
        return Err(Error::invalid(format!(
            "capture spool has {total_groups} unique groups; target {} and source quotas are not met",
            config.page_target
        )));
    }

    let staging = data_dir
        .join("corpora")
        .join(format!(".staging-{source_run_id}"));
    let preserved = data_dir
        .join("corpora")
        .join(format!(".interrupted-{source_run_id}"));
    if preserved.exists() {
        if !["pages.jsonl", "aliases.jsonl", "passages.jsonl"]
            .iter()
            .all(|name| preserved.join(name).is_file())
        {
            return Err(Error::invalid(
                "preserved materialization is missing a complete JSONL file set",
            ));
        }
        let snapshot = seal_existing_materialized_spool(
            data_dir,
            &crawl_dir,
            source_run_id,
            config,
            &started_at,
            &completed_at,
            rejected_count,
            failed_count,
            &spool,
            &preserved,
        )?;
        emit_shared(
            events,
            "crawl.materialization.snapshot_sealed",
            "success",
            json!({"source_run_id": source_run_id, "snapshot_id": snapshot.id, "unique_groups": total_groups, "reused_preserved_jsonl": true}),
        );
        return Ok(snapshot);
    }
    if staging.exists() {
        fs::rename(&staging, &preserved).map_err(|error| {
            Error::storage(format!("preserve interrupted materialization: {error}"))
        })?;
    }
    let result = CrawlResult {
        captures: Vec::new(),
        rejected_count,
        failed_count,
        stop_reason: "page_target_reached".into(),
    };
    let snapshot = materialize_spooled(
        data_dir,
        &crawl_dir,
        source_run_id,
        config,
        &started_at,
        &completed_at,
        result,
        &spool,
    )?;
    emit_shared(
        events,
        "crawl.materialization.snapshot_sealed",
        "success",
        json!({"source_run_id": source_run_id, "snapshot_id": snapshot.id, "unique_groups": total_groups}),
    );
    Ok(snapshot)
}

fn read_source_run_summary(
    data_dir: &std::path::Path,
    source_run_id: &str,
    config_path: &std::path::Path,
    config: &super::Config,
) -> Result<(String, String, usize, usize), Error> {
    let path = data_dir
        .join("logs")
        .join(format!("{source_run_id}.jsonl"));
    let file = File::open(&path)
        .map_err(|error| Error::storage(format!("open source Crawl event log: {error}")))?;
    let mut first_timestamp = None;
    let mut first_event = None;
    let mut recorded_config_path = None;
    let mut last_timestamp = None;
    let mut last_event = None;
    let mut policy_matches = false;
    let mut recorded_config_digest = None;
    let mut rejected_count = 0;
    let mut failed_count = 0;
    let expected_policy = serde_json::to_value(config.policy())
        .map_err(|error| Error::invalid(format!("serialize Crawl policy: {error}")))?;
    for line in BufReader::new(file).lines() {
        let line = line
            .map_err(|error| Error::storage(format!("read source Crawl event log: {error}")))?;
        let event: Value = serde_json::from_str(&line)
            .map_err(|error| Error::invalid(format!("invalid source Crawl event log: {error}")))?;
        let timestamp = event
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid("source Crawl event has no timestamp"))?
            .to_owned();
        if event.get("run_id").and_then(Value::as_str) != Some(source_run_id) {
            return Err(Error::invalid("source Crawl event has the wrong run ID"));
        }
        first_timestamp.get_or_insert_with(|| timestamp.clone());
        last_timestamp = Some(timestamp);
        let event_name = event.get("event").and_then(Value::as_str).unwrap_or_default();
        first_event.get_or_insert_with(|| event_name.to_owned());
        last_event = Some(event_name.to_owned());
        if event_name == "crawl.started" {
            recorded_config_path = event
                .get("data")
                .and_then(|data| data.get("config_path"))
                .and_then(Value::as_str)
                .map(std::path::PathBuf::from);
        }
        if event_name == "crawl.policy" {
            policy_matches = event.get("data") == Some(&expected_policy);
        }
        if event_name == "crawl.config" {
            let digest = event
                .get("data")
                .and_then(|data| data.get("source_config_sha256"))
                .and_then(Value::as_str)
                .ok_or_else(|| Error::invalid("source Crawl config event has no digest"))?;
            if recorded_config_digest.is_some_and(|previous| previous != digest) {
                return Err(Error::invalid("source Crawl config digest is inconsistent"));
            }
            recorded_config_digest = Some(digest.to_owned());
        }
        rejected_count += usize::from(event_name == "page.rejected");
        failed_count += usize::from(event_name == "fetch.failed");
    }
    if last_event.as_deref() != Some("crawl.concurrency") {
        return Err(Error::invalid(
            "source Crawl did not finish capture and concurrency reporting",
        ));
    }
    if first_event.as_deref() != Some("crawl.started") || !policy_matches {
        return Err(Error::invalid(
            "source Crawl config or policy does not match the supplied config",
        ));
    }
    let current_config_path = fs::canonicalize(config_path)
        .map_err(|error| Error::invalid(format!("resolve supplied Crawl config: {error}")))?;
    let original_config_path = recorded_config_path
        .ok_or_else(|| Error::invalid("source Crawl log has no config path"))?;
    let original_config_path = fs::canonicalize(original_config_path)
        .map_err(|error| Error::invalid(format!("resolve source Crawl config: {error}")))?;
    if original_config_path != current_config_path {
        return Err(Error::invalid(
            "supplied Crawl config path differs from the source Crawl config path",
        ));
    }
    if recorded_config_digest.is_some_and(|digest| digest != config.digest) {
        return Err(Error::invalid(
            "supplied Crawl config digest differs from the source Crawl config digest",
        ));
    }
    Ok((
        first_timestamp.ok_or_else(|| Error::invalid("source Crawl event log is empty"))?,
        last_timestamp.ok_or_else(|| Error::invalid("source Crawl event log is empty"))?,
        rejected_count,
        failed_count,
    ))
}

fn run_index_build(cli: &Cli) -> i32 {
    let Operation::IndexBuild {
        corpus,
        resume,
        mode,
    } = &cli.operation
    else {
        unreachable!("index build operation required")
    };
    let invocation_id = new_run_id();
    let build_run_id = resume.as_deref().unwrap_or(&invocation_id);
    let mut events = match Events::open(&cli.data_dir, &invocation_id, "index") {
        Ok(events) => events,
        Err(error) => {
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    events.emit(
        "index.started",
        "success",
        json!({
            "corpus_snapshot_id": corpus,
            "build_run_id": build_run_id,
            "resume": resume.is_some(),
            "mode": mode.as_str(),
            "data_dir": cli.data_dir
        }),
    );
    let index = IndexGeneration::at(&cli.data_dir);
    let result = if resume.is_some() {
        index.resume_build(corpus, build_run_id, *mode)
    } else {
        index.build(corpus, build_run_id, *mode)
    };
    match result {
        Ok(summary) => {
            if let Some(keyword) = summary.get("keyword").filter(|value| !value.is_null()) {
                events.emit("keyword.completed", "success", keyword.clone());
            }
            if let Some(semantic) = summary.get("semantic").filter(|value| !value.is_null()) {
                events.emit("semantic.completed", "success", semantic.clone());
            }
            events.emit("index.completed", "success", json!(summary));
            println!(
                "index build complete: generation_id={} pages={} passages={}",
                summary["generation_id"].as_str().unwrap_or_default(),
                summary["page_count"].as_u64().unwrap_or_default(),
                summary["passage_count"].as_u64().unwrap_or_default(),
            );
            0
        }
        Err(error) => {
            events.emit(
                "index.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(cli, &error);
            error.exit_code
        }
    }
}

fn run_index_activate(cli: &Cli) -> i32 {
    let Operation::IndexActivate { generation } = &cli.operation else {
        unreachable!("index activate operation required")
    };
    let run_id = new_run_id();
    let mut events = match Events::open(&cli.data_dir, &run_id, "index") {
        Ok(events) => events,
        Err(error) => {
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    events.emit(
        "index.activate.started",
        "success",
        json!({"generation_id": generation, "data_dir": cli.data_dir}),
    );
    match IndexGeneration::at(&cli.data_dir).lifecycle(LifecycleAction::Activate {
        generation_id: generation,
    }) {
        Ok(summary) => {
            events.emit("index.activate.completed", "success", summary.clone());
            println!(
                "index activation complete: generation_id={}",
                summary["generation_id"].as_str().unwrap_or_default()
            );
            0
        }
        Err(error) => {
            events.emit(
                "index.activate.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(cli, &error);
            error.exit_code
        }
    }
}

fn run_index_recover(cli: &Cli) -> i32 {
    let run_id = new_run_id();
    let mut events = match Events::open(&cli.data_dir, &run_id, "index") {
        Ok(events) => events,
        Err(error) => {
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    events.emit(
        "index.recover.started",
        "success",
        json!({"data_dir": cli.data_dir}),
    );
    match IndexGeneration::at(&cli.data_dir).lifecycle(LifecycleAction::Recover) {
        Ok(summary) => {
            events.emit("index.recover.completed", "success", summary.clone());
            println!(
                "index recovery complete: generation_id={}",
                summary["generation_id"].as_str().unwrap_or_default()
            );
            0
        }
        Err(error) => {
            events.emit(
                "index.recover.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(cli, &error);
            error.exit_code
        }
    }
}

fn run_index_verify(cli: &Cli) -> i32 {
    match IndexGeneration::at(&cli.data_dir).open_active() {
        Ok(generation) => {
            println!("generation verified: {}", generation.info().generation_id);
            0
        }
        Err(error) => {
            report_error(cli, &error);
            error.exit_code
        }
    }
}

fn run_index_prune(cli: &Cli) -> i32 {
    let Operation::IndexPrune { retain } = &cli.operation else {
        unreachable!("index prune operation required")
    };
    let run_id = new_run_id();
    let mut events = match Events::open(&cli.data_dir, &run_id, "index") {
        Ok(events) => events,
        Err(error) => {
            report_error(cli, &error);
            return error.exit_code;
        }
    };
    events.emit(
        "index.prune.started",
        "success",
        json!({"data_dir": cli.data_dir, "retained": retain}),
    );
    match IndexGeneration::at(&cli.data_dir).lifecycle(LifecycleAction::Prune {
        explicitly_retained: retain,
    }) {
        Ok(summary) => {
            events.emit("index.prune.completed", "success", summary.clone());
            println!(
                "index prune complete: removed={}",
                summary["removed"].as_array().map_or(0, Vec::len)
            );
            0
        }
        Err(error) => {
            events.emit(
                "index.prune.failed",
                "failure",
                json!({"code": error.code, "message": error.message}),
            );
            report_error(cli, &error);
            error.exit_code
        }
    }
}

pub(crate) fn report_error(cli: &Cli, error: &Error) {
    if cli.json_errors {
        eprintln!(
            "{}",
            json!({"error": {"code": error.code, "message": error.message, "request_id": Value::Null}})
        );
    } else {
        eprintln!("scout crawl failed: {}: {}", error.code, error.message);
    }
}
