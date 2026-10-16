use super::{
    benchmark::run_benchmark,
    cli::parse_cli,
    corpus::materialize_existing_spool,
    crawl::execute,
    crawl::load_config,
    evaluation::run_evaluate,
    generation::{IndexGeneration, LifecycleAction},
    http::run_serve,
    observability::{Events, emit_shared},
    storage::new_run_id,
};
use crate::cli::{Cli, Operation};
use crate::distributed::events::CrawlEvent;
use crate::distributed::frontier::FrontierStore;
use crate::distributed::recrawl_policy::{RecrawlPolicyStore, SourcePolicyConfig};
use crate::error::Error;
use chrono::Utc;
use serde_json::{Value, json};
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::sync::{Arc, Mutex};

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
        Operation::EventsConsume { .. } => return run_events_consume(&cli),
        Operation::EventsExport { .. } => return run_events_export(&cli),
        Operation::EventsConsumerRegister { .. }
        | Operation::EventsConsumerRetire { .. }
        | Operation::EventsAcknowledge { .. }
        | Operation::EventsArchive { .. }
        | Operation::EventsPreflight { .. }
        | Operation::EventsPrune { .. } => return run_events_control(&cli),
        Operation::RecrawlEnable { .. }
        | Operation::RecrawlConfigure { .. }
        | Operation::RecrawlImport { .. }
        | Operation::RecrawlPending
        | Operation::RecrawlReview { .. } => return run_recrawl_policy(&cli),
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
        Operation::EventsConsume { .. }
        | Operation::EventsExport { .. }
        | Operation::EventsConsumerRegister { .. }
        | Operation::EventsConsumerRetire { .. }
        | Operation::EventsAcknowledge { .. }
        | Operation::EventsArchive { .. }
        | Operation::EventsPreflight { .. }
        | Operation::EventsPrune { .. } => {
            unreachable!("event operation returned above")
        }
        Operation::RecrawlEnable { .. }
        | Operation::RecrawlConfigure { .. }
        | Operation::RecrawlImport { .. }
        | Operation::RecrawlPending
        | Operation::RecrawlReview { .. } => unreachable!("recrawl operation returned above"),
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
    let snapshot = match execute(&cli.data_dir, &run_id, &config, events.clone()) {
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

fn run_recrawl_policy(cli: &Cli) -> i32 {
    let result = (|| -> Result<(), Error> {
        let database_url = std::env::var("SCOUT_DATABASE_URL").map_err(|_| {
            Error::invalid("SCOUT_DATABASE_URL must be set for recrawl policy operations")
        })?;
        let mut frontier = FrontierStore::connect(&database_url)?;
        frontier.apply_migrations()?;
        let mut store = RecrawlPolicyStore::connect(&database_url)?;
        match &cli.operation {
            Operation::RecrawlEnable { enabled, actor } => store.set_enabled(*enabled, actor)?,
            Operation::RecrawlConfigure {
                config,
                source_id,
                policy_version,
                current_hours,
                minimum_hours,
                maximum_hours,
                max_interval_decrease_fraction,
                enable_source,
                actor,
                reason,
            } => {
                let config = load_config(config)?;
                store.configure_source(
                    &config,
                    source_id,
                    &SourcePolicyConfig {
                        policy_version: policy_version.clone(),
                        current_interval_hours: *current_hours,
                        minimum_interval_hours: *minimum_hours,
                        maximum_interval_hours: *maximum_hours,
                        max_routine_interval_decrease_fraction: *max_interval_decrease_fraction,
                        enabled: *enable_source,
                    },
                    actor,
                    reason,
                )?;
            }
            Operation::RecrawlImport {
                config,
                recommendation,
            } => {
                let config = load_config(config)?;
                let payload = fs::read_to_string(recommendation).map_err(|error| {
                    Error::storage(format!(
                        "read recrawl recommendation {}: {error}",
                        recommendation.display()
                    ))
                })?;
                let result = store.import_recommendation(&payload, &config, Utc::now())?;
                println!(
                    "{}",
                    serde_json::to_string(&result).unwrap_or_else(|_| "{}".into())
                );
            }
            Operation::RecrawlPending => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&store.pending_reviews()?).map_err(|error| {
                        Error::storage(format!("serialize recrawl review queue: {error}"))
                    })?
                );
            }
            Operation::RecrawlReview {
                config,
                recommendation_id,
                approve,
                actor,
                reason,
            } => {
                let config = load_config(config)?;
                let result = store.decide(
                    recommendation_id,
                    *approve,
                    actor,
                    reason,
                    &config,
                    Utc::now(),
                )?;
                println!(
                    "{}",
                    serde_json::to_string(&result).unwrap_or_else(|_| "{}".into())
                );
            }
            _ => unreachable!("recrawl operation required"),
        }
        Ok(())
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            report_error(cli, &error);
            error.exit_code
        }
    }
}

fn run_events_consume(cli: &Cli) -> i32 {
    match consume_events(cli) {
        Ok(()) => 0,
        Err(error) => {
            if cli.json_errors {
                report_error(cli, &error);
            } else {
                eprintln!(
                    "scout events consume failed: {}: {}",
                    error.code, error.message
                );
            }
            error.exit_code
        }
    }
}

fn consume_events(cli: &Cli) -> Result<(), Error> {
    let Operation::EventsConsume {
        run_id,
        consumer_id,
        batch_size,
        poll_interval_ms,
        follow,
    } = &cli.operation
    else {
        unreachable!("events consume operation required")
    };
    let database_url = std::env::var("SCOUT_DATABASE_URL")
        .map_err(|_| Error::invalid("SCOUT_DATABASE_URL must be set for events consume"))?;
    if database_url.trim().is_empty() {
        return Err(Error::invalid(
            "SCOUT_DATABASE_URL must be set for events consume",
        ));
    }
    let mut frontier = FrontierStore::connect(&database_url)?;
    frontier.apply_migrations()?;
    let stdout = std::io::stdout();
    loop {
        let mut snapshot_published = false;
        let processed =
            frontier.consume_local_events(run_id, consumer_id, *batch_size, |event| {
                let mut output = stdout.lock();
                serde_json::to_writer(&mut output, event)
                    .map_err(|error| Error::storage(format!("serialize Crawl event: {error}")))?;
                output
                    .write_all(b"\n")
                    .and_then(|_| output.flush())
                    .map_err(|error| {
                        Error::storage(format!("flush Crawl event output: {error}"))
                    })?;
                snapshot_published = matches!(event, CrawlEvent::CorpusSnapshotPublished(_));
                Ok(())
            })?;
        if snapshot_published || !follow {
            return Ok(());
        }
        if processed == 0 {
            std::thread::sleep(std::time::Duration::from_millis(*poll_interval_ms));
        }
    }
}

fn run_events_export(cli: &Cli) -> i32 {
    match export_events(cli) {
        Ok(()) => 0,
        Err(error) => {
            if cli.json_errors {
                report_error(cli, &error);
            } else {
                eprintln!(
                    "scout events export failed: {}: {}",
                    error.code, error.message
                );
            }
            error.exit_code
        }
    }
}

fn export_events(cli: &Cli) -> Result<(), Error> {
    let Operation::EventsExport {
        run_id,
        consumer_id,
        max_records,
        max_batch_bytes,
        max_local_bytes,
        max_batches,
        max_elapsed_ms,
        poll_interval_ms,
        follow,
    } = &cli.operation
    else {
        unreachable!("events export operation required")
    };
    let database_url = std::env::var("SCOUT_DATABASE_URL")
        .map_err(|_| Error::invalid("SCOUT_DATABASE_URL must be set for events export"))?;
    if database_url.trim().is_empty() {
        return Err(Error::invalid(
            "SCOUT_DATABASE_URL must be set for events export",
        ));
    }
    let mut frontier = FrontierStore::connect(&database_url)?;
    frontier.apply_migrations()?;
    let queue_root = cli.data_dir.join("crawl-event-batches");
    let output_root = queue_root.join(run_id);
    let mut published_batches = 0usize;
    loop {
        if max_batches.is_some_and(|limit| published_batches >= limit) {
            println!(
                "{}",
                json!({
                    "run_id": run_id,
                    "published_batches": published_batches,
                    "pause_state": "batch_limit",
                    "resume_required": true,
                })
            );
            return Ok(());
        }
        let published = frontier.export_local_event_batch(
            run_id,
            consumer_id,
            &queue_root,
            &output_root,
            *max_records,
            *max_batch_bytes,
            *max_local_bytes,
            std::time::Duration::from_millis(*max_elapsed_ms),
            std::time::Duration::from_millis(*poll_interval_ms),
            *follow,
        )?;
        match published {
            super::distributed::frontier::events::EventExportOutcome::PendingBackoff {
                next_attempt_at_utc,
            } => {
                if *follow {
                    std::thread::sleep(std::time::Duration::from_millis(*poll_interval_ms));
                    continue;
                }
                println!(
                    "{}",
                    json!({
                        "run_id": run_id, "published_batches": published_batches,
                        "status": "pending_backoff", "next_attempt_at_utc": next_attempt_at_utc,
                    })
                );
                return Ok(());
            }
            super::distributed::frontier::events::EventExportOutcome::NoPendingRecords => {
                if *follow {
                    std::thread::sleep(std::time::Duration::from_millis(*poll_interval_ms));
                    continue;
                }
                println!(
                    "{}",
                    json!({
                        "run_id": run_id, "published_batches": published_batches,
                        "record_count": 0, "status": "no_pending_records",
                        "checkpoint_scope": "local-export-only",
                    })
                );
                return Ok(());
            }
            super::distributed::frontier::events::EventExportOutcome::Published(batch) => {
                published_batches += 1;
                let batch_path = batch.directory.to_string_lossy().into_owned();
                println!(
                    "{}",
                    json!({
                        "batch_id": batch.batch_id, "path": batch_path,
                        "record_count": batch.record_count, "records_bytes": batch.records_bytes,
                        "manifest_sha256": batch.manifest_sha256,
                        "records_sha256": batch.records_sha256,
                        "coverage_sha256": batch.coverage_sha256,
                        "snapshot_published": batch.snapshot_published,
                    })
                );
                if batch.snapshot_published || (!follow && max_batches.is_none()) {
                    return Ok(());
                }
            }
        }
    }
}

fn run_events_control(cli: &Cli) -> i32 {
    let result = (|| -> Result<(), Error> {
        let database_url = std::env::var("SCOUT_DATABASE_URL").map_err(|_| {
            Error::invalid("SCOUT_DATABASE_URL must be set for events recovery commands")
        })?;
        if database_url.trim().is_empty() {
            return Err(Error::invalid(
                "SCOUT_DATABASE_URL must be set for events recovery commands",
            ));
        }
        let mut frontier = FrontierStore::connect(&database_url)?;
        frontier.apply_migrations()?;
        match &cli.operation {
            Operation::EventsConsumerRegister {
                consumer_id,
                required,
                replay_days,
            } => {
                frontier.register_analytics_consumer(consumer_id, *required, *replay_days)?;
                println!(
                    "{}",
                    json!({"consumer_id": consumer_id, "required": required, "replay_days": replay_days, "status": "active"})
                );
            }
            Operation::EventsConsumerRetire { consumer_id } => {
                frontier.retire_analytics_consumer(consumer_id)?;
                println!(
                    "{}",
                    json!({"consumer_id": consumer_id, "status": "retired"})
                );
            }
            Operation::EventsAcknowledge { receipt_file } => {
                let bytes = fs::read(receipt_file).map_err(|error| {
                    Error::storage(format!("read Databricks ingestion receipt: {error}"))
                })?;
                let receipt: super::distributed::frontier::analytics::IngestionReceipt =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        Error::invalid(format!("parse Databricks ingestion receipt: {error}"))
                    })?;
                let batch_id = receipt.batch_id.clone();
                let consumer_id = receipt.consumer_id.clone();
                frontier.acknowledge_event_batch(receipt)?;
                println!(
                    "{}",
                    json!({"batch_id": batch_id, "consumer_id": consumer_id, "status": "ingestion_receipt_verified"})
                );
            }
            Operation::EventsArchive { batch_id } => {
                frontier.archive_event_batch(batch_id, &cli.data_dir)?;
                println!(
                    "{}",
                    json!({"batch_id": batch_id, "archive": cli.data_dir.join("analytics-archive"), "status": "verified"})
                );
            }
            Operation::EventsPreflight {
                run_id,
                consumer_id,
            } => {
                let result = frontier.replay_preflight(run_id, consumer_id, &cli.data_dir)?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&result).map_err(|error| Error::storage(
                        format!("serialize replay preflight: {error}")
                    ))?
                );
            }
            Operation::EventsPrune {
                run_id,
                batch_id,
                execute,
                max_events,
            } => {
                let target = match (run_id, batch_id) {
                    (Some(run_id), None) => {
                        super::distributed::frontier::analytics_prune::PruneTarget::Run(
                            run_id.clone(),
                        )
                    }
                    (None, Some(batch_id)) => {
                        super::distributed::frontier::analytics_prune::PruneTarget::Batch(
                            batch_id.clone(),
                        )
                    }
                    _ => {
                        return Err(Error::invalid(
                            "events prune requires exactly one run or batch target",
                        ));
                    }
                };
                let report =
                    frontier.prune_analytics(target, &cli.data_dir, *execute, *max_events)?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).map_err(|error| {
                        Error::storage(format!("serialize analytics prune report: {error}"))
                    })?
                );
            }
            _ => unreachable!("events control operation required"),
        }
        Ok(())
    })();
    match result {
        Ok(()) => 0,
        Err(error) => {
            if cli.json_errors {
                report_error(cli, &error);
            } else {
                eprintln!(
                    "scout events recovery failed: {}: {}",
                    error.code, error.message
                );
            }
            error.exit_code
        }
    }
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
