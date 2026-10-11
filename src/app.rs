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
use crate::error::Error;
use serde_json::{Value, json};
use std::ffi::OsString;
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
