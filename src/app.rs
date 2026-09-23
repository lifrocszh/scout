use super::{Arc, Cli, Error, Mutex, Operation, OsString, Value, fs, json};
use super::{
    benchmark::run_benchmark,
    cli::parse_cli,
    config::load_config,
    corpus::{IncompleteCrawl, materialize, write_incomplete},
    crawl::Crawler,
    evaluation::run_evaluate,
    generation::{activate_generation, build_generation, prune_generations, recover_generation},
    http::run_serve,
    observability::{Events, emit_shared},
    search::load_active_generation,
    storage::{new_run_id, now},
};

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
        Operation::Crawl { config } => config,
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
            write_incomplete(
                &crawl_dir,
                &run_id,
                &config,
                &started_at,
                IncompleteCrawl {
                    captures: &crawler.captures,
                    rejected: crawler.rejected_count,
                    failed: crawler.failed_count,
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
    let snapshot = match materialize(
        &cli.data_dir,
        &crawl_dir,
        &run_id,
        &config,
        &started_at,
        &completed_at,
        crawl_result,
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

fn run_index_build(cli: &Cli) -> i32 {
    let Operation::IndexBuild { corpus } = &cli.operation else {
        unreachable!("index build operation required")
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
        "index.started",
        "success",
        json!({"corpus_snapshot_id": corpus, "data_dir": cli.data_dir}),
    );
    let result = build_generation(&cli.data_dir, corpus, &run_id);
    match result {
        Ok(summary) => {
            if let Some(keyword) = summary.get("keyword").cloned() {
                events.emit("keyword.completed", "success", keyword);
            }
            if let Some(semantic) = summary.get("semantic").cloned() {
                events.emit("semantic.completed", "success", semantic);
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
    match activate_generation(&cli.data_dir, generation) {
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
    match recover_generation(&cli.data_dir) {
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
    match load_active_generation(&cli.data_dir) {
        Ok(generation) => {
            println!("generation verified: {}", generation.manifest.generation_id);
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
    match prune_generations(&cli.data_dir, retain) {
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
