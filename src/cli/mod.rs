#[derive(Debug)]
pub(crate) struct Cli {
    pub(crate) operation: Operation,
    pub(crate) data_dir: PathBuf,
    pub(crate) json_errors: bool,
}

#[derive(Debug)]
pub(crate) enum Operation {
    Crawl {
        config: PathBuf,
        materialize_spool: Option<String>,
    },
    Coordinator {
        config: PathBuf,
        bind: String,
        run_id: Option<String>,
    },
    Worker {
        coordinator: String,
        worker_id: Option<String>,
        max_tasks: Option<usize>,
    },
    IndexBuild {
        corpus: String,
        resume: Option<String>,
        mode: BuildMode,
    },
    IndexActivate {
        generation: String,
    },
    IndexRecover,
    IndexVerify,
    IndexPrune {
        retain: Vec<String>,
    },
    Serve {
        bind: String,
        access_log: Option<PathBuf>,
    },
    Evaluate {
        package: PathBuf,
    },
    Benchmark {
        config: PathBuf,
    },
}

use crate::generation::BuildMode;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

pub(crate) fn parse_cli<I>(args: I) -> Result<Cli, String>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter().skip(1);
    let command = args.next().ok_or("expected crawl or index build command")?;
    let index_command = if command.to_str() == Some("index") {
        Some(
            args.next()
                .ok_or("expected index build, activate, recover, verify, or prune command")?,
        )
    } else {
        None
    };
    let mut config = None;
    let mut materialize_spool = None;
    let mut corpus = None;
    let mut resume = None;
    let mut build_mode = None;
    let mut generation = None;
    let mut retain = Vec::new();
    let mut bind = None;
    let mut run_id = None;
    let mut coordinator = None;
    let mut worker_id = None;
    let mut max_tasks = None;
    let mut access_log = None;
    let mut package = None;
    let mut data_dir = None;
    let mut json_errors = false;
    let command = match command.to_str() {
        Some("crawl") => "crawl",
        Some("coordinator") => "coordinator",
        Some("worker") => "worker",
        Some("index") => "index",
        Some("serve") => "serve",
        Some("evaluate") => "evaluate",
        Some("benchmark") => "benchmark",
        Some(value) => return Err(format!("unknown command {value}")),
        None => return Err("command is not valid UTF-8".into()),
    };
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--config") => {
                config = Some(PathBuf::from(args.next().ok_or("--config needs a path")?))
            }
            Some("--materialize-spool") => {
                materialize_spool = Some(
                    args.next()
                        .ok_or("--materialize-spool needs a Crawl run ID")?
                        .to_string_lossy()
                        .into(),
                )
            }
            Some("--corpus") => {
                corpus = Some(
                    args.next()
                        .ok_or("--corpus needs an ID")?
                        .to_string_lossy()
                        .into(),
                )
            }
            Some("--resume") => {
                resume = Some(
                    args.next()
                        .ok_or("--resume needs a staging run ID")?
                        .to_string_lossy()
                        .into(),
                )
            }
            Some("--mode") => {
                let value = args
                    .next()
                    .ok_or("--mode needs hybrid, semantic-only, or keyword-only")?;
                build_mode = Some(match value.to_str() {
                    Some("hybrid") => BuildMode::Hybrid,
                    Some("semantic-only") => BuildMode::SemanticOnly,
                    Some("keyword-only") => BuildMode::KeywordOnly,
                    Some(_) => {
                        return Err("--mode must be hybrid, semantic-only, or keyword-only".into());
                    }
                    None => return Err("--mode is not valid UTF-8".into()),
                });
            }
            Some("--generation") => {
                generation = Some(
                    args.next()
                        .ok_or("--generation needs an ID")?
                        .to_string_lossy()
                        .into(),
                )
            }
            Some("--retain") => retain.push(
                args.next()
                    .ok_or("--retain needs an ID")?
                    .to_string_lossy()
                    .into(),
            ),
            Some("--bind") => {
                bind = Some(
                    args.next()
                        .ok_or("--bind needs an address")?
                        .to_string_lossy()
                        .into(),
                )
            }
            Some("--run-id") => {
                run_id = Some(
                    args.next()
                        .ok_or("--run-id needs an ID")?
                        .to_string_lossy()
                        .into(),
                )
            }
            Some("--coordinator") => {
                coordinator = Some(
                    args.next()
                        .ok_or("--coordinator needs an address")?
                        .to_string_lossy()
                        .into(),
                )
            }
            Some("--worker-id") => {
                worker_id = Some(
                    args.next()
                        .ok_or("--worker-id needs an ID")?
                        .to_string_lossy()
                        .into(),
                )
            }
            Some("--max-tasks") => {
                let value = args.next().ok_or("--max-tasks needs a positive integer")?;
                let parsed = value
                    .to_str()
                    .ok_or("--max-tasks is not valid UTF-8")?
                    .parse::<usize>()
                    .map_err(|_| "--max-tasks needs a positive integer")?;
                if parsed == 0 {
                    return Err("--max-tasks needs a positive integer".into());
                }
                max_tasks = Some(parsed);
            }
            Some("--access-log") => {
                access_log = Some(PathBuf::from(
                    args.next().ok_or("--access-log needs a path")?,
                ))
            }
            Some("--package") => {
                package = Some(PathBuf::from(
                    args.next().ok_or("--package needs a directory")?,
                ))
            }
            Some("--data-dir") => {
                data_dir = Some(PathBuf::from(args.next().ok_or("--data-dir needs a path")?))
            }
            Some("--json") => json_errors = true,
            Some("--help") | Some("-h") => {
                println!(
                    "scout crawl --config <sources.toml> --data-dir <dir> [--json]\nscout crawl --config <sources.toml> --data-dir <dir> --materialize-spool <run-id> [--json]\nscout coordinator --config <sources.toml> --data-dir <dir> [--bind <private-address>] [--run-id <crawl-run-id>]\nscout worker --coordinator <http://address:port> --data-dir <dir> [--worker-id <id>] [--max-tasks <count>]\nscout index build --corpus <snapshot-id> --data-dir <dir> [--mode <hybrid|semantic-only|keyword-only>] (default: keyword-only) [--resume <run-id>] [--json]\nscout index activate --generation <generation-id> --data-dir <dir> [--json]\nscout index recover --data-dir <dir> [--json]\nscout index verify --data-dir <dir>\nscout index prune --data-dir <dir> [--retain <generation-id>]... [--json]\nscout serve --data-dir <dir> --bind <address> [--access-log <path>]\nscout evaluate --package <dir> --data-dir <dir>\nscout benchmark --config <benchmark.toml> --data-dir <dir>"
                );
                return Err(String::new());
            }
            Some(value) => return Err(format!("unknown argument {value}")),
            None => return Err("argument is not valid UTF-8".into()),
        }
    }
    if resume.is_some()
        && !(command == "index"
            && index_command.as_deref().and_then(OsStr::to_str) == Some("build"))
    {
        return Err("--resume is only valid for index build".into());
    }
    if build_mode.is_some()
        && !(command == "index"
            && index_command.as_deref().and_then(OsStr::to_str) == Some("build"))
    {
        return Err("--mode is only valid for index build".into());
    }
    if materialize_spool.is_some() && command != "crawl" {
        return Err("--materialize-spool is only valid for crawl".into());
    }
    if coordinator.is_some() && command != "worker" {
        return Err("--coordinator is only valid for worker".into());
    }
    if worker_id.is_some() && command != "worker" {
        return Err("--worker-id is only valid for worker".into());
    }
    if max_tasks.is_some() && command != "worker" {
        return Err("--max-tasks is only valid for worker".into());
    }
    if run_id.is_some() && command != "coordinator" {
        return Err("--run-id is only valid for coordinator".into());
    }
    let operation = match command {
        "crawl" => Operation::Crawl {
            config: config.ok_or("missing --config")?,
            materialize_spool,
        },
        "coordinator" => Operation::Coordinator {
            config: config.ok_or("missing --config")?,
            bind: bind.unwrap_or_else(|| "127.0.0.1:50051".into()),
            run_id,
        },
        "worker" => Operation::Worker {
            coordinator: coordinator.ok_or("missing --coordinator")?,
            worker_id,
            max_tasks,
        },
        "index" => match index_command.as_deref().and_then(OsStr::to_str) {
            Some("build") => Operation::IndexBuild {
                corpus: corpus.ok_or("missing --corpus")?,
                resume,
                mode: build_mode.unwrap_or(BuildMode::KeywordOnly),
            },
            Some("activate") => Operation::IndexActivate {
                generation: generation.ok_or("missing --generation")?,
            },
            Some("recover") => Operation::IndexRecover,
            Some("verify") => Operation::IndexVerify,
            Some("prune") => Operation::IndexPrune { retain },
            _ => {
                return Err(
                    "expected index build, activate, recover, verify, or prune command".into(),
                );
            }
        },
        "serve" => Operation::Serve {
            bind: bind.unwrap_or_else(|| "127.0.0.1:8080".into()),
            access_log,
        },
        "evaluate" => Operation::Evaluate {
            package: package.ok_or("missing --package")?,
        },
        "benchmark" => Operation::Benchmark {
            config: config.ok_or("missing --config")?,
        },
        _ => unreachable!("validated command"),
    };
    Ok(Cli {
        operation,
        data_dir: data_dir.ok_or("missing --data-dir")?,
        json_errors,
    })
}
