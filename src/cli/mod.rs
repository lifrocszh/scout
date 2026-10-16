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
        scheduled: bool,
    },
    EventsConsume {
        run_id: String,
        consumer_id: String,
        batch_size: usize,
        poll_interval_ms: u64,
        follow: bool,
    },
    EventsExport {
        run_id: String,
        consumer_id: String,
        max_records: usize,
        max_batch_bytes: usize,
        max_local_bytes: u64,
        max_batches: Option<usize>,
        max_elapsed_ms: u64,
        poll_interval_ms: u64,
        follow: bool,
    },
    EventsConsumerRegister {
        consumer_id: String,
        required: bool,
        replay_days: u32,
    },
    EventsConsumerRetire {
        consumer_id: String,
    },
    EventsAcknowledge {
        receipt_file: PathBuf,
    },
    EventsArchive {
        batch_id: String,
    },
    EventsPreflight {
        run_id: String,
        consumer_id: String,
    },
    EventsPrune {
        run_id: Option<String>,
        batch_id: Option<String>,
        execute: bool,
        max_events: usize,
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
    RecrawlEnable {
        enabled: bool,
        actor: String,
    },
    RecrawlConfigure {
        config: PathBuf,
        source_id: String,
        policy_version: String,
        current_hours: f64,
        minimum_hours: f64,
        maximum_hours: f64,
        max_interval_decrease_fraction: f64,
        enable_source: bool,
        actor: String,
        reason: String,
    },
    RecrawlImport {
        config: PathBuf,
        recommendation: PathBuf,
    },
    RecrawlPending,
    RecrawlReview {
        config: PathBuf,
        recommendation_id: String,
        approve: bool,
        actor: String,
        reason: String,
    },
}

use crate::generation::BuildMode;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

const MAX_LOCAL_EVENT_QUEUE_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const EVENT_BATCH_MANIFEST_RESERVE_BYTES: u64 = 1024 * 1024;

pub(crate) fn parse_cli<I>(args: I) -> Result<Cli, String>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter().skip(1);
    let command = args
        .next()
        .ok_or("expected crawl, coordinator, events, index, or recrawl command")?;
    let index_command = if command.to_str() == Some("index") {
        Some(
            args.next()
                .ok_or("expected index build, activate, recover, verify, or prune command")?,
        )
    } else {
        None
    };
    let events_command = if command.to_str() == Some("events") {
        Some(args.next().ok_or("expected an events action")?)
    } else {
        None
    };
    let recrawl_command = if command.to_str() == Some("recrawl") {
        Some(
            args.next()
                .ok_or("expected recrawl enable, configure, import, pending, or review")?,
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
    let mut consumer_id = None;
    let mut batch_id = None;
    let mut receipt_file = None;
    let mut required_consumer = false;
    let mut replay_days = None;
    let mut batch_size = None;
    let mut max_records = None;
    let mut max_batch_bytes = None;
    let mut max_local_bytes = None;
    let mut max_batches = None;
    let mut max_elapsed_ms = None;
    let mut max_prune_events = None;
    let mut execute_prune = false;
    let mut poll_interval_ms = None;
    let mut follow = false;
    let mut scheduled = false;
    let mut coordinator = None;
    let mut worker_id = None;
    let mut max_tasks = None;
    let mut access_log = None;
    let mut package = None;
    let mut data_dir = None;
    let mut json_errors = false;
    let mut source_id = None;
    let mut policy_version = None;
    let mut current_hours = None;
    let mut minimum_hours = None;
    let mut maximum_hours = None;
    let mut max_interval_decrease_fraction = None;
    let mut enable_source = false;
    let mut enabled = None;
    let mut actor = None;
    let mut reason = None;
    let mut recommendation = None;
    let mut recommendation_id = None;
    let mut approve = None;
    let command = match command.to_str() {
        Some("crawl") => "crawl",
        Some("coordinator") => "coordinator",
        Some("worker") => "worker",
        Some("index") => "index",
        Some("serve") => "serve",
        Some("evaluate") => "evaluate",
        Some("benchmark") => "benchmark",
        Some("events") => "events",
        Some("recrawl") => "recrawl",
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
            Some("--consumer-id") => {
                consumer_id = Some(
                    args.next()
                        .ok_or("--consumer-id needs a stable ID")?
                        .to_str()
                        .ok_or("--consumer-id is not valid UTF-8")?
                        .to_owned(),
                )
            }
            Some("--batch-id") => batch_id = Some(next_string(&mut args, "--batch-id")?),
            Some("--receipt-file") => {
                receipt_file = Some(PathBuf::from(
                    args.next().ok_or("--receipt-file needs a path")?,
                ))
            }
            Some("--required") => required_consumer = true,
            Some("--replay-days") => {
                let value = next_string(&mut args, "--replay-days")?;
                let parsed = value
                    .parse::<u32>()
                    .map_err(|_| "--replay-days needs an integer from 1 to 3650")?;
                if !(1..=3650).contains(&parsed) {
                    return Err("--replay-days must be between 1 and 3650".into());
                }
                replay_days = Some(parsed);
            }
            Some("--batch-size") => {
                let value = args
                    .next()
                    .ok_or("--batch-size needs an integer from 1 to 128")?;
                let parsed = value
                    .to_str()
                    .ok_or("--batch-size is not valid UTF-8")?
                    .parse::<usize>()
                    .map_err(|_| "--batch-size needs an integer from 1 to 128")?;
                if !(1..=128).contains(&parsed) {
                    return Err("--batch-size must be between 1 and 128".into());
                }
                batch_size = Some(parsed);
            }
            Some("--max-records") => {
                let value = args
                    .next()
                    .ok_or("--max-records needs an integer from 1 to 128")?;
                let parsed = value
                    .to_str()
                    .ok_or("--max-records is not valid UTF-8")?
                    .parse::<usize>()
                    .map_err(|_| "--max-records needs an integer from 1 to 128")?;
                if !(1..=128).contains(&parsed) {
                    return Err("--max-records must be between 1 and 128".into());
                }
                max_records = Some(parsed);
            }
            Some("--max-bytes") => {
                let value = args
                    .next()
                    .ok_or("--max-bytes needs an integer from 4096 to 67108864")?;
                let parsed = value
                    .to_str()
                    .ok_or("--max-bytes is not valid UTF-8")?
                    .parse::<usize>()
                    .map_err(|_| "--max-bytes needs an integer from 4096 to 67108864")?;
                if !(4096..=64 * 1024 * 1024).contains(&parsed) {
                    return Err("--max-bytes must be between 4096 and 67108864".into());
                }
                max_batch_bytes = Some(parsed);
            }
            Some("--max-local-bytes") => {
                let value = args.next().ok_or("--max-local-bytes needs an integer")?;
                let parsed = value
                    .to_str()
                    .ok_or("--max-local-bytes is not valid UTF-8")?
                    .parse::<u64>()
                    .map_err(|_| "--max-local-bytes needs an integer")?;
                max_local_bytes = Some(parsed);
            }
            Some("--max-batches") => {
                let value = args
                    .next()
                    .ok_or("--max-batches needs an integer from 1 to 10000")?;
                let parsed = value
                    .to_str()
                    .ok_or("--max-batches is not valid UTF-8")?
                    .parse::<usize>()
                    .map_err(|_| "--max-batches needs an integer from 1 to 10000")?;
                if !(1..=10_000).contains(&parsed) {
                    return Err("--max-batches must be between 1 and 10000".into());
                }
                max_batches = Some(parsed);
            }
            Some("--max-elapsed-ms") => {
                let value = args
                    .next()
                    .ok_or("--max-elapsed-ms needs a positive integer")?;
                let parsed = value
                    .to_str()
                    .ok_or("--max-elapsed-ms is not valid UTF-8")?
                    .parse::<u64>()
                    .map_err(|_| "--max-elapsed-ms needs a positive integer")?;
                if parsed == 0 {
                    return Err("--max-elapsed-ms needs a positive integer".into());
                }
                max_elapsed_ms = Some(parsed);
            }
            Some("--max-events") => {
                let parsed = next_string(&mut args, "--max-events")?
                    .parse::<usize>()
                    .map_err(|_| "--max-events needs an integer from 1 to 128")?;
                if !(1..=128).contains(&parsed) {
                    return Err("--max-events must be between 1 and 128".into());
                }
                max_prune_events = Some(parsed);
            }
            Some("--execute") => execute_prune = true,
            Some("--poll-interval-ms") => {
                let value = args
                    .next()
                    .ok_or("--poll-interval-ms needs a positive integer")?;
                let parsed = value
                    .to_str()
                    .ok_or("--poll-interval-ms is not valid UTF-8")?
                    .parse::<u64>()
                    .map_err(|_| "--poll-interval-ms needs a positive integer")?;
                if parsed == 0 {
                    return Err("--poll-interval-ms needs a positive integer".into());
                }
                poll_interval_ms = Some(parsed);
            }
            Some("--follow") => follow = true,
            Some("--scheduled") => scheduled = true,
            Some("--source-id") => source_id = Some(next_string(&mut args, "--source-id")?),
            Some("--policy-version") => {
                policy_version = Some(next_string(&mut args, "--policy-version")?)
            }
            Some("--current-hours") => {
                current_hours = Some(next_f64(&mut args, "--current-hours")?)
            }
            Some("--minimum-hours") => {
                minimum_hours = Some(next_f64(&mut args, "--minimum-hours")?)
            }
            Some("--maximum-hours") => {
                maximum_hours = Some(next_f64(&mut args, "--maximum-hours")?)
            }
            Some("--max-interval-decrease-fraction") => {
                max_interval_decrease_fraction =
                    Some(next_f64(&mut args, "--max-interval-decrease-fraction")?)
            }
            Some("--enable-source") => enable_source = true,
            Some("--enabled") => {
                enabled = Some(match next_string(&mut args, "--enabled")?.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err("--enabled must be true or false".into()),
                });
            }
            Some("--actor") => actor = Some(next_string(&mut args, "--actor")?),
            Some("--reason") => reason = Some(next_string(&mut args, "--reason")?),
            Some("--recommendation") => {
                recommendation = Some(PathBuf::from(
                    args.next().ok_or("--recommendation needs a file path")?,
                ))
            }
            Some("--recommendation-id") => {
                recommendation_id = Some(next_string(&mut args, "--recommendation-id")?)
            }
            Some("--approve") if approve.is_none() => approve = Some(true),
            Some("--deny") if approve.is_none() => approve = Some(false),
            Some("--approve") | Some("--deny") => {
                return Err("choose exactly one of --approve or --deny".into());
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
                    "scout crawl --config <sources.toml> --data-dir <dir> [--json]\nscout crawl --config <sources.toml> --data-dir <dir> --materialize-spool <run-id> [--json]\nscout coordinator --config <sources.toml> --data-dir <dir> [--bind <private-address>] [--run-id <crawl-run-id>] [--scheduled]\nscout worker --coordinator <http://address:port> --data-dir <dir> [--worker-id <id>] [--max-tasks <count>]\nscout events consume --run-id <crawl-run-id> --consumer-id <stable-id> [--batch-size <1..128>] [--poll-interval-ms <n>] [--follow]\nscout events export --run-id <crawl-run-id> --consumer-id <stable-id> [--max-records <1..128>] [--max-bytes <4096..67108864>] [--max-local-bytes <n>] [--max-batches <1..10000>] [--max-elapsed-ms <n>] [--poll-interval-ms <n>] [--follow]\nscout index build --corpus <snapshot-id> --data-dir <dir> [--mode <hybrid|semantic-only|keyword-only>] (default: keyword-only) [--resume <run-id>] [--json]\nscout index activate --generation <generation-id> --data-dir <dir> [--json]\nscout index recover --data-dir <dir> [--json]\nscout index verify --data-dir <dir>\nscout index prune --data-dir <dir> [--retain <generation-id>]... [--json]\nscout serve --data-dir <dir> --bind <address> [--access-log <path>]\nscout evaluate --package <dir> --data-dir <dir>\nscout benchmark --config <benchmark.toml> --data-dir <dir>\nscout recrawl enable --enabled <true|false> --actor <id>\nscout recrawl configure --config <sources.toml> --source-id <id> --policy-version <id> --current-hours <h> --minimum-hours <h> --maximum-hours <h> --max-interval-decrease-fraction <0..1> --actor <id> --reason <text> [--enable-source]\nscout recrawl import --config <sources.toml> --recommendation <json>\nscout recrawl pending\nscout recrawl review --config <sources.toml> --recommendation-id <sha256:id> (--approve|--deny) --actor <id> --reason <text>"
                );
                println!(
                    "scout events consumer-register --consumer-id <id> [--required] [--replay-days <1..3650>]\nscout events consumer-retire --consumer-id <id>\nscout events acknowledge --receipt-file <databricks-receipt.json>\nscout events archive --batch-id <batch-id> --data-dir <dir>\nscout events preflight --run-id <crawl-run-id> --consumer-id <id> --data-dir <dir>\nscout events prune (--run-id <crawl-run-id>|--batch-id <batch-id>) [--max-events <1..128>] [--execute] --data-dir <dir>"
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
    if scheduled && command != "coordinator" {
        return Err("--scheduled is only valid for coordinator".into());
    }
    let events_action = events_command.as_deref().and_then(OsStr::to_str);
    let event_consume = command == "events" && events_action == Some("consume");
    let event_export = command == "events" && events_action == Some("export");
    let event_preflight = command == "events" && events_action == Some("preflight");
    let event_consumer_register = command == "events" && events_action == Some("consumer-register");
    let event_consumer_retire = command == "events" && events_action == Some("consumer-retire");
    let event_acknowledge = command == "events" && events_action == Some("acknowledge");
    let event_archive = command == "events" && events_action == Some("archive");
    let event_prune = command == "events" && events_action == Some("prune");
    if run_id.is_some()
        && command != "coordinator"
        && !event_consume
        && !event_export
        && !event_preflight
        && !event_prune
    {
        return Err("--run-id is only valid for coordinator and event commands".into());
    }
    if (consumer_id.is_some()
        || batch_size.is_some()
        || max_records.is_some()
        || max_batch_bytes.is_some()
        || max_local_bytes.is_some()
        || max_batches.is_some()
        || max_elapsed_ms.is_some()
        || poll_interval_ms.is_some()
        || follow)
        && !(event_consume
            || event_export
            || event_preflight
            || event_consumer_register
            || event_consumer_retire)
    {
        return Err("event options are not valid for this events command".into());
    }
    if (batch_size.is_some()) && !event_consume {
        return Err("--batch-size is only valid for events consume".into());
    }
    if (max_records.is_some()
        || max_batch_bytes.is_some()
        || max_local_bytes.is_some()
        || max_batches.is_some()
        || max_elapsed_ms.is_some())
        && !event_export
    {
        return Err("batch bounds are only valid for events export".into());
    }
    if batch_id.is_some() && !(event_archive || event_prune) {
        return Err("--batch-id is only valid for events archive or prune".into());
    }
    if (max_prune_events.is_some() || execute_prune) && !event_prune {
        return Err("--max-events and --execute are only valid for events prune".into());
    }
    if event_prune && (run_id.is_some() == batch_id.is_some()) {
        return Err("events prune requires exactly one of --run-id or --batch-id".into());
    }
    if event_archive && run_id.is_some() {
        return Err("--run-id is not valid for events archive".into());
    }
    if receipt_file.is_some() != event_acknowledge {
        return Err("--receipt-file is only valid for events acknowledge".into());
    }
    if (required_consumer || replay_days.is_some()) && !event_consumer_register {
        return Err(
            "--required and --replay-days are only valid for events consumer-register".into(),
        );
    }
    if event_preflight && (run_id.is_none() || consumer_id.is_none()) {
        return Err("events preflight requires --run-id and --consumer-id".into());
    }
    if event_consumer_register || event_consumer_retire {
        if consumer_id.is_none() {
            return Err("events consumer commands require --consumer-id".into());
        }
    }
    if (event_acknowledge && batch_id.is_some()) || (!event_acknowledge && receipt_file.is_some()) {
        return Err("receipt options are only valid for events acknowledge".into());
    }
    if event_consumer_retire && (required_consumer || replay_days.is_some()) {
        return Err("consumer policy options are only valid for events consumer-register".into());
    }
    if event_export {
        let batch_bytes = max_batch_bytes.unwrap_or(64 * 1024 * 1024) as u64;
        let local_bytes = max_local_bytes.unwrap_or(4 * 1024 * 1024 * 1024);
        if local_bytes > MAX_LOCAL_EVENT_QUEUE_BYTES
            || local_bytes < batch_bytes.saturating_add(EVENT_BATCH_MANIFEST_RESERVE_BYTES)
        {
            return Err("--max-local-bytes must fit one bounded batch plus its manifest reserve and be at most 1 TiB".into());
        }
        if max_batches.is_some() && follow {
            return Err("--max-batches is only valid for bounded, non-follow export".into());
        }
    }
    let recrawl_action = recrawl_command.as_deref().and_then(OsStr::to_str);
    let recrawl_mode = command == "recrawl";
    if recrawl_mode {
        match recrawl_action {
            Some("enable") if enabled.is_some() && actor.is_some() => {}
            Some("configure")
                if config.is_some()
                    && source_id.is_some()
                    && policy_version.is_some()
                    && current_hours.is_some()
                    && minimum_hours.is_some()
                    && maximum_hours.is_some()
                    && max_interval_decrease_fraction.is_some()
                    && actor.is_some()
                    && reason.is_some() => {}
            Some("import") if config.is_some() && recommendation.is_some() => {}
            Some("pending") => {}
            Some("review")
                if config.is_some()
                    && recommendation_id.is_some()
                    && approve.is_some()
                    && actor.is_some()
                    && reason.is_some() => {}
            Some("enable" | "configure" | "import" | "pending" | "review") => {
                return Err("missing required option for recrawl command".into());
            }
            _ => {
                return Err(
                    "expected recrawl enable, configure, import, pending, or review".into(),
                );
            }
        }
    } else if source_id.is_some()
        || policy_version.is_some()
        || current_hours.is_some()
        || minimum_hours.is_some()
        || maximum_hours.is_some()
        || max_interval_decrease_fraction.is_some()
        || enable_source
        || enabled.is_some()
        || actor.is_some()
        || reason.is_some()
        || recommendation.is_some()
        || recommendation_id.is_some()
        || approve.is_some()
    {
        return Err("recrawl options are only valid for recrawl commands".into());
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
            scheduled,
        },
        "events" => match events_command.as_deref().and_then(OsStr::to_str) {
            Some("consume") => Operation::EventsConsume {
                run_id: run_id.ok_or("missing --run-id")?,
                consumer_id: consumer_id.ok_or("missing --consumer-id")?,
                batch_size: batch_size.unwrap_or(32),
                poll_interval_ms: poll_interval_ms.unwrap_or(1_000),
                follow,
            },
            Some("export") => Operation::EventsExport {
                run_id: run_id.ok_or("missing --run-id")?,
                consumer_id: consumer_id.ok_or("missing --consumer-id")?,
                max_records: max_records.unwrap_or(32),
                max_batch_bytes: max_batch_bytes.unwrap_or(64 * 1024 * 1024),
                max_local_bytes: max_local_bytes.unwrap_or(4 * 1024 * 1024 * 1024),
                max_batches,
                max_elapsed_ms: max_elapsed_ms.unwrap_or(5_000),
                poll_interval_ms: poll_interval_ms.unwrap_or(250),
                follow,
            },
            Some("consumer-register") => Operation::EventsConsumerRegister {
                consumer_id: consumer_id.ok_or("missing --consumer-id")?,
                required: required_consumer,
                replay_days: replay_days.unwrap_or(90),
            },
            Some("consumer-retire") => Operation::EventsConsumerRetire {
                consumer_id: consumer_id.ok_or("missing --consumer-id")?,
            },
            Some("acknowledge") => Operation::EventsAcknowledge {
                receipt_file: receipt_file.ok_or("missing --receipt-file")?,
            },
            Some("archive") => Operation::EventsArchive {
                batch_id: batch_id.ok_or("missing --batch-id")?,
            },
            Some("preflight") => Operation::EventsPreflight {
                run_id: run_id.ok_or("missing --run-id")?,
                consumer_id: consumer_id.ok_or("missing --consumer-id")?,
            },
            Some("prune") => Operation::EventsPrune {
                run_id,
                batch_id,
                execute: execute_prune,
                max_events: max_prune_events.unwrap_or(128),
            },
            _ => return Err("expected events consume, export, consumer-register, consumer-retire, acknowledge, archive, preflight, or prune command".into()),
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
        "recrawl" => match recrawl_action {
            Some("enable") => Operation::RecrawlEnable {
                enabled: enabled.ok_or("missing --enabled")?,
                actor: actor.ok_or("missing --actor")?,
            },
            Some("configure") => Operation::RecrawlConfigure {
                config: config.ok_or("missing --config")?,
                source_id: source_id.ok_or("missing --source-id")?,
                policy_version: policy_version.ok_or("missing --policy-version")?,
                current_hours: current_hours.ok_or("missing --current-hours")?,
                minimum_hours: minimum_hours.ok_or("missing --minimum-hours")?,
                maximum_hours: maximum_hours.ok_or("missing --maximum-hours")?,
                max_interval_decrease_fraction: max_interval_decrease_fraction.ok_or("missing --max-interval-decrease-fraction")?,
                enable_source,
                actor: actor.ok_or("missing --actor")?,
                reason: reason.ok_or("missing --reason")?,
            },
            Some("import") => Operation::RecrawlImport {
                config: config.ok_or("missing --config")?,
                recommendation: recommendation.ok_or("missing --recommendation")?,
            },
            Some("pending") => Operation::RecrawlPending,
            Some("review") => Operation::RecrawlReview {
                config: config.ok_or("missing --config")?,
                recommendation_id: recommendation_id.ok_or("missing --recommendation-id")?,
                approve: approve.ok_or("missing --approve or --deny")?,
                actor: actor.ok_or("missing --actor")?,
                reason: reason.ok_or("missing --reason")?,
            },
            _ => unreachable!("validated recrawl command"),
        },
        _ => unreachable!("validated command"),
    };
    Ok(Cli {
        operation,
        data_dir: match data_dir {
            Some(path) => path,
            None if command == "events" || command == "recrawl" => PathBuf::from("data"),
            None => return Err("missing --data-dir".into()),
        },
        json_errors,
    })
}

fn next_string<I>(args: &mut I, option: &str) -> Result<String, String>
where
    I: Iterator<Item = OsString>,
{
    args.next()
        .ok_or_else(|| format!("{option} needs a value"))?
        .into_string()
        .map_err(|_| format!("{option} value is not valid UTF-8"))
}

fn next_f64<I>(args: &mut I, option: &str) -> Result<f64, String>
where
    I: Iterator<Item = OsString>,
{
    let value = next_string(args, option)?;
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("{option} needs a number"))?;
    if !parsed.is_finite() {
        return Err(format!("{option} must be finite"));
    }
    Ok(parsed)
}
