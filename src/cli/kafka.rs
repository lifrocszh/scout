//! `scout kafka relay|reduce`: the optional Kafka ingestion log (ADR 0003).
use super::next_string;
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KafkaCommand {
    Relay {
        run_id: String,
        consumer_id: String,
        bootstrap: Option<String>,
        topic_prefix: String,
        partitions: i32,
        follow: bool,
        poll_interval_ms: u64,
    },
    Reduce {
        run_id: String,
        config: PathBuf,
        bootstrap: Option<String>,
        topic_prefix: String,
        follow: bool,
        poll_interval_ms: u64,
        record_delay_ms: u64,
    },
}

pub(crate) const USAGE: &str = "scout kafka relay --run-id <crawl-run-id> --data-dir <coordinator-data-dir> [--bootstrap <host:port,...>] [--topic-prefix <prefix>] [--partitions <1..64>] [--consumer-id <id>] [--follow] [--poll-interval-ms <n>] [--metrics-bind <address>]\nscout kafka reduce --run-id <crawl-run-id> --config <sources.toml> --data-dir <replica-dir> [--bootstrap <host:port,...>] [--topic-prefix <prefix>] [--follow] [--poll-interval-ms <n>] [--record-delay-ms <n>] [--metrics-bind <address>]";

/// What `kafka` parses besides its command: `--data-dir`, `--json`, `--metrics-bind`.
pub(crate) struct Parsed {
    pub(crate) command: KafkaCommand,
    pub(crate) data_dir: Option<PathBuf>,
    pub(crate) json_errors: bool,
    pub(crate) metrics_bind: Option<String>,
}

/// Parse the arguments after `kafka`.
pub(crate) fn parse<I>(mut args: I) -> Result<Parsed, String>
where
    I: Iterator<Item = OsString>,
{
    let action = match args.next().as_ref().and_then(|arg| arg.to_str()) {
        Some(action @ ("relay" | "reduce")) => action.to_owned(),
        Some("--help" | "-h") => return Err(USAGE.into()),
        _ => return Err(format!("expected kafka relay or kafka reduce\n{USAGE}")),
    };
    let relay = action == "relay";
    let mut run_id = None;
    let mut config = None;
    let mut consumer_id = None;
    let mut bootstrap = None;
    let mut topic_prefix = None;
    let mut partitions = None;
    let mut follow = false;
    let mut poll_interval_ms = None;
    let mut record_delay_ms = None;
    let mut data_dir = None;
    let mut json_errors = false;
    let mut metrics_bind = None;
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--run-id") => run_id = Some(next_string(&mut args, "--run-id")?),
            Some("--bootstrap") => bootstrap = Some(next_string(&mut args, "--bootstrap")?),
            Some("--topic-prefix") => {
                topic_prefix = Some(next_string(&mut args, "--topic-prefix")?)
            }
            Some("--follow") => follow = true,
            Some("--poll-interval-ms") => {
                poll_interval_ms = Some(number(&mut args, "--poll-interval-ms", 10, 60_000)?)
            }
            Some("--data-dir") => {
                data_dir = Some(PathBuf::from(next_string(&mut args, "--data-dir")?))
            }
            Some("--json") => json_errors = true,
            Some("--metrics-bind") => {
                metrics_bind = Some(next_string(&mut args, "--metrics-bind")?)
            }
            Some("--consumer-id") if relay => {
                consumer_id = Some(next_string(&mut args, "--consumer-id")?)
            }
            Some("--partitions") if relay => {
                partitions = Some(number(&mut args, "--partitions", 1, 64)? as i32)
            }
            Some("--config") if !relay => {
                config = Some(PathBuf::from(next_string(&mut args, "--config")?))
            }
            Some("--record-delay-ms") if !relay => {
                record_delay_ms = Some(number(&mut args, "--record-delay-ms", 0, 60_000)?)
            }
            Some(value) => return Err(format!("unknown argument {value} for kafka {action}")),
            None => return Err("argument is not valid UTF-8".into()),
        }
    }
    let run_id = run_id.ok_or("missing --run-id")?;
    let topic_prefix = topic_prefix.unwrap_or_else(|| "scout".into());
    let poll_interval_ms = poll_interval_ms.unwrap_or(500);
    let command = if relay {
        if data_dir.is_none() {
            return Err("kafka relay needs --data-dir: the coordinator's, where bodies are".into());
        }
        KafkaCommand::Relay {
            run_id,
            consumer_id: consumer_id.unwrap_or_else(|| "kafka-relay".into()),
            bootstrap,
            topic_prefix,
            partitions: partitions.unwrap_or(3),
            follow,
            poll_interval_ms,
        }
    } else {
        if data_dir.is_none() {
            return Err("kafka reduce needs --data-dir for its replica".into());
        }
        KafkaCommand::Reduce {
            run_id,
            config: config.ok_or("kafka reduce needs --config, the run's sources.toml")?,
            bootstrap,
            topic_prefix,
            follow,
            poll_interval_ms,
            record_delay_ms: record_delay_ms.unwrap_or(0),
        }
    };
    Ok(Parsed {
        command,
        data_dir,
        json_errors,
        metrics_bind,
    })
}

fn number<I>(args: &mut I, option: &str, min: u64, max: u64) -> Result<u64, String>
where
    I: Iterator<Item = OsString>,
{
    next_string(args, option)?
        .parse::<u64>()
        .ok()
        .filter(|value| (min..=max).contains(value))
        .ok_or_else(|| format!("{option} must be an integer from {min} to {max}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Operation, parse_cli};

    fn parse(args: &[&str]) -> Result<KafkaCommand, String> {
        let cli = parse_cli(
            ["scout", "kafka"]
                .iter()
                .chain(args)
                .map(|arg| OsString::from(*arg)),
        )?;
        match cli.operation {
            Operation::Kafka(command) => Ok(command),
            other => panic!("unexpected operation {other:?}"),
        }
    }

    #[test]
    fn relay_and_reduce_parse_with_defaults() {
        assert_eq!(
            parse(&["relay", "--run-id", "r1", "--data-dir", "d"]).unwrap(),
            KafkaCommand::Relay {
                run_id: "r1".into(),
                consumer_id: "kafka-relay".into(),
                bootstrap: None,
                topic_prefix: "scout".into(),
                partitions: 3,
                follow: false,
                poll_interval_ms: 500,
            }
        );
        assert_eq!(
            parse(&[
                "reduce",
                "--run-id",
                "r1",
                "--config",
                "s.toml",
                "--data-dir",
                "replica",
                "--follow",
                "--record-delay-ms",
                "250",
                "--bootstrap",
                "kafka:9092",
            ])
            .unwrap(),
            KafkaCommand::Reduce {
                run_id: "r1".into(),
                config: "s.toml".into(),
                bootstrap: Some("kafka:9092".into()),
                topic_prefix: "scout".into(),
                follow: true,
                poll_interval_ms: 500,
                record_delay_ms: 250,
            }
        );
    }

    #[test]
    fn invalid_kafka_arguments_are_rejected() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["produce"]).is_err());
        assert!(parse(&["relay", "--data-dir", "d"]).is_err());
        assert!(parse(&["relay", "--run-id", "r1"]).is_err());
        assert!(parse(&["reduce", "--run-id", "r1", "--data-dir", "d"]).is_err());
        assert!(
            parse(&[
                "relay",
                "--run-id",
                "r1",
                "--data-dir",
                "d",
                "--partitions",
                "0"
            ])
            .is_err()
        );
        assert!(
            parse(&[
                "relay",
                "--run-id",
                "r1",
                "--data-dir",
                "d",
                "--config",
                "x"
            ])
            .is_err()
        );
        assert!(
            parse(&[
                "reduce",
                "--run-id",
                "r1",
                "--config",
                "x",
                "--data-dir",
                "d",
                "--partitions",
                "2"
            ])
            .is_err()
        );
    }
}
