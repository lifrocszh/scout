//! `scout dlq list|requeue`: inspect and requeue a run's dead-letter tasks.
use super::next_string;
use crate::distributed::frontier::dead_letters::{DeadLetterSelection, MAX_LIST_LIMIT};
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DeadLetterCommand {
    List {
        run_id: String,
        limit: usize,
    },
    Requeue {
        run_id: String,
        selection: DeadLetterSelection,
    },
}

pub(crate) const USAGE: &str = "expected dlq list --run-id <id> [--limit <n>] or dlq requeue --run-id <id> (--all | --task-id <id>...)";

/// Parse the arguments after `dlq`. Returns the command, `--data-dir` and `--json`.
pub(crate) fn parse<I>(mut args: I) -> Result<(DeadLetterCommand, Option<PathBuf>, bool), String>
where
    I: Iterator<Item = OsString>,
{
    let action = args.next().ok_or(USAGE)?;
    let action = match action.to_str() {
        Some(action @ ("list" | "requeue")) => action.to_owned(),
        _ => return Err(USAGE.into()),
    };
    let mut run_id = None;
    let mut limit = None;
    let mut all = false;
    let mut task_ids = Vec::new();
    let mut data_dir = None;
    let mut json_errors = false;
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--run-id") => run_id = Some(next_string(&mut args, "--run-id")?),
            Some("--limit") if action == "list" => {
                let value = next_string(&mut args, "--limit")?;
                limit = Some(
                    value
                        .parse::<usize>()
                        .ok()
                        .filter(|limit| (1..=MAX_LIST_LIMIT).contains(limit))
                        .ok_or(format!("--limit must be from 1 to {MAX_LIST_LIMIT}"))?,
                );
            }
            Some("--all") if action == "requeue" => all = true,
            Some("--task-id") if action == "requeue" => {
                let value = next_string(&mut args, "--task-id")?;
                task_ids.push(
                    value
                        .parse::<i64>()
                        .ok()
                        .filter(|id| *id > 0)
                        .ok_or("--task-id must be a positive integer")?,
                );
            }
            Some("--data-dir") => {
                data_dir = Some(PathBuf::from(next_string(&mut args, "--data-dir")?))
            }
            Some("--json") => json_errors = true,
            Some(value) => return Err(format!("unknown argument {value} for dlq {action}")),
            None => return Err("argument is not valid UTF-8".into()),
        }
    }
    let run_id = run_id.ok_or("missing --run-id")?;
    let command = if action == "list" {
        DeadLetterCommand::List {
            run_id,
            limit: limit.unwrap_or(100),
        }
    } else {
        let selection = match (all, task_ids.is_empty()) {
            (true, true) => DeadLetterSelection::All,
            (false, false) => DeadLetterSelection::Tasks(task_ids),
            _ => return Err("dlq requeue needs exactly one of --all or --task-id".into()),
        };
        DeadLetterCommand::Requeue { run_id, selection }
    };
    Ok((command, data_dir, json_errors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Operation, parse_cli};

    fn parse(args: &[&str]) -> Result<DeadLetterCommand, String> {
        let cli = parse_cli(
            ["scout", "dlq"]
                .iter()
                .chain(args)
                .map(|arg| OsString::from(*arg)),
        )?;
        match cli.operation {
            Operation::DeadLetters(command) => Ok(command),
            other => panic!("unexpected operation {other:?}"),
        }
    }

    #[test]
    fn list_and_requeue_parse() {
        assert_eq!(
            parse(&["list", "--run-id", "r1"]).unwrap(),
            DeadLetterCommand::List {
                run_id: "r1".into(),
                limit: 100
            }
        );
        assert_eq!(
            parse(&["list", "--run-id", "r1", "--limit", "5"]).unwrap(),
            DeadLetterCommand::List {
                run_id: "r1".into(),
                limit: 5
            }
        );
        assert_eq!(
            parse(&["requeue", "--run-id", "r1", "--all"]).unwrap(),
            DeadLetterCommand::Requeue {
                run_id: "r1".into(),
                selection: DeadLetterSelection::All
            }
        );
        assert_eq!(
            parse(&[
                "requeue",
                "--run-id",
                "r1",
                "--task-id",
                "4",
                "--task-id",
                "9"
            ])
            .unwrap(),
            DeadLetterCommand::Requeue {
                run_id: "r1".into(),
                selection: DeadLetterSelection::Tasks(vec![4, 9])
            }
        );
    }

    #[test]
    fn invalid_dlq_arguments_are_rejected() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["purge", "--run-id", "r1"]).is_err());
        assert!(parse(&["list"]).is_err());
        assert!(parse(&["list", "--run-id", "r1", "--limit", "0"]).is_err());
        assert!(parse(&["list", "--run-id", "r1", "--all"]).is_err());
        assert!(parse(&["requeue", "--run-id", "r1"]).is_err());
        assert!(parse(&["requeue", "--run-id", "r1", "--all", "--task-id", "1"]).is_err());
        assert!(parse(&["requeue", "--run-id", "r1", "--task-id", "-1"]).is_err());
    }
}
