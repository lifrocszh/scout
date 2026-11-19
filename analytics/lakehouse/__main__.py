"""Local lakehouse CLI: python3 -m analytics.lakehouse <command> --help

Exit codes: 0 ok, 1 a data-quality rule failed, 2 bad input or usage.
"""

import argparse
import sys
import tempfile
from pathlib import Path

from . import fixture, load, quality, query, sources
from . import tables as t
from .transform import parse_utc

DEFAULT_LAKE = "data/lakehouse"


def add_lake(parser):
    parser.add_argument(
        "--lake", default=DEFAULT_LAKE, help=f"lakehouse root (default {DEFAULT_LAKE})"
    )


def add_freshness(parser):
    parser.add_argument(
        "--freshness-sla-hours",
        type=float,
        help="fail when an Origin's newest snapshot is older than this",
    )
    parser.add_argument(
        "--as-of", type=parse_utc, help="clock for the freshness rule (RFC 3339, default now)"
    )


def print_report(report):
    print("\n".join(report.lines()))


def cmd_load_snapshot(args):
    if args.batch_dir:
        inputs = [sources.snapshot_from_batch(args.batch_dir, args.near_duplicates)]
    else:
        ids = args.snapshot_id or sources.snapshot_ids(args.data_dir)
        if not ids:
            raise sources.SourceError(f"no complete snapshots under {args.data_dir}/corpora")
        inputs = (sources.snapshot_from_data_dir(args.data_dir, snapshot_id) for snapshot_id in ids)
    for snapshot in inputs:
        print_report(load.load_snapshot(args.lake, snapshot, args.freshness_sla_hours, args.as_of))


def cmd_load_events(args):
    if args.jsonl:
        source, ref = sources.JsonlEventSource(args.jsonl), str(args.jsonl)
    else:
        source, ref = sources.EventBatchSource(args.batch_dir), str(args.batch_dir)
    print_report(load.load_events(args.lake, source, ref, args.freshness_sla_hours, args.as_of))


def cmd_check(args):
    as_of = args.as_of or t.now_utc()
    for result in quality.check_lake(query.connect(args.lake), as_of, args.freshness_sla_hours):
        print(result.line())


def cmd_query(args):
    con = query.connect(args.lake)
    if args.sql:
        statements = [("--sql", args.sql)]
    elif args.file:
        statements = [(args.file, Path(args.file).read_text(encoding="utf-8"))]
    else:
        statements = [
            (str(path.relative_to(query.SQL_DIR.parent.parent)), path.read_text(encoding="utf-8"))
            for path in query.example_files()
        ]
    for label, sql in statements:
        print(f"-- {label}")
        print(query.format_table(con.sql(sql)))
        print()


def cmd_demo(args):
    """Build the tables from the fixture crawl, run the checks and the example queries."""
    work = tempfile.TemporaryDirectory(prefix="scout-lakehouse-demo-")
    lake = args.lake or str(Path(work.name) / "lake")
    data_dir, events, as_of = fixture.DATA_DIR, fixture.EVENTS, fixture.fixture_as_of()
    if args.broken:
        data_dir, events, as_of = fixture.broken_copy(args.broken, Path(work.name) / "broken")
        print(f"fixture broken on purpose: {args.broken}")
    print(f"lake {lake}")
    for report in fixture.load_all(lake, data_dir, events, args.freshness_sla_hours, as_of):
        print_report(report)
    print("reload (must add nothing):")
    for report in fixture.load_all(lake, data_dir, events, args.freshness_sla_hours, as_of):
        if report.inserted:
            raise quality.QualityError(
                [
                    quality.CheckResult(
                        "idempotent.reload",
                        False,
                        f"{report.input_ref} inserted {report.inserted} rows again",
                    )
                ]
            )
        print(f"  {report.input_ref}: no new rows")
    args.lake, args.sql, args.file = lake, None, None
    cmd_query(args)
    work.cleanup()


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python3 -m analytics.lakehouse",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)

    snap = commands.add_parser("load-snapshot", help="load Corpus snapshots (idempotent)")
    add_lake(snap)
    where = snap.add_mutually_exclusive_group(required=True)
    where.add_argument("--data-dir", help="Scout data directory")
    where.add_argument(
        "--batch-dir", help="published snapshot batch (scripts/databricks/snapshot_batch.py export)"
    )
    snap.add_argument(
        "--snapshot-id",
        action="append",
        help="with --data-dir; repeatable (default: all, oldest first)",
    )
    snap.add_argument(
        "--near-duplicates", help="with --batch-dir: the crawl's near-duplicates.json"
    )
    add_freshness(snap)
    snap.set_defaults(run=cmd_load_snapshot)

    events = commands.add_parser("load-events", help="load durable crawl events (idempotent)")
    add_lake(events)
    where = events.add_mutually_exclusive_group(required=True)
    where.add_argument("--jsonl", help="`scout events consume` output; - for stdin")
    where.add_argument(
        "--batch-dir", help="a published `scout events export` batch, or a directory of them"
    )
    add_freshness(events)
    events.set_defaults(run=cmd_load_events)

    check = commands.add_parser("check", help="run the lake data-quality rules")
    add_lake(check)
    add_freshness(check)
    check.set_defaults(run=cmd_check)

    run_query = commands.add_parser(
        "query", help="run SQL with DuckDB (default: analytics/sql/examples)"
    )
    add_lake(run_query)
    what = run_query.add_mutually_exclusive_group()
    what.add_argument("--sql", help="one SQL statement")
    what.add_argument("--file", help="a SQL file")
    run_query.set_defaults(run=cmd_query)

    demo = commands.add_parser("demo", help="fixture crawl -> tables -> checks -> example queries")
    demo.add_argument("--lake", help="keep the lake here (default: a temporary directory)")
    demo.add_argument(
        "--broken", choices=fixture.BREAKAGES, help="break the fixture on purpose; must exit 1"
    )
    demo.add_argument("--freshness-sla-hours", type=float, default=24.0)
    demo.set_defaults(run=cmd_demo)

    args = parser.parse_args(argv)
    try:
        args.run(args)
    except quality.QualityError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except (sources.SourceError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
