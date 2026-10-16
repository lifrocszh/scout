#!/usr/bin/env python3
"""Export a bounded, explicit list of retained Crawl runs and snapshots."""

import argparse
import re
import subprocess
import json
import sys
from pathlib import Path


RUN_ID = re.compile(r"^run-[a-f0-9]{64}$")
SNAPSHOT_ID = re.compile(r"^snapshot-[a-f0-9]{64}$")
MAX_IDS = 100
MAX_BATCHES = 10_000
MAX_LOCAL_BYTES = 1024 ** 4


def read_ids(path, pattern, label):
    try:
        values = [line.strip() for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, UnicodeError) as error:
        raise ValueError(f"cannot read {label} list {path}: {error}") from error
    if not values:
        raise ValueError(f"{label} list must contain at least one ID")
    if len(values) > MAX_IDS:
        raise ValueError(f"{label} list exceeds the {MAX_IDS}-ID limit")
    if len(values) != len(set(values)) or any(not pattern.fullmatch(value) for value in values):
        raise ValueError(f"{label} list contains a duplicate or malformed ID")
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--consumer-id", required=True)
    parser.add_argument("--run-ids-file")
    parser.add_argument("--max-batches-per-run", type=int, default=256)
    parser.add_argument("--snapshot-ids-file")
    parser.add_argument("--snapshot-output-root", default="./databricks-snapshot-exports")
    parser.add_argument("--max-local-bytes", type=int, default=4 * 1024 ** 3)
    args = parser.parse_args()
    try:
        if not 1 <= args.max_batches_per_run <= MAX_BATCHES:
            raise ValueError("--max-batches-per-run must be between 1 and 10000")
        if not 67 * 1024 * 1024 <= args.max_local_bytes <= MAX_LOCAL_BYTES:
            raise ValueError("--max-local-bytes must be between 67 MiB and 1 TiB")
        runs = read_ids(args.run_ids_file, RUN_ID, "run") if args.run_ids_file else []
        snapshots = read_ids(args.snapshot_ids_file, SNAPSHOT_ID, "snapshot") if args.snapshot_ids_file else []
        if not runs and not snapshots:
            raise ValueError("supply a run ID list, a snapshot ID list, or both")

        # Preflight every requested run before the helper publishes any batch.
        for run_id in runs:
            result = subprocess.run([
                "scout", "events", "preflight", "--run-id", run_id,
                "--consumer-id", args.consumer_id, "--data-dir", args.data_dir,
            ], check=True, capture_output=True, text=True)
            preflight = json.loads(result.stdout)
            if preflight.get("status") != "complete":
                raise ValueError(
                    f"recovery preflight for {run_id} is {preflight.get('status')}; "
                    f"replayable_gaps={preflight.get('replayable_gaps', [])}; "
                    f"unrecoverable_gaps={preflight.get('unrecoverable_gaps', [])}; "
                    "resolve the retained batches or unrecoverable gaps before backfill"
                )

        for run_id in runs:
            subprocess.run([
                "scout", "events", "export", "--run-id", run_id,
                "--consumer-id", args.consumer_id, "--data-dir", args.data_dir,
                "--max-records", "32", "--max-bytes", str(64 * 1024 * 1024),
                "--max-local-bytes", str(args.max_local_bytes),
                "--max-batches", str(args.max_batches_per_run),
            ], check=True)
        for snapshot_id in snapshots:
            subprocess.run([
                sys.executable, str(Path(__file__).with_name("snapshot_batch.py")),
                "export", "--data-dir", args.data_dir, "--snapshot-id", snapshot_id,
                "--output-root", args.snapshot_output_root,
            ], check=True)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(2, f"error: {error}\n")


if __name__ == "__main__":
    main()
