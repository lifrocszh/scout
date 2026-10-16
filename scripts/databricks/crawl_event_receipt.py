#!/usr/bin/env python3
"""Save a completed Databricks crawl-event notebook receipt for Scout."""

import argparse
import json
import shutil
import subprocess
from pathlib import Path


SCHEMA = "scout.databricks-crawl-event-ingestion-receipt.v1"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="Databricks job run ID")
    parser.add_argument("--output", required=True, help="local receipt JSON path")
    parser.add_argument("--profile")
    args = parser.parse_args()
    executable = shutil.which("databricks")
    if not executable:
        parser.error("Databricks CLI is required")
    command = [executable] + (["--profile", args.profile] if args.profile else [])
    command += ["jobs", "get-run-output", "--run-id", args.run_id]
    output = subprocess.run(command, check=True, capture_output=True, text=True)
    response = json.loads(output.stdout)
    result = response.get("notebook_output", {}).get("result")
    if not isinstance(result, str):
        parser.error("job run has no notebook receipt output; confirm it completed successfully")
    receipt = json.loads(result)
    if not isinstance(receipt, dict) or receipt.get("schema_version") != SCHEMA:
        parser.error("job output is not a Scout crawl-event ingestion receipt")
    if receipt.get("status") != "complete":
        parser.error("downstream batch did not complete")
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(f"Saved receipt for {receipt.get('batch_id')} to {path}")


if __name__ == "__main__":
    main()
