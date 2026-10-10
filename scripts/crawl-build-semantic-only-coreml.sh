#!/usr/bin/env bash
set -euo pipefail

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
  echo "Usage: $0 <sources.toml> <index-profile-template.toml> <data-dir>"
  echo "Crawl, build a semantic-only Generation with Apple CoreML, activate, and verify it."
  exit 0
fi
if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <sources.toml> <index-profile-template.toml> <data-dir>" >&2
  exit 2
fi

ROOT=$(cd "$(dirname "$0")/.." && pwd)
exec "$ROOT/scripts/crawl-build-partial-pipeline.sh" semantic-only coreml "$@"
