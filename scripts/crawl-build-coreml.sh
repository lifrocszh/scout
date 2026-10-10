#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <sources.toml> <index-profile-template.toml> <data-dir>" >&2
  exit 2
fi

ROOT=$(cd "$(dirname "$0")/.." && pwd)
CONFIG=$1
PROFILE_TEMPLATE=$2
DATA=$3
BIN="$ROOT/target/release/scout"

cargo build --manifest-path "$ROOT/Cargo.toml" --release --features apple-coreml
mkdir -p "$DATA/config"

crawl_output=$("$BIN" crawl --config "$CONFIG" --data-dir "$DATA")
printf '%s\n' "$crawl_output"
SNAPSHOT=$(printf '%s\n' "$crawl_output" | sed -n 's/^crawl complete: .*snapshot_id=\([^ ]*\).*/\1/p')
[[ -n "$SNAPSHOT" ]] || { echo "Could not read snapshot ID from crawl output" >&2; exit 1; }

cp "$PROFILE_TEMPLATE" "$DATA/config/index.toml"
python3 - "$DATA/config/index.toml" "$SNAPSHOT" <<'PY'
from pathlib import Path
import re, sys

path = Path(sys.argv[1])
text = path.read_text()
text, count = re.subn(r'^snapshot_id = "[^"]*"$', f'snapshot_id = "{sys.argv[2]}"', text, count=1, flags=re.M)
if count != 1:
    raise SystemExit("index profile template must contain one snapshot_id")
text, count = re.subn(r'^embedding_execution_provider = "[^"]*"$', 'embedding_execution_provider = "apple_coreml_gpu"', text, count=1, flags=re.M)
if count == 0:
    text = text.rstrip() + '\nembedding_execution_provider = "apple_coreml_gpu"\n'
path.write_text(text)
PY

build_output=$("$BIN" index build --corpus "$SNAPSHOT" --data-dir "$DATA" --mode hybrid)
printf '%s\n' "$build_output"
GENERATION=$(printf '%s\n' "$build_output" | sed -n 's/^index build complete: generation_id=\([^ ]*\).*/\1/p')
[[ -n "$GENERATION" ]] || { echo "Could not read Generation ID from build output" >&2; exit 1; }

"$BIN" index activate --generation "$GENERATION" --data-dir "$DATA"
"$BIN" index verify --data-dir "$DATA"
printf 'Ready: snapshot=%s generation=%s data-dir=%s\n' "$SNAPSHOT" "$GENERATION" "$DATA"
