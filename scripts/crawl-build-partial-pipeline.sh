#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "Internal usage: $0 <semantic-only|keyword-only> <coreml|cpu> <sources.toml> <index-profile-template.toml> <data-dir>" >&2
  exit 2
fi

MODE=$1
BACKEND=$2
CONFIG=$3
PROFILE_TEMPLATE=$4
DATA=$5
if [[ "$MODE" != semantic-only && "$MODE" != keyword-only ]]; then
  echo "Mode must be semantic-only or keyword-only" >&2
  exit 2
fi
if [[ "$BACKEND" != coreml && "$BACKEND" != cpu ]]; then
  echo "Backend must be coreml or cpu" >&2
  exit 2
fi
if [[ "$MODE" == keyword-only && "$BACKEND" == coreml ]]; then
  echo "keyword-only does not use CoreML" >&2
  exit 2
fi

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$ROOT/target/release/scout"
if [[ "$BACKEND" == coreml ]]; then
  cargo build --manifest-path "$ROOT/Cargo.toml" --release --features apple-coreml
else
  cargo build --manifest-path "$ROOT/Cargo.toml" --release
fi
mkdir -p "$DATA/config"

crawl_output=$("$BIN" crawl --config "$CONFIG" --data-dir "$DATA")
printf '%s\n' "$crawl_output"
SNAPSHOT=$(printf '%s\n' "$crawl_output" | sed -n 's/^crawl complete: .*snapshot_id=\([^ ]*\).*/\1/p')
[[ -n "$SNAPSHOT" ]] || { echo "Could not read snapshot ID from crawl output" >&2; exit 1; }

cp "$PROFILE_TEMPLATE" "$DATA/config/index.toml"
python3 - "$DATA/config/index.toml" "$SNAPSHOT" "$BACKEND" <<'PY'
from pathlib import Path
import re, sys

path = Path(sys.argv[1])
text = path.read_text()
text, count = re.subn(r'^snapshot_id = "[^"]*"$', f'snapshot_id = "{sys.argv[2]}"', text, count=1, flags=re.M)
if count != 1:
    raise SystemExit("index profile template must contain one snapshot_id")
provider = "apple_coreml_gpu" if sys.argv[3] == "coreml" else "cpu"
text, count = re.subn(r'^embedding_execution_provider = "[^"]*"$', f'embedding_execution_provider = "{provider}"', text, count=1, flags=re.M)
if count == 0:
    text = text.rstrip() + f'\nembedding_execution_provider = "{provider}"\n'
path.write_text(text)
PY

build_output=$("$BIN" index build --corpus "$SNAPSHOT" --data-dir "$DATA" --mode "$MODE")
printf '%s\n' "$build_output"
GENERATION=$(printf '%s\n' "$build_output" | sed -n 's/^index build complete: generation_id=\([^ ]*\).*/\1/p')
[[ -n "$GENERATION" ]] || { echo "Could not read Generation ID from build output" >&2; exit 1; }

"$BIN" index activate --generation "$GENERATION" --data-dir "$DATA"
"$BIN" index verify --data-dir "$DATA"
printf 'Ready: mode=%s snapshot=%s generation=%s data-dir=%s\n' "$MODE" "$SNAPSHOT" "$GENERATION" "$DATA"
