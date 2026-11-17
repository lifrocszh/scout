#!/usr/bin/env bash
# Search-quality regression gate: crawl the fixture corpus (data/eval/corpus),
# build keyword, semantic and hybrid indexes, run `scout evaluate` over the 64
# labelled queries, and compare NDCG@10, Recall@10 and MRR per mode with the
# committed baseline (data/eval/baseline.json). Exits 1 on a regression.
#
# Usage: scripts/ci/search-quality.sh [--update-baseline]
#   --update-baseline   after the run, overwrite data/eval/baseline.json with the
#                       results (see docs/search-quality-gate.md)
# Env:   SCOUT_BIN                 scout binary (default target/debug/scout)
#        SEARCH_QUALITY_DIR        work directory (default data/search-quality, ignored by git).
#                                  Its models/ subdirectory survives between runs: it caches the
#                                  BGE Small download, which is verified against the hashes pinned in
#                                  data/eval/index-profile.toml.
#        SEARCH_QUALITY_MODE       hybrid (default: all three modes) or keyword-only (no model
#                                  download; gates the keyword mode only)
#        SEARCH_QUALITY_MAX_DROP   largest tolerated drop per metric (default 0.02)
set -euo pipefail
cd "$(dirname "$0")/../.."

update_baseline=0
case "${1:-}" in
  "") ;;
  --update-baseline) update_baseline=1 ;;
  *) echo "usage: $0 [--update-baseline]" >&2; exit 2 ;;
esac

bin=${SCOUT_BIN:-target/debug/scout}
dir=${SEARCH_QUALITY_DIR:-data/search-quality}
mode=${SEARCH_QUALITY_MODE:-hybrid}
# Fixed on purpose: Page IDs derive from URLs, and the sitemap lists absolute URLs.
port=18099
origin="http://127.0.0.1:$port"
helper=scripts/ci/search_quality.py

[ -x "$bin" ] || { echo "search-quality: $bin not found; run cargo build --locked first" >&2; exit 2; }
case "$mode" in
  hybrid) gated=keyword,semantic,hybrid ;;
  keyword-only) gated=keyword; [ "$update_baseline" = 0 ] || { echo "search-quality: --update-baseline needs SEARCH_QUALITY_MODE=hybrid" >&2; exit 2; } ;;
  *) echo "search-quality: SEARCH_QUALITY_MODE must be hybrid or keyword-only" >&2; exit 2 ;;
esac

step() { printf '\nsearch-quality: %s\n' "$*" >&2; }

# quiet <event-log> <command...>: scout writes its JSON event stream to stderr.
# Keep it in a file (the helper reads the crawl's events) and show its tail only
# if the command fails.
quiet() {
  local log=$1 code=0
  shift
  "$@" 2>"$log" || code=$?
  [ "$code" = 0 ] || { tail -n 20 "$log" >&2; return "$code"; }
}

# Start clean, but keep the downloaded model.
mkdir -p "$dir"
find "$dir" -mindepth 1 -maxdepth 1 ! -name models -exec rm -rf {} +
started=$(date +%s)

python3 -m http.server "$port" --bind 127.0.0.1 --directory data/eval/corpus >"$dir/http.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  kill -0 "$server" 2>/dev/null || { echo "search-quality: could not serve the fixture on port $port (in use?)" >&2; cat "$dir/http.log" >&2; exit 1; }
  curl -fsS "$origin/robots.txt" >/dev/null 2>&1 && break
  sleep 0.1
done

step "crawl the fixture corpus"
quiet "$dir/crawl.jsonl" "$bin" crawl --config data/eval/sources.toml --data-dir "$dir" >"$dir/crawl.out"
snapshot=$(sed -n 's/^crawl complete: .*snapshot_id=\([^ ]*\).*/\1/p' "$dir/crawl.out")
[ -n "$snapshot" ] || { echo "search-quality: could not read the snapshot ID from the crawl output" >&2; exit 1; }

mkdir -p "$dir/config"
sed "s|@SNAPSHOT_ID@|$snapshot|" data/eval/index-profile.toml >"$dir/config/index.toml"

step "build the $mode index"
if [ "$mode" = hybrid ]; then build_mode=hybrid; else build_mode=keyword-only; fi
out=$(quiet "$dir/build.jsonl" "$bin" index build --corpus "$snapshot" --data-dir "$dir" --mode "$build_mode")
generation=$(sed -n 's/^index build complete: generation_id=\([^ ]*\).*/\1/p' <<<"$out")
[ -n "$generation" ] || { echo "search-quality: could not read the Generation ID from the build output" >&2; exit 1; }
quiet "$dir/activate.jsonl" "$bin" index activate --generation "$generation" --data-dir "$dir" >/dev/null
quiet "$dir/verify.jsonl" "$bin" index verify --data-dir "$dir" >/dev/null

step "evaluate"
model_digest=$(sed -n 's/^onnx_sha256 = "\(.*\)"/\1/p' data/eval/index-profile.toml)
python3 "$helper" package --crawl-log "$dir/crawl.jsonl" --origin "$origin" --snapshot "$snapshot" \
  --generation "$generation" --model-digest "$model_digest" --out "$dir/package"
quiet "$dir/evaluate.jsonl" "$bin" evaluate --package "$dir/package" --data-dir "$dir" >"$dir/evaluate.out"
report=$(sed -n 's/^evaluation complete: .* report=\(.*\)$/\1/p' "$dir/evaluate.out")
[ -f "$report" ] || { echo "search-quality: could not find the evaluation report" >&2; exit 1; }
python3 "$helper" summarize --report "$report" --out "$dir/results.json" >/dev/null
cat "$dir/results.json"
echo "search-quality: measured in $(( $(date +%s) - started ))s" >&2

if [ "$update_baseline" = 1 ]; then
  python3 "$helper" baseline --results "$dir/results.json"
else
  python3 "$helper" compare --results "$dir/results.json" --modes "$gated"
fi
