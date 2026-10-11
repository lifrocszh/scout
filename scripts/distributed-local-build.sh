#!/usr/bin/env bash
# Managed keyword build for the local menu. Monitoring never activates a Generation.
set -euo pipefail

[[ $# == 3 ]] || { echo "Usage: $0 <scout-binary> <snapshot-id> <data-dir>" >&2; exit 2; }
BIN=$1
SNAPSHOT=$2
DATA_DIR=$3
STATE_DIR="$DATA_DIR/distributed-local-ui"
mkdir -p "$STATE_DIR"
if ! mkdir "$STATE_DIR/build.lock" 2>/dev/null; then
  echo 'A managed build is already in progress; see Generation monitor.' >&2
  exit 1
fi

GENERATION=
PHASE=starting
write_state() {
  PHASE=$1
  printf 'phase=%s\nsnapshot_id=%s\ngeneration_id=%s\nupdated_at=%s\n' \
    "$PHASE" "$SNAPSHOT" "$GENERATION" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    > "$STATE_DIR/build-state.tmp.$$"
  mv -f "$STATE_DIR/build-state.tmp.$$" "$STATE_DIR/build-state"
}

finish() {
  local result=$?
  if ((result != 0)); then
    write_state "failed ($PHASE)"
  fi
  rm -f "$STATE_DIR/build.pid" "$STATE_DIR/build-job.pid"
  rmdir "$STATE_DIR/build.lock" 2>/dev/null || true
}
trap finish EXIT
printf '%s\n' "$$" > "$STATE_DIR/build-job.pid"
OUTPUT="$STATE_DIR/build-output.log"
LOG="$STATE_DIR/build.log"
write_state building
"$BIN" index build --corpus "$SNAPSHOT" --data-dir "$DATA_DIR" --mode keyword-only \
  > "$OUTPUT" 2> "$LOG" &
pid=$!
printf '%s\n' "$pid" > "$STATE_DIR/build.pid"
wait "$pid"
GENERATION=$(sed -n 's/^index build complete: generation_id=\([^ ]*\).*/\1/p' "$OUTPUT")
[[ "$GENERATION" =~ ^generation-[a-fA-F0-9]{64}$ ]] || {
  echo 'Build did not report a valid Generation ID; see build-output.log and build.log.' >&2
  exit 1
}
write_state activating
"$BIN" index activate --generation "$GENERATION" --data-dir "$DATA_DIR" >> "$LOG" 2>&1 &
pid=$!
printf '%s\n' "$pid" > "$STATE_DIR/build.pid"
wait "$pid"
write_state verifying
"$BIN" index verify --data-dir "$DATA_DIR" >> "$LOG" 2>&1 &
pid=$!
printf '%s\n' "$pid" > "$STATE_DIR/build.pid"
wait "$pid"
write_state ready
