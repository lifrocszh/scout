#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
PG_FORMULA=postgresql@17
PG_MAJOR=17
DB_NAME=scout
DEFAULT_BIND=127.0.0.1:50051
WORKER_IDS=(local-worker-1 local-worker-2 local-worker-3)

usage() {
  cat <<'USAGE'
Usage:
  scripts/distributed-local.sh setup
  scripts/distributed-local.sh start <sources.toml> <data-dir>
  scripts/distributed-local.sh stop <data-dir>
  scripts/distributed-local.sh restart <sources.toml> <data-dir>
  scripts/distributed-local.sh status <data-dir>
  scripts/distributed-local.sh materialize <sources.toml> <data-dir> [run-id]
  scripts/distributed-local.sh cleanup <data-dir>

setup installs/starts Homebrew PostgreSQL 17 and creates the local scout database.
SCOUT_COORDINATOR_BIND and SCOUT_COORDINATOR_URL configure the private gRPC endpoint.
USAGE
}

fail() {
  echo "distributed-local: $*" >&2
  return 1
}

resolve_file() {
  local path=$1 link parent base
  [[ -f "$path" ]] || fail "file does not exist: $path" || return 1
  case "$path" in
    /*) ;;
    *) path="$PWD/$path" ;;
  esac
  [[ "$path" != *$'\n'* ]] || fail "paths containing newlines are not supported" || return 1
  while [[ -L "$path" ]]; do
    link=$(readlink "$path")
    case "$link" in
      /*) path=$link ;;
      *) path="$(dirname "$path")/$link" ;;
    esac
  done
  parent=$(cd -P "$(dirname "$path")" && pwd)
  base=$(basename "$path")
  printf '%s/%s\n' "$parent" "$base"
}

resolve_dir() {
  local path=$1
  mkdir -p "$path"
  (cd "$path" && pwd -P)
}

pg_bin() {
  command -v brew >/dev/null 2>&1 || fail "install Homebrew first" || return 1
  brew --prefix "$PG_FORMULA" 2>/dev/null | sed 's#$#/bin#'
}

require_pg17() {
  local bin=$1 version major server_version_num server_major listen
  [[ -x "$bin/psql" ]] || fail "Homebrew $PG_FORMULA is missing; run setup" || return 1
  version=$("$bin/psql" --version | awk '{print $3}')
  major=${version%%.*}
  [[ "$major" == "$PG_MAJOR" ]] || fail "PostgreSQL $PG_MAJOR is required; found $version"
  server_version_num=$(PGHOST=/tmp "$bin/psql" -XAtqc 'SHOW server_version_num' postgres 2>/dev/null) || \
    fail "PostgreSQL is not running on the local Unix socket; run setup" || return 1
  server_major=$((server_version_num / 10000))
  [[ "$server_major" == "$PG_MAJOR" ]] || fail "PostgreSQL server $PG_MAJOR is required; found $server_major"
  listen=$(PGHOST=/tmp "$bin/psql" -XAtqc 'SHOW listen_addresses' postgres | tr -d '[:space:]')
  [[ "$listen" == localhost ]] || fail "PostgreSQL must listen on localhost only; run setup (found: $listen)"
}

setup_postgres() {
  [[ $(uname -s) == Darwin ]] || fail "native setup requires macOS" || return 1
  command -v brew >/dev/null 2>&1 || fail "install Homebrew first" || return 1
  if ! brew list --versions "$PG_FORMULA" >/dev/null 2>&1; then
    brew install "$PG_FORMULA"
  fi
  local bin version listen attempt
  bin=$(pg_bin)
  version=$("$bin/psql" --version | awk '{print $3}')
  [[ "${version%%.*}" == "$PG_MAJOR" ]] || fail "Homebrew $PG_FORMULA must provide PostgreSQL $PG_MAJOR"
  brew services start "$PG_FORMULA"
  for ((attempt = 0; attempt < 30; attempt++)); do
    if PGHOST=/tmp "$bin/psql" -XAtqc 'SELECT 1' postgres >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  PGHOST=/tmp "$bin/psql" -XAtqc 'SELECT 1' postgres >/dev/null 2>&1 || \
    fail "PostgreSQL did not start on /tmp after 30 seconds" || return 1

  listen=$(PGHOST=/tmp "$bin/psql" -XAtqc 'SHOW listen_addresses' postgres | tr -d '[:space:]')
  if [[ "$listen" != localhost ]]; then
    PGHOST=/tmp "$bin/psql" -v ON_ERROR_STOP=1 -d postgres \
      -c "ALTER SYSTEM SET listen_addresses = 'localhost'" >/dev/null
    brew services restart "$PG_FORMULA"
    for ((attempt = 0; attempt < 30; attempt++)); do
      if PGHOST=/tmp "$bin/psql" -XAtqc 'SELECT 1' postgres >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
  fi
  listen=$(PGHOST=/tmp "$bin/psql" -XAtqc 'SHOW listen_addresses' postgres | tr -d '[:space:]')
  [[ "$listen" == localhost ]] || fail "PostgreSQL must listen on localhost only (found: $listen)" || return 1
  local db_exists
  db_exists=$(PGHOST=/tmp "$bin/psql" -XAtqc \
    "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = '$DB_NAME')" postgres)
  if [[ "$db_exists" != t ]]; then
    PGHOST=/tmp "$bin/createdb" "$DB_NAME"
  fi
  echo "PostgreSQL $version ready; the coordinator connects over the local Unix socket."
  echo "PostgreSQL TCP listens on localhost only; it is not reachable over Tailscale or the public network."
  echo "Set SCOUT_DATABASE_URL='postgresql:///$DB_NAME?host=%2Ftmp' for the coordinator process."
}

state_paths() {
  DATA_DIR=$(resolve_dir "$1")
  STATE_DIR="$DATA_DIR/distributed-local"
  mkdir -p "$STATE_DIR"
  RUN_STATE="$STATE_DIR/run-state"
}

read_state_value() {
  local key=$1 file=${2:-$RUN_STATE}
  [[ -f "$file" ]] || return 0
  sed -n "s/^${key}=//p" "$file" | head -n 1
}

write_run_state() {
  local run_id=$1 config_sha=$2 config_path=$3 temporary history history_tmp
  mkdir -p "$STATE_DIR/runs"
  history="$STATE_DIR/runs/$run_id.state"
  temporary="$RUN_STATE.tmp.$$"
  history_tmp="$history.tmp.$$"
  (umask 077; printf 'run_id=%s\nsource_config_sha256=%s\nsource_config_path=%s\n' \
    "$run_id" "$config_sha" "$config_path" > "$history_tmp")
  mv -f "$history_tmp" "$history"
  (umask 077; printf 'run_id=%s\nsource_config_sha256=%s\nsource_config_path=%s\n' \
    "$run_id" "$config_sha" "$config_path" > "$temporary")
  mv -f "$temporary" "$RUN_STATE"
}

new_run_id() {
  printf 'run-%s\n' "$(uuidgen | shasum -a 256 | awk '{print $1}')"
}

get_run_id() {
  local config=$1 digest stored_digest stored_run_id stored_path
  digest=$(shasum -a 256 "$config" | awk '{print $1}')
  stored_digest=$(read_state_value source_config_sha256)
  stored_run_id=$(read_state_value run_id)
  stored_path=$(read_state_value source_config_path)
  if [[ -n "$stored_run_id" && "$stored_digest" == "$digest" && "$stored_path" == "$config" ]]; then
    RUN_ID=$stored_run_id
    echo "Resuming crawl run $RUN_ID (config path and SHA-256 match)."
  else
    RUN_ID=$(new_run_id)
    write_run_state "$RUN_ID" "$digest" "$config"
    if [[ -n "$stored_run_id" ]]; then
      echo "Config path or contents changed; starting $RUN_ID. Prior PostgreSQL run $stored_run_id is preserved."
    else
      echo "Starting crawl run $RUN_ID."
    fi
  fi
}

managed_pid() {
  local file=$1 role=$2 pid command
  [[ -f "$file" ]] || return 1
  pid=$(cat "$file")
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  command=$(ps -p "$pid" -o command= 2>/dev/null || true)
  case "$command" in
    *"scout $role"*"--data-dir $DATA_DIR"*) printf '%s\n' "$pid"; return 0 ;;
    *) return 1 ;;
  esac
}

pid_file() {
  case "$1" in
    coordinator) printf '%s/coordinator.pid\n' "$STATE_DIR" ;;
    worker-1) printf '%s/worker-1.pid\n' "$STATE_DIR" ;;
    worker-2) printf '%s/worker-2.pid\n' "$STATE_DIR" ;;
    worker-3) printf '%s/worker-3.pid\n' "$STATE_DIR" ;;
  esac
}

write_pid() {
  local file=$1 pid=$2 temporary
  temporary="$file.tmp.$$"
  printf '%s\n' "$pid" > "$temporary"
  mv -f "$temporary" "$file"
}

list_active_pids() {
  local name file pid
  for name in coordinator worker-1 worker-2 worker-3; do
    file=$(pid_file "$name")
    if pid=$(managed_pid "$file" "$([[ "$name" == coordinator ]] && echo coordinator || echo worker)"); then
      printf '%s %s\n' "$name" "$pid"
    elif [[ -f "$file" ]]; then
      rm -f "$file"
    fi
  done
}

signal_active() {
  local signal=$1 name file role pid
  for name in worker-1 worker-2 worker-3 coordinator; do
    file=$(pid_file "$name")
    role=worker
    [[ "$name" == coordinator ]] && role=coordinator
    if pid=$(managed_pid "$file" "$role"); then
      kill -"$signal" "$pid" 2>/dev/null || true
    elif [[ -f "$file" ]]; then
      rm -f "$file"
    fi
  done
}

wait_for_stop() {
  local timeout=$1 elapsed=0 active
  while (( elapsed < timeout )); do
    active=$(list_active_pids)
    [[ -z "$active" ]] && return 0
    sleep 1
    elapsed=$((elapsed + 1))
  done
  return 1
}

stop_processes() {
  [[ -n "${DATA_DIR:-}" ]] || return 0
  signal_active INT
  if ! wait_for_stop "${SCOUT_STOP_TIMEOUT_SECONDS:-90}"; then
    echo "Some Scout processes did not stop after SIGINT; sending SIGKILL so leases can expire." >&2
    signal_active KILL
    wait_for_stop 5 || true
  fi
  local name file
  for name in coordinator worker-1 worker-2 worker-3; do
    file=$(pid_file "$name")
    if ! managed_pid "$file" "$([[ "$name" == coordinator ]] && echo coordinator || echo worker)" >/dev/null; then
      rm -f "$file"
    fi
  done
}

wait_for_coordinator() {
  local bind=$1 host port attempt
  host=${bind%:*}
  port=${bind##*:}
  if [[ "$host" == \[*\] ]]; then
    host=${host:1:${#host}-2}
  fi
  for ((attempt = 0; attempt < 30; attempt++)); do
    if nc -z -w 1 "$host" "$port" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_local() {
  local config=$1 bind url database_url id worker_name worker_index
  config=$(resolve_file "$config")
  state_paths "$2"
  if [[ -n "$(list_active_pids)" ]]; then
    fail "processes already running; use stop first"
    return 1
  fi
  local lock="$STATE_DIR/start.lock"
  mkdir "$lock" 2>/dev/null || { fail "another start is in progress"; return 1; }
  START_LOCK=$lock
  START_COMPLETE=0
  trap 'exit 130' INT TERM
  trap 'finish_start' EXIT

  if [[ -n "${SCOUT_BIN:-}" ]]; then
    SCOUT_BIN=$(resolve_file "$SCOUT_BIN")
    [[ -x "$SCOUT_BIN" ]] || { rmdir "$lock"; fail "SCOUT_BIN is not executable"; return 1; }
  else
    (cd "$ROOT" && cargo build --release --bin scout)
    SCOUT_BIN="$ROOT/target/release/scout"
  fi
  local bin
  bin=$(pg_bin)
  require_pg17 "$bin"
  database_url=${SCOUT_DATABASE_URL:-postgresql:///$DB_NAME?host=%2Ftmp}
  case "$database_url" in
    "postgresql:///$DB_NAME?host=%2Ftmp"|"postgres:///$DB_NAME?host=%2Ftmp") ;;
    *) fail "local launcher requires the local Unix-socket database URL postgresql:///$DB_NAME?host=%2Ftmp"; return 1 ;;
  esac
  PGHOST=/tmp "$bin/psql" -XAtqc 'SELECT 1' "$DB_NAME" >/dev/null || {
    fail "database '$DB_NAME' is unavailable; run setup"
    return 1
  }
  export SCOUT_DATABASE_URL="$database_url"
  get_run_id "$config"
  bind=${SCOUT_COORDINATOR_BIND:-$DEFAULT_BIND}
  url=${SCOUT_COORDINATOR_URL:-http://$bind}

  local coordinator_log="$STATE_DIR/coordinator-$RUN_ID.log"
  nohup "$SCOUT_BIN" coordinator --config "$config" --data-dir "$DATA_DIR" \
    --bind "$bind" --run-id "$RUN_ID" > "$coordinator_log" 2>&1 < /dev/null &
  local coordinator_pid=$!
  write_pid "$(pid_file coordinator)" "$coordinator_pid"
  if ! wait_for_coordinator "$bind"; then
    echo "Coordinator failed to listen; see $coordinator_log" >&2
    return 1
  fi
  if ! managed_pid "$(pid_file coordinator)" coordinator >/dev/null; then
    echo "Coordinator port is open, but the launched process exited; see $coordinator_log" >&2
    return 1
  fi
  printf 'Started coordinator pid=%s bind=%s run_id=%s log=%s\n' \
    "$coordinator_pid" "$bind" "$RUN_ID" "$coordinator_log"

  worker_index=1
  for id in "${WORKER_IDS[@]}"; do
    worker_name="worker-$worker_index"
    local worker_log="$STATE_DIR/$worker_name-$RUN_ID.log"
    (
      unset SCOUT_DATABASE_URL SCOUT_TEST_DATABASE_URL
      exec nohup "$SCOUT_BIN" worker --coordinator "$url" --data-dir "$DATA_DIR" --worker-id "$id"
    ) > "$worker_log" 2>&1 < /dev/null &
    local worker_pid=$!
    write_pid "$(pid_file "$worker_name")" "$worker_pid"
    printf 'Started %s pid=%s worker_id=%s log=%s\n' \
      "$worker_name" "$worker_pid" "$id" "$worker_log"
    worker_index=$((worker_index + 1))
  done
  START_COMPLETE=1
  rmdir "$lock" 2>/dev/null || true
  START_LOCK=
  trap - EXIT INT TERM
  echo "Run state: $RUN_STATE"
}

finish_start() {
  local status=$?
  if [[ "${START_COMPLETE:-0}" != 1 ]]; then
    stop_processes
  fi
  if [[ -n "${START_LOCK:-}" ]]; then
    rmdir "$START_LOCK" 2>/dev/null || true
  fi
  trap - EXIT
  return "$status"
}

status_local() {
  state_paths "$1"
  local run_id config_sha active
  run_id=$(read_state_value run_id)
  config_sha=$(read_state_value source_config_sha256)
  printf 'run_id=%s\nsource_config_sha256=%s\nsource_config_path=%s\n' \
    "${run_id:-none}" "${config_sha:-none}" "$(read_state_value source_config_path)"
  active=$(list_active_pids)
  if [[ -z "$active" ]]; then
    echo "No local distributed processes are running."
  else
    while read -r name pid; do
      printf '%s pid=%s\n' "$name" "$pid"
    done <<< "$active"
  fi
}

materialize_local() {
  local config=$1 run_id history config_path config_sha actual_sha
  config=$(resolve_file "$config")
  state_paths "$2"
  if [[ -n "$(list_active_pids)" ]]; then
    fail "stop the coordinator and workers before materializing"
    return 1
  fi
  run_id=${3:-$(read_state_value run_id)}
  [[ -n "$run_id" ]] || { fail "no saved run ID; pass one explicitly"; return 1; }
  history="$STATE_DIR/runs/$run_id.state"
  if [[ -f "$history" ]]; then
    config_path=$(read_state_value source_config_path "$history")
    config_sha=$(read_state_value source_config_sha256 "$history")
    actual_sha=$(shasum -a 256 "$config" | awk '{print $1}')
    [[ "$config" == "$config_path" && "$actual_sha" == "$config_sha" ]] || {
      fail "materialization must use the saved config path and contents for run $run_id"
      return 1
    }
  fi
  SCOUT_BIN=${SCOUT_BIN:-$ROOT/target/release/scout}
  [[ -x "$SCOUT_BIN" ]] || { fail "build scout first or set SCOUT_BIN"; return 1; }
  "$SCOUT_BIN" crawl --config "$config" --data-dir "$DATA_DIR" --materialize-spool "$run_id"
}

main() {
  local command=${1:-}
  case "$command" in
    setup)
      [[ $# -eq 1 ]] || { usage; return 2; }
      setup_postgres
      ;;
    start)
      [[ $# -eq 3 ]] || { usage; return 2; }
      start_local "$2" "$3"
      ;;
    stop)
      [[ $# -eq 2 ]] || { usage; return 2; }
      state_paths "$2"
      stop_processes
      echo "Local coordinator and workers stopped; PostgreSQL and crawl data were kept."
      ;;
    restart)
      [[ $# -eq 3 ]] || { usage; return 2; }
      state_paths "$3"
      stop_processes
      start_local "$2" "$3"
      ;;
    status)
      [[ $# -eq 2 ]] || { usage; return 2; }
      status_local "$2"
      ;;
    materialize)
      [[ $# -eq 3 || $# -eq 4 ]] || { usage; return 2; }
      materialize_local "$2" "$3" "${4:-}"
      ;;
    cleanup)
      [[ $# -eq 2 ]] || { usage; return 2; }
      state_paths "$2"
      stop_processes
      rm -rf "$STATE_DIR"
      echo "Removed local launcher logs/state only; PostgreSQL, crawls, blobs, and corpora were kept."
      ;;
    --help|-h|help)
      usage
      ;;
    *)
      usage >&2
      return 2
      ;;
  esac
}

main "$@"
