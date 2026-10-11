#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
LAUNCHER="$ROOT/scripts/distributed-local.sh"
SCOUT_BIN="$ROOT/target/release/scout"
DATA_DIR=./data/distributed
REFRESH_SECONDS=5

cd "$ROOT"

if [[ ! -t 0 || ! -t 1 ]]; then
  echo "Run this menu from an interactive terminal." >&2
  exit 2
fi

if [[ -z "${NO_COLOR:-}" && "${TERM:-dumb}" != dumb ]]; then
  RESET=$'\033[0m'
  BOLD=$'\033[1m'
  CYAN=$'\033[36m'
  GREEN=$'\033[32m'
  YELLOW=$'\033[33m'
else
  RESET=
  BOLD=
  CYAN=
  GREEN=
  YELLOW=
fi

clear_screen() {
  printf '\033[2J\033[H'
}

state_file() {
  printf '%s/distributed-local/run-state\n' "$DATA_DIR"
}

serve_pid_file() {
  printf '%s/distributed-local-ui/serve.pid\n' "$DATA_DIR"
}

managed_serve_pid() {
  local file pid command
  file=$(serve_pid_file)
  [[ -f "$file" ]] || return 1
  pid=$(cat "$file")
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  command=$(ps -p "$pid" -o command= 2>/dev/null || true)
  case "$command" in
    *"$SCOUT_BIN serve --data-dir $DATA_DIR --bind "*) printf '%s\n' "$pid"; return 0 ;;
    *) return 1 ;;
  esac
}

state_value() {
  local key=$1 file
  file=$(state_file)
  [[ -f "$file" ]] || return 0
  sed -n "s/^${key}=//p" "$file" | head -n 1
}

active_status() {
  "$LAUNCHER" status "$DATA_DIR" 2>/dev/null || true
}

active_workers() {
  printf '%s\n' "$1" | awk '$1 ~ /^worker-[1-3]$/ {n++} END {print n+0}'
}

coordinator_state() {
  if printf '%s\n' "$1" | awk '$1 == "coordinator" {found=1} END {exit !found}'; then
    printf 'UP\n'
  else
    printf 'down\n'
  fi
}

draw_header() {
  clear_screen
  printf '%s\n' "${BOLD}${CYAN}SCOUT  /  LOCAL DISTRIBUTED CRAWL${RESET}"
  printf '%s\n' '──────────────────────────────────────────────'
  printf 'Data folder: %s\n' "$DATA_DIR"
  local run_id status workers coordinator serve_state
  run_id=$(state_value run_id)
  if [[ -n "$run_id" ]]; then
    status=$(active_status)
    workers=$(active_workers "$status")
    coordinator=$(coordinator_state "$status")
    printf 'Saved run:   %s\n' "$run_id"
    printf 'Processes:   coordinator %s · workers %s/3\n' "$coordinator" "$workers"
  else
    printf 'Saved run:   none\n'
  fi
  if managed_serve_pid >/dev/null; then
    serve_state=UP
  else
    serve_state=down
  fi
  printf 'Search API:  %s\n' "$serve_state"
  printf '%s\n' '──────────────────────────────────────────────'
}

pause() {
  read -r -p 'Press Enter to continue… ' _ || true
}

choose_config() {
  local config choice index
  local configs=()
  shopt -s nullglob
  configs=(data/configs/sources*.toml)
  shopt -u nullglob

  printf '\n%sSource configuration%s\n' "$BOLD" "$RESET"
  if ((${#configs[@]} == 0)); then
    echo 'No data/configs/sources*.toml files found.'
    return 1
  fi
  for index in "${!configs[@]}"; do
    printf '  %d) %s\n' "$((index + 1))" "${configs[$index]}"
  done
  printf '  c) Enter another TOML path\n  b) Back\n'
  read -r -p 'Choose config: ' choice
  case "$choice" in
    b|B) return 1 ;;
    c|C)
      read -r -p 'Config path: ' config
      ;;
    ''|*[!0-9]*) echo 'Choose a listed number, c, or b.'; return 1 ;;
    *)
      index=$((choice - 1))
      if ((index < 0 || index >= ${#configs[@]})); then
        echo 'That config number is out of range.'
        return 1
      fi
      config=${configs[$index]}
      ;;
  esac
  if [[ ! -f "$config" ]]; then
    echo "Config file not found: $config"
    return 1
  fi
  SELECTED_CONFIG=$config
}

choose_data_dir() {
  local selected
  read -r -p "Data folder [$DATA_DIR]: " selected
  DATA_DIR=${selected:-$DATA_DIR}
}

event_count() {
  local event=$1 file=$2 count
  if [[ -f "$file" ]]; then
    count=$(grep -c "\"event\":\"$event\"" "$file" 2>/dev/null || true)
    printf '%s\n' "${count:-0}"
  else
    printf '?\n'
  fi
}

choose_snapshot() {
  local path choice index
  local candidates=() snapshots=()
  shopt -s nullglob
  candidates=("$DATA_DIR"/corpora/snapshot-*)
  shopt -u nullglob
  for path in "${candidates[@]}"; do
    [[ -f "$path/snapshot-manifest.json" ]] && snapshots+=("${path##*/}")
  done

  if ((${#snapshots[@]} == 0)); then
    echo 'No completed Corpus snapshots found in this data folder.'
    return 1
  fi
  printf '\n%sCorpus snapshot%s\n' "$BOLD" "$RESET"
  for index in "${!snapshots[@]}"; do
    printf '  %d) %s\n' "$((index + 1))" "${snapshots[$index]}"
  done
  printf '  b) Back\n'
  read -r -p 'Choose snapshot: ' choice
  case "$choice" in
    b|B) return 1 ;;
    ''|*[!0-9]*) echo 'Choose a listed number or b.'; return 1 ;;
  esac
  index=$((choice - 1))
  if ((index < 0 || index >= ${#snapshots[@]})); then
    echo 'That snapshot number is out of range.'
    return 1
  fi
  SELECTED_SNAPSHOT=${snapshots[$index]}
}

prepare_index_profile() {
  local snapshot=$1 profile template current created=0 answer matches
  profile="$DATA_DIR/config/index.toml"
  template="$ROOT/data/configs/archive/index-real-model-10k-low-memory.toml"
  if [[ ! -f "$profile" ]]; then
    mkdir -p "$(dirname "$profile")"
    cp "$template" "$profile"
    sed -i '' '/^model_name = /d' "$profile"
    created=1
    printf 'Created keyword build profile from %s\n' "${template##*/}"
  fi
  if grep -q '^model_name = ' "$profile"; then
    echo 'Remove model_name from the index profile; the crawl config owns model selection.'
    return 1
  fi
  matches=$(grep -c '^snapshot_id = "' "$profile" || true)
  if [[ "$matches" != 1 ]]; then
    echo "Expected one snapshot_id in $profile; edit the profile and retry."
    return 1
  fi
  current=$(sed -n 's/^snapshot_id = "\([^"]*\)"$/\1/p' "$profile" | head -n 1)
  if [[ "$current" != "$snapshot" ]]; then
    if ((created == 0)); then
      read -r -p "Profile pins $current. Update it to $snapshot? [y/N] " answer
      [[ "$answer" == y || "$answer" == Y ]] || return 1
    else
      printf 'Pinning new profile to %s\n' "$snapshot"
    fi
    sed -i '' "s/^snapshot_id = \".*\"$/snapshot_id = \"$snapshot\"/" "$profile"
  fi
}

build_keyword_index() {
  local output generation
  if ! choose_snapshot; then
    pause
    return
  fi
  if ! prepare_index_profile "$SELECTED_SNAPSHOT"; then
    pause
    return
  fi
  printf '\n%sCompiling Scout release build…%s\n' "$BOLD" "$RESET"
  if ! (cd "$ROOT" && cargo build --release --bin scout); then
    echo 'Build failed; scroll up for compiler details.'
    pause
    return
  fi
  printf '\n%sBuilding keyword index for %s…%s\n' "$BOLD" "$SELECTED_SNAPSHOT" "$RESET"
  if ! output=$("$SCOUT_BIN" index build --corpus "$SELECTED_SNAPSHOT" \
    --data-dir "$DATA_DIR" --mode keyword-only); then
    echo 'Index build failed; see the operational log for details.'
    pause
    return
  fi
  printf '%s\n' "$output"
  generation=$(printf '%s\n' "$output" | \
    sed -n 's/^index build complete: generation_id=\([^ ]*\).*/\1/p')
  if [[ -z "$generation" ]]; then
    echo 'Could not read the Generation ID from build output.'
    pause
    return
  fi
  printf '\n%sActivating Generation %s…%s\n' "$GREEN" "$generation" "$RESET"
  if ! "$SCOUT_BIN" index activate --generation "$generation" --data-dir "$DATA_DIR"; then
    pause
    return
  fi
  if ! "$SCOUT_BIN" index verify --data-dir "$DATA_DIR"; then
    pause
    return
  fi
  printf '\nKeyword search ready. Search mode: keyword.\n'
  pause
}

wait_for_serve() {
  local url=$1 attempt
  for ((attempt = 0; attempt < 30; attempt++)); do
    if curl -fsS --connect-timeout 1 --max-time 2 "$url/v1/readyz" >/dev/null 2>&1; then
      return 0
    fi
    managed_serve_pid >/dev/null || return 1
    sleep 1
  done
  return 1
}

stop_serve_process() {
  local file pid attempt
  file=$(serve_pid_file)
  if pid=$(managed_serve_pid); then
    kill -INT "$pid" 2>/dev/null || true
    for ((attempt = 0; attempt < 10; attempt++)); do
      managed_serve_pid >/dev/null || break
      sleep 1
    done
    if pid=$(managed_serve_pid); then
      echo 'Search API did not stop; sending SIGKILL.' >&2
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
  rm -f "$file"
}

start_serve() {
  local bind url log pid
  if pid=$(managed_serve_pid); then
    printf 'Search API already running (pid=%s).\n' "$pid"
    pause
    return
  fi
  if [[ ! -x "$SCOUT_BIN" ]]; then
    printf '\n%sCompiling Scout release build…%s\n' "$BOLD" "$RESET"
    if ! (cd "$ROOT" && cargo build --release --bin scout); then
      echo 'Build failed; scroll up for compiler details.'
      pause
      return
    fi
  fi
  printf '\n%sChecking active Generation…%s\n' "$BOLD" "$RESET"
  if ! "$SCOUT_BIN" index verify --data-dir "$DATA_DIR"; then
    echo 'Build and activate a Generation before starting the Search API.'
    pause
    return
  fi
  read -r -p "Bind address [${SCOUT_SERVE_BIND:-127.0.0.1:8080}]: " bind
  bind=${bind:-${SCOUT_SERVE_BIND:-127.0.0.1:8080}}
  url="http://$bind"
  log="$DATA_DIR/logs/serve.log"
  mkdir -p "$(dirname "$(serve_pid_file)")" "$(dirname "$log")"
  rm -f "$(serve_pid_file)"
  printf '\n%sStarting Search API at %s…%s\n' "$GREEN" "$url" "$RESET"
  nohup "$SCOUT_BIN" serve --data-dir "$DATA_DIR" --bind "$bind" \
    > "$log" 2>&1 < /dev/null &
  pid=$!
  printf '%s\n' "$pid" > "$(serve_pid_file)"
  if ! wait_for_serve "$url"; then
    echo "Search API did not become ready; see $log"
    stop_serve_process
    tail -n 20 "$log" 2>/dev/null || true
    pause
    return
  fi
  printf '\nSearch API ready at %s (pid=%s).\n' "$url" "$pid"
  printf 'Keyword search: curl -X POST %s/v1/search -H \'content-type: application/json\' -d \'{"query":"ownership and borrowing","mode":"keyword","limit":10}\'\n' "$url"
  pause
}

stop_serve() {
  local pid file
  file=$(serve_pid_file)
  if ! pid=$(managed_serve_pid); then
    rm -f "$file"
    echo 'Search API is not running for this data folder.'
    pause
    return
  fi
  printf 'Stopping Search API (pid=%s)…\n' "$pid"
  stop_serve_process
  echo 'Search API stopped.'
  pause
}

monitor_run() {
  local run_id config log status workers coordinator accepted rejected target key
  run_id=$(state_value run_id)
  if [[ -z "$run_id" ]]; then
    echo 'No saved crawl run for this data folder.'
    pause
    return
  fi
  config=$(state_value source_config_path)
  log="$DATA_DIR/logs/$run_id.jsonl"
  target=
  if [[ -f "$config" ]]; then
    target=$(sed -n 's/^[[:space:]]*page_target[[:space:]]*=[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$config" | head -n 1)
  fi

  while true; do
    status=$(active_status)
    workers=$(active_workers "$status")
    coordinator=$(coordinator_state "$status")
    accepted=$(event_count capture.accepted "$log")
    rejected=$(event_count page.rejected "$log")
    draw_header
    printf '\n%sRUN MONITOR%s\n\n' "$BOLD" "$RESET"
    printf 'Run ID:            %s\n' "$run_id"
    if [[ -n "$target" && "$accepted" != '?' ]]; then
      printf 'Accepted captures: %s / %s\n' "$accepted" "$target"
    else
      printf 'Accepted captures: %s\n' "$accepted"
    fi
    printf 'Rejected pages:    %s\n' "$rejected"
    printf 'Coordinator:       %s\n' "$coordinator"
    printf 'Workers:           %s / 3\n' "$workers"
    printf 'Updated:           %s\n' "$(date '+%H:%M:%S')"
    printf '\n%s[m]%s menu   %s[s]%s stop crawl   Refreshes every %s seconds\n' \
      "$GREEN" "$RESET" "$YELLOW" "$RESET" "$REFRESH_SECONDS"

    if [[ "$coordinator" == down && "$workers" == 0 ]]; then
      printf '\n%sNo Scout processes detected. Choose Materialize after the crawl is stopped.%s\n' "$YELLOW" "$RESET"
      pause
      return
    fi

    key=
    if read -r -s -n 1 -t "$REFRESH_SECONDS" key; then
      case "$key" in
        m|M) return ;;
        s|S)
          printf '\nStopping coordinator and workers…\n'
          "$LAUNCHER" stop "$DATA_DIR"
          pause
          return
          ;;
      esac
    fi
  done
}

start_run() {
  if ! choose_config; then
    pause
    return
  fi
  choose_data_dir
  local status
  status=$(active_status)
  if printf '%s\n' "$status" | grep -qE '^(coordinator|worker-[1-3]) pid='; then
    printf '\nA crawl already runs in this data folder. Opening its monitor.\n'
    monitor_run
    return
  fi

  printf '\n%sCompiling Scout release build…%s\n' "$BOLD" "$RESET"
  if ! (cd "$ROOT" && cargo build --release --bin scout); then
    echo 'Build failed; scroll up for compiler details.'
    pause
    return
  fi
  printf '\n%sStarting crawl run…%s\n' "$GREEN" "$RESET"
  if ! SCOUT_BIN="$SCOUT_BIN" "$LAUNCHER" start "$SELECTED_CONFIG" "$DATA_DIR"; then
    pause
    return
  fi
  monitor_run
}

setup_postgres() {
  printf '\n%sPreparing local PostgreSQL…%s\n' "$BOLD" "$RESET"
  if ! "$LAUNCHER" setup; then
    pause
    return
  fi
  pause
}

stop_run() {
  printf '\n%sStopping coordinator and workers…%s\n' "$BOLD" "$RESET"
  if ! "$LAUNCHER" stop "$DATA_DIR"; then
    pause
    return
  fi
  pause
}

materialize_run() {
  local run_id config status
  run_id=$(state_value run_id)
  config=$(state_value source_config_path)
  if [[ -z "$run_id" || -z "$config" ]]; then
    echo 'No saved run/config found for this data folder.'
    pause
    return
  fi
  status=$(active_status)
  if printf '%s\n' "$status" | grep -qE '^(coordinator|worker-[1-3]) pid='; then
    echo 'Stop the coordinator and workers before materializing. Choose Stop from the menu.'
    pause
    return
  fi
  printf '\n%sCompiling Scout for materialization…%s\n' "$BOLD" "$RESET"
  if ! (cd "$ROOT" && cargo build --release --bin scout); then
    echo 'Build failed; scroll up for compiler details.'
    pause
    return
  fi
  printf '\n%sSealing Corpus from run %s…%s\n' "$BOLD" "$run_id" "$RESET"
  if ! SCOUT_BIN="$SCOUT_BIN" "$LAUNCHER" materialize "$config" "$DATA_DIR"; then
    pause
    return
  fi
  pause
}

view_log() {
  local run_id path
  run_id=$(state_value run_id)
  if [[ -z "$run_id" ]]; then
    echo 'No saved run for this data folder.'
    pause
    return
  fi
  path="$DATA_DIR/distributed-local/coordinator-$run_id.log"
  if [[ ! -f "$path" ]]; then
    echo "Coordinator log not found: $path"
  else
    printf '\n%sLatest coordinator log lines%s\n\n' "$BOLD" "$RESET"
    tail -n 35 "$path"
  fi
  pause
}

quit_ui() {
  local status answer
  status=$(active_status)
  if printf '%s\n' "$status" | grep -qE '^(coordinator|worker-[1-3]) pid=' || managed_serve_pid >/dev/null; then
    read -r -p 'Crawl or Search API still active. Leave it running in background? [y/N] ' answer
    [[ "$answer" == y || "$answer" == Y ]] || return 1
  fi
  return 0
}

main() {
  local choice
  while true; do
    draw_header
    printf '\n%sWhat next?%s\n\n' "$BOLD" "$RESET"
    printf '  1) Start or resume a crawl\n'
    printf '  2) Monitor this crawl\n'
    printf '  3) Stop coordinator and workers\n'
    printf '  4) Materialize Corpus\n'
    printf '  5) Build and activate keyword index\n'
    printf '  6) Start Search API\n'
    printf '  7) Stop Search API\n'
    printf '  8) Set up local PostgreSQL\n'
    printf '  9) View coordinator log\n'
    printf ' 10) Change data folder\n'
    printf '  q) Quit\n\n'
    read -r -p 'Select: ' choice
    case "$choice" in
      1) start_run ;;
      2) monitor_run ;;
      3) stop_run ;;
      4) materialize_run ;;
      5) build_keyword_index ;;
      6) start_serve ;;
      7) stop_serve ;;
      8) setup_postgres ;;
      9) view_log ;;
      10) choose_data_dir ;;
      q|Q)
        if quit_ui; then
          clear_screen
          printf '%sScout crawl UI closed.%s\n' "$CYAN" "$RESET"
          return 0
        fi
        ;;
      *) echo 'Choose 1–10 or q.'; pause ;;
    esac
  done
}

main "$@"
