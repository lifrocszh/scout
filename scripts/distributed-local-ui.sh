#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
LAUNCHER="$ROOT/scripts/distributed-local.sh"
MONITOR="$ROOT/scripts/distributed-local-monitor.py"
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

postgres_status() {
  local psql version candidate
  psql=
  for candidate in /opt/homebrew/opt/postgresql@17/bin/psql /usr/local/opt/postgresql@17/bin/psql; do
    if [[ -x "$candidate" ]]; then
      psql=$candidate
      break
    fi
  done
  psql=${psql:-$(command -v psql || true)}
  if [[ -z "$psql" ]]; then
    printf 'client not found — Home > 1 to set up\n'
  elif version=$(PGCONNECT_TIMEOUT=2 PGOPTIONS='-c statement_timeout=1500' \
      "$psql" -XAtqw -h /tmp -d scout -c 'SHOW server_version_num' 2>/dev/null); then
    if [[ "$version" =~ ^17[0-9]{4}$ ]]; then
      printf 'ready — local scout database accessible\n'
    else
      printf 'connected, but PostgreSQL 17 required — Home > 1\n'
    fi
  else
    printf 'unavailable — server down or database inaccessible; Home > 1\n'
  fi
}

draw_header() {
  clear_screen
  printf '%s\n' "${BOLD}${CYAN}SCOUT  /  LOCAL DISTRIBUTED CRAWL${RESET}"
  printf '%s\n' '──────────────────────────────────────────────'
  printf '%s\n' 'Scout collects web pages and turns them into a local search index.'
  printf 'Data folder: %s\n' "$DATA_DIR"
  printf 'PostgreSQL:  %s\n' "$(postgres_status)"
  local run_id status workers coordinator config phase path snapshots=0
  run_id=$(state_value run_id)
  status=$(active_status)
  workers=$(active_workers "$status")
  coordinator=$(coordinator_state "$status")
  if [[ -n "$run_id" ]]; then
    if [[ "$coordinator" == UP ]]; then
      printf 'Crawl:       running — coordinator up, %s/3 workers present\n' "$workers"
    elif ((workers > 0)); then
      printf 'Crawl:       needs attention — coordinator down, %s/3 workers present\n' "$workers"
    else
      printf 'Crawl:       stopped — saved run available to inspect or resume\n'
    fi
    config=$(state_value source_config_path)
    config=${config:-unknown}
    printf 'Source file: %s\n' "${config##*/}"
  else
    printf 'Crawl:       no saved run in this folder\n'
  fi
  for path in "$DATA_DIR"/corpora/snapshot-*/snapshot-manifest.json; do
    [[ ! -f "$path" ]] || snapshots=$((snapshots + 1))
  done
  phase=$(sed -n 's/^phase=//p' "$DATA_DIR/distributed-local-ui/build-state" 2>/dev/null | head -n 1 || true)
  if managed_build_job; then
    printf 'Collections: %s saved · index build running (%s)\n' "$snapshots" "${phase:-starting}"
  elif [[ -d "$DATA_DIR/distributed-local-ui/build.lock" ]]; then
    printf 'Collections: %s saved · build lock needs attention (Build > Monitor)\n' "$snapshots"
  elif [[ -n "$phase" ]]; then
    printf 'Collections: %s saved · last index job: %s\n' "$snapshots" "$phase"
  else
    printf 'Collections: %s saved · no index job recorded\n' "$snapshots"
  fi
  if [[ -s "$DATA_DIR/CURRENT" ]]; then
    printf 'Search index: selected (checked before serving)'
  else
    printf 'Search index: none selected'
  fi
  if managed_serve_pid >/dev/null; then
    printf ' · API process running\n'
  else
    printf ' · API stopped\n'
  fi
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
  printf 'Source files list websites to crawl and limits on how much to collect.\n'
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
    echo 'No saved collections here yet. Use Build search index > Save collected pages first.'
    return 1
  fi
  printf '\n%sCorpus snapshot%s\n' "$BOLD" "$RESET"
  printf 'Choose the saved page collection to turn into a search index.\n'
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

managed_build_job() {
  local file pid command absolute_data
  file="$DATA_DIR/distributed-local-ui/build-job.pid"
  [[ -f "$file" ]] || return 1
  pid=$(cat "$file")
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  absolute_data=$(cd "$DATA_DIR" && pwd -P)
  command=$(ps -p "$pid" -o command= 2>/dev/null || true)
  case "$command" in
    *"$ROOT/scripts/distributed-local-build.sh"*"$absolute_data"*) return 0 ;;
    *) return 1 ;;
  esac
}

build_keyword_index() {
  local absolute_data state_dir
  if [[ -d "$DATA_DIR/distributed-local-ui/build.lock" ]]; then
    echo 'Managed build lock exists. Opening Generation monitor.'
    monitor_run --view generation
    return
  fi
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
  absolute_data=$(cd "$DATA_DIR" && pwd -P)
  state_dir="$absolute_data/distributed-local-ui"
  mkdir -p "$state_dir"
  nohup bash "$ROOT/scripts/distributed-local-build.sh" "$SCOUT_BIN" "$SELECTED_SNAPSHOT" "$absolute_data" \
    > "$state_dir/build-launch-$(date '+%Y%m%d-%H%M%S')-$$.log" 2>&1 < /dev/null &
  printf '\nKeyword build started. Activation and verification follow successful build.\n'
  monitor_run --view generation
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
  printf '\n%sCompiling Scout release build…%s\n' "$BOLD" "$RESET"
  if ! (cd "$ROOT" && cargo build --release --bin scout); then
    echo 'Build failed; scroll up for compiler details.'
    pause
    return
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
  printf '%s\n' "Keyword search: curl -X POST $url/v1/search -H 'content-type: application/json' -d '{\"query\":\"ownership and borrowing\",\"mode\":\"keyword\",\"limit\":10}'"
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
  if ! command -v python3 >/dev/null 2>&1; then
    echo 'Monitoring requires Python 3 (standard library only).'
    pause
    return
  fi
  if ! python3 -B "$MONITOR" --data-dir "$DATA_DIR" --binary "$SCOUT_BIN" \
    --refresh "$REFRESH_SECONDS" "$@"; then
    echo 'Monitor failed; see the error above.'
    pause
  fi
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
    echo 'Stop the crawl first: Home > Crawl pages > Stop crawl.'
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
  if printf '%s\n' "$status" | grep -qE '^(coordinator|worker-[1-3]) pid=' || managed_serve_pid >/dev/null || managed_build_job; then
    read -r -p 'Crawl, Generation build, or Search API still active. Leave it running in background? [y/N] ' answer
    [[ "$answer" == y || "$answer" == Y ]] || return 1
  fi
  return 0
}

menu_option() {
  printf '  %s) %s\n     %s\n' "$1" "$2" "$3"
}

workflow_menu() {
  local section=$1 choice run_id
  while true; do
    draw_header
    case "$section" in
      crawl)
        printf '\n%sCRAWL PAGES%s\n' "$BOLD" "$RESET"
        printf 'A source file chooses websites, crawl limits, and the page target.\n'
        run_id=$(state_value run_id)
        printf 'Saved run: %s\n\n' "${run_id:-none}"
        menu_option 1 'Start or resume crawl' 'Choose websites and a folder; collect pages in the background.'
        menu_option 2 'Monitor pages and workers' 'See pages, remaining URLs, workers, speed, and machine resources.'
        menu_option 3 'Stop crawl' 'Stop the coordinator and workers; saved progress stays available.'
        menu_option 4 'View recent crawl log' 'Show recent coordinator messages to help investigate problems.'
        ;;
      build)
        printf '\n%sBUILD SEARCH INDEX%s\n' "$BOLD" "$RESET"
        printf 'First save collected pages as a Corpus; then build a searchable Generation.\n'
        printf 'Stop the crawl before saving its collection.\n\n'
        menu_option 1 'Save collected pages' 'Seal the saved crawl into a Corpus snapshot for indexing.'
        menu_option 2 'Build keyword search index' 'Build, select, and verify an index from a saved collection.'
        menu_option 3 'Monitor index build' 'Follow build phases, passage progress, and activation or validation errors.'
        ;;
      search)
        printf '\n%sSEARCH API%s\n' "$BOLD" "$RESET"
        printf 'The API lets applications search the selected index over local HTTP.\n'
        printf 'Build an index first. Starting the API prints its address and an example query.\n\n'
        menu_option 1 'Start Search API' 'Check the selected index, then serve it at a local address.'
        menu_option 2 'Stop Search API' 'Stop serving searches; saved pages and index remain available.'
        ;;
    esac
    printf '\n  b) Back to home\n\n'
    read -r -p 'Select an option, then press Enter: ' choice
    case "$section:$choice" in
      crawl:1) start_run ;;
      crawl:2) monitor_run ;;
      crawl:3) stop_run ;;
      crawl:4) view_log ;;
      build:1) materialize_run ;;
      build:2) build_keyword_index ;;
      build:3) monitor_run --view generation ;;
      search:1) start_serve ;;
      search:2) stop_serve ;;
      *:b|*:B) return ;;
      *) echo 'Choose a listed option or b to return home.'; pause ;;
    esac
  done
}

main() {
  local choice
  while true; do
    draw_header
    printf '%sHOME — follow steps 1–4 · checked %s%s\n\n' "$BOLD" "$(date '+%H:%M:%S')" "$RESET"
    menu_option 1 'Set up PostgreSQL' 'Install or start the local database needed for crawling.'
    menu_option 2 'Crawl pages' 'Choose websites; start, resume, monitor, or stop collecting pages.'
    menu_option 3 'Build search index' 'Save collected pages, then build and monitor a keyword index.'
    menu_option 4 'Search API' 'Start or stop the local HTTP service for searching your index.'
    menu_option 5 'Change data folder' 'Switch workspace; running jobs stay in their original folder.'
    printf '  r) Refresh status    q) Quit menu (running jobs need confirmation)\n'
    read -r -p 'Select an option, then press Enter: ' choice
    case "$choice" in
      1) setup_postgres ;;
      2) workflow_menu crawl ;;
      3) workflow_menu build ;;
      4) workflow_menu search ;;
      5)
        printf '\nChoose a workspace to inspect or use for the next action.\n'
        printf 'This does not move data or stop jobs in the previous folder.\n'
        choose_data_dir
        ;;
      r|R) ;;
      q|Q)
        if quit_ui; then
          clear_screen
          printf '%sScout crawl UI closed.%s\n' "$CYAN" "$RESET"
          return 0
        fi
        ;;
      *) echo 'Choose 1–5, r to refresh, or q to quit.'; pause ;;
    esac
  done
}

main "$@"
