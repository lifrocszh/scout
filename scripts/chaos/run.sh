#!/usr/bin/env bash
# Failure-injection suite for the Compose stack (issue #10). Each scenario starts
# a fresh stack (scripts/chaos/compose.chaos.yaml on top of compose.yaml), crawls
# 160 slow synthetic pages with 3 workers, injects one fault part-way through,
# and lets the stack recover on its own: no step after the fault touches the
# stack except to heal the fault itself (unpause, remove the toxic).
#
# Scenarios
#   baseline           no fault; its snapshot fingerprint is what the others must match
#   worker-kill        SIGKILL a worker that holds a lease (kill -9 of its container's init)
#   stale-lease        a worker stalls past its lease, then reports (SCOUT_FAULT_STALL_FIRST_LEASE_SECS)
#   coordinator-kill   SIGKILL the coordinator (kill -9 of its container's init)
#   postgres-pause     freeze Postgres for CHAOS_PAUSE_SECS (docker pause)
#   network-latency    100 ms +-20 ms each way between workers and coordinator (Toxiproxy)
#   network-partition  cut workers off the coordinator for CHAOS_PARTITION_SECS (Toxiproxy)
#
# Every scenario asserts:
#   - the crawl completes and the coordinator and every worker container end with exit 0;
#   - every frontier task succeeded, once: one accepted result per task, written by the
#     lease epoch that holds the row (a fenced-out holder never wrote);
#   - the materialized snapshot equals the baseline's (same URLs, same content identities);
# plus the scenario's own outcome (lease reclaimed, stale write fenced, restart, ...), and
# records the recovery time. See docs/runbook.md.
#
# Usage: scripts/chaos/run.sh [scenario ...|fast|all]   (default: all)
#        fast = baseline worker-kill stale-lease (the pull-request set)
# Env:   SCOUT_IMAGE         image to run; unset builds ./Dockerfile as scout:local
#        CHAOS_OUT           results directory (default target/chaos)
#        CHAOS_WORKERS       workers per run (default 3)
#        CHAOS_FAULT_AT      pages succeeded before a fault is injected (default 40 of 160)
#        CHAOS_PAUSE_SECS, CHAOS_PARTITION_SECS, CHAOS_STALL_SECS   fault lengths (10, 10, 10)
#        CHAOS_TIMEOUT_SECS  limit for one crawl (default 300)
#        TOXIPROXY_PORT      host port of the Toxiproxy API (default 8474)
#        TOXIPROXY_IMAGE     Toxiproxy image (default ghcr.io/shopify/toxiproxy:2.12.0)
#        COMPOSE_FILE        compose files (default compose.yaml:scripts/chaos/compose.chaos.yaml)
set -euo pipefail
cd "$(dirname "$0")/../.."
# Process faults kill a container's init from the host, so they need root or passwordless sudo.
for tool in jq curl docker; do
  command -v "$tool" >/dev/null || { echo "chaos: $tool is required" >&2; exit 2; }
done

all_scenarios=(baseline worker-kill stale-lease coordinator-kill postgres-pause network-latency network-partition)
case "${1:-all}" in
  all) scenarios=("${all_scenarios[@]}") ;;
  fast) scenarios=(baseline worker-kill stale-lease) ;;
  *) scenarios=("$@") ;;
esac
for name in "${scenarios[@]}"; do
  [[ " ${all_scenarios[*]} " == *" $name "* ]] || { echo "chaos: unknown scenario: $name" >&2; exit 2; }
done
# Every comparison needs a baseline fingerprint: run it first even when not asked for.
ordered=(baseline)
for name in "${scenarios[@]}"; do [ "$name" = baseline ] || ordered+=("$name"); done
scenarios=("${ordered[@]}")

out=${CHAOS_OUT:-target/chaos}
workers=${CHAOS_WORKERS:-3}
fault_at=${CHAOS_FAULT_AT:-40}
pause_secs=${CHAOS_PAUSE_SECS:-10}
partition_secs=${CHAOS_PARTITION_SECS:-10}
stall_secs=${CHAOS_STALL_SECS:-10}
timeout_secs=${CHAOS_TIMEOUT_SECS:-300}
pages_per_origin=${CHAOS_PAGES_PER_ORIGIN:-40}
expected_pages=$((4 * pages_per_origin))
toxiproxy="http://127.0.0.1:${TOXIPROXY_PORT:-8474}"

export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-$(openssl rand -hex 16)}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-scout-chaos}
# Append more override files with ':' (a registry mirror, a local image, ...).
export COMPOSE_FILE=${COMPOSE_FILE:-compose.yaml:scripts/chaos/compose.chaos.yaml}
# The stale-lease worker is a `compose run` container; teardown removes it.
export COMPOSE_IGNORE_ORPHANS=true
export SCOUT_METRICS_LINGER_SECS=${SCOUT_METRICS_LINGER_SECS:-6}
export CHAOS_PAGES_PER_ORIGIN=$pages_per_origin
run_id=${SCOUT_CRAWL_RUN_ID:-run-5c0a7f1e0d3b4a29b8e6f4a1c7d2e9035b8a6f4c1e7d0a39b2c5e8f1a4d7b0c3}

mkdir -p "$out"
results="$out/results.jsonl"
: >"$results"
baseline_fingerprint=
sampler=
scenario=
dir=

dc() { docker compose --progress quiet "$@"; }
log() { echo "chaos[$scenario]: $*" >&2; }
fail() { echo "chaos[$scenario]: FAIL: $*" >&2; exit 1; }
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
sql() { dc exec -T postgres psql -U scout -d scout -v ON_ERROR_STOP=1 -Atc "$1"; }
succeeded() { sql "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id='$run_id' AND status='succeeded'"; }

# Wait until <command> succeeds, checking every 0.5 s; fail after <secs>.
wait_until() {
  local secs=$1 what=$2; shift 2
  local deadline=$(( $(date +%s) + secs ))
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || fail "timed out after ${secs}s waiting for $what"
    sleep 0.5
  done
}
at_least_succeeded() { [ "$(succeeded 2>/dev/null || echo 0)" -ge "$1" ]; }
reclaimed() { [ "$(sql "SELECT lease_epoch > $2 FROM scout_crawl_tasks WHERE task_id = $1" 2>/dev/null)" = t ]; }
run_completed() { [ "$(sql "SELECT status FROM scout_crawl_runs WHERE crawl_run_id='$run_id'" 2>/dev/null)" = completed ]; }

# Coordinator /metrics, read inside its container (the port is not published).
scrape() {
  dc exec -T coordinator bash -c \
    'exec 3<>/dev/tcp/127.0.0.1/9101 && printf "GET /metrics HTTP/1.0\r\n\r\n" >&3 && cat <&3' 2>/dev/null
}

# Keep the last scrape of every coordinator process (one file per container start), so the
# counters of a coordinator that was killed and restarted still add up.
sample_metrics() {
  local cid started body
  while :; do
    cid=$(dc ps -q coordinator 2>/dev/null || true)
    if [ -n "$cid" ] && started=$(docker inspect -f '{{.State.StartedAt}}' "$cid" 2>/dev/null) &&
      body=$(scrape) && grep -q '^scout_build_info' <<<"$body"; then
      printf '%s\n' "$body" >"$dir/metrics-${started//[^0-9]/}.prom"
    fi
    sleep 1
  done
}

# metric <name> [label filter regex]: sum over every coordinator process and matching series.
metric() {
  cat "$dir"/metrics-*.prom 2>/dev/null |
    awk -v name="$1" -v filter="${2:-}" '{ n = $1; sub(/\{.*/, "", n) } n == name && $1 ~ filter { sum += $2 } END { printf "%d", sum }'
}

worker_id_of() { # <container> -> the worker ID of its latest start
  docker logs "$1" 2>&1 | grep '^{' | jq -r 'select(.event == "worker.started") | .data.worker_id' 2>/dev/null | tail -1
}

restarts() { # <service> -> restarts summed over its containers
  local total=0 cid
  for cid in $(dc ps -a -q "$1"); do
    total=$((total + $(docker inspect -f '{{.RestartCount}}' "$cid")))
  done
  echo "$total"
}

as_root() { if [ "$(id -u)" = 0 ]; then "$@"; else sudo -n "$@"; fi; }

# SIGKILL a container's init process from the host, the way a kernel OOM kill or a crashed
# node ends it. `docker kill` would count as a manual stop, and the restart policy ignores those.
crash() { as_root kill -KILL "$(docker inspect -f '{{.State.Pid}}' "$1")"; }

toxi() { curl -fsS -X "$1" "$toxiproxy$2" ${3:+-H 'content-type: application/json' -d "$3"} >/dev/null; }

stop_sampler() {
  if [ -n "$sampler" ]; then kill "$sampler" 2>/dev/null || true; wait "$sampler" 2>/dev/null || true; fi
  sampler=
}

teardown() {
  stop_sampler
  dc down -v --remove-orphans >/dev/null 2>&1 || true
}

write_summary() {
  [ -s "$results" ] || return 0
  {
    echo "| Scenario | Result | Crawl (s) | Recovery (s) | Stale results fenced | Leases reclaimed | Worker restarts | Coordinator restarts | Snapshot |"
    echo "|---|---|---|---|---|---|---|---|---|"
    jq -r 'if .passed then "| \(.scenario) | pass | \(.crawl_secs) | \(.recovery_secs // "-") | \(.stale_results) | \(.lease_expirations) | \(.worker_restarts) | \(.coordinator_restarts) | `\(.snapshot_fingerprint[0:12])` |"
           else "| \(.scenario) | **FAIL** | | | | | | | |" end' "$results"
  } >"$out/summary.md"
}

on_exit() {
  local status=$?
  if [ "$status" -ne 0 ] && [ -n "$scenario" ]; then
    jq -nc --arg s "$scenario" '{scenario: $s, passed: false}' >>"$results"
  fi
  write_summary
  if [ "$status" -ne 0 ] && [ -n "$dir" ]; then
    dc ps -a >&2 || true
    dc logs --no-color --timestamps >"$dir/compose.log" 2>&1 || true
    docker logs --timestamps scout-chaos-stale-worker >"$dir/stale-worker.log" 2>&1 || rm -f "$dir/stale-worker.log"
    echo "chaos: logs in $dir/compose.log" >&2
  fi
  teardown
  exit "$status"
}
trap on_exit EXIT

start_stack() {
  teardown
  dc up -d postgres chaos-fixture toxiproxy coordinator >/dev/null
  wait_until 60 "Toxiproxy API" curl -fsS "$toxiproxy/version" -o /dev/null
  sample_metrics &
  sampler=$!
}

start_workers() { dc up -d --no-recreate --scale worker="$workers" worker >/dev/null; }

# ---- scenarios: each one injects its fault, heals it if needed, and sets `recovery` -----------
# `recovery` is seconds from the fault (or its end, for faults with a length) until the
# behaviour the scenario names has happened. `checks` collects scenario-specific JSON.

scenario_baseline() {
  start_workers
  recovery=null
  checks='{}'
}

scenario_worker_kill() {
  start_workers
  wait_until 120 "$fault_at pages" at_least_succeeded "$fault_at"
  local cid id held= attempt
  # Freeze a worker, so its leases cannot change while we look, and kill it only if it holds one.
  for attempt in $(seq 1 50); do
    cid=$(dc ps -q worker | shuf -n 1)
    id=$(worker_id_of "$cid")
    docker pause "$cid" >/dev/null
    held=$(sql "SELECT string_agg(task_id || ':' || lease_epoch, ' ') FROM scout_crawl_tasks
                WHERE crawl_run_id='$run_id' AND status='leased' AND lease_owner='$id'")
    if [ -n "$held" ]; then break; fi
    docker unpause "$cid" >/dev/null
    sleep 0.2
  done
  [ -n "$held" ] || fail "no worker held a lease in 50 tries"
  # Killed while frozen: it never runs another instruction after we read its leases.
  crash "$cid"
  docker unpause "$cid" >/dev/null 2>&1 || true
  local killed_at; killed_at=$(now)
  log "killed worker $id holding task:epoch $held"
  local pair task epoch
  for pair in $held; do
    task=${pair%%:*}; epoch=${pair##*:}
    wait_until 60 "task $task to be reclaimed" reclaimed "$task" "$epoch"
  done
  recovery=$(since "$killed_at")
  log "every lease of the killed worker was reclaimed ${recovery}s after the kill"
  checks=$(jq -nc --arg id "$id" --arg held "$held" '{killed_worker: $id, orphaned_leases: ($held | split(" "))}')
  victim=$id
  orphans=$held
}

scenario_stale_lease() {
  # The stalling worker starts first, so it is sure to claim a task while the crawl is young.
  dc run -d --no-deps --name scout-chaos-stale-worker \
    -e SCOUT_FAULT_STALL_FIRST_LEASE_SECS="$stall_secs" worker >/dev/null
  # awk reads to the end: `grep -m1`/`grep -q` would close the pipe early, and pipefail would fail it.
  stall_event() { docker logs scout-chaos-stale-worker 2>&1 | awk '/"event":"worker.fault_stall"/ && !n++ { print; f = 1 } END { exit !f }'; }
  wait_until 60 "the stall to start" stall_event
  local stalled_at event task epoch id
  stalled_at=$(now)
  start_workers
  event=$(stall_event)
  task=$(jq -r '.data.task_id' <<<"$event"); epoch=$(jq -r '.data.lease_epoch' <<<"$event")
  id=$(jq -r '.data.worker_id' <<<"$event")
  log "worker $id holds task $task (epoch $epoch) for ${stall_secs}s without renewing"
  wait_until 60 "task $task to be reclaimed" reclaimed "$task" "$epoch"
  recovery=$(since "$stalled_at")
  log "task $task reclaimed ${recovery}s after the stall began"
  stale_report() { docker logs scout-chaos-stale-worker 2>&1 | grep '"event":"worker.task_stale"' | grep "\"task_id\":$task[,}]" >/dev/null; }
  wait_until $((stall_secs + 30)) "the stale report to be fenced" stale_report
  log "the late report for task $task was rejected as stale"
  checks=$(jq -nc --arg id "$id" --argjson task "$task" --argjson epoch "$epoch" \
    '{stalled_worker: $id, task_id: $task, stalled_epoch: $epoch}')
  victim=$id
  orphans="$task:$epoch"
}

scenario_coordinator_kill() {
  start_workers
  wait_until 120 "$fault_at pages" at_least_succeeded "$fault_at"
  local before killed_at
  crash "$(dc ps -q coordinator)"
  killed_at=$(now)
  before=$(succeeded)
  log "killed the coordinator at $before pages"
  wait_until 120 "progress after the coordinator restart" at_least_succeeded $((before + 1))
  recovery=$(since "$killed_at")
  log "first new page accepted ${recovery}s after the kill"
  checks=$(jq -nc --argjson before "$before" '{pages_at_kill: $before}')
}

scenario_postgres_pause() {
  start_workers
  wait_until 120 "$fault_at pages" at_least_succeeded "$fault_at"
  local before healed_at
  before=$(succeeded)
  dc pause postgres >/dev/null
  log "paused Postgres at $before pages for ${pause_secs}s"
  sleep "$pause_secs"
  dc unpause postgres >/dev/null
  healed_at=$(now)
  wait_until 120 "progress after unpause" at_least_succeeded $((before + 1))
  recovery=$(since "$healed_at")
  log "first new page accepted ${recovery}s after unpause"
  checks=$(jq -nc --argjson before "$before" --argjson secs "$pause_secs" '{pages_at_pause: $before, pause_secs: $secs}')
}

scenario_network_latency() {
  local toxic
  for stream in upstream downstream; do
    toxic=$(jq -nc --arg s "$stream" '{name: ("latency_" + $s), type: "latency", stream: $s, attributes: {latency: 100, jitter: 20}}')
    toxi POST /proxies/coordinator/toxics "$toxic"
  done
  log "added 100 ms +-20 ms latency in both directions for the whole crawl"
  start_workers
  recovery=0
  checks='{"latency_ms": 100, "jitter_ms": 20}'
}

scenario_network_partition() {
  start_workers
  wait_until 120 "$fault_at pages" at_least_succeeded "$fault_at"
  local healed_at before
  toxi POST /proxies/coordinator '{"enabled": false}'
  log "partitioned the workers from the coordinator for ${partition_secs}s"
  sleep "$partition_secs"
  before=$(succeeded)
  toxi POST /proxies/coordinator '{"enabled": true}'
  healed_at=$(now)
  wait_until 120 "progress after the partition healed" at_least_succeeded $((before + 1))
  recovery=$(since "$healed_at")
  log "first new page accepted ${recovery}s after the partition healed"
  checks=$(jq -nc --argjson before "$before" --argjson secs "$partition_secs" '{pages_at_heal: $before, partition_secs: $secs}')
}

# ---- shared assertions ------------------------------------------------------------------------

wait_exited_zero() { # <service>: every container of it exited 0 (for worker: the stalling one too)
  local cid state code extra=
  [ "$1" != worker ] || extra=$(docker ps -a -q -f name='^scout-chaos-stale-worker$')
  for cid in $(dc ps -a -q "$1") $extra; do
    state=$(docker inspect -f '{{.State.Status}}' "$cid")
    [ "$state" = exited ] || return 1
    code=$(docker inspect -f '{{.State.ExitCode}}' "$cid")
    [ "$code" = 0 ] || return 1
  done
}

check_frontier() {
  local row total ok bad results fenced captures reclaimed
  row=$(sql "SELECT count(*),
                    count(*) FILTER (WHERE status = 'succeeded'),
                    count(*) FILTER (WHERE status <> 'succeeded'),
                    count(*) FILTER (WHERE lease_epoch > 1)
             FROM scout_crawl_tasks WHERE crawl_run_id = '$run_id'")
  IFS='|' read -r total ok bad reclaimed <<<"$row"
  row=$(sql "SELECT count(*),
                    count(*) FILTER (WHERE r.lease_epoch <> t.lease_epoch),
                    count(*) FILTER (WHERE r.capture_metadata IS NOT NULL)
             FROM scout_crawl_task_results r JOIN scout_crawl_tasks t USING (task_id)
             WHERE t.crawl_run_id = '$run_id'")
  IFS='|' read -r results fenced captures <<<"$row"
  [ "$total" = "$expected_pages" ] || fail "frontier has $total tasks, want $expected_pages"
  [ "$bad" = 0 ] || fail "$bad tasks did not succeed"
  [ "$results" = "$total" ] || fail "$results accepted results for $total tasks"
  [ "$fenced" = 0 ] || fail "$fenced accepted results came from a lease epoch that no longer holds the task"
  [ "$captures" = "$expected_pages" ] || fail "$captures captures, want $expected_pages"
  log "frontier: $ok/$total tasks succeeded once, each result from the current lease epoch; $reclaimed reclaimed"
  frontier=$(jq -nc --argjson t "$total" --argjson r "$results" --argjson re "$reclaimed" \
    '{tasks: $t, accepted_results: $r, reclaimed_tasks: $re}')
}

# The victim's leases must have ended with another holder's result.
check_victim() {
  local pair task epoch owner
  for pair in $orphans; do
    task=${pair%%:*}; epoch=${pair##*:}
    owner=$(sql "SELECT r.lease_owner || ':' || r.lease_epoch FROM scout_crawl_task_results r WHERE task_id = $task")
    [ "${owner%%:*}" != "$victim" ] || fail "task $task was accepted from the faulted worker $victim"
    [ "${owner##*:}" -gt "$epoch" ] || fail "task $task accepted at epoch ${owner##*:}, not after $epoch"
  done
  log "the faulted worker's tasks were completed by other workers at a later epoch"
}

snapshot_fingerprint() {
  local outp snapshot
  outp=$(dc run --rm --no-deps --entrypoint scout index \
    crawl --config /etc/scout/sources.toml --data-dir /data --materialize-spool "$run_id" 2>"$dir/materialize.log")
  snapshot=$(sed -n 's/^crawl materialization complete: .*snapshot_id=\([^ ]*\).*/\1/p' <<<"$outp")
  [ -n "$snapshot" ] || fail "materialize printed no snapshot ID: $outp"
  dc run --rm --no-deps --entrypoint cat index "/data/corpora/$snapshot/pages.jsonl" >"$dir/pages.jsonl"
  local pages unique
  pages=$(wc -l <"$dir/pages.jsonl")
  unique=$(jq -r '.normalized_final_url' "$dir/pages.jsonl" | sort -u | wc -l)
  [ "$pages" = "$expected_pages" ] || fail "snapshot has $pages pages, want $expected_pages"
  [ "$unique" = "$pages" ] || fail "snapshot has $((pages - unique)) duplicate URLs"
  jq -r '[.normalized_final_url, .content_identity] | @tsv' "$dir/pages.jsonl" | sort | sha256sum | cut -c1-64
}

run_scenario() {
  scenario=$1
  dir="$out/$scenario"
  rm -rf "$dir"; mkdir -p "$dir"
  recovery=null checks='{}' victim= orphans=
  log "starting"
  local started; started=$(now)
  start_stack
  "scenario_${scenario//-/_}"
  wait_until "$timeout_secs" "the crawl to complete" run_completed
  local crawl_secs; crawl_secs=$(since "$started")
  wait_until 60 "coordinator exit 0" wait_exited_zero coordinator
  wait_until 60 "workers exit 0" wait_exited_zero worker
  stop_sampler
  check_frontier
  [ -z "$victim" ] || check_victim

  local stale expirations renew_rejected worker_restarts coordinator_restarts fingerprint
  stale=$(metric scout_stale_results_total)
  expirations=$(metric scout_lease_expirations_total)
  renew_rejected=$(metric scout_lease_renewals_total 'outcome="rejected"')
  worker_restarts=$(restarts worker)
  coordinator_restarts=$(restarts coordinator)
  ls "$dir"/metrics-*.prom >/dev/null 2>&1 || fail "no coordinator metrics were scraped"

  case $scenario in
    baseline | network-latency)
      [ "$stale" = 0 ] || fail "$stale stale results in a run that should lose no lease"
      [ "$expirations" = 0 ] || fail "$expirations leases expired in a run that should lose none"
      ;;
    worker-kill)
      [ "$expirations" -ge 1 ] || fail "no lease expiry was reclaimed after the kill"
      [ "$worker_restarts" -ge 1 ] || fail "the killed worker was not restarted"
      ;;
    stale-lease)
      [ "$expirations" -ge 1 ] || fail "the stalled lease was not reclaimed"
      [ "$(metric scout_stale_results_total 'rpc="complete_task"')" -ge 1 ] ||
        fail "scout_stale_results_total{rpc=\"complete_task\"} did not count the late report"
      ;;
    coordinator-kill)
      [ "$coordinator_restarts" -ge 1 ] || fail "the coordinator was not restarted"
      dc logs --no-color coordinator | grep '"event":"coordinator.resumed"' >/dev/null ||
        fail "the restarted coordinator did not resume the run"
      ;;
    network-partition)
      [ "$worker_restarts" -ge 1 ] || fail "no worker restarted after losing the coordinator"
      ;;
  esac

  fingerprint=$(snapshot_fingerprint)
  if [ "$scenario" = baseline ]; then
    baseline_fingerprint=$fingerprint
  elif [ "$fingerprint" != "$baseline_fingerprint" ]; then
    diff <(jq -r '.normalized_final_url' "$out/baseline/pages.jsonl" | sort) \
      <(jq -r '.normalized_final_url' "$dir/pages.jsonl" | sort) >&2 || true
    fail "snapshot $fingerprint differs from the baseline $baseline_fingerprint"
  fi
  log "snapshot matches the baseline ($fingerprint)"

  jq -nc --arg s "$scenario" --argjson crawl "$crawl_secs" --argjson rec "$recovery" \
    --argjson stale "$stale" --argjson exp "$expirations" --argjson rr "$renew_rejected" \
    --argjson wr "$worker_restarts" --argjson cr "$coordinator_restarts" --arg fp "$fingerprint" \
    --argjson frontier "$frontier" --argjson checks "$checks" \
    '{scenario: $s, passed: true, crawl_secs: $crawl, recovery_secs: $rec,
      stale_results: $stale, lease_expirations: $exp, renewals_rejected: $rr,
      worker_restarts: $wr, coordinator_restarts: $cr, snapshot_fingerprint: $fp,
      frontier: $frontier, details: $checks}' >>"$results"
  dc logs --no-color --timestamps >"$dir/compose.log" 2>&1 || true
  docker logs --timestamps scout-chaos-stale-worker >"$dir/stale-worker.log" 2>&1 || rm -f "$dir/stale-worker.log"
  log "PASS (crawl ${crawl_secs}s, recovery ${recovery/null/-}s, stale $stale, lease expirations $expirations)"
  teardown
  scenario=
}

for name in "${scenarios[@]}"; do
  run_scenario "$name"
done
dir=

write_summary
cat "$out/summary.md"
echo "chaos: PASS (${#scenarios[@]} scenarios; results in $results)"
