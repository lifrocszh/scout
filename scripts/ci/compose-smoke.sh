#!/usr/bin/env bash
# Smoke test for the Compose stack: start it with N workers, let it crawl the
# fixture site and build a keyword index, then check /v1/readyz and search
# results against the known fixture pages. The obs profile (Prometheus, Grafana)
# runs too, and compose-obs-checks.sh asserts that the key metrics reached
# Prometheus and that the Grafana dashboard is provisioned. Tears the stack down
# (volumes included) on exit, and dumps logs first if anything failed.
#
# Usage: scripts/ci/compose-smoke.sh [workers]        (default 3)
# Env:   SCOUT_IMAGE          image to run; unset builds ./Dockerfile as scout:local
#        POSTGRES_PASSWORD    default: random
#        SCOUT_HTTP_PORT      default: 8080
#        PROMETHEUS_PORT, GRAFANA_PORT   default: 9090, 3000
#        SMOKE_MEMORY_LIMIT_MIB  budget for the sum of per-container peak memory (default 2048)
set -euo pipefail
cd "$(dirname "$0")/../.."
command -v jq >/dev/null || { echo "smoke: jq is required" >&2; exit 2; }

workers=${1:-3}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-$(openssl rand -hex 16)}
export SCOUT_HTTP_PORT=${SCOUT_HTTP_PORT:-8080}
# A dedicated project name, so tearing down never touches a developer's own
# `scout` stack or its volumes.
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-scout-smoke}
# Include the obs profile in every compose command below, `down -v` included.
export COMPOSE_PROFILES=obs
export GRAFANA_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-$(openssl rand -hex 12)}
# The fixture crawl ends in seconds. Keep the coordinator and workers serving
# /metrics for longer than a scrape plus a service-discovery refresh (2 s each),
# so Prometheus gets their final counters.
export SCOUT_METRICS_LINGER_SECS=${SCOUT_METRICS_LINGER_SECS:-8}
api="http://127.0.0.1:$SCOUT_HTTP_PORT"
fixture="http://fixture:8080"
memory_limit_mib=${SMOKE_MEMORY_LIMIT_MIB:-2048}
peak_file=$(mktemp)
sampler=

fail() { echo "smoke: FAIL: $*" >&2; exit 1; }
pass() { echo "smoke: ok: $*"; }

cleanup() {
  local status=$?
  if [ -n "$sampler" ]; then
    pkill -P "$sampler" 2>/dev/null || true # the docker stats and awk children
    kill "$sampler" 2>/dev/null || true
  fi
  if [ "$status" -ne 0 ]; then
    echo "::group::docker compose ps -a" >&2
    docker compose ps -a >&2 || true
    echo "::endgroup::" >&2
    echo "::group::docker compose logs" >&2
    docker compose logs --no-color --tail=300 >&2 || true
    echo "::endgroup::" >&2
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f "$peak_file"
  exit "$status"
}
trap cleanup EXIT

# Streams `docker stats` and records each container's peak memory (whole MiB) in
# a file as "<name> <MiB>" lines. Summing per-container peaks over-estimates the
# true peak, which is the safe direction for a budget check. Plain bash on
# purpose: mawk buffers a pipe and would not see the stream in time.
sample_memory() {
  local name usage value unit
  declare -A peaks=()
  # docker stats emits cursor-control codes even when its output is piped.
  docker stats --format '{{.Name}} {{.MemUsage}}' 2>/dev/null | sed -u 's/\x1b\[[0-9;]*[A-Za-z]//g' |
    while read -r name usage _; do
      case $name in "$COMPOSE_PROJECT_NAME"-*) ;; *) continue ;; esac # not ours
      value=${usage%%[A-Za-z]*}; unit=${usage#"$value"}; value=${value%%.*}
      case $unit in GiB) value=$((value * 1024)) ;; MiB) ;; *) value=0 ;; esac
      if [ "$value" -gt "${peaks[$name]:--1}" ]; then
        peaks[$name]=$value
        for name in "${!peaks[@]}"; do echo "$name ${peaks[$name]}"; done >"$peak_file"
      fi
    done
}

search() { # <query> -> response JSON (keyword mode: the default index is keyword-only)
  curl -fsS -X POST "$api/v1/search" -H 'content-type: application/json' \
    -d "$(jq -nc --arg q "$1" '{query: $q, mode: "keyword", limit: 5}')"
}

sample_memory &
sampler=$!

started=$(date +%s)
docker compose up -d --scale worker="$workers"
pass "stack up with $workers workers in $(( $(date +%s) - started ))s"

for _ in $(seq 1 30); do
  curl -fsS "$api/v1/readyz" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS "$api/v1/readyz" | jq -e '.status == "ready"' >/dev/null || fail "/v1/readyz is not ready"
pass "/v1/readyz is ready"

# Workers exit 0 when the crawl ends. A worker that starts after it ended leaves
# quietly (see docker/stack.sh), so require that most of them joined and did work.
for id in $(docker compose ps -a -q worker); do
  code=$(docker inspect -f '{{.State.ExitCode}}' "$id")
  [ "$code" = 0 ] || fail "a worker exited with $code"
done
joined=$(docker compose logs --no-color worker | grep -c '"event":"worker.started"' || true)
[ "$joined" -ge 2 ] || fail "expected at least 2 of $workers workers to join the crawl, saw $joined"
tasks=$(docker compose logs --no-color worker | grep -o '"completed_tasks":[0-9]*' | cut -d: -f2 | paste -sd' ' -)
pass "$joined of $workers workers joined the crawl (completed tasks per worker: $tasks)"
for service in coordinator index; do
  code=$(docker inspect -f '{{.State.ExitCode}}' "$(docker compose ps -a -q "$service")")
  [ "$code" = 0 ] || fail "$service exited with $code"
done
pass "coordinator and index exited 0"

# The fixture has 11 crawlable pages; the one in robots.txt Disallow is excluded.
pages=$(curl -fsS "$api/v1/generation" | jq -r '.page_count')
[ "$pages" = 11 ] || fail "expected 11 pages in the Generation, got $pages"
pass "Generation holds 11 pages"

top=$(search 'rebalance consumer group' | jq -r '.results[0].url')
[ "$top" = "$fixture/tutorials/scaling-consumers.html" ] || fail "unexpected top result for 'rebalance consumer group': $top"
pass "keyword search finds the scaling tutorial first"

# The query is plain text: Tantivy syntax characters are searched for, not parsed, so none of these
# is an HTTP 400 (the curl in search() would fail) or an empty result.
for query in '--rebalance' 'rebalance: (consumers' 'consumers "rebalance'; do
  count=$(search "$query" | jq -r '.results | length')
  [ "$count" -ge 1 ] || fail "plain-text query '$query' returned no results"
done
pass "keyword search treats syntax characters as text"

# Only the sitemap lists this page, so finding it shows sitemap discovery works.
hit=$(search 'gunnera' | jq -r '.results[0].url')
[ "$hit" = "$fixture/archive/changelog.html" ] || fail "sitemap-only page not indexed: $hit"
pass "sitemap-only page is indexed"

# robots.txt disallows /private/, so a correct crawl never indexes it.
leaked=$(search 'quokka' | jq -r '.results | length')
[ "$leaked" = 0 ] || fail "robots.txt-disallowed page leaked into the index ($leaked results)"
pass "robots.txt-disallowed page is not indexed"

scripts/ci/compose-obs-checks.sh

sleep 2 # one more sampling interval
[ -s "$peak_file" ] || fail "the memory sampler recorded nothing"
peak=$(awk '{ sum += $2 } END { printf "%d", sum }' "$peak_file")
echo "smoke: per-container peak memory (MiB):" >&2
sort "$peak_file" | sed 's/^/smoke:   /' >&2
echo "smoke: sum of per-container peaks: ${peak} MiB (budget ${memory_limit_mib} MiB)"
[ "$peak" -lt "$memory_limit_mib" ] || fail "peak memory ${peak} MiB exceeds ${memory_limit_mib} MiB"
echo "smoke: PASS"
