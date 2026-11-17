#!/usr/bin/env bash
# Smoke test for the core Compose stack: start it with N workers, let it crawl
# the fixture site and build a keyword index, then check /v1/readyz and search
# results against the known fixture pages. Tears the stack down (volumes
# included) on exit, and dumps logs first if anything failed.
#
# Usage: scripts/ci/compose-smoke.sh [workers]        (default 3)
# Env:   SCOUT_IMAGE          image to run; unset builds ./Dockerfile as scout:local
#        POSTGRES_PASSWORD    default: random
#        SCOUT_HTTP_PORT      default: 8080
#        SMOKE_MEMORY_LIMIT_MIB  peak memory budget across containers (default 2048)
set -euo pipefail
cd "$(dirname "$0")/../.."
command -v jq >/dev/null || { echo "smoke: jq is required" >&2; exit 2; }

workers=${1:-3}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-$(openssl rand -hex 16)}
export SCOUT_HTTP_PORT=${SCOUT_HTTP_PORT:-8080}
# A dedicated project name, so tearing down never touches a developer's own
# `scout` stack or its volumes.
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-scout-smoke}
api="http://127.0.0.1:$SCOUT_HTTP_PORT"
fixture="http://fixture:8080"
memory_limit_mib=${SMOKE_MEMORY_LIMIT_MIB:-2048}
peak_file=$(mktemp)
sampler=

fail() { echo "smoke: FAIL: $*" >&2; exit 1; }
pass() { echo "smoke: ok: $*"; }

cleanup() {
  local status=$?
  [ -z "$sampler" ] || kill "$sampler" 2>/dev/null || true
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

# Sum of container memory (MiB) once a second; the maximum is kept in a file.
sample_memory() {
  local peak=0 total
  while sleep 1; do
    total=$(docker stats --no-stream --format '{{.MemUsage}}' 2>/dev/null | awk '
      { v = $1; u = v; gsub(/[0-9.]/, "", u); sub(/[A-Za-z]+$/, "", v)
        if (u == "GiB") v *= 1024; else if (u == "KiB" || u == "kB") v /= 1024; else if (u == "B") v /= 1048576
        sum += v }
      END { printf "%d", sum }')
    if [ "${total:-0}" -gt "$peak" ]; then peak=$total; echo "$peak" >"$peak_file"; fi
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

# Every worker connected, and the one-shot steps finished cleanly.
registered=$(docker compose logs --no-color worker | grep -c '"event":"worker.started"' || true)
[ "$registered" -eq "$workers" ] || fail "expected $workers workers to start, saw $registered"
pass "$workers workers registered with the coordinator"
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

# Only the sitemap lists this page, so finding it shows sitemap discovery works.
hit=$(search 'gunnera' | jq -r '.results[0].url')
[ "$hit" = "$fixture/archive/changelog.html" ] || fail "sitemap-only page not indexed: $hit"
pass "sitemap-only page is indexed"

# robots.txt disallows /private/, so a correct crawl never indexes it.
leaked=$(search 'quokka' | jq -r '.results | length')
[ "$leaked" = 0 ] || fail "robots.txt-disallowed page leaked into the index ($leaked results)"
pass "robots.txt-disallowed page is not indexed"

sleep 2 # let the sampler take one more reading with only the long-lived services up
peak=$(cat "$peak_file" 2>/dev/null || echo 0)
echo "smoke: peak memory across containers: ${peak} MiB (budget ${memory_limit_mib} MiB)"
[ "$peak" -lt "$memory_limit_mib" ] || fail "peak memory ${peak} MiB exceeds ${memory_limit_mib} MiB"
echo "smoke: PASS"
