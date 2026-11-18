#!/usr/bin/env bash
# End-to-end check of the optional Kafka ingestion log (ADR 0003) on Docker Compose: run the
# stack with the `kafka` and `obs` profiles and a deliberately slow reducer, then check that
#
#   * the core stack is unaffected: the crawl, index and search API finish as without Kafka;
#   * the reducer rebuilt the snapshot from Kafka alone and it equals the live one (parity);
#   * no capture was lost or doubled (applied results == replica spool rows == live captures);
#   * the reducer's lag grew while it was slow, then drained to zero, and the relay caught up;
#   * Prometheus scraped the relay and the reducer, so the Grafana Kafka panels have data.
#
# Every wait polls observable state (container status, Prometheus series) with a deadline.
# Tears the stack down (volumes included) on exit, and dumps logs first if anything failed.
#
# Usage: scripts/ci/compose-kafka-checks.sh [workers]        (default 2)
# Env:   SCOUT_IMAGE                  image to run; unset builds ./Dockerfile as scout:local
#        SCOUT_KAFKA_RECORD_DELAY_MS  reducer slowdown per result (default 1500)
#        POSTGRES_PASSWORD, SCOUT_HTTP_PORT, PROMETHEUS_PORT, GRAFANA_PORT, KAFKA_PORT
set -euo pipefail
cd "$(dirname "$0")/../.."
command -v jq >/dev/null || { echo "kafka: jq is required" >&2; exit 2; }

workers=${1:-2}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-$(openssl rand -hex 16)}
export SCOUT_HTTP_PORT=${SCOUT_HTTP_PORT:-8080}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-scout-kafka}
export COMPOSE_PROFILES=obs,kafka
export GRAFANA_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-$(openssl rand -hex 12)}
# Short-lived processes keep /metrics up for a last scrape (2 s interval and discovery refresh).
export SCOUT_METRICS_LINGER_SECS=${SCOUT_METRICS_LINGER_SECS:-8}
# A slow consumer: lag has to build up behind the crawl and then drain.
export SCOUT_KAFKA_RECORD_DELAY_MS=${SCOUT_KAFKA_RECORD_DELAY_MS:-1500}
prom="http://127.0.0.1:${PROMETHEUS_PORT:-9090}"
results_topic=scout.crawl-results
window=30m

fail() { echo "kafka: FAIL: $*" >&2; exit 1; }
pass() { echo "kafka: ok: $*"; }

cleanup() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "::group::docker compose ps -a" >&2
    docker compose ps -a >&2 || true
    echo "::endgroup::" >&2
    echo "::group::docker compose logs" >&2
    docker compose logs --no-color --tail=200 >&2 || true
    echo "::endgroup::" >&2
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

container() { docker compose ps -a -q "$1" | head -n1; }
state() { docker inspect -f '{{.State.Status}}' "$(container "$1")" 2>/dev/null || echo missing; }
exit_code() { docker inspect -f '{{.State.ExitCode}}' "$(container "$1")"; }

# wait_exited <service> <seconds>
wait_exited() {
  local service=$1 deadline=$((SECONDS + $2))
  until [ "$(state "$service")" = exited ]; do
    [ "$SECONDS" -lt "$deadline" ] || fail "$service did not finish within $2 s (state: $(state "$service"))"
    sleep 2
  done
}

prom_value() {
  curl -fsS -G "$prom/api/v1/query" --data-urlencode "query=$1" |
    jq -r '.data.result[0].value[1] // empty'
}

# wait_for <description> <awk condition on v> <promql>
wait_for() {
  local description=$1 condition=$2 expr=$3 value=
  for _ in $(seq 1 30); do
    value=$(prom_value "$expr" 2>/dev/null || true)
    if [ -n "$value" ] && awk -v v="$value" "BEGIN { exit !($condition) }"; then
      pass "$description: $value"
      return 0
    fi
    sleep 2
  done
  fail "$description: got '${value:-no data}' for: $expr"
}

started=$SECONDS
docker compose up -d --scale worker="$workers"
pass "stack up with the kafka and obs profiles in $((SECONDS - started))s"

# The core stack first: Kafka must not change what it does.
for service in coordinator index; do
  wait_exited "$service" 600
  [ "$(exit_code "$service")" = 0 ] || fail "$service exited with $(exit_code "$service")"
done
curl -fsS "http://127.0.0.1:$SCOUT_HTTP_PORT/v1/readyz" | jq -e '.status == "ready"' >/dev/null ||
  fail "/v1/readyz is not ready"
pass "crawl, index and serve finished as without Kafka"

live=$(docker compose logs --no-color index |
  sed -n 's/.*crawl materialization complete: .*snapshot_id=\([^ ]*\).*/\1/p' | tail -n1)
[ -n "$live" ] || fail "no live snapshot ID in the index logs"

wait_exited kafka-reducer 600
wait_exited kafka-relay 120
for service in kafka-relay kafka-reducer; do
  [ "$(exit_code "$service")" = 0 ] || fail "$service exited with $(exit_code "$service")"
done
relay=$(docker logs "$(container kafka-relay)" 2>/dev/null | grep '^{' | tail -n1)
summary=$(docker logs "$(container kafka-reducer)" 2>/dev/null | grep '^{' | tail -n1)
echo "kafka: relay summary: $relay"
echo "kafka: reducer summary: $summary"
jq -e '.pending_events == 0' <<<"$relay" >/dev/null || fail "the relay left events behind"
jq -e '.parity == true' <<<"$summary" >/dev/null || fail "the replica differs from the live snapshot"
jq -e --arg live "$live" '.replica_snapshot_id == $live and .live_snapshot_id == $live' \
  <<<"$summary" >/dev/null || fail "replica snapshot is not the live snapshot $live"
pass "snapshot rebuilt from Kafka equals the live snapshot $live"

captures=$(docker compose exec -T postgres psql -U scout -d scout -tAc \
  "SELECT count(*) FROM scout_crawl_task_results WHERE crawl_run_id = '$(docker compose exec -T postgres psql -U scout -d scout -tAc 'SELECT crawl_run_id FROM scout_crawl_runs LIMIT 1')' AND capture_metadata IS NOT NULL")
jq -e --argjson n "$captures" '.results_applied == $n and .spool_captures == $n' <<<"$summary" >/dev/null ||
  fail "expected $captures applied results and spool rows"
pass "$captures captures applied once each, none lost or doubled"

# Backpressure: with a slow reducer the lag builds up, then drains; the crawl never waited for it.
wait_for "reducer and relay scraped" "v >= 2" \
  "count(last_over_time(scout_build_info{role=~\"kafka-relay|kafka-reducer\"}[$window]))"
wait_for "peak consumer lag on $results_topic" "v >= 3" \
  "max_over_time(sum(scout_kafka_consumer_lag_records{topic=\"$results_topic\"})[$window:2s])"
wait_for "final consumer lag" "v == 0" \
  "sum(last_over_time(scout_kafka_consumer_lag_records[$window]))"
wait_for "final relay lag" "v == 0" "sum(last_over_time(scout_kafka_relay_pending_events[$window]))"
wait_for "parity gauge" "v == 1" "min(last_over_time(scout_kafka_snapshot_parity[$window]))"
wait_for "results applied (metric)" "v >= $captures" \
  "sum(max_over_time(scout_kafka_consumed_records_total{topic=\"$results_topic\",outcome=\"applied\"}[$window]))"

echo "kafka: PASS in $((SECONDS - started))s"
