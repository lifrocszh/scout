#!/usr/bin/env bash
# Observability checks for a Compose stack started with the obs profile, after the
# fixture crawl has finished and the search API has served a few keyword queries.
# compose-smoke.sh runs this; it also works by hand against your own stack:
#
#   SCOUT_METRICS_LINGER_SECS=8 docker compose --profile obs up -d --scale worker=3
#   scripts/ci/compose-obs-checks.sh
#
# It asks Prometheus (not the processes) for the key series, so it covers the
# whole path: /metrics endpoints, scrape config, worker discovery. Then it checks
# that Grafana is up with the provisioned datasource and dashboard, and that every
# dashboard query is valid PromQL.
#
# The coordinator and workers exit when the crawl ends, and Prometheus marks their
# series stale from then on, so instant queries skip them. The checks use
# *_over_time(...[15m]) selectors, which still see what was scraped before.
#
# Env: PROMETHEUS_PORT (9090), GRAFANA_PORT (3000), GRAFANA_ADMIN_PASSWORD (admin),
#      COMPOSE_PROJECT_NAME (for `docker compose exec`).
set -euo pipefail
cd "$(dirname "$0")/../.."
command -v jq >/dev/null || { echo "obs: jq is required" >&2; exit 2; }

prom="http://127.0.0.1:${PROMETHEUS_PORT:-9090}"
grafana="http://127.0.0.1:${GRAFANA_PORT:-3000}"
dashboard=deploy/grafana/dashboards/scout.json
window=15m

fail() { echo "obs: FAIL: $*" >&2; exit 1; }
pass() { echo "obs: ok: $*"; }

# Value of the first series of a PromQL instant query; empty when there is none.
prom_value() {
  curl -fsS -G "$prom/api/v1/query" --data-urlencode "query=$1" |
    jq -r '.data.result[0].value[1] // empty'
}

# wait_for_at_least <description> <minimum> <promql>: poll until the value reaches the minimum,
# because the last scrape of a process that just exited can still be on its way.
wait_for_at_least() {
  local description=$1 minimum=$2 expr=$3 value=
  for _ in $(seq 1 30); do
    value=$(prom_value "$expr" 2>/dev/null || true)
    if [ -n "$value" ] && awk -v v="$value" -v m="$minimum" 'BEGIN { exit !(v >= m) }'; then
      pass "$description: $value (>= $minimum)"
      return 0
    fi
    sleep 2
  done
  fail "$description: got '${value:-no data}', want >= $minimum for: $expr"
}

for _ in $(seq 1 30); do
  curl -fsS "$prom/-/ready" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS "$prom/-/ready" >/dev/null || fail "Prometheus is not ready at $prom"

# Prometheus loaded deploy/prometheus/alerts.yml and every alert rule evaluates without error. A rule
# stays "unknown" until its group's first evaluation (evaluation_interval, 15 s). Whether an alert
# fires is covered by the promtool unit tests (scripts/ci/prometheus-rules.sh), not here.
expected_alerts=$(grep -c '^ *- alert:' deploy/prometheus/alerts.yml)
loaded=0
for _ in $(seq 1 40); do
  loaded=$(curl -fsS "$prom/api/v1/rules" | jq '[.data.groups[].rules[] | select(.type == "alerting" and .health == "ok")] | length' 2>/dev/null || echo 0)
  [ "$loaded" = "$expected_alerts" ] && break
  sleep 2
done
[ "$loaded" = "$expected_alerts" ] || fail "Prometheus evaluates $loaded of the $expected_alerts alert rules in alerts.yml without error"
pass "all $expected_alerts alert rules are loaded and evaluate"

# The fixture has 11 crawlable pages and one more that only the sitemap lists.
wait_for_at_least "tasks claimed" 11 "sum(max_over_time(scout_task_claims_total[$window]))"
wait_for_at_least "task results accepted" 11 "sum(max_over_time(scout_task_completions_total{outcome=\"accepted\"}[$window]))"
wait_for_at_least "pages fetched by workers" 11 "sum(max_over_time(scout_pages_fetched_total{outcome=\"success\"}[$window]))"
wait_for_at_least "page requests for the fixture origin" 11 "sum(max_over_time(scout_origin_requests_total{origin=\"http://fixture:8080\",kind=\"page\"}[$window]))"
wait_for_at_least "frontier tasks succeeded" 11 "sum(last_over_time(scout_frontier_tasks{status=\"succeeded\"}[$window]))"
wait_for_at_least "DB pool wait observations" 1 "sum(max_over_time(scout_db_pool_wait_seconds_count[$window]))"
wait_for_at_least "workers scraped" 2 "count(last_over_time(scout_build_info{role=\"worker\"}[$window]))"
wait_for_at_least "coordinator scraped" 1 "count(last_over_time(scout_build_info{role=\"coordinator\"}[$window]))"
wait_for_at_least "serve scraped" 1 "count(last_over_time(scout_build_info{role=\"serve\"}[$window]))"
wait_for_at_least "keyword searches timed" 3 "sum(max_over_time(scout_search_duration_seconds_count{mode=\"keyword\"}[$window]))"

# Rare events do not happen in a clean crawl, but their series must exist at zero so
# that the first one shows up in rate().
for series in scout_lease_expirations_total scout_stale_results_total scout_lease_renewals_total \
  scout_task_failures_total scout_frontier_expired_leases scout_db_pool_connections \
  scout_frontier_dead_letter_requeues; do
  wait_for_at_least "$series is exposed" 1 "count(last_over_time($series[$window]))"
done

# The same text a scraper sees, straight from the live search API (bash only: the image has no curl).
exposition=$(docker compose exec -T serve bash -c \
  'exec 3<>/dev/tcp/127.0.0.1/9103 && printf "GET /metrics HTTP/1.0\r\n\r\n" >&3 && cat <&3') ||
  fail "could not read /metrics from the serve container"
grep -q '^# TYPE scout_search_duration_seconds histogram' <<<"$exposition" || fail "serve /metrics has no search latency histogram"
grep -q 'Content-Type: text/plain; version=0.0.4' <<<"$exposition" || fail "serve /metrics has the wrong content type"
pass "serve /metrics exposes the search latency histogram"
# The served Corpus's duplicate counts come from the reduce step's near-duplicates.json.
grep -q '^scout_corpus_pages{origin="http://fixture:8080"} [1-9]' <<<"$exposition" ||
  fail "serve /metrics has no Corpus page count for the fixture origin"
pass "serve /metrics exposes the Corpus duplicate gauges"

for _ in $(seq 1 30); do
  curl -fsS "$grafana/api/health" 2>/dev/null | jq -e '.database == "ok"' >/dev/null && break
  sleep 2
done
curl -fsS "$grafana/api/health" | jq -e '.database == "ok"' >/dev/null || fail "Grafana is not healthy at $grafana"
pass "Grafana is healthy"

curl -fsS -u "admin:${GRAFANA_ADMIN_PASSWORD:-admin}" "$grafana/api/datasources/uid/scout-prometheus/health" |
  jq -e '.status == "OK"' >/dev/null || fail "the provisioned Prometheus datasource is not healthy"
pass "provisioned Prometheus datasource is healthy"

panels=$(curl -fsS "$grafana/api/dashboards/uid/scout-overview" | jq '[.dashboard.panels[] | select(.type != "row")] | length')
expected=$(jq '[.panels[] | select(.type != "row")] | length' "$dashboard")
[ "$panels" = "$expected" ] || fail "Grafana serves $panels dashboard panels, $dashboard has $expected"
pass "dashboard 'scout-overview' is provisioned with $panels panels"

# Every query in the dashboard must be valid PromQL against the live Prometheus.
queries=0
while IFS= read -r expr; do
  expr=${expr//\$__rate_interval/30s}
  expr=${expr//\$__range/$window}
  curl -fsS -G "$prom/api/v1/query" --data-urlencode "query=$expr" | jq -e '.status == "success"' >/dev/null ||
    fail "dashboard query is invalid: $expr"
  queries=$((queries + 1))
done < <(jq -r '.panels[].targets[]?.expr' "$dashboard")
[ "$queries" -ge 20 ] || fail "expected at least 20 dashboard queries, found $queries"
pass "all $queries dashboard queries are valid PromQL"
echo "obs: PASS"
