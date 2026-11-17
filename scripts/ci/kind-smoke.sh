#!/usr/bin/env bash
# Smoke test for the Helm release on kind. Run `make kind-up` first.
#
#   1. The default release (fixture crawl -> index -> serve): /v1/readyz, the same search
#      assertions as the Compose smoke test, `helm test`, probes, metrics, a parked coordinator.
#   2. Scaling: a second, slower crawl over synthetic origins starts on one worker; scaling to 8
#      with `kubectl scale` (no config change) must put the new workers to work before it ends.
#   3. HPA (HPA=1): with metrics-server installed, the autoscaler reads the workers' CPU.
#
# Usage: scripts/ci/kind-smoke.sh
# Env:   CLUSTER, NAMESPACE, RELEASE, IMAGE_REPO, IMAGE_TAG   as in the Makefile
#        SCOUT_HTTP_PORT   host port the NodePort is mapped to (default 8080)
#        HPA=1             also run phase 3 (installs metrics-server)
set -euo pipefail
cd "$(dirname "$0")/../.."
command -v jq >/dev/null || { echo "kind-smoke: jq is required" >&2; exit 2; }

CLUSTER=${CLUSTER:-scout}
NAMESPACE=${NAMESPACE:-scout}
RELEASE=${RELEASE:-scout}
api="http://127.0.0.1:${SCOUT_HTTP_PORT:-8080}"
fixture="http://$RELEASE-fixture:8080"
tmp=$(mktemp -d)

k() { kubectl --context "kind-$CLUSTER" -n "$NAMESPACE" "$@"; }
fail() { echo "kind-smoke: FAIL: $*" >&2; exit 1; }
pass() { echo "kind-smoke: ok: $*"; }
deploy() { make --no-print-directory kind-deploy CLUSTER="$CLUSTER" NAMESPACE="$NAMESPACE" RELEASE="$RELEASE" \
  IMAGE_REPO="${IMAGE_REPO:-scout}" IMAGE_TAG="${IMAGE_TAG:-kind}" HELM_ARGS="$*" >/dev/null; }

cleanup() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    echo "::group::cluster state" >&2
    k get pods,jobs,deploy,hpa -o wide >&2 || true
    k describe pods >&2 || true
    echo "::endgroup::" >&2
    for component in coordinator worker serve index postgres; do
      echo "::group::logs: $component" >&2
      k logs -l "app.kubernetes.io/component=$component" --all-containers --prefix --tail=120 >&2 || true
      echo "::endgroup::" >&2
    done
  fi
  rm -rf "$tmp"
  exit "$status"
}
trap cleanup EXIT

search() { # <query> -> response JSON (keyword mode: the default index is keyword-only)
  curl -fsS -X POST "$api/v1/search" -H 'content-type: application/json' \
    -d "$(jq -nc --arg q "$1" '{query: $q, mode: "keyword", limit: 5}')"
}

worker_logs() { k logs -l app.kubernetes.io/component=worker -c worker --prefix --tail=-1 --ignore-errors 2>/dev/null; }

# Distinct worker pods that finished at least one task.
workers_with_tasks() { worker_logs | grep '"event":"worker.task_completed"' | cut -d' ' -f1 | sort -u | wc -l; }

# ---- 1. the default release ------------------------------------------------------------------
k rollout status "deploy/$RELEASE-serve" --timeout=5m >/dev/null
k wait --for=condition=complete job -l app.kubernetes.io/component=index --timeout=3m >/dev/null
curl -fsS "$api/v1/readyz" | jq -e '.status == "ready"' >/dev/null || fail "/v1/readyz is not ready"
pass "/v1/readyz is ready through the NodePort"

pages=$(curl -fsS "$api/v1/generation" | jq -r '.page_count')
[ "$pages" = 11 ] || fail "expected 11 pages in the Generation, got $pages"
pass "Generation holds 11 pages"

top=$(search 'rebalance consumer group' | jq -r '.results[0].url')
[ "$top" = "$fixture/tutorials/scaling-consumers.html" ] || fail "unexpected top result: $top"
pass "keyword search finds the scaling tutorial first"
hit=$(search 'gunnera' | jq -r '.results[0].url')
[ "$hit" = "$fixture/archive/changelog.html" ] || fail "sitemap-only page not indexed: $hit"
pass "sitemap-only page is indexed"
leaked=$(search 'quokka' | jq -r '.results | length')
[ "$leaked" = 0 ] || fail "robots.txt-disallowed page leaked into the index ($leaked results)"
pass "robots.txt-disallowed page is not indexed"

helm --kube-context "kind-$CLUSTER" -n "$NAMESPACE" test "$RELEASE" --timeout 3m >/dev/null || fail "helm test failed"
pass "helm test"

# The coordinator stopped itself after the crawl and the pod is parked, still ready.
k exec "deploy/$RELEASE-coordinator" -- test -e /tmp/crawl-finished || fail "the coordinator pod is not parked"
k wait --for=condition=available "deploy/$RELEASE-coordinator" --timeout=1m >/dev/null
pass "the coordinator stopped after the crawl and its pod is parked and ready"

joined=$(worker_logs | grep -c '"event":"worker.started"' || true)
[ "$joined" -ge 2 ] || fail "expected at least 2 workers to join the crawl, saw $joined"
pass "$joined workers joined the crawl"

# Probes and metrics through the API server's pod proxy (no port-forward needed).
proxy() { k get --raw "/api/v1/namespaces/$NAMESPACE/pods/$1/proxy/$2"; }
serve_pod=$(k get pod -l app.kubernetes.io/component=serve -o name | head -n1 | cut -d/ -f2)
proxy "$serve_pod:9103" metrics | grep -q '^scout_search_requests_total{mode="keyword",outcome="ok"} [1-9]' ||
  fail "serve metrics do not count the searches above"
pass "serve /metrics counts the searches"
fetched=0
for pod in $(k get pod -l app.kubernetes.io/component=worker -o name | cut -d/ -f2); do
  n=$(proxy "$pod:9102" metrics | sed -n 's/^scout_pages_fetched_total{outcome="success"} //p' | cut -d. -f1)
  fetched=$((fetched + ${n:-0}))
done
[ "$fetched" -ge 11 ] || fail "workers report $fetched fetched pages in /metrics, expected at least 11"
pass "worker /metrics report $fetched fetched pages"

# ---- 2. scaling workers with no config change --------------------------------------------------
# 8 synthetic origins x 100 pages, 200 ms each, crawled by one worker to begin with.
origins=8 pages_per_origin=100 target=700
{
  cat <<EOF
schema_version = "scout.sources.v1"
contact_url = "https://github.com/lifrocszh/scout"
user_agent = "ScoutSmoke/0.1 (+https://github.com/lifrocszh/scout)"
embedding_model = "BAAI/bge-small-en-v1.5"
page_target = $target
max_frontier = 2000
max_total_attempts = 5000
max_duration_seconds = 600
global_concurrency = 64
origin_concurrency = 2
min_start_spacing_ms = 0
EOF
  for i in $(seq 0 $((origins - 1))); do
    cat <<EOF

[[sources]]
source_id = "origin-$i"
seeds = ["http://$RELEASE-fixture:$((9000 + i))/p/0"]
allowed_origins = ["http://$RELEASE-fixture:$((9000 + i))"]
path_prefixes = ["/p/"]
EOF
  done
} >"$tmp/sources.toml"
run2="run-$(openssl rand -hex 32)"
deploy --set "crawl.runId=$run2" --set index.enabled=false --set fixture.mode=synthetic \
  --set "fixture.synthetic.origins=$origins" --set "fixture.synthetic.pagesPerOrigin=$pages_per_origin" \
  --set fixture.synthetic.latencyMs=200 --set worker.replicas=1 --set-file "crawl.sources=$tmp/sources.toml"
k rollout status "deploy/$RELEASE-worker" --timeout=3m >/dev/null
pass "the second crawl runs on 1 worker"
k scale "deploy/$RELEASE-worker" --replicas=8 >/dev/null
k rollout status "deploy/$RELEASE-worker" --timeout=3m >/dev/null
ready=$(k get "deploy/$RELEASE-worker" -o jsonpath='{.status.readyReplicas}')
[ "$ready" = 8 ] || fail "expected 8 ready workers after scaling, got ${ready:-none}"
pass "scaled to 8 ready workers with kubectl scale"

for _ in $(seq 1 90); do
  k exec "deploy/$RELEASE-coordinator" -- test -e /tmp/crawl-finished 2>/dev/null && break
  sleep 5
done
k exec "deploy/$RELEASE-coordinator" -- test -e /tmp/crawl-finished || fail "the second crawl did not finish in time"
busy=$(workers_with_tasks)
[ "$busy" -ge 5 ] || fail "only $busy workers completed tasks after scaling to 8; expected at least 5"
pass "the second crawl finished; $busy workers completed tasks"

# ---- 3. the HPA ---------------------------------------------------------------------------------
if [ "${HPA:-0}" = 1 ]; then
  make --no-print-directory kind-metrics-server CLUSTER="$CLUSTER" >/dev/null
  run3="run-$(openssl rand -hex 32)"
  deploy --set "crawl.runId=$run3" --set index.enabled=false --set fixture.mode=synthetic \
    --set "fixture.synthetic.origins=$origins" --set "fixture.synthetic.pagesPerOrigin=$pages_per_origin" \
    --set fixture.synthetic.latencyMs=400 --set worker.autoscaling.enabled=true --set worker.autoscaling.minReplicas=1 \
    --set worker.autoscaling.maxReplicas=4 --set-file "crawl.sources=$tmp/sources.toml"
  k rollout status "deploy/$RELEASE-worker" --timeout=3m >/dev/null
  active=
  for _ in $(seq 1 60); do
    active=$(k get hpa "$RELEASE-worker" -o jsonpath='{.status.conditions[?(@.type=="ScalingActive")].status}' 2>/dev/null || true)
    [ "$active" = True ] && break
    sleep 5
  done
  [ "$active" = True ] || fail "the HPA never read worker CPU: $(k get hpa "$RELEASE-worker" -o jsonpath='{.status.conditions}')"
  cpu=$(k get hpa "$RELEASE-worker" -o jsonpath='{.status.currentMetrics[0].resource.current.averageUtilization}')
  pass "the HPA reads worker CPU (${cpu:-0}% of requests)"
fi

echo "kind-smoke: PASS"
