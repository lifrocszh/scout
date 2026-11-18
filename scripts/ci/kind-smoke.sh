#!/usr/bin/env bash
# Smoke test for the Helm release on kind. Run `make kind-up` first.
#
#   1. The default release (fixture crawl -> index -> serve): /v1/readyz, the same search
#      assertions as the Compose smoke test, `helm test`, probes, metrics, a parked coordinator.
#   2. Scaling: a second, slower crawl over synthetic origins starts on one worker; scaling to 8
#      with `kubectl scale` (no config change) must put the new workers to work before it ends.
#   3. Failure: `kubectl delete pod` of one worker in the middle of a 3-worker crawl. The worker must
#      stop on SIGTERM and hand its lease back, the Deployment must replace it, and the crawl must
#      still capture every page once (the assertions of scripts/chaos/run.sh).
#   4. HPA (HPA=1): with metrics-server installed, the autoscaler reads the workers' CPU.
#
# Usage: scripts/ci/kind-smoke.sh
# Env:   CLUSTER, NAMESPACE, RELEASE, IMAGE_REPO, IMAGE_TAG   as in the Makefile
#        SCOUT_HTTP_PORT   host port the NodePort is mapped to (default 8080)
#        HPA=1             also run phase 4 (installs metrics-server)
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
    # A deleted pod's logs are gone from the cluster; phase 3 kept a copy.
    for log in "$tmp"/deleted-worker-*.log; do
      [ -e "$log" ] || continue
      echo "::group::logs of the deleted worker: ${log##*/}" >&2
      cat "$log" >&2
      echo "::endgroup::" >&2
    done
    for component in coordinator worker serve index postgres; do
      echo "::group::logs: $component" >&2
      k logs -l "app.kubernetes.io/component=$component" --all-containers --prefix --tail=120 >&2 || true
      echo "::endgroup::" >&2
    done
  fi
  jobs -p | xargs -r kill 2>/dev/null || true
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
# Crawl config over the synthetic origins (deploy/helm/scout/files/origins.py): <origins> hosts on
# ports 9000.., each a tree of pages under /p/.
synthetic_sources() { # <origins> <page_target>
  cat <<EOF
schema_version = "scout.sources.v1"
contact_url = "https://github.com/lifrocszh/scout"
user_agent = "ScoutSmoke/0.1 (+https://github.com/lifrocszh/scout)"
embedding_model = "BAAI/bge-small-en-v1.5"
page_target = $2
max_frontier = 2000
max_total_attempts = 5000
max_duration_seconds = 600
global_concurrency = 64
origin_concurrency = 2
min_start_spacing_ms = 0
EOF
  for i in $(seq 0 $(($1 - 1))); do
    cat <<EOF

[[sources]]
source_id = "origin-$i"
seeds = ["http://$RELEASE-fixture:$((9000 + i))/p/0"]
allowed_origins = ["http://$RELEASE-fixture:$((9000 + i))"]
path_prefixes = ["/p/"]
EOF
  done
}

# 8 synthetic origins x 100 pages, 200 ms each, crawled by one worker to begin with.
origins=8 pages_per_origin=100 target=700
synthetic_sources "$origins" "$target" >"$tmp/sources.toml"
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

# ---- 3. a worker pod deleted in the middle of a crawl ------------------------------------------
# 3 workers crawl 4 synthetic origins x 16 pages, 3 s each. Once some pages are done, `kubectl delete
# pod` removes a worker that holds a lease: a normal graceful delete, so the kubelet sends SIGTERM.
# drainSeconds is 0 here, so the worker hands its in-flight task back at once instead of finishing it
# (finishing inside the window is tests/worker_drain.rs) and another pod has to claim that task.
# Nothing touches the release after the delete. What must hold is what scripts/chaos/run.sh checks:
# every task succeeded once, from the lease epoch that holds it, and the snapshot has every page once.
fault_origins=4 fault_pages_per_origin=16 fault_workers=3 fault_at=12
fault_pages=$((fault_origins * fault_pages_per_origin))
released_error='worker stopped before the task finished' # what release_lease() sends as the failure
run_fault="run-$(openssl rand -hex 32)"

sql() { k exec "pod/$RELEASE-postgres-0" -- psql -U scout -d scout -v ON_ERROR_STOP=1 -Atc "$1"; }
succeeded() { sql "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id = '$run_fault' AND status = 'succeeded'"; }
released() { sql "SELECT count(*) FROM scout_crawl_tasks WHERE crawl_run_id = '$run_fault' AND last_error = '$released_error'"; }
at_least_succeeded() { [ "$(succeeded 2>/dev/null || echo 0)" -ge "$1" ]; }
crawl_finished() { k exec "deploy/$RELEASE-coordinator" -- test -e /tmp/crawl-finished 2>/dev/null; }
# Worker pods that are Ready and not being deleted, one name per line.
ready_workers() {
  k get pods -l app.kubernetes.io/component=worker -o json 2>/dev/null |
    jq -r '.items[] | select(.metadata.deletionTimestamp == null)
           | select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) | .metadata.name' | sort
}
# Poll every 2 s until <command> succeeds; fail after <seconds>.
wait_until() {
  local secs=$1 what=$2 deadline
  shift 2
  deadline=$((SECONDS + secs))
  until "$@"; do
    [ "$SECONDS" -lt "$deadline" ] || fail "timed out after ${secs}s waiting for $what"
    sleep 2
  done
}
# Sets $victim to a worker pod that holds a lease right now (a worker's ID is its pod name).
pick_victim() {
  victim=$(sql "SELECT lease_owner FROM scout_crawl_tasks
                WHERE crawl_run_id = '$run_fault' AND status = 'leased' ORDER BY random() LIMIT 1" 2>/dev/null) &&
    [ -n "$victim" ] && k get "pod/$victim" >/dev/null 2>&1
}

synthetic_sources "$fault_origins" 1000 >"$tmp/sources-fault.toml"
deploy --set "crawl.runId=$run_fault" --set index.enabled=false --set fixture.mode=synthetic \
  --set "fixture.synthetic.origins=$fault_origins" --set "fixture.synthetic.pagesPerOrigin=$fault_pages_per_origin" \
  --set fixture.synthetic.latencyMs=3000 --set worker.replicas=0 --set worker.drainSeconds=0 \
  --set-file "crawl.sources=$tmp/sources-fault.toml"
k rollout status "deploy/$RELEASE-coordinator" --timeout=3m >/dev/null
k scale "deploy/$RELEASE-worker" --replicas="$fault_workers" >/dev/null
k rollout status "deploy/$RELEASE-worker" --timeout=3m >/dev/null
initial=$(ready_workers)
[ "$(grep -c . <<<"$initial")" = "$fault_workers" ] || fail "expected $fault_workers ready workers, got: ${initial:-none}"
pass "the third crawl runs on $fault_workers workers"

wait_until 180 "$fault_at pages to succeed" at_least_succeeded "$fault_at"
victims=() orphans=()
for attempt in 1 2 3; do
  [ "$(succeeded)" -lt "$fault_pages" ] || fail "the crawl finished before a worker could be deleted"
  wait_until 60 "a worker pod that holds a lease" pick_victim
  # A deleted pod's log is gone with it: follow it from the start into a file.
  log="$tmp/deleted-worker-$attempt.log"
  timeout 300 kubectl --context "kind-$CLUSTER" -n "$NAMESPACE" logs -f "$victim" -c worker >"$log" 2>&1 &
  logger=$!
  wait_until 30 "the log of $victim" grep -q worker.started "$log"
  echo "kind-smoke: deleting $victim at $(succeeded) of $fault_pages pages" >&2
  k delete "pod/$victim" --timeout=90s >/dev/null || fail "kubectl delete pod/$victim did not finish"
  # The follower ends with the container; give it a few seconds, then stop it either way.
  for _ in 1 2 3 4 5; do kill -0 "$logger" 2>/dev/null || break; sleep 1; done
  kill "$logger" 2>/dev/null || true
  wait "$logger" 2>/dev/null || true
  victims+=("$victim")
  # The worker caught SIGTERM and shut down by itself; the grace period did not run out.
  grep -q '"event":"worker.stopped"' "$log" || fail "$victim logged no worker.stopped: it did not stop on SIGTERM"
  while IFS= read -r pair; do
    [ -z "$pair" ] || orphans+=("$pair:$victim")
  done < <(jq -Rr 'fromjson? | select(.event == "worker.task_released" and .outcome == "success")
                   | "\(.data.task_id):\(.data.lease_epoch)"' "$log")
  [ "${#orphans[@]}" -gt 0 ] && break
  echo "kind-smoke: $victim held no task when it stopped; deleting another worker" >&2
done
[ "${#orphans[@]}" -gt 0 ] || fail "${#victims[@]} deleted workers, none held a task to hand back"
[ "$(released)" -ge 1 ] || fail "the coordinator recorded no released lease (last_error '$released_error')"
pass "${victims[*]} stopped on SIGTERM and handed back task:epoch:worker ${orphans[*]}"

replaced() { # as many new workers as deleted ones, all Ready
  local ready; ready=$(ready_workers)
  [ "$(grep -c . <<<"$ready")" = "$fault_workers" ] &&
    [ "$(comm -13 <(printf '%s\n' "$initial") <(printf '%s\n' "$ready") | grep -c .)" = "${#victims[@]}" ]
}
wait_until 180 "the Deployment to bring back $fault_workers Ready workers" replaced
pass "the Deployment replaced ${#victims[@]} deleted worker(s) with Ready pods"

wait_until 300 "the crawl to finish" crawl_finished
run_status=$(sql "SELECT status FROM scout_crawl_runs WHERE crawl_run_id = '$run_fault'")
[ "$run_status" = completed ] || fail "the crawl run is $run_status, not completed"

# Every task succeeded once (scripts/chaos/run.sh, check_frontier): none failed or dead-lettered,
# one accepted result per task from the epoch that holds it, and a capture for every page.
IFS='|' read -r total ok dead reclaimed <<<"$(sql "SELECT count(*),
    count(*) FILTER (WHERE status = 'succeeded'),
    count(*) FILTER (WHERE status = 'failed' OR dead_lettered_at IS NOT NULL),
    count(*) FILTER (WHERE lease_epoch > 1)
  FROM scout_crawl_tasks WHERE crawl_run_id = '$run_fault'")"
IFS='|' read -r results fenced captures <<<"$(sql "SELECT count(*),
    count(*) FILTER (WHERE r.lease_epoch <> t.lease_epoch),
    count(*) FILTER (WHERE r.capture_metadata IS NOT NULL)
  FROM scout_crawl_task_results r JOIN scout_crawl_tasks t USING (task_id)
  WHERE t.crawl_run_id = '$run_fault'")"
[ "$total" = "$fault_pages" ] || fail "the frontier has $total tasks, expected $fault_pages"
[ "$dead" = 0 ] || fail "$dead tasks failed or were dead-lettered"
[ "$ok" = "$total" ] || fail "only $ok of $total tasks succeeded"
[ "$results" = "$total" ] || fail "$results accepted results for $total tasks"
[ "$fenced" = 0 ] || fail "$fenced accepted results came from a lease epoch that no longer holds the task"
[ "$captures" = "$fault_pages" ] || fail "$captures captures, expected $fault_pages"
pass "all $total tasks succeeded once, none failed or dead-lettered ($reclaimed claimed again after a lease ended)"

# The tasks the deleted workers handed back were finished by other workers at a later epoch.
for pair in "${orphans[@]}"; do
  IFS=: read -r task epoch from <<<"$pair"
  IFS=: read -r owner owner_epoch <<<"$(sql "SELECT lease_owner || ':' || lease_epoch FROM scout_crawl_task_results WHERE task_id = $task")"
  [ "$owner" != "$from" ] || fail "task $task was accepted from $from, which handed it back"
  [ "$owner_epoch" -gt "$epoch" ] || fail "task $task was accepted at epoch $owner_epoch, not after $epoch"
done
pass "every handed-back task was finished by another worker at a later epoch"

# The snapshot has every page of the fixture once, the same set a crawl with no fault produces.
snapshot_out=$(k exec "deploy/$RELEASE-coordinator" -- scout crawl --config /etc/scout/sources.toml \
  --data-dir /data --materialize-spool "$run_fault" 2>"$tmp/materialize.log") ||
  { cat "$tmp/materialize.log" >&2; fail "materializing the crawl failed"; }
snapshot=$(sed -n 's/^crawl materialization complete: .*snapshot_id=\([^ ]*\).*/\1/p' <<<"$snapshot_out")
[ -n "$snapshot" ] || fail "materialize printed no snapshot ID: $snapshot_out"
k exec "deploy/$RELEASE-coordinator" -- cat "/data/corpora/$snapshot/pages.jsonl" >"$tmp/pages.jsonl" 2>/dev/null
expected=$(for o in $(seq 0 $((fault_origins - 1))); do
  for n in $(seq 0 $((fault_pages_per_origin - 1))); do echo "http://$RELEASE-fixture:$((9000 + o))/p/$n"; done
done | sort)
actual=$(jq -r '.normalized_final_url' "$tmp/pages.jsonl" | sort)
[ "$actual" = "$expected" ] || { diff <(echo "$expected") <(echo "$actual") >&2 || true; fail "the snapshot is not the fixture's $fault_pages pages, each once"; }
pass "the snapshot holds all $fault_pages pages of the fixture, each once"

# ---- 4. the HPA ---------------------------------------------------------------------------------
if [ "${HPA:-0}" = 1 ]; then
  make --no-print-directory kind-metrics-server CLUSTER="$CLUSTER" >/dev/null
  run_hpa="run-$(openssl rand -hex 32)"
  deploy --set "crawl.runId=$run_hpa" --set index.enabled=false --set fixture.mode=synthetic \
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
