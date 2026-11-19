#!/usr/bin/env bash
# Checks the Prometheus configuration and alert rules in deploy/prometheus with promtool:
#   promtool check config   prometheus.yml and prometheus-k8s.yml, including the rule files they load
#   promtool check rules    alerts.yml
#   promtool test rules     alerts_test.yml: the unit tests of the alerts
# and that every alert has a "### <Name>" section in docs/runbook.md, which its runbook_url points to.
#
# promtool comes from the same prom/prometheus image the Compose obs profile runs, so it needs
# Docker. PROMTOOL=/path/to/promtool uses a local binary instead (no Docker).
set -euo pipefail
cd "$(dirname "$0")/../.."

rules=deploy/prometheus/alerts.yml
runbook=docs/runbook.md

if [ -z "${PROMTOOL:-}" ]; then
  image=$(sed -n 's|^ *image: \(prom/prometheus:[^ ]*\).*|\1|p' compose.yaml)
  [ -n "$image" ] || { echo "prometheus-rules: no prom/prometheus image in compose.yaml" >&2; exit 2; }
  command -v docker >/dev/null || { echo "prometheus-rules: needs docker, or PROMTOOL=<binary>" >&2; exit 2; }
fi

# promtool <args>: runs inside deploy/prometheus, where the configs name the rule files.
promtool() {
  if [ -n "${PROMTOOL:-}" ]; then
    (cd deploy/prometheus && "$PROMTOOL" "$@")
  else
    docker run --rm --entrypoint promtool -v "$PWD/deploy/prometheus:/prometheus-config:ro" \
      -w /prometheus-config "$image" "$@"
  fi
}

step() { printf '\nprometheus-rules: %s\n' "$*" >&2; }

step "check config (Compose)"
promtool check config prometheus.yml
step "check config (Kubernetes)"
promtool check config prometheus-k8s.yml
step "check rules"
promtool check rules alerts.yml
step "test rules"
promtool test rules alerts_test.yml

step "runbook sections"
missing=0
while IFS= read -r name; do
  grep -qx "### $name" "$runbook" || { echo "prometheus-rules: $runbook has no '### $name' section" >&2; missing=1; }
done < <(sed -n 's/^ *- alert: \(.*\)$/\1/p' "$rules")
[ "$missing" = 0 ] || exit 1
echo "prometheus-rules: PASS"
