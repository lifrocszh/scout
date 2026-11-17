#!/usr/bin/env bash
# What to look at when the release does not come up: pod and event state, then the current and the
# previous logs of every container. Prints to stdout; never fails.
# Env: CLUSTER, NAMESPACE (defaults scout, scout)
CLUSTER=${CLUSTER:-scout}
NAMESPACE=${NAMESPACE:-scout}
k() { kubectl --context "kind-$CLUSTER" -n "$NAMESPACE" "$@"; }

echo "::group::pods, jobs, deployments, volumes"
k get pods,jobs,deploy,pvc -o wide
echo "::endgroup::"
echo "::group::describe pods"
k describe pods
echo "::endgroup::"
echo "::group::events"
k get events --sort-by=.lastTimestamp
echo "::endgroup::"
for pod in $(k get pods -o name); do
  for previous in "" "--previous"; do
    echo "::group::logs: $pod ${previous:-current}"
    # shellcheck disable=SC2086
    k logs "$pod" --all-containers --prefix --tail=100 $previous 2>&1
    echo "::endgroup::"
  done
done
exit 0
