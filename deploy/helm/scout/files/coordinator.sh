#!/usr/bin/env bash
# Entrypoint of the coordinator pod.
#
# The coordinator has to exit once the crawl completes: its last event, crawl.concurrency, is
# written at shutdown, and the index step refuses a crawl log that does not end with it. A
# Deployment restarts a container that exits. So this runs docker/stack.sh (the wrapper Compose
# uses), which runs the coordinator until crawl.completed and then stops it, and then parks the pod
# instead of exiting. A restart of a finished crawl parks again without re-running it.
set -uo pipefail

bash /etc/scout/stack.sh coordinator &
child=$!
stopping=0
trap 'stopping=1; kill -TERM "$child" 2>/dev/null' TERM INT
wait "$child"
status=$?
if [ "$stopping" = 1 ]; then
  wait "$child" 2>/dev/null
  exit 0
fi
if [ "$status" -ne 0 ]; then
  echo "coordinator: exited with status $status; letting the Deployment restart it" >&2
  exit "$status"
fi

# Probes read this file: a parked pod is healthy and ready.
touch /tmp/crawl-finished
echo "coordinator: the crawl has finished and the coordinator has stopped; parked" >&2
trap 'exit 0' TERM INT
while :; do
  sleep 3600 &
  wait $!
done
