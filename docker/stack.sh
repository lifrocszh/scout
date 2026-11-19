#!/usr/bin/env bash
# Glue for the Compose stack's one-shot crawl -> index flow. Mounted read-only
# at /opt/scout/stack.sh and run as the entrypoint of two services, using the
# same scout image as every other role.
#
#   stack.sh coordinator   Run `scout coordinator`, then stop it once the crawl
#                          has completed, so the capture spool is closed before
#                          it is materialized (same order as
#                          scripts/distributed-local.sh stop -> materialize).
#   stack.sh worker        Run `scout worker` against the coordinator, or exit 0
#                          if the crawl has already completed (a restart).
#   stack.sh index         Materialize the finished crawl into a snapshot, build
#                          an index Generation (keyword-only unless
#                          SCOUT_INDEX_MODE says otherwise), activate and
#                          verify it. A no-op when a Generation is already
#                          active, so `docker compose up` is safe to repeat.
#   stack.sh kafka-relay   Optional (Compose `kafka` profile, chart kafka.enabled): copy the run's
#                          outbox into Kafka until its snapshot is published (ADR 0003).
#   stack.sh kafka-reduce  Optional: rebuild the run's snapshot from Kafka alone into its own
#                          data directory and exit 0 only if it equals the live snapshot.
#
# Settings come from the environment (see compose.yaml); nothing is hard-coded.
set -euo pipefail

DATA=${SCOUT_DATA_DIR:-/data}
CONFIG=${SCOUT_SOURCES_CONFIG:-/etc/scout/sources.toml}
PROFILE=${SCOUT_INDEX_PROFILE:-/etc/scout/index-profile.toml}
RUN_ID=${SCOUT_CRAWL_RUN_ID:?SCOUT_CRAWL_RUN_ID must be run- plus 64 hex characters}
PORT=${SCOUT_COORDINATOR_PORT:-50051}
# Where workers reach the coordinator. The chaos suite points it at a Toxiproxy
# in front of the coordinator (scripts/chaos/compose.chaos.yaml).
COORDINATOR_HOST=${SCOUT_COORDINATOR_HOST:-coordinator}
EVENT_LOG="$DATA/logs/$RUN_ID.jsonl"

log() { printf 'stack: %s\n' "$*" >&2; }

crawl_completed() { grep -qs '"event":"crawl.completed"' "$EVENT_LOG"; }

coordinator() {
  if crawl_completed; then
    log "crawl $RUN_ID already completed; nothing to crawl"
    return 0
  fi
  # The coordinator refuses non-private bind addresses, so 0.0.0.0 is out:
  # bind the container's own address on the Compose network. It has no
  # authentication, so compose.yaml never publishes this port. Kubernetes
  # passes the pod IP (SCOUT_COORDINATOR_BIND_IP, from the downward API).
  local ip=${SCOUT_COORDINATOR_BIND_IP:-}
  [ -n "$ip" ] || ip=$(hostname -i | awk '{print $1}')
  case $ip in *:*) ip="[$ip]" ;; esac # IPv6 needs brackets in host:port
  scout coordinator --config "$CONFIG" --data-dir "$DATA" --run-id "$RUN_ID" \
    --bind "$ip:$PORT" &
  local pid=$!
  # `docker compose stop` sends SIGINT here (stop_signal); pass it on.
  trap 'kill -INT "$pid" 2>/dev/null || true' INT TERM
  while kill -0 "$pid" 2>/dev/null; do
    if crawl_completed; then
      log "crawl $RUN_ID completed; stopping the coordinator"
      kill -INT "$pid"
      break
    fi
    sleep 1
  done
  wait "$pid"
}

worker() {
  # compose.yaml cannot wait for a healthy coordinator: on a restart the crawl
  # is already complete, so the coordinator exits 0 before it ever turns
  # healthy. Wait for its port instead, and leave quietly if the crawl is done.
  until (exec 3<>"/dev/tcp/$COORDINATOR_HOST/$PORT") 2>/dev/null; do
    if crawl_completed; then
      log "crawl $RUN_ID already completed; no work for this worker"
      return 0
    fi
    sleep 1
  done
  exec scout worker --coordinator "http://$COORDINATOR_HOST:$PORT" --data-dir /tmp/scout-worker
}

index() {
  if scout index verify --data-dir "$DATA" >/dev/null 2>&1; then
    log "an index Generation is already active in $DATA; nothing to do"
    return 0
  fi
  local out snapshot generation
  out=$(scout crawl --config "$CONFIG" --data-dir "$DATA" --materialize-spool "$RUN_ID")
  printf '%s\n' "$out"
  snapshot=$(sed -n 's/^crawl materialization complete: .*snapshot_id=\([^ ]*\).*/\1/p' <<<"$out")
  [ -n "$snapshot" ] || { log "could not read the snapshot ID from the materialize output"; return 1; }

  mkdir -p "$DATA/config"
  sed "s|@SNAPSHOT_ID@|$snapshot|" "$PROFILE" >"$DATA/config/index.toml"

  out=$(scout index build --corpus "$snapshot" --data-dir "$DATA" \
    --mode "${SCOUT_INDEX_MODE:-keyword-only}")
  printf '%s\n' "$out"
  generation=$(sed -n 's/^index build complete: generation_id=\([^ ]*\).*/\1/p' <<<"$out")
  [ -n "$generation" ] || { log "could not read the Generation ID from the build output"; return 1; }

  scout index activate --generation "$generation" --data-dir "$DATA"
  scout index verify --data-dir "$DATA"
  log "ready: snapshot=$snapshot generation=$generation"
}

kafka_relay() {
  # Follows the outbox from the start of the crawl and exits once the run's published snapshot
  # (its last outbox event) is in Kafka. Bodies are read from the coordinator's data directory.
  exec scout kafka relay --run-id "$RUN_ID" --data-dir "$DATA" --follow \
    --bootstrap "${SCOUT_KAFKA_BOOTSTRAP:?SCOUT_KAFKA_BOOTSTRAP must name the Kafka broker}"
}

kafka_reduce() {
  # SCOUT_KAFKA_RECORD_DELAY_MS slows the reducer down on purpose (backpressure demo).
  exec scout kafka reduce --run-id "$RUN_ID" --config "$CONFIG" \
    --data-dir "${SCOUT_REPLICA_DIR:-$DATA}" --follow \
    --record-delay-ms "${SCOUT_KAFKA_RECORD_DELAY_MS:-0}" \
    --bootstrap "${SCOUT_KAFKA_BOOTSTRAP:?SCOUT_KAFKA_BOOTSTRAP must name the Kafka broker}"
}

case "${1:-}" in
  coordinator) coordinator ;;
  worker) worker ;;
  index) index ;;
  kafka-relay) kafka_relay ;;
  kafka-reduce) kafka_reduce ;;
  *) echo "usage: stack.sh coordinator|worker|index|kafka-relay|kafka-reduce" >&2; exit 2 ;;
esac
