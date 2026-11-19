#!/usr/bin/env bash
# Run the #[ignore]d PostgreSQL-backed tests, one fresh database per test binary.
# Usage: PGHOST=localhost PGUSER=postgres PGPASSWORD=postgres scripts/ci/postgres-tests.sh [target...]
# Targets are `lib` or a tests/*.rs stem; default is every binary with ignored tests except the
# kafka_* ones, which also need a broker (SCOUT_TEST_KAFKA_BOOTSTRAP) and run in CI's test-kafka job.
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${PGHOST:?}" "${PGUSER:?}" "${PGPASSWORD:?}"
port="${PGPORT:-5432}"

targets=("$@")
if [ ${#targets[@]} -eq 0 ]; then
  targets=(lib)
  for file in $(grep -l '#\[ignore' tests/*.rs); do
    case $(basename "$file") in kafka_*) continue ;; esac
    targets+=("$(basename "$file" .rs)")
  done
fi

failed=()
for target in "${targets[@]}"; do
  # Some tests refuse databases without these exact names/prefixes.
  case "$target" in
    recrawl_policy_verification) db=scout_ticket10_20261014 ;;
    *) db="scout_task15_${target}" ;;
  esac
  psql -q -d postgres -c "DROP DATABASE IF EXISTS $db" -c "CREATE DATABASE $db"
  if [ "$target" = distributed_recovery ]; then
    # Appends ` application_name=...` to the DSN, so it needs the keyword form.
    url="host=$PGHOST port=$port user=$PGUSER password=$PGPASSWORD dbname=$db"
  else
    # URL form: tests parse the database name or add application_name as a query pair.
    url="postgres://$PGUSER:$PGPASSWORD@$PGHOST:$port/$db"
  fi
  if [ "$target" = lib ]; then selector=(--lib); else selector=(--test "$target"); fi
  echo "::group::cargo test ${selector[*]} -- --ignored"
  if ! SCOUT_TEST_DATABASE_URL="$url" cargo test --locked "${selector[@]}" -- --ignored --test-threads=1; then
    failed+=("$target")
  fi
  echo "::endgroup::"
done

if [ ${#failed[@]} -gt 0 ]; then
  echo "::error::PostgreSQL tests failed in: ${failed[*]}"
  exit 1
fi
