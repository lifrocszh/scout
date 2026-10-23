# Databricks transport retries

Snapshot and crawl-event upload and submit commands share bounded durable
retry state. State lives outside immutable batch folders at
`~/.scout/databricks-transport`; set `SCOUT_DATABRICKS_STATE_DIR` to use another
local directory. The scope includes batch, upload or submit operation, CLI
profile, configured workspace host, and Unity Catalog Volume. Keep the same
profile and Databricks host when resuming an operation.

Each CLI subprocess has a timeout. The whole upload or submit invocation has
an elapsed-time cap that includes command waits and retry delays. Defaults are
three attempts, a two second exponential delay capped at 30 seconds, a 60
second subprocess timeout, and a 30 minute operation timeout. Override them
with `SCOUT_DATABRICKS_MAX_ATTEMPTS`,
`SCOUT_DATABRICKS_RETRY_BASE_DELAY_SECONDS`,
`SCOUT_DATABRICKS_MAX_RETRY_DELAY_SECONDS`,
`SCOUT_DATABRICKS_COMMAND_TIMEOUT_SECONDS`, and
`SCOUT_DATABRICKS_OPERATION_TIMEOUT_SECONDS`.

Upload attempts use a distinct persisted remote attempt ID. Files upload to
that attempt path with `_PUBLISHED` last. A successful path is reused on later
invocations. Job submissions persist their request identity and a stable
Databricks idempotency token before calling the CLI. An uncertain response is
retried with the same token, so the CLI can return the existing run instead
of creating a duplicate. The request keeps queueing disabled and the notebook
task timeout at one hour.

After bounded attempts are exhausted, use the command printed in the error,
then rerun the original operation. For example:

```sh
python3 scripts/databricks/transport_state.py retry \
  --kind snapshot --operation upload \
  --batch-id snapshot-<64-hex-characters> \
  --volume dbfs:/Volumes/<catalog>/<schema>/<volume> --profile <profile>
```

For a submit retry, also pass the original `--workspace-path`. An exhausted
unknown submission keeps its existing token; rerunning it can recover the
Databricks run ID. To launch a new run after the recorded run is confirmed
terminal and failed, use `--new-job --confirmed-terminal-failure`. This
increments the submission generation and creates a new token. It requires a
recorded run ID, so an unknown submission cannot be turned into a new run
without reconciliation first.

The state files use atomic replacement, file and directory sync, restrictive
permissions, and a per-operation process lock. They record attempt count,
next retry time, timeouts, last error, remote attempt/path, and Databricks run
ID. The helper does not run a background worker; retry happens only when the
operator invokes the original command.
