# Coordinator storage pressure

The coordinator checks local capacity before granting new task leases. It scans
all retained Crawl spool files under `$DATA_DIR/crawls`, exported event batches
under `$DATA_DIR/crawl-event-batches`, and the default analytics archive at
`$DATA_DIR/analytics-archive`. It reports each directory's allocated bytes and
filesystem free space in the `coordinator.storage_capacity` operational event.
Archive and export bytes count toward a configured shared-storage quota only
when they share a filesystem with the Crawl spools.
The scan fails closed if a managed root or any path below it is a symlink, or
if a nested mount uses another filesystem; those layouts would make usage
accounting incomplete. Symlinked parent components outside the managed roots
(such as the operating system's temporary-directory alias) are allowed.

The free-space guard is enabled by default and keeps at least 1 GiB free on the
Crawl spool filesystem:

- `SCOUT_COORDINATOR_MIN_FREE_BYTES` sets that reserve in bytes; default
  `1073741824`.
- `SCOUT_COORDINATOR_STORAGE_QUOTA_BYTES` optionally caps tracked bytes on the
  Crawl spool filesystem. It is unset by default.

The coordinator reserves `3 × max_body_bytes + 1 MiB` for each live task,
including a completion still being written. This covers a temporary body-file
write and the peak overlap of SQLite page and WAL writes during durable
projection. A queued result whose task lease has already committed reserves
`2 × max_body_bytes + 1 MiB` for the SQLite projection; its published body
file already counts toward measured Crawl spool usage. These bounds use the
existing maximum body size and result queue limits.

When no safe reservation remains, `coordinator.storage_capacity` reports
`paused: true`, `pressure_reason`, the measured byte totals, and the
`resume_condition`. The dispatcher then holds new task claims while existing
request permits and result processing continue. It samples every five seconds
and automatically resumes after free space and any configured quota can fit at
least two more maximum-size capture reservations. Operators resume a paused
coordinator by freeing space or moving retained data off the tracked filesystem
while preserving required replay artifacts; moving files into the in-project
analytics archive does not lower a shared-filesystem quota. No command or
restart is needed.

Storage pressure is not a Crawl completion reason. Existing target, duration,
and frontier completion rules remain authoritative. If capacity cannot be
measured, the coordinator reports `pressure_reason: "measurement_unavailable"`
and fails closed for new claims while continuing accepted-result handling.
Databricks availability does not affect Crawl intake or completion.
