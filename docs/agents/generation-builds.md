# Generation builds

Use this procedure for real-model index builds and persisted Generation
recreation.

## Native WSL storage

Use the project-local `data/` directory as the build, staging, and persistence
root. This checkout is on native WSL/Linux storage, so high-write embedding and
USearch artifacts can remain in one tree for the whole run. Keep `data/`
ignored.

Use one `--data-dir` for crawling, building, activation, verification, and
smoke checks. Keep any lightweight `progress.json` beside the run data; do not
mirror growing artifacts into a second tree.

## Safe default

On native WSL, use one worker with the low-memory FastEmbed profile:

```toml
embedding_batch_size = 4
embedding_workers = 1
embedding_intra_op_threads = 1
```

Raise concurrency only after checking available RAM and swap. The observed
high-throughput profile exhausted the host while the low-memory profile
completed the 140,024-vector build.

## Completion gates

1. Build reports the complete page, passage, alias, and vector counts.
2. Activate the Generation and run `index verify` against the same project-local
   `data/` tree.
3. Smoke-test health, readiness, generation metadata, and one hybrid search
   against that tree.
4. Record the Generation ID, counts, profile, and manifest/artifact hashes in
   `docs/runs/`.

Do not activate a partial staging tree. A source module, TOML profile, or
successful build step is not a persisted Generation; the completion evidence
is the sealed generation manifest, keyword and semantic artifacts, checksums,
and active `CURRENT` pointer.
