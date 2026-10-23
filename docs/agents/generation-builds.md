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

## Active development profile

For current Scout development and comparative benchmark work, use
`BAAI/bge-small-en-v1.5` with `embedding_execution_provider = "openvino_gpu"`
when the target hardware and runtime support it. Set the model in the crawl
config and the provider in `config/index.toml`; both are sealed into the
Corpus/Generation provenance. This names the active reference profile; it
does not change the supported model/provider defaults or remove CPU support.
MiniLM remains a supported comparison model.

Evidence is currently limited to a verified 2k BGE GPU Generation and three
repeatable 2k builds ([Run 13](../runs/run-13-openvino-gpu-2k-repeatability-2026-09-27.md));
the relevance labels remain provisional. Complete the 10k build checkpoint before
drawing conclusions at that scale; separately, ADR-0001 sets the MVP performance
baseline at a real 10,000-Page Corpus with the fixed 64-query Evaluation package.
CPU/GPU build-time ratios remain descriptive because the recorded CPU runs
predate the WSL kernel update ([Run 12 comparison](../runs/run-12-minilm-openvino-gpu-2k-2026-09-26.md)).

## Safe default

`scout index build --corpus <snapshot-id> --data-dir data` defaults to
keyword-only. This builds the Tantivy index without initializing an embedding
runtime, embedding Passages, or creating vector indexes. Keyword search remains
available with `mode=keyword`; semantic and hybrid search need Generations that
include those components.

On native WSL, the portable CPU default uses one worker with the low-memory
FastEmbed profile:

```toml
embedding_batch_size = 4
embedding_workers = 1
embedding_intra_op_threads = 1
```

Raise concurrency only after checking available RAM and swap. The observed
high-throughput profile exhausted the host while the low-memory profile
completed the 140,024-vector build.

## Embedding model selection

Set top-level `embedding_model` in the crawl config before crawling. Supported
values are `BAAI/bge-small-en-v1.5` (default) and
`sentence-transformers/all-MiniLM-L6-v2`. The choice is included in the crawl
config digest, crawl policy, and sealed snapshot manifest. Set model identity
only here: `config/index.toml` must omit `model_name`; index builds derive it
from the sealed snapshot and reject a model override. Semantic query embeds
with the model recorded in the active Generation profile, so query-time model
selection is not available.

MiniLM uses mean pooling, 384 dimensions, Apache-2.0 metadata, no query
instruction, and source `hf://Qdrant/all-MiniLM-L6-v2-onnx`. Set matching model
metadata and file hashes in the index profile. FastEmbed 7.0.1 fetches its
built-in model from the repository's current revision; `model_revision` and
hash fields are recorded in Generation metadata but do not pin or verify the
downloaded files.

## Embedding execution provider

`embedding_execution_provider = "cpu"` is the default. Set it to
`"openvino_gpu"` in `config/index.toml` to request Intel GPU execution. The
provider is sealed in the Generation profile and reused for semantic queries.
OpenVINO GPU mode requires compiling both the index and serving binary with
`--features openvino-gpu`, an OpenVINO-enabled ONNX Runtime supplied through
`ORT_LIB_PATH`, and the OpenVINO GPU runtime and WSL GPU driver. The bundled
Linux ONNX Runtime does not include OpenVINO. GPU mode fails if the requested
provider is unavailable or falls back to CPU.

On macOS, Apple Core ML can be requested with
`embedding_execution_provider = "apple_coreml_gpu"` in `config/index.toml` and
`cargo build --release --features apple-coreml`. The provider selects Core ML
`CPUAndGPU` compute units and keeps ONNX Runtime's CPU fallback for unsupported
operators. This path is macOS-only; verify acceleration on the target Mac, as
Core ML may delegate only supported model operations to the GPU.

For a one-command fresh Crawl-to-verified-Generation run, use
`scripts/crawl-build-coreml.sh <sources.toml> <index-profile-template.toml> <data-dir>`.
The supplied profile template must match the model selected by the Crawl config;
the script updates its snapshot ID and provider, then builds, activates, and
verifies the Generation.

## Resume and embedding reuse

For an interrupted build, resume only with the original `run-<sha256>` ID and the same
`--mode` used for that build. The example explicitly resumes a hybrid build:

```sh
scout index build --corpus <snapshot-id> --resume <run-id> --data-dir data --mode hybrid
```

Resume verifies that the exact Corpus snapshot, build profile, catalog,
embedding model assets, and runtime fingerprint still match. Its durable
chunk checkpoints live under `data/generation-build-checkpoints/`, outside the
immutable Generation. Completed checkpoints are reused; missing or corrupt
chunks are recomputed. A partial staging Generation remains ineligible for
activation.

Cross-build embedding reuse lives under
`data/generation-embedding-cache/`, also outside immutable Generations. The
cache key hashes the exact document input together with the verified model and
runtime fingerprint. The record checksum, dimension, key, and finite float
values are checked on read; an invalid entry becomes a cache miss. Cache data
never bypasses the normal Generation validator or becomes part of its catalog,
manifest digest, or activation pointer. Older sealed Generations without a
recorded runtime fingerprint are not valid cache sources by label alone.

## Completion gates

1. Build reports the complete page, passage, alias, and vector counts.
2. Activate the Generation and run `index verify` against the same project-local
   `data/` tree.
3. Smoke-test health, readiness, Generation metadata, and a search mode supported
   by that Generation (hybrid for a hybrid build) against that tree.
4. Record the Generation ID, counts, profile, and manifest/artifact hashes in
   `docs/runs/`.

Do not activate a partial staging tree. A source module, TOML profile, or
successful build step is not a persisted Generation; the completion evidence
is the sealed generation manifest, keyword and semantic artifacts, checksums,
and active `CURRENT` pointer.
