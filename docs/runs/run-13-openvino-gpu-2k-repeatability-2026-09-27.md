# Run 13: BGE OpenVINO GPU repeat and Ticket 31 serving checkpoint

**Classification:** 2k checkpoint evidence
**Status:** Build, activation, verification, Evaluation, HTTP smoke, and guarded benchmark completed. Benchmark flagged concurrency 2 as unstable.

Run 13 is second repeat after Run 11. It used the same frozen Corpus, Evaluation, and BGE GPU build profile in isolated root `data/experiments/openvino-gpu-2k-repeat-13`. Ticket 31's 10k scale checkpoint remains incomplete.

## Build and Evaluation

- Corpus: `snapshot-4ab6f3b944327df3e89fe29a1b535a52f62d4a1407fc98a472701e929d90d0e9` — 2,082 Pages, 42,905 Passages, 6 aliases. Manifest SHA-256: `2e3702498d51fda8dce94c2d69b7f9963c0f9653bce9e4bb96a64d8c469384e6`.
- Evaluation: `scout-2k-page-64-v2` — 64 fixed queries. Query SHA-256: `049763f2b69275e0afdfb6e3d21cb94f9edacc374f031eb49ef2ca9e0120bf31`; judgment SHA-256: `13e4cd317f8cbc79e76133ec9bc6521d89eedcff2d44fed92dc8043ee56677a8`.
- Build profile SHA-256: `b348acfbf5aa2ce94d69159da509727ca3b8c44990dcfaee1a24918f51487ee6`. Provider `openvino_gpu`; batch 4; one worker; intra-op threads 1.
- Build completed in 557.454 s. Guarded peak process-tree RSS was 1,300,897,792 B; minimum `MemAvailable` was 5,829,144,576 B; free swap stayed at 2 GiB; swap-out growth was zero.
- Generation `generation-49490253561adfa44305573404c501c18b473752a3e0794965607e787c3f53f1` sealed and verified. Its `CURRENT` manifest digest is `822c97df806df68d9b1ed2ddd6ecd386d069b9991e8ad41df15ed4af4a522262`; sealed manifest-file SHA-256 is `9a4f232b33d8059fd4633e5e7facd4c5e23c1f7f905ded8569796f8fa9106673`. Benchmark plan locks the sealed file hash.
- HTTP health, readiness, Generation metadata, and hybrid search returned 200. Smoke checked endpoint correctness only.
- Held-out Evaluation gate passed with the same metrics as Runs 11 and 12. Top-10 Page identities match Run 11 on all 64 queries in all three modes. Labels remain provisional two-pass Codex consensus, not independent human judgments.

Run 11's roughly 17× speedup over Run 7 remains descriptive because Run 7's CPU comparison used an earlier WSL kernel. No same-kernel CPU control was run here.

## Ticket 31 benchmark

Benchmark run: `run-50282cd9ab3b282caa7c4db9b045e84d89fbfbcdceb063ef81b005332347d25b`. The locked plan used verified Run 13 `CURRENT`, the same Corpus snapshot, and the fixed 64-query Evaluation. It ran three repetitions, two warmups, ten measured passes, one cold-open check, fixed query order, and concurrency `[1, 2]`. Fresh staging was disabled. No build or activation occurred during measured passes.

Concurrency 1 and 2 each recorded 30 measured passes and 1,920 per-query latency samples. All 73 benchmark sample records completed; no query error surfaced. The harness aborts on a query error and does not emit a separate per-query error counter.

| Concurrency | Pass duration p50 / p95 | QPS p50 / p95 | Stability                                             |
| ----------: | ----------------------: | ------------: | ----------------------------------------------------- |
|           1 |        1,436 / 1,540 ms | 44.53 / 47.22 | Within 15% spread                                     |
|           2 |            896 / 970 ms | 70.75 / 76.66 | **Unstable:** duration spread 16.7%; QPS spread 16.9% |

Per-query hybrid latency across measured passes:

| Concurrency |      p50 |      p95 |      p99 | Observed min–max |
| ----------: | -------: | -------: | -------: | ---------------: |
|           1 | 21.68 ms | 30.72 ms | 38.59 ms |   15.35–55.18 ms |
|           2 | 27.19 ms | 36.94 ms | 44.28 ms |   17.34–91.13 ms |

One cold-open check took 2,126 ms. Here “cold-open” means a fresh `index verify` subprocess that opens and verifies the Generation. It is not cold first-query latency or an HTTP serving measurement. Warm timing measures in-process hybrid retrieval; the separate HTTP smoke was correctness-only.

The independent guard sampled every 2 seconds. It recorded peak process-tree RSS of 1,536,241,664 B (1.43 GiB), minimum host `MemAvailable` of 5,483,757,568 B (5.11 GiB), 2 GiB free swap throughout, and zero swap-out. No OOM or partial artifact was recorded. Scout's own 1-second sampler recorded 1,664,950,272 B summed per-process `VmHWM`; sampled current RSS was lower. GPU device memory telemetry was unavailable, so these are host/process memory figures.

Concurrency 2 produced higher observed median throughput, but its spread crosses the benchmark's 15% instability threshold. Treat these numbers as a bounded 2k checkpoint, not a production target or an improvement claim.

## Evidence and limits

- [Guarded benchmark config](run-13-openvino-gpu-2k-repeatability-2026-09-27/serving-benchmark/benchmark.toml)
- [Preflight lock and resource gate](run-13-openvino-gpu-2k-repeatability-2026-09-27/serving-benchmark/launch-preflight.json)
- [Locked benchmark plan](run-13-openvino-gpu-2k-repeatability-2026-09-27/serving-benchmark/benchmark-output/plan.json)
- [Benchmark report](run-13-openvino-gpu-2k-repeatability-2026-09-27/serving-benchmark/benchmark-output/report.json)
- [Per-pass samples](run-13-openvino-gpu-2k-repeatability-2026-09-27/serving-benchmark/benchmark-output/samples.jsonl)
- [Scout resource summary](run-13-openvino-gpu-2k-repeatability-2026-09-27/serving-benchmark/benchmark-output/resource-summary.json)
- [Independent guard summary](run-13-openvino-gpu-2k-repeatability-2026-09-27/serving-benchmark/external-resource-summary.json)
- [Three-run repeatability comparison](openvino-gpu-2k-repeatability-2026-09-27.json)
- [Ticket 31](../../.scratch/scout/issues/31-measure-activated-search-across-concurrency-levels.md)

The three builds match on build profile, semantic artifact, Evaluation metrics, and top-10 Page identities. Keyword artifacts and Generation manifest digests differ, so whole-Generation byte identity is not established. Relevance labels remain provisional. Ticket 31 stays partial until its 10k checkpoint is complete; ADR-0001's final MVP performance baseline still requires a real 100,000-Page Corpus.
