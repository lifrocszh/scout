# Run 12: MiniLM OpenVINO GPU 2k experiment

Status: complete as a one-run 2k checkpoint. MiniLM GPU built a verified Generation, passed HTTP smoke, and completed the frozen 64-query Evaluation. Its held-out hybrid-over-semantic gate still fails because the 95% paired-bootstrap nDCG@10 lower bound is below zero. This does not change the default model or provider, nor Ticket 33 resolved status.

The run started on 2026-09-26 and completed just after midnight on 2026-09-27 in Asia/Singapore. It reused Run 9’s exact sealed MiniLM snapshot and frozen package. It did not crawl or modify Run 9 or BGE active pointers.

## Device proof and isolated inputs

Before building, a real inference on MiniLM ONNX model SHA-256 `bbd7b466f6d58e646fdc2bd5fd67b2f5e93c0b687011bd4548c420f7bd46f0c5` ran through ONNX Runtime 1.24.1 `OpenVINOExecutionProvider` with `device_type=GPU` and `session.disable_cpu_ep_fallback=1`. It returned `[1, 8, 384]` float32 output; ORT profiling recorded one OpenVINO node event and logged all nodes assigned to OpenVINO. Evidence: [device inference check](run-12-minilm-openvino-gpu-2k-2026-09-26/minilm-device-inference-check.json) and [profile](run-12-minilm-openvino-gpu-2k-2026-09-26/minilm-device-inference-profile.json).

The isolated build root is `data/experiments/minilm-openvino-gpu-2k`. Its Corpus is Run 9 snapshot `snapshot-3d78feb09f989b5a807cdcaa6c480f36ce2939c414a54ba719711560e9209e79` (manifest SHA-256 `49c386fa4bf43911a8ab80a81406dfba60060d5ec1a26f5d646f761afa964363`): 2,082 Pages, 42,905 Passages, six aliases. Config profile preserves MiniLM model revision `5f1b8cd78bc4fb444dd171e59b18f3a3af89a079`, batch size 4, one worker, one intra-op thread, and all retrieval settings; only `embedding_execution_provider` changed from `cpu` to `openvino_gpu`.

The copied Eval package keeps Run 9 queries and judgments byte-for-byte: SHA-256 `049763f2b69275e0afdfb6e3d21cb94f9edacc374f031eb49ef2ca9e0120bf31` and `13e4cd317f8cbc79e76133ec9bc6521d89eedcff2d44fed92dc8043ee56677a8`. Only retrieval/comparison metadata was rebound. See [package provenance](run-12-minilm-openvino-gpu-2k-2026-09-26/evaluation-package-provenance.json) and [frozen package](run-12-minilm-openvino-gpu-2k-2026-09-26/evaluation-package/). Judgment labels are provisional two-pass Codex consensus, not independent human adjudication.

## Build and validation

The isolated OpenVINO-enabled release binary compiled offline with `--locked`, `--features openvino-gpu`, and one Cargo job in 735.837 seconds. Its independent 2-second sampler recorded 1,007,747,072 bytes peak process-tree RSS, 6,142,861,312 bytes minimum `MemAvailable`, 2 GiB minimum free swap, and zero swap-out. See [compile resource summary](run-12-minilm-openvino-gpu-2k-2026-09-26/isolated-cargo-build-resource-summary.json).

The new Generation is `generation-57d6dc9f3ce642ca7f19f6510f05ab31867a117a07ba02ebb3628965a3e96b87`, manifest SHA-256 `f052d04e758cb19fd95dfc51483c270756874b80730cb188d1378bcc59b8299c`; it contains 2,082 Pages, 42,905 Passages, and 42,905 keyword and semantic entries. Semantic artifact SHA-256 is `c1918d4d1e878875214f056df80f12849c3b7432f01ecc7b8661e4d843e4ff63`. Activation and `index verify` succeeded only in the isolated root. Evidence includes the [sealed Generation manifest](run-12-minilm-openvino-gpu-2k-2026-09-26/generation-manifest.json), [build profile](run-12-minilm-openvino-gpu-2k-2026-09-26/build-profile.json), [checksums](run-12-minilm-openvino-gpu-2k-2026-09-26/checksums.sha256), and [validation summary](run-12-minilm-openvino-gpu-2k-2026-09-26/validation-summary.json).

The independent 2-second Generation-build sampler recorded 346.897 seconds full wall time; 1,421,230,080 bytes peak process-tree RSS; 5,696,069,632 bytes minimum `MemAvailable`; 2 GiB minimum free swap; and zero swap-out. No guard fired. Semantic stage took 329.986 seconds, of which passage embedding took 315.377 seconds (136.012 passages/second); semantic-stage throughput was 130.020 passages/second. Other measured stages: model initialization 1.955 seconds, vector writes 5.030 seconds, vector-index insertion 7.159 seconds, vector validation 0.148 seconds, persistence 0.148 seconds, reopen validation 0.004 seconds, and smoke search 0.016 seconds. Process RSS excludes GPU device memory. See [build summary](run-12-minilm-openvino-gpu-2k-2026-09-26/gpu-build-resource-summary.json), [samples](run-12-minilm-openvino-gpu-2k-2026-09-26/gpu-build-resource-samples.jsonl), and [semantic manifest](run-12-minilm-openvino-gpu-2k-2026-09-26/semantic-manifest.json).

HTTP health, readiness, Generation metadata, and hybrid search all returned 200 and matched the isolated Generation identity. The server was stopped after smoke. [Smoke evidence](run-12-minilm-openvino-gpu-2k-2026-09-26/serving-smoke.json).

## Four-run retrieval comparison

Each metric cell is `MRR@10 / nDCG@10 / Recall@100`. Full uses all 64 queries; development uses 16; held-out uses 48. Held-out gate bootstrap intervals compare hybrid nDCG@10 against keyword and semantic using 10,000 paired resamples. The archived [full evaluator report](run-12-minilm-openvino-gpu-2k-2026-09-26/evaluation-report.json) includes per-query rankings and diagnostics.

| Run | Split | Keyword | Semantic | Hybrid | Hybrid−keyword nDCG 95% CI | Hybrid−semantic nDCG 95% CI | Gate |
|---|---|---|---|---|---:|---:|---|
| Run 7 BGE cpu | full | 0.4844/0.4926/0.5156 | 0.6172/0.6516/0.7500 | 0.6484/0.6864/0.7969 | — | — | — |
| Run 7 BGE cpu | development | 0.5625/0.5789/0.6250 | 0.7396/0.7582/0.8125 | 0.6771/0.7284/0.8750 | — | — | — |
| Run 7 BGE cpu | held-out | 0.4583/0.4638/0.4792 | 0.5764/0.6161/0.7292 | 0.6389/0.6725/0.7708 | [0.11492, 0.30609] | [0.01121, 0.10302] | pass |
| Run 9 MiniLM cpu | full | 0.4844/0.4926/0.5156 | 0.6406/0.6688/0.7500 | 0.6534/0.6893/0.7969 | — | — | — |
| Run 9 MiniLM cpu | development | 0.5625/0.5789/0.6250 | 0.7500/0.7664/0.8125 | 0.7031/0.7471/0.8750 | — | — | — |
| Run 9 MiniLM cpu | held-out | 0.4583/0.4638/0.4792 | 0.6042/0.6362/0.7292 | 0.6368/0.6700/0.7708 | [0.12050, 0.29807] | [-0.01002, 0.08358] | fail |
| Run 11 BGE openvino_gpu | full | 0.4844/0.4926/0.5156 | 0.6172/0.6516/0.7500 | 0.6458/0.6844/0.7969 | — | — | — |
| Run 11 BGE openvino_gpu | development | 0.5625/0.5789/0.6250 | 0.7396/0.7582/0.8125 | 0.6667/0.7202/0.8750 | — | — | — |
| Run 11 BGE openvino_gpu | held-out | 0.4583/0.4638/0.4792 | 0.5764/0.6161/0.7292 | 0.6389/0.6725/0.7708 | [0.11492, 0.30609] | [0.01121, 0.10302] | pass |
| Run 12 MiniLM openvino_gpu | full | 0.4844/0.4926/0.5156 | 0.6445/0.6755/0.7656 | 0.6612/0.6950/0.7969 | — | — | — |
| Run 12 MiniLM openvino_gpu | development | 0.5625/0.5789/0.6250 | 0.7500/0.7664/0.8125 | 0.7031/0.7471/0.8750 | — | — | — |
| Run 12 MiniLM openvino_gpu | held-out | 0.4583/0.4638/0.4792 | 0.6094/0.6452/0.7500 | 0.6472/0.6777/0.7708 | [0.12963, 0.30370] | [-0.01055, 0.08127] | fail |

Run 12’s held-out hybrid-over-keyword nDCG uplift mean is 0.21387 with CI `[0.12963, 0.30370]`. Hybrid-over-semantic uplift mean is 0.03245 with CI `[-0.01055, 0.08127]`; the mean clears the 0.03 threshold, but the lower bound does not clear zero, so the gate fails. Run 9 CPU also failed this gate: mean 0.03373, CI `[-0.01002, 0.08358]`. Both BGE runs passed.

| Run | Model/provider | Full build wall | Semantic stage | Embedding time | Semantic-stage throughput | Embedding-only throughput | Peak process-tree RSS | Min `MemAvailable` | Swap-out delta |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Run 7 | BGE / cpu | 9693.105s | 9674.967s | 9654.447s | 4.435 p/s | 4.444 p/s | 1.096 GiB | 3.595 GiB | 3837952 B |
| Run 9 | MiniLM / cpu | 5023.427s | 5008.406s | 4979.847s | 8.567 p/s | 8.616 p/s | 1.044 GiB | 4.574 GiB | 0 B |
| Run 11 | BGE / openvino_gpu | 567.813s | 550.430s | 530.794s | 77.948 p/s | 80.832 p/s | 1.385 GiB | 5.354 GiB | 0 B |
| Run 12 | MiniLM / openvino_gpu | 346.897s | 329.986s | 315.377s | 130.021 p/s | 136.044 p/s | 1.324 GiB | 5.305 GiB | 0 B |

Run 12 was about 14.5× faster end to end than Run 9 MiniLM CPU and 15.2× faster for semantic stage. It used the same MiniLM model, snapshot, and Eval package, with provider as the sole Generation-profile change. This wall-time ratio remains descriptive: Run 9 CPU preceded the WSL kernel update, while Run 12 GPU followed it. BGE CPU Run 7 versus GPU Run 11 was about 17.1× end-to-end and 17.6× semantic-stage faster, also descriptive across that kernel update.

Within-model CPU/GPU retrieval comparisons are matched on snapshot and frozen Eval. Keyword ranking lists stayed identical 64/64 for both pairs. For BGE, semantic and hybrid ordered top-10 lists matched on 59/64 queries overall (14/16 dev, 45/48 held-out). For MiniLM, they matched on 59/64 overall; all 16 dev lists matched, while 43/48 held-out lists matched. MiniLM GPU therefore changed five held-out semantic and hybrid ordered top-10 lists. Its held-out aggregate hybrid MRR/nDCG improved by `+0.01042/+0.00769`; hybrid Recall@100 was unchanged. Semantic MRR/nDCG/Recall improved by `+0.00521/+0.00897/+0.02083`. Its relevance gate still failed because hybrid did not establish the required nDCG gain over semantic.

## Interpretation and limits

Run 7 BGE CPU and Run 11 BGE GPU share a frozen BGE Corpus snapshot and Eval; profiles differ only by execution provider. Run 9 MiniLM CPU and Run 12 MiniLM GPU likewise share the sealed MiniLM snapshot and Eval, and profiles differ only by provider. The GPU implementation uses OpenVINO `device_type=GPU`; CPU provider fallback is disabled.

Both CPU controls predate WSL kernel 6.18.33.2; both GPU builds ran after the update. Therefore every CPU/GPU build-time ratio above is descriptive rather than a same-kernel controlled timing estimate. MiniLM’s source crawl changed four non-target Pages versus the BGE snapshot, so cross-model metrics are descriptive across snapshots, not a causal model-only comparison. The 46 judged target Pages are unchanged across those snapshots.

This is one run per configuration with provisional labels. It claims no repeatability, no 100,000-Page MVP baseline, and no default model/provider change. Full machine-readable metrics, bootstrap intervals, resource stats, provenance, and ordered top-10 counts are in [comparison-all-runs.json](run-12-minilm-openvino-gpu-2k-2026-09-26/comparison-all-runs.json).
