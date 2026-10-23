# Separate pilot and MVP benchmark baselines

**Status:** accepted

Scout separates a small pilot Smoke benchmark from the MVP Benchmark baseline: the pilot checks correctness, artifacts, and harness only, with no numeric performance targets. A real 10,000-Page Corpus with the fixed 64-query Evaluation package establishes the MVP performance baseline, making full reproducible measurement practical on local hardware. Optimization claims require changing one variable while preserving the inputs and Reference profile, then completing three repeatable runs without correctness or relevance regression.
