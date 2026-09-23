# Separate pilot and MVP benchmark baselines

**Status:** accepted

Scout separates a small pilot Smoke benchmark from the MVP Benchmark baseline. The pilot is for correctness, artifact, and harness checks only; it must not set numeric performance targets. A real 100,000-Page Corpus with the fixed 64-query Evaluation package establishes the MVP performance baseline. Each optimization changes one variable while preserving the inputs and Reference profile, and requires three repeatable runs with no correctness or relevance regression before it is claimed as an improvement.
