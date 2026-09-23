# Scout

Scout is a single-node search system for crawling technical documentation and retrieving relevant source pages through keyword, semantic, and hybrid search.

## Language

**Corpus**:
A reproducible collection of Pages admitted from explicitly allowlisted sources.
_Avoid_: Dataset, crawl dump

**Corpus snapshot**:
An immutable, versioned view of one completed Corpus and its Crawl Manifest. It is the fixed input to an Index Generation; Pages are not added, removed, or changed inside the snapshot.

**Source**:
An explicit crawl record containing seed URLs, allowed Origins and path boundaries, and an optional shared politeness group.
_Avoid_: Website, domain

**Page**:
Normalized content from one normalized final URL after redirects. A Page is Scout's search-result identity and the unit counted by corpus-size targets. Exact extracted-content duplicates within one Source and Corpus generation share one deterministic representative Page; duplicate URLs remain generation-scoped aliases, while near-duplicate Pages remain distinct. Redirect aliases and HTML canonical hints are provenance, not Page identity.
_Avoid_: Document, webpage

**Passage**:
A heading-bounded section derived from exactly one Page and used as the common keyword and semantic retrieval unit. It preserves heading paths and preformatted/code structure, may split at content boundaries, and always remains grouped under its parent Page.
_Avoid_: Document, chunk

**Page ID**:
An opaque, stable identity derived from a Page's normalized final URL. It remains stable when the URL remains stable, independent of index-local identifiers.

**Passage ID**:
An opaque, stable child identity derived from a Page and its deterministic Passage position. Identical Page content reproduces identical Passage IDs; content or boundary changes may create new IDs.

**Relevance judgment**:
A human assessment of how well a Page answers one frozen evaluation query, using Scout's ordered 0–3 relevance scale.
_Avoid_: Passage judgment

**Evaluation query**:
A frozen technical question used to compare Page rankings against human relevance judgments.
_Avoid_: benchmark prompt

**Evaluation package**:
The versioned set of Evaluation queries, source and intent strata, development/held-out split, Corpus snapshot identity, retrieval provenance, and human Page judgments used for one relevance comparison.
_Avoid_: query list

**Quality gate**:
A fixed relevance condition that a candidate retrieval configuration must satisfy before it can be treated as an improvement, including hybrid uplift, bootstrap confidence, and non-regression checks.
_Avoid_: score target

**Operational event**:
A structured JSONL record describing one Scout lifecycle, crawl, index, Generation, search, or benchmark outcome.
_Avoid_: Access log

**Access log**:
An optional NCSA combined-format record describing one HTTP request, separate from Scout's structured operational events.
_Avoid_: Operational event

**Reference profile**:
A named hardware and software environment against which Benchmark runs are compared.
_Avoid_: benchmark machine

**Benchmark run**:
One measured execution of a fixed workload against immutable Scout inputs under a named Reference profile.
_Avoid_: performance test

**Benchmark baseline**:
The first reproducible Benchmark run for a frozen Corpus snapshot, Evaluation query package, Reference profile, and current implementation. Later Benchmark runs are compared with it rather than replacing it.
_Avoid_: best score, latest run

**Smoke benchmark**:
A correctness and harness check over the small immutable pilot input. It may expose regressions and broken artifacts, but it does not establish production performance targets.
_Avoid_: performance baseline

**MVP benchmark baseline**:
The first reproducible Benchmark run over the real MVP Corpus and fixed evaluation workload. It establishes the initial performance reference and the evidence for later numeric targets.
_Avoid_: pilot baseline

**Performance target**:
A numeric latency, throughput, capacity, or resource threshold derived from the MVP benchmark baseline rather than from the pilot.
_Avoid_: pilot target

**Benchmark instability**:
A Benchmark result whose `(maximum - minimum) / median` spread exceeds the accepted stability threshold and must be rerun before an improvement claim.
_Avoid_: noisy score

**Quality scorecard**:
A named set of relevance measurements for Evaluation queries, including ranking quality and non-regression gates.
_Avoid_: search score

**Performance scorecard**:
A named set of service and resource measurements for Benchmark runs, including latency, throughput, indexing cost, memory, and stability.
_Avoid_: speed score

**Incomplete Crawl**:
A Crawl that stops before a valid completion condition and whose captured Pages cannot form a Corpus snapshot.
_Avoid_: partial Corpus

**Generation integrity failure**:
A checksum, count, identity, configuration, or artifact-open mismatch that makes an Index Generation unsafe to activate or serve.
_Avoid_: corrupt live index

**Page alias**:
A fetched normalized URL whose extracted content is an exact duplicate of a representative Page within the same Source and Corpus generation. It remains provenance for that Page, not an independent Page identity.

**Extracted content identity**:
The deterministic, structure-preserving representation of a Page's useful content, with transport and presentation metadata excluded. It is used to recognize exact aliases; near-duplicate similarity is not part of the MVP domain. Useful content may be prose or preformatted/code text and does not require a heading.

**Crawl Manifest**:
The versioned record identifying a crawl's allowed sources and captured Page content hashes.
_Avoid_: URL list

**Origin**:
The scheme, host, and effective-port tuple that owns one robots policy and politeness budget.
_Avoid_: Domain, host

**Index Generation**:
An immutable, internally consistent retrieval snapshot built from one Corpus snapshot and exact retrieval/model configuration. It contains the keyword and semantic indexes, Page/Passage catalog, compatibility metadata, counts, and integrity evidence; raw bodies remain outside it. Its lifecycle is building, validated, sealed, active, or retired. Several sealed Generations may coexist, but activation publishes one complete Generation and each search operation uses one Generation only.
_Avoid_: Live index, index version

**Generation manifest**:
The immutable record that identifies an Index Generation, its Corpus snapshot and Crawl Manifest, schemas and configurations, model artifacts, counts, checksums, build metadata, and lifecycle state.

**Generation catalog**:
The immutable Page/Passage identity and metadata record shared by both retrieval indexes. Each indexed Passage has one catalog record, and catalog/index count or identity mismatches invalidate the Generation.
