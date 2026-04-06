# RAG Benchmark Pack Templates

Contract version: `v1`

These CSV templates define the analyst-authored retrieval benchmark pack for
`qsr-audit`. Fill them manually, keep unknown values blank, and do not treat
retrieval judgments as audited facts by themselves.

Required files:

- `queries.csv`
- `judgments.csv`

Optional files:

- `filters.csv`
- `query_groups.csv`

Authoring rules:

- `query_id` values must be unique.
- `relevance_label` must be one of `not_relevant`, `relevant`, or `highly_relevant`.
- Use `brand_filter`, `metric_filter`, and `publish_status_scope` only when the
  query truly implies those constraints.
- `publish_status_scope` must be one of `all`, `publishable`, `advisory`,
  `blocked`, or `non_blocked`.
- Use pipe separators for multi-value fields such as `brand_filter`,
  `metric_filter`, or `expected_source_kinds`.
- Leave unknown values blank rather than guessing.
- `doc_id` and `chunk_id` in `judgments.csv` must point to chunk/doc IDs that
  exist in the local vetted retrieval corpus.
- Use `must_appear_in_top_k` only for high-confidence expectations such as a
  direct blocked KPI decision or a precise provenance lookup.

The benchmark validator writes output under `artifacts/rag/benchmarks/validation/`.
Retrieval benchmark runs write output under `artifacts/rag/benchmarks/`.
