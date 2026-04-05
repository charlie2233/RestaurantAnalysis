# Gold Publishing Gate

This document defines how the repository decides which Gold KPI rows are safe for external export.

## Purpose

- The workbook is a hypothesis artifact, not a source of truth.
- Bronze and Silver are working layers.
- Gold is the first layer eligible for downstream reporting, but not every Gold row is automatically safe to publish.
- The Gold publishing gate applies an explicit in-repo policy to decide whether a KPI row is `publishable`, `advisory`, or `blocked`.

## Policy location

- Policy module: `src/qsr_audit/gold/policy.py`
- Current policy version: `gold-publish-policy v1.0.0`

The policy is code, not hidden constants. Every metric rule is explicit and testable.

## Current metric posture

### Publishable when evidence is strong

- `rank`
- `store_count`
- `system_sales`
- `auv`

These metrics require:

- external reference evidence
- complete provenance
- strong confidence
- acceptable reconciliation grade
- no policy-defined hard fail such as an unresolved implied AUV contradiction for `auv`

### Advisory-only under the current policy

- `fte_mid`
- `margin_mid_pct`

These metrics stay advisory because they are often workbook estimates rather than externally reported operating facts. Advisory means analyst context only, not external publication.

## Blocking logic

The gate blocks a KPI row when any hard-fail condition is present, including:

- required provenance fields are missing
- no external reference evidence exists for a strict external metric
- reconciliation contradiction exceeds the configured threshold when reference evidence exists
- validation emits a metric-specific hard fail
- the policy explicitly marks the condition as blocking

## Advisory logic

A KPI row becomes advisory when:

- the policy marks the metric as advisory-only
- the evidence is estimated-only under a strict external metric
- non-blocking warnings remain unresolved
- syntheticness or cross-sheet issues require analyst caution but do not justify a hard block

Advisory rows are not publishable. They must never be silently promoted into exportable outputs.

## Command

```bash
qsr-audit gate-gold
```

Primary outputs:

- `data/gold/gold_publish_decisions.parquet`
- `data/gold/publishable_kpis.parquet`
- `data/gold/blocked_kpis.parquet`
- `reports/audit/gold_publish_scorecard.md`
- `reports/audit/gold_publish_scorecard.json`

## Analyst guidance

- Do not invent missing provenance to clear a blocked row.
- Do not treat advisory rows as “basically publishable.”
- If a strict external metric is blocked, fix the evidence chain first: provenance, validation contradiction, or reconciliation support.
- If a row stays advisory because the metric is inherently estimated, keep it out of external KPI exports unless a future policy revision explicitly changes that rule.

## What still needs analyst evidence

Under the current policy, these metrics are still not safe for external use without stronger analyst-provided evidence:

- `fte_mid`
- `margin_mid_pct`

Even for external-facing metrics like `store_count`, `system_sales`, and `auv`, publication still depends on manual reference evidence and complete provenance. Unknown values should remain blank until that evidence exists.
