# Reference Evidence Backlog

This backlog is the first manual evidence-collection queue for the repo. It is
not evidence itself.

Current local context:

- `reports/reconciliation/reconciliation_summary.md` shows `5` of `30` brands
  with external reference coverage after the current QSR 50 demo batch.
- `reports/audit/gold_publish_scorecard.md` now shows `2` publishable KPI rows,
  `62` advisory rows, and `116` blocked rows.
- `reports/validation/validation_summary.md` shows five AUV contradiction
  errors: Starbucks, Taco Bell, Raising Cane's, Dutch Bros, and Shake Shack.
- The first analyst cycle should therefore prioritize external-facing KPI
  fields: `store_count`, `system_sales`, and `auv`.

Use this backlog with the committed reference templates under
`data/reference/templates/`. Leave unknown values blank, preserve provenance,
mark `reported` vs `estimated`, and do not infer missing values.

## Rows Landed In The Current Cycle

| Brand | Source row landed | Resulting state |
|---|---|---|
| Starbucks | `data/reference/qsr50_reference.csv` | External QSR 50 row is now attached, but the evidence is marked `estimated`, `store_count` remains advisory, and `system_sales` / `auv` are still blocked. |
| Taco Bell | `data/reference/qsr50_reference.csv` | External QSR 50 row is now attached; `store_count` is publishable, while `system_sales` and `auv` still need stronger follow-up. |
| Raising Cane's | `data/reference/qsr50_reference.csv` | External QSR 50 row is now attached; `system_sales` is publishable, while `store_count` and `auv` remain blocked. |
| Dutch Bros | `data/reference/qsr50_reference.csv` | External QSR 50 row is now attached; all four KPI fields have first-pass external coverage, but rank / `store_count` / `system_sales` still fail reconciliation thresholds and `auv` still needs corroboration. |
| Shake Shack | `data/reference/qsr50_reference.csv` | External QSR 50 row is now attached as `estimated`; all four KPI fields now have first-pass external coverage, but estimated evidence plus large reconciliation deltas still keep them out of publishable status. |

## Top 10 Active Gaps

| Priority | Brand | Metric | Expected source type | Why now | Current blocker |
|---|---|---|---|---|---|
| P0 | Dutch Bros | `auv` | `sec_filings_reference` | Local validation still shows a 17.1% implied-AUV mismatch after the QSR 50 row landed. | QSR 50 now supplies first-pass evidence, but a corroborating second source is still required before the contradiction can clear Gold publication. |
| P0 | Shake Shack | `auv` | `sec_filings_reference` | Local validation still shows an 81.8% implied-AUV mismatch after the QSR 50 row landed. | QSR 50 now supplies first-pass evidence, but the evidence is estimated and the contradiction remains too severe for release-safe use. |
| P1 | McDonald's | `system_sales` | `sec_filings_reference` | McDonald's is a high-visibility anchor brand and system sales is an external-facing KPI. | No populated manual reference row exists locally; reconciliation still reports missing evidence. |
| P1 | McDonald's | `store_count` | `sec_filings_reference` | Store count is a likely first publishable KPI once external evidence is attached. | No populated manual reference row exists locally; reconciliation still reports missing evidence. |
| P1 | Domino's | `system_sales` | `sec_filings_reference` | Domino's has no current AUV contradiction, so system sales is a good clean-gap evidence target. | No populated manual reference row exists locally; workbook-only provenance remains. |
| P1 | Chipotle | `store_count` | `sec_filings_reference` | Chipotle is a major public chain and store count is a likely forecast-safe target later. | No populated manual reference row exists locally; workbook-only provenance remains. |
| P1 | Chick-fil-A | `auv` | `technomic_reference` | Chick-fil-A is strategically important and AUV is likely to require a manually reviewed secondary source. | No populated manual reference row exists locally; private-brand evidence needs careful manual intake. |
| P1 | Starbucks | `auv` | `sec_filings_reference` | External evidence now exists, but the current row is still estimated and the contradiction remains material. | QSR 50 landed, but the workbook claim still conflicts with the external row and the current evidence is not strong enough for publishable use. |
| P1 | Taco Bell | `auv` | `technomic_reference` | Taco Bell now has one external row, but the remaining contradiction is still large enough to hold Gold publication. | QSR 50 landed, but a secondary source is still needed to confirm whether the workbook value or the external row should win. |
| P1 | Raising Cane's | `auv` | `technomic_reference` | Raising Cane's now has one external row, but the contradiction still blocks release-safe use. | QSR 50 landed, but a corroborating second source is still needed before the AUV can be treated as credible. |

## Operating Notes

- Treat `expected source type` as the next intake lane, not as proof that the
  metric is definitely available there.
- Resolve the remaining contradiction rows before treating those KPIs as
  credible for external use.
- After each new row or coherent `1-3` row batch from the same source, rerun
  the reconciliation and Gold gate loop before moving further down the queue.
- After each filled row, rerun:

```bash
qsr-audit reconcile --core data/silver/core_brand_metrics.parquet --reference-dir data/reference/
qsr-audit gate-gold
qsr-audit preflight-release
```

- Keep this backlog updated as manual reference rows land or as a blocker turns
  out to be a source-availability problem rather than a simple missing row.
