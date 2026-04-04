"""Validation report generation for analyst consumption."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pandas as pd

from qsr_audit.validate.models import ValidationArtifacts, ValidationFinding, ValidationRun


def write_validation_outputs(
    run: ValidationRun,
    *,
    output_dir: Path = Path("reports/validation"),
    gold_dir: Path = Path("data/gold"),
) -> ValidationArtifacts:
    """Write markdown, JSON, and parquet validation outputs."""

    output_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)

    summary_path = output_dir / "validation_summary.md"
    results_path = output_dir / "validation_results.json"
    flags_path = gold_dir / "validation_flags.parquet"

    summary_path.write_text(render_validation_summary(run), encoding="utf-8")
    results_path.write_text(render_validation_results_json(run), encoding="utf-8")

    flags_frame = pd.DataFrame([finding.as_record() for finding in run.findings])
    if flags_frame.empty:
        flags_frame = pd.DataFrame(
            columns=[
                "severity",
                "category",
                "check_name",
                "dataset",
                "message",
                "sheet_name",
                "field_name",
                "brand_name",
                "row_number",
                "expected",
                "observed",
                "details",
            ]
        )
    flags_frame["details"] = flags_frame["details"].map(
        lambda value: json.dumps(value, ensure_ascii=False, default=str)
    )
    flags_frame.to_parquet(flags_path, index=False)

    return ValidationArtifacts(
        summary_markdown=summary_path,
        results_json=results_path,
        flags_parquet=flags_path,
    )


def render_validation_summary(run: ValidationRun) -> str:
    """Render a concise markdown summary for analysts."""

    counts = run.counts
    lines = [
        "# Validation Summary",
        "",
        f"- Source: `{run.source_path}`",
        f"- Source kind: `{run.source_kind}`",
        f"- Status: `{'PASS' if run.passed else 'FAIL'}`",
        f"- Errors: {counts['error']}",
        f"- Warnings: {counts['warning']}",
        f"- Info: {counts['info']}",
        "",
        "## Findings",
        "",
        "| Severity | Dataset | Category | Check | Message |",
        "|---|---|---|---|---|",
    ]
    for finding in run.findings:
        lines.append(
            "| "
            + " | ".join(
                [
                    finding.severity,
                    finding.dataset,
                    finding.category,
                    finding.check_name,
                    _escape_markdown(finding.message),
                ]
            )
            + " |"
        )
    if not run.findings:
        lines.append("| info | workbook | summary | none | No findings emitted. |")
    return "\n".join(lines)


def render_validation_results_json(run: ValidationRun) -> str:
    """Render the machine-readable validation results."""

    payload = {
        "source_path": str(run.source_path),
        "source_kind": run.source_kind,
        "passed": run.passed,
        "counts": run.counts,
        "findings": [finding.as_record() for finding in run.findings],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def summarize_findings(findings: list[ValidationFinding]) -> dict[str, int]:
    """Return severity counts for a findings list."""

    return dict(Counter(finding.severity for finding in findings))


def _escape_markdown(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", "<br>")
