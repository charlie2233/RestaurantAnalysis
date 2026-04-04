"""Human-readable validation report generation."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, is_dataclass
from dataclasses import field as dc_field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

ValidationSeverity = Literal["error", "warning", "info"]


@dataclass(frozen=True)
class ValidationIssue:
    """A single validation finding."""

    severity: ValidationSeverity
    category: str
    message: str
    sheet: str | None = None
    field: str | None = None
    row_number: int | None = None
    value: Any | None = None
    expected: str | None = None
    observed: str | None = None
    details: dict[str, Any] = dc_field(default_factory=dict)


@dataclass(frozen=True)
class ValidationArtifact:
    """Paths for generated validation report outputs."""

    summary_markdown: Path
    results_json: Path


@dataclass(frozen=True)
class ValidationReport:
    """Structured validation outcome for rendering."""

    workbook_name: str
    generated_at: str
    issues: tuple[ValidationIssue, ...]
    metadata: dict[str, Any] = dc_field(default_factory=dict)

    @property
    def severity_counts(self) -> dict[str, int]:
        counts = Counter(issue.severity for issue in self.issues)
        return {severity: counts.get(severity, 0) for severity in ("error", "warning", "info")}

    @property
    def has_errors(self) -> bool:
        return self.severity_counts["error"] > 0


def build_validation_report(
    workbook_name: str,
    issues: list[ValidationIssue] | tuple[ValidationIssue, ...],
    *,
    metadata: dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> ValidationReport:
    """Create a normalized report object from collected issues."""

    timestamp = generated_at or datetime.now(UTC).isoformat()
    return ValidationReport(
        workbook_name=workbook_name,
        generated_at=timestamp,
        issues=tuple(issues),
        metadata=metadata or {},
    )


def render_validation_summary_markdown(report: ValidationReport) -> str:
    """Render an analyst-friendly markdown summary."""

    lines: list[str] = []
    counts = report.severity_counts
    total = len(report.issues)

    lines.append("# Validation Summary")
    lines.append("")
    lines.append(f"- Workbook: `{report.workbook_name}`")
    lines.append(f"- Generated at: `{report.generated_at}`")
    lines.append(f"- Total findings: `{total}`")
    lines.append(f"- `error`: `{counts['error']}`")
    lines.append(f"- `warning`: `{counts['warning']}`")
    lines.append(f"- `info`: `{counts['info']}`")
    lines.append("")

    if report.metadata:
        lines.append("## Run Metadata")
        lines.append("")
        for key in sorted(report.metadata):
            lines.append(f"- {key}: `{report.metadata[key]}`")
        lines.append("")

    grouped = defaultdict(list)
    for issue in report.issues:
        grouped[issue.severity].append(issue)

    for severity in ("error", "warning", "info"):
        issues = grouped.get(severity, [])
        lines.append(f"## {severity.title()}s")
        lines.append("")
        if not issues:
            lines.append("None.")
            lines.append("")
            continue

        for issue in issues:
            location = _format_issue_location(issue)
            lines.append(f"- **{issue.category}**: {issue.message}{location}")
            if issue.expected or issue.observed:
                detail_bits: list[str] = []
                if issue.expected is not None:
                    detail_bits.append(f"expected `{issue.expected}`")
                if issue.observed is not None:
                    detail_bits.append(f"observed `{issue.observed}`")
                lines.append(f"  - {', '.join(detail_bits)}")
            if issue.value is not None:
                lines.append(f"  - value: `{issue.value}`")
            if issue.details:
                for key in sorted(issue.details):
                    lines.append(f"  - {key}: `{issue.details[key]}`")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def render_validation_results_json(report: ValidationReport) -> str:
    """Serialize validation results as stable JSON."""

    payload = _report_to_jsonable(report)
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_validation_outputs(
    report: ValidationReport,
    output_dir: Path,
) -> ValidationArtifact:
    """Write markdown and JSON validation outputs."""

    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "validation_summary.md"
    results_path = output_dir / "validation_results.json"

    summary_path.write_text(render_validation_summary_markdown(report), encoding="utf-8")
    results_path.write_text(render_validation_results_json(report), encoding="utf-8")

    return ValidationArtifact(summary_markdown=summary_path, results_json=results_path)


def _format_issue_location(issue: ValidationIssue) -> str:
    parts: list[str] = []
    if issue.sheet:
        parts.append(f"sheet `{issue.sheet}`")
    if issue.field:
        parts.append(f"field `{issue.field}`")
    if issue.row_number is not None:
        parts.append(f"row `{issue.row_number}`")
    if not parts:
        return ""
    return f" ({', '.join(parts)})"


def _report_to_jsonable(report: ValidationReport) -> dict[str, Any]:
    return {
        "workbook_name": report.workbook_name,
        "generated_at": report.generated_at,
        "severity_counts": report.severity_counts,
        "has_errors": report.has_errors,
        "metadata": report.metadata,
        "issues": [_issue_to_jsonable(issue) for issue in report.issues],
    }


def _issue_to_jsonable(issue: ValidationIssue) -> dict[str, Any]:
    payload = asdict(issue) if is_dataclass(issue) else dict(issue)
    return _jsonify(payload)


def _jsonify(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return _jsonify(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonify(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonify(item) for item in value]
    return value
