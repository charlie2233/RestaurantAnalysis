"""Analyst-facing scorecard assembly for validation, reconciliation, and syntheticness."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_BASE_DIR = Path(".")
DEFAULT_VALIDATION_RESULTS_PATH = Path("reports/validation/validation_results.json")
DEFAULT_VALIDATION_FLAGS_PATH = Path("data/gold/validation_flags.parquet")
DEFAULT_RECONCILED_CORE_PATH = Path("data/gold/reconciled_core_metrics.parquet")
DEFAULT_PROVENANCE_REGISTRY_PATH = Path("data/gold/provenance_registry.parquet")
DEFAULT_SYNTHETICNESS_SIGNALS_PATH = Path("data/gold/syntheticness_signals.parquet")
DEFAULT_VALIDATION_SUMMARY_PATH = Path("reports/validation/validation_summary.md")
DEFAULT_RECONCILIATION_SUMMARY_PATH = Path("reports/reconciliation/reconciliation_summary.md")
DEFAULT_SYNTHETICNESS_REPORT_PATH = Path("reports/validation/syntheticness_report.md")

VALIDATION_FLAG_COLUMNS = [
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
RECONCILED_CORE_COLUMNS = [
    "rank",
    "brand_name",
    "canonical_brand_name",
    "category",
    "us_store_count_2024",
    "systemwide_revenue_usd_billions_2024",
    "average_unit_volume_usd_thousands",
    "fte_mid",
    "margin_mid_pct",
    "brand_match_confidence",
    "brand_match_method",
    "reference_source_count",
    "reference_source_names",
    "rank_reference_value",
    "rank_absolute_error",
    "rank_relative_error",
    "rank_credibility_grade",
    "rank_reference_source_name",
    "rank_reference_source_type",
    "rank_reference_confidence_score",
    "store_count_reference_value",
    "store_count_absolute_error",
    "store_count_relative_error",
    "store_count_credibility_grade",
    "store_count_reference_source_name",
    "store_count_reference_source_type",
    "store_count_reference_confidence_score",
    "system_sales_reference_value",
    "system_sales_absolute_error",
    "system_sales_relative_error",
    "system_sales_credibility_grade",
    "system_sales_reference_source_name",
    "system_sales_reference_source_type",
    "system_sales_reference_confidence_score",
    "auv_reference_value",
    "auv_absolute_error",
    "auv_relative_error",
    "auv_credibility_grade",
    "auv_reference_source_name",
    "auv_reference_source_type",
    "auv_reference_confidence_score",
    "overall_credibility_grade",
    "reconciliation_warning",
]
PROVENANCE_COLUMNS = [
    "source_type",
    "source_name",
    "source_url_or_doc_id",
    "as_of_date",
    "method_reported_or_estimated",
    "confidence_score",
    "notes",
    "extra",
]
SYNTHETICNESS_COLUMNS = [
    "signal_type",
    "title",
    "plain_english",
    "strength",
    "dataset",
    "field_name",
    "method",
    "sample_size",
    "score",
    "benchmark",
    "p_value",
    "z_score",
    "threshold",
    "observed",
    "expected",
    "interpretation",
    "caveat",
    "details",
]

NUMERIC_RECONCILIATION_FIELD_MAP = {
    "rank": "rank",
    "store_count": "store_count",
    "system_sales": "system_sales",
    "auv": "auv",
}

PROVENANCE_FIELD_MAP = {
    "rank": "rank_reference_confidence_score",
    "store_count": "store_count_reference_confidence_score",
    "system_sales": "system_sales_reference_confidence_score",
    "auv": "auv_reference_confidence_score",
}


@dataclass(frozen=True)
class ArtifactStatus:
    """Availability status for a local report artifact."""

    path: Path
    present: bool
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"path": str(self.path), "present": self.present, "note": self.note}


@dataclass(frozen=True)
class IssueSummary:
    """A concise issue summary suitable for report and JSON output."""

    severity: str
    category: str
    check_name: str
    message: str
    source: str
    brand_name: str | None = None
    field_name: str | None = None
    row_number: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "category": self.category,
            "check_name": self.check_name,
            "message": self.message,
            "source": self.source,
            "brand_name": self.brand_name,
            "field_name": self.field_name,
            "row_number": self.row_number,
            "details": _json_safe(self.details),
        }


@dataclass(frozen=True)
class ValidationSummary:
    """Validation status distilled for executive review."""

    passed: bool
    failed_checks: int
    passed_checks: int
    warning_count: int
    info_count: int
    warning_counts_by_category: dict[str, int]
    issues: tuple[IssueSummary, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "failed_checks": self.failed_checks,
            "passed_checks": self.passed_checks,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "warning_counts_by_category": dict(self.warning_counts_by_category),
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass(frozen=True)
class FieldProvenanceSummary:
    """Coverage and confidence summary for a field's reference provenance."""

    field_name: str
    coverage_count: int
    coverage_rate: float
    mean_confidence: float | None
    provenance_grade: str
    missing_brands: tuple[str, ...] = ()
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "field_name": self.field_name,
            "coverage_count": self.coverage_count,
            "coverage_rate": self.coverage_rate,
            "mean_confidence": self.mean_confidence,
            "provenance_grade": self.provenance_grade,
            "missing_brands": list(self.missing_brands),
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ReconciliationErrorSummary:
    """Worst-case reconciliation comparison for a brand-field pair."""

    brand_name: str
    canonical_brand_name: str | None
    field_name: str
    workbook_value: float | int | None
    reference_value: float | int | None
    absolute_error: float | int | None
    relative_error: float | None
    credibility_grade: str
    reference_source_name: str | None = None
    reference_source_type: str | None = None
    warning: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "brand_name": self.brand_name,
            "canonical_brand_name": self.canonical_brand_name,
            "field_name": self.field_name,
            "workbook_value": self.workbook_value,
            "reference_value": self.reference_value,
            "absolute_error": self.absolute_error,
            "relative_error": self.relative_error,
            "credibility_grade": self.credibility_grade,
            "reference_source_name": self.reference_source_name,
            "reference_source_type": self.reference_source_type,
            "warning": self.warning,
        }


@dataclass(frozen=True)
class SyntheticnessOverview:
    """Syntheticness summary for the core metrics table."""

    signal_count: int
    count_by_strength: dict[str, int]
    count_by_signal_type: dict[str, int]
    brands_flagged: tuple[str, ...] = ()
    top_signals: tuple[IssueSummary, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_count": self.signal_count,
            "count_by_strength": dict(self.count_by_strength),
            "count_by_signal_type": dict(self.count_by_signal_type),
            "brands_flagged": list(self.brands_flagged),
            "top_signals": [signal.to_dict() for signal in self.top_signals],
        }


@dataclass(frozen=True)
class BrandScorecard:
    """Brand-level analyst scorecard."""

    brand_name: str
    canonical_brand_name: str | None
    rank: int | None
    normalized_metrics: dict[str, Any]
    invariant_results: tuple[IssueSummary, ...]
    provenance_grades: dict[str, str]
    reconciliation_error_summary: dict[str, ReconciliationErrorSummary]
    syntheticness_signals: tuple[IssueSummary, ...]
    open_issues: tuple[IssueSummary, ...]
    overall_credibility_grade: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "brand_name": self.brand_name,
            "canonical_brand_name": self.canonical_brand_name,
            "rank": self.rank,
            "normalized_metrics": _json_safe(self.normalized_metrics),
            "invariant_results": [issue.to_dict() for issue in self.invariant_results],
            "provenance_grades": dict(self.provenance_grades),
            "reconciliation_error_summary": {
                key: value.to_dict() for key, value in self.reconciliation_error_summary.items()
            },
            "syntheticness_signals": [issue.to_dict() for issue in self.syntheticness_signals],
            "open_issues": [issue.to_dict() for issue in self.open_issues],
            "overall_credibility_grade": self.overall_credibility_grade,
        }


@dataclass(frozen=True)
class GlobalCredibilityScorecard:
    """Executive-level credibility scorecard."""

    total_brands: int
    brands_with_reference_coverage: int
    brands_without_reference_coverage: int
    validation_summary: ValidationSummary
    warning_counts_by_category: dict[str, int]
    fields_with_weakest_provenance: tuple[FieldProvenanceSummary, ...]
    biggest_reconciliation_errors: tuple[ReconciliationErrorSummary, ...]
    syntheticness_overview: SyntheticnessOverview
    reconciliation_warning_count: int
    open_issues: tuple[IssueSummary, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_brands": self.total_brands,
            "brands_with_reference_coverage": self.brands_with_reference_coverage,
            "brands_without_reference_coverage": self.brands_without_reference_coverage,
            "validation_summary": self.validation_summary.to_dict(),
            "warning_counts_by_category": dict(self.warning_counts_by_category),
            "fields_with_weakest_provenance": [
                field.to_dict() for field in self.fields_with_weakest_provenance
            ],
            "biggest_reconciliation_errors": [
                error.to_dict() for error in self.biggest_reconciliation_errors
            ],
            "syntheticness_overview": self.syntheticness_overview.to_dict(),
            "reconciliation_warning_count": self.reconciliation_warning_count,
            "open_issues": [issue.to_dict() for issue in self.open_issues],
        }


@dataclass(frozen=True)
class ReportingInputs:
    """Loaded local report inputs and their availability."""

    validation_results: dict[str, Any] | None
    validation_flags: pd.DataFrame
    reconciled_core_metrics: pd.DataFrame
    provenance_registry: pd.DataFrame
    syntheticness_signals: pd.DataFrame
    artifact_statuses: tuple[ArtifactStatus, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "validation_results": _json_safe(self.validation_results),
            "validation_flags": self.validation_flags.to_dict(orient="records"),
            "reconciled_core_metrics": self.reconciled_core_metrics.to_dict(orient="records"),
            "provenance_registry": self.provenance_registry.to_dict(orient="records"),
            "syntheticness_signals": self.syntheticness_signals.to_dict(orient="records"),
            "artifact_statuses": [status.to_dict() for status in self.artifact_statuses],
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class AnalystScorecardBundle:
    """Top-level scorecard bundle returned by the report assembly layer."""

    generated_at: datetime
    inputs: ReportingInputs
    global_scorecard: GlobalCredibilityScorecard
    brand_scorecards: tuple[BrandScorecard, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "inputs": self.inputs.to_dict(),
            "global_scorecard": self.global_scorecard.to_dict(),
            "brand_scorecards": [scorecard.to_dict() for scorecard in self.brand_scorecards],
        }


def load_reporting_inputs(base_dir: Path = DEFAULT_BASE_DIR) -> ReportingInputs:
    """Load local validation, reconciliation, provenance, and syntheticness artifacts."""

    root = base_dir.expanduser().resolve()
    statuses: list[ArtifactStatus] = []
    warnings: list[str] = []

    validation_results, status = _load_json(
        root / DEFAULT_VALIDATION_RESULTS_PATH, default=None, required=False
    )
    statuses.append(status)
    if status.note:
        warnings.append(status.note)

    validation_flags, status = _load_parquet(
        root / DEFAULT_VALIDATION_FLAGS_PATH,
        columns=VALIDATION_FLAG_COLUMNS,
        required=False,
    )
    statuses.append(status)
    if status.note:
        warnings.append(status.note)

    reconciled_core_metrics, status = _load_parquet(
        root / DEFAULT_RECONCILED_CORE_PATH,
        columns=RECONCILED_CORE_COLUMNS,
        required=False,
    )
    statuses.append(status)
    if status.note:
        warnings.append(status.note)

    provenance_registry, status = _load_parquet(
        root / DEFAULT_PROVENANCE_REGISTRY_PATH,
        columns=PROVENANCE_COLUMNS,
        required=False,
    )
    statuses.append(status)
    if status.note:
        warnings.append(status.note)

    syntheticness_signals, status = _load_parquet(
        root / DEFAULT_SYNTHETICNESS_SIGNALS_PATH,
        columns=SYNTHETICNESS_COLUMNS,
        required=False,
    )
    statuses.append(status)
    if status.note:
        warnings.append(status.note)

    for optional_path in (
        root / DEFAULT_VALIDATION_SUMMARY_PATH,
        root / DEFAULT_RECONCILIATION_SUMMARY_PATH,
        root / DEFAULT_SYNTHETICNESS_REPORT_PATH,
    ):
        if not optional_path.exists():
            warnings.append(f"Optional report file missing: `{optional_path}`.")
        statuses.append(ArtifactStatus(path=optional_path, present=optional_path.exists()))

    return ReportingInputs(
        validation_results=validation_results,
        validation_flags=validation_flags,
        reconciled_core_metrics=reconciled_core_metrics,
        provenance_registry=provenance_registry,
        syntheticness_signals=syntheticness_signals,
        artifact_statuses=tuple(statuses),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def build_analyst_scorecard_bundle(base_dir: Path = DEFAULT_BASE_DIR) -> AnalystScorecardBundle:
    """Build the full analyst-facing scorecard bundle from local artifacts only."""

    inputs = load_reporting_inputs(base_dir)
    global_scorecard = build_global_credibility_scorecard(inputs)
    brand_scorecards = build_brand_scorecards(inputs)
    generated_at = datetime.now(tz=UTC)
    return AnalystScorecardBundle(
        generated_at=generated_at,
        inputs=inputs,
        global_scorecard=global_scorecard,
        brand_scorecards=tuple(brand_scorecards),
    )


def build_global_credibility_scorecard(inputs: ReportingInputs) -> GlobalCredibilityScorecard:
    """Compute executive-level credibility metrics across all loaded artifacts."""

    validation_summary = _build_validation_summary(inputs.validation_results, inputs.validation_flags)
    warning_counts_by_category = dict(validation_summary.warning_counts_by_category)
    warning_counts_by_category["reconciliation"] = len(
        _nonempty_strings(
            inputs.reconciled_core_metrics.get(
                "reconciliation_warning", pd.Series(dtype="object")
            )
        )
    )
    syntheticness_overview = _build_syntheticness_overview(inputs.syntheticness_signals)
    if syntheticness_overview.signal_count:
        warning_counts_by_category["syntheticness"] = sum(
            syntheticness_overview.count_by_strength.get(strength, 0)
            for strength in ("weak", "moderate", "strong")
        )

    total_brands = _brand_count(inputs.reconciled_core_metrics, inputs.validation_flags)
    brands_with_reference_coverage = _count_reference_coverage(inputs.reconciled_core_metrics)
    weakest_provenance_fields = _weakest_provenance_fields(inputs.reconciled_core_metrics)
    biggest_reconciliation_errors = _biggest_reconciliation_errors(inputs.reconciled_core_metrics)
    open_issues = _build_global_open_issues(inputs, validation_summary, syntheticness_overview)

    return GlobalCredibilityScorecard(
        total_brands=total_brands,
        brands_with_reference_coverage=brands_with_reference_coverage,
        brands_without_reference_coverage=max(0, total_brands - brands_with_reference_coverage),
        validation_summary=validation_summary,
        warning_counts_by_category=warning_counts_by_category,
        fields_with_weakest_provenance=tuple(weakest_provenance_fields),
        biggest_reconciliation_errors=tuple(biggest_reconciliation_errors),
        syntheticness_overview=syntheticness_overview,
        reconciliation_warning_count=warning_counts_by_category.get("reconciliation", 0),
        open_issues=tuple(open_issues),
    )


def build_brand_scorecards(inputs: ReportingInputs) -> list[BrandScorecard]:
    """Build one scorecard per brand."""

    if inputs.reconciled_core_metrics.empty:
        brand_names = _brand_names_from_validation_flags(inputs.validation_flags)
        return [
            BrandScorecard(
                brand_name=brand_name,
                canonical_brand_name=None,
                rank=None,
                normalized_metrics={},
                invariant_results=(),
                provenance_grades={},
                reconciliation_error_summary={},
                syntheticness_signals=(),
                open_issues=(
                    IssueSummary(
                        severity="warning",
                        category="reporting",
                        check_name="missing_reconciled_core_metrics",
                        message="Reconciled core metrics are unavailable, so brand-level scorecards are incomplete.",
                        source="reporting",
                        brand_name=brand_name,
                    ),
                ),
                overall_credibility_grade=None,
            )
            for brand_name in brand_names
        ]

    validation_findings = inputs.validation_flags.copy()
    validation_findings["brand_name"] = validation_findings["brand_name"].fillna("")
    syntheticness_frame = _syntheticness_with_details(inputs.syntheticness_signals)

    cards: list[BrandScorecard] = []
    for row in inputs.reconciled_core_metrics.sort_values(
        by=["rank", "brand_name"], na_position="last"
    ).to_dict(orient="records"):
        brand_name = str(row.get("brand_name") or "")
        canonical_brand_name = _maybe_str(row.get("canonical_brand_name"))
        provenance_grades = _build_provenance_grades(row)
        reconciliation_error_summary = _build_reconciliation_error_summary(row)
        invariant_results = _brand_validation_findings(validation_findings, brand_name, row)
        syntheticness_signals = _brand_syntheticness_issues(syntheticness_frame, brand_name)
        open_issues = _brand_open_issues(
            invariant_results,
            reconciliation_error_summary,
            syntheticness_signals,
            row.get("reconciliation_warning"),
        )
        normalized_metrics = _normalized_metric_subset(row)

        cards.append(
            BrandScorecard(
                brand_name=brand_name,
                canonical_brand_name=canonical_brand_name,
                rank=_to_int(row.get("rank")),
                normalized_metrics=normalized_metrics,
                invariant_results=tuple(invariant_results),
                provenance_grades=provenance_grades,
                reconciliation_error_summary=reconciliation_error_summary,
                syntheticness_signals=tuple(syntheticness_signals),
                open_issues=tuple(open_issues),
                overall_credibility_grade=_maybe_str(row.get("overall_credibility_grade")),
            )
        )
    return cards


def _build_validation_summary(
    validation_results: dict[str, Any] | None,
    validation_flags: pd.DataFrame,
) -> ValidationSummary:
    if validation_results:
        passed = bool(validation_results.get("passed", False))
        counts = validation_results.get("counts", {}) or {}
        warning_count = int(counts.get("warning", 0))
        info_count = int(counts.get("info", 0))
        failed_checks = int(counts.get("error", 0))
        passed_checks = max(0, int(sum(int(value) for value in counts.values())) - failed_checks)
    else:
        passed = not _has_errors(validation_flags)
        failed_checks = int((validation_flags["severity"] == "error").sum()) if not validation_flags.empty else 0
        warning_count = int((validation_flags["severity"] == "warning").sum()) if not validation_flags.empty else 0
        info_count = int((validation_flags["severity"] == "info").sum()) if not validation_flags.empty else 0
        passed_checks = max(0, len(validation_flags) - failed_checks)

    warning_counts_by_category = _warning_counts_by_category(validation_flags)
    issues = tuple(
        _issue_from_validation_row(row)
        for row in validation_flags.to_dict(orient="records")
        if str(row.get("severity")) in {"error", "warning"}
    )
    return ValidationSummary(
        passed=passed,
        failed_checks=failed_checks,
        passed_checks=passed_checks,
        warning_count=warning_count,
        info_count=info_count,
        warning_counts_by_category=warning_counts_by_category,
        issues=issues,
    )


def _warning_counts_by_category(validation_flags: pd.DataFrame) -> dict[str, int]:
    if validation_flags.empty:
        return {}
    warnings = validation_flags[validation_flags["severity"] == "warning"]
    return dict(Counter(warnings["category"].fillna("unknown").astype(str)))


def _has_errors(validation_flags: pd.DataFrame) -> bool:
    if validation_flags.empty or "severity" not in validation_flags.columns:
        return False
    return bool((validation_flags["severity"].fillna("").astype(str) == "error").any())


def _build_global_open_issues(
    inputs: ReportingInputs,
    validation_summary: ValidationSummary,
    syntheticness_overview: SyntheticnessOverview,
) -> list[IssueSummary]:
    issues = [issue for issue in validation_summary.issues if issue.severity in {"error", "warning"}]
    issues.extend(
        IssueSummary(
            severity="warning",
            category="reconciliation",
            check_name="reconciliation_warning",
            message=warning,
            source="reconciliation",
        )
        for warning in _nonempty_strings(inputs.reconciled_core_metrics.get("reconciliation_warning", pd.Series(dtype="object")))
    )
    issues.extend(
        issue
        for issue in syntheticness_overview.top_signals
        if issue.severity in {"warning", "error"}
    )
    issues.extend(
        IssueSummary(
            severity="warning",
            category="artifacts",
            check_name="missing_artifact",
            message=status.note or f"Artifact missing: {status.path}",
            source="load",
            details={"path": str(status.path)},
        )
        for status in inputs.artifact_statuses
        if not status.present and status.note
    )
    return _dedupe_issues(issues)


def _build_syntheticness_overview(syntheticness_signals: pd.DataFrame) -> SyntheticnessOverview:
    if syntheticness_signals.empty:
        return SyntheticnessOverview(
            signal_count=0,
            count_by_strength={},
            count_by_signal_type={},
            brands_flagged=(),
            top_signals=(),
        )

    details_frame = _syntheticness_with_details(syntheticness_signals)
    count_by_strength = dict(Counter(details_frame["strength"].fillna("unknown").astype(str)))
    count_by_signal_type = dict(Counter(details_frame["signal_type"].fillna("unknown").astype(str)))

    flagged_brands = sorted(
        {
            str(details.get("brand_name"))
            for details in details_frame["details"]
            if isinstance(details, dict) and details.get("brand_name")
        }
    )
    top_signals = tuple(
        IssueSummary(
            severity="warning" if row["strength"] in {"weak", "moderate", "strong"} else "info",
            category="syntheticness",
            check_name=str(row["signal_type"]),
            message=str(row["plain_english"]),
            source="syntheticness",
            brand_name=_maybe_str(row["brand_name"]),
            field_name=_maybe_str(row["field_name"]),
            details={
                "strength": row["strength"],
                "sample_size": _to_int(row.get("sample_size")),
                "score": _maybe_float(row.get("score")),
                "details": _json_safe(row.get("details")),
            },
        )
        for row in details_frame.to_dict(orient="records")
        if row["strength"] in {"weak", "moderate", "strong"} or _maybe_str(row.get("brand_name"))
    )
    return SyntheticnessOverview(
        signal_count=len(details_frame),
        count_by_strength=count_by_strength,
        count_by_signal_type=count_by_signal_type,
        brands_flagged=tuple(flagged_brands),
        top_signals=top_signals[:12],
    )


def _brand_validation_findings(
    validation_flags: pd.DataFrame,
    brand_name: str,
    row: dict[str, Any],
) -> list[IssueSummary]:
    if validation_flags.empty:
        return []
    brand_rows = validation_flags[
        validation_flags["brand_name"].fillna("").astype(str).eq(brand_name)
    ]
    row_number = _to_int(row.get("row_number"))
    if row_number is not None and "row_number" in validation_flags.columns:
        row_rows = validation_flags[validation_flags["row_number"].fillna(-1).astype(int).eq(row_number)]
        brand_rows = pd.concat([brand_rows, row_rows], ignore_index=True).drop_duplicates()
    issues = [
        _issue_from_validation_row(validation_row)
        for validation_row in brand_rows.to_dict(orient="records")
        if str(validation_row.get("severity")) in {"error", "warning"}
    ]
    return _dedupe_issues(issues)


def _build_provenance_grades(row: dict[str, Any]) -> dict[str, str]:
    grades: dict[str, str] = {}
    for field_name, column_name in PROVENANCE_FIELD_MAP.items():
        grade = _grade_from_provenance_row(row.get(column_name))
        grades[field_name] = grade
    brand_confidence = _maybe_float(row.get("brand_match_confidence"))
    grades["brand_match"] = _grade_from_confidence(brand_confidence)
    return grades


def _build_reconciliation_error_summary(
    row: dict[str, Any]
) -> dict[str, ReconciliationErrorSummary]:
    summary: dict[str, ReconciliationErrorSummary] = {}
    brand_name = str(row.get("brand_name") or "")
    canonical_brand_name = _maybe_str(row.get("canonical_brand_name"))
    for field_name, prefix in NUMERIC_RECONCILIATION_FIELD_MAP.items():
        summary[field_name] = ReconciliationErrorSummary(
            brand_name=brand_name,
            canonical_brand_name=canonical_brand_name,
            field_name=field_name,
            workbook_value=_maybe_number(row.get(_metric_column_name(field_name))),
            reference_value=_maybe_number(row.get(f"{prefix}_reference_value")),
            absolute_error=_maybe_number(row.get(f"{prefix}_absolute_error")),
            relative_error=_maybe_float(row.get(f"{prefix}_relative_error")),
            credibility_grade=_maybe_str(row.get(f"{prefix}_credibility_grade")) or "MISSING",
            reference_source_name=_maybe_str(row.get(f"{prefix}_reference_source_name")),
            reference_source_type=_maybe_str(row.get(f"{prefix}_reference_source_type")),
            warning=_maybe_str(row.get("reconciliation_warning")),
        )
    return summary


def _brand_syntheticness_issues(
    syntheticness_frame: pd.DataFrame,
    brand_name: str,
) -> list[IssueSummary]:
    if syntheticness_frame.empty:
        return []
    if "details" not in syntheticness_frame.columns:
        return []
    matched = syntheticness_frame[
        syntheticness_frame["brand_name"].fillna("").astype(str).eq(brand_name)
    ]
    issues = [
        IssueSummary(
            severity="warning",
            category="syntheticness",
            check_name=str(row["signal_type"]),
            message=str(row["plain_english"]),
            source="syntheticness",
            brand_name=brand_name,
            field_name=_maybe_str(row.get("field_name")),
            details={
                "strength": row["strength"],
                "sample_size": _to_int(row.get("sample_size")),
                "score": _maybe_float(row.get("score")),
                "details": _json_safe(row.get("details")),
            },
        )
        for row in matched.to_dict(orient="records")
        if str(row.get("strength")) in {"moderate", "strong"}
    ]
    return _dedupe_issues(issues)


def _brand_open_issues(
    invariant_results: list[IssueSummary],
    reconciliation_error_summary: dict[str, ReconciliationErrorSummary],
    syntheticness_signals: list[IssueSummary],
    reconciliation_warning: object,
) -> list[IssueSummary]:
    issues = list(invariant_results)
    issues.extend(
        IssueSummary(
            severity="warning",
            category="reconciliation",
            check_name=f"{field_name}_reconciliation",
            message=_issue_message_from_error(error),
            source="reconciliation",
            brand_name=error.brand_name,
            field_name=field_name,
            details=error.to_dict(),
        )
        for field_name, error in reconciliation_error_summary.items()
        if error.credibility_grade in {"D", "F", "MISSING"} and error.reference_value is not None
    )
    if reconciliation_warning:
        issues.append(
            IssueSummary(
                severity="warning",
                category="reconciliation",
                check_name="reconciliation_warning",
                message=str(reconciliation_warning),
                source="reconciliation",
            )
        )
    issues.extend(syntheticness_signals)
    return _dedupe_issues(issues)


def _issue_from_validation_row(row: dict[str, Any]) -> IssueSummary:
    return IssueSummary(
        severity=str(row.get("severity") or "info"),
        category=str(row.get("category") or "unknown"),
        check_name=str(row.get("check_name") or "unknown"),
        message=str(row.get("message") or ""),
        source="validation",
        brand_name=_maybe_str(row.get("brand_name")),
        field_name=_maybe_str(row.get("field_name")),
        row_number=_to_int(row.get("row_number")),
        details={
            "dataset": _maybe_str(row.get("dataset")),
            "sheet_name": _maybe_str(row.get("sheet_name")),
            "expected": _maybe_str(row.get("expected")),
            "observed": _maybe_str(row.get("observed")),
            "details": _json_safe(row.get("details")),
        },
    )


def _issue_message_from_error(error: ReconciliationErrorSummary) -> str:
    if error.absolute_error is None:
        return error.warning or f"No reference value available for `{error.field_name}`."
    if error.relative_error is None:
        return (
            f"{error.brand_name} differs from reference on `{error.field_name}` by {error.absolute_error}."
        )
    return (
        f"{error.brand_name} differs from reference on `{error.field_name}` by "
        f"{error.absolute_error} ({error.relative_error:.1%})."
    )


def _weakest_provenance_fields(reconciled_core_metrics: pd.DataFrame) -> list[FieldProvenanceSummary]:
    if reconciled_core_metrics.empty:
        return []
    summaries: list[FieldProvenanceSummary] = []
    total_rows = len(reconciled_core_metrics)
    for field_name, column_name in PROVENANCE_FIELD_MAP.items():
        series = reconciled_core_metrics.get(column_name, pd.Series(dtype="float64"))
        non_null = series.dropna()
        coverage_count = int(non_null.shape[0])
        coverage_rate = coverage_count / total_rows if total_rows else 0.0
        mean_confidence = float(non_null.mean()) if coverage_count else None
        grade = _grade_from_field_provenance(coverage_rate=coverage_rate, mean_confidence=mean_confidence)
        missing_brands = tuple(
            _nonempty_strings(
                reconciled_core_metrics.loc[reconciled_core_metrics[column_name].isna(), "brand_name"]
            )
        )
        notes = None
        if not coverage_count:
            notes = f"No reference values were available for `{field_name}`."
        summaries.append(
            FieldProvenanceSummary(
                field_name=field_name,
                coverage_count=coverage_count,
                coverage_rate=coverage_rate,
                mean_confidence=mean_confidence,
                provenance_grade=grade,
                missing_brands=missing_brands,
                notes=notes,
            )
        )
    return sorted(
        summaries,
        key=lambda item: (
            _grade_rank(item.provenance_grade),
            item.coverage_rate,
            item.mean_confidence if item.mean_confidence is not None else -1.0,
        ),
    )


def _biggest_reconciliation_errors(
    reconciled_core_metrics: pd.DataFrame,
) -> list[ReconciliationErrorSummary]:
    if reconciled_core_metrics.empty:
        return []
    errors: list[ReconciliationErrorSummary] = []
    for row in reconciled_core_metrics.to_dict(orient="records"):
        brand_name = str(row.get("brand_name") or "")
        canonical_brand_name = _maybe_str(row.get("canonical_brand_name"))
        for field_name, prefix in NUMERIC_RECONCILIATION_FIELD_MAP.items():
            absolute_error = _maybe_number(row.get(f"{prefix}_absolute_error"))
            relative_error = _maybe_float(row.get(f"{prefix}_relative_error"))
            reference_value = _maybe_number(row.get(f"{prefix}_reference_value"))
            workbook_value = _maybe_number(row.get(_metric_column_name(field_name)))
            if absolute_error is None and reference_value is None:
                continue
            errors.append(
                ReconciliationErrorSummary(
                    brand_name=brand_name,
                    canonical_brand_name=canonical_brand_name,
                    field_name=field_name,
                    workbook_value=workbook_value,
                    reference_value=reference_value,
                    absolute_error=absolute_error,
                    relative_error=relative_error,
                    credibility_grade=_maybe_str(row.get(f"{prefix}_credibility_grade"))
                    or "MISSING",
                    reference_source_name=_maybe_str(row.get(f"{prefix}_reference_source_name")),
                    reference_source_type=_maybe_str(row.get(f"{prefix}_reference_source_type")),
                    warning=_maybe_str(row.get("reconciliation_warning")),
                )
            )
    errors = [
        error
        for error in errors
        if error.absolute_error is not None and error.reference_value is not None
    ]
    return sorted(
        errors,
        key=lambda error: (
            error.absolute_error if error.absolute_error is not None else -1.0,
            error.relative_error if error.relative_error is not None else -1.0,
        ),
        reverse=True,
    )[:12]


def _build_syntheticness_overview_frame(syntheticness_signals: pd.DataFrame) -> pd.DataFrame:
    if syntheticness_signals.empty:
        return syntheticness_signals.copy()
    frame = syntheticness_signals.copy()
    if "details" not in frame.columns:
        frame["details"] = None
    frame["details"] = frame["details"].map(_parse_json_maybe)
    frame["brand_name"] = frame["details"].map(
        lambda value: value.get("brand_name") if isinstance(value, dict) else None
    )
    return frame


def _brand_names_from_validation_flags(validation_flags: pd.DataFrame) -> list[str]:
    if validation_flags.empty or "brand_name" not in validation_flags.columns:
        return []
    return sorted(
        {
            str(value).strip()
            for value in validation_flags["brand_name"].dropna().astype(str)
            if str(value).strip()
        }
    )


def _brand_count(
    reconciled_core_metrics: pd.DataFrame,
    validation_flags: pd.DataFrame,
) -> int:
    if not reconciled_core_metrics.empty and "brand_name" in reconciled_core_metrics.columns:
        return int(reconciled_core_metrics["brand_name"].nunique())
    return len(_brand_names_from_validation_flags(validation_flags))


def _count_reference_coverage(reconciled_core_metrics: pd.DataFrame) -> int:
    if reconciled_core_metrics.empty or "reference_source_count" not in reconciled_core_metrics.columns:
        return 0
    return int((reconciled_core_metrics["reference_source_count"].fillna(0).astype(float) > 0).sum())


def _syntheticness_with_details(syntheticness_signals: pd.DataFrame) -> pd.DataFrame:
    if syntheticness_signals.empty:
        return syntheticness_signals.copy()
    frame = syntheticness_signals.copy()
    frame["details"] = frame.get("details", pd.Series(dtype="object")).map(_parse_json_maybe)
    frame["brand_name"] = frame["details"].map(
        lambda value: value.get("brand_name") if isinstance(value, dict) else None
    )
    return frame


def _normalized_metric_subset(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank": _to_int(row.get("rank")),
        "brand_name": _maybe_str(row.get("brand_name")),
        "canonical_brand_name": _maybe_str(row.get("canonical_brand_name")),
        "category": _maybe_str(row.get("category")),
        "us_store_count_2024": _maybe_number(row.get("us_store_count_2024")),
        "systemwide_revenue_usd_billions_2024": _maybe_number(
            row.get("systemwide_revenue_usd_billions_2024")
        ),
        "average_unit_volume_usd_thousands": _maybe_number(
            row.get("average_unit_volume_usd_thousands")
        ),
        "fte_mid": _maybe_number(row.get("fte_mid")),
        "margin_mid_pct": _maybe_number(row.get("margin_mid_pct")),
        "brand_match_confidence": _maybe_float(row.get("brand_match_confidence")),
        "reference_source_count": _to_int(row.get("reference_source_count")),
        "overall_credibility_grade": _maybe_str(row.get("overall_credibility_grade")),
    }


def _metric_column_name(field_name: str) -> str:
    return {
        "rank": "rank",
        "store_count": "us_store_count_2024",
        "system_sales": "systemwide_revenue_usd_billions_2024",
        "auv": "average_unit_volume_usd_thousands",
    }[field_name]


def _grade_from_field_provenance(
    *,
    coverage_rate: float,
    mean_confidence: float | None,
) -> str:
    confidence = mean_confidence if mean_confidence is not None else 0.0
    if coverage_rate == 0.0:
        return "F"
    if coverage_rate >= 0.8 and confidence >= 0.8:
        return "A"
    if coverage_rate >= 0.6 and confidence >= 0.65:
        return "B"
    if coverage_rate >= 0.4 and confidence >= 0.5:
        return "C"
    if coverage_rate > 0.0:
        return "D"
    return "F"


def _grade_from_provenance_row(value: object) -> str:
    confidence = _maybe_float(value)
    return _grade_from_confidence(confidence)


def _grade_from_confidence(confidence: float | None) -> str:
    if confidence is None:
        return "MISSING"
    if confidence >= 0.8:
        return "A"
    if confidence >= 0.65:
        return "B"
    if confidence >= 0.5:
        return "C"
    if confidence >= 0.35:
        return "D"
    return "F"


def _grade_rank(grade: str) -> int:
    return {"A": 0, "B": 1, "C": 2, "D": 3, "F": 4, "MISSING": 5}.get(grade, 9)


def _nonempty_strings(values: pd.Series | list[Any] | tuple[Any, ...]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = _maybe_str(value)
        if text:
            result.append(text)
    return result


def _parse_json_maybe(value: object) -> dict[str, Any] | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


def _maybe_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _maybe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _maybe_number(value: object) -> float | int | None:
    number = _maybe_float(value)
    if number is None:
        return None
    if float(number).is_integer():
        return int(round(number))
    return number


def _to_int(value: object) -> int | None:
    number = _maybe_float(value)
    if number is None:
        return None
    return int(round(number))


def _load_json(
    path: Path,
    *,
    default: dict[str, Any] | None,
    required: bool,
) -> tuple[dict[str, Any] | None, ArtifactStatus]:
    if not path.exists():
        note = f"Missing JSON artifact: `{path}`." if required else f"Optional JSON artifact missing: `{path}`."
        return default, ArtifactStatus(path=path, present=False, note=note)
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError as exc:
        note = f"Could not parse JSON artifact `{path}`: {exc}"
        return default, ArtifactStatus(path=path, present=False, note=note)
    return data, ArtifactStatus(path=path, present=True)


def _load_parquet(
    path: Path,
    *,
    columns: list[str],
    required: bool,
) -> tuple[pd.DataFrame, ArtifactStatus]:
    if not path.exists():
        note = f"Missing parquet artifact: `{path}`." if required else f"Optional parquet artifact missing: `{path}`."
        return pd.DataFrame(columns=columns), ArtifactStatus(path=path, present=False, note=note)
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:  # pragma: no cover - defensive
        note = f"Could not load parquet artifact `{path}`: {exc}"
        return pd.DataFrame(columns=columns), ArtifactStatus(path=path, present=False, note=note)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame, ArtifactStatus(path=path, present=True)


def _dedupe_issues(issues: list[IssueSummary]) -> list[IssueSummary]:
    seen: set[tuple[Any, ...]] = set()
    unique: list[IssueSummary] = []
    for issue in issues:
        key = (
            issue.severity,
            issue.category,
            issue.check_name,
            issue.message,
            issue.source,
            issue.brand_name,
            issue.field_name,
            issue.row_number,
            json.dumps(_json_safe(issue.details), sort_keys=True, ensure_ascii=False, default=str),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(issue)
    return unique


__all__ = [
    "AnalystScorecardBundle",
    "ArtifactStatus",
    "BrandScorecard",
    "DEFAULT_BASE_DIR",
    "DEFAULT_PROVENANCE_REGISTRY_PATH",
    "DEFAULT_RECONCILIATION_SUMMARY_PATH",
    "DEFAULT_RECONCILED_CORE_PATH",
    "DEFAULT_SYNTHETICNESS_REPORT_PATH",
    "DEFAULT_SYNTHETICNESS_SIGNALS_PATH",
    "DEFAULT_VALIDATION_FLAGS_PATH",
    "DEFAULT_VALIDATION_RESULTS_PATH",
    "DEFAULT_VALIDATION_SUMMARY_PATH",
    "FieldProvenanceSummary",
    "GlobalCredibilityScorecard",
    "IssueSummary",
    "ReconciliationErrorSummary",
    "ReportingInputs",
    "SyntheticnessOverview",
    "ValidationSummary",
    "build_analyst_scorecard_bundle",
    "build_brand_scorecards",
    "build_global_credibility_scorecard",
    "load_reporting_inputs",
]
