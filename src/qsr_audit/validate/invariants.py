"""Arithmetic and cross-sheet workbook invariants."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from qsr_audit.validate.models import ValidationFinding

CORE_BRAND_NAME_COLUMN = "brand_name"
CORE_RANK_COLUMN = "rank"
CORE_STORE_COUNT_COLUMN = "us_store_count_2024"
CORE_SYSTEM_SALES_COLUMN = "systemwide_revenue_usd_billions_2024"
CORE_AUV_COLUMN = "average_unit_volume_usd_thousands"


@dataclass(frozen=True)
class InvariantBundle:
    """Normalized tables required for invariant checks."""

    core_brand_metrics: pd.DataFrame
    ai_strategy_registry: pd.DataFrame


def evaluate_invariants(
    core_brand_metrics: pd.DataFrame,
    ai_strategy_registry: pd.DataFrame,
    *,
    tolerance_auv: float,
) -> list[ValidationFinding]:
    """Evaluate workbook arithmetic and cross-sheet invariants."""

    findings: list[ValidationFinding] = []
    findings.extend(check_rank_uniqueness(core_brand_metrics))
    findings.extend(check_core_brand_uniqueness(core_brand_metrics))
    findings.extend(check_brand_alignment(core_brand_metrics, ai_strategy_registry))
    findings.extend(check_implied_auv(core_brand_metrics, tolerance_auv=tolerance_auv))
    findings.extend(check_monotonic_ranges(core_brand_metrics))
    return findings


def check_rank_uniqueness(core_brand_metrics: pd.DataFrame) -> list[ValidationFinding]:
    duplicated = core_brand_metrics[core_brand_metrics.duplicated(CORE_RANK_COLUMN, keep=False)]
    if duplicated.empty:
        return [
            ValidationFinding(
                severity="info",
                category="uniqueness",
                check_name="core_brand_metrics.rank_unique",
                dataset="core_brand_metrics",
                message=f"Rank values are unique across {len(core_brand_metrics)} core rows.",
                sheet_name="QSR Top30 核心数据",
                details={"row_count": len(core_brand_metrics)},
            )
        ]

    offending = (
        duplicated.groupby(CORE_RANK_COLUMN)[[CORE_BRAND_NAME_COLUMN, "row_number", "source_sheet"]]
        .apply(lambda frame: frame.to_dict(orient="records"))
        .to_dict()
    )
    return [
        ValidationFinding(
            severity="error",
            category="uniqueness",
            check_name="core_brand_metrics.rank_unique",
            dataset="core_brand_metrics",
            message=(
                "Duplicate rank values found in the core table. "
                f"Offending ranks: {sorted(offending)}."
            ),
            sheet_name="QSR Top30 核心数据",
            details={"duplicates": offending},
        )
    ]


def check_core_brand_uniqueness(core_brand_metrics: pd.DataFrame) -> list[ValidationFinding]:
    duplicated = core_brand_metrics[
        core_brand_metrics.duplicated(CORE_BRAND_NAME_COLUMN, keep=False)
    ]
    if duplicated.empty:
        return [
            ValidationFinding(
                severity="info",
                category="uniqueness",
                check_name="core_brand_metrics.brand_unique",
                dataset="core_brand_metrics",
                message=f"Core brand names are unique across {len(core_brand_metrics)} rows.",
                sheet_name="QSR Top30 核心数据",
                details={"row_count": len(core_brand_metrics)},
            )
        ]

    duplicate_brands = (
        duplicated.groupby(CORE_BRAND_NAME_COLUMN)[["rank", "row_number"]]
        .apply(lambda frame: frame.to_dict(orient="records"))
        .to_dict()
    )
    return [
        ValidationFinding(
            severity="error",
            category="uniqueness",
            check_name="core_brand_metrics.brand_unique",
            dataset="core_brand_metrics",
            message="Duplicate brand names found in the core table.",
            sheet_name="QSR Top30 核心数据",
            details={"duplicates": duplicate_brands},
        )
    ]


def check_brand_alignment(
    core_brand_metrics: pd.DataFrame,
    ai_strategy_registry: pd.DataFrame,
) -> list[ValidationFinding]:
    core_brands = _normalized_brand_set(core_brand_metrics[CORE_BRAND_NAME_COLUMN])
    ai_brands = _normalized_brand_set(ai_strategy_registry[CORE_BRAND_NAME_COLUMN])

    extra_ai_brands = sorted(ai_brands - core_brands)
    missing_ai_brands = sorted(core_brands - ai_brands)
    overlap = sorted(core_brands & ai_brands)

    findings = []

    if extra_ai_brands:
        findings.append(
            ValidationFinding(
                severity="warning",
                category="cross_sheet",
                check_name="brand_alignment.extra_ai_brands",
                dataset="ai_strategy_registry",
                message=(
                    "AI strategy sheet includes brands that are not present in the Top 30 core table: "
                    + ", ".join(extra_ai_brands)
                ),
                sheet_name="AI策略与落地效果",
                details={"extra_ai_brands": extra_ai_brands},
            )
        )

    if missing_ai_brands:
        findings.append(
            ValidationFinding(
                severity="warning",
                category="cross_sheet",
                check_name="brand_alignment.missing_ai_brands",
                dataset="core_brand_metrics",
                message=(
                    "Core brands are missing corresponding AI strategy rows: "
                    + ", ".join(missing_ai_brands)
                ),
                sheet_name="AI策略与落地效果",
                details={"missing_ai_brands": missing_ai_brands},
            )
        )

    if not extra_ai_brands and not missing_ai_brands:
        findings.append(
            ValidationFinding(
                severity="info",
                category="cross_sheet",
                check_name="brand_alignment.exact_match",
                dataset="cross_sheet",
                sheet_name="AI策略与落地效果",
                message=(f"AI sheet brands align exactly with all {len(core_brands)} core brands."),
                details={
                    "core_brand_count": len(core_brands),
                    "ai_brand_count": len(ai_brands),
                    "overlap_count": len(overlap),
                },
            )
        )
    else:
        findings.append(
            ValidationFinding(
                severity="info",
                category="cross_sheet",
                check_name="brand_alignment.coverage",
                dataset="cross_sheet",
                sheet_name="AI策略与落地效果",
                message=(
                    f"AI sheet covers {len(overlap)} of {len(core_brands)} core brands "
                    f"({len(missing_ai_brands)} missing; {len(extra_ai_brands)} extra)."
                ),
                details={
                    "core_brand_count": len(core_brands),
                    "ai_brand_count": len(ai_brands),
                    "overlap_count": len(overlap),
                    "missing_ai_brands": missing_ai_brands,
                    "extra_ai_brands": extra_ai_brands,
                },
            )
        )

    return findings


def check_implied_auv(
    core_brand_metrics: pd.DataFrame,
    *,
    tolerance_auv: float,
) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    failing_rows: list[dict[str, object]] = []
    passing_count = 0

    for record in core_brand_metrics[
        [
            CORE_BRAND_NAME_COLUMN,
            "row_number",
            CORE_STORE_COUNT_COLUMN,
            CORE_SYSTEM_SALES_COLUMN,
            CORE_AUV_COLUMN,
        ]
    ].to_dict(orient="records"):
        brand = record[CORE_BRAND_NAME_COLUMN]
        row_number = _to_int(record["row_number"])
        store_count = _to_float(record[CORE_STORE_COUNT_COLUMN])
        system_sales_b = _to_float(record[CORE_SYSTEM_SALES_COLUMN])
        actual_auv_k = _to_float(record[CORE_AUV_COLUMN])

        if (
            store_count is None
            or store_count <= 0
            or system_sales_b is None
            or actual_auv_k is None
        ):
            findings.append(
                ValidationFinding(
                    severity="error",
                    category="arithmetic",
                    check_name="implied_auv_k",
                    dataset="core_brand_metrics",
                    message=f"{brand} is missing values needed to compute implied AUV.",
                    sheet_name="QSR Top30 核心数据",
                    brand_name=str(brand),
                    row_number=row_number,
                    details={
                        "store_count": store_count,
                        "system_sales_b": system_sales_b,
                        "actual_auv_k": actual_auv_k,
                    },
                )
            )
            continue

        expected_auv_k = system_sales_b * 1_000_000 / store_count
        if expected_auv_k == 0:
            if actual_auv_k == 0:
                passing_count += 1
                continue
            relative_delta = None
        else:
            relative_delta = abs(actual_auv_k - expected_auv_k) / expected_auv_k

        if relative_delta is not None and relative_delta <= tolerance_auv:
            passing_count += 1
            continue

        failing_rows.append(
            {
                "brand_name": brand,
                "row_number": row_number,
                "actual_auv_k": actual_auv_k,
                "expected_auv_k": expected_auv_k,
                "relative_delta": relative_delta,
            }
        )

    if passing_count:
        findings.append(
            ValidationFinding(
                severity="info",
                category="arithmetic",
                check_name="implied_auv_k",
                dataset="core_brand_metrics",
                message=(
                    f"{passing_count} core rows matched implied AUV within {tolerance_auv:.0%} tolerance."
                ),
                sheet_name="QSR Top30 核心数据",
                details={"passing_rows": passing_count, "tolerance_auv": tolerance_auv},
            )
        )

    for row in failing_rows:
        if row["relative_delta"] is None:
            delta_text = (
                f"expected {row['expected_auv_k']:.1f}k but recorded {row['actual_auv_k']:.1f}k"
            )
        else:
            delta_pct = row["relative_delta"] * 100
            delta_text = (
                f"{row['expected_auv_k']:.1f}k vs recorded {row['actual_auv_k']:.1f}k "
                f"({delta_pct:.1f}% delta; tolerance {tolerance_auv:.0%})"
            )
        findings.append(
            ValidationFinding(
                severity="error",
                category="arithmetic",
                check_name="implied_auv_k",
                dataset="core_brand_metrics",
                message=(f"{row['brand_name']} has implied AUV {delta_text}."),
                sheet_name="QSR Top30 核心数据",
                brand_name=str(row["brand_name"]),
                row_number=_to_int(row["row_number"]),
                details=row,
            )
        )

    return findings


def check_monotonic_ranges(core_brand_metrics: pd.DataFrame) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    findings.extend(
        _check_monotonic_range(
            core_brand_metrics,
            prefix="fte",
            min_column="fte_min",
            mid_column="fte_mid",
            max_column="fte_max",
            label="FTE range",
        )
    )
    findings.extend(
        _check_monotonic_range(
            core_brand_metrics,
            prefix="margin",
            min_column="margin_min_pct",
            mid_column="margin_mid_pct",
            max_column="margin_max_pct",
            label="margin range",
        )
    )
    return findings


def _check_monotonic_range(
    frame: pd.DataFrame,
    *,
    prefix: str,
    min_column: str,
    mid_column: str,
    max_column: str,
    label: str,
) -> list[ValidationFinding]:
    invalid_rows: list[dict[str, object]] = []
    for record in frame[
        [CORE_BRAND_NAME_COLUMN, "row_number", min_column, mid_column, max_column]
    ].to_dict(orient="records"):
        minimum = _to_float(record[min_column])
        midpoint = _to_float(record[mid_column])
        maximum = _to_float(record[max_column])
        row_number = _to_int(record["row_number"])
        brand_name = str(record[CORE_BRAND_NAME_COLUMN])

        if minimum is None or midpoint is None or maximum is None:
            invalid_rows.append(
                {
                    "brand_name": brand_name,
                    "row_number": row_number,
                    "minimum": minimum,
                    "midpoint": midpoint,
                    "maximum": maximum,
                }
            )
            continue

        if not (minimum <= midpoint <= maximum):
            invalid_rows.append(
                {
                    "brand_name": brand_name,
                    "row_number": row_number,
                    "minimum": minimum,
                    "midpoint": midpoint,
                    "maximum": maximum,
                }
            )

    if not invalid_rows:
        return [
            ValidationFinding(
                severity="info",
                category="allowed_range",
                check_name=f"{prefix}_range_order",
                dataset="core_brand_metrics",
                message=f"{label} min <= mid <= max holds for all {len(frame)} core rows.",
                sheet_name="QSR Top30 核心数据",
                details={"row_count": len(frame)},
            )
        ]

    findings = [
        ValidationFinding(
            severity="error",
            category="allowed_range",
            check_name=f"{prefix}_range_order",
            dataset="core_brand_metrics",
            message=f"{label} order check failed for {len(invalid_rows)} core rows.",
            sheet_name="QSR Top30 核心数据",
            details={"invalid_rows": invalid_rows},
        )
    ]
    for row in invalid_rows[:10]:
        findings.append(
            ValidationFinding(
                severity="error",
                category="allowed_range",
                check_name=f"{prefix}_range_order.row",
                dataset="core_brand_metrics",
                message=(
                    f"{row['brand_name']} has {label} values "
                    f"{row['minimum']} <= {row['midpoint']} <= {row['maximum']} violated."
                ),
                sheet_name="QSR Top30 核心数据",
                brand_name=str(row["brand_name"]),
                row_number=_to_int(row["row_number"]),
                details=row,
            )
        )
    return findings


def _normalized_brand_set(values: pd.Series) -> set[str]:
    return {str(value) for value in values.dropna().astype(str).str.strip() if str(value).strip()}


def _to_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "CORE_AUV_COLUMN",
    "CORE_BRAND_NAME_COLUMN",
    "CORE_RANK_COLUMN",
    "CORE_STORE_COUNT_COLUMN",
    "CORE_SYSTEM_SALES_COLUMN",
    "InvariantBundle",
    "check_brand_alignment",
    "check_core_brand_uniqueness",
    "check_implied_auv",
    "check_monotonic_ranges",
    "check_rank_uniqueness",
    "evaluate_invariants",
]
