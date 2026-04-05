"""Versioned Gold publishing gate policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

GateReasonSeverity = Literal["error", "warning", "info"]
PublishStatus = Literal["publishable", "advisory", "blocked"]


@dataclass(frozen=True)
class MetricGatePolicy:
    """Per-metric publishing policy for Gold export decisions."""

    metric_name: str
    value_column: str
    reference_prefix: str | None
    default_status: PublishStatus
    require_reference_evidence: bool
    require_complete_provenance: bool
    minimum_confidence_score: float | None
    allowed_reconciliation_grades: tuple[str, ...]
    publishable_methods: tuple[str, ...]
    hard_fail_validation_checks: tuple[str, ...]
    advisory_validation_checks: tuple[str, ...]
    description: str

    @property
    def advisory_only(self) -> bool:
        return self.default_status == "advisory"


@dataclass(frozen=True)
class GoldPublishPolicy:
    """Top-level versioned policy bundle."""

    policy_id: str
    version: str
    description: str
    required_provenance_fields: tuple[str, ...]
    synthetic_warning_strengths: tuple[str, ...]
    reconciliation_relative_error_hard_fail: float
    metric_policies: tuple[MetricGatePolicy, ...]

    def metric_policy_map(self) -> dict[str, MetricGatePolicy]:
        return {policy.metric_name: policy for policy in self.metric_policies}


GOLD_PUBLISH_POLICY_V1 = GoldPublishPolicy(
    policy_id="gold-publish-policy",
    version="v1.0.0",
    description=(
        "Conservative Gold publishing policy for workbook-derived KPI exports. "
        "External-facing metrics require manual reference evidence, complete provenance, "
        "and no unresolved contradictions. Estimated operational metrics remain advisory-only "
        "unless a future policy revision explicitly promotes them."
    ),
    required_provenance_fields=(
        "source_type",
        "source_name",
        "source_url_or_doc_id",
        "as_of_date",
        "method_reported_or_estimated",
        "confidence_score",
    ),
    synthetic_warning_strengths=("moderate", "strong"),
    reconciliation_relative_error_hard_fail=0.05,
    metric_policies=(
        MetricGatePolicy(
            metric_name="rank",
            value_column="rank",
            reference_prefix="rank",
            default_status="publishable",
            require_reference_evidence=True,
            require_complete_provenance=True,
            minimum_confidence_score=0.8,
            allowed_reconciliation_grades=("A", "B"),
            publishable_methods=("reported", "reported_and_estimated", "mixed"),
            hard_fail_validation_checks=(
                "core_brand_metrics.rank_unique",
                "core_brand_metrics.brand_unique",
            ),
            advisory_validation_checks=("brand_alignment.missing_ai_brands",),
            description="Workbook rank is only exportable when external ranking evidence is strong.",
        ),
        MetricGatePolicy(
            metric_name="store_count",
            value_column="us_store_count_2024",
            reference_prefix="store_count",
            default_status="publishable",
            require_reference_evidence=True,
            require_complete_provenance=True,
            minimum_confidence_score=0.8,
            allowed_reconciliation_grades=("A", "B"),
            publishable_methods=("reported", "reported_and_estimated", "mixed"),
            hard_fail_validation_checks=("core_brand_metrics.brand_unique",),
            advisory_validation_checks=("brand_alignment.missing_ai_brands",),
            description="Store counts require strong external evidence before publication.",
        ),
        MetricGatePolicy(
            metric_name="system_sales",
            value_column="systemwide_revenue_usd_billions_2024",
            reference_prefix="system_sales",
            default_status="publishable",
            require_reference_evidence=True,
            require_complete_provenance=True,
            minimum_confidence_score=0.8,
            allowed_reconciliation_grades=("A", "B"),
            publishable_methods=("reported", "reported_and_estimated", "mixed"),
            hard_fail_validation_checks=("core_brand_metrics.brand_unique",),
            advisory_validation_checks=("brand_alignment.missing_ai_brands",),
            description="System sales require manual reference support and no unresolved contradictions.",
        ),
        MetricGatePolicy(
            metric_name="auv",
            value_column="average_unit_volume_usd_thousands",
            reference_prefix="auv",
            default_status="publishable",
            require_reference_evidence=True,
            require_complete_provenance=True,
            minimum_confidence_score=0.8,
            allowed_reconciliation_grades=("A", "B"),
            publishable_methods=("reported", "reported_and_estimated", "mixed"),
            hard_fail_validation_checks=("implied_auv_k",),
            advisory_validation_checks=("brand_alignment.missing_ai_brands",),
            description="AUV is blocked when the workbook arithmetic invariant contradicts the claimed value.",
        ),
        MetricGatePolicy(
            metric_name="fte_mid",
            value_column="fte_mid",
            reference_prefix=None,
            default_status="advisory",
            require_reference_evidence=False,
            require_complete_provenance=False,
            minimum_confidence_score=None,
            allowed_reconciliation_grades=(),
            publishable_methods=(),
            hard_fail_validation_checks=("fte_range_order", "fte_range_order.row"),
            advisory_validation_checks=("brand_alignment.missing_ai_brands",),
            description=(
                "Labor FTE estimates remain advisory-only because they are often workbook estimates "
                "rather than externally reported metrics."
            ),
        ),
        MetricGatePolicy(
            metric_name="margin_mid_pct",
            value_column="margin_mid_pct",
            reference_prefix=None,
            default_status="advisory",
            require_reference_evidence=False,
            require_complete_provenance=False,
            minimum_confidence_score=None,
            allowed_reconciliation_grades=(),
            publishable_methods=(),
            hard_fail_validation_checks=("margin_range_order", "margin_range_order.row"),
            advisory_validation_checks=("brand_alignment.missing_ai_brands",),
            description=(
                "Restaurant-level operating margin estimates remain advisory-only until stronger "
                "analyst-supplied evidence is attached."
            ),
        ),
    ),
)

__all__ = [
    "GOLD_PUBLISH_POLICY_V1",
    "GateReasonSeverity",
    "GoldPublishPolicy",
    "MetricGatePolicy",
    "PublishStatus",
]
