"""Gold-only strategy recommendation pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.strategy.archetypes import (
    ArchetypeMatch,
    is_drive_thru_candidate,
    match_brand_archetypes,
    parse_franchise_share,
)

READINESS_ORDER = {"hold": 0, "caution": 1, "ready": 2}
RECONCILIATION_FIELDS = ("rank", "store_count", "system_sales", "auv")


@dataclass(frozen=True)
class StrategyRule:
    """A deterministic strategy rule template."""

    initiative_code: str
    initiative_name: str
    priority_bucket: str
    recommendation_summary: str
    rationale: str
    guardrail: str
    evidence_fields: tuple[str, ...]
    minimum_readiness: str = "hold"


@dataclass(frozen=True)
class StrategyContext:
    """Gold-derived strategy context for a single brand."""

    brand_name: str
    canonical_brand_name: str
    source_layer: str
    source_sheet: str
    source_row_number: int | None
    primary_archetype_code: str
    primary_archetype_name: str
    matched_archetype_codes: tuple[str, ...]
    matched_archetypes: tuple[str, ...]
    category: str
    ownership_model: str
    drive_thru_candidate: bool
    strategy_readiness: str
    reference_source_count: int
    overall_credibility_grade: str
    validation_error_count: int
    validation_warning_count: int
    brand_synthetic_signal_count: int
    weakest_provenance_fields: tuple[str, ...]
    largest_reconciliation_errors: tuple[str, ...]
    operational_signals: tuple[str, ...]
    evidence_snapshot: dict[str, Any]
    open_issues: tuple[str, ...]
    plain_english_caveat: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "brand_name": self.brand_name,
            "canonical_brand_name": self.canonical_brand_name,
            "source_layer": self.source_layer,
            "source_sheet": self.source_sheet,
            "source_row_number": self.source_row_number,
            "primary_archetype_code": self.primary_archetype_code,
            "primary_archetype_name": self.primary_archetype_name,
            "matched_archetype_codes": list(self.matched_archetype_codes),
            "matched_archetypes": list(self.matched_archetypes),
            "category": self.category,
            "ownership_model": self.ownership_model,
            "drive_thru_candidate": self.drive_thru_candidate,
            "strategy_readiness": self.strategy_readiness,
            "reference_source_count": self.reference_source_count,
            "overall_credibility_grade": self.overall_credibility_grade,
            "validation_error_count": self.validation_error_count,
            "validation_warning_count": self.validation_warning_count,
            "brand_synthetic_signal_count": self.brand_synthetic_signal_count,
            "weakest_provenance_fields": list(self.weakest_provenance_fields),
            "largest_reconciliation_errors": list(self.largest_reconciliation_errors),
            "operational_signals": list(self.operational_signals),
            "evidence_snapshot": self.evidence_snapshot,
            "open_issues": list(self.open_issues),
            "plain_english_caveat": self.plain_english_caveat,
        }


@dataclass(frozen=True)
class StrategyRecommendation:
    """One rules-based recommendation row."""

    brand_name: str
    canonical_brand_name: str
    source_layer: str
    source_sheet: str
    source_row_number: int | None
    primary_archetype_code: str
    primary_archetype_name: str
    matched_archetype_codes: tuple[str, ...]
    matched_archetypes: tuple[str, ...]
    strategy_readiness: str
    priority_rank: int
    priority_bucket: str
    initiative_code: str
    initiative_name: str
    recommendation_summary: str
    rationale: str
    guardrail: str
    evidence_fields: tuple[str, ...]
    evidence_snapshot: dict[str, Any]
    validation_error_count: int
    validation_warning_count: int
    brand_synthetic_signal_count: int
    reference_source_count: int
    overall_credibility_grade: str
    weakest_provenance_fields: tuple[str, ...]
    largest_reconciliation_errors: tuple[str, ...]
    open_issues: tuple[str, ...]
    plain_english_caveat: str
    no_roi_claim: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "brand_name": self.brand_name,
            "canonical_brand_name": self.canonical_brand_name,
            "source_layer": self.source_layer,
            "source_sheet": self.source_sheet,
            "source_row_number": self.source_row_number,
            "primary_archetype_code": self.primary_archetype_code,
            "primary_archetype_name": self.primary_archetype_name,
            "matched_archetype_codes": list(self.matched_archetype_codes),
            "matched_archetypes": list(self.matched_archetypes),
            "strategy_readiness": self.strategy_readiness,
            "priority_rank": self.priority_rank,
            "priority_bucket": self.priority_bucket,
            "initiative_code": self.initiative_code,
            "initiative_name": self.initiative_name,
            "recommendation_summary": self.recommendation_summary,
            "rationale": self.rationale,
            "guardrail": self.guardrail,
            "evidence_fields": list(self.evidence_fields),
            "evidence_snapshot": self.evidence_snapshot,
            "validation_error_count": self.validation_error_count,
            "validation_warning_count": self.validation_warning_count,
            "brand_synthetic_signal_count": self.brand_synthetic_signal_count,
            "reference_source_count": self.reference_source_count,
            "overall_credibility_grade": self.overall_credibility_grade,
            "weakest_provenance_fields": list(self.weakest_provenance_fields),
            "largest_reconciliation_errors": list(self.largest_reconciliation_errors),
            "open_issues": list(self.open_issues),
            "plain_english_caveat": self.plain_english_caveat,
            "no_roi_claim": self.no_roi_claim,
        }


@dataclass(frozen=True)
class StrategyArtifacts:
    """Files produced by the strategy pipeline."""

    recommendations_parquet_path: Path
    recommendations_json_path: Path
    playbook_markdown_path: Path


@dataclass(frozen=True)
class StrategyRun:
    """Full strategy pipeline result."""

    contexts: tuple[StrategyContext, ...]
    recommendations: tuple[StrategyRecommendation, ...]
    warnings: tuple[str, ...]
    artifacts: StrategyArtifacts


DATA_FOUNDATION_RULE = StrategyRule(
    initiative_code="data_foundation",
    initiative_name="Resolve Gold contradictions before scaling automation",
    priority_bucket="now",
    recommendation_summary=(
        "Stabilize validation errors, missing coverage, and analyst review before advancing any risky automation."
    ),
    rationale=(
        "Gold validation contradictions mean the current workbook cannot safely support higher-risk automation bets."
    ),
    guardrail=(
        "Recommendations stay directional until Gold validation contradictions are resolved."
    ),
    evidence_fields=("overall_credibility_grade", "reference_source_count"),
)

DRIVE_THRU_VOICE_RULE = StrategyRule(
    initiative_code="drive_thru_voice_ai",
    initiative_name="Pilot drive-thru voice AI cautiously",
    priority_bucket="later",
    recommendation_summary=(
        "Treat drive-thru voice AI as a narrow pilot only after stronger operational instrumentation is in place."
    ),
    rationale=(
        "The supplemental plan explicitly places backend operations AI ahead of risky customer-facing automation."
    ),
    guardrail=(
        "Do not scale drive-thru voice AI until queue, labor, and exception telemetry are dependable."
    ),
    evidence_fields=("category", "us_store_count_2024", "average_unit_volume_usd_thousands"),
    minimum_readiness="ready",
)

RULE_LIBRARY: dict[str, tuple[StrategyRule, ...]] = {
    "premium_service": (
        StrategyRule(
            initiative_code="ops_backoffice_ai",
            initiative_name="Prioritize backend / operations AI",
            priority_bucket="now",
            recommendation_summary=(
                "Start with labor forecasting, prep planning, and service exception monitoring."
            ),
            rationale=(
                "High-AUV, labor-led formats need precision and consistency more than blanket automation."
            ),
            guardrail=(
                "Protect high-touch service quality and avoid replacing core hospitality moments."
            ),
            evidence_fields=("average_unit_volume_usd_thousands", "fte_mid", "margin_mid_pct"),
        ),
        StrategyRule(
            initiative_code="service_labor_copilot",
            initiative_name="Protect service quality with labor and recovery copilots",
            priority_bucket="next",
            recommendation_summary=(
                "Use labor deployment and service-recovery copilots before aggressive guest-facing automation."
            ),
            rationale="Premium service brands still depend on human execution quality.",
            guardrail=(
                "Keep AI in a support role for service teams rather than replacing customer-facing moments."
            ),
            evidence_fields=("average_unit_volume_usd_thousands", "fte_mid"),
        ),
        StrategyRule(
            initiative_code="quality_exception_ai",
            initiative_name="Add quality and exception monitoring",
            priority_bucket="next",
            recommendation_summary=(
                "Use targeted QA, exception monitoring, and narrow computer-vision checks."
            ),
            rationale=(
                "Premium brands can reduce execution drift without flattening the service model."
            ),
            guardrail="Keep tooling tied to exception reduction, not speculative full automation.",
            evidence_fields=("margin_mid_pct", "central_kitchen_supply_chain_model"),
        ),
    ),
    "beverage_ops": (
        StrategyRule(
            initiative_code="ops_backoffice_ai",
            initiative_name="Prioritize backend / operations AI",
            priority_bucket="now",
            recommendation_summary=(
                "Start with daypart demand forecasting, labor scheduling, and queue monitoring."
            ),
            rationale=(
                "Beverage-led formats benefit first from tighter staffing, queue control, and prep timing."
            ),
            guardrail="Keep customer-facing automation behind stronger operational telemetry.",
            evidence_fields=("category", "fte_mid", "us_store_count_2024"),
        ),
        StrategyRule(
            initiative_code="beverage_equipment_queueing",
            initiative_name="Add beverage-station telemetry and queue control",
            priority_bucket="next",
            recommendation_summary=(
                "Use equipment telemetry, recipe adherence alerts, and pickup queue orchestration."
            ),
            rationale=(
                "Drink quality and speed are tightly linked to equipment uptime and recipe consistency."
            ),
            guardrail="Use telemetry and QA before attempting broader front-end automation changes.",
            evidence_fields=("category", "central_kitchen_supply_chain_model", "fte_mid"),
        ),
        StrategyRule(
            initiative_code="digital_queueing_personalization",
            initiative_name="Lean into digital queueing and personalization where fit is strong",
            priority_bucket="next",
            recommendation_summary=(
                "Use repeat-order signals for pickup orchestration and targeted personalization."
            ),
            rationale=(
                "Beverage formats have the strongest fit for repeatable queueing and personalization flows."
            ),
            guardrail="Only expand personalization where repeat behavior and customer data are already visible.",
            evidence_fields=(
                "category",
                "us_store_count_2024",
                "average_unit_volume_usd_thousands",
            ),
            minimum_readiness="caution",
        ),
    ),
    "throughput_model": (
        StrategyRule(
            initiative_code="ops_backoffice_ai",
            initiative_name="Prioritize backend / operations AI",
            priority_bucket="now",
            recommendation_summary=(
                "Tighten order-flow analytics, queue balancing, and throughput exception monitoring."
            ),
            rationale=(
                "Simple, high-throughput formats benefit first from tighter queue and labor coordination."
            ),
            guardrail="Start with operations AI before any customer-facing automation changes.",
            evidence_fields=(
                "fte_mid",
                "us_store_count_2024",
                "central_kitchen_supply_chain_model",
            ),
        ),
        StrategyRule(
            initiative_code="targeted_station_automation",
            initiative_name="Automate narrow repetitive bottlenecks first",
            priority_bucket="next",
            recommendation_summary=(
                "Target fryer, packout, or assembly pinch points before considering anything larger."
            ),
            rationale=(
                "Repetitive bottlenecks are easier to automate safely than whole-station replacements."
            ),
            guardrail="Keep automation narrow and measurable; do not jump straight to end-to-end robotics.",
            evidence_fields=("fte_mid", "central_kitchen_supply_chain_model"),
            minimum_readiness="caution",
        ),
        StrategyRule(
            initiative_code="digital_queue_visibility",
            initiative_name="Improve pickup sequencing and queue visibility",
            priority_bucket="next",
            recommendation_summary=(
                "Use pickup sequencing and ETA visibility where ordering volume is concentrated."
            ),
            rationale=(
                "High-throughput models often benefit from better queue visibility without large front-end changes."
            ),
            guardrail="Use queueing tools only where order-flow concentration is observable.",
            evidence_fields=("us_store_count_2024", "average_unit_volume_usd_thousands"),
            minimum_readiness="caution",
        ),
        StrategyRule(
            initiative_code="full_robotics_watchlist",
            initiative_name="Keep full robotics on a later-stage watchlist",
            priority_bucket="later",
            recommendation_summary=(
                "Treat end-to-end robotics as a later-stage watchlist item, not a v1 recommendation."
            ),
            rationale=(
                "The operating model may be automation-friendly, but the plan still favors smaller modules first."
            ),
            guardrail="Avoid portfolio-wide robotics bets without tighter evidence and process stability.",
            evidence_fields=("fte_mid", "margin_mid_pct"),
            minimum_readiness="ready",
        ),
    ),
    "franchise_standardized": (
        StrategyRule(
            initiative_code="ops_backoffice_ai",
            initiative_name="Prioritize backend / operations AI",
            priority_bucket="now",
            recommendation_summary=(
                "Start with unit-level variance detection, labor forecasting, and benchmark reporting."
            ),
            rationale=(
                "Franchise-heavy systems typically benefit first from consistency, benchmarking, and exception detection."
            ),
            guardrail="Use standardized operational AI before broad customer-facing automation.",
            evidence_fields=("ownership_model", "us_store_count_2024"),
        ),
        StrategyRule(
            initiative_code="franchise_benchmarking",
            initiative_name="Roll out standardized franchise ops analytics",
            priority_bucket="next",
            recommendation_summary=(
                "Push standardized forecasting, inventory, and compliance analytics before bespoke pilots."
            ),
            rationale=(
                "Highly franchised systems scale faster through repeatable back-office tooling than custom automation."
            ),
            guardrail="Tie analytics to existing SOPs instead of creating new operational truth in the strategy layer.",
            evidence_fields=("ownership_model", "us_store_count_2024"),
            minimum_readiness="caution",
        ),
        StrategyRule(
            initiative_code="workflow_copilots",
            initiative_name="Deploy manager and SOP workflow copilots",
            priority_bucket="next",
            recommendation_summary=(
                "Use manager copilots, SOP search, and localized training support."
            ),
            rationale="Standardized operating models can scale guidance and training efficiently.",
            guardrail="Keep copilots tied to existing SOPs rather than generating new source-of-truth metrics.",
            evidence_fields=("ownership_model", "central_kitchen_supply_chain_model"),
            minimum_readiness="caution",
        ),
        StrategyRule(
            initiative_code="targeted_automation",
            initiative_name="Use targeted automation only where variance is already low",
            priority_bucket="later",
            recommendation_summary=(
                "Target repetitive prep or QA tasks only after unit-level process variation is under control."
            ),
            rationale=(
                "Standardized formats are better candidates for narrow automation once ops baselines are stable."
            ),
            guardrail="Avoid large hardware programs before unit-level variance is demonstrably under control.",
            evidence_fields=("ownership_model", "margin_mid_pct"),
            minimum_readiness="ready",
        ),
    ),
    "digital_delivery": (
        StrategyRule(
            initiative_code="ops_backoffice_ai",
            initiative_name="Prioritize backend / operations AI",
            priority_bucket="now",
            recommendation_summary=(
                "Start with demand routing, make-line load balancing, and ETA risk detection."
            ),
            rationale=(
                "Delivery-like flows benefit first from better demand routing and production balancing."
            ),
            guardrail="Start with fulfillment operations instead of headline customer-facing automation.",
            evidence_fields=(
                "category",
                "us_store_count_2024",
                "average_unit_volume_usd_thousands",
            ),
        ),
        StrategyRule(
            initiative_code="digital_queueing_personalization",
            initiative_name="Lean into digital queueing and personalization where fit is strong",
            priority_bucket="next",
            recommendation_summary=(
                "Optimize ordering paths, reorder prompts, and pickup timing where digital behavior is strong."
            ),
            rationale=(
                "This archetype has the strongest fit for digital queueing and personalization."
            ),
            guardrail="Do not force personalization without observable repeat-order or digital behavior signals.",
            evidence_fields=(
                "category",
                "us_store_count_2024",
                "average_unit_volume_usd_thousands",
            ),
            minimum_readiness="caution",
        ),
        StrategyRule(
            initiative_code="pack_accuracy_handoff_ai",
            initiative_name="Improve pack accuracy and handoff quality",
            priority_bucket="next",
            recommendation_summary=(
                "Use pack-check, handoff QA, and exception monitoring for fulfillment accuracy."
            ),
            rationale="Delivery-like flows are sensitive to handoff quality and order accuracy.",
            guardrail="Favor fulfillment QA over speculative front-end automation claims.",
            evidence_fields=("category", "margin_mid_pct"),
            minimum_readiness="caution",
        ),
    ),
    "assembly_automation": (
        StrategyRule(
            initiative_code="ops_backoffice_ai",
            initiative_name="Prioritize backend / operations AI",
            priority_bucket="now",
            recommendation_summary=(
                "Stabilize forecast, labor, and exception telemetry before hardware changes."
            ),
            rationale=(
                "Even automation-friendly formats need operational telemetry before robotics decisions."
            ),
            guardrail="Do not let the strategy layer override Gold metrics or invent ROI assumptions.",
            evidence_fields=("central_kitchen_supply_chain_model", "fte_mid", "margin_mid_pct"),
        ),
        StrategyRule(
            initiative_code="targeted_station_automation",
            initiative_name="Automate narrow repetitive stations first",
            priority_bucket="next",
            recommendation_summary=(
                "Automate the narrowest repetitive stations before discussing any full-line automation."
            ),
            rationale=(
                "Assembly-line signals make targeted automation more practical than end-to-end robotics."
            ),
            guardrail="Start with one repetitive module, not a full-line replacement.",
            evidence_fields=("central_kitchen_supply_chain_model", "fte_mid", "category"),
            minimum_readiness="caution",
        ),
        StrategyRule(
            initiative_code="vision_qc",
            initiative_name="Use computer vision for quality and safety checks",
            priority_bucket="next",
            recommendation_summary=(
                "Use computer vision for line balance, quality, and safety checks before robotics escalation."
            ),
            rationale=(
                "Vision systems can improve repetitive flows without forcing a large robotics program."
            ),
            guardrail="Keep vision tied to measurable process checks and exception handling.",
            evidence_fields=("central_kitchen_supply_chain_model", "fte_mid"),
            minimum_readiness="caution",
        ),
        StrategyRule(
            initiative_code="full_robotics_watchlist",
            initiative_name="Keep full robotics on a later-stage watchlist",
            priority_bucket="later",
            recommendation_summary=(
                "Treat full robotics as a later-stage watchlist item unless process variance is already low."
            ),
            rationale=(
                "The supplemental plan explicitly places targeted automation ahead of full robotics."
            ),
            guardrail="Require stable workflows and stronger external evidence before larger robotics pushes.",
            evidence_fields=("fte_mid", "margin_mid_pct"),
            minimum_readiness="ready",
        ),
    ),
}


def build_strategy_context(*, gold_dir: Path) -> list[StrategyContext]:
    """Build one Gold-only strategy context per reconciled brand."""

    reconciled = pd.read_parquet(gold_dir / "reconciled_core_metrics.parquet")
    validation_flags = pd.read_parquet(gold_dir / "validation_flags.parquet")
    syntheticness = pd.read_parquet(gold_dir / "syntheticness_signals.parquet")

    validation_by_brand = _validation_counts_by_brand(validation_flags)
    synthetic_by_brand = _synthetic_counts_by_brand(syntheticness)

    contexts: list[StrategyContext] = []
    for row in reconciled.to_dict(orient="records"):
        brand_name = _clean_text(row.get("brand_name"))
        canonical_brand_name = _clean_text(row.get("canonical_brand_name")) or brand_name
        matches = match_brand_archetypes(row)
        primary_match = matches[0]
        validation_counts = validation_by_brand.get(
            canonical_brand_name, {"error": 0, "warning": 0, "issues": ()}
        )
        synthetic_counts = synthetic_by_brand.get(
            canonical_brand_name, {"moderate_or_strong": 0, "issues": ()}
        )
        reference_source_count = _as_int(row.get("reference_source_count")) or 0
        overall_grade = _clean_text(row.get("overall_credibility_grade")) or "MISSING"
        strategy_readiness = _infer_strategy_readiness(
            validation_error_count=validation_counts["error"],
            reference_source_count=reference_source_count,
            overall_credibility_grade=overall_grade,
        )
        weakest_provenance_fields = _weakest_provenance_fields(row)
        largest_reconciliation_errors = _largest_reconciliation_errors(row)
        evidence_snapshot = _build_evidence_snapshot(row)

        contexts.append(
            StrategyContext(
                brand_name=brand_name,
                canonical_brand_name=canonical_brand_name,
                source_layer="gold",
                source_sheet=_clean_text(row.get("source_sheet")),
                source_row_number=_as_int(row.get("row_number")),
                primary_archetype_code=primary_match.archetype_code,
                primary_archetype_name=primary_match.archetype_name,
                matched_archetype_codes=tuple(match.archetype_code for match in matches),
                matched_archetypes=tuple(match.archetype_name for match in matches),
                category=_clean_text(row.get("category")),
                ownership_model=_clean_text(row.get("ownership_model")),
                drive_thru_candidate=is_drive_thru_candidate(row),
                strategy_readiness=strategy_readiness,
                reference_source_count=reference_source_count,
                overall_credibility_grade=overall_grade,
                validation_error_count=validation_counts["error"],
                validation_warning_count=validation_counts["warning"],
                brand_synthetic_signal_count=synthetic_counts["moderate_or_strong"],
                weakest_provenance_fields=weakest_provenance_fields,
                largest_reconciliation_errors=largest_reconciliation_errors,
                operational_signals=_build_operational_signals(row, primary_match),
                evidence_snapshot=evidence_snapshot,
                open_issues=_build_open_issues(
                    validation_issues=validation_counts["issues"],
                    reconciliation_warning=row.get("reconciliation_warning"),
                    synthetic_issues=synthetic_counts["issues"],
                    weakest_provenance_fields=weakest_provenance_fields,
                ),
                plain_english_caveat=_build_plain_english_caveat(
                    strategy_readiness=strategy_readiness,
                    reference_source_count=reference_source_count,
                    overall_credibility_grade=overall_grade,
                ),
            )
        )

    return contexts


def build_brand_recommendations(
    contexts: list[StrategyContext],
) -> list[StrategyRecommendation]:
    """Build one ordered recommendation stack per brand context."""

    recommendations: list[StrategyRecommendation] = []
    for context in contexts:
        selected_rules: list[StrategyRule] = []
        seen_codes: set[str] = set()

        if context.strategy_readiness == "hold":
            selected_rules.append(DATA_FOUNDATION_RULE)
            seen_codes.add(DATA_FOUNDATION_RULE.initiative_code)

        for archetype_code in context.matched_archetype_codes:
            for rule in RULE_LIBRARY.get(archetype_code, ()):
                if rule.initiative_code in seen_codes:
                    continue
                if not _rule_allowed(context.strategy_readiness, rule.minimum_readiness):
                    continue
                selected_rules.append(rule)
                seen_codes.add(rule.initiative_code)

        if (
            context.drive_thru_candidate
            and DRIVE_THRU_VOICE_RULE.initiative_code not in seen_codes
            and _rule_allowed(context.strategy_readiness, DRIVE_THRU_VOICE_RULE.minimum_readiness)
        ):
            selected_rules.append(DRIVE_THRU_VOICE_RULE)

        for priority_rank, rule in enumerate(selected_rules, start=1):
            evidence_snapshot = _select_evidence_snapshot(
                context.evidence_snapshot,
                rule.evidence_fields,
            )
            recommendations.append(
                StrategyRecommendation(
                    brand_name=context.brand_name,
                    canonical_brand_name=context.canonical_brand_name,
                    source_layer=context.source_layer,
                    source_sheet=context.source_sheet,
                    source_row_number=context.source_row_number,
                    primary_archetype_code=context.primary_archetype_code,
                    primary_archetype_name=context.primary_archetype_name,
                    matched_archetype_codes=context.matched_archetype_codes,
                    matched_archetypes=context.matched_archetypes,
                    strategy_readiness=context.strategy_readiness,
                    priority_rank=priority_rank,
                    priority_bucket=rule.priority_bucket,
                    initiative_code=rule.initiative_code,
                    initiative_name=rule.initiative_name,
                    recommendation_summary=rule.recommendation_summary,
                    rationale=rule.rationale,
                    guardrail=_merge_guardrail(context, rule.guardrail),
                    evidence_fields=rule.evidence_fields,
                    evidence_snapshot=evidence_snapshot,
                    validation_error_count=context.validation_error_count,
                    validation_warning_count=context.validation_warning_count,
                    brand_synthetic_signal_count=context.brand_synthetic_signal_count,
                    reference_source_count=context.reference_source_count,
                    overall_credibility_grade=context.overall_credibility_grade,
                    weakest_provenance_fields=context.weakest_provenance_fields,
                    largest_reconciliation_errors=context.largest_reconciliation_errors,
                    open_issues=context.open_issues,
                    plain_english_caveat=context.plain_english_caveat,
                )
            )
    return recommendations


def generate_strategy_recommendations(
    settings: Settings | None = None,
    *,
    gold_dir: Path | None = None,
    strategy_dir: Path | None = None,
    report_dir: Path | None = None,
) -> StrategyRun:
    """Run the Gold-only strategy pipeline and write local outputs."""

    resolved_settings = settings or Settings()
    resolved_gold_dir = (gold_dir or resolved_settings.data_gold).expanduser().resolve()
    resolved_strategy_dir = (strategy_dir or resolved_settings.strategy_dir).expanduser().resolve()
    resolved_report_dir = (
        (report_dir or (resolved_settings.reports_dir / "strategy")).expanduser().resolve()
    )

    contexts = build_strategy_context(gold_dir=resolved_gold_dir)
    recommendations = build_brand_recommendations(contexts)
    warnings = tuple(
        dict.fromkeys(
            f"{context.canonical_brand_name}: {context.plain_english_caveat}"
            for context in contexts
            if context.strategy_readiness != "ready"
        )
    )
    artifacts = write_strategy_outputs(
        contexts=contexts,
        recommendations=recommendations,
        strategy_dir=resolved_strategy_dir,
        report_dir=resolved_report_dir,
    )
    return StrategyRun(
        contexts=tuple(contexts),
        recommendations=tuple(recommendations),
        warnings=warnings,
        artifacts=artifacts,
    )


def generate_strategy_outputs(
    settings: Settings | None = None,
    *,
    gold_dir: Path | None = None,
    strategy_dir: Path | None = None,
    report_dir: Path | None = None,
) -> StrategyRun:
    """Backward-compatible alias for generating Gold-only strategy outputs."""

    return generate_strategy_recommendations(
        settings=settings,
        gold_dir=gold_dir,
        strategy_dir=strategy_dir,
        report_dir=report_dir,
    )


def write_strategy_outputs(
    *,
    contexts: list[StrategyContext],
    recommendations: list[StrategyRecommendation],
    strategy_dir: Path,
    report_dir: Path,
) -> StrategyArtifacts:
    """Write strategy outputs to local parquet/json and Markdown files."""

    from qsr_audit.strategy.reporting import render_strategy_playbook

    strategy_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    recommendations_parquet_path = strategy_dir / "recommendations.parquet"
    recommendations_json_path = strategy_dir / "recommendations.json"
    report_json_path = report_dir / "recommendations.json"
    playbook_markdown_path = report_dir / "strategy_playbook.md"

    frame = pd.DataFrame([_recommendation_record(row) for row in recommendations])
    frame.to_parquet(recommendations_parquet_path, index=False)
    payload = {
        "contexts": [context.to_dict() for context in contexts],
        "recommendations": [recommendation.to_dict() for recommendation in recommendations],
    }
    for json_path in [recommendations_json_path, report_json_path]:
        json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    playbook_markdown_path.write_text(
        render_strategy_playbook(
            contexts=contexts,
            recommendations=recommendations,
        ),
        encoding="utf-8",
    )

    return StrategyArtifacts(
        recommendations_parquet_path=recommendations_parquet_path,
        recommendations_json_path=recommendations_json_path,
        playbook_markdown_path=playbook_markdown_path,
    )


def _build_operational_signals(
    row: dict[str, Any],
    primary_match: ArchetypeMatch,
) -> tuple[str, ...]:
    signals = [
        primary_match.rationale,
        f"Category: {_clean_text(row.get('category'))}",
        f"Ownership: {_clean_text(row.get('ownership_model'))}",
    ]
    if row.get("average_unit_volume_usd_thousands") is not None:
        signals.append(f"Recorded AUV (k): {row.get('average_unit_volume_usd_thousands')}")
    if row.get("fte_mid") is not None:
        signals.append(f"FTE midpoint: {row.get('fte_mid')}")
    if row.get("margin_mid_pct") is not None:
        signals.append(f"Margin midpoint (%): {row.get('margin_mid_pct')}")
    franchise_share = parse_franchise_share(_clean_text(row.get("ownership_model")))
    if franchise_share is not None:
        signals.append(f"Franchise-like share: {franchise_share:.0%}")
    if is_drive_thru_candidate(row):
        signals.append("Explicit drive-thru or drive-in cues are present.")
    return tuple(signals)


def _build_evidence_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank": _as_int(row.get("rank")),
        "category": _clean_text(row.get("category")),
        "ownership_model": _clean_text(row.get("ownership_model")),
        "central_kitchen_supply_chain_model": _clean_text(
            row.get("central_kitchen_supply_chain_model")
        ),
        "us_store_count_2024": _as_int(row.get("us_store_count_2024")),
        "systemwide_revenue_usd_billions_2024": _as_float(
            row.get("systemwide_revenue_usd_billions_2024")
        ),
        "average_unit_volume_usd_thousands": _as_float(
            row.get("average_unit_volume_usd_thousands")
        ),
        "fte_mid": _as_float(row.get("fte_mid")),
        "margin_mid_pct": _as_float(row.get("margin_mid_pct")),
        "reference_source_count": _as_int(row.get("reference_source_count")) or 0,
        "overall_credibility_grade": _clean_text(row.get("overall_credibility_grade")) or "MISSING",
    }


def _validation_counts_by_brand(validation_flags: pd.DataFrame) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in validation_flags.to_dict(orient="records"):
        brand_name = _clean_text(row.get("brand_name"))
        if not brand_name:
            continue
        bucket = grouped.setdefault(brand_name, {"error": 0, "warning": 0, "issues": ()})
        severity = _clean_text(row.get("severity"))
        if severity == "error":
            bucket["error"] += 1
        elif severity == "warning":
            bucket["warning"] += 1

        if severity in {"error", "warning"}:
            message = _clean_text(row.get("message"))
            if message:
                bucket["issues"] = tuple((*bucket["issues"], message))
    return grouped


def _synthetic_counts_by_brand(
    syntheticness_signals: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in syntheticness_signals.to_dict(orient="records"):
        brand_name = _synthetic_brand_name(row)
        if not brand_name:
            continue
        bucket = grouped.setdefault(brand_name, {"moderate_or_strong": 0, "issues": ()})
        strength = _clean_text(row.get("strength"))
        if strength in {"moderate", "strong"}:
            bucket["moderate_or_strong"] += 1
            issue = _clean_text(row.get("plain_english"))
            if issue:
                bucket["issues"] = tuple((*bucket["issues"], issue))
    return grouped


def _synthetic_brand_name(row: dict[str, Any]) -> str:
    details = _parse_jsonish(row.get("details"))
    brand_name = _clean_text(details.get("brand_name"))
    if brand_name:
        return brand_name
    return ""


def _infer_strategy_readiness(
    *,
    validation_error_count: int,
    reference_source_count: int,
    overall_credibility_grade: str,
) -> str:
    if validation_error_count > 0:
        return "hold"
    if reference_source_count == 0 or overall_credibility_grade in {"MISSING", "D", "F"}:
        return "caution"
    return "ready"


def _build_open_issues(
    *,
    validation_issues: tuple[str, ...],
    reconciliation_warning: Any,
    synthetic_issues: tuple[str, ...],
    weakest_provenance_fields: tuple[str, ...],
) -> tuple[str, ...]:
    issues = list(validation_issues)
    issues.extend(_split_warning_text(reconciliation_warning))
    issues.extend(synthetic_issues)
    if weakest_provenance_fields:
        issues.append("Weak provenance fields: " + ", ".join(weakest_provenance_fields))
    deduped: list[str] = []
    for issue in issues:
        text = _clean_text(issue)
        if text and text not in deduped:
            deduped.append(text)
    return tuple(deduped)


def _build_plain_english_caveat(
    *,
    strategy_readiness: str,
    reference_source_count: int,
    overall_credibility_grade: str,
) -> str:
    if strategy_readiness == "hold":
        return "Gold validation contradictions remain unresolved. Recommendations are directional only until those contradictions are fixed."
    if reference_source_count == 0 or overall_credibility_grade in {"MISSING", "D", "F"}:
        return "Reference coverage or reconciliation credibility is limited. Treat these priorities as exploratory, not execution-ready."
    return "These priorities are Gold-derived analyst guidance. They still do not replace operator judgment or proven ROI evidence."


def _weakest_provenance_fields(row: dict[str, Any]) -> tuple[str, ...]:
    weak_fields: list[str] = []
    for field in RECONCILIATION_FIELDS:
        grade = _clean_text(row.get(f"{field}_credibility_grade"))
        confidence = _as_float(row.get(f"{field}_reference_confidence_score"))
        if grade in {"MISSING", "D", "F"}:
            weak_fields.append(field)
            continue
        if confidence is not None and confidence < 0.7:
            weak_fields.append(field)
    return tuple(dict.fromkeys(weak_fields))


def _largest_reconciliation_errors(row: dict[str, Any]) -> tuple[str, ...]:
    errors: list[tuple[float, str]] = []
    for field in RECONCILIATION_FIELDS:
        relative_error = _as_float(row.get(f"{field}_relative_error"))
        if relative_error is None:
            continue
        errors.append((abs(relative_error), f"{field}: {abs(relative_error):.1%} relative error"))
    errors.sort(key=lambda item: item[0], reverse=True)
    return tuple(label for _, label in errors[:3] if _)


def _merge_guardrail(context: StrategyContext, base_guardrail: str) -> str:
    notes = [base_guardrail]
    if context.strategy_readiness != "ready":
        notes.append(
            f"Strategy readiness is `{context.strategy_readiness}`, so treat this as analyst guidance rather than an execution mandate."
        )
    if context.reference_source_count == 0:
        notes.append("Reference coverage is missing, so external benchmarking remains limited.")
    if context.validation_error_count > 0:
        notes.append(
            "Resolve Gold validation contradictions before operationalizing this recommendation."
        )
    return " ".join(notes)


def _rule_allowed(strategy_readiness: str, minimum_readiness: str) -> bool:
    return READINESS_ORDER[strategy_readiness] >= READINESS_ORDER[minimum_readiness]


def _select_evidence_snapshot(
    evidence_snapshot: dict[str, Any],
    evidence_fields: tuple[str, ...],
) -> dict[str, Any]:
    return {
        field: evidence_snapshot.get(field)
        for field in evidence_fields
        if field in evidence_snapshot
    }


def _recommendation_record(recommendation: StrategyRecommendation) -> dict[str, Any]:
    record = recommendation.to_dict()
    for field in [
        "matched_archetype_codes",
        "matched_archetypes",
        "evidence_fields",
        "weakest_provenance_fields",
        "largest_reconciliation_errors",
        "open_issues",
    ]:
        record[field] = json.dumps(record[field], ensure_ascii=False)
    record["evidence_snapshot"] = json.dumps(record["evidence_snapshot"], ensure_ascii=False)
    return record


def _parse_jsonish(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _split_warning_text(value: Any) -> tuple[str, ...]:
    text = _clean_text(value)
    if not text:
        return ()
    return tuple(part.strip() for part in text.split("|") if part.strip())


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "StrategyArtifacts",
    "StrategyContext",
    "StrategyRecommendation",
    "StrategyRun",
    "build_brand_recommendations",
    "build_strategy_context",
    "generate_strategy_outputs",
    "generate_strategy_recommendations",
    "write_strategy_outputs",
]
