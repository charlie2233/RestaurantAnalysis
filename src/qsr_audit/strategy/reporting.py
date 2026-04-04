"""Markdown reporting helpers for the Gold-only strategy layer."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

from qsr_audit.strategy.archetypes import ARCHETYPE_LABELS

if TYPE_CHECKING:
    from qsr_audit.strategy.pipeline import StrategyContext, StrategyRecommendation


def render_strategy_playbook(
    *,
    contexts: list[StrategyContext],
    recommendations: list[StrategyRecommendation],
) -> str:
    """Render the strategy playbook as analyst-facing Markdown."""

    readiness_counts = Counter(context.strategy_readiness for context in contexts)
    archetype_counts = Counter(context.primary_archetype_name for context in contexts)
    recommendations_by_brand = _recommendations_by_brand(recommendations)
    top_themes = Counter(
        recommendation.initiative_name
        for recommendation in recommendations
        if recommendation.priority_rank <= 2 and recommendation.initiative_code != "data_foundation"
    )
    watchlist = [
        context for context in contexts if context.strategy_readiness in {"hold", "caution"}
    ]
    reusable_plays = _reusable_plays(recommendations)

    lines = [
        "# Strategy Playbook",
        "",
        "## Guardrails",
        "",
        "This playbook consumes Gold-layer validated and reconciled outputs only. It is an interpretation layer, not a metric-definition layer.",
        "",
        "- It does not redefine source-of-truth metrics; it interprets validated and reconciled operational patterns.",
        "- Recommendations are rules-based in v1 and should be treated as analyst guidance, not automatic execution mandates.",
        "- Validation caveat: brands with Gold validation errors remain directional only until contradictions are resolved.",
        "- Provenance caveat: weak or missing reference coverage lowers confidence in prioritization, but does not itself imply bad faith or fabrication.",
        "- No speculative ROI claims are made here without explicit provenance.",
        "",
        "## Executive Summary",
        "",
        f"- Brands profiled: `{len(contexts)}`",
        f"- Readiness buckets: `{dict(readiness_counts)}`",
        f"- Brands blocked by validation or weak provenance: `{len(watchlist)}`",
        f"- Top initiative themes: `{dict(top_themes.most_common(3))}`",
        "",
        "## Portfolio Priorities",
        "",
        "- Backend and operations AI comes before risky customer-facing automation.",
        "- Drive-thru voice AI stays in a cautious pilot posture unless the operating model is clearly instrumented and ready.",
        "- Targeted automation comes before any full-robotics push.",
        "- Digital queueing and personalization only rise when format fit is strong.",
        "",
        "## Brand Watchlist",
        "",
    ]

    if watchlist:
        for context in watchlist:
            reason = context.open_issues[0] if context.open_issues else context.plain_english_caveat
            lines.append(
                f"- {context.canonical_brand_name}: `{context.strategy_readiness}` because {reason}"
            )
    else:
        lines.append("- No brands are currently flagged as `hold` or `caution`.")

    lines.extend(["", "## Archetype Coverage", ""])
    for label in ARCHETYPE_LABELS.values():
        lines.append(f"- {label}: `{archetype_counts.get(label, 0)}`")

    lines.extend(["", "## Recommended Plays", ""])
    for recommendation in reusable_plays:
        lines.append(f"### {recommendation.initiative_name}")
        lines.append("")
        lines.append(f"- Summary: {recommendation.recommendation_summary}")
        lines.append(f"- Rationale: {recommendation.rationale}")
        lines.append(f"- Guardrail: {recommendation.guardrail}")
        lines.append("")

    lines.extend(["## Analyst Appendix", ""])
    lines.append(
        "| Brand | Archetype | Readiness | Priority 1 | Priority 2 | Weak provenance | Biggest reconciliation errors |"
    )
    lines.append("|---|---|---|---|---|---|---|")
    for context in contexts:
        brand_recommendations = recommendations_by_brand.get(context.canonical_brand_name, [])
        top_two = [recommendation.initiative_code for recommendation in brand_recommendations[:2]]
        while len(top_two) < 2:
            top_two.append("")
        weakest = ", ".join(context.weakest_provenance_fields) or "None"
        biggest_errors = ", ".join(context.largest_reconciliation_errors) or "None"
        lines.append(
            "| "
            + " | ".join(
                [
                    context.canonical_brand_name,
                    context.primary_archetype_name,
                    context.strategy_readiness,
                    *top_two,
                    weakest,
                    biggest_errors,
                ]
            )
            + " |"
        )

    lines.extend(["", "## Brand-Level Notes", ""])
    for context in contexts:
        lines.append(f"### {context.canonical_brand_name}")
        lines.append("")
        lines.append(f"- Primary archetype: `{context.primary_archetype_name}`")
        lines.append(f"- Matched archetypes: `{list(context.matched_archetypes)}`")
        lines.append(f"- Strategy readiness: `{context.strategy_readiness}`")
        lines.append(f"- Plain-English caveat: {context.plain_english_caveat}")
        lines.append(f"- Operational signals: `{list(context.operational_signals)}`")
        lines.append(
            f"- Weak provenance fields: `{list(context.weakest_provenance_fields) if context.weakest_provenance_fields else ['None']}`"
        )
        lines.append(
            f"- Largest reconciliation errors: `{list(context.largest_reconciliation_errors) if context.largest_reconciliation_errors else ['None']}`"
        )
        lines.append(
            f"- Open issues: `{list(context.open_issues) if context.open_issues else ['None']}`"
        )
        for recommendation in recommendations_by_brand.get(context.canonical_brand_name, []):
            lines.append(
                f"- Priority {recommendation.priority_rank} [{recommendation.priority_bucket}]: `{recommendation.initiative_code}`"
            )
            lines.append(f"  Recommendation: {recommendation.recommendation_summary}")
            lines.append(f"  Rationale: {recommendation.rationale}")
            lines.append(f"  Guardrail: {recommendation.guardrail}")
        lines.append("")

    return "\n".join(lines) + "\n"


def _recommendations_by_brand(
    recommendations: list[StrategyRecommendation],
) -> dict[str, list[StrategyRecommendation]]:
    grouped: dict[str, list[StrategyRecommendation]] = {}
    for recommendation in recommendations:
        grouped.setdefault(recommendation.canonical_brand_name, []).append(recommendation)
    for brand_name in grouped:
        grouped[brand_name] = sorted(grouped[brand_name], key=lambda row: row.priority_rank)
    return grouped


def _reusable_plays(
    recommendations: list[StrategyRecommendation],
) -> list[StrategyRecommendation]:
    plays: list[StrategyRecommendation] = []
    seen_codes: set[str] = set()
    for recommendation in sorted(
        recommendations,
        key=lambda row: (row.priority_rank, row.priority_bucket, row.initiative_name),
    ):
        if recommendation.initiative_code in seen_codes:
            continue
        seen_codes.add(recommendation.initiative_code)
        plays.append(recommendation)
        if len(plays) == 6:
            break
    return plays


__all__ = ["render_strategy_playbook"]
