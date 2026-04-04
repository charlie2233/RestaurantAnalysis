"""Gold-only strategy recommendation exports."""

from qsr_audit.strategy.archetypes import (
    ARCHETYPE_LABELS,
    ArchetypeMatch,
    is_drive_thru_candidate,
    match_brand_archetypes,
    parse_franchise_share,
)
from qsr_audit.strategy.pipeline import (
    StrategyArtifacts,
    StrategyContext,
    StrategyRecommendation,
    StrategyRun,
    build_brand_recommendations,
    build_strategy_context,
    generate_strategy_outputs,
    generate_strategy_recommendations,
    write_strategy_outputs,
)
from qsr_audit.strategy.reporting import render_strategy_playbook

__all__ = [
    "ARCHETYPE_LABELS",
    "ArchetypeMatch",
    "StrategyArtifacts",
    "StrategyContext",
    "StrategyRecommendation",
    "StrategyRun",
    "build_brand_recommendations",
    "build_strategy_context",
    "generate_strategy_outputs",
    "generate_strategy_recommendations",
    "is_drive_thru_candidate",
    "match_brand_archetypes",
    "parse_franchise_share",
    "render_strategy_playbook",
    "write_strategy_outputs",
]
