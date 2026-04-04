"""Brand-archetype helpers for the Gold-only strategy layer."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ArchetypeMatch:
    """A deterministic archetype match derived from Gold-layer fields."""

    archetype_code: str
    archetype_name: str
    confidence: str
    rationale: str
    evidence_fields: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "archetype_code": self.archetype_code,
            "archetype_name": self.archetype_name,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "evidence_fields": list(self.evidence_fields),
        }


ARCHETYPE_LABELS = {
    "premium_service": "high-AUV human-led premium service",
    "beverage_ops": "coffee / beverage operational AI",
    "throughput_model": "extreme menu simplicity / high-throughput model",
    "franchise_standardized": "franchise-heavy standardized model",
    "digital_delivery": "digital-native delivery-heavy model",
    "assembly_automation": "robotics-forward assembly-line model",
}


def match_brand_archetypes(row: dict[str, Any]) -> list[ArchetypeMatch]:
    """Match Gold-layer rows to deterministic strategy archetypes."""

    category = _fold(row.get("category"))
    supply_chain = _fold(row.get("central_kitchen_supply_chain_model"))
    auv = _as_float(row.get("average_unit_volume_usd_thousands"))
    fte_mid = _as_float(row.get("fte_mid"))
    store_count = _as_float(row.get("us_store_count_2024"))
    margin_mid = _as_float(row.get("margin_mid_pct"))
    franchise_share = parse_franchise_share(str(row.get("ownership_model") or ""))

    matches: list[ArchetypeMatch] = []

    premium_service_match = (
        auv is not None
        and auv >= 3000
        and fte_mid is not None
        and fte_mid >= 20
        and margin_mid is not None
        and margin_mid >= 20
        and "无烹饪设备" not in supply_chain
    )
    if premium_service_match:
        matches.append(
            ArchetypeMatch(
                archetype_code="premium_service",
                archetype_name=ARCHETYPE_LABELS["premium_service"],
                confidence="high",
                rationale=(
                    "High AUV, higher staffing, and healthy margin signals imply a service model where labor quality still matters."
                ),
                evidence_fields=(
                    "average_unit_volume_usd_thousands",
                    "fte_mid",
                    "margin_mid_pct",
                    "category",
                ),
            )
        )

    beverage_tokens = ("咖啡", "饮品", "甜甜圈")
    beverage_supply_tokens = ("咖啡机", "糖浆", "门店仅制作饮品", "饮品门店现场制作")
    if any(token in category for token in beverage_tokens) or any(
        token in supply_chain for token in beverage_supply_tokens
    ):
        matches.append(
            ArchetypeMatch(
                archetype_code="beverage_ops",
                archetype_name=ARCHETYPE_LABELS["beverage_ops"],
                confidence="high",
                rationale=(
                    "Beverage-led formats benefit from labor forecasting, queue control, and equipment uptime."
                ),
                evidence_fields=(
                    "category",
                    "central_kitchen_supply_chain_model",
                    "fte_mid",
                ),
            )
        )

    throughput_signals = [
        "无烹饪设备" in supply_chain,
        "极简" in supply_chain,
        "减少等待" in supply_chain,
        "hot-n-ready" in supply_chain or "hotnready" in supply_chain,
        "现场组装" in supply_chain,
        "加热+组装" in supply_chain,
        (fte_mid is not None and fte_mid <= 12 and store_count is not None and store_count >= 2000),
    ]
    if any(throughput_signals):
        matches.append(
            ArchetypeMatch(
                archetype_code="throughput_model",
                archetype_name=ARCHETYPE_LABELS["throughput_model"],
                confidence=(
                    "high" if sum(bool(signal) for signal in throughput_signals) >= 2 else "medium"
                ),
                rationale=(
                    "The operating model appears optimized for throughput, menu discipline, or low-touch assembly."
                ),
                evidence_fields=(
                    "central_kitchen_supply_chain_model",
                    "fte_mid",
                    "us_store_count_2024",
                ),
            )
        )

    if franchise_share is not None and franchise_share >= 0.85:
        matches.append(
            ArchetypeMatch(
                archetype_code="franchise_standardized",
                archetype_name=ARCHETYPE_LABELS["franchise_standardized"],
                confidence="high" if franchise_share >= 0.95 else "medium",
                rationale=(
                    "A heavily franchised system favors standardized back-office tooling before bespoke automation."
                ),
                evidence_fields=("ownership_model",),
            )
        )

    if any(token in category for token in ("披萨", "鸡翅")) or any(
        token in supply_chain for token in ("外卖", "配送", "pickup", "delivery")
    ):
        matches.append(
            ArchetypeMatch(
                archetype_code="digital_delivery",
                archetype_name=ARCHETYPE_LABELS["digital_delivery"],
                confidence="medium",
                rationale=(
                    "The category mix is a reasonable proxy for digital ordering, delivery orchestration, and pickup timing."
                ),
                evidence_fields=("category", "central_kitchen_supply_chain_model"),
            )
        )

    heavy_cook_tokens = ("现场压力油炸", "压力油炸", "现炸", "大锅翻炒")
    assembly_signals = [
        "组装" in supply_chain,
        "预制" in supply_chain,
        "面包" in category,
        "三明治" in category,
        "fast-casual" in category,
        fte_mid is not None and fte_mid <= 15,
    ]
    if any(assembly_signals) and not any(token in supply_chain for token in heavy_cook_tokens):
        matches.append(
            ArchetypeMatch(
                archetype_code="assembly_automation",
                archetype_name=ARCHETYPE_LABELS["assembly_automation"],
                confidence=(
                    "high" if sum(bool(signal) for signal in assembly_signals) >= 2 else "medium"
                ),
                rationale=(
                    "The flow looks standardized enough for targeted assembly aids, but not for blanket robotics claims."
                ),
                evidence_fields=(
                    "category",
                    "central_kitchen_supply_chain_model",
                    "fte_mid",
                ),
            )
        )

    if not matches:
        matches.append(
            ArchetypeMatch(
                archetype_code=(
                    "franchise_standardized"
                    if franchise_share is not None and franchise_share >= 0.7
                    else "premium_service"
                ),
                archetype_name=ARCHETYPE_LABELS[
                    "franchise_standardized"
                    if franchise_share is not None and franchise_share >= 0.7
                    else "premium_service"
                ],
                confidence="low",
                rationale=(
                    "No strong archetype fit was found, so this brand is routed through a conservative default path."
                ),
                evidence_fields=("category", "ownership_model"),
            )
        )

    return matches


def is_drive_thru_candidate(row: dict[str, Any]) -> bool:
    """Return whether a brand looks drive-thru-relevant enough for cautious voice-AI discussion."""

    category = _fold(row.get("category"))
    supply_chain = _fold(row.get("central_kitchen_supply_chain_model"))
    drive_thru_tokens = ("drive-thru", "drive thru", "drive-in", "drive in", "得来速")
    return any(token in category for token in drive_thru_tokens) or any(
        token in supply_chain for token in drive_thru_tokens
    )


def parse_franchise_share(value: str) -> float | None:
    """Parse franchise share from free-form ownership strings."""

    text = value.strip()
    if not text:
        return None

    franchise_match = re.search(r"(\d+(?:\.\d+)?)\s*%加盟", text)
    if franchise_match:
        return float(franchise_match.group(1)) / 100.0

    authorized_match = re.search(r"(\d+(?:\.\d+)?)\s*%授权", text)
    direct_match = re.search(r"(\d+(?:\.\d+)?)\s*%直营", text)
    if authorized_match:
        return float(authorized_match.group(1)) / 100.0
    if direct_match:
        return max(0.0, 1.0 - float(direct_match.group(1)) / 100.0)
    return None


def _fold(value: Any) -> str:
    return str(value or "").strip().casefold()


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "ARCHETYPE_LABELS",
    "ArchetypeMatch",
    "is_drive_thru_candidate",
    "match_brand_archetypes",
    "parse_franchise_share",
]
