"""Workbook validation orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from qsr_audit.config import Settings
from qsr_audit.contracts.workbook import SILVER_OUTPUT_FILES
from qsr_audit.ingest import load_workbook_sheets
from qsr_audit.normalize import (
    normalize_ai_strategy_registry,
    normalize_core_brand_metrics,
    normalize_data_notes_and_key_findings,
)
from qsr_audit.validate.invariants import evaluate_invariants
from qsr_audit.validate.models import ValidationArtifacts, ValidationFinding, ValidationRun
from qsr_audit.validate.reporting import write_validation_outputs
from qsr_audit.validate.schemas import build_schema_bundle, validate_schema


@dataclass(frozen=True)
class ValidationTables:
    """Normalized tables used during validation."""

    core_brand_metrics: pd.DataFrame
    ai_strategy_registry: pd.DataFrame
    data_notes: pd.DataFrame
    key_findings: pd.DataFrame


def validate_workbook(
    input_path: Path,
    settings: Settings | None = None,
    *,
    tolerance_auv: float = 0.05,
    output_dir: Path | None = None,
    gold_dir: Path | None = None,
) -> ValidationRun:
    """Validate workbook outputs from either a raw workbook or a Silver directory."""

    resolved_source = input_path.expanduser().resolve()
    source_kind, tables = load_validation_tables(resolved_source)

    resolved_output_dir = output_dir
    resolved_gold_dir = gold_dir
    if settings is not None:
        resolved_output_dir = resolved_output_dir or settings.reports_dir / "validation"
        resolved_gold_dir = resolved_gold_dir or settings.data_gold
    resolved_output_dir = resolved_output_dir or Path("reports/validation")
    resolved_gold_dir = resolved_gold_dir or Path("data/gold")

    schema_bundle = build_schema_bundle()
    findings: list[ValidationFinding] = []
    findings.extend(validate_schema("core_brand_metrics", tables.core_brand_metrics, schema_bundle))
    findings.extend(
        validate_schema("ai_strategy_registry", tables.ai_strategy_registry, schema_bundle)
    )
    findings.extend(validate_schema("data_notes", tables.data_notes, schema_bundle))
    findings.extend(validate_schema("key_findings", tables.key_findings, schema_bundle))
    findings.extend(
        evaluate_invariants(
            tables.core_brand_metrics,
            tables.ai_strategy_registry,
            tolerance_auv=tolerance_auv,
        )
    )

    artifacts = ValidationArtifacts(
        summary_markdown=resolved_output_dir / "validation_summary.md",
        results_json=resolved_output_dir / "validation_results.json",
        flags_parquet=resolved_gold_dir / "validation_flags.parquet",
    )
    run = ValidationRun(
        source_path=resolved_source,
        source_kind=source_kind,
        findings=tuple(findings),
        artifacts=artifacts,
    )
    artifacts = write_validation_outputs(
        run, output_dir=resolved_output_dir, gold_dir=resolved_gold_dir
    )
    return ValidationRun(
        source_path=resolved_source,
        source_kind=source_kind,
        findings=tuple(findings),
        artifacts=artifacts,
    )


def load_validation_tables(source_path: Path) -> tuple[str, ValidationTables]:
    """Load normalized validation tables from either raw workbook or Silver directory."""

    if source_path.is_file():
        if source_path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
            raw_sheets = load_workbook_sheets(source_path)
            core_brand_metrics = normalize_core_brand_metrics(raw_sheets["QSR Top30 核心数据"])
            ai_strategy_registry = normalize_ai_strategy_registry(raw_sheets["AI策略与落地效果"])
            data_notes, key_findings = normalize_data_notes_and_key_findings(
                raw_sheets["数据说明与来源"]
            )
            return (
                "raw_workbook",
                ValidationTables(
                    core_brand_metrics=core_brand_metrics,
                    ai_strategy_registry=ai_strategy_registry,
                    data_notes=data_notes,
                    key_findings=key_findings,
                ),
            )

        if source_path.suffix.lower() == ".parquet":
            silver_dir = _resolve_silver_dir(source_path.parent)
            return (
                "silver_parquet",
                ValidationTables(
                    core_brand_metrics=pd.read_parquet(
                        silver_dir / SILVER_OUTPUT_FILES["core_brand_metrics"]
                    ),
                    ai_strategy_registry=pd.read_parquet(
                        silver_dir / SILVER_OUTPUT_FILES["ai_strategy_registry"]
                    ),
                    data_notes=pd.read_parquet(silver_dir / SILVER_OUTPUT_FILES["data_notes"]),
                    key_findings=pd.read_parquet(silver_dir / SILVER_OUTPUT_FILES["key_findings"]),
                ),
            )

        raise ValueError(f"Unsupported validation input: {source_path}")

    if source_path.is_dir():
        silver_dir = _resolve_silver_dir(source_path)
        return (
            "silver_directory",
            ValidationTables(
                core_brand_metrics=pd.read_parquet(
                    silver_dir / SILVER_OUTPUT_FILES["core_brand_metrics"]
                ),
                ai_strategy_registry=pd.read_parquet(
                    silver_dir / SILVER_OUTPUT_FILES["ai_strategy_registry"]
                ),
                data_notes=pd.read_parquet(silver_dir / SILVER_OUTPUT_FILES["data_notes"]),
                key_findings=pd.read_parquet(silver_dir / SILVER_OUTPUT_FILES["key_findings"]),
            ),
        )

    raise FileNotFoundError(f"Validation source does not exist: {source_path}")


def _resolve_silver_dir(source_path: Path) -> Path:
    if all((source_path / filename).exists() for filename in SILVER_OUTPUT_FILES.values()):
        return source_path

    nested = source_path / "silver"
    if all((nested / filename).exists() for filename in SILVER_OUTPUT_FILES.values()):
        return nested

    missing = [
        filename
        for filename in SILVER_OUTPUT_FILES.values()
        if not (source_path / filename).exists()
    ]
    if nested.exists():
        missing = [
            filename
            for filename in SILVER_OUTPUT_FILES.values()
            if not (nested / filename).exists()
        ]

    raise FileNotFoundError(
        f"Could not locate Silver outputs under {source_path} or {nested}. Missing: {missing}"
    )
