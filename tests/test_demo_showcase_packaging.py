"""Regression coverage for the 5-brand demo showcase layer."""

from __future__ import annotations

import html
import subprocess
from pathlib import Path

import pytest
from qsr_audit.cli import app
from typer.testing import CliRunner

from tests.helpers import build_settings
from tests.test_demo_happy_path import (
    _set_settings_env,
    _write_demo_workbook,
    _write_qsr50_reference_csv,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
README_PATH = REPO_ROOT / "README.md"
MAKEFILE_PATH = REPO_ROOT / "Makefile"


def _prepare_demo_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    settings = build_settings(tmp_path)
    workbook_path = settings.data_raw / "demo_fixture.xlsx"
    _write_demo_workbook(workbook_path)
    _write_qsr50_reference_csv(settings.data_reference / "qsr50_reference.csv")
    _set_settings_env(monkeypatch, settings)
    return settings, workbook_path


def _packaging_surface() -> tuple[str, str] | None:
    runner = CliRunner()
    cli_help = runner.invoke(app, ["package-demo", "--help"])
    if cli_help.exit_code == 0:
        return ("cli", "package-demo")

    makefile_text = MAKEFILE_PATH.read_text(encoding="utf-8")
    if "demo-bundle:" in makefile_text:
        return ("make", "demo-bundle")

    return None


def _invoke_packaging_surface() -> subprocess.CompletedProcess[str] | object:
    surface = _packaging_surface()
    if surface is None:
        pytest.xfail("package-demo / demo-bundle are not wired in this branch yet")

    kind, command = surface
    if kind == "cli":
        runner = CliRunner()
        return runner.invoke(app, [command])

    return subprocess.run(
        ["make", command],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _assert_packaging_output(result, settings) -> None:
    if hasattr(result, "exit_code"):
        assert result.exit_code == 0, result.stdout
    else:
        assert result.returncode == 0, result.stdout + result.stderr

    bundle_root = settings.artifacts_dir / "demo_bundle"
    hub_path = settings.reports_dir / "demo" / "index.html"
    assert bundle_root.exists()
    assert hub_path.exists()

    bundle_files = {
        path.relative_to(bundle_root).as_posix()
        for path in bundle_root.rglob("*")
        if path.is_file()
    }
    assert "reports/demo/index.html" in bundle_files
    assert "reports/validation/core_scorecard.html" in bundle_files
    assert "reports/reconciliation/brand_deltas.csv" in bundle_files
    assert "reports/summary/top_risks.md" in bundle_files

    hub_html = html.unescape(hub_path.read_text(encoding="utf-8"))
    for brand in ("Starbucks", "Taco Bell", "Raising Cane's", "Dutch Bros", "Shake Shack"):
        assert brand in hub_html
    assert "publishable" in hub_html.lower()
    assert "advisory" in hub_html.lower()
    assert "blocked" in hub_html.lower()
    assert "core_scorecard.html" in hub_html
    assert "brand_deltas.csv" in hub_html
    assert "top_risks.md" in hub_html

    forbidden_roots = {
        settings.reports_dir / "strategy",
        settings.strategy_dir / "demo_bundle",
        settings.reports_dir / "demo_bundle",
    }
    for forbidden_root in forbidden_roots:
        assert not forbidden_root.exists()


def test_demo_happy_path_keeps_bundle_artifacts_out_of_forbidden_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, _ = _prepare_demo_inputs(tmp_path, monkeypatch)

    runner = CliRunner()
    result = runner.invoke(app, ["demo-happy-path"])

    assert result.exit_code == 0, result.stdout
    assert (settings.reports_dir / "demo" / "index.html").exists()
    assert not (settings.artifacts_dir / "demo_bundle").exists()
    assert not (settings.reports_dir / "demo_bundle").exists()
    assert not (settings.strategy_dir / "demo_bundle").exists()


def test_demo_showcase_packaging_surface_matches_documentation() -> None:
    surface = _packaging_surface()
    if surface is None:
        pytest.xfail("package-demo / demo-bundle are not wired in this branch yet")

    readme = README_PATH.read_text(encoding="utf-8")
    if surface[0] == "cli":
        assert "qsr-audit package-demo" in readme
    if surface[0] == "make":
        assert "make demo-bundle" in readme
    assert "5-brand happy-path demo" in readme.lower()
    assert "what the demo proves" in readme.lower()
    assert "what the demo does not prove" in readme.lower()
    assert "artifacts/demo_bundle/" in readme
    assert "reports/demo/index.html" in readme
    assert "core_scorecard.html" in readme
    assert "brand_deltas.csv" in readme
    assert "top_risks.md" in readme


def test_demo_showcase_packaging_generates_bundle_and_hub_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings, _ = _prepare_demo_inputs(tmp_path, monkeypatch)
    result = _invoke_packaging_surface()
    _assert_packaging_output(result, settings)
