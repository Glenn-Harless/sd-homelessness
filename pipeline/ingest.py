"""Download/verify raw data for SD homelessness analysis.

Unlike prior projects, there's no single clean CSV endpoint.
PIT count data is compiled manually from HUD Exchange and RTFH reports.
This module verifies the manually-created CSVs exist and are readable,
and copies cross-reference budget data from the sd-city-budget project.
"""

from __future__ import annotations

from pathlib import Path

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Cross-reference: homelessness spending from city budget project
BUDGET_PARQUET = Path.home() / "dev-brain" / "sd-city-budget" / "data" / "aggregated" / "dept_budget_trends.parquet"

EXPECTED_CSVS = [
    "pit_counts.csv",
    "pit_subpopulations.csv",
    "pit_geography.csv",
]


def verify_raw_data() -> list[Path]:
    """Verify that manually-compiled CSVs exist and are non-empty."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for name in EXPECTED_CSVS:
        path = RAW_DIR / name
        if path.exists() and path.stat().st_size > 0:
            print(f"  [ok] {name} ({path.stat().st_size:,} bytes)")
            paths.append(path)
        else:
            print(f"  [missing] {name} — create manually from RTFH/HUD reports")
    return paths


def check_budget_crossref() -> bool:
    """Check if cross-reference budget parquet is available."""
    if BUDGET_PARQUET.exists():
        size = BUDGET_PARQUET.stat().st_size
        print(f"  [ok] budget cross-reference ({size:,} bytes)")
        return True
    print("  [warn] sd-city-budget parquet not found — spending tab will be empty")
    return False


def ingest(*, force: bool = False) -> list[Path]:
    """Verify all data sources. Returns list of available file paths."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    paths = verify_raw_data()
    check_budget_crossref()
    return paths


if __name__ == "__main__":
    ingest()
