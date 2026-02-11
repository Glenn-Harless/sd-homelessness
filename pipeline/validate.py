"""Data validation checks for SD Homelessness pipeline outputs.

Run after the pipeline to catch data quality issues before publishing.

Usage:
    uv run python -m pipeline.validate
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

AGG = Path(__file__).resolve().parent.parent / "data" / "aggregated"
PROCESSED = Path(__file__).resolve().parent.parent / "data" / "processed"

passed = 0
failed = 0
warnings = 0


def _check(name: str, ok: bool, detail: str = "") -> None:
    global passed, failed
    if ok:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        msg = f"  FAIL  {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)


def _warn(name: str, detail: str) -> None:
    global warnings
    warnings += 1
    print(f"  WARN  {name} — {detail}")


def validate() -> int:
    """Run all validation checks. Returns number of failures."""
    con = duckdb.connect()

    print("=" * 60)
    print("Data Validation")
    print("=" * 60)

    # ── 1. File existence ──
    print("\n-- File existence --")
    processed_path = PROCESSED / "pit_data.parquet"
    _check("pit_data.parquet exists", processed_path.exists())

    expected_aggs = [
        "pit_trends",
        "pit_subpopulations",
        "pit_geography",
        "homelessness_spending",
    ]
    for name in expected_aggs:
        path = AGG / f"{name}.parquet"
        _check(f"{name}.parquet exists", path.exists())

    # ── 2. Row counts (non-empty) ──
    print("\n-- Row counts --")
    for name in expected_aggs:
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        count = con.execute(f"SELECT count(*) FROM '{path}'").fetchone()[0]
        _check(f"{name} has rows", count > 0, f"got {count:,} rows")

    # ── 3. PIT trends integrity ──
    print("\n-- PIT trends integrity --")
    trends_path = AGG / "pit_trends.parquet"
    if trends_path.exists():
        # Check year range
        yr_range = con.execute(f"""
            SELECT MIN(year), MAX(year) FROM '{trends_path}'
        """).fetchone()
        min_yr, max_yr = yr_range
        _check("Min year >= 2007", min_yr is not None and min_yr >= 2007, f"min={min_yr}")
        _check("Max year >= 2023", max_yr is not None and max_yr >= 2023, f"max={max_yr}")

        # Check sheltered + unsheltered ≈ total (within 5% tolerance)
        mismatch = con.execute(f"""
            SELECT year, total, sheltered + unsheltered AS computed_total,
                   ABS(total - (sheltered + unsheltered)) AS diff
            FROM '{trends_path}'
            WHERE total IS NOT NULL
              AND sheltered IS NOT NULL
              AND unsheltered IS NOT NULL
              AND ABS(total - (sheltered + unsheltered)) > total * 0.05
        """).fetchall()
        _check(
            "sheltered + unsheltered ≈ total (within 5%)",
            len(mismatch) == 0,
            f"{len(mismatch)} years with mismatch: {mismatch}" if mismatch else "",
        )

        # Check recent years have reasonable counts (5K-20K for SD)
        recent = con.execute(f"""
            SELECT year, total FROM '{trends_path}'
            WHERE year >= 2020
            ORDER BY year
        """).fetchall()
        for yr, total in recent:
            _check(
                f"PIT {yr} total in range 5K-20K",
                5000 < total < 20000,
                f"got {total:,}",
            )

    # ── 4. Subpopulations ──
    print("\n-- Subpopulations --")
    subpop_path = AGG / "pit_subpopulations.parquet"
    if subpop_path.exists():
        groups = con.execute(f"""
            SELECT DISTINCT group_name FROM '{subpop_path}'
            ORDER BY group_name
        """).fetchall()
        group_list = [r[0] for r in groups]
        _check("Has Chronically Homeless group", "Chronically Homeless" in group_list)
        _check("Has Veterans group", "Veterans" in group_list)

        # Subpopulation counts should be < total for same year
        subpop_check = con.execute(f"""
            SELECT s.year, s.group_name, s.count, t.total
            FROM '{subpop_path}' s
            JOIN '{trends_path}' t ON s.year = t.year
            WHERE s.count > t.total
        """).fetchall()
        _check(
            "Subpopulation counts < total PIT count",
            len(subpop_check) == 0,
            f"{len(subpop_check)} violations" if subpop_check else "",
        )

    # ── 5. Geography ──
    print("\n-- Geography --")
    geo_path = AGG / "pit_geography.parquet"
    if geo_path.exists():
        regions = con.execute(f"""
            SELECT DISTINCT region FROM '{geo_path}'
            ORDER BY region
        """).fetchall()
        region_list = [r[0] for r in regions]
        _check("Has City of San Diego region", "City of San Diego" in region_list,
               f"regions: {region_list}")

        # Sum of regions ≈ total PIT for same year (within 10%)
        geo_totals = con.execute(f"""
            SELECT g.year, SUM(g.total) AS geo_total, t.total AS pit_total
            FROM '{geo_path}' g
            JOIN '{trends_path}' t ON g.year = t.year
            GROUP BY g.year, t.total
            HAVING ABS(SUM(g.total) - t.total) > t.total * 0.10
        """).fetchall()
        _check(
            "Geographic totals ≈ PIT total (within 10%)",
            len(geo_totals) == 0,
            f"{len(geo_totals)} year mismatches: {geo_totals}" if geo_totals else "",
        )

    # ── 6. Spending cross-reference ──
    print("\n-- Spending cross-reference --")
    spending_path = AGG / "homelessness_spending.parquet"
    if spending_path.exists():
        spending_fys = con.execute(f"""
            SELECT fiscal_year, amount FROM '{spending_path}'
            ORDER BY fiscal_year
        """).fetchall()
        _check("Spending has FY2021+", any(fy >= 2021 for fy, _ in spending_fys),
               f"years: {[fy for fy, _ in spending_fys]}")
        for fy, amt in spending_fys:
            _check(
                f"FY{fy} spending > $1M",
                amt > 1_000_000,
                f"${amt:,.0f}",
            )

    # ── 7. NULL rates on critical columns ──
    print("\n-- NULL rates --")
    if processed_path.exists():
        total_rows = con.execute(f"SELECT count(*) FROM '{processed_path}'").fetchone()[0]
        for col in ["year", "total_homeless", "sheltered", "unsheltered"]:
            null_count = con.execute(f"""
                SELECT count(*) FROM '{processed_path}' WHERE {col} IS NULL
            """).fetchone()[0]
            pct = (null_count / total_rows * 100) if total_rows > 0 else 0
            if pct > 10:
                _warn(f"{col} NULL rate", f"{pct:.1f}% ({null_count:,}/{total_rows:,})")
            else:
                _check(f"{col} NULL rate < 10%", True, f"{pct:.1f}%")

    # ── 8. File sizes ──
    print("\n-- File sizes --")
    proc_size = processed_path.stat().st_size / 1024 if processed_path.exists() else 0
    _check("pit_data.parquet < 1MB", proc_size < 1024, f"{proc_size:.1f}KB")

    total_agg = sum(
        (AGG / f"{n}.parquet").stat().st_size for n in expected_aggs
        if (AGG / f"{n}.parquet").exists()
    ) / 1024
    _check("Total aggregated < 1MB", total_agg < 1024, f"{total_agg:.1f}KB")

    # ── Summary ──
    con.close()
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed, {warnings} warnings")
    print("=" * 60)

    return failed


def main() -> None:
    failures = validate()
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
