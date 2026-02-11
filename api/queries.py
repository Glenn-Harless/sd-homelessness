"""Shared query layer — all SQL lives here.

Both the FastAPI endpoints and MCP tools call these functions.
Each function creates a fresh DuckDB connection, queries parquet files,
and returns list[dict] (or dict for single-row responses).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# Resolve parquet directory relative to repo root
_ROOT = Path(__file__).resolve().parent.parent
_AGG = str(_ROOT / "data" / "aggregated")


def _q(where: str, condition: str) -> str:
    """Append a condition to a WHERE clause safely."""
    if not where:
        return f"WHERE {condition}"
    return f"{where} AND {condition}"


def _where(
    year_min: int | None,
    year_max: int | None,
) -> str:
    """Build a WHERE clause from optional year range."""
    clauses: list[str] = []
    if year_min is not None:
        clauses.append(f"year >= {int(year_min)}")
    if year_max is not None:
        clauses.append(f"year <= {int(year_max)}")
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


def _run(sql: str) -> list[dict]:
    """Execute SQL and return list of row dicts."""
    con = duckdb.connect()
    df = con.execute(sql).fetchdf()
    con.close()
    return df.to_dict(orient="records")


# ── 1. Filter options ──


def get_filter_options() -> dict:
    """Return available years and regions."""
    con = duckdb.connect()
    years = sorted(
        con.execute(
            f"SELECT DISTINCT year FROM '{_AGG}/pit_trends.parquet' "
            "WHERE year IS NOT NULL ORDER BY year"
        ).fetchdf()["year"].tolist()
    )
    try:
        regions = con.execute(
            f"SELECT DISTINCT region FROM '{_AGG}/pit_geography.parquet' "
            "WHERE region IS NOT NULL ORDER BY region"
        ).fetchdf()["region"].tolist()
    except Exception:
        regions = []
    con.close()
    return {
        "years": [int(y) for y in years],
        "regions": regions,
    }


# ── 2. Overview ──


def get_overview(year: int | None = None) -> dict:
    """Latest PIT count + change from prior year.

    If year is None, uses the most recent year available.
    """
    con = duckdb.connect()

    if year is None:
        year = con.execute(
            f"SELECT MAX(year) FROM '{_AGG}/pit_trends.parquet'"
        ).fetchone()[0]

    current = con.execute(
        f"SELECT total, sheltered, unsheltered FROM '{_AGG}/pit_trends.parquet' "
        f"WHERE year = {int(year)}"
    ).fetchone()

    prior = con.execute(
        f"SELECT total FROM '{_AGG}/pit_trends.parquet' "
        f"WHERE year = {int(year) - 1}"
    ).fetchone()

    con.close()

    if current is None:
        return {"year": year, "total": 0, "sheltered": 0, "unsheltered": 0,
                "prior_year_total": None, "yoy_change": None, "yoy_pct": None}

    total, sheltered, unsheltered = current
    result = {
        "year": int(year),
        "total": int(total),
        "sheltered": int(sheltered),
        "unsheltered": int(unsheltered),
        "prior_year_total": None,
        "yoy_change": None,
        "yoy_pct": None,
    }

    if prior:
        prior_total = int(prior[0])
        result["prior_year_total"] = prior_total
        result["yoy_change"] = int(total) - prior_total
        result["yoy_pct"] = round((int(total) - prior_total) / prior_total * 100, 1)

    return result


# ── 3. PIT Trends ──


def get_pit_trends(
    year_min: int = 2011,
    year_max: int = 2024,
) -> list[dict]:
    """Annual PIT totals over time: year, total, sheltered, unsheltered."""
    w = _where(year_min, year_max)
    return _run(
        f"SELECT year, total, sheltered, unsheltered "
        f"FROM '{_AGG}/pit_trends.parquet' {w} "
        f"ORDER BY year"
    )


# ── 4. Subpopulations ──


def get_subpopulations(
    year_min: int = 2011,
    year_max: int = 2024,
    group: str | None = None,
) -> list[dict]:
    """Demographic subgroup counts by year."""
    w = _where(year_min, year_max)
    if group:
        safe_group = group.replace("'", "''")
        w = _q(w, f"group_name = '{safe_group}'")
    return _run(
        f"SELECT year, group_name, count "
        f"FROM '{_AGG}/pit_subpopulations.parquet' {w} "
        f"ORDER BY year, group_name"
    )


# ── 5. Geography ──


def get_geography(
    year: int | None = None,
    region: str | None = None,
) -> list[dict]:
    """Subregional PIT counts. Defaults to most recent year."""
    con = duckdb.connect()

    if year is None:
        year = con.execute(
            f"SELECT MAX(year) FROM '{_AGG}/pit_geography.parquet'"
        ).fetchone()[0]

    w = f"WHERE year = {int(year)}"
    if region:
        safe_region = region.replace("'", "''")
        w += f" AND region = '{safe_region}'"

    df = con.execute(
        f"SELECT year, region, total, sheltered, unsheltered "
        f"FROM '{_AGG}/pit_geography.parquet' {w} "
        f"ORDER BY total DESC"
    ).fetchdf()
    con.close()
    return df.to_dict(orient="records")


# ── 6. Spending trends ──


def get_spending_trends(
    fy_min: int = 2021,
    fy_max: int = 2026,
) -> list[dict]:
    """City homelessness department spending by fiscal year."""
    return _run(
        f"SELECT fiscal_year, amount "
        f"FROM '{_AGG}/homelessness_spending.parquet' "
        f"WHERE fiscal_year >= {int(fy_min)} AND fiscal_year <= {int(fy_max)} "
        f"ORDER BY fiscal_year"
    )
