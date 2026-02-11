"""Clean, enrich, and aggregate San Diego homelessness data using DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
AGGREGATED_DIR = Path(__file__).resolve().parent.parent / "data" / "aggregated"
DB_PATH = Path(__file__).resolve().parent.parent / "db" / "homelessness.duckdb"

# Cross-reference budget parquet
BUDGET_PARQUET = Path.home() / "dev-brain" / "sd-city-budget" / "data" / "aggregated" / "dept_budget_trends.parquet"


def transform(*, db_path: Path | None = None) -> None:
    """Load raw CSVs, build processed + aggregated parquets."""
    db = db_path or DB_PATH
    db.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AGGREGATED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db))

    # ── Load raw CSVs ──
    _load_raw_tables(con)

    # ── Export processed parquet (full PIT data) ──
    _export_processed(con)

    # ── Build aggregations ──
    _build_aggregations(con)

    # ── Build spending cross-reference ──
    _build_spending_crossref(con)

    con.close()
    print("Transform complete.")


def _load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Load the raw CSVs into DuckDB tables."""
    csv_files = {
        "raw_pit_counts": RAW_DIR / "pit_counts.csv",
        "raw_pit_subpopulations": RAW_DIR / "pit_subpopulations.csv",
        "raw_pit_geography": RAW_DIR / "pit_geography.csv",
    }
    for name, path in csv_files.items():
        if not path.exists():
            print(f"  [warn] {path.name} not found, skipping")
            con.execute(f"DROP TABLE IF EXISTS {name}")
            con.execute(f"CREATE TABLE {name} (dummy INTEGER)")
            continue
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"""
            CREATE TABLE {name} AS
            SELECT * FROM read_csv('{path}', header=true, ignore_errors=true)
        """)
        count = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        print(f"  Loaded {name}: {count:,} rows")


def _export_processed(con: duckdb.DuckDBPyConnection) -> None:
    """Export the full processed PIT dataset to parquet."""
    processed_path = PROCESSED_DIR / "pit_data.parquet"
    con.execute(f"""
        COPY (
            SELECT
                TRY_CAST(year AS INTEGER) AS year,
                TRY_CAST(total_homeless AS INTEGER) AS total_homeless,
                TRY_CAST(sheltered AS INTEGER) AS sheltered,
                TRY_CAST(unsheltered AS INTEGER) AS unsheltered,
                TRY_CAST(chronically_homeless AS INTEGER) AS chronically_homeless,
                TRY_CAST(veterans AS INTEGER) AS veterans,
                TRY_CAST(families_persons AS INTEGER) AS families_persons,
                TRY_CAST(youth_under25 AS INTEGER) AS youth_under25,
                TRY_CAST(first_time_homeless AS INTEGER) AS first_time_homeless
            FROM raw_pit_counts
            WHERE TRY_CAST(year AS INTEGER) IS NOT NULL
            ORDER BY year
        ) TO '{processed_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    size_kb = processed_path.stat().st_size / 1024
    print(f"  Exported processed data -> {processed_path} ({size_kb:.1f} KB)")


def _build_aggregations(con: duckdb.DuckDBPyConnection) -> None:
    """Build pre-computed aggregation parquet files for the dashboard."""

    # 1) PIT trends — annual totals over time
    con.execute(f"""
        COPY (
            SELECT
                TRY_CAST(year AS INTEGER) AS year,
                TRY_CAST(total_homeless AS INTEGER) AS total,
                TRY_CAST(sheltered AS INTEGER) AS sheltered,
                TRY_CAST(unsheltered AS INTEGER) AS unsheltered
            FROM raw_pit_counts
            WHERE TRY_CAST(year AS INTEGER) IS NOT NULL
            ORDER BY year
        ) TO '{AGGREGATED_DIR}/pit_trends.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print("  [agg] pit_trends")

    # 2) PIT subpopulations — demographic subgroups by year
    con.execute(f"""
        COPY (
            SELECT
                TRY_CAST(year AS INTEGER) AS year,
                group_name,
                TRY_CAST(count AS INTEGER) AS count
            FROM raw_pit_subpopulations
            WHERE TRY_CAST(year AS INTEGER) IS NOT NULL
              AND TRY_CAST(count AS INTEGER) IS NOT NULL
            ORDER BY year, group_name
        ) TO '{AGGREGATED_DIR}/pit_subpopulations.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print("  [agg] pit_subpopulations")

    # 3) PIT geography — subregional breakdowns
    con.execute(f"""
        COPY (
            SELECT
                TRY_CAST(year AS INTEGER) AS year,
                region,
                TRY_CAST(total AS INTEGER) AS total,
                TRY_CAST(sheltered AS INTEGER) AS sheltered,
                TRY_CAST(unsheltered AS INTEGER) AS unsheltered
            FROM raw_pit_geography
            WHERE TRY_CAST(year AS INTEGER) IS NOT NULL
            ORDER BY year, region
        ) TO '{AGGREGATED_DIR}/pit_geography.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print("  [agg] pit_geography")


def _build_spending_crossref(con: duckdb.DuckDBPyConnection) -> None:
    """Extract homelessness spending from the city budget project."""
    if not BUDGET_PARQUET.exists():
        print("  [skip] homelessness_spending — budget parquet not found")
        return

    con.execute(f"""
        COPY (
            SELECT
                fiscal_year,
                SUM(amount) AS amount
            FROM '{BUDGET_PARQUET}'
            WHERE dept_name = 'Homelessness Strategies & Solutions'
              AND budget_cycle = 'adopted'
              AND revenue_or_expense = 'Expense'
              AND source = 'budget'
            GROUP BY fiscal_year
            ORDER BY fiscal_year
        ) TO '{AGGREGATED_DIR}/homelessness_spending.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print("  [agg] homelessness_spending")


if __name__ == "__main__":
    transform()
