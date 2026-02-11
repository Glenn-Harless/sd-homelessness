# SD Homelessness — Trend Analysis Dashboard

## Project Overview
San Diego homelessness trend analysis. Annual Point-in-Time (PIT) count data from HUD/RTFH (2011-2024), subpopulation demographics, geographic breakdowns, and city budget cross-reference. The 7th project in a series of San Diego civic data dashboards, following the sd-city-budget architecture exactly.

## Architecture — Follow sd-city-budget Pattern

### Project Structure
```
pipeline/       # Data ingestion + transformation
data/raw/       # Manually compiled CSVs (PIT counts, subpopulations, geography)
data/processed/ # Full parquet — commit to git (tiny)
data/aggregated/# Pre-aggregated parquets for dashboard
dashboard/      # Streamlit app
api/            # Shared queries, FastAPI, MCP server
```

### Dashboard Rules
- **Use DuckDB for all data access** — no Polars/pandas for loading full datasets.
- `query()` helper: fresh `duckdb.connect()` per call, returns pandas DataFrame.
- Each query should return small aggregated DataFrames (~10-50 rows).
- `requirements.txt` at project root for Streamlit Cloud (not pyproject.toml).

### Pipeline
- Use DuckDB for transforms (consistent with all prior projects)
- `uv` for dependency management, `pyproject.toml` for project config
- Data source: manually compiled CSVs from HUD Exchange and RTFH reports
- Cross-reference: city budget data from sd-city-budget project

### Deployment
- `.gitignore`: raw data is gitignored except the manually compiled CSVs
- Parquet files are committed directly to git (tiny — <10KB total)

## Data Shape
Annual point-in-time snapshots, not continuous time series:
- PIT totals: year, total, sheltered, unsheltered
- Subpopulations: year, group_name, count (Chronically Homeless, Veterans, Families)
- Geography: year, region, total, sheltered, unsheltered (2023-2024 only)
- Spending: fiscal_year, amount (FY2021-FY2026, from sd-city-budget)

## Key Commands
- `uv run python -m pipeline.build` — run full pipeline (ingest → transform → validate)
- `uv run streamlit run dashboard/app.py` — run dashboard locally
- `uv run uvicorn api.main:app` — run API server
- `uv run python -m pipeline.validate` — run validation only

## Data Notes
- PIT counts are single-night snapshots (January). Actual homelessness is 2-3x higher.
- Geographic data only available for 2023-2024.
- Subpopulation coverage varies by year. Missing values are NULL, not zero.
- The spending cross-reference queries sd-city-budget parquets directly.
- No data fabrication — missing data is noted honestly, not interpolated.

## Related Projects
- `sd-city-budget/` — city budget data, source of the spending cross-reference
- `sd-get-it-done/` — 311 service requests (the first project in this series)
