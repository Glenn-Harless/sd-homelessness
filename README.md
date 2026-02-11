# San Diego Homelessness Trend Analysis

Interactive dashboard tracking homelessness trends in San Diego County through the annual Point-in-Time (PIT) count. Part of a series of San Diego civic data dashboards.

## Dashboard

Five tabs exploring different dimensions of the data:

- **Overview** — Latest PIT count headline, sheltered/unsheltered split, year-over-year change
- **Trends** — Multi-year PIT count with sheltered/unsheltered breakdown
- **Demographics** — Subpopulation trends (chronically homeless, veterans, families)
- **Geography** — Subregional breakdown within San Diego County
- **Spending** — City budget cross-reference: Homelessness Strategies & Solutions dept spending vs. PIT count

## Data Sources

- **HUD Exchange** — CoC CA-601 PIT count data (2011–2024)
- **RTFH** — Regional Task Force on Homelessness annual reports
- **sd-city-budget** — City department spending cross-reference (FY2021–FY2026)

## Quick Start

```bash
uv sync
uv run python -m pipeline.build    # ingest → transform → validate
uv run streamlit run dashboard/app.py
```

## API

```bash
uv run uvicorn api.main:app        # FastAPI at http://localhost:8000/docs
```

## Architecture

```
pipeline/        # ingest.py, transform.py, validate.py, build.py
data/raw/        # Manually compiled CSVs from HUD/RTFH reports
data/aggregated/ # Pre-aggregated parquets for dashboard queries
dashboard/       # Streamlit app
api/             # Shared queries, FastAPI, MCP server
```

All data access uses DuckDB querying parquet files. Dashboard queries return small aggregated DataFrames (~10-50 rows).
