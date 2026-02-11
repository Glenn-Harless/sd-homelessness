"""FastAPI app — thin wrappers around the shared query layer."""

from __future__ import annotations

from fastapi import FastAPI, Query

from api import queries
from api.models import (
    FilterOptions,
    GeographyCount,
    OverviewResponse,
    PITTrend,
    SpendingTrend,
    Subpopulation,
)

app = FastAPI(
    title="San Diego Homelessness API",
    description=(
        "Query San Diego homelessness data: PIT counts, trends, subpopulations, "
        "geographic breakdowns, and city spending. Data covers 2011-2024 for PIT counts "
        "and FY2021-FY2026 for city spending."
    ),
    version="0.1.0",
)


@app.get("/health")
def health():
    """Debug endpoint — shows data path and file availability."""
    from pathlib import Path
    agg = Path(queries._AGG)
    files = sorted(p.name for p in agg.glob("*.parquet")) if agg.exists() else []
    return {"agg_path": str(agg), "exists": agg.exists(), "files": files}


@app.get("/")
def root():
    return {
        "message": "San Diego Homelessness API",
        "docs": "/docs",
        "endpoints": [
            "/filters", "/overview", "/trends", "/subpopulations",
            "/geography", "/spending",
        ],
    }


@app.get("/filters", response_model=FilterOptions)
def filters():
    """Available years and regions for filtering."""
    return queries.get_filter_options()


@app.get("/overview", response_model=OverviewResponse)
def overview(
    year: int | None = Query(None, description="PIT count year (default: most recent)"),
):
    """Latest PIT count with year-over-year change."""
    return queries.get_overview(year)


@app.get("/trends", response_model=list[PITTrend])
def trends(
    year_min: int = Query(2011, description="Start year"),
    year_max: int = Query(2024, description="End year"),
):
    """Annual PIT totals over time: total, sheltered, unsheltered."""
    return queries.get_pit_trends(year_min, year_max)


@app.get("/subpopulations", response_model=list[Subpopulation])
def subpopulations(
    year_min: int = Query(2011, description="Start year"),
    year_max: int = Query(2024, description="End year"),
    group: str | None = Query(None, description="Filter by group (e.g. 'Veterans', 'Chronically Homeless')"),
):
    """Demographic subgroup counts by year."""
    return queries.get_subpopulations(year_min, year_max, group)


@app.get("/geography", response_model=list[GeographyCount])
def geography(
    year: int | None = Query(None, description="PIT count year (default: most recent)"),
    region: str | None = Query(None, description="Filter by region (e.g. 'City of San Diego')"),
):
    """PIT counts by subregion within San Diego County."""
    return queries.get_geography(year, region)


@app.get("/spending", response_model=list[SpendingTrend])
def spending(
    fy_min: int = Query(2021, description="Start fiscal year"),
    fy_max: int = Query(2026, description="End fiscal year"),
):
    """City Homelessness Strategies & Solutions department spending by fiscal year."""
    return queries.get_spending_trends(fy_min, fy_max)
