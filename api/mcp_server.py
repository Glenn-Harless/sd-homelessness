"""MCP server for San Diego homelessness data.

Exposes 6 tools that let Claude query homelessness parquet files directly.
Uses FastMCP (v2) with stdio transport — spawned by Claude Code as a subprocess.
"""

from __future__ import annotations

from fastmcp import FastMCP

from api import queries

mcp = FastMCP(
    "San Diego Homelessness",
    instructions=(
        "San Diego homelessness data covering 2011-2024 PIT counts and FY2021-FY2026 "
        "city spending. Call get_filter_options first to see available years and regions. "
        "PIT counts are annual point-in-time snapshots from January."
    ),
)


@mcp.tool()
def get_filter_options() -> dict:
    """Get available filter values: years and regions.

    Call this first to see what values are valid for other tools.
    """
    return queries.get_filter_options()


@mcp.tool()
def get_overview(year: int | None = None) -> dict:
    """Get PIT count overview for a given year: total, sheltered, unsheltered, YoY change.

    If year is omitted, returns the most recent year. Counts are people.
    """
    return queries.get_overview(year)


@mcp.tool()
def get_pit_trends(
    year_min: int = 2011,
    year_max: int = 2024,
) -> list[dict]:
    """Get annual PIT count trends: total, sheltered, unsheltered by year.

    Returns a time series suitable for charting. Counts are people.
    """
    return queries.get_pit_trends(year_min, year_max)


@mcp.tool()
def get_subpopulations(
    year_min: int = 2011,
    year_max: int = 2024,
    group: str | None = None,
) -> list[dict]:
    """Get demographic subgroup counts: Chronically Homeless, Veterans, Families.

    Optionally filter by group name. Returns year, group_name, and count.
    """
    return queries.get_subpopulations(year_min, year_max, group)


@mcp.tool()
def get_geography(
    year: int | None = None,
    region: str | None = None,
) -> list[dict]:
    """Get PIT counts by subregion within San Diego County.

    Geographic data available for 2023-2024. Returns year, region, total,
    sheltered, unsheltered.
    """
    return queries.get_geography(year, region)


@mcp.tool()
def get_spending_trends(
    fy_min: int = 2021,
    fy_max: int = 2026,
) -> list[dict]:
    """Get city Homelessness Strategies & Solutions department spending.

    Returns fiscal_year and amount (USD). Department was created in FY2021.
    This is the city's dedicated homelessness department budget only —
    does not include county, state, or federal spending.
    """
    return queries.get_spending_trends(fy_min, fy_max)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
