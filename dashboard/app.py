"""Streamlit dashboard for San Diego homelessness trend analysis."""

from __future__ import annotations

from pathlib import Path

import duckdb
import plotly.graph_objects as go
import streamlit as st

# ── Parquet paths (relative to repo root, where Streamlit Cloud runs) ──
_AGG = "data/aggregated"

# Resolve paths for local dev (running from project root or dashboard/)
_root = Path(__file__).resolve().parent.parent
if (_root / _AGG).exists():
    _AGG = str(_root / _AGG)

st.set_page_config(
    page_title="San Diego Homelessness",
    page_icon="\U0001f3e0",
    layout="wide",
)

CHART_COLOR = "#83c9ff"
CHART_COLOR_2 = "#2a6496"


def query(sql: str, params: list | None = None):
    """Run SQL against parquet files and return a pandas DataFrame."""
    con = duckdb.connect()
    return con.execute(sql, params or []).fetchdf()


# ── Sidebar filters ──
st.sidebar.title("Filters")


@st.cache_data(ttl=3600)
def _sidebar_options():
    years = sorted(query(f"""
        SELECT DISTINCT year FROM '{_AGG}/pit_trends.parquet'
        WHERE year IS NOT NULL
        ORDER BY year
    """)["year"].tolist())

    # Regions (if geographic data exists)
    try:
        regions = query(f"""
            SELECT DISTINCT region FROM '{_AGG}/pit_geography.parquet'
            WHERE region IS NOT NULL
            ORDER BY region
        """)["region"].tolist()
    except Exception:
        regions = []

    return years, regions


all_years, all_regions = _sidebar_options()

if all_years:
    year_range = st.sidebar.slider(
        "Year Range",
        min_value=int(min(all_years)),
        max_value=int(max(all_years)),
        value=(int(min(all_years)), int(max(all_years))),
        help="PIT counts are conducted annually in January.",
    )
else:
    year_range = (2011, 2024)

if all_regions:
    selected_regions = st.sidebar.multiselect(
        "Region",
        options=all_regions,
        default=None,
        placeholder="All regions",
        help="Subregions within San Diego County. Geographic data is available for 2023-2024.",
    )
else:
    selected_regions = []

st.sidebar.caption(
    "Data from the annual Point-in-Time (PIT) count — a federally mandated "
    "single-night census of people experiencing homelessness, conducted each January."
)

# ── Header ──
st.title("San Diego Homelessness")
st.markdown(
    "Tracking homelessness trends in San Diego County through the annual Point-in-Time (PIT) count. "
    "Data covers **2011-2024** from HUD and the Regional Task Force on Homelessness (RTFH). "
    "The PIT count is a single-night snapshot conducted each January — it represents a lower bound "
    "on the total number of people experiencing homelessness over the course of a year."
)

# ==================================================================
# Tab layout
# ==================================================================
tab_names = ["Overview", "Trends", "Demographics", "Geography", "Spending"]
tab_overview, tab_trends, tab_demographics, tab_geography, tab_spending = st.tabs(tab_names)

# ── TAB 1: Overview ──
with tab_overview:
    # Latest year data
    latest = query(f"""
        SELECT * FROM '{_AGG}/pit_trends.parquet'
        WHERE year = (SELECT MAX(year) FROM '{_AGG}/pit_trends.parquet')
    """)
    prior = query(f"""
        SELECT * FROM '{_AGG}/pit_trends.parquet'
        WHERE year = (SELECT MAX(year) - 1 FROM '{_AGG}/pit_trends.parquet')
    """)

    if not latest.empty:
        latest_year = int(latest["year"].iloc[0])
        latest_total = int(latest["total"].iloc[0])
        latest_sheltered = int(latest["sheltered"].iloc[0])
        latest_unsheltered = int(latest["unsheltered"].iloc[0])

        if not prior.empty:
            prior_total = int(prior["total"].iloc[0])
            yoy_change = latest_total - prior_total
            yoy_pct = yoy_change / prior_total * 100
            delta_str = f"{yoy_change:+,} ({yoy_pct:+.1f}%)"
        else:
            delta_str = None

        st.subheader(f"{latest_year} Point-in-Time Count")

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Homeless", f"{latest_total:,}", delta=delta_str, delta_color="inverse")
        col2.metric("Sheltered", f"{latest_sheltered:,}")
        col3.metric("Unsheltered", f"{latest_unsheltered:,}")

        # Sheltered vs unsheltered donut
        unsheltered_pct = latest_unsheltered / latest_total * 100
        sheltered_pct = latest_sheltered / latest_total * 100

        col_chart, col_context = st.columns([1, 1])

        with col_chart:
            fig = go.Figure(go.Pie(
                labels=["Unsheltered", "Sheltered"],
                values=[latest_unsheltered, latest_sheltered],
                hole=0.5,
                marker=dict(colors=[CHART_COLOR, CHART_COLOR_2]),
                textinfo="label+percent",
                textfont=dict(size=14),
            ))
            fig.update_layout(
                height=350,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

        with col_context:
            st.markdown(f"""
**{latest_year} Snapshot**

- **{unsheltered_pct:.0f}%** of people experiencing homelessness were **unsheltered** — sleeping in
  vehicles, tents, or on the street.
- **{sheltered_pct:.0f}%** were in **emergency shelters, transitional housing, or safe havens**.
- The PIT count is a single-night snapshot. The actual number of people who experience
  homelessness over the course of a year is estimated to be **2-3x higher**.
""")

        # Mini trend sparkline
        st.subheader("Trend at a Glance")
        spark = query(f"""
            SELECT year AS "Year", total AS "Total Homeless"
            FROM '{_AGG}/pit_trends.parquet'
            ORDER BY year
        """)
        if not spark.empty:
            spark["Year"] = spark["Year"].astype(str)
            st.line_chart(spark.set_index("Year"), color=CHART_COLOR)

# ── TAB 2: Trends ──
with tab_trends:
    st.subheader("PIT Count Over Time")

    trends = query(f"""
        SELECT year, total, sheltered, unsheltered
        FROM '{_AGG}/pit_trends.parquet'
        WHERE year BETWEEN {year_range[0]} AND {year_range[1]}
        ORDER BY year
    """)

    if not trends.empty:
        # Total trend with sheltered/unsheltered breakdown
        chart_data = trends.copy()
        chart_data = chart_data.rename(columns={
            "year": "Year",
            "sheltered": "Sheltered",
            "unsheltered": "Unsheltered",
        })[["Year", "Sheltered", "Unsheltered"]]
        chart_data["Year"] = chart_data["Year"].astype(str)
        st.bar_chart(chart_data.set_index("Year"), color=[CHART_COLOR, CHART_COLOR_2])
        st.caption("Stacked bar shows sheltered (light) and unsheltered (dark) breakdown each year.")

        # Total line chart
        st.subheader("Total PIT Count Trend")
        total_data = trends[["year", "total"]].copy()
        total_data = total_data.rename(columns={"year": "Year", "total": "Total"})
        total_data["Year"] = total_data["Year"].astype(str)
        st.line_chart(total_data.set_index("Year"), color=CHART_COLOR)

        # Year-over-year changes
        st.subheader("Year-Over-Year Change")
        yoy = trends.copy()
        yoy["change"] = yoy["total"].diff()
        yoy["pct_change"] = yoy["total"].pct_change() * 100
        yoy = yoy.dropna(subset=["change"])
        yoy = yoy.sort_values("year", ascending=False)
        yoy_display = yoy[["year", "total", "change", "pct_change"]].copy()
        yoy_display.columns = ["Year", "Total", "Change", "% Change"]
        yoy_display["Change"] = yoy_display["Change"].apply(lambda x: f"{x:+,.0f}")
        yoy_display["% Change"] = yoy_display["% Change"].apply(lambda x: f"{x:+.1f}%")
        yoy_display["Total"] = yoy_display["Total"].apply(lambda x: f"{x:,}")
        st.dataframe(yoy_display, use_container_width=True, hide_index=True)
    else:
        st.info("No PIT count data available for the selected year range.")

# ── TAB 3: Demographics ──
with tab_demographics:
    st.subheader("Subpopulation Trends")
    st.caption(
        "Key subgroups within the homeless population. Individuals may belong to multiple "
        "subgroups. Chronically homeless: individuals with a disability who have been "
        "continuously homeless for 1+ year or 4+ times in 3 years."
    )

    subpop = query(f"""
        SELECT year, group_name, count
        FROM '{_AGG}/pit_subpopulations.parquet'
        WHERE year BETWEEN {year_range[0]} AND {year_range[1]}
        ORDER BY year, group_name
    """)

    if not subpop.empty:
        # Pivoted line chart
        pivot = subpop.pivot_table(
            index="year", columns="group_name", values="count", fill_value=0
        )
        pivot.index = pivot.index.astype(str)
        st.line_chart(pivot)

        # Latest year breakdown
        latest_year_subpop = subpop["year"].max()
        latest_subpop = subpop[subpop["year"] == latest_year_subpop].copy()
        latest_subpop = latest_subpop.rename(columns={
            "group_name": "Subpopulation",
            "count": "Count",
        })[["Subpopulation", "Count"]]
        latest_subpop = latest_subpop.sort_values("Count", ascending=False)

        st.subheader(f"{int(latest_year_subpop)} Subpopulation Breakdown")
        st.bar_chart(
            latest_subpop.set_index("Subpopulation"),
            horizontal=True,
            color=CHART_COLOR,
        )

        # As percentage of total
        total_for_year = query(f"""
            SELECT total FROM '{_AGG}/pit_trends.parquet'
            WHERE year = {int(latest_year_subpop)}
        """)
        if not total_for_year.empty:
            total = int(total_for_year["total"].iloc[0])
            pct_data = latest_subpop.copy()
            pct_data["% of Total"] = pct_data["Count"].apply(
                lambda x: f"{x / total * 100:.1f}%"
            )
            pct_data["Count"] = pct_data["Count"].apply(lambda x: f"{x:,}")
            st.dataframe(pct_data, use_container_width=True, hide_index=True)
    else:
        st.info("No subpopulation data available for the selected year range.")

# ── TAB 4: Geography ──
with tab_geography:
    st.subheader("Homelessness by Subregion")
    st.caption(
        "Geographic breakdown of PIT counts within San Diego County. "
        "Data is available for 2023-2024."
    )

    geo_where = f"WHERE year BETWEEN {year_range[0]} AND {year_range[1]}"
    if selected_regions:
        escaped = ", ".join(f"'{r.replace(chr(39), chr(39)*2)}'" for r in selected_regions)
        geo_where += f" AND region IN ({escaped})"

    geo = query(f"""
        SELECT year, region, total, sheltered, unsheltered
        FROM '{_AGG}/pit_geography.parquet'
        {geo_where}
        ORDER BY year, total DESC
    """)

    if not geo.empty:
        # Latest year geographic bar chart
        latest_geo_year = int(geo["year"].max())
        latest_geo = geo[geo["year"] == latest_geo_year].copy()

        st.subheader(f"{latest_geo_year} PIT Count by Region")
        chart_geo = latest_geo[["region", "sheltered", "unsheltered"]].copy()
        chart_geo = chart_geo.rename(columns={
            "region": "Region",
            "sheltered": "Sheltered",
            "unsheltered": "Unsheltered",
        })
        st.bar_chart(
            chart_geo.set_index("Region"),
            horizontal=True,
            color=[CHART_COLOR, CHART_COLOR_2],
        )

        # Detail table
        detail_geo = latest_geo.rename(columns={
            "year": "Year",
            "region": "Region",
            "total": "Total",
            "sheltered": "Sheltered",
            "unsheltered": "Unsheltered",
        })
        detail_geo["Unsheltered %"] = (
            detail_geo["Unsheltered"] / detail_geo["Total"] * 100
        ).apply(lambda x: f"{x:.0f}%")
        detail_geo["Total"] = detail_geo["Total"].apply(lambda x: f"{x:,}")
        detail_geo["Sheltered"] = detail_geo["Sheltered"].apply(lambda x: f"{x:,}")
        detail_geo["Unsheltered"] = detail_geo["Unsheltered"].apply(lambda x: f"{x:,}")
        st.dataframe(
            detail_geo[["Region", "Total", "Sheltered", "Unsheltered", "Unsheltered %"]],
            use_container_width=True,
            hide_index=True,
        )

        # YoY comparison if multiple years
        available_years = sorted(geo["year"].unique())
        if len(available_years) > 1:
            st.subheader("Year-Over-Year by Region")
            for region in sorted(latest_geo["region"].unique()):
                region_data = geo[geo["region"] == region].sort_values("year")
                if len(region_data) > 1:
                    first = region_data.iloc[0]
                    last = region_data.iloc[-1]
                    change = int(last["total"]) - int(first["total"])
                    pct = change / int(first["total"]) * 100 if first["total"] else 0
                    # Green = decrease (better), Red = increase (worse)
                    color = "red" if change > 0 else "green"
                    st.markdown(
                        f"**{region}**: {int(first['total']):,} ({int(first['year'])}) "
                        f"&rarr; {int(last['total']):,} ({int(last['year'])}) — "
                        f":{color}[{change:+,} ({pct:+.1f}%)]"
                    )
    else:
        st.info("No geographic data available for the selected filters.")

# ── TAB 5: Spending ──
with tab_spending:
    st.subheader("City Homelessness Spending vs. PIT Count")
    st.caption(
        "Budget data from the City of San Diego's 'Homelessness Strategies & Solutions' "
        "department (created FY2021). Sourced from the sd-city-budget project. "
        "Does more spending correlate with fewer homeless? "
        "PIT year roughly aligns with the start of the fiscal year (FY2024 = July 2023, PIT 2024 = January 2024)."
    )

    st.info(
        "**Note on FY2021 spending:** The FY2021 figure includes one-time pandemic-era federal funding "
        "(CARES Act, ARPA), which significantly increased the city's homelessness-related expenditures "
        "for that year. Subsequent years reflect baseline departmental budgets.",
        icon="\u2139\ufe0f",
    )

    spending = query(f"""
        SELECT fiscal_year, amount
        FROM '{_AGG}/homelessness_spending.parquet'
        ORDER BY fiscal_year
    """)

    if not spending.empty:
        # Spending trend
        spend_chart = spending.copy()
        spend_chart["Amount ($M)"] = spend_chart["amount"] / 1e6
        spend_chart["Fiscal Year"] = spend_chart["fiscal_year"].astype(str)

        col_spend, col_pit = st.columns(2)

        with col_spend:
            st.subheader("Annual Budget")
            st.bar_chart(
                spend_chart.set_index("Fiscal Year")[["Amount ($M)"]],
                color=CHART_COLOR,
                y_label="Millions ($)",
            )

        with col_pit:
            st.subheader("PIT Count (Same Period)")
            pit_for_spend = query(f"""
                SELECT year AS "Year", total AS "Total"
                FROM '{_AGG}/pit_trends.parquet'
                WHERE year >= 2021
                ORDER BY year
            """)
            if not pit_for_spend.empty:
                pit_for_spend["Year"] = pit_for_spend["Year"].astype(str)
                st.bar_chart(
                    pit_for_spend.set_index("Year"),
                    color=CHART_COLOR_2,
                )

        # Combined view
        st.subheader("Spending vs. Outcomes")
        combined = spending.copy()
        combined = combined.rename(columns={"fiscal_year": "year"})

        pit_all = query(f"""
            SELECT year, total FROM '{_AGG}/pit_trends.parquet'
            WHERE year >= 2021
        """)

        if not pit_all.empty:
            merged = combined.merge(pit_all, on="year", how="inner")
            if not merged.empty:
                display_df = merged.copy()
                display_df["Budget"] = display_df["amount"].apply(lambda x: f"${x / 1e6:.1f}M")
                display_df["PIT Count"] = display_df["total"].apply(lambda x: f"{x:,}")
                display_df["Cost per Person"] = (display_df["amount"] / display_df["total"]).apply(
                    lambda x: f"${x:,.0f}"
                )
                display_df = display_df.rename(columns={"year": "Year"})
                st.dataframe(
                    display_df[["Year", "Budget", "PIT Count", "Cost per Person"]],
                    use_container_width=True,
                    hide_index=True,
                )

                st.markdown(
                    "**Note**: The city's homelessness budget does not represent all public spending "
                    "on homelessness in San Diego. County, state, and federal programs contribute "
                    "significantly. The 'cost per person' figure is a rough metric — it divides the "
                    "city's department budget by the PIT count, not the total population served."
                )
    else:
        st.info(
            "Spending data not available. Run the sd-city-budget pipeline first to "
            "generate the cross-reference parquet."
        )
