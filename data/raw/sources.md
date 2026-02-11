# SD Homelessness Data Sources

## Primary: HUD Point-in-Time (PIT) Count Data

**Source**: HUD Exchange — CoC CA-601 (San Diego City and County)
**URL**: https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/
**Format**: Excel workbooks with CoC-level data
**Coverage**: 2007–2024 (annual, single-night snapshot in January)
**Status**: Data compiled into `pit_counts.csv` from HUD published figures and RTFH reports

The PIT count is a federally mandated annual census of people experiencing homelessness,
conducted on a single night in January. It is the most authoritative measure of
homelessness at the local level.

**Key limitation**: PIT is a point-in-time snapshot — it undercounts the total number of
people who experience homelessness over the course of a year (estimated 2-3x higher).

## Secondary: RTFH Annual Reports

**Source**: Regional Task Force on Homelessness (RTFH)
**URL**: https://www.rtfhsd.org/reports-data/
**Format**: PDF reports with detailed breakdowns
**Coverage**: 2011–2024 (annual PIT count reports)
**Status**: Used to supplement HUD data with subpopulation and geographic breakdowns

RTFH is the Continuum of Care (CoC) lead agency for San Diego. They conduct the
annual PIT count and publish detailed reports including:
- Total counts (sheltered/unsheltered)
- Subpopulations (veterans, families, youth, chronically homeless)
- Geographic breakdowns by subregion within San Diego County

## Cross-Reference: City Budget Data

**Source**: sd-city-budget project (completed)
**Path**: `/Users/glennharless/dev-brain/sd-city-budget/data/aggregated/dept_budget_trends.parquet`
**Filter**: `dept_name = 'Homelessness Strategies & Solutions'`, `budget_cycle = 'adopted'`, `revenue_or_expense = 'Expense'`
**Coverage**: FY2021–FY2026 (department created in FY2021)
**Status**: Directly queryable from sister project's parquet files

## Not Used

### San Diego County Homelessness Data Portal
- URL: https://data.sandiegocounty.gov/stories/s/Homelessness/85ch-mkw4/
- Embedded Tableau dashboards; no downloadable structured datasets found

### SDHC Shelter Dashboard
- URL: https://sdhc.org/homelessness-solutions/city-homeless-shelters-services/dashboard/
- Embedded Tableau dashboard; no downloadable data
- Would provide shelter capacity/utilization data if extractable

### City Reports Page
- URL: https://www.sandiego.gov/homelessness-strategies-and-solutions/data-reports
- Links to RTFH reports and general information; no unique structured data
