"""Pydantic response models for FastAPI's auto-generated OpenAPI docs."""

from __future__ import annotations

from pydantic import BaseModel


class FilterOptions(BaseModel):
    years: list[int]
    regions: list[str]


class OverviewResponse(BaseModel):
    year: int
    total: int
    sheltered: int
    unsheltered: int
    prior_year_total: int | None
    yoy_change: int | None
    yoy_pct: float | None


class PITTrend(BaseModel):
    year: int
    total: int
    sheltered: int
    unsheltered: int


class Subpopulation(BaseModel):
    year: int
    group_name: str
    count: int


class GeographyCount(BaseModel):
    year: int
    region: str
    total: int
    sheltered: int
    unsheltered: int


class SpendingTrend(BaseModel):
    fiscal_year: int
    amount: float
