from typing import Literal

from indexing_pipeline.extraction.ontology.meta_graph import (
    BaseEntity,
)


class Company(BaseEntity):
    """A business organization."""

    sub_label: Literal["Company"]


class Executive(BaseEntity):
    """A high-level manager or director in an organization."""

    sub_label: Literal["Executive"]
    title: str | None = None


class Technology(BaseEntity):
    """A technology or innovation that a company uses or develops."""

    sub_label: Literal["Technology"]


class FinancialInformation(BaseEntity):
    """Financial information about a company."""

    sub_label: Literal["FinancialInformation"]
    type: str | None = None
    value: float | None = None


class Product(BaseEntity):
    """A specific product or service offered by a company."""

    sub_label: Literal["Product"]


class Service(BaseEntity):
    """A service offered by a company."""

    sub_label: Literal["Service"]


class Regulation(BaseEntity):
    """A specific rule or law,\
         such as from the SEC or GAAP, that affects the company."""

    sub_label: Literal["Regulation"]


class FinancialMetric(BaseEntity):
    """A key financial figure, such as Revenue, Net Income, or EBITDA."""

    sub_label: Literal["FinancialMetric"]
    value: float | None = None
    period: str | None = None


class RiskFactor(BaseEntity):
    """A risk that could negatively\
         impact the company's performance, as disclosed in filings."""

    sub_label: Literal["RiskFactor"]


class LegalProceeding(BaseEntity):
    """A lawsuit or other legal action involving the company."""

    sub_label: Literal["LegalProceeding"]


class GeographicRegion(BaseEntity):
    """A country, state, or other region\
         where the company operates or sells products."""

    sub_label: Literal["GeographicRegion"]


class BusinessSegment(BaseEntity):
    """A specific division or part of a company\
         that is managed and reported on separately."""

    sub_label: Literal["BusinessSegment"]


class Market(BaseEntity):
    """A market or industry that a company operates in."""

    sub_label: Literal["Market"]
    estimated_size: int | None = None
    estimated_growth_rate: float | None = None
