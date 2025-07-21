from typing import Literal

from pydantic import BaseModel, Field

from indexing_pipeline.extraction.ontology.meta_graph import (
    BaseRelationship,
    schema_factory,
)

from .entities import (
    BusinessSegment,
    Company,
    Executive,
    FinancialInformation,
    FinancialMetric,
    GeographicRegion,
    LegalProceeding,
    Market,
    Product,
    Regulation,
    RiskFactor,
    Service,
    Technology,
)


class ExecutiveManagesCompany(BaseRelationship):
    label: Literal["MANAGES"]
    source_entity: Executive = Field(
        ..., description="An executive who manages a company."
    )
    target_entity: Company = Field(
        ..., description="A company managed by an executive."
    )


class CompanyCompetesWith(BaseRelationship):
    label: Literal["COMPETES_WITH"]
    source_entity: Company = Field(..., description="A company.")
    target_entity: Company = Field(..., description="A competing company.")


class CompanyOffersProduct(BaseRelationship):
    label: Literal["OFFERS_PRODUCT"]
    source_entity: Company = Field(..., description="A company offering a product.")
    target_entity: Product = Field(..., description="A product offered by a company.")


class CompanyOffersService(BaseRelationship):
    label: Literal["OFFERS_SERVICE"]
    source_entity: Company = Field(..., description="A company offering a service.")
    target_entity: Service = Field(..., description="A service offered by a company.")


class CompanyOperatesInRegion(BaseRelationship):
    label: Literal["OPERATES_IN"]
    source_entity: Company = Field(..., description="A company operating in a region.")
    target_entity: GeographicRegion = Field(
        ..., description="A region where a company operates."
    )


class CompanyHasBusinessSegment(BaseRelationship):
    label: Literal["HAS_SEGMENT"]
    source_entity: Company = Field(
        ..., description="A company with a business segment."
    )
    target_entity: BusinessSegment = Field(
        ..., description="A business segment of a company."
    )


class CompanyFacesRisk(BaseRelationship):
    label: Literal["FACES_RISK"]
    source_entity: Company = Field(..., description="A company facing a risk.")
    target_entity: RiskFactor = Field(
        ..., description="A risk factor affecting a company."
    )


class CompanySubjectToRegulation(BaseRelationship):
    label: Literal["SUBJECT_TO"]
    source_entity: Company = Field(..., description="A company subject to regulation.")
    target_entity: Regulation = Field(
        ..., description="A regulation that applies to a company."
    )


class CompanyInvolvedInLegalProceeding(BaseRelationship):
    label: Literal["INVOLVED_IN"]
    source_entity: Company = Field(
        ..., description="A company involved in a legal proceeding."
    )
    target_entity: LegalProceeding = Field(
        ..., description="A legal proceeding involving a company."
    )


class CompanyReportsFinancialMetric(BaseRelationship):
    label: Literal["REPORTS_METRIC"]
    source_entity: Company = Field(
        ..., description="A company reporting a financial metric."
    )
    target_entity: FinancialMetric = Field(
        ..., description="A financial metric reported by a company."
    )


class CompanyUsesTechnology(BaseRelationship):
    label: Literal["USES_TECHNOLOGY"]
    source_entity: Company = Field(..., description="A company using a technology.")
    target_entity: Technology = Field(
        ..., description="A technology used by a company."
    )


class CompanyOperatesInMarket(BaseRelationship):
    label: Literal["OPERATES_IN"]
    source_entity: Company = Field(..., description="A company operating in a market.")
    target_entity: Market = Field(..., description="A market where a company operates.")


class CompanyReportsFinancialInformation(BaseRelationship):
    label: Literal["REPORTS_INFORMATION"]
    source_entity: Company = Field(
        ..., description="A company reporting financial information."
    )
    target_entity: FinancialInformation = Field(
        ..., description="Financial information reported by a company."
    )


financial_schema: type[BaseModel] = (
    CompanyReportsFinancialInformation
    | CompanyOperatesInMarket
    | CompanyUsesTechnology
    | CompanySubjectToRegulation
    | CompanyInvolvedInLegalProceeding
    | CompanyReportsFinancialMetric
    | CompanyFacesRisk
    | CompanyHasBusinessSegment
    | CompanyOperatesInRegion
    | CompanyOffersService
    | CompanyOffersProduct
    | CompanyCompetesWith
    | ExecutiveManagesCompany
)


FinancialGraph = schema_factory(financial_schema)
