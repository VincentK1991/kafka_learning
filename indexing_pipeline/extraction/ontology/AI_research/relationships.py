from typing import Literal

from pydantic import Field

from indexing_pipeline.extraction.ontology.meta_graph import (
    BaseRelationship,
    schema_factory,
)

from .entites import (
    Algorithm,
    Dataset,
    Experiment,
    Hypothesis,
    Limitation,
    Metric,
    Model,
    ResearchProblem,
    Result,
    Task,
)


class Addresses(BaseRelationship):
    label: Literal["ADDRESSES"]
    source_entity: Model | Algorithm = Field(
        ..., description="A model or algorithm that addresses a research problem."
    )
    target_entity: ResearchProblem = Field(
        ..., description="A research problem addressed by a model or algorithm."
    )


class EvaluatedOn(BaseRelationship):
    label: Literal["EVALUATED_ON"]
    source_entity: Model | Algorithm = Field(
        ..., description="A model or algorithm that is evaluated on a dataset."
    )
    target_entity: Dataset = Field(
        ..., description="A dataset used to evaluate a model or algorithm."
    )


class Achieves(BaseRelationship):
    label: Literal["ACHIEVES"]
    source_entity: Model | Algorithm = Field(
        ..., description="A model or algorithm that achieves a certain result."
    )
    target_entity: Result = Field(
        ..., description="A result achieved by a model or algorithm."
    )


class MeasuredBy(BaseRelationship):
    label: Literal["MEASURED_BY"]
    source_entity: Result = Field(
        ..., description="A result that is measured by a metric."
    )
    target_entity: Metric = Field(..., description="A metric used to measure a result.")


class AppliedTo(BaseRelationship):
    label: Literal["APPLIED_TO"]
    source_entity: Model | Algorithm = Field(
        ..., description="A model or algorithm that is applied to a task."
    )
    target_entity: Task = Field(..., description="A task to which a model is applied.")


class HasLimitation(BaseRelationship):
    label: Literal["HAS_LIMITATION"]
    source_entity: Model | Algorithm = Field(
        ..., description="A model or algorithm that has a limitation."
    )
    target_entity: Limitation = Field(
        ..., description="A limitation of a model or algorithm."
    )


class Validates(BaseRelationship):
    label: Literal["VALIDATES"]
    source_entity: Experiment = Field(
        ..., description="An experiment that validates a hypothesis."
    )
    target_entity: Hypothesis = Field(
        ..., description="A hypothesis validated by an experiment."
    )


class Uses(BaseRelationship):
    label: Literal["USES"]
    source_entity: Experiment = Field(
        ..., description="An experiment that uses a model, algorithm, or dataset."
    )
    target_entity: Model | Algorithm | Dataset = Field(
        ..., description="A model, algorithm, or dataset used in an experiment."
    )


ai_research_schema = (
    Addresses
    | EvaluatedOn
    | Achieves
    | MeasuredBy
    | AppliedTo
    | HasLimitation
    | Validates
    | Uses
)


AIResearchGraph = schema_factory(ai_research_schema)
