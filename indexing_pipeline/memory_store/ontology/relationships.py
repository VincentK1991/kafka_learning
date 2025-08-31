from typing import Literal

from pydantic import Field

from indexing_pipeline.extraction.ontology.meta_graph import (
    BaseRelationship,
    schema_factory,
)

from .memories import (
    Belief,
    Fact,
    Goal,
    Interest,
)


class SUPPORTS(BaseRelationship):
    label: Literal["SUPPORTS"]
    source_memory: Fact | Belief = Field(
        ..., description="A fact, belief that is the foundation of the relationship."
    )
    target_memory: Fact | Belief = Field(
        ..., description="A fact, belief that is supported by the foundation."
    )


class CONTRADICTS(BaseRelationship):
    label: Literal["CONTRADICTS"]
    source_memory: Fact | Belief = Field(
        ..., description="A fact, belief that is the foundation of the relationship."
    )
    target_memory: Fact | Belief = Field(
        ..., description="A fact, belief that is contradicted by the foundation."
    )


class EXPLAINS(BaseRelationship):
    label: Literal["EXPLAINS"]
    source_memory: Fact = Field(
        ..., description="A fact that explains the relationship."
    )
    target_memory: Belief | Interest | Goal = Field(
        ..., description="A belief, interest, or goal that is explained by the fact."
    )


class CAUSES(BaseRelationship):
    label: Literal["CAUSES"]
    source_memory: Belief = Field(
        ..., description="A belief that causes the interest or goal."
    )
    target_memory: Interest | Goal = Field(
        ..., description="A interest, goal that is caused by the belief."
    )


memory_schema = SUPPORTS | CONTRADICTS | EXPLAINS | CAUSES


MemoryGraph = schema_factory(memory_schema)
