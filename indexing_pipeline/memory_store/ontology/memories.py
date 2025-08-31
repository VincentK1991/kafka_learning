from typing import Literal

from indexing_pipeline.memory_store.ontology.meta_memory import BaseMemory


class Fact(BaseMemory):
    """The specific personal fact."""

    sub_label: Literal["Fact"]


class Belief(BaseMemory):
    """A specific belief or opinion."""

    sub_label: Literal["Belief"]


class Interest(BaseMemory):
    """A specific interest or hobby."""

    sub_label: Literal["Interest"]


class Goal(BaseMemory):
    """A specific goal or objective."""

    sub_label: Literal["Goal"]
