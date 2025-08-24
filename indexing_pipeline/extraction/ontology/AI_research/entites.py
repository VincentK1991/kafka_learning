from typing import Literal

from indexing_pipeline.extraction.ontology.meta_graph import BaseEntity


class ResearchProblem(BaseEntity):
    """The specific problem or question the research paper is trying to address."""

    sub_label: Literal["ResearchProblem"]


class Model(BaseEntity):
    """A specific model architecture or type used or proposed in the paper."""

    sub_label: Literal["Model"]


class Algorithm(BaseEntity):
    """A specific algorithm or method being proposed or used."""

    sub_label: Literal["Algorithm"]


class Dataset(BaseEntity):
    """A dataset used for training, validation, or testing in the research."""

    sub_label: Literal["Dataset"]


class Metric(BaseEntity):
    """An evaluation metric used to measure the performance of a model or algorithm."""

    sub_label: Literal["Metric"]
    value: float | None = None


class Task(BaseEntity):
    """A specific task the research is focused on, e.g., Image Classification."""

    sub_label: Literal["Task"]


class Experiment(BaseEntity):
    """An experimental setup or a specific experiment conducted in the paper."""

    sub_label: Literal["Experiment"]


class Result(BaseEntity):
    """The outcome of an experiment, often a metric value on a specific dataset."""

    sub_label: Literal["Result"]


class Hypothesis(BaseEntity):
    """A hypothesis or assumption made in the research paper."""

    sub_label: Literal["Hypothesis"]


class Limitation(BaseEntity):
    """A limitation of the work described in the paper."""

    sub_label: Literal["Limitation"]
