import uuid
from enum import Enum

from pydantic import BaseModel, Field

from indexing_pipeline.extraction.ontology.AI_research.relationships import (
    AIResearchGraph,
)
from indexing_pipeline.extraction.ontology.finance.relationships import (
    FinancialGraph,
)


class DomainOntology(Enum):
    FINANCE = "finance"
    AI_RESEARCH = "ai_research"


# Mapping from domain ontology to their corresponding graph classes
DOMAIN_TO_GRAPH_MAPPING: dict[DomainOntology, type[BaseModel]] = {
    DomainOntology.FINANCE: FinancialGraph,
    DomainOntology.AI_RESEARCH: AIResearchGraph,
}


def get_graph_class(domain: DomainOntology) -> type[BaseModel]:
    """Get the graph class for a given domain ontology."""
    if domain not in DOMAIN_TO_GRAPH_MAPPING:
        raise ValueError(f"Unsupported domain: {domain}")
    return DOMAIN_TO_GRAPH_MAPPING[domain]


class TextContent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    content: str
    domain: DomainOntology


class ChunkWithEmbedding(BaseModel):
    chunk: str
    embedding: list[float]


class TextContentWithChunks(TextContent):
    chunks: list[str]


class TextContentWithChunksAndEmbedding(TextContent):
    chunks_with_embedding: list[ChunkWithEmbedding]


class ExtractedDataWithEmbedding(BaseModel):
    name: str
    content: str
    chunks_with_embedding: list[ChunkWithEmbedding]
    extracted_graphs: list[BaseModel]
