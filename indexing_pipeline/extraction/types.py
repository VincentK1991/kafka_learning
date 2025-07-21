import uuid

from pydantic import BaseModel, Field


class TextContent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    content: str


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
    extracted_graphs: list[type[BaseModel]]
