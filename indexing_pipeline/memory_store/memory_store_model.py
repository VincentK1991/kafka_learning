from pydantic import BaseModel


class MemoryContent(BaseModel):
    user_id: str
    content: str


class ChunkWithEmbedding(BaseModel):
    chunk: str
    embedding: list[float]


class MemoryContentWithChunks(MemoryContent):
    chunks: list[str]


class MemoryContentWithChunksAndEmbedding(MemoryContent):
    chunks_with_embedding: list[ChunkWithEmbedding]


class ExtractedDataWithEmbedding(BaseModel):
    user_id: str
    content: str
    chunks_with_embedding: list[ChunkWithEmbedding]
    extracted_graphs: list[BaseModel]
