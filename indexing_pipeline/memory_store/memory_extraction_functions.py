import asyncio
from typing import Any

from agents import Agent, Runner
from pydantic import BaseModel

from indexing_pipeline.extraction.chunking import chunk_text
from indexing_pipeline.extraction.embedding.embedding import embed_text
from indexing_pipeline.memory_store.memory_store_model import (
    ChunkWithEmbedding,
    ExtractedDataWithEmbedding,
    MemoryContent,
    MemoryContentWithChunks,
    MemoryContentWithChunksAndEmbedding,
)
from indexing_pipeline.memory_store.ontology.meta_memory import User
from indexing_pipeline.memory_store.ontology.relationships import MemoryGraph
from shared.neo4j import get_neo4j_connector


def create_extraction_agent(schema: type[BaseModel]):
    agent = Agent(
        name="graph_extraction_agent",
        instructions="""
    You are a graph extraction agent that can extract the graph from the text.
    pay attention to the relevant and important information in the text.

    You should extract as many graphs as possible from the text.
    You should also infer the concept, implicit knowledge or
     background information related to the subject domain
     or relations of ideas between concepts mentioned in the text and
      well known concepts even though
    they may not be explicitly mentioned in the text.

    The name and description of the graph nodes
    should be specific and descriptive such that they can be used to
    understand the graph and the relationships between the nodes.

    For example,

    """,
        output_type=schema,
        model="gpt-4.1-mini",
    )

    async def run(user_message: str):
        result = await Runner.run(agent, user_message)
        return result.final_output

    return run


async def create_text_content(user_id: str, content: str) -> MemoryContent:
    return MemoryContent(content=content, user_id=user_id)


def chunk_data(text_content: MemoryContent) -> MemoryContentWithChunks:
    """Chunk the extracted data."""
    chunks = chunk_text(text_content.content)
    return MemoryContentWithChunks(
        content=text_content.content,
        chunks=chunks,
        user_id=text_content.user_id,
    )


async def embed_chunks(
    text_content_with_chunks: MemoryContentWithChunks,
) -> MemoryContentWithChunksAndEmbedding:
    """perform embedding on the chunks"""
    embeddings = await embed_text(text_content_with_chunks.chunks)
    chunks_with_embedding = [
        ChunkWithEmbedding(chunk=chunk, embedding=embedding)
        for chunk, embedding in zip(
            text_content_with_chunks.chunks, embeddings, strict=False
        )
    ]
    return MemoryContentWithChunksAndEmbedding(
        user_id=text_content_with_chunks.user_id,
        content=text_content_with_chunks.content,
        chunks_with_embedding=chunks_with_embedding,
    )


async def extract_entity_relationship(
    text_content_with_chunks: MemoryContentWithChunksAndEmbedding,
) -> ExtractedDataWithEmbedding:
    """Transform the data to a pandas dataframe."""
    tasks = []
    extraction_function = create_extraction_agent(MemoryGraph)
    for chunk_with_embedding in text_content_with_chunks.chunks_with_embedding:
        tasks.append(extraction_function(chunk_with_embedding.chunk))
    extracted_graphs = await asyncio.gather(*tasks)
    return ExtractedDataWithEmbedding(
        user_id=text_content_with_chunks.user_id,
        content=text_content_with_chunks.content,
        chunks_with_embedding=text_content_with_chunks.chunks_with_embedding,
        extracted_graphs=extracted_graphs,
    )


async def convert_object_to_cypher(
    extracted_data_with_embedding: ExtractedDataWithEmbedding,
) -> list[tuple[str, dict[str, Any]]]:
    """Convert the object to cypher."""
    cypher_queries = []
    # create a reference node
    user_node = User(
        user_id=extracted_data_with_embedding.user_id,
        name="User",
        label="User",
    )
    cypher_query_params = await user_node.to_cypher()
    cypher_queries.append(cypher_query_params)
    user_id = user_node.user_id
    # create a reference chunk node and link it to the reference node
    # create entities and relationships
    for extracted_graph in extracted_data_with_embedding.extracted_graphs:
        cypher_queries_for_graph = await extracted_graph.to_cypher(user_id)
        for cypher_query_params in cypher_queries_for_graph:
            cypher_queries.append(cypher_query_params)
    return cypher_queries


async def index_to_database(
    cypher_queries: list[tuple[str, dict[str, Any]]],
) -> dict[str, Any]:
    """Index the transformed data to database."""

    async with get_neo4j_connector() as connector:
        result_ids = []
        for cypher_query, cypher_params in cypher_queries:
            result = await connector.execute_query(cypher_query, cypher_params)
            result_ids.append(result)
    entities_ids = [i[0]["entity_id"] for i in result_ids if "entity_id" in i[0]]

    return {"status": "success", "entity_ids": entities_ids}


async def extract_memory_embed_index_data(title: str, content: str) -> dict[str, Any]:
    """
    perform extraction, chunking, embedding, entity relationship extraction,
    and index the data to database.
    """
    print(f"strp 1 of 6 Extracting and indexing data for {title}")
    text_content = await create_text_content(title, content)
    print(f"step 2 of 6 Chunking data for {title}")
    text_content_with_chunks = chunk_data(text_content)
    print(f"step 3 of 6 Embedding chunks for {title}")
    text_content_with_chunks_and_embedding = await embed_chunks(
        text_content_with_chunks
    )
    print(f"step 4 of 6 Extracting entity relationship for {title}")
    extracted_data_with_embedding = await extract_entity_relationship(
        text_content_with_chunks_and_embedding
    )
    print(f"step 5 of 6 Converting object to cypher for {title}")
    cypher_queries = await convert_object_to_cypher(extracted_data_with_embedding)
    print(f"step 6 of 6 Indexing data to database for {title}")
    return await index_to_database(cypher_queries)
