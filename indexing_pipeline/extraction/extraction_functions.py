import asyncio
from typing import Any

from agents import Agent, Runner
from pydantic import BaseModel

from indexing_pipeline.extraction.chunking import chunk_text
from indexing_pipeline.extraction.embedding.embedding import embed_text
from indexing_pipeline.extraction.extraction_models import (
    ChunkWithEmbedding,
    DomainOntology,
    ExtractedDataWithEmbedding,
    TextContent,
    TextContentWithChunks,
    TextContentWithChunksAndEmbedding,
    get_graph_class,
)
from indexing_pipeline.extraction.ontology.meta_graph import (
    Reference,
    ReferenceChunk,
)
from shared.neo4j import get_neo4j_connector


def create_domain_classifier_agent():
    agent = Agent(
        name="domain_classifier_agent",
        instructions="""
classify the text into a domain ontology.
    """,
        output_type=DomainOntology,
        model="gpt-4.1-mini",
    )

    async def run(user_message: str):
        result = await Runner.run(agent, user_message)
        return result.final_output

    return run


def create_extraction_agent_with_ontology(schema: type[BaseModel]):
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
    name should be "Apple Inc." instead of "a tech company"
    description should be "Apple Inc. is a tech company that ..."
    instead of "a business entity"

    """,
        output_type=schema,
        model="gpt-4.1-mini",
    )

    async def run(user_message: str):
        result = await Runner.run(agent, user_message)
        return result.final_output

    return run


async def create_text_content(title: str, text: str) -> TextContent:
    classifier_agent = create_domain_classifier_agent()
    domain = await classifier_agent(text)
    return TextContent(content=text, name=title, domain=domain)


def chunk_data(text_content: TextContent) -> TextContentWithChunks:
    """Chunk the extracted data."""
    chunks = chunk_text(text_content.content)
    return TextContentWithChunks(
        content=text_content.content,
        chunks=chunks,
        name=text_content.name,
        domain=text_content.domain,
    )


async def embed_chunks(
    text_content_with_chunks: TextContentWithChunks,
) -> TextContentWithChunksAndEmbedding:
    """perform embedding on the chunks"""
    embeddings = await embed_text(text_content_with_chunks.chunks)
    chunks_with_embedding = [
        ChunkWithEmbedding(chunk=chunk, embedding=embedding)
        for chunk, embedding in zip(
            text_content_with_chunks.chunks, embeddings, strict=False
        )
    ]
    return TextContentWithChunksAndEmbedding(
        name=text_content_with_chunks.name,
        content=text_content_with_chunks.content,
        chunks_with_embedding=chunks_with_embedding,
        domain=text_content_with_chunks.domain,
    )


async def extract_entity_relationship(
    text_content_with_chunks: TextContentWithChunksAndEmbedding,
) -> ExtractedDataWithEmbedding:
    """Transform the data to a pandas dataframe."""
    tasks = []
    graph_class = get_graph_class(text_content_with_chunks.domain)
    extraction_function = create_extraction_agent_with_ontology(graph_class)
    for chunk_with_embedding in text_content_with_chunks.chunks_with_embedding:
        tasks.append(extraction_function(chunk_with_embedding.chunk))
    extracted_graphs = await asyncio.gather(*tasks)
    return ExtractedDataWithEmbedding(
        name=text_content_with_chunks.name,
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
    reference_node = Reference(
        name=extracted_data_with_embedding.name,
        content=extracted_data_with_embedding.content,
        label="Reference",
    )
    cypher_query_params = await reference_node.to_cypher()
    cypher_queries.append(cypher_query_params)
    reference_id = reference_node.id
    # create a reference chunk node and link it to the reference node
    reference_chunk_nodes = []
    for index, chunk_with_embedding in enumerate(
        extracted_data_with_embedding.chunks_with_embedding
    ):
        reference_chunk_node = ReferenceChunk(
            name=f"{extracted_data_with_embedding.name}_{index}",
            chunk=chunk_with_embedding.chunk,
            embedding=chunk_with_embedding.embedding,
            reference_id=reference_id,
            label="ReferenceChunk",
        )
        reference_chunk_nodes.append(reference_chunk_node)
        cypher_query_params = await reference_chunk_node.to_cypher()
        cypher_queries.append(cypher_query_params)

    # create entities and relationships
    for extracted_graph in extracted_data_with_embedding.extracted_graphs:
        cypher_queries_for_graph = await extracted_graph.to_cypher(reference_id)
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


async def extract_embed_index_data(title: str, content: str) -> dict[str, Any]:
    """
    perform extraction, chunking, embedding, entity relationship extraction,
    and index the data to database.
    """
    text_content = await create_text_content(title, content)
    text_content_with_chunks = chunk_data(text_content)
    text_content_with_chunks_and_embedding = await embed_chunks(
        text_content_with_chunks
    )
    extracted_data_with_embedding = await extract_entity_relationship(
        text_content_with_chunks_and_embedding
    )
    cypher_queries = await convert_object_to_cypher(extracted_data_with_embedding)
    return await index_to_database(cypher_queries)
