from typing import Any

from indexing_pipeline.normalization.normalization_query import (
    CHECK_ENTITY_EXISTS_QUERY,
    ENTITY_NORMALIZATION_QUERY,
    RELATIONSHIP_NORMALIZATION_QUERY,
)
from shared.neo4j import get_neo4j_connector


async def check_entity_exists(source_entity_id: str) -> bool:
    async with get_neo4j_connector() as connector:
        result = await connector.execute_query(
            CHECK_ENTITY_EXISTS_QUERY,
            {"source_entity_id": source_entity_id},
        )
        return result


async def normalize_entity(source_entity_id: str) -> dict[str, Any]:
    async with get_neo4j_connector() as connector:
        result = await connector.execute_query(
            ENTITY_NORMALIZATION_QUERY,
            {
                "source_entity_id": source_entity_id,
                "semantic_score_threshold": 0.5,
                "combined_score_threshold": 0.5,
                "string_similarity_threshold": 0.9,
            },
        )
        return result


async def normalize_relationship(source_entity_id: str) -> dict[str, Any]:
    async with get_neo4j_connector() as connector:
        result = await connector.execute_query(
            RELATIONSHIP_NORMALIZATION_QUERY,
            {
                "source_entity_id": source_entity_id,
                "embedding_similarity_threshold": 0.5,
            },
        )
        return result


async def normalize_entity_and_relationship(source_entity_id: str) -> dict[str, Any]:
    entity_result = await normalize_entity(source_entity_id)
    relationship_result = await normalize_relationship(source_entity_id)
    return {
        "entity_normalization_result": entity_result,
        "relationship_normalization_result": relationship_result,
    }
