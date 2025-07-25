from typing import Any

from indexing_pipeline.normalization.normalization_query import (
    CHECK_ENTITY_EXISTS_QUERY,
    ENTITY_NORMALIZATION_QUERY,
    MERGE_DUPLICATE_RELATIONSHIPS_QUERY,
    MERGE_MENTION_RELATIONSHIPS_QUERY,
    MERGE_SIMILAR_EMBEDDING_RELATIONSHIPS_QUERY,
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
    """
    Normalize relationships for an entity using separate queries to avoid transaction conflicts.
    """
    results = {}

    async with get_neo4j_connector() as connector:
        # Step 1: Merge duplicate relationships (same name/type)
        duplicate_result = await connector.execute_query(
            MERGE_DUPLICATE_RELATIONSHIPS_QUERY,
            {"source_entity_id": source_entity_id},
        )
        results["duplicate_relationships"] = duplicate_result

        # Step 2: Merge multiple mention relationships
        mention_result = await connector.execute_query(
            MERGE_MENTION_RELATIONSHIPS_QUERY,
            {"source_entity_id": source_entity_id},
        )
        results["mention_relationships"] = mention_result

        # Step 3: Merge relationships with similar embeddings
        embedding_result = await connector.execute_query(
            MERGE_SIMILAR_EMBEDDING_RELATIONSHIPS_QUERY,
            {
                "source_entity_id": source_entity_id,
                "embedding_similarity_threshold": 0.5,
            },
        )
        results["embedding_relationships"] = embedding_result

    return results


async def normalize_entity_and_relationship(source_entity_id: str) -> dict[str, Any]:
    entity_result = await normalize_entity(source_entity_id)
    relationship_result = await normalize_relationship(source_entity_id)
    return {
        "entity_normalization_result": entity_result,
        "relationship_normalization_result": relationship_result,
    }
