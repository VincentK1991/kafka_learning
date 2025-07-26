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
    """
    Check if an entity exists in the database.
    """
    print(f"🔍 Checking existence for entity: {source_entity_id}")

    try:
        async with get_neo4j_connector() as connector:
            result = await connector.execute_query(
                CHECK_ENTITY_EXISTS_QUERY,
                {"source_entity_id": source_entity_id},
            )

            # Extract the boolean result
            exists = (
                bool(result[0]["node_exists"]) if result and len(result) > 0 else False
            )
            print(f"📋 Entity {source_entity_id} exists: {exists}")
            return exists

    except Exception as e:
        print(f"❌ Error checking entity existence for {source_entity_id}: {e}")
        return False


async def normalize_entity(source_entity_id: str) -> dict[str, Any]:
    """
    Normalize entities by merging similar ones using semantic, string, and topology similarity.
    """
    print(f"🔍 Starting entity normalization for: {source_entity_id}")

    try:
        async with get_neo4j_connector() as connector:
            print("📊 Executing entity normalization query...")
            result = await connector.execute_query(
                ENTITY_NORMALIZATION_QUERY,
                {
                    "source_entity_id": source_entity_id,
                    "semantic_score_threshold": 0.5,
                    "combined_score_threshold": 0.5,
                    "string_similarity_threshold": 0.9,
                },
            )
            print(
                f"✅ Entity normalization completed successfully for: {source_entity_id}"
            )
            print(f"📋 Result: {result}")
            return result

    except Exception as e:
        print(f"❌ Error in entity normalization for {source_entity_id}: {e}")
        print(f"🔍 Error type: {type(e).__name__}")

        # If it's a deleted node error, return empty result instead of crashing
        if "has been deleted in this transaction" in str(e):
            print(
                f"⚠️  Node deletion detected - returning empty result for {source_entity_id}"
            )
            return {"entity_normalization": "skipped_due_to_deletion"}
        else:
            # Re-raise other types of errors
            raise


async def normalize_relationship(source_entity_id: str) -> dict[str, Any]:
    """
    Normalize relationships for an entity using separate queries to avoid transaction conflicts.
    """
    print(f"🔗 Starting relationship normalization for: {source_entity_id}")
    results = {}

    try:
        async with get_neo4j_connector() as connector:
            # Step 1: Merge duplicate relationships (same name/type)
            print("📝 Step 1: Merging duplicate relationships...")
            try:
                duplicate_result = await connector.execute_query(
                    MERGE_DUPLICATE_RELATIONSHIPS_QUERY,
                    {"source_entity_id": source_entity_id},
                )
                results["duplicate_relationships"] = duplicate_result
                print(f"✅ Step 1 completed: {duplicate_result}")
            except Exception as e:
                print(f"❌ Step 1 failed: {e}")
                results["duplicate_relationships"] = {"error": str(e)}

            # Step 2: Merge multiple mention relationships
            print("📝 Step 2: Merging mention relationships...")
            try:
                mention_result = await connector.execute_query(
                    MERGE_MENTION_RELATIONSHIPS_QUERY,
                    {"source_entity_id": source_entity_id},
                )
                results["mention_relationships"] = mention_result
                print(f"✅ Step 2 completed: {mention_result}")
            except Exception as e:
                print(f"❌ Step 2 failed: {e}")
                results["mention_relationships"] = {"error": str(e)}

            # Step 3: Merge relationships with similar embeddings
            print("📝 Step 3: Merging embedding relationships...")
            try:
                embedding_result = await connector.execute_query(
                    MERGE_SIMILAR_EMBEDDING_RELATIONSHIPS_QUERY,
                    {
                        "source_entity_id": source_entity_id,
                        "embedding_similarity_threshold": 0.5,
                    },
                )
                results["embedding_relationships"] = embedding_result
                print(f"✅ Step 3 completed: {embedding_result}")
            except Exception as e:
                print(f"❌ Step 3 failed: {e}")
                results["embedding_relationships"] = {"error": str(e)}

    except Exception as e:
        print(
            f"❌ Fatal error in relationship normalization for {source_entity_id}: {e}"
        )
        return {"error": str(e)}

    print(f"🔗 Relationship normalization completed for: {source_entity_id}")
    return results


async def normalize_entity_and_relationship(source_entity_id: str) -> dict[str, Any]:
    entity_result = await normalize_entity(source_entity_id)
    relationship_result = await normalize_relationship(source_entity_id)
    return {
        "entity_normalization_result": entity_result,
        "relationship_normalization_result": relationship_result,
    }
