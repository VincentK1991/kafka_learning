from shared.neo4j import get_neo4j_connector


async def create_indices():
    async with get_neo4j_connector() as connector:
        # Check if the index already exists
        result = await connector.execute_query(
            "SHOW INDEXES YIELD name WHERE name = 'memory_embeddings'"
        )
        if not result:
            await connector.execute_query(
                """
                CREATE VECTOR INDEX memory_embeddings FOR (e:Memory) ON (e.embedding)
                OPTIONS {
                    indexConfig: {
                        `vector.dimensions`: 256,
                        `vector.similarity_function`: 'cosine'
                    }
                }
                """
            )
