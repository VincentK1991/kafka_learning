import asyncio
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field

from indexing_pipeline.extraction.embedding.embedding import embed_text
from indexing_pipeline.extraction.ontology.base_ontology import NodeBase


class User(NodeBase):
    label: Literal["User"]
    user_id: str = Field(..., description="The unique identifier of the user")

    async def to_cypher(self) -> tuple[str, dict[str, Any]]:
        properties = self.get_properties()

        cypher_query = """
        MERGE (n:User {{user_id: $user_id}})
        ON CREATE SET n += $properties
        RETURN n.id AS user_id
        """
        cypher_params = {"user_id": self.user_id, "properties": properties}
        return cypher_query, cypher_params


class BaseMemory(NodeBase):
    label: Literal["Memory"]
    name: str = Field(..., description="The specific name/identifier of the memory")
    description: str = Field(
        ...,
        description="The description of the memory,\
             should be specific and descriptive such that it can be used to\
             understand what the memory is",
    )

    async def to_cypher(self, id: str, user_id: str) -> tuple[str, dict[str, Any]]:
        properties = self.get_properties()
        properties.update({"name": self.name})
        text_to_embed = f"{self.name or ''} {self.description or ''}".strip()
        embedding = await embed_text([text_to_embed])
        properties.update({"embedding": embedding[0]})
        properties.update({"id": id})

        # Get sub_label dynamically since it's defined in subclasses
        sub_label = getattr(self, "sub_label", "Entity")
        cypher_query = f"""
        MERGE (r:User {{user_id: $user_id}})
        MERGE (n:{self.label}:{sub_label} {{name: $name}})
        ON CREATE SET
          n += $properties
        ON MATCH SET
          n.id = $properties.id
        MERGE (r)-[:ASSOCIATED_WITH]->(n)
        RETURN n.id AS entity_id
        """

        cypher_params = {
            "name": self.name,
            "user_id": user_id,
            "properties": properties,
        }

        return cypher_query.strip(), cypher_params

    def get_properties(self):
        return self.model_dump(exclude={"name", "label", "sub_label"})


class BaseRelationship(NodeBase):
    description: str = Field(
        ...,
        description="The description of the relationship,\
             should be specific and descriptive such that it can be used to\
             understand the relationship between the memories",
    )
    source_memory: Any  # Allow subclasses to override with specific entity types
    target_memory: Any  # Allow subclasses to override with specific entity types

    async def to_cypher(
        self, source_id: str, target_id: str
    ) -> tuple[str, dict[str, Any]]:
        properties = self.get_properties()
        text_to_embed = f"{self.name or ''} {self.description or ''}".strip()
        embedding = await embed_text([text_to_embed])
        properties.update({"embedding": embedding[0]})
        properties.update({"name": getattr(self, "name", None)})

        # Note: Relationship labels cannot be parameterized in Cypher
        rel_label = getattr(self, "label", "RELATED_TO")

        cypher_query = f"""
        MERGE (s:{self.source_memory.label}:{self.source_memory.sub_label}\
             {{id: $source_id}})
        MERGE (t:{self.target_memory.label}:{self.target_memory.sub_label}\
             {{id: $target_id}})
        MERGE (s)-[r:{rel_label} {{name: $name}}]->(t)
        ON CREATE SET r += $properties
        RETURN r.id AS relationship_id
        """

        cypher_params = {
            "source_id": source_id,
            "target_id": target_id,
            "name": getattr(self, "name", None),
            "properties": properties,
        }

        return cypher_query.strip(), cypher_params

    def get_properties(self):
        return self.model_dump(
            exclude={"name", "label", "source_memory", "target_memory"}
        )


def schema_factory(schema: Any) -> type[BaseModel]:
    class Graph(BaseModel):
        extracted_graphs: list[schema]  # type: ignore

        async def to_cypher(
            self, user_id: str
        ) -> list[tuple[str, dict[str, Any]]]:
            # Collect all coroutines for parallel execution
            tasks = []

            for rel in self.extracted_graphs:
                source_id = str(uuid.uuid4())
                target_id = str(uuid.uuid4())
                # Add source entity cypher task
                tasks.append(rel.source_entity.to_cypher(source_id, user_id))
                # Add target entity cypher task
                tasks.append(rel.target_entity.to_cypher(target_id, user_id))
                # Add relationship cypher task
                tasks.append(rel.to_cypher(source_id, target_id))

            # Execute all tasks in parallel
            results = await asyncio.gather(*tasks)

            # Process results back into the expected format
            cypher_queries = []
            for i in range(0, len(results), 3):
                # Each group of 3 results corresponds to: relationship,
                # source_entity, target_entity
                source_result = results[i]
                target_result = results[i + 1]
                rel_result = results[i + 2]

                cypher_queries.append(source_result)
                cypher_queries.append(target_result)
                cypher_queries.append(rel_result)

            return cypher_queries

    return Graph
