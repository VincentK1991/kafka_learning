import asyncio
import uuid
from typing import Any, Literal

from pydantic import BaseModel, Field

from indexing_pipeline.extraction.embedding.embedding import embed_text

from .base_ontology import NodeBase


class Reference(NodeBase):
    label: Literal["Reference"]
    name: str
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    content: str
    updates_of: str | None = None

    async def to_cypher(self) -> tuple[str, dict[str, Any]]:
        properties = self.get_properties()
        properties.update({"name": self.name})
        if self.updates_of:
            cypher_query = f"""
            MERGE (r:Reference {{id: $updates_of}})
            MERGE (n:{self.label} {{name: $name}})
            ON CREATE SET n += $properties
            MERGE (r)-[:UPDATED_BY]->(n)
            RETURN n.id AS reference_id
            """
            cypher_params = {"name": self.name, "properties": properties}
            return cypher_query, cypher_params

        cypher_query = f"""
        MERGE (n:{self.label} {{name: $name}})
        ON CREATE SET n += $properties
        RETURN n.id AS reference_id
        """

        cypher_params = {"name": self.name, "properties": properties}
        return cypher_query, cypher_params


class ReferenceChunk(NodeBase):
    label: Literal["ReferenceChunk"]
    name: str
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    reference_id: str
    chunk: str
    embedding: list[float]

    async def to_cypher(self) -> tuple[str, dict[str, Any]]:
        properties = self.get_properties()
        properties.update({"name": self.name})

        cypher_query = f"""
        MERGE (r:Reference {{id: $reference_id}})
        MERGE (n:{self.label} {{name: $name}})
        ON CREATE SET n += $properties
        MERGE (n)-[:CHUNK_OF]->(r)
        RETURN n.id AS reference_chunk_id
        """

        cypher_params = {
            "name": self.name,
            "reference_id": self.reference_id,
            "properties": properties,
        }

        return cypher_query.strip(), cypher_params


class BaseEntity(NodeBase):
    label: Literal["Entity"]
    name: str = Field(..., description="The specific name/identifier of the node")
    description: str = Field(
        ...,
        description="The description of the entity,\
             should be specific and descriptive such that it can be used to\
             understand what the entity is",
    )

    async def to_cypher(self, id: str, reference_id: str) -> tuple[str, dict[str, Any]]:
        properties = self.get_properties()
        properties.update({"name": self.name})
        text_to_embed = f"{self.name or ''} {self.description or ''}".strip()
        embedding = await embed_text([text_to_embed])
        properties.update({"embedding": embedding[0]})
        properties.update({"id": id})

        # Get sub_label dynamically since it's defined in subclasses
        sub_label = getattr(self, "sub_label", "Entity")
        cypher_query = f"""
        MERGE (r:Reference {{id: $reference_id}})
        MERGE (n:{self.label}:{sub_label} {{name: $name}})
        ON CREATE SET
          n += $properties
        ON MATCH SET
          n.id = $properties.id
        MERGE (r)-[:MENTIONS]->(n)
        RETURN n.id AS entity_id
        """

        cypher_params = {
            "name": self.name,
            "reference_id": reference_id,
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
             understand the relationship between the entities",
    )
    source_entity: Any  # Allow subclasses to override with specific entity types
    target_entity: Any  # Allow subclasses to override with specific entity types

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
        MERGE (s:{self.source_entity.label}:{self.source_entity.sub_label}\
             {{id: $source_id}})
        MERGE (t:{self.target_entity.label}:{self.target_entity.sub_label}\
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
            exclude={"name", "label", "source_entity", "target_entity"}
        )


def schema_factory(schema: Any) -> type[BaseModel]:
    class Graph(BaseModel):
        extracted_graphs: list[schema]  # type: ignore

        async def to_cypher(
            self, reference_id: str
        ) -> list[tuple[str, dict[str, Any]]]:
            # Collect all coroutines for parallel execution
            tasks = []

            for rel in self.extracted_graphs:
                source_id = str(uuid.uuid4())
                target_id = str(uuid.uuid4())
                # Add source entity cypher task
                tasks.append(rel.source_entity.to_cypher(source_id, reference_id))
                # Add target entity cypher task
                tasks.append(rel.target_entity.to_cypher(target_id, reference_id))
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
