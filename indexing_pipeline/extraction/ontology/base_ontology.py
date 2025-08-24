from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()


class NodeBase(BaseModel):
    def set_dynamic_field(self, field_name: str, value):
        """Set a field dynamically by bypassing Pydantic validation"""
        # Option 1: Using object.__setattr__()
        object.__setattr__(self, field_name, value)

        # Option 2: Direct __dict__ manipulation (alternative)
        # self.__dict__[field_name] = value

    """
    Base Pydantic class for creating graph nodes with name and label fields.
    Provides methods to generate Cypher queries using MERGE ON CREATE SET pattern
    to avoid node duplication.
    """

    name: str = Field(..., description="The specific name/identifier of the node")

    # async def to_cypher(self) -> tuple[str, dict[str, Any]]:
    #     """
    #     Generate a Cypher query to create a node using MERGE ON CREATE SET pattern.
    #     This avoids creating duplicate nodes with the same name and label.

    #     Args:
    #         additional_properties: Optional dictionary of
    #         additional properties to set on the node

    #     Returns:
    #         str: Cypher query string for creating the node
    #     """
    #     # Get all properties for the node
    #     properties = self.get_properties()
    #     properties.update({"name": self.name})

    #     # Generate the Cypher query with parameters
    #     cypher_query = f"""
    #     MERGE (n:{self.label} {{name: $name}})
    #     ON CREATE SET n += $properties
    #     RETURN n
    #     """

    #     cypher_params = {"name": self.name, "properties": properties}

    #     return cypher_query.strip(), cypher_params

    def get_properties(self):
        return self.model_dump(exclude={"name", "label"})
