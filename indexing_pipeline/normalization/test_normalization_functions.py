entity_normalization_query = """
MATCH (e1:Entity)
WHERE e1.embedding IS NOT NULL AND e1.id = $source_entity_id

// Get semantic candidates
CALL db.index.vector.queryNodes('entity_embeddings', 20, e1.embedding)
YIELD node AS e2, score AS semantic_score
WHERE e1 <> e2
  AND labels(e1)[-1] = labels(e2)[-1]
  AND semantic_score > $semantic_score_threshold
  // Lower threshold for initial filtering

// Calculate string similarity
WITH e1, e2, semantic_score,
     apoc.text.levenshteinSimilarity(e1.name, e2.name) AS string_similarity

//return e1.name, e2.name, semantic_score, string_similarity

OPTIONAL MATCH (e1)-[]-(shared_neighbor)-[]-(e2)
WITH e1, e2, semantic_score, string_similarity,
     count(DISTINCT shared_neighbor) AS shared_connections

OPTIONAL MATCH (e1)-[]-(n1)
WITH e1, e2, semantic_score, string_similarity, shared_connections,
     count(DISTINCT n1) AS e1_connections

OPTIONAL MATCH (e2)-[]-(n2)
WITH e1, e2, semantic_score, string_similarity, shared_connections, e1_connections,
     count(DISTINCT n2) AS e2_connections

// Calculate topology similarity
WITH e1, e2, semantic_score, string_similarity, shared_connections, e1_connections, e2_connections,
     CASE
       WHEN (e1_connections + e2_connections - shared_connections) > 0
       THEN shared_connections * 1.0 / (e1_connections + e2_connections - shared_connections)
       ELSE 0
     END AS topology_similarity

// calculated combined score

WITH e1, e2,
     (semantic_score * 0.5 + string_similarity * 0.4 + topology_similarity * 0.1) AS combined_score,
     semantic_score, string_similarity, topology_similarity


"""


test_query = """
match (e1:Entity)
where e1.id = '	dc3b0e57-d332-44da-8feb-0159a94fe51e'

with e1
match (e2:Entity)
where e2.id = '	c8e7633f-d75f-48eb-b55b-269af89f38fc'
WITH [e2, e1] as nodes  // First node is the target (kept)
CALL apoc.refactor.mergeNodes(nodes, {
    properties: {
        name: 'discard',     // Keep first node's value
        id: 'discard',        // Remove this property
        tags: 'combine',       // Combine arrays/values
        '.*': 'discard'        // Default for unspecified properties
    },
    mergeRels: false
})
YIELD node
RETURN node
"""
