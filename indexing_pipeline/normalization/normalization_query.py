CHECK_ENTITY_EXISTS_QUERY = """
MATCH (n {id: $source_entity_id})
RETURN COUNT(n) > 0 AS node_exists
"""

ENTITY_NORMALIZATION_QUERY = """
MATCH (e1:Entity)
WHERE e1.embedding IS NOT NULL AND e1.id = $source_entity_id

// Get semantic candidates
CALL db.index.vector.queryNodes('entity_embeddings', 20, e1.embedding)
YIELD node AS e2, score AS semantic_score
WHERE e1 <> e2
  AND labels(e1)[-1] = labels(e2)[-1]
  AND semantic_score > $semantic_score_threshold

// Calculate string similarity
WITH e1, e2, semantic_score,
     apoc.text.levenshteinSimilarity(e1.name, e2.name) AS string_similarity

// Calculate topology similarity
OPTIONAL MATCH (e1)-[]-(shared_neighbor)-[]-(e2)
WITH e1, e2, semantic_score, string_similarity,
     count(DISTINCT shared_neighbor) AS shared_connections

OPTIONAL MATCH (e1)-[]-(n1)
WITH e1, e2, semantic_score, string_similarity, shared_connections,
     count(DISTINCT n1) AS e1_connections

OPTIONAL MATCH (e2)-[]-(n2)
WITH e1, e2, semantic_score, string_similarity, shared_connections, e1_connections,
     count(DISTINCT n2) AS e2_connections

WITH e1, e2, semantic_score, string_similarity,\
     shared_connections, e1_connections, e2_connections,
     CASE
       WHEN (e1_connections + e2_connections - shared_connections) > 0
       THEN shared_connections * 1.0 / (e1_connections\
         + e2_connections - shared_connections)
       ELSE 0
     END AS topology_similarity

// Calculate combined score
WITH e1, e2,
     (semantic_score * 0.5 + string_similarity * 0.4\
         + topology_similarity * 0.1) AS combined_score,
     semantic_score, string_similarity, topology_similarity

// Filter by combined score threshold and collect nodes to merge
WHERE combined_score > $combined_score_threshold\
     OR string_similarity > $string_similarity_threshold
WITH e1, collect(e2) AS nodes_to_merge,
     collect({
         node: e2,
         combined_score: combined_score,
         semantic_score: semantic_score,
         string_similarity: string_similarity,
         topology_similarity: topology_similarity
     }) AS merge_details

// Only proceed if there are nodes to merge
WHERE size(nodes_to_merge) > 0

// Prepare nodes list with original node first (to keep it)
WITH e1, nodes_to_merge, merge_details,
     [e1] + nodes_to_merge AS all_nodes

// Perform the merge operation
CALL apoc.refactor.mergeNodes(all_nodes, {
    properties: {
        name: 'discard',     // Keep original node's name
        id: 'discard',       // Keep original node's id
        embedding: 'discard', // Keep original embedding
        tags: 'combine'       // Combine arrays/values
    },
    mergeRels: false  // Handle relationships separately if needed
})
YIELD node AS merged_node

// Return results
RETURN
    merged_node.id AS entity_id,
    merged_node AS entity,
    size(nodes_to_merge) AS nodes_merged_count,
    [detail IN merge_details | {
        merged_node_id: detail.node.id,
        combined_score: detail.combined_score,
        semantic_score: detail.semantic_score,
        string_similarity: detail.string_similarity,
        topology_similarity: detail.topology_similarity
    }] AS merge_details
"""

MERGE_DUPLICATE_RELATIONSHIPS_QUERY = """
MATCH (e:Entity)
WHERE e.id = $source_entity_id

// Handle outgoing relationships from E
MATCH (e)-[r]->(target)
WITH e, target, type(r) as rel_type, r.name as rel_name,
     collect(r) as same_rels
WHERE size(same_rels) > 1 AND rel_name IS NOT NULL

// Keep primary relationship and delete duplicates
WITH e, target, rel_type, rel_name, same_rels,
     same_rels[0] as keep_rel,
     same_rels[1..] as delete_rels

// Mark the kept relationship as normalized
SET keep_rel.merged_at = datetime()

// Delete duplicate relationships
FOREACH (rel IN delete_rels | DELETE rel)

// Handle incoming relationships to E (same logic)
WITH e
MATCH (source)-[r]->(e)
WITH e, source, type(r) as rel_type, r.name as rel_name,
     collect(r) as same_rels
WHERE size(same_rels) > 1 AND rel_name IS NOT NULL

WITH e, source, rel_type, rel_name, same_rels,
     same_rels[0] as keep_rel,
     same_rels[1..] as delete_rels

SET keep_rel.merged_at = datetime()

FOREACH (rel IN delete_rels | DELETE rel)

RETURN count(*) as duplicate_relationships_merged
"""

MERGE_MENTION_RELATIONSHIPS_QUERY = """
MATCH (e:Entity)
WHERE e.id = $source_entity_id

MATCH (ref:Reference)-[mentions:MENTIONS]-(e)
WITH e, collect(mentions) as all_mentions
WHERE size(all_mentions) > 1

// Keep the first mention relationship only
WITH e, all_mentions,
     all_mentions[0] as primary_mention,
     all_mentions[1..] as other_mentions

// Mark primary mention as normalized
SET primary_mention.merged_at = datetime()

// Delete other mention relationships
FOREACH (rel IN other_mentions | DELETE rel)

RETURN count(*) as mention_relationships_merged
"""

MERGE_SIMILAR_EMBEDDING_RELATIONSHIPS_QUERY = """
MATCH (e:Entity)
WHERE e.id = $source_entity_id

// Find relationships with embeddings (outgoing)
MATCH (e)-[r1]->(target)
WHERE r1.embedding IS NOT NULL
MATCH (e)-[r2]->(target)
WHERE r2.embedding IS NOT NULL
  AND elementId(r1) < elementId(r2)
  AND type(r1) = type(r2)

// Calculate cosine similarity between embeddings
WITH e, target, r1, r2,
     gds.similarity.cosine(r1.embedding, r2.embedding) as embedding_similarity
WHERE embedding_similarity > $embedding_similarity_threshold

// Group relationships by similarity
WITH e, target, type(r1) as rel_type,
     collect([r1, r2]) as similar_rel_pairs

// For each similar pair, merge them
UNWIND similar_rel_pairs as rel_pair
WITH e, target, rel_type, rel_pair[0] as keep_rel, rel_pair[1] as merge_rel

// Keep primary relationship properties as-is
SET keep_rel.embedding_merged = true,
    keep_rel.merged_at = datetime()

// Delete the merged relationship
DELETE merge_rel

// Handle incoming relationships with embeddings (same logic)
WITH e
MATCH (source)-[r1]->(e)
WHERE r1.embedding IS NOT NULL
MATCH (source)-[r2]->(e)
WHERE r2.embedding IS NOT NULL
  AND elementId(r1) < elementId(r2)
  AND type(r1) = type(r2)

WITH e, source, r1, r2,
     gds.similarity.cosine(r1.embedding, r2.embedding) as embedding_similarity
WHERE embedding_similarity > $embedding_similarity_threshold

WITH e, source, type(r1) as rel_type,
     collect([r1, r2]) as similar_rel_pairs

UNWIND similar_rel_pairs as rel_pair
WITH e, source, rel_type, rel_pair[0] as keep_rel, rel_pair[1] as merge_rel

SET keep_rel.embedding_merged = true,
    keep_rel.merged_at = datetime()

DELETE merge_rel

RETURN count(*) as embedding_relationships_merged
"""
