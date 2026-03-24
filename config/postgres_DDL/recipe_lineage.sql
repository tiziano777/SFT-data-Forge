/* ============================================================
   RECIPE LINEAGE
   View delle relazioni padre/figlio + BFS ricorsivo
   ============================================================

   La tabella recipe ha un self-join via derived_from:
     - recipe padre  (derived_from IS NULL)
     - recipe figlia (derived_from = recipe_padre.id)

   Edge unico:
     recipe:derivedFrom  →  figlia → padre
   ============================================================ */

/* ----------------------------------------
   VIEW: v_recipe_edges
   ---------------------------------------- */
CREATE OR REPLACE VIEW v_recipe_edges AS

SELECT
    'Recipe'            AS source_type,
    r.id                AS source_id,
    'recipe:derivedFrom' AS edge_type,
    'Recipe'            AS target_type,
    r.derived_from      AS target_id
FROM recipe r
WHERE r.derived_from IS NOT NULL;


/* ----------------------------------------
   FUNCTION: get_recipe_lineage
   BFS bidirezionale con ciclo-guard (path array)
   ---------------------------------------- */
CREATE OR REPLACE FUNCTION get_recipe_lineage(
    p_node_id   UUID,
    p_max_depth INTEGER DEFAULT 5
)
RETURNS TABLE (
    depth           INTEGER,
    from_id         UUID,
    from_name       TEXT,
    via_edge        TEXT,
    node_id         UUID,
    node_name       TEXT,
    node_description TEXT,
    node_scope      TEXT,
    node_tasks      TEXT[],
    node_tags       TEXT[],
    node_version    TEXT,
    node_issued     TIMESTAMPTZ,
    node_modified   TIMESTAMPTZ
)
LANGUAGE sql
AS $$
WITH RECURSIVE graph_path AS (

    -- Root
    SELECT
        0                   AS depth,
        NULL::UUID          AS from_id,
        NULL::TEXT          AS via_edge,
        p_node_id           AS node_id,
        ARRAY[p_node_id]    AS visited

    UNION ALL

    -- Navigazione bidirezionale (salgo verso padre, scendo verso figli)
    SELECT
        gp.depth + 1,
        gp.node_id,
        e.edge_type,
        CASE
            WHEN gp.node_id = e.source_id THEN e.target_id
            ELSE e.source_id
        END,
        gp.visited || CASE
            WHEN gp.node_id = e.source_id THEN e.target_id
            ELSE e.source_id
        END
    FROM graph_path gp
    JOIN v_recipe_edges e
      ON gp.node_id = e.source_id
      OR gp.node_id = e.target_id
    WHERE gp.depth < p_max_depth
      AND NOT (
          CASE WHEN gp.node_id = e.source_id THEN e.target_id ELSE e.source_id END
      ) = ANY(gp.visited)
)
SELECT
    gp.depth,
    gp.from_id,
    r_from.name         AS from_name,
    gp.via_edge,
    gp.node_id,
    r.name              AS node_name,
    r.description       AS node_description,
    r.scope             AS node_scope,
    r.tasks             AS node_tasks,
    r.tags              AS node_tags,
    r.version           AS node_version,
    r.issued            AS node_issued,
    r.modified          AS node_modified
FROM graph_path gp
JOIN recipe r           ON r.id = gp.node_id
LEFT JOIN recipe r_from ON r_from.id = gp.from_id
WHERE gp.depth > 0;
$$;


/* ----------------------------------------
   ESEMPIO DI UTILIZZO
   ----------------------------------------
   SELECT * FROM get_recipe_lineage(
       '<uuid-della-recipe-radice>'::UUID,
       5   -- hop massimi
   );
   ---------------------------------------- */