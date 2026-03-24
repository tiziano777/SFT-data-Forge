/* ============================================================
   SYSTEM PROMPT LINEAGE
   View delle relazioni padre/figlio + BFS ricorsivo
   ============================================================

   La tabella system_prompt ha un self-join via derived_from:
     - sp_parent (derived_from IS NULL o punta a un antenato)
     - sp_child  (derived_from = sp_parent.id)

   Edge unico:
     sp:derivedFrom  →  figlio → padre (derived_from)
   ============================================================ */

/* ----------------------------------------
   VIEW: v_system_prompt_edges
   Normalizza le relazioni come grafo diretto
   ---------------------------------------- */
CREATE OR REPLACE VIEW v_system_prompt_edges AS

-- Un system prompt è derivato da un altro (figlio → padre)
SELECT
    'SystemPrompt'          AS source_type,
    sp.id                   AS source_id,
    'sp:derivedFrom'        AS edge_type,
    'SystemPrompt'          AS target_type,
    sp.derived_from         AS target_id
FROM system_prompt sp
WHERE sp.derived_from IS NOT NULL
  AND sp.deleted = FALSE;


/* ----------------------------------------
   FUNCTION: get_system_prompt_lineage
   BFS bidirezionale con ciclo-guard (path array)
   ---------------------------------------- */
CREATE OR REPLACE FUNCTION get_system_prompt_lineage(
    p_node_id   UUID,
    p_max_depth INTEGER DEFAULT 5
)
RETURNS TABLE (
    depth       INTEGER,
    from_id     UUID,
    from_name   TEXT,
    via_edge    TEXT,
    node_id     UUID,
    node_name   TEXT,
    node_version TEXT,
    node_quality DECIMAL(3,2),
    node_lang   TEXT,
    node_deleted BOOLEAN
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
    JOIN v_system_prompt_edges e
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
    sp_from.name        AS from_name,
    gp.via_edge,
    gp.node_id,
    sp.name             AS node_name,
    sp.version          AS node_version,
    sp.quality_score    AS node_quality,
    sp._lang            AS node_lang,
    sp.deleted          AS node_deleted
FROM graph_path gp
JOIN system_prompt sp       ON sp.id = gp.node_id
LEFT JOIN system_prompt sp_from ON sp_from.id = gp.from_id
WHERE gp.depth > 0;
$$;


/* ----------------------------------------
   ESEMPIO DI UTILIZZO
   ----------------------------------------
   SELECT * FROM get_system_prompt_lineage(
       '<uuid-del-nodo-radice>'::UUID,
       5   -- hop massimi
   );
   ---------------------------------------- */