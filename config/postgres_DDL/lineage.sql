--- ============================================================
---VIEW: v_graph_edges
---   Unifica le relazioni con logica Inter-layer e Intra-layer
--- ============================================================ 
CREATE OR REPLACE VIEW v_graph_edges AS

-- 1. Card -> Card (composition) 
-- Trasformiamo i nomi in UUID per coerenza
SELECT
    'Card' AS source_type, parent.id AS source_id,
    'ml:composes' AS edge_type,
    'Card' AS target_type, child.id AS target_id
FROM card_composition cc
JOIN dataset_card parent ON parent.dataset_name = cc.parent_card_name
JOIN dataset_card child  ON child.dataset_name = cc.child_card_name

UNION ALL

-- 2. Card -> Dataset (describes)
SELECT
    'Card', dc.id, 'ml:describes', 'Dataset', d.id
FROM dataset_card dc
JOIN dataset d ON d.derived_card = dc.id

UNION ALL

-- 3. Dataset -> Distribution (manifests_as)
SELECT
    'Dataset', d.id, 'ml:manifests_as', 'Distribution', dist.id
FROM dataset d
JOIN distribution dist ON dist.dataset_id = d.id

UNION ALL

-- 4. A) Inter-layer datasets (processedBy)
-- DatasetA deriva da DatasetB se stepA > stepB
SELECT
    'Dataset', d_a.id, 'ml:datasetProcessedBy', 'Dataset', d_b.id
FROM dataset d_a
JOIN dataset d_b ON d_a.derived_dataset = d_b.id
WHERE d_a.step > d_b.step

UNION ALL

-- 5. B) Intra-layer Distribution (refinedBy)
-- Stesso step, stessa card di origine
SELECT
    'Distribution', dist_a.id, 'ml:refinedBy', 'Distribution', dist_b.id
FROM distribution dist_a
JOIN distribution dist_b ON dist_a.derived_from = dist_b.id
JOIN dataset d_a ON dist_a.dataset_id = d_a.id
WHERE dist_a.step = dist_b.step 
  AND d_a.derived_card IS NOT NULL

UNION ALL

-- 6. C) Inter-layer Distribution (distributionProcessedBy)
-- Step diverso
SELECT
    'Distribution', dist_a.id, 'ml:distributionProcessedBy', 'Distribution', dist_b.id
FROM distribution dist_a
JOIN distribution dist_b ON dist_a.derived_from = dist_b.id
WHERE dist_a.step > dist_b.step;

CREATE OR REPLACE FUNCTION get_lineage_from_node(
    p_node_id   UUID,
    p_node_type TEXT,
    p_max_depth INTEGER DEFAULT 5
)
RETURNS TABLE (
    depth       INTEGER,
    from_type   TEXT,
    from_id     UUID,
    via_edge    TEXT,
    node_type   TEXT,
    node_id     UUID
)
LANGUAGE sql
AS $$
WITH RECURSIVE graph_path AS (
    -- Root del traversal
    SELECT
        0 AS depth,
        NULL::TEXT AS from_type,
        NULL::UUID AS from_id,
        NULL::TEXT AS via_edge,
        p_node_type AS node_type,
        p_node_id AS node_id,
        ARRAY[(p_node_type, p_node_id)::TEXT] AS path

    UNION ALL

    -- Navigazione bi-direzionale
    SELECT
        gp.depth + 1,
        gp.node_type,
        gp.node_id,
        e.edge_type,
        -- Se il mio nodo corrente è il source, vado al target. Se è il target, vado al source.
        CASE WHEN gp.node_id = e.source_id THEN e.target_type ELSE e.source_type END,
        CASE WHEN gp.node_id = e.source_id THEN e.target_id ELSE e.source_id END,
        gp.path || (CASE WHEN gp.node_id = e.source_id THEN e.target_type ELSE e.source_type END, 
                    CASE WHEN gp.node_id = e.source_id THEN e.target_id ELSE e.source_id END)::TEXT
    FROM graph_path gp
    JOIN v_graph_edges e 
      ON (gp.node_id = e.source_id AND gp.node_type = e.source_type)
      OR (gp.node_id = e.target_id AND gp.node_type = e.target_type)
    WHERE gp.depth < p_max_depth
      AND NOT (CASE WHEN gp.node_id = e.source_id THEN e.target_type ELSE e.source_type END, 
               CASE WHEN gp.node_id = e.source_id THEN e.target_id ELSE e.source_id END)::TEXT = ANY(gp.path)
)
SELECT depth, from_type, from_id, via_edge, node_type, node_id 
FROM graph_path 
WHERE depth > 0;
$$;



---------------------------------------

--- ============================================================
---   RECIPE LINEAGE
---   View delle relazioni padre/figlio + BFS ricorsivo
---   ============================================================

---   La tabella recipe ha un self-join via derived_from:
---     - recipe padre  (derived_from IS NULL)
---     - recipe figlia (derived_from = recipe_padre.id)

---   Edge unico:
---     recipe:derivedFrom  →  figlia → padre
---   ============================================================ 

--- ----------------------------------------
---   VIEW: v_recipe_edges
------------------------------------------ 
CREATE OR REPLACE VIEW v_recipe_edges AS

SELECT
    'Recipe'            AS source_type,
    r.id                AS source_id,
    'recipe:derivedFrom' AS edge_type,
    'Recipe'            AS target_type,
    r.derived_from      AS target_id
FROM recipe r
WHERE r.derived_from IS NOT NULL;


----------------------------------------
---FUNCTION: get_recipe_lineage
-- BFS bidirezionale con ciclo-guard (path array)
---------------------------------------- 


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


--- ============================================================
---   SYSTEM PROMPT LINEAGE
---   View delle relazioni padre/figlio + BFS ricorsivo
---   ============================================================

---   La tabella system_prompt ha un self-join via derived_from:
---   - sp_parent (derived_from IS NULL o punta a un antenato)
---   - sp_child  (derived_from = sp_parent.id)

---   Edge unico:
---     sp:derivedFrom  →  figlio → padre (derived_from)
---   ============================================================ 

----------------------------------------
---   VIEW: v_system_prompt_edges
---   Normalizza le relazioni come grafo diretto
---------------------------------------- 

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


----------------------------------------
---   FUNCTION: get_system_prompt_lineage
---   BFS bidirezionale con ciclo-guard (path array)
----------------------------------------
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


----------------------------------------
--- ESEMPIO DI UTILIZZO
----------------------------------------
---   SELECT * FROM get_system_prompt_lineage(
---       '<uuid-del-nodo-radice>'::UUID,
---       5   -- hop massimi
---   );
---------------------------------------- 

