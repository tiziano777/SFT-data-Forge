import pandas as pd
from pyvis.network import Network
import streamlit.components.v1 as components
from data_class.repository.table.recipe_repository import RecipeRepository

# ============================================================
# COLORI
# ============================================================

DEPTH_COLORS = {
    0: "#00CCFF",
    1: "rgba(80,220,130,0.9)",
    2: "rgba(60,180,100,0.85)",
    3: "rgba(40,140,80,0.8)",
    4: "rgba(20,100,60,0.75)",
}
DEFAULT_NODE_COLOR = "rgba(20,80,50,0.7)"

EDGE_COLOR = {
    "recipe:derivedFrom": "#FFAA00"
}

ROOT_COLOR = "#00CCFF"


# ============================================================
# DB HELPERS
# ============================================================

def fetch_lineage(db, node_id: str, max_depth: int) -> list[dict]:
    query = "SELECT * FROM get_recipe_lineage(%s::UUID, %s::INTEGER)"
    with db as conn:
        res = conn.query(query, (node_id, max_depth))
        return [dict(r) for r in res]


def get_node_details(db, node_id: str) -> dict | None:
    query = "SELECT * FROM recipe WHERE id = %s"
    with db as conn:
        res = conn.query(query, (node_id,))
        rows = [dict(r) for r in res]
        return rows[0] if rows else None


# ============================================================
# GRAPH BUILDER
# ============================================================

def build_graph(df: pd.DataFrame, root_id: str) -> Network:
    # FIX 1: directed=False per frecce non direzionate
    net = Network(height="700px", width="100%", bgcolor="#1e1e1e", font_color="white", directed=False)
    added = set()

    depth_map: dict[str, int] = {}
    for _, r in df.iterrows():
        nid = str(r.get("node_id"))
        depth = r.get("depth", 0)
        if nid not in depth_map or depth < depth_map[nid]:
            depth_map[nid] = depth

    def node_color(nid, depth):
        if nid == root_id:
            return ROOT_COLOR
        return DEPTH_COLORS.get(depth, DEFAULT_NODE_COLOR)

    def add_node(nid, name, scope, version, tags, depth, is_root=False):
        if nid in added:
            return
        color = node_color(nid, depth)
        label = name or str(nid)[:8]
        tooltip_parts = [f"ID: {nid}", f"Nome: {name}", f"Scope: {scope}", f"Versione: {version}"]
        if tags:
            tooltip_parts.append(f"Tags: {', '.join(tags)}")
        net.add_node(
            str(nid),
            label=label,
            color=color,
            title="\n".join(tooltip_parts),
            shape="dot",
            size=30 if is_root else 22,
            borderWidth=3 if is_root else 1
        )
        added.add(nid)

    root_as_from = df[df["from_id"].astype(str) == str(root_id)]
    if not root_as_from.empty:
        r0 = root_as_from.iloc[0]
        add_node(root_id, r0.get("from_name"), None, None, None, 0, is_root=True)
    else:
        add_node(root_id, None, None, None, None, 0, is_root=True)

    for _, r in df.iterrows():
        fid = str(r.get("from_id")) if r.get("from_id") else None
        nid = str(r.get("node_id"))
        depth = r.get("depth", 1)

        if fid:
            add_node(
                fid,
                r.get("from_name"), None, None, None,
                max(0, depth - 1),
                is_root=(fid == str(root_id))
            )
        add_node(
            nid,
            r.get("node_name"),
            r.get("node_scope"),
            r.get("node_version"),
            r.get("node_tags"),
            depth,
            is_root=(nid == str(root_id))
        )

        if fid:
            edge_col = EDGE_COLOR.get(r.get("via_edge"), "#999999")
            net.add_edge(str(r.get("node_id")), str(r.get("from_id")),
                label=r.get("via_edge", ""),
                color=edge_col,
                width=2
            )

    net.set_options("""
    var options = {
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -55,
          "centralGravity": 0.01,
          "springLength": 130
        },
        "minVelocity": 0.75,
        "solver": "forceAtlas2Based"
      },
      "interaction": { "hover": true, "navigationButtons": true }
    }
    """)
    return net


# ============================================================
# MAIN HANDLER
# ============================================================

def recipe_lineage_handler(st_app):
    db = st_app.session_state.db_manager
    st_app.title("🍳 Recipe Lineage Explorer")
    st_app.caption("Visualizza la catena di derivazione di una Recipe (versioni padre/figlio).")

    repo = RecipeRepository(db)

    all_recipes = repo.get_all()
    options = [r.name for r in all_recipes]
    label_to_id = {r.name: str(r.id) for r in all_recipes}

    selected_name = st_app.selectbox("Seleziona Recipe", ["--"] + options)
    selected_id = label_to_id.get(selected_name) if selected_name != "--" else None

    max_depth = st_app.slider("Hop massimi (BFS)", min_value=1, max_value=15, value=4)

    if st_app.button("🚀 Visualizza Lineage", type="primary", disabled=not selected_id):
        rows = fetch_lineage(db, selected_id, max_depth)

        if not rows:
            st_app.info("Nessuna relazione trovata: questa Recipe non ha antenati né discendenti.")
            details = get_node_details(db, selected_id)
            if details:
                with st_app.expander("📋 Dettagli Recipe", expanded=True):
                    st_app.json({k: str(v) for k, v in details.items()})
            return

        df = pd.DataFrame(rows)

        df["from_id"] = df["from_id"].astype(str)
        df["node_id"] = df["node_id"].astype(str)
        df = df[df["from_id"] != df["node_id"]]
        df["pair_key"] = df.apply(lambda r: "|".join(sorted([r["from_id"], r["node_id"]])), axis=1)
        df = df.drop_duplicates(subset=["pair_key", "via_edge"]).reset_index(drop=True)

        tab_graph, tab_data, tab_detail = st_app.tabs(["🎨 Grafo", "📋 Tabella Lineage", "🔍 Dettaglio Recipe"])

        with tab_graph:
            net = build_graph(df, selected_id)
            # FIX 2: altezza fissa nel componente HTML per evitare schiacciamento
            raw_html = net.generate_html()
            components.html(raw_html, height=750)

            st_app.markdown("""
            **Legenda colori nodo**: 🔵 Nodo selezionato &nbsp; 🟢 Derivazioni (più scuro = più lontano)
            """)

        with tab_data:
            display_cols = [
                "depth", "from_name", "via_edge", "node_name",
                "node_scope", "node_version", "node_tags", "node_issued", "node_modified"
            ]
            available = [c for c in display_cols if c in df.columns]
            st_app.dataframe(df[available], use_container_width=True)

        with tab_detail:
            details = get_node_details(db, selected_id)
            st_app.subheader(f"📄 {selected_name}")
            if details:
                col1, col2, col3 = st_app.columns(3)
                col1.metric("Versione", details.get("version", "N/A"))
                col2.metric("Scope", details.get("scope", "N/A"))
                col3.metric("Tasks", len(details.get("tasks") or []))

                st_app.text_area("Descrizione", value=details.get("description", ""), height=120, disabled=True)

                if details.get("tags"):
                    st_app.write("**Tags:**", " · ".join(details["tags"]))
                if details.get("tasks"):
                    st_app.write("**Tasks:**", " · ".join(details["tasks"]))

                with st_app.expander("Tutti i campi"):
                    st_app.json({k: str(v) for k, v in details.items()})




