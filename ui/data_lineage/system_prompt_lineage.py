import pandas as pd
import streamlit as st
from pyvis.network import Network
import streamlit.components.v1 as components
from data_class.repository.table.system_prompt_repository import SystemPromptRepository

# ============================================================
# COLORI
# ============================================================

st.markdown("""
<style>
    iframe {
        border: none !important;
        padding: 0 !important;
        margin: 0 !important;
        width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)

# Qualità del prompt → colore nodo (verde = alta qualità)
def select_color(deleted):
    if deleted:
        return "#FF4444"
    return "#04E804"    

EDGE_COLOR = {
    "sp:derivedFrom": "#AA66FF"
}

ROOT_COLOR = "#00CCFF"   # nodo radice selezionato


# ============================================================
# DB HELPERS
# ============================================================

def fetch_lineage(db, node_id: str, max_depth: int) -> list[dict]:
    query = """
        SELECT * FROM get_system_prompt_lineage(%s::UUID, %s::INTEGER)
    """
    with db as conn:
        res = conn.query(query, (node_id, max_depth))
        return [dict(r) for r in res]


def get_node_details(db, node_id: str) -> dict | None:
    query = "SELECT * FROM system_prompt WHERE id = %s"
    with db as conn:
        res = conn.query(query, (node_id,))
        rows = [dict(r) for r in res]
        return rows[0] if rows else None


# ============================================================
# GRAPH BUILDER - VERSIONE NON DIREZIONATA
# ============================================================

def build_graph(df: pd.DataFrame, root_id: str) -> Network:
    net = Network(height="100%", width="100%", bgcolor="#1e1e1e", font_color="white", directed=False)
    added = set()
    
    # Crea un dizionario per memorizzare tutti gli attributi di ogni nodo
    node_attrs = {}
    
    # Prima raccogli tutti gli attributi dal dataframe
    for _, r in df.iterrows():
        # Nodo FROM
        if r.get("from_id"):
            fid = str(r.get("from_id"))
            if fid not in node_attrs and fid != 'None':
                node_attrs[fid] = {
                    'name': r.get("from_name"),
                    'quality': None,  # FROM potrebbe non avere questi dati nella query
                    'version': None,
                    'lang': None,
                    'deleted': False
                }
        
        # Nodo TO (node_id)
        nid = str(r.get("node_id"))
        if nid not in node_attrs and nid != 'None':
            node_attrs[nid] = {
                'name': r.get("node_name"),
                'quality': r.get("node_quality"),
                'version': r.get("node_version"),
                'lang': r.get("node_lang"),
                'deleted': r.get("node_deleted", False)
            }
    
    def add_node(nid, is_root=False):
        if nid in added or nid == 'None':
            return
        
        attrs = node_attrs.get(nid, {})
        color = ROOT_COLOR if is_root else select_color(attrs.get('deleted', False))
        border = "#FF4444" if attrs.get('deleted', False) else "#FFFFFF"
        
        label = attrs.get('name') or str(nid)[:8]
        
        tooltip_lines = [
            f"ID: {nid}",
            f"Nome: {attrs.get('name', 'N/A')}",
        ]
        
        if attrs.get('version') is not None:
            tooltip_lines.append(f"Versione: {attrs['version']}")
        if attrs.get('quality') is not None:
            tooltip_lines.append(f"Qualità: {attrs['quality']}")
        if attrs.get('lang') is not None:
            tooltip_lines.append(f"Lingua: {attrs['lang']}")
        tooltip_lines.append(f"Eliminato: {attrs.get('deleted', False)}")
        
        net.add_node(
            str(nid),
            label=label,
            color={"background": color, "border": border},
            title="\n".join(tooltip_lines),
            shape="dot",
            size=28 if is_root else 20,
            borderWidth=3 if is_root else 1
        )
        added.add(nid)
    
    # Aggiungi tutti i nodi raccolti
    for nid in node_attrs.keys():
        add_node(nid, is_root=(nid == str(root_id)))
    
    # Aggiungi gli archi (ora senza frecce direzionali)
    for _, r in df.iterrows():
        fid = str(r.get("from_id")) if r.get("from_id") else None
        nid = str(r.get("node_id"))
        
        if fid and fid != 'None' and nid != 'None' and fid != nid:
            edge_col = EDGE_COLOR.get(r.get("via_edge"), "#999999")
            net.add_edge(
                fid,
                nid,
                label=r.get("via_edge", ""),
                color=edge_col,
                width=2,
                arrows=None  # <-- IMPORTANTE: Nessuna freccia
            )
    
    # Configurazione physics
    net.set_options("""
    var options = {
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.01,
          "springLength": 120,
          "springConstant": 0.08
        },
        "solver": "forceAtlas2Based",
        "stabilization": {
          "enabled": true,
          "iterations": 1000,
          "updateInterval": 25
        },
        "minVelocity": 0.75
      },
      "interaction": {
        "hover": true,
        "navigationButtons": true,
        "tooltipDelay": 200,
        "hideEdgesOnDrag": false
      },
      "edges": {
        "smooth": {
          "enabled": true,
          "type": "continuous"
        }
      }
    }
    """)
    return net


# ============================================================
# MAIN HANDLER
# ============================================================

def system_prompt_lineage_handler(st_app):
    db = st_app.session_state.db_manager
    st_app.title("🧠 System Prompt Lineage Explorer")
    st_app.caption("Visualizza la catena di derivazione di un System Prompt (grafo non direzionato)")

    repo = SystemPromptRepository(db)

    # --- Selezione nodo ---
    all_prompts = repo.get_all()
    options = [sp.name for sp in all_prompts if not sp.deleted]
    label_to_id = {sp.name: str(sp.id) for sp in all_prompts if not sp.deleted}

    selected_name = st_app.selectbox("Seleziona System Prompt", ["--"] + options)
    selected_id = label_to_id.get(selected_name) if selected_name != "--" else None

    max_depth = st_app.slider("Hop massimi (BFS)", min_value=1, max_value=15, value=4)

    # Aggiungi opzione per mostrare/nascondere le etichette degli archi
    show_edge_labels = st_app.checkbox("Mostra etichette relazioni", value=True)

    if st_app.button("🚀 Visualizza Lineage", type="primary", disabled=not selected_id):
        rows = fetch_lineage(db, selected_id, max_depth)

        if not rows:
            st_app.info("Nessuna relazione trovata: questo System Prompt non ha antenati né discendenti.")
            # Mostriamo comunque i dettagli del nodo selezionato
            details = get_node_details(db, selected_id)
            if details:
                with st_app.expander("📋 Dettagli nodo", expanded=True):
                    st_app.json(details)
            return

        df = pd.DataFrame(rows)

        # Deduplica archi (bidirezionalità → stessa coppia può apparire 2 volte)
        df["from_id"] = df["from_id"].astype(str)
        df["node_id"] = df["node_id"].astype(str)
        df = df[df["from_id"] != df["node_id"]]
        df["pair_key"] = df.apply(lambda r: "|".join(sorted([r["from_id"], r["node_id"]])), axis=1)
        df = df.drop_duplicates(subset=["pair_key", "via_edge"]).reset_index(drop=True)

        tab_graph, tab_data, tab_detail = st_app.tabs(["🎨 Grafo", "📋 Tabella Lineage", "🔍 Dettaglio Nodo"])

        with tab_graph:
            graph_height = 700
            net = build_graph(df, selected_id)
            
            # Se l'utente non vuole vedere le etichette, le rimuoviamo
            if not show_edge_labels:
                for edge in net.edges:
                    edge['label'] = ''
            
            html = net.generate_html()
            components.html(html, height=graph_height, width=None)

            # Legenda colori
            col1, col2, col3 = st_app.columns(3)
            with col1:
                st_app.markdown("🔵 **Nodo radice** (selezionato)")
            with col2:
                st_app.markdown("🟢 **Prompt attivo**")
            with col3:
                st_app.markdown("🔴 **Prompt eliminato**")
            
            st_app.markdown("💜 **Relazione:** `sp:derivedFrom`")

        with tab_data:
            display_cols = [
                "depth", "from_name", "via_edge", "node_name",
                "node_version", "node_quality", "node_lang", "node_deleted"
            ]
            available = [c for c in display_cols if c in df.columns]
            st_app.dataframe(df[available], use_container_width=True)

        with tab_detail:
            # Mostra dettagli del nodo radice selezionato
            details = get_node_details(db, selected_id)
            st_app.subheader(f"📄 {selected_name}")
            if details:
                col1, col2, col3 = st_app.columns(3)
                col1.metric("Versione", details.get("version", "N/A"))
                col2.metric("Qualità", f"{details.get('quality_score', 0):.2f}")
                col3.metric("Lingua", details.get("_lang", "N/A"))
                st_app.text_area("Prompt", value=details.get("prompt", ""), height=200, disabled=True)
                with st_app.expander("Tutti i campi"):
                    st_app.json({k: str(v) for k, v in details.items()})