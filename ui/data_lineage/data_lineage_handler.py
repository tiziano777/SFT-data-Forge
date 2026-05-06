import streamlit as st
import pandas as pd
from pyvis.network import Network
import streamlit.components.v1 as components
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.dataset_card_repository import DatasetCardRepository

# ============================================================
# CONFIGURAZIONE ESTETICA (Colori e Opacità)
# ============================================================

# Colori base per tipo di nodo (quando lo step non è applicabile, es. Card)
NODE_TYPE_COLORS = {
    "Card": "#00CCFF",         # Ciano
    "Dataset": "#44FF44",      # Verde (fallback)
    "Distribution": "#FFAA00"  # Arancio (fallback)
}

# Palette di maturità basata sullo STEP (1-3) - colori distinti per Dataset e Distribution
DATASET_STEP_COLORS = {
    1: "rgba(255,100,100,0.6)",   # datasetS1: red opaco chiaro
    2: "rgba(255,255,100,0.7)",   # datasetS2: yellow opaco
    3: "rgba(100,255,150,0.6)"    # datasetS3: verde opaco chiaro
}

DISTRIBUTION_STEP_COLORS = {
    1: "rgba(150,0,0,0.95)",      # DistributionS1: Red scuro acceso
    2: "rgba(180,160,0,0.95)",    # DistributionS2: yellow scuro acceso
    3: "rgba(0,150,0,0.95)"       # DistributionS3: verde scuro
}

# Colori per le nuove relazioni
EDGE_COLORS = {
    "ml:composes": "#AA00FF",                # Viola
    "ml:describes": "#00AAFF",               # Blu
    "ml:manifests_as": "#FF4444",            # Rosso
    "ml:datasetProcessedBy": "#FFAA00",      # Arancio (Inter-layer)
    "ml:refinedBy": "#FFFF00",               # Giallo (Intra-layer)
    "ml:distributionProcessedBy": "#00FFCC"  # Turchese (Inter-layer Dist)
}

# Funzione: recupera il lineage arricchito con i campi 'step' per i nodi
def fetch_lineage(db, node_id, node_type, max_depth):
    query = """
        WITH lineage AS (
            SELECT * FROM get_lineage_from_node(%s::UUID, %s::TEXT, %s::INTEGER)
        )
        SELECT 
            l.*,
            COALESCE(d.step, dist.step) as node_step,
            COALESCE(d_from.step, dist_from.step) as from_step
        FROM lineage l
        LEFT JOIN dataset d ON l.node_id = d.id AND l.node_type = 'Dataset'
        LEFT JOIN distribution dist ON l.node_id = dist.id AND l.node_type = 'Distribution'
        LEFT JOIN dataset d_from ON l.from_id = d_from.id AND l.from_type = 'Dataset'
        LEFT JOIN distribution dist_from ON l.from_id = dist_from.id AND l.from_type = 'Distribution';
    """
    with db as conn:
        res = conn.query(query, (node_id, node_type, max_depth))
        return [dict(r) for r in res]

# Funzione: recupera tutti i campi della riga corrispondente al nodo selezionato
def get_node_details(db, node_id, node_type):
    if not node_id:
        return None
    if node_type == 'Dataset':
        q = "SELECT * FROM dataset WHERE id = %s"
    elif node_type == 'Distribution':
        q = "SELECT * FROM distribution WHERE id = %s"
    else:
        q = "SELECT * FROM dataset_card WHERE id = %s"
    with db as conn:
        res = conn.query(q, (node_id,))
        rows = [dict(r) for r in res]
        return rows[0] if rows else None

# Funzione: costruisce il grafo pyvis applicando colori e shape custom
def build_graph(df):
    net = Network(height="100%", width="100%", bgcolor="#1e1e1e", font_color="white", directed=False)
    added_nodes = set()

    def add_styled_node(nid, ntype, nstep, nname=None):
        if not nid:
            return
        if nid in added_nodes:
            return
        # Scegliamo il colore: se c'è uno step usiamo la palette specifica per tipo, altrimenti il colore tipo
        if ntype == 'Dataset':
            base_color = DATASET_STEP_COLORS.get(nstep, NODE_TYPE_COLORS.get(ntype, "#888"))
        elif ntype == 'Distribution':
            base_color = DISTRIBUTION_STEP_COLORS.get(nstep, NODE_TYPE_COLORS.get(ntype, "#888"))
        else:
            base_color = NODE_TYPE_COLORS.get(ntype, "#888")

        # shape: distribution -> square, card -> diamond, dataset -> dot
        shape = 'square' if ntype == 'Distribution' else ('diamond' if ntype == 'Card' else 'dot')
        size = 25 if ntype == "Dataset" else 20

        # Label: preferiamo mostrare il nome (se presente) invece dell'UUID
        display_label = nname if nname else f"{ntype}"
        # Tooltip: includiamo ID, tipo, nome e step (ma non per Card)
        tooltip_lines = [f"ID: {nid}", f"Tipo: {ntype}"]
        if nname:
            tooltip_lines.append(f"Nome: {nname}")
        if ntype != 'Card':
            tooltip_lines.append(f"Step: {nstep if nstep else 'N/A'}")

        net.add_node(
            str(nid),
            label=display_label,
            color=base_color,
            title="\n".join(tooltip_lines),
            shape=shape,
            size=size
        )
        added_nodes.add(nid)

    for _, r in df.iterrows():
        add_styled_node(r.get("from_id"), r.get("from_type"), r.get("from_step"), r.get('from_name'))
        add_styled_node(r.get("node_id"), r.get("node_type"), r.get("node_step"), r.get('node_name'))
        if r.get("from_id"):
            net.add_edge(
                str(r.get("from_id")),
                str(r.get("node_id")),
                label=r.get("via_edge"),
                color=EDGE_COLORS.get(r.get("via_edge"), "#999999"),
                width=2
            )

    net.set_options("""
    var options = {
      "physics": {
        "forceAtlas2Based": { "gravitationalConstant": -50, "centralGravity": 0.01, "springLength": 100 },
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

def data_lineage_handler(st_app):
    db = st_app.session_state.db_manager
    st.title("🔍 ML Lineage Explorer")

    if 'lineage_history' not in st.session_state:
        st.session_state['lineage_history'] = []
    if 'lineage_index' not in st.session_state:
        st.session_state['lineage_index'] = -1
    if 'current_node_id' not in st.session_state:
        st.session_state['current_node_id'] = ''
    if 'current_node_type' not in st.session_state:
        st.session_state['current_node_type'] = 'Card'

    node_type = st.selectbox(
        "Tipo nodo",
        ["Card", "Dataset", "Distribution"],
        key='current_node_type'
    )

    dataset_repo = DatasetRepository(db)
    distribution_repo = DistributionRepository(db)
    card_repo = DatasetCardRepository(db)

    # Selezione dinamica del nodo
    if node_type == 'Card':
        cards = card_repo.get_all()
        options = [c.dataset_name for c in cards]
        label_to_id = {c.dataset_name: c.id for c in cards}
        selected = st.selectbox('Seleziona Card', ['--'] + options, index=0)
        if selected and selected != '--':
            st.session_state['current_node_id'] = label_to_id[selected]
    elif node_type == 'Dataset':
        datasets = dataset_repo.get_all()
        options = [d.name for d in datasets]
        label_to_id = {d.name: d.id for d in datasets}
        selected = st.selectbox('Seleziona Dataset', ['--'] + options, index=0)
        if selected and selected != '--':
            st.session_state['current_node_id'] = label_to_id[selected]
    else:
        dists = distribution_repo.get_all()
        options = [f"{(getattr(d,'name',None) or '')} ({d.uri})" for d in dists]
        label_to_id = {f"{(getattr(d,'name',None) or '')} ({d.uri})": d.id for d in dists}
        selected = st.selectbox('Seleziona Distribution', ['--'] + options, index=0)
        if selected and selected != '--':
            st.session_state['current_node_id'] = label_to_id[selected]

    # Controlli di navigazione: rimosso Prev/Next, manteniamo solo Reset
    if st.button("🔄 Reset history"):
        st.session_state['lineage_history'], st.session_state['lineage_index'] = [], -1
        st.rerun()

    max_depth = st.slider("Profondità massima (BFS)", 1, 25, 5)

    if st.button("🚀 Visualizza Lineage", type="primary"):
        if not st.session_state['current_node_id']:
            st.warning("Seleziona un nodo valido")
            return

        entry = (st.session_state['current_node_id'], st.session_state['current_node_type'])
        if not st.session_state['lineage_history'] or st.session_state['lineage_history'][-1] != entry:
            st.session_state['lineage_history'].append(entry)
            st.session_state['lineage_index'] = len(st.session_state['lineage_history']) - 1

        # Recupero dati arricchiti con Step
        rows = fetch_lineage(db, st.session_state['current_node_id'], st.session_state['current_node_type'], max_depth)

        if not rows:
            st.info("Nessuna relazione trovata per questo nodo.")
            return

        df = pd.DataFrame(rows)

        # Rimuoviamo auto-relazioni e doppioni dovuti a bidirezionalita':
        # normalizziamo gli id a stringhe e creiamo una chiave 'pair_key' ordinata
        df['from_id'] = df['from_id'].astype(str)
        df['node_id'] = df['node_id'].astype(str)
        df = df[df['from_id'] != df['node_id']]
        df['pair_key'] = df.apply(lambda r: '|'.join(sorted([r['from_id'], r['node_id']])), axis=1)
        df = df.drop_duplicates(subset=['pair_key', 'via_edge']).reset_index(drop=True)

        # Arricchimento: recuperiamo nomi/titoli per node_id e from_id tramite le repository
        def fetch_name(repo_db, rid, rtype):
            if not rid or rid == 'None':
                return None
            try:
                if rtype == 'Dataset':
                    obj = dataset_repo.get_by_id(rid)
                    return getattr(obj, 'name', None) if obj else None
                elif rtype == 'Distribution':
                    obj = distribution_repo.get_by_id(rid)
                    return (getattr(obj, 'name', None) or getattr(obj, 'uri', None)) if obj else None
                else:
                    obj = card_repo.get_by_id(rid)
                    return getattr(obj, 'dataset_name', None) if obj else None
            except Exception:
                return None

        df['node_name'] = df.apply(lambda r: fetch_name(db, r.get('node_id'), r.get('node_type')), axis=1)
        df['from_name'] = df.apply(lambda r: fetch_name(db, r.get('from_id'), r.get('from_type')), axis=1)

        # Recuperiamo per le distributions anche i campi `query` e `script`
        def fetch_dist_fields(rid):
            if not rid or rid == 'None':
                return (None, None)
            try:
                dist = distribution_repo.get_by_id(rid)
                if not dist:
                    return (None, None)
                return (getattr(dist, 'query', None), getattr(dist, 'script', None))
            except Exception:
                return (None, None)

        # Applichiamo solo quando il tipo è Distribution, altrimenti None
        node_dist_fields = df.apply(lambda r: fetch_dist_fields(r.get('node_id')) if r.get('node_type') == 'Distribution' else (None, None), axis=1)
        from_dist_fields = df.apply(lambda r: fetch_dist_fields(r.get('from_id')) if r.get('from_type') == 'Distribution' else (None, None), axis=1)

        df['node_query'] = [t[0] for t in node_dist_fields]
        df['node_script'] = [t[1] for t in node_dist_fields]
        df['from_query'] = [t[0] for t in from_dist_fields]
        df['from_script'] = [t[1] for t in from_dist_fields]

        # Costruiamo una mappa id->nome per l'uso nella visualizzazione del grafo
        name_map = {}
        for _, r in df.iterrows():
            nid = str(r.get('node_id'))
            fid = str(r.get('from_id'))
            if r.get('node_name'):
                name_map[nid] = r.get('node_name')
            if r.get('from_name'):
                name_map[fid] = r.get('from_name')

        tab1, tab2 = st.tabs(["🎨 Grafo Interattivo", "📋 Dati Lineage"])
        # Rimuoviamo lo slider per evitare rerun continui: usiamo un'altezza fissa
        graph_height = 700

        with tab1:
            # Passiamo anche la mappa dei nomi al builder del grafo
            net = build_graph(df)
            net.width = '100%'
            net.height = f"{graph_height}px"

            # Mostriamo l'HTML generato da pyvis senza iniezioni JS (comunicazione tra iframe rimossa)
            raw_html = net.generate_html()
            components.html(raw_html, height=graph_height + 50)

        with tab2:
            # Pulsante per scaricare il dataframe come CSV (con colonne query/script prefissate)
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⬇️ Scarica CSV",
                data=csv,
                file_name=f"lineage_{st.session_state.get('current_node_id', 'export')}.csv",
                mime='text/csv'
            )
            st.dataframe(df, use_container_width=True)

        # Se nella querystring è presente 'selected' mostriamo i dettagli del nodo
        params = st.query_params
        if 'selected' in params and params['selected']:
            sel = params['selected'][0]
            try:
                node_id, node_type = sel.split('|')
            except Exception:
                node_id, node_type = sel, st.session_state.get('current_node_type', 'Card')
            details = get_node_details(db, node_id, node_type)
            with st.expander("📋 Dettagli nodo selezionato", expanded=True):
                if details:
                    st.json(details)
                else:
                    st.info('Dettagli non trovati per il nodo selezionato.')



