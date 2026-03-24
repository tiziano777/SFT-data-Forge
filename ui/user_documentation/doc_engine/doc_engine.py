import streamlit as st
from pathlib import Path

DOC_ROOT = Path(__file__).resolve().parents[1] / "docs"


def scan_docs():
    if not DOC_ROOT.exists():
        return []
    docs = []
    for folder in sorted(DOC_ROOT.iterdir()):
        if not folder.is_dir():
            continue
        md_files = list(folder.glob("*.md"))
        if not md_files:
            continue
        file = md_files[0]  # un solo .md per cartella
        slug = folder.name
        label = slug.split("_", 1)[-1].replace("_", " ").title()
        docs.append({
            "slug": slug,
            "label": label,
            "path": file,
        })
    return docs


def render_page(docs):
    if not docs:
        st.error("Nessun documento trovato in docs/")
        return

    if "doc_slug" not in st.session_state:
        st.session_state["doc_slug"] = docs[0]["slug"]

    # Bottoni stilizzati come testo cliccabile in riga
    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] button {
        background: none !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0 !important;
        font-size: 15px !important;
        color: #555 !important;
        cursor: pointer !important;
    }
    div[data-testid="stHorizontalBlock"] button:hover {
        color: #1f77b4 !important;
        text-decoration: underline !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # Riga navigazione capitoli
    cols = st.columns(len(docs))
    for i, doc in enumerate(docs):
        with cols[i]:
            is_active = doc["slug"] == st.session_state["doc_slug"]
            label = f"**{doc['label']}**" if is_active else doc["label"]
            if st.button(label, key=f"nav_{doc['slug']}", use_container_width=True):
                st.session_state["doc_slug"] = doc["slug"]
                st.rerun()

    st.markdown("---")

    # Documento corrente
    current = next((d for d in docs if d["slug"] == st.session_state["doc_slug"]), docs[0])

    try:
        content = current["path"].read_text(encoding="utf-8").replace('\r\n', '\n')
    except Exception as e:
        st.error(f"Errore lettura file: {e}")
        return

    st.markdown(content)

    try:
        content = current["path"].read_text(encoding="utf-8").replace('\r\n', '\n')
    except Exception as e:
        st.error(f"Errore lettura file: {e}")
        return

    # --- LOGICA PER IL TASTO DOWNLOAD DINAMICO ---
    TAG_DOWNLOAD = "[DOWNLOAD_TOOLKIT_PYTHON]"
    
    if TAG_DOWNLOAD in content:
        # Dividiamo il testo in ciò che viene prima e dopo il tag
        parts = content.split(TAG_DOWNLOAD)
        
        # Mostriamo la prima parte
        st.markdown(parts[0])
        
        # Inseriamo il vero tasto Streamlit
        # Assicurati che il path del file sia corretto rispetto alla root
        path_to_script = Path("ui/user_documentation/user_intervention_project_download.zip") 
        
        if path_to_script.exists():
            with open(path_to_script, "rb") as f:
                st.download_button(
                    label="📥 Scarica Python Example Code",
                    data=f,
                    file_name="ui/user_documentation/user_intervention_project_download.zip",
                    mime="application/zip",
                    use_container_width=True
                )
        else:
            st.warning(f"File sorgente non trovato nel path: {path_to_script}")
            
        # Mostriamo la parte restante del documento
        st.markdown(parts[1])
    else:
        # Se non c'è il tag, renderizziamo normalmente
        st.markdown(content)

    # --- Navigazione sequenziale ---
    current_idx = next(i for i, d in enumerate(docs) if d["slug"] == current["slug"])
    st.markdown("---")
    col_prev, col_mid, col_next = st.columns([1, 3, 1])

    with col_prev:
        if current_idx > 0:
            prev = docs[current_idx - 1]
            if st.button(f"⬅ {prev['label']}", key="nav_prev", use_container_width=True):
                st.session_state["doc_slug"] = prev["slug"]
                st.rerun()

    with col_next:
        if current_idx < len(docs) - 1:
            nxt = docs[current_idx + 1]
            if st.button(f"{nxt['label']} ➡", key="nav_next", use_container_width=True):
                st.session_state["doc_slug"] = nxt["slug"]
                st.rerun()