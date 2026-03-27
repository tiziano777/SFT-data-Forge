import os
import sys
import json
import subprocess

import streamlit as st

BASE_PREFIX = os.getenv("BASE_PREFIX", "file://")
BINDED_PROCESSED_DATA_DIR = os.getenv("BINDED_PROCESSED_DATA_DIR")

from data_class.repository.table.dataset_repository import DatasetRepository
from utils.path_utils import to_internal_path

def _get_distribution_info(st_app):
    """Recupera distribuzione corrente dalla session."""
    distribution = st_app.session_state.get("current_distribution")
    if not distribution:
        st_app.error("Nessuna distribuzione selezionata. Torna alla selezione distribuzioni.")
        return None
    return distribution

def _get_source_dataset(db_manager, source_distribution):
    """Recupera dataset sorgente dal DB."""
    try:
        dataset_repo = DatasetRepository(db_manager)
        return dataset_repo.get_by_id(source_distribution.dataset_id)
    except Exception as e:
        st.error(f"❌ Errore nel recupero del dataset sorgente: {e}")
        return None

def _get_languages_from_source_dataset(source_dataset):
    """Estrae lingue disponibili dal dataset sorgente."""
    default_languages = ["it", "en", "de", "es", "pt", "fr"]
    return source_dataset.languages if (source_dataset and source_dataset.languages) else default_languages

def _launch_preprocessing_worker(
    source_dataset_uri: str,
    source_distribution_uri: str,
    processed_base_uri: str,
    glob_pattern: str,
    source_dataset_id: str,
    source_distribution_id: str,
    default_metadata: dict,
    output_format: str = "jsonl.gz",
):
    """
    Lancia worker in background con parametri semplificati.
    
    Il worker riceverà solo:
    - URI completi (binded, pronti per DB)
    - IDs delle entità sorgente
    - Metadata base
    
    Il worker farà autonomamente:
    - Conversione scope (binded → container)
    - Estrazione path relativi
    - Costruzione struttura output
    - Creazione entità DB con URI binded
    """
    worker_script = os.path.join(os.path.dirname(__file__), "parallel_preprocessing_worker.py")

    worker_params = {
        "source_dataset_uri": source_dataset_uri,
        "source_distribution_uri": source_distribution_uri,
        "processed_base_uri": processed_base_uri,
        "glob_pattern": glob_pattern,
        "source_dataset_id": source_dataset_id,
        "source_distribution_id": source_distribution_id,
        "default_metadata": default_metadata,
        "output_format": output_format,
    }

    try:
        params_json = json.dumps(worker_params, default=str)
        subprocess.Popen(
            [sys.executable, worker_script, params_json],
            stdout=None,
            stderr=None,
            start_new_session=True,
            close_fds=True,
        )
        return True
    except Exception as e:
        print(f"Errore nel lancio del worker: {e}")
        return False

def show_parallel_preprocessing(st_app):
    st_app.markdown("## 🚀 Parallel Preprocessing - RAW to PROCESSED")
    st_app.markdown("---")

    st_app.sidebar.markdown("### Layer Transformation")
    st_app.sidebar.markdown(f"**Source Layer:** RAW")
    st_app.sidebar.markdown(f"**Target Layer:** PROCESSED")

    # Recupera distribuzione e dataset sorgente
    distribution = _get_distribution_info(st_app)
    if not distribution:
        if st_app.button("📂 Torna alla Selezione Distribution"):
            st_app.session_state.current_stage = "raw_distribution_selection"
            st_app.rerun()
        return

    source_dataset = _get_source_dataset(st_app.session_state.db_manager, distribution)
    if not source_dataset:
        st_app.error("❌ Impossibile recuperare il dataset sorgente")
        return

    available_languages = _get_languages_from_source_dataset(source_dataset)

    # ------------------------------------------------------------------
    # URI per il worker (già binded, pronti per DB)
    # ------------------------------------------------------------------
    source_dataset_uri = source_dataset.uri  # es: file:///Users/.../nfs/data-download/velvet_v1/allenai/ai2_arc
    source_distribution_uri = distribution.uri  # es: file:///Users/.../nfs/data-download/velvet_v1/allenai/ai2_arc/ARC-Challenge
    processed_base_uri = f"{BASE_PREFIX}{BINDED_PROCESSED_DATA_DIR}"  # es: file:///Users/.../nfs/processed-data

    # ------------------------------------------------------------------
    # Mostra trasformazione layer
    # ------------------------------------------------------------------
    st_app.subheader("🔄 Trasformazione Layer")
    col1, col2 = st_app.columns(2)

    with col1:
        st_app.markdown("### 📤 Sorgente (RAW)")
        st_app.write(f"**Dataset ID:** {distribution.dataset_id}")
        st_app.write(f"**Dataset Name:** {source_dataset.name}")
        st_app.write(f"**Dataset URI:** `{source_dataset_uri}`")
        st_app.write(f"**Distribution URI:** `{source_distribution_uri}`")
        lang_label = ", ".join(available_languages) if available_languages else "Default [it, en, de, es, pt, fr]"
        st_app.write(f"**Lingue:** {lang_label}")

    with col2:
        st_app.markdown("### 📥 Destinazione (PROCESSED)")
        st_app.write(f"**Nome Dataset:** `processed__{source_dataset.name}`")
        st_app.write(f"**Base URI:** `{processed_base_uri}`")

    st_app.markdown("---")
    st_app.subheader("📋 Metadati Iniziali")

    distribution_name = "/".join(st_app.session_state.get("selected_path_parts", []))

    # Converte URI binded in path container per l'adapter
    source_dataset_path_container = to_internal_path(source_dataset_uri).replace(BASE_PREFIX, "")
    
    default_metadata = {
        "_dataset_name": source_dataset.name,
        "_dataset_uri": source_dataset.uri,
        "_dataset_path": source_dataset.uri, 
        "distribution": distribution_name,
        "_available_languages": available_languages,
        "_derived_from_uri": distribution.uri,
    }

    st_app.json(default_metadata, expanded=False)

    st_app.markdown("---")
    st_app.markdown("#### 🌐 Distribuzioni per Lingua (Previste)")
    st_app.info("Il worker creerà automaticamente una distribution per ogni lingua:")
    for lang in available_languages:
        st_app.write(f"**{lang.upper()}:** `{distribution.name}__{lang}`")

    st_app.markdown("---")
    st_app.subheader("🎯 Esecuzione Preprocessing")

    output_format = st_app.selectbox(
        "Formato di output",
        ["jsonl.gz", "parquet"],
        index=0,
        help="Seleziona il formato per i file processati."
    )

    col1, col2 = st_app.columns(2)

    with col1:
        if st_app.button("🚀 Avvia Preprocessing in Background", type="primary", use_container_width=True):
            if _launch_preprocessing_worker(
                source_dataset_uri=source_dataset_uri,
                source_distribution_uri=source_distribution_uri,
                processed_base_uri=processed_base_uri,
                glob_pattern=distribution.glob or "**/*",
                source_dataset_id=str(source_dataset.id),
                source_distribution_id=str(distribution.id),
                default_metadata=default_metadata,
                output_format=output_format,
            ):
                st_app.success("🚀 Processo di preprocessing avviato!")
                st_app.info(
                    "**Il preprocessing sta girando in background.**\n\n"
                    "- Il worker creerà automaticamente la struttura di output\n"
                    "- Le entità database verranno create con URI corretti\n"
                    "- Puoi chiudere questa pagina — il processo continuerà."
                )
            else:
                st_app.error("❌ Impossibile avviare il processo di preprocessing.")

    st_app.markdown("---")
    col1, col2 = st_app.columns(2)

    with col1:
        if st_app.button("📂 Torna alla Distribution", use_container_width=True):
            st_app.session_state.current_stage = "raw_distribution_main"
            st_app.rerun()

    with col2:
        if st_app.button("🏠 Torna alla Home", use_container_width=True):
            from utils.streamlit_func import reset_dashboard_session_state
            from config.state_vars import home_vars
            reset_dashboard_session_state(st_app, home_vars)
            st_app.session_state.current_stage = "home"
            st_app.rerun()


