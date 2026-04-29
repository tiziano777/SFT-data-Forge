# ui/distribution/distribution_action_selection_handler.py
from pathlib import Path
import gzip
import numpy as np
import json

import os
from utils.path_utils import to_binded_path, to_internal_path


BASE_PREFIX = os.getenv("BASE_PREFIX")
STATS_DATA_DIR = os.getenv("STATS_DATA_DIR")
MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR")
BINDED_STATS_DATA_DIR = os.getenv("BINDED_STATS_DATA_DIR")
BINDED_MAPPED_DATA_DIR = os.getenv("BINDED_MAPPED_DATA_DIR")
CHAT_TEMPLATE_STATS_EXTENSION = os.getenv("CHAT_TEMPLATE_STATS_EXTENSION")
LOW_LEVEL_STATS_EXTENSION = os.getenv("LOW_LEVEL_STATS_EXTENSION")
import logging

from utils.streamlit_func import reset_dashboard_session_state
from utils.extract_glob import generate_filtered_globs
from utils.fs_func import list_files
from utils.sample_reader import load_dataset_samples
from utils.serializer import process_record_for_json
from config.state_vars import distribution_keys,home_vars
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.mapping_repository import MappingRepository
from data_class.repository.table.dataset_repository import DatasetRepository 
from data_class.entity.table.distribution import Distribution

# Configurazione logger
logger = logging.getLogger(__name__)


def _initialize_repositories(st_app):
    """Inizializza i repository necessari."""
    return {
        'distribution': DistributionRepository(st_app.session_state.db_manager),
        'mapping': MappingRepository(st_app.session_state.db_manager),
        'dataset': DatasetRepository(st_app.session_state.db_manager)
    }

def _materialize_distribution(st_app, distribution: Distribution):
    """Materializza la distribution nel file system in formato JSONL.gz (Step 3)"""

    internal_base_path = to_internal_path(distribution.uri.replace(BASE_PREFIX, ""))
    
    os.makedirs(internal_base_path, exist_ok=True)

    import duckdb
    query = distribution.query.strip()
    result = None
    try:
        with duckdb.connect() as conn:
            result = conn.execute(query).fetchdf()
    except Exception as e:
        st_app.error(f"❌ Errore durante l'esecuzione della query: {str(e)}")
        st_app.session_state.current_stage = "mapped_distribution_selection"
        st_app.rerun()
        return
    
    try:
        df_to_save = result.copy()
        destination_path = Path(internal_base_path)

        if destination_path.exists() and any(destination_path.iterdir()):
            files = [f for f in destination_path.iterdir() if not f.name.startswith('.')]
            if files:
                st_app.error(f"❌ La cartella '{destination_path}' esiste già e non è vuota!")
                return False
            
        destination_path.mkdir(parents=True, exist_ok=True)

        # Trasformazione in record JSON
        try:
            records = df_to_save.to_dict(orient='records')
            records = [process_record_for_json(r) for r in records]
        except Exception as e:
            logger.error(f"Error converting DataFrame to dict: {e}")
            st_app.error(f"❌ Errore nella conversione del DataFrame: {str(e)}")
            return
        

        # Stima e Salvataggio (Chunking)
        max_size_mb = 120
        # Calcolo approssimativo dimensione
        estimated_size_mb = len(json.dumps(records[:100])) * (len(records)/100) / (1024*1024) if records else 0
        
        if estimated_size_mb <= max_size_mb:
            output_file = destination_path / "query_results_00001.jsonl.gz"
            with gzip.open(output_file, 'wt', encoding='utf-8', compresslevel=5) as f:
                for record in records:
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
        else:
            num_chunks = int(np.ceil(estimated_size_mb / max_size_mb))
            chunk_size = int(np.ceil(len(records) / num_chunks))
            for i in range(num_chunks):
                chunk = records[i*chunk_size : (i+1)*chunk_size]
                output_file = destination_path / f"query_results_{i+1:05d}.jsonl.gz"
                with gzip.open(output_file, 'wt', encoding='utf-8', compresslevel=5) as f:
                    for record in chunk:
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
            
        # --- AGGIORNAMENTO DB ---
        st_app.session_state.current_distribution.materialized = True
        
        ds_repo = DatasetRepository(st_app.session_state.db_manager)
        dataset = ds_repo.get_by_id(st_app.session_state.current_distribution.dataset_id)
        if dataset:
            dataset.globs = generate_filtered_globs(dataset.uri.replace(BASE_PREFIX, ""))
            ds_repo.update(dataset)

        dist_repo = DistributionRepository(st_app.session_state.db_manager)
        dist_repo.update(st_app.session_state.current_distribution)

        # --- SINCRONIZZAZIONE SESSION STATE ---
        st_app.session_state.samples = load_dataset_samples(str(destination_path))
        
        # Calcolo sicuro dei path parts per la navigazione UI
        base_dataset_path = st_app.session_state.get('selected_dataset_path', '')
        if base_dataset_path:
            rel_path = st_app.session_state.current_distribution_path.replace(base_dataset_path, "").strip("/")
            st_app.session_state.selected_path_parts = rel_path.split("/") if rel_path else []
        
        logger.info(f"✅ Materializzazione completata Layer 3: {destination_path}")
        return
        
    except Exception as e:
        st_app.error(f"❌ Errore salvataggio: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        st_app.session_state.current_stage = "mapped_distribution_selection"
        st_app.rerun()

def stats_exists(distribution_uri: str) -> bool:
    """Verifica se esistono file di statistiche low-level nella cartella mapped."""
    local_path = distribution_uri.replace(BASE_PREFIX, '').replace(MAPPED_DATA_DIR, STATS_DATA_DIR)
    target_dir = local_path.replace(BINDED_MAPPED_DATA_DIR, BINDED_STATS_DATA_DIR) + LOW_LEVEL_STATS_EXTENSION
    
    if not os.path.exists(to_internal_path(target_dir)):
        logger.debug(f"Low level stats dir not found: {target_dir}")
        return False

    files = [f for f in os.listdir(to_internal_path(target_dir)) if f.endswith('.parquet')]
    return len(files) > 0



def _render_main_action_buttons(st_app, distribution: Distribution):
    """Renderizza i bottoni principali per le azioni sulla distribution."""
    
    current_sample = st_app.session_state.samples[0] if st_app.session_state.samples else {}
    
    is_metadata_missing = distribution.lang is None or distribution.lang == "multi" or distribution.lang == "un" or distribution.split is None
    if is_metadata_missing:
        st_app.warning("⚠️ Lingua o Tipo non censiti. Modifica i metadati per sbloccare le azioni.")
    
    col1, col2, col3, col4 = st_app.columns(4)
    
    with col1:
        if st_app.button(
            "✏️ Modifica Metadati Distribuzione", 
            use_container_width=True,
            key=f"edit_metadata_{distribution.id}"
        ):
            st_app.session_state.current_stage = "mapped_distribution_metadata"
            st_app.rerun()
            
    with col2:
        if st_app.button(
            "🔍 Query Current Distribution",
            use_container_width=True,
            disabled=is_metadata_missing,
            key=f"query_distribution_{distribution.id}" 
        ):
            st_app.session_state.current_stage = "mapped_query_current_distribution"
            st_app.rerun()
        
            
    with col3:
        already_has_low = stats_exists(distribution.uri)
        if st_app.button(
            "📊 Estrai Low Stats",
            use_container_width=True,
            disabled=is_metadata_missing or already_has_low,
            key=f"extract_low_stats_{distribution.id}"
        ):
            st_app.session_state.current_stage = "mapped_run_low_level_stats_extraction"
            st_app.rerun()

    with col4:
        has_low = stats_exists(distribution.uri)
        if st_app.button(
            "📊 Advanced Query",
            use_container_width=True,
            disabled=is_metadata_missing or not has_low,
            key=f"low_level_query_stats_{distribution.id}"
        ):
            st_app.session_state.current_stage = "mapped_query_low_level_stats_current_distribution"
            st_app.rerun()


def _render_navigation_buttons(st_app):
    """Renderizza i bottoni di navigazione."""
    st_app.markdown("---")
    
    col1, col2 = st_app.columns([1, 1])
    
    with col1:
        if st_app.button("📂 Cambia Distribution", 
                        use_container_width=True):
            reset_dashboard_session_state(st_app, distribution_keys)
            st_app.session_state.current_stage = "mapped_distribution_selection"
            st_app.rerun()
    
    with col2:
        if st_app.button("🏠 Torna alla Home", use_container_width=True):
            reset_dashboard_session_state(st_app, home_vars)
            st_app.session_state.current_stage = "home"
            st_app.rerun()

def _handle_existing_distribution(st_app, distribution: Distribution):
    """Gestisce il caso in cui la distribution esiste nel database."""
    st_app.session_state.current_distribution = distribution
    
    st_app.subheader("📋 Informazioni Distribution")
    
    col_info1, col_info2 = st_app.columns(2)
    
    with col_info1:
        st_app.write(f"**Titolo:** {distribution.name}")
        st_app.write(f"**Formato:** {distribution.format}")
        st_app.write(f"**Versione:** {distribution.version}")
        st_app.write(f"**Lingua:** {distribution.lang}")

    with col_info2:
        st_app.write(f"**Licenza:** {distribution.license}")
        st_app.write(f"**Tipo:** {distribution.split}")
        st_app.write(f"**tags:** {distribution.tags}")
    
    if distribution.description:
        st_app.write(f"**Descrizione:** {distribution.description}")
    
    st_app.markdown("---")
    st_app.subheader("🎯 Azioni Disponibili")
    
    _render_main_action_buttons(st_app, distribution)
    _render_navigation_buttons(st_app)

def show_distribution(st_app):
    """Interfaccia principale per la selezione delle azioni sulla distribution."""
    st_app.header("🎯 Scegli l'Operazione")
    st_app.write("Quale operazione desideri eseguire su distribution?")

    if 'samples' not in st_app.session_state:
        st_app.session_state.samples = []

    current_distribution_path = getattr(st_app.session_state, "current_distribution_path", None)
    if not current_distribution_path:
        current_distribution = getattr(st_app.session_state, "current_distribution", None)
        if current_distribution and getattr(current_distribution, "uri", None):
            try:
                current_distribution_path = current_distribution.uri.replace(BASE_PREFIX, "")
                st_app.session_state.current_distribution_path = current_distribution_path
            except Exception as e:
                logger.warning(f"Unable to derive current_distribution_path: {e}")
                current_distribution_path = None

    if not getattr(st_app.session_state, "current_distribution", None):
        st_app.warning("⚠️ Nessuna distribuzione selezionata.")
        if st_app.button("🔙 Torna alla selezione", key="back_to_selection"):
            reset_dashboard_session_state(st_app, distribution_keys)
            st_app.session_state.current_stage = "mapped_distribution_selection"
            st_app.rerun()
        return

    logger.info(f"Current distribution path rilevato: {current_distribution_path}")
    internal_check_path = to_internal_path(current_distribution_path)

    if not current_distribution_path or not list_files(internal_check_path):
        st_app.info("""ℹ️ **Nessun file trovato materializza la distribution per continuare**""")
        if st_app.button(" Materializza Distribution "):
            _materialize_distribution(st_app, st_app.session_state.current_distribution)
            st_app.session_state.current_stage = "mapped_distribution_main"
            st_app.rerun()
            return
    else:
        repos = _initialize_repositories(st_app)
        
        distribution_path = st_app.session_state.current_distribution_path
        distribution_uri_raw = f"{BASE_PREFIX}{distribution_path}"
        distribution_uri = to_binded_path(distribution_uri_raw)
        
        logger.info(f"Ricerca distribution processed con URI: {distribution_uri}")

        try:
            distribution = repos['distribution'].get_by_uri(distribution_uri)
            
            if distribution:
                logger.info(f"✅ Distribution processed trovata - ID: {distribution.id}")
                
                _handle_existing_distribution(st_app, distribution)
            else:
                logger.error(f"❌ Distribution processed NON trovata nel DB per URI: {distribution_uri}")
                st_app.error(f"❌ Distribution non trovata nel database.")
                
                if st_app.button("🔙 Torna alla selezione", key="back_to_selection_no_dist"):
                    reset_dashboard_session_state(st_app, distribution_keys)
                    st_app.session_state.current_stage = "processed_distribution_selection"
                    st_app.rerun()

        except Exception as e:
            st_app.error(f"❌ Errore durante il recupero: {str(e)}")
            logger.exception("Errore nel recupero distribution processed")
            if st_app.button("🔄 Riprova", key="retry_button"):
                st_app.rerun()