# ui/distribution/distribution_raw_action_selection_handler.py
import os
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import traceback

BASE_PREFIX = os.getenv("BASE_PREFIX")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
STATS_DATA_DIR = os.getenv("STATS_DATA_DIR")
LOW_LEVEL_STATS_EXTENSION = os.getenv("LOW_LEVEL_STATS_EXTENSION")

import logging
from config.state_vars import home_vars

from utils.path_utils import to_internal_path
from utils.streamlit_func import reset_dashboard_session_state
from utils.extract_glob import generate_filtered_globs
from utils.serializer import convert_to_serializable
from utils.fs_func import list_files
from utils.sample_reader import load_dataset_samples
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.mapping_repository import MappingRepository 
from data_class.entity.table.distribution import Distribution

# Configurazione logger
logger = logging.getLogger(__name__)

# Chiavi di stato da eliminare
KEYS_TO_DELETE = [
    "selected_dataset_id", "available_dataset_labels", "current_distribution_path",
    "samples", "current_distribution", "selected_path_parts", 
    "selected_dataset_uri", "selected_dataset_path"
]

def _initialize_repositories(st_app):
    """Inizializza i repository necessari."""
    return {
        'distribution': DistributionRepository(st_app.session_state.db_manager),
        'mapping': MappingRepository(st_app.session_state.db_manager),
        'dataset': DatasetRepository(st_app.session_state.db_manager)
    }

def _materialize_distribution(st_app, distribution: Distribution):
    """Materializza la distribution nel file system"""
    # Determiniamo il path interno corretto subito
    raw_rel_path = distribution.uri.replace(BASE_PREFIX, "")
    internal_base_path = to_internal_path(raw_rel_path)
    
    # Crea folder se non esiste (usando path interno)
    os.makedirs(internal_base_path, exist_ok=True)
    logger.info(f"Cartella creata per materializzazione: {internal_base_path}")
    
    import duckdb
    query = distribution.query.strip()
    result = None
    try:
        with duckdb.connect() as conn:
            result = conn.execute(query).fetchdf()
    except Exception as e:
        st_app.error(f"❌ Errore durante l'esecuzione della query: {str(e)}")
        logger.error(f"Errore esecuzione query distribution: {e}")
        st_app.session_state.current_stage = "raw_distribution_selection"
        st_app.rerun()
        return

    try:
        df_to_save = result.copy()
        
        # Conversione a PyArrow Table
        try:
            table = pa.Table.from_pandas(df_to_save, preserve_index=False)
        except pa.ArrowInvalid:
            for col in df_to_save.columns:
                if df_to_save[col].dtype == 'object':
                    try:
                        df_to_save[col] = df_to_save[col].apply(
                            lambda x: convert_to_serializable(x) if x is not None else None
                        )
                    except Exception as e:
                        logger.warning(f"Unable to serialize column '{col}' to JSON: {e}")
                        st_app.warning(f"⚠️ Impossibile serializzare colonna '{col}' in JSON: {e}")
                        st_app.session_state.current_stage = "raw_distribution_selection"
                        st_app.rerun()
                        return
            table = pa.Table.from_pandas(df_to_save, preserve_index=False)

        # Gestione Path di destinazione coerente
        # Usiamo Path(internal_base_path) per garantire che operiamo dentro /app/nfs/...
        destination_path = Path(internal_base_path)

        # Controllo se cartella esiste ed è vuota
        if destination_path.exists() and any(destination_path.iterdir()):
            # Verifica se ci sono solo file temporanei o se è davvero occupata
            files = [f for f in destination_path.iterdir() if not f.name.startswith('.')]
            if files:
                logger.error(f"❌ La cartella '{destination_path}' esiste già e non è vuota!")
                st_app.error(f"❌ La cartella '{destination_path}' esiste già e non è vuota!")
                return False
            
        destination_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Destinazione finale: {destination_path}")

        # Salvataggio file Parquet
        estimated_size_mb = table.nbytes / (1024 * 1024)
        max_size_mb = 120
        
        if estimated_size_mb <= max_size_mb:
            output_file = destination_path / "query_results_00001.parquet"
            pq.write_table(table, output_file)
            logger.info(f"✅ Risultati salvati in: `{output_file}`")
        else:
            num_chunks = int(np.ceil(estimated_size_mb / max_size_mb))
            chunk_size = int(np.ceil(table.num_rows / num_chunks))

            progress_bar = st_app.progress(0)
            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min(start_idx + chunk_size, table.num_rows)
                chunk_table = table.slice(offset=start_idx, length=end_idx - start_idx)
                
                file_name = f"query_results_{i+1:05d}.parquet"
                output_file = destination_path / file_name
                pq.write_table(chunk_table, output_file)
                progress_bar.progress((i + 1) / num_chunks)
            
            progress_bar.progress(1.0)
            logger.info(f"Risultati salvati in {num_chunks} file Parquet.")
        
        # Aggiornamento Database
        st_app.session_state.current_distribution.materialized = True
        
        repo = DatasetRepository(st_app.session_state.db_manager)
        dataset = repo.get_by_id(st_app.session_state.current_distribution.dataset_id)
        if dataset:
            dataset.globs = generate_filtered_globs(dataset.uri.replace(BASE_PREFIX, ""))
            repo.update(dataset)

        repo = DistributionRepository(st_app.session_state.db_manager)
        repo.update(st_app.session_state.current_distribution)

        # Caricamento campioni: ora destination_path è già il path interno corretto
        st_app.session_state.samples = load_dataset_samples(str(destination_path))
        st_app.session_state.selected_path_parts = st_app.session_state.current_distribution_path.replace(st_app.session_state.selected_dataset_path+'/', "").split("/")
        
        logger.info(f"Samples caricati dopo materializzazione: {len(st_app.session_state.samples)}")
        return
        
    except Exception as e:
        st_app.error(f"❌ Errore durante il salvataggio dei risultati: {str(e)}")
        logger.error(f"Errore salvataggio risultati query distribution: {e}, {traceback.format_exc()}")
        st_app.session_state.current_stage = "raw_distribution_selection"
        st_app.rerun()

def src_schema_exists(distribution: Distribution) -> bool:
    """True se lo schema sorgente è valorizzato (non vuoto)."""
    schema = distribution.src_schema or {}
    return isinstance(schema, dict) and len(schema) > 0

def stats_exists(distribution_path: str) -> bool:
    """True se le statistiche sono valorizzate (non vuoto)."""
    distribution_path_stats_dir =  distribution_path.replace(BASE_PREFIX, '').replace(RAW_DATA_DIR, STATS_DATA_DIR)
    distribution_path_stats_file = distribution_path_stats_dir + LOW_LEVEL_STATS_EXTENSION
    if os.path.exists(distribution_path_stats_file):
        return True
    logger.warning(f"Stats file not found: {distribution_path_stats_file}")
    return False

def enable_query_current_distribution(distribution: Distribution) -> bool:
    """Abilita il bottone di query solo se src_schema è presente."""
    return src_schema_exists(distribution) 

def enable_advanced_query_current_distribution(distribution: Distribution) -> bool:
    """Abilita il bottone di query solo se src_schema è presente."""
    return src_schema_exists(distribution) and stats_exists(distribution.uri)

def _handle_existing_distribution(st_app, distribution: Distribution):
    """Gestisce il caso in cui la distribution esiste nel database."""
    # Salva la distribution corrente nello stato della sessione
    st_app.session_state.current_distribution = distribution
    
    # Mostra informazioni sulla distribution
    st_app.subheader("📋 Informazioni Distribution")
    
    col_info1, col_info2 = st_app.columns(2)
    
    with col_info1:
        st_app.write(f"**Name:** {distribution.name}")
        st_app.write(f"**Formato:** {distribution.format}")
        st_app.write(f"**Versione:** {distribution.version}")
        st_app.write(f"**Lingua:** {distribution.lang}")

    with col_info2:
        st_app.write(f"**Schema sorgente:** {'✅ Presente' if src_schema_exists(distribution) else '❌ Assente'}")
        st_app.write(f"**Licenza:** {distribution.license}")
        st_app.write(f"**Tipo:** {distribution.split}")
        st_app.write(f"**tags:** {distribution.tags}")
    # Descrizione se presente
    if distribution.description:
        st_app.write(f"**Descrizione:** {distribution.description}")
    
    st_app.markdown("---")
    st_app.subheader("🎯 Azioni Disponibili")
    
    # Bottoni principali
    _render_main_action_buttons(st_app, distribution)
    
    # Bottoni aggiuntivi
    _render_extra_action_buttons(st_app, distribution)
    
    # Navigazione
    _render_navigation_buttons(st_app)

#########################################################################

def _render_main_action_buttons(st_app, distribution: Distribution):
    """Renderizza i bottoni principali per le azioni sulla distribution."""
    
    # Logica di priorità: se lang è None, blocca tutto tranne il tasto metadati
    is_metadata_missing = distribution.lang is None or distribution.lang == "multi" or distribution.lang == "un" or distribution.split == None
    
    if is_metadata_missing:
        st_app.warning("⚠️ Lingua o Tipo di distribution non censita, alcune funzionalità potrebbero essere disabilitate.")

    col1, col2, col3 = st_app.columns(3)

    with col1:
        # Disabilitato se mancano metadati
        if st_app.button("🔄 Estrai Schema Sorgente", use_container_width=True, 
                        disabled=is_metadata_missing,
                        key="extract_schema_button"):
            st_app.session_state.current_stage = "raw_schema_extraction_options"
            st_app.rerun()

    with col2:
        # SEMPRE ABILITATO: necessario per sbloccare gli altri
        if st_app.button("✏️ Modifica Metadati Distribuzione", 
                        use_container_width=True,
                        key="edit_metadata_button"):
            st_app.session_state.current_stage = "raw_distribution_metadata"
            st_app.rerun()

    with col3:
        # Disabilitato se mancano metadati O se manca lo schema
        if st_app.button(
            "🔀 Parallel Dataset Load",
            use_container_width=True,
            disabled=is_metadata_missing or not src_schema_exists(distribution),
            key="parallel_load_button"
        ):
            st_app.session_state.current_stage = "run_parallel_preprocessing"
            st_app.rerun()

def _render_extra_action_buttons(st_app, distribution: Distribution):
    """Renderizza i bottoni aggiuntivi per le azioni speciali."""
    
    # Logica di priorità metadati
    is_metadata_missing = distribution.lang is None or distribution.lang == "multi" or distribution.lang == "un" or distribution.split == None
    
    col_extra1, col_extra2, col_extra3 = st_app.columns(3)

    with col_extra1:
        # Disabilitato se mancano metadati O se la logica specifica lo nega
        if st_app.button(
            "🔍 Query Current Dataset",
            use_container_width=True,
            disabled=is_metadata_missing or not enable_query_current_distribution(distribution),
            key="query_dataset_button"
        ):
            st_app.session_state.current_stage = "raw_query_current_distribution"
            st_app.rerun()
            
def _render_navigation_buttons(st_app):
    """Renderizza i bottoni di navigazione."""
    st_app.markdown("---")
    
    col1, col2 = st_app.columns([1, 1])
    
    with col1:
        if st_app.button("📂 Cambia Distribution", 
                        use_container_width=True,
                        key="change_distribution_button"):
            reset_dashboard_session_state(st_app, KEYS_TO_DELETE)
            st_app.session_state.current_stage = "raw_distribution_selection"
            st_app.rerun()
    
    with col2:
        if st_app.button("🏠 Torna alla Home", 
                        use_container_width=True,
                        key="home_button"):
            reset_dashboard_session_state(st_app, home_vars)
            st_app.session_state.current_stage = "home"
            st_app.rerun()


def show_distribution(st_app):
    """Interfaccia principale per la selezione delle azioni sulla distribution."""
    st_app.header("🎯 Scegli l'Operazione")
    st_app.write("Quale operazione desideri eseguire sul tuo dataset?")

    # Ensure current_distribution_path exists in session_state to avoid AttributeError
    current_distribution_path = getattr(st_app.session_state, "current_distribution_path", None)
    if not current_distribution_path:
        current_distribution = getattr(st_app.session_state, "current_distribution", None)
        if current_distribution and getattr(current_distribution, "uri", None):
            try:
                current_distribution_path = current_distribution.uri.replace(BASE_PREFIX, "")
                st_app.session_state.current_distribution_path = current_distribution_path
            except Exception as e:
                logger.warning(f"Unable to derive current_distribution_path from current_distribution: {e}")
                current_distribution_path = None

    # Ensure current_distribution exists in session_state
    if not getattr(st_app.session_state, "current_distribution", None):
        st_app.warning("⚠️ Nessuna distribuzione selezionata. Torna indietro e seleziona una distribuzione.")
        if st_app.button("🔙 Torna alla selezione delle distribuzioni", key="back_to_selection"):
            reset_dashboard_session_state(st_app, KEYS_TO_DELETE)
            st_app.rerun()
        return

    logger.info(f"Current distribution path rilevato: {current_distribution_path}")
    
    internal_check_path = to_internal_path(current_distribution_path)
    # Proceed only if we have a valid path
    
    if not current_distribution_path or not list_files(internal_check_path):
        st_app.info("""ℹ️ **Nessun file trovato. Materializza la distribution per continuare.**""")
        if st_app.button("Materializza Distribution", key="materialize_distribution"):
            _materialize_distribution(st_app, st_app.session_state.current_distribution)
            st_app.session_state.current_stage = "raw_distribution_main"
            st_app.rerun()
            return
    else:
        # Inizializzazione repository
        repos = _initialize_repositories(st_app)

        # Costruisci path e URI - normalizza usando to_binded_path
        distribution_path = st_app.session_state.current_distribution_path
        distribution_uri_raw = f"{BASE_PREFIX}{distribution_path}"
        
        # Normalizza l'URI per matchare quello salvato nel DB (es. /app/nfs -> /nfs)
        from utils.path_utils import to_binded_path
        distribution_uri = to_binded_path(distribution_uri_raw)

        logger.info(f"Ricerca distribution con URI normalizzato: {distribution_uri}")

        try:
            # Cerca distribution esistente
            distribution = repos['distribution'].get_by_uri(distribution_uri)
            
            if distribution:
                logger.info(f"✅ Distribution trovata - ID: {distribution.id}, name: {distribution.name}, Lang: {distribution.lang}, Split: {distribution.split}")
                _handle_existing_distribution(st_app, distribution)
            else:
                logger.error(f"❌ Distribution NON trovata per URI: {distribution_uri}")
                st_app.error(f"❌ Distribution non trovata nel database per URI: {distribution_uri}")
                st_app.info("La distribution potrebbe non essere stata registrata correttamente.")
                
                if st_app.button("🔙 Torna alla selezione", key="back_to_selection_no_dist"):
                    reset_dashboard_session_state(st_app, KEYS_TO_DELETE)
                    st_app.session_state.current_stage = "raw_distribution_selection"
                    st_app.rerun()

        except Exception as e:
            st_app.error(f"❌ Errore durante il recupero delle informazioni della distribution: {str(e)}")
            logger.exception(f"Errore nel recupero distribution")

            # Bottone di ripiego
            if st_app.button("🔄 Riprova", key="retry_button"):
                st_app.rerun()


