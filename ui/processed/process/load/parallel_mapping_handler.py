# ui/load/parallel_mapping_handler.py
import json
import os
import tokenize
import uuid
from datetime import date
from utils.path_utils import to_binded_path
from utils.streamlit_func import reset_dashboard_session_state
import traceback
import subprocess
import sys

import logging
logger = logging.getLogger(__name__)

from data_class.entity.table.dataset import Dataset
from data_class.entity.table.distribution import Distribution
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.mapping_repository import MappingRepository
from data_class.repository.table.schema_template_repository import SchemaTemplateRepository


BASE_PREFIX = os.getenv("BASE_PREFIX")

BINDED_PROCESSED_DATA_DIR = os.getenv("BINDED_PROCESSED_DATA_DIR")
BINDED_MAPPED_DATA_DIR = os.getenv("BINDED_MAPPED_DATA_DIR")
BINDED_STATS_DATA_DIR = os.getenv("BINDED_STATS_DATA_DIR")
MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
STATS_DATA_DIR = os.getenv("STATS_DATA_DIR")
CHAT_TEMPLATE_STATS_EXTENSION = os.getenv("CHAT_TEMPLATE_STATS_EXTENSION")
LOW_LEVEL_STATS_EXTENSION = os.getenv("LOW_LEVEL_STATS_EXTENSION")

from config.state_vars import distribution_keys

def chat_stats_exists(distribution_uri: str) -> bool:
    """True se le statistiche sono valorizzate (non vuoto)."""
    distribution_path_stats_dir = distribution_uri.replace(BASE_PREFIX, '').replace(BINDED_PROCESSED_DATA_DIR, BINDED_STATS_DATA_DIR)
    distribution_path_stats_file = distribution_path_stats_dir + CHAT_TEMPLATE_STATS_EXTENSION
    if os.path.exists(distribution_path_stats_file):
        return True
    return False

def stats_exists(distribution_path: str) -> bool:
    """True se le statistiche sono valorizzate (non vuoto)."""
    distribution_path_stats_dir = distribution_path.replace(BASE_PREFIX, '').replace(BINDED_PROCESSED_DATA_DIR, BINDED_STATS_DATA_DIR)
    distribution_path_stats_file = distribution_path_stats_dir + LOW_LEVEL_STATS_EXTENSION
    if os.path.exists(distribution_path_stats_file):
        return True
    print(f"Stats file not found: {distribution_path_stats_file}")
    return False

def show_parallel_mapping(st):
    st.header("Esecuzione Pipeline ETL")
    st.info("La pipeline elabora i dati in parallelo. Potrebbe volerci del tempo per i dataset più grandi.")

    input_distribution_path = st.session_state.current_distribution_path
    input_dataset_path = st.session_state.selected_dataset_path

    # Usa le variabili INTERNE per il replace
    output_distribution_path_internal = input_distribution_path.replace(
        PROCESSED_DATA_DIR,  # /app/nfs/processed-data
        MAPPED_DATA_DIR      # /app/nfs/mapped-data
    )

    output_dataset_path_internal = input_dataset_path.replace(
        PROCESSED_DATA_DIR,
        MAPPED_DATA_DIR
    )

    # Path statistiche (sempre in formato interno)
    stats_base_path_internal = output_distribution_path_internal.replace(
        MAPPED_DATA_DIR,
        STATS_DATA_DIR      # /app/nfs/stats-data
    )
    low_level_stats_path_internal = stats_base_path_internal + LOW_LEVEL_STATS_EXTENSION
    chat_stats_path_internal = stats_base_path_internal + CHAT_TEMPLATE_STATS_EXTENSION

    # Converti in binded SOLO per visualizzazione e DB
    output_distribution_path_binded = to_binded_path(output_distribution_path_internal)
    output_dataset_path_binded = to_binded_path(output_dataset_path_internal)
    low_level_stats_path_binded = to_binded_path(low_level_stats_path_internal)
    chat_stats_path_binded = to_binded_path(chat_stats_path_internal)

    # Visualizzazione (sempre binded)
    st.write(f"**Percorso di input:** `{to_binded_path(input_distribution_path)}`")
    st.write(f"**Percorso di input dataset:** `{to_binded_path(input_dataset_path)}`")
    st.write(f"**Percorso di output distribuzione:** `{output_distribution_path_binded}`")
    st.write(f"**Percorso di output dataset:** `{output_dataset_path_binded}`")
    st.write(f"**Percorso di output statistiche di basso livello:** `{low_level_stats_path_binded}`")
    st.write(f"**Percorso di output statistiche template chat:** `{chat_stats_path_binded}`")
    st.write("")
    st.write("---")

    # Creazione delle directory necessarie (usando path interni)
    if not os.path.exists(output_distribution_path_internal):
        os.makedirs(output_distribution_path_internal, exist_ok=True)

    distribution = st.session_state.current_distribution

    # Inizializza i repository
    dataset_repo = DatasetRepository(st.session_state.db_manager)
    distribution_repo = DistributionRepository(st.session_state.db_manager)
    mapping_repo = MappingRepository(st.session_state.db_manager)
    schema_template_repo = SchemaTemplateRepository(st.session_state.db_manager)

    # Recupera il dataset usando il repository
    existing_dataset = dataset_repo.get_by_uri(BASE_PREFIX + to_binded_path(input_dataset_path))
    
    # Recupera le mappature usando il repository
    mappings = mapping_repo.get_by_distribution_id(distribution.id)
    
    if len(mappings) == 0:
        st.error("Nessuna mappatura trovata per questa distribuzione.")
        return
    elif len(mappings) == 1:
        mapping_dict = mappings[0]
    else:
        mapping_options = {f"template_id: {m.schema_template_id} ": m for m in mappings}
        selected_mapping_key = st.selectbox("Seleziona la mappatura", options=list(mapping_options.keys()))
        mapping_dict = mapping_options[selected_mapping_key]

    dst_schema = schema_template_repo.get_by_id(mapping_dict.schema_template_id).schema
    mapping = mapping_dict.mapping

    if "messages" in dst_schema['properties'].keys():
        perform_chat_stats = st.checkbox("📦 Calcola anche le stats del chat template (opzionale)")
    else:
        perform_chat_stats = False

    # Bottone per avviare la pipeline ETL
    if st.button("Map dataset", key="preprocess_dataset_btn"):
        st.info("🔄 Avvio pipeline ETL in background...")
        
        # I path nel worker_params devono essere in formato INTERNO (per Docker)
        worker_params = {
            "input_distribution_path": input_distribution_path,  # già in formato interno
            "output_distribution_path": output_distribution_path_internal,
            "input_dataset_path": input_dataset_path,  # già in formato interno
            "output_dataset_path": output_dataset_path_internal,
            "low_level_stats_path": low_level_stats_path_internal,
            "chat_stats_path": chat_stats_path_internal if perform_chat_stats else None,
            "mapping": mapping,
            "dst_schema": dst_schema,
            "src_schema": distribution.src_schema,
            "glob_pattern": st.session_state.current_distribution.glob,
            "perform_chat_stats": perform_chat_stats
        }

        try:
            # Creazione directory (usando path interni)
            os.makedirs(low_level_stats_path_internal, exist_ok=True)
            if perform_chat_stats:
                os.makedirs(chat_stats_path_internal, exist_ok=True)
            os.makedirs(output_distribution_path_internal, exist_ok=True)

            worker_script = os.path.join(os.path.dirname(__file__), "mapping_worker.py")
            
            subprocess.Popen(
                [sys.executable, worker_script, json.dumps(worker_params)],
                stdout=None, 
                stderr=None, 
                start_new_session=True,
                close_fds=True
            )
            
            # Logica DB (usa i path in formato binded per gli URI)
            st.write("📝 Registrazione metadati su DB...")
            target_dataset_uri = BASE_PREFIX + output_dataset_path_binded
            new_dataset_name = f"mapped__{existing_dataset.name}"

            mapped_dataset_ent = Dataset(
                id=str(uuid.uuid4()),
                uri=target_dataset_uri,
                name=new_dataset_name,
                languages=existing_dataset.languages if existing_dataset.languages else ["un"],
                derived_card=existing_dataset.derived_card,
                derived_dataset=existing_dataset.id, 
                dataset_type=existing_dataset.dataset_type,
                globs=existing_dataset.globs,
                description=f"Mapped version of {existing_dataset.name}",
                source=existing_dataset.source,
                version=existing_dataset.version if existing_dataset.version else "1.0",
                issued=date.today(),
                modified=date.today(),
                license=existing_dataset.license if existing_dataset.license else "unknown",
                step=3
            )

            try:
                target_dataset = dataset_repo.upsert_by_uri(mapped_dataset_ent)
            except Exception:
                target_dataset = dataset_repo.upsert_by_name(mapped_dataset_ent)

            if target_dataset:
                target_distribution_uri = BASE_PREFIX + output_distribution_path_binded
                mapped_dist_ent = Distribution(
                    id=str(uuid.uuid4()),
                    uri=target_distribution_uri,
                    tokenized_uri=None,
                    dataset_id=target_dataset.id,
                    glob="*.jsonl.gz",
                    format="jsonl.gz",
                    name=f"mapped__{distribution.name}",
                    query=distribution.query,
                    split=distribution.split if distribution.split else "unknown",
                    license=distribution.license if distribution.license else "unknown",
                    derived_from=distribution.id,
                    src_schema=dst_schema,
                    lang=distribution.lang if distribution.lang else "un",
                    materialized=True,
                    step=3,
                    issued=date.today(),
                    modified=date.today()
                )
                distribution_repo.upsert_by_uri(mapped_dist_ent)
                st.success("🚀 Pipeline avviata!")
                st.balloons()

        except Exception as e:
            st.error(f"❌ Errore: {e}")
            logger.error(traceback.format_exc())
                    
    if st.button("🏠 Torna alla Distribution"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "processed_distribution_main"
        st.rerun()
    

