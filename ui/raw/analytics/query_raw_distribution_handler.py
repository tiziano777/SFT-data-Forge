# ui/query/query_distribution_handler.py
import os
import traceback
import gzip
import json
from pathlib import Path
from typing import Dict, Any, Optional
import jsonschema
import duckdb
import numpy as np

import logging

from utils.streamlit_func import reset_dashboard_session_state
from utils.sample_reader import load_dataset_samples
from utils.extract_glob import generate_dataset_globs
from utils.serializer import process_record_for_json

from data_class.repository.table.distribution_repository import DistributionRepository  # CORRETTO: repository non data_class.repository
from data_class.repository.table.dataset_repository import DatasetRepository

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from data_class.entity.table.distribution import Distribution
from data_class.entity.table.dataset import Dataset
from utils.path_utils import to_internal_path, to_binded_path

# Display limit per Streamlit UI
DISPLAY_LIMIT = 500

# Configurazione ambiente
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
BINDED_RAW_DATA_DIR = os.getenv("BINDED_RAW_DATA_DIR")
BASE_PREFIX = os.getenv("BASE_PREFIX")
METADATA_FILE_SUFFIX = os.getenv("METADATA_FILE_SUFFIX")

from config.state_vars import distribution_keys

# Chiavi di stato da resettare
KEYS_TO_RESET = [
    "show_save_interface", "show_query_interface", "show_info", "show_stats",
    "show_save_result", "last_query", "show_schema", "show_preview", "run_query",
    "show_structure", "valid_for_query", "query_result_df", "options_expanded",
    "save_error_msg", "save_success_msg", "limit_results_input", "folder_name_input",
    "metadata_entries"
]

### AUXILIARY FUNCTIONS ###

def _initialize_repositories(st_app):
    """Inizializza i repository necessari."""
    return {
        'distribution': DistributionRepository(st_app.session_state.db_manager),
        'dataset': DatasetRepository(st_app.session_state.db_manager)
    }

def _validate_data_and_schema(st, dataset_path: str, src_schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Validazione file e schema, mostra sample."""
    st.session_state.valid_for_query = False
    
    if not dataset_path or not os.path.isdir(dataset_path):
        st.error(f"❌ Percorso dataset non valido {dataset_path}")
        return None
        
    '''
    files = os.listdir(dataset_path)
    if not files:
        st.warning("[listdir] ⚠️ Nessun file trovato nella cartella del dataset.")
        return None
    '''
        
    try:
        samples = load_dataset_samples(dataset_path)
        jsonschema.Draft7Validator(src_schema).validate(samples[0])
        st.success("✅ Il dataset è elegibile per interrogazioni.")
        st.session_state.valid_for_query = True
        return src_schema
    except jsonschema.exceptions.ValidationError as ve:
        st.error(f"❌ Il sample non è valido rispetto allo schema: {ve.message}")
    except Exception as e:
        st.error(f"❌ Errore nella lettura/validazione: {e}")
        st.session_state.pop('query_result_df', None)
    
    return None

def _compact_sql_query(query: str) -> str:
    """Rimuove spazi multipli, a capo e tabulazioni da una query SQL."""
    import re
    query = query.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    query = re.sub(r'\s+', ' ', query)
    return query.strip()

def _get_file_extension(distribution_path: str) -> Optional[str]:
    """
    Recupera l'estensione del file dal file system interno.
    Converte il path in internal_path e scansiona ricorsivamente.
    """
    try:
        
        if not os.path.exists(distribution_path):
            logger.error(f"[_get_file_extension] Path non trovato: {distribution_path}")
            return None

        # 2. Ricerca del primo file valido (anche in sottocartelle)
        first_file = None
        for root, dirs, files in os.walk(distribution_path):
            # Filtriamo file nascosti
            valid_files = [f for f in files if not f.startswith('.')]
            if valid_files:
                first_file = valid_files[0]
                break
        
        if not first_file:
            return None
        
        # 3. Logica estensioni (composte e semplici)
        parts = first_file.split('.')
        if len(parts) < 2:
            return None
        
        compression_exts = {'gz', 'bz2', 'xz', 'zip', '7z', 'rar', 'zst', 'tgz', 'tbz2'}
        
        # Gestione estensioni doppie tipo .jsonl.gz
        if parts[-1].lower() in compression_exts and len(parts) > 2:
            return '.' + '.'.join(parts[-2:])
        
        return '.' + parts[-1]
    except Exception as e:
        logger.error(f"[_get_file_extension] Errore: {e}")
        return None

### RENDERING FUNCTIONS ###

def _render_navigation_header(st):
    """Renderizza l'header con navigazione."""
    st.header("🔍 Query RAW Dataset")
    st.write("Interroga il tuo dataset nel formato sorgente usando SQL")
    st.write("Visualizza il src schema e usa DuckDB per eseguire query SQL sui tuoi dati")
    
    if st.button("🏠 Torna alla Distribution", key="back_to_distribution"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "raw_distribution_main"
        st.rerun()

def _render_additional_features(st):
    """Renderizza le funzionalità aggiuntive."""
    st.markdown("---")
    st.subheader("🔧 Funzionalità Aggiuntive")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.button("📈 Statistiche Dataset", key="btn_stats", 
                 on_click=lambda: st.session_state.update(
                     show_stats=True, show_preview=False, show_structure=False
                 ))
    with col2:
        st.button("🔍 Anteprima Dati", key="btn_preview", 
                 on_click=lambda: st.session_state.update(
                     show_preview=True, show_stats=False, show_structure=False
                 ))
    with col3:
        st.button("🏗️ Struttura Tabelle", key="btn_structure", 
                 on_click=lambda: st.session_state.update(
                     show_structure=True, show_stats=False, show_preview=False
                 ))

def _render_schema_view(st, src_schema: Dict[str, Any]):
    """Renderizza la visualizzazione dello schema."""
    st.markdown("---")
    st.subheader("📋 Schema del Dataset")
    
    try:
        st.json(src_schema, expanded=True)
        
        if st.button("🔼 Nascondi Schema", key="btn_hide_schema"):
            st.session_state.update(show_schema=False)
            st.rerun()
            
    except Exception as e:
        st.error(f"❌ Errore nella lettura dello schema: {str(e)}")

### QUERY INTERFACE AND EXECUTION ###

def _render_query_interface(st, data_path: str, file_extension: str):
    """Renderizza l'interfaccia di query SQL."""
    st.markdown("---")
    st.subheader("💻 Interfaccia Query SQL")
    
    # MODIFICA CRITICA: Convertiamo il path in quello interno al container
    internal_data_path = to_internal_path(data_path)
    
    # Lista file supportati
    supported_extensions = ['.json', '.jsonl', '.csv', '.gz', '.parquet', '.jsonl.gz'] 
    
    try:
        # Verifichiamo che il path esista prima di iterare
        p = Path(internal_data_path)
        if not p.exists():
            st.error(f"❌ Path non trovato nel container: `{internal_data_path}`")
            logger.error(f"[_render_query_interface] Path inesistente: {internal_data_path}")
            return

        # Scansione dei file usando il path interno
        files = [f for f in p.iterdir() if f.suffix.lower() in [ext.lower() for ext in supported_extensions] and f.is_file()]
    except Exception as e:
        st.error(f"❌ Errore nell'accesso ai file: {str(e)}")
        logger.error(f"[_render_query_interface] Errore iterdir su {internal_data_path}: {e}")
        return

    if not files:
        st.warning(f"⚠️ Nessun file supportato trovato in `{internal_data_path}`")
        return
    
    with st.expander("📁 File Supportati Disponibili", expanded=False):
        for file in files:
            st.write(f"• `{file.name}`")
    
    # Area query SQL
    st.markdown("**Scrivi la tua query SQL:**")
    # Nota: DuckDB deve usare il path che "vede" lui (internal_data_path)
    example_query = f"SELECT * FROM '{internal_data_path}/*{file_extension}'"
    
    query = st.text_area(
        "Query SQL",
        value=st.session_state.get('last_query', example_query),
        height=200,
        key="query_text_area",
        help="Scrivi la tua query SQL. Non è necessario includere LIMIT."
    )
    
    # Parametri esecuzione
    col_exec1, col_exec2 = st.columns([3, 1])
    
    with col_exec1:
        limit_results = st.number_input(
            "Limite risultati per l'esecuzione SQL (LIMIT)", 
            min_value=1,
            value=st.session_state.get("limit_results_input", 50),
            key="limit_results_input",
            help="Numero massimo di righe che DuckDB leggerà ed elaborerà."
        )
    
    def execute_query_callback():
        st.session_state.run_query = True
        st.session_state.last_query = st.session_state.query_text_area
        st.session_state.update(show_info=False, show_save_result=False)
        st.rerun()
    
    with col_exec2:
        st.write("")  
        st.write("") 
        if st.button("▶️ Esegui Query", type="primary", key="btn_execute_query"):
            execute_query_callback()

def _execute_sql_query(st):
    """Esegue la query SQL e gestisce i risultati."""
    if not st.session_state.get('run_query', False):
        return
        
    st.session_state.run_query = False
    
    try:
        with duckdb.connect() as conn:
            st.info("🔄 Esecuzione query in corso...")

            query_base = st.session_state.last_query.strip()
            user_limit = st.session_state.get("limit_results_input", 50)
            effective_limit = min(DISPLAY_LIMIT, user_limit)

            # Applica LIMIT se non presente
            query_to_execute = query_base.rstrip(' \t\n\r;')
            query_lower = query_to_execute.lower()

            if " limit " in query_lower:
                st.warning("⚠️ **ATTENZIONE**: LIMIT specificato nella query. Il valore della UI verrà ignorato.")
                query_to_execute += ";"
            else:
                query_to_execute = f"{query_base} LIMIT {effective_limit};"
                st.info(f"✨ Aggiunta clausola **LIMIT {effective_limit}**")
            
            # Esegui query
            print("Eseguendo query SQL:", query_to_execute)
            result = conn.execute(query_to_execute).fetchdf()
            st.session_state.executed_query = query_to_execute
            
            # Converti colonne object in stringhe
            '''
            for col in result.columns:
                if result[col].dtype == 'object':
                    try:
                        result[col] = result[col].astype(str)
                    except Exception as e:
                        st.warning(f"⚠️ Impossibile convertire colonna '{col}' a stringa: {e}")
            '''
            
            st.session_state.query_result_df = result
                       
    except Exception as e:
        st.error(f"❌ Errore nell'esecuzione della query: {str(e)}")
        
        if 'query_result_df' in st.session_state:
            del st.session_state.query_result_df
        
        # Suggerimenti per errori comuni
        if "no such file" in str(e).lower():
            st.info("💡 **Suggerimento**: Verifica il path ai file")
        elif "syntax error" in str(e).lower():
            st.info("💡 **Suggerimento**: Controlla la sintassi SQL")

def _render_query_results(st, repos: Dict):
    """Renderizza i risultati della query e le opzioni aggiuntive."""
    if 'query_result_df' not in st.session_state:
        return
        
    result = st.session_state.query_result_df
    
    st.success(f"✅ Query eseguita con successo! Trovate {len(result)} righe.")
    
    # Statistiche risultati
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Righe totali", len(result))
    with col2:
        st.metric("Colonne", len(result.columns))
    with col3:
        st.metric("Righe visualizzate", len(result))
    
    # Visualizza tabella risultati
    st.dataframe(result, height=400)
    
    if 'options_expanded' not in st.session_state:
        st.session_state.options_expanded = False
    
    with st.expander("📊 Opzioni Aggiuntive", expanded=st.session_state.options_expanded):
        _render_result_options(st, result, repos)

def _render_result_options(st, result, repos: Dict):
    """Renderizza le opzioni per i risultati della query."""
    # Inizializza stati
    st.session_state.setdefault('show_info', False)
    st.session_state.setdefault('show_save_result', False)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Download CSV
        csv = result.to_csv(index=False)
        st.download_button(
            label="📥 Scarica risultati (CSV)",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv",
            key="download_csv_opt"
        )
    
    with col2:
        if st.button("ℹ️ Info Dataset", key="info_dataset_opt"):
            st.session_state.options_expanded = True
            st.session_state.show_info = True
            st.session_state.show_save_result = False
            st.rerun()
        
    with col3:
        if st.button("💾 Salva Risultati", key="save_results_opt"):
            st.session_state.options_expanded = True
            st.session_state.show_save_result = True
            st.session_state.show_info = False
            st.rerun()
    
    # Pulsante chiudi expander
    if st.session_state.options_expanded and not st.session_state.show_info and not st.session_state.show_save_result:
        if st.button("🔼 Chiudi Opzioni Aggiuntive", key="close_options_expander"):
            st.session_state.options_expanded = False
            st.rerun()

    # Info Dataset
    if st.session_state.get('show_info', False):
        _render_dataset_info(st, result)
        
    # Salva Risultati
    if st.session_state.get('show_save_result', False):
        _render_save_interface(st, result, repos)

def _render_dataset_info(st, result):
    """Renderizza le informazioni sul dataset."""
    st.markdown("---")
    st.write("**Informazioni sui tipi di dati:**")
    tipo_df = result.dtypes.astype(str).to_frame("Tipo")
    st.dataframe(tipo_df)
    
    if st.button("🔼 Chiudi Info Dataset", key="close_info_opt"):
        st.session_state.show_info = False
        st.rerun()

def _render_save_interface(st, result, repos: Dict):
    """Renderizza l'interfaccia di salvataggio risultati."""
    st.markdown("---")
    st.subheader("💾 Salva Risultati Query")

    distribution_path = st.session_state.current_distribution_path.split("/")
    distribution_path = "/".join(distribution_path[:-1])

    folder_name = st.text_input(
        "Nome della cartella di destinazione",
        placeholder="nome_query_risultati",
        key="folder_name_input",
        help=f"Inserisci il nome della cartella dove salvare i risultati. Sarà creata in: {distribution_path}/[nome_cartella]"
    )

    materialize_dataset = st.checkbox("📦 Materializza il dataset (opzionale)",)

    col1, col2 = st.columns(2)

    with col1:
        if st.button("💾 Conferma Salvataggio", type="primary", key="confirm_save_opt"):
            destination_path = "/".join([distribution_path] + [folder_name])

            # Mostra il path binded nella UI
            binded_path = to_binded_path(destination_path)
            st.info(f"📁 Path binded: {binded_path}")

            _handle_save_confirmation(st, result, Path(destination_path), materialize_dataset, repos)
    with col2:
        if st.button("❌ Annulla", key="cancel_save_opt"):
            st.session_state.show_save_result = False
            st.rerun()

# SAVE QUERY RESULTS HANDLER AS DISTRIBUTION #

def _handle_save_confirmation(st, result, destination_path: Path, materialize_dataset: bool, repos: Dict):
    """Gestisce la conferma del salvataggio."""
    if not destination_path or not destination_path:
        st.error("❌ Inserisci un nome valido per la cartella")
        st.rerun()
        return

    with st.spinner("Salvataggio in corso..."):
        if materialize_dataset:
            success = _save_query_results(st, result, destination_path, repos)
        else:
            success = _create_query_distribution(st, destination_path, False, repos)

    if success and not materialize_dataset:
        st.session_state.save_success_msg = f"✅ Distribuzione acquisita nel sistema"
        st.session_state.save_state = 1  # Stato di successo
    elif success and materialize_dataset:
        st.session_state.save_success_msg = f"✅ Risultati salvati e distribuzione creata con successo in `{str(destination_path)}`"
        st.session_state.save_state = 1  # Stato di successo
    else:
        st.session_state.save_error_msg = "❌ Salvataggio fallito. Controlla il log."
        st.session_state.save_state = 2  # Stato di errore

def _save_query_results(st: Any, result_df, destination_path: Path, repos: Dict) -> bool:
    """
    Salva i risultati della query in file JSONL.GZ suddivisi
    in base alla dimensione in memoria e alla dimensione target per file.
    """
    TARGET_FILE_SIZE_MB = 120.0  # Dimensione massima target per file (non rigida)
    BYTES_TO_MB = 1024 * 1024
    
    try:
        # 1. Controlli Iniziali e Preparazione Cartella
        if result_df.empty:
            st.info("⚠️ DataFrame vuoto. Nessun file da salvare.")
            return _create_query_distribution(st, destination_path, False, repos)

        if destination_path.exists() and any(destination_path.iterdir()):
            st.error(f"❌ '{destination_path}' esiste già e non è vuota!")
            logger.error(f"[_save_query_results] Cartella esiste già: {destination_path}")
            return False

        destination_path.mkdir(parents=True, exist_ok=True)
        st.info(f"📁 Creata nuova cartella: {destination_path}")

        # 2. Calcolo della Suddivisione (Logica Semplificata Richiesta)
        # Dimensione totale del DataFrame in memoria (in MB)
        total_size_bytes = result_df.memory_usage(deep=True).sum()
        total_size_mb = total_size_bytes / BYTES_TO_MB
        # Calcolo del rapporto e logica di arrotondamento
        ratio = total_size_mb / TARGET_FILE_SIZE_MB
        if ratio % 1 > 0.5:
            num_chunks = int(np.ceil(ratio))
        else:
            num_chunks = int(np.floor(ratio))
            num_chunks = max(1, num_chunks) 
            
        num_records = len(result_df)
        chunk_size = int(np.ceil(num_records / num_chunks))

        logger.info(f"[_save_query_results] Dimensione totale (memoria): {total_size_mb:.2f} MB")
        logger.info(f"[_save_query_results] Suddivisione in {num_chunks} chunk (target {TARGET_FILE_SIZE_MB} MB), chunk_size: {chunk_size}")
        
        # 3. Pre-calcolo: determiniamo se la colonna _filename esiste
        filename_column_exists = '_filename' in result_df.columns
        
        # 4. Salvataggio per Chunk
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min(start_idx + chunk_size, num_records)
            
            if start_idx >= num_records: 
                break 

            # OPTIMIZATION: Estrazione diretta senza copia iniziale
            chunk_df = result_df.iloc[start_idx:end_idx]
            file_name = f"query_results_{i+1:05d}.jsonl.gz"
            output_file = destination_path / file_name
            
            # OPTIMIZATION: Modifica mirata del campo _filename con assign() 
            # che è più efficiente di copiare l'intero dataframe
            if filename_column_exists:
                # Usiamo assign() che modifica in modo efficiente
                chunk_df = chunk_df.assign(_filename=file_name)
            else:
                # Se la colonna non esiste, la creiamo
                chunk_df = chunk_df.assign(_filename=file_name)
            
            status_text.text(f"Salvataggio file {i+1}/{num_chunks} ({len(chunk_df)} record)...")

            with gzip.open(output_file, 'wt', encoding='utf-8') as f_gz:
                for idx, row in chunk_df.iterrows():
                    row_dict_clean = process_record_for_json(row.to_dict())
                    f_gz.write(json.dumps(row_dict_clean, ensure_ascii=False) + '\n')
            
            del chunk_df
            progress_bar.progress((i + 1) / num_chunks)
        
        progress_bar.progress(1.0)
        status_text.empty()
        
        files_created = list(destination_path.glob("*.jsonl.gz"))
        st.success(f"✅ Salvataggio completato in {len(files_created)} file!")
        st.info(f"📊 Totale: {num_records} record(s) in {len(files_created)} file(s)")

        return _create_query_distribution(st, destination_path, True, repos)
        
    except Exception as e:
        st.error(f"❌ Errore nel salvataggio: {str(e)}")
        logger.error(f"[_save_query_results] Errore generale: {str(e)}")
        return False

def _create_query_distribution(st, destination_path: Path, materialize: bool, repos: Dict) -> bool:
    """Crea una nuova distribuzione, gestendo la creazione di un nuovo dataset se necessario."""
    try:
        from datetime import datetime, timezone
        import copy
        
        current_dist = st.session_state.current_distribution
        old_dataset = repos['dataset'].get_by_id(current_dist.dataset_id)
        
        if not old_dataset:
            st.error("❌ Dataset originale non trovato nel database.")
            return False

        # 1. Prepariamo l'URI della nuova distribuzione
        new_dist_uri = f"{BASE_PREFIX}{to_binded_path(str(destination_path))}"
        
        # 2. Logica Critica: Verifica appartenenza gerarchica
        # Se l'URI del dataset NON è contenuto nell'URI della nuova distribuzione
        if old_dataset.uri not in new_dist_uri:
            logger.info("⚠️ Nuova distribuzione fuori dal path del dataset originale. Creazione nuovo Dataset.")
            
            # Creiamo il nuovo dataset ereditando i campi
            new_dataset = Dataset(
                id=None, # Sarà generato dal DB
                uri=new_dist_uri, # Il nuovo dataset ha come radice l'uri della distribuzione
                name=f"dataset_{destination_path.name}",
                languages=copy.deepcopy(old_dataset.languages),
                derived_card=old_dataset.derived_card,
                derived_dataset=current_dist.dataset_id, # Punta al dataset genitore
                dataset_type=old_dataset.dataset_type,
                globs=['*.parquet'] if materialize else [],
                description=f"Dataset derivato da query su {old_dataset.name}",
                source=old_dataset.source,
                version=old_dataset.version,
                license=old_dataset.license,
                step=1,
                issued=datetime.now(timezone.utc),
                modified=datetime.now(timezone.utc)
            )
            
            # Inseriamo il nuovo dataset per ottenere l'ID
            new_dataset_obj = repos['dataset'].insert(new_dataset)
            target_dataset_id = new_dataset_obj.id
            logger.info(f"✅ Creato nuovo Dataset ereditato con ID: {target_dataset_id}")
        else:
            # Caso standard: la distribuzione è una sottocartella del dataset esistente
            target_dataset_id = old_dataset.id

        # 3. Creazione della Nuova Distribuzione
        tags = current_dist.tags or []
        if not isinstance(tags, list):
            tags = list(tags) if tags else []

        new_distribution = Distribution(
            id=None,
            uri=new_dist_uri,
            tokenized_uri=None,
            dataset_id=target_dataset_id, # Punterà al nuovo dataset o al vecchio
            glob='*.parquet',
            format='parquet',
            query=_compact_sql_query(st.session_state.get('executed_query', '')),
            derived_from=current_dist.id,
            split=current_dist.split,
            src_schema={},
            name=f"query__{destination_path.name}",
            description=f"Risultati query su {current_dist.name}",
            lang=current_dist.lang,
            tags=tags + ["query"], 
            license=current_dist.license,
            version=current_dist.version,
            issued=datetime.now(timezone.utc),
            modified=datetime.now(timezone.utc),
            materialized=materialize,
            step=1
        )

        dist_result = repos['distribution'].insert(new_distribution)

        # 4. Aggiornamento globs se materializzato
        if materialize and dist_result:
            target_ds = repos['dataset'].get_by_id(target_dataset_id)
            if target_ds:
                target_ds.globs = generate_dataset_globs(target_ds.uri)
                repos['dataset'].update(target_ds)

        if dist_result:
            st.success(f"✅ Distribuzione registrata con successo (ID: {dist_result.id})")
            return True
        else:
            return False
            
    except Exception as e:
        st.error(f"❌ Errore nella creazione distribuzione/dataset: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    
# MAIN FUNCTION TO SHOW RAW QUERY DATASET INTERFACE #

def show_raw_query_dataset(st):
    """Funzione principale per la query del dataset."""
    _render_navigation_header(st)
    
    # Inizializzazione repository
    repos = _initialize_repositories(st)
    
    # Setup path e metadati
    distribution_path = st.session_state.get("current_distribution_path")
    if not distribution_path:
        st.error("❌ Nessuna distribution selezionata")
        return
      
    file_extension = _get_file_extension(to_internal_path(distribution_path))
    if not file_extension:
        st.warning("[_get_file_extension] ⚠️ Nessun file trovato nella cartella del dataset.")
        return
    
    src_schema = st.session_state.current_distribution.src_schema
    
    # Validazione
    validated_schema = _validate_data_and_schema(st, to_internal_path(distribution_path), src_schema)
    
    if st.session_state.get('valid_for_query', False) and validated_schema:
        _render_main_interface(st, distribution_path, validated_schema, file_extension, repos)
    else:
        _render_alternative_options(st)

def _render_main_interface(st, data_path: str, src_schema: Dict[str, Any], 
                          file_extension: str, repos: Dict):
    """Renderizza l'interfaccia principale di query."""
    # Pulsanti principali
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📋 Visualizza Schema Dataset", key="btn_show_schema"):
            st.session_state.update(show_schema=True, show_query_interface=False)
            st.rerun()
    
    with col2:
        if st.button("💻 Esegui Query SQL", key="btn_show_query"):
            st.session_state.update(show_query_interface=True, show_schema=False)
            st.rerun()
    
    # Funzionalità aggiuntive
    _render_additional_features(st)
    
    # Gestione feature aggiuntive
    _handle_additional_features(st, data_path, file_extension)
    
    # Schema view
    if st.session_state.get('show_schema', False):
        _render_schema_view(st, src_schema)
    
    # Query interface
    if st.session_state.get('show_query_interface', False):
        _render_query_interface(st, data_path, file_extension)
        _execute_sql_query(st)
        _render_query_results(st, repos)

def _render_alternative_options(st):
    """Renderizza opzioni alternative quando la query non è disponibile."""
    if not st.session_state.get('valid_for_query', False):
        col1, col2 = st.columns(2)
        with col1:
            st.info("⚠️ Generazione del mapping attualmente non disponibile")
            '''
            if st.button("🗺️ Genera Mapping", disabled= True):
                st.session_state.current_stage = "select_target_schema"
                st.rerun()
            '''
        with col2:
            if st.button("🔀 Converti Dataset con Mapping esistente"):
                st.session_state.current_stage = "run_parallel_mapping"
                st.rerun()

def _handle_additional_features(st, data_path: str, file_extension: str):
    """Gestisce le funzionalità aggiuntive."""
    if st.session_state.get("show_stats", False):
        st.markdown("---")
        show_dataset_stats(st, data_path, file_extension)
        if st.button("🔼 Chiudi Statistiche", key="close_stats"):
            st.session_state.update(show_stats=False)
            st.rerun()
            
    if st.session_state.get("show_preview", False):
        st.markdown("---")
        show_data_preview(st, data_path, file_extension)
        if st.button("🔼 Chiudi Anteprima", key="close_preview"):
            st.session_state.update(show_preview=False)
            st.rerun()
            
    if st.session_state.get("show_structure", False):
        st.markdown("---")
        show_table_structure(st, data_path, file_extension)
        if st.button("🔼 Chiudi Struttura", key="close_structure"):
            st.session_state.update(show_structure=False)
            st.rerun()

# Funzioni di supporto
def show_dataset_stats(st, dataset_path, file_extension):
    """Mostra statistiche generali del dataset"""
    try:
        with duckdb.connect() as conn:
            query = f"SELECT COUNT(*) as total_rows FROM '{dataset_path}/*{file_extension}'"
            result = conn.execute(query).fetchone()
            
            st.success(f"📊 **Statistiche Dataset**")
            st.metric("Righe totali", f"{result[0]:,}")
            
            total_size = sum(f.stat().st_size for f in Path(dataset_path).glob("*"))
            st.metric("Dimensione totale", f"{total_size / (1024*1024):.2f} MB")
            
    except Exception as e:
        st.error(f"❌ Errore nel calcolo delle statistiche: {str(e)}")

def show_data_preview(st, dataset_path, file_extension):
    """Mostra un'anteprima dei dati"""
    try:
        with duckdb.connect() as conn:
            query = f"SELECT * FROM '{dataset_path}/*{file_extension}' LIMIT 5"
            result = conn.execute(query).fetchdf()
            
            st.success("👀 **Anteprima Dati (prime 5 righe)**")
            st.dataframe(result)
            
    except Exception as e:
        st.error(f"❌ Errore nella visualizzazione dell'anteprima: {str(e)}")

def show_table_structure(st, dataset_path, file_extension):
    """Mostra la struttura delle tabelle"""
    try:
        with duckdb.connect() as conn:
            query = f"DESCRIBE SELECT * FROM '{dataset_path}/*{file_extension}' LIMIT 1"
            result = conn.execute(query).fetchdf()
            
            st.success("🏗️ **Struttura Tabelle**")
            st.dataframe(result)
            
    except Exception as e:
        st.error(f"❌ Errore nella visualizzazione della struttura: {str(e)}")


