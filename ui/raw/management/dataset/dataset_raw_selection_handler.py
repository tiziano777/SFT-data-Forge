import os
import logging

from utils.path_utils import to_internal_path

# === CONFIGURAZIONE VARIABILI D'AMBIENTE E MAPPING DOCKER ===
BASE_PREFIX = os.getenv("BASE_PREFIX")
BASE_PATH = os.getenv("BASE_PATH")
BINDED_BASE_PATH = os.getenv("BINDED_BASE_PATH")

from utils.fs_func import list_dirs, list_files
from utils.streamlit_func import reset_session_state
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from data_class.repository.vocabulary.vocab_license_repository import VocabLicenseRepository
from data_class.repository.vocabulary.vocab_dataset_type_repository import VocabDatasetTypeRepository
from data_class.repository.table.dataset_card_repository import DatasetCardRepository

# === CONFIGURAZIONE LOGGER ===
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

# Chiavi di stato da eliminare
KEYS_TO_DELETE = ["selected_path_parts", "wizard_selection_mode", "wizard_selection_step"]

# === FUNZIONI DI UTILITÀ E NORMALIZZAZIONE ===

def normalize_uri(local_path: str) -> str:
    """
    Converte il percorso interno al container nel formato URI reale del DB (file:///path/...)
    Gestisce il mapping tra BASE_PATH (/app/nfs) e BINDED_BASE_PATH (/nfs).
    """
    abs_local_path = os.path.abspath(local_path).replace("\\", "/")
    
    if abs_local_path.startswith(BASE_PATH):
        db_path = abs_local_path.replace(BASE_PATH, BINDED_BASE_PATH, 1)
    else:
        db_path = abs_local_path

    db_path = "/" + db_path.lstrip("/")
    prefix = BASE_PREFIX.rstrip("/")
    full_uri = f"{prefix}{db_path}"
    
    if full_uri.startswith("file:/") and not full_uri.startswith("file:///"):
        full_uri = full_uri.replace("file:/", "file:///", 1)
    elif full_uri.startswith("file:////"):
        full_uri = full_uri.replace("file:////", "file:///", 1)
        
    return full_uri

def denormalize_uri(uri: str) -> str:
    """
    Gestisce il mapping inverso: BINDED_BASE_PATH -> BASE_PATH
    """
    # Rimuovi il prefixo file://
    if uri.startswith(BASE_PREFIX):
        path = uri.replace(BASE_PREFIX, "", 1)
    else:
        path = uri
    
    # Rimuovi eventuali /// iniziali e normalizza
    path = "/" + path.lstrip("/")
    
    # Traduzione Path: Host (Binded) -> Container
    local_path = to_internal_path(path)

    return os.path.normpath(local_path)

def check_child_dataset_exists(dataset_repo: DatasetRepository, selected_path: str) -> bool:
    try:
        selected_uri = normalize_uri(selected_path)
        children = dataset_repo.get_by_uri_prefix(f"{selected_uri}/")
        return len(children) > 0
    except Exception as e:
        logger.error(f"Errore verifica figli: {e}")
        return False

def check_parent_dataset_exists(dataset_repo: DatasetRepository, selected_path: str, base_path: str) -> str:
    try:
        current_path = os.path.normpath(selected_path)
        base_path_norm = os.path.normpath(base_path)
        
        while current_path != base_path_norm and current_path != os.path.dirname(current_path):
            parent_path = os.path.dirname(current_path)
            if len(parent_path) < len(base_path_norm):
                break
            
            parent_uri = normalize_uri(parent_path)
            if dataset_repo.get_by_uri(parent_uri):
                return parent_uri
            current_path = parent_path
        return ""
    except Exception as e:
        logger.error(f"Errore verifica padri: {e}")
        return ""

# === WIZARD INTERFACE ===

def show_dataset_selection_wizard(st_app, base_path: str):
    """
    Wizard principale per la selezione del dataset.
    Step 1: Scelta modalità (Nuovo dataset / Dataset esistente)
    Step 2a: Se nuovo → Navigazione filesystem o URI manuale
    Step 2b: Se esistente → Ricerca con filtri
    """
    
    dataset_repo = DatasetRepository(st_app.session_state.db_manager)
    
    # Inizializza wizard step
    if "wizard_selection_step" not in st_app.session_state:
        st_app.session_state.wizard_selection_step = 1
    
    if "wizard_selection_mode" not in st_app.session_state:
        st_app.session_state.wizard_selection_mode = None
    
    step = st_app.session_state.wizard_selection_step
    
    st_app.header("📂 Selezione Dataset")
    st_app.progress((step - 1) / 2, text=f"Step {step}/2")
    
    if step == 1:
        _show_mode_selection_step(st_app)
    elif step == 2:
        mode = st_app.session_state.wizard_selection_mode
        if mode == "new":
            _show_new_dataset_navigation(st_app, base_path, dataset_repo)
        elif mode == "existing":
            _show_existing_dataset_search(st_app, base_path, dataset_repo)

def _show_mode_selection_step(st_app):
    """Step 1: Selezione modalità censimento"""
    st_app.markdown("### 🎯 Seleziona modalità di censimento")
    
    col1, col2 = st_app.columns(2)
    
    with col1:
        if st_app.button("🆕 Censisci NUOVO Dataset", use_container_width=True, type="primary"):
            st_app.session_state.wizard_selection_mode = "new"
            st_app.session_state.wizard_selection_step = 2
            st_app.session_state.selected_path_parts = []
            st_app.rerun()
        st_app.markdown("""
        **Cosa fa:**
        - Naviga nel filesystem per selezionare un nuovo mount point
        - Oppure incolla direttamente un URI
        - Censisce un dataset non ancora registrato
        """)
    
    with col2:
        if st_app.button("🔍 Cerca Dataset ESISTENTE", use_container_width=True, type="secondary"):
            st_app.session_state.wizard_selection_mode = "existing"
            st_app.session_state.wizard_selection_step = 2
            st_app.rerun()
        st_app.markdown("""
        **Cosa fa:**
        - Ricerca tra i dataset già censiti (step=1)
        - Usa filtri per nome, lingua, licenza, tipo
        - Modifica metadati di un dataset esistente
        """)
    
    if st_app.button("🏠 Home"):
        reset_session_state(st_app, KEYS_TO_DELETE)
        st_app.session_state.current_stage = "home"
        st_app.rerun()

def _show_new_dataset_navigation(st_app, base_path: str, dataset_repo: DatasetRepository):
    """Step 2a: Navigazione filesystem per nuovo dataset"""
    st_app.markdown("### 🆕 Censimento Nuovo Dataset")
    
    # Tab per separare navigazione guidata da URI manuale
    tab1, tab2 = st_app.tabs(["📁 Navigazione Guidata", "📝 URI Manuale"])
    
    with tab1:
        _show_filesystem_navigation(st_app, base_path, dataset_repo)
    
    with tab2:
        _show_manual_uri_input(st_app, base_path, dataset_repo)
    
    # Bottone indietro comune
    if st_app.button("← Indietro alla scelta modalità"):
        st_app.session_state.wizard_selection_step = 1
        st_app.session_state.wizard_selection_mode = None
        st_app.rerun()

def _show_filesystem_navigation(st_app, base_path: str, dataset_repo: DatasetRepository):
    """Navigazione guidata nel filesystem"""
    if "selected_path_parts" not in st_app.session_state:
        st_app.session_state.selected_path_parts = []

    current_path = os.path.normpath(os.path.join(base_path, *st_app.session_state.selected_path_parts))
    st_app.write(f"**Percorso corrente:** `{current_path}`")

    try:
        subdirs = list_dirs(current_path)
        files = list_files(current_path)
    except Exception as e:
        logger.error(f"Errore list_dirs: {e}")
        st_app.error("Errore nel caricamento delle sottocartelle.")
        subdirs, files = [], []

    if subdirs:
        selected_subdir = st_app.selectbox(
            "Seleziona sottocartella", 
            [""] + subdirs, 
            key="folder_select_new"
        )
        if selected_subdir:
            st_app.session_state.selected_path_parts.append(selected_subdir)
            st_app.rerun()
    else:
        st_app.info("📁 Nessuna sottocartella trovata. Questo potrebbe essere il mount point del dataset.")

    if files:
        with st_app.expander(f"📄 File presenti ({len(files)})"):
            st_app.write(", ".join(files[:10]))
            if len(files) > 10:
                st_app.write(f"... e altri {len(files) - 10} file")

    # Bottoni azione
    col1, col2, col3 = st_app.columns(3)
    with col1:
        if st_app.session_state.selected_path_parts and st_app.button("⬅️ Indietro", key="nav_back"):
            st_app.session_state.selected_path_parts.pop()
            st_app.rerun()
    with col2:
        if st_app.button("✅ Conferma selezione", type="primary", key="nav_confirm"):
            _handle_new_dataset_confirmation(st_app, dataset_repo, current_path, base_path)
    with col3:
        if st_app.button("🏠 Home", key="nav_home"):
            reset_session_state(st_app, KEYS_TO_DELETE)
            st_app.session_state.current_stage = "home"
            st_app.rerun()

def _show_manual_uri_input(st_app, base_path: str, dataset_repo: DatasetRepository):
    """Input manuale dell'URI del dataset"""
    st_app.markdown("#### Incolla URI completo del dataset")
    st_app.info(f"Formato atteso: `/path/to/dataset`")
    
    manual_uri = st_app.text_input(
        "URI Dataset",
        placeholder=f"/esempio/path/dataset",
        key="manual_uri_input"
    )
    
    if st_app.button("✅ Valida e Conferma URI", type="primary", key="uri_confirm"):
        if not manual_uri.strip():
            st_app.error("❌ URI vuoto. Inserisci un URI valido.")
            return
        
        # Converti URI in path locale per validazione
        local_path = denormalize_uri(manual_uri)
        
        if not os.path.exists(local_path):
            st_app.warning(f"⚠️ Path non accessibile dal container: {local_path}")
            st_app.info("Procedo comunque con la conferma (il path potrebbe esistere nell'ambiente di produzione)")
        
        _handle_new_dataset_confirmation(st_app, dataset_repo, local_path, base_path)

def _handle_new_dataset_confirmation(st_app, dataset_repo: DatasetRepository, selected_path: str, base_path: str):
    """Gestisce la conferma di selezione per un nuovo dataset"""
    dataset_uri = normalize_uri(selected_path)
    
    logger.info(f"🆕 Tentativo censimento nuovo dataset: {dataset_uri}")

    # Verifica che il dataset non esista già
    try:
        existing_ds = dataset_repo.get_by_uri(dataset_uri)
        if existing_ds:
            st_app.error(f"❌ Dataset già censito con URI: {dataset_uri}")
            st_app.info("Usa la modalità 'Cerca Dataset ESISTENTE' per modificarlo.")
            return
    except Exception as e:
        logger.error(f"💥 Errore verifica esistenza: {e}")

    # Controlli integrità gerarchica
    if check_parent_dataset_exists(dataset_repo, selected_path, base_path):
        st_app.error("❌ Errore: Dataset padre già registrato. Non puoi censire un sotto-path.")
        return
    
    if check_child_dataset_exists(dataset_repo, selected_path):
        st_app.error("❌ Errore: Dataset figli già presenti. Non puoi censire un path genitore.")
        return

    # Tutto ok: prepara pre-fill vuoto e vai al form
    st_app.session_state.prefill_dataset = None
    st_app.session_state.prefill_card_data = None
    st_app.session_state.selected_dataset_uri = dataset_uri
    st_app.session_state.selected_dataset_path = selected_path
    st_app.session_state.current_stage = "raw_dataset_metadata_editing"
    st_app.rerun()

def _show_existing_dataset_search(st_app, base_path: str, dataset_repo: DatasetRepository):
    """Step 2b: Ricerca dataset esistenti con filtri"""
    st_app.markdown("### 🔍 Ricerca Dataset Esistente")
    
    # Carica vocabolari per filtri
    try:
        vocab_lang_repo = VocabLanguageRepository(st_app.session_state.db_manager)
        vocab_license_repo = VocabLicenseRepository(st_app.session_state.db_manager)
        vocab_type_repo = VocabDatasetTypeRepository(st_app.session_state.db_manager)
        
        lang_options = [l.code for l in vocab_lang_repo.get_all() if getattr(l, "code", None)]
        license_options = [l.code for l in vocab_license_repo.get_all() if getattr(l, "code", None)]
        type_options = [t.code for t in vocab_type_repo.get_all() if getattr(t, "code", None)]
    except Exception:
        lang_options, license_options, type_options = [], [], []
    
    # Barra di ricerca e filtri
    col_s1, col_s2, col_s3, col_s4 = st_app.columns([4, 2, 2, 2])
    with col_s1:
        search_query = st_app.text_input("🔍 Cerca per nome", key="dataset_search_existing").strip().lower()
    with col_s2:
        selected_langs = st_app.multiselect("Lingue", options=lang_options, key="filter_langs")
    with col_s3:
        selected_licenses = st_app.multiselect("Licenze", options=license_options, key="filter_licenses")
    with col_s4:
        selected_types = st_app.multiselect("Tipo", options=type_options, key="filter_types")
    
    # Recupera tutti i dataset con step=1
    try:
        all_datasets = dataset_repo.get_all()
        # Filtra solo quelli con step=1
        datasets_step1 = [ds for ds in all_datasets if getattr(ds, 'step', None) == 1]
    except Exception as e:
        logger.error(f"Errore recupero dataset: {e}")
        st_app.error("Errore nel caricamento dei dataset.")
        datasets_step1 = []
    
    # Applica filtri
    filtered_datasets = []
    for ds in datasets_step1:
        # Filtro nome
        if search_query and search_query not in getattr(ds, 'name', '').lower():
            continue
        
        # Filtro lingue
        if selected_langs:
            ds_langs = getattr(ds, 'languages', []) or []
            if not any(lang in ds_langs for lang in selected_langs):
                continue
        
        # Filtro licenze
        if selected_licenses:
            ds_license = getattr(ds, 'license', None)
            if ds_license not in selected_licenses:
                continue
        
        # Filtro tipo
        if selected_types:
            ds_type = getattr(ds, 'dataset_type', None)
            if ds_type not in selected_types:
                continue
        
        filtered_datasets.append(ds)
    
    # Mostra risultati
    st_app.markdown(f"**Risultati:** {len(filtered_datasets)} dataset trovati")
    
    if not filtered_datasets:
        st_app.info("Nessun dataset trovato con i filtri selezionati.")
    else:
        # Crea lista di opzioni per selectbox
        dataset_options = {
            f"{ds.name} ({ds.uri})": ds for ds in filtered_datasets
        }
        
        selected_option = st_app.selectbox(
            "Seleziona dataset da modificare",
            options=[""] + list(dataset_options.keys()),
            key="existing_dataset_select"
        )
        
        if selected_option:
            selected_ds = dataset_options[selected_option]
            
            # Mostra anteprima
            with st_app.expander("👁️ Anteprima Dataset", expanded=True):
                col1, col2 = st_app.columns(2)
                with col1:
                    st_app.write(f"**Nome:** {selected_ds.name}")
                    st_app.write(f"**URI:** {selected_ds.uri}")
                    st_app.write(f"**Versione:** {getattr(selected_ds, 'version', 'N/A')}")
                with col2:
                    st_app.write(f"**Licenza:** {getattr(selected_ds, 'license', 'N/A')}")
                    st_app.write(f"**Tipo:** {getattr(selected_ds, 'dataset_type', 'N/A')}")
                    langs = getattr(selected_ds, 'languages', [])
                    st_app.write(f"**Lingue:** {', '.join(langs) if langs else 'N/A'}")
                
                if getattr(selected_ds, 'description', None):
                    st_app.markdown(f"**Descrizione:** {selected_ds.description}")
            
            if st_app.button("✅ Modifica questo dataset", type="primary", key="confirm_existing"):
                _handle_existing_dataset_selection(st_app, selected_ds, dataset_repo)
    
    # Bottone indietro
    if st_app.button("← Indietro alla scelta modalità", key="existing_back"):
        st_app.session_state.wizard_selection_step = 1
        st_app.session_state.wizard_selection_mode = None
        st_app.rerun()

def _handle_existing_dataset_selection(st_app, dataset_entity, dataset_repo: DatasetRepository):
    """Gestisce la selezione di un dataset esistente per modifica"""
    logger.info(f"✏️ Selezionato dataset esistente: {dataset_entity.uri}")
    
    # Prepara pre-fill dai dati esistenti
    st_app.session_state.prefill_dataset = {
        'id': dataset_entity.id,
        'uri': dataset_entity.uri,
        'name': dataset_entity.name,
        'description': getattr(dataset_entity, 'description', ''),
        'license': getattr(dataset_entity, 'license', None),
        'source': getattr(dataset_entity, 'source', ''),
        'version': getattr(dataset_entity, 'version', '1.0.0'),
        'languages': getattr(dataset_entity, 'languages', []) or [],
        'globs': getattr(dataset_entity, 'globs', ''),
        'derived_card': getattr(dataset_entity, 'derived_card', None),
        'derived_dataset': getattr(dataset_entity, 'derived_dataset', None),
        'step': getattr(dataset_entity, 'step', 1),
        'dataset_type': getattr(dataset_entity, 'dataset_type', None)
    }
    
    # Recupera card se presente
    card_id = st_app.session_state.prefill_dataset.get('derived_card')
    if card_id:
        try:
            card_repo = DatasetCardRepository(st_app.session_state.db_manager)
            card_entity = card_repo.get_by_id(card_id) if hasattr(card_repo, 'get_by_id') else card_repo.get(card_id)
            if card_entity:
                st_app.session_state.prefill_card_data = {
                    'dataset_name': getattr(card_entity, 'dataset_name', ''),
                    'dataset_id': getattr(card_entity, 'dataset_id', ''),
                    'modality': getattr(card_entity, 'modality', None),
                    'dataset_description': getattr(card_entity, 'dataset_description', ''),
                    'languages': getattr(card_entity, 'languages', []) or [],
                    'license': getattr(card_entity, 'license', None),
                    'quality': getattr(card_entity, 'quality', 3),
                    'notes': getattr(card_entity, 'notes', ''),
                    'source_url': getattr(card_entity, 'source_url', ''),
                    'download_url': getattr(card_entity, 'download_url', ''),
                    'has_reasoning': bool(getattr(card_entity, 'has_reasoning', False)),
                    'fields': getattr(card_entity, 'fields', []),
                    'sources': getattr(card_entity, 'sources', []),
                    'source_type': getattr(card_entity, 'source_type', None),
                    'vertical': getattr(card_entity, 'vertical', []),
                    'contents': getattr(card_entity, 'contents', [])
                }
                logger.info("✅ Card caricata per pre-fill.")
        except Exception as e:
            logger.warning(f"⚠️ Errore caricamento card: {e}")
            st_app.session_state.prefill_card_data = None
    else:
        st_app.session_state.prefill_card_data = None
    
    # Converti URI in path locale
    local_path = denormalize_uri(dataset_entity.uri)
    
    st_app.session_state.selected_dataset_uri = dataset_entity.uri
    st_app.session_state.selected_dataset_path = local_path
    st_app.session_state.current_stage = "raw_dataset_metadata_editing"
    st_app.rerun()

def show_dataset_selection(st_app, base_path: str):
    """Entry point principale per la selezione del dataset"""
    show_dataset_selection_wizard(st_app, base_path)

