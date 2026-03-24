# ui/dataset_selection/processed/dataset_processed_selection_handler.py
import os
import logging

from typing import List
from utils.streamlit_func import reset_session_state
from utils.fs_func import list_dirs, list_files
from utils.path_utils import to_binded_path, to_internal_path
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.entity.table.dataset import Dataset
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from data_class.repository.vocabulary.vocab_license_repository import VocabLicenseRepository
from data_class.repository.vocabulary.vocab_dataset_type_repository import VocabDatasetTypeRepository

BASE_PREFIX = os.getenv("BASE_PREFIX")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")

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
KEYS_TO_DELETE = [
    "selected_path_parts", "available_datasets", "available_dataset_labels",
    "wizard_mode", "wizard_step", "wizard_parent_dataset",
    "step_datasets", "step_dataset_labels",
    "path_confirmed", "editing_uri", "editing_path",
    "prefill_from_parent", "prefill_parent_uri", "prefill_parent_data",
]


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_uri_to_path(uri: str) -> str:
    if not uri:
        return ""
    return os.path.normpath(uri.replace(BASE_PREFIX, ""))

def _normalize_uri(path: str) -> str:
    """Converte un path fisico in URI con BASE_PREFIX."""
    normalized = os.path.normpath(path)
    return f"{BASE_PREFIX}{normalized}"

def _build_dataset_labels(datasets: List[Dataset], DATA_DIR: str) -> List[str]:
    return [
        f"{dataset.name} - {dataset.version} - {dataset.uri.replace(BASE_PREFIX + DATA_DIR, '')}"
        for dataset in datasets
    ]

def _build_labels(datasets: List[Dataset]) -> List[str]:
    return [
        f"{dataset.name} - {dataset.version} [{dataset.uri}]"
        for dataset in datasets
    ]

# ─────────────────────────────────────────────────────────────────────────────
# Controlli esistenza dataset nel DB
# ─────────────────────────────────────────────────────────────────────────────

def _validate_uri_conflicts(target_uri: str, all_step2_datasets: list) -> dict:
    """
    Controlla conflitti tra target_uri e tutti gli URI step=2 già nel DB.
    Tutti i confronti avvengono in memoria sugli URI esatti restituiti dal DB,
    senza nessuna trasformazione di path.

    Ritorna un dict con:
      - 'exact':    URI identico già censito (stringa o None)
      - 'parents':  URI già censiti che sono PREFISSO del target (target sarebbe figlio)
      - 'children': URI già censiti che hanno target come PREFISSO (target sarebbe padre)
    """
    target_norm = target_uri.rstrip("/")
    target_prefix = target_norm + "/"

    exact = None
    parents = []
    children = []

    for ds in all_step2_datasets:
        uri = (ds.uri or "").rstrip("/")
        if not uri:
            continue
        uri_prefix = uri + "/"

        if uri == target_norm:
            exact = ds.uri
        elif target_norm.startswith(uri_prefix):
            # uri è antenato del target → target sarebbe figlio
            parents.append(ds.uri)
        elif ds.uri.startswith(target_prefix):
            # uri è discendente del target → target sarebbe padre
            children.append(ds.uri)

    return {"exact": exact, "parents": parents, "children": children}

# ─────────────────────────────────────────────────────────────────────────────
# Caricamento dataset dal DB
# ─────────────────────────────────────────────────────────────────────────────

def _load_available_datasets(st_app, DATA_DIR, dataset_repo: DatasetRepository):
    try:
        if DATA_DIR:
            uri_prefix = f"{BASE_PREFIX}{DATA_DIR}"
            datasets = dataset_repo.get_by_uri_prefix_or_step(uri_prefix, step=2)
            logger.info(f"Caricati {len(datasets)} dataset (Path: {uri_prefix} | Step: 2)")
            st_app.session_state.available_datasets = datasets
            st_app.session_state.available_dataset_labels = _build_dataset_labels(datasets, DATA_DIR)
            if not datasets:
                st_app.warning(f"❌ Nessun dataset trovato con URI nel path {uri_prefix} o con Step 2.")
                return False
        else:
            st_app.error("DATA_DIR (Processed) non configurato correttamente.")
            return False
        return True
    except Exception as e:
        st_app.error("Errore nel caricamento dei dataset dal database.")
        logger.exception(f"Errore in _load_available_datasets: {e}")
        return False


def _load_step_datasets(st_app, dataset_repo: DatasetRepository, step: int):
    """Carica tutti i dataset con lo step specificato come candidati padre per la strada B del wizard."""
    try:
        datasets = dataset_repo.get_by_step(step=step)
        logger.info(f"Caricati {len(datasets)} dataset con Step={step}")
        st_app.session_state.step_datasets = datasets
        st_app.session_state.step_dataset_labels = _build_labels(datasets)
        if not datasets:
            st_app.warning(f"❌ Nessun dataset con Step={step} trovato. Impossibile procedere.")
            return False
        return True
    except Exception as e:
        st_app.error(f"Errore nel caricamento dei dataset Step={step}.")
        logger.exception(f"Errore in _load_step_datasets: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# Vocabolari e filtri
# ─────────────────────────────────────────────────────────────────────────────

def _get_vocab_options(st_app):
    """Carica le opzioni dei vocabolari per i filtri."""
    try:
        vocab_lang_repo = VocabLanguageRepository(st_app.session_state.db_manager)
        vocab_license_repo = VocabLicenseRepository(st_app.session_state.db_manager)
        vocab_type_repo = VocabDatasetTypeRepository(st_app.session_state.db_manager)
        langs = vocab_lang_repo.get_all() if hasattr(vocab_lang_repo, "get_all") else []
        licenses = vocab_license_repo.get_all() if hasattr(vocab_license_repo, "get_all") else []
        types = vocab_type_repo.get_all() if hasattr(vocab_type_repo, "get_all") else []
        return (
            [l.code for l in langs if getattr(l, "code", None)],
            [l.code for l in licenses if getattr(l, "code", None)],
            [t.code for t in types if getattr(t, "code", None)],
        )
    except Exception as e:
        logger.error(f"Errore caricamento vocabulary options: {e}")
        return [], [], []

def _apply_filters(st_app, datasets, suffix=""):
    """Barra di ricerca + filtri multiselect. Ritorna la lista filtrata."""
    lang_options, license_options, type_options = _get_vocab_options(st_app)

    col_s1, col_s2, col_s3, col_s4 = st_app.columns([4, 2, 2, 2])
    with col_s1:
        search_query = st_app.text_input(
            "🔍 Cerca (name, description, uri)",
            key=f"dataset_search_input{suffix}"
        ).strip().lower()
    with col_s2:
        selected_langs = st_app.multiselect("Lingue (iso)", options=lang_options, key=f"lang_filter{suffix}")
    with col_s3:
        selected_licenses = st_app.multiselect("Licenze", options=license_options, key=f"license_filter{suffix}")
    with col_s4:
        selected_types = st_app.multiselect("Type", options=type_options, key=f"type_filter{suffix}")

    filtered = []
    for ds in datasets:
        if search_query:
            dn = (getattr(ds, "name", "") or "").lower()
            dd = (getattr(ds, "description", "") or "").lower()
            du = (getattr(ds, "uri", "") or "").lower()
            if not (search_query in dn or search_query in dd or search_query in du):
                continue

        if selected_langs:
            ds_langs = getattr(ds, "languages", None) or getattr(ds, "language", None) or []
            if isinstance(ds_langs, str):
                ds_langs = [s.strip() for s in ds_langs.split(",") if s.strip()]
            if not any(lang.lower() in [l.lower() for l in ds_langs] for lang in selected_langs):
                continue

        if selected_licenses:
            ds_license = getattr(ds, "license", None) or getattr(ds, "license_code", None) or ""
            ds_licenses = [l.lower() for l in ds_license] if isinstance(ds_license, list) else [str(ds_license).lower()]
            if not any(lc.lower() in ds_licenses for lc in selected_licenses):
                continue

        if selected_types:
            ds_type = getattr(ds, "dataset_type", None) or getattr(ds, "type", None) or ""
            ds_types = [t.lower() for t in ds_type] if isinstance(ds_type, list) else [str(ds_type).lower()]
            if not any(t.lower() in ds_types for t in selected_types):
                continue

        filtered.append(ds)

    if not filtered:
        st_app.warning("❌ Nessun dataset corrisponde ai filtri selezionati.")

    return filtered

# ─────────────────────────────────────────────────────────────────────────────
# WIZARD — Step 0: scelta modalità
# ─────────────────────────────────────────────────────────────────────────────

def _show_wizard_choice(st_app):
    st_app.header("📂 Censimento Dataset Processato (Step 2)")
    st_app.markdown("Scegli come vuoi procedere:")
    st_app.markdown("---")

    col_a, col_b = st_app.columns(2)
    with col_a:
        st_app.markdown("### 📋 Dataset già censito")
        st_app.markdown(
            "Seleziona un dataset processato già presente nel catalogo "
            "e accedi al form di metadatazione per aggiornarne le informazioni."
        )
        if st_app.button("✅ Seleziona dataset esistente", key="wizard_choice_existing", use_container_width=True, type="primary"):
            st_app.session_state.wizard_mode = "existing"
            st_app.session_state.wizard_step = 0
            st_app.rerun()

    with col_b:
        st_app.markdown("### 🆕 Dataset preprocessato non ancora acquisito")
        st_app.markdown(
            "Censisci un dataset che hai processato manualmente a partire da un dataset "
            "con Step=1, ma che non è ancora stato registrato nel catalogo."
        )
        if st_app.button("➕ Censisci nuovo dataset derivato", key="wizard_choice_new", use_container_width=True):
            st_app.session_state.wizard_mode = "new_untracked"
            st_app.session_state.wizard_step = 1
            st_app.rerun()

    st_app.markdown("---")
    if st_app.button("🏠 Torna alla Home", key="home_button_wizard"):
        reset_session_state(st_app, KEYS_TO_DELETE)
        st_app.session_state.current_stage = "home"
        st_app.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# WIZARD — Strada A: selezione dataset esistente censito
# ─────────────────────────────────────────────────────────────────────────────

def _show_existing_selection(st_app, DATA_DIR, dataset_repo):
    st_app.header("📂 Seleziona Dataset Processato o Step 2")

    st_app.session_state.setdefault("available_datasets", [])
    st_app.session_state.setdefault("available_dataset_labels", [])

    if not st_app.session_state.available_datasets:
        if not _load_available_datasets(st_app, DATA_DIR, dataset_repo):
            _back_to_wizard_choice(st_app)
            return

    datasets = st_app.session_state.available_datasets or []
    filtered_datasets = _apply_filters(st_app, datasets, suffix="_existing")
    dataset_labels_for_ui = _build_dataset_labels(filtered_datasets, DATA_DIR)

    dataset_options = [""] + dataset_labels_for_ui
    selected_label = st_app.selectbox(
        "Scegli dataset (In-Path o validati Step 2)",
        dataset_options,
        index=0,
        key="dataset_select_existing"
    )

    label_to_record = {lab: rec for lab, rec in zip(dataset_labels_for_ui, filtered_datasets)}

    if selected_label and selected_label in label_to_record:
        selected_dataset = label_to_record[selected_label]
        st_app.session_state.selected_dataset_id = selected_dataset.id
        st_app.session_state.selected_dataset_uri = selected_dataset.uri
        st_app.session_state.selected_dataset_path = _normalize_uri_to_path(selected_dataset.uri)

        if selected_dataset.step == 2:
            st_app.success(f"✅ Dataset validato per il layer Processed (Step 2): {selected_dataset.name}")
        else:
            st_app.info(f"📂 Dataset rilevato nel path fisico (Step {selected_dataset.step}): {selected_dataset.name}")

        st_app.subheader("📋 Informazioni Dataset")
        col1, col2 = st_app.columns(2)
        with col1:
            st_app.write(f"**Nome:** {selected_dataset.name}")
            st_app.write(f"**Versione:** {selected_dataset.version}")
            st_app.write(f"**URI:** {selected_dataset.uri}")
            st_app.write(f"**Type:** {selected_dataset.dataset_type}")
        with col2:
            st_app.write(f"**Step Corrente:** {selected_dataset.step}")
            st_app.write(f"**Licenza:** {selected_dataset.license or 'Non specificata'}")
            st_app.write(f"**Descrizione:** {selected_dataset.description or 'Non specificata'}")

        if st_app.button("📝 Procedi alla Metadatazione", key="proceed_to_metadata_existing", type="primary"):
            logger.info(f"✅ Passaggio alla metadatazione per DS ID: {selected_dataset.id}")
            st_app.session_state.prefill_from_parent = False
            st_app.session_state.current_stage = "processed_dataset_metadata_editing"
            st_app.rerun()

    _back_to_wizard_choice(st_app)


# ─────────────────────────────────────────────────────────────────────────────
# WIZARD — Strada B, Step 1: selezione dataset padre (step=1) o derivato (step=2)
# ─────────────────────────────────────────────────────────────────────────────

def _show_parent_selection(st_app, dataset_repo):
    st_app.header("🆕 Censimento Dataset Derivato — Selezione Origine")
    
    # Toggle per decidere la sorgente della derivazione (Step 1 o Step 2)
    is_same_step = st_app.toggle(
        "Il dataset deriva da un'elaborazione di un dataset allo stesso livello (Step 2)?", 
        value=False,
        help="Attiva se stai creando un dataset a partire da uno già processato (Step 2). Disattiva se deriva dallo Step 1."
    )

    target_step = 2 if is_same_step else 1
    
    st_app.info(
        f"Seleziona il dataset di **Step={target_step}** da cui deriva il tuo dataset. "
        "I metadati verranno ereditati e pre-compilati nel form successivo."
    )

    # Chiavi di session state dinamiche per isolare i dati tra i due step
    datasets_key = f"step{target_step}_datasets"
    labels_key = f"step{target_step}_dataset_labels"

    # Inizializzazione chiavi nel session_state se non presenti
    if datasets_key not in st_app.session_state:
        st_app.session_state[datasets_key] = []
    if labels_key not in st_app.session_state:
        st_app.session_state[labels_key] = []

    # Caricamento dei dataset se la lista è vuota
    if not st_app.session_state[datasets_key]:
        if not _load_step_datasets(st_app, dataset_repo, step=target_step):
            # Se la funzione fallisce o non trova dati, _load_step_datasets gestisce già il warning
            _back_to_wizard_choice(st_app)
            return
        
        # Dopo il caricamento con successo, spostiamo i dati caricati nelle chiavi specifiche per il toggle
        # (Assumendo che _load_step_datasets scriva in st_app.session_state.step_datasets)
        st_app.session_state[datasets_key] = st_app.session_state.get("step_datasets", [])
        st_app.session_state[labels_key] = st_app.session_state.get("step_dataset_labels", [])

    datasets = st_app.session_state[datasets_key]
    filtered_datasets = _apply_filters(st_app, datasets, suffix=f"_step{target_step}")
    
    # Ricostruiamo le label sui dataset filtrati
    current_labels = _build_labels(filtered_datasets)

    dataset_options = [""] + current_labels
    selected_label = st_app.selectbox(
        f"Seleziona il dataset Step={target_step} sorgente",
        dataset_options,
        index=0,
        key=f"dataset_select_step{target_step}"
    )

    label_to_record = {lab: rec for lab, rec in zip(current_labels, filtered_datasets)}

    if selected_label and selected_label in label_to_record:
        parent_dataset = label_to_record[selected_label]

        st_app.subheader("📋 Dataset Sorgente Selezionato")
        col1, col2 = st_app.columns(2)
        with col1:
            st_app.write(f"**Nome:** {parent_dataset.name}")
            st_app.write(f"**Versione:** {parent_dataset.version}")
            st_app.write(f"**URI:** {parent_dataset.uri}")
            st_app.write(f"**Type:** {getattr(parent_dataset, 'dataset_type', 'N/D')}")
        with col2:
            st_app.write(f"**Step Sorgente:** {parent_dataset.step}")
            st_app.write(f"**Licenza:** {parent_dataset.license or 'Non specificata'}")
            st_app.write(f"**Lingue:** {', '.join(parent_dataset.languages) if getattr(parent_dataset, 'languages', None) else 'N/D'}")
            st_app.write(f"**Descrizione:** {parent_dataset.description or 'Non specificata'}")

        st_app.markdown("---")
        st_app.markdown(
            f"**Confermando**, il nuovo dataset erediterà i metadati di `{parent_dataset.name}`. "
            "Potrai modificare tutti i campi nel form successivo."
        )

        if st_app.button("✅ Conferma dipendenza e procedi", key="confirm_parent", type="primary"):
            st_app.session_state.wizard_parent_dataset = parent_dataset
            # Reset navigazione per il prossimo step
            st_app.session_state.pop("selected_path_parts", None)
            st_app.session_state.pop("path_confirmed", None)
            st_app.session_state.wizard_step = 2
            st_app.rerun()

    _back_to_wizard_choice(st_app)

# ─────────────────────────────────────────────────────────────────────────────
# WIZARD — Strada B, Step 2: navigazione filesystem e selezione URI
# ─────────────────────────────────────────────────────────────────────────────

def _show_filesystem_navigation(st_app, dataset_repo: DatasetRepository):
    """
    Navigazione guidata nel filesystem a partire da PROCESSED_DATA_DIR.
    Include navigazione reattiva e tasto rapido per risalire i livelli.
    """
    parent = st_app.session_state.get("wizard_parent_dataset")
    st_app.header("🆕 Censimento Dataset Derivato — Selezione Percorso")

    if parent:
        st_app.info(f"📦 Dataset padre: **{parent.name}** (`{parent.uri}`)")

    base_path = PROCESSED_DATA_DIR
    if not base_path:
        st_app.error("❌ Variabile d'ambiente PROCESSED_DATA_DIR non configurata.")
        _back_to_wizard_choice(st_app)
        return

    # Inizializza parti del path selezionato
    if "selected_path_parts" not in st_app.session_state:
        st_app.session_state.selected_path_parts = []

    # Path fisico corrente (leggibile dal container)
    internal_base = to_internal_path(base_path)
    current_internal_path = os.path.normpath(
        os.path.join(internal_base, *st_app.session_state.selected_path_parts)
    )

    # Path "binded" (mostrato all'utente e salvato nel DB)
    current_binded_path = to_binded_path(os.path.normpath(
        os.path.join(base_path, *st_app.session_state.selected_path_parts)
    ))
    current_uri = _normalize_uri(current_binded_path)

    st_app.write(f"**Percorso corrente:** `{current_uri}`")

    # 1. LETTURA FILESYSTEM
    try:
        subdirs = list_dirs(current_internal_path)
        files = list_files(current_internal_path)
    except Exception as e:
        logger.error(f"Errore list_dirs: {e}")
        st_app.error("Errore nel caricamento delle sottocartelle.")
        subdirs, files = [], []

    # 2. NAVIGAZIONE REATTIVA + TASTO RAPIDO "BACK"
    col_nav, col_up = st_app.columns([0.8, 0.2])
    
    with col_nav:
        selected_subdir = st_app.selectbox(
            "📁 Seleziona sottocartella per scendere:",
            [""] + sorted(subdirs),
            key=f"folder_select_{len(st_app.session_state.selected_path_parts)}",
            disabled=not subdirs
        )
        if selected_subdir:
            st_app.session_state.selected_path_parts.append(selected_subdir)
            st_app.session_state.pop("path_confirmed", None)
            st_app.rerun()

    with col_up:
        st_app.write(" ") # Spacer per allineamento verticale con la selectbox
        st_app.write(" ")
        if st_app.button("⬅️ Back", key="quick_up", help="Torna alla cartella superiore", 
                        disabled=not st_app.session_state.selected_path_parts):
            st_app.session_state.selected_path_parts.pop()
            st_app.session_state.pop("path_confirmed", None)
            st_app.rerun()

    if not subdirs:
        st_app.info("📁 Nessuna sottocartella trovata qui.")

    # 3. ANTEPRIMA FILE
    if files:
        with st_app.expander(f"📄 File presenti ({len(files)})"):
            st_app.write(", ".join(files[:15]))
            if len(files) > 15:
                st_app.write(f"... e altri {len(files) - 15} file")

    st_app.markdown("---")

    # 4. BOTTONI AZIONE GLOBALI
    col1, col2, col3 = st_app.columns(3)

    with col1:
        # Torna alla scelta del padre se siamo alla root, altrimenti mostra placeholder o altro
        _back_to_wizard_choice(st_app, label="🔙 Cambia dataset padre")

    with col2:
        if st_app.button("✅ Conferma percorso", type="primary", key="nav_confirm_global", use_container_width=True):
            _handle_path_confirmation(
                st_app, dataset_repo,
                current_binded_path, current_uri, current_internal_path,
            )

    with col3:
        if st_app.button("🔄 Reset a base", key="nav_reset_global", use_container_width=True):
            st_app.session_state.selected_path_parts = []
            st_app.session_state.pop("path_confirmed", None)
            st_app.rerun()
def _handle_path_confirmation(st_app, dataset_repo: DatasetRepository, binded_path: str, uri: str, internal_path: str):
    """
    Valida il percorso selezionato in memoria confrontando il target_uri con tutti
    i dataset step=2 già presenti nel DB (URI esatti, nessuna trasformazione di path).

    Blocca l'avanzamento se:
      - URI identico già censito
      - URI è sotto-path di un dataset già censito (target sarebbe figlio)
      - URI è genitore di dataset già censiti (target sarebbe padre)
    """
    # Recupera tutti i dataset step=2 dal DB in un unico round-trip
    try:
        all_step2 = dataset_repo.get_by_step(step=2)
    except Exception as e:
        logger.error(f"Errore recupero dataset step=2: {e}")
        st_app.error("Errore durante la verifica nel DB. Riprova.")
        return

    conflicts = _validate_uri_conflicts(uri, all_step2)

    if conflicts["exact"]:
        st_app.error(
            f"❌ Esiste già un dataset censito con URI: `{conflicts['exact']}`\n\n"
            "Usa la modalità **'Seleziona dataset esistente'** per modificarlo."
        )
        return

    if conflicts["parents"]:
        st_app.error(
            f"❌ Questo percorso è un sotto-path di dataset già censiti:\n"
            + "\n".join(f"  • `{u}`" for u in conflicts["parents"])
            + "\n\nNon puoi censire un figlio di un dataset già registrato."
        )
        return

    if conflicts["children"]:
        st_app.error(
            f"❌ Esistono dataset già censiti che sono sotto-path di questo percorso:\n"
            + "\n".join(f"  • `{u}`" for u in conflicts["children"])
            + "\n\nNon puoi censire un genitore di dataset già registrati."
        )
        return

    # ── Tutto ok: prepara session_state e vai al form ──
    parent = st_app.session_state.get("wizard_parent_dataset")

    st_app.session_state.selected_dataset_uri = uri
    st_app.session_state.selected_dataset_path = internal_path
    st_app.session_state.selected_dataset_id = None

    st_app.session_state.prefill_from_parent = True
    st_app.session_state.prefill_parent_uri = parent.uri if parent else None
    if parent:
        base_name = parent.name or ""
        st_app.session_state.prefill_parent_data = {
            "name": f"processed__{base_name}" if base_name else "",
            "version": parent.version or "1.0",
            "license": parent.license,
            "source": getattr(parent, "source", None),
            "description": getattr(parent, "description", None),
            "languages": list(parent.languages) if getattr(parent, "languages", None) else [],
            "dataset_type": getattr(parent, "dataset_type", None),
            "derived_card": getattr(parent, "derived_card", None),
            "derived_dataset": parent.uri,
        }
    else:
        st_app.session_state.prefill_parent_data = {}

    logger.info(f"✅ Path confermato e validato: binded={binded_path} | uri={uri} | internal={internal_path}")
    st_app.session_state.current_stage = "processed_dataset_metadata_editing"
    st_app.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Helper bottone "torna al wizard"
# ─────────────────────────────────────────────────────────────────────────────

def _back_to_wizard_choice(st_app, label="← Torna alla scelta iniziale"):
    st_app.markdown("---")
    col_back, col_home = st_app.columns([2, 1])
    with col_back:
        if st_app.button(label, key=f"back_wizard_{label[:10]}", use_container_width=True):
            st_app.session_state.wizard_mode = None
            st_app.session_state.wizard_step = 0
            for key in ["available_datasets", "available_dataset_labels",
                        "step_datasets", "step_dataset_labels",
                        "wizard_parent_dataset", "selected_path_parts", "path_confirmed"]:
                st_app.session_state.pop(key, None)
            st_app.rerun()
    with col_home:
        if st_app.button("🏠 Torna alla Home", key=f"home_button_{label[:10]}", use_container_width=True):
            reset_session_state(st_app, KEYS_TO_DELETE)
            st_app.session_state.current_stage = "home"
            st_app.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point principale
# ─────────────────────────────────────────────────────────────────────────────

def show_dataset_selection(st_app, DATA_DIR):
    """
    Entry point del wizard per il censimento di dataset processati (Step 2).

    Flusso:
      wizard_mode=None           → Step 0: scelta iniziale
      wizard_mode="existing"     → Strada A: selezione dataset già censito
      wizard_mode="new_untracked"
        wizard_step=1            → Strada B Step 1: selezione dataset padre (step=1)
        wizard_step=2            → Strada B Step 2: navigazione filesystem e selezione URI
        (wizard_step=3 implicito → redirect al form gestito da _handle_path_confirmation)
    """
    dataset_repo = DatasetRepository(st_app.session_state.db_manager)

    st_app.session_state.setdefault("wizard_mode", None)
    st_app.session_state.setdefault("wizard_step", 0)

    mode = st_app.session_state.wizard_mode

    if mode is None:
        _show_wizard_choice(st_app)

    elif mode == "existing":
        _show_existing_selection(st_app, DATA_DIR, dataset_repo)

    elif mode == "new_untracked":
        step = st_app.session_state.wizard_step
        if step == 1:
            _show_parent_selection(st_app, dataset_repo)
        elif step == 2:
            _show_filesystem_navigation(st_app, dataset_repo)
        else:
            # Fallback: ricomincia dalla selezione padre
            st_app.session_state.wizard_step = 1
            st_app.rerun()
    else:
        st_app.session_state.wizard_mode = None
        st_app.rerun()