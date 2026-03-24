# ui/dataset_selection/mapped/dataset_mapped_selection_handler.py
import os
import logging
import traceback

from typing import List
from utils.streamlit_func import reset_session_state
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.entity.table.dataset import Dataset
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from data_class.repository.vocabulary.vocab_license_repository import VocabLicenseRepository
from data_class.repository.vocabulary.vocab_dataset_type_repository import VocabDatasetTypeRepository

BASE_PREFIX = os.getenv("BASE_PREFIX")

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
]

def _normalize_uri_to_path(uri: str) -> str:
    if not uri:
        return ""
    return os.path.normpath(uri.replace(BASE_PREFIX, ""))

def _build_dataset_labels(datasets: List[Dataset], DATA_DIR: str) -> List[str]:
    return [
        f"{dataset.name} - {dataset.version} - {dataset.uri.replace(BASE_PREFIX + DATA_DIR, '')}"
        for dataset in datasets
    ]

def _build_step_labels(datasets: List[Dataset]) -> List[str]:
    return [
        f"{dataset.name} - {dataset.version} [{dataset.uri}]"
        for dataset in datasets
    ]

def check_parent_dataset_exists(dataset_repo: DatasetRepository, selected_path: str, base_path: str) -> str:
    try:
        selected_path_normalized = os.path.normpath(selected_path)
        base_path_normalized = os.path.normpath(base_path)
        current_path = selected_path_normalized

        while current_path != base_path_normalized and current_path != os.path.dirname(current_path):
            parent_path = os.path.dirname(current_path)
            if parent_path == base_path_normalized or len(parent_path) <= len(base_path_normalized):
                break
            parent_uri = f"{BASE_PREFIX}{parent_path}"
            existing_dataset = dataset_repo.get_by_uri(parent_uri)
            if existing_dataset:
                return parent_uri
            current_path = parent_path
        return ""
    except Exception as e:
        logger.error(f"❌ Errore durante la verifica dei dataset padre: {e}")
        logger.debug(traceback.format_exc())
        return ""

def _load_available_datasets(st_app, DATA_DIR, dataset_repo: DatasetRepository):
    try:
        if DATA_DIR:
            uri_prefix = f"{BASE_PREFIX}{DATA_DIR}"
            datasets = dataset_repo.get_by_uri_prefix_or_step(uri_prefix, step=3)
            logger.info(f"Caricati {len(datasets)} dataset (Path: {uri_prefix} | Step: 3)")
            st_app.session_state.available_datasets = datasets
            st_app.session_state.available_dataset_labels = _build_dataset_labels(datasets, DATA_DIR)
            if not datasets:
                st_app.warning(f"❌ Nessun dataset trovato nel path {uri_prefix} o con Step 3.")
                return False
        else:
            st_app.error("DATA_DIR non configurato. Impossibile filtrare i dataset.")
            return False
        return True
    except Exception as e:
        st_app.error("Errore critico durante il caricamento dei dataset dal database.")
        logger.exception(f"Errore in _load_available_datasets: {e}")
        return False

def _load_step_datasets(st_app, dataset_repo: DatasetRepository, step: int):
    """Carica tutti i dataset con step=2 come candidati padre per la strada B del wizard."""
    try:
        datasets = dataset_repo.get_by_step(step=step)
        logger.info(f"Caricati {len(datasets)} dataset con Step={step}")
        st_app.session_state.step_datasets = datasets
        st_app.session_state.step_dataset_labels = _build_step_labels(datasets)
        if not datasets:
            st_app.warning(f"❌ Nessun dataset con Step={step} trovato. Impossibile procedere.")
            return False
        return True
    except Exception as e:
        st_app.error(f"Errore nel caricamento dei dataset Step={step}.")
        logger.exception(f"Errore in _load_step_datasets: {e}")
        return False

def _get_vocab_options(st_app):
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
    st_app.header("📂 Censimento Dataset Mappato (Step 3)")
    st_app.markdown("Scegli come vuoi procedere:")
    st_app.markdown("---")

    col_a, col_b = st_app.columns(2)
    with col_a:
        st_app.markdown("### 📋 Dataset già censito")
        st_app.markdown(
            "Seleziona un dataset mappato già presente nel catalogo "
            "e accedi al form di metadatazione per aggiornarne le informazioni."
        )
        if st_app.button("✅ Seleziona dataset esistente", key="wizard_choice_existing", use_container_width=True, type="primary"):
            st_app.session_state.wizard_mode = "existing"
            st_app.session_state.wizard_step = 0
            st_app.rerun()

    with col_b:
        st_app.markdown("### 🆕 Dataset mappato non ancora acquisito")
        st_app.markdown(
            "Censisci un dataset che hai mappato manualmente a partire da un dataset "
            "dallo Step=2 (Processed), ma che non è ancora stato registrato nel catalogo."
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
    st_app.header("📂 Seleziona Dataset Mappato o Step 3")

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
        "Scegli dataset (In-Path o validati Step 3)",
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

        if selected_dataset.step == 3:
            st_app.success(f"✅ Dataset FINALIZZATO (Step 3): {selected_dataset.name}")
        else:
            st_app.info(f"📂 Dataset in lavorazione (Step {selected_dataset.step}): {selected_dataset.name}")

        st_app.subheader("📋 Informazioni Dataset")
        col1, col2 = st_app.columns(2)
        with col1:
            st_app.write(f"**Nome:** {selected_dataset.name}")
            st_app.write(f"**Versione:** {selected_dataset.version}")
            st_app.write(f"**URI:** {selected_dataset.uri}")
            st_app.write(f"**Type:** {selected_dataset.dataset_type}")
        with col2:
            st_app.write(f"**Step:** {selected_dataset.step}")
            st_app.write(f"**Licenza:** {selected_dataset.license or 'Non specificata'}")
            st_app.write(f"**Descrizione:** {selected_dataset.description or 'Non specificata'}")

        if st_app.button("📝 Procedi alla Metadatazione", key="proceed_to_metadata_existing", type="primary"):
            st_app.session_state.prefill_from_parent = False
            st_app.session_state.current_stage = "mapped_dataset_metadata_editing"
            st_app.rerun()

    _back_to_wizard_choice(st_app)


# ─────────────────────────────────────────────────────────────────────────────
# WIZARD — Strada B, Step 1: selezione dataset padre (step=2 O STEP=3)
# ─────────────────────────────────────────────────────────────────────────────

def _show_parent_selection(st_app, dataset_repo):
    st_app.header("🆕 Censimento Dataset Derivato — Selezione Origine")
    
    # Toggle per decidere la sorgente della derivazione (Step 2 o Step 3)
    is_same_step = st_app.toggle(
        "Il dataset deriva da un'elaborazione di un dataset allo stesso livello (Step 3)?", 
        value=False,
        help="Attiva se il dataset deriva da uno già mappato (Step 3). Disattiva se deriva dal Processed (Step 2)."
    )

    target_step = 3 if is_same_step else 2
    
    st_app.info(
        f"Seleziona il dataset di **Step={target_step}** da cui deriva il tuo dataset. "
        "I metadati verranno ereditati e pre-compilati nel form successivo."
    )

    # Chiavi dinamiche per isolare i dati nel session_state in base al toggle
    datasets_key = f"step{target_step}_datasets"
    labels_key = f"step{target_step}_dataset_labels"

    # Inizializzazione chiavi
    if datasets_key not in st_app.session_state:
        st_app.session_state[datasets_key] = []
    if labels_key not in st_app.session_state:
        st_app.session_state[labels_key] = []

    # Caricamento dinamico tramite la funzione generalizzata
    if not st_app.session_state[datasets_key]:
        if _load_step_datasets(st_app, dataset_repo, step=target_step):
            # Trasferiamo i dati dalla chiave fissa usata dalla funzione a quella dinamica del toggle
            st_app.session_state[datasets_key] = st_app.session_state.get("step_datasets", [])
            st_app.session_state[labels_key] = st_app.session_state.get("step_dataset_labels", [])
        else:
            _back_to_wizard_choice(st_app)
            return

    datasets = st_app.session_state[datasets_key] or []
    filtered_datasets = _apply_filters(st_app, datasets, suffix=f"_step{target_step}")
    
    # Generazione etichette per la selectbox
    current_labels = _build_step_labels(filtered_datasets)

    dataset_options = [""] + current_labels
    selected_label = st_app.selectbox(
        f"Seleziona il dataset Step={target_step} padre",
        dataset_options,
        index=0,
        key=f"dataset_select_step_{target_step}" # Chiave widget univoca per evitare conflitti al cambio toggle
    )

    label_to_record = {lab: rec for lab, rec in zip(current_labels, filtered_datasets)}

    if selected_label and selected_label in label_to_record:
        parent_dataset = label_to_record[selected_label]

        st_app.subheader("📋 Dataset Padre Selezionato")
        col1, col2 = st_app.columns(2)
        with col1:
            st_app.write(f"**Nome:** {parent_dataset.name}")
            st_app.write(f"**Versione:** {parent_dataset.version}")
            st_app.write(f"**URI:** {parent_dataset.uri}")
            st_app.write(f"**Type:** {getattr(parent_dataset, 'dataset_type', 'N/D')}")
        with col2:
            st_app.write(f"**Step:** {parent_dataset.step}")
            st_app.write(f"**Licenza:** {parent_dataset.license or 'Non specificata'}")
            st_app.write(f"**Lingue:** {', '.join(parent_dataset.languages) if getattr(parent_dataset, 'languages', None) else 'N/D'}")
            st_app.write(f"**Descrizione:** {parent_dataset.description or 'Non specificata'}")

        st_app.markdown("---")
        st_app.markdown(
            f"**Confermando**, il nuovo dataset erediterà i metadati di `{parent_dataset.name}`. "
            "Potrai modificare tutti i campi nel form successivo."
        )

        if st_app.button("✅ Conferma dipendenza e procedi al form", key="confirm_parent", type="primary", use_container_width=True):
            st_app.session_state.wizard_parent_dataset = parent_dataset
            # Reset navigazione e passaggio allo step finale del wizard
            st_app.session_state.pop("selected_path_parts", None)
            st_app.session_state.pop("path_confirmed", None)
            st_app.session_state.wizard_step = 2
            st_app.rerun()

    _back_to_wizard_choice(st_app)

# ─────────────────────────────────────────────────────────────────────────────
# WIZARD — Strada B, Step 2: riepilogo + redirect al form con pre-fill
# ─────────────────────────────────────────────────────────────────────────────

def _prepare_prefill_and_redirect(st_app, DATA_DIR):
    parent = st_app.session_state.get("wizard_parent_dataset")
    if not parent:
        st_app.error("❌ Nessun dataset padre in sessione. Ricomincia dal wizard.")
        _back_to_wizard_choice(st_app)
        return

    st_app.header("🆕 Censimento Dataset Derivato — Riepilogo")
    st_app.success(f"✅ Dipendenza confermata: il nuovo dataset deriva da **{parent.name}**")
    st_app.info(
        "Il form verrà pre-compilato con i metadati ereditati. "
        "**URI e Step** vengono gestiti automaticamente (Step=3, URI dal path fisico). "
        "Tutti gli altri campi sono modificabili."
    )

    with st_app.expander("🔍 Anteprima metadati ereditati", expanded=True):
        col1, col2 = st_app.columns(2)
        with col1:
            st_app.write(f"**Nome (base):** {parent.name}")
            st_app.write(f"**Versione (base):** {parent.version}")
            st_app.write(f"**Licenza:** {parent.license or 'Non specificata'}")
            st_app.write(f"**Type:** {getattr(parent, 'dataset_type', 'N/D')}")
        with col2:
            st_app.write(f"**Lingue:** {', '.join(parent.languages) if getattr(parent, 'languages', None) else 'N/D'}")
            st_app.write(f"**Source:** {parent.source or 'N/D'}")
            st_app.write(f"**derived_dataset (auto):** {parent.uri}")
            st_app.write(f"**Step (auto):** 3")

    st_app.markdown("---")
    col_go, col_back = st_app.columns([2, 1])
    with col_go:
        if st_app.button("📝 Apri Form di Metadatazione", key="open_form_prefill", type="primary", use_container_width=True):
            st_app.session_state.prefill_from_parent = True
            st_app.session_state.prefill_parent_uri = parent.uri
            st_app.session_state.prefill_parent_data = {
                "name": parent.name,
                "version": parent.version,
                "license": parent.license,
                "source": getattr(parent, "source", None),
                "description": getattr(parent, "description", None),
                "languages": list(parent.languages) if getattr(parent, "languages", None) else [],
                "dataset_type": getattr(parent, "dataset_type", None),
                "derived_card": getattr(parent, "derived_card", None),
                "derived_dataset": parent.uri,
            }
            st_app.session_state.selected_dataset_uri = None
            st_app.session_state.selected_dataset_path = None
            st_app.session_state.selected_dataset_id = None
            st_app.session_state.current_stage = "mapped_dataset_metadata_editing"
            st_app.rerun()
    with col_back:
        if st_app.button("← Cambia dataset padre", key="back_to_parent_sel", use_container_width=True):
            st_app.session_state.wizard_step = 1
            st_app.session_state.wizard_parent_dataset = None
            st_app.rerun()

    _back_to_wizard_choice(st_app, label="🔄 Ricomincia da capo")

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
            st_app.session_state.pop("available_datasets", None)
            st_app.session_state.pop("available_dataset_labels", None)
            st_app.session_state.pop("step_datasets", None)
            st_app.session_state.pop("step_dataset_labels", None)
            st_app.session_state.pop("wizard_parent_dataset", None)
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
    Entry point del wizard per il censimento di dataset mappati (Step 3).

    Flusso:
      - wizard_mode=None            → scelta iniziale
      - wizard_mode="existing"      → Strada A: selezione dataset già censito
      - wizard_mode="new_untracked", wizard_step=1 → Strada B step 1: selezione padre step=2
      - wizard_mode="new_untracked", wizard_step=2 → Strada B step 2: riepilogo + redirect al form
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
            _prepare_prefill_and_redirect(st_app, DATA_DIR)
        else:
            st_app.session_state.wizard_step = 1
            st_app.rerun()
    else:
        st_app.session_state.wizard_mode = None
        st_app.rerun()

