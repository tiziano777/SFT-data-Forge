# ui/raw/management/distribution/distribution_raw_selection_handler.py
import os
import logging
from typing import List
from datetime import datetime, timezone

from utils.fs_func import list_dirs, list_files
from utils.path_utils import to_binded_path, to_internal_path
from utils.sample_reader import load_dataset_samples
from utils.extract_glob import generate_dataset_globs
from utils.streamlit_func import reset_dashboard_session_state
from config.state_vars import distribution_keys
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.entity.table.dataset import Dataset
from data_class.entity.table.distribution import Distribution
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from data_class.repository.vocabulary.vocab_license_repository import VocabLicenseRepository
from data_class.repository.vocabulary.vocab_distribution_split_repository import VocabDistributionSplitRepository

BASE_PREFIX=os.getenv("BASE_PREFIX")
RAW_DATA_DIR=os.getenv("RAW_DATA_DIR")

# Configurazione logger
logger = logging.getLogger(__name__)

# Chiavi di stato da eliminare
KEYS_TO_DELETE = [
    "available_datasets", "selected_dataset_uri", "available_dataset_labels", 
    "selected_dataset_id", "available_dataset_labels"
]

# --- FUNZIONI DI UTILITÀ ORIGINALI ---

def _normalize_uri_to_path(uri: str) -> str:
    if not uri: return ""
    return os.path.normpath(uri.replace(BASE_PREFIX, ""))

def _build_dataset_labels(datasets: List[Dataset]) -> List[str]:
    return [f"{dataset.name} - {dataset.version} - {dataset.uri.replace(BASE_PREFIX+RAW_DATA_DIR, '')}" for dataset in datasets]

def _load_available_datasets(st_app, dataset_repo: DatasetRepository, uri_prefix: str = None):
    """Carica i dataset disponibili filtrando per prefisso URI OR Step 1 (Raw)."""
    try:
        # Utilizzo della logica parametrica: match path OR step 1
        if uri_prefix:
            datasets = dataset_repo.get_by_uri_prefix_or_step(uri_prefix, step=1)
        else:
            datasets = dataset_repo.get_all()
            
        st_app.session_state.available_datasets = datasets
        st_app.session_state.available_dataset_labels = _build_dataset_labels(datasets)
        return True
    except Exception as e:
        st_app.error("Errore caricando i dataset raw dal DB.")
        logger.exception(f"Errore nel caricamento dei dataset: {e}")
        return False

def _build_unmaterialized_distribution_labels(distributions: List[Distribution]) -> List[str]:
    labels = []
    for dist in distributions:
        uri = dist.uri
        subpath = ""
        if uri.startswith(BASE_PREFIX):
            clean_path = uri.replace(BASE_PREFIX, "")
            if clean_path.startswith(RAW_DATA_DIR):
                subpath = clean_path[len(RAW_DATA_DIR):].lstrip('/')
            else:
                subpath = clean_path
        label = f"{dist.name} - {dist.version or '1.0'} - {subpath or 'no subpath'}"
        labels.append(label)
    return labels

def _initialize_repositories(st_app):
    return {
        'dataset': DatasetRepository(st_app.session_state.db_manager),
        'distribution': DistributionRepository(st_app.session_state.db_manager)
    }

def _find_raw_dataset_version(original_dataset: Dataset) -> str:
    original_path = _normalize_uri_to_path(original_dataset.uri)
    dataset_name = os.path.basename(original_path)
    raw_base = RAW_DATA_DIR
    try:
        direct_path = os.path.join(raw_base, dataset_name)
        if os.path.exists(direct_path): return direct_path
        for item in os.listdir(raw_base):
            item_path = os.path.join(raw_base, item)
            if os.path.isdir(item_path):
                nested_path = os.path.join(item_path, dataset_name)
                if os.path.exists(nested_path): return nested_path
        return raw_base
    except Exception: return raw_base

# --- LOGICA DI OTTIMIZZAZIONE FLAT ---

def find_leaf_distributions(base_path: str) -> List[str]:
    """
    Scansione ricorsiva per trovare cartelle con file,
    escludendo path tecnici tramite semplice substring match.
    """
    import re

    leaf_paths = []
    if not os.path.exists(base_path):
        return []

    # 🚫 blacklist semplice: match su QUALSIASI substring
    blacklist_pattern = re.compile(r"\.cache|logs|__pycache__", re.IGNORECASE)

    for root, dirs, files in os.walk(base_path):
        # filtro brutale: se il path contiene roba tecnica → skip
        if blacklist_pattern.search(root):
            continue

        if files:
            rel_path = os.path.relpath(root, base_path)
            if rel_path == ".":
                continue
            leaf_paths.append(rel_path.replace("\\", "/"))

    return sorted(leaf_paths)

def show_distribution_selection(st_app):
    """Interfaccia con validazione Step 1 sulle Distribution Raw."""
    st_app.header("📂 Seleziona una Distribution (Raw Data)")

    st_app.session_state.setdefault("available_datasets", [])
    st_app.session_state.setdefault("selected_path_parts", [])
    st_app.session_state.setdefault("selected_dataset_path", RAW_DATA_DIR)

    repos = _initialize_repositories(st_app)

    # 1️⃣ Caricamento dataset (URI match o Step 1)
    if not st_app.session_state.available_datasets:
        uri_filter = f"{BASE_PREFIX}{RAW_DATA_DIR}" if RAW_DATA_DIR else None
        if not _load_available_datasets(st_app, repos['dataset'], uri_prefix=uri_filter): 
            return

    dataset_options = [""] + st_app.session_state.available_dataset_labels
    selected_label = st_app.selectbox("Scegli dataset (Raw o Step 1)", dataset_options, key="dataset_select")

    label_to_record = {lab: rec for lab, rec in zip(st_app.session_state.available_dataset_labels, st_app.session_state.available_datasets)}

    if selected_label and selected_label in label_to_record:
        selected_dataset = label_to_record[selected_label]
        if st_app.session_state.get("selected_dataset_id") != selected_dataset.id:
            st_app.session_state.selected_dataset_id = selected_dataset.id
            st_app.session_state.selected_dataset_uri = selected_dataset.uri
            st_app.session_state.selected_dataset_path = _find_raw_dataset_version(selected_dataset)
            st_app.session_state.selected_path_parts = []
            st_app.rerun()

        # --- SEZIONE FLAT NAVIGATION (FISICA) ---
        st_app.subheader("📁 Seleziona la Distribution fisica")
        
        # Modifica: utilizza l'URI del dataset selezionato come base_path e converte nel path interno di Docker
        base_path = to_internal_path(_normalize_uri_to_path(st_app.session_state.selected_dataset_uri))
        
        # --- SEARCH E FILTERS PER DISTRIBUTION ---
        try:
            vocab_lang_repo = VocabLanguageRepository(st_app.session_state.db_manager)
            vocab_license_repo = VocabLicenseRepository(st_app.session_state.db_manager)
            vocab_split_repo = VocabDistributionSplitRepository(st_app.session_state.db_manager)
            lang_options = [l.code for l in (vocab_lang_repo.get_all() if hasattr(vocab_lang_repo, 'get_all') else [])]
            license_options = [l.code for l in (vocab_license_repo.get_all() if hasattr(vocab_license_repo, 'get_all') else [])]
            split_options = [s.code for s in (vocab_split_repo.get_all() if hasattr(vocab_split_repo, 'get_all') else [])]
        except Exception as e:
            logger.error(f"Errore caricamento vocabulary options per distribution: {e}")
            lang_options = []
            license_options = []
            split_options = []

        col_f1, col_f2, col_f3, col_f4 = st_app.columns([4,2,2,2])
        with col_f1:
            dist_search = st_app.text_input("🔍 Cerca distribution (name, description, uri)", key="dist_search_raw").strip().lower()
        with col_f2:
            dist_langs = st_app.multiselect("Lingue", options=lang_options, key="dist_lang_filter_raw")
        with col_f3:
            dist_licenses = st_app.multiselect("Licenze", options=license_options, key="dist_license_filter_raw")
        with col_f4:
            dist_splits = st_app.multiselect("Splits", options=split_options, key="dist_split_filter_raw")

        with st_app.spinner("Scansione cartelle raw..."):
            all_leaves = find_leaf_distributions(base_path)

            # Applichiamo i filtri alle leaves: se sono selezionati filtri, richiediamo il record DB per verificare attributi
            filtered_leaves = []
            for leaf in all_leaves:
                if not leaf or leaf == ".":
                    continue
                current_path = os.path.normpath(os.path.join(base_path, leaf))
                distribution_uri = f"{BASE_PREFIX}{current_path}"

                # default include
                include_leaf = True

                # search filter
                if dist_search:
                    inc = False
                    if dist_search in leaf.lower():
                        inc = True
                    else:
                        try:
                            drec = repos['distribution'].get_by_uri(distribution_uri)
                            if drec:
                                dn = (getattr(drec, 'name', '') or '').lower()
                                dd = (getattr(drec, 'description', '') or '').lower()
                                du = (getattr(drec, 'uri', '') or '').lower()
                                if dist_search in dn or dist_search in dd or dist_search in du:
                                    inc = True
                        except Exception:
                            inc = False
                    if not inc:
                        include_leaf = False

                # other filters require DB record
                if include_leaf and (dist_langs or dist_licenses or dist_splits):
                    try:
                        drec = repos['distribution'].get_by_uri(distribution_uri)
                        if not drec:
                            include_leaf = False
                        else:
                            # lang
                            if dist_langs:
                                rec_lang = getattr(drec, 'lang', None) or getattr(drec, 'language', None) or ''
                                rec_langs = [s.strip().lower() for s in (rec_lang if isinstance(rec_lang, list) else [rec_lang]) if s]
                                if not any(l.lower() in rec_langs for l in dist_langs):
                                    include_leaf = False
                            # license
                            if include_leaf and dist_licenses:
                                rec_license = getattr(drec, 'license', None) or getattr(drec, 'license_code', None) or ''
                                rec_licenses = [s.strip().lower() for s in (rec_license if isinstance(rec_license, list) else [rec_license]) if s]
                                if not any(lc.lower() in rec_licenses for lc in dist_licenses):
                                    include_leaf = False
                            # split
                            if include_leaf and dist_splits:
                                rec_split = getattr(drec, 'split', None) or ''
                                rec_splits = [s.strip().lower() for s in (rec_split if isinstance(rec_split, list) else [rec_split]) if s]
                                if not any(sp.lower() in rec_splits for sp in dist_splits):
                                    include_leaf = False
                    except Exception:
                        include_leaf = False

                if include_leaf:
                    filtered_leaves.append(leaf)

            # fine filtro leaves

            # fallback message handled later

        if not filtered_leaves:
            distribution_uri = st_app.session_state.selected_dataset_uri
            distribution_internal = to_internal_path(distribution_uri.replace(BASE_PREFIX, ""))
            
            # Verifica se ci sono file direttamente nel dataset (nessuna sottocartella)
            if len(list_dirs(distribution_internal)) == 0 and len(list_files(distribution_internal)) > 0:
                st_app.info(f"📁 Il dataset stesso contiene file e sarà trattato come distribuzione")
                st_app.info(f"Percorso: {distribution_internal}")
                
                # Mostra anteprima dei file trovati
                files = list_files(distribution_internal)[:5]  # Primi 5 file
                if files:
                    st_app.write("File trovati:")
                    for f in files:
                        st_app.write(f"- {os.path.basename(f)}")
                
                if st_app.button("✅ Conferma questo dataset come distribuzione", key="confirm_dataset_as_distribution"):
                    _handle_dataset_as_distribution(st_app, distribution_internal, repos['distribution'])
            else:
                st_app.info("Nessuna distribuzione trovata in questo dataset")
        else:
            current_leaf_str = "/".join(st_app.session_state.selected_path_parts)
            def_idx = filtered_leaves.index(current_leaf_str) if current_leaf_str in filtered_leaves else 0

            selected_leaf = st_app.selectbox(
                "Distribuzioni rilevate su disco:",
                options=filtered_leaves,
                index=def_idx,
                format_func=lambda x: f"📦 {x}",
                key="dist_leaf_select_processed"
            )

            st_app.session_state.selected_path_parts = [] if selected_leaf == "" else selected_leaf.replace("\\", "/").split("/")
            current_path = os.path.normpath(os.path.join(base_path, *st_app.session_state.selected_path_parts))
            st_app.info(f"Current path: {current_path}")
            if st_app.button("✅ Conferma questa distribuzione", key="confirm_distribution"):
                # Controllo coerenza Distribution per Step 1
                distribution_uri = f"{BASE_PREFIX}{current_path}"
                existing_dist = repos['distribution'].get_by_uri(distribution_uri)
                
                if existing_dist and existing_dist.step != 1:
                    st_app.error(f"🚨 ALERT AMMINISTRATORE: La distribuzione ha step={existing_dist.step}. Deve essere 1 per il layer Raw!")
                else:
                    _confirm_distribution_selection(st_app, current_path, repos['distribution'])

        # --- SEZIONE UNMATERIALIZED (Solo Step 1) ---
        st_app.divider()
        st_app.subheader("📋 Distribuzioni Logiche (Step 1)")
        
        dist_uri_filter = f"{BASE_PREFIX}{st_app.session_state.selected_dataset_path}"
        # Recupero distribuzioni tramite Path o Step 1
        available_dists = repos['distribution'].get_by_uri_prefix_or_step(dist_uri_filter, step=1)
        
        # Applichiamo filtri anche alle distribuzioni logiche
        unmat_all = [d for d in available_dists if d.dataset_id == st_app.session_state.selected_dataset_id and not d.materialized]
        unmat = []
        for d in unmat_all:
            include = True
            # search
            if dist_search:
                dn = (getattr(d, 'name', '') or '').lower()
                dd = (getattr(d, 'description', '') or '').lower()
                du = (getattr(d, 'uri', '') or '').lower()
                if dist_search not in dn and dist_search not in dd and dist_search not in du:
                    include = False
            # langs
            if include and dist_langs:
                rec_lang = getattr(d, 'lang', None) or getattr(d, 'language', None) or ''
                rec_langs = [s.strip().lower() for s in (rec_lang if isinstance(rec_lang, list) else [rec_lang]) if s]
                if not any(l.lower() in rec_langs for l in dist_langs):
                    include = False
            # licenses
            if include and dist_licenses:
                rec_license = getattr(d, 'license', None) or getattr(d, 'license_code', None) or ''
                rec_licenses = [s.strip().lower() for s in (rec_license if isinstance(rec_license, list) else [rec_license]) if s]
                if not any(lc.lower() in rec_licenses for lc in dist_licenses):
                    include = False
            # splits
            if include and dist_splits:
                rec_split = getattr(d, 'split', None) or ''
                rec_splits = [s.strip().lower() for s in (rec_split if isinstance(rec_split, list) else [rec_split]) if s]
                if not any(sp.lower() in rec_splits for sp in dist_splits):
                    include = False
            if include:
                unmat.append(d)

        if unmat:
            unmat_labels = _build_unmaterialized_distribution_labels(unmat)
            sel_unmat_label = st_app.selectbox("Scegli distribuzione:", [""] + unmat_labels, key="unmaterialized_dist_select")
            unmat_map = {lab: dist for lab, dist in zip(unmat_labels, unmat)}

            if sel_unmat_label and sel_unmat_label in unmat_map:
                sel_dist = unmat_map[sel_unmat_label]

                if sel_dist.step != 1:
                    st_app.error(f"🚨 ALERT AMMINISTRATORE: Distribution con step errato ({sel_dist.step}). Richiesto Step=1.")
                elif st_app.button("✅ Usa questa distribuzione", key="use_unmaterialized_btn"):
                    st_app.session_state.current_distribution = sel_dist
                    reset_dashboard_session_state(st_app, distribution_keys)
                    path = sel_dist.uri.replace(BASE_PREFIX, "")
                    st_app.session_state.current_distribution_path = path
                    if os.path.exists(path):
                        st_app.session_state.samples = load_dataset_samples(path)
                    st_app.session_state.current_stage = "raw_distribution_main"
                    st_app.rerun()
        else:
            st_app.info("Nessuna distribuzione non materializzata con Step 1 trovata.")

# --- LOGICA DI CONFERMA E CREAZIONE ---

def _create_new_distribution(st_app, current_path: str, distribution_uri: str, distribution_repo: DistributionRepository):
    try:
        # Convert the distribution URI to its binded path
        binded_distribution_uri = to_binded_path(distribution_uri)

        # Check if the distribution already exists
        existing_distribution = distribution_repo.get_by_uri(binded_distribution_uri)
        if existing_distribution:
            st_app.session_state.current_distribution = existing_distribution
            reset_dashboard_session_state(st_app, distribution_keys)
            st_app.session_state.current_distribution_path = current_path
            st_app.session_state.samples = load_dataset_samples(current_path)
            st_app.session_state.current_stage = "raw_distribution_main"
            return

        file_glob = generate_dataset_globs(current_path)[0]
        _, ext = os.path.splitext(file_glob)
        dataset_repo = DatasetRepository(st_app.session_state.db_manager)
        selected_dataset = dataset_repo.get_by_id(st_app.session_state.selected_dataset_id)

        new_distribution = Distribution(
            id=None, uri=binded_distribution_uri, tokenized_uri=None, dataset_id=st_app.session_state.selected_dataset_id,
            glob=file_glob, format=ext.lstrip('.') or 'unknown',
            name="/".join(st_app.session_state.selected_path_parts),
            query=None, derived_from=None, src_schema={}, description="",
            split="unknown", lang='un', tags=[], license=selected_dataset.license or 'unknown',
            version=selected_dataset.version or '1.0', issued=datetime.now(timezone.utc), modified=datetime.now(timezone.utc)
        )
        result = distribution_repo.insert(new_distribution)
        if result:
            st_app.session_state.current_distribution = result
            reset_dashboard_session_state(st_app, distribution_keys)
            st_app.session_state.current_distribution_path = current_path
            st_app.session_state.samples = load_dataset_samples(current_path)
            st_app.session_state.current_stage = "raw_distribution_main"
    except Exception as e:
        st_app.error("Errore creando la nuova distribution.")
        logger.exception(e)

def _confirm_distribution_selection(st_app, current_path: str, distribution_repo: DistributionRepository):
    """Verifica e conferma per il layer Raw (Step 1)."""
    base_path = st_app.session_state.selected_dataset_path
    selected_relpath = os.path.relpath(current_path, base_path)
    current_dist_path = os.path.join(base_path, selected_relpath)
    distribution_uri = f"{BASE_PREFIX}{current_dist_path}"
    try:
        existing = distribution_repo.get_by_uri(distribution_uri)
        if existing:
            if existing.step == 1:
                st_app.session_state.current_distribution = existing
                reset_dashboard_session_state(st_app, distribution_keys)
                st_app.session_state.current_distribution_path = current_dist_path
                st_app.session_state.samples = load_dataset_samples(current_dist_path)
                st_app.session_state.current_stage = "raw_distribution_main"
                st_app.rerun()
            else:
                st_app.error(f"Impossibile procedere: Lo step della distribuzione è {existing.step}. Deve essere 1.")
        else:
            _create_new_distribution(st_app, current_path, distribution_uri, distribution_repo)
            st_app.rerun()
    except Exception as e:
        st_app.error("Errore durante la conferma della selezione raw.")
        logger.exception(e)

def _handle_dataset_as_distribution(st_app, base_path: str, distribution_repo: DistributionRepository):
    """Gestisce il caso speciale in cui il dataset stesso è la distribution (nessuna sottocartella)."""
    
    # Il percorso corrente è il base_path stesso
    current_path = base_path
    distribution_uri = f"{BASE_PREFIX}{current_path}"
    
    try:
        # Verifica se esiste già una distribution per questo URI
        existing = distribution_repo.get_by_uri(distribution_uri)
        
        if existing:
            if existing.step == 1:
                st_app.session_state.current_distribution = existing
                reset_dashboard_session_state(st_app, distribution_keys)
                st_app.session_state.current_distribution_path = current_path
                st_app.session_state.samples = load_dataset_samples(current_path)
                st_app.session_state.current_stage = "raw_distribution_main"
                st_app.rerun()
            else:
                st_app.error(f"🚨 ALERT AMMINISTRATORE: La distribuzione ha step={existing.step}. Deve essere 1 per il layer Raw!")
        else:
            # Crea nuova distribution per il dataset stesso
            _create_distribution_from_dataset(st_app, current_path, distribution_uri, distribution_repo)
            st_app.rerun()
            
    except Exception as e:
        st_app.error("Errore durante la gestione del dataset come distribuzione.")
        logger.exception(e)

def _create_distribution_from_dataset(st_app, current_path: str, distribution_uri: str, distribution_repo: DistributionRepository):
    """Crea una nuova distribution partendo dal dataset (quando non ci sono sottocartelle)."""
    try:
        # Convert the distribution URI to its binded path
        binded_distribution_uri = to_binded_path(distribution_uri)
        
        # Genera glob patterns per i file nel dataset
        file_patterns = generate_dataset_globs(current_path)
        if not file_patterns:
            # Fallback: prendi il primo file come pattern
            files = list_files(current_path)
            if files:
                first_file = files[0]
                _, ext = os.path.splitext(first_file)
                file_glob = os.path.join(current_path, f"*{ext}")
                file_patterns = [file_glob]
            else:
                # Nessun file trovato
                st_app.warning("Nessun file trovato nel dataset")
                return
        
        file_glob = file_patterns[0]
        _, ext = os.path.splitext(file_glob)
        
        # Recupera il dataset selezionato
        dataset_repo = DatasetRepository(st_app.session_state.db_manager)
        selected_dataset = dataset_repo.get_by_id(st_app.session_state.selected_dataset_id)
        
        # Determina il titolo (usa il nome del dataset)
        dataset_name = os.path.basename(current_path)
        new_distribution= distribution_repo.get_by_uri(binded_distribution_uri)
        if new_distribution:
            logger.info(f"Distribution already exists for URI: {binded_distribution_uri}")
            st_app.session_state.current_distribution = new_distribution
            reset_dashboard_session_state(st_app, distribution_keys)
            st_app.session_state.current_distribution_path = current_path
            st_app.session_state.samples = load_dataset_samples(current_path)
            st_app.session_state.current_stage = "raw_distribution_main"
        else:
            new_distribution = Distribution(
                id=None, 
                uri=binded_distribution_uri, 
                dataset_id=st_app.session_state.selected_dataset_id,
                glob=file_glob, 
                format=ext.lstrip('.') or 'unknown',
                name=dataset_name,
                query=None, 
                derived_from=None, 
                src_schema={}, 
                description=f"Distribution for dataset {dataset_name}",
                split="unknown", 
                lang='un', 
                tags=[], 
                license=selected_dataset.license or 'unknown',
                version=selected_dataset.version or '1.0', 
                issued=datetime.now(timezone.utc), 
                modified=datetime.now(timezone.utc),
                step=1
            )
            
            result = distribution_repo.insert(new_distribution)
            if result:
                st_app.session_state.current_distribution = result
                reset_dashboard_session_state(st_app, distribution_keys)
                st_app.session_state.current_distribution_path = current_path
                st_app.session_state.samples = load_dataset_samples(current_path)
                st_app.session_state.current_stage = "raw_distribution_main"
                st_app.success(f"✅ Distribuzione creata con successo per il dataset {dataset_name}")
            else:
                st_app.error("Errore durante l'inserimento della distribuzione nel database")
                
    except Exception as e:
        st_app.error("Errore creando la nuova distribution dal dataset.")
        logger.exception(e)

