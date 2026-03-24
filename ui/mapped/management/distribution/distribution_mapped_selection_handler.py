# mapped/management/distribution/distribution_mapped_selection_handler.py
import os
import logging
import re
import token
from typing import List
from datetime import datetime, timezone

from utils.fs_func import list_files, list_dirs
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

BASE_PREFIX = os.getenv("BASE_PREFIX")
MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR")

# Configurazione logger
logger = logging.getLogger(__name__)

# Chiavi di stato da eliminare
KEYS_TO_DELETE = [
    "available_datasets", "selected_dataset_uri", "available_dataset_labels", 
    "selected_dataset_id", "selected_path_parts"
]

def _normalize_uri_to_path(uri: str) -> str:
    """Converte URI in path normalizzato."""
    if not uri:
        return ""
    return os.path.normpath(uri.replace(BASE_PREFIX, ""))

def _build_dataset_labels(datasets: List[Dataset]) -> List[str]:
    """Costruisce label user-friendly per i dataset."""
    return [
        f"{dataset.name} - {dataset.version} - {dataset.uri.replace(BASE_PREFIX + MAPPED_DATA_DIR, '')}" 
        for dataset in datasets
    ]

def _initialize_repositories(st_app):
    """Inizializza repository necessari."""
    return {
        'dataset': DatasetRepository(st_app.session_state.db_manager),
        'distribution': DistributionRepository(st_app.session_state.db_manager)
    }

def _build_unmaterialized_distribution_labels(distributions: List[Distribution]) -> List[str]:
    """Costruisce label per distribuzioni non materializzate."""
    labels = []
    for dist in distributions:
        uri = dist.uri
        subpath = ""
        if uri.startswith(BASE_PREFIX):
            clean_path = uri.replace(BASE_PREFIX, "")
            if clean_path.startswith(MAPPED_DATA_DIR):
                subpath = clean_path[len(MAPPED_DATA_DIR):].lstrip('/')
            else:
                subpath = clean_path
        label = f"{dist.name} - {dist.version or '1.0'} - {subpath or 'no subpath'}"
        labels.append(label)
    return labels

def _find_mapped_dataset_version(original_dataset: Dataset) -> str:
    """
    Trova il path fisico del dataset mapped.
    
    IMPORTANTE: Usa direttamente l'URI del dataset per determinare il path,
    invece di cercare in base al nome (che potrebbe non essere univoco).
    """
    try:
        # Converte l'URI binded in path container
        logger.info(f"Original dataset URI: {original_dataset.uri}")
        dataset_path = to_internal_path(original_dataset.uri).replace(BASE_PREFIX, "")
        
        # Verifica che il path esista
        if os.path.exists(dataset_path):
            logger.info(f"Path dataset trovato: {dataset_path}")
            return dataset_path
        return MAPPED_DATA_DIR
    except Exception as e:
        logger.error(f"Errore nel trovare il path del dataset mapped: {e}")
        
    # Fallback: usa MAPPED_DATA_DIR
    logger.warning(f"Path dataset non trovato, uso fallback")
    return MAPPED_DATA_DIR


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


def _load_available_datasets(st_app, dataset_repo: DatasetRepository, uri_prefix: str = None):
    """Carica i dataset disponibili filtrando per prefisso URI OR Step 3."""
    try:
        # Utilizzo della logica parametrica: match path OR step 3 (Mapped Layer)
        if uri_prefix:
            datasets = dataset_repo.get_by_uri_prefix_or_step(uri_prefix, step=3)
        else:
            datasets = dataset_repo.get_all()
            # Filtra solo Step 3
            datasets = [d for d in datasets if d.step == 3]
            
        st_app.session_state.available_datasets = datasets
        st_app.session_state.available_dataset_labels = _build_dataset_labels(datasets)
        logger.info(f"Caricati {len(datasets)} dataset mapped (Step 3)")
        return True
    except Exception as e:
        st_app.error("Errore caricando i dataset mapped dal DB.")
        logger.exception(f"Errore nel caricamento dei dataset: {e}")
        return False

def _load_vocab_options(st_app):
    """Carica opzioni dai vocabolari per i filtri."""
    try:
        vocab_lang_repo = VocabLanguageRepository(st_app.session_state.db_manager)
        vocab_license_repo = VocabLicenseRepository(st_app.session_state.db_manager)
        vocab_split_repo = VocabDistributionSplitRepository(st_app.session_state.db_manager)
        
        lang_options = [l.code for l in (vocab_lang_repo.get_all() if hasattr(vocab_lang_repo, 'get_all') else [])]
        license_options = [l.code for l in (vocab_license_repo.get_all() if hasattr(vocab_license_repo, 'get_all') else [])]
        split_options = [s.code for s in (vocab_split_repo.get_all() if hasattr(vocab_split_repo, 'get_all') else [])]
        
        return lang_options, license_options, split_options
    except Exception as e:
        logger.error(f"Errore caricamento vocabulary options: {e}")
        return [], [], []

def _apply_distribution_filters(
    distribution,
    dist_search: str,
    dist_langs: List[str],
    dist_licenses: List[str],
    dist_splits: List[str]
) -> bool:
    """
    Applica filtri a una distribuzione.
    
    Returns:
        True se la distribuzione passa tutti i filtri, False altrimenti.
    """
    # Filtro ricerca testuale
    if dist_search:
        search_lower = dist_search.lower()
        name = (getattr(distribution, 'name', '') or '').lower()
        description = (getattr(distribution, 'description', '') or '').lower()
        uri = (getattr(distribution, 'uri', '') or '').lower()
        
        if not (search_lower in name or search_lower in description or search_lower in uri):
            return False
    
    # Filtro lingue
    if dist_langs:
        rec_lang = getattr(distribution, 'lang', None) or getattr(distribution, 'language', None) or ''
        rec_langs = [s.strip().lower() for s in (rec_lang if isinstance(rec_lang, list) else [rec_lang]) if s]
        if not any(l.lower() in rec_langs for l in dist_langs):
            return False
    
    # Filtro licenze
    if dist_licenses:
        rec_license = getattr(distribution, 'license', None) or getattr(distribution, 'license_code', None) or ''
        rec_licenses = [s.strip().lower() for s in (rec_license if isinstance(rec_license, list) else [rec_license]) if s]
        if not any(lc.lower() in rec_licenses for lc in dist_licenses):
            return False
    
    # Filtro splits
    if dist_splits:
        rec_split = getattr(distribution, 'split', None) or ''
        rec_splits = [s.strip().lower() for s in (rec_split if isinstance(rec_split, list) else [rec_split]) if s]
        if not any(sp.lower() in rec_splits for sp in dist_splits):
            return False
    
    return True

def show_distribution_selection(st_app):
    """Interfaccia principale con validazione Step 3 sulle Distribution Mapped."""
    st_app.header("📂 Seleziona una Distribution (Mapped Data)")

    # Inizializzazione session state
    st_app.session_state.setdefault("available_datasets", [])
    st_app.session_state.setdefault("selected_path_parts", [])
    st_app.session_state.setdefault("selected_dataset_path", MAPPED_DATA_DIR)
    st_app.session_state.setdefault("selected_dataset_id", None)

    repos = _initialize_repositories(st_app)

    # 1️⃣ Caricamento dataset (URI match specifico per MAPPED)
    if not st_app.session_state.available_datasets:
        uri_filter = f"{BASE_PREFIX}{MAPPED_DATA_DIR}" if MAPPED_DATA_DIR else None
        if not _load_available_datasets(st_app, repos['dataset'], uri_prefix=uri_filter): 
            return

    dataset_options = [""] + st_app.session_state.available_dataset_labels
    selected_label = st_app.selectbox("Scegli dataset (Mapped - Step 3)", dataset_options, key="dataset_select")

    label_to_record = {lab: rec for lab, rec in zip(st_app.session_state.available_dataset_labels, st_app.session_state.available_datasets)}

    if selected_label and selected_label in label_to_record:
        selected_dataset = label_to_record[selected_label]
        
        if st_app.session_state.get("selected_dataset_id") != selected_dataset.id:
            st_app.session_state.selected_dataset_id = selected_dataset.id
            st_app.session_state.selected_dataset_uri = selected_dataset.uri
            st_app.session_state.selected_dataset_path = _find_mapped_dataset_version(selected_dataset)
            st_app.session_state.selected_path_parts = []
            logger.info(f"Selezionato dataset mapped: {selected_dataset.name} (ID: {selected_dataset.id})")
            st_app.rerun()

        # --- SEZIONE FILTRI ---
        try:
            vocab_lang_repo = VocabLanguageRepository(st_app.session_state.db_manager)
            vocab_license_repo = VocabLicenseRepository(st_app.session_state.db_manager)
            vocab_split_repo = VocabDistributionSplitRepository(st_app.session_state.db_manager)
            lang_options = [l.code for l in (vocab_lang_repo.get_all() if hasattr(vocab_lang_repo, 'get_all') else [])]
            license_options = [l.code for l in (vocab_license_repo.get_all() if hasattr(vocab_license_repo, 'get_all') else [])]
            split_options = [s.code for s in (vocab_split_repo.get_all() if hasattr(vocab_split_repo, 'get_all') else [])]
        except Exception as e:
            logger.error(f"Errore caricamento opzioni vocabolario: {e}")
            lang_options, license_options, split_options = [], [], []

        col_f1, col_f2, col_f3, col_f4 = st_app.columns([4,2,2,2])
        with col_f1:
            dist_search = st_app.text_input("🔍 Cerca distribution (name, description, uri)", key="dist_search_mapped").strip().lower()
        with col_f2:
            dist_langs = st_app.multiselect("Lingue", options=lang_options, key="dist_lang_filter_mapped")
        with col_f3:
            dist_licenses = st_app.multiselect("Licenze", options=license_options, key="dist_license_filter_mapped")
        with col_f4:
            dist_splits = st_app.multiselect("Splits", options=split_options, key="dist_split_filter_mapped")

        # --- 2️⃣ SEZIONE DISTRIBUZIONI FISICHE (MATERIALIZED) ---
        st_app.subheader("📁 Seleziona la Distribution fisica")
        base_path = to_internal_path(_normalize_uri_to_path(st_app.session_state.selected_dataset_uri))
        
        filtered_leaves = []
        with st_app.spinner("Scansione cartelle processate..."):
            all_leaves = find_leaf_distributions(base_path)
            
            for leaf in all_leaves:
                if not leaf or leaf == ".": continue
                    
                
                current_path = os.path.normpath(os.path.join(base_path, leaf))
                distribution_uri = f"{BASE_PREFIX}{current_path}"
                include_leaf = True

                try:
                    drec = repos['distribution'].get_by_uri(distribution_uri)
                    if drec and drec.dataset_id != selected_dataset.id:
                        include_leaf = False
                    
                    if include_leaf and dist_search:
                        dn = (getattr(drec, 'name', '') or '').lower() if drec else ""
                        if dist_search not in leaf.lower() and dist_search not in dn:
                            include_leaf = False
                    
                    if include_leaf and drec and (dist_langs or dist_licenses or dist_splits):
                        if dist_langs:
                            rec_lang = getattr(drec, 'lang', None) or getattr(drec, 'language', None) or ''
                            rec_langs = [s.strip().lower() for s in (rec_lang if isinstance(rec_lang, list) else [rec_lang]) if s]
                            if not any(l.lower() in rec_langs for l in dist_langs): 
                                include_leaf = False
                except Exception:
                    if dist_search and dist_search not in leaf.lower(): 
                        include_leaf = False

                if include_leaf and leaf != "":  # Escludi il caso root per la selezione fisica
                    filtered_leaves.append(leaf)

            logger.info(f"Trovate {len(filtered_leaves)} distribuzioni mapped dopo filtri")

        # --- GESTIONE FALLBACK: DATASET COME DISTRIBUZIONE ---
        if not filtered_leaves:
            distribution_uri = st_app.session_state.selected_dataset_uri
            distribution_internal = to_internal_path(distribution_uri.replace(BASE_PREFIX, ""))
            
            # Verifica se ci sono file direttamente nel dataset (nessuna sottocartella)
            if len(list_dirs(distribution_internal)) == 0 and len(list_files(distribution_internal)) > 0:
                st_app.info(f"📁 Il dataset stesso contiene file e sarà trattato come distribuzione")
                st_app.info(f"Percorso: {distribution_internal}")
                
                # Mostra anteprima dei file trovati
                files = list_files(distribution_internal)[:5]
                if files:
                    st_app.write("File trovati:")
                    for f in files:
                        st_app.write(f"- {os.path.basename(f)}")
                
                if st_app.button("✅ Conferma questo dataset come distribuzione", key="confirm_dataset_as_distribution_mapped"):
                    _handle_dataset_as_distribution(st_app, distribution_internal, repos['distribution'])
            else:
                st_app.info(f"Nessuna distribuzione fisica trovata in questo dataset: {distribution_internal}")
        else:
            # Selezione tra le leaf trovate
            current_leaf_str = "/".join(st_app.session_state.selected_path_parts)
            def_idx = filtered_leaves.index(current_leaf_str) if current_leaf_str in filtered_leaves else 0

            selected_leaf = st_app.selectbox(
                "Distribuzioni rilevate su disco:",
                options=filtered_leaves,
                index=def_idx,
                format_func=lambda x: f"📦 {x}",
                key="dist_leaf_select_mapped"
            )

            st_app.session_state.selected_path_parts = [] if selected_leaf == "" else selected_leaf.replace("\\", "/").split("/")
            
            current_full_path = os.path.normpath(os.path.join(base_path, *st_app.session_state.selected_path_parts))
            st_app.info(f"Current path: {current_full_path}")

            if st_app.button("✅ Conferma questa distribuzione fisica", key="confirm_distribution_mapped"):
                dist_uri = f"{BASE_PREFIX}{current_full_path}"
                existing_dist = repos['distribution'].get_by_uri(dist_uri)
                # Validazione specifica per Step 3
                if existing_dist and existing_dist.step != 3:
                    st_app.error(f"🚨 ALERT AMMINISTRATORE: La distribuzione ha step={existing_dist.step}. Deve essere 3 per il layer Mapped!")
                else:
                    _confirm_distribution_selection(st_app, current_full_path, repos['distribution'])

        # --- 3️⃣ SEZIONE DISTRIBUZIONI LOGICHE (UNMATERIALIZED) ---
        st_app.divider()
        st_app.subheader("📋 Distribuzioni Logiche (Step 3)")
        
        try:
            dist_uri_filter = f"{BASE_PREFIX}{st_app.session_state.selected_dataset_path}"
            available_dists = repos['distribution'].get_by_uri_prefix_or_step(dist_uri_filter, step=3)
            
            unmat = [d for d in available_dists if d.dataset_id == st_app.session_state.selected_dataset_id and not d.materialized]
            
            # Applicazione filtri search/meta
            unmat_filtered = []
            for d in unmat:
                include = True
                if dist_search:
                    dn = (getattr(d, 'name', '') or '').lower()
                    if dist_search not in dn and dist_search not in d.uri.lower(): 
                        include = False
                if include and dist_langs:
                    rec_lang = getattr(d, 'lang', None) or getattr(d, 'language', None) or ''
                    rec_langs = [s.strip().lower() for s in (rec_lang if isinstance(rec_lang, list) else [rec_lang]) if s]
                    if not any(l.lower() in rec_langs for l in dist_langs):
                        include = False
                if include and dist_licenses:
                    rec_license = getattr(d, 'license', None) or getattr(d, 'license_code', None) or ''
                    rec_licenses = [s.strip().lower() for s in (rec_license if isinstance(rec_license, list) else [rec_license]) if s]
                    if not any(lc.lower() in rec_licenses for lc in dist_licenses):
                        include = False
                if include and dist_splits:
                    rec_split = getattr(d, 'split', None) or ''
                    rec_splits = [s.strip().lower() for s in (rec_split if isinstance(rec_split, list) else [rec_split]) if s]
                    if not any(sp.lower() in rec_splits for sp in dist_splits):
                        include = False
                if include:
                    unmat_filtered.append(d)

            logger.info(f"Trovate {len(unmat_filtered)} distribuzioni logiche non materializzate (Step 3)")

            if unmat_filtered:
                unmat_labels = _build_unmaterialized_distribution_labels(unmat_filtered)
                sel_unmat_label = st_app.selectbox("Scegli distribuzione logica:", [""] + unmat_labels, key="unmat_dist_select_mapped")
                unmat_map = {lab: dist for lab, dist in zip(unmat_labels, unmat_filtered)}

                if sel_unmat_label and sel_unmat_label in unmat_map:
                    sel_dist = unmat_map[sel_unmat_label]
                    
                    if sel_dist.step != 3:
                        st_app.error(f"🚨 ALERT AMMINISTRATORE: Distribution non pronta (Step: {sel_dist.step}). Richiesto Step=3.")
                    elif st_app.button("✅ Usa questa distribuzione logica", key="use_unmaterialized_btn_mapped"):
                        logger.info(f"Usando distribution logica mapped: {sel_dist.name} (ID: {sel_dist.id})")
                        st_app.session_state.current_distribution = sel_dist
                        reset_dashboard_session_state(st_app, distribution_keys)
                        path = sel_dist.uri.replace(BASE_PREFIX, "")
                        st_app.session_state.current_distribution_path = path
                        if os.path.exists(path):
                            st_app.session_state.samples = load_dataset_samples(path)
                        st_app.session_state.current_stage = "mapped_distribution_main"
                        st_app.rerun()
            else:
                st_app.info("Nessuna distribuzione non materializzata con Step 3 trovata.")
        except Exception as e:
            st_app.error(f"Errore caricamento distribuzioni logiche: {e}")

def _create_new_distribution(st_app, current_path: str, distribution_uri: str, distribution_repo: DistributionRepository):
    """Crea una nuova distribuzione nel DB."""
    try:
        binded_uri = to_binded_path(distribution_uri)

        # Controlla se esiste già
        existing_distribution = distribution_repo.get_by_uri(binded_uri)
        if existing_distribution:
            st_app.session_state.current_distribution = existing_distribution
            st_app.session_state.current_distribution_path = current_path
            st_app.session_state.samples = load_dataset_samples(current_path)
            reset_dashboard_session_state(st_app, distribution_keys)
            st_app.session_state.current_stage = "mapped_distribution_main"
            return

        # Estrai metadata dal path
        file_glob = generate_dataset_globs(current_path)[0]
        _, ext = os.path.splitext(file_glob)
        
        dataset_repo = DatasetRepository(st_app.session_state.db_manager)
        ds = dataset_repo.get_by_id(st_app.session_state.selected_dataset_id)

        logger.info(f"Creando nuova distribution mapped con URI: {binded_uri}")

        new_dist = Distribution(
            id=None,
            uri=binded_uri,
            tokenized_uri=None,
            dataset_id=ds.id,
            glob=file_glob,
            format=ext.lstrip('.') or 'unknown',
            name="/".join(st_app.session_state.selected_path_parts),
            query=None,
            derived_from=None,
            src_schema={},
            description="",
            split="unknown",
            lang='un',
            tags=[],
            license=ds.license or 'unknown',
            version=ds.version or '1.0',
            issued=datetime.now(timezone.utc),
            modified=datetime.now(timezone.utc),
            materialized=True,
            step=3
        )
        
        result = distribution_repo.insert(new_dist)
        if result:
            logger.info(f"✅ Distribution mapped creata - ID: {result.id}")
            st_app.session_state.current_distribution = result
            st_app.session_state.current_distribution_path = current_path
            st_app.session_state.samples = load_dataset_samples(current_path)
            reset_dashboard_session_state(st_app, distribution_keys)
            st_app.session_state.current_stage = "mapped_distribution_main"
    except Exception as e:
        st_app.error("Errore nella creazione della nuova distribution.")
        logger.exception(e)

def _confirm_distribution_selection(st_app, current_path: str, distribution_repo: DistributionRepository):
    """Verifica e conferma per il layer Mapped (Step 3)."""
    base_path = to_internal_path(_normalize_uri_to_path(st_app.session_state.selected_dataset_uri))
    selected_relpath = os.path.relpath(current_path, base_path)
    current_dist_path = os.path.join(base_path, selected_relpath)
    distribution_uri_raw = f"{BASE_PREFIX}{current_dist_path}"
    distribution_uri = to_binded_path(distribution_uri_raw)

    logger.info(f"Conferma distribution mapped con URI: {distribution_uri}")

    try:
        existing = distribution_repo.get_by_uri(distribution_uri)
        if existing:
            logger.info(f"Distribution mapped trovata - ID: {existing.id}, Step: {existing.step}")
            
            if existing.step == 3:
                st_app.session_state.current_distribution = existing
                st_app.session_state.current_distribution_path = current_dist_path
                st_app.session_state.samples = load_dataset_samples(current_dist_path)
                reset_dashboard_session_state(st_app, distribution_keys)
                st_app.session_state.current_stage = "mapped_distribution_main"
                st_app.rerun()
            else:
                st_app.error(f"Impossibile procedere: Lo step è {existing.step}. Richiesto 3.")
        else:
            logger.info("Distribution mapped non trovata, creazione...")
            _create_new_distribution(st_app, current_path, distribution_uri, distribution_repo)
            st_app.rerun()
    except Exception as e:
        st_app.error("Errore durante la conferma.")
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
            if existing.step == 3:
                st_app.session_state.current_distribution = existing
                reset_dashboard_session_state(st_app, distribution_keys)
                st_app.session_state.current_distribution_path = current_path
                st_app.session_state.samples = load_dataset_samples(current_path)
                st_app.session_state.current_stage = "mapped_distribution_main"
                st_app.rerun()
            else:
                st_app.error(f"🚨 ALERT AMMINISTRATORE: La distribuzione ha step={existing.step}. Deve essere 3 per il layer Mapped!")
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

        new_distribution = distribution_repo.get_by_uri(binded_distribution_uri)
        if new_distribution:
            logger.info(f"Distribution already exists for URI: {binded_distribution_uri}")
            st_app.session_state.current_distribution = new_distribution
            reset_dashboard_session_state(st_app, distribution_keys)
            st_app.session_state.current_distribution_path = current_path
            st_app.session_state.samples = load_dataset_samples(current_path)
            st_app.session_state.current_stage = "mapped_distribution_main"
        else:
            new_distribution = Distribution(
                id=None, 
                uri=binded_distribution_uri, 
                tokenized_uri=None,
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
                materialized=True,
                step=3
            )

            result = distribution_repo.insert(new_distribution)
            if result:
                st_app.session_state.current_distribution = result
                reset_dashboard_session_state(st_app, distribution_keys)
                st_app.session_state.current_distribution_path = current_path
                st_app.session_state.samples = load_dataset_samples(current_path)
                st_app.session_state.current_stage = "mapped_distribution_main"
                st_app.success(f"✅ Distribuzione creata con successo per il dataset {dataset_name}")
            else:
                st_app.error("Errore durante l'inserimento della distribuzione nel database")
            
    except Exception as e:
        st_app.error("Errore creando la nuova distribution dal dataset.")
        logger.exception(e)