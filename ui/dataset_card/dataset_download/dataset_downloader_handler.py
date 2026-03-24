import os
import re
from typing import Optional, Tuple
import streamlit as st

from data_class.entity.table.dataset_card import DatasetCard
from ui.dataset_card.dataset_download.download_model import DownloadConfig
from ui.dataset_card.dataset_download.downloader import DatasetDownloader
from utils.fs_func import list_dirs
from datetime import datetime
from utils.extract_glob import generate_dataset_globs

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
BINDED_RAW_DATA_DIR = os.getenv("BINDED_RAW_DATA_DIR")
# Manteniamo BASE_PATH per compatibilità con il codice esistente
BASE_PATH = RAW_DATA_DIR

# Nuovi import necessari per acquisizione metadati
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.entity.table.dataset import Dataset as DatasetEntity
from ui.raw.analytics.query_raw_distribution_handler import BASE_PREFIX

class DownloadPathNavigator:
    """Gestisce la navigazione e selezione del percorso di download"""
    
    def __init__(self, base_path: str):
        self.base_path = base_path
        self._init_session_state()
    
    def _init_session_state(self):
        if "download_path_parts" not in st.session_state:
            st.session_state.download_path_parts = []
        if "selected_download_path" not in st.session_state:
            st.session_state.selected_download_path = None
    
    def render_navigation_interface(self) -> Tuple[Optional[str], str]:
        """Renderizza l'interfaccia di navigazione del filesystem"""
        current_path = self._get_current_path()
        
        st.write(f"**Percorso corrente:** `{current_path}`")
        
        # Navigazione cartelle esistenti
        self._render_existing_folders(current_path)
        
        # Creazione nuova cartella
        self._render_folder_creation(current_path)
        
        # Controlli di navigazione
        return self._render_navigation_controls(current_path)
    
    def _get_current_path(self) -> str:
        """
        Calcola il percorso corrente come RAW_DATA_DIR + subpath selezionato.
        """
        if not RAW_DATA_DIR:
            raise ValueError("RAW_DATA_DIR non è definito. Assicurati che la variabile d'ambiente sia configurata.")
        return os.path.join(RAW_DATA_DIR, *st.session_state.download_path_parts)
    
    def _render_existing_folders(self, current_path: str):
        try:
            subdirs = list_dirs(current_path)
            if subdirs:
                st.write("#### 📂 Cartelle disponibili")
                
                # Filtraggio con ricerca
                search_query = st.text_input("🔍 Cerca cartelle:", key="download_path_search").lower()
                filtered_dirs = [d for d in subdirs if search_query in d.lower()] if search_query else subdirs
                
                if filtered_dirs:
                    selected_dir = st.selectbox("Seleziona una cartella:", [""] + filtered_dirs, key="download_path_select")
                    if selected_dir:
                        st.session_state.download_path_parts.append(selected_dir)
                        st.rerun()
                else:
                    st.info("🔍 Nessuna cartella trovata")
            else:
                st.info("📁 Nessuna sottocartella disponibile")
        except Exception as e:
            st.error(f"❌ Errore caricamento cartelle: {e}")
    
    def _render_folder_creation(self, current_path: str):
        st.write("#### 📁 Crea nuova cartella")
        col1, col2 = st.columns([3, 1])
        
        with col1:
            new_folder = st.text_input("Nome nuova cartella:", placeholder="es: mio_dataset", label_visibility="collapsed")
        
        with col2:
            if st.button("Crea", disabled=not new_folder, use_container_width=True):
                self._create_new_folder(current_path, new_folder.strip())
    
    def _create_new_folder(self, current_path: str, folder_name: str):
        try:
            new_path = os.path.join(current_path, folder_name)
            os.makedirs(new_path, exist_ok=True)
            st.session_state.download_path_parts.append(folder_name)
            st.success(f"✅ Cartella creata: `{new_path}`")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Errore creazione cartella: {e}")
    
    def _render_navigation_controls(self, current_path: str) -> Tuple[Optional[str], str]:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.session_state.download_path_parts and st.button("⬅️ Indietro", use_container_width=True):
                st.session_state.download_path_parts.pop()
                st.rerun()
        
        with col2:
            if st.session_state.download_path_parts and st.button("🏠 Radice", use_container_width=True):
                st.session_state.download_path_parts = []
                st.rerun()
        
        with col3:
            if st.button("✅ Usa questa cartella", type="primary", use_container_width=True):
                st.session_state.selected_download_path = current_path
                return current_path, "confermato"
        
        return current_path if st.session_state.download_path_parts else None, "selezionato"

class DownloadSourceSelector:
    """Gestisce la selezione della fonte di download"""
    
    @staticmethod
    def render_source_selection(dataset_card: DatasetCard) -> Tuple[Optional[str], DownloadConfig, bool]:
        """Renderizza la selezione della fonte di download"""
        available_sources = DownloadSourceSelector._get_available_sources(dataset_card)
        
        if not available_sources:
            st.error("❌ Nessuna fonte di download disponibile")
            return None, DownloadConfig(source=''), False
        
        selected_source = st.radio("Seleziona la fonte:", available_sources, horizontal=True, key="download_source")
        
        if selected_source == "URL Direct" and dataset_card.download_url:
            return DownloadSourceSelector._handle_url_source(dataset_card)
        elif selected_source == "Hugging Face":
            return DownloadSourceSelector._handle_huggingface_source(dataset_card)
        
        return None, DownloadConfig(source=''), False
    
    @staticmethod
    def _get_available_sources(dataset_card: DatasetCard) -> list:
        sources = []
        if dataset_card.download_url:
            sources.append("URL Direct")
        if dataset_card.source_url and 'huggingface.co' in dataset_card.source_url:
            sources.append("Hugging Face")
        
        # Sempre disponibile Hugging Face come fallback
        if "Hugging Face" not in sources:
            sources.append("Hugging Face")
        
        return sources
    
    @staticmethod
    def _handle_url_source(dataset_card: DatasetCard) -> Tuple[str, DownloadConfig, bool]:
        st.info(f"**URL di download:** {dataset_card.download_url}")
        config = DownloadConfig(
            source='url',
            url=dataset_card.download_url
        )
        return "URL Direct", config, True
    
    @staticmethod
    def _handle_huggingface_source(dataset_card: DatasetCard) -> Tuple[str, DownloadConfig, bool]:
        repo_id = DownloadSourceSelector._get_huggingface_repo_id(dataset_card)
        if not repo_id:
            return None, DownloadConfig(source=''), False
        
        download_type = st.radio("Tipo di download:", ["Dataset completo", "File specifico"], horizontal=True, key="hf_download_type")
        
        config = DownloadConfig(
            source='huggingface',
            repo_id=repo_id,
            download_type=download_type.lower()
        )
        
        if download_type == "File specifico":
            file_path = st.text_input("Percorso file:", placeholder="es: data/train.csv", key="hf_file_path")
            config.file_path = file_path
        
        return "Hugging Face", config, True
    
    @staticmethod
    def _get_huggingface_repo_id(dataset_card: DatasetCard) -> Optional[str]:
        """Estrae o richiede il repository ID di Hugging Face"""
        if dataset_card.source_url and 'huggingface.co' in dataset_card.source_url:
            match = re.search(r'huggingface\.co/datasets/([^/]+/[^/?]+)', dataset_card.source_url)
            if match:
                auto_repo = match.group(1)
                st.info(f"**Repository Hugging Face:** `{auto_repo}`")
                return st.text_input("Conferma repository:", value=auto_repo, key="hf_repo_input")
        
        return st.text_input("Repository ID Hugging Face:", placeholder="es: microsoft/DialogRACE", key="hf_repo_input_manual")

def show_download_interface(st):
    """Interfaccia principale per il download dei dataset"""
    st.header("📥 Download Dataset")
    
    dataset_card = st.session_state.get('selected_dataset_card')
    if not dataset_card:
        st.error("Nessuna dataset card selezionata")
        if st.button("🔙 Torna alle Dataset Card"):
            st.session_state.current_stage = "dataset_card_action_selection"
            st.rerun()
        return
    
    # Converti in oggetto DatasetCard se è un dizionario
    if isinstance(dataset_card, dict):
        dataset_card = DatasetCard(**dataset_card)
    
    downloader = DatasetDownloader(st.session_state.db_manager, BASE_PATH)
    
    st.write(f"**Dataset:** {dataset_card.dataset_name}")
    
    # Selezione percorso
    st.subheader("📍 Seleziona percorso di download")
    selected_path, path_status = _render_path_selection(st, dataset_card)
    
    # Selezione fonte
    st.subheader("🌐 Sorgente download")
    download_source, download_config, source_ready = DownloadSourceSelector.render_source_selection(dataset_card)
    
    # Pulsante download
    st.markdown("---")
    _render_download_action(st, downloader, dataset_card, selected_path, path_status, download_config, source_ready)

def _render_path_selection(st, dataset_card: DatasetCard) -> Tuple[Optional[str], str]:
    """Renderizza la selezione del percorso"""
    mode = st.radio("Modalità selezione:", ["Naviga filesystem", "Inserisci manualmente"], horizontal=True, key="path_selection_mode")
    
    if mode == "Naviga filesystem":
        navigator = DownloadPathNavigator(BASE_PATH)
        return navigator.render_navigation_interface()
    else:
        return _render_manual_path_input(st, dataset_card)

def _render_manual_path_input(st, dataset_card: DatasetCard) -> Tuple[Optional[str], str]:
    """Renderizza l'input manuale del percorso"""
    st.info("💡 Inserisci il percorso relativo rispetto alla directory base")
    st.write(f"**Directory base:** `{BASE_PATH}`")
    
    relative_path = st.text_input(
        "Percorso relativo:",
        value=dataset_card.dataset_name.lower().replace(' ', '_'),
        key="manual_path_input"
    )
    
    if not relative_path:
        return None, "non_selezionato"
    
    full_path = os.path.join(BASE_PATH, relative_path)
    
    if os.path.exists(full_path):
        st.success(f"✅ **Percorso esistente:** `{full_path}`")
        return full_path, "confermato"
    else:
        st.warning(f"⚠️ **Nuovo percorso:** `{full_path}`")
        return full_path, "selezionato"


'''
def _execute_download(st, downloader: DatasetDownloader, dataset_card,
                     download_path: str, config):
    """Helper UI per gestire il feedback del download background"""
    with st.spinner("📥 Inizializzazione processo..."):
        result = downloader.execute_download(dataset_card, download_path, config)
    
    if result.success:
        st.success(f"✅ {result.message}")
        # Puliamo eventuali vecchi comandi CLI per evitare confusione
        st.session_state.generated_cli_command = None 
    else:
        st.error(f"❌ {result.message}")
'''


def _render_download_action(st, downloader: DatasetDownloader, dataset_card,
                          selected_path: str, path_status: str, 
                          download_config, source_ready: bool):
    """Interfaccia con pulsanti sdoppiati: Background vs CLI"""
    
    repo_id = download_config.repo_id if download_config.source == 'huggingface' else ""
    download_ready = source_ready and selected_path and "/" in repo_id

    # 1. MOSTRA COMANDO CLI (Se generato in precedenza)
    if st.session_state.get("generated_cli_command"):
        st.info("📋 **Copia questo comando per l'Host esterno:**")
        st.code(st.session_state.generated_cli_command, language="bash")
        if st.button("🗑️ Chiudi comando", key="btn_clear_cli"):
            st.session_state.generated_cli_command = None
            st.rerun()

        # Inserisco diceria e bottone acquisizione metadati
        st.markdown("---")
        st.write("Una volta concluso il download, premi qui per acquisire metadati")
        if st.button("📥 Acquisisci metadati", key="btn_acquire_metadata"):
            # Usa le informazioni salvate in session_state per ricostruire il contesto
            last_info = st.session_state.get("last_download_info")
            _acquire_metadata(last_info)

        st.markdown("---")

    # 2. ANTEPRIMA PATH GERARCHICO
    if download_ready:
        repo_parts = [p for p in repo_id.split('/') if p]
        final_path_preview = os.path.join(selected_path, *repo_parts)
        st.success(f"📂 **Destinazione finale:** `{final_path_preview}`")
    else:
        st.warning("⚠️ Completa la selezione (Path e Sorgente) per procedere.")

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        # PULSANTE 2: SOLO GENERAZIONE TESTO
        if st.button("💻 GENERA COMANDO CLI", disabled=not download_ready, use_container_width=True):
            # Chiama la funzione specifica per il comando
            cmd = downloader.get_cli_command(repo_id, selected_path)
            st.session_state.generated_cli_command = cmd
            
            # CALCOLO IL PATH FINALE (inclusi i segmenti del repo_id) prima di salvarlo
            repo_parts = [p for p in repo_id.split('/') if p]
            final_storage_path = os.path.join(selected_path, *repo_parts)
            
            # Salvo le informazioni necessarie per l'acquisizione metadati lato UI
            st.session_state.last_download_info = {
                "repo_id": repo_id,
                "selected_path": final_storage_path, # Salviamo il path atomico finale
                "download_config": download_config.__dict__ if hasattr(download_config, "__dict__") else None,
                "derived_card": getattr(dataset_card, "id", None)
            }
            st.rerun()

# Nuova funzione helper per acquisire metadati dal path/ repo_id e inserire il Dataset
def _acquire_metadata(info: dict):
    """
    Deriva URI e nome dal repo_id o dal percorso, crea un Dataset e lo upserta nel DB.
    """
    try:
        if not info:
            st.error("Nessuna informazione di download disponibile per acquisire metadati.")
            return

        # Recupera la dataset_card corrente dalla session state
        dataset_card = st.session_state.get('selected_dataset_card')
        if isinstance(dataset_card, dict):
            dataset_card = DatasetCard(**dataset_card)

        repo_id = info.get("repo_id") or ""
        # Questo è il path COMPLETO esempio(/app/nfs/data-download/velvet_v1/allenai)
        selected_path = info.get("selected_path")
        derived_card = info.get("derived_card")
        download_config = info.get("download_config") or {}

        # Determina publisher e dataset segment per i globs
        publisher = None
        dataset_segment = None
        if repo_id and "/" in repo_id:
            parts = repo_id.split('/')
            publisher = parts[0]
            dataset_segment = parts[1]
        
        # LOGICA DI MAPPING PATH (Container -> Host)
        # selected_path è es: /app/nfs/data-download/velvet_v1/allenai
        # RAW_DATA_DIR è es: /app/nfs/data-download
        # BINDED_RAW_DATA_DIR è es: file:///Users/.../nfs/data-download
        
        binded_path_for_db = selected_path
        if BINDED_RAW_DATA_DIR and RAW_DATA_DIR and selected_path.startswith(RAW_DATA_DIR):
            rel_path = os.path.relpath(selected_path, RAW_DATA_DIR)
            # Rimuoviamo eventuali prefissi 'file://' se presenti in BINDED_RAW_DATA_DIR per usare os.path.join,
            # lo rimetteremo dopo tramite BASE_PREFIX
            base_binded = BINDED_RAW_DATA_DIR.replace("file://", "")
            binded_path_for_db = os.path.join(base_binded, rel_path)

        # Costruisci URI finale
        uri = f"{BASE_PREFIX}{binded_path_for_db}"

        if not uri:
            st.error("Impossibile derivare URI dal repository o dal percorso selezionato.")
            return

        # Controlla se il dataset esiste già sul DB
        repo = DatasetRepository(st.session_state.db_manager)
        existing = repo.get_by_uri(uri)

        # Se esiste, riusa i globs, altrimenti genera i globs scansionando il path container
        if existing:
            globs = existing.globs or []
        else:
            if selected_path and os.path.exists(selected_path):
                try:
                    globs = generate_dataset_globs(selected_path)
                except Exception:
                    st.warning("⚠️ Errore generazione globs dal percorso. Verranno usati globs di default.")
                    globs = []
            else:
                os.makedirs(os.path.dirname(selected_path), exist_ok=True)
                globs = generate_dataset_globs(dataset_segment)

        # Nome e description derivati dalla card
        name = getattr(dataset_card, 'dataset_name', None) or repo_id or (dataset_segment or 'dataset')
        description = getattr(dataset_card, 'dataset_description', '') if dataset_card else ''
        source = getattr(dataset_card, 'source_url', None) or getattr(dataset_card, 'download_url', None) or download_config.get('source')

        # languages e license
        languages = ["en"]
        if dataset_card and getattr(dataset_card, 'languages', None):
            langs = getattr(dataset_card, 'languages')
            if isinstance(langs, (list, tuple)) and len(langs) > 0:
                languages = list(langs)
            elif isinstance(langs, str) and langs:
                languages = [langs]

        license_val = getattr(dataset_card, 'license', None) if dataset_card else 'unknown'

        # Costruzione dell'entità Dataset
        dataset_entity = DatasetEntity(
            uri=uri,
            derived_card=derived_card,
            derived_dataset=None,
            dataset_type='unknown',
            step=1,
            globs=globs,
            languages=languages,
            name=name,
            description=description,
            source=source,
            version='1.0.0',
            issued=datetime.utcnow(),
            modified=datetime.utcnow(),
            license=license_val
        )
        
        st.info(f"inserimento: {str(dataset_entity)}")
        
        try:
            result = repo.upsert_by_uri(dataset_entity)
        except Exception as e:
            err_str = str(e).lower()
            if 'unique' in err_str and 'name' in err_str:
                result = repo.upsert_by_name(dataset_entity)
            else:
                raise e

        if result:
            st.success(f"Metadati acquisiti. Dataset creato/aggiornato con URI: {result.uri}")
            st.session_state.generated_cli_command = None
            st.session_state.last_download_info = None
        else:
            st.error("Errore durante la creazione/aggiornamento del dataset nel DB.")

    except Exception as e:
        st.error(f"Errore acquisizione metadati: {e}")