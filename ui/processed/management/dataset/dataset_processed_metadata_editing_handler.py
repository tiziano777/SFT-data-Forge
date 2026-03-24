# ui/processed/management/dataset_processed_metadata_editing_handler.py
import logging
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import streamlit as st
import os

from utils.extract_glob import generate_dataset_globs
from utils.path_utils import to_binded_path
from utils.streamlit_func import reset_dashboard_session_state
from config.state_vars import home_vars
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from data_class.entity.table.dataset import Dataset
from data_class.repository.table.dataset_card_repository import DatasetCardRepository

POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")
BASE_PREFIX = os.getenv("BASE_PREFIX")

logger = logging.getLogger(__name__)

class DatasetMetadataEditingHandler:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.dataset_repo = DatasetRepository(db_manager)
        self.language_repo = VocabLanguageRepository(db_manager)
        self.dataset_card_repo = DatasetCardRepository(db_manager)
        self.available_languages = self._load_available_languages()

    # ─────────────────────────────────────────────────────────────────────────
    # Setup helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _load_available_languages(self) -> List[str]:
        try:
            languages = self.language_repo.get_all()
            return [lang.code for lang in languages]
        except Exception as e:
            logger.error(f"Errore caricando le lingue: {e}")
            return []

    def _get_license_options(self) -> List[str]:
        try:
            with self.db_manager as db:
                license_rows = db.select(table=POSTGRES_DB_SCHEMA + "." + "vocab_license", columns=["code"])
                return [row["code"] for row in license_rows]
        except Exception as e:
            logger.error(f"Errore caricando le licenze: {e}")
            return []

    def _get_dataset_type_options(self) -> List[str]:
        try:
            with self.db_manager as db:
                rows = db.select(table=POSTGRES_DB_SCHEMA + "." + "vocab_dataset_type", columns=["code"])
                return [row["code"] for row in rows]
        except Exception as e:
            logger.error(f"Errore caricando i dataset_type: {e}")
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Recupero / costruzione dati form
    # ─────────────────────────────────────────────────────────────────────────

    def _get_existing_dataset(self, dataset_uri: str) -> Optional[Dict[str, Any]]:
        """Recupera i metadati di un dataset già censito per URI."""
        if not dataset_uri:
            return None
        try:
            dataset = self.dataset_repo.get_by_uri(dataset_uri)
            if dataset:
                return {
                    'id': dataset.id,
                    'uri': dataset.uri,
                    'name': dataset.name,
                    'description': dataset.description,
                    'license': dataset.license,
                    'source': dataset.source,
                    'version': dataset.version,
                    'languages': dataset.languages,
                    'globs': dataset.globs,
                    'derived_card': dataset.derived_card,
                    'derived_dataset': dataset.derived_dataset,
                    'step': 2,
                    'dataset_type': getattr(dataset, 'dataset_type', None),
                }
            return None
        except Exception as e:
            logger.error(f"Errore nel recupero dataset esistente: {e}")
            return None

    def _build_prefill_from_parent(self) -> Optional[Dict[str, Any]]:
        """
        Costruisce il dict di prefill a partire dai dati ereditati dal padre
        (già preparati dal selection handler e salvati in session_state).
        """
        if not st.session_state.get("prefill_from_parent"):
            return None
        parent_data = st.session_state.get("prefill_parent_data", {})
        if not parent_data:
            return None

        return {
            'id': None,
            'uri': st.session_state.get("selected_dataset_uri"),
            'name': parent_data.get("name", ""),
            'description': parent_data.get("description", ""),
            'license': parent_data.get("license"),
            'source': parent_data.get("source"),
            'version': parent_data.get("version", "1.0"),
            'languages': parent_data.get("languages", []),
            'globs': [],
            'derived_card': parent_data.get("derived_card"),
            'derived_dataset': parent_data.get("derived_dataset"),
            'step': 2,
            'dataset_type': parent_data.get("dataset_type"),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Inizializzazione session state per il form
    # ─────────────────────────────────────────────────────────────────────────

    def _initialize_session_state(
        self,
        existing_dataset: Optional[Dict[str, Any]],
        prefill_data: Optional[Dict[str, Any]],
        dataset_internal_path: str,
    ) -> bool:
        """
        Scansiona i globs dal path fisico (internal) e inizializza le lingue selezionate.
        Viene eseguita una sola volta per path (lazy init con cache su last_scanned_path).
        """
        if not dataset_internal_path:
            st.error("❌ Path interno del dataset non disponibile.")
            return False

        if (
            "globs" not in st.session_state
            or st.session_state.get("last_scanned_path") != dataset_internal_path
        ):
            try:
                st.session_state.globs = generate_dataset_globs(dataset_internal_path)
                st.session_state.last_scanned_path = dataset_internal_path
                if not st.session_state.globs:
                    st.warning("⚠️ Nessun file trovato nel percorso. I globs saranno vuoti.")
            except Exception as e:
                logger.error(f"Errore scan globs: {e}")
                st.error(f"Errore durante la scansione dei file: {e}")
                return False

        if "selected_languages" not in st.session_state:
            source = existing_dataset or prefill_data or {}
            langs = source.get("languages") or []
            st.session_state.selected_languages = list(langs)

        return True

    # ─────────────────────────────────────────────────────────────────────────
    # Render sezioni form
    # ─────────────────────────────────────────────────────────────────────────

    def _render_language_section(self):
        st.subheader("🌐 Lingue del dataset")
        current_languages = st.session_state.get("selected_languages", [])
        available = [l for l in self.available_languages if l not in current_languages]

        col1, col2 = st.columns([3, 1])
        with col1:
            new_lang = st.selectbox("Aggiungi lingua", available, key="add_lang_select")
        with col2:
            if st.button("➕ Aggiungi", use_container_width=True):
                st.session_state.selected_languages.append(new_lang)
                st.rerun()

        if current_languages:
            st.write("Selezionate: " + ", ".join([f"`{l}`" for l in current_languages]))
            if st.button("🗑️ Svuota lingue"):
                st.session_state.selected_languages = []
                st.rerun()

    def _render_metadata_form(
        self,
        existing_dataset: Optional[Dict[str, Any]],
        prefill_data: Optional[Dict[str, Any]],
        is_prefill_mode: bool,
    ) -> Dict[str, Any]:
        st.subheader("📋 Metadati")
        ds = existing_dataset or prefill_data or {}

        if is_prefill_mode and not existing_dataset:
            st.info("ℹ️ Campi pre-compilati dal dataset padre. Modifica se necessario.")

        # Info tecniche in sola lettura
        with st.expander("🔍 Info Tecnica (Sola Lettura)", expanded=False):
            st.code(f"URI (binded): {st.session_state.get('editing_uri')}")
            st.code(f"Path (internal): {st.session_state.get('editing_path')}")
            st.code(f"Globs: {st.session_state.get('globs', [])}")

        with st.form("dataset_metadata_form"):
            name = st.text_input("Nome *", value=ds.get("name", ""))
            description = st.text_area("Descrizione", value=ds.get("description", "") or "")

            lic_options = self._get_license_options()
            idx_lic = lic_options.index(ds["license"]) if ds.get("license") in lic_options else 0
            selected_license = st.selectbox("Licenza *", lic_options, index=idx_lic)

            type_options = self._get_dataset_type_options()
            idx_type = (
                type_options.index(ds.get("dataset_type"))
                if ds.get("dataset_type") in type_options
                else 0
            )
            selected_dataset_type = st.selectbox(
                "Tipo dataset", type_options if type_options else ["n/a"], index=idx_type
            )

            source = st.text_input("Source Link", value=ds.get("source", "") or "")
            version = st.text_input("Versione", value=ds.get("version", "1.0") or "1.0")

            submitted = st.form_submit_button("💾 SALVA E SINCRONIZZA", type="primary")

        return {
            "submitted": submitted,
            "name": name,
            "description": description,
            "license": selected_license,
            "dataset_type": selected_dataset_type,
            "source": source,
            "version": version,
            "step": 2,
            "derived_card": ds.get("derived_card"),
            "derived_dataset": ds.get("derived_dataset"),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Salvataggio
    # ─────────────────────────────────────────────────────────────────────────

    def _save_dataset(self, form_data: Dict[str, Any], dataset_uri: str):
        """
        Salva (insert o update) il dataset nel DB.
        - dataset_uri è l'URI "binded" (quello vero, mostrato all'utente e salvato nel DB).
        - Il parent_id viene recuperato da wizard_parent_dataset se presente.
        """
        current_languages = st.session_state.get("selected_languages", [])
        current_globs = st.session_state.get("globs", [])

        if not form_data.get("name") or not current_languages:
            st.error("⚠️ Nome e Lingue sono obbligatori.")
            return

        try:
            existing_dataset = self.dataset_repo.get_by_uri(dataset_uri)
            parent_obj = st.session_state.get("wizard_parent_dataset")
            parent_id = parent_obj.id if parent_obj else None

            if existing_dataset:
                # UPDATE
                existing_dataset.name = form_data["name"]
                existing_dataset.description = form_data["description"]
                existing_dataset.license = form_data["license"]
                existing_dataset.source = form_data["source"] or None
                existing_dataset.version = form_data["version"]
                existing_dataset.languages = current_languages
                existing_dataset.globs = current_globs
                existing_dataset.modified = datetime.now(timezone.utc)
                existing_dataset.step = 2
                existing_dataset.derived_card = form_data.get("derived_card")
                existing_dataset.derived_dataset = parent_id
                existing_dataset.dataset_type = form_data.get("dataset_type")

                if self.dataset_repo.update(existing_dataset) > 0:
                    st.success("✅ Dataset aggiornato con successo!")
                    self._cleanup_and_home()
                else:
                    st.error("❌ Aggiornamento fallito. Verificare i log.")

            else:
                # INSERT — l'URI salvato è già quello binded (corretto per il DB)
                new_ds = Dataset(
                    id=None,
                    uri=to_binded_path(dataset_uri),
                    name=form_data["name"],
                    languages=current_languages,
                    globs=current_globs,
                    description=form_data["description"],
                    source=form_data["source"] or None,
                    version=form_data["version"],
                    issued=datetime.now(timezone.utc),
                    modified=datetime.now(timezone.utc),
                    license=form_data["license"],
                    step=2,
                    derived_card=form_data.get("derived_card"),
                    derived_dataset=parent_id,
                    dataset_type=form_data.get("dataset_type"),
                )
                if self.dataset_repo.insert(new_ds):
                    st.success("✅ Nuovo dataset creato con successo!")
                    self._cleanup_and_home()
                else:
                    st.error("❌ Inserimento fallito. Verificare i log.")

        except Exception as e:
            st.error(f"Errore durante il salvataggio: {e}")
            logger.error(traceback.format_exc())

    # ─────────────────────────────────────────────────────────────────────────
    # Cleanup e navigazione
    # ─────────────────────────────────────────────────────────────────────────

    def _cleanup_and_home(self):
        self._cleanup_wizard_state()
        reset_dashboard_session_state(st, home_vars)
        st.session_state.current_stage = "home"
        st.rerun()

    def _cleanup_wizard_state(self):
        keys = [
            "wizard_mode", "wizard_step", "wizard_parent_dataset",
            "prefill_from_parent", "prefill_parent_uri", "prefill_parent_data",
            "selected_path_parts", "path_confirmed",
            "editing_uri", "editing_path",
            "selected_dataset_uri", "selected_dataset_path", "selected_dataset_id",
            "globs", "last_scanned_path", "selected_languages",
        ]
        for key in keys:
            st.session_state.pop(key, None)

    def _render_back_button(self, st_ref):
        st_ref.markdown("---")
        if st_ref.button("🔙 Annulla e torna alla Home", key="back_nav_final"):
            self._cleanup_wizard_state()
            st_ref.session_state.current_stage = "home"
            st_ref.rerun()

    # ─────────────────────────────────────────────────────────────────────────
    # Entry point principale del form
    # ─────────────────────────────────────────────────────────────────────────

    def show_form(self, st_ref):
        """
        Entry point del form di metadatazione.

        L'URI e il path sono già stati risolti dal selection handler e salvati in:
          - st.session_state.selected_dataset_uri  → URI "binded" (salvato nel DB)
          - st.session_state.selected_dataset_path → path interno al container

        Questa funzione NON si occupa più di navigazione filesystem.
        """
        is_prefill_mode = st_ref.session_state.get("prefill_from_parent", False)

        # Recupera URI e path già risolti (da entrambe le strade A e B)
        dataset_uri = st_ref.session_state.get("selected_dataset_uri")
        dataset_internal_path = st_ref.session_state.get("selected_dataset_path")

        if not dataset_uri or not dataset_internal_path:
            st_ref.error("❌ URI o path del dataset mancanti. Torna alla selezione.")
            self._render_back_button(st_ref)
            return

        # Imposta le chiavi di editing usate nei widget di sola lettura
        st_ref.session_state["editing_uri"] = dataset_uri
        st_ref.session_state["editing_path"] = dataset_internal_path

        # Recupera dataset esistente (se l'URI è già censito → update)
        existing_ds = self._get_existing_dataset(dataset_uri)

        # Costruisce prefill (solo strada B: nuovo dataset derivato)
        prefill_data = self._build_prefill_from_parent() if is_prefill_mode else None

        # Inizializza stato sessione (globs + lingue)
        if not self._initialize_session_state(existing_ds, prefill_data, dataset_internal_path):
            return

        # Header contestuale
        if existing_ds:
            st_ref.header(f"📝 Aggiornamento Dataset: {existing_ds.get('name', '')}")
        elif is_prefill_mode:
            parent_name = st_ref.session_state.get("prefill_parent_data", {}).get("name", "")
            st_ref.header(f"🆕 Nuovo Dataset Derivato da: {parent_name}")
        else:
            st_ref.header("📝 Metadatazione Dataset")

        # Render sezioni
        self._render_language_section()
        form_result = self._render_metadata_form(existing_ds, prefill_data, is_prefill_mode)

        if form_result["submitted"]:
            self._save_dataset(form_result, dataset_uri)

        self._render_back_button(st_ref)

# ─────────────────────────────────────────────────────────────────────────────
# Funzione di ingresso chiamata dal router principale
# ─────────────────────────────────────────────────────────────────────────────

def show_dataset_metadata_editing(st_ref):
    if "db_manager" not in st_ref.session_state:
        st_ref.error("Errore: DB Manager non disponibile.")
        return
    handler = DatasetMetadataEditingHandler(st_ref.session_state.db_manager)
    handler.show_form(st_ref)