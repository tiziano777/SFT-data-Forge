# ui/mapped/management/dataset/dataset_mapped_metadata_editing_handler.py
import logging
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import psycopg
import streamlit as st

from utils.path_utils import to_internal_path
from utils.fs_func import list_dirs
from utils.extract_glob import generate_dataset_globs
from utils.streamlit_func import reset_dashboard_session_state
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from data_class.entity.table.dataset import Dataset
from config.state_vars import home_vars
from data_class.repository.table.dataset_card_repository import DatasetCardRepository

import os

BASE_PREFIX = os.getenv("BASE_PREFIX")
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

logger = logging.getLogger(__name__)


class DatasetMetadataEditingHandler:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.dataset_repo = DatasetRepository(db_manager)
        self.language_repo = VocabLanguageRepository(db_manager)
        self.dataset_card_repo = DatasetCardRepository(db_manager)
        self.available_languages = self._load_available_languages()

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

    def _get_existing_dataset(self, dataset_uri: str) -> Optional[Dict[str, Any]]:
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
                    'step': 3,
                    'dataset_type': getattr(dataset, 'dataset_type', None)
                }
            return None
        except Exception as e:
            logger.error(f"Errore nel recupero dataset esistente: {e}")
            return None

    def _build_prefill_from_parent(self) -> Optional[Dict[str, Any]]:
        """
        Costruisce il dict di pre-fill a partire dai dati del dataset padre (step=2).
        Restituisce None se non siamo in modalità prefill.
        """
        if not st.session_state.get("prefill_from_parent"):
            return None

        parent_data = st.session_state.get("prefill_parent_data", {})
        if not parent_data:
            return None

        base_name = parent_data.get("name", "")
        derived_name = f"mapped__{base_name}" if base_name else ""

        return {
            'id': None,
            'uri': None,
            'name': derived_name,
            'description': parent_data.get("description", ""),
            'license': parent_data.get("license"),
            'source': parent_data.get("source"),
            'version': parent_data.get("version", "1.0"),
            'languages': parent_data.get("languages", []),
            'globs': [],
            'derived_card': parent_data.get("derived_card"),
            'derived_dataset': parent_data.get("derived_dataset"), 
            'step': 3,
            'dataset_type': parent_data.get("dataset_type"),
        }

    def _initialize_session_state(
        self,
        existing_dataset: Optional[Dict[str, Any]],
        prefill_data: Optional[Dict[str, Any]],
        dataset_path: str
    ) -> bool:
        if not dataset_path:
            return False

        if "globs" not in st.session_state:
            try:
                st.session_state.globs = generate_dataset_globs(dataset_path)
                if not st.session_state.globs:
                    st.error("❌ Nessun file trovato nel percorso specificato.")
                    return False
            except Exception as e:
                logger.error(f"Errore scan globs: {e}")
                return False

        if "selected_languages" not in st.session_state:
            source = existing_dataset or prefill_data or {}
            langs = source.get("languages") or []
            st.session_state.selected_languages = list(langs)

        return True

    def _resolve_uri_for_new_dataset(self, st) -> Optional[str]:
        st.subheader("📁 Selezione Percorso Nuovo Dataset")

        base_physical_path = st.session_state.get("dataset_path", "/app/nfs/mapped-data")

        st.info(
            f"Naviga nelle sottocartelle di `{base_physical_path}`. "
            f"Il prefisso `{BASE_PREFIX}` verrà aggiunto all'URI finale."
        )

        if "new_ds_nav_parts" not in st.session_state:
            st.session_state.new_ds_nav_parts = []

        current_physical_path = os.path.normpath(
            os.path.join(base_physical_path, *st.session_state.new_ds_nav_parts)
        )

        relative_subpath = "/".join(st.session_state.new_ds_nav_parts)
        resolved_uri = f"{BASE_PREFIX}{base_physical_path}/{relative_subpath}".rstrip("/")

        st.success(f"📍 **URI risultante:** `{resolved_uri}`")

        try:
            subdirs = list_dirs(current_physical_path)
        except Exception:
            st.error(f"Errore nell'accesso al percorso: {current_physical_path}")
            subdirs = []

        # FIX BUG 3: selectbox con indice fisso a 0, append solo su cambio esplicito
        subdir_options = [""] + sorted(subdirs)
        selected_subdir = st.selectbox(
            "📁 Seleziona sottocartella per scendere:",
            subdir_options,
            index=0,                      # ← sempre reset a "" dopo ogni rerun
            key="navigation_selectbox",
            help="Scegli una cartella per navigare all'interno",
            disabled=not subdirs
        )

        col_back, col_down = st.columns(2)
        with col_back:
            if st.session_state.new_ds_nav_parts:
                if st.button("⬅️ Back", key="nav_up_btn", use_container_width=True):
                    st.session_state.new_ds_nav_parts.pop()
                    st.rerun()

        with col_down:
            if st.button("➡️ Entra nella cartella", key="nav_down_btn",
                        use_container_width=True, disabled=not selected_subdir):
                if selected_subdir:
                    st.session_state.new_ds_nav_parts.append(selected_subdir)
                    st.rerun()

        if not subdirs:
            st.info("ℹ️ Nessuna sottocartella trovata in questa posizione.")

        st.markdown("---")

        col_res, col_conf = st.columns(2)
        with col_res:
            if st.button("🔄 Reset navigazione", key="nav_reset_btn", use_container_width=True):
                st.session_state.new_ds_nav_parts = []
                st.rerun()

        with col_conf:
            if st.button("✅ Conferma questo URI", type="primary",
                        key="nav_confirm_btn", use_container_width=True):
                return resolved_uri

        return None
    
    def _render_language_section(self):
        st.subheader("🌐 Lingue del dataset")
        current_languages = st.session_state.get('selected_languages', [])

        available = [l for l in self.available_languages if l not in current_languages]
        col1, col2 = st.columns([3, 1])
        with col1:
            new_lang = st.selectbox("Aggiungi lingua", [""] + available, key="add_lang_select")
        with col2:
            if st.button("➕ Aggiungi", use_container_width=True):
                if new_lang and new_lang not in st.session_state.selected_languages:
                    st.session_state.selected_languages.append(new_lang)

        if current_languages:
            st.write("Selezionate: " + ", ".join([f"`{l}`" for l in current_languages]))
            if st.button("🗑️ Svuota lingue"):
                st.session_state.selected_languages = []

    
    def _render_metadata_form(
        self,
        existing_dataset: Optional[Dict[str, Any]],
        prefill_data: Optional[Dict[str, Any]],
        is_prefill_mode: bool
    ):
        st.subheader("📋 Metadati")
        ds = existing_dataset or prefill_data or {}

        if is_prefill_mode and not existing_dataset:
            st.info(
                "ℹ️ I campi sono pre-compilati con i metadati ereditati dal dataset padre (Step=2). "
                "Puoi modificare qualsiasi valore prima di salvare."
            )

        with st.expander("🔍 Info Tecnica", expanded=False):
            uri_display = st.session_state.get("editing_uri") or "(URI non ancora definito)"
            st.code(f"URI: {uri_display}")
            st.code(f"Globs: {st.session_state.get('globs', [])}")
            if ds.get("derived_dataset"):
                st.info(f"🔗 Derivato da: {ds.get('derived_dataset')}")
            if ds.get("derived_card"):
                try:
                    derived_card_id = ds.get("derived_card")
                    card_entity = None
                    all_cards = self.dataset_card_repo.get_all()
                    for c in all_cards:
                        if getattr(c, 'id', None) == derived_card_id:
                            card_entity = c
                            break

                    if card_entity:
                        st.markdown("**📄 Card di riferimento (overview):**")
                        fields_to_show = [
                            ("dataset_name", "Nome"), ("modality", "Modalità"),
                            ("dataset_description", "Descrizione"), ("publisher", "Publisher"),
                            ("notes", "Note"), ("source_url", "Source URL"),
                            ("download_url", "Download URL"), ("languages", "Lingue"),
                            ("core_skills", "Core Skills"), ("tasks", "Tasks"),
                            ("license", "Licenza"), ("has_reasoning", "Has Reasoning"),
                            ("quality", "Qualità"), ("sources", "Sources"),
                            ("source_type", "Source Type"), ("fields", "Fields"),
                            ("vertical", "Vertical"), ("contents", "Contents"),
                            ("last_update", "Last Update"), ("created_at", "Created At")
                        ]
                        for attr, label in fields_to_show:
                            val = getattr(card_entity, attr, None)
                            if isinstance(val, (list, tuple)):
                                display_val = ", ".join([str(x) for x in val]) if val else ""
                            elif val is None:
                                display_val = ""
                            else:
                                display_val = str(val)
                            st.write(f"**{label}:** {display_val}")
                    else:
                        st.info("📄 Card di riferimento: (nome non trovato)")
                except Exception:
                    st.info("📄 Card di riferimento: (non disponibile)")

        with st.form("dataset_metadata_form"):
            name = st.text_input("Nome *", value=ds.get("name", ""))
            description = st.text_area("Descrizione", value=ds.get("description", "") or "")

            lic_options = self._get_license_options()
            idx_lic = lic_options.index(ds["license"]) if ds.get("license") in lic_options else 0
            selected_license = st.selectbox("Licenza *", lic_options, index=idx_lic)

            type_options = self._get_dataset_type_options()
            if type_options:
                idx_type = (
                    type_options.index(ds.get("dataset_type"))
                    if ds.get("dataset_type") in type_options
                    else 0
                )
            else:
                idx_type = 0
            selected_dataset_type = st.selectbox(
                "Tipo dataset", type_options if type_options else ["n/a"], index=idx_type
            )

            source = st.text_input("Source Link", value=ds.get("source", "") or "")
            version = st.text_input("Versione", value=ds.get("version", "1.0") or "1.0")

            if is_prefill_mode and ds.get("derived_dataset"):
                st.text_input(
                    "Derivato da (URI padre Step=2 — sola lettura)",
                    value=ds.get("derived_dataset", ""),
                    disabled=True,
                    key="derived_dataset_readonly"
                )

            submitted = st.form_submit_button("💾 SALVA E SINCRONIZZA", type="primary")

        return {
            "submitted": submitted,
            "name": name,
            "description": description,
            "license": selected_license,
            "dataset_type": selected_dataset_type,
            "source": source,
            "version": version,
            "step": 3,
            "derived_card": ds.get("derived_card"),
            "derived_dataset": ds.get("derived_dataset"),
        }

    def _save_dataset(self, form_data: Dict[str, Any], dataset_uri: str):
        current_languages = st.session_state.get("selected_languages", [])
        current_globs = st.session_state.get("globs", [])

        if not current_languages or not form_data["name"]:
            st.error("⚠️ Nome e Lingue sono obbligatori.")
            return

        try:
            existing_dataset = self.dataset_repo.get_by_uri(dataset_uri)

            if existing_dataset:
                existing_dataset.name = form_data["name"]
                existing_dataset.description = form_data["description"]
                existing_dataset.license = form_data["license"]
                existing_dataset.source = form_data["source"] or None
                existing_dataset.version = form_data["version"]
                existing_dataset.languages = current_languages
                existing_dataset.globs = current_globs
                existing_dataset.modified = datetime.now(timezone.utc)
                existing_dataset.step = 3
                existing_dataset.derived_card = form_data.get("derived_card")
                existing_dataset.derived_dataset = st.session_state.wizard_parent_dataset.id
                existing_dataset.dataset_type = form_data.get("dataset_type")

                if self.dataset_repo.update(existing_dataset) > 0:
                    st.success("✅ Dataset aggiornato!")
                    self._cleanup_wizard_state()
                    reset_dashboard_session_state(st, home_vars)
                    st.session_state.current_stage = "home"
                    st.rerun()
            else:
                new_ds = Dataset(
                    id=None,
                    uri=dataset_uri,
                    name=form_data["name"],
                    languages=current_languages,
                    globs=current_globs,
                    description=form_data["description"],
                    source=form_data["source"],
                    version=form_data["version"],
                    issued=datetime.now(timezone.utc),
                    modified=datetime.now(timezone.utc),
                    license=form_data["license"],
                    step=3,
                    derived_card=form_data.get("derived_card"),
                    derived_dataset=st.session_state.wizard_parent_dataset.id,
                    dataset_type=form_data.get("dataset_type")
                )
                if self.dataset_repo.insert(new_ds):
                    st.success("✅ Nuovo dataset creato!")
                    self._cleanup_wizard_state()
                    reset_dashboard_session_state(st, home_vars)
                    st.session_state.current_stage = "home"
                    st.rerun()
        except psycopg.errors.UniqueViolation as e:
            st.error(f"❌ Errore: Duplication error {e} torna indietro o cambia valore per censire dataset esistente")
            logger.error(f"UniqueViolation: {e}")
        except Exception as e:
            st.error(f"Errore: {e}")
            logger.error(traceback.format_exc())

    def _cleanup_wizard_state(self):
        for key in [
            "wizard_mode", "wizard_step", "wizard_parent_dataset",
            "prefill_from_parent", "prefill_parent_uri", "prefill_parent_data",
            "step2_datasets", "step2_dataset_labels",
        ]:
            st.session_state.pop(key, None)

    def show_form(self, st):
        is_prefill_mode = st.session_state.get("prefill_from_parent", False)
        prefill_data = self._build_prefill_from_parent() if is_prefill_mode else None

        # ── Strada A: dataset già censito ────────────────────────────────────
        if not is_prefill_mode:
            if st.session_state.get("selected_dataset_uri"):
                st.session_state["editing_uri"] = st.session_state.get("selected_dataset_uri")
                st.session_state["editing_path"] = st.session_state.get("selected_dataset_path")

            uri = st.session_state.get("editing_uri")
            path = st.session_state.get("editing_path")

            if not uri or not path:
                st.error("❌ Context Lost. Ritorna alla selezione.")
                if st.button("🔙 Selezione"):
                    st.session_state.current_stage = "dataset_selection"
                    st.rerun()
                return

            existing_dataset = self._get_existing_dataset(uri)
            dataset_path = to_internal_path(uri.replace(BASE_PREFIX, ""))

            if not self._initialize_session_state(existing_dataset, None, dataset_path):
                return

            self._render_language_section()
            form_result = self._render_metadata_form(existing_dataset, None, is_prefill_mode=False)

            if form_result["submitted"]:
                self._save_dataset(form_result, uri)

        # ── Strada B: nuovo dataset derivato da padre step=2 ─────────────────
        else:
            st.header("🆕 Censimento Dataset Derivato — Metadatazione")

            parent_uri = st.session_state.get("prefill_parent_uri", "")
            if parent_uri:
                st.markdown(f"**Dataset padre (Step=2):** `{parent_uri}`")

            # FIX BUG 1: se l'URI è già stato confermato in un rerun precedente, non mostrare più il navigatore
            already_confirmed_uri = st.session_state.get("editing_uri")
            already_confirmed_path = st.session_state.get("editing_path")

            if already_confirmed_uri and already_confirmed_path:
                # URI già confermato: mostra solo un riepilogo e permetti reset
                st.success(f"📍 **URI confermato:** `{already_confirmed_uri}`")
                if st.button("🔄 Cambia URI", key="change_uri_btn"):
                    st.session_state.pop("editing_uri", None)
                    st.session_state.pop("editing_path", None)
                    st.session_state.pop("globs", None)
                    st.session_state.pop("selected_languages", None)
                    st.session_state.new_ds_nav_parts = []
                    st.rerun()
                new_uri = already_confirmed_uri
                dataset_path = already_confirmed_path
            else:
                # URI non ancora confermato: mostra il navigatore
                new_uri = self._resolve_uri_for_new_dataset(st)

                if new_uri:
                    dataset_path = to_internal_path(new_uri.replace(BASE_PREFIX, ""))
                    st.session_state["editing_uri"] = new_uri
                    st.session_state["editing_path"] = dataset_path
                else:
                    st.warning("⚠️ Naviga e conferma il percorso del nuovo dataset per continuare.")
                    self._render_back_button(st)
                    return

            if not self._initialize_session_state(None, prefill_data, dataset_path):
                return

            self._render_language_section()
            form_result = self._render_metadata_form(None, prefill_data, is_prefill_mode=True)

            if form_result["submitted"]:
                self._save_dataset(form_result, new_uri)

    def _render_back_button(self, st):
        st.markdown("---")
        if st.button("← Torna al wizard", key="back_to_wizard_from_form"):
            st.session_state.current_stage = "dataset_selection"
            st.session_state.wizard_mode = "new_untracked"
            st.session_state.wizard_step = 2
            st.rerun()


def show_dataset_metadata_editing(st):
    """Funzione helper per mostrare l'interfaccia di editing metadati."""
    if "db_manager" not in st.session_state:
        st.error("Errore: DB Manager non disponibile nello stato di sessione.")
        return

    handler = DatasetMetadataEditingHandler(st.session_state.db_manager)
    handler.show_form(st)