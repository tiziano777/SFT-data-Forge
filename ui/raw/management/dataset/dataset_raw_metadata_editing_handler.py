import logging
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import json
import psycopg

import streamlit as st

from utils.extract_glob import generate_dataset_globs
from utils.streamlit_func import reset_dashboard_session_state
from config.state_vars import home_vars
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.dataset_card_repository import DatasetCardRepository
from data_class.repository.table.card_composition_repository import CardCompositionRepository
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from data_class.repository.vocabulary.vocab_task_repository import VocabTaskRepository
from data_class.repository.vocabulary.vocab_core_skill_repository import VocabCoreSkillRepository
from data_class.repository.vocabulary.vocab_modality_repository import VocabModalityRepository
from data_class.repository.vocabulary.vocab_license_repository import VocabLicenseRepository
from data_class.repository.vocabulary.vocab_field_repository import VocabFieldRepository
from data_class.repository.vocabulary.vocab_source_category_repository import VocabSourceCategoryRepository
from data_class.repository.vocabulary.vocab_source_type_repository import VocabSourceTypeRepository
from data_class.repository.vocabulary.vocab_vertical_repository import VocabVerticalRepository
from data_class.repository.vocabulary.vocab_content_repository import VocabContentRepository
from data_class.repository.vocabulary.skill_task_taxonomy_repository import SkillTaskTaxonomyRepository
from data_class.entity.table.dataset import Dataset
from data_class.entity.table.dataset_card import DatasetCard
from data_class.entity.table.card_composition import CardComposition

import os

POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

logger = logging.getLogger(__name__)

class DatasetMetadataEditingHandler:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.dataset_repo = DatasetRepository(db_manager)
        self.dataset_card_repo = DatasetCardRepository(db_manager)
        self.card_composition_repo = CardCompositionRepository(db_manager)
        self.language_repo = VocabLanguageRepository(db_manager)
        self.vocab_repos = self._initialize_vocabulary_repositories()
        self.available_languages = self._load_available_languages()

    def _initialize_vocabulary_repositories(self) -> Dict:
        """Inizializza tutti i repository dei vocabolari"""
        return {
            'task': VocabTaskRepository(self.db_manager),
            'language': VocabLanguageRepository(self.db_manager),
            'core_skill': VocabCoreSkillRepository(self.db_manager),
            'license': VocabLicenseRepository(self.db_manager),
            'modality': VocabModalityRepository(self.db_manager),
            'field': VocabFieldRepository(self.db_manager),
            'source_category': VocabSourceCategoryRepository(self.db_manager),
            'source_type': VocabSourceTypeRepository(self.db_manager),
            'vertical': VocabVerticalRepository(self.db_manager),
            'content': VocabContentRepository(self.db_manager),
            'skill_task_taxonomy': SkillTaskTaxonomyRepository(self.db_manager)
        }

    def _get_vocabulary_options(self, repo) -> Dict[str, str]:
        """Recupera opzioni da repository vocabolario"""
        if repo is None:
            return {}
        try:
            items = repo.get_all()
            return {item.code: item.description for item in items}
        except Exception as e:
            logger.error(f"Errore caricamento vocabolario: {e}")
            return {}

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
                    'step': dataset.step,
                    'dataset_type': getattr(dataset, 'dataset_type', None)
                }
            return None
        except Exception as e:
            logger.error(f"Errore nel recupero dataset esistente: {e}")
            return None

    def _initialize_session_state(self, existing_dataset: Optional[Dict[str, Any]]):
        dataset_path = st.session_state.get("editing_path")
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
            if existing_dataset and existing_dataset.get("languages"):
                st.session_state.selected_languages = list(existing_dataset["languages"])
            else:
                st.session_state.selected_languages = []
        return True

    # ==================== WIZARD DATASET CARD (5 STEP) ====================
    
    def _show_new_dataset_wizard(self, st, dataset_uri: str):
        """Wizard a 5 step: 3 per Dataset Card + 1 per scelta modalità lingue + 1 per Dataset"""
        step = st.session_state.get("new_dataset_wizard_step", 1)
        
        # If a dataset with same URI exists, prepare prefill data for the wizard
        if "prefill_dataset_checked" not in st.session_state:
            try:
                existing = self.dataset_repo.get_by_uri(dataset_uri)
                if existing:
                    # store useful prefill info
                    st.session_state.prefill_dataset = {
                        'id': existing.id,
                        'uri': existing.uri,
                        'name': existing.name,
                        'description': existing.description,
                        'license': existing.license,
                        'source': existing.source,
                        'version': existing.version,
                        'languages': existing.languages,
                        'globs': existing.globs,
                        'derived_card': existing.derived_card,
                        'derived_dataset': existing.derived_dataset,
                        'step': existing.step,
                        'dataset_type': getattr(existing, 'dataset_type', None)
                    }
                    # if dataset references a card, try to prefill card data
                    if existing.derived_card:
                        all_cards = self.dataset_card_repo.get_all()
                        for c in all_cards:
                            if getattr(c, 'id', None) == existing.derived_card:
                                st.session_state.prefill_card_data = {
                                    'dataset_name': c.dataset_name,
                                    'modality': c.modality,
                                    'dataset_description': c.dataset_description,
                                    'languages': getattr(c, 'languages', []) or [],
                                    'license': getattr(c, 'license', None),
                                    'quality': getattr(c, 'quality', 3),
                                    'notes': getattr(c, 'notes', None),
                                    'source_url': getattr(c, 'source_url', None),
                                    'download_url': getattr(c, 'download_url', None),
                                    'has_reasoning': getattr(c, 'has_reasoning', False),
                                    'fields': getattr(c, 'fields', []),
                                    'sources': getattr(c, 'sources', []),
                                    'source_type': getattr(c, 'source_type', None),
                                    'vertical': getattr(c, 'vertical', []),
                                    'contents': getattr(c, 'contents', [])
                                }
                                break
                else:
                    st.session_state.prefill_dataset = None
                    st.session_state.prefill_card_data = None
            except Exception:
                st.session_state.prefill_dataset = None
                st.session_state.prefill_card_data = None
            st.session_state.prefill_dataset_checked = True

        st.markdown("### 🆕 Nuovo Dataset - Creazione Card + Metadati")
        st.progress((step - 1) / 5, text=f"Step {step}/5")
        
        if step == 1:
            self._show_mix_selection_step(st)
        elif step == 2:
            self._show_task_skill_selection_step(st)
        elif step == 3:
            self._show_card_metadata_step(st)
        elif step == 4:
            self._show_language_mode_selection_step(st)
        elif step == 5:
            self._show_dataset_metadata_step(st, dataset_uri)

    def _show_mix_selection_step(self, st):
        """Step 1: Selezione componenti MIX"""
        st.markdown("#### 🧬 Step 1: Definizione MIX e dipendenze (Dataset Card)")
        
        st.markdown("Seleziona i dataset card ereditati.\n Definisci i pesi per ogni componente (la somma non deve necessariamente essere 1, verranno normalizzati).\n Se lasci vuoto, il dataset sarà considerato atomico (non-MIX).")
        
        all_cards = self.dataset_card_repo.get_all()
        card_names = sorted([c.dataset_name for c in all_cards])
        
        saved_components = st.session_state.get("wizard_mix_components", [])
        saved_weights = st.session_state.get("wizard_mix_weights", {})
        
        selected_components = st.multiselect(
            "Seleziona i Dataset componenti", 
            options=card_names, 
            default=saved_components,
            key="wizard_mix_select",
            help="Lascia vuoto per dataset atomico (non-MIX)"
        )
        
        component_weights = {}
        if selected_components:
            st.markdown("**Definisci i pesi per ogni componente:**")
            cols = st.columns(len(selected_components))
            for i, comp_name in enumerate(selected_components):
                with cols[i]:
                    default_w = saved_weights.get(comp_name, 0.0)
                    component_weights[comp_name] = st.number_input(
                        f"Peso {comp_name}", 0.0, 1.0, default_w, 0.01, 
                        key=f"wizard_w_{comp_name}"
                    )
            total_weight = sum(component_weights.values())
            if total_weight > 0:
                st.info(f"Peso totale: {total_weight:.2f}")
        
        all_datasets = self.dataset_repo.get_by_step(step=1)
        all_datasets_name = [d.name for d in all_datasets]

        selected_derived_dataset = st.selectbox(
            "Seleziona eventuale Dataset derivato (se il dataset che stai censendo è una derivazione di un dataset esistente)", 
            
            options=[None] + all_datasets_name, 
            key="wizard_derived_dataset_mix_select",
            help="Lascia vuoto per dataset atomico"
        )
        if selected_derived_dataset:
            derived_dataset_id = None
            for d in all_datasets:
                if d.name == selected_derived_dataset:
                    derived_dataset_id = d.id
        else:
            derived_dataset_id = None
        st.session_state.wizard_derived_dataset = derived_dataset_id

        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("Avanti →", type="primary", use_container_width=True):
                st.session_state.wizard_mix_components = selected_components
                st.session_state.wizard_mix_weights = component_weights
                st.session_state.new_dataset_wizard_step = 2
                st.rerun()
        with col2:
            if st.button("❌ Annulla", use_container_width=True):
                self._reset_wizard_state(st)
                st.session_state.current_stage = "home"
                reset_dashboard_session_state(st, home_vars)
                st.rerun()

    def _show_task_skill_selection_step(self, st):
        """Step 2: Selezione Tasks e Skills correlate"""
        st.markdown("#### 🎯 Step 2: Selezione Tasks e Skills (Dataset Card)")
        
        task_options = self._get_vocabulary_options(self.vocab_repos.get('task'))
        core_skill_options = self._get_vocabulary_options(self.vocab_repos.get('core_skill'))
        taxonomy_repo = self.vocab_repos.get('skill_task_taxonomy')
        
        saved_tasks = st.session_state.get("wizard_tasks", [])
        saved_skills = st.session_state.get("wizard_skills", [])
        
        selected_tasks = st.multiselect(
            "Seleziona Tasks *",
            options=list(task_options.keys()),
            default=saved_tasks,
            format_func=lambda x: f"{x} - {task_options.get(x, '')}",
            key="wizard_task_select",
            help="Seleziona almeno una task"
        )
        
        selected_skills = []
        if selected_tasks:
            suggested_skill_codes = taxonomy_repo.get_skills_by_tasks(selected_tasks)
            
            if suggested_skill_codes:
                st.markdown("---")
                st.markdown(f"**Skills associate alle Tasks selezionate:** {len(suggested_skill_codes)}")
            
                available_skills = list(set(suggested_skill_codes + saved_skills))
                
                selected_skills = st.multiselect(
                    "Seleziona Core Skills",
                    options=available_skills,
                    format_func=lambda x: f"{x} - {core_skill_options.get(x, 'N/A')}",
                    key="wizard_skill_select"
                )
                
                with st.expander("🔗 Visualizza relazioni Task-Skill"):
                    for task in selected_tasks:
                        task_skills = taxonomy_repo.get_skills_by_tasks([task])
                        if task_skills:
                            st.write(f"**{task}** → {', '.join(task_skills)}")
            else:
                st.warning("Nessuna skill correlata trovata. Seleziona manualmente:")
                selected_skills = st.multiselect(
                    "Seleziona Core Skills",
                    options=list(core_skill_options.keys()),
                    default=saved_skills,
                    format_func=lambda x: f"{x} - {core_skill_options.get(x, '')}",
                    key="wizard_skill_manual"
                )
        
        col1, col2, col3 = st.columns([1, 1, 3])
        with col1:
            if st.button("← Indietro", use_container_width=True):
                st.session_state.new_dataset_wizard_step = 1
                st.rerun()
        with col2:
            if st.button("Avanti →", type="primary", use_container_width=True):
                st.session_state.wizard_tasks = selected_tasks
                st.session_state.wizard_skills = selected_skills
                st.session_state.new_dataset_wizard_step = 3
                st.rerun()
        with col3:
            if st.button("❌ Annulla", use_container_width=True):
                self._reset_wizard_state(st)
                st.session_state.current_stage = "home"
                reset_dashboard_session_state(st,home_vars)
                st.rerun()
        
    def _show_card_metadata_step(self, st):
        """Step 3: Metadati Dataset Card"""
        st.markdown("#### 📝 Step 3: Metadati Dataset Card")
        
        modalities = self._get_vocabulary_options(self.vocab_repos.get('modality'))
        licenses = self._get_vocabulary_options(self.vocab_repos.get('license'))
        fields_options = self._get_vocabulary_options(self.vocab_repos.get('field'))
        source_cat_options = self._get_vocabulary_options(self.vocab_repos.get('source_category'))
        source_type_options = self._get_vocabulary_options(self.vocab_repos.get('source_type'))
        vertical_options = self._get_vocabulary_options(self.vocab_repos.get('vertical'))
        content_options = self._get_vocabulary_options(self.vocab_repos.get('content'))
        
        # Use prefill_card_data if available
        prefill = st.session_state.get('prefill_card_data', {}) or {}
        default_name = prefill.get('dataset_name', '')
        default_id = prefill.get('id', '')
        default_modality = prefill.get('modality', None)
        default_description = prefill.get('dataset_description', '')
        default_license = prefill.get('license', None)
        default_quality = prefill.get('quality', 3)
        default_notes = prefill.get('notes', '')
        default_source_url = prefill.get('source_url', '')
        default_download_url = prefill.get('download_url', '')
        default_fields = prefill.get('fields', [])
        default_sources = prefill.get('sources', [])
        default_source_type = prefill.get('source_type', None)
        default_verticals = prefill.get('vertical', [])
        default_contents = prefill.get('contents', [])
        
        with st.form("wizard_card_metadata_form"):
            col_n, col_q = st.columns([3, 1])
            with col_n:
                dataset_name = st.text_input("Nome Dataset Card *", value=default_name, help="Verrà convertito in snake_case")
                dataset_id = st.text_input("ID Dataset *", value=default_id, help="ID univoco per il dataset")
            with col_q:
                quality = st.slider("Qualità *", 1, 5, value=int(default_quality))
            
            dataset_description = st.text_area("Descrizione *", height=100, value=default_description)
            
            c1, c2, c3 = st.columns(3)
            with c1:
                selected_modality = st.selectbox("Modalità *", list(modalities.keys()), index=(list(modalities.keys()).index(default_modality) if default_modality in list(modalities.keys()) else 0), format_func=lambda x: f"{x} - {modalities.get(x, '')}")
            with c2:
                # Languages will be chosen in the next step (outside the form)
                st.info("Le lingue verranno definite nello step successivo (Step 4). Seleziona modalità di inserimento lingue nel passo successivo.")
            with c3:
                selected_license = st.selectbox("Licenza *", list(licenses.keys()), index=(list(licenses.keys()).index(default_license) if default_license in list(licenses.keys()) else 0), format_func=lambda x: f"{x} - {licenses.get(x, '')}")
            
            c4, c5 = st.columns(2)
            with c4:
                selected_fields = st.multiselect("Fields", list(fields_options.keys()), default=default_fields, format_func=lambda x: f"{x} - {fields_options.get(x, '')}")
                selected_verticals = st.multiselect("Vertical", list(vertical_options.keys()), default=default_verticals, format_func=lambda x: f"{x} - {vertical_options.get(x, '')}")
            with c5:
                selected_sources = st.multiselect("Source Categories", list(source_cat_options.keys()), default=default_sources, format_func=lambda x: f"{x} - {source_cat_options.get(x, '')}")
                selected_contents = st.multiselect("Contents", list(content_options.keys()), default=default_contents, format_func=lambda x: f"{x} - {content_options.get(x, '')}")
            
            selected_source_type = st.selectbox("Source Type", [None] + list(source_type_options.keys()), index=(0 if default_source_type is None else ([None] + list(source_type_options.keys())).index(default_source_type)), format_func=lambda x: (f"{x} - {source_type_options.get(x, '')}" if x else "Nessuno"))
            
            notes = st.text_area("Note", value=default_notes)
            source_url = st.text_input("Source URL", value=default_source_url)
            download_url = st.text_input("Download URL", value=default_download_url)
            has_reasoning = st.checkbox("Contiene elementi di reasoning", value=prefill.get('has_reasoning', False))
            
            col1, col2, col3 = st.columns([1, 1, 3])
            with col1:
                back_btn = st.form_submit_button("← Indietro", use_container_width=True)
            with col2:
                submit_btn = st.form_submit_button("Avanti →", type="primary", use_container_width=True)
            with col3:
                cancel_btn = st.form_submit_button("❌ Annulla", use_container_width=True)
            
            if back_btn:
                st.session_state.new_dataset_wizard_step = 2
                st.rerun()
            
            if cancel_btn:
                self._reset_wizard_state(st)
                st.session_state.current_stage = "home"
                reset_dashboard_session_state(st,home_vars)
                st.rerun()
            
            if submit_btn:
                clean_name = dataset_name.replace(" ", "_").strip()
                if not clean_name or not dataset_description:
                    st.error("⚠️ Nome e Descrizione sono obbligatori")
                    return
                
                # Salva dati card nello stato (lingue saranno gestite nello step successivo)
                st.session_state.wizard_card_data = {
                    'dataset_name': clean_name,
                    'dataset_id': dataset_id,
                    'modality': selected_modality,
                    'dataset_description': dataset_description,
                    'languages': prefill.get('languages', []),  # initial languages maybe from prefill
                    'license': selected_license,
                    'quality': quality,
                    'notes': notes,
                    'source_url': source_url,
                    'download_url': download_url,
                    'has_reasoning': has_reasoning,
                    'fields': selected_fields,
                    'sources': selected_sources,
                    'source_type': selected_source_type,
                    'vertical': selected_verticals,
                    'contents': selected_contents
                }
                st.session_state.new_dataset_wizard_step = 4
                st.rerun()

    def _show_language_mode_selection_step(self, st):
        """Step 4: selezione modalità inserimento lingue (enum / list)"""
        st.markdown("#### 🈷️ Step 4: Modalità inserimento Lingue")
        st.write("Scegli come vuoi inserire le lingue nel form successivo.")
        
        current = st.session_state.get("wizard_creation_lang_mode", "enum")
        mode = st.radio("Modalità", options=["enum", "list"], index=0 if current == "enum" else 1, key="wizard_creation_lang_mode_radio")
        st.session_state.wizard_creation_lang_mode = mode
        
        col1, col2, col3 = st.columns([1, 1, 3])
        with col1:
            if st.button("← Indietro", use_container_width=True):
                st.session_state.new_dataset_wizard_step = 3
                st.rerun()
        with col2:
            if st.button("Avanti →", type="primary", use_container_width=True):
                st.session_state.new_dataset_wizard_step = 5
                st.rerun()
        with col3:
            if st.button("❌ Annulla", use_container_width=True):
                self._reset_wizard_state(st)
                reset_dashboard_session_state(st,home_vars)
                st.session_state.current_stage = "home"
                st.rerun()

    def _show_dataset_metadata_step(self, st, dataset_uri: str):
        """Step 5: Metadati Dataset (auto-fill da Card)"""
        st.markdown("#### 📦 Step 5: Metadati Dataset Fisico")
        
        card_data = st.session_state.get("wizard_card_data", {})
        prefill_dataset = st.session_state.get('prefill_dataset', {}) or {}
        
        # Auto-fill dai dati della card o dal prefill dataset
        default_name = prefill_dataset.get('name', card_data.get('dataset_name', ''))
        default_description = prefill_dataset.get('description', card_data.get('dataset_description', ''))
        default_license = prefill_dataset.get('license', card_data.get('license', ''))
        default_source = prefill_dataset.get('source', card_data.get('source_url', ''))
        default_languages = prefill_dataset.get('languages', card_data.get('languages', []))
        
        # Inizializza lingue se non presenti
        if "selected_languages" not in st.session_state:
            st.session_state.selected_languages = list(default_languages) if default_languages else []

        # Sezione Lingue: rispetta la modalità scelta nello step precedente
        st.subheader("🌐 Lingue del dataset")
        mode = st.session_state.get('wizard_creation_lang_mode', 'enum')
        if mode == 'enum':
            # show available languages multiselect
            available_langs = [l for l in self.available_languages]
            selected = st.multiselect("Lingue *", options=available_langs, default=st.session_state.get('selected_languages', []), key='wizard_final_languages')
            st.session_state.selected_languages = selected
        else:
            lang_list_text = st.text_input("Lingue (lista JSON)", value=json.dumps(st.session_state.get('selected_languages', [])), key='wizard_final_languages_json')
            if lang_list_text:
                try:
                    parsed = json.loads(lang_list_text)
                    if isinstance(parsed, list) and all(isinstance(x, str) for x in parsed):
                        st.session_state.selected_languages = parsed
                        st.success(f"✅ {len(parsed)} lingue riconosciute: {', '.join(parsed)}")
                    else:
                        st.error("Formato non valido: inserisci un array JSON di stringhe.")
                except Exception:
                    st.error('JSON non valido. Usa doppi apici, es: ["it","en"]')

        # Sezione Form metadati dataset
        st.subheader("📋 Metadati Dataset")
        with st.expander("🔍 Info Tecnica", expanded=False):
            st.code(f"URI: {dataset_uri}")
            st.code(f"Globs: {st.session_state.get('globs')}")
            if prefill_dataset.get("derived_dataset"):
                st.info(f"🔗 Derivato da: {prefill_dataset.get('derived_dataset')}")
            if prefill_dataset.get("derived_card"):
                try:
                    # Risolvi il nome della card tramite il repository e mostra un overview dei metadati
                    derived_card_id = prefill_dataset.get("derived_card")
                    card_entity = None
                    all_cards = self.dataset_card_repo.get_all()
                    for c in all_cards:
                        if getattr(c, 'id', None) == derived_card_id:
                            card_entity = c
                            break

                    if card_entity:
                        st.markdown("**📄 Card di riferimento (overview):**")
                        fields_to_show = [
                            ("dataset_name", "Nome"),
                            ("modality", "Modalità"),
                            ("dataset_description", "Descrizione"),
                            ("publisher", "Publisher"),
                            ("notes", "Note"),
                            ("source_url", "Source URL"),
                            ("download_url", "Download URL"),
                            ("languages", "Lingue"),
                            ("core_skills", "Core Skills"),
                            ("tasks", "Tasks"),
                            ("license", "Licenza"),
                            ("has_reasoning", "Has Reasoning"),
                            ("quality", "Qualità"),
                            ("sources", "Sources"),
                            ("source_type", "Source Type"),
                            ("fields", "Fields"),
                            ("vertical", "Vertical"),
                            ("contents", "Contents"),
                            ("last_update", "Last Update"),
                            ("created_at", "Created At")
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

        with st.form("wizard_dataset_metadata_form"):
            name = st.text_input("Nome Dataset *", value=default_name, help="Nome fisico del dataset")
            description = st.text_area("Descrizione", value=default_description)
            
            lic_options = self._get_license_options()
            idx_lic = lic_options.index(default_license) if default_license in lic_options else 0
            selected_license = st.selectbox("Licenza *", lic_options, index=idx_lic)
            
            type_options = self._get_dataset_type_options()
            selected_dataset_type = st.selectbox("Tipo dataset", type_options if type_options else ["un"], index=0)
            
            source = st.text_input("Source Link", value=default_source)
            version = st.text_input("Versione", value="1.0")
            
            col1, col2, col3 = st.columns([1, 1, 3])
            with col1:
                back_btn = st.form_submit_button("← Indietro", use_container_width=True)
            with col2:
                submit_btn = st.form_submit_button("🚀 CREA CARD + DATASET", type="primary", use_container_width=True)
            with col3:
                cancel_btn = st.form_submit_button("❌ Annulla", use_container_width=True)
            
            if back_btn:
                st.session_state.new_dataset_wizard_step = 4
                st.rerun()
            
            if cancel_btn:
                self._reset_wizard_state(st)
                reset_dashboard_session_state(st,home_vars)
                st.session_state.current_stage = "home"
                st.rerun()
            
            if submit_btn:
                if not name or not st.session_state.get('selected_languages'):
                    st.error("⚠️ Nome e Lingue sono obbligatori")
                    return
                
                try:
                    # 1. Crea Dataset Card
                    card_data = st.session_state.wizard_card_data
                    mix_weights = st.session_state.get("wizard_mix_weights", {})
                    tasks = st.session_state.get("wizard_tasks", [])
                    skills = st.session_state.get("wizard_skills", [])
                    
                    new_card = DatasetCard(
                        id=None,
                        dataset_name=card_data['dataset_name'],
                        dataset_id=card_data['dataset_id'],
                        modality=card_data['modality'],
                        dataset_description=card_data['dataset_description'],
                        notes=card_data.get('notes'),
                        source_url=card_data.get('source_url'),
                        download_url=card_data.get('download_url'),
                        languages=st.session_state.get('selected_languages', []),
                        core_skills=skills,
                        tasks=tasks,
                        license=card_data['license'],
                        has_reasoning=card_data.get('has_reasoning', False),
                        quality=card_data.get('quality', 3),
                        sources=card_data.get('sources', []),
                        source_type=card_data.get('source_type'),
                        fields=card_data.get('fields', []),
                        vertical=card_data.get('vertical', []),
                        contents=card_data.get('contents', []),
                        last_update=datetime.now(timezone.utc),
                        created_at=datetime.now(timezone.utc)
                    )
                    
                    created_card = self.dataset_card_repo.insert(new_card)
                    if not created_card:
                        st.error("❌ Errore creazione Dataset Card")
                        return
                    
                    # 2. Crea Composizione MIX
                    for comp_name, weight in mix_weights.items():
                        self.card_composition_repo.insert(
                            CardComposition(
                                parent_card_name=created_card.dataset_name,
                                child_card_name=comp_name,
                                weight=weight
                            )
                        )
                    
                    # 3. Crea Dataset fisico
                    new_dataset = Dataset(
                        id=None,
                        uri=dataset_uri,
                        name=name,
                        languages=st.session_state.get('selected_languages', []),
                        globs=st.session_state.get('globs', []),
                        description=description,
                        source=source,
                        version=version,
                        issued=datetime.now(timezone.utc),
                        modified=datetime.now(timezone.utc),
                        license=selected_license,
                        step=1,
                        derived_card=created_card.id,  # Link alla card appena creata
                        derived_dataset=st.session_state.wizard_derived_dataset,
                        dataset_type=selected_dataset_type
                    )
                    
                    if self.dataset_repo.insert(new_dataset):
                        st.success(f"✅ Dataset Card '{created_card.dataset_name}' e Dataset '{name}' creati con successo!")
                        self._reset_wizard_state(st)
                        reset_dashboard_session_state(st, home_vars)
                        st.session_state.current_stage = "home"
                        st.rerun()
                    else:
                        st.error("❌ Errore creazione Dataset")
                        
                except Exception as e:
                    st.error(f"Errore durante la creazione: {e}")
                    logger.error(traceback.format_exc())

    def _reset_wizard_state(self, st):
        """Resetta lo stato del wizard"""
        keys_to_remove = [
            'new_dataset_wizard_step',
            'wizard_mix_components',
            'wizard_mix_weights',
            'wizard_tasks',
            'wizard_skills',
            'wizard_card_data'
        ]
        for key in keys_to_remove:
            if key in st.session_state:
                del st.session_state[key]

    # ==================== FORM DATASET ESISTENTE ====================

    def _render_language_section(self):
        st.subheader("🌐 Lingue del dataset")
        current_languages = st.session_state.get('selected_languages', [])
        
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

    def _render_metadata_form(self, existing_dataset: Optional[Dict[str, Any]]):
        st.subheader("📋 Metadati")
        ds = existing_dataset if existing_dataset else {}
        
        with st.expander("🔍 Info Tecnica", expanded=False):
            st.code(f"URI: {st.session_state.get('editing_uri')}")
            st.code(f"Globs: {st.session_state.get('globs', [])}")
            if ds.get("derived_dataset"):
                st.info(f"🔗 Derivato da: {ds.get('derived_dataset')}")
            if ds.get("derived_card"):
                try:
                    # Risolvi l'entità della card tramite repository e mostra i suoi campi in formato chiave: valore
                    derived_card_id = ds.get("derived_card")
                    card_entity = None
                    all_cards = self.dataset_card_repo.get_all()
                    for c in all_cards:
                        if getattr(c, 'id', None) == derived_card_id:
                            card_entity = c
                            break

                    if card_entity:
                        st.markdown("**📄 Card di riferimento (overview):**")
                        # Mostriamo coppie chiave: valore in modo leggibile
                        fields_to_show = [
                            ("dataset_name", "Nome"),
                            ("modality", "Modalità"),
                            ("dataset_description", "Descrizione"),
                            ("publisher", "Publisher"),
                            ("notes", "Note"),
                            ("source_url", "Source URL"),
                            ("download_url", "Download URL"),
                            ("languages", "Lingue"),
                            ("core_skills", "Core Skills"),
                            ("tasks", "Tasks"),
                            ("license", "Licenza"),
                            ("has_reasoning", "Has Reasoning"),
                            ("quality", "Qualità"),
                            ("sources", "Sources"),
                            ("source_type", "Source Type"),
                            ("fields", "Fields"),
                            ("vertical", "Vertical"),
                            ("contents", "Contents"),
                            ("last_update", "Last Update"),
                            ("created_at", "Created At")
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
            description = st.text_area("Descrizione", value=ds.get("description", ""))
            
            lic_options = self._get_license_options()
            idx_lic = lic_options.index(ds["license"]) if ds.get("license") in lic_options else 0
            selected_license = st.selectbox("Licenza *", lic_options, index=idx_lic)

            type_options = self._get_dataset_type_options()
            if type_options:
                idx_type = type_options.index(ds["dataset_type"]) if ds.get("dataset_type") in type_options else 0
            else:
                idx_type = 0
            selected_dataset_type = st.selectbox("Tipo dataset", type_options if type_options else ["un"], index=idx_type)
            
            source = st.text_input("Source Link", value=ds.get("source", ""))
            version = st.text_input("Versione", value=ds.get("version", "1.0"))

            submitted = st.form_submit_button("💾 SALVA E SINCRONIZZA", type="primary")

        return {
            "submitted": submitted,
            "name": name,
            "description": description,
            "license": selected_license,
            "dataset_type": selected_dataset_type,
            "source": source,
            "version": version,
            "step": 1,
            "derived_card": ds.get("derived_card"),
            "derived_dataset": ds.get("derived_dataset")
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
                # UPDATE
                existing_dataset.name = form_data["name"]
                existing_dataset.description = form_data["description"]
                existing_dataset.license = form_data["license"]
                existing_dataset.source = form_data["source"] or None
                existing_dataset.version = form_data["version"]
                existing_dataset.languages = current_languages
                existing_dataset.globs = current_globs
                existing_dataset.modified = datetime.now(timezone.utc)
                existing_dataset.step = form_data.get("step", 1)
                existing_dataset.derived_card = form_data.get("derived_card")
                existing_dataset.derived_dataset = form_data.get("derived_dataset")
                existing_dataset.dataset_type = form_data.get("dataset_type")

                if self.dataset_repo.update(existing_dataset) > 0:
                    st.success("✅ Dataset aggiornato!")
                    reset_dashboard_session_state(st, home_vars)
                    st.session_state.current_stage = "home"
                    reset_dashboard_session_state(st, home_vars)
                    st.rerun()
            else:
                # Non dovremmo mai arrivare qui (gestito dal wizard)
                st.error("❌ Dataset non trovato. Usa il wizard per creare nuovi dataset.")
        except psycopg.errors.UniqueViolation as e:
            st.error(f"❌ Errore: Duplication error {e} torna indietro o cambia valore per censire dataset esistente")
            logger.error(f"UniqueViolation: {e}")
        except Exception as e:
            st.error(f"Errore: {e}")
            logger.error(traceback.format_exc())

    def show_form(self, st):
        # Persistenza dati navigazione
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

        # Verifica se dataset esiste
        existing_dataset = self._get_existing_dataset(uri)
        
        if existing_dataset:
            # DATASET ESISTENTE → Form semplice
            if not self._initialize_session_state(existing_dataset):
                return

            st.markdown(f"### ✏️ Modifica Dataset: `{existing_dataset['name']}`")
            self._render_language_section()
            form_result = self._render_metadata_form(existing_dataset)

            if form_result["submitted"]:
                self._save_dataset(form_result, uri)
        else:
            # DATASET NUOVO → Wizard 4 step
            if not self._initialize_session_state(None):
                return
            
            # Inizializza wizard se non presente
            if "new_dataset_wizard_step" not in st.session_state:
                st.session_state.new_dataset_wizard_step = 1
            
            self._show_new_dataset_wizard(st, uri)

def show_dataset_metadata_editing(st):
    """Funzione helper per mostrare l'interfaccia di editing metadati."""
    if "db_manager" not in st.session_state:
        st.error("Errore: DB Manager non disponibile nello stato di sessione.")
        return
         
    handler = DatasetMetadataEditingHandler(st.session_state.db_manager)
    handler.show_form(st)