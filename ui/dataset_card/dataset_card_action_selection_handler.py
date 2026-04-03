import streamlit as st
from typing import List, Optional, Dict
from datetime import datetime, timezone
import json

from config.state_vars import home_vars
from utils.streamlit_func import reset_dashboard_session_state

from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.entity.table.dataset_card import DatasetCard
from data_class.entity.table.card_composition import CardComposition
from data_class.repository.table.dataset_card_repository import DatasetCardRepository
from data_class.repository.vocabulary.vocab_task_repository import VocabTaskRepository
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from data_class.repository.vocabulary.vocab_core_skill_repository import VocabCoreSkillRepository
from data_class.repository.vocabulary.vocab_modality_repository import VocabModalityRepository
from data_class.repository.vocabulary.vocab_license_repository import VocabLicenseRepository
from data_class.repository.table.card_composition_repository import CardCompositionRepository
from data_class.repository.vocabulary.vocab_field_repository import VocabFieldRepository
from data_class.repository.vocabulary.vocab_source_category_repository import VocabSourceCategoryRepository
from data_class.repository.vocabulary.vocab_source_type_repository import VocabSourceTypeRepository
from data_class.repository.vocabulary.vocab_vertical_repository import VocabVerticalRepository
from data_class.repository.vocabulary.vocab_content_repository import VocabContentRepository
from data_class.repository.vocabulary.skill_task_taxonomy_repository import SkillTaskTaxonomyRepository

import os
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

def show_dataset_card_action_selection(st):
    """Handler principale per gestire azioni dataset card"""
    st.markdown("## 📋 Dataset Card Management")
    st.markdown("---")
    
    dataset_card_repo = DatasetCardRepository(st.session_state.db_manager)
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔍 Cerca Dataset Card", use_container_width=True):
            st.session_state.dataset_card_action = "search_dataset_card"
            st.rerun()
    with col2:
        if st.button("➕ Crea Nuova Dataset Card", use_container_width=True):
            st.session_state.dataset_card_action = "create_dataset_card"
            # Inizializza step di creazione
            st.session_state.creation_step = 1
            st.session_state.mix_data = {}
            st.session_state.task_skill_data = {}
            st.session_state.creation_lang_input_mode = "enum"  # new default mode for language input
            st.rerun()
    
    action = st.session_state.get("dataset_card_action")
    
    if action == "search_dataset_card":
        show_dataset_card_search(st, dataset_card_repo)
    elif action == "create_dataset_card":
        show_dataset_card_creation_wizard(st, dataset_card_repo)
    elif action == "edit_dataset_card":
        card_to_edit = st.session_state.get("editing_card")
        if card_to_edit:
            show_dataset_card_creation_form(st, dataset_card_repo, edit_mode=True, card_entity=card_to_edit)
    
    if st.button("← Torna alla Home"):
        st.session_state.current_stage = "home"
        reset_dashboard_session_state(st, home_vars)
        st.rerun()

def initialize_vocabulary_repositories(st) -> Dict:
    """Inizializza tutti i repository dei vocabolari"""
    db_manager = st.session_state.db_manager
    return {
        'task': VocabTaskRepository(db_manager),
        'language': VocabLanguageRepository(db_manager),
        'core_skill': VocabCoreSkillRepository(db_manager),
        'license': VocabLicenseRepository(db_manager),
        'modality': VocabModalityRepository(db_manager),
        'field': VocabFieldRepository(db_manager),
        'source_category': VocabSourceCategoryRepository(db_manager),
        'source_type': VocabSourceTypeRepository(db_manager),
        'vertical': VocabVerticalRepository(db_manager),
        'content': VocabContentRepository(db_manager),
        'skill_task_taxonomy': SkillTaskTaxonomyRepository(db_manager)
    }

def get_vocabulary_options_from_repo(repo) -> Dict[str, str]:
    """Recupera le opzioni da un repository di vocabolario"""
    if repo is None:
        return {}
    
    try:
        items = repo.get_all()
        return {item.code: item.description for item in items}
    except Exception as e:
        st.error(f"Errore nel caricamento del vocabolario: {str(e)}")
        return {}

def show_dataset_card_creation_wizard(st, dataset_card_repo: DatasetCardRepository):
    """Wizard multi-step per creazione dataset card"""
    step = st.session_state.get("creation_step", 1)
    
    st.markdown("### ➕ Crea Nuova Dataset Card")
    st.progress((step - 1) / 4, text=f"Step {step}/4")
    
    if step == 1:
        show_mix_selection_step(st, dataset_card_repo)
    elif step == 2:
        show_task_skill_selection_step(st)
    elif step == 3:
        show_language_mode_selection_step(st)
    elif step == 4:
        show_metadata_form_step(st, dataset_card_repo)

def show_mix_selection_step(st, dataset_card_repo: DatasetCardRepository):
    """Step 1: Selezione componenti MIX"""
    st.markdown("#### 🧬 Step 1: Definizione MIX")
    
    comp_repo = CardCompositionRepository(st.session_state.db_manager)
    all_cards = dataset_card_repo.get_all()
    card_names = sorted([c.dataset_name for c in all_cards])
    
    # Recupera dati salvati se esistono
    saved_components = st.session_state.mix_data.get("selected_components", [])
    saved_weights = st.session_state.mix_data.get("component_weights", {})
    
    selected_components = st.multiselect(
        "Seleziona i Dataset componenti", 
        options=card_names, 
        default=saved_components,
        key="mix_components_selection",
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
                    key=f"w_{comp_name}"
                )
        
        total_weight = sum(component_weights.values())
        if total_weight > 0:
            st.info(f"Peso totale: {total_weight:.2f}")
    
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Avanti →", type="primary", use_container_width=True):
            st.session_state.mix_data = {
                "selected_components": selected_components,
                "component_weights": component_weights
            }
            st.session_state.creation_step = 2
            st.rerun()
    with col2:
        if st.button("❌ Annulla", use_container_width=True):
            st.session_state.dataset_card_action = None
            st.rerun()

def show_task_skill_selection_step(st):
    """Step 2: Selezione Tasks e Skills correlate"""
    st.markdown("#### 🎯 Step 2: Selezione Tasks e Skills")
    
    vocab_repos = initialize_vocabulary_repositories(st)
    task_options = get_vocabulary_options_from_repo(vocab_repos.get('task'))
    core_skill_options = get_vocabulary_options_from_repo(vocab_repos.get('core_skill'))
    taxonomy_repo = vocab_repos.get('skill_task_taxonomy')
    
    # Recupera dati salvati
    saved_tasks = st.session_state.task_skill_data.get("selected_tasks", [])
    saved_skills = st.session_state.task_skill_data.get("selected_skills", [])
    
    # Selezione Tasks
    selected_tasks = st.multiselect(
        "Seleziona Tasks *",
        options=list(task_options.keys()),
        default=saved_tasks,
        format_func=lambda x: f"{x} - {task_options.get(x, '')}",
        key="task_selection",
        help="Seleziona almeno una task"
    )
    
    # Mostra skills correlate solo se ci sono tasks selezionate
    selected_skills = []
    if selected_tasks:
        st.markdown("---")
        st.markdown("**Skills associate alle Tasks selezionate:**")
        
        # Recupera skills suggerite dalla taxonomy
        suggested_skill_codes = taxonomy_repo.get_skills_by_tasks(selected_tasks)
        
        if suggested_skill_codes:
            st.info(f"Trovate {len(suggested_skill_codes)} skills correlate alle tasks selezionate")
            
            # Filtra le options per mostrare solo skills suggerite + quelle già selezionate
            available_skills = list(set(suggested_skill_codes + saved_skills))
            
            selected_skills = st.multiselect(
                "Seleziona Core Skills",
                options=available_skills,
                format_func=lambda x: f"{x} - {core_skill_options.get(x, 'N/A')}",
                key="skill_selection",
                help="Skills pre-selezionate in base alle tasks"
            )
            
            # Mostra mapping task->skills
            with st.expander("🔗 Visualizza relazioni Task-Skill"):
                for task in selected_tasks:
                    task_skills = taxonomy_repo.get_skills_by_tasks([task])
                    if task_skills:
                        st.write(f"**{task}** → {', '.join(task_skills)}")
        else:
            st.warning("Nessuna skill correlata trovata nella taxonomy. Seleziona manualmente:")
            selected_skills = st.multiselect(
                "Seleziona Core Skills",
                options=list(core_skill_options.keys()),
                default=saved_skills,
                format_func=lambda x: f"{x} - {core_skill_options.get(x, '')}",
                key="skill_selection_manual"
            )
    
    # Navigazione
    col1, col2, col3 = st.columns([1, 1, 3])
    with col1:
        if st.button("← Indietro", use_container_width=True):
            st.session_state.creation_step = 1
            st.rerun()
    with col2:
        if st.button("Avanti →", type="primary", use_container_width=True):
            st.session_state.task_skill_data = {
                "selected_tasks": selected_tasks,
                "selected_skills": selected_skills
            }
            st.session_state.creation_step = 3
            st.rerun()
    with col3:
        if st.button("❌ Annulla", use_container_width=True):
            st.session_state.dataset_card_action = None
            st.rerun()

def show_metadata_form_step(st, dataset_card_repo: DatasetCardRepository):
    """Step 4: Form metadati completi"""
    st.markdown("#### 📝 Step 4: Metadati Dataset")
    
    vocab_repos = initialize_vocabulary_repositories(st)
    comp_repo = CardCompositionRepository(st.session_state.db_manager)
    
    # Carica vocabolari
    modalities = get_vocabulary_options_from_repo(vocab_repos.get('modality'))
    languages = get_vocabulary_options_from_repo(vocab_repos.get('language'))
    licenses = get_vocabulary_options_from_repo(vocab_repos.get('license'))
    fields_options = get_vocabulary_options_from_repo(vocab_repos.get('field'))
    source_cat_options = get_vocabulary_options_from_repo(vocab_repos.get('source_category'))
    source_type_options = get_vocabulary_options_from_repo(vocab_repos.get('source_type'))
    vertical_options = get_vocabulary_options_from_repo(vocab_repos.get('vertical'))
    content_options = get_vocabulary_options_from_repo(vocab_repos.get('content'))
    
    with st.form("metadata_form", clear_on_submit=False):
        col_n, col_q = st.columns([3, 1])
        with col_n:
            dataset_id = st.text_input(
                "ID Dataset *",
                help="Identificativo univoco del dataset"
            )
            dataset_name = st.text_input(
                "Nome Dataset *", 
                help="Verrà convertito in snake_case"
            )
        with col_q:
            quality = st.slider("Qualità *", 1, 5, 3)
        
        dataset_description = st.text_area("Descrizione *", height=100)
        
        c1, c2, c3 = st.columns(3)
        with c1:
            selected_modality = st.selectbox(
                "Modalità *", list(modalities.keys()),
                format_func=lambda x: f"{x} - {modalities.get(x, '')}"
            )
        with c2:
            # La modalità scelta viene dalla fase precedente (creation_lang_input_mode)
            lang_input_mode = st.session_state.get("creation_lang_input_mode", "enum")
            selected_languages = []

            if lang_input_mode == "enum":
                selected_languages = st.multiselect(
                    "Lingue *",
                    options=list(languages.keys()),
                    key="languages_multiselect",
                )
            else:
                lang_list_text = st.text_input(
                    "Lingue (lista JSON)",
                    placeholder='es. ["it", "en", "de"]',
                    key="languages_list_input",
                )
                if lang_list_text:
                    try:
                        parsed = json.loads(lang_list_text)
                        if isinstance(parsed, list) and all(isinstance(x, str) for x in parsed):
                            selected_languages = parsed
                            st.success(f"✅ {len(parsed)} lingue riconosciute: {', '.join(parsed)}")
                        else:
                            st.error("Formato non valido: inserisci un array JSON di stringhe.")
                    except Exception:
                        st.error('JSON non valido. Usa doppi apici, es: ["it","en"]')
        with c3:
            selected_license = st.selectbox(
                "Licenza *", list(licenses.keys()),
                format_func=lambda x: f"{x} - {licenses.get(x, '')}"
            )
        
        c4, c5 = st.columns(2)
        with c4:
            selected_fields = st.multiselect(
                "Fields",
                options=list(fields_options.keys()),
                format_func=lambda x: f"{x} - {fields_options.get(x, '')}"
            )
            selected_verticals = st.multiselect(
                "Vertical",
                options=list(vertical_options.keys()),
                format_func=lambda x: f"{x} - {vertical_options.get(x, '')}"
            )
        with c5:
            selected_sources = st.multiselect(
                "Source Categories",
                options=list(source_cat_options.keys()),
                format_func=lambda x: f"{x} - {source_cat_options.get(x, '')}"
            )
            selected_contents = st.multiselect(
                "Contents",
                options=list(content_options.keys()),
                format_func=lambda x: f"{x} - {content_options.get(x, '')}"
            )
        
        selected_source_type = st.selectbox(
            "Source Type",
            options=[None] + list(source_type_options.keys()),
            format_func=lambda x: (f"{x} - {source_type_options.get(x, '')}" if x else "Nessuno")
        )
        
        publisher = st.text_input("Publisher")
        notes = st.text_area("Note")
        source_url = st.text_input("Source URL")
        download_url = st.text_input("Download URL")
        has_reasoning = st.checkbox("Contiene elementi di reasoning")
        
        col1, col2, col3 = st.columns([1, 1, 3])
        with col1:
            back_btn = st.form_submit_button("← Indietro", use_container_width=True)
        with col2:
            submit_btn = st.form_submit_button("🚀 Crea Dataset Card", type="primary", use_container_width=True)
        with col3:
            cancel_btn = st.form_submit_button("❌ Annulla", use_container_width=True)
        
        if back_btn:
            # torna al passo di scelta modalità lingue
            st.session_state.creation_step = 3
            st.rerun()
        
        if cancel_btn:
            st.session_state.dataset_card_action = None
            st.rerun()
        
        if submit_btn:
            clean_name = dataset_name.replace(" ", "_").strip()
            err = validate_dataset_card_input(clean_name, dataset_id, dataset_description, selected_languages)
            if err:
                st.error(err)
                return
            
            try:
                # Recupera dati dagli step precedenti
                mix_data = st.session_state.get("mix_data", {})
                task_skill_data = st.session_state.get("task_skill_data", {})
                
                create_dataset_card_with_composition(
                    st, dataset_card_repo, comp_repo,
                    dataset_name=clean_name,
                    dataset_id=dataset_id,
                    modality=selected_modality,
                    dataset_description=dataset_description,
                    languages=selected_languages,
                    tasks=task_skill_data.get("selected_tasks", []),
                    core_skills=task_skill_data.get("selected_skills", []),
                    license=selected_license,
                    publisher=publisher,
                    notes=notes,
                    source_url=source_url,
                    download_url=download_url,
                    has_reasoning=has_reasoning,
                    components=mix_data.get("component_weights", {}),
                    quality=quality,
                    fields=selected_fields,
                    sources=selected_sources,
                    source_type=selected_source_type,
                    vertical=selected_verticals,
                    contents=selected_contents
                )
            except Exception as e:
                st.error(f"Errore durante la creazione: {e}")

def validate_dataset_card_input(
    dataset_name: str, 
    dataset_id: str,
    dataset_description: str, 
    languages: List[str]
) -> Optional[str]:
    """Valida gli input obbligatori"""
    if not dataset_name or not dataset_id:
        return "Il nome e ID del dataset sono obbligatori"
    if not dataset_description:
        return "La descrizione del dataset è obbligatoria"
    if not languages:
        return "Seleziona almeno una lingua"
    return None

def show_download_section(st, card: DatasetCard):
    """Mostra la sezione di download per una dataset card"""
    dataset_repo = DatasetRepository(st.session_state.db_manager)
    
    physical_dataset = None
    try:
        physical_dataset = dataset_repo.get_by_derived_card(card.id)
    except Exception as e:
        st.error(f"Errore controllo materializzazione: {e}")
    
    is_downloaded = physical_dataset is not None
    
    if is_downloaded:
        st.success(f"✅ Dataset fisico materializzato: `{physical_dataset.name}` (ID: {physical_dataset.id})")
        with st.expander("Visualizza dettagli dataset fisico"):
            st.json(physical_dataset.to_dict())
    
    download_url = card.download_url or card.source_url
    if download_url:
        col_dl1, col_dl2 = st.columns([1, 3])
        with col_dl1:
            if st.button("📥 Download Dataset", type="primary", key=f"download_{card.id}"):
                st.session_state.selected_dataset_card = card
                st.session_state.download_url = download_url
                st.session_state.current_stage = "download"
                st.rerun()
        with col_dl2:
            st.info("Dataset non ancora materializzato. Clicca per scaricarlo.")
    else:
        st.warning("⚠️ Nessun URL di download disponibile.")

def create_dataset_card_with_composition(st, repo: DatasetCardRepository, comp_repo: CardCompositionRepository, **form_data):
    """Persistenza dataset card con composizione"""
    try:
        dataset_card = DatasetCard(
            id=None,
            dataset_id= form_data['dataset_id'],
            dataset_name=form_data['dataset_name'],
            modality=form_data['modality'],
            dataset_description=form_data['dataset_description'],
            publisher=form_data.get('publisher'),
            notes=form_data.get('notes'),
            source_url=form_data.get('source_url'),
            download_url=form_data.get('download_url'),
            languages=form_data['languages'],
            core_skills=form_data.get('core_skills', []),
            tasks=form_data.get('tasks', []),
            license=form_data['license'],
            has_reasoning=form_data.get('has_reasoning', False),
            quality=form_data.get('quality', 3),
            sources=form_data.get('sources', []),
            source_type=form_data.get('source_type', None),
            fields=form_data.get('fields', []),
            vertical=form_data.get('vertical', []),
            contents=form_data.get('contents', []),
            last_update=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc)
        )
        new_card = repo.insert(dataset_card)
        if new_card:
            components = form_data.get('components', {})
            for comp_name, weight in components.items():
                comp_repo.insert(CardComposition(parent_card_name=new_card.dataset_name, child_card_name=comp_name, weight=weight))
            st.success(f"Dataset '{new_card.dataset_name}' creato con successo!")
            st.session_state.dataset_card_action = None
            st.rerun()
    except Exception as e:
        st.error(f"Errore: {e}")

def show_dataset_card_search(st, repo: DatasetCardRepository):
    """Interfaccia per la ricerca di dataset card"""
    st.markdown("### 🔍 Cerca Dataset Card")
    
    if "quality_sort_state" not in st.session_state:
        st.session_state.quality_sort_state = "disabled"
    if "materialization_filter_state" not in st.session_state:
        st.session_state.materialization_filter_state = "disabled"

    vocab_repos = initialize_vocabulary_repositories(st)
    lang_options = get_vocabulary_options_from_repo(vocab_repos.get('language'))
    task_options = get_vocabulary_options_from_repo(vocab_repos.get('task'))
    core_options = get_vocabulary_options_from_repo(vocab_repos.get('core_skill'))
    license_options = get_vocabulary_options_from_repo(vocab_repos.get('license'))
    field_options = get_vocabulary_options_from_repo(vocab_repos.get('field'))
    source_cat_options = get_vocabulary_options_from_repo(vocab_repos.get('source_category'))
    source_type_options = get_vocabulary_options_from_repo(vocab_repos.get('source_type'))
    vertical_options = get_vocabulary_options_from_repo(vocab_repos.get('vertical'))
    content_options = get_vocabulary_options_from_repo(vocab_repos.get('content'))

    col_search, col_sort, col_mat = st.columns([4, 1, 1])
    
    with col_search:
        search_term = st.text_input(
            "Cerca per nome o componente:",
            placeholder="Es: Glaive, Pii Masking...",
            key="dataset_search_input"
        )
    
    with col_sort:
        st.write("Quality sort")
        icons = {"disabled": "⚪ Off", "desc": "⬇️ Max", "asc": "⬆️ Min"}
        current_state = st.session_state.quality_sort_state
        
        if st.button(f"{icons[current_state]}", help="Cambia ordine per qualità", use_container_width=True):
            if current_state == "disabled":
                st.session_state.quality_sort_state = "desc"
            elif current_state == "desc":
                st.session_state.quality_sort_state = "asc"
            else:
                st.session_state.quality_sort_state = "disabled"
            st.rerun()

    with col_mat:
        st.write("materialized")
        mat_icons = {
            "disabled": "⚪ Tutti",
            "not_materialized": "📦 ❌ Non materializzati",
            "materialized": "📦 ✅ Materializzati"
        }
        current_mat = st.session_state.materialization_filter_state
        if st.button(f"{mat_icons[current_mat]}", help="Filtra per materializzazione", use_container_width=True):
            if current_mat == "disabled":
                st.session_state.materialization_filter_state = "not_materialized"
            elif current_mat == "not_materialized":
                st.session_state.materialization_filter_state = "materialized"
            else:
                st.session_state.materialization_filter_state = "disabled"
            st.rerun()

    with st.expander("Altri filtri (Lingue / Tasks / Core Skills / License / etc.)"):
        c1, c2 = st.columns(2)
        with c1:
            st.multiselect("Lingue", options=list(lang_options.keys()), format_func=lambda x: f"{x} - {lang_options.get(x, '')}", key="search_filter_languages")
            st.multiselect("Tasks", options=list(task_options.keys()), format_func=lambda x: f"{x} - {task_options.get(x, '')}", key="search_filter_tasks")
            st.multiselect("Fields", options=list(field_options.keys()), format_func=lambda x: f"{x} - {field_options.get(x, '')}", key="search_filter_fields")
            st.multiselect("Vertical", options=list(vertical_options.keys()), format_func=lambda x: f"{x} - {vertical_options.get(x, '')}", key="search_filter_verticals")
        with c2:
            st.multiselect("Core Skills", options=list(core_options.keys()), format_func=lambda x: f"{x} - {core_options.get(x, '')}", key="search_filter_core_skills")
            st.multiselect("License", options=list(license_options.keys()), format_func=lambda x: f"{x} - {license_options.get(x, '')}", key="search_filter_licenses")
            st.multiselect("Source Categories", options=list(source_cat_options.keys()), format_func=lambda x: f"{x} - {source_cat_options.get(x, '')}", key="search_filter_sources")
            st.multiselect("Contents", options=list(content_options.keys()), format_func=lambda x: f"{x} - {content_options.get(x, '')}", key="search_filter_contents")
            st.selectbox("Source Type", options=[None] + list(source_type_options.keys()), format_func=lambda x: (f"{x} - {source_type_options.get(x, '')}" if x else "Tutti"), key="search_filter_source_type")

    if st.button("Cerca", key="dataset_search_button") or search_term is not None:
        search_dataset_cards(st, repo, search_term)

def search_dataset_cards(st, repo: DatasetCardRepository, search_term: str):
    """Ricerca con filtri e sorting"""
    try:
        comp_repo = CardCompositionRepository(st.session_state.db_manager)
        all_cards = repo.get_all()
        dataset_repo = DatasetRepository(st.session_state.db_manager)
        
        selected_langs = st.session_state.get("search_filter_languages", []) or []
        selected_tasks = st.session_state.get("search_filter_tasks", []) or []
        selected_cores = st.session_state.get("search_filter_core_skills", []) or []
        selected_licenses = st.session_state.get("search_filter_licenses", []) or []
        selected_fields = st.session_state.get("search_filter_fields", []) or []
        selected_sources = st.session_state.get("search_filter_sources", []) or []
        selected_verticals = st.session_state.get("search_filter_verticals", []) or []
        selected_contents = st.session_state.get("search_filter_contents", []) or []
        selected_source_type = st.session_state.get("search_filter_source_type", None)

        search_term_lower = search_term.lower() if search_term else ""
        results = []
        
        for card in all_cards:
            name_match = search_term_lower in card.dataset_name.lower()
            comp_match = False
            components = comp_repo.get_children_by_parent(card.dataset_name)
            if any(search_term_lower in c.child_card_name.lower() for c in components):
                comp_match = True
            
            if not search_term or name_match or comp_match:
                if selected_langs:
                    card_langs = card.languages or []
                    if not any(l in selected_langs for l in card_langs):
                        continue
                if selected_tasks:
                    card_tasks = getattr(card, 'tasks', []) or []
                    if not any(t in selected_tasks for t in card_tasks):
                        continue
                if selected_cores:
                    card_cores = getattr(card, 'core_skills', []) or []
                    if not any(c in selected_cores for c in card_cores):
                        continue
                if selected_licenses:
                    card_license = getattr(card, 'license', None)
                    if not card_license or card_license not in selected_licenses:
                        continue
                if selected_fields:
                    card_fields = getattr(card, 'fields', []) or []
                    if not any(f in selected_fields for f in card_fields):
                        continue
                if selected_sources:
                    card_sources = getattr(card, 'sources', []) or []
                    if not any(s in selected_sources for s in card_sources):
                        continue
                if selected_verticals:
                    card_verticals = getattr(card, 'vertical', []) or []
                    if not any(v in selected_verticals for v in card_verticals):
                        continue
                if selected_contents:
                    card_contents = getattr(card, 'contents', []) or []
                    if not any(cn in selected_contents for cn in card_contents):
                        continue
                if selected_source_type:
                    card_src_type = getattr(card, 'source_type', None)
                    if not card_src_type or card_src_type != selected_source_type:
                        continue

                mat_state = st.session_state.get("materialization_filter_state", "disabled")
                if mat_state == "disabled":
                    results.append(card)
                else:
                    try:
                        physical_dataset = dataset_repo.get_by_derived_card(card.id)
                        is_mat = physical_dataset is not None
                    except Exception:
                        is_mat = False

                    if mat_state == "not_materialized" and not is_mat:
                        results.append(card)
                    elif mat_state == "materialized" and is_mat:
                        results.append(card)
        
        if results:
            sort_state = st.session_state.get("quality_sort_state", "disabled")
            if sort_state == "desc":
                results.sort(key=lambda x: (x.quality if x.quality else 0), reverse=True)
            elif sort_state == "asc":
                results.sort(key=lambda x: (x.quality if x.quality else 0), reverse=False)
            else:
                results.sort(key=lambda x: x.created_at if x.created_at else datetime.min, reverse=True)
            
            st.success(f"Trovati {len(results)} dataset (Ordine: {sort_state})")
            for i, card in enumerate(results):
                with st.expander(f"📊 {card.dataset_name}", expanded=(i == 0)):
                    show_dataset_card_details(st, card, repo)
        else:
            st.info("Nessun risultato trovato.")
    except Exception as e:
        st.error(f"Errore ricerca: {e}")

def show_dataset_card_details(st, card: DatasetCard, repo: DatasetCardRepository):
    """Visualizzazione completa dei dettagli di una dataset card"""
    comp_repo = CardCompositionRepository(st.session_state.db_manager)
    components = comp_repo.get_children_by_parent(card.dataset_name)

    col_text, col_edit, col_del = st.columns([6, 1, 1])
    
    with col_edit:
        if st.button("✏️", key=f"edit_btn_{card.id}", help="Modifica Metadati"):
            st.session_state.editing_card = card
            st.session_state.dataset_card_action = "edit_dataset_card"
            st.rerun()
            
    with col_del:
        if st.button("❌", key=f"del_btn_{card.id}", help="Elimina Card"):
            st.session_state.deleting_card_id = card.id
            st.session_state.confirm_delete = True

    if st.session_state.get("confirm_delete") and st.session_state.get("deleting_card_id") == card.id:
        st.warning(f"Sei sicuro di voler eliminare la card '{card.dataset_name}'?")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Sì, elimina", key=f"conf_yes_{card.id}", type="primary"):
                try:
                    repo.delete(card.id)
                    st.success("Card eliminata!")
                    st.session_state.confirm_delete = False
                    st.rerun()
                except Exception as e:
                    st.error(f"Errore: {e}")
        with c2:
            if st.button("Annulla", key=f"conf_no_{card.id}"):
                st.session_state.confirm_delete = False
                st.rerun()

    # Sezione informazioni complete
    quality_level = card.quality if card.quality else 0
    
    st.markdown("### 📊 Informazioni Generali")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Nome", card.dataset_name)
        st.metric("ID Dataset", card.dataset_id)
        st.metric("Modalità", card.modality or "N/A")
        st.metric("Tipo di Fonte", card.source_type or "N/A")
        st.metric("Landscape", card.fields or "N/A")
        st.metric("Uso Verticale", card.vertical or "N/A")
        st.metric("Contenuti", card.contents or "N/A")
    with col2:
        st.metric("Qualità", f"{'⭐' * quality_level}")
        st.metric("Licenza", card.license or "N/A")
    with col3:
        st.metric("Last Update", card.last_update.strftime('%Y-%m-%d') if card.last_update else 'N/A')
        st.metric("Reasoning", "✅" if card.has_reasoning else "❌")

    if card.dataset_description:
        st.markdown("**Descrizione:**")
        st.info(card.dataset_description)
    
    if card.publisher:
        st.write(f"**Publisher:** {card.publisher}")
    
    if card.notes:
        with st.expander("📝 Note"):
            st.write(card.notes)

    # Vocabolari
    st.markdown("---")
    st.markdown("### 🏷️ Classificazione")
    
    try:
        vocab_repos = initialize_vocabulary_repositories(st)
        lang_map = get_vocabulary_options_from_repo(vocab_repos.get('language'))
        task_map = get_vocabulary_options_from_repo(vocab_repos.get('task'))
        core_map = get_vocabulary_options_from_repo(vocab_repos.get('core_skill'))
        field_map = get_vocabulary_options_from_repo(vocab_repos.get('field'))
        source_map = get_vocabulary_options_from_repo(vocab_repos.get('source_category'))
        source_type_map = get_vocabulary_options_from_repo(vocab_repos.get('source_type'))
        vertical_map = get_vocabulary_options_from_repo(vocab_repos.get('vertical'))
        content_map = get_vocabulary_options_from_repo(vocab_repos.get('content'))

        col1, col2 = st.columns(2)
        
        with col1:
            card_langs = getattr(card, 'languages', None) or []
            if card_langs:
                readable_langs = [f"{l} ({lang_map.get(l, 'N/A')})" for l in card_langs]
                st.write(f"**🌍 Lingue:** {', '.join(readable_langs)}")

            card_tasks = getattr(card, 'tasks', None) or []
            if card_tasks:
                readable_tasks = [f"{t} ({task_map.get(t, 'N/A')})" for t in card_tasks]
                st.write(f"**🎯 Tasks:** {', '.join(readable_tasks)}")

            card_cores = getattr(card, 'core_skills', None) or []
            if card_cores:
                readable_cores = [f"{c} ({core_map.get(c, 'N/A')})" for c in card_cores]
                st.write(f"**💡 Core Skills:** {', '.join(readable_cores)}")

            card_fields = getattr(card, 'fields', None) or []
            if card_fields:
                readable_fields = [f"{f} ({field_map.get(f, 'N/A')})" for f in card_fields]
                st.write(f"**📚 Fields:** {', '.join(readable_fields)}")
        
        with col2:
            card_sources = getattr(card, 'sources', None) or []
            if card_sources:
                readable_sources = [f"{s} ({source_map.get(s, 'N/A')})" for s in card_sources]
                st.write(f"**📦 Source Categories:** {', '.join(readable_sources)}")

            card_src_type = getattr(card, 'source_type', None)
            if card_src_type:
                st.write(f"**🔗 Source Type:** {card_src_type} ({source_type_map.get(card_src_type, 'N/A')})")

            card_verticals = getattr(card, 'vertical', None) or []
            if card_verticals:
                readable_verticals = [f"{v} ({vertical_map.get(v, 'N/A')})" for v in card_verticals]
                st.write(f"**🏢 Vertical:** {', '.join(readable_verticals)}")

            card_contents = getattr(card, 'contents', None) or []
            if card_contents:
                readable_contents = [f"{c} ({content_map.get(c, 'N/A')})" for c in card_contents]
                st.write(f"**📄 Contents:** {', '.join(readable_contents)}")
    except Exception:
        pass

    # URLs
    if card.source_url or card.download_url:
        st.markdown("---")
        st.markdown("### 🔗 Links")
        if card.source_url:
            st.write(f"**Source URL:** [{card.source_url}]({card.source_url})")
        if card.download_url:
            st.write(f"**Download URL:** [{card.download_url}]({card.download_url})")
    
    # Composizione
    if components:
        st.markdown("---")
        st.markdown("### 🧬 Composizione MIX")
        st.table([{"Componente": c.child_card_name, "Peso": f"{c.weight:.2f}"} for c in components])

    st.markdown("---")
    show_download_section(st, card)

def show_dataset_card_creation_form(st, dataset_card_repo: DatasetCardRepository, edit_mode: bool = False, card_entity: DatasetCard = None):
    """Form per modifica dataset card esistente"""
    st.markdown(f"### ✏️ Modifica Dataset Card: {card_entity.dataset_name}")
    
    vocab_repos = initialize_vocabulary_repositories(st)
    comp_repo = CardCompositionRepository(st.session_state.db_manager)
    
    # MIX Section
    st.markdown("#### 🧬 Definizione MIX")
    all_cards = dataset_card_repo.get_all()
    card_names = sorted([c.dataset_name for c in all_cards if c.dataset_name != card_entity.dataset_name])
    
    current_comps = comp_repo.get_children_by_parent(card_entity.dataset_name)
    default_components = [c.child_card_name for c in current_comps]
    existing_comp_weights = {c.child_card_name: c.weight for c in current_comps}

    selected_components = st.multiselect(
        "Seleziona i Dataset componenti", 
        options=card_names, 
        default=default_components,
        key="mix_components_selection"
    )
    
    component_weights = {}
    if selected_components:
        cols = st.columns(len(selected_components))
        for i, comp_name in enumerate(selected_components):
            with cols[i]:
                default_w = existing_comp_weights.get(comp_name, 0.0)
                component_weights[comp_name] = st.number_input(
                    f"Peso {comp_name}", 0.0, 1.0, default_w, 0.01, 
                    key=f"w_{comp_name}"
                )
    
    st.markdown("---")

    # Nuova sezione: scelta modalità lingue per la modifica (fuori dal form)
    if "edit_lang_input_mode" not in st.session_state:
        # default: usa 'enum' e mantiene la configurazione esistente
        st.session_state.edit_lang_input_mode = "enum"

    st.markdown("#### 🈷️ Modalità inserimento Lingue (Modifica)")
    st.write("Scegli come vuoi inserire le lingue per questo dataset prima di modificare i metadati.")
    current_edit = st.session_state.get("edit_lang_input_mode", "enum")
    edit_mode_choice = st.radio(
        "Modalità",
        options=["enum", "list"],
        index=0 if current_edit == "enum" else 1,
        key="edit_creation_lang_mode_radio"
    )
    st.session_state.edit_lang_input_mode = edit_mode_choice

    # Form Metadati
    with st.form("dataset_card_form", clear_on_submit=False):
        col_n, col_i = st.columns([3, 1])
        with col_n:
            dataset_id = st.text_input("ID Dataset *", value=card_entity.dataset_id)
            dataset_name = st.text_input("Nome Dataset *", value=card_entity.dataset_name)
        with col_i:
            quality = st.slider("Qualità *", 1, 5, value=int(card_entity.quality) if card_entity.quality else 3)
            
        dataset_description = st.text_area("Descrizione *", height=100, value=card_entity.dataset_description)
        
        modalities = get_vocabulary_options_from_repo(vocab_repos.get('modality'))
        languages = get_vocabulary_options_from_repo(vocab_repos.get('language'))
        licenses = get_vocabulary_options_from_repo(vocab_repos.get('license'))
        tasks = get_vocabulary_options_from_repo(vocab_repos.get('task'))
        core_skills_options = get_vocabulary_options_from_repo(vocab_repos.get('core_skill'))
        fields_options = get_vocabulary_options_from_repo(vocab_repos.get('field'))
        source_cat_options = get_vocabulary_options_from_repo(vocab_repos.get('source_category'))
        source_type_options = get_vocabulary_options_from_repo(vocab_repos.get('source_type'))
        vertical_options = get_vocabulary_options_from_repo(vocab_repos.get('vertical'))
        content_options = get_vocabulary_options_from_repo(vocab_repos.get('content'))
        
        c1, c2, c3 = st.columns(3)
        with c1:
            mod_keys = list(modalities.keys())
            default_mod_idx = mod_keys.index(card_entity.modality) if card_entity.modality in mod_keys else 0
            selected_modality = st.selectbox("Modalità *", mod_keys, index=default_mod_idx, format_func=lambda x: f"{x} - {modalities.get(x, '')}")
            
        with c2:
            # Usa la modalità scelta nella sezione sopra (edit_lang_input_mode)
            lang_input_mode = st.session_state.get("edit_lang_input_mode", "enum")
            selected_languages = []
            if lang_input_mode == "enum":
                selected_languages = st.multiselect("Lingue *", list(languages.keys()), default=card_entity.languages if getattr(card_entity, 'languages', None) else [])
            else:
                list_default = json.dumps(card_entity.languages if getattr(card_entity, 'languages', None) else [])
                lang_list_text = st.text_input(
                    "Valore",
                    value=list_default,
                    placeholder='es. ["item1", "item2"]',
                    key="edit_languages_list_input",
                    label_visibility="collapsed",
                    help="Inserisci una lista JSON di stringhe, es: [\"it\", \"en\"]"
                )
                if lang_list_text:
                    try:
                        parsed = json.loads(lang_list_text)
                        if isinstance(parsed, list) and all(isinstance(x, str) for x in parsed):
                            selected_languages = parsed
                        else:
                            st.error("Formato non valido: inserisci un array JSON di stringhe.")
                    except Exception:
                        st.error("JSON non valido. Usa doppi apici per le stringhe, es: [\"it\",\"en\"]")
        with c3:
            lic_keys = list(licenses.keys())
            default_lic_idx = lic_keys.index(card_entity.license) if card_entity.license in lic_keys else 0
            selected_license = st.selectbox("Licenza *", lic_keys, index=default_lic_idx, format_func=lambda x: f"{x} - {licenses.get(x, '')}")

        c4, c5 = st.columns(2)
        with c4:
            selected_tasks = st.multiselect("Tasks", options=list(tasks.keys()), default=card_entity.tasks if getattr(card_entity, 'tasks', None) else [], format_func=lambda x: f"{x} - {tasks.get(x, '')}")
        with c5:
            selected_core_skills = st.multiselect("Core Skills", options=list(core_skills_options.keys()), default=card_entity.core_skills if getattr(card_entity, 'core_skills', None) else [], format_func=lambda x: f"{x} - {core_skills_options.get(x, '')}")

        c6, c7 = st.columns(2)
        with c6:
            selected_fields = st.multiselect("Fields", options=list(fields_options.keys()), default=card_entity.fields if getattr(card_entity, 'fields', None) else [], format_func=lambda x: f"{x} - {fields_options.get(x, '')}")
            selected_verticals = st.multiselect("Vertical", options=list(vertical_options.keys()), default=card_entity.vertical if getattr(card_entity, 'vertical', None) else [], format_func=lambda x: f"{x} - {vertical_options.get(x, '')}")
        with c7:
            selected_sources = st.multiselect("Source Categories", options=list(source_cat_options.keys()), default=card_entity.sources if getattr(card_entity, 'sources', None) else [], format_func=lambda x: f"{x} - {source_cat_options.get(x, '')}")
            selected_contents = st.multiselect("Contents", options=list(content_options.keys()), default=card_entity.contents if getattr(card_entity, 'contents', None) else [], format_func=lambda x: f"{x} - {content_options.get(x, '')}")

        src_type_keys = [None] + list(source_type_options.keys())
        default_src_type = card_entity.source_type if getattr(card_entity, 'source_type', None) in source_type_options else None
        selected_source_type = st.selectbox("Source Type", options=src_type_keys, index=0 if default_src_type is None else src_type_keys.index(default_src_type), format_func=lambda x: (f"{x} - {source_type_options.get(x, '')}" if x else "Nessuno"))

        publisher = st.text_input("Publisher", value=card_entity.publisher if card_entity.publisher else "")
        notes = st.text_area("Note", value=card_entity.notes if card_entity.notes else "")
        source_url = st.text_input("Source URL", value=card_entity.source_url if card_entity.source_url else "")
        download_url = st.text_input("Download URL", value=card_entity.download_url if card_entity.download_url else "")
        has_reasoning = st.checkbox("Contiene elementi di reasoning", value=card_entity.has_reasoning)

        submitted = st.form_submit_button("💾 Aggiorna Dataset Card", type="primary")

        if submitted:
            clean_name = dataset_name.replace(" ", "_").strip()
            err = validate_dataset_card_input(clean_name, dataset_id, dataset_description, selected_languages)
            if err:
                st.error(err)
                return

            try:
                card_entity.dataset_name = clean_name
                card_entity.dataset_id = dataset_id
                card_entity.modality = selected_modality
                card_entity.dataset_description = dataset_description
                card_entity.languages = selected_languages
                card_entity.tasks = selected_tasks
                card_entity.core_skills = selected_core_skills
                card_entity.license = selected_license
                card_entity.quality = quality
                card_entity.publisher = publisher
                card_entity.notes = notes
                card_entity.source_url = source_url
                card_entity.download_url = download_url
                card_entity.has_reasoning = has_reasoning
                card_entity.fields = selected_fields
                card_entity.sources = selected_sources
                card_entity.source_type = selected_source_type
                card_entity.vertical = selected_verticals
                card_entity.contents = selected_contents
                
                dataset_card_repo.update(card_entity)

                with st.session_state.db_manager as db:
                    db.delete(POSTGRES_DB_SCHEMA + "." + "card_composition", where="parent_card_name = %s", params=(clean_name,))

                for comp_name, weight in component_weights.items():
                    comp_repo.insert(CardComposition(parent_card_name=clean_name, child_card_name=comp_name, weight=weight))
                
                st.success("Dataset Card aggiornata con successo!")
                st.session_state.dataset_card_action = "search_dataset_card"
                st.rerun()
            except Exception as e:
                st.error(f"Errore durante l'operazione: {e}")

    if st.button("❌ Annulla"):
        st.session_state.dataset_card_action = "search_dataset_card"
        st.rerun()

def show_language_mode_selection_step(st):
    """Step intermedio per scegliere la modalità di inserimento delle lingue (enum / list)"""
    st.markdown("#### 🈷️ Step 3: Modalità inserimento Lingue")
    st.write("Scegli come vuoi inserire le lingue nel form successivo.")
    
    current = st.session_state.get("creation_lang_input_mode", "enum")
    mode = st.radio("Modalità", options=["enum", "list"], index=0 if current == "enum" else 1, key="creation_lang_mode_radio")
    st.session_state.creation_lang_input_mode = mode
    
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("← Indietro", use_container_width=True):
            st.session_state.creation_step = 2
            st.rerun()
    with col2:
        if st.button("Avanti →", type="primary", use_container_width=True):
            st.session_state.creation_step = 4
            st.rerun()