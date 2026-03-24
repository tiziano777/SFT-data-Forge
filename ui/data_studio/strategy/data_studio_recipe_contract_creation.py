from utils.sample_reader import load_dataset_samples
from utils.path_utils import to_internal_path
from data_class.repository.table.system_prompt_repository import SystemPromptRepository
from data_class.repository.table.schema_template_repository import SchemaTemplateRepository
from data_class.repository.vocabulary.vocab_chat_type_repository import VocabChatTypeRepository
from data_class.repository.vocabulary.vocab_language_repository import VocabLanguageRepository
from db.impl.postgres.loader.postgres_db_loader import get_db_manager

import os
BASE_PREFIX = os.getenv("BASE_PREFIX")

import logging
logger = logging.getLogger(__name__)
db_manager=get_db_manager()

vocab_chat_type_repo = VocabChatTypeRepository(db_manager)
vocab_chat_types = vocab_chat_type_repo.get_all()
codes= [vocab.code for vocab in vocab_chat_types]

system_prompt_repo =  SystemPromptRepository(db_manager)
system_prompts = system_prompt_repo.get_all()

schema_template_repo = SchemaTemplateRepository(db_manager)

vocab_lang_repo = VocabLanguageRepository(db_manager)
langs = vocab_lang_repo.get_all()
langs = [lang.code for lang in langs]

import jsonschema
from utils.sample_reader import load_dataset_samples

# Questa funzione è un helper per validare la distribuzione
# rispetto allo schema associato al chat type selezionato.
def validate_distribution_online(uri_path, schema_dict):
    """
    Carica il primo sample usando load_dataset_samples e lo valida.
    """
    try:
        # load_dataset_samples si aspetta la cartella che contiene i file
        # assumendo che uri_path sia il path della distribuzione
        #print(f"Validating distribution at {uri_path} against target schema template")
        samples = load_dataset_samples(to_internal_path(uri_path), k=1)
        
        if not samples or len(samples) == 0:
            return False, "Impossibile leggere campioni dal file o cartella vuota."
        
        first_sample = samples[0]
        
        # Validazione con Draft7Validator come richiesto
        validator = jsonschema.Draft7Validator(schema_dict)
        errors = sorted(validator.iter_errors(first_sample), key=lambda e: e.path)
        
        if not errors:
            return True, None
        
        # Formattiamo il primo errore in modo leggibile per l'utente
        first_error = errors[0]
        error_path = " -> ".join([str(p) for p in first_error.path])
        return False, f"Errore validazione in [{error_path}]: {first_error.message}"
        
    except Exception as e:
        return False, f"Errore critico durante la validazione: {str(e)}"

def _update_chat_type_in_state(st, dist_id):
    """Callback per la selezione del chat_type con validazione immediata."""
    chat_type = st.session_state.get(f"chat_type_{dist_id}")
    dist_info = st.session_state.receipt[dist_id]
    if chat_type:
        # Recupera entità vocabolario e schema relativo
        chat_type_entry = vocab_chat_type_repo.get_by_code(chat_type)
        if chat_type_entry:
            schema_obj = schema_template_repo.get_by_id(chat_type_entry.schema_id)
            if schema_obj:
                # Esegue la validazione online sul file reale
                is_valid, error_msg = validate_distribution_online(dist_info["dist_uri"], schema_obj.schema)
                
                if is_valid:
                    st.session_state.receipt[dist_id]["chat_type"] = chat_type
                    st.session_state.receipt[dist_id]["schema_template"] = schema_obj.schema
                    st.session_state.receipt[dist_id]["validation_error"] = None
                else:
                    # Invalida la scelta se non passa il test
                    st.session_state.receipt[dist_id]["chat_type"] = None
                    st.session_state.receipt[dist_id]["validation_error"] = error_msg
            else:
                st.session_state.receipt[dist_id]["validation_error"] = "Schema template non trovato."
    else:
        st.session_state.receipt[dist_id]["chat_type"] = None
        st.session_state.receipt[dist_id]["validation_error"] = None

# *--------- ** MAIN FUNCTIONS ** --------------*

def data_studio_recipe_contract_creation(st):
    """Wizard multi-step for creating receipt"""
    step = st.session_state.get("creation_step", 1)
    st.progress((step - 1) /2, text=f"Step {step}/2")

    if step == 1:
        # Step 1: Chat_type and schema template selection/extraction
        show_chat_type_selection_step(st)
    elif step == 2:
        show_system_prompt_selection_step(st)

def show_chat_type_selection_step(st):
    """Visualizza lo step di selezione e validazione dei Chat Type."""
    if st.session_state.get("receipt") is None:
        st.session_state.receipt = {}
        old_recipe = st.session_state.get("old_recipe", {})
        for dataset_key in st.session_state.get('dist_bag'):
            for dist in st.session_state['dist_bag'][dataset_key]['dist']:
                st.session_state.receipt[str(dist.id)] = {
                    "dist_id": dist.id,
                    "dist_name": dist.name,
                    "dist_uri": dist.uri.replace(BASE_PREFIX, ""),
                    "tokenized_uri": dist.tokenized_uri.replace(BASE_PREFIX, "") if dist.tokenized_uri else None,
                    "chat_type": None,
                    "schema_template": None,
                    "system_prompt": None,
                    "system_prompt_name": None,
                    "replica": st.session_state.recipe_replicas[str(dist.id)],
                    "samples": st.session_state.dist_stats_cache[str(dist.id)]["samples"],
                    "tokens": st.session_state.dist_stats_cache[str(dist.id)]["tokens"],
                    "words": st.session_state.dist_stats_cache[str(dist.id)]["words"]
                }
                # Pre-fill chat_type dalla vecchia ricetta
                prefill_chat_type = old_recipe.get(dist.uri, {}).get("template_strategy")
                if prefill_chat_type and prefill_chat_type in codes:
                    st.session_state[f"chat_type_{dist.id}"] = prefill_chat_type
                    _update_chat_type_in_state(st, str(dist.id))

    st.write("### Step 1: Chat_type Selection & Online Validation")
    st.write("Seleziona il Chat Type corretto. Il sistema validerà automaticamente lo schema sul primo record del file.")

    dataset = st.session_state.get('dist_bag')
    distributions = [dist for key in dataset.keys() for dist in dataset[key]['dist']]
    
    vocab_chat_type = codes
    all_valid = True

    for dist in distributions:
        dist_id = str(dist.id)
        data = st.session_state.receipt[dist_id]
        
        col1, col2 = st.columns([0.6, 0.4])

        with col1:
            st.write(f"**Distribution:** {dist.name}")
            # Logica visualizzazione stato
            if data.get("chat_type"):
                st.success(f"✅ Valido: `{data['chat_type']}`")
            elif data.get("validation_error"):
                st.error(f"❌ {data['validation_error']}")
                all_valid = False
            else:
                st.warning("⚠️ Chat type non impostato")
                all_valid = False

        logger.debug(f"Distribution {dist_id}")
        with col2:
            st.selectbox(
                f"Tipo Chat per {dist.name}",
                options=[None] + vocab_chat_type,
                key=f"chat_type_{dist.id}",
                on_change=_update_chat_type_in_state,
                args=(st, dist_id)
            )

    st.markdown("---")
    
    # Navigazione
    col_nav1, col_nav2 = st.columns(2)
    
    with col_nav1:
        if st.button("⬅️ Back to Recipe Builder", key="to_pipeline", use_container_width=True):
            st.session_state.current_stage = "data_studio_stage_area"
            if "creation_step" in st.session_state: del st.session_state["creation_step"]
            if "receipt" in st.session_state: del st.session_state["receipt"] 
            st.rerun()

    with col_nav2:
        # Il tasto Next è abilitato solo se tutte le distribuzioni sono validate
        if all_valid:
            if st.button("Next Step ➡️", type="primary", use_container_width=True):
                st.session_state["creation_step"] = 2
                st.rerun()
        else:
            st.button("Next Step ➡️", disabled=True, use_container_width=True, 
                      help="Correggi gli errori di validazione per proseguire.")

def show_system_prompt_selection_step(st):
    st.write("### Step 2: System Prompt Assignment")
    st.write("This step is **optional**. You can select multiple prompts for each distribution to avoid overfitting.")

    # 1. CARICAMENTO CACHE PROMPT
    if "system_prompts_cache" not in st.session_state:
        st.session_state.system_prompts_cache = system_prompt_repo.get_all()

    current_prompts = st.session_state.system_prompts_cache
    prompt_map = {sp.name: sp for sp in current_prompts}
    prompt_names = list(prompt_map.keys())

    # --- EXPANDER UNICO IN ALTO PER CREAZIONE ---
    with st.expander("➕ Create New System Prompt", expanded=False):
        with st.form(key="global_new_sp_form"):
            col_n, col_d = st.columns(2)
            new_name = col_n.text_input("Name")
            new_description = col_d.text_input("Description")
            new_prompt = st.text_area("Prompt Content", height=150)
            
            f_col1, f_col2 = st.columns(2)
            new_lang = f_col1.selectbox("Language", options=langs)
            quality_score = f_col2.slider("Quality Score", 0.0, 1.0, 0.7, 0.01)
            
            if st.form_submit_button("💾 Save & Refresh Lists"):
                if new_name and new_prompt:
                    try:
                        from datetime import datetime
                        from data_class.entity.table.system_prompt import SystemPrompt
                        new_sp = SystemPrompt(
                            id=None, name=new_name, description=new_description,
                            prompt=new_prompt, _lang=new_lang, length=len(new_prompt),
                            quality_score=quality_score, issued=datetime.utcnow(), modified=datetime.utcnow()
                        )
                        system_prompt_repo.insert(new_sp)
                        st.session_state.system_prompts_cache = system_prompt_repo.get_all()
                        st.success(f"Prompt '{new_name}' created!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
                else:
                    st.error("Name and Prompt are required.")

    st.write("---")

    # 2. ASSEGNAZIONE PER OGNI DISTRIBUZIONE
    dataset = st.session_state.get("dist_bag")
    distributions = [dist for key in dataset.keys() for dist in dataset[key]["dist"]]

    for dist in distributions:
        dist_id = str(dist.id)
        
        # Inizializzazione pulita
        if not isinstance(st.session_state.receipt[dist_id].get("system_prompt_name"), list):
            old_recipe = st.session_state.get("old_recipe", {})
            old_sp_names = old_recipe.get(dist.uri, {}).get("system_prompt_names", [])
            valid_old_names = [n for n in old_sp_names if n in prompt_map]
            st.session_state.receipt[dist_id]["system_prompt_name"] = valid_old_names
            st.session_state.receipt[dist_id]["system_prompt"] = [
                prompt_map[name].prompt for name in valid_old_names
            ]
        
        current_selected_names = st.session_state.receipt[dist_id]["system_prompt_name"]

        # UI Riga
        col_info, col_sel = st.columns([1, 3])
        with col_info:
            st.markdown(f"**{dist.name}**")
            st.caption(f"Selected: {len(current_selected_names)}")

        with col_sel:
            selected_names = st.multiselect(
                f"Prompts for {dist.name}",
                options=prompt_names,
                default=[n for n in current_selected_names if n in prompt_names],
                key=f"ms_sp_{dist_id}",
                label_visibility="collapsed"
            )

            # LOGICA CORRETTA: Creiamo la lista locale ogni volta che cambia la selezione
            if set(selected_names) != set(current_selected_names):
                local_prompts_content = []
                for name in selected_names:
                    sp_obj = prompt_map[name]
                    local_prompts_content.append(sp_obj.prompt)
                
                st.session_state.receipt[dist_id]["system_prompt_name"] = selected_names
                st.session_state.receipt[dist_id]["system_prompt"] = local_prompts_content
                st.rerun()

        # Preview (Invariato)
        if selected_names:
            with st.expander(f"Preview selected prompts for {dist.name}"):
                for name in selected_names:
                    if name in prompt_map:
                        st.text_area(f"Prompt: {name}", value=prompt_map[name].prompt[:1500], height=100, disabled=True, key=f"prev_{dist_id}_{name}")
        st.write("") 

    # 3. NAVIGAZIONE
    st.markdown("---")
    c_back, c_next = st.columns(2)
    with c_back:
        if st.button("⬅️ Step 1"):
            st.session_state["creation_step"] = 1
            st.rerun()
    with c_next:
        if st.button("Review & Finalize ➡️", type="primary", use_container_width=True):
            st.session_state.current_stage = "data_studio_final_review"
            st.rerun()

