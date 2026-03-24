# ui/schema/schema_extraction_handler.py
import json
import ast
import uuid
import os
from typing import Dict, Any



from utils.streamlit_func import reset_dashboard_session_state
from langgraph.types import Command
from agents.states.src_schema_state import State
from data_class.repository.table.distribution_repository import DistributionRepository

import logging
logger = logging.getLogger(__name__)

from config.state_vars import distribution_keys

def _initialize_repositories(st_app):
    """Inizializza i repository necessari."""
    return {
        'distribution': DistributionRepository(st_app.session_state.db_manager)
    }

def _save_schema_to_distribution(st, schema: Dict[str, Any], repos: Dict):
    """Salva lo schema nella distribution corrente."""
    try:
        distribution = repos['distribution'].get_by_id(st.session_state.current_distribution.id)
        distribution.src_schema = schema
        
        rows_affected = repos['distribution'].update(distribution)
        if rows_affected > 0:
            st.session_state.current_distribution = distribution
            return True
        return False
    except Exception as e:
        logger.error(f"Errore nel salvataggio schema: {e}")
        return False

def show_schema_options(st):
    """Mostra la sezione di estrazione o importazione dello schema."""
    st.subheader("🔍 Estrazione Schema Sorgente")

    # Pulsante di navigazione
    if st.button("⬅️ Torna alla Distribution", key="back_to_distribution"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "raw_distribution_main"
        st.rerun()
    
    st.markdown("---")
    st.write("Scegli come vuoi ottenere lo schema sorgente:")

    # Opzioni di estrazione
    col_extraction1, col_extraction2 = st.columns(2)
    
    with col_extraction1:
        st.info("⚠️ L'estrazione con LLM è temporaneamente disabilitata.")
        '''if st.button("🤖 Estrai con LLM", type="primary", key="llm_extraction_button"):
            st.session_state.current_stage = "raw_schema_extraction"
            st.session_state.pipeline_started = False
            st.session_state.deterministic_extraction = False
            st.rerun()'''
    
    with col_extraction2:
        if st.button("⚡ Estrai Deterministicamente", type="primary", key="deterministic_extraction_button"):
            st.session_state.current_stage = "raw_schema_extraction"
            st.session_state.pipeline_started = False
            st.session_state.deterministic_extraction = True
            st.rerun()
        
    st.markdown("---")

def _start_schema_pipeline(st, langfuse_handler):
    """Avvia la pipeline di estrazione schema."""
    if "thread_id" not in st.session_state:
        st.session_state.thread_id = str(uuid.uuid4())

    try:
        samples = json.loads(json.dumps(st.session_state.samples))
        logger.info(f"Campioni caricati per estrazione schema: {samples}")
    except Exception as e:
        st.error(f"❌ Errore nel parsing dei campioni JSON: {e}")
        return

    distribution_path = st.session_state.get("current_distribution_path")
    if not distribution_path:
        st.error("❌ Nessun percorso di distribuzione trovato.")
        return
    
    # Stato iniziale della pipeline
    init_state = State(
        samples=samples,
        output_path=distribution_path,
    )
    
    # Configurazione per estrazione deterministica
    if st.session_state.get("deterministic_extraction", False):
        st.subheader("⚡ Estrazione Schema Deterministica")
        init_state = init_state.copy(update={"deterministic": True})
    else:
        st.subheader("🤖 Estrazione Schema con LLM")
    
    st.write("Analisi del dataset in corso...")
    
    config = {
        "configurable": {"thread_id": st.session_state.thread_id}, 
        "callbacks": [langfuse_handler]
    }

    try:
        with st.spinner("🔄 Avvio pipeline di estrazione schema..."):
            result = st.session_state.src_schema_graph.invoke(init_state, config=config)
        
        st.session_state.interrupt = result.get("__interrupt__")
        st.session_state.state = result
        st.session_state.pipeline_running = True
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Errore durante l'avvio della pipeline: {e}")
        logger.error(f"Errore avvio pipeline schema: {e}")

def _render_success_interface(st, repos: Dict):
    """Renderizza l'interfaccia di successo dopo la validazione."""
    st.success("✅ Schema validato correttamente!")
    st.subheader("📋 Schema Generato")
    st.json(st.session_state.state.get("generated_schema"))
    
    # Salva lo schema nella distribution
    generated_schema = st.session_state.state.get("generated_schema")
    if generated_schema and _save_schema_to_distribution(st, generated_schema, repos):
        st.session_state.src_schema = generated_schema
        st.success("✅ Schema salvato nel database!")
    
    st.markdown("---")
    col1, col2 = st.columns(2)
    if col1.button("✅ Continua", key="continue_button"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "raw_distribution_main"
        st.rerun()
    if col2.button("🔄 Torna Indietro", key="back_button"):
        st.session_state.validation_success = False
        st.rerun()

def _render_manual_edit_interface(st, resume_callback):
    """Renderizza l'interfaccia di modifica manuale."""
    st.markdown("---")
    st.subheader("✏️ Modifica Manuale Schema")
    st.write("Modifica lo schema JSON e conferma per la validazione.")
    
    interrupt_val = st.session_state.interrupt
    if isinstance(interrupt_val, list):
        interrupt_val = interrupt_val[0]
    
    schema_str_to_edit = interrupt_val.value.get("assistant_output", "{}")
    
    edited_schema_str = st.text_area(
        "Schema JSON:", 
        value=schema_str_to_edit, 
        height=400,
        key="manual_edit_textarea"
    )

    col1, col2 = st.columns(2)
    if col1.button("✅ Conferma Modifiche", use_container_width=True, key="confirm_manual_edit"):
        try:
            # Validazione JSON
            logger.info(f"Schema JSON modificato dall'utente: {edited_schema_str}")
            json.loads(edited_schema_str)
            feedback_schema = json.dumps(json.loads(edited_schema_str.strip()), indent=2)
            resume_callback({"action": "manual", "feedback": feedback_schema})
        except json.JSONDecodeError:
            logger.error("Errore di parsing JSON durante la conferma della modifica manuale.")
            st.error("❌ Errore: JSON non valido.")
        except Exception as e:
            logger.error(f"Errore imprevisto durante la conferma della modifica manuale: {e}")
            st.error(f"❌ Errore imprevisto: {e}")

    if col2.button("❌ Annulla Modifica", use_container_width=True, key="cancel_manual_edit"):
        st.session_state.manual_edit_active = False
        st.rerun()

def _render_review_interface(st, resume_callback):
    """Renderizza l'interfaccia di review e feedback."""
    interrupt = st.session_state.interrupt
    if isinstance(interrupt, list):
        interrupt = interrupt[0]

    st.markdown("---")
    st.subheader("📊 Campioni Dataset")
    st.json(st.session_state.samples)

    st.markdown("---")
    st.subheader("🔍 Schema Generato")
    schema_str = interrupt.value.get("assistant_output", "{}")
    
    try:
        st.json(ast.literal_eval(schema_str))
    except (ValueError, SyntaxError):
        st.code(schema_str, language='json')
    
    # Mostra errori di validazione precedenti
    validation_error = st.session_state.state.get("validation_error")
    if validation_error:
        st.error(f"⚠️ **Errore Validazione Precedente:** {validation_error}")
        st.warning("Lo schema è stato rigenerato. Fornisci feedback per migliorarlo.")

    st.markdown("---")
    
    # Interfaccia diversa per estrazione deterministica vs LLM
    if not st.session_state.get("deterministic_extraction", False):
        feedback_text = st.text_area(
            "💡 Feedback per migliorare la generazione:",
            key="feedback_input"
        )
        
        cols = st.columns(4)
        if cols[0].button("➡️ Prosegui Validazione", use_container_width=True, key="proceed_validation"):
            resume_callback({"action": "break"})
        if cols[1].button("🔄 Ritenta con Feedback", use_container_width=True, key="retry_with_feedback"):
            resume_callback({"action": "continue", "feedback": feedback_text})
        if cols[2].button("↩️ Ricomincia", use_container_width=True, key="restart_button"):
            resume_callback({"action": "restart"})
        if cols[3].button("✏️ Modifica Manuale", use_container_width=True, key="manual_edit_button"):
            st.session_state.manual_edit_active = True
            st.rerun()
    else:
        cols = st.columns(2)
        if cols[0].button("➡️ Prosegui Validazione", use_container_width=True, key="proceed_deterministic"):
            resume_callback({"action": "break"})
        if cols[1].button("✏️ Modifica Manuale", use_container_width=True, key="manual_edit_deterministic"):
            st.session_state.manual_edit_active = True
            st.rerun()

def show_schema_extraction(st, langfuse_handler):
    """
    Mostra la sezione di estrazione dello schema e gestisce la pipeline.
    """
    st.subheader("🔍 Estrazione Schema Sorgente")

    if "deterministic_extraction" not in st.session_state:
        st.info("ℹ️ Seleziona la tecnica di estrazione nella schermata precedente.")
        return

    # Inizializzazione repository
    repos = _initialize_repositories(st)

    # Inizializzazione stati di sessione
    st.session_state.setdefault("pipeline_running", False)
    st.session_state.setdefault("manual_edit_active", False)
    st.session_state.setdefault("interrupt", None)
    st.session_state.setdefault("validation_success", False)

    def resume_pipeline(decision: dict):
        """Riprende la pipeline con una decisione."""
        config = {
            "configurable": {"thread_id": st.session_state.thread_id}, 
            "callbacks": [langfuse_handler]
        }
        
        try:
            with st.spinner("🔄 Pipeline in elaborazione..."):
                result = st.session_state.src_schema_graph.invoke(
                    Command(resume=decision), config=config
                )
            
            if "__interrupt__" in result:
                st.info("📝 La pipeline richiede ulteriore feedback.")
                st.session_state.interrupt = result["__interrupt__"]
                st.session_state.state = result
                st.session_state.manual_edit_active = False
                st.session_state.validation_success = False
            else:
                st.success("✅ Pipeline completata con successo!")
                st.session_state.interrupt = None
                st.session_state.pipeline_running = False
                st.session_state.manual_edit_active = False
                st.session_state.state = result
                st.session_state.validation_success = bool(st.session_state.state.get("valid"))

        except Exception as e:
            st.error(f"❌ Errore durante la ripresa: {e}")
            logger.error(f"Errore ripresa pipeline: {e}")
            st.session_state.validation_success = False
            
        st.rerun()

    # Gestione dei diversi stati della pipeline
    if not st.session_state.pipeline_running and not st.session_state.manual_edit_active and not st.session_state.validation_success:
        _start_schema_pipeline(st, langfuse_handler)
    elif st.session_state.validation_success:
        _render_success_interface(st, repos)
    elif st.session_state.manual_edit_active:
        _render_manual_edit_interface(st, resume_pipeline)
    elif st.session_state.interrupt:
        _render_review_interface(st, resume_pipeline)

    # Pulsante di navigazione
    if st.button("⬅️ Torna alle Opzioni Schema", key="back_to_options"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "raw_schema_extraction_options"
        st.rerun()