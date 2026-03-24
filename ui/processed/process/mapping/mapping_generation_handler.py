import json
import ast
import uuid
import traceback
import logging

logger = logging.getLogger(__name__)

from utils.streamlit_func import reset_dashboard_session_state
from langgraph.types import Command 
from agents.states.mapping_schema_state import State as MappingState 
from config.state_vars import distribution_keys
from ui.processed.process.mapping.dst_schema_selection import select_target_schema_stage
from data_class.repository.table.mapping_repository import MappingRepository
from data_class.entity.table.mapping import Mapping

def create_new_mapping_record(st):
        """Crea un nuovo record di mapping nel database e restituisce l'ID"""
        try:
            mapping_repo = get_mapping_repository(st)
            
            # Assicurati che la connessione sia aperta
            if not mapping_repo.db.conn or mapping_repo.db.conn.closed:
                mapping_repo.db.connect()
            
            # Crea un nuovo mapping vuoto
            from datetime import datetime, timezone
            new_mapping = Mapping(
                id=None,
                serial=None,
                distribution_id=st.session_state.distribution_id,
                schema_template_id=st.session_state.schema_template_id,
                mapping={},
                version="1.0.0",
                issued=datetime.now(timezone.utc),
                modified=datetime.now(timezone.utc)
            )
            
            # Salva nel database
            saved_mapping = mapping_repo.insert(new_mapping)
            
            if saved_mapping:
                # Aggiorna session_state con il nuovo mapping
                st.session_state.mapping = {
                    'id': saved_mapping.id,
                    'serial': saved_mapping.serial,
                    'distribution_id': saved_mapping.distribution_id,
                    'schema_template_id': saved_mapping.schema_template_id,
                    'mapping': saved_mapping.mapping,
                    'version': saved_mapping.version,
                    'issued': saved_mapping.issued,
                    'modified': saved_mapping.modified
                }
                return saved_mapping.id
            else:
                raise Exception("Impossibile creare il nuovo mapping nel database")
                
        except Exception as e:
            logger.error(f"Errore nella creazione del nuovo mapping: {e}")
            logger.error(traceback.format_exc())
            raise
    

def get_mapping_repository(st):
    """Factory function per ottenere il repository Mapping"""
    db_manager = st.session_state.get('db_manager')
    if not db_manager:
        raise ValueError("Database manager non trovato nella sessione")
    return MappingRepository(db_manager)

def show_select_target_schema(st):
    """Funzione principale semplificata."""
    
    # 1. Stage di Selezione Schema
    schema_selected = select_target_schema_stage(st)
    if not schema_selected:
        return 

    # INIZIALIZZAZIONE: Assicurati che mapping esista nello session_state
    st.session_state.setdefault("mapping", None)
    mapping_ready = False
    
    if schema_selected:
        try:
            mapping_repo = get_mapping_repository(st)
            
            # Assicurati che la connessione sia aperta
            if not mapping_repo.db.conn or mapping_repo.db.conn.closed:
                mapping_repo.db.connect()
                
            st.session_state.schema_template_id = st.session_state.dst_schema_id
            
            # CORREZIONE: Accedi agli attributi dell'oggetto Distribution invece di usare ['id']
            st.session_state.distribution_id = st.session_state.current_distribution.id

            # Cerca mapping esistente usando il repository
            existing_mappings = mapping_repo.get_by_distribution_id(st.session_state.distribution_id)
            
            # Filtra per schema_template_id
            mapping_record = None
            for mapping in existing_mappings:
                if mapping.schema_template_id == st.session_state.schema_template_id:
                    mapping_record = mapping
                    break
            
            if mapping_record:
                st.session_state.mapping = {
                    'id': mapping_record.id,
                    'distribution_id': mapping_record.distribution_id,
                    'schema_template_id': mapping_record.schema_template_id,
                    'mapping': mapping_record.mapping,
                    'version': mapping_record.version,
                    'issued': mapping_record.issued,
                    'modified': mapping_record.modified
                }

                if mapping_record.mapping and mapping_record.mapping != {}:
                    st.subheader("📋 Mapping Pre-esistente Trovato")
                    st.json(mapping_record.mapping, expanded=False)
                    st.warning("⚠️ Esiste già un mapping. Generandone uno nuovo, verrà sovrascritto.")
            else:
                # Se non esiste mapping, imposta a None
                st.session_state.mapping = None
                
            mapping_ready = True
                
        except Exception as e:
            st.error(f"❌ Errore nella lettura del mapping: {e}")
            mapping_ready = False

    # Pulsanti
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("⬅️ Back to Distribution"):
            reset_dashboard_session_state(st, distribution_keys)
            st.session_state.current_stage = "processed_distribution_main"
            st.rerun()
            
    with col2:
        st.info("⚠️ La generazione automatica del mapping è attualmente disabilitata.")
        '''
        if mapping_ready:
            if st.button("🚀 Generate New Mapping", type="primary"):
                # 🔥 Reset pulito per nuova generazione
                st.session_state.existing_mapping_id = st.session_state.mapping['id'] if st.session_state.mapping else None
                
                st.session_state.current_stage = "mapping_generation"
                st.session_state.pipeline_running = False
                st.session_state.manual_edit_active = False
                st.session_state.interrupt = None
                st.session_state.state = None
                st.rerun()
        else:
            st.button("🚀 Generate New Mapping", disabled=True)
        '''
    with col3:
        if st.button("🛠️ Manual Mapping", type="primary", use_container_width=True):
            # Crea nuovo record mapping se non esiste
            if not st.session_state.mapping:
                try:
                    with st.spinner("Creazione record mapping..."):
                        mapping_id = create_new_mapping_record(st)
                        st.session_state.existing_mapping_id = mapping_id
                except Exception as e:
                    st.error(f"❌ Errore nella creazione del mapping: {e}")
                    return
            
            st.session_state.current_stage = "manual_mapping"
            st.rerun()

def show_mapping_generation(st, langfuse_handler):
    """Gestisce la visualizzazione e l'esecuzione della pipeline di mapping (STEP 6)."""
    st.subheader("Generazione e Validazione del Mapping")

    # Inizializzazione degli stati di sessione specifici per il mapping
    st.session_state.setdefault("pipeline_running", False)
    st.session_state.setdefault("manual_edit_active", False)
    st.session_state.setdefault("interrupt", None)
    st.session_state.setdefault("mapping", None)  # INIZIALIZZAZIONE AGGIUNTA

    # ===================================================================
    # FUNZIONE HELPER PER GESTIRE __INTERRUPT__ DELLA PIPELINE DI MAPPING
    # ===================================================================
    def resume_mapping_pipeline(decision: dict):
        """Invia una decisione alla pipeline di mapping."""
        config = {"configurable": {"thread_id": st.session_state.thread_id_mapping}, "callbacks": [langfuse_handler]}
        
        # 🔥 DEBUG DETTAGLIATO
        logger.info(f"Decisione utente - Action: {decision.get('action')}")
        if decision.get('action') == 'manual':
            logger.info(f"Manual mapping preview: {str(decision.get('feedback', ''))[:200]}...")
        
        try:
            with st.spinner("La pipeline di mapping sta elaborando..."):
                result = st.session_state.mapping_graph.invoke(Command(resume=decision), config=config)
            
            # 🔥 DEBUG dello stato risultante
            logger.info(f"Risultato - Interrupt: {'__interrupt__' in result}")
            if 'mapping' in result and result['mapping']:
                logger.info(f"Mapping nel risultato: {result['mapping'].get('_lang')}")
            
            # Gestione interrupt
            if "__interrupt__" in result:
                logger.info("Pipeline in attesa di revisione...")
                st.session_state.interrupt = result["__interrupt__"]
                st.session_state.state = result
                st.session_state.manual_edit_active = False
            else:
                st.success("✅ Mapping approvato e salvato con successo!")
                st.session_state.interrupt = None
                st.session_state.pipeline_running = False
                st.session_state.manual_edit_active = False
                st.session_state.state = result
                st.session_state.current_stage = "mapping_results"
        
        except Exception as e:
            logger.error(f"Errore durante la ripresa: {e}")
            st.error(f"Errore: {e}")

    st.rerun()

    # ===================================================================
    # 1. AVVIO INIZIALE DELLA PIPELINE DI MAPPING
    # ===================================================================
    if not st.session_state.pipeline_running and not st.session_state.manual_edit_active:
        if "thread_id_mapping" not in st.session_state:
            st.session_state.thread_id_mapping = str(uuid.uuid4())
        
        try:
            # CORREZIONE: Accedi agli attributi dell'oggetto Distribution
            src_schema = st.session_state.current_distribution.src_schema
            st.session_state.metadata = {
                "name": st.session_state.current_distribution.name,
                "description": st.session_state.current_distribution.description,
                "tags": st.session_state.current_distribution.tags
            }
        except Exception as e:
            st.error(f"Errore nel caricamento dei file di schema sorgente o metadati: {e}")
            reset_dashboard_session_state(st, distribution_keys)
            st.session_state.current_stage = "processed_distribution_main"

        # ✅ VERIFICA CRITICA: Assicurati che non ci sia mapping pre-esistente nello stato
        # CORREZIONE: Verifica che mapping esista e abbia il campo 'mapping'
        if (st.session_state.mapping and 
            st.session_state.mapping.get('mapping') and 
            st.session_state.mapping['mapping'] != {}):
            
            st.warning("⚠️ **Attenzione**: È stato rilevato un mapping pre-esistente nello stato. Questo potrebbe influenzare la generazione.")

        print("Samples:", st.session_state.samples)
        print("Destination Schema:", st.session_state.dst_schema)
        print("Metadata:", st.session_state.metadata)
        
        # CORREZIONE CRITICA: Crea un nuovo mapping se non esiste
        if not st.session_state.mapping:
            try:
                with st.spinner("Creazione nuovo record di mapping..."):
                    mapping_id = create_new_mapping_record(st)
                    st.session_state.existing_mapping_id = mapping_id
            except Exception as e:
                st.error(f"❌ Errore nella creazione del nuovo mapping: {e}")
                return
        else:
            mapping_id = st.session_state.mapping['id']
            st.session_state.existing_mapping_id = mapping_id
        
        # VERIFICA FINALE: Assicurati che tutti i campi obbligatori siano presenti
        required_fields = [
            ("samples", st.session_state.samples),
            ("src_schema", src_schema),
            ("dst_schema", st.session_state.dst_schema),
            ("output_path", mapping_id)
        ]
        
        missing_fields = [field for field, value in required_fields if not value]
        if missing_fields:
            st.error(f"❌ Campi obbligatori mancanti: {', '.join(missing_fields)}")
            return

        mapping_state = MappingState(
            samples=st.session_state.samples,
            src_schema=src_schema,
            dst_schema=st.session_state.dst_schema,
            metadata=st.session_state.metadata,
            output_path=str(mapping_id), 
            feedback=None,  # ✅ ASSICURATI che il feedback sia None al primo avvio
            mapping=None,   # ✅ FORZA mapping vuoto per nuova generazione
            chat_history=[] # ✅ RESET chat history per nuova generazione
        )
        
        config = {"configurable": {"thread_id": st.session_state.thread_id_mapping}, "callbacks": [langfuse_handler]}
        
        try:
            with st.spinner("Avvio della pipeline di mapping..."):
                result = st.session_state.mapping_graph.invoke(mapping_state, config=config)
            
            st.session_state.interrupt = result.get("__interrupt__")
            st.session_state.state = result
            st.session_state.pipeline_running = True
            st.rerun()
        except Exception as e:
            st.error(f"Errore durante l'avvio della pipeline di mapping: {e}")
            st.error(traceback.format_exc())

    # ===================================================================
    # 2. INTERFACCIA DI MODIFICA MANUALE
    # ===================================================================
    elif st.session_state.manual_edit_active:
        st.markdown("---")
        st.subheader("Modifica Manuale del Mapping")
        
        interrupt_val = st.session_state.interrupt
        if isinstance(interrupt_val, list): interrupt_val = interrupt_val[0]
        mapping_str_to_edit = interrupt_val.value.get("assistant_output", "{}")
        
        edited_mapping_str = st.text_area("Mapping JSON:", value=mapping_str_to_edit, height=400)

        col1, col2 = st.columns(2)
        if col1.button("✅ Confirm Edit", use_container_width=True):
            try:
                feedback_mapping = json.dumps(json.loads(edited_mapping_str))
                resume_mapping_pipeline({"action": "manual", "feedback": feedback_mapping})
            except json.JSONDecodeError:
                st.error("Errore: il testo inserito non è un JSON valido.")

        if col2.button("❌ Clear Edit", use_container_width=True):
            st.session_state.manual_edit_active = False
            st.rerun()

    # ===================================================================
    # 3. INTERFACCIA DI REVIEW (FEEDBACK UMANO) - MODIFICATA CON st.form()
    # ===================================================================
    elif st.session_state.interrupt:
        interrupt = st.session_state.interrupt
        if isinstance(interrupt, list): interrupt = interrupt[0]

        st.markdown("---")
        st.subheader("Esame del Mapping Generato")
        
        mapping_str = interrupt.value.get("assistant_output", "{}")
        try:
            st.json(ast.literal_eval(mapping_str), expanded=False)
        except (ValueError, SyntaxError):
            st.code(mapping_str, language='json')
        
        validation_error = st.session_state.state.get("validation_error")
        if validation_error:
            st.error(f"⚠️ **Errore di Validazione Precedente:**\n\n{validation_error}")
            st.warning("Il mapping è stato rigenerato. Per favore, rivedilo.")

        st.markdown("---")
        
        # 🔥 MODIFICA PRINCIPALE: Utilizzo di st.form() per preservare l'input
        with st.form("feedback_form"):
            feedback_text = st.text_area("Feedback per migliorare la generazione:")
            
            cols = st.columns(4)
            submitted_continue = cols[0].form_submit_button("➡️ Continue to Validation", use_container_width=True)
            submitted_retry = cols[1].form_submit_button("🔄 Retry with Feedback", use_container_width=True)
            submitted_restart = cols[2].form_submit_button("↩️ Restart from Scratch", use_container_width=True)
            submitted_edit = cols[3].form_submit_button("✏️ Manual Edit", use_container_width=True)

        # Gestione delle azioni DOPO il form
        if submitted_continue:
            resume_mapping_pipeline({"action": "break"})
        elif submitted_retry:
            resume_mapping_pipeline({"action": "continue", "feedback": feedback_text})  # ✅ Feedback preservato
        elif submitted_restart:
            resume_mapping_pipeline({"action": "restart"})
        elif submitted_edit:
            st.session_state.manual_edit_active = True
            st.rerun()

    # ===================================================================
    # Pulsante per tornare indietro
    # ===================================================================
    if st.button("⬅️ Back to Target Schema Selection"):
        st.session_state.current_stage = "select_target_schema"
        # Resetta tutti gli stati specifici di questa pipeline
        for key in ["pipeline_running", "manual_edit_active", "interrupt", "state", "thread_id_mapping"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

def show_mapping_results(st):
    """Visualizza i risultati finali del mapping."""
    st.subheader("🎉 Risultati del Mapping")
    st.write("Confronta i campioni grezzi a sinistra con i dati mappati a destra per validare il risultato.")

    final_state = st.session_state.get("state")
    if final_state and hasattr(final_state, 'get'):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Samples Originali**")
            st.json(final_state.get("samples"), expanded=False)

        with col2:
            st.markdown("**Risultato del Mapping**")
            st.json(final_state.get("mapped_samples"), expanded=False)
        
        st.markdown("---")

        end_col1,end_col2 = st.columns(2)
        with end_col1:
            if st.button("🛠️ Back to mapping Generation"):
                st.session_state.current_stage = "mapping_generation"
                st.session_state.pipeline_running = False
                st.session_state.manual_edit_active = False
                st.session_state.interrupt = None
                st.rerun()
        with end_col2:
            if st.button("✅ OK! Go Back to Distribution🪣"):
                reset_dashboard_session_state(st, distribution_keys)
                st.session_state.current_stage = "processed_distribution_main"
                st.rerun()

    else:
        st.warning("Nessun risultato di mapping trovato. Per favore, esegui prima la pipeline.")
        if st.button("🛠️ Back to mapping Generation"):
            st.session_state.current_stage = "mapping_generation"
            st.rerun()
        if st.button("⬅️ Back To Distribution"):
            reset_dashboard_session_state(st, distribution_keys)
            st.session_state.current_stage = "processed_distribution_main"
            st.rerun()