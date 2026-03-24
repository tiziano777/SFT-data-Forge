# ui/schema/schema_extraction_handler.py
import json
import logging
from typing import Dict, Any, List
from genson import SchemaBuilder

from utils.streamlit_func import reset_dashboard_session_state
from data_class.repository.table.distribution_repository import DistributionRepository

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

def _extract_deterministic_schema(samples: List[Dict]) -> Dict:
    """
    Estrae deterministicamente lo schema JSON dai campioni usando genson.
    
    Args:
        samples: Lista di campioni (dizionari) da analizzare
        
    Returns:
        Schema JSON Schema generato
    """
    try:
        builder = SchemaBuilder()
        
        # Aggiungi tutti i campioni al builder
        for sample in samples:
            builder.add_object(sample)
        
        # Genera lo schema
        schema = builder.to_schema()
        
        logger.info(f"Schema estratto deterministicamente con genson: {len(schema.get('properties', {}))} proprietà")
        return schema
        
    except Exception as e:
        logger.error(f"Errore nell'estrazione deterministica dello schema: {e}")
        # Restituisci uno schema minimale in caso di errore
        return {
            "type": "object",
            "properties": {},
            "description": "Errore nell'estrazione dello schema"
        }

def show_schema_options(st):
    """Mostra la sezione di estrazione o importazione dello schema."""
    st.subheader("🔍 Estrazione Schema Sorgente")

    # Pulsante di navigazione
    if st.button("⬅️ Torna alla Distribution", key="back_to_distribution"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "processed_distribution_main"
        st.rerun()
    
    st.markdown("---")
    st.write("Scegli come vuoi ottenere lo schema sorgente:")

    # Opzioni di estrazione
    col_extraction1, col_extraction2 = st.columns(2)
    
    with col_extraction1:
        st.info("⚠️ L'estrazione con LLM è temporaneamente disabilitata.")
        # Mantenuto il commento ma senza pulsante attivo
        '''
        if st.button("🤖 Estrai con LLM", type="primary", key="llm_extraction_button"):
            st.session_state.current_stage = "raw_schema_extraction"
            st.session_state.pipeline_started = False
            st.session_state.deterministic_extraction = False
            st.rerun()
        '''
    
    with col_extraction2:
        if st.button("⚡ Estrai Deterministicamente", type="primary", key="deterministic_extraction_button"):
            st.session_state.current_stage = "processed_schema_extraction"
            st.session_state.deterministic_extraction = True
            st.rerun()
        
    st.markdown("---")

def _process_deterministic_extraction(st, repos: Dict) -> bool:
    """
    Processa l'estrazione deterministica dello schema usando genson.
    """
    
    # Verifica presenza campioni
    if "samples" not in st.session_state or not st.session_state.samples:
        st.error("❌ Nessun campione disponibile per l'estrazione.")
        return False
    
    try:
        # Assicuriamoci che i samples siano in un formato valido
        samples = st.session_state.samples
        
        # Se samples è una stringa JSON, la parsiamo
        if isinstance(samples, str):
            samples = json.loads(samples)
        
        # Se samples è un dizionario con una chiave che contiene i campioni
        if isinstance(samples, dict) and "samples" in samples:
            samples = samples["samples"]
        
        # Assicuriamoci che samples sia una lista
        if not isinstance(samples, list):
            if isinstance(samples, dict):
                samples = [samples]
            else:
                st.error(f"❌ Formato campioni non valido: attesa lista o dizionario, ricevuto {type(samples)}")
                return False
        
        logger.info(f"Campioni caricati per estrazione schema: {len(samples)} campioni")
        
    except Exception as e:
        st.error(f"❌ Errore nel processing dei campioni: {e}")
        logger.error(f"Errore nel processing dei campioni: {e}")
        return False
    
    # Estrai schema deterministicamente con genson
    with st.spinner("⚡ Estrazione schema in corso (genson)..."):
        try:
            generated_schema = _extract_deterministic_schema(samples)
            
            # Salva nella sessione
            st.session_state.generated_schema = generated_schema
            
            logger.info(f"Schema generato con successo: {json.dumps(generated_schema, indent=2)[:200]}...")
            
            # Nota: non salviamo ancora nella distribution, aspettiamo la conferma dell'utente
            return True
            
        except Exception as e:
            st.error(f"❌ Errore durante l'estrazione dello schema: {e}")
            logger.error(f"Errore estrazione schema: {e}")
            return False

def _render_manual_edit_interface(st):
    """Renderizza l'interfaccia di modifica manuale."""
    st.markdown("---")
    st.subheader("✏️ Modifica Manuale Schema")
    st.write("Modifica lo schema JSON e conferma per completare.")
    
    # Recupera lo schema corrente (generato o modificato in precedenza)
    current_schema = st.session_state.get("edited_schema", st.session_state.get("generated_schema", {}))
    schema_str = json.dumps(current_schema, indent=2) if current_schema else "{}"
    
    edited_schema_str = st.text_area(
        "Schema JSON:", 
        value=schema_str, 
        height=400,
        key="manual_edit_textarea",
        help="Modifica lo schema JSON secondo le tue esigenze"
    )

    col1, col2 = st.columns(2)
    if col1.button("✅ Conferma Modifiche", use_container_width=True, key="confirm_manual_edit"):
        try:
            # Validazione JSON
            validated_schema = json.loads(edited_schema_str)
            st.session_state.generated_schema = validated_schema
            st.session_state.edited_schema = validated_schema
            st.session_state.manual_edit_active = False
            st.session_state.validation_success = True
            st.rerun()
        except json.JSONDecodeError as e:
            logger.error(f"Errore di parsing JSON: {e}")
            st.error(f"❌ Errore: JSON non valido - {e}")
        except Exception as e:
            logger.error(f"Errore imprevisto: {e}")
            st.error(f"❌ Errore imprevisto: {e}")

    if col2.button("❌ Annulla Modifica", use_container_width=True, key="cancel_manual_edit"):
        st.session_state.manual_edit_active = False
        st.rerun()

def _render_success_interface(st, repos: Dict):
    """Renderizza l'interfaccia di successo."""
    st.success("✅ Schema validato e salvato correttamente!")
    st.subheader("📋 Schema Generato")
    
    # Mostra statistiche dello schema
    schema = st.session_state.generated_schema
    if "properties" in schema:
        st.info(f"📊 Lo schema contiene {len(schema['properties'])} proprietà")
    
    st.json(schema)
    
    st.markdown("---")
    col1, col2 = st.columns(2)
    if col1.button("✅ Continua alla Distribution", key="continue_button"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "processed_distribution_main"
        st.rerun()
    
    if col2.button("✏️ Modifica Manuale", key="edit_from_success"):
        st.session_state.manual_edit_active = True
        st.session_state.validation_success = False
        st.session_state.edited_schema = st.session_state.generated_schema
        st.rerun()

def _render_review_interface(st, repos: Dict):
    """Renderizza l'interfaccia di review prima della conferma finale."""
    st.markdown("---")
    st.subheader("📊 Anteprima Campioni")
    
    # Mostra un'anteprima dei primi campioni
    samples = st.session_state.samples
    if isinstance(samples, list) and len(samples) > 0:
        st.write(f"**Totale campioni:** {len(samples)}")
        st.json(samples[:3] if len(samples) > 3 else samples, expanded=False)  # Mostra solo i primi 3
    else:
        st.json(samples)

    st.markdown("---")
    st.subheader("🔍 Schema Generato (JSON Schema)")
    
    # Mostra statistiche dello schema
    schema = st.session_state.generated_schema
    col1, col2, col3 = st.columns(3)
    with col1:
        if "properties" in schema:
            st.metric("Proprietà", len(schema["properties"]))
    with col2:
        if "required" in schema:
            st.metric("Campi obbligatori", len(schema["required"]))
    with col3:
        st.metric("Tipo", schema.get("type", "object"))
    
    st.json(schema)

    st.markdown("---")
    
    cols = st.columns(3)
    if cols[0].button("✅ Conferma e Salva Schema", use_container_width=True, key="confirm_schema"):
        # Salva definitivamente lo schema nella distribution
        if _save_schema_to_distribution(st, st.session_state.generated_schema, repos):
            st.session_state.src_schema = st.session_state.generated_schema
            st.session_state.validation_success = True
            st.success("✅ Schema salvato nel database!")
            st.rerun()
        else:
            st.error("❌ Errore nel salvataggio dello schema nel database")
    
    if cols[1].button("✏️ Modifica Manuale", use_container_width=True, key="manual_edit_from_review"):
        st.session_state.manual_edit_active = True
        st.session_state.edited_schema = st.session_state.generated_schema
        st.rerun()
    
    if cols[2].button("🔄 Rigenera Schema", use_container_width=True, key="regenerate_schema"):
        st.session_state.generated_schema = None
        st.session_state.manual_edit_active = False
        st.rerun()

def show_schema_extraction(st, langfuse_handler=None):
    """
    Mostra la sezione di estrazione dello schema.
    Versione semplificata e deterministica con genson per l'estrazione.
    """
    st.subheader("🔍 Estrazione Schema Sorgente")

    if "deterministic_extraction" not in st.session_state:
        st.info("ℹ️ Seleziona la tecnica di estrazione nella schermata precedente.")
        return

    # Inizializzazione repository
    repos = _initialize_repositories(st)

    # Inizializzazione stati di sessione
    st.session_state.setdefault("manual_edit_active", False)
    st.session_state.setdefault("validation_success", False)
    st.session_state.setdefault("generated_schema", None)

    # Verifica se dobbiamo avviare l'estrazione
    if st.session_state.deterministic_extraction and not st.session_state.generated_schema and not st.session_state.manual_edit_active:
        # Esegui estrazione deterministica con genson
        success = _process_deterministic_extraction(st, repos)
        if success:
            st.rerun()
        else:
            st.error("❌ Errore durante l'estrazione dello schema")
            # Offri la possibilità di reinserire manualmente lo schema
            if st.button("✏️ Inserisci Schema Manualmente"):
                st.session_state.manual_edit_active = True
                st.session_state.generated_schema = {"type": "object", "properties": {}}
                st.rerun()

    # Gestione dei diversi stati dell'interfaccia
    if st.session_state.validation_success:
        _render_success_interface(st, repos)
    elif st.session_state.manual_edit_active:
        _render_manual_edit_interface(st)
    elif st.session_state.generated_schema:
        _render_review_interface(st, repos)

    # Pulsante di navigazione
    if st.button("⬅️ Torna alle Opzioni Schema", key="back_to_options"):
        # Reset selettivo per tornare alle opzioni
        keys_to_keep = ['current_distribution', 'current_distribution_path', 'samples', 'db_manager']
        for key in list(st.session_state.keys()):
            if key not in keys_to_keep and not key.startswith('_'):
                del st.session_state[key]
        st.session_state.current_stage = "processed_schema_extraction_options"
        st.rerun()