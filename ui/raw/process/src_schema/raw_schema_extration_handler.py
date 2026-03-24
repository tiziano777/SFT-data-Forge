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
    """Initialize required repositories."""
    return {
        'distribution': DistributionRepository(st_app.session_state.db_manager)
    }

def _save_schema_to_distribution(st, schema: Dict[str, Any], repos: Dict):
    """Save the schema to the current distribution."""
    try:
        distribution = repos['distribution'].get_by_id(st.session_state.current_distribution.id)
        distribution.src_schema = schema
        
        rows_affected = repos['distribution'].update(distribution)
        if rows_affected > 0:
            st.session_state.current_distribution = distribution
            return True
        return False
    except Exception as e:
        logger.error(f"Error saving schema: {e}")
        return False

def _extract_deterministic_schema(samples: List[Dict]) -> Dict:
    """
    Deterministically extracts JSON schema from samples using genson.

    Args:
        samples: List of samples (dictionaries) to analyze

    Returns:
        Generated JSON Schema
    """
    try:
        builder = SchemaBuilder()
        
        # Add all samples to the builder
        for sample in samples:
            builder.add_object(sample)
        
        # Generate the schema
        schema = builder.to_schema()
        
        logger.info(f"Schema extracted deterministically with genson: {len(schema.get('properties', {}))} properties")
        return schema
        
    except Exception as e:
        logger.error(f"Error in deterministic schema extraction: {e}")
        # Return a minimal schema in case of error
        return {
            "type": "object",
            "properties": {},
            "description": "Error in schema extraction"
        }

def show_schema_options(st):
    """Show the schema extraction or import section."""
    st.subheader("🔍 Source Schema Extraction")

    # Navigation button
    if st.button("⬅️ Back to Distribution", key="back_to_distribution"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "raw_distribution_main"
        st.rerun()
    
    st.markdown("---")
    st.write("Choose how you want to obtain the source schema:")

    # Extraction options
    col_extraction1, col_extraction2 = st.columns(2)
    
    with col_extraction1:
        st.info("⚠️ LLM extraction is temporarily disabled.")
        # Kept the comment but without an active button
    
    with col_extraction2:
        if st.button("⚡ Extract Deterministically", type="primary", key="deterministic_extraction_button"):
            st.session_state.current_stage = "raw_schema_extraction"
            st.session_state.deterministic_extraction = True
            st.rerun()
        
    st.markdown("---")

def _process_deterministic_extraction(st, repos: Dict) -> bool:
    """
    Process the deterministic schema extraction using genson.
    """

    # Check for samples
    if "samples" not in st.session_state or not st.session_state.samples:
        st.error("❌ No samples available for extraction.")
        return False
    
    try:
        # Make sure the samples are in a valid format
        samples = st.session_state.samples

        # If samples is a JSON string, parse it
        if isinstance(samples, str):
            samples = json.loads(samples)
        
        # If samples is a dict with a key containing the samples
        if isinstance(samples, dict) and "samples" in samples:
            samples = samples["samples"]
        
        # Make sure samples is a list
        if not isinstance(samples, list):
            if isinstance(samples, dict):
                samples = [samples]
            else:
                st.error(f"❌ Invalid sample format: expected list or dict, received {type(samples)}")
                return False
        
        logger.info(f"Samples loaded for schema extraction: {len(samples)} samples")
        
    except Exception as e:
        st.error(f"❌ Error processing samples: {e}")
        logger.error(f"Error processing samples: {e}")
        return False
    
    # Extract schema deterministically with genson
    with st.spinner("⚡ Schema extraction in progress (genson)..."):
        try:
            generated_schema = _extract_deterministic_schema(samples)
            
            # Save to session
            st.session_state.generated_schema = generated_schema
            
            logger.info(f"Schema generated successfully: {json.dumps(generated_schema, indent=2)[:200]}...")

            # Note: we don't save to the distribution yet, we wait for user confirmation
            return True
            
        except Exception as e:
            st.error(f"❌ Error during schema extraction: {e}")
            logger.error(f"Error during schema extraction: {e}")
            return False

def _render_manual_edit_interface(st):
    """Render the manual edit interface."""
    st.markdown("---")
    st.subheader("✏️ Manual Schema Edit")
    st.write("Edit the JSON schema and confirm to complete.")
    
    # Retrieve the current schema (generated or previously modified)
    current_schema = st.session_state.get("edited_schema", st.session_state.get("generated_schema", {}))
    schema_str = json.dumps(current_schema, indent=2) if current_schema else "{}"
    
    edited_schema_str = st.text_area(
        "Schema JSON:", 
        value=schema_str, 
        height=400,
        key="manual_edit_textarea",
        help="Edit the JSON schema according to your needs"
    )

    col1, col2 = st.columns(2)
    if col1.button("✅ Confirm Changes", use_container_width=True, key="confirm_manual_edit"):
        try:
            # JSON validation
            validated_schema = json.loads(edited_schema_str)
            st.session_state.generated_schema = validated_schema
            st.session_state.edited_schema = validated_schema
            st.session_state.manual_edit_active = False
            st.session_state.validation_success = True
            st.rerun()
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            st.error(f"❌ Error: Invalid JSON - {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            st.error(f"❌ Unexpected error: {e}")

    if col2.button("❌ Cancel Edit", use_container_width=True, key="cancel_manual_edit"):
        st.session_state.manual_edit_active = False
        st.rerun()

def _render_success_interface(st, repos: Dict):
    """Render the success interface."""
    st.success("✅ Schema validated and saved successfully!")
    st.subheader("📋 Generated Schema")

    # Show schema statistics
    schema = st.session_state.generated_schema
    if "properties" in schema:
        st.info(f"📊 The schema contains {len(schema['properties'])} properties")
    
    st.json(schema)
    
    st.markdown("---")
    col1, col2 = st.columns(2)
    if col1.button("✅ Continue to Distribution", key="continue_button"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "raw_distribution_main"
        st.rerun()
    
    if col2.button("✏️ Manual Edit", key="edit_from_success"):
        st.session_state.manual_edit_active = True
        st.session_state.validation_success = False
        st.session_state.edited_schema = st.session_state.generated_schema
        st.rerun()

def _render_review_interface(st, repos: Dict):
    """Render the review interface before the final confirmation."""
    st.markdown("---")
    st.subheader("📊 Sample Preview")

    # Show a preview of the first samples
    samples = st.session_state.samples
    if isinstance(samples, list) and len(samples) > 0:
        st.write(f"**Total samples:** {len(samples)}")
        st.json(samples[:3] if len(samples) > 3 else samples, expanded=False)  # Show only the first 3
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
        st.session_state.current_stage = "raw_schema_extraction_options"
        st.rerun()