from utils.streamlit_func import reset_dashboard_session_state
import logging
from data_class.repository.table.schema_template_repository import SchemaTemplateRepository

logger = logging.getLogger(__name__)

keys_to_delete = ["pipeline_running", "manual_edit_active", "interrupt", "state", "thread_id_mapping","schema_template_id","distribution_id","mapping","dst_schema","back_to_src_schema_btn","target_schema_file_select","selected_target_schema_file", "schema_sample", "selected_languages", "languages_valid"]


def get_schema_template_repository(st):
    """Factory function per ottenere il repository SchemaTemplate"""
    db_manager = st.session_state.get('db_manager')
    if not db_manager:
        raise ValueError("Database manager non trovato nella sessione")
    return SchemaTemplateRepository(db_manager)


# --- STAGE 1: SELEZIONE DELLO SCHEMA DI DESTINAZIONE ---

def select_target_schema_stage(st):
    """Mostra la sezione di selezione del target schema e gestisce il cambio."""
    st.subheader("1️⃣ Seleziona Schema di Destinazione")
    st.write("Seleziona uno schema JSON esistente da usare come destinazione per la trasformazione.")
    
    # 1. Carica gli schemi disponibili usando il repository
    schema_repo = get_schema_template_repository(st)
    all_schemas = schema_repo.find_all()
    
    # Converti in formato compatibile con la logica esistente
    schemas = []
    names = []
    for schema in all_schemas:
        schemas.append({
            'id': schema.id,
            'serial': schema.serial,
            'name': schema.name,
            'schema': schema.schema
        })
        names.append(schema.name)
    
    logger.info("Available schemas: %s", schemas)

    if not schemas:
        st.error("Nessun file schema JSON trovato.")
        reset_dashboard_session_state(st, keys_to_delete)
        st.session_state.current_stage = "distribution_main"
        st.rerun()
        return False

    previous_dst_schema_id = st.session_state.get("dst_schema_id") # Memorizza l'ID precedente
    
    selected_name = st.selectbox(
        "Seleziona un file schema di destinazione:",
        options=[""] + names,
        key="target_schema_file_select",
        index=(names.index(st.session_state.get("selected_target_schema_file")) + 1) if st.session_state.get("selected_target_schema_file") in names else 0
    )
    st.session_state.selected_target_schema_file = selected_name

    # 3. Gestione del cambio di schema
    selected_schema_data = next((s for s in schemas if s['name'] == selected_name), None)

    if selected_name and selected_schema_data:
        current_dst_schema_id = selected_schema_data['id']
        st.session_state.dst_schema_id = current_dst_schema_id
        st.session_state.dst_schema = selected_schema_data['schema'] # Salva il JSON completo

        # 4. Visualizzazione e passaggio di stato
        st.markdown("---")
        st.subheader("Anteprima dello Schema di Destinazione Selezionato")
        st.markdown(f"**Schema Template ID:** {st.session_state.dst_schema_id}")
        st.json(st.session_state.dst_schema, expanded=False)
        
        return True 
    else:
        st.session_state.dst_schema_id = None
        st.session_state.selected_target_schema_file = ""
        st.warning("Per favore, seleziona uno schema per continuare.")
        return False