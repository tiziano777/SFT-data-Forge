# ui/mapping/manual_mapping.py
import json
import logging
from typing import Dict, List
logger = logging.getLogger(__name__)
from data_class.repository.table.mapping_repository import MappingRepository
from data_class.entity.table.mapping import Mapping
from datetime import datetime, timezone

from ui.processed.process.mapping.udf_creation_handler import show_udf_creation_step

def get_mapping_repository(st):
    """Factory function per ottenere il repository Mapping"""
    db_manager = st.session_state.get('db_manager')
    if not db_manager:
        raise ValueError("Database manager non trovato nella sessione")
    return MappingRepository(db_manager)

def validate_manual_mapping(mapping_spec: Dict, src_schema: Dict, dst_schema: Dict, samples: List[Dict]) -> tuple:
    """
    Valida un mapping manuale contro gli schemi e i campioni.
    
    Returns:
        tuple: (is_valid, mapped_samples, error_messages)
    """
    try:
        from mappings.mapper import Mapper
        mapper = Mapper(mapping_spec=mapping_spec, src_schema=src_schema, dst_schema=dst_schema)
        mapped_samples = []
        error_messages = []
        all_valid = True
        
        for i, sample in enumerate(samples):
            transformed_sample, valid_mapping, errors = mapper.apply_mapping(sample)
            mapped_samples.append(transformed_sample)

            if not valid_mapping:
                all_valid = False
                error_messages.append(f"Sample {i+1}: {', '.join(errors)}")
                logger.error(f"Validation failed for sample {i+1}: {errors}")
        
        return all_valid, mapped_samples, error_messages
        
    except Exception as e:
        logger.error(f"Errore durante la validazione del mapping: {e}")
        return False, [], [f"Errore di validazione: {str(e)}"]

def save_mapping_to_db(st, mapping_spec: Dict) -> tuple[bool, str]:
    """
    Salva il mapping nel database usando UPSERT
    Returns: (success, message)
    """
    try:
        mapping_repo = get_mapping_repository(st)
        
        # Determina se è un inserimento o aggiornamento
        existing_mappings = mapping_repo.get_by_distribution_id(
            st.session_state.current_distribution.id
        )
        
        is_update = any(
            m.schema_template_id == st.session_state.schema_template_id 
            for m in existing_mappings
        )
        
        mapping = Mapping(
            id=None,  # Il DB gestirà l'ID automaticamente
            serial=None,
            version=st.session_state.current_distribution.version,
            mapping=mapping_spec,
            distribution_id=st.session_state.current_distribution.id,
            schema_template_id=st.session_state.schema_template_id,
            issued=datetime.now(timezone.utc),
            modified=datetime.now(timezone.utc)
        )
        
        mapping_record = mapping_repo.upsert(mapping)
        
        if mapping_record:
            action = "aggiornato" if is_update else "creato"
            logger.info(f"Mapping {action} con ID: {mapping_record.id}")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Errore durante il salvataggio del mapping: {e}")
        return False

def get_template_mapping() -> Dict:
    """Restituisce un template precompilato per il mapping"""
    return json.loads("""
    {
        "_lang": ["_lang"],
        "system": ["set_fixed_value", null],
        "context": ["set_fixed_value", null],
        "_id_hash": ["_id_hash"],
        "_subpath": ["_subpath"],
        "template": ["set_fixed_value", "simple_chat"],
        "_filename": ["_filename"],
        "_dataset_name": ["_dataset_name"],
        "_dataset_path": ["_dataset_path"],
        "messages[0].role": ["set_fixed_value", "USER"],
        "messages[1].role": ["set_fixed_value", "ASSISTANT"],
        "messages[0].think": ["set_fixed_value", null],
        "messages[1].think": ["set_fixed_value", null],
        "messages[0].content": ["text"],
        "messages[0].context": ["set_fixed_value", null],
        "messages[1].content": ["answer"],
        "messages[1].context": ["set_fixed_value", null],
        "messages[0].functioncall": ["set_fixed_value", null],
        "messages[1].functioncall": ["set_fixed_value", null]
    }""")

def show_mapping_definition_step(st):
    """Step 1: Definizione del mapping manuale"""
    
    st.markdown("### 📝 Definizione del Mapping")
    
    # Informazioni sugli schemi
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Schema Sorgente**")
        if hasattr(st.session_state.current_distribution, 'src_schema'):
            st.json(st.session_state.current_distribution.src_schema, expanded=False)
        else:
            st.warning("Schema sorgente non disponibile")
    
    with col2:
        st.markdown("**Schema Destinazione**")
        if hasattr(st.session_state, 'dst_schema'):
            st.json(st.session_state.dst_schema, expanded=False)
        else:
            st.warning("Schema destinazione non disponibile")
    
    st.markdown("---")
    
    # Editor del mapping
    st.markdown("### 🎯 Definizione delle Regole di Mapping")
    
    if st.button('Definisci nuova mapping function (UDF)'):
        st.session_state.manual_current_step = "user_defined_query_option"
        st.rerun()

    # Template precompilato
    template_mapping = get_template_mapping()
    
    # Opzione 1: Editor JSON avanzato
    st.markdown("#### Opzione 1: Editor JSON")
    
    mapping_json = st.text_area(
        "Inserisci il mapping in formato JSON:",
        value=json.dumps(template_mapping, indent=2),
        height=400,
        help="Definisci le regole di mapping tra schema sorgente e destinazione. Usa il template come punto di partenza."
    )

    # Pulsante per caricare template
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Carica Mapping", use_container_width=True):
            st.session_state.mapping = json.loads(mapping_json)
            st.rerun()
    
    
    # Anteprima del mapping
    try:
        mapping_data = json.loads(mapping_json)
        st.session_state.mapping = mapping_data
        
        st.markdown("#### 👁️ Anteprima Mapping")
        st.json(mapping_data, expanded=False)
        
    except json.JSONDecodeError as e:
        st.error(f"❌ JSON non valido: {e}")
        return
    
    st.markdown("---")
    
    # Pulsanti di navigazione
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("⬅️ Torna alla Selezione Schema", use_container_width=True):
            st.session_state.current_stage = "select_target_schema"
            st.rerun()
    
    with col2:
        if st.button("🔍 Procedi alla Validazione", type="primary", use_container_width=True):
            if st.session_state.mapping:
                st.session_state.manual_current_step = "mapping_validation"
                st.rerun()
            else:
                st.error("Definisci prima il mapping")

def show_validation_step(st):
    """Step 2: Validazione del mapping"""
    
    st.markdown("### 🔍 Validazione del Mapping")
    
    if not st.session_state.mapping:
        st.error("Nessun mapping da validare")
        if st.button("⬅️ Torna alla Definizione"):
            st.session_state.manual_current_step = "mapping_definition"
            st.rerun()
        return
    
    # Mostra il mapping corrente
    st.json(st.session_state.mapping, expanded=False)
    
    # Pulsante di validazione
    if st.button("🚀 Avvia Validazione", type="primary"):
        with st.spinner("Validazione in corso..."):
            # Esegui la validazione
            is_valid, mapped_samples, error_messages = validate_manual_mapping(
                mapping_spec=st.session_state.mapping,
                src_schema=st.session_state.current_distribution.src_schema,
                dst_schema=st.session_state.dst_schema,
                samples=st.session_state.samples
            )
            
            # Salva i risultati
            st.session_state.manual_validation_results = {
                "is_valid": is_valid,
                "mapped_samples": mapped_samples,
                "error_messages": error_messages
            }
            
            st.rerun()
    
    # Mostra risultati validazione se disponibili
    if st.session_state.manual_validation_results:
        results = st.session_state.manual_validation_results
        
        st.markdown("---")
        st.markdown("#### 📊 Risultati Validazione")
        
        if results["is_valid"]:
            st.success("✅ Mapping valido!")
            
            # Mostra anteprima trasformazione
            st.markdown("##### 👁️ Anteprima Trasformazione")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Sample Originale**")
                if st.session_state.samples:
                    st.json(st.session_state.samples[0],expanded=False)
            
            with col2:
                st.markdown("**Sample Trasformato**")
                if results["mapped_samples"]:
                    st.json(results["mapped_samples"][0],expanded=False)
            
            # Pulsanti di navigazione
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("✏️ Modifica Mapping", use_container_width=True):
                    st.session_state.manual_current_step = "mapping_definition"
                    st.rerun()
            
            with col2:
                if st.button("💾 Salva e Procedi", type="primary", use_container_width=True):
                    # Salva nel database
                    if save_mapping_to_db(st, st.session_state.mapping):
                        st.session_state.manual_current_step = "mapping_results"
                        st.rerun()
                    else:
                        
                        st.error("Errore nel salvataggio del mapping")
        
        else:
            st.error("❌ Mapping non valido")
            
            # Mostra errori
            st.markdown("##### ❌ Errori di Validazione")
            for error in results["error_messages"]:
                st.error(error)
            
            # Pulsanti
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("⬅️ Torna alla Definizione", use_container_width=True):
                    st.session_state.manual_current_step = "mapping_definition"
                    st.rerun()
            
            with col2:
                if st.button("🔄 Riprova Validazione", type="secondary", use_container_width=True):
                    del st.session_state.manual_validation_results
                    st.rerun()

def show_results_step(st):
    """Step 3: Risultati finali"""
    
    st.markdown("### 🎉 Mapping Salvato con Successo!")
    
    if not st.session_state.manual_validation_results:
        st.error("Nessun risultato di validazione disponibile")
        return
    
    results = st.session_state.manual_validation_results
    
    # Confronto dettagliato
    st.markdown("#### 📊 Confronto Dettagliato")
    
    for i, (original, mapped) in enumerate(zip(st.session_state.samples, results["mapped_samples"])):
        with st.expander(f"Sample {i+1}", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Originale**")
                st.json(original,expanded=False)
            
            with col2:
                st.markdown("**Trasformato**")
                st.json(mapped,expanded=False)

    # Pulsanti finali
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🛠️ Crea Nuovo Mapping", use_container_width=True):
            # Reset per nuovo mapping
            for key in ["mapping", "manual_validation_results", "manual_current_step"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.session_state.manual_current_step = "mapping_definition"
            st.rerun()
    
    with col2:
        if st.button("📋 Modifica Questo Mapping", use_container_width=True):
            st.session_state.manual_current_step = "mapping_definition"
            st.rerun()
    
    with col3:
        if st.button("✅ Torna alla Distribuzione", type="primary", use_container_width=True):
            from utils.streamlit_func import reset_dashboard_session_state
            from config.state_vars import distribution_keys
            
            reset_dashboard_session_state(st, distribution_keys)
            st.session_state.current_stage = "processed_distribution_main"
            st.rerun()

#########################

def show_manual_mapping(st):
    """Interfaccia principale per la creazione manuale del mapping"""
    
    # Inizializzazione stati
    st.session_state.setdefault("mapping", {})
    st.session_state.setdefault("manual_validation_results", None)
    st.session_state.setdefault("manual_current_step", "mapping_definition")
    
    st.subheader("🛠️ Creazione Manuale del Mapping")
    
 
    if st.session_state.manual_current_step == "user_defined_query_option":
        show_udf_creation_step(st) 

    # Step 1: Definizione del Mapping
    if st.session_state.manual_current_step == "mapping_definition":
        show_mapping_definition_step(st)
    
    # Step 2: Validazione
    elif st.session_state.manual_current_step == "mapping_validation":
        show_validation_step(st)
    
    # Step 3: Risultati
    elif st.session_state.manual_current_step == "mapping_results":
        show_results_step(st)
