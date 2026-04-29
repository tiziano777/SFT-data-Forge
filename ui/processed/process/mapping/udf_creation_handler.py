"""
Handler per la creazione di User Defined Functions (UDF)
Gestisce l'interfaccia Streamlit per definire, validare e salvare UDF personalizzate
con supporto per parametri complessi (stringhe, numeri, liste, oggetti)
"""

from typing import Dict, Any
import json

from data_class.entity.table.udf import Udf
'''
from data_class.repository.table.udf_repository import UdfRepository
from mappings.udf import (
    validate_user_function,
    execute_user_function_safely,
    FN_PLACEHOLDER
)
'''

def _reset_udf_state(st):
    """Reset di tutti gli stati di sessione relativi alla UDF"""
    from mappings.udf import FN_PLACEHOLDER
 
    st.session_state.udf_name = ""
    st.session_state.udf_description = ""
    st.session_state.udf_example_params = []
    st.session_state.udf_example_params_initialized = False
    st.session_state.udf_function_def = FN_PLACEHOLDER
    st.session_state.udf_validation_result = None
    st.session_state.udf_execution_results = None
    st.session_state.udf_current_substep = "form"
    st.session_state.manual_current_step = "mapping_definition"

def _show_udf_form(st):
    """Mostra il form per l'inserimento dei dati della UDF"""

    st.info("💡 Definisci una funzione personalizzata che prende `func_name: str` come primo parametro e ritorna `str` o `list[str]`")

    # Campo: Nome
    with st.container():
        st.markdown("### 📝 Nome della Funzione")
        name = st.text_input(
            "Nome univoco per identificare la UDF",
            value=st.session_state.udf_name,
            placeholder="es. extract_keywords_from_text",
            help="Il nome deve essere univoco nel sistema",
            key="udf_name_input"
        )
        st.session_state.udf_name = name
    
    st.markdown("---")
    
    # Campo: Descrizione
    with st.container():
        st.markdown("### 📄 Descrizione")
        description = st.text_area(
            "Descrivi cosa fa la funzione e quando usarla",
            value=st.session_state.udf_description,
            placeholder="es. Estrae parole chiave rilevanti da un testo usando algoritmi di NLP",
            height=100,
            key="udf_description_input"
        )
        st.session_state.udf_description = description
    
    st.markdown("---")
    
    # Campo: Parametri di esempio (lista dinamica con tipo)
    with st.container():
        st.markdown("### 🧪 Parametri di Test")
        st.caption("Inserisci alcuni valori di esempio per testare la funzione. Supporta stringhe, numeri, liste e oggetti JSON.")
        
        _manage_example_params_advanced(st, st.session_state.udf_name)
    
    st.markdown("---")
    
    # Campo: Definizione della funzione
    with st.container():
        st.markdown("### 💻 Definizione della Funzione Python")
        st.caption("⚠️ Requisiti: primo parametro `func_name: str`, ritorno `list[str]`")
        
        function_def = st.text_area(
            "Inserisci il codice Python della funzione",
            value=st.session_state.udf_function_def,
            height=300,
            help="La funzione deve essere sintatticamente corretta e rispettare i requisiti",
            key="udf_function_def_input"
        )
        st.session_state.udf_function_def = function_def
        
        # Mostra esempio
        with st.expander("📚 Vedi esempio di funzione valida con parametri multipli"):
            st.code('''
def process_query(func_name: str, max_results: int = 10, filters: list = None, config: dict = None) -> list[str]:
    """
    Esempio di funzione con parametri di tipi diversi
    """
    results = []
    
    # Usa il primo parametro obbligatorio
    words = func_name.split()
    
    # Usa parametri opzionali
    if filters:
        words = [w for w in words if w not in filters]
    
    if config and 'uppercase' in config and config['uppercase']:
        words = [w.upper() for w in words]
    
    # Limita i risultati
    for word in words[:max_results]:
        results.append(word)
    
    return results
''', language='python')
    
    st.markdown("---")
    
    # Bottoni di azione
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("⬅️ Torna Indietro", use_container_width=True):
            _reset_udf_state(st)
            st.session_state.current_stage = "select_target_schema"
            st.rerun()
    
    with col2:
        if st.button("🔄 Reset Form", use_container_width=True):
            _reset_udf_state(st)
            st.rerun()
    
    with col3:
        if st.button("✅ Valida Funzione", type="primary", use_container_width=True):
            if _validate_form_inputs(st):
                st.session_state.udf_current_substep = "validation"
                st.rerun()

def _manage_example_params_advanced(st, name: str):
    """Gestisce l'inserimento di parametri con tipi complessi (str, int, float, list, dict)"""
    
    st.markdown("**Aggiungi nuovo parametro:**")
    # Initialize example params only once. Do not auto-add if name is empty
    if 'udf_example_params_initialized' not in st.session_state:
        st.session_state.udf_example_params_initialized = False

    if not st.session_state.udf_example_params_initialized:
        if name and name.strip():
            if not st.session_state.udf_example_params:
                st.session_state.udf_example_params.append({
                    'type': 'str',
                    'value': name,
                    'display': _format_param_display('str', name)
                })
        # Mark as initialized so user deletions won't be auto-recreated
        st.session_state.udf_example_params_initialized = True


    col1, col2, col3 = st.columns([2, 3, 1])
    
    with col1:
        param_type = st.selectbox(
            "Tipo",
            options=["str", "int", "float", "list", "dict"],
            key="new_param_type",
            label_visibility="collapsed",
            help="Seleziona il tipo di dato del parametro"
        )
    
    with col2:
        if param_type in ["str", "int", "float"]:
            new_param_value = st.text_input(
                "Valore",
                placeholder=f"es. {'testo' if param_type == 'str' else '42' if param_type == 'int' else '3.14'}",
                key="new_param_value",
                label_visibility="collapsed"
            )
        elif param_type == "list":
            new_param_value = st.text_input(
                "Valore",
                placeholder='es. ["item1", "item2", 3, 4.5]',
                key="new_param_value",
                label_visibility="collapsed",
                help="Inserisci una lista in formato JSON"
            )
        else:  # dict
            new_param_value = st.text_input(
                "Valore",
                placeholder='es. {"key": "value", "number": 42}',
                key="new_param_value",
                label_visibility="collapsed",
                help="Inserisci un oggetto in formato JSON"
            )
    
    with col3:
        if st.button("➕", use_container_width=True, help="Aggiungi parametro"):
            if new_param_value and new_param_value.strip():
                # Valida e converte il valore
                parsed_value = _parse_param_value(param_type, new_param_value.strip())
                
                if parsed_value is not None:
                    param_entry = {
                        'type': param_type,
                        'value': parsed_value,
                        'display': _format_param_display(param_type, parsed_value)
                    }
                    st.session_state.udf_example_params.append(param_entry)
                    st.rerun()
                else:
                    st.error(f"❌ Valore non valido per tipo '{param_type}'")
    
    # Mostra lista parametri esistenti
    if st.session_state.udf_example_params:
        st.markdown("---")
        st.markdown("**Parametri di test correnti:**")
        
        for idx, param in enumerate(st.session_state.udf_example_params):
            col1, col2, col3 = st.columns([1, 4, 1])
            
            with col1:
                st.markdown(f"**{param['type']}**")
            
            with col2:
                st.text(f"{param['display']}")
            
            with col3:
                if st.button("🗑️", key=f"delete_param_{idx}"):
                    st.session_state.udf_example_params.pop(idx)
                    st.rerun()
    else:
        st.info("➕ Nessun parametro di test inserito. Aggiungi almeno un set di parametri per testare la funzione.")

def _parse_param_value(param_type: str, value_str: str) -> Any:
    """
    Converte una stringa nel tipo appropriato
    
    Returns:
        Il valore convertito o None se la conversione fallisce
    """
    try:
        if param_type == "str":
            return value_str
        
        elif param_type == "int":
            return int(value_str)
        
        elif param_type == "float":
            return float(value_str)
        
        elif param_type == "list":
            # Prova a parsare come JSON
            parsed = json.loads(value_str)
            if isinstance(parsed, list):
                return parsed
            return None
        
        elif param_type == "dict":
            # Prova a parsare come JSON
            parsed = json.loads(value_str)
            if isinstance(parsed, dict):
                return parsed
            return None
        
        return None
    
    except (ValueError, json.JSONDecodeError):
        return None

def _format_param_display(param_type: str, value: Any) -> str:
    """Formatta il valore per la visualizzazione"""
    if param_type in ["list", "dict"]:
        return json.dumps(value, ensure_ascii=False)
    return str(value)

def _validate_form_inputs(st) -> bool:
    """Valida che tutti i campi obbligatori siano compilati"""

    from mappings.udf import FN_PLACEHOLDER
    
    errors = []
    
    if not st.session_state.udf_name.strip():
        errors.append("❌ Il nome della funzione è obbligatorio")
    
    if not st.session_state.udf_function_def.strip() or st.session_state.udf_function_def == FN_PLACEHOLDER:
        errors.append("❌ La definizione della funzione è obbligatoria")
    
    if not st.session_state.udf_example_params:
        errors.append("❌ Inserisci almeno un set di parametri di test")
    
    if errors:
        for error in errors:
            st.error(error)
        return False
    
    return True

def _show_validation_results(st):
    """Mostra i risultati della validazione e dei test sulla funzione"""

    from mappings.udf import validate_user_function, execute_user_function_safely

    st.markdown("### 🔍 Validazione della Funzione")
    
    # Esegui validazione
    with st.spinner("⏳ Validazione in corso..."):
        validation_result = validate_user_function(
            st.session_state.udf_function_def,
            func_name=None  # Auto-detect
        )
        st.session_state.udf_validation_result = validation_result
    
    # Mostra risultati validazione
    if validation_result['is_valid']:
        st.success(f"✅ Funzione `{validation_result['function_name']}` validata con successo!")
        
        # Mostra eventuali warning
        if validation_result['warnings']:
            with st.expander("⚠️ Avvisi (non bloccanti)"):
                for warning in validation_result['warnings']:
                    st.warning(warning)
        
        # Esegui test con parametri di esempio
        st.markdown("---")
        st.markdown("### 🧪 Test con Parametri di Esempio")
        
        execution_results = []
        all_tests_passed = True
        
        # Raggruppa tutti i parametri per creare un singolo test
        if st.session_state.udf_example_params:
            with st.expander(f"📊 Test con {len(st.session_state.udf_example_params)} parametri"):
                # Prepara i parametri per l'esecuzione
                test_params = _prepare_execution_params(st.session_state.udf_example_params)
                
                st.markdown("**Parametri passati alla funzione:**")
                st.json(test_params)
                
                result = execute_user_function_safely(
                    st.session_state.udf_function_def,
                    validation_result['function_name'],
                    test_params
                )
                
                execution_results.append({
                    'all_params': st.session_state.udf_example_params,
                    'params_dict': test_params,
                    'result': result
                })
                
                if result['success']:
                    st.success(f"✅ Esecuzione completata in {result['execution_time']:.3f}s")
                    st.markdown("**Risultato:**")
                    
                    # CORREZIONE: Gestione differenziata per lista vs stringa
                    if result.get('return_type') == 'str' or isinstance(result['result'], str):
                        # Se è una stringa, usiamo st.code() o st.text() invece di st.json()
                        st.markdown("**Tipo: Stringa**")
                        
                        # Opzione 1: Visualizza come codice (mantiene formattazione)
                        st.code(result['result'], language='text')
                        # Opzione 2: Visualizza come testo normale (con apici)
                        # st.text(f'"{result["result"]}"')
                        # Opzione 3: Visualizza in un box con sintassi evidenziata
                        # st.markdown(f'```\n{result["result"]}\n```')
                    else:
                        # Se è una lista, usa st.json()
                        st.markdown("**Tipo: Lista di stringhe**")
                        st.json(result['result'])
                else:
                    st.error(f"❌ Errore: {result['error']}")
                    all_tests_passed = False

        st.session_state.udf_execution_results = execution_results
        
        # Bottoni di navigazione
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("⬅️ Modifica Funzione", use_container_width=True):
                st.session_state.udf_current_substep = "form"
                st.rerun()
        
        with col2:
            if all_tests_passed:
                if st.button("✅ Conferma e Salva", type="primary", use_container_width=True):
                    st.session_state.udf_current_substep = "confirmation"
                    st.rerun()
            else:
                st.button("❌ Alcuni test falliti", disabled=True, use_container_width=True)
                st.error("⚠️ Correggi gli errori prima di procedere")
    
    else:
        # Validazione fallita
        st.error("❌ La funzione contiene errori")
        
        st.markdown("**Errori riscontrati:**")
        for error in validation_result['errors']:
            st.error(error)
        
        if validation_result['warnings']:
            st.markdown("**Avvisi:**")
            for warning in validation_result['warnings']:
                st.warning(warning)
        
        # Bottone per tornare indietro
        st.markdown("---")
        if st.button("⬅️ Modifica Funzione", use_container_width=True):
            st.session_state.udf_current_substep = "form"
            st.rerun()

def _prepare_execution_params(param_sets: list[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Prepara i parametri per l'esecuzione della funzione.
    Il primo set DEVE essere di tipo 'str' e diventerà 'func_name'.
    Gli altri set diventano parametri aggiuntivi con nomi generati.
    
    Args:
        param_sets: Lista di dizionari con 'type' e 'value'
    
    Returns:
        Dict con 'func_name' come prima chiave + parametri aggiuntivi
    """
    if not param_sets:
        return {'func_name': 'default_query'}
    
    # Il primo parametro DEVE essere stringa e diventa func_name
    first_param = param_sets[0]
    params = {
        'func_name': first_param['value'] if first_param['type'] == 'str' else str(first_param['value'])
    }
    
    # Parametri aggiuntivi (se presenti)
    for idx, param_set in enumerate(param_sets[1:], start=1):
        param_name = f"param_{idx}"  # Genera nome: param_1, param_2, etc.
        params[param_name] = param_set['value']
    
    return params

def _show_confirmation_and_save(st):
    """Mostra riepilogo finale e salva/aggiorna la UDF nel database"""
    
    st.markdown("### 📋 Riepilogo UDF")
    
    # Riepilogo dati
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Nome:**")
            st.code(st.session_state.udf_name)
            
            st.markdown("**Funzione:**")
            st.code(st.session_state.udf_validation_result['function_name'])
        
        with col2:
            st.markdown("**Descrizione:**")
            st.info(st.session_state.udf_description or "Nessuna descrizione")
            
            st.markdown("**Numero parametri di test:**")
            st.write(f"{len(st.session_state.udf_example_params)} set di parametri")
            
            # Mostra se siamo in modalità modifica
            if st.session_state.get('udf_edit_mode'):
                st.info(f"📝 **Modalità Modifica** - ID: `{st.session_state.get('udf_edit_id')}`")
    
    with st.expander("💻 Codice della funzione"):
        st.code(st.session_state.udf_function_def, language='python')
    
    with st.expander("🧪 Risultati dei test"):
        for idx, test_result in enumerate(st.session_state.udf_execution_results):
            st.markdown(f"**Test #{idx + 1}**")
            st.markdown("**Parametri utilizzati:**")
            
            # Mostra tutti i parametri del test
            for param in test_result['all_params']:
                st.text(f"  • {param['type']}: {param['display']}")
            
            st.markdown("**Dizionario parametri:**")
            st.json(test_result['params_dict'])
            
            st.markdown("**Risultato:**")
            st.code(test_result['result']['result'])
            st.markdown("---")
    
    # Bottoni finali
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("⬅️ Torna Indietro", use_container_width=True):
            st.session_state.udf_current_substep = "validation"
            st.rerun()
    
    with col2:
        # Bottone per salvare/aggiornare con upsert
        button_text = "💾 Salva Modifiche" if st.session_state.get('udf_edit_mode') else "💾 Salva UDF"
        
        if st.button(button_text, type="primary", use_container_width=True):
            # Prepara i parametri di esempio per il salvataggio
            example_params = []
            for param in st.session_state.udf_example_params:
                param_dict = {
                    'type': param['type'],
                    'value': param['value']
                }
                example_params.append(json.dumps(param_dict))
            
            # Crea oggetto Udf
            from datetime import datetime, timezone 
            from data_class.entity.table.udf import Udf

            udf_to_save = Udf(
                id=st.session_state.get('udf_edit_id'),
                name=st.session_state.udf_name,
                description=st.session_state.udf_description,
                function_definition=st.session_state.udf_function_def,
                example_params=example_params,
                issued=st.session_state.get('udf_original_issued') or datetime.now(timezone.utc),
                modified=datetime.now(timezone.utc)
            )
            
            # Esegui UPSERT
            with st.spinner("💾 Salvataggio in corso..."):
                try:
                    from data_class.entity.table.udf import Udf
                    from data_class.repository.table.udf_repository import UdfRepository
    
                    db_manager = st.session_state.db_manager
                    udf_repo = UdfRepository(db_manager)
                    
                    result = udf_repo.upsert(udf_to_save)
                    
                    if result:
                        action = "aggiornata" if st.session_state.get('udf_edit_mode') else "creata"
                        st.success(f"✅ UDF '{st.session_state.udf_name}' {action} con successo!")
                        
                        st.session_state['udfs_need_reload'] = True
                        # Reset stato e redirect
                        _reset_udf_state(st)
                        st.session_state.current_stage = "select_target_schema"
                        st.rerun()
                    else:
                        st.error("❌ Errore durante il salvataggio della UDF.")
                        
                except Exception as e:
                    st.error(f"❌ Errore durante il salvataggio: {str(e)}")
                    import traceback
                    st.error("**Dettagli errore:**")
                    st.code(traceback.format_exc())
    
    with col3:
        if st.button("❌ Annulla", use_container_width=True):
            # Annulla tutto e torna alla lista
            _reset_udf_state(st)
            st.session_state.udf_current_substep = "list"
            st.rerun()

## PIPE 2: GESTIONE UDF ESISTENTI

# ============================================================================
#                          PIPE2: GESTIONE UDF ESISTENTI
# ============================================================================

def _show_udf_list(st):
    """Mostra la lista delle UDF esistenti con ricerca e azioni"""
    from data_class.repository.table.udf_repository import UdfRepository

    st.markdown("### 📚 Gestione UDF Esistenti")
    st.markdown("---")
    
    # Carica tutte le UDF dal database
    db_manager = st.session_state.db_manager
    udf_repo = UdfRepository(db_manager)
    
    try:
        with st.spinner("📥 Caricamento UDF dal database..."):
            all_udfs = udf_repo.get_all()
        
        if not all_udfs:
            st.info("📭 Nessuna UDF trovata nel database")
            st.markdown("---")
            col1, col2 = st.columns([1, 2])
            with col1:
                if st.button("🔄 Definisci Nuova UDF",key="define_new_udf_from_list", use_container_width=True):
                    st.session_state.udf_current_substep = "form"
                    st.rerun()
            with col2:
                if st.button("⬅️ Torna Indietro al mapping", key="back_to_mapping_from_empty_udf", use_container_width=True):
                    _reset_udf_state(st)
                    st.session_state.current_stage = "select_target_schema"
                    st.rerun()
            return
        
        # Barra di ricerca
        st.markdown("#### 🔍 Ricerca UDF")
        search_query = st.text_input(
            "Cerca per nome",
            placeholder="Inserisci il nome o parte del nome della UDF",
            key="udf_search_query"
        )
        
        # Filtra UDF in base alla query di ricerca
        if search_query and search_query.strip():
            filtered_udfs = [
                udf for udf in all_udfs 
                if search_query.lower() in udf.name.lower()
            ]
        else:
            filtered_udfs = all_udfs
        
        st.markdown(f"**Risultati:** {len(filtered_udfs)} UDF trovate")
        st.markdown("---")
        
        # Mostra lista UDF
        if not filtered_udfs:
            st.warning("🔍 Nessuna UDF corrisponde alla ricerca")
        else:
            for udf in filtered_udfs:
                _show_udf_card(st, udf)
        
        # Bottone per tornare indietro
        st.markdown("---")
        col1, col2 = st.columns([1, 1])
        
        with col1:
            if st.button("⬅️ Torna alla Creazione", use_container_width=True):
                st.session_state.udf_current_substep = "form"
                st.rerun()
        
        with col2:
            if st.button("🔄 Ricarica Lista", use_container_width=True):
                st.rerun()
    
    except Exception as e:
        st.error(f"❌ Errore durante il caricamento delle UDF: {str(e)}")
        st.exception(e)

def _load_udf_for_edit(st, udf: Udf):
    """Carica i dati di una UDF nello stato per la modifica"""
    
    # Imposta modalità edit
    st.session_state.udf_edit_mode = True
    st.session_state.udf_edit_id = udf.id
    st.session_state.udf_original_issued = udf.issued
    
    # Carica dati nel form
    st.session_state.udf_name = udf.name
    st.session_state.udf_description = udf.description or ""
    st.session_state.udf_function_def = udf.function_definition
    
    # Carica parametri di esempio
    example_params = []
    if udf.example_params:
        for param_json in udf.example_params:
            try:
                param = json.loads(param_json)
                example_params.append({
                    'type': param['type'],
                    'value': param['value'],
                    'display': _format_param_display(param['type'], param['value'])
                })
            except json.JSONDecodeError:
                pass
    
    st.session_state.udf_example_params = example_params
    # When loading an existing UDF, consider params initialized to avoid overwriting
    st.session_state.udf_example_params_initialized = True
    
    # Reset altri stati
    st.session_state.udf_validation_result = None
    st.session_state.udf_execution_results = None
    
    # Vai al form
    st.session_state.udf_current_substep = "form"

def _show_udf_card(st, udf: Udf):
    """Mostra una card con i dettagli di una singola UDF e azioni disponibili"""

    with st.expander(f"📦 **{udf.name}**", expanded=False):
        # Informazioni principali
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.markdown(f"**Descrizione:**")
            st.info(udf.description if udf.description else "Nessuna descrizione")
            
            st.markdown(f"**ID:** `{udf.id}`")
            st.markdown(f"**Creata:** {udf.issued.strftime('%Y-%m-%d %H:%M:%S') if udf.issued else 'N/A'}")
            st.markdown(f"**Modificata:** {udf.modified.strftime('%Y-%m-%d %H:%M:%S') if udf.modified else 'N/A'}")
        
        with col2:
            st.markdown("**Azioni:**")
            
            # Bottone Modifica
            if st.button("✏️", key=f"edit_{udf.id}", help="Modifica UDF", use_container_width=True):
                _load_udf_for_edit(st, udf)
                st.rerun()
            
            # Bottone Elimina - Usa un form per gestire il popup di conferma
            delete_key = f"delete_btn_{udf.id}"
            if st.button("🗑️", key=delete_key, help="Elimina UDF", use_container_width=True, type="secondary"):
                # Mostra popup di conferma
                st.session_state[f'delete_confirm_{udf.id}'] = True
        
        # Popup di conferma eliminazione
        if st.session_state.get(f'delete_confirm_{udf.id}'):
            st.markdown("---")
            
            # Container per il popup
            with st.container():
                st.warning("⚠️ **Conferma Eliminazione**")
                st.markdown(f"Sei sicuro di voler eliminare la UDF **`{udf.name}`**?")
                st.markdown("**Questa azione è irreversibile.**")
                
                col1, col2, col3 = st.columns([1, 1, 2])
                
                with col1:
                    if st.button("✅ Conferma", 
                                key=f"confirm_delete_{udf.id}", 
                                type="primary", 
                                use_container_width=True):
                        # Esegui eliminazione
                        success = _delete_udf(st, udf.id, udf.name)
                        if success:
                            # Reset stato e refresh
                            st.session_state[f'delete_confirm_{udf.id}'] = False
                            st.rerun()
                
                with col2:
                    if st.button("❌ Annulla", 
                                key=f"cancel_delete_{udf.id}", 
                                use_container_width=True):
                        st.session_state[f'delete_confirm_{udf.id}'] = False
                        st.rerun()
        
        # Mostra dettagli funzione
        with st.expander("💻 Definizione Funzione"):
            st.code(udf.function_definition, language='python')
        
        # Mostra parametri di esempio
        with st.expander("🧪 Parametri di Test"):
            if udf.example_params:
                for idx, param_json in enumerate(udf.example_params):
                    try:
                        param = json.loads(param_json)
                        st.text(f"{idx + 1}. {param['type']}: {_format_param_display(param['type'], param['value'])}")
                    except json.JSONDecodeError:
                        st.text(f"{idx + 1}. {param_json}")
            else:
                st.info("Nessun parametro di test definito")

def _delete_udf(st, udf_id: str, udf_name: str) -> bool:
    """Elimina una UDF dal database. Ritorna True se successo."""
    from data_class.repository.table.udf_repository import UdfRepository

    db_manager = st.session_state.db_manager
    try:
        udf_repo = UdfRepository(db_manager)
        
        # Mostra spinner durante l'operazione
        with st.spinner(f"🗑️ Eliminazione di '{udf_name}' in corso..."):
            rows_deleted = udf_repo.delete(udf_id)
        
        if rows_deleted > 0:
            # Mostra messaggio di successo temporaneo
            st.toast(f"✅ UDF '{udf_name}' eliminata con successo!", icon="✅")
            return True
        else:
            st.error("❌ Nessuna riga eliminata. UDF non trovata.")
            return False
    
    except Exception as e:
        st.error(f"❌ Errore durante l'eliminazione: {str(e)}")
        # Opzionale: log dettagliato per debug
        import traceback
        st.error("**Dettagli errore:**")
        st.code(traceback.format_exc())
        return False
    
# MAIN HANDLER FUNCTION

def show_udf_creation_step(st):
    """
    Interfaccia completa per la creazione di una nuova User Defined Function
    
    Args:
        st: modulo streamlit
        db_manager: istanza di PostgresDBManager per accesso al database
    """
    from mappings.udf import FN_PLACEHOLDER

    # Inizializzazione stati di sessione per UDF
    st.session_state.setdefault("udf_name", "")
    st.session_state.setdefault("udf_description", "")
    st.session_state.setdefault("udf_example_params", [])  # Lista di dict con {type, value}
    st.session_state.setdefault("udf_function_def", FN_PLACEHOLDER)
    st.session_state.setdefault("udf_validation_result", None)
    st.session_state.setdefault("udf_execution_results", None)
    st.session_state.setdefault("udf_current_substep", "form")  # form, validation, confirmation
    
    st.subheader("🔧 Crea Nuova User Defined Query")
    st.markdown("---")

    col1, col2 = st.columns([1, 2])
    with col1:
        if st.button("⬅️ Torna Indietro al mapping"):
            _reset_udf_state(st)
            st.session_state.current_stage = "select_target_schema"
            st.rerun()
    with col2:
        # Pulsante per gestire UDF esistenti (solo se non in modalità edit o lista)
        if st.session_state.udf_current_substep not in ["list"]:
            if st.button("📚 Gestisci UDF Esistenti", use_container_width=True):
                st.session_state.udf_current_substep = "list"
                st.rerun()
    st.markdown("---")

    # Pipe1: Substep 1: Form di inserimento dati
    if st.session_state.udf_current_substep == "form":
        _show_udf_form(st)
    
    # Pipe1: Substep 2: Validazione e test
    elif st.session_state.udf_current_substep == "validation":
        _show_validation_results(st)
    
    # Pipe1: Substep 3: Conferma e salvataggio
    elif st.session_state.udf_current_substep == "confirmation":
        _show_confirmation_and_save(st)

    
    # Pipe2: Lista e gestione UDF esistenti
    elif st.session_state.udf_current_substep == "list":
        _show_udf_list(st)

