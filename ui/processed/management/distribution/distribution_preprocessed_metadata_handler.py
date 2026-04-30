# ui/distribution/distribution_metadata_handler.py
import tokenize
import uuid
import json
import traceback
import ast
import logging
from typing import Optional, Dict

from config.state_vars import distribution_keys
from utils.streamlit_func import reset_dashboard_session_state
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.vocabulary.vocab_license_repository import VocabLicenseRepository
from data_class.repository.vocabulary.vocab_distribution_split_repository import VocabDistributionSplitRepository
from data_class.entity.table.distribution import Distribution

from langgraph.types import Command 

# Configurazione logger
logger = logging.getLogger(__name__)

def _initialize_repositories(st_app):
    """Inizializza i repository necessari."""
    return {
        'distribution': DistributionRepository(st_app.session_state.db_manager),
        'dataset': DatasetRepository(st_app.session_state.db_manager),
        'license': VocabLicenseRepository(st_app.session_state.db_manager),
        'distribution_split': VocabDistributionSplitRepository(st_app.session_state.db_manager) 
    }

def _load_vocabulary_options(repo, default_value: Optional[str] = None) -> tuple:
    """Carica le opzioni da un repository di vocabolario."""
    try:
        items = repo.get_all()
        options = [item.code for item in items]
        default_index = options.index(default_value) if default_value in options else 0
        return options, default_index
    except Exception as e:
        logger.error(f"Errore nel caricamento vocabolario: {e}")
        return [], 0

def _render_metadata_form(st, distribution: Distribution, vocab_options: Dict):
    """Renderizza il form per l'editing dei metadati."""
    with st.form("metadata_form", clear_on_submit=False):
        st.subheader("📋 Informazioni principali")
        name = st.text_input("Nome *", value=distribution.name, key="name_input")
        tokenized_uri = st.text_input("URI Tokenizzato", value=distribution.tokenized_uri or "", key="tokenized_uri_input")
        description = st.text_area("Descrizione", value=distribution.description or "", 
                                 height=150, key="description_input")

        version = st.text_input("Versione *", value=distribution.version, key="version_input")

        # Read-only display of generation provenance
        query_display = st.text_area("Query", value=distribution.query or "", height=80, key="query_display", disabled=True)
        script_display = st.text_area("Script", value=distribution.script or "", height=80, key="script_display", disabled=True)

        split_options, split_default_index = vocab_options.get('distribution_split', ([], 0))
        if split_options:
            distribution_split = st.selectbox(
                "Tipo di suddivisione",
                options=split_options,
                index=split_default_index,
                key="split_select"
            )
        else:
            distribution_split = st.text_input(
                "Tipo di suddivisione",
                value=distribution.split or "unknown",
                key="split_input"
            )

        st.subheader("Language")

        dataset = DatasetRepository(db_manager=st.session_state.db_manager).get_by_id(id=distribution.dataset_id)
        languages = dataset.languages if dataset and dataset.languages else []
        lang = st.selectbox(
            "Lingua (codice ISO 639-1 o ISO 639-3)", 
            options=languages,
            index=languages.index(distribution.lang) if distribution.lang in languages else 0,
            key="lang_input"
        )

        st.subheader("🏷️ Tags")

        # Tags
        tags_str = st.text_area(
            "Tag (separati da virgola)", 
            value=",".join(distribution.tags) if distribution.tags else "",
            key="tags_input"
        )

        st.subheader("📄 Licenza e note")
        
        # Licenza
        license_options, license_default_index = vocab_options.get('license', ([], 0))
        if license_options:
            license_value = st.selectbox(
                "Licenza *", 
                options=license_options,
                index=license_default_index,
                key="license_select"
            )
        else:
            license_value = st.text_input(
                "Licenza *", 
                value=distribution.license or "unknown",
                key="license_input"
            )

        # Pulsanti di azione
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            submit = st.form_submit_button("💾 Salva modifiche", key="save_button")
        with col2:
            st.info("⚙️ La generazione automatica dei metadati è temporaneamente disabilitata.")
            #gen = st.form_submit_button("⚙️  automaticamente", key="generate_button", disabled=True)
        with col3:
            reset = st.form_submit_button("🔄 Reset", key="reset_button")

        return {
            'submitted': submit,
            #'generated': gen,
            'reset': reset,
            'name': name,
            'description': description,
            'lang': lang,
            'split': distribution_split,
            'version': version,
            'tokenized_uri': tokenized_uri,
            'tags_str': tags_str,
            'license': license_value
        }

def _update_distribution_metadata(st, distribution: Distribution, form_data: Dict, repos: Dict):
    """Aggiorna i metadati della distribution nel database."""
    try:
        # Parsing tags
        tags = [t.strip() for t in form_data['tags_str'].split(",") if t.strip()]
        
        # Aggiorna l'entità distribution
        distribution.name = form_data['name']
        distribution.description = form_data['description']
        distribution.lang = form_data['lang']
        distribution.version = form_data['version']
        distribution.tags = tags
        distribution.license = form_data['license']
        distribution.split = form_data['split']
        distribution.tokenized_uri = form_data['tokenized_uri']
    
        # Salva nel database
        rows_affected = repos['distribution'].update(distribution)
        
        if rows_affected > 0:
            # Ricarica la distribution aggiornata
            updated_distribution = repos['distribution'].get_by_id(distribution.id)
            st.session_state.current_distribution = updated_distribution
            st.success("✅ Metadati aggiornati con successo.")
            return True
        else:
            st.error("❌ Nessuna modifica apportata al dataset.")
            return False

    except Exception as e:
        st.error(f"❌ Errore durante l'aggiornamento nel DB: {e}")
        logger.error(f"Errore nell'aggiornamento distribution: {e}")
        logger.error(traceback.format_exc())
        return False

def show_metadata_editor(st):
    """Form di inserimento e modifica dei metadati di una distribution."""
    
    st.title("🧩 Editor dei Metadati della Distribution")

    # Verifica che la distribution corrente sia presente
    if not hasattr(st.session_state, 'current_distribution') or not st.session_state.current_distribution:
        st.error("❌ Nessuna distribution selezionata.")
        if st.button("📂 Vai alla Selezione Distribution", key="go_to_selection"):
            st.session_state.current_stage = "processed_distribution_selection"
            st.rerun()
        return

    # Inizializzazione repository
    repos = _initialize_repositories(st)
    distribution = st.session_state.current_distribution

    # Carica opzioni dai vocabolari 
    vocab_options = {
        'license': _load_vocabulary_options(
            repos['license'], 
            distribution.license
        ),
        'distribution_split': _load_vocabulary_options(
            repos['distribution_split'],
            distribution.split
        )
    }

    # Render del form
    form_result = _render_metadata_form(st, distribution, vocab_options)

    # Gestione azioni del form
    if form_result['submitted']:
        # Validazione campi obbligatori
        if not form_result['name'] or not form_result['version'] or not form_result['license']:
            st.error("⚠️ I campi contrassegnati con * sono obbligatori.")
        else:
            success = _update_distribution_metadata(st, distribution, form_result, repos)
            if success:
                st.rerun()
    elif form_result['reset']:
        # Reset del form
        st.rerun()
    '''
    elif form_result['generated']:
        # Avvia la generazione automatica
        st.info("🚀 Avvio della pipeline di generazione automatica dei metadati...")
        if "thread_id_metadata" not in st.session_state:
            st.session_state.thread_id_metadata = str(uuid.uuid4())
        st.session_state.current_stage = "processed_metadata_generation"
        st.rerun()
    '''

    # Pulsante di navigazione
    if st.button("⬅️ Torna alla Distribution", use_container_width=True, key="back_button"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "processed_distribution_main"
        st.rerun()

# Le restanti funzioni rimangono invariate
def show_metadata_generation(st, langfuse_handler):
    """Mostra la sezione di generazione automatica dei metadati."""
    
    st.subheader("🤖 Generazione Automatica Metadati")
    st.write("Genera automaticamente i metadati chiave per il tuo dataset utilizzando l'AI.")

    # Inizializzazione repository
    repos = _initialize_repositories(st)

    # Inizializzazione stati di sessione
    st.session_state.setdefault("pipeline_running", False)
    st.session_state.setdefault("manual_edit_active", False)
    st.session_state.setdefault("interrupt", None)

    def resume_metadata_pipeline(decision: dict):
        """Invia una decisione alla pipeline che gestisce la sua risposta."""
        config = {
            "configurable": {"thread_id": st.session_state.thread_id_metadata}, 
            "callbacks": [langfuse_handler]
        }
        
        try:
            with st.spinner("La pipeline di metadata sta elaborando..."):
                result = st.session_state.metadata_graph.invoke(
                    Command(resume=decision), config=config
                )

            # Gestione interrupt
            if "__interrupt__" in result:
                logger.info("La pipeline richiede un'altra revisione.")
                st.session_state.interrupt = result["__interrupt__"]
                st.session_state.state = result
                st.session_state.manual_edit_active = False
            else:
                # Pipeline completata con successo
                st.success("✅ Metadati approvati e salvati con successo!")
                st.balloons()
                st.session_state.interrupt = None
                st.session_state.pipeline_running = False
                st.session_state.manual_edit_active = False
                st.session_state.state = result
                
                # Salva i metadati generati nel database
                _save_generated_metadata(st, result, repos)

        except Exception as e:
            logger.error(f"Errore durante la ripresa della pipeline: {e}")
            st.error(f"❌ Errore durante la ripresa della pipeline: {e}")

        st.rerun()

    def _save_generated_metadata(st, result, repos):
        """Salva i metadati generati dalla pipeline nel database."""
        try:
            output_metadata = {}
            
            # Estrai i metadati dal risultato della pipeline
            if isinstance(result, dict):
                if "metadata" in result:
                    output_metadata = result["metadata"]
                elif "output" in result:
                    output_metadata = result["output"]
                else:
                    # Fallback: cerca assistant_output in interrupt
                    if st.session_state.interrupt:
                        interrupt_val = st.session_state.interrupt
                        if isinstance(interrupt_val, list):
                            interrupt_val = interrupt_val[0]
                        metadata_str = interrupt_val.value.get("assistant_output", "{}")
                        try:
                            output_metadata = json.loads(metadata_str)
                        except json.JSONDecodeError:
                            logger.warning("Impossibile decodificare i metadati generati")

            if output_metadata:
                # Aggiorna la distribution con i nuovi metadati
                distribution = repos['distribution'].get_by_id(
                    st.session_state.current_distribution.id
                )
                
                # Aggiorna solo i campi presenti nell'output
                for key, value in output_metadata.items():
                    if hasattr(distribution, key):
                        setattr(distribution, key, value)
                
                # Salva le modifiche
                repos['distribution'].update(distribution)
                
                # Aggiorna lo stato della sessione
                st.session_state.current_distribution = distribution

        except Exception as e:
            st.error(f"❌ Errore durante il salvataggio dei metadati generati: {e}")
            logger.error(f"Errore nel salvataggio metadati: {e}")

    # 1. AVVIO INIZIALE DELLA PIPELINE
    if not st.session_state.pipeline_running and not st.session_state.manual_edit_active:
        _start_metadata_pipeline(st, langfuse_handler)

    # 2. INTERFACCIA DI MODIFICA MANUALE
    elif st.session_state.manual_edit_active:
        _render_manual_edit_interface(st, resume_metadata_pipeline)

    # 3. INTERFACCIA DI REVIEW (FEEDBACK UMANO)
    elif st.session_state.interrupt:
        _render_review_interface(st, resume_metadata_pipeline)

    # Pulsante per tornare indietro
    if st.button("⬅️ Torna all'Editor Metadati", key="back_to_editor"):
        reset_dashboard_session_state(st, distribution_keys)
        st.session_state.current_stage = "distribution_metadata"
        st.rerun()

def _start_metadata_pipeline(st, langfuse_handler):
    """Avvia la pipeline di generazione metadati."""
    if "thread_id_metadata" not in st.session_state:
        st.session_state.thread_id_metadata = str(uuid.uuid4())

    try:
        metadata_path = st.session_state.get("current_distribution_path", "")
        samples = st.session_state.get("samples", [])
        
        logger.info("Avvio pipeline metadati con samples:")
        logger.info(samples)

        from agents.states.dataset_metadata_state import State as MetadataState
        
        metadata_state = MetadataState(
            samples=samples,
            output_path=metadata_path
        )

        config = {
            "configurable": {"thread_id": st.session_state.thread_id_metadata}, 
            "callbacks": [langfuse_handler]
        }

        with st.spinner("🔄 Avvio della pipeline per metadatazione..."):
            result = st.session_state.metadata_graph.invoke(metadata_state, config=config)

        st.session_state.interrupt = result.get("__interrupt__")
        st.session_state.state = result
        st.session_state.pipeline_running = True
        st.rerun()

    except Exception as e:
        st.error(f"❌ Errore durante l'avvio della pipeline: {e}")
        logger.error(f"Errore avvio pipeline: {e}")

def _render_manual_edit_interface(st, resume_callback):
    """Renderizza l'interfaccia di modifica manuale."""
    st.markdown("---")
    st.subheader("✏️ Modifica Manuale dei Metadati")

    interrupt_val = st.session_state.interrupt
    if isinstance(interrupt_val, list):
        interrupt_val = interrupt_val[0]
    
    metadata_str_to_edit = interrupt_val.value.get("assistant_output", "{}")

    edited_metadata_str = st.text_area(
        "Metadati JSON:", 
        value=metadata_str_to_edit, 
        height=400,
        key="manual_edit_textarea"
    )

    col1, col2 = st.columns(2)
    if col1.button("✅ Conferma Modifiche", use_container_width=True, key="confirm_manual_edit"):
        try:
            # Validazione JSON
            json.loads(edited_metadata_str)
            feedback_metadata = json.dumps(json.loads(edited_metadata_str))
            resume_callback({"action": "manual", "feedback": feedback_metadata})
        except json.JSONDecodeError:
            st.error("❌ Errore: il testo inserito non è un JSON valido.")

    if col2.button("❌ Annulla Modifica", use_container_width=True, key="cancel_manual_edit"):
        st.session_state.manual_edit_active = False
        st.rerun()

def _render_review_interface(st, resume_callback):
    """Renderizza l'interfaccia di review e feedback."""
    interrupt_val = st.session_state.interrupt
    if isinstance(interrupt_val, list): 
        interrupt_val = interrupt_val[0]
    
    metadata_str = interrupt_val.value.get("assistant_output", "{}")

    st.markdown("---")
    st.subheader("📊 Metadati Generati")
    
    try:
        st.json(ast.literal_eval(metadata_str))
    except (ValueError, SyntaxError):
        st.code(metadata_str, language='json')

    st.markdown("---")

    feedback_text = st.text_area(
        "💡 Feedback per migliorare la generazione:",
        placeholder="Inserisci qui il tuo feedback...",
        key="feedback_textarea"
    )

    cols = st.columns(4)
    if cols[0].button("💾 Salva Metadati", use_container_width=True, key="save_metadata_button"):
        resume_callback({"action": "break"})

    if cols[1].button("🔄 Ritenta con Feedback", use_container_width=True, key="retry_with_feedback_button"):
        resume_callback({"action": "continue", "feedback": feedback_text})
        
    if cols[2].button("↩️ Ricomincia da Capo", use_container_width=True, key="restart_button"):
        resume_callback({"action": "restart"})
        
    if cols[3].button("✏️ Modifica Manuale", use_container_width=True, key="manual_edit_button"):
        st.session_state.manual_edit_active = True
        st.rerun()