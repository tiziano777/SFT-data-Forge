# dashboard.py
import streamlit as st
st.set_page_config(layout="wide")
import os, yaml
from langchain_google_genai import ChatGoogleGenerativeAI
from langfuse import Langfuse
from langfuse.langchain import CallbackHandler

# ======================================================

# === STATE VARS IMPORTS ===
from config.state_vars import home_vars

# === UTIL IMPORTS ===
from utils.streamlit_func import reset_dashboard_session_state

# === DB imports ===
from db.impl.postgres.loader.postgres_db_loader import get_db_manager

# === UI imports ===
from ui.raw.management.distribution.distribution_raw_metadata_handler import show_metadata_editor as show_raw_metadata_editor, show_metadata_generation as show_raw_metadata_generation
from ui.processed.management.distribution.distribution_preprocessed_metadata_handler import show_metadata_editor as show_processed_metadata_editor  , show_metadata_generation as show_processed_metadata_generation
from ui.raw.process.src_schema.raw_schema_extration_handler import show_schema_options as show_raw_schema_extraction_options
from ui.processed.process.src_schema.processed_schema_extraction_handler import show_schema_options as show_processed_schema_extraction_options
from ui.raw.process.src_schema.raw_schema_extration_handler import show_schema_extraction as show_raw_schema_extraction
from ui.processed.process.src_schema.processed_schema_extraction_handler import show_schema_extraction as show_processed_schema_extraction
from ui.processed.process.mapping.mapping_generation_handler import show_select_target_schema, show_mapping_generation, show_mapping_results
from ui.processed.process.mapping.manual_mapping_handler import show_manual_mapping
from ui.mapped.management.dataset.dataset_mapped_metadata_editing_handler import show_dataset_metadata_editing as mapped_dataset_metadata_editing
from ui.raw.management.distribution.distribution_raw_action_selection_handler import show_distribution as show_distribution_raw
from ui.processed.management.distribution.distribution_preprocessed_action_selection_handler import show_distribution as show_processed_distribution
from ui.processed.management.distribution.distribution_preprocessed_selection_handler import show_distribution_selection as show_processed_distribution_selection
from ui.raw.management.distribution.distribution_raw_selection_handler import show_distribution_selection as show_raw_distribution_selection 
from ui.processed.process.load.parallel_mapping_handler import show_parallel_mapping
from ui.data_studio.selection.data_studio_selection_handler import data_studio
from ui.data_studio.strategy.data_studio_stage_area import show_data_studio_stage_area
from ui.data_studio.strategy.data_studio_recipe_contract_creation import data_studio_recipe_contract_creation
from ui.data_studio.strategy.data_studio_recipe_contract_final_step import data_studio_recipe_contract_final_step
from ui.recipe_management.recipe_management_handler import recipe_management_handler
from ui.data_lineage.data_lineage_handler import data_lineage_handler
from ui.data_lineage.system_prompt_lineage import system_prompt_lineage_handler
from ui.data_lineage.recipe_lineage import recipe_lineage_handler 
from ui.processed.analytics.query.query_preprocessed_distribution_handler import show_processed_query_dataset
from ui.raw.analytics.query_raw_distribution_handler import show_raw_query_dataset
from ui.processed.analytics.stats_query.distribution_processed_stats_query_handler import show_processed_distribution_stats_query_handler
from ui.mapped.analytics.advanced_query.distribution_mapped_stats_query_handler import show_mapped_distribution_chat_stats_query_handler

from ui.user_documentation.documentation_handler import documentation
from ui.raw.management.dataset.dataset_raw_selection_handler import show_dataset_selection as show_raw_dataset_selection
from ui.processed.management.dataset.dataset_processed_selection_handler import show_dataset_selection as show_processed_dataset_selection
from ui.processed.management.dataset.dataset_processed_metadata_editing_handler import show_dataset_metadata_editing as processed_dataset_metadata_editing
from ui.raw.management.dataset.dataset_raw_metadata_editing_handler import show_dataset_metadata_editing as raw_dataset_metadata_editing
from ui.mapped.management.dataset.dataset_mapped_selection_handler import show_dataset_selection as show_mapped_dataset_selection
from ui.dataset_card.dataset_card_action_selection_handler import show_dataset_card_action_selection  
from ui.dataset_card.dataset_download.dataset_downloader_handler import show_download_interface
from ui.raw.process.preprocessing.parallel_preprocessing_handler import show_parallel_preprocessing
from ui.processed.analytics.stats.preprocessed_distribution_stats_handler import show_processed_low_level_stats_extraction
from ui.mapped.process.low_level_stats.mapped_distribution_stats_handler import show_mapped_low_level_stats_extraction
from ui.mapped.management.distribution.distribution_mapped_selection_handler import show_distribution_selection as show_mapped_distribution_selection
from ui.mapped.management.distribution.distribution_mapped_action_selection_handler import show_distribution as show_mapped_distribution
from ui.mapped.management.distribution.distribution_mapped_metadata_handler import show_metadata_editor as show_mapped_metadata_editor
from ui.mapped.analytics.query.query_mapped_distribution_handler import show_mapped_query_dataset
from ui.mapped.process.chat_template_stats.mapped_distribution_chat_template_stats_handler import show_mapped_chat_template_stats_extraction
from ui.mapped.analytics.advanced_query.distribution_mapped_low_level_stats_query_handler import show_mapped_distribution_low_level_stats_query_handler
from ui.processed.process.mapping.udf_creation_handler import show_udf_creation_step
from ui.prompt_management.prompt_action_selection_handler import show_system_prompt_management
from ui.template_schema_management.template_schema_action_selection_handler import show_template_schema_management
from ui.chat_type_management.chat_type_management_handler import show_vocab_chat_type_management

# === Pipeline imports ===
from agents.pipelines.source_schema_pipeline import create_pipeline
from agents.pipelines.mapping_schema_pipeline import create_pipeline as create_mapping_pipeline
from agents.pipelines.distribution_metadata_generation_pipeline import create_pipeline as create_distribution_metadata_pipeline

from agents.nodes.src_schema_nodes.schema_node import SchemaNode
from agents.nodes.src_schema_nodes.human_review_node import HumanReviewNode
from agents.nodes.src_schema_nodes.validation_node import ValidationNode
from agents.nodes.src_schema_nodes.schema_writer_node import SchemaWriter

from agents.nodes.mapping_schema_nodes.mapping_node import MappingNode 
from agents.nodes.mapping_schema_nodes.human_review_node import HumanReviewNode as MappingHumanReviewNode
from agents.nodes.mapping_schema_nodes.validation_node import ValidationNode as MappingValidationNode
from agents.nodes.mapping_schema_nodes.mapping_writer_node import MappingWriter 

from agents.nodes.metadata_nodes.metadata_node import MetadataNode 
from agents.nodes.metadata_nodes.human_review_node import HumanReviewNode as MetadataHumanReviewNode
from agents.nodes.metadata_nodes.metadata_writer_node import MetadataWriter


# === CONFIGURATION ===

BASE_PATH = os.getenv("BASE_PATH")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")

MODEL_CONFIG = "./config/gemini2.0-flash.yml"
PROMPTS_PATH = "./config/prompts.yml"


#@st.cache_data
#def load_config():
#    with open(MODEL_CONFIG, "r", encoding="utf-8") as f:
#        llmConfig = yaml.safe_load(f)
#        llmConfig["gemini_api_key"] = os.environ.get("GEMINI_API_KEY")
#    with open(PROMPTS_PATH, "r", encoding="utf-8") as f:
#        prompts = yaml.safe_load(f)
#    return llmConfig, prompts

#llmConfig, prompts = load_config()
#apikey= llmConfig["gemini_api_key"]

#geminiLLM = ChatGoogleGenerativeAI(
#    model = llmConfig["model_name"],
#    google_api_key = apikey,
#    temperature = llmConfig["temperature"],
#    max_output_tokens = llmConfig["max_output_tokens"],
#    top_p = llmConfig["top_p"],
#    top_k = llmConfig.get("top_k", None),
#)

#langfuse = Langfuse(
#    public_key=os.environ.get('LANGFUSE_PUBLIC_KEY'),
#    secret_key=os.environ.get('LANGFUSE_PRIVATE_KEY'),
#    host=os.environ.get('LANGFUSE_STRING_CONNECTION')
#)

langfuse_handler = CallbackHandler()


# === RESOURCE FACTORIES ===
@st.cache_resource
def init_db_manager():
    return get_db_manager()


#@st.cache_resource
#def init_src_schema_graph():
#    return create_pipeline(llm_node=SchemaNode(llm=geminiLLM, prompt=prompts["source_schema_extraction_prompt"], feedback_prompt=prompts["feedback_prompt"]),human_node=HumanReviewNode(),validation_node=ValidationNode(),writer_node=SchemaWriter())

#@st.cache_resource
#def init_mapping_graph():
#    return create_mapping_pipeline(
#        llm_node=MappingNode(llm=geminiLLM, prompt=prompts["mapping_schema_prompt"], feedback_prompt=prompts["mapping_schema_feedback_prompt"]),
#        human_node=MappingHumanReviewNode(),
#        validation_node=MappingValidationNode(),
#        writer_node=MappingWriter()
#    )


#@st.cache_resource
#def init_distribution_metadata_graph():
#    return create_distribution_metadata_pipeline(
#        llm_node=MetadataNode(llm=geminiLLM, prompt=prompts["distribution_metadata_inference_prompt"], feedback_prompt=prompts["distribution_metadata_inference_feedback_prompt"]),
#        human_node=MetadataHumanReviewNode(),
#        writer_node=MetadataWriter()
#    )

# === STATE INITIALIZATION ===
if "db_manager" not in st.session_state:
    st.session_state.db_manager = init_db_manager()

#if "src_schema_graph" not in st.session_state:
#    st.session_state.src_schema_graph = init_src_schema_graph()


#if "mapping_graph" not in st.session_state:
#    st.session_state.mapping_graph = init_mapping_graph()
#if "metadata_graph" not in st.session_state:
#    st.session_state.metadata_graph = init_distribution_metadata_graph()

if "current_stage" not in st.session_state:
    st.session_state.current_stage = "home"


# === DISPATCHER DICTIONARY ===
STAGE_HANDLERS = {
    "raw_dataset_selection": lambda st: show_raw_dataset_selection(st, st.session_state.dataset_path),
    "processed_dataset_selection": lambda st: show_processed_dataset_selection(st, st.session_state.dataset_path),
    "mapped_dataset_selection": lambda st: show_mapped_dataset_selection(st, st.session_state.dataset_path),
    "mapped_dataset_metadata_editing": lambda st: mapped_dataset_metadata_editing(st),
    "mapped_run_chat_template_stats_extraction": lambda st: show_mapped_chat_template_stats_extraction(st),
    "raw_dataset_metadata_editing": raw_dataset_metadata_editing,
    "processed_dataset_metadata_editing": processed_dataset_metadata_editing,
    "raw_distribution_selection": show_raw_distribution_selection,
    "raw_distribution_main": show_distribution_raw,
    "processed_distribution_selection": show_processed_distribution_selection,
    "processed_distribution_main": show_processed_distribution,
    "raw_distribution_metadata": lambda st: show_raw_metadata_editor(st),
    "raw_metadata_generation": lambda st: show_raw_metadata_generation(st, langfuse_handler),
    "processed_distribution_metadata": lambda st: show_processed_metadata_editor(st),
    "processed_metadata_generation": lambda st: show_processed_metadata_generation(st, langfuse_handler),
    "raw_schema_extraction_options": show_raw_schema_extraction_options,
    "raw_schema_extraction": lambda st: show_raw_schema_extraction(st, langfuse_handler),
    "processed_schema_extraction_options": show_processed_schema_extraction_options,
    "processed_schema_extraction": lambda st: show_processed_schema_extraction(st, langfuse_handler),
    "select_target_schema": show_select_target_schema,
    "mapping_generation": lambda st: show_mapping_generation(st, langfuse_handler),
    "manual_mapping": lambda st: show_manual_mapping(st), 
    "mapping_results": show_mapping_results,
    "run_parallel_mapping": show_parallel_mapping,
    "run_parallel_preprocessing": show_parallel_preprocessing,
    "processed_run_low_level_stats_extraction": lambda st: show_processed_low_level_stats_extraction(st),
    "mapped_run_low_level_stats_extraction": lambda st: show_mapped_low_level_stats_extraction(st),
    "processed_query_current_distribution": show_processed_query_dataset,
    "processed_query_advanced_current_distribution": show_processed_distribution_stats_query_handler,
    "mapped_distribution_selection": lambda st: show_mapped_distribution_selection(st),
    "mapped_distribution_main": show_mapped_distribution,
    "mapped_distribution_metadata": lambda st: show_mapped_metadata_editor(st),
    "mapped_query_current_distribution": show_mapped_query_dataset,
    "mapped_query_advanced_current_distribution": show_mapped_distribution_chat_stats_query_handler,
    "raw_query_current_distribution": show_raw_query_dataset,
    "data_studio": data_studio,
    "data_studio_stage_area": lambda st: show_data_studio_stage_area(st),
    "data_studio_recipe_contract_creation": lambda st: data_studio_recipe_contract_creation(st),
    "data_studio_final_review": lambda st: data_studio_recipe_contract_final_step(st),
    "recipe_management": lambda st: recipe_management_handler(st),
    "docs": documentation,
    "dataset_card_action_selection": show_dataset_card_action_selection,
    "download": show_download_interface,
    "user_defined_query_option": lambda st: show_udf_creation_step(st),
    "mapped_query_low_level_stats_current_distribution": lambda st: show_mapped_distribution_low_level_stats_query_handler(st),
    "system_prompt_management": lambda st: show_system_prompt_management(st),
    "template_schema_management": lambda st: show_template_schema_management(st),
    "chat_type_management": lambda st: show_vocab_chat_type_management(st),
    "data_lineage": lambda st: data_lineage_handler(st),
    "system_prompt_lineage": lambda st: system_prompt_lineage_handler(st),
    "recipe_lineage": lambda st: recipe_lineage_handler(st)
}

# === MAIN ===
def main():
    st.markdown("## WELCOME TO SFT DATA FORGE",)
    st.markdown("---")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Layer 0: Dataset Card Management")
    dataset_card_btn = st.sidebar.button("📋 Dataset Card", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Layer 1: Raw Data Curation ")
    dataset_btn = st.sidebar.button("🗄️ RAW Dataset Workflow", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    dataset_distribution_btn = st.sidebar.button("🪣 RAW Dataset Distribution Workflow",on_click=lambda: reset_dashboard_session_state(st, home_vars))

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Layer 2: Processed Data Curation ")
    processed_dataset_btn = st.sidebar.button("💼 Processed Dataset Workflow", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    processed_distribution_btn = st.sidebar.button("📊 Processed Dataset Distribution Workflow", on_click=lambda: reset_dashboard_session_state(st, home_vars))

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Layer 3: Mapped Data Curation ")
    mapped_dataset_btn = st.sidebar.button("⛏ Mapped Dataset Workflow", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    mapped_data_btn = st.sidebar.button("🧩 Mapped Dataset Distribution Workflow", on_click=lambda: reset_dashboard_session_state(st, home_vars))

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Layer 4: Recipe Creation")
    data_studio_btn = st.sidebar.button("🎨 Data Studio", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    recipe_management_btn = st.sidebar.button("🍽️ Recipe Management ", on_click=lambda: reset_dashboard_session_state(st, home_vars))

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Observability Layer")
    data_lineage_btn = st.sidebar.button("🔗 Data Lineage", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    system_prompt_lineage_btn = st.sidebar.button("🧠 System Prompt Lineage", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    recipe_lineage_btn = st.sidebar.button("🍳 Recipe Lineage", on_click=lambda: reset_dashboard_session_state(st, home_vars))

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Prompt Management")
    prompt_mgmt_btn = st.sidebar.button("🧠 Prompt Management", on_click=lambda: reset_dashboard_session_state(st, home_vars))


    st.sidebar.markdown("---")
    st.sidebar.markdown("### Schema Templates Management")
    template_schema_btn = st.sidebar.button("📐 Schema Templates Management", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Chat Type Management")
    chat_type_mgmt_btn = st.sidebar.button("💬 Chat Type Management", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("📚 Documentation & Logs")
    docs_btn = st.sidebar.button("📖 Docs", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    # logs_btn = st.sidebar.button("📜 Logs", on_click=lambda: reset_dashboard_session_state(st, home_vars))
    st.sidebar.markdown("---")
    st.sidebar.json(st.session_state)

    if dataset_card_btn:
        st.session_state.current_stage = "dataset_card_action_selection"
        st.rerun()
    elif dataset_btn:
        st.session_state.dataset_path = RAW_DATA_DIR
        st.session_state.current_stage = "raw_dataset_selection"
        st.rerun()
    elif dataset_distribution_btn:
        st.session_state.dataset_path = RAW_DATA_DIR
        st.session_state.current_stage = "raw_distribution_selection"
        st.rerun()
    elif processed_distribution_btn:
        st.session_state.dataset_path = PROCESSED_DATA_DIR
        st.session_state.current_stage = "processed_distribution_selection"
        st.rerun()
    elif processed_dataset_btn:
        st.session_state.dataset_path = PROCESSED_DATA_DIR
        st.session_state.current_stage = "processed_dataset_selection"
        st.rerun()
    elif mapped_dataset_btn:
        st.session_state.dataset_path = MAPPED_DATA_DIR
        st.session_state.current_stage = "mapped_dataset_selection"
        st.rerun()
    elif mapped_data_btn:
        st.session_state.dataset_path = MAPPED_DATA_DIR
        st.session_state.current_stage = "mapped_distribution_selection"
        st.rerun()
    elif data_studio_btn:
        st.session_state.current_stage = "data_studio"
        st.rerun()
    elif recipe_management_btn:
        st.session_state.current_stage = "recipe_management"
        st.rerun()
    elif data_lineage_btn:
        st.session_state.current_stage = "data_lineage"
        st.rerun()
    elif system_prompt_lineage_btn:
        st.session_state.current_stage = "system_prompt_lineage"
        st.rerun()
    elif recipe_lineage_btn:
        st.session_state.current_stage = "recipe_lineage"
        st.rerun()
    elif prompt_mgmt_btn:
        st.session_state.current_stage = "system_prompt_management"
        st.rerun()
    elif template_schema_btn:
        st.session_state.current_stage = "template_schema_management"
        st.rerun()
    elif chat_type_mgmt_btn:
        st.session_state.current_stage = "chat_type_management"
        st.rerun()
    elif docs_btn:
        st.session_state.current_stage = "docs"
        st.rerun()

    handler = STAGE_HANDLERS.get(st.session_state.current_stage)
    if handler:
        handler(st)
    elif st.session_state.current_stage == "home":
        st.markdown("### Manage your data with maximum efficiency:")
        st.markdown("---")
        
        # Dataset Card button also on home for better visibility
        st.markdown("### 📋 Dataset Card Management")
        st.markdown("""
        **Track and manage your datasets not yet downloaded:**
        - **Registration**: Register and catalog external datasets not yet materialized
        - **Tracking**: Monitor the status of identified datasets
        - **Materialization**: Download and import selected datasets into the system
        - **Metadata**: Define and manage metadata for registered datasets
        """)
        if st.button("Open Dataset Card", key="home_dataset_card"):
            st.session_state.current_stage = "dataset_card_action_selection"
            st.rerun()
        
        st.markdown("### 🗄️ Dataset Curation Workflow  (Dataset Ingestion) 🗄️")
        st.markdown("""
        Utilizza questa sezione per **censire i tuoi dataset**, passo dopo passo, in un flusso guidato.
        """)

        st.markdown("### 🪣 Dataset Distribution Workflow (Subfolder) 🪣")
        st.markdown("""
        Crea le **distribuzioni dei tuoi dataset** in modo semplice ed efficace.
        In questa sezione potrai:
        - Selezionare una specifica distribuzione da un dataset esistente.
        - Generare automaticamente i metadati della distribuzione.
        - Estrarre lo schema sorgente.
        - Interrogare la distribuzione per ottenere informazioni rapide e/o creare altre distribuzioni derivate.
        - preprocessare i dati grezzi in formato jsonl.gz (o .parquet) concatenandoli con core metadata e salvandoli nel sistema.
        """)

        st.markdown("### 💼 Processed Dataset Workflow 💼")
        st.markdown("""
        Gestisci i tuoi dataset processati.
        Questa sezione ti permette di:
        - Selezionare un dataset processato esistente.
        - Censire il dataset processato.
        """)

        st.markdown("### 📊 Processed Distribution Workflow 📊")
        st.markdown("""
        Gestisci i tuoi dataset processati.
        Questa sezione ti permette di:
        - Selezionare una distribuzione processata esistente.
        - Interrogare la distribuzione processata per ottenere informazioni rapide e/o creare altre distribuzioni derivate.
        - Definire le mappature tra lo schema sorgente e quello di destinazione.
        - Mappare i dati in parallelo e salvarli nel sistema.
        """)

        st.markdown("### ⛏ Mapped Dataset Workflow ⛏")
        st.markdown("""
        Gestisci i tuoi dataset mappati.
        Questa sezione ti permette di:
        - Selezionare un dataset mappato esistente.
        - Censire il dataset mappato.
        """)
        st.markdown("### 🧩 Mapped Distribution Workflow 🧩")
        st.markdown("""
        Gestisci le tue distribuzioni mappate.
        Questa sezione ti permette di:
        - Selezionare una distribuzione mappata esistente.
        - Censire la distribuzione mappata.
        - Estrarre **Metadata Stats** dai dati mappati per analisi approfondite.
        - Interrogare la distribuzione mappata e le sue stats per ottenere informazioni rapide e/o creare altre distribuzioni derivate.
        """)

        st.markdown("### 🧾 Data Studio (Receipt) 🧾")
        st.markdown("""
        Accedi a un ambiente interattivo per **esplorare i dataset dai metadati** e creare **ricette personalizzate**.
        Questo strumento ti consente di:
        - Combinare dataset diversi, applicare filtri specifici e generare nuove viste dei dati in modo intuitivo.
        - Salvare gli step di trasformazione come ricette riutilizzabili.
        - Salvare le ricette materializzate per il training di modelli ML.
        - Mappare i dati processati in un formato training-ready specifico per modelli LLM.
        """)

        st.markdown("### 🔍 Data Lineage")
        st.markdown("""
        Accedi alla sezione di **Data Lineage** per esplorare e gestire l'ontologia dei dati.
        Qui puoi:
        - Scoprire nuove fonti di dati e comprenderne la struttura.
        - Gestire le relazioni tra diversi dataset e metadati.
        - Creare e modificare le definizioni di ontologia per adattarle alle tue esigenze.
        """)
    else:
        st.error(f"Stage sconosciuto: {st.session_state.current_stage}")


if __name__ == "__main__":
    main()