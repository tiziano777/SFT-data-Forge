import duckdb as dk
import os
import json
import pandas as pd
import yaml
from uuid import UUID
from datetime import datetime
from utils.path_utils import to_internal_path
BASE_PREFIX = os.getenv("BASE_PREFIX")

from data_class.repository.table.distribution_repository import DistributionRepository
from db.impl.postgres.loader.postgres_db_loader import get_db_manager

import logging
logger = logging.getLogger(__name__)

db_manager = get_db_manager()
dist_repo = DistributionRepository(db_manager)

## # DOWNLOAD PREPARATION
def _prepare_recipe_for_download(recipe_dict):
    """
    Trasforma la ricetta: usa dist_uri come chiave e converte UUID in stringhe.
    """
    transformed_recipe = {}
    
    for old_key, details in recipe_dict.items():
        # Copia profonda dei dettagli per non modificare l'originale in session_state
        new_details = details.copy()
        
        # 1. Gestione UUID: convertiamo l'id originale (chiave o campo) in stringa
        # Se dist_id è un oggetto UUID, lo rendiamo stringa leggibile
        if "dist_id" in new_details and isinstance(new_details["dist_id"], UUID):
            new_details["dist_id"] = str(new_details["dist_id"])
        
        # Gestiamo anche la vecchia chiave se era un UUID
        current_id_str = str(old_key)
        new_details["dist_id"] = current_id_str # Inseriamo l'ID originale come campo interno
        
        # 2. Identifichiamo la nuova chiave (dist_uri)
        new_key = new_details.get("dist_uri", current_id_str)
        
        # 3. Pulizia di eventuali altri campi UUID annidati (opzionale ma sicuro)
        for k, v in new_details.items():
            if isinstance(v, UUID):
                new_details[k] = str(v)
                
        transformed_recipe[new_key] = new_details
        
    return transformed_recipe

### QUERY AND MATERIALIZATION WITH DUCKDB
def _compact_sql_query(query: str) -> str:
    """Rimuove spazi multipli, a capo e tabulazioni da una query SQL."""
    import re
    query = query.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    query = re.sub(r'\s+', ' ', query)
    return query.strip()

def _materialize_with_sampling(dist_id, source_uri, target_file_path, sampling_rate: float):
    # 1. Preparazione parametri e path
    sampling_pct = sampling_rate * 100
    source_path = to_internal_path(source_uri.replace(BASE_PREFIX, ""))
    source_path = f"{source_path.rstrip('/')}/*.jsonl.gz"
    
    output_directory = target_file_path.rstrip("/")
    if output_directory.endswith(".jsonl.gz"):
        output_directory = output_directory.replace(".jsonl.gz", "")
    
    target_uri = BASE_PREFIX + output_directory
    physical_path = to_internal_path(output_directory.replace(BASE_PREFIX, ""))

    # --- CHECK 1: Esistenza logica nel DB ---
    existing_dist = dist_repo.get_by_uri(target_uri)
    
    # --- CHECK 2: Esistenza fisica su disco ---
    # Verifichiamo se la cartella esiste e se contiene file
    is_physically_present = os.path.exists(physical_path) and any(os.scandir(physical_path))

    # Se esiste sia nel DB che fisicamente, non facciamo nulla
    if existing_dist and is_physically_present:
        return existing_dist

    # 2. Preparazione metadata (necessaria se dobbiamo creare l'entry o se i file mancano)
    distribution = dist_repo.get_by_id(dist_id)
    new_dist = distribution.copy()

    new_dist.id = None
    new_dist.derived_from = dist_id
    new_dist.uri = target_uri
    new_dist.name = f"Downsampled__{sampling_rate:.2f}__{distribution.name}"
    new_dist.version = "1.0"
    new_dist.issued = datetime.utcnow()
    new_dist.modified = datetime.utcnow()
    new_dist.materialized = True

    # Se i file sono presenti fisicamente ma l'entry DB mancava (Check 1 fallito)
    # inseriamo l'entry senza rieseguire la query pesante
    if is_physically_present and not existing_dist:
        return dist_repo.insert(new_dist)

    # 3. Query di esportazione (Eseguita solo se i file NON esistono fisicamente)
    limit_bytes = 124 * 1024 * 1024
    query = f"""
        COPY (
            SELECT * FROM read_json_auto('{source_path}', union_by_name=True) 
            USING SAMPLE {sampling_pct}% (SYSTEM)
        ) TO '{physical_path}' 
        (
            FORMAT JSONL, 
            COMPRESSION GZIP,
            FILE_SIZE_BYTES {limit_bytes},
            FILENAME_PATTERN "downsampled__data_{{i}}"
        );
    """
    
    new_dist.query = _compact_sql_query(query)

    # Assicuriamoci che la cartella padre esista prima di chiamare DuckDB
    os.makedirs(os.path.dirname(physical_path), exist_ok=True)

    with dk.connect() as conn:
        conn.execute(query)

    # Salvataggio finale nel DB — controlliamo per URI per evitare UniqueViolation
    existing_by_uri = dist_repo.get_by_uri(new_dist.uri)
    if existing_by_uri:
        return existing_by_uri
    return dist_repo.insert(new_dist)

### RECIPE FINALIZATION AND SPLITTING LOGIC
def _split_receipt_strategy(st):
    # Recuperiamo il dizionario originale
    original_recipe = st.session_state.get("receipt", {})
    
    # Inizializziamo i due dizionari di output
    int_recipe = {}
    float_recipe = {}

    for dist_id, strategy in original_recipe.items():
        replica_total = strategy["replica"]
        
        # Caso 1: Downsampling (replica < 1)
        # Rimane solo nella strategia float
        if replica_total < 1:
            float_recipe[dist_id] = strategy.copy()
            continue

        # Caso 2: Numero intero (es. 2.0, 3)
        # Va solo nella strategia int
        if replica_total % 1 == 0:
            int_recipe[dist_id] = strategy.copy()
            int_recipe[dist_id]["replica"] = int(replica_total)
            continue

        # Caso 3: Split tra parte intera e decimale (es. 2.5)
        int_part = int(replica_total)
        decimal_part = round(replica_total - int_part, 2)

        # Prepariamo le basi
        int_strategy = strategy.copy()
        float_strategy = strategy.copy()

        # Calcolo proporzioni basato sulla replica totale originale
        # Formula: (Valore Totale / Replica Totale) * Nuova Replica
        for field in ["samples", "words", "tokens"]:
            float_strategy[field] = int( strategy[field] * decimal_part)
            int_strategy[field] = int(strategy[field] * int_part)

        int_strategy["replica"] = int_part
        float_strategy["replica"] = decimal_part

        int_recipe[dist_id] = int_strategy
        float_recipe[dist_id] = float_strategy
    
    return int_recipe, float_recipe


def _check_float_recipe_materialization(float_recipe):
    """
    Per ogni entry nel float_recipe, controlla se la distribution è già stata
    materializzata fisicamente su disco.
    Ritorna un dict { dist_id: bool } dove True = già presente su disco.
    """
    already_materialized = {}
    for dist_id, strategy in float_recipe.items():
        target_file_path = strategy["dist_uri"].split("/")
        target_file_path = '/'.join(
            target_file_path[:-1] + [f"downsampled__{strategy['replica']}__{target_file_path[-1]}"]
        )
        physical_path = to_internal_path(target_file_path.replace(BASE_PREFIX, ""))
        is_present = os.path.exists(physical_path) and any(os.scandir(physical_path))
        already_materialized[dist_id] = is_present
    return already_materialized


def _materialize_float_recipe(float_recipe, force_resample: dict = None):
    """
    force_resample: dict { dist_id: bool } — se True per un dist_id,
    forza la rigenerazione cancellando i file fisici e l'entry DB esistente.
    """
    if force_resample is None:
        force_resample = {}

    final_float_recipe = dict()
    for dist_id, strategy in float_recipe.items():
        target_file_path = strategy["dist_uri"].split("/")
        target_file_path = '/'.join(
            target_file_path[:-1] + [f"downsampled__{strategy['replica']}__{target_file_path[-1]}"]
        )

        should_force = force_resample.get(dist_id, False)

        if should_force:
            # Cancelliamo solo i file fisici, l'entità DB rimane intatta
            physical_path = to_internal_path(target_file_path.replace(BASE_PREFIX, ""))
            if os.path.exists(physical_path):
                import shutil
                shutil.rmtree(physical_path)
                logger.info(f"Removed existing physical files at {physical_path} for resampling")

        new_dist = dist_repo.get_by_uri(BASE_PREFIX + target_file_path.lstrip("/"))

        if not new_dist or should_force:
            # Nuova distribution oppure force: rigeneriamo i file fisici.
            # _materialize_with_sampling gestirà il caso in cui l'entry DB
            # esiste già (is_physically_present=False + existing_dist presente).
            logger.info(f"Materializing distribution {dist_id} with sampling rate {strategy['replica']} to {target_file_path}")
            new_dist = _materialize_with_sampling(dist_id, strategy["dist_uri"], target_file_path, strategy["replica"])

        logger.info(f"Created new distribution {str(new_dist)}")
        final_float_recipe[str(new_dist.id)] = {
            "dist_id": new_dist.id,
            "dist_name": new_dist.name,
            "dist_uri": new_dist.uri.replace(BASE_PREFIX, ""),
            "tokenized_uri": new_dist.tokenized_uri.replace(BASE_PREFIX, "") if new_dist.tokenized_uri else None,
            "chat_type": strategy["chat_type"],
            "replica": 1,
            "system_prompt": strategy["system_prompt"],
            "system_prompt_name": strategy["system_prompt_name"],
            "samples": strategy["samples"],
            "tokens": strategy["tokens"],
            "words": strategy["words"],
        }
    return final_float_recipe

        
### STORE FINAL RECIPE IN DB
def _store_recipe_in_db(st, final_recipe):
    try:
        from datetime import datetime
        from data_class.repository.table.recipe_repository import RecipeRepository
        from data_class.repository.table.strategy_repository import StrategyRepository
        from data_class.repository.table.strategy_system_prompt_repository import StrategySystemPromptRepository
        from data_class.entity.table.strategy import Strategy
        from data_class.entity.table.strategy_system_prompt import StrategySystemPrompt

        # Inizializzazione Repository
        recipe_repository = RecipeRepository(db_manager)
        strategy_repository = StrategyRepository(db_manager)
        ssp_repository = StrategySystemPromptRepository(db_manager)

        # 1. Gestione Recipe
        recipe = recipe_repository.get_by_name(st.session_state.recipe_entity.name)
        if not recipe:
            recipe = recipe_repository.insert(st.session_state.recipe_entity)

        # 2. Iterazione sulle strategie della ricetta
        for dist_id, strategy_data in final_recipe.items():
            strategy_entity = Strategy(
                id=None,
                recipe_id=recipe.id,
                distribution_id=strategy_data["dist_id"],
                replication_factor=strategy_data["replica"],
                template_strategy=strategy_data["chat_type"],
                issued=datetime.utcnow(),
                modified=datetime.utcnow()
            )
            
            inserted_strategy = strategy_repository.insert(strategy_entity)
            
            if inserted_strategy and "system_prompt_name" in strategy_data:
                prompts_list = strategy_data["system_prompt_name"]
                
                if isinstance(prompts_list, list):
                    for p_data in prompts_list:
                        ssp_entity = StrategySystemPrompt(
                            strategy_id=inserted_strategy.id,
                            system_prompt_name=p_data
                        )
                        ssp_repository.insert(ssp_entity)
        
        return True, None
    except Exception as e:
        msg = f"Error storing recipe in DB: {str(e)}"
        logger.error(msg)
        return False, msg
    
### STREAMLIT APP LOGIC
def data_studio_recipe_contract_final_step(st):
    # Inizializzazione stati per mantenere i dati tra i refresh della pagina
    if 'final_recipe_obj' not in st.session_state:
        st.session_state.final_recipe_obj = None
    if 'recipe_ready' not in st.session_state:
        st.session_state.recipe_ready = False

    int_recipe, float_recipe = _split_receipt_strategy(st)
    
    st.write("### Final Recipe Review, Splitted by Integer and Decimal Replication Strategies")
    st.write("#### Integer Replication Strategies")
    st.dataframe(int_recipe.values())
    st.write("#### Fractional Replication Strategies")
    st.dataframe(float_recipe.values())

    # --- TOGGLE DI MATERIALIZZAZIONE (sempre visibili se esiste float_recipe) ---
    # materialize_choices: { dist_id: bool }
    # True  = materializza (default per tutti)
    # False = salta (solo per già materializzati, se utente lo sceglie)
    materialize_choices = {}

    if float_recipe:
        materialization_status = _check_float_recipe_materialization(float_recipe)
        
        st.write("#### Downsampling Materialization Control")
        st.caption(
            "New distributions are materialized automatically. "
            "For already existing ones you can choose to skip or regenerate the sampling."
        )

        for dist_id, strategy in float_recipe.items():
            is_already_present = materialization_status.get(dist_id, False)
            dist_name = strategy.get("dist_name", strategy.get("dist_uri", str(dist_id)))
            replica = strategy["replica"]

            if is_already_present:
                # Già materializzata: mostriamo il toggle, default OFF (non rigenerare)
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"**{dist_name}** — rate `{replica}` &nbsp; ⚠️ *already materialized*")
                with col2:
                    regenerate = st.toggle(
                        "Regenerate",
                        value=False,
                        key=f"resample_toggle_{dist_id}",
                        help="OFF = keep existing downsampled data. ON = delete and resample from scratch."
                    )
                # materialize=True sempre (o esiste già, o la rigeneriamo), force è separato
                materialize_choices[dist_id] = {"materialize": True, "force": regenerate}
            else:
                # Nuova: materializzazione automatica, nessun controllo necessario
                st.markdown(f"**{dist_name}** — rate `{replica}` &nbsp; 🆕 *will be materialized*")
                materialize_choices[dist_id] = {"materialize": True, "force": False}

    # --- LOGICA DI MATERIALIZZAZIONE ---
    if st.button("Confirm and Create Recipe"):
        force_resample = {
            dist_id: choices["force"]
            for dist_id, choices in materialize_choices.items()
        }
        materialized_float = _materialize_float_recipe(float_recipe, force_resample=force_resample)
        st.session_state.final_recipe_obj = int_recipe | materialized_float
        st.session_state.recipe_ready = True
        st.success("Recipe created successfully!")

    # --- LOGICA DI ESPORTAZIONE (Solo se la ricetta è pronta) ---
    if st.session_state.recipe_ready:
        final_recipe = st.session_state.final_recipe_obj
        
        st.divider()
        st.write("### Export & Download")
        download_recipe = _prepare_recipe_for_download(final_recipe)
        for entry in download_recipe.values():
            entry.pop("schema_template", None)
            entry.pop("validation_error", None)
        st.dataframe(download_recipe.values())

        save_format = st.selectbox("Select format to save the recipe", ["JSON", "CSV", "YAML"])
        
        if st.button(f"Confirm & Save to DB"):
            result, msg = _store_recipe_in_db(st, final_recipe)
            
            if result:
                st.success(f"Recipe stored in Database!")
                
                if save_format == "JSON":
                    data = json.dumps(download_recipe, indent=4)
                    file_name = f"{st.session_state.recipe_entity.name}_recipe.json"
                    mime = "application/json"
                elif save_format == "CSV":
                    data = pd.DataFrame(download_recipe.values()).to_csv(index=False)
                    file_name = f"{st.session_state.recipe_entity.name}_recipe.csv"
                    mime = "text/csv"
                elif save_format == "YAML":
                    data = yaml.dump(download_recipe)
                    file_name = f"{st.session_state.recipe_entity.name}_recipe.yaml"
                    mime = "text/yaml"

                st.download_button(
                    label=f"📥 Download {file_name}",
                    data=data,
                    file_name=file_name,
                    mime=mime,
                    key="download_btn"
                )
            else:
                st.error(f"Failed to save recipe in DB: {msg}")

    # --- NAVIGAZIONE ---
    st.divider()
    if st.button("⬅️⬅️ Back to Contract Creation", key="back_to_strategy"):
        st.session_state.recipe_ready = False
        st.session_state.final_recipe_obj = None
        st.session_state.current_stage = "data_studio_recipe_contract_creation"
        st.rerun()