import logging
import uuid
import yaml
import plotly.graph_objects as go
from datetime import datetime, timezone
import yaml
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _load_all_data(st):
    from data_class.repository.table.recipe_repository import RecipeRepository
    from data_class.repository.table.strategy_repository import StrategyRepository
    from data_class.repository.vocabulary.vocab_chat_type_repository import VocabChatTypeRepository
    from data_class.repository.table.strategy_system_prompt_repository import StrategySystemPromptRepository
    from data_class.repository.table.system_prompt_repository import SystemPromptRepository
    from data_class.repository.table.distribution_repository import DistributionRepository

    db = st.session_state.db_manager
    recipe_repo = RecipeRepository(db)
    strategy_repo = StrategyRepository(db)
    vocab_chat_type_repo = VocabChatTypeRepository(db)
    ssp_repo = StrategySystemPromptRepository(db)
    system_prompt_repo = SystemPromptRepository(db)
    dist_repo = DistributionRepository(db)

    recipes = recipe_repo.get_all()
    chat_type_codes = [ct.code for ct in vocab_chat_type_repo.get_all()]
    prompt_names = [sp.name for sp in system_prompt_repo.get_all()]

    return {
        "recipe_repo": recipe_repo,
        "strategy_repo": strategy_repo,
        "ssp_repo": ssp_repo,
        "dist_repo": dist_repo,
        "recipes": recipes,
        "chat_type_codes": chat_type_codes,
        "prompt_names": prompt_names,
    }

def _get_strategies_for_recipe(strategy_repo, ssp_repo, dist_repo, recipe_id):
    strategies = strategy_repo.get_by_recipe_id(recipe_id)
    enriched = []
    for s in strategies:
        dist = dist_repo.get_by_id(s.distribution_id)
        ssps = ssp_repo.get_by_strategy_id(s.id)
        enriched.append({
            "strategy": s,
            "distribution": dist,
            "prompt_names": [ssp.system_prompt_name for ssp in ssps],
        })
    return enriched

def _show_recipe_metadata_editor(st, recipe, data):
    """
    Form per modificare i metadati della recipe (nome, descrizione, scope, tasks, tags).
    """
    from data_class.repository.vocabulary.vocab_task_repository import VocabTaskRepository
    from data_class.repository.vocabulary.vocab_dataset_type_repository import VocabDatasetTypeRepository

    recipe_repo = data["recipe_repo"]
    vocab_task_repo = VocabTaskRepository(st.session_state.db_manager)
    vocab_dataset_type_repo = VocabDatasetTypeRepository(st.session_state.db_manager)

    SCOPE = [item.code for item in vocab_dataset_type_repo.get_all()]
    TASKS_VOCAB = [item.code for item in vocab_task_repo.get_all()]

    default_name = recipe.name
    default_desc = recipe.description or ""
    default_scope = recipe.scope if recipe.scope in SCOPE else (SCOPE[0] if SCOPE else None)
    default_tasks = [t for t in recipe.tasks if t in TASKS_VOCAB] if recipe.tasks else []
    default_tags = ", ".join(recipe.tags) if recipe.tags else ""

    st.info(f"Edizione metadati per: **{recipe.name}**")
    
    with st.form(key=f"edit_meta_form_{recipe.id}"):
        col1, col2 = st.columns([2, 1])
        name = col1.text_input("Recipe Name *", value=default_name)
        scope_idx = SCOPE.index(default_scope) if default_scope in SCOPE else 0
        scope = col2.selectbox("Scope *", SCOPE, index=scope_idx)
        
        description = st.text_area("Description", value=default_desc)
        tasks = st.multiselect("Tasks", TASKS_VOCAB, default=default_tasks)
        tags = st.text_input("Tags (comma separated)", value=default_tags)

        c1, c2, _ = st.columns([1, 1, 3])
        save_btn = c1.form_submit_button("💾 Save Metadata", type="primary", use_container_width=True)
        cancel_btn = c2.form_submit_button("❌ Cancel", use_container_width=True)

        if save_btn:
            if name and tasks:
                # Aggiorniamo i campi dell'oggetto esistente
                recipe.name = name
                recipe.description = description
                recipe.scope = scope
                recipe.tasks = tasks
                recipe.tags = [t.strip() for t in tags.split(",")] if tags else []
                
                # Rimuoviamo eventuali riferimenti a 'modified' se presenti nell'oggetto 
                # per lasciare che il repository lo gestisca internamente se previsto.
                try:
                    recipe_repo.update(recipe)
                    st.success("✅ Metadati aggiornati!")
                    st.session_state[f"editing_meta_{recipe.id}"] = False
                    # Forza il reload dei dati
                    st.session_state.pop("rm_data_cache", None)
                    st.rerun()
                except Exception as e:
                    # Se l'errore persiste, è probabile che il repository richieda 
                    # una pulizia specifica dell'oggetto prima dell'update.
                    st.error(f"Errore durante l'aggiornamento: {e}")
            else:
                st.error("Nome e Tasks sono obbligatori")
        
        if cancel_btn:
            st.session_state[f"editing_meta_{recipe.id}"] = False
            st.rerun()

def _aggregate_recipe_totals(enriched_strategies, stats_cache):
    totals = {"samples": 0, "tokens": 0, "words": 0, "langs": {}}
    for item in enriched_strategies:
        dist = item["distribution"]
        if dist is None:
            continue
        rep = item["strategy"].replication_factor
        stats = stats_cache.get(str(dist.id), {"samples": 0, "tokens": 0, "words": 0})
        totals["samples"] += int(stats["samples"] * rep)
        totals["tokens"] += int(stats["tokens"] * rep)
        totals["words"] += int(stats["words"] * rep)
        lang = getattr(dist, "lang", "N/D") or "N/D"
        totals["langs"][lang] = totals["langs"].get(lang, 0) + int(stats["samples"] * rep)
    return totals

def _filter_recipes(recipes, search_query: str, lang_filter: str):
    if not search_query and not lang_filter:
        return recipes
    q = search_query.lower().strip() if search_query else ""
    filtered = []
    for r in recipes:
        if q and q not in f"{r.name} {r.description or ''}".lower():
            continue
        filtered.append(r)
    return filtered

# ─────────────────────────────────────────────
# YML UPLOAD
# ─────────────────────────────────────────────

def _validate_yml_structure(data: dict) -> str | None:
    """Valida la struttura del YAML. Ritorna errore o None se valido."""
    required_top = ["name", "description", "scope", "tasks", "entries"]
    for key in required_top:
        if key not in data:
            return f"Campo obbligatorio mancante: '{key}'"
    if not isinstance(data.get("entries"), dict) or not data["entries"]:
        return "Il campo 'entries' deve essere un dizionario non vuoto con almeno una distribution."
    for uri, entry in data["entries"].items():
        if not isinstance(entry, dict):
            return f"L'entry '{uri}' deve essere un dizionario."
        for field in ["dist_id", "chat_type", "replica"]:
            if field not in entry:
                return f"Entry '{uri}': campo obbligatorio mancante '{field}'."
        sp_list = entry.get("system_prompt", [])
        sp_names = entry.get("system_prompt_name", [])
        if not isinstance(sp_list, list) or not isinstance(sp_names, list):
            return f"Entry '{uri}': system_prompt e system_prompt_name devono essere liste."
        if len(sp_list) != len(sp_names):
            return f"Entry '{uri}': system_prompt e system_prompt_name devono avere la stessa lunghezza."
    return None


def _import_recipe_from_yml(st, data: dict) -> str:
    """
    Importa una recipe da dati YAML validati.
    Ritorna messaggio di successo o lancia Exception con errore descrittivo.
    """
    from data_class.entity.table.recipe import Recipe
    from data_class.entity.table.strategy import Strategy
    from data_class.entity.table.strategy_system_prompt import StrategySystemPrompt
    from data_class.entity.table.system_prompt import SystemPrompt
    from data_class.repository.table.recipe_repository import RecipeRepository
    from data_class.repository.table.strategy_repository import StrategyRepository
    from data_class.repository.table.strategy_system_prompt_repository import StrategySystemPromptRepository
    from data_class.repository.table.system_prompt_repository import SystemPromptRepository
    from data_class.repository.table.distribution_repository import DistributionRepository

    db = st.session_state.db_manager
    recipe_repo = RecipeRepository(db)
    strategy_repo = StrategyRepository(db)
    ssp_repo = StrategySystemPromptRepository(db)
    sp_repo = SystemPromptRepository(db)
    dist_repo = DistributionRepository(db)

    yml_id = data.get("id")
    yml_name = data["name"]
    yml_desc = data["description"]
    yml_scope = data["scope"]
    yml_tasks = data.get("tasks", [])
    yml_tags = data.get("tags", [])
    yml_derived_from = data.get("derived_from")

    # ── Risoluzione Recipe ──
    existing_by_id = recipe_repo.get_by_id(yml_id) if yml_id else None
    existing_by_name = recipe_repo.get_by_name(yml_name)

    recipe_entity = None

    if existing_by_id:
        # UUID esiste già → crea nuova recipe con nuovo UUID, derived_from = vecchio UUID
        recipe_entity = Recipe(
            name=yml_name if not existing_by_name else f"{yml_name}_{uuid.uuid4().hex[:6]}",
            description=yml_desc,
            scope=yml_scope,
            tasks=yml_tasks,
            tags=yml_tags,
            derived_from=str(existing_by_id.id),
        )
        recipe_entity = recipe_repo.insert(recipe_entity)
    elif existing_by_name and not yml_id:
        # Name esiste, UUID assente → upsert sulla recipe esistente
        existing_by_name.description = yml_desc
        existing_by_name.scope = yml_scope
        existing_by_name.tasks = yml_tasks
        existing_by_name.tags = yml_tags
        existing_by_name.modified = datetime.now(timezone.utc)
        if yml_derived_from:
            existing_by_name.derived_from = yml_derived_from
        recipe_repo.update(existing_by_name)
        recipe_entity = existing_by_name
    else:
        # Nuova recipe
        recipe_entity = Recipe(
            name=yml_name,
            description=yml_desc,
            scope=yml_scope,
            tasks=yml_tasks,
            tags=yml_tags,
            derived_from=yml_derived_from,
        )
        recipe_entity = recipe_repo.insert(recipe_entity)

    if not recipe_entity or not recipe_entity.id:
        raise RuntimeError("Impossibile creare/trovare la recipe nel DB.")

    # ── Processa ogni entry (distribution + strategy + system prompts) ──
    created_strategies = 0
    for uri, entry in data["entries"].items():
        dist_id = entry.get("dist_id")
        chat_type = entry.get("chat_type")
        replica = int(entry.get("replica", 1))
        sp_contents = entry.get("system_prompt", [])
        sp_names = entry.get("system_prompt_name", [])

        # Verifica distribution esiste
        dist = dist_repo.get_by_id(dist_id) if dist_id else None
        if not dist:
            raise RuntimeError(f"Distribution con ID '{dist_id}' non trovata nel DB (entry: {uri}).")

        # Crea strategy
        strategy = Strategy(
            recipe_id=str(recipe_entity.id),
            distribution_id=str(dist.id),
            replication_factor=replica,
            template_strategy=chat_type,
        )
        strategy = strategy_repo.insert(strategy)
        if not strategy:
            raise RuntimeError(f"Impossibile creare strategy per distribution '{uri}'.")
        created_strategies += 1

        # Processa system prompts posizionalmente
        for sp_name, sp_content in zip(sp_names, sp_contents):
            existing_sp = sp_repo.get_by_name(sp_name)
            if not existing_sp:
                # Crea nuovo system prompt
                new_sp = SystemPrompt(
                    id=None,
                    name=sp_name,
                    description=f"Imported from recipe '{yml_name}'",
                    prompt=sp_content,
                    length=len(sp_content),
                    _lang="un",
                )
                sp_repo.insert(new_sp)

            # Crea strategy_system_prompt
            ssp_repo.insert(StrategySystemPrompt(
                strategy_id=str(strategy.id),
                system_prompt_name=sp_name,
            ))

    return f"Recipe '{recipe_entity.name}' importata con successo ({created_strategies} strategies create)."


def _construct_recipe_download(recipe, enriched_strategies):
    """
    Costruisce la struttura della recipe pronta per l'export in YML.
    Non tenta di recuperare il contenuto testuale dei system prompts (se non
    disponibile) — inserisce solo i nomi posizionali.
    """
    entries = {}
    for item in enriched_strategies:
        s = item["strategy"]
        dist = item.get("distribution")
        prompts = item.get("prompt_names", []) or []

        # Chiave: usiamo la URI se presente, altrimenti l'id della distribution
        key = None
        if dist and getattr(dist, "uri", None):
            key = dist.uri
        else:
            key = str(getattr(dist, "id", s.distribution_id))

        entries[key] = {
            "dist_id": str(getattr(dist, "id", s.distribution_id)) if dist else str(s.distribution_id),
            "dist_name": getattr(dist, "name", None) if dist else None,
            "dist_uri": getattr(dist, "uri", None) if dist else None,
            "tokenized_uri": getattr(dist, "tokenized_uri", None) if dist else None,
            "chat_type": s.template_strategy if s else None,
            "replica": int(s.replication_factor) if s else 1,
            "system_prompt": [],
            "system_prompt_name": list(prompts),
            "samples": None,
            "tokens": None,
            "words": None,
        }

    download = {
        "id": str(getattr(recipe, "id", None)) if recipe else None,
        "name": getattr(recipe, "name", None) if recipe else None,
        "description": getattr(recipe, "description", None) if recipe else None,
        "scope": getattr(recipe, "scope", None) if recipe else None,
        "tasks": getattr(recipe, "tasks", None) if recipe else None,
        "tags": getattr(recipe, "tags", None) if recipe else None,
        "derived_from": getattr(recipe, "derived_from", None) if recipe else None,
        "entries": entries,
    }
    return download


# ─────────────────────────────────────────────
# EDIT SECTION
# ─────────────────────────────────────────────

def _init_edit_widget_keys(st, enriched_strategies):
    """
    Inizializza le widget keys nel session_state SOLO se non esistono ancora.

    Perché è fondamentale:
    - Streamlit ad ogni rerun ri-esegue tutto il file dall'alto in basso.
    - Se passiamo `value=` / `default=` / `index=` ai widget, Streamlit li IGNORA
      quando la key è già nel session_state (comportamento documentato).
    - Quindi per i rerun intermedi (cambio di un widget qualsiasi) i valori
      dell'utente restano intatti perché non sovrascriviamo le key esistenti.
    - Al primo ingresso in edit mode le key non esistono, quindi le popoliamo
      con i valori correnti dal DB.
    """
    for item in enriched_strategies:
        s = item["strategy"]
        s_id = str(s.id)

        if f"edit_rep_{s_id}" not in st.session_state:
            st.session_state[f"edit_rep_{s_id}"] = int(s.replication_factor)

        if f"edit_ct_{s_id}" not in st.session_state:
            st.session_state[f"edit_ct_{s_id}"] = s.template_strategy or ""

        if f"edit_sp_{s_id}" not in st.session_state:
            st.session_state[f"edit_sp_{s_id}"] = list(item["prompt_names"])

        if f"edit_del_{s_id}" not in st.session_state:
            st.session_state[f"edit_del_{s_id}"] = False


def _clear_edit_widget_keys(st, enriched_strategies):
    for item in enriched_strategies:
        s_id = str(item["strategy"].id)
        for prefix in ["edit_rep_", "edit_ct_", "edit_sp_", "edit_del_"]:
            st.session_state.pop(f"{prefix}{s_id}", None)

def _show_edit_section(st, recipe, enriched_strategies, chat_type_codes, prompt_names, data):
    strategy_repo = data["strategy_repo"]
    ssp_repo = data["ssp_repo"]

    # Popola le widget keys solo al primo ingresso in edit mode.
    # Nei rerun successivi (cambio widget) le keys sono già presenti → skip.
    _init_edit_widget_keys(st, enriched_strategies)

    st.markdown("#### ✏️ Edit Strategies")
    col_weights = [3, 1.5, 2, 3, 1]
    header = st.columns(col_weights)
    for col, label in zip(header, ["Distribution", "Replica (int)", "Chat Type", "System Prompts", "Delete"]):
        col.markdown(f"**{label}**")
    st.markdown("---")

    for item in enriched_strategies:
        s = item["strategy"]
        dist = item["distribution"]
        s_id = str(s.id)

        row = st.columns(col_weights)
        dist_name = dist.name if dist else f"[{s.distribution_id}]"
        dist_lang = getattr(dist, "lang", "N/D") if dist else "N/D"
        row[0].markdown(f"**{dist_name}** `{dist_lang}`")

        # NON passiamo value=/index=/default= — la key è già nel session_state
        # e Streamlit userà quel valore automaticamente.
        row[1].number_input(
            "Replica", min_value=1, step=1,
            key=f"edit_rep_{s_id}",
            label_visibility="collapsed"
        )
        row[2].selectbox(
            "Chat Type", options=chat_type_codes,
            key=f"edit_ct_{s_id}",
            label_visibility="collapsed"
        )
        row[3].multiselect(
            "System Prompts", options=prompt_names,
            key=f"edit_sp_{s_id}",
            label_visibility="collapsed"
        )
        row[4].checkbox(
            "🗑️", key=f"edit_del_{s_id}",
            help="Segna per eliminare questa strategy"
        )

    any_marked = any(
        st.session_state.get(f"edit_del_{str(item['strategy'].id)}", False)
        for item in enriched_strategies
    )
    if any_marked:
        st.warning("⚠️ Alcune strategy sono marcate per l'eliminazione. Conferma con 'Save Changes'.")

    st.markdown("---")
    btn_cancel, btn_save = st.columns(2)

    with btn_cancel:
        if st.button("❌ Cancel", key=f"cancel_edit_{recipe.id}", use_container_width=True):
            _clear_edit_widget_keys(st, enriched_strategies)
            st.session_state[f"editing_{recipe.id}"] = False
            st.rerun()

    with btn_save:
        if st.button("💾 Save Changes", key=f"save_edit_{recipe.id}", type="primary", use_container_width=True):
            try:
                # Leggiamo i valori DIRETTAMENTE dalle widget keys.
                # Al momento del click su Save, Streamlit ha già scritto in session_state
                # i valori correnti di tutti i widget → sono garantiti aggiornati.
                edits = {
                    str(item["strategy"].id): {
                        "replica":      st.session_state[f"edit_rep_{item['strategy'].id}"],
                        "chat_type":    st.session_state[f"edit_ct_{item['strategy'].id}"],
                        "prompt_names": st.session_state[f"edit_sp_{item['strategy'].id}"],
                        "to_delete":    st.session_state.get(f"edit_del_{item['strategy'].id}", False),
                    }
                    for item in enriched_strategies
                }
                _apply_edits(strategy_repo, ssp_repo, edits)
                st.success("✅ Recipe aggiornata con successo!")

                _clear_edit_widget_keys(st, enriched_strategies)
                st.session_state[f"editing_{recipe.id}"] = False
                # Invalida cache per forzare reload da DB al prossimo render
                for k in [f"rm_strategies_{recipe.id}", f"rm_stats_{recipe.id}"]:
                    st.session_state.pop(k, None)
                st.rerun()

            except Exception as e:
                logger.error(f"Errore durante il salvataggio: {e}", exc_info=True)
                st.error(f"Errore durante il salvataggio: {e}")

def _apply_edits(strategy_repo, ssp_repo, edits: dict):
    """
    edits: dict[s_id -> {replica, chat_type, prompt_names, to_delete}]
    I valori sono già estratti dalle widget keys prima della chiamata.
    """
    from data_class.entity.table.strategy_system_prompt import StrategySystemPrompt

    for s_id, state in edits.items():
        if state["to_delete"]:
            # ON DELETE CASCADE rimuove anche i SSP collegati
            strategy_repo.delete_by_id(s_id)
            continue

        strategy = strategy_repo.get_by_id(s_id)
        if strategy is None:
            logger.warning(f"Strategy {s_id} non trovata in DB, skip.")
            continue

        strategy.replication_factor = int(state["replica"])
        strategy.template_strategy = state["chat_type"]
        strategy.modified = datetime.utcnow()

        updated = strategy_repo.update(strategy)
        if updated is None:
            raise RuntimeError(f"strategy_repo.update() ha restituito None per id={s_id}")

        # SSP: delete tutto + re-insert lista aggiornata
        ssp_repo.delete_by_strategy_id(s_id)
        for prompt_name in state["prompt_names"]:
            ssp_repo.insert(StrategySystemPrompt(strategy_id=s_id, system_prompt_name=prompt_name))

# ─────────────────────────────────────────────
# DELETE SECTION
# ─────────────────────────────────────────────

def _show_delete_confirmation(st, recipe, recipe_repo):
    confirm_key = f"confirm_delete_{recipe.id}"

    if st.session_state.get(confirm_key) != "awaiting":
        if st.button("🗑️", key=f"del_btn_{recipe.id}", help="Elimina recipe", use_container_width=True):
            st.session_state[confirm_key] = "awaiting"
            st.rerun()
    else:
        st.error(f"Eliminare **{recipe.name}**?")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ Sì", key=f"confirm_yes_{recipe.id}", type="primary", use_container_width=True):
                try:
                    recipe_repo.delete(recipe.id)
                    st.session_state.pop(confirm_key, None)
                    st.session_state.pop("rm_data_cache", None)
                    st.success(f"Recipe '{recipe.name}' eliminata.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Errore eliminazione: {e}")
        with c2:
            if st.button("❌", key=f"confirm_no_{recipe.id}", use_container_width=True):
                st.session_state.pop(confirm_key, None)
                st.rerun()

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

def recipe_management_handler(st):
    from ui.data_studio.strategy.stats_retriver.retriver import DistributionStatsRetriever

    st.subheader("🍽️ Recipe Management")

    # ── YML Upload Box ──
    with st.expander("📤 Import Recipe from YML", expanded=False):
        uploaded_file = st.file_uploader(
            "Trascina o seleziona un file .yml",
            type=["yml", "yaml"],
            key="recipe_yml_upload",
        )
        if uploaded_file is not None:
            if st.button("🚀 Importa Recipe", key="btn_import_yml", type="primary"):
                try:
                    content = uploaded_file.read().decode("utf-8")
                    data = yaml.safe_load(content)
                    if not isinstance(data, dict):
                        st.error("Il file YML non contiene un dizionario valido.")
                    else:
                        validation_err = _validate_yml_structure(data)
                        if validation_err:
                            st.error(f"Validazione fallita: {validation_err}")
                        else:
                            result_msg = _import_recipe_from_yml(st, data)
                            st.success(result_msg)
                            st.session_state.pop("rm_data_cache", None)
                            st.rerun()
                except yaml.YAMLError as e:
                    st.error(f"Errore parsing YAML: {e}")
                except RuntimeError as e:
                    st.error(f"Errore import: {e}")
                except Exception as e:
                    st.error(f"Errore imprevisto: {e}")

    st.write("---")

    if "rm_data_cache" not in st.session_state:
        with st.spinner("Caricamento ricette..."):
            st.session_state.rm_data_cache = _load_all_data(st)

    data = st.session_state.rm_data_cache
    recipes = data["recipes"]
    chat_type_codes = data["chat_type_codes"]
    prompt_names = data["prompt_names"]

    # ── Filtri ──
    fcol1, fcol2 = st.columns([3, 1])
    with fcol1:
        search_query = st.text_input(
            "🔍 Search recipes", placeholder="Cerca per nome o descrizione...", key="rm_search"
        )
    with fcol2:
        lang_filter = st.selectbox("🌐 Language", options=["All"], key="rm_lang_filter")

    filtered_recipes = _filter_recipes(recipes, search_query, None if lang_filter == "All" else lang_filter)
    st.caption(f"Showing {len(filtered_recipes)} / {len(recipes)} recipes")
    st.write("---")

    if not filtered_recipes:
        st.info("Nessuna recipe trovata.")
        return

    for recipe in filtered_recipes:

        # ── Cache strategies ──
        strat_key = f"rm_strategies_{recipe.id}"
        if strat_key not in st.session_state:
            st.session_state[strat_key] = _get_strategies_for_recipe(
                data["strategy_repo"], data["ssp_repo"], data["dist_repo"], recipe.id
            )
        enriched_strategies = st.session_state[strat_key]

        # ── Cache stats ──
        stats_key = f"rm_stats_{recipe.id}"
        if stats_key not in st.session_state:
            stats_cache = {}
            for item in enriched_strategies:
                dist = item["distribution"]
                if dist is None:
                    continue
                try:
                    retriever = DistributionStatsRetriever()
                    mini_bag = {str(dist.id): {"dist": [dist]}}
                    stats_cache.update(retriever.fetch_all_stats(mini_bag))
                except Exception as e:
                    logger.warning(f"Stats non disponibili per dist {dist.id}: {e}")
                    stats_cache[str(dist.id)] = {"samples": 0, "tokens": 0, "words": 0}
            st.session_state[stats_key] = stats_cache
        stats_cache = st.session_state[stats_key]

        totals = _aggregate_recipe_totals(enriched_strategies, stats_cache)

        expander_label = (
            f"**{recipe.name}** &nbsp;|&nbsp; "
            f"v{recipe.version or '1.0.0'} &nbsp;|&nbsp; "
            f"📦 {len(enriched_strategies)} strategies &nbsp;|&nbsp; "
            f"🔢 {totals['samples']:,} samples &nbsp; "
            f"🔤 {totals['tokens']:,} tokens"
        )

        with st.expander(expander_label, expanded=False):

            # ── EDIT METADATA SECTION (COMPARE IN ALTO) ──
            editing_meta = st.session_state.get(f"editing_meta_{recipe.id}", False)
            if editing_meta:
                _show_recipe_metadata_editor(st, recipe, data)
                st.write("---")

            # ── Info recipe + bottoni azione ──
            info_col, action_col = st.columns([5, 1])

            with info_col:
                st.markdown(f"**Description:** {recipe.description or '—'}")
                meta_parts = []
                for label, val in [
                    ("Scope", recipe.scope),
                    ("Version", recipe.version),
                    ("Issued", str(recipe.issued)[:10] if recipe.issued else None),
                    ("Modified", str(recipe.modified)[:10] if recipe.modified else None),
                ]:
                    if val:
                        meta_parts.append(f"{label}: `{val}`")
                if meta_parts:
                    st.caption(" &nbsp;|&nbsp; ".join(meta_parts))

                if recipe.tags:
                    tags_html = " ".join([
                        f'<span style="background-color:#1e3a5f;color:#7ec8e3;padding:2px 9px;border-radius:10px;'
                        f'font-size:0.73rem;font-weight:600;margin:2px;display:inline-block;">{t}</span>'
                        for t in recipe.tags
                    ])
                    st.markdown(
                        f'<div style="margin-top:4px;"><span style="font-size:0.75rem;color:#aaa;margin-right:6px;">'
                        f'Tags</span>{tags_html}</div>', unsafe_allow_html=True
                    )
                if recipe.tasks:
                    task_html = " ".join([
                        f'<span style="background-color:#2d1f4e;color:#c4a7e7;padding:2px 9px;border-radius:10px;'
                        f'font-size:0.73rem;font-weight:600;margin:2px;display:inline-block;">📌 {t}</span>'
                        for t in recipe.tasks
                    ])
                    st.markdown(
                        f'<div style="margin-top:4px;"><span style="font-size:0.75rem;color:#aaa;margin-right:6px;">'
                        f'Tasks</span>{task_html}</div>', unsafe_allow_html=True
                    )

            with action_col:
                editing_strategies = st.session_state.get(f"editing_{recipe.id}", False)
                # Mostra bottoni solo se non è già in corso un editing
                if not editing_strategies and not editing_meta:
                    c1, c2 = st.columns(2)
                    if c1.button("📝", key=f"edit_meta_btn_{recipe.id}", help="Modifica metadati recipe"):
                        st.session_state[f"editing_meta_{recipe.id}"] = True
                        st.rerun()
                    if c2.button("✏️", key=f"edit_btn_{recipe.id}", help="Modifica strategies"):
                        st.session_state[f"editing_{recipe.id}"] = True
                        st.rerun()
                # Always offer a YAML download of the recipe (constructed from DB data)
                try:
                    download_struct = _construct_recipe_download(recipe, enriched_strategies)
                    yaml_str = yaml.dump(download_struct, sort_keys=False)
                    st.download_button(
                        label="📥 Download YML",
                        data=yaml_str,
                        file_name=f"{recipe.name}_recipe.yaml",
                        mime="text/yaml",
                        key=f"download_yml_btn_{recipe.id}",
                    )
                except Exception as e:
                    logger.debug(f"Failed to prepare download for recipe {recipe.id}: {e}")
                
                _show_delete_confirmation(st, recipe, data["recipe_repo"])

            st.markdown("---")

            # ── Totali + pie lingua ──
            tc1, tc2, tc3, tc4 = st.columns([1, 1, 1, 2])
            tc1.metric("Samples", f"{totals['samples']:,}")
            tc2.metric("Tokens", f"{totals['tokens']:,}")
            tc3.metric("Words", f"{totals['words']:,}")
            with tc4:
                lang_data = {l: c for l, c in totals["langs"].items() if c > 0}
                if lang_data:
                    fig = go.Figure(data=[go.Pie(
                        labels=list(lang_data.keys()), values=list(lang_data.values()),
                        hole=0.45, textinfo="label+percent", textfont=dict(size=10),
                        marker=dict(line=dict(color="#1a1a2e", width=1.5)),
                    )])
                    fig.update_layout(
                        margin=dict(t=5, b=5, l=5, r=5), height=130,
                        showlegend=False, paper_bgcolor="rgba(0,0,0,0)"
                    )
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"plotly_chart_{recipe.id}")

            st.markdown("---")

            # ── Tabella: read-only o edit ──
            if editing_strategies:
                _show_edit_section(st, recipe, enriched_strategies, chat_type_codes, prompt_names, data)
            else:
                col_weights = [3, 1.2, 1.5, 1.5, 1.5, 3]
                header = st.columns(col_weights)
                for col, label in zip(header, ["Distribution", "Lang", "Replica", "Chat Type", "Samples", "System Prompts"]):
                    col.markdown(f"**{label}**")
                st.markdown("---")

                for item in enriched_strategies:
                    s = item["strategy"]
                    dist = item["distribution"]
                    d_id_str = str(dist.id) if dist else None
                    stats = stats_cache.get(d_id_str, {"samples": 0, "tokens": 0, "words": 0}) if d_id_str else {}
                    rep = s.replication_factor

                    row = st.columns(col_weights)
                    row[0].markdown(f"**{dist.name if dist else '—'}**")
                    row[1].markdown(f"`{getattr(dist, 'lang', 'N/D') if dist else '—'}`")
                    row[2].markdown(f"`×{rep}`")
                    row[3].markdown(f"`{s.template_strategy or '—'}`")
                    row[4].markdown(f"`{int(stats.get('samples', 0) * rep):,}`")

                    prompts = item["prompt_names"]
                    if prompts:
                        badges = " ".join([
                            f'<span style="background-color:#1a3a2e;color:#7ec3a3;padding:2px 8px;'
                            f'border-radius:8px;font-size:0.7rem;display:inline-block;margin:1px;">{p}</span>'
                            for p in prompts
                        ])
                        row[5].markdown(badges, unsafe_allow_html=True)
                    else:
                        row[5].caption("—")

