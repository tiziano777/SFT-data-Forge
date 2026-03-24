import os
import logging

from utils.streamlit_func import reset_dashboard_session_state
from config.state_vars import home_vars
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.dataset_card_repository import DatasetCardRepository
from data_class.repository.table.card_composition_repository import CardCompositionRepository
from data_class.entity.table.dataset import Dataset
from data_class.entity.table.distribution import Distribution

BASE_PREFIX = os.getenv("BASE_PREFIX", "")
MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR", "")

logger = logging.getLogger(__name__)


def _build_dist_label(dist: Distribution) -> str:
    path_short = dist.uri.replace(BASE_PREFIX, "")
    return f"📦 {dist.name} (v.{dist.version}) - {path_short}"


def _load_bag_from_recipe( recipe, strategy_repo, dist_repo, card_repo, comp_repo, ds_repo, ssp_repo, naming_convention: str = "derived"):
    """
    Dato un recipe object, carica tutte le strategy collegate,
    recupera le distribution e ricostruisce il dist_bag.
    Ritorna il bag ricostruito e la nuova recipe_entity prefillata.

    naming_convention: "derived" | "extended" — determina prefisso e tag della nuova recipe.
    """
    from data_class.entity.table.recipe import Recipe
    from datetime import datetime

    strategies = strategy_repo.get_by_recipe_id(recipe.id)
    new_bag = {}
    old_recipe = {}

    for strategy in strategies:
        dist_obj = dist_repo.get_by_id(strategy.distribution_id)
        if dist_obj is None:
            logger.warning(f"Distribution {strategy.distribution_id} non trovata, skip.")
            continue

        # Costruisci old_recipe per pre-fill nei passi successivi
        ssp_list = ssp_repo.get_by_strategy_id(strategy.id)
        old_recipe[dist_obj.uri] = {
            "replication_factor": strategy.replication_factor,
            "template_strategy": strategy.template_strategy,
            "system_prompt_names": [ssp.system_prompt_name for ssp in ssp_list]
        }

        # Recupera il dataset parent della distribution
        ds_obj = ds_repo.get_by_id(dist_obj.dataset_id)
        if ds_obj is None:
            logger.warning(f"Dataset per distribution {dist_obj.id} non trovato, skip.")
            continue

        card = card_repo.get_by_id(ds_obj.derived_card) if ds_obj.derived_card else card_repo.get_by_name(ds_obj.name)
        comp = comp_repo.get_children_by_parent(card.dataset_name) if card else []

        ds_key = str(ds_obj.name)
        if ds_key not in new_bag:
            new_bag[ds_key] = {
                'dist': [dist_obj],
                'ds': ds_obj,
                'card': card,
                'composition': comp
            }
        else:
            if not any(d.id == dist_obj.id for d in new_bag[ds_key]['dist']):
                new_bag[ds_key]['dist'].append(dist_obj)

    # Prefill recipe entity
    new_tags = list(recipe.tags) if recipe.tags else []
    if naming_convention not in new_tags:
        new_tags.append(naming_convention)

    label = naming_convention.upper()
    new_recipe = Recipe(
        id=None,
        name=f"{naming_convention}__{recipe.name}",
        description=f"[{label}] {recipe.description}",
        scope=recipe.scope,
        tasks=list(recipe.tasks) if recipe.tasks else [],
        issued=datetime.now().isoformat(),
        modified=datetime.now().isoformat(),
        tags=new_tags,
        derived_from=recipe.id
    )

    return new_bag, new_recipe, old_recipe


def render_recipe_form(st, prefill_expanded: bool = False):
    """Expander con form per inserimento recipe."""

    from data_class.repository.vocabulary.vocab_task_repository import VocabTaskRepository
    from data_class.repository.vocabulary.vocab_dataset_type_repository import VocabDatasetTypeRepository
    from data_class.repository.table.recipe_repository import RecipeRepository
    from data_class.entity.table.recipe import Recipe
    from datetime import datetime

    recipe_repository = RecipeRepository(st.session_state.db_manager)
    vocab_task_repo = VocabTaskRepository(st.session_state.db_manager)
    vocab_dataset_type_repository = VocabDatasetTypeRepository(st.session_state.db_manager)

    SCOPE = [item.code for item in vocab_dataset_type_repository.get_all()]
    TASKS_VOCABULARY = [item.code for item in vocab_task_repo.get_all()]

    # Leggi prefill dalla session_state se presente
    prefill: Recipe | None = st.session_state.get("recipe_entity", None)

    default_name = prefill.name if prefill else ""
    default_desc = prefill.description if prefill else ""
    default_scope = prefill.scope if prefill and prefill.scope in SCOPE else (SCOPE[0] if SCOPE else None)
    default_tasks = [t for t in prefill.tasks if t in TASKS_VOCABULARY] if prefill and prefill.tasks else []
    default_tags = ", ".join(prefill.tags) if prefill and prefill.tags else ""
    
    default_derived_from = prefill.derived_from if prefill and prefill.derived_from else None
    default_derived_from_name = recipe_repository.get_by_id(prefill.derived_from).name if default_derived_from else None

    with st.expander("📋 Recipe Configuration", expanded=prefill_expanded):
        with st.form(key="recipe_form"):
            name = st.text_input("Recipe Name *", value=default_name)
            description = st.text_area("Description *", value=default_desc)

            scope_idx = SCOPE.index(default_scope) if default_scope in SCOPE else 0
            scope = st.selectbox("Scope *", SCOPE, index=scope_idx)

            tasks = st.multiselect("Tasks", TASKS_VOCABULARY, default=default_tasks)
            tags = st.text_input("Tags (comma separated)", value=default_tags)
            
            recipe_names = [""] + [r.name for r in recipe_repository.get_all()]

            if default_derived_from is not None and default_derived_from_name in recipe_names:
                default_index = recipe_names.index(default_derived_from_name)
            else:
                default_index = 0
            derived_from = st.selectbox("Derived From (optional)", recipe_names, index=default_index)


            if st.form_submit_button("Save Recipe"):
                tags_list = [t.strip() for t in tags.split(",")] if tags else []
                if name and tasks:
                    recipe = Recipe(
                        id=None,
                        name=name,
                        description=description,
                        scope=scope,
                        tasks=tasks,
                        issued=datetime.now().isoformat(),
                        modified=datetime.now().isoformat(),
                        tags=tags_list,
                        derived_from= recipe_repository.get_by_name(derived_from).id if derived_from else None
                    )
                    is_valid, error_message = recipe_repository.is_valid(recipe)
                    if is_valid:
                        st.session_state.recipe_entity = recipe
                        st.session_state.recipe_form_expanded = False
                        st.success("✅ Recipe saved!")
                        st.rerun()
                    else:
                        st.error(f"❌ Recipe not valid: {error_message}")
                else:
                    st.error("Name and Tasks are required")


def show_distribution_details(st, item: dict, idx: int):
    """
    Visualizzazione arricchita che mette in risalto le caratteristiche
    distintive della distribution (Step, Modality, reasoning, skills).
    """
    dist: Distribution = item['dist']
    ds: Dataset = item['ds']
    card = item.get('card')
    composition = item.get('composition', [])

    col_text, col_view, col_del = st.columns([6, 1, 1])

    with col_del:
        if st.button("🗑️", key=f"del_btn_{dist.id}_{idx}", help="Rimuovi dal Bag"):
            # idx è nel formato "ds_key_distidx"
            parts = str(idx).rsplit("_", 1)
            ds_key = parts[0]
            d_idx = int(parts[1])
            if ds_key in st.session_state.dist_bag:
                st.session_state.dist_bag[ds_key]['dist'].pop(d_idx)
                if not st.session_state.dist_bag[ds_key]['dist']:
                    del st.session_state.dist_bag[ds_key]
            st.rerun()

    st.markdown("#### ⚡ Quick Look")
    h1, h2, h3, h4 = st.columns(4)
    with h1:
        st.metric("Step", f"Stage {dist.step}")
    with h2:
        mod = card.modality.upper() if card else "N/D"
        st.metric("Modality", mod)
    with h3:
        st.metric("Format", dist.format.upper())
    with h4:
        reasoning = "✅" if (card and card.has_reasoning) else "❌"
        st.metric("Reasoning", reasoning)

    st.write("---")

    t1, t2, t3 = st.tabs(["📂 File System & Origin", "🧠 Skills & Tasks", "🧬 Composition (MIX)"])

    with t1:
        st.markdown(f"**URI Originale:** `{dist.uri}`")
        st.markdown(f"**URI Tokenized:** `{dist.tokenized_uri}`" if dist.tokenized_uri else "URI tokenized non disponibile")
        st.markdown(f"**Glob Pattern:**")
        st.code(dist.glob, language="bash")
        c1, c2 = st.columns(2)
        with c1:
            st.write(f"**License:** `{dist.license}`")
            st.write(f"**Version:** `{dist.version}`")
        with c2:
            st.write(f"**Issued:** {dist.issued}")
            st.write(f"**Modified:** {dist.modified}")

    with t2:
        if card:
            st.markdown(f"**ID Dataset:** `{card.dataset_id}`")
            st.markdown(f"**Nome Dataset:** `{card.dataset_name}`")
            if card.core_skills:
                st.write("**Core Skills:**")
                skills_html = "".join([
                    f'<span style="background-color: #2e7d32; color: white; padding: 2px 8px; '
                    f'border-radius: 10px; margin-right: 5px; font-size: 0.8em;">{s}</span>'
                    for s in card.core_skills
                ])
                st.markdown(skills_html, unsafe_allow_html=True)
            st.write("")
            if card.tasks:
                st.write("**Target Tasks:**")
                tasks_html = "".join([
                    f'<span style="background-color: #1565c0; color: white; padding: 2px 8px; '
                    f'border-radius: 10px; margin-right: 5px; font-size: 0.8em;">{t}</span>'
                    for t in card.tasks
                ])
                st.markdown(tasks_html, unsafe_allow_html=True)
            if card.dataset_description:
                st.info(card.dataset_description)
        else:
            st.info("Nessun metadato semantico disponibile (Card mancante).")

    with t3:
        if composition:
            st.write("**Composizione del Dataset MIX:**")
            comp_data = [
                {
                    "Componente": c.child_card_name,
                    "Peso (%)": f"{float(c.weight) * 100:.1f}%" if c.weight else "0.0%",
                    "Barra": float(c.weight) if c.weight else 0
                }
                for c in composition
            ]
            st.table(comp_data)
        else:
            st.write("Questo è un dataset atomico (non derivato da MIX).")


def _render_load_from_recipe_section(st, recipe_repo, strategy_repo, dist_repo, card_repo, comp_repo, ds_repo, ssp_repo):
    """
    Sezione in cima alla pagina per caricare il bag da una recipe esistente.
    """
    # Toggle visibilità sezione
    if "show_load_recipe_section" not in st.session_state:
        st.session_state.show_load_recipe_section = False

    btn_label = "✖ Cancel" if st.session_state.show_load_recipe_section else "📂 Start from existing recipe"
    if st.button(btn_label, key="toggle_load_recipe"):
        st.session_state.show_load_recipe_section = not st.session_state.show_load_recipe_section
        st.rerun()

    if not st.session_state.show_load_recipe_section:
        return

    with st.container(border=True):
        st.markdown("#### 📂 Load from existing Recipe")
        st.caption("Il bag verrà ricostruito dalle distribution collegate. Potrai modificarlo prima di procedere.")

        all_recipes = recipe_repo.get_all()
        if not all_recipes:
            st.warning("Nessuna recipe disponibile.")
            return

        recipe_map = {f"{r.name} (id={r.id})": r for r in all_recipes}
        selected_recipe_label = st.selectbox(
            "Seleziona Recipe",
            [""] + list(recipe_map.keys()),
            key="load_recipe_selectbox"
        )

        if selected_recipe_label:
            selected_recipe = recipe_map[selected_recipe_label]

            # Toggle per scegliere la naming convention
            is_extended = st.toggle(
                "Extended mode",
                value=False,
                help="Attiva per creare una ricetta **extended**. Disattiva per creare una ricetta **derived**.",
                key="recipe_naming_convention_toggle"
            )
            naming_convention = "extended" if is_extended else "derived"

            # Preview info recipe selezionata
            with st.expander("ℹ️ Recipe Info", expanded=False):
                st.write(f"**Scope:** {selected_recipe.scope}")
                st.write(f"**Tasks:** {', '.join(selected_recipe.tasks) if selected_recipe.tasks else '-'}")
                st.write(f"**Tags:** {', '.join(selected_recipe.tags) if selected_recipe.tags else '-'}")
                st.write(f"**Description:** {selected_recipe.description}")
                st.write(f"**Derived From:** {selected_recipe.derived_from if selected_recipe.derived_from else 'N/A'}")

            if st.button("⬇️ Load into Bag", key="confirm_load_recipe", type="primary", use_container_width=True):
                with st.spinner("Caricamento distributions dalla recipe..."):
                    new_bag, derived_recipe, old_recipe = _load_bag_from_recipe(
                        selected_recipe, strategy_repo, dist_repo,
                        card_repo, comp_repo, ds_repo, ssp_repo, naming_convention
                    )

                if not new_bag:
                    st.error("❌ Nessuna distribution valida trovata per questa recipe.")
                else:
                    st.session_state.dist_bag = new_bag
                    st.session_state.old_recipe = old_recipe
                    st.session_state.recipe_entity = derived_recipe
                    st.session_state.recipe_form_expanded = True
                    st.session_state.show_load_recipe_section = False
                    st.session_state.studio_reset_counter += 1
                    n_dist = sum(len(v['dist']) for v in new_bag.values())
                    st.success(f"✅ Caricati {n_dist} distribution da {len(new_bag)} dataset!")
                    st.rerun()


def data_studio(st):
    st.write("## 🎨 Data Studio")
    st.write("---")

    # Inizializzazione session state
    if "dist_bag" not in st.session_state:
        st.session_state.dist_bag = {}
    if "studio_reset_counter" not in st.session_state:
        st.session_state.studio_reset_counter = 0
    if "recipe_form_expanded" not in st.session_state:
        st.session_state.recipe_form_expanded = False

    ds_repo = DatasetRepository(st.session_state.db_manager)
    dist_repo = DistributionRepository(st.session_state.db_manager)
    card_repo = DatasetCardRepository(st.session_state.db_manager)
    comp_repo = CardCompositionRepository(st.session_state.db_manager)

    # Import lazy dei repository necessari per il load-from-recipe
    from data_class.repository.table.recipe_repository import RecipeRepository
    from data_class.repository.table.strategy_repository import StrategyRepository
    from data_class.repository.table.strategy_system_prompt_repository import StrategySystemPromptRepository

    recipe_repo = RecipeRepository(st.session_state.db_manager)
    strategy_repo = StrategyRepository(st.session_state.db_manager)
    ssp_repo = StrategySystemPromptRepository(st.session_state.db_manager)

    # ── SEZIONE LOAD FROM EXISTING RECIPE (in cima) ──────────────────────────
    _render_load_from_recipe_section(
        st, recipe_repo, strategy_repo, dist_repo, card_repo, comp_repo, ds_repo, ssp_repo
    )
    st.write("---")

    # ── BARRE DI RICERCA & SELEZIONE DATASET/DISTRIBUTION ────────────────────
    col_s1, col_s2 = st.columns(2)
    search_ds = col_s1.text_input("🔍 Cerca Dataset", key=f"search_ds_{st.session_state.studio_reset_counter}")
    search_dist = col_s2.text_input("🔍 Cerca Distribution", key=f"search_dist_{st.session_state.studio_reset_counter}")

    all_ds = ds_repo.get_all()
    mapped_prefix = f"{BASE_PREFIX}{MAPPED_DATA_DIR}"
    valid_ds = [d for d in all_ds if (d.uri and d.uri.startswith(mapped_prefix)) or (getattr(d, 'step', 0) == 3)]

    if search_ds:
        valid_ds = [d for d in valid_ds if search_ds.lower() in d.name.lower() or search_ds.lower() in d.uri.lower()]

    ds_map = {f"{d.name} (v{d.version})": d for d in valid_ds}

    selected_ds_label = st.selectbox(
        "1. Seleziona Dataset",
        [""] + list(ds_map.keys()),
        key=f"ds_sel_{st.session_state.studio_reset_counter}"
    )

    if selected_ds_label:
        ds_obj = ds_map[selected_ds_label]
        distributions = dist_repo.get_by_dataset_id_and_materialized(ds_obj.id, True)

        if search_dist:
            distributions = [d for d in distributions if search_dist.lower() in d.name.lower() or search_dist.lower() in d.uri.lower()]

        dist_map = {_build_dist_label(d): d for d in distributions}

        selected_dist_label = st.selectbox(
            "2. Seleziona Distribution",
            [""] + list(dist_map.keys()),
            key=f"dist_sel_{st.session_state.studio_reset_counter}"
        )

        if selected_dist_label:
            dist_obj = dist_map[selected_dist_label]
            if st.button("➕ Add to Bag", use_container_width=True, type="primary"):
                ds_id_key = str(ds_obj.name)
                already_in = False
                if ds_id_key in st.session_state.dist_bag:
                    if any(d.id == dist_obj.id for d in st.session_state.dist_bag[ds_id_key]['dist']):
                        already_in = True

                if not already_in:
                    card = card_repo.get_by_id(ds_obj.derived_card) if ds_obj.derived_card else card_repo.get_by_name(ds_obj.name)
                    comp = comp_repo.get_children_by_parent(card.dataset_name) if card else []

                    if ds_id_key not in st.session_state.dist_bag:
                        st.session_state.dist_bag[ds_id_key] = {
                            'dist': [dist_obj],
                            'ds': ds_obj,
                            'card': card,
                            'composition': comp
                        }
                    else:
                        st.session_state.dist_bag[ds_id_key]['dist'].append(dist_obj)

                    st.session_state.studio_reset_counter += 1
                    st.rerun()
                else:
                    st.warning("Distribuzione già nel bag.")

    st.write("---")

    # ── BAG VIEWER ───────────────────────────────────────────────────────────
    total_dist_count = sum(len(v['dist']) for v in st.session_state.dist_bag.values())
    st.write(f"### 🛍️ Selected Bag ({total_dist_count} dist in {len(st.session_state.dist_bag)} datasets)")

    for ds_id, group in st.session_state.dist_bag.items():
        with st.expander(f"📦 Dataset: {group['ds'].name} ({len(group['dist'])} distributions)", expanded=False):
            for d_idx, d_obj in enumerate(group['dist']):
                temp_item = {
                    'dist': d_obj,
                    'ds': group['ds'],
                    'card': group['card'],
                    'composition': group['composition']
                }
                st.markdown(f"**{d_idx + 1}. {d_obj.name}**")
                show_distribution_details(st, temp_item, f"{ds_id}_{d_idx}")
                if d_idx < len(group['dist']) - 1:
                    st.divider()

    st.write("---")

    # ── RECIPE FORM ───────────────────────────────────────────────────────────
    st.write("### 📝 Save Recipe Details")
    if 'recipe_entity' not in st.session_state:
        st.session_state.recipe_entity = None

    prefill_expanded = st.session_state.get("recipe_form_expanded", False)
    render_recipe_form(st, prefill_expanded=prefill_expanded)

    # ── NAVIGATION ────────────────────────────────────────────────────────────
    col1, col2 = st.columns([1, 2])
    with col1:
        if st.button("🏠 Home", key="back_home"):
            if "dist_bag" in st.session_state:
                del st.session_state.dist_bag
            reset_dashboard_session_state(st, home_vars)
            st.session_state.current_stage = "home"
            st.rerun()
    with col2:
        button_disabled = st.session_state.recipe_entity is None
        if st.button("🚀 Proceed to Receipt Builder", key="to_pipeline", type="primary", disabled=button_disabled):
            st.session_state.current_stage = "data_studio_stage_area"
            st.rerun()