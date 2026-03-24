import os
import logging

from config.state_vars import home_vars
from utils.streamlit_func import reset_dashboard_session_state
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.dataset_card_repository import DatasetCardRepository
from data_class.repository.table.card_composition_repository import CardCompositionRepository
from data_class.entity.table.distribution import Distribution
import duckdb
from ui.dataset_comparator.comparator import DistributionComparator

BASE_PREFIX = os.getenv("BASE_PREFIX", "")
MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR", "")
STATS_DATA_DIR = os.getenv("STATS_DATA_DIR", "")
LOW_LEVEL_STATS_EXTENSION = os.getenv("LOW_LEVEL_STATS_EXTENSION", "")

logger = logging.getLogger(__name__)


def _normalize_uri_to_path(uri: str) -> str:
    if not uri: return ""
    return os.path.normpath(uri.replace(BASE_PREFIX, ""))


def _build_dist_label(dist: Distribution) -> str:
    path_short = dist.uri.replace(BASE_PREFIX, "")
    return f"📦 {dist.name} (v.{dist.version}) - {path_short}"


def _initialize_repositories(st_app):
    return {
        'dataset': DatasetRepository(st_app.session_state.db_manager),
        'distribution': DistributionRepository(st_app.session_state.db_manager),
        'dataset_card': DatasetCardRepository(st_app.session_state.db_manager),
        'composition': CardCompositionRepository(st_app.session_state.db_manager)
    }


def _safe_write(kv_callable, label, value):
    """Helper: mostra label solo se value non è vuoto/null."""
    try:
        if value is None:
            return
        if isinstance(value, (list, dict)) and len(value) == 0:
            return
        if isinstance(value, str) and not value.strip():
            return
        kv_callable(f"**{label}:** {value}")
    except Exception:
        return


def _show_preview(st_app, ds_obj, dist_obj, repos):
    """Compact preview of dataset+distribution using dataset card and composition when available.
    Show dataset-level info and dataset card only when fields are present (no redundant/null values).
    """
    if not ds_obj or not dist_obj:
        st_app.info("Nessuna selezione da mostrare.")
        return

    card = None
    composition = []
    try:
        card = repos['dataset_card'].get_by_id(ds_obj.derived_card) if getattr(ds_obj, 'derived_card', None) else repos['dataset_card'].get_by_name(ds_obj.name)
        if card:
            composition = repos['composition'].get_children_by_parent(card.dataset_name)
    except Exception as e:
        logger.debug("Errore loading card/composition: %s", e)

    with st_app.expander(f"{dist_obj.name} — [{ds_obj.name}]", expanded=True):
        # Dataset-level info (mostrare solo campi utili e non ridondanti)
        _safe_write(st_app.write, "Dataset", f"{ds_obj.name} (v{getattr(ds_obj, 'version', 'N/D')})")
        _safe_write(st_app.write, "Dataset URI", getattr(ds_obj, 'uri', None))
        _safe_write(st_app.write, "Dataset Description", getattr(ds_obj, 'description', None))

        st_app.write("---")

        # Distribution info (mostrare campi rilevanti)
        _safe_write(st_app.write, "Distribution URI", getattr(dist_obj, 'uri', None))
        _safe_write(st_app.write, "Version", getattr(dist_obj, 'version', None))
        _safe_write(st_app.write, "Step", getattr(dist_obj, 'step', None))
        _safe_write(st_app.write, "Format", getattr(dist_obj, 'format', None))

        # Card info - mostrala solo se presente e con campi non vuoti
        if card:
            st_app.write("---")
            st_app.write(f"**Card:** {card.dataset_name} — Modality: {getattr(card, 'modality', 'N/D')}")

            if getattr(card, 'dataset_description', None):
                st_app.info(card.dataset_description)

            if getattr(card, 'core_skills', None):
                skills = [s for s in card.core_skills if s]
                if skills:
                    st_app.write("**Core Skills:**")
                    st_app.write(", ".join(skills))

            if getattr(card, 'tasks', None):
                tasks = [t for t in card.tasks if t]
                if tasks:
                    st_app.write("**Target Tasks:**")
                    st_app.write(", ".join(tasks))

            # Composition: mostralo solo se non vuoto
            if composition:
                comp_data = []
                for c in composition:
                    try:
                        weight_val = float(c.weight) if getattr(c, 'weight', None) else 0
                    except Exception:
                        weight_val = 0
                    comp_data.append({
                        "Componente": getattr(c, 'child_card_name', getattr(c, 'child_card', 'N/D')),
                        "Peso (%)": f"{weight_val * 100:.1f}%"
                    })
                if comp_data:
                    st_app.write("**Composizione del Card:**")
                    st_app.table(comp_data)
        else:
            st_app.info("Nessun metadato semantico disponibile (Card mancante).")


def _get_stats_path_for_distribution(distribution: Distribution) -> str:
    """Return the stats directory path (local) for a distribution."""
    if not distribution or not distribution.uri:
        return ""
    
    # Remove base prefix from URI
    data_path = distribution.uri.replace(BASE_PREFIX, '')
    
    # Handle case where mapped_data_dir might be empty
    if MAPPED_DATA_DIR and data_path.startswith(MAPPED_DATA_DIR):
        stats_path = data_path.replace(MAPPED_DATA_DIR, STATS_DATA_DIR)
    else:
        stats_path = data_path
    
    # Add extension if needed
    if LOW_LEVEL_STATS_EXTENSION and not stats_path.endswith(LOW_LEVEL_STATS_EXTENSION):
        stats_path = stats_path + LOW_LEVEL_STATS_EXTENSION
    
    # Ensure it's a valid path
    stats_path = os.path.normpath(stats_path)
    
    # Debug logging
    logger.debug(f"Stats path for distribution {distribution.name}: {stats_path}")
    
    return stats_path


def show_distribution_pair_selection(st_app):
    """Seleziona due dataset+distribution per confronto: dataset1->dist1 e dataset2->dist2.
    Include ricerca per nome/descrizione e preview delle scelte.
    """
    st_app.header("📂 Seleziona due Distribution (Per confronto)")

    # Documentazione metriche buttons
    docs_col1, docs_col2, docs_col3 = st_app.columns([1,1,4])
    with docs_col1:
        if st_app.button("📘 Metriche"):
            st_app.session_state.show_doc = 'description'
            st_app.rerun()
    with docs_col2:
        if st_app.button("🧭 Use Cases"):
            st_app.session_state.show_doc = 'scope'
            st_app.rerun()
    with docs_col3:
        # If viewing docs, show content and back button
        if st_app.session_state.get('show_doc'):
            base_dir = os.path.dirname(__file__)
            doc_name = st_app.session_state.get('show_doc')
            md_path = os.path.join(base_dir, f"{doc_name}.md")
            try:
                with open(md_path, 'r', encoding='utf-8') as fh:
                    content = fh.read()
                st_app.markdown(content)
            except Exception as e:
                logger.exception(e)
                st_app.error(f"Errore apertura file doc: {md_path}")

            if st_app.button("◀ Torna alla selezione", key="back_from_doc"):
                st_app.session_state.show_doc = None
                st_app.rerun()
            # stop rendering the rest of the UI while in doc view
            return

    # Initialize session state
    st_app.session_state.setdefault('available_datasets', [])
    st_app.session_state.setdefault('available_dataset_labels', [])
    st_app.session_state.setdefault('selected_dataset_ids', [None, None])
    st_app.session_state.setdefault('selected_distributions', [None, None])
    st_app.session_state.setdefault('selected_dataset_paths', [MAPPED_DATA_DIR, MAPPED_DATA_DIR])
    st_app.session_state.setdefault('comp_search_terms', ["", ""])
    st_app.session_state.setdefault('last_comparison_report', None)
    st_app.session_state.setdefault('last_comparison_results', None)

    repos = _initialize_repositories(st_app)

    # Caricamento dataset (mapped prefix o step 3)
    if not st_app.session_state.available_datasets:
        uri_filter = f"{BASE_PREFIX}{MAPPED_DATA_DIR}" if MAPPED_DATA_DIR else BASE_PREFIX
        try:
            datasets = repos['dataset'].get_by_uri_prefix_or_step(uri_filter, step=3)
        except Exception as e:
            st_app.error("Errore caricamento dataset")
            logger.exception(e)
            return

        st_app.session_state.available_datasets = datasets
        st_app.session_state.available_dataset_labels = [f"{d.name} (v{d.version})" for d in datasets]

    # Two columns for dataset+distribution selection
    left_col, right_col = st_app.columns(2)

    ds_maps = [None, None]
    dist_maps = [None, None]
    selected_ds_objs = [None, None]
    selected_dist_objs = [None, None]

    for i, col in enumerate([left_col, right_col]):
        with col:
            st_app.subheader(f"Distribution {'A' if i == 0 else 'B'}")
            
            # search box
            search_key = f"comp_search_ds_{i}"
            search_val = col.text_input(
                "🔍 Cerca Dataset (name o description)", 
                value=st_app.session_state.comp_search_terms[i],
                key=search_key
            )
            
            # Update session state
            st_app.session_state.comp_search_terms[i] = search_val

            # build options filtered by search
            if search_val:
                filtered = [
                    d for d in st_app.session_state.available_datasets 
                    if (search_val.lower() in d.name.lower()) 
                    or (search_val.lower() in getattr(d, 'description', '').lower())
                ]
            else:
                filtered = st_app.session_state.available_datasets

            lab_map = {f"{d.name} (v{d.version})": d for d in filtered}
            ds_maps[i] = lab_map

            options = [""] + list(lab_map.keys())
            sel_key = f"comp_ds_select_{i}"
            
            # Get previously selected dataset label if any
            prev_selected_label = ""
            if st_app.session_state.selected_dataset_ids[i]:
                try:
                    prev_ds = repos['dataset'].get_by_id(st_app.session_state.selected_dataset_ids[i])
                    prev_selected_label = f"{prev_ds.name} (v{prev_ds.version})"
                except Exception:
                    prev_selected_label = ""
            
            selected_label = col.selectbox(
                f"1. Seleziona Dataset", 
                options, 
                index=options.index(prev_selected_label) if prev_selected_label in options else 0,
                key=sel_key
            )

            if selected_label:
                ds_obj = lab_map[selected_label]
                selected_ds_objs[i] = ds_obj

                # reset if dataset changed
                if st_app.session_state.selected_dataset_ids[i] != ds_obj.id:
                    st_app.session_state.selected_dataset_ids[i] = ds_obj.id
                    st_app.session_state.selected_dataset_paths[i] = _normalize_uri_to_path(ds_obj.uri)
                    st_app.session_state.selected_distributions[i] = None

                # build distributions for this dataset
                distributions = repos['distribution'].get_by_dataset_id(ds_obj.id)
                mapped_prefix = f"{BASE_PREFIX}{MAPPED_DATA_DIR}"
                distributions = [
                    d for d in distributions 
                    if (d.uri and d.uri.startswith(mapped_prefix)) 
                    or getattr(d, 'step', 0) == 3
                ]

                dist_labels = [""] + [_build_dist_label(d) for d in distributions]
                dist_map = {_build_dist_label(d): d for d in distributions}
                dist_maps[i] = dist_map

                dist_key = f"comp_dist_select_{i}"
                
                # Get previously selected distribution label if any
                prev_dist_label = ""
                if st_app.session_state.selected_distributions[i]:
                    prev_dist = st_app.session_state.selected_distributions[i]
                    prev_dist_label = _build_dist_label(prev_dist) if hasattr(prev_dist, 'id') else ""
                
                sel_dist_label = col.selectbox(
                    "2. Seleziona Distribution", 
                    dist_labels, 
                    index=dist_labels.index(prev_dist_label) if prev_dist_label in dist_labels else 0,
                    key=dist_key
                )
                
                if sel_dist_label:
                    sel_dist = dist_map.get(sel_dist_label)
                    selected_dist_objs[i] = sel_dist
                    
                    # Update session state only if different
                    if sel_dist and (not st_app.session_state.selected_distributions[i] or 
                                   getattr(st_app.session_state.selected_distributions[i], 'id', None) != getattr(sel_dist, 'id', None)):
                        st_app.session_state.selected_distributions[i] = sel_dist

    # Prevent choosing same distribution on both sides
    if (selected_dist_objs[0] and selected_dist_objs[1] and 
        getattr(selected_dist_objs[0], 'id', None) == getattr(selected_dist_objs[1], 'id', None)):
        st_app.warning("Seleziona due distribution diverse per il confronto (non puoi scegliere la stessa distribution su entrambi i lati).")

    # Confirm button - removed automatic rerun on selection change
    if st_app.button("✅ Conferma selezioni", key="confirm_comp_selections"):
        left = selected_dist_objs[0]
        right = selected_dist_objs[1]
        left_ds = selected_ds_objs[0]
        right_ds = selected_ds_objs[1]

        if not left or not right:
            st_app.error("Devi selezionare entrambe le distribution.")
        elif getattr(left, 'id', None) == getattr(right, 'id', None):
            st_app.error("Seleziona due distribution diverse (hai scelto la stessa su entrambi i lati).")
        else:
            # Update session state with confirmed selections
            st_app.session_state.selected_distributions = [left, right]
            st_app.session_state.selected_dataset_ids = [
                getattr(left_ds, 'id', None) if left_ds else None,
                getattr(right_ds, 'id', None) if right_ds else None
            ]
            st_app.success("Selezioni salvate.")
            st_app.rerun()

    # show current bag / selection with previews
    st_app.divider()
    st_app.subheader("Selezioni correnti")

    cur_left = st_app.session_state.selected_distributions[0]
    cur_right = st_app.session_state.selected_distributions[1]

    # attempt to get dataset objects for preview
    ds_left = None
    ds_right = None
    if st_app.session_state.selected_dataset_ids[0]:
        try:
            ds_left = repos['dataset'].get_by_id(st_app.session_state.selected_dataset_ids[0])
        except Exception:
            ds_left = None
    if st_app.session_state.selected_dataset_ids[1]:
        try:
            ds_right = repos['dataset'].get_by_id(st_app.session_state.selected_dataset_ids[1])
        except Exception:
            ds_right = None

    col_preview_l, col_preview_r = st_app.columns(2)
    with col_preview_l:
        st_app.subheader("A")
        if cur_left and ds_left:
            _show_preview(st_app, ds_left, cur_left, repos)
        else:
            st_app.info("A: non selezionata")

    with col_preview_r:
        st_app.subheader("B")
        if cur_right and ds_right:
            _show_preview(st_app, ds_right, cur_right, repos)
        else:
            st_app.info("B: non selezionata")

    # Compare button: read stats via duckdb, run comparator, clear selections and store results
    st_app.divider()
    
    # Check if both distributions are selected and confirmed
    if cur_left and cur_right:
        st_app.subheader("Confronto")
        
        # Show distribution info
        st_app.write(f"**Distribution A:** {cur_left.name} (v{cur_left.version})")
        st_app.write(f"**Distribution B:** {cur_right.name} (v{cur_right.version})")
        
        # Compare button
        if st_app.button("🔎 Compare", key="run_compare", type="primary"):
            try:
                stats_a_path = _get_stats_path_for_distribution(cur_left)
                stats_b_path = _get_stats_path_for_distribution(cur_right)

                st_app.write(f"🔍 Percorsi statistiche:")
                st_app.write(f"- **A:** `{stats_a_path}`")
                st_app.write(f"- **B:** `{stats_b_path}`")

                if not stats_a_path or not stats_b_path:
                    st_app.error("Path statistiche non valido per una o entrambe le distribution.")
                elif not os.path.exists(stats_a_path) or not os.path.exists(stats_b_path):
                    st_app.error(f"Path statistiche non trovato. Verifica che i file esistano.")
                    st_app.write(f"Esistenza path A ({stats_a_path}): {os.path.exists(stats_a_path)}")
                    st_app.write(f"Esistenza path B ({stats_b_path}): {os.path.exists(stats_b_path)}")
                else:
                    # Check for parquet files
                    parquet_files_a = [f for f in os.listdir(stats_a_path) if f.endswith('.parquet')]
                    parquet_files_b = [f for f in os.listdir(stats_b_path) if f.endswith('.parquet')]
                    
                    if not parquet_files_a or not parquet_files_b:
                        st_app.error(f"Nessun file parquet trovato nei path delle statistiche.")
                    else:
                        st_app.write(f"📊 File parquet trovati: A={len(parquet_files_a)}, B={len(parquet_files_b)}")
                        
                        with st_app.spinner("Caricamento statistiche e confronto in corso..."):
                            try:
                                conn = duckdb.connect(':memory:')
                                
                                # Load all parquet files from directories
                                df_a = conn.execute(f"""
                                    SELECT * FROM read_parquet('{stats_a_path}/*.parquet')
                                """).fetchdf()
                                
                                df_b = conn.execute(f"""
                                    SELECT * FROM read_parquet('{stats_b_path}/*.parquet')
                                """).fetchdf()
                                
                                conn.close()
                                
                                st_app.write(f"📈 Statistiche caricate: A={len(df_a)} righe, B={len(df_b)} righe")
                                
                                # Pass the stats DataFrames to the comparator
                                comparator = DistributionComparator(df_a, df_b)
                                results = comparator.compare_all()
                                report = comparator.generate_summary_report()

                                # Store results to session so they persist after rerun
                                st_app.session_state.last_comparison_report = report
                                st_app.session_state.last_comparison_results = results
                                
                                st_app.success("✅ Confronto completato")
                                st_app.rerun()
                                
                            except Exception as db_error:
                                logger.exception(db_error)
                                st_app.error(f"Errore durante il caricamento dei dati: {db_error}")
                                
            except Exception as e:
                logger.exception(e)
                st_app.error(f"Errore durante il confronto: {e}")

    # If a previous comparison exists, show it
    if st_app.session_state.get('last_comparison_report'):
        st_app.markdown("---")
        st_app.subheader("📋 Ultimo Confronto")
        
        # Display report in a more readable format
        st_app.text_area(
            "Report del Confronto",
            value=st_app.session_state.get('last_comparison_report', ''),
            height=300,
            disabled=True
        )
        
        # Option to show detailed results
        if st_app.button("📊 Mostra risultati dettagliati"):
            results = st_app.session_state.get('last_comparison_results')
            if results:
                st_app.json(results)
        
        if st_app.button("🗑️ Cancella confronto"):
            st_app.session_state.pop('last_comparison_report', None)
            st_app.session_state.pop('last_comparison_results', None)
            st_app.rerun()

    # back home
    st_app.divider()
    if st_app.button("🏠 Torna alla Home", key="comp_home"):
        # clear selection state
        for k in ["selected_distributions", "selected_dataset_ids", "selected_dataset_paths", 
                  "comp_search_terms", "last_comparison_report", "last_comparison_results"]:
            if k in st_app.session_state: 
                del st_app.session_state[k]
        reset_dashboard_session_state(st_app, home_vars)
        st_app.session_state.current_stage = "home"
        st_app.rerun()