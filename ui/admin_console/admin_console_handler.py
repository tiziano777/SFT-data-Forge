import os
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from db.impl.postgres.loader.postgres_db_loader import get_db_manager
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.dataset_card_repository import DatasetCardRepository
from data_class.repository.table.strategy_repository import StrategyRepository
from data_class.entity.table.strategy import Strategy
from utils.path_utils import to_binded_path, to_internal_path

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")
BASE_PREFIX = os.getenv("BASE_PREFIX", "")
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


def _entity_to_dict(entity):
    """Converte qualsiasi entità in dict completo. Usa to_dict() se disponibile, altrimenti asdict."""
    if hasattr(entity, 'to_dict'):
        return entity.to_dict()
    return asdict(entity)


def _show_entity_table(entity_dict):
    """Mostra un dict come tabella chiave-valore leggibile."""
    rows = []
    for k, v in entity_dict.items():
        display_val = str(v) if v is not None else "—"
        rows.append({"Campo": k, "Valore": display_val})
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _get_repos():
    db = get_db_manager()
    return (
        DatasetRepository(db),
        DistributionRepository(db),
        DatasetCardRepository(db),
        StrategyRepository(db),
    )


def _log_deletion(entity_type: str, deleted_entities: list[dict]):
    """Scrive un file di log con le entità eliminate."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"{entity_type}_{ts}.log"
    lines = [f"Deletion log - {entity_type} - {ts}\n{'='*60}\n"]
    for ent in deleted_entities:
        lines.append(f"  [{ent['type']}] id={ent['id']} name={ent.get('name', 'N/A')}\n")
    log_file.write_text("".join(lines))
    return log_file


def show_admin_console(st):
    st.title("Admin Console - Dependency Explorer")

    dataset_repo, dist_repo, card_repo, strategy_repo = _get_repos()

    # === SEZIONE 0: ALIGNMENT ===
    with st.expander("🔄 Distribution Alignment Check", expanded=False):
        _show_distribution_alignment(st, dist_repo)

    # === SEZIONE 1: RICERCA ===
    st.header("1. Ricerca Entità")

    col1, col2, col3 = st.columns(3)
    with col1:
        entity_type = st.selectbox("Tipo entità", ["dataset", "distribution", "dataset_card"])
    with col2:
        step_filter = None
        if entity_type in ("dataset", "distribution"):
            step_filter = st.selectbox("Filtro step", [None, 1, 2, 3], format_func=lambda x: "Tutti" if x is None else str(x))
    with col3:
        lang_filter = st.text_input("Filtro lingua (es. 'it', 'en')", value="").strip() or None

    search_text = st.text_input("Cerca per nome/descrizione", value="").strip()

    # Esegui ricerca
    results = _search_entities(entity_type, step_filter, lang_filter, search_text,
                               dataset_repo, dist_repo, card_repo)

    if not results:
        st.info("Nessun risultato trovato.")
        return

    st.write(f"**{len(results)} risultati trovati**")

    # Selezione entità
    options = {f"{r.id} — {getattr(r, 'name', None) or getattr(r, 'dataset_name', '')}": r for r in results}
    selected_key = st.selectbox("Seleziona entità", list(options.keys()))
    selected = options[selected_key]

    st.divider()

    # === SEZIONE 2: VISUALIZZAZIONE DIPENDENZE ===
    st.header("2. Dipendenze e Impatto Eliminazione")

    if entity_type == "dataset_card":
        _show_card_details(selected, dataset_repo)
    elif entity_type == "dataset":
        _show_dataset_details(selected, dataset_repo, dist_repo, strategy_repo, card_repo)
    elif entity_type == "distribution":
        _show_distribution_details(selected, dataset_repo, dist_repo, strategy_repo)


def _search_entities(entity_type, step_filter, lang_filter, search_text,
                     dataset_repo, dist_repo, card_repo):
    if entity_type == "dataset":
        items = dataset_repo.get_all()
        if step_filter is not None:
            items = [i for i in items if i.step == step_filter]
        if lang_filter:
            items = [i for i in items if lang_filter in (i.languages or [])]
        if search_text:
            s = search_text.lower()
            items = [i for i in items if s in (i.name or "").lower() or s in (i.description or "").lower()]
    elif entity_type == "distribution":
        items = dist_repo.get_all()
        if step_filter is not None:
            items = [i for i in items if i.step == step_filter]
        if lang_filter:
            items = [i for i in items if i.lang == lang_filter]
        if search_text:
            s = search_text.lower()
            items = [i for i in items if s in (i.name or "").lower() or s in (i.description or "").lower()]
    else:  # dataset_card
        items = card_repo.get_all()
        if lang_filter:
            items = [i for i in items if lang_filter in (i.languages or [])]
        if search_text:
            s = search_text.lower()
            items = [i for i in items if s in (i.dataset_name or "").lower() or s in (i.dataset_description or "").lower()]
    return items


# === DATASET CARD ===

def _show_card_details(card, dataset_repo):
    st.subheader(f"Dataset Card: {card.dataset_name}")

    # Mostra tutti gli attributi
    with st.expander("📋 Attributi entità", expanded=True):
        _show_entity_table(_entity_to_dict(card))

    # Dipendenze verso il basso: dataset che hanno derived_card = card.id
    dependent_datasets = _get_datasets_depending_on_card(card.id, dataset_repo)

    st.subheader("Dipendenze (dataset con derived_card = questa card)")
    if dependent_datasets:
        for d in dependent_datasets:
            with st.expander(f"📦 Dataset: {d.name} (`{d.id}`)"):
                _show_entity_table(_entity_to_dict(d))
        st.error(f"❌ NON eliminabile: {len(dependent_datasets)} dataset dipendono da questa card.")
    else:
        st.success("✅ Eliminabile: nessun dataset dipende da questa card.")
        if st.button("🗑️ Elimina Dataset Card", key="del_card"):
            _execute_card_deletion(card, dataset_repo)


def _get_datasets_depending_on_card(card_id, dataset_repo):
    all_ds = dataset_repo.get_all()
    return [d for d in all_ds if d.derived_card == card_id]


def _execute_card_deletion(card, dataset_repo):
    db = get_db_manager()
    card_repo = DatasetCardRepository(db)
    deleted = []
    try:
        card_repo.delete(card.id)
        deleted.append({"type": "dataset_card", "id": card.id, "name": card.dataset_name})
        log_file = _log_deletion("dataset_card", deleted)
        st.success(f"Card eliminata. Log: `{log_file}`")
        for d in deleted:
            st.write(f"  ✓ [{d['type']}] {d['id']} - {d['name']}")
    except Exception as e:
        st.error(f"Errore durante eliminazione: {e}")


# === DATASET ===

def _show_dataset_details(dataset, dataset_repo, dist_repo, strategy_repo, card_repo):
    st.subheader(f"Dataset: {dataset.name}")

    # Mostra tutti gli attributi
    with st.expander("📋 Attributi entità", expanded=True):
        _show_entity_table(_entity_to_dict(dataset))

    # Relazioni verso l'alto
    st.subheader("⬆️ Relazioni verso l'alto")

    if dataset.derived_card:
        card = card_repo.get_by_id(dataset.derived_card)
        label = f"🃏 Derived Card: {card.dataset_name if card else 'N/A'} (`{dataset.derived_card}`)"
        with st.expander(label):
            if card:
                _show_entity_table(_entity_to_dict(card))
            else:
                st.warning("Card non trovata nel DB")
    else:
        st.info("Nessuna Derived Card")

    if dataset.derived_dataset:
        parent = dataset_repo.get_by_id(dataset.derived_dataset)
        label = f"📦 Dataset padre (derived_from): {parent.name if parent else 'N/A'} (`{dataset.derived_dataset}`)"
        with st.expander(label):
            if parent:
                _show_entity_table(_entity_to_dict(parent))
            else:
                st.warning("Dataset padre non trovato nel DB")
    else:
        st.info("Nessun Dataset padre (è un dataset radice)")

    # Relazioni verso il basso
    st.subheader("⬇️ Relazioni verso il basso")

    child_datasets = _get_child_datasets(dataset.id, dataset_repo)
    child_distributions = dist_repo.get_by_dataset_id(dataset.id)

    st.write(f"**Dataset figli (derived_dataset = questo): {len(child_datasets)}**")
    for cd in child_datasets:
        with st.expander(f"📦 {cd.name} (`{cd.id}`)"):
            _show_entity_table(_entity_to_dict(cd))

    st.write(f"**Distribution figlie (dataset_id = questo): {len(child_distributions)}**")
    for dd in child_distributions:
        with st.expander(f"📄 {dd.name} (`{dd.id}`)"):
            _show_entity_table(_entity_to_dict(dd))

    # Analisi eliminabilità
    st.divider()
    st.subheader("Analisi impatto eliminazione")

    # Verifica se strategy RESTRICT bloccherebbe
    blocking_strategies = []
    for dist in child_distributions:
        strategies = _get_strategies_for_distribution(dist.id, strategy_repo)
        if strategies:
            blocking_strategies.extend([(dist, s) for s in strategies])

    if blocking_strategies:
        st.error(f"❌ BLOCCATO da {len(blocking_strategies)} strategy con RESTRICT:")
        for dist, strat in blocking_strategies:
            st.write(f"  - Strategy `{strat.id}` → Distribution `{dist.name}` (`{dist.id}`)")
        st.warning("Impossibile eliminare: prima rimuovere le strategy bloccanti.")
    else:
        st.warning("⚠️ Effetti della cancellazione (CASCADE/SET NULL):")
        st.write(f"  - **{len(child_distributions)} distribution** verranno eliminate (CASCADE)")
        st.write(f"  - **{len(child_datasets)} dataset figli** avranno `derived_dataset` impostato a NULL (SET NULL)")
        st.success("✅ Eliminazione possibile (nessun RESTRICT bloccante)")

        if st.button("🗑️ Elimina Dataset (con cascade)", key="del_dataset"):
            _execute_dataset_deletion(dataset, child_distributions, child_datasets)


def _get_child_datasets(dataset_id, dataset_repo):
    all_ds = dataset_repo.get_all()
    return [d for d in all_ds if d.derived_dataset == dataset_id]


def _get_strategies_for_distribution(dist_id, strategy_repo):
    """Cerca strategy che referenziano questa distribution (RESTRICT)."""
    db = get_db_manager()
    table = f"{POSTGRES_DB_SCHEMA}.strategy"
    query = f"SELECT * FROM {table} WHERE distribution_id = %s"
    with db as conn:
        rows = conn.query(query, (dist_id,))
        return [Strategy(**row) for row in rows] if rows else []


def _execute_dataset_deletion(dataset, child_distributions, child_datasets):
    db = get_db_manager()
    dataset_repo = DatasetRepository(db)
    deleted = []
    try:
        # Il DB fa CASCADE sulle distribution e SET NULL sui derived_dataset
        dataset_repo.delete(dataset.id)
        deleted.append({"type": "dataset", "id": dataset.id, "name": dataset.name})
        for dist in child_distributions:
            deleted.append({"type": "distribution (CASCADE)", "id": dist.id, "name": dist.name})
        for cd in child_datasets:
            deleted.append({"type": "dataset (derived_dataset → NULL)", "id": cd.id, "name": cd.name})

        log_file = _log_deletion("dataset", deleted)
        st.success(f"Dataset eliminato. Log: `{log_file}`")
        for d in deleted:
            st.write(f"  ✓ [{d['type']}] {d['id']} - {d['name']}")
    except Exception as e:
        st.error(f"Errore durante eliminazione: {e}")


# === DISTRIBUTION ===

def _show_distribution_details(distribution, dataset_repo, dist_repo, strategy_repo):
    st.subheader(f"Distribution: {distribution.name}")

    # Mostra tutti gli attributi
    with st.expander("📋 Attributi entità", expanded=True):
        _show_entity_table(_entity_to_dict(distribution))

    # Relazioni verso l'alto
    st.subheader("⬆️ Relazioni verso l'alto")

    parent_dataset = dataset_repo.get_by_id(distribution.dataset_id)
    label = f"📦 Dataset padre: {parent_dataset.name if parent_dataset else 'N/A'} (`{distribution.dataset_id}`)"
    with st.expander(label):
        if parent_dataset:
            _show_entity_table(_entity_to_dict(parent_dataset))
        else:
            st.warning("Dataset padre non trovato nel DB")

    if distribution.derived_from:
        parent_dist = dist_repo.get_by_id(distribution.derived_from)
        label = f"📄 Distribution padre (derived_from): {parent_dist.name if parent_dist else 'N/A'} (`{distribution.derived_from}`)"
        with st.expander(label):
            if parent_dist:
                _show_entity_table(_entity_to_dict(parent_dist))
            else:
                st.warning("Distribution padre non trovata nel DB")
    else:
        st.info("Nessuna Distribution padre (è una distribution radice)")

    # Relazioni verso il basso
    st.subheader("⬇️ Relazioni verso il basso")

    child_distributions = _get_child_distributions(distribution.id, dist_repo)
    st.write(f"**Distribution figlie (derived_from = questa): {len(child_distributions)}**")
    for cd in child_distributions:
        with st.expander(f"📄 {cd.name} (`{cd.id}`)"):
            _show_entity_table(_entity_to_dict(cd))

    # Analisi eliminabilità
    st.divider()
    st.subheader("Analisi impatto eliminazione")

    blocking_strategies = _get_strategies_for_distribution(distribution.id, strategy_repo)

    if blocking_strategies:
        st.error(f"❌ BLOCCATO da {len(blocking_strategies)} strategy con RESTRICT:")
        for strat in blocking_strategies:
            st.write(f"  - Strategy `{strat.id}` (recipe: `{strat.recipe_id}`)")
        st.warning("Impossibile eliminare: prima rimuovere le strategy bloccanti.")
    else:
        st.warning("⚠️ Effetti della cancellazione:")
        st.write(f"  - **{len(child_distributions)} distribution figlie** avranno `derived_from` impostato a NULL (SET NULL)")
        st.write("  - Mapping collegati: eliminati (CASCADE)")
        st.success("✅ Eliminazione possibile (nessun RESTRICT bloccante)")

        if st.button("🗑️ Elimina Distribution", key="del_dist"):
            _execute_distribution_deletion(distribution, child_distributions)


def _get_child_distributions(dist_id, dist_repo):
    all_dists = dist_repo.get_all()
    return [d for d in all_dists if d.derived_from == dist_id]


def _execute_distribution_deletion(distribution, child_distributions):
    db = get_db_manager()
    dist_repo = DistributionRepository(db)
    deleted = []
    try:
        dist_repo.delete(distribution.id)
        deleted.append({"type": "distribution", "id": distribution.id, "name": distribution.name})
        for cd in child_distributions:
            deleted.append({"type": "distribution (derived_from → NULL)", "id": cd.id, "name": cd.name})

        log_file = _log_deletion("distribution", deleted)
        st.success(f"Distribution eliminata. Log: `{log_file}`")
        for d in deleted:
            st.write(f"  ✓ [{d['type']}] {d['id']} - {d['name']}")
    except Exception as e:
        st.error(f"Errore durante eliminazione: {e}")


# === DISTRIBUTION ALIGNMENT ===


def _get_disk_path(distribution) -> str:
    """Convert distribution URI (DB) to a path the app can verify on disk.
    DB stores BASE_PREFIX + disk_path. Strip prefix, then convert to internal/container path.
    """
    uri = to_internal_path(distribution.uri.replace(BASE_PREFIX, ""))
    logger.info(f"Mapping distribution URI to disk path: DB URI='{distribution.uri}' → stripped='{uri}'")
    return uri


def _log_alignment_action(action: str, distributions: list):
    """Log alignment actions to admin_console/logs."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"alignment_{action}_{ts}.log"
    lines = [f"Alignment log - action: {action} - {ts}\n{'='*60}\n"]
    for d in distributions:
        disk_path = _get_disk_path(d)
        lines.append(f"  id={d.id} | name={d.name} | uri={to_binded_path(d.uri)} | disk_path={disk_path}\n")
    lines.append(f"\nTotal: {len(distributions)} distribution(s)\n")
    log_file.write_text("".join(lines))
    return log_file


def _show_distribution_alignment(st, dist_repo):
    st.caption(
        "Find distributions marked as `materialized=True` in DB "
        "but with no corresponding file on disk."
    )

    if st.button("🔍 Run alignment check"):
        st.session_state["alignment_ran"] = True

    if not st.session_state.get("alignment_ran"):
        return

    all_materialized = [d for d in dist_repo.get_all() if d.materialized]

    orphans = []
    for d in all_materialized:
        disk_path = _get_disk_path(d)
        logger.info(f"Mapped distribution id={d.id} to disk path: {disk_path}")
        if not Path(disk_path).exists():
            orphans.append(d)

    if not orphans:
        st.success("All materialized distributions have a matching file on disk.")
        return

    st.warning(f"**{len(orphans)}** distribution(s) marked materialized but NOT found on disk.")

    # Select all / Deselect all
    col_all, col_none = st.columns(2)
    with col_all:
        if st.button("✅ Select all"):
            st.session_state["alignment_selected"] = [d.id for d in orphans]
            st.rerun()
    with col_none:
        if st.button("⬜ Deselect all"):
            st.session_state["alignment_selected"] = []
            st.rerun()

    # Initialize selection state
    if "alignment_selected" not in st.session_state:
        st.session_state["alignment_selected"] = []

    # Checkboxes for each orphan
    selected_ids = []
    for d in orphans:
        disk_path = _get_disk_path(d)
        checked = st.checkbox(
            f"{d.name} — `{disk_path}`",
            value=d.id in st.session_state["alignment_selected"],
            key=f"align_{d.id}",
        )
        if checked:
            selected_ids.append(d.id)

    st.session_state["alignment_selected"] = selected_ids

    if not selected_ids:
        st.info("Select at least one distribution to proceed.")
        return

    st.divider()
    st.write(f"**{len(selected_ids)}** selected. Choose action:")

    col_del, col_unmat = st.columns(2)

    with col_del:
        if st.button("🗑️ Delete from DB", type="primary"):
            to_act = [d for d in orphans if d.id in selected_ids]
            try:
                for d in to_act:
                    dist_repo.delete(d.id)
                log_file = _log_alignment_action("delete", to_act)
                st.success(f"Deleted {len(to_act)} distribution(s). Log: `{log_file.name}`")
                st.session_state.pop("alignment_ran", None)
                st.session_state.pop("alignment_selected", None)
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    with col_unmat:
        if st.button("📝 Set materialized=False"):
            to_act = [d for d in orphans if d.id in selected_ids]
            try:
                for d in to_act:
                    d.materialized = False
                    dist_repo.update(d)
                log_file = _log_alignment_action("set_materialized_false", to_act)
                st.success(f"Updated {len(to_act)} distribution(s). Log: `{log_file.name}`")
                st.session_state.pop("alignment_ran", None)
                st.session_state.pop("alignment_selected", None)
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
