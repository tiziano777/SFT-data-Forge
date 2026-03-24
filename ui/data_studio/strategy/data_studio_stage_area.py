import logging
import plotly.graph_objects as go
from .stats_retriver.retriver import DistributionStatsRetriever

logger = logging.getLogger(__name__)

def show_data_studio_stage_area(st):
    st.write("## 🧾 Receipt Builder")
    st.write("---")

    # Inizializzazione flag di conferma se non esiste
    if "is_confirmed" not in st.session_state:
        st.session_state.is_confirmed = False

    if "dist_bag" not in st.session_state or not st.session_state.dist_bag:
        st.warning("Il bag è vuoto. Torna al Data Studio.")
        if st.button("⬅️ Torna al Data Studio"):
            st.session_state.current_stage = "data_studio"
            st.rerun()
        return

    # --- 1. RECUPERO STATISTICHE (CACHE) ---
    if "dist_stats_cache" not in st.session_state:
        with st.spinner("📊 Recupero statistiche in corso..."):
            try:
                retriever = DistributionStatsRetriever()
                st.session_state.dist_stats_cache = retriever.fetch_all_stats(st.session_state.dist_bag)
            except Exception as e:
                logger.error(f"Errore durante fetch_all_stats: {str(e)}", exc_info=True)
                st.error(f"Errore nel recupero statistiche: {str(e)}")
                st.session_state.dist_stats_cache = {}

    if "recipe_replicas" not in st.session_state:
        old_recipe = st.session_state.get("old_recipe", {})
        st.session_state.recipe_replicas = {
            str(dist.id): float(old_recipe.get(dist.uri, {}).get("replication_factor", 1))
            for ds_id in st.session_state.dist_bag
            for dist in st.session_state.dist_bag[ds_id]['dist']
        }

    # --- PREPARAZIONE DATI ---
    all_langs = set()
    ds_names = {ds_id: group['ds'].name for ds_id, group in st.session_state.dist_bag.items()}
    for ds_id, group in st.session_state.dist_bag.items():
        for d in group['dist']:
            lang = getattr(d, 'lang', 'N/D')
            if lang: all_langs.add(lang)

    sorted_langs = sorted(list(all_langs))
    sorted_ds_ids = sorted(ds_names.keys(), key=lambda x: ds_names[x])

    # --- TOTALI IN ALTO ---
    recipe_totals = aggregate_recipe_stats(
        st.session_state.dist_bag,
        st.session_state.recipe_replicas,
        st.session_state.dist_stats_cache
    )

    # Sezione Totali (Sempre visibile)
    with st.container():
        st.subheader("📊 Recipe Totals")
        tc1, tc2, tc3, tc4 = st.columns([1, 1, 1, 3])
        tc1.metric("Samples", f"{recipe_totals['samples']:,}")
        tc2.metric("Tokens", f"{recipe_totals['tokens']:,}")
        tc3.metric("Words", f"{recipe_totals['words']:,}")
        with tc4:
            lang_data = {lang: count for lang, count in recipe_totals['langs'].items() if count > 0}
            if lang_data:
                fig = go.Figure(data=[go.Pie(
                    labels=list(lang_data.keys()),
                    values=list(lang_data.values()),
                    hole=0.45,
                    textinfo='label+percent',
                    textfont=dict(size=11),
                    marker=dict(line=dict(color='#1a1a2e', width=1.5)),
                )])
                fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=160, showlegend=False, paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        st.write("---")

    # --- LOGICA DI VISUALIZZAZIONE CONDIZIONALE ---
    if not st.session_state.is_confirmed:
        # --- SEZIONE EDITING (FILTRI + TABELLA) ---
        col_filters, col_main = st.columns([1.5, 6])
        
        with col_filters:
            st.subheader("🎯 Filtri")
            selected_langs = [l for l in sorted_langs if st.checkbox(l, value=True, key=f"f_lang_{l}")]
            st.write("---")
            selected_ds_ids = [d_id for d_id in sorted_ds_ids if st.checkbox(ds_names[d_id], value=True, key=f"f_ds_{d_id}")]

        with col_main:
            st.subheader("📦 Dataset Configuration")
            col_weights = [3, 1.2, 1.2, 1.2, 1.4]
            for ds_id in selected_ds_ids:
                group = st.session_state.dist_bag[ds_id]
                visible_dists = [d for d in group['dist'] if getattr(d, 'lang', 'N/D') in selected_langs]
                if not visible_dists: continue

                # --- Calcolo totali aggregati dell'expander (aggiustati per replica) ---
                exp_samples = sum(
                    int(st.session_state.dist_stats_cache.get(str(d.id), {'samples': 0})['samples'] *
                        st.session_state.recipe_replicas.get(str(d.id), 1.0))
                    for d in visible_dists
                )
                exp_tokens = sum(
                    int(st.session_state.dist_stats_cache.get(str(d.id), {'tokens': 0})['tokens'] *
                        st.session_state.recipe_replicas.get(str(d.id), 1.0))
                    for d in visible_dists
                )
                exp_words = sum(
                    int(st.session_state.dist_stats_cache.get(str(d.id), {'words': 0})['words'] *
                        st.session_state.recipe_replicas.get(str(d.id), 1.0))
                    for d in visible_dists
                )

                expander_label = (
                    f"**{ds_names[ds_id].upper()}** &nbsp;|&nbsp; "
                    f"🔢 {exp_samples:,} samples &nbsp; "
                    f"🔤 {exp_tokens:,} tokens &nbsp; "
                    f"📝 {exp_words:,} words"
                )

                with st.expander(expander_label, expanded=True):
                    # --- Header colonne ---
                    header = st.columns(col_weights)
                    header[0].markdown("**Distribution**")
                    header[1].markdown("**Samples**")
                    header[2].markdown("**Tokens**")
                    header[3].markdown("**Words**")
                    header[4].markdown("**Replica**")
                    st.markdown("---")

                    for d in visible_dists:
                        d_id_str = str(d.id)
                        d_stats = st.session_state.dist_stats_cache.get(d_id_str, {'samples': 0, 'tokens': 0, 'words': 0})
                        current_replica = st.session_state.recipe_replicas.get(d_id_str, 1.0)

                        row = st.columns(col_weights)
                        row[0].markdown(f"**{d.name}** `{getattr(d, 'lang', 'N/D')}`")
                        row[1].markdown(f"`{int(d_stats['samples'] * current_replica):,}`")
                        row[2].markdown(f"`{int(d_stats['tokens'] * current_replica):,}`")
                        row[3].markdown(f"`{int(d_stats['words'] * current_replica):,}`")

                        new_val = row[4].number_input(
                            "Replica", min_value=0.0, step=0.01,
                            value=float(current_replica),
                            key=f"in_{d_id_str}",
                            label_visibility="collapsed"
                        )
                        if new_val != current_replica:
                            st.session_state.recipe_replicas[d_id_str] = new_val
                            st.rerun()

                    # --- Tag colorati ---
                    card = getattr(group.get('ds'), 'card', None) or group.get('card', None)
                    tasks = []
                    core_skills = []
                    if card is not None:
                        core_skills = getattr(card, 'core_skills', []) or []
                        tasks = getattr(card, 'tasks', []) or []

                    if core_skills or tasks:
                        st.markdown("---")
                        if core_skills:
                            skill_tags_html = " ".join([
                                f'<span style="background-color:#1e3a5f;color:#7ec8e3;padding:3px 10px;'
                                f'border-radius:12px;font-size:0.75rem;font-weight:600;margin:2px;display:inline-block;">'
                                f'⚙ {skill}</span>'
                                for skill in core_skills
                            ])
                            st.markdown(
                                f'<div style="margin-bottom:6px;"><span style="font-size:0.78rem;color:#aaa;margin-right:6px;">Core Skills</span>'
                                f'{skill_tags_html}</div>',
                                unsafe_allow_html=True
                            )
                        if tasks:
                            task_tags_html = " ".join([
                                f'<span style="background-color:#2d1f4e;color:#c4a7e7;padding:3px 10px;'
                                f'border-radius:12px;font-size:0.75rem;font-weight:600;margin:2px;display:inline-block;">'
                                f'📌 {task}</span>'
                                for task in tasks
                            ])
                            st.markdown(
                                f'<div><span style="font-size:0.78rem;color:#aaa;margin-right:6px;">Tasks</span>'
                                f'{task_tags_html}</div>',
                                unsafe_allow_html=True
                            )
            st.write("---")
            btn_col1, btn_col2 = st.columns([1, 1])
            with btn_col1:
                if st.button("⬅️ Back to Studio"):
                    st.session_state.current_stage = "data_studio"
                    st.rerun()
            with btn_col2:
                if st.button("🚀 Proceed", type="primary", use_container_width=True):
                    st.session_state.is_confirmed = True
                    st.rerun()
    else:
        # --- SEZIONE CONFERMA FINALE (STATISTICHE AGGREGATE) ---
        st.success("✅ Configurazione Pronta per la Submission")
        
        # Preparazione dati per i grafici
        ds_labels, ds_samples, ds_tokens, ds_words = [], [], [], []
        for ds_id, group in st.session_state.dist_bag.items():
            ds_labels.append(ds_names[ds_id])
            s_sum, t_sum, w_sum = 0, 0, 0
            for d in group['dist']:
                rep = st.session_state.recipe_replicas.get(str(d.id), 1.0)
                stats = st.session_state.dist_stats_cache.get(str(d.id), {'samples':0, 'tokens':0, 'words':0})
                s_sum += stats['samples'] * rep
                t_sum += stats['tokens'] * rep
                w_sum += stats['words'] * rep
            ds_samples.append(s_sum); ds_tokens.append(t_sum); ds_words.append(w_sum)

        # 1) Row: Pie Charts
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### 🌍 Global Tasks Distribution")

            # Extract core skill distribution
            skill_distribution = {}
            for ds_id, group in st.session_state.dist_bag.items():
                card = getattr(group.get('ds'), 'card', None) or group.get('card', None)
                if card and hasattr(card, 'tasks') and card.tasks:
                    core_skill = card.tasks[0]  # Take the first skill as the core skill
                    total_samples = sum(
                        st.session_state.dist_stats_cache.get(str(d.id), {'samples': 0})['samples'] * 
                        st.session_state.recipe_replicas.get(str(d.id), 1.0)
                        for d in group['dist']
                    )
                    skill_distribution[core_skill] = skill_distribution.get(core_skill, 0) + total_samples

            # Create pie chart for skill distribution
            if skill_distribution:
                fig_skills = go.Figure(data=[go.Pie(
                    labels=list(skill_distribution.keys()),
                    values=list(skill_distribution.values()),
                    hole=0.4
                )])
                fig_skills.update_layout(height=300, margin=dict(t=20, b=20, l=0, r=0))
                st.plotly_chart(fig_skills, use_container_width=True)
        with c2:
            st.markdown("##### 📦 Samples per Dataset")
            fig_ds_pie = go.Figure(data=[go.Pie(labels=ds_labels, values=ds_samples, hole=0.4)])
            fig_ds_pie.update_layout(height=300, margin=dict(t=20, b=20, l=0, r=0))
            st.plotly_chart(fig_ds_pie, use_container_width=True)

        # 2) Row: Histogram (Full Width)
        st.markdown("##### 📊 Aggregated Metrics per Dataset")
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Bar(x=ds_labels, y=ds_samples, name='Samples'))
        fig_hist.add_trace(go.Bar(x=ds_labels, y=ds_tokens, name='Tokens'))
        fig_hist.add_trace(go.Bar(x=ds_labels, y=ds_words, name='Words'))
        fig_hist.update_layout(barmode='group', yaxis_type="log", height=450)
        st.plotly_chart(fig_hist, use_container_width=True)

        # Pulsanti di navigazione finale
        st.write("---")
        fcol1, fcol2 = st.columns(2)
        with fcol1:
            if st.button("⬅️ Modifica Repliche", use_container_width=True):
                st.session_state.is_confirmed = False
                st.rerun()
        with fcol2:
            if st.button("➡️ Create Recipe Data Contract", type="primary", use_container_width=True):
                st.session_state.current_stage = "data_studio_recipe_contract_creation"
                st.rerun()

def aggregate_recipe_stats(dist_bag, replicas, stats_cache):
    totals = {'samples': 0, 'tokens': 0, 'words': 0, 'langs': {}}
    for ds_id, group in dist_bag.items():
        for d in group['dist']:
            d_id_str = str(d.id)
            rep = replicas.get(d_id_str, 1.0)
            d_stats = stats_cache.get(d_id_str, {'samples': 0, 'tokens': 0, 'words': 0})
            totals['samples'] += int(d_stats['samples'] * rep)
            totals['tokens'] += int(d_stats['tokens'] * rep)
            totals['words'] += int(d_stats['words'] * rep)
            lang = getattr(d, 'lang', 'N/D')
            totals['langs'][lang] = totals['langs'].get(lang, 0) + int(d_stats['samples'] * rep)
            
    return totals

