# /ui/prompt_management/prompt_action_selection_handler.py

from config.state_vars import home_vars
from utils.streamlit_func import reset_dashboard_session_state
from data_class.repository.table.system_prompt_repository import SystemPromptRepository
from data_class.entity.table.system_prompt import SystemPrompt
from datetime import datetime, timezone


def show_system_prompt_management(st):
    st.markdown("## Prompt Management")
    st.markdown("Qui puoi gestire i prompt di sistema.")

    # ---------------------------
    # Init stato base
    # ---------------------------
    st.session_state.setdefault("show_new_system_prompt_form", False)
    st.session_state.setdefault("show_system_prompts_list", False)
    st.session_state.setdefault("system_prompt_search", "")
    st.session_state.setdefault("show_language_filter", False)
    # toggle for the length range filter UI
    st.session_state.setdefault("show_length_filter", False)
    # stores a tuple (min, max) selected by the user
    st.session_state.setdefault("system_prompt_length_range", None)

    repo = SystemPromptRepository(st.session_state.db_manager)
    all_prompts = repo.get_all()

    # ---------------------------
    # Helper
    # ---------------------------
    def _any_edit_open():
        return any(
            k.startswith("show_edit_form_") and st.session_state.get(k)
            for k in st.session_state
        )

    def _clear_edit_and_confirm_flags():
        for k in list(st.session_state.keys()):
            if (
                k.startswith("show_edit_form_")
                or k.startswith("confirm_delete_")
                or k.startswith("force_expand_")
            ):
                del st.session_state[k]

    # ---------------------------
    # Pulsanti principali
    # ---------------------------
    show_buttons = (
        not st.session_state.show_new_system_prompt_form
        and not st.session_state.show_system_prompts_list
        and not _any_edit_open()
    )

    if show_buttons:
        col1, col2 = st.columns(2)

        if col1.button("➕ Create New System Prompt"):
            st.session_state.show_new_system_prompt_form = True
            st.session_state.show_system_prompts_list = False
            _clear_edit_and_confirm_flags()
            st.rerun()

        if col2.button("👁️ Show System Prompts"):
            st.session_state.show_system_prompts_list = True
            st.session_state.show_new_system_prompt_form = False
            _clear_edit_and_confirm_flags()
            st.rerun()

    # ---------------------------
    # Form creazione
    # ---------------------------
    if st.session_state.show_new_system_prompt_form:
        st.session_state.show_system_prompts_list = False

        with st.form("new_system_prompt_form"):
            name = st.text_input("Name*")
            description = st.text_area("Description*", height=100)
            prompt_text = st.text_area("Prompt*", height=200)
            lang = st.text_input("Language*", value="en")
            length_input = st.number_input("Length (0 = auto)", min_value=0, value=0)
            quality_score = st.number_input("Quality Score (0.0 - 1.0)", min_value=0.00, max_value=1.00, value=0.00, step=0.05)
            derived_from = st.selectbox("Derived From (optional)", options=[None] + [p.name for p in all_prompts])

            save_clicked = st.form_submit_button("💾 Save")
            cancel_clicked = st.form_submit_button("⬅️ Cancel")

            if save_clicked:
                if derived_from:
                    derived_from = repo.get_by_name(derived_from).id if derived_from else None

                try:
                    length_val = length_input if length_input > 0 else len(prompt_text or "")
                    entity = SystemPrompt(
                        id=None,
                        name=name,
                        description=description,
                        prompt=prompt_text,
                        _lang=lang,
                        length=length_val,
                        derived_from=derived_from,
                        quality_score=quality_score,
                        deleted=False,
                        version="1.0",
                    )

                    repo = SystemPromptRepository(st.session_state.db_manager)
                    inserted = repo.insert(entity)

                    if inserted:
                        st.success(f"System prompt salvato: {inserted.name}")
                    else:
                        st.error("Salvataggio fallito")

                    st.session_state.show_new_system_prompt_form = False
                    _clear_edit_and_confirm_flags()
                    st.rerun()

                except Exception as e:
                    st.error(f"Errore durante il salvataggio: {e}")

            if cancel_clicked:
                st.session_state.show_new_system_prompt_form = False
                _clear_edit_and_confirm_flags()
                st.rerun()

    # ---------------------------
    # Lista prompt
    # ---------------------------
    if st.session_state.show_system_prompts_list:
        try:
            langs = sorted({getattr(p, "_lang", "") for p in all_prompts if getattr(p, "_lang", None)})

            col_search, col_filter = st.columns([4, 1])
            # add an extra column for the length filter toggle
            col_search, col_filter, col_len = st.columns([4, 1, 1])
            query = col_search.text_input(
                "Search",
                value=st.session_state.system_prompt_search,
            )
            st.session_state.system_prompt_search = query

            if col_filter.button("🗂️ Lang Filter"):
                st.session_state.show_language_filter = not st.session_state.show_language_filter
                st.rerun()

            if col_len.button("📏 Length Filter"):
                st.session_state.show_length_filter = not st.session_state.show_length_filter
                st.rerun()

            selected_langs = []
            if st.session_state.show_language_filter:
                selected_langs = st.multiselect("Languages", options=langs)

            # Length range filter UI inside an expander
            if st.session_state.show_length_filter:
                # compute max length from the table; fallback to 20000
                lengths = []
                for _p in all_prompts:
                    try:
                        l = int(getattr(_p, "length", None) or len(getattr(_p, "prompt", "") or ""))
                    except Exception:
                        l = 0
                    lengths.append(l)
                max_length = max(lengths) if lengths else 20000
                if not max_length or max_length <= 0:
                    max_length = 20000

                current_range = st.session_state.get("system_prompt_length_range") or (0, int(max_length))
                with st.expander("Length Range Filter"):
                    selected_range = st.slider(
                        "Prompt length (chars)", 0, int(max_length), (int(current_range[0]), int(current_range[1]))
                    )
                    st.session_state.system_prompt_length_range = (int(selected_range[0]), int(selected_range[1]))

            def matches_query(p, q):
                if not q:
                    return False
                q = q.lower()
                return (
                    q in (p.name or "").lower()
                    or q in (p.description or "").lower()
                    or q in (p.prompt or "").lower()
                )

            if selected_langs and query:
                filtered = [
                    p for p in all_prompts
                    if getattr(p, "_lang", "") in selected_langs or matches_query(p, query)
                ]
            elif selected_langs:
                filtered = [p for p in all_prompts if getattr(p, "_lang", "") in selected_langs]
            elif query:
                filtered = [p for p in all_prompts if matches_query(p, query)]
            else:
                filtered = all_prompts

            # Apply length range filter to the already filtered list
            length_range = st.session_state.get("system_prompt_length_range")
            if length_range:
                min_l, max_l = length_range
                def _get_len(pp):
                    try:
                        return int(getattr(pp, "length", None) or len(getattr(pp, "prompt", "") or ""))
                    except Exception:
                        return 0

                filtered = [p for p in filtered if _get_len(p) >= int(min_l) and _get_len(p) <= int(max_l)]

            st.markdown(f"### Risultati: {len(filtered)}")

            for p in filtered:
                expanded = st.session_state.get(f"force_expand_{p.id}", False)

                with st.expander(f"{p.name} — {p.description[:50]}...", expanded=expanded):
                    b1, b2, content = st.columns([1, 1, 8])

                    derived_from = repo.get_by_id(p.derived_from).name if p.derived_from else ''

                    if b1.button("✏️", key=f"edit_{p.id}"):
                        st.session_state[f"show_edit_form_{p.id}"] = True
                        st.session_state[f"force_expand_{p.id}"] = True
                        st.session_state.pop(f"confirm_delete_{p.id}", None)
                        st.rerun()

                    if b2.button("🗑️", key=f"delete_{p.id}"):
                        st.session_state[f"confirm_delete_{p.id}"] = True
                        st.session_state[f"force_expand_{p.id}"] = True
                        st.session_state.pop(f"show_edit_form_{p.id}", None)
                        st.rerun()

                    content.write("**Description:**")
                    content.write(p.description)
                    content.write("**Prompt:**")
                    content.code(p.prompt[:1500] + ("..." if len(p.prompt) > 1500 else ""))
                    content.write(f"**Language:** {getattr(p, '_lang', '')}")
                    content.write(f"**Length:** {getattr(p, 'length', '')}")
                    content.write(f"**Quality Score:** {getattr(p, 'quality_score', '')}")
                    content.write(f"**Derived From:** {derived_from}")
                    content.write(f"**Version:** {getattr(p, 'version', '')}")
                    content.write(f"**Issued:** {getattr(p, 'issued', '')}")
                    content.write(f"**Modified:** {getattr(p, 'modified', '')}")

                    # -------- DELETE CONFIRM (FIX) --------
                    if st.session_state.get(f"confirm_delete_{p.id}"):
                        content.warning("Confermi eliminazione? Operazione irreversibile.")
                        c1, c2 = content.columns(2)

                        if c1.button("Conferma", key=f"confirm_{p.id}"):
                            deleted = repo.delete(p.id)
                            if deleted:
                                st.success("Eliminato")
                            else:
                                st.error("Eliminazione fallita")

                            _clear_edit_and_confirm_flags()
                            st.session_state.show_system_prompts_list = True
                            st.rerun()

                        if c2.button("Annulla", key=f"cancel_{p.id}"):
                            # user clicked back/cancel
                            if f"confirm_delete_{p.id}" in st.session_state:
                                del st.session_state[f"confirm_delete_{p.id}"]
                            del st.session_state[f"force_expand_{p.id}"]
                            st.rerun()

                    # -------- EDIT FORM --------
                    if st.session_state.get(f"show_edit_form_{p.id}"):
                        with content.form(f"edit_form_{p.id}"):
                            name_e = st.text_input("Name", p.name)
                            desc_e = st.text_area("Description", p.description, height=100)
                            prompt_e = st.text_area("Prompt", p.prompt, height=200)
                            lang_e = st.text_input("Language", getattr(p, "_lang", ""))
                            length_e = st.number_input("Length", min_value=0, value=int(getattr(p, "length", 0)))
                            quality_score_e = st.number_input("Quality Score (0.0 - 1.0)", min_value=0.0, max_value=1.0, value=float(getattr(p, "quality_score", 0.0)), step=0.01)
                            
                            # Nota: st.text non restituisce un valore. Usiamo il valore originale di p.version
                            st.write(f"Current Version: {getattr(p, 'version', '1.0')}")
                            
                            col_save, col_cancel = st.columns(2)
                            save_e = col_save.form_submit_button("💾 Save")
                            cancel_e = col_cancel.form_submit_button("❌ Cancel")

                            if save_e:
                                length_val = length_e if length_e > 0 else len(prompt_e or "")
                                updated_entity = SystemPrompt(
                                    id=p.id,
                                    name=name_e,
                                    description=desc_e,
                                    prompt=prompt_e,
                                    _lang=lang_e,
                                    length=length_val,
                                    quality_score=quality_score_e,
                                    deleted=getattr(p, "deleted", False),
                                    derived_from=getattr(p, "derived_from", None),
                                    version=getattr(p, "version", "1.0"),
                                    issued=p.issued,
                                    modified=datetime.now(timezone.utc),
                                )
                                
                                updated = repo.update(updated_entity)
                                if updated:
                                    st.success("Aggiornato con successo!")
                                    _clear_edit_and_confirm_flags()
                                    st.rerun()
                                else:
                                    st.error("Update fallito.")

                            if cancel_e:
                                _clear_edit_and_confirm_flags()
                                st.rerun()
                                
            if st.button("⬅️ Close prompts list"):
                st.session_state.show_system_prompts_list = False
                _clear_edit_and_confirm_flags()
                st.rerun()

        except Exception as e:
            st.error(f"Errore caricamento prompts: {e}")

    # ---------------------------
    # Torna alla home
    # ---------------------------
    if st.button("🏠 Torna alla Home"):
        reset_dashboard_session_state(st, home_vars)
        st.session_state.current_stage = "home"
        st.rerun()
