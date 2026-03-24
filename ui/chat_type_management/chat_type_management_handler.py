# ui/chat_type_management/chat_type_management_handler.py
from config.state_vars import home_vars
from utils.streamlit_func import reset_dashboard_session_state
from data_class.repository.vocabulary.vocab_chat_type_repository import VocabChatTypeRepository
from data_class.entity.vocabulary.vocab_chat_type import VocabChatType
from data_class.repository.table.schema_template_repository import SchemaTemplateRepository


def show_vocab_chat_type_management(st):
    st.markdown("## Vocab Chat Type Management")
    st.markdown("I chat type servono come label per identificare la strategia di apply_chat_template da usare in fase di processing per il training dei modelli di linguaggio. Ogni chat type è associato ad uno schema che definisce la struttura dei messaggi e dei parametri che la strategia si aspetta.")
    st.markdown("---")
    st.markdown("Qui puoi creare, modificare e rimuovere i tipi di chat.")
    st.markdown("---")

    # ---------------------------
    # Stato base
    # ---------------------------
    st.session_state.setdefault("show_new_chat_type_form", False)
    st.session_state.setdefault("show_chat_types_list", False)
    st.session_state.setdefault("chat_type_search", "")

    # ---------------------------
    # Helper
    # ---------------------------
    def _any_edit_open():
        return any(
            k.startswith("show_edit_chat_type_form_") and st.session_state.get(k)
            for k in st.session_state
        )

    def _clear_edit_and_confirm_flags():
        for k in list(st.session_state.keys()):
            if (
                k.startswith("show_edit_chat_type_form_")
                or k.startswith("confirm_delete_chat_type_")
                or k.startswith("force_expand_chat_type_")
            ):
                del st.session_state[k]

    def _load_schema_options():
        """Ritorna dict {schema_id: display_label} per il selectbox."""
        try:
            schema_repo = SchemaTemplateRepository(st.session_state.db_manager)
            schemas = schema_repo.find_all()
            return {s.id: f"{s.name} (v{s.version})" for s in schemas}
        except Exception as e:
            st.warning(f"Impossibile caricare gli schema: {e}")
            return {}

    # ---------------------------
    # Pulsanti principali
    # ---------------------------
    show_buttons = (
        not st.session_state.show_new_chat_type_form
        and not st.session_state.show_chat_types_list
        and not _any_edit_open()
    )

    if show_buttons:
        col1, col2 = st.columns(2)

        if col1.button("📄 Create New Chat Type"):
            st.session_state.show_new_chat_type_form = True
            st.session_state.show_chat_types_list = False
            _clear_edit_and_confirm_flags()
            st.rerun()

        if col2.button("📑 Show Chat Types"):
            st.session_state.show_chat_types_list = True
            st.session_state.show_new_chat_type_form = False
            _clear_edit_and_confirm_flags()
            st.rerun()

    # ---------------------------
    # Creazione chat type
    # ---------------------------
    if st.session_state.show_new_chat_type_form:
        st.session_state.show_chat_types_list = False

        schema_options = _load_schema_options()
        schema_ids = list(schema_options.keys())
        schema_labels = [schema_options[sid] for sid in schema_ids]

        with st.form("new_chat_type_form"):
            code = st.text_input("Code")
            description = st.text_area("Description", height=100)

            if schema_ids:
                selected_idx = st.selectbox(
                    "Schema",
                    options=range(len(schema_ids)),
                    format_func=lambda i: schema_labels[i],
                )
                selected_schema_id = schema_ids[selected_idx]
            else:
                st.warning("Nessuno schema disponibile.")
                selected_schema_id = st.text_input("Schema ID (manuale)")

            save_clicked = st.form_submit_button("💾 Save")
            cancel_clicked = st.form_submit_button("⬅️ Cancel")

            if save_clicked:
                try:
                    if not code.strip():
                        st.error("Il campo 'Code' è obbligatorio.")
                    elif not selected_schema_id:
                        st.error("Seleziona uno schema valido.")
                    else:
                        entity = VocabChatType(
                            id=None,
                            code=code.strip(),
                            description=description.strip() or None,
                            schema_id=selected_schema_id,
                        )

                        repo = VocabChatTypeRepository(st.session_state.db_manager)
                        inserted = repo.insert(entity)

                        if inserted:
                            st.success(f"Chat type salvato: {inserted.code}")
                        else:
                            st.error("Salvataggio fallito")

                        st.session_state.show_new_chat_type_form = False
                        _clear_edit_and_confirm_flags()
                        st.rerun()

                except Exception as e:
                    st.error(f"Errore durante il salvataggio: {e}")

            if cancel_clicked:
                st.session_state.show_new_chat_type_form = False
                _clear_edit_and_confirm_flags()
                st.rerun()

    # ---------------------------
    # Lista chat types
    # ---------------------------
    if st.session_state.show_chat_types_list:
        try:
            repo = VocabChatTypeRepository(st.session_state.db_manager)
            all_types = repo.get_all()

            schema_options = _load_schema_options()

            query = st.text_input("Search", value=st.session_state.chat_type_search)
            st.session_state.chat_type_search = query

            def matches_query(t, q):
                if not q:
                    return False
                q = q.lower()
                return (
                    q in (t.code or "").lower()
                    or q in (t.description or "").lower()
                )

            chat_types = (
                [t for t in all_types if matches_query(t, query)]
                if query
                else all_types
            )

            st.markdown(f"### Risultati: {len(chat_types)}")

            for t in chat_types:
                expanded = st.session_state.get(f"force_expand_chat_type_{t.id}", False)
                label = f"{t.code} — {(t.description or '')[:50]}"

                with st.expander(label, expanded=expanded):
                    b1, b2, content = st.columns([1, 1, 8])

                    if b1.button("✏️", key=f"edit_ct_{t.id}"):
                        st.session_state[f"show_edit_chat_type_form_{t.id}"] = True
                        st.session_state[f"force_expand_chat_type_{t.id}"] = True
                        st.session_state.pop(f"confirm_delete_chat_type_{t.id}", None)
                        st.rerun()

                    if b2.button("🗑️", key=f"delete_ct_{t.id}"):
                        st.session_state[f"confirm_delete_chat_type_{t.id}"] = True
                        st.session_state[f"force_expand_chat_type_{t.id}"] = True
                        st.session_state.pop(f"show_edit_chat_type_form_{t.id}", None)
                        st.rerun()

                    content.write(f"**ID:** {t.id}")
                    content.write(f"**Code:** {t.code}")
                    content.write(f"**Description:** {t.description or '—'}")
                    schema_label = schema_options.get(t.schema_id, t.schema_id)
                    content.write(f"**Schema:** {schema_label}")

                    # -------- DELETE CONFIRM --------
                    if st.session_state.get(f"confirm_delete_chat_type_{t.id}"):
                        content.warning("Confermi eliminazione? Operazione irreversibile.")
                        c1, c2 = content.columns(2)

                        if c1.button("Conferma", key=f"confirm_ct_{t.id}"):
                            deleted = repo.delete(t.id)
                            if deleted:
                                st.success("Eliminato")
                            else:
                                st.error("Eliminazione fallita")

                            _clear_edit_and_confirm_flags()
                            st.session_state.show_chat_types_list = True
                            st.rerun()

                        if c2.button("Annulla", key=f"cancel_ct_{t.id}"):
                            del st.session_state[f"confirm_delete_chat_type_{t.id}"]
                            del st.session_state[f"force_expand_chat_type_{t.id}"]
                            st.rerun()

                    # -------- EDIT FORM --------
                    if st.session_state.get(f"show_edit_chat_type_form_{t.id}"):
                        schema_ids = list(schema_options.keys())
                        schema_labels = [schema_options[sid] for sid in schema_ids]

                        # indice corrente nello schema selectbox
                        current_schema_idx = (
                            schema_ids.index(t.schema_id)
                            if t.schema_id in schema_ids
                            else 0
                        )

                        with content.form(f"edit_chat_type_form_{t.id}"):
                            content.write(f"**ID (DB):** {t.id}")

                            code_e = st.text_input("Code", t.code)
                            desc_e = st.text_area("Description", t.description or "", height=100)

                            if schema_ids:
                                selected_idx_e = st.selectbox(
                                    "Schema",
                                    options=range(len(schema_ids)),
                                    index=current_schema_idx,
                                    format_func=lambda i: schema_labels[i],
                                )
                                selected_schema_id_e = schema_ids[selected_idx_e]
                            else:
                                st.warning("Nessuno schema disponibile.")
                                selected_schema_id_e = st.text_input("Schema ID (manuale)", t.schema_id)

                            save_e = st.form_submit_button("💾 Save")
                            cancel_e = st.form_submit_button("⬅️ Cancel")

                            if save_e:
                                if not code_e.strip():
                                    st.error("Il campo 'Code' è obbligatorio.")
                                else:
                                    updated = repo.update(
                                        VocabChatType(
                                            id=t.id,
                                            code=code_e.strip(),
                                            description=desc_e.strip() or None,
                                            schema_id=selected_schema_id_e,
                                        )
                                    )

                                    if updated:
                                        st.success("Aggiornato")
                                    else:
                                        st.error("Update fallito")

                                    _clear_edit_and_confirm_flags()
                                    st.session_state.show_chat_types_list = True
                                    st.rerun()

                            if cancel_e:
                                del st.session_state[f"show_edit_chat_type_form_{t.id}"]
                                del st.session_state[f"force_expand_chat_type_{t.id}"]
                                st.rerun()

            if st.button("⬅️ Close list"):
                st.session_state.show_chat_types_list = False
                _clear_edit_and_confirm_flags()
                st.rerun()

        except Exception as e:
            st.error(f"Errore caricamento chat types: {e}")

    # ---------------------------
    # Torna alla home
    # ---------------------------
    if st.button("🏠 Torna alla Home"):
        reset_dashboard_session_state(st, home_vars)
        st.session_state.current_stage = "home"
        st.rerun()
    
