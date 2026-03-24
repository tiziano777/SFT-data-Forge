# /ui/template_schema_management/template_schema_action_selection_handler.py

import json
from datetime import datetime, timezone

from config.state_vars import home_vars
from utils.streamlit_func import reset_dashboard_session_state
from data_class.repository.table.schema_template_repository import SchemaTemplateRepository
from data_class.entity.table.schema_template import SchemaTemplate


def show_template_schema_management(st):
    st.markdown("## Destination Schema Templates")
    st.markdown("Qui puoi creare, modificare e rimuovere i template di schema.")

    # ---------------------------
    # Stato base
    # ---------------------------
    st.session_state.setdefault("show_new_template_form", False)
    st.session_state.setdefault("show_templates_list", False)
    st.session_state.setdefault("template_search", "")

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
        not st.session_state.show_new_template_form
        and not st.session_state.show_templates_list
        and not _any_edit_open()
    )

    if show_buttons:
        col1, col2 = st.columns(2)

        if col1.button("📄 Create New Schema Template"):
            st.session_state.show_new_template_form = True
            st.session_state.show_templates_list = False
            _clear_edit_and_confirm_flags()
            st.rerun()

        if col2.button("📑 Show Schema Templates"):
            st.session_state.show_templates_list = True
            st.session_state.show_new_template_form = False
            _clear_edit_and_confirm_flags()
            st.rerun()

    # ---------------------------
    # Creazione template
    # ---------------------------
    if st.session_state.show_new_template_form:
        st.session_state.show_templates_list = False

        with st.form("new_schema_template_form"):
            name = st.text_input("Name")
            description = st.text_area("Description", height=100)
            schema_text = st.text_area("Schema (JSON)", height=200)

            save_clicked = st.form_submit_button("💾 Save")
            cancel_clicked = st.form_submit_button("⬅️ Cancel")

            if save_clicked:
                try:
                    schema_obj = json.loads(schema_text) if schema_text.strip() else {}

                    entity = SchemaTemplate(
                        id=None,
                        serial=None,
                        name=name,
                        schema=schema_obj,
                        description=description,
                        version="1.0",
                        issued=datetime.now(timezone.utc),
                        modified=datetime.now(timezone.utc),
                    )

                    repo = SchemaTemplateRepository(st.session_state.db_manager)
                    inserted = repo.save(entity)

                    if inserted:
                        st.success(f"Template salvato: {inserted.name}")
                    else:
                        st.error("Salvataggio fallito")

                    st.session_state.show_new_template_form = False
                    _clear_edit_and_confirm_flags()
                    st.rerun()

                except json.JSONDecodeError as je:
                    st.error(f"Schema JSON non valido: {je}")
                except Exception as e:
                    st.error(f"Errore durante il salvataggio: {e}")

            if cancel_clicked:
                st.session_state.show_new_template_form = False
                _clear_edit_and_confirm_flags()
                st.rerun()

    # ---------------------------
    # Lista template
    # ---------------------------
    if st.session_state.show_templates_list:
        try:
            repo = SchemaTemplateRepository(st.session_state.db_manager)
            all_templates = repo.find_all()

            query = st.text_input("Search", value=st.session_state.template_search)
            st.session_state.template_search = query

            def matches_query(t, q):
                if not q:
                    return False
                q = q.lower()
                return (
                    q in (t.name or "").lower()
                    or q in (t.description or "").lower()
                    or q in json.dumps(t.schema or {}).lower()
                )

            templates = (
                [t for t in all_templates if matches_query(t, query)]
                if query
                else all_templates
            )

            st.markdown(f"### Risultati: {len(templates)}")

            for t in templates:
                expanded = st.session_state.get(f"force_expand_{t.id}", False)

                with st.expander(f"{t.name} — {t.description[:50]}...", expanded=expanded):
                    b1, b2, content = st.columns([1, 1, 8])

                    if b1.button("✏️", key=f"edit_{t.id}"):
                        st.session_state[f"show_edit_form_{t.id}"] = True
                        st.session_state[f"force_expand_{t.id}"] = True
                        st.session_state.pop(f"confirm_delete_{t.id}", None)
                        st.rerun()

                    if b2.button("🗑️", key=f"delete_{t.id}"):
                        st.session_state[f"confirm_delete_{t.id}"] = True
                        st.session_state[f"force_expand_{t.id}"] = True
                        st.session_state.pop(f"show_edit_form_{t.id}", None)
                        st.rerun()

                    content.write("**Description:**")
                    content.write(t.description)
                    content.write("**Schema:**")
                    content.json(t.schema or {})

                    content.write(f"**Serial:** {getattr(t, 'serial', '')}")
                    content.write(f"**Version:** {getattr(t, 'version', '')}")
                    content.write(f"**Issued:** {getattr(t, 'issued', '')}")
                    content.write(f"**Modified:** {getattr(t, 'modified', '')}")

                    # -------- DELETE CONFIRM (FIX) --------
                    if st.session_state.get(f"confirm_delete_{t.id}"):
                        content.warning("Confermi eliminazione? Operazione irreversibile.")
                        c1, c2 = content.columns(2)

                        if c1.button("Conferma", key=f"confirm_{t.id}"):
                            deleted = repo.delete(t.id)
                            if deleted:
                                st.success("Eliminato")
                            else:
                                st.error("Eliminazione fallita")

                            _clear_edit_and_confirm_flags()
                            st.session_state.show_templates_list = True
                            st.rerun()

                        if c2.button("Annulla", key=f"cancel_{t.id}"):
                            # user clicked back/cancel
                            if f"confirm_delete_{t.id}" in st.session_state:
                                del st.session_state[f"confirm_delete_{t.id}"]
                            del st.session_state[f"force_expand_{t.id}"]
                            st.rerun()

                    # -------- EDIT FORM --------
                    if st.session_state.get(f"show_edit_form_{t.id}"):
                        with content.form(f"edit_form_{t.id}"):
                            content.write(f"**Serial (DB):** {getattr(t, 'serial', '')}")

                            name_e = st.text_input("Name", t.name)
                            desc_e = st.text_area("Description", t.description, height=100)
                            schema_e = st.text_area(
                                "Schema (JSON)",
                                json.dumps(t.schema or {}, indent=2),
                                height=200,
                            )
                            version_e = st.text_input("Version", t.version)

                            save_e = st.form_submit_button("💾 Save")
                            cancel_e = st.form_submit_button("⬅️ Cancel")

                            if save_e:
                                schema_obj = json.loads(schema_e) if schema_e.strip() else {}

                                updated = repo.update(
                                    SchemaTemplate(
                                        id=t.id,
                                        serial=t.serial,
                                        name=name_e,
                                        schema=schema_obj,
                                        description=desc_e,
                                        version=version_e,
                                        issued=t.issued,
                                        modified=datetime.now(timezone.utc),
                                    )
                                )

                                if updated:
                                    st.success("Aggiornato")
                                else:
                                    st.error("Update fallito")

                                _clear_edit_and_confirm_flags()
                                st.session_state.show_templates_list = True
                                st.rerun()

                            if cancel_e:
                                del st.session_state[f"show_edit_form_{t.id}"]
                                del st.session_state[f"force_expand_{t.id}"]
                                st.rerun()

            if st.button("⬅️ Close templates list"):
                st.session_state.show_templates_list = False
                _clear_edit_and_confirm_flags()
                st.rerun()

        except Exception as e:
            st.error(f"Errore caricamento template: {e}")

    # ---------------------------
    # Torna alla home
    # ---------------------------
    if st.button("🏠 Torna alla Home"):
        reset_dashboard_session_state(st, home_vars)
        st.session_state.current_stage = "home"
        st.rerun()
