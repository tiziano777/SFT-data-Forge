def clean_session_state(st,keys):
    """Rimuove (del) le chiavi specificate da st.session_state se esistono."""
    for key in keys:
        if key in st.session_state:
            del st.session_state[key]

def navigate_to_and_clean(st, stage, keys_to_delete):
    clean_session_state(st, keys_to_delete)
    st.session_state.current_stage = stage

def reset_session_state(st_app, keys_to_reset: list[str]):
    """Elimina variabili di sessione relative alla fase 'distribution'."""
    for key in keys_to_reset:
        if key in st_app.session_state:
            del st_app.session_state[key]

def reset_dashboard_session_state(st_app, keys_to_maintain: list[str] = []):
    """Resetta lo stato della sessione mantenendo solo le chiavi specificate."""
    keys_to_delete = [key for key in st_app.session_state.keys() if key not in keys_to_maintain]
    for key in keys_to_delete:
        del st_app.session_state[key]
