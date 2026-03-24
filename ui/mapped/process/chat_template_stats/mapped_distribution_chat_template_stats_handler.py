# ui/chat_template_stats/mapped_distribution_chat_template_stats_handler.py
import streamlit as st
import subprocess
import sys
import os

def _launch_detached_worker(distribution_uri: str):
    """Lancia il file worker.py in un processo totalmente indipendente."""
    # Percorso assoluto del worker (stessa cartella)
    worker_path = os.path.join(os.path.dirname(__file__), "chat_template_stats_runner.py")
    
    try:
        # Popen lancia il processo e continua l'esecuzione di Streamlit
        subprocess.Popen(
            [sys.executable, worker_path, distribution_uri],
            stdout=None, # Puoi ridirigere su un file se vuoi i log (es. open("log.txt", "a"))
            stderr=None,
            start_new_session=True, # Detach fondamentale su Linux/Mac
            close_fds=True
        )
        return True
    except Exception as e:
        st.error(f"Errore tecnico nel lancio: {e}")
        return False

def show_mapped_chat_template_stats_extraction(st_app):
    st_app.header("Estrazione statistiche del template chat")

    col1, col2 = st_app.columns(2)

    with col1:
        if st_app.button(
            "📊 Avvia Estrazione in Background",
            use_container_width=True,
        ):
            path = st_app.session_state.current_distribution_path
            
            if _launch_detached_worker(path):
                st_app.success("🚀 Pipeline avviata con successo!")
                st_app.info(
                    "Il processamento sta usando tutti i core della macchina in background. "
                    "Puoi continuare a usare l'app o chiuderla."
                )
            else:
                st_app.error("Errore durante l'avvio della pipeline.")

    with col2:
        if st_app.button("⬅️ Back to Distribution", use_container_width=True):
            st_app.session_state.current_stage = "mapped_distribution_main"
            st_app.rerun()