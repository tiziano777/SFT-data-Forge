import subprocess
import sys
import os

def _launch_mapped_low_level_worker(distribution_uri: str):
    """Lancia il worker delle statistiche mappate in background."""
    worker_script = os.path.join(os.path.dirname(__file__), "mapped_low_level_stats_worker.py")
    
    try:
        # Lancio detached
        subprocess.Popen(
            [sys.executable, worker_script, distribution_uri],
            stdout=None, 
            stderr=None, 
            start_new_session=True,
            close_fds=True
        )
        return True
    except Exception as e:
        print(f"Errore nel lancio del processo: {e}")
        return False

def show_mapped_low_level_stats_extraction(st_app):
    st_app.header("Estrazione statistiche a basso livello")
    
    col1, col2 = st_app.columns(2)

    with col1:
        if st_app.button(
            "📊 Estrai statistiche (Background Mode)",
            use_container_width=True,
        ):
            path = st_app.session_state.current_distribution_path
            
            # Lanciamo il worker esterno invece di eseguire qui la logica
            if _launch_mapped_low_level_worker(path):
                st_app.success("🚀 Pipeline avviata con successo in background!")
                st_app.info(
                    "Il sistema sta elaborando i dati mappati usando tutti i core CPU. "
                    "Puoi navigare liberamente nell'app."
                )
            else:
                st_app.error("Si è verificato un errore durante l'avvio del processo.")

    with col2:
        if st_app.button(
            "⬅️ Back to Distribution",
            use_container_width=True,
        ):
            st_app.session_state.current_stage = "mapped_distribution_main"
            st_app.rerun()