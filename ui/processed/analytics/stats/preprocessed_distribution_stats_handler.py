import subprocess
import sys
import os

def _launch_low_level_worker(distribution_uri: str):
    # Usa un percorso assoluto per il log così sai dove trovarlo
    log_path = os.path.join(os.getcwd(), "worker_debug.log")
    worker_script = os.path.join(os.path.dirname(__file__), "preprocessed_low_level_stats_worker.py")
    
    try:
        with open(log_path, "a") as f:
            f.write(f"\n--- Avvio worker per: {distribution_uri} ---\n")
            subprocess.Popen(
                [sys.executable, worker_script, distribution_uri],
                stdout=f,  # Scrive l'output nel file
                stderr=f,  # Scrive gli errori nel file
                start_new_session=True,
                close_fds=True,
                cwd=os.getcwd() # Assicura che il worker parta dalla cartella corretta
            )
        return True
    except Exception as e:
        print(f"Errore nel lancio: {e}")
        return False

def show_processed_low_level_stats_extraction(st_app):
    st_app.header("Estrazione statistiche a basso livello")
    
    col1, col2 = st_app.columns(2)

    with col1:
        if st_app.button(
            "📊 Estrai statistiche in Background",
            use_container_width=True,
        ):
            # Recupero del path dalla session state
            path = st_app.session_state.current_distribution_path
            
            if _launch_low_level_worker(path):
                st_app.success("🚀 Processo avviato correttamente!")
                st_app.info(
                    "L'estrazione sta girando in background usando tutti i core disponibili. "
                    "Riceverai i risultati nella cartella delle statistiche appena pronti."
                )
            else:
                st_app.error("Impossibile avviare il processo di estrazione.")

    with col2:
        if st_app.button(
            "⬅️ Back to Distribution",
            use_container_width=True,
        ):
            st_app.session_state.current_stage = "processed_distribution_main"
            st_app.rerun()