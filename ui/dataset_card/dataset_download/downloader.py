import os
import subprocess
import sys
from datetime import datetime

# Import relativo corretto per il tuo progetto
from .download_model import DownloadResult

class DatasetDownloader:
    def __init__(self, db_manager, base_path: str):
        self.db_manager = db_manager
        self.base_path = base_path

    def _build_hierarchical_path(self, base_path: str, repo_id: str) -> str:
        """
        Crea la struttura Author/Dataset assicurando che base_path sia valido.
        Risolve il bug del NoneType intercettando input vuoti dall'interfaccia.
        """
        effective_base = base_path if base_path is not None else self.base_path
        if not effective_base:
            effective_base = os.getcwd()

        if not repo_id:
            return os.path.abspath(effective_base)

        # Pulizia del repo_id per evitare slash multipli o iniziali
        clean_repo_parts = [p for p in repo_id.split('/') if p]
        
        # Casting a stringa per os.path.join per prevenire errori di posixpath
        return os.path.abspath(os.path.join(str(effective_base), *clean_repo_parts))

    def _create_dataset_record(self, dataset_card, local_path, repo_id, remote_url):
        """Registrazione del dataset sul database tramite il DB Manager."""
        if hasattr(self.db_manager, 'register_dataset'):
            return self.db_manager.register_dataset(dataset_card, local_path, repo_id, remote_url)
        return None



    # --- BACKGROUND WORKER METHOD ---

    def run_background_download(self, repo_id: str, save_dir: str, dataset_card, config) -> DownloadResult:
        """Esegue il download gestito internamente dal container tramite worker Python."""
        try:
            abs_save_dir_container = self._build_hierarchical_path(save_dir, repo_id)
            
            # Setup directory logs nel container
            log_dir = os.path.join(abs_save_dir_container, "logs")
            os.makedirs(log_dir, exist_ok=True)
            
            log_path = os.path.join(log_dir, f"bg_{datetime.now().strftime('%H%M%S')}.log")
            token = os.getenv("HF_ACCESS_TOKEN", "none")
            
            # Percorso dello script worker relativo al pacchetto attuale
            current_dir = os.path.dirname(os.path.abspath(__file__))
            worker_script = os.path.join(current_dir, "raw_datatrove_pipe", "hf_raw_download_worker.py")
            
            # Lancio del processo worker
            subprocess.Popen(
                [
                    sys.executable, 
                    worker_script, 
                    repo_id, 
                    abs_save_dir_container, 
                    token, 
                    str(getattr(config, 'max_workers', 4)), 
                    log_path
                ],
                start_new_session=True,
                close_fds=True
            )
            
            # Registrazione a DB
            dataset_id = self._create_dataset_record(
                dataset_card, 
                abs_save_dir_container, 
                repo_id, 
                f"https://huggingface.co/datasets/{repo_id}"
            )
            
            return DownloadResult(
                success=True, 
                dataset_id=dataset_id, 
                message=f"Worker avviato con successo. Destinazione: {abs_save_dir_container}"
            )
        except Exception as e:
            return DownloadResult(success=False, message=f"Errore inizializzazione Worker: {str(e)}")

    def execute_download(self, dataset_card, save_dir: str, config) -> DownloadResult:
        """Entrypoint principale per la gestione del download."""
        if hasattr(config, 'source') and config.source == 'huggingface':
            return self.run_background_download(config.repo_id, save_dir, dataset_card, config)
        
        return DownloadResult(success=False, message="Sorgente di download non supportata.")