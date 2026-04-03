import requests
import os
from typing import Optional
import logging
from datetime import datetime, timezone
from ui.dataset_card.dataset_download.download_model import DownloadConfig, DownloadResult
from data_class.entity.table.dataset_card import DatasetCard
from data_class.entity.table.dataset import Dataset
from data_class.repository.table.dataset_repository import DatasetRepository

BASE_PREFIX = os.getenv("BASE_PREFIX")
HF_ACCESS_TOKEN = os.getenv("HF_ACCESS_TOKEN")
logger = logging.getLogger(__name__)

class DatasetDownloader:
    """Gestisce il download dei dataset e la registrazione nel DB con isolamento del Mount Point"""
    
    def __init__(self, db_manager, base_path: str):
        self.db_manager = db_manager
        self.base_path = base_path
        self.dataset_repo = DatasetRepository(db_manager)

    def _create_dataset_record(self, dataset_card: DatasetCard, base_download_path: str, 
                            repo_id: Optional[str] = None, source_url: str = None) -> Optional[str]:
        """
        Crea un record nel database isolando lo scan dei glob sul Mount Point specifico.
        
        Args:
            dataset_card: La card logica di riferimento.
            base_download_path: Il percorso base scelto dall'utente (es. /mnt/data/subdir1).
            repo_id: L'identificativo (es. 'author/dataset'). Se presente, definisce il Mount Point.
            source_url: URL sorgente per i metadati.
        """
        try:
            # 1. DETERMINAZIONE MOUNT POINT FISICO
            # Se scarichiamo microsoft/phi-3 in /downloads, il mount point è /downloads/microsoft/
            if repo_id:
                mount_point_path = os.path.join(base_download_path, repo_id.split("/")[0])
            else:
                mount_point_path = base_download_path
            
            # Assicuriamoci che la cartella esista per lo scanner
            os.makedirs(mount_point_path, exist_ok=True)
            
            # 2. CALCOLO URI E SCAN GLOBS
            # La URI punta all'identità specifica, non alla cartella contenitore
            dataset_uri = f"{BASE_PREFIX}{mount_point_path}"
            
            from utils.extract_glob import generate_filtered_globs
            
            # Lo scan parte dal MOUNT POINT specifico: così i glob sono puliti e relativi
            # ed evitiamo di vedere altri dataset presenti in base_download_path
            dataset_globs = generate_filtered_globs(mount_point_path)
            
            if not dataset_globs:
                logger.warning(f"Nessun file trovato nel Mount Point isolato: {mount_point_path}")

            # 3. CREAZIONE ENTITY
            dataset_entity = Dataset(
                id=None,
                uri=dataset_uri,
                name=dataset_card.dataset_name,
                dataset_type= 'unknown',
                languages=dataset_card.languages or ['en'],
                derived_card=dataset_card.id,
                globs=dataset_globs,
                description=dataset_card.dataset_description or f"Dataset {dataset_card.dataset_name}",
                source=source_url or dataset_card.source_url,
                version='1.0',
                issued=datetime.now(timezone.utc),
                modified=datetime.now(timezone.utc),
                license=dataset_card.license or 'unknown',
                step=2
            )
            
            # Upsert basato sulla URI specifica del dataset
            result = self.dataset_repo.upsert_by_uri(dataset_entity)
            
            if result and result.id:
                logger.info(f"Dataset registrato correttamente. Mount Point: {mount_point_path}")
                return result.id
            return None
            
        except Exception as e:
            logger.error(f"Errore creazione record dataset (Mount Point Isolation): {str(e)}")
            return None

    def download_from_huggingface(self, repo_id: str, save_dir: str, 
                                dataset_card: DatasetCard, file_path: Optional[str] = None) -> DownloadResult:
        """Download da HF con isolamento dell'identità del dataset"""
        try:
            import subprocess
            import sys
            
            # Prepariamo il worker
            langs = ",".join(dataset_card.languages or ['en'])
            worker_script = os.path.join(os.path.dirname(__file__), "hf_datatrove_preprocessed_download_worker.py")
            log_path = os.path.join(os.getcwd(), "hf_download_debug.log")

            with open(log_path, "a") as f:
                f.write(f"\n--- Avvio Download: {repo_id} ({datetime.now(timezone.utc)}) ---\n")
                # Il worker gestirà la creazione della struttura repo_id dentro save_dir
                subprocess.Popen(
                    [sys.executable, worker_script, repo_id, langs, save_dir],
                    stdout=f,
                    stderr=f,
                    start_new_session=True,
                    close_fds=True,
                    cwd=os.getcwd()
                )

            # Registriamo il record usando il repo_id per isolare il Mount Point
            dataset_id = self._create_dataset_record(
                dataset_card=dataset_card,
                base_download_path=save_dir, 
                repo_id=repo_id, 
                source_url=f"https://huggingface.co/datasets/{repo_id}"
            )

            return DownloadResult(success=True, dataset_id=dataset_id, message="Pipeline avviata su Mount Point isolato.", file_path=save_dir)
        except Exception as e:
            return DownloadResult(success=False, message=str(e))
        
    def download_from_url(self, url: str, save_path: str, dataset_card: DatasetCard) -> DownloadResult:
        """Scarica un dataset da URL (il save_path deve essere già specifico per il dataset)"""
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            with open(save_path, 'wb') as file:
                file.write(response.content)
            
            downloaded_path = save_path
            
            # Gestione ZIP: se estraiamo, la cartella estratta diventa il nostro Mount Point
            if save_path.endswith('.zip'):
                try:
                    import zipfile
                    extract_dir = save_path.replace('.zip', '')
                    with zipfile.ZipFile(save_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    downloaded_path = extract_dir
                except Exception as e:
                    logger.warning(f"Errore estrazione: {e}")
            
            # Per URL diretti, il Mount Point è la cartella finale (downloaded_path)
            dataset_id = self._create_dataset_record(
                dataset_card=dataset_card,
                base_download_path=downloaded_path,
                repo_id=None, # Nessun repo_id, lo scan parte da downloaded_path
                source_url=url
            )
            
            return DownloadResult(
                success=True,
                message="Dataset scaricato e registrato.",
                dataset_id=dataset_id,
                file_path=downloaded_path
            )
            
        except Exception as e:
            logger.error(f"Errore download URL: {str(e)}")
            return DownloadResult(success=False, message=str(e))

    def execute_download(self, dataset_card: DatasetCard, download_path: str, 
                        config: DownloadConfig) -> DownloadResult:
        """Esecuzione orchestrata del download"""
        try:
            if config.source == 'url' and config.url:
                return self.download_from_url(config.url, download_path, dataset_card)
            elif config.source == 'huggingface' and config.repo_id:
                return self.download_from_huggingface(
                    repo_id=config.repo_id,
                    save_dir=download_path,
                    dataset_card=dataset_card,
                    file_path=config.file_path
                )
            return DownloadResult(success=False, message="Configurazione non valida")
        except Exception as e:
            logger.error(f"Errore execute_download: {str(e)}")
            return DownloadResult(success=False, message=str(e))
        

