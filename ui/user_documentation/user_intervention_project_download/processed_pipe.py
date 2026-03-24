import sys
import json
import multiprocessing
from pathlib import Path


# --- DIPENDENZE RICHIESTE ---
# pip install datatrove[io] 
# (e le tue datatrove_pipelines personalizzate se necessarie)

# =================================================================
# CONFIGURAZIONE MANUALE PER L'UTENTE
# =================================================================
# L'utente può modificare questi parametri per farli puntare ai propri dati locali
USER_CONFIG = {
    "source_dataset_uri": "file:///path/to/your/raw/dataset_folder",
    "source_distribution_uri": "file:///path/to/your/raw/dataset_folder/sub_folder",
    "processed_base_uri": "file:///path/to/your/output/processed_data",
    "glob_pattern": "**/*.jsonl",  # Pattern per trovare i file sorgente
    "source_dataset_id": "manual-test-id",
    "source_distribution_id": "manual-dist-id",
    "default_metadata": {
        "project": "manual_test",
        "author": "dev_user"
    }
}

# =================================================================
# LOGICA DI UTILITÀ (ESTRATTA DAI TUOI MODULI)
# =================================================================

class PathExtractor:
    """Gestisce la conversione tra URI e path locali."""
    def __init__(self, source_dataset_uri, source_distribution_uri, processed_base_uri):
        self.source_dataset_uri = source_dataset_uri
        self.source_distribution_uri = source_distribution_uri
        self.processed_base_uri = processed_base_uri
        
        # Rimozione prefissi per uso locale
        self.source_dist_path = Path(source_distribution_uri.replace("file://", ""))
        self.source_dataset_path = Path(source_dataset_uri.replace("file://", ""))
        self.target_base_path = Path(processed_base_uri.replace("file://", ""))
        
        # Calcolo nomi relativi
        self.dataset_name = self.source_dataset_path.name
        self.dist_relative_name = self.source_dist_path.name

    def get_target_path(self):
        """Costruisce il path di output finale."""
        return self.target_base_path / self.dataset_name

# =================================================================
# CORE PIPELINE (DATATROVE)
# =================================================================

def run_local_pipeline(config):
    from datatrove.io import DataFolder
    from datatrove.executor import LocalPipelineExecutor
    # Nota: Assicurati che UnifiedReader e CustomJsonlWriter siano nel tuo PYTHONPATH
    # Se sono moduli custom, l'utente deve averli nella stessa cartella
    try:
        from datatrove_pipelines.processed_pipeline.reader.unified_reader import UnifiedReader
        from datatrove_pipelines.processed_pipeline.writer.writer import CustomJsonlWriter
    except ImportError:
        print("⚠️ Moduli custom non trovati. Usare reader/writer standard di Datatrove per il test.")
        return

    pe = PathExtractor(
        config["source_dataset_uri"], 
        config["source_distribution_uri"], 
        config["processed_base_uri"]
    )

    print(f"🚀 Avvio elaborazione: {pe.source_dist_path} -> {pe.get_target_path()}")

    # 1. Setup Lettore
    reader = UnifiedReader(
        data_folder=DataFolder(path=str(pe.source_dist_path)),
        glob_pattern=config["glob_pattern"],
        default_metadata=config["default_metadata"]
    )

    # 2. Setup Scrittore
    writer = CustomJsonlWriter(
        target_path=str(pe.get_target_path()),
        distribution_relative=pe.dist_relative_name,
        compression="gzip"
    )

    # 3. Esecuzione
    n_cores = multiprocessing.cpu_count()
    executor = LocalPipelineExecutor(
        pipeline=[reader, writer],
        tasks=n_cores,
        workers=max(1, int(n_cores * 0.8))
    )
    
    executor.run()
    print("✅ Pipeline completata con successo!")
    
    # Simula quello che verrebbe inviato al DB
    generate_mock_db_receipt(pe, config)

def generate_mock_db_receipt(pe, config):
    """Mostra all'utente come verrebbero registrati i dati nel sistema."""
    receipt = {
        "database_action": "INSERT/UPSERT",
        "new_dataset": {
            "name": f"processed__{pe.dataset_name}",
            "uri": f"file://{pe.get_target_path()}",
        },
        "new_distributions": [
            {
                "lang": "it",
                "uri": f"file://{pe.get_target_path()}/{pe.dist_relative_name}/it",
                "derived_from": config["source_distribution_id"]
            }
        ]
    }
    print("\n" + "="*50)
    print("PROSPETTO DATI PER IL DATABASE (JSON)")
    print("="*50)
    print(json.dumps(receipt, indent=4))

# =================================================================
# MAIN ENTRY POINT
# =================================================================

if __name__ == "__main__":
    print("--- VELVET OFFLINE MODULE EXAMPLE ---")
    
    # Controllo se l'utente ha passato un JSON via CLI o usa la USER_CONFIG interna
    if len(sys.argv) > 1:
        try:
            active_config = json.loads(sys.argv[1])
            print("usando configurazione da CLI...")
        except:
            active_config = USER_CONFIG
    else:
        print("Usando configurazione predefinita nel file...")
        active_config = USER_CONFIG

    # Verifica esistenza path sorgente
    src_check = Path(active_config["source_distribution_uri"].replace("file://", ""))
    if not src_check.exists():
        print(f"❌ ERRORE: Il path sorgente {src_check} non esiste!")
        print("Modifica 'USER_CONFIG' nel file prima di lanciare lo script.")
        sys.exit(1)

    run_local_pipeline(active_config)