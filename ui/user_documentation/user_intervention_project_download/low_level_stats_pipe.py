import os
import sys
import multiprocessing
import json
from pathlib import Path

# --- DIPENDENZE RICHIESTE ---
try:
    from datatrove.executor import LocalPipelineExecutor 
    from datatrove_pipelines.low_level_stats_pipeline.reader.unified_reader import UnifiedReader
    from datatrove_pipelines.low_level_stats_pipeline.stats.low_level_stats import DocStats
except ImportError as e:
    print(f"❌ Errore: Dipendenze 'datatrove' o moduli 'datatrove_pipelines' non trovati.\n{e}")
    sys.exit(1)

# =================================================================
# CONFIGURAZIONE MANUALE PER L'UTENTE PROGRAMMATORE
# =================================================================
USER_CONFIG = {
    # Path della distribuzione mappata (input)
    "input_distribution_path": "/path/to/local/mapped-data/my_dataset/subset_it",
    
    # Cartella base dove salvare le statistiche (output)
    "output_stats_base_path": "/path/to/local/stats-data",
    
    # Estensione/Sottocartella specifica (es. /low_level_stats)
    "low_level_extension": "/low_level_stats",
    
    "glob_pattern": "*.jsonl.gz",
    "recursive": False # Come nel tuo script originale
}

# =================================================================
# LOW-LEVEL STATS LOGIC
# =================================================================

def run_low_level_standalone(config):
    """Esegue l'estrazione delle statistiche testuali di base localmente."""
    
    input_path = config["input_distribution_path"]
    
    # Logica di costruzione path coerente con l'architettura Velvet
    # Trasforma il nome della cartella input in una sottocartella di output
    output_path = str(Path(config["output_stats_base_path"]) / 
                      Path(input_path).name / 
                      config["low_level_extension"].lstrip("/"))

    print("\n" + "="*50)
    print("📊 LOW-LEVEL DOC STATS (OFFLINE)")
    print("="*50)
    print(f"📂 Input:  {input_path}")
    print(f"📂 Output: {output_path}")

    # 1. Preparazione Directory
    os.makedirs(output_path, exist_ok=True)

    # 2. Setup Pipeline Datatrove
    n_cores = multiprocessing.cpu_count()
    
    reader = UnifiedReader(
        data_folder=input_path,
        glob_pattern=config["glob_pattern"],
        recursive=config["recursive"],
        text_key="text",
        id_key="id",
        default_metadata={} 
    )
    
    # Lo stage che analizza il contenuto testuale (DocStats)
    low_level_stats = DocStats(output_folder=output_path)

    # 3. Esecuzione
    executor = LocalPipelineExecutor(
        pipeline=[reader, low_level_stats],
        tasks=int(n_cores * 10),
        workers=max(1, int(n_cores * 0.85)),
        logging_dir=None,
    )
    
    try:
        executor.run()
        print("\n✅ Successo: Statistiche di basso livello salvate.")
        
        # Mostra riepilogo file creati
        files = list(Path(output_path).glob("*"))
        print(f"\nGenerati {len(files)} file di statistiche in: {output_path}")
        
    except Exception as e:
        print(f"\n❌ Errore nel worker: {e}")
        import traceback
        traceback.print_exc()

# =================================================================
# MAIN ENTRY POINT
# =================================================================

if __name__ == "__main__":
    # Supporta sia USER_CONFIG che input diretto via CLI
    if len(sys.argv) > 1:
        # Se riceve un path testuale semplice
        if not sys.argv[1].startswith('{'):
            active_config = USER_CONFIG.copy()
            active_config["input_distribution_path"] = sys.argv[1]
        else:
            active_config = json.loads(sys.argv[1])
    else:
        active_config = USER_CONFIG

    if not os.path.exists(active_config["input_distribution_path"]):
        print(f"❌ Errore: Path '{active_config['input_distribution_path']}' non trovato.")
        sys.exit(1)
        
    run_low_level_standalone(active_config)