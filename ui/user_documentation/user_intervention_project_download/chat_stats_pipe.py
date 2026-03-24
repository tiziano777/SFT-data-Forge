import os
import sys
import multiprocessing
import json
from pathlib import Path

# --- DIPENDENZE RICHIESTE ---
try:
    from datatrove.executor import LocalPipelineExecutor 
    from datatrove_pipelines.chat_template_stats_pipeline.reader.unified_reader import UnifiedReader
    from datatrove_pipelines.chat_template_stats_pipeline.stats.chat_template_stats import ChatTemplateStats
except ImportError as e:
    print(f"❌ Errore: Dipendenze 'datatrove' o moduli 'datatrove_pipelines' non trovati.\n{e}")
    sys.exit(1)

# =================================================================
# CONFIGURAZIONE MANUALE PER L'UTENTE PROGRAMMATORE
# =================================================================
# L'utente imposta i path locali per testare l'estrazione stats
USER_CONFIG = {
    # Path della distribuzione mappata (dove ci sono i .jsonl.gz con schema target)
    "input_distribution_path": "/path/to/local/mapped-data/my_dataset/subset_it",
    
    # Cartella dove verranno salvate le statistiche finali
    "output_stats_base_path": "/path/to/local/stats-data",
    
    # Estensione/Sottocartella per le chat stats (es. /chat_template_stats)
    "chat_stats_extension": "/chat_template_stats",
    
    "glob_pattern": "*.jsonl.gz"
}

# =================================================================
# STATS PIPELINE LOGIC
# =================================================================

def run_stats_standalone(config):
    """Esegue l'estrazione delle statistiche chat localmente."""
    
    input_path = config["input_distribution_path"]
    
    # Costruzione del path di output simulando la logica del sistema Velvet
    # In Velvet: mapped-data -> stats-data + extension
    # Qui lo semplifichiamo per l'utente locale:
    output_path = str(Path(config["output_stats_base_path"]) / 
                      Path(input_path).name / 
                      config["chat_stats_extension"].lstrip("/"))

    print("\n" + "="*50)
    print("📊 CHAT TEMPLATE STATS EXTRACTION (OFFLINE)")
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
        recursive=True,
        text_key="text", # Non usato per chat stats ma richiesto dal reader
        id_key="id",
        default_metadata={} 
    )
    
    # Lo stage che calcola effettivamente le stats sui messaggi
    chat_template_stats = ChatTemplateStats(output_folder=output_path)

    # 3. Esecuzione
    executor = LocalPipelineExecutor(
        pipeline=[reader, chat_template_stats],
        tasks=int(n_cores * 4), # Ottimizzato per uso locale
        workers=max(1, int(n_cores * 0.85)),
        logging_dir=None,
    )
    
    try:
        executor.run()
        print("\n✅ Successo: Statistiche salvate correttamente.")
        
        # Generazione report per lo sviluppatore
        generate_summary(output_path)
        
    except Exception as e:
        print(f"\n❌ Errore durante l'esecuzione: {e}")
        import traceback
        traceback.print_exc()

def generate_summary(output_path):
    """Mostra all'utente cosa ha prodotto lo script."""
    files_created = list(Path(output_path).glob("*"))
    print("\n--- RISULTATI GENERATI ---")
    for f in files_created:
        print(f" - {f.name} ({os.path.getsize(f) / 1024:.2f} KB)")
    print("--------------------------")
    print(f"💡 Questi file possono ora essere visualizzati nel modulo 'Stats Dashboard' di Velvet.")

# =================================================================
# MAIN ENTRY POINT
# =================================================================

if __name__ == "__main__":
    # Permette il lancio rapido o l'override tramite JSON CLI
    if len(sys.argv) > 1:
        try:
            # Se l'utente passa un solo argomento che non è JSON, lo trattiamo come path di input
            if not sys.argv[1].startswith('{'):
                active_config = USER_CONFIG.copy()
                active_config["input_distribution_path"] = sys.argv[1]
            else:
                active_config = json.loads(sys.argv[1])
        except:
            active_config = USER_CONFIG
    else:
        active_config = USER_CONFIG

    # Verifica minima esistenza input
    if not os.path.exists(active_config["input_distribution_path"]):
        print(f"❌ Errore: Il path di input '{active_config['input_distribution_path']}' non esiste.")
        sys.exit(1)
        
    run_stats_standalone(active_config)