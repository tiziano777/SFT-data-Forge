import os
import sys
import json
import multiprocessing
import traceback
from pathlib import Path

# --- DIPENDENZE RICHIESTE ---
# Assicurati che la cartella 'datatrove_pipelines' sia presente nel progetto
try:
    from datatrove.executor import LocalPipelineExecutor 
    from datatrove_pipelines.mapped_pipeline.reader.unified_reader import UnifiedReader
    from datatrove_pipelines.mapped_pipeline.extractor.map import MapperExtractor
    from datatrove_pipelines.mapped_pipeline.writer.writer import CustomJsonlWriter
    from datatrove_pipelines.mapped_pipeline.stats.low_level_stats import DocStats
except ImportError as e:
    print(f"❌ Errore: Dipendenze mancanti o cartella 'datatrove_pipelines' non trovata.\n{e}")
    sys.exit(1)

# =================================================================
# CONFIGURAZIONE MANUALE PER L'UTENTE PROGRAMMATORE
# =================================================================
# Qui l'utente definisce i suoi path locali e le regole di trasformazione
USER_CONFIG = {
    # Path locali (simulando file://)
    "input_dataset_path": "/path/to/local/processed-data/dataset_name",
    "input_distribution_path": "/path/to/local/processed-data/dataset_name/subset_it",
    
    "output_dataset_path": "/path/to/local/mapped-data/dataset_name",
    "output_distribution_path": "/path/to/local/mapped-data/dataset_name/subset_it",
    
    "low_level_stats_path": "/path/to/local/stats-data/low_level",

    # Parametri ETL
    "glob_pattern": "*.jsonl.gz",
    
    # Definizione del mapping (Lo schema che l'utente vuole testare)
    "src_schema": {}, # Opzionale, schema sorgente
    "dst_schema": {
        "properties": {
            "messages": {"type": "array"}, # Se presente abilita chat_stats
            "text": {"type": "string"}
        }
    },
    "mapping": {
        "content": "text",
        "chat_history": "messages"
        # Aggiungere qui le regole di mapping chiave_dest: chiave_orig
    }
}

# =================================================================
# CORE MAPPING PIPELINE
# =================================================================

def run_mapping_standalone(config):
    """Esegue la pipeline di mapping e stats localmente."""
    try:
        print("\n" + "="*50)
        print("🚀 AVVIO MAPPING PIPELINE (OFFLINE MODE)")
        print("="*50)

        # 1. Validazione e creazione directory
        paths_to_check = [
            config['low_level_stats_path'],
            config['output_distribution_path']
        ]

        for p in paths_to_check:
            os.makedirs(p, exist_ok=True)
            print(f"✅ Directory verificata: {p}")

        # 2. Inizializzazione Reader
        # Legge i dati processati (Step 2)
        reader = UnifiedReader(
            data_folder=config['input_distribution_path'],
            glob_pattern=config['glob_pattern'],
            recursive=True
        )

        # 3. Inizializzazione Mapper
        # Applica le trasformazioni definite nel dizionario 'mapping'
        mapper = MapperExtractor(
            mapping_spec=config['mapping'],
            dst_schema=config['dst_schema'],
            src_schema=config['src_schema']
        )

        # 4. Inizializzazione Writer
        # Il CustomJsonlWriter usa i base_path per ricostruire la struttura
        writer = CustomJsonlWriter(
            base_input_path=config['input_dataset_path'],
            base_output_path=config['output_dataset_path'],
        )

        # 5. Costruzione Pipeline
        pipeline = [
            reader,
            DocStats(output_folder=config['low_level_stats_path']), # Stats base
            mapper,
            writer
        ]
        


        # 6. Esecuzione Parallela
        n_workers = max(1, int(multiprocessing.cpu_count() * 0.85))
        print(f"⚙️ Utilizzo di {n_workers} core...")

        executor = LocalPipelineExecutor(
            pipeline=pipeline,
            tasks=n_workers * 2,
            workers=n_workers,
            logging_dir=None
        )
        
        executor.run()
        
        print("\n" + "="*50)
        print("✅ ELABORAZIONE COMPLETATA")
        print(f"📂 Dati mappati: {config['output_distribution_path']}")
        print(f"📊 Stats: {config['low_level_stats_path']}")
        print("="*50)

        # Simulazione ricevuta per il programmatore (quello che andrebbe nel DB)
        generate_db_summary(config)

    except Exception as e:
        print(f"\n❌ ERRORE CRITICO:\n{traceback.format_exc()}")

def generate_db_summary(config):
    """Mostra all'utente i metadati che verrebbero salvati nel DB Velvet."""
    summary = {
        "action": "DB_REGISTER_MAPPED_DATA",
        "dataset_name": f"mapped__{Path(config['input_dataset_path']).name}",
        "distribution_uri": f"file://{config['output_distribution_path']}",
        "step": 3,
        "derived_from": "original_distribution_id",
        "stats_files": {
            "low_level": config['low_level_stats_path']
        }
    }
    print("\nPROSPETTO PER DATABASE (MOCK):")
    print(json.dumps(summary, indent=4))

if __name__ == "__main__":
    # Possibilità di passare un JSON da riga di comando o usare USER_CONFIG
    if len(sys.argv) > 1:
        try:
            active_params = json.loads(sys.argv[1])
        except:
            active_params = USER_CONFIG
    else:
        active_params = USER_CONFIG
        
    run_mapping_standalone(active_params)