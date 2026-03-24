import os
import sys
import multiprocessing

from datatrove_pipelines.low_level_stats_pipeline.reader.unified_reader import UnifiedReader
from datatrove_pipelines.low_level_stats_pipeline.stats.low_level_stats import DocStats
from datatrove.executor import LocalPipelineExecutor 

# Caricamento configurazioni

MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR")
STATS_DATA_DIR = os.getenv("STATS_DATA_DIR")
LOW_LEVEL_STATS_EXTENSION = os.getenv("LOW_LEVEL_STATS_EXTENSION")

def run_mapped_stats_pipeline(distribution_uri: str):
    input_path = distribution_uri
    output_path = distribution_uri.replace(MAPPED_DATA_DIR, STATS_DATA_DIR) + LOW_LEVEL_STATS_EXTENSION
    
    # Sfruttiamo tutti i core della macchina
    n_cores = multiprocessing.cpu_count()
    os.makedirs(output_path, exist_ok=True)
    
    try:
        reader = UnifiedReader(
            data_folder=input_path,
            glob_pattern="*.jsonl.gz",
            recursive=False, # Manteniamo il parametro come nel tuo snippet originale
            text_key="text",
            id_key="id",
            default_metadata={} 
        )
        
        low_level_stats = DocStats(
            output_folder=output_path,
        )

        pipeline = [reader, low_level_stats]
        
        # Esecuzione parallela su macchina singola
        pipe = LocalPipelineExecutor(
            pipeline=pipeline,
            tasks=n_cores*10,    # Divide il lavoro sui core
            workers=int(n_cores*0.85),  # Esegue i processi in parallelo
            logging_dir=None
        )
        pipe.run()
        print(f"Successo: Statistiche salvate in {output_path}")
        
    except Exception as e:
        print(f"Errore nel worker: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        dist_uri = sys.argv[1]
        run_mapped_stats_pipeline(dist_uri)