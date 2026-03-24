import os
import sys
import multiprocessing

from datatrove_pipelines.chat_template_stats_pipeline.reader.unified_reader import UnifiedReader
from datatrove_pipelines.chat_template_stats_pipeline.stats.chat_template_stats import ChatTemplateStats
from datatrove.executor import LocalPipelineExecutor 

# Caricamento configurazioni

MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR")
STATS_DATA_DIR = os.getenv("STATS_DATA_DIR")
# Assicurati che nel .env sia "/chat_template_stats"
CHAT_TEMPLATE_STATS_EXTENSION = os.getenv("CHAT_TEMPLATE_STATS_EXTENSION")

def run_stats_pipeline(distribution_uri: str):
    input_path = distribution_uri
    
    # 1. Trasformiamo il path da MAPPED a STATS
    # 2. Aggiungiamo il suffisso della cartella specifica (es. /chat_template_stats)
    output_path = distribution_uri.replace(MAPPED_DATA_DIR, STATS_DATA_DIR) + CHAT_TEMPLATE_STATS_EXTENSION
    
    # Creazione fisica della directory prima di avviare Datatrove
    os.makedirs(output_path, exist_ok=True)
    
    n_cores = multiprocessing.cpu_count()
    
    reader = UnifiedReader(
        data_folder=input_path,
        glob_pattern="*.jsonl.gz",
        recursive=True,
        text_key="text",
        id_key="id",
        default_metadata={} 
    )
    
    # Passiamo il path completo di extension a ChatTemplateStats
    chat_template_stats = ChatTemplateStats(output_folder=output_path)

    executor = LocalPipelineExecutor(
        pipeline=[reader, chat_template_stats],
        tasks=int(n_cores * 10),
        workers=int(n_cores * 0.85),
        logging_dir=None,
    )
    
    try:
        executor.run()
        print(f"✅ Successo: Chat stats salvate in {output_path}")
    except Exception as e:
        print(f"❌ Errore nel worker: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_path = sys.argv[1]
        run_stats_pipeline(target_path)