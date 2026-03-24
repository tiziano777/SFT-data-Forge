import sys
import multiprocessing

from loguru import logger
from datasets import get_dataset_split_names 

from datatrove_pipelines.download_pipeline.reader.hf_reader import CustomHuggingFaceReader
from datatrove_pipelines.download_pipeline.writer.writer import CustomJsonlWriter 
from datatrove.executor import LocalPipelineExecutor



def run_hf_download_pipeline(repo_id: str, languages: list, output_base: str):
    """
    Esegue la pipeline di download. 
    output_base conterrà il path completo (es. /.../processed-data/velvet_v1)
    """
    try:
        # Rileva automaticamente tutti gli split disponibili
        available_splits = get_dataset_split_names(repo_id)
        logger.info(f"🔍 Split rilevati per {repo_id}: {available_splits}")
    except Exception as e:
        logger.warning(f"⚠️ Impossibile rilevare gli split, uso 'train' come fallback: {e}")
        available_splits = ["train"]

    for split_name in available_splits:
        logger.info(f"🚀 Avvio pipeline su: {output_base}")
        logger.info(f"📂 Split: {split_name}")
        
        # Inizializza Reader con il path della UI
        reader = CustomHuggingFaceReader(
            dataset=repo_id,
            base_output_path=output_base,
            streaming=True,
            available_languages=languages,
            dataset_options={"split": split_name}
        )

        # Inizializza Writer con il path della UI
        writer = CustomJsonlWriter(
            base_input_path=repo_id, 
            base_output_path=output_base
        )

        pipeline = [reader, writer]
        
        # Log del processo Datatrove
        #log_dir = os.path.join(os.getcwd(), "logs_hf_download", repo_id.replace("/", "_"), split_name)
        #os.makedirs(log_dir, exist_ok=True)

        n_cores = multiprocessing.cpu_count()
        executor = LocalPipelineExecutor(
            pipeline=pipeline,
            tasks=n_cores, 
            workers=max(1, int(n_cores * 0.8)),
            logging_dir=None #log_dir
        )
        
        executor.run()
        logger.info(f"✅ Split {split_name} completato.")

if __name__ == "__main__":
    # Aspettiamo 3 argomenti: repo_id, langs, output_base
    if len(sys.argv) > 3:
        repo_id = sys.argv[1]
        langs = sys.argv[2].split(",")
        output_base = sys.argv[3] 
        run_hf_download_pipeline(repo_id, langs, output_base)
    else:
        logger.error("❌ Mancano argomenti: necessario repo_id, langs, output_base")