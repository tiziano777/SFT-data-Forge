import os
import sys
datatrove_base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(datatrove_base_dir)

# Assicurati che i file hf_reader.py e writer.py siano nella stessa cartella
from reader.hf_reader import CustomHuggingFaceReader
from writer.writer import CustomJsonlWriter
from datatrove.executor.local import LocalPipelineExecutor # La classe che hai postato

def test_pipeline():
    # 1. Configurazione percorsi
    # In un ambiente reale sarebbero i tuoi path NFS
    BASE_INPUT = "./test_input"
    BASE_OUTPUT = "./test_output"
    LOGS_DIR = "./pipeline_logs"

    # Creiamo le cartelle di test
    os.makedirs(BASE_INPUT, exist_ok=True)
    os.makedirs(BASE_OUTPUT, exist_ok=True)

    # 2. Inizializzazione del Reader
    # Usiamo un dataset piccolo di Hugging Face per il test (es. 'fka/awesome-chatgpt-prompts')
    reader = CustomHuggingFaceReader(
        dataset="fka/awesome-chatgpt-prompts",
        base_output_path=BASE_OUTPUT,
        dataset_options={"split": "train"},
        limit=50,  # Leggiamo solo 50 documenti per il test
        distribution_key="category", # Se presente nel dataset
        available_languages=["en","it","fr"],
    )

    # 3. Inizializzazione del Writer
    writer = CustomJsonlWriter(
        base_input_path=BASE_INPUT,
        base_output_path=BASE_OUTPUT,
    )

    # 4. Composizione della Pipeline
    # La pipeline è una lista di step
    pipeline = [
        reader,
        writer
    ]

    # 5. Configurazione dell'Executor
    # tasks: numero totale di frammenti (shards)
    # workers: quanti processi paralleli usare
    executor = LocalPipelineExecutor(
        pipeline=pipeline,
        tasks=2,           
        workers=2,         
        logging_dir=None,
        skip_completed=False # Per rieseguire il test pulito ogni volta
    )

    # 6. Avvio
    print("🚀 Avvio della pipeline DataTrove...")
    executor.run()
    print("🏁 Pipeline completata!")

if __name__ == "__main__":
    # Nota: su macOS/Windows è necessario il blocco if __name__ == "__main__"
    # per il corretto funzionamento di multiprocessing
    test_pipeline()