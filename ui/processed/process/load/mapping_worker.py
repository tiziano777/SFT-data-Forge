import os
import sys
import json
import multiprocessing
import traceback
from pathlib import Path

from datatrove.executor import LocalPipelineExecutor 
from datatrove_pipelines.mapped_pipeline.reader.unified_reader import UnifiedReader
from datatrove_pipelines.mapped_pipeline.extractor.map import MapperExtractor
from datatrove_pipelines.mapped_pipeline.writer.unified_writer import UnifiedWriter
from datatrove_pipelines.mapped_pipeline.stats.low_level_stats import DocStats


def run_mapping_pipeline(args):
    try:
        # 1. Decodifica dei parametri originali
        input_dist_path = args['input_distribution_path']   
        output_dist_path = args['output_distribution_path']
        
        # Recupero i path dei dataset passati esplicitamente dalla UI
        base_input_root = args.get('input_dataset_path')
        base_output_root = args.get('output_dataset_path')
        
        low_level_stats_path = args['low_level_stats_path']
        mapping = args['mapping']
        dst_schema = args['dst_schema']
        src_schema = args['src_schema']
        glob_pattern = args['glob_pattern']
        output_format = args.get("output_format", "jsonl.gz")
        
        # 🔴 MANCA LA VALIDAZIONE! Aggiungiamola:
        
        print("🔍 PARAMETRI RICEVUTI:")
        print(f"   input_dist_path: {input_dist_path}")
        print(f"   base_input_root: {base_input_root}")
        print(f"   output_dist_path: {output_dist_path}")
        print(f"   base_output_root: {base_output_root}")
        print(f"   low_level_stats_path: {low_level_stats_path}")
        
        # VALIDAZIONE CRITICA - Verifica che i path esistano
        if not os.path.exists(input_dist_path):
            raise ValueError(f"❌ input_dist_path non esiste: {input_dist_path}")
            
        if not base_input_root:
            raise ValueError("❌ base_input_root non può essere None o vuoto!")
            
        if not base_output_root:
            raise ValueError("❌ base_output_root non può essere None o vuoto!")
        
        # Verifica che input_dist_path sia effettivamente sotto base_input_root
        try:
            input_path_obj = Path(input_dist_path)
            base_input_obj = Path(base_input_root)
            relative_path = input_path_obj.relative_to(base_input_obj)
            print(f"✅ Path relativo calcolato: {relative_path}")
        except ValueError:
            # Tentativo di correzione automatica
            print(f"⚠️ input_dist_path non è sotto base_input_root!")
            print(f"   input_dist_path: {input_dist_path}")
            print(f"   base_input_root: {base_input_root}")
            
            # Prova a correggere prendendo il parent del parent
            corrected_base = str(Path(input_dist_path).parent.parent.parent)
            print(f"   Correzione automatica -> base_input_root: {corrected_base}")
            base_input_root = corrected_base
        
        # Verifica che i path di stats siano validi
        if low_level_stats_path:
            os.makedirs(low_level_stats_path, exist_ok=True)
            print(f"✅ Directory stats creata/verificata: {low_level_stats_path}")
            

        print(f"🎯 Writer Base Input (Dataset Root): {base_input_root}")
        print(f"🎯 Writer Base Output (Dataset Root): {base_output_root}")
        
        # 3. Inizializzazione Reader
        reader = UnifiedReader(
            data_folder=input_dist_path,
            glob_pattern=glob_pattern,
            recursive=True,
            text_key="text", 
            id_key="id"
        )
        print(f"✅ Reader inizializzato con data_folder: {input_dist_path}")

        # 4. Inizializzazione Mapper
        mapper = MapperExtractor(
            mapping_spec=mapping,
            dst_schema=dst_schema,
            src_schema=src_schema
        )
        print(f"✅ Mapper inizializzato")

        # 5. Inizializzazione Writer
        print(f"🎯 Inizializzazione Writer: \n base_input: {base_input_root} \n base_output: {base_output_root}")
        writer = UnifiedWriter(
            output_format=output_format,
            base_input_path=base_input_root,
            base_output_path=base_output_root,
        )
        print(f"✅ Writer inizializzato con base_input: {base_input_root}, base_output: {base_output_root}")

        # 6. Costruzione Pipeline
        pipeline = [reader]
        pipeline.append(DocStats(output_folder=low_level_stats_path))
        pipeline.append(mapper)
        
        pipeline.append(writer)
        
        print(f"✅ Pipeline costruita con {len(pipeline)} stage")

        # 7. Configurazione ed Esecuzione
        n_cores = multiprocessing.cpu_count()
        n_workers = max(1, int(n_cores * 0.85))
        
        print(f"🚀 Avvio esecuzione con {n_workers} workers (tasks: {n_workers * 4})...")
        
        pipe = LocalPipelineExecutor(
            pipeline=pipeline,
            tasks=n_workers * 4,
            workers=n_workers,
            logging_dir=None
        )
        
        pipe.run()
        print("✅ Pipeline completata con successo.")

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"❌ Errore critico nel worker:\n{error_msg}")
        # Log più dettagliato
        with open("worker_error.log", "a") as f:
            f.write(f"\n--- {os.path.basename(__file__)} error at {__import__('datetime').datetime.now()} ---\n")
            f.write(f"Args ricevuti: {json.dumps(args, indent=2, default=str)}\n")
            f.write(error_msg)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            params = json.loads(sys.argv[1])
            run_mapping_pipeline(params)
        except Exception as e:
            print(f"❌ Errore nel parsing dei parametri JSON: {e}")
            sys.exit(1)
    else:
        print("❌ Nessun parametro ricevuto dal worker.")
        sys.exit(1)