import os
import json
import traceback
import gzip
import pyarrow.parquet as pq
import numpy as np
from typing import List, Dict, Any, Tuple, Callable
from concurrent.futures import ProcessPoolExecutor, as_completed

from mappings.mapper import Mapper

def make_serializable(obj):
    """Converte ndarray e tipi numpy in tipi Python serializzabili in JSON."""
    if isinstance(obj, np.ndarray):
        return [make_serializable(x) for x in obj.tolist()]
    elif isinstance(obj, np.generic):  # np.int64, np.float32, ecc.
        return obj.item()
    elif isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(x) for x in obj]
    else:
        return obj

def truncate_strings(obj, max_len=1_000_000):
    """
    Ricorsiva: applica estrazione tag e troncamento su tutti i campi stringa,
    gestisce dict, list e valori primitivi, senza creare ridondanza di `value`.
    """
    if obj is None:
        return None
    elif isinstance(obj, str):
        return obj[:max_len]
    elif isinstance(obj, list):
        return [truncate_strings(x, max_len=max_len) for x in obj]
    elif isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            processed = truncate_strings(v, max_len=max_len)
            if isinstance(processed, dict):
                if "value" in processed and not isinstance(v, str):
                    for key, val in processed.items():
                        new_dict[key] = val
                else:
                    for key, val in processed.items():
                        new_dict[key] = val
            else:
                new_dict[k] = processed
        return new_dict
    else:
        return obj

def parse_input_path(input_path: str) -> List[str]:
    supported_extensions = ('.parquet', '.jsonl.gz', ".jsonl", ".json")
    files_to_process = []
    if os.path.isfile(input_path):
        if input_path.endswith(supported_extensions): 
            files_to_process.append(input_path)
    elif os.path.isdir(input_path):
        for root, _, files in os.walk(input_path):
            for filename in files:
                if filename.endswith(supported_extensions): 
                    files_to_process.append(os.path.join(root, filename))
    else: 
        print(f"Errore: Il percorso di input '{input_path}' non è valido.")
    return files_to_process

def process_file(file_path: str, mapper_mapping: Dict[str, Any], src_schema: Dict[str, Any], dst_schema: Dict[str, Any], output_path: str, file_index: int) -> Tuple[str, bool, int, Any]:
    """
    Processa un singolo file di dati applicando le stesse trasformazioni del sample reader.
    """
    mapper = Mapper(mapping_spec=mapper_mapping, src_schema=src_schema, dst_schema=dst_schema)

    output_filename = os.path.splitext(os.path.basename(file_path))[0]
    jsonl_gz_filepath = os.path.join(output_path, f"{output_filename}_mapped_{file_index}.jsonl.gz")
    mapped_samples = []
    processed_count = 0
    skipped_count = 0
    
    print(f"Processing file: {file_path} -> {jsonl_gz_filepath}")
    
    try:
        samples = []
        
        # CARICAMENTO DATI - Stesso metodo del sample reader
        if file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    samples = data
                elif isinstance(data, dict):
                    samples = [data]
                else:
                    print(f"Formato JSON non riconosciuto in '{file_path}'.")
                    return (os.path.basename(file_path), False, 0, "Formato JSON non riconosciuto")

        elif file_path.endswith('.jsonl'):
            with open(file_path, 'r', encoding='utf-8') as f:
                samples = [json.loads(line) for line in f.readlines()]
        
        elif file_path.endswith('.jsonl.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                samples = [json.loads(line) for line in f.readlines()]
        
        elif file_path.endswith('.parquet'):
            table = pq.read_table(file_path, columns=None)
            df = table.to_pandas()
            samples = df.to_dict('records')
        
        else:
            return (os.path.basename(file_path), False, 0, f"Estensione non supportata: {file_path}")

        # APPLICA LE STESSE TRASFORMAZIONI DEL SAMPLE READER
        print(f"📥 Campioni caricati: {len(samples)}")
        
        # 1. Rendili serializzabili
        samples = [make_serializable(s) for s in samples]
        
        # 2. Tronca le stringhe
        samples = [truncate_strings(s, max_len=1_000_000) for s in samples]
        
        print(f"🔄 Campioni normalizzati: {len(samples)}")

        # DEBUG: Controlla la struttura dopo la normalizzazione
        if samples and len(samples) > 0:
            first_sample = samples[0]
            print(f"🔍 Primo campione normalizzato - Campi: {list(first_sample.keys())}")
            if 'text' in first_sample:
                text_val = first_sample['text']
                print(f"   Campo 'text': tipo={type(text_val)}, valore={text_val}")
                if isinstance(text_val, list):
                    print(f"   ✅ 'text' è un array con {len(text_val)} elementi")

        # APPLICA IL MAPPING
        for i, sample in enumerate(samples):
            try:
                if i < 2:  # Debug per primi campioni
                    print(f"🎯 Applicando mapping al campione {i}: {list(sample.keys())}")
                
                mapped_sample, success, errors = mapper.apply_mapping(sample)
                
                if success and mapped_sample:
                    mapped_samples.append(mapped_sample)
                    processed_count += 1
                    if i < 2:
                        print(f"   ✅ Mapping riuscito per campione {i}")
                else:
                    print(f"❌ Errori di mapping nel file {file_path}, campione {i}: {errors}")
                    skipped_count += 1
                    
            except Exception as e:
                print(f"❌ Errore durante il mapping del campione {i} in {file_path}: {e}")
                skipped_count += 1
                continue

        print(f"📊 File {file_path}: {processed_count} campioni elaborati, {len(mapped_samples)} campioni mappati, {skipped_count} saltati")

        # SCRITTURA OUTPUT
        if mapped_samples:
            try:
                with gzip.open(jsonl_gz_filepath, 'wt', encoding='utf-8') as f:
                    for sample in mapped_samples:
                        f.write(json.dumps(sample, ensure_ascii=False) + '\n')
                print(f"💾 Scritti {len(mapped_samples)} campioni in {jsonl_gz_filepath}")
            except Exception as e:
                print(f"❌ Errore durante la scrittura del file {jsonl_gz_filepath}: {e}")
                return (os.path.basename(file_path), False, processed_count, e)
        else:
            print(f"⚠️ ATTENZIONE: Nessun campione mappato per il file {file_path}")
            try:
                with gzip.open(jsonl_gz_filepath, 'wt', encoding='utf-8') as f:
                    f.write("")
            except:
                pass

        return (os.path.basename(file_path), True, processed_count, None)
        
    except Exception as e:
        traceback.print_exc()
        print(f"❌ Errore durante l'elaborazione del file {file_path}: {e}")
        return (os.path.basename(file_path), False, processed_count, e)

def run_parallel_mapping(input_path: str, output_path: str, mapping: Dict[str, Any], src_schema: Dict[str, Any], dst_schema: Dict[str, Any], progress_callback: Callable[[float], None]) -> Dict[str, int]:
    files_to_process = parse_input_path(input_path)
    if not files_to_process:
        print("Nessun file da processare trovato")
        return {"total_files": 0, "successful_files": 0, "total_processed_samples": 0}
    
    os.makedirs(output_path, exist_ok=True)

    print(f"📁 File da processare: {len(files_to_process)}")
    print(f"📍 Output path: {output_path}")

    with ProcessPoolExecutor(max_workers=min(os.cpu_count()-1, len(files_to_process))) as executor:
        futures = {
            executor.submit(process_file, file, mapping, src_schema, dst_schema, output_path, idx): file
            for idx, file in enumerate(files_to_process)
        }
        total_files = len(files_to_process)
        successful_files = 0
        total_processed_samples = 0
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                filename, success, processed_count, error = result
                progress_callback((i + 1) / total_files)
                
                if success:
                    successful_files += 1
                    total_processed_samples += processed_count
                    print(f"✅ {filename}: {processed_count} campioni processati")
                else:
                    print(f"❌ {filename}: fallito - {error}")
            else:
                print(f"❌ Risultato vuoto per un file")
                
        print(f"🎯 Risultato finale: {successful_files}/{total_files} file successo, {total_processed_samples} campioni totali")
        
        return {
            "total_files": total_files,
            "successful_files": successful_files,
            "total_processed_samples": total_processed_samples,
        }

