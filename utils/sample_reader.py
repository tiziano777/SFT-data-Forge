import os
import json
import gzip
import pandas as pd
import pyarrow.parquet as pq
import numpy as np

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
    
    Args:
        obj: dict, list o valore primitivo
        max_len: lunghezza massima dei testi
    
    Returns:
        dict/list/valore troncato con tag estratti allo stesso livello
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
                # Merge dei tag estratti con eventuale value troncato
                if "value" in processed and not isinstance(v, str):
                    # value non originale → solo tag
                    for key, val in processed.items():
                        new_dict[key] = val
                else:
                    for key, val in processed.items():
                        new_dict[key] = val
            else:
                new_dict[k] = processed
        return new_dict

    else:
        # int, float, bool, None → ritorna così com'è
        return obj

def load_dataset_samples(data_folder, k=50, max_len=1_000_000):
    """
    Cerca un file di dati supportato nella cartella specificata e ne estrae un campione JSON-serializzabile.
    
    Args:
        data_folder (str): Il percorso della sottocartella 'data'.
        k (int): Il numero di campioni da estrarre.

    Returns:
        list: Una lista di dizionari JSON-safe che rappresentano i campioni,
              o None se non vengono trovati file.
    """
    if not os.path.isdir(data_folder):
        print(f"La cartella dei dati '{data_folder}' non esiste.")
        return None

    supported_extensions = ['json', '.jsonl', '.csv', '.gz', '.parquet', '.jsonl.gz', '.tsv', '.tsv.gz', '.warc', '.warc.gz']
    data_files = [f for f in os.listdir(data_folder) if any(f.endswith(ext) for ext in supported_extensions)]
    
    if not data_files:
        print(f"Nessun file supportato trovato in '{data_folder}'.")
        return []
        
    file_path = os.path.join(data_folder, data_files[0])
    
    try:
        if file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    samples = data[:k]
                elif isinstance(data, dict):
                    samples = [data]  # Un singolo oggetto JSON
                else:
                    print(f"Formato JSON non riconosciuto, forse JSONL... in '{file_path}'.")
                    return None

        elif file_path.endswith('.jsonl'):
            with open(file_path, 'r', encoding='utf-8') as f:
                samples = [json.loads(line) for line in f.readlines()[:k]]
        
        elif file_path.endswith('.jsonl.gz') or file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                samples = [json.loads(line) for line in f.readlines()[:k]]
        
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path, nrows=k)
            samples = df.to_dict('records')
        
        elif file_path.endswith('.parquet'):
            table = pq.read_table(file_path, columns=None)
            df = table.to_pandas().head(k)
            samples = df.to_dict('records')
        
        elif file_path.endswith('.tsv'):
            df = pd.read_csv(file_path, sep='\t', nrows=k)
            samples = df.to_dict('records')
        
        elif file_path.endswith('.tsv.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                df = pd.read_csv(f, sep='\t', nrows=k)
                samples = df.to_dict('records')
        
        elif file_path.endswith('.warc') or file_path.endswith('.warc.gz'):
            try:
                from warcio.archiveiterator import ArchiveIterator
            except ImportError:
                print("Il pacchetto 'warcio' non è installato. Installa con 'pip install warcio'.")
                return None

            samples = []
            open_func = gzip.open if file_path.endswith('.gz') else open
            with open_func(file_path, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    if record.rec_type == 'response':
                        payload = record.content_stream().read().decode('utf-8', errors='ignore')
                        samples.append({
                            'url': record.rec_headers.get_header('WARC-Target-URI'),
                            'date': record.rec_headers.get_header('WARC-Date'),
                            'content': payload
                        })
                        if len(samples) >= k:
                            break
        
        else:
            return None

        #print(f"PRE Campioni caricati da {file_path}: {samples}")

        # 🔑 Normalizza i campioni per renderli JSON-serializzabili
        samples = [make_serializable(s) for s in samples]
        print(f"Campioni caricati da {file_path}") # : {samples}
        samples = [truncate_strings(s, max_len=max_len) for s in samples]  # tronca i testi lunghi

        #print(f"POST Campioni caricati da {file_path}: {samples}")

        return samples

    except Exception as e:
        print(f"Errore nel caricamento del campione da {file_path}: {e}")
        return None
