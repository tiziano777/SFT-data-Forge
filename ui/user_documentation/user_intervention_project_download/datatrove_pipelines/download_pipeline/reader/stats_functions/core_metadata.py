# stats_functions.core_metadata.py
import hashlib
from typing import Dict, Any
import logging
logger = logging.getLogger(__name__)

import fasttext
import os
MODEL_PATH = "datatrove_pipelines/processed_pipeline/reader/stats_functions/models/lid.176.bin"
_FT_MODEL = None

def _get_ft_model():
    global _FT_MODEL
    if _FT_MODEL is None:
        if not os.path.exists(MODEL_PATH):
            logger.error(f"Modello fastText non trovato in {MODEL_PATH}")
            raise FileNotFoundError(f"Modello fastText non trovato in {MODEL_PATH}")
        _FT_MODEL = fasttext.load_model(MODEL_PATH)
    return _FT_MODEL

# AUX FUNCTIONS
def _extract_text_from_data(data_dict: dict) -> str:
    """
    Estrae il testo da un dizionario in modo deterministico.
    Ordina le chiavi alfanumericamente per garantire che l'hash
    del risultato sia consistente indipendentemente dall'ordine dei campi.
    """
    if not data_dict:
        return ""
    
    # 1. Otteniamo le chiavi e ordiniamole alfanumericamente
    sorted_keys = sorted(data_dict.keys())
    
    parts = []
    for key in sorted_keys:
        value = data_dict[key]
        # 2. Saltiamo i valori None per evitare variazioni nell'hash
        if value is None:
            continue
            
        # 3. Convertiamo in stringa. 
        # Opzionale: puoi usare str(value).strip() se vuoi ignorare spazi extra
        parts.append(str(value))
    
    # e per coerenza nell'unione dei testi.
    return "".join(parts)
     
def _compute_id_hash(text: str) -> str:
        """Calcola l'hash SHA256 del testo per generare l'ID."""
        if not text:
            raise ValueError("Il testo per il calcolo dell'hash è vuoto.")
        hash_obj = hashlib.sha256(text.encode('utf-8'))
        return hash_obj.hexdigest()

def _extract_heuristic_text_for_lang(data_dict: dict) -> str:
    """
    Estrae un campione di testo pulito per il language detection.
    Cerca la prima chiave che ha un valore > 64 caratteri e almeno uno spazio.
    """
    if not data_dict:
        return ""
    
    for value in data_dict.values():
        if value and isinstance(value, str):
            val_strip = value.strip()
            # Euristica: lunghezza > 64 e presenza di almeno uno spazio (indice di frase)
            if len(val_strip) > 64 and " " in val_strip:
                return val_strip
    
    # Fallback: se nessuna chiave soddisfa i requisiti, concateniamo tutto come prima
    return " ".join([str(v) for v in data_dict.values() if v])

def _detect_lang(data_dict: dict, available_langs: list) -> str:
    """Rileva la lingua con euristica sulla selezione del testo."""
    
    # 1. Cortocircuito: se c'è una sola lingua, usiamola senza dubbi
    if len(available_langs) == 1:
        return available_langs[0]
    
    # 2. Selezione euristica del testo per FastText
    heuristic_text = _extract_heuristic_text_for_lang(data_dict)
    
    # Se il testo è comunque troppo povero, fallback sulla prima disponibile
    if not heuristic_text or len(heuristic_text.strip()) < 10:
        return available_langs[0] if available_langs else "un"

    try:
        model = _get_ft_model()
        # Pulizia per FastText (limita a 500 caratteri del blocco scelto)
        clean_sample = heuristic_text.replace("\n", " ").strip()[:500]
        
        labels, probabilities = model.predict(clean_sample, k=1)
        detected_lang = labels[0].replace("__label__", "")
        
        # Se la lingua rilevata è tra quelle ammesse, ottimo
        if detected_lang in available_langs:
            return detected_lang
        
        # Analisi Critica: Se il modello rileva una lingua fuori set (es. 'it' invece di 'en'),
        # in un dataset curato è molto probabile un errore di rilevazione su testo tecnico.
        # Restituiamo la prima lingua disponibile invece di 'un'.
        if available_langs:
            return available_langs[0]
            
        return 'un'
            
    except Exception as e:
        logger.error(f"⚠️ Errore durante detection: {e}")
        return available_langs[0] if available_langs else "un"
     
def _clean_data_dict(original_data: dict) -> dict:
        """Pulisce il dizionario dati rimuovendo None e valori problematici."""
        cleaned = {}
        for key, value in original_data.items():
            if value is None:
                continue
                
            # Se il valore è una stringa, puliscila
            if isinstance(value, str):
                cleaned_value = value.strip()
                if cleaned_value:  # Solo se non è vuota dopo strip
                    cleaned[key] = cleaned_value
            else:
                cleaned[key] = value
                
        return cleaned

# Main function to be used as default adapter
     
def hf_default_adapter_aux(data: dict, path: str, id_in_file: int | str, 
                       default_metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Versione ottimizzata con euristica per language detection e mantenimento subpath.
    """
    try:
        # 1. Preparazione dati e ID (usa tutto il contenuto)
        cleaned_data = _clean_data_dict(data)
        text_content = _extract_text_from_data(cleaned_data)
        document_id = _compute_id_hash(text_content)

        # 2. Gestione metadati e lingue
        default_meta = default_metadata or {}
        available_langs = default_meta.get("_available_languages", [])

        # 3. Language Detection Euristico
        iso_lang = _detect_lang(cleaned_data, available_langs)
        
        # 4. Gestione Path
        file_name = path.split('/')[-1]
        path_parts = path.split('/')
        subpath = '/'.join(path_parts) if len(path_parts) > 1 else ''

        result = {
            'id': document_id,
            'text': text_content,
            'metadata': {
                'data': cleaned_data,
                '_dataset_name': default_meta.get("_dataset_name", "unknown"),
                '_filename': file_name,
                '_dataset_path': default_meta.get("_dataset_path", ""),
                '_subpath': subpath,
                '_id_hash': document_id,
                '_lang': iso_lang,
            }
        }

        return result
        
    except Exception as e:
        logger.error(f"❌ Errore nell'adapter per file {path}: {e}")
        raise e
    


