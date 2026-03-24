import re
from copy import deepcopy
from typing import Any, List, Dict

# NOTA IMPORTANTE sulla firma delle funzioni:
# Per mantenere la coerenza con la logica in _process_operation, dove 
# operation = [funzione_name, arg1, arg2, ...], tutte le funzioni devono accettare
# come primo argomento il nome della funzione (func_name: str).
# Gli argomenti successivi sono specifici per ogni funzione.
# operation = [funzione_name, arg1, arg2, ...]
# result: List[Any] = funzione_name(arg1, arg2, ...)

# ======================================================================================
# ELENCO DELLE FUNZIONI DI TRASFORMAZIONE DISPONIBILI
# ======================================================================================

TRANSFORM_FUNCTIONS = [
    "set_fixed_value",
    "concat",
    "map_enum",
    "remove_strings",
    "remove_regex_strings",
    "remove_prefix",
    "extract_tag_content",
    "remove_tag_content",
]

# ======================================================================================
# FUNZIONI DI TRASFORMAZIONE PER IL MAPPER
# ======================================================================================

def set_fixed_value(func_name: str, arg: Any) -> List[Any]:
    """Restituisce sempre una lista con il valore costante `arg`."""
    return [deepcopy(arg)]

def concat(func_name: str, *args: Any) -> List[str]:
    """
    Concatena argomenti eterogenei in un'unica stringa, separata da spazi.
    Ignora valori None e coercizza tutto in str.
    """
    string_args = [str(arg) for arg in args if arg not in (None, "")]
    return [" ".join(string_args)]

def map_enum(func_name: str, text_key, mapping_dict: Dict[str, Any]) -> List[Any]:
    """
    Mappa i valori estratti da un path sorgente (chiave schema sorgente) a un nuovo insieme di valori,
    secondo una mappatura discreta.
    Se `text` è un singolo valore, viene convertito in lista.
    Utile per mappare i nostri enum [USER/ASSITANT], oppure chat types.
    """
    if not isinstance(text_key, list):
        text_key = [text_key]

    mapped_list = [
        mapping_dict.get(value) for value in text_key
    ]
    return mapped_list

def remove_strings(func_name: str, text: str, strings_to_remove: List[str]) -> List[str]:
    """
    Rimuove tutte le occorrenze delle stringhe specificate in `strings_to_remove`
    dalla stringa di input `text`.
    """
    if not text or not strings_to_remove:
        return [text]

    for s in strings_to_remove:
        text = text.replace(s, "")
    
    # Pulizia finale per rimuovere spazi multipli
    cleaned_text = " ".join(text.split())
    return [cleaned_text]

def remove_regex_strings(func_name: str, text: str, regex_patterns: List[str]) -> List[str]:
    """
    Rimuove tutte le occorrenze dei pattern RegEx specificati da una stringa.
    """
    if not text or not regex_patterns:
        return [text]

    for pattern_str in regex_patterns:
        text = re.sub(pattern_str, "", text, flags=re.DOTALL | re.IGNORECASE)

    return [" ".join(text.split())]

def remove_prefix(func_name: str, prefix: str, text: str) -> List[str]:
    """
    Rimuove un prefisso specifico da una stringa, se presente.
    """
    idx = text.find(prefix)
    if idx != -1:
        text = text[idx + len(prefix):].lstrip()
    return [text]

def extract_tag_content(func_name: str, tag_name: str, text: str) -> List[Any]:
    """
    Estrae il contenuto del PRIMO tag XML/HTML-like specificato da 'tag_name' 
    da una stringa, restituendolo formattato come [contenuto] all'interno di una lista.
    """
    # 1. Pattern di Estrazione usando il tag_name parametrico
    #    Cattura non avida (.*?) e re.escape(tag_name) per robustezza.
    pattern = re.compile(
        # Tag di apertura | Gruppo di cattura | Tag di chiusura
        rf"<\s*{re.escape(tag_name)}\s*>(.*?)</\s*{re.escape(tag_name)}\s*>",
        re.DOTALL | re.IGNORECASE,
    )
    # 2. Ricerca 
    match = pattern.search(text)
    # 3. Preparazione del Risultato
    if match:
        # Estraiamo il contenuto (Gruppo 1) e lo formattiamo come [contenuto]
        content = match.group(1).strip()
        formatted_output = f"[{content}]"
        return [formatted_output]
    else:
        return [None]

def remove_tag_content(func_name: str, tag_name: str, text: str) -> List[str]:
    """
    Rimuove il tag XML/HTML-like specificato (e tutto il suo contenuto) 
    dalla stringa di input 'text'. Esegue la sostituzione in modo ricorsivo
    per gestire l'annidamento, e restituisce il risultato in una lista singola.
    """
    # 1. Pattern per catturare l'intero blocco tag + contenuto
    pattern = re.compile(
        rf"<\s*{re.escape(tag_name)}\s*>.*?</\s*{re.escape(tag_name)}\s*>",
        re.DOTALL | re.IGNORECASE,
    )

    # 2. Rimozione Ricorsiva
    # Continuiamo a sostituire finché il pattern trova corrispondenze. 
    # Questo è ESSENZIALE per eliminare i tag annidati dello stesso tipo, 
    # ad es. <div> <div> contenuto </div> </div>
    while pattern.search(text):
        # Sostituiamo l'intero blocco tag+contenuto con uno spazio.
        text = pattern.sub(" ", text)
        
    # 3. Pulizia Finale e Formattazione Output
    final_content = " ".join(text.split())

    return [final_content]

     

