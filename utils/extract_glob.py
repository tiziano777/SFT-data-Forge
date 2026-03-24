# utils/extract_glob.py
import os
import pathlib
from collections import defaultdict
from typing import List, Dict, Set
import fnmatch

def generate_dataset_globs(root_path: str) -> List[str]:
    """
    Scansiona ricorsivamente la directory radice e genera un elenco di pattern glob 
    minimali che coprono tutti i file trovati, raggruppati per estensione e 
    percorso relativo.

    Questa funzione risolve il problema delle 'foglie' (cartelle con soli file)
    generando pattern del tipo: 'cartella/sottocartella/*.estensione'.

    Args:
        root_path (str): Il percorso assoluto o relativo della directory radice del dataset.

    Returns:
        List[str]: Un elenco di stringhe glob relative al root_path.
    """
    
    # 1. Normalizza il percorso radice per renderlo assoluto
    root_path = os.path.abspath(root_path)
    if not os.path.isdir(root_path):
        print(f"Errore: La directory radice '{root_path}' non esiste.")
        return []

    # Struttura per memorizzare le estensioni trovate per ogni percorso relativo.
    # Esempio: {'train': {'.png', '.jpg'}, 'validation/labels': {'.txt'}}
    extensions_by_dir: Dict[str, set] = defaultdict(set)
    
    print(f"--- Inizio scansione da: {root_path} ---")

    # 2. Traversata ricorsiva della directory (os.walk)
    for dirpath, dirnames, filenames in os.walk(root_path):
        if filenames:
            # 3. Calcola il percorso relativo: questo è fondamentale!
            # Rimuove il root_path dal percorso corrente per ottenere il prefisso del glob.
            rel_path = os.path.relpath(dirpath, root_path)
            
            # Se siamo nella root stessa, rel_path sarà '.'
            if rel_path == '.':
                rel_path = ""
            
            # 4. Estrai e aggrega le estensioni dei file
            for filename in filenames:
                # Usa pathlib per gestire correttamente le estensioni (anche .tar.gz)
                extension = pathlib.Path(filename).suffix
                
                # Se l'estensione è vuota (file senza estensione), usiamo un placeholder
                if not extension:
                    extension = "NO_EXT"
                
                extensions_by_dir[rel_path].add(extension)

    # 5. Genera i pattern glob finali
    final_globs: List[str] = []
    
    for rel_path, extensions in extensions_by_dir.items():
        for ext in extensions:
            # Rimuovi il punto iniziale dall'estensione se presente
            clean_ext = ext.lstrip('.')

            # Se era un file senza estensione, il pattern è semplicemente '*'
            if ext == "NO_EXT":
                 # Costruisce il glob: 'percorso/al/file/*'
                glob_pattern = os.path.join(rel_path, '*')
            else:
                # Costruisce il glob: 'percorso/al/file/*.estensione'
                glob_pattern = os.path.join(rel_path, f"*.{clean_ext}")
            
            # Normalizza il percorso per il glob (usa '/' su tutti i sistemi operativi per i globs)
            # e rimuove eventuali '/' iniziali se rel_path era vuoto
            final_globs.append(glob_pattern.replace(os.path.sep, '/'))

    # Filtra i duplicati e restituisce l'elenco
    return sorted(list(set(final_globs)))

def generate_filtered_globs(dataset_path: str) -> List[str]:
    """
    Genera e filtra i glob patterns per un dataset, escludendo:
    - Glob semplici "*" (poco informativi)
    - File/cartelle nascosti (iniziando con ".") eccetto *.md
    - File/cartelle che iniziano con "_"
    - Paths di cache di Hugging Face
    """
    all_globs = _generate_all_globs(dataset_path)
    filtered_globs = _apply_glob_filters(all_globs)
    return sorted(filtered_globs)

def _generate_all_globs(dataset_path: str) -> Set[str]:
    """Genera tutti i possibili glob patterns ricorsivamente"""
    globs = set()
    
    try:
        for root, dirs, files in os.walk(dataset_path):
            # Converti il percorso relativo in glob pattern
            rel_path = os.path.relpath(root, dataset_path)
            
            if rel_path == '.':
                # Aggiungi pattern per la root
                for file in files:
                    globs.add(f"*{os.path.splitext(file)[1]}" if os.path.splitext(file)[1] else file)
                for dir_name in dirs:
                    globs.add(f"{dir_name}/*")
            else:
                # Aggiungi pattern per sottocartelle
                glob_pattern = f"{rel_path}/*"
                globs.add(glob_pattern)
                
                # Aggiungi pattern specifici per estensioni nelle sottocartelle
                for file in files:
                    file_ext = os.path.splitext(file)[1]
                    if file_ext:
                        globs.add(f"{rel_path}/*{file_ext}")
    
    except Exception as e:
        print(f"Errore nella generazione globs: {e}")
        # Fallback: pattern generico
        globs.add("*")
    
    return globs

def _apply_glob_filters(globs: Set[str]) -> List[str]:
    """Applica i filtri per rimuovere glob non desiderati"""
    filtered = set()
    
    for glob_pattern in globs:
        # Escludi glob semplici "*"
        if glob_pattern == "*":
            continue
            
        # Escludi pattern che iniziano con "." (eccetto *.md)
        if _is_hidden_pattern(glob_pattern):
            continue
            
        # Escludi pattern che iniziano con "_"
        if _is_underscore_pattern(glob_pattern):
            continue
            
        # Escludi paths di cache di Hugging Face
        if _is_huggingface_cache(glob_pattern):
            continue
            
        # Pattern valido, aggiungi alla lista filtrata
        filtered.add(glob_pattern)
    
    return list(filtered)

def _is_hidden_pattern(glob_pattern: str) -> bool:
    """Verifica se il pattern si riferisce a file/cartelle nascosti"""
    parts = glob_pattern.split('/')
    
    for part in parts:
        # Escludi parti che iniziano con "." (eccetto per *.md)
        if part.startswith('.') and not part.endswith('.md') and '*' not in part:
            return True
            
        # Escludi pattern che corrispondono a file nascosti
        if part.startswith('.*') and not part == '*.md':
            return True
    
    return False

def _is_underscore_pattern(glob_pattern: str) -> bool:
    """Verifica se il pattern si riferisce a file/cartelle che iniziano con _"""
    parts = glob_pattern.split('/')
    
    for part in parts:
        if part.startswith('_') and '*' not in part:
            return True
        if part.startswith('_*'):
            return True
    
    return False

def _is_huggingface_cache(glob_pattern: str) -> bool:
    """Verifica se il pattern si riferisce a cache di Hugging Face"""
    cache_patterns = [
        '.cache/huggingface/*',
        '.cache/huggingface/**/*',
        '**/.cache/**/*',
        '**/*.lock',
        '**/*.metadata'
    ]
    
    glob_lower = glob_pattern.lower()
    
    # Controlla pattern espliciti di cache
    if any(fnmatch.fnmatch(glob_lower, pattern) for pattern in cache_patterns):
        return True
        
    # Controlla se contiene ".cache" in qualsiasi parte del path
    if '.cache' in glob_lower:
        return True
        
    # Controlla file di lock e metadata
    if glob_lower.endswith('.lock') or glob_lower.endswith('.metadata'):
        return True
    
    return False

