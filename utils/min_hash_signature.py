# Contenuto di una libreria esterna, es. external_utils.py
from datasketch import MinHash
from typing import Set, List, Union

def create_minhash_signature(set_of_items: Union[Set[str], List[str]], num_hashes: int = 128) -> List[int]:
    """
    Crea un vettore di hash (firma MinHash) per stimare la somiglianza tra set.
    
    :param set_of_items: Il set di elementi (es. token, shingle di un testo, feature).
    :param num_hashes: Il numero di funzioni hash da usare (dimensione del vettore firma).
    :return: Una lista di interi che rappresenta la firma MinHash.
    """
    # 1. Inizializza l'oggetto MinHash
    m = MinHash(num_perm=num_hashes)
    
    # 2. Aggiorna l'hash con ogni elemento nel set
    for item in set_of_items:
        # È cruciale che ogni item sia codificato in byte
        m.update(item.encode('utf8'))
        
    # 3. Restituisci il vettore hash
    # La .hashvalues rappresenta la "firma" del set
    return m.hashvalues.tolist()

# Esempio d'uso:
'''
# Dataset A: Alcuni token o "shingle" (sottostringhe)
set_A = {"streamlit", "python", "form", "metadata", "validation", "json", "schema"}

# Genera la firma per il Dataset A
minhash_A = create_minhash_signature(set_A)
# print(f"Firma MinHash A (primi 10 valori): {minhash_A[:10]}")

# Vantaggio della robustezza:
# Crea un Dataset B leggermente modificato
set_B = {"streamlit", "python", "form", "metadata", "validation", "pydantic", "schema"} # 'json' cambiato in 'pydantic'

minhash_B = create_minhash_signature(set_B)

# Per calcolare la somiglianza (necessita di riconvertire a oggetto MinHash)
from datasketch import MinHash

m_A = MinHash(num_perm=128)
m_A.hashvalues = minhash_A # Ricarica i valori della firma

m_B = MinHash(num_perm=128)
m_B.hashvalues = minhash_B # Ricarica i valori della firma

# Stima della somiglianza di Jaccard:
# similarity = (MinHash(num_perm=128).hashvalues).jaccard(m_B)
# print(f"Somiglianza stimata tra A e B: {similarity:.4f}") 
# Il valore sarà vicino a (6/7) ≈ 0.857, dimostrando la robustezza.
'''

