from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Dict, Union

class RelationalDBManager(ABC):
    """
    Interfaccia moderna per Database Relazionali.
    Punti chiave:
    - Parametri flessibili (posizionali o nominati).
    - Risultati sempre mappati come dizionari per indipendenza dall'ordine delle colonne.
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def query(self, sql: str, params: Union[Tuple, Dict, None] = None) -> List[Dict]:
        """Esegue SELECT e restituisce lista di dizionari."""
        pass

    @abstractmethod
    def execute_command(self, sql: str, params: Union[Tuple, Dict, None] = None) -> int:
        """Esegue UPDATE/DELETE/DDL e restituisce il numero di righe colpite."""
        pass

    @abstractmethod
    def execute_and_return(self, sql: str, params: Union[Tuple, Dict, None] = None) -> Optional[Dict]:
        """Esegue query con clausola RETURNING e restituisce la riga come dizionario."""
        pass

