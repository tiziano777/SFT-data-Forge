import duckdb

from typing import List, Tuple, Any, Optional
from db.abstracts.RelationalDBManager import RelationalDBManager

class DuckDBManager(RelationalDBManager):
    """
    Gestore del database specifico per DuckDB.
    """
    
    def __init__(self, database_path: str = ':memory:'):
        """
        Inizializza il gestore di DuckDB.
        :param database_path: Percorso al file DuckDB. ':memory:' per un database in memoria.
        """
        self.database_path = database_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        
    def __enter__(self):
        """Apre la connessione DuckDB e inizia una transazione esplicita."""
        self.conn = duckdb.connect(database=self.database_path, read_only=False)
        # Avvia esplicitamente la transazione. Tutte le operazioni seguenti saranno parte di essa.
        self._execute("BEGIN TRANSACTION") 
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Gestisce commit/rollback e chiude la connessione."""
        if self.conn:
            if exc_type is None:
                # Nessuna eccezione: esegue il commit
                try:
                    self.conn.commit()
                except Exception as e:
                    # Gestisce il caso limite in cui il commit fallisce
                    print(f"Attenzione: Errore durante il commit: {e}")
                    self.conn.rollback() # Tenta un rollback in caso di fallimento commit
            else:
                # Eccezione: esegue il rollback. 
                # Se la transazione è stata avviata con BEGIN, questo funzionerà.
                self.conn.rollback()
                
            self.conn.close()
        self.conn = None
    
    def _execute(self, query: str, params: Optional[Tuple] = None) -> Any:
        """
        Esegue la query sul cursore/connessione DuckDB.
        """
        if not self.conn:
            raise RuntimeError("Connessione DuckDB non attiva. Utilizzare 'with DuckDBManager(...):'")
        
        # DuckDB usa il metodo .execute() direttamente sulla connessione
        return self.conn.execute(query, params)

    # --- Implementazione dei Metodi DDL e CRUD ---

    def create_schema_from_file(self, ddl_file_path: str):
        """
        Carica ed esegue tutte le query DDL da un file.
        Nota: DuckDB non supporta `CREATE SCHEMA` nel senso di Postgres, ma le DDL adattate funzionano.
        """
        try:
            with open(ddl_file_path, 'r') as f:
                ddl_script = f.read()
            
            # DuckDB accetta script multi-statement
            self._execute(ddl_script)
            print(f"Schema creato con successo dal file: {ddl_file_path}")
        except Exception as e:
            print(f"Errore durante l'esecuzione del DDL: {e}")
            raise

    def insert(self, table: str, columns: List[str], values: List[Any]) -> Optional[int]:
        """
        Esegue un'operazione INSERT.
        DuckDB supporta i parametri posizionali '?' (come la maggior parte dei DB API).
        Ritorna sempre None perché DuckDB non ha una funzione standard `lastrowid`
        per i UUID o una sequenza generica (se non usata esplicitamente).
        """
        col_names = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])
        query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        
        # DuckDB non ha un cursore separato, usiamo .execute()
        self._execute(query, tuple(values)) 
        
        # Se avessimo una colonna SERIAL, potremmo usare `SELECT last_insert_rowid()` 
        # ma per UUID non è applicabile.
        return None 

    def select(self, table: str, columns: List[str] = ['*'], where: Optional[str] = None, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Esegue un'operazione SELECT e ritorna una lista di tuple.
        """
        col_names = ', '.join(columns)
        query = f"SELECT {col_names} FROM {table}"
        if where:
            query += f" WHERE {where}"
            
        cursor = self._execute(query, params)
        # DuckDB usa .fetchall() sul cursore risultante dall'execute
        return cursor.fetchall() 

    def update(self, table: str, set_clauses: List[str], where: str, params: Tuple) -> int:
        """
        Esegue un'operazione UPDATE e ritorna il numero di righe modificate.
        """
        set_str = ', '.join(set_clauses)
        query = f"UPDATE {table} SET {set_str} WHERE {where}"
        
        cursor = self._execute(query, params)
        # In DuckDB, l'informazione sul conteggio delle righe è sul cursore
        return cursor.rowcount

    def delete(self, table: str, where: str, params: Optional[Tuple] = None) -> int:
        """
        Esegue un'operazione DELETE e ritorna il numero di righe eliminate.
        """
        query = f"DELETE FROM {table} WHERE {where}"
        
        cursor = self._execute(query, params)
        return cursor.rowcount
