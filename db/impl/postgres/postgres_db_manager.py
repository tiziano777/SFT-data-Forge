import logging
import psycopg
import os

from typing import List, Tuple, Optional, Dict, Any, Union
from psycopg.rows import dict_row
from db.abstracts.RelationalDBManager import RelationalDBManager

logger = logging.getLogger(__name__)

class PostgresDBManager(RelationalDBManager):
    """
    Implementazione PostgreSQL basata su Psycopg 3.
    Include utility per il caricamento schema e gestione tabelle/schemi.
    """

    def __init__(self, dbname, user, password, host='localhost', port='5432', connect_timeout=5, gssencmode='disable'):
            # Rimosse le virgole interne che creavano una tupla e aggiunti spazi tra i parametri
            self.conn_info = (
                f"dbname={dbname} user={user} password={password} "
                f"host={host} port={port} connect_timeout={connect_timeout} "
                f"gssencmode={gssencmode} sslmode=prefer target_session_attrs=read-write"
            )
            self.conn: Optional[psycopg.Connection] = None
            self.target_schema = os.getenv('DB_SCHEMA')

    def connect(self):
        if self.conn is None or self.conn.closed:
            try:
                self.conn = psycopg.connect(self.conn_info, autocommit=False)
                with self.conn.cursor() as cur:
                    cur.execute(f"SET search_path TO {self.target_schema}, public;")
                # Dumper JSONB solo per dict
                self.conn.adapters.register_dumper(dict, psycopg.types.json.JsonbDumper)
            except psycopg.Error as e:
                logger.error(f"Errore critico connessione Postgres: {e}")
                raise

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                if exc_type is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()
            finally:
                self.close()

    def _execute(self, sql: str, params: Union[Tuple, Dict, Any, None] = None):
        if not self.conn:
            self.connect()
        cur = self.conn.cursor(row_factory=dict_row)
        try:
            cur.execute(sql, params)
            return cur
        except psycopg.Error as e:
            logger.error(f"Database Error:\nQuery: {sql}\nParams: {params}\nError: {e}")
            raise

    # --- Utility per Loader e Schema Management ---

    def drop_schema(self, schema_name: str, cascade: bool = True):
        """Elimina uno schema se esiste."""
        cascade_str = "CASCADE" if cascade else ""
        sql = f"DROP SCHEMA IF EXISTS {schema_name} {cascade_str}"
        self.execute_command(sql)
        logger.info(f"Schema {schema_name} eliminato correttamente.")

    def create_schema_from_file(self, ddl_file_path: str):
        """Legge un file SQL ed esegue lo script per inizializzare il DB."""
        if not os.path.exists(ddl_file_path):
            raise FileNotFoundError(f"File DDL non trovato: {ddl_file_path}")
            
        with open(ddl_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Usiamo il metodo interno per garantire la gestione della connessione
        if not self.conn:
            self.connect()
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql_script)
                self.conn.commit()
                logger.info(f"Schema caricato con successo da {ddl_file_path}")
        except psycopg.Error as e:
            self.conn.rollback()
            logger.error(f"Errore durante l'esecuzione dello script DDL: {e}")
            raise

    # --- Operazioni CRUD Generiche ---

    def select(self, table: str, columns: List[str] = None, where: str = None, params: tuple = None) -> List[Dict]:
        cols = ", ".join(columns) if columns else "*"
        sql = f"SELECT {cols} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return self.query(sql, params)

    def insert(self, table: str, columns: List[str], values: List[Any], returning: str = None) -> Optional[Dict]:
        cols_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(values))
        sql = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"
        if returning:
            sql += f" RETURNING {returning}"
            return self.execute_and_return(sql, tuple(values))
        self.execute_command(sql, tuple(values))
        return None

    def update(self, table: str, set_clauses: List[str], where: str, params: tuple) -> int:
        set_str = ", ".join(set_clauses)
        sql = f"UPDATE {table} SET {set_str} WHERE {where}"
        return self.execute_command(sql, params)

    def delete(self, table: str, where: str, params: tuple) -> int:
        sql = f"DELETE FROM {table} WHERE {where}"
        return self.execute_command(sql, params)

    # --- Metodi Astratti ---

    def query(self, sql: str, params: Union[Tuple, Dict, Any, None] = None) -> List[Dict]:
        with self._execute(sql, params) as cur:
            return cur.fetchall()

    def execute_and_return(self, sql: str, params: Union[Tuple, Dict, Any, None] = None) -> Optional[Dict]:
        with self._execute(sql, params) as cur:
            return cur.fetchone()

    def execute_command(self, sql: str, params: Union[Tuple, Dict, Any, None] = None) -> int:
        with self._execute(sql, params) as cur:
            return cur.rowcount


