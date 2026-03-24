import logging
import os
from typing import Optional, Dict, Any, Tuple
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.table.recipe import Recipe

logger = logging.getLogger(__name__)
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class RecipeRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = f"{POSTGRES_DB_SCHEMA}.recipe"

    def _prepare_params(self, entity: Recipe, include_id: bool = False) -> Dict[str, Any]:
        params = {
            "name": entity.name,
            "description": entity.description,
            "scope": entity.scope,
            "version": entity.version,
            "tags": entity.tags if isinstance(entity.tags, list) else [],
            "tasks": entity.tasks if isinstance(entity.tasks, list) else [],
            "issued": entity.issued,
            "modified": entity.modified,
            "derived_from": entity.derived_from
        }
        if include_id:
            params["id"] = entity.id
        return params

    def insert(self, entity: Recipe) -> Optional[Recipe]:
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        try:
            with self.db as db:
                row = db.execute_and_return(query, params)
                return Recipe(**row) if row else None
        except Exception as e:
            logger.error(f"Errore inserimento Recipe: {e}")
            raise

    def get_by_id(self, id: str) -> Optional[Recipe]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return Recipe(**row) if row else None
    
    def get_by_name(self, name: str) -> Optional[Recipe]:
        query = f"SELECT * FROM {self.table} WHERE name = %s"
        with self.db as db:
            row = db.execute_and_return(query, (name,))
            return Recipe(**row) if row else None

    def get_all(self) -> list[Recipe]:
        query = f"SELECT * FROM {self.table}"
        with self.db as db:
            rows = db.query(query)
            return [Recipe(**row) for row in rows] if rows else []

    def update(self, entity: Recipe) -> int:
        if not entity.id: raise ValueError("ID mancante")
        params = self._prepare_params(entity, include_id=True)
        set_clauses = [f"{c} = %({c})s" for c in params.keys() if c != "id"]
        query = f"UPDATE {self.table} SET {', '.join(set_clauses)} WHERE id = %(id)s"
        with self.db as db:
            return db.execute_command(query, params)
    
    def delete(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))

    def is_valid(self, entity: Recipe) -> Tuple[bool, Optional[str]]:
        """
        Verifica se l'entità Recipe è valida effettuando un test atomico.
        """
        try:
            params = self._prepare_params(entity)
            cols = ", ".join(params.keys())
            placeholders = ", ".join([f"%({c})s" for c in params.keys()])
            
            query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING id"
            
            # Se self.db as db apre già una transazione, non toccare autocommit
            with self.db as db:
                try:
                    # Esegui l'insert nel blocco protetto
                    db.execute_and_return(query, params)
                    
                    # Forza SEMPRE il rollback per la validazione
                    db.conn.rollback() 
                    return True, None
                    
                except Exception as e:
                    # In caso di errore di vincolo (Unique, Not Null, etc.)
                    db.conn.rollback()
                    error_msg = str(e).split('\n')[0] # Prende solo la prima riga dell'errore DB
                    logger.debug(f"Validazione fallita: {error_msg}")
                    return False, error_msg
                    
        except Exception as e:
            logger.error(f"Errore durante la validazione: {e}")
            return False, f"Errore critico di validazione: {str(e)}"
            