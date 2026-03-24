# repository/vocabulary/vocab_task_repository.py
import logging
from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.vocabulary.vocab_task import VocabTask

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class VocabTaskRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "vocab_task"

    def _prepare_params(self, entity: VocabTask, include_id: bool = False) -> Dict[str, Any]:
        """Prepara i parametri mappando l'entità VocabTask."""
        params = {
            "code": entity.code,
            "description": entity.description
        }
        if include_id and entity.id:
            params["id"] = entity.id
        return params

    def insert(self, entity: VocabTask) -> Optional[VocabTask]:
        """Inserimento sicuro con Named Placeholders e mapping automatico."""
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return VocabTask(**row) if row else None

    def get_by_id(self, id: str) -> Optional[VocabTask]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return VocabTask(**row) if row else None

    def get_by_code(self, code: str) -> Optional[VocabTask]:
        query = f"SELECT * FROM {self.table} WHERE code = %s"
        with self.db as db:
            row = db.execute_and_return(query, (code,))
            return VocabTask(**row) if row else None

    def get_all(self) -> List[VocabTask]:
        query = f"SELECT * FROM {self.table} ORDER BY code ASC"
        with self.db as db:
            rows = db.query(query)
            return [VocabTask(**row) for row in rows]

    def update(self, entity: VocabTask) -> int:
        """Aggiorna il task usando il mapping nominativo."""
        if not entity.id:
            raise ValueError("ID mancante per l'aggiornamento del task")
            
        params = self._prepare_params(entity, include_id=True)
        set_clauses = ", ".join([f"{col} = %({col})s" for col in params.keys() if col != 'id'])
        
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def delete(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))