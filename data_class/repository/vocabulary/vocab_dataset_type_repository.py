# repository/vocab_distribution_split_repository.py
import logging
from typing import List, Optional, Dict, Any
from data_class.entity.vocabulary.vocab_dataset_type import VocabDatasetType
from db.impl.postgres.postgres_db_manager import PostgresDBManager

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class VocabDatasetTypeRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "vocab_dataset_type"

    def _prepare_params(self, entity: VocabDatasetType, include_id: bool = False) -> Dict[str, Any]:
        """Prepara i parametri per la tabella vocab_dataset_type."""
        params = {
            "code": entity.code,
            "description": entity.description
        }
        if include_id and entity.id:
            params["id"] = entity.id
        return params

    def insert(self, entity: VocabDatasetType) -> Optional[VocabDatasetType]:
        """Inserimento sicuro con Named Placeholders."""
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return VocabDatasetType(**row) if row else None

    def get_by_id(self, id: str) -> Optional[VocabDatasetType]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return VocabDatasetType(**row) if row else None

    def get_by_code(self, code: str) -> Optional[VocabDatasetType]:
        query = f"SELECT * FROM {self.table} WHERE code = %s"
        with self.db as db:
            row = db.execute_and_return(query, (code,))
            return VocabDatasetType(**row) if row else None

    def get_all(self) -> List[VocabDatasetType]:
        query = f"SELECT * FROM {self.table} ORDER BY code ASC"
        with self.db as db:
            rows = db.query(query)
            return [VocabDatasetType(**row) for row in rows]

    def update(self, entity: VocabDatasetType) -> int:
        """Aggiornamento sicuro tramite ID."""
        if not entity.id:
            raise ValueError("ID richiesto per l'aggiornamento del tipo di dataset")
            
        params = self._prepare_params(entity, include_id=True)
        set_clauses = ", ".join([f"{col} = %({col})s" for col in params.keys() if col != 'id'])
        
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def delete(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))