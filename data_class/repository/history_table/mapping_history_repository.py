# repository/mapping_history_repository.py
import logging
from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.history_table.mapping_history import MappingHistory

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")


class MappingHistoryRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "mapping_history"

    def _prepare_params(self, entity: MappingHistory, include_id: bool = False) -> Dict[str, Any]:
        """
        Prepara i parametri per il DB.
        MappingHistory è una tabella append-only: salviamo lo stato di un mapping in un dato momento.
        """
        params = {
            "mapping_id": entity.mapping_id,
            "schema_template_id": entity.schema_template_id,
            "mapping": entity.mapping if isinstance(entity.mapping, dict) else {},
            "version": entity.version,
            "modified_at": entity.modified_at
        }
        if include_id and entity.id:
            params["id"] = entity.id
        return params

    def insert(self, entity: MappingHistory) -> Optional[MappingHistory]:
        """Inserimento dello storico con Named Placeholders."""
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return MappingHistory(**row) if row else None

    def get_by_mapping_id(self, mapping_id: int) -> List[MappingHistory]:
        """Recupera la cronologia di un mapping specifico, ordinata per data."""
        query = f"SELECT * FROM {self.table} WHERE mapping_id = %s ORDER BY modified_at DESC"
        with self.db as db:
            rows = db.query(query, (mapping_id,))
            return [MappingHistory(**row) for row in rows]

    def get_all(self) -> List[MappingHistory]:
        """Recupera tutto lo storico presente nel sistema."""
        query = f"SELECT * FROM {self.table} ORDER BY modified_at DESC"
        with self.db as db:
            rows = db.query(query)
            return [MappingHistory(**row) for row in rows]

    def get_by_id(self, id: int) -> Optional[MappingHistory]:
        """Recupera una specifica istanza storica tramite ID."""
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return MappingHistory(**row) if row else None