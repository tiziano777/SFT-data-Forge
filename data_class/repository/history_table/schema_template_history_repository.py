# repository/schema_template_history_repository.py
import logging
from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.history_table.schema_template_history import SchemaTemplateHistory

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class SchemaTemplateHistoryRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "schema_template_history"

    def _prepare_params(self, entity: SchemaTemplateHistory, include_id: bool = False) -> Dict[str, Any]:
        """
        Prepara i parametri per il DB.
        Gestisce il campo 'schema' come dizionario nativo per Psycopg 3.
        """
        params = {
            "schema_template_id": entity.schema_template_id,
            "schema": entity.schema if isinstance(entity.schema, dict) else {},
            "version": entity.version,
            "modified_at": entity.modified_at
        }
        if include_id and entity.id:
            params["id"] = entity.id
        return params

    def insert(self, entity: SchemaTemplateHistory) -> Optional[SchemaTemplateHistory]:
        """Inserimento dello storico schema con Named Placeholders."""
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return SchemaTemplateHistory(**row) if row else None

    def get_by_schema_template_id(self, schema_template_id: int) -> List[SchemaTemplateHistory]:
        """Recupera la cronologia di un template specifico, dal più recente al più vecchio."""
        query = f"SELECT * FROM {self.table} WHERE schema_template_id = %s ORDER BY modified_at DESC"
        with self.db as db:
            rows = db.query(query, (schema_template_id,))
            return [SchemaTemplateHistory(**row) for row in rows]

    def get_all(self) -> List[SchemaTemplateHistory]:
        """Recupera l'intero storico degli schemi."""
        query = f"SELECT * FROM {self.table} ORDER BY modified_at DESC"
        with self.db as db:
            rows = db.query(query)
            return [SchemaTemplateHistory(**row) for row in rows]

    def get_by_id(self, id: int) -> Optional[SchemaTemplateHistory]:
        """Recupera una singola istanza storica."""
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return SchemaTemplateHistory(**row) if row else None