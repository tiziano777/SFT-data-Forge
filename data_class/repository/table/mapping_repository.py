# repository/mapping_repository.py
import logging
from typing import List, Optional, Dict, Any
from data_class.entity.table.mapping import Mapping
from db.impl.postgres.postgres_db_manager import PostgresDBManager

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class MappingRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "mapping"

    def _prepare_params(self, entity: Mapping, include_id: bool = False) -> Dict[str, Any]:
        """
        Prepara i parametri per il DB.
        Psycopg 3 gestisce il campo 'mapping' (dict) come JSONB nativo.
        """
        params = {
            "distribution_id": entity.distribution_id,
            "schema_template_id": entity.schema_template_id,
            "mapping": entity.mapping if isinstance(entity.mapping, dict) else {},
            "version": entity.version,
            "issued": entity.issued,
            "modified": entity.modified
        }
        if include_id:
            params["id"] = entity.id
        return params

    def insert(self, entity: Mapping) -> Optional[Mapping]:
        """Inserimento order-agnostic con ritorno entità."""
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Mapping(**row) if row else None

    def upsert(self, entity: Mapping) -> Optional[Mapping]:
        """
        UPSERT SOTA: Gestisce il conflitto su (schema_template_id, distribution_id).
        Usa Named Placeholders per la massima chiarezza.
        """
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"""
            INSERT INTO {self.table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT (schema_template_id, distribution_id) 
            DO UPDATE SET 
                mapping = EXCLUDED.mapping,
                version = EXCLUDED.version,
                modified = EXCLUDED.modified
            RETURNING *
        """
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Mapping(**row) if row else None

    def get_by_id(self, id: str) -> Optional[Mapping]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return Mapping(**row) if row else None

    def get_by_distribution_id(self, distribution_id: str) -> List[Mapping]:
        query = f"SELECT * FROM {self.table} WHERE distribution_id = %s"
        with self.db as db:
            rows = db.query(query, (distribution_id,))
            return [Mapping(**row) for row in rows]

    def get_all(self) -> List[Mapping]:
        query = f"SELECT * FROM {self.table}"
        with self.db as db:
            rows = db.query(query)
            return [Mapping(**row) for row in rows]

    def update(self, entity: Mapping) -> int:
        """Aggiornamento sicuro tramite ID."""
        if not entity.id:
            raise ValueError("ID mancante per l'update del mapping")
            
        params = self._prepare_params(entity, include_id=True)
        set_clauses = ", ".join([f"{col} = %({col})s" for col in params.keys() if col != 'id'])
        
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def delete(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))