# repository/system_prompt_repository.py
import logging
from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.table.system_prompt import SystemPrompt

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class SystemPromptRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "system_prompt"

    def _prepare_params(self, entity: SystemPrompt, include_id: bool = False) -> Dict[str, Any]:
        """
        Prepara i parametri per il DB.
        Sfrutta i Named Placeholders per evitare errori nell'ordine dei campi.
        """
        params = {
            "name": entity.name,
            "description": entity.description,
            "prompt": entity.prompt,
            "_lang": entity._lang,
            "length": entity.length if entity.length is not None else len(entity.prompt or ""),
            "derived_from": entity.derived_from,
            "quality_score": entity.quality_score,
            "deleted": entity.deleted,
            "version": entity.version,
            "issued": entity.issued,
            "modified": entity.modified
        }
        if include_id:
            params["id"] = entity.id
        return params

    def insert(self, entity: SystemPrompt) -> Optional[SystemPrompt]:
        """Inserimento sicuro con ritorno dell'entità mappata automaticamente."""
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return SystemPrompt(**row) if row else None

    def update(self, entity: SystemPrompt) -> int:
        """Aggiornamento tramite ID con named placeholders."""
        if not entity.id:
            raise ValueError("ID mancante per l'aggiornamento del system prompt")
            
        params = self._prepare_params(entity, include_id=True)
        set_clauses = ", ".join([f"{col} = %({col})s" for col in params.keys() if col != 'id'])
        
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def get_by_id(self, id: int) -> Optional[SystemPrompt]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return SystemPrompt(**row) if row else None

    def get_by_name(self, name: str) -> Optional[SystemPrompt]:
        query = f"SELECT * FROM {self.table} WHERE name = %s"
        with self.db as db:
            row = db.execute_and_return(query, (name,))
            return SystemPrompt(**row) if row else None

    def get_all(self) -> List[SystemPrompt]:
        query = f"SELECT * FROM {self.table} WHERE deleted = FALSE ORDER BY name ASC"
        with self.db as db:
            rows = db.query(query)
            return [SystemPrompt(**row) for row in rows]
    
    def get_all_including_deleted(self) -> List[SystemPrompt]:
        query = f"SELECT * FROM {self.table} ORDER BY name ASC"
        with self.db as db:
            rows = db.query(query)
            return [SystemPrompt(**row) for row in rows]
    
    def get_all_deleted(self) -> List[SystemPrompt]:
        query = f"SELECT * FROM {self.table} WHERE deleted = TRUE ORDER BY name ASC"
        with self.db as db:
            rows = db.query(query)
            return [SystemPrompt(**row) for row in rows]

    def delete(self, id: int) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))