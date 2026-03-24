# repository/system_prompt_history_repository.py
import logging
from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.history_table.system_prompt_history import SystemPromptHistory

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class SystemPromptHistoryRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "system_prompt_history"

    def _prepare_params(self, entity: SystemPromptHistory, include_id: bool = False) -> Dict[str, Any]:
        """Mapping dei parametri per lo storico dei prompt."""
        params = {
            "system_prompt_id": entity.system_prompt_id,
            "name": entity.name,
            "description": entity.description,
            "prompt": entity.prompt,
            "_lang": entity._lang,
            "version": entity.version,
            "length": entity.length,
            "issued": entity.issued,
            "modified": entity.modified
        }
        if include_id and entity.id:
            params["id"] = entity.id
        return params

    def insert(self, entity: SystemPromptHistory) -> Optional[SystemPromptHistory]:
        """Inserimento append-only dello storico prompt."""
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return SystemPromptHistory(**row) if row else None

    def get_by_system_prompt_id(self, system_prompt_id: int) -> List[SystemPromptHistory]:
        """Recupera la cronologia di un prompt specifico per ID, ordinata per data decrescente."""
        query = f"SELECT * FROM {self.table} WHERE system_prompt_id = %s ORDER BY modified DESC"
        with self.db as db:
            rows = db.query(query, (system_prompt_id,))
            return [SystemPromptHistory(**row) for row in rows]

    def get_all(self) -> List[SystemPromptHistory]:
        """Recupera tutto lo storico dei prompt di sistema."""
        query = f"SELECT * FROM {self.table} ORDER BY modified DESC"
        with self.db as db:
            rows = db.query(query)
            return [SystemPromptHistory(**row) for row in rows]

    def get_by_id(self, id: int) -> Optional[SystemPromptHistory]:
        """Recupera un singolo snapshot storico tramite ID."""
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return SystemPromptHistory(**row) if row else None